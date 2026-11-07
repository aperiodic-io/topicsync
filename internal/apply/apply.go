// Package apply executes a plan against a cluster and verifies the result.
//
// Creation is paced: topics are created in batches, and the next batch waits until every partition of the last one
// has a leader. A broker can report itself unready while partitions are leaderless (a readiness gate), so creating
// hundreds of topics at once can flap a whole cluster's readiness; batches keep that bounded.
//
// One action's failure does not stop the others: every action is tried, failures are collected, and the run fails at
// the end. Errors the cluster marks retriable are retried with backoff. After executing, the cluster is observed again
// and planned again: a run succeeds only if nothing is left to do.
package apply

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aperiodic-io/topicsync/internal/cluster"
	"github.com/aperiodic-io/topicsync/internal/plan"
	"github.com/aperiodic-io/topicsync/spec"
)

// Options tune execution.
type Options struct {
	// BatchSize is how many topics are created (or repartitioned) before waiting for their leaders.
	BatchSize int
	// BatchPause is the pause after a batch's leaders are ready, before the next batch.
	BatchPause time.Duration
	// LeaderTimeout bounds the wait for a batch's leaders; past it the batch's topics are failures.
	LeaderTimeout time.Duration
	// LeaderPoll is how often leaders are checked while waiting.
	LeaderPoll time.Duration
	// Retries is how many times a retriable error is retried.
	Retries int
	// RetryBackoff is the first retry's wait; it doubles each retry.
	RetryBackoff time.Duration
	// Logger receives one line per action.
	Logger *slog.Logger
	// Sleep waits; tests replace it. It returns ctx's error if ctx ends first.
	Sleep func(ctx context.Context, d time.Duration) error

	stats *Stats
}

// Stats are a run's counters, for its metrics.
type Stats struct {
	// Retries counts every retry of a retriable error.
	Retries int
	// LeaderWait is the time spent waiting for new partitions' leaders.
	LeaderWait time.Duration
}

// DefaultOptions are production defaults.
func DefaultOptions() Options {
	return Options{
		BatchSize: 20, BatchPause: 2 * time.Second, LeaderTimeout: 2 * time.Minute, LeaderPoll: 500 * time.Millisecond,
		Retries: 5, RetryBackoff: 500 * time.Millisecond, Logger: slog.Default(), Sleep: sleep,
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Failure is an action that did not succeed.
type Failure struct {
	Action plan.Action
	Err    error
}

// Result is what an execution did.
type Result struct {
	Executed map[plan.Kind]int
	// AlreadyDone counts actions found already done (a topic created by someone else in between, a delete of a topic
	// already gone).
	AlreadyDone int
	Failures    []Failure
}

// Err summarizes the failures, nil if there were none.
func (r Result) Err() error {
	if len(r.Failures) == 0 {
		return nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d of the plan's actions failed:", len(r.Failures))
	for i, f := range r.Failures {
		if i == 20 {
			fmt.Fprintf(&b, "\n  ... and %d more", len(r.Failures)-20)
			break
		}
		fmt.Fprintf(&b, "\n  %s %s: %v", f.Action.Kind, f.Action.Topic, f.Err)
	}
	return errors.New(b.String())
}

// Execute runs a plan's actions in order: creates, config changes, partition increases, deletes.
func Execute(ctx context.Context, c cluster.Cluster, p *plan.Plan, o Options) Result {
	o = withDefaults(o)
	r := Result{Executed: map[plan.Kind]int{}}
	byKind := map[plan.Kind][]plan.Action{}
	for _, a := range p.Actions {
		byKind[a.Kind] = append(byKind[a.Kind], a)
	}

	paced := func(actions []plan.Action, do func(plan.Action) (bool, error)) {
		for start := 0; start < len(actions); start += o.BatchSize {
			if ctx.Err() != nil {
				for _, a := range actions[start:] {
					r.Failures = append(r.Failures, Failure{Action: a, Err: ctx.Err()})
				}
				return
			}
			end := min(start+o.BatchSize, len(actions))
			var done []plan.Action
			for _, a := range actions[start:end] {
				already, err := o.retry(ctx, a, func() error {
					ok, err := do(a)
					if ok && err == nil {
						return errAlreadyDone
					}
					return err
				})
				switch {
				case err != nil:
					r.Failures = append(r.Failures, Failure{Action: a, Err: err})
				case already:
					r.AlreadyDone++
					done = append(done, a)
				default:
					r.Executed[a.Kind]++
					done = append(done, a)
				}
			}
			if len(done) > 0 {
				if err := o.waitLeaders(ctx, c, done); err != nil {
					for _, a := range done {
						r.Failures = append(r.Failures, Failure{Action: a, Err: err})
					}
				}
			}
			if end < len(actions) {
				_ = o.Sleep(ctx, o.BatchPause)
			}
		}
	}

	paced(byKind[plan.Create], func(a plan.Action) (bool, error) {
		err := c.CreateTopic(ctx, spec.DesiredTopic{Name: a.Topic, Partitions: a.Partitions, ReplicationFactor: a.ReplicationFactor, Configs: a.Configs})
		if errors.Is(err, cluster.ErrTopicExists) {
			o.Logger.Warn("topic already exists: created by someone else since it was observed; its configs are verified after this run", "topic", a.Topic)
			return true, nil
		}
		if err == nil {
			o.Logger.Info("created", "topic", a.Topic, "partitions", a.Partitions, "replicationFactor", a.ReplicationFactor)
		}
		return false, err
	})

	for _, a := range byKind[plan.AlterConfigs] {
		if ctx.Err() != nil {
			r.Failures = append(r.Failures, Failure{Action: a, Err: ctx.Err()})
			continue
		}
		set := make(map[string]string, len(a.Changes))
		for _, ch := range a.Changes {
			set[ch.Key] = ch.To
		}
		_, err := o.retry(ctx, a, func() error { return c.AlterConfigs(ctx, a.Topic, set) })
		if err != nil {
			r.Failures = append(r.Failures, Failure{Action: a, Err: err})
			continue
		}
		r.Executed[a.Kind]++
		for _, ch := range a.Changes {
			o.Logger.Info("config set", "topic", a.Topic, "key", ch.Key, "from", ch.From, "fromSource", ch.FromSource, "to", ch.To)
		}
	}

	paced(byKind[plan.AddPartitions], func(a plan.Action) (bool, error) {
		err := c.SetPartitions(ctx, a.Topic, a.ToPartitions)
		if err == nil {
			o.Logger.Info("partitions raised", "topic", a.Topic, "from", a.FromPartitions, "to", a.ToPartitions)
		}
		return false, err
	})

	for _, a := range byKind[plan.Delete] {
		if ctx.Err() != nil {
			r.Failures = append(r.Failures, Failure{Action: a, Err: ctx.Err()})
			continue
		}
		already, err := o.retry(ctx, a, func() error {
			err := c.DeleteTopic(ctx, a.Topic)
			if errors.Is(err, cluster.ErrUnknownTopic) {
				return errAlreadyDone
			}
			return err
		})
		switch {
		case err != nil:
			r.Failures = append(r.Failures, Failure{Action: a, Err: err})
		case already:
			r.AlreadyDone++
		default:
			r.Executed[a.Kind]++
			o.Logger.Warn("deleted (pruned)", "topic", a.Topic)
		}
	}
	return r
}

var errAlreadyDone = errors.New("already done")

// retry runs fn until it succeeds, fails for good, or the retries run out. It reports whether fn found the action
// already done.
func (o Options) retry(ctx context.Context, a plan.Action, fn func() error) (bool, error) {
	backoff := o.RetryBackoff
	for attempt := 0; ; attempt++ {
		err := fn()
		switch {
		case err == nil:
			return false, nil
		case errors.Is(err, errAlreadyDone):
			return true, nil
		case !cluster.IsRetriable(err) || attempt >= o.Retries:
			return false, err
		}
		o.Logger.Warn("retrying", "action", string(a.Kind), "topic", a.Topic, "attempt", attempt+1, "error", err)
		if o.stats != nil {
			o.stats.Retries++
		}
		if serr := o.Sleep(ctx, backoff); serr != nil {
			return false, fmt.Errorf("%w (after: %w)", serr, err)
		}
		backoff *= 2
	}
}

func (o Options) waitLeaders(ctx context.Context, c cluster.Cluster, actions []plan.Action) error {
	topics := make([]string, len(actions))
	for i, a := range actions {
		topics[i] = a.Topic
	}
	start := time.Now()
	if o.stats != nil {
		defer func() { o.stats.LeaderWait += time.Since(start) }()
	}
	deadline := start.Add(o.LeaderTimeout)
	for {
		ready, err := c.LeadersReady(ctx, topics)
		if err == nil && ready {
			return nil
		}
		if err != nil && !cluster.IsRetriable(err) && !errors.Is(err, cluster.ErrUnknownTopic) {
			return fmt.Errorf("checking partition leaders: %w", err)
		}
		if time.Now().After(deadline) {
			if err != nil {
				return fmt.Errorf("partitions without a leader after %s: %w", o.LeaderTimeout, err)
			}
			return fmt.Errorf("partitions without a leader after %s", o.LeaderTimeout)
		}
		if serr := o.Sleep(ctx, o.LeaderPoll); serr != nil {
			return serr
		}
	}
}

func withDefaults(o Options) Options {
	d := DefaultOptions()
	if o.BatchSize <= 0 {
		o.BatchSize = d.BatchSize
	}
	if o.LeaderTimeout <= 0 {
		o.LeaderTimeout = d.LeaderTimeout
	}
	if o.LeaderPoll <= 0 {
		o.LeaderPoll = d.LeaderPoll
	}
	if o.Retries < 0 {
		o.Retries = 0
	}
	if o.RetryBackoff <= 0 {
		o.RetryBackoff = d.RetryBackoff
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.Sleep == nil {
		o.Sleep = sleep
	}
	return o
}

// Outcome is a full run: the plan, what executing it did, and the plan left afterwards.
type Outcome struct {
	Plan   *plan.Plan
	Result Result
	// Final is the plan computed from the cluster after executing; empty when the run converged.
	Final *plan.Plan
	// Stage is where a failed run failed: observe, plan, execute or verify (empty on success).
	Stage Stage
	Stats Stats
	// Durations of the stages.
	ObserveDuration, ExecuteDuration, VerifyDuration time.Duration
}

// Stage is a step of a run.
type Stage string

const (
	StageObserve Stage = "observe"
	StagePlan    Stage = "plan"
	StageExecute Stage = "execute"
	StageVerify  Stage = "verify"
)

// ErrNotConverged is the error for a run whose final plan is not empty although every action succeeded: something
// changed the cluster meanwhile, or the cluster does not keep what it was told (a config it silently ignores).
var ErrNotConverged = errors.New("the cluster still differs from the desired state after applying")

// observe reads the cluster, retrying retriable failures: a request that reached a stalled broker is tried again
// (the client picks a broker per request).
func (o Options) observe(ctx context.Context, c cluster.Cluster) (cluster.State, error) {
	var state cluster.State
	_, err := o.retry(ctx, plan.Action{Kind: "observe"}, func() error {
		var err error
		state, err = c.Observe(ctx)
		return err
	})
	return state, err
}

// Run observes, plans, executes and verifies. dryRun stops after planning.
func Run(ctx context.Context, c cluster.Cluster, d *spec.Desired, po plan.Options, eo Options, dryRun bool) (out Outcome, err error) {
	eo = withDefaults(eo)
	eo.stats = &out.Stats
	t := time.Now()
	state, err := eo.observe(ctx, c)
	out.ObserveDuration = time.Since(t)
	if err != nil {
		out.Stage = StageObserve
		return out, fmt.Errorf("observing the cluster: %w", err)
	}
	p, err := plan.Compute(d, state, po)
	if err != nil {
		out.Stage = StagePlan
		return out, err
	}
	out.Plan = p
	if dryRun || p.Empty() {
		out.Final = p
		return out, nil
	}
	t = time.Now()
	out.Result = Execute(ctx, c, p, eo)
	out.ExecuteDuration = time.Since(t)
	t = time.Now()
	state, err = eo.observe(ctx, c)
	out.VerifyDuration = time.Since(t)
	if err != nil {
		out.Stage = StageVerify
		return out, errors.Join(out.Result.Err(), fmt.Errorf("observing the cluster after applying: %w", err))
	}
	// the verification never prunes: a delete that failed is in Result already
	verify := po
	verify.Prune = false
	final, err := plan.Compute(d, state, verify)
	if err != nil {
		out.Stage = StageVerify
		return out, errors.Join(out.Result.Err(), fmt.Errorf("planning after applying: %w", err))
	}
	out.Final = final
	if rerr := out.Result.Err(); rerr != nil {
		out.Stage = StageExecute
		return out, rerr
	}
	if !final.Empty() {
		out.Stage = StageVerify
		return out, fmt.Errorf("%w:\n%s", ErrNotConverged, final.Text())
	}
	return out, nil
}
