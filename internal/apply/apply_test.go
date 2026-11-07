package apply

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aperiodic-io/topicsync/internal/cluster"
	"github.com/aperiodic-io/topicsync/internal/plan"
	"github.com/aperiodic-io/topicsync/spec"
)

type sleeps struct {
	mu  sync.Mutex
	all []time.Duration
}

func (s *sleeps) sleep(ctx context.Context, d time.Duration) error {
	s.mu.Lock()
	s.all = append(s.all, d)
	s.mu.Unlock()
	return ctx.Err()
}

func (s *sleeps) count(d time.Duration) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, x := range s.all {
		if x == d {
			n++
		}
	}
	return n
}

func opts(s *sleeps) Options {
	return Options{BatchSize: 2, BatchPause: 7 * time.Second, LeaderTimeout: time.Hour, LeaderPoll: time.Millisecond, Retries: 3,
		RetryBackoff: 100 * time.Millisecond, Sleep: s.sleep}
}

func desiredN(t *testing.T, n int, prune bool) *spec.Desired {
	t.Helper()
	f := spec.File{APIVersion: spec.APIVersion, Cluster: "c", Defaults: spec.Defaults{Partitions: 2, ReplicationFactor: 1, Configs: map[string]string{"cleanup.policy": "delete"}}}
	for i := range n {
		f.Topics = append(f.Topics, spec.Topic{Name: fmt.Sprintf("m.%02d", i)})
	}
	if prune {
		f.Prune = spec.PruneRule{Enabled: true, Patterns: []string{`^m\..*$`}}
	}
	d, err := f.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// Creation is paced: a batch of topics, then a wait until their partitions have leaders, then a pause, then the next.
func TestCreatesArePacedInBatchesAndWaitForLeaders(t *testing.T) {
	f := cluster.NewFake(map[string]string{"cleanup.policy": "delete"})
	f.LeaderDelay = 3 // every new topic needs three leader checks
	var s sleeps
	out, err := Run(context.Background(), f, desiredN(t, 5, false), plan.DefaultOptions(), opts(&s), false)
	if err != nil {
		t.Fatal(err)
	}
	if out.Result.Executed[plan.Create] != 5 || f.Len() != 5 {
		t.Fatalf("created %d, cluster has %d", out.Result.Executed[plan.Create], f.Len())
	}
	if got := s.count(7 * time.Second); got != 2 {
		t.Errorf("%d pauses between 3 batches of 2, want 2", got)
	}
	if got := s.count(time.Millisecond); got < 3*3 {
		t.Errorf("%d leader polls, want at least 3 per batch", got)
	}
	if !out.Final.Empty() {
		t.Errorf("not converged: %s", out.Final.Text())
	}
}

func TestLeadersThatNeverComeFailTheBatchButNotTheOthers(t *testing.T) {
	f := cluster.NewFake(map[string]string{"cleanup.policy": "delete"})
	f.LeaderDelay = 1 << 30
	var s sleeps
	o := opts(&s)
	o.LeaderTimeout = 10 * time.Millisecond
	o.Sleep = func(ctx context.Context, d time.Duration) error { time.Sleep(time.Millisecond); return nil }
	out, err := Run(context.Background(), f, desiredN(t, 3, false), plan.DefaultOptions(), o, false)
	if err == nil || !strings.Contains(err.Error(), "without a leader") {
		t.Fatalf("err %v", err)
	}
	if len(out.Result.Failures) != 3 || f.Len() != 3 {
		t.Errorf("%d failures, %d topics: every topic is created, every batch times out", len(out.Result.Failures), f.Len())
	}
}

func TestRetriableErrorsAreRetriedWithBackoff(t *testing.T) {
	f := cluster.NewFake(map[string]string{"cleanup.policy": "delete"})
	f.FailOn["create:m.00"] = &cluster.RetriableError{Err: errors.New("NOT_CONTROLLER")}
	var s sleeps
	out, err := Run(context.Background(), f, desiredN(t, 1, false), plan.DefaultOptions(), opts(&s), false)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(f.CallsOf("create:m.00")); got != 2 {
		t.Errorf("%d create calls, want 2 (one retry)", got)
	}
	if s.count(100*time.Millisecond) != 1 || out.Result.Executed[plan.Create] != 1 {
		t.Errorf("sleeps %v", s.all)
	}
}

func TestRetriesRunOutAndBackOffExponentially(t *testing.T) {
	f := cluster.NewFake(map[string]string{"cleanup.policy": "delete"})
	f.FailAlways["create:m.00"] = &cluster.RetriableError{Err: errors.New("REQUEST_TIMED_OUT")}
	var s sleeps
	_, err := Run(context.Background(), f, desiredN(t, 1, false), plan.DefaultOptions(), opts(&s), false)
	if err == nil {
		t.Fatal("no error")
	}
	if got := len(f.CallsOf("create:m.00")); got != 4 {
		t.Errorf("%d calls, want 1 + 3 retries", got)
	}
	for _, d := range []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond} {
		if s.count(d) != 1 {
			t.Errorf("no backoff of %s in %v", d, s.all)
		}
	}
}

// One topic's refusal (an invalid config, say) must not stop the others: every action is tried and the run fails at
// the end, naming the failure.
func TestAPermanentFailureIsNotRetriedAndDoesNotStopTheRest(t *testing.T) {
	f := cluster.NewFake(map[string]string{"cleanup.policy": "delete"})
	f.FailAlways["create:m.01"] = errors.New("INVALID_CONFIG: retention.bytes")
	var s sleeps
	out, err := Run(context.Background(), f, desiredN(t, 4, false), plan.DefaultOptions(), opts(&s), false)
	if err == nil || !strings.Contains(err.Error(), "create m.01: INVALID_CONFIG") {
		t.Fatalf("err %v", err)
	}
	if len(f.CallsOf("create:m.01")) != 1 {
		t.Error("a permanent error was retried")
	}
	if f.Len() != 3 || out.Result.Executed[plan.Create] != 3 {
		t.Errorf("%d topics created, want the 3 others", f.Len())
	}
}

// Someone else created a topic between observing and creating: not a failure; its configs are then verified.
func TestATopicCreatedMeanwhileIsAlreadyDoneAndVerified(t *testing.T) {
	f := cluster.NewFake(map[string]string{"cleanup.policy": "delete"})
	d := desiredN(t, 1, false)
	state, _ := f.Observe(context.Background())
	p, err := plan.Compute(d, state, plan.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	f.Put("m.00", 2, 1, map[string]string{"cleanup.policy": "delete"})
	var s sleeps
	r := Execute(context.Background(), f, p, opts(&s))
	if r.AlreadyDone != 1 || r.Err() != nil {
		t.Fatalf("result %+v", r)
	}

	// and if what they created differs, the run does not converge in this pass but says so
	f2 := cluster.NewFake(map[string]string{"cleanup.policy": "delete"})
	f2.Put("m.00", 2, 1, map[string]string{"cleanup.policy": "compact"})
	r = Execute(context.Background(), f2, p, opts(&s))
	state, _ = f2.Observe(context.Background())
	again, _ := plan.Compute(d, state, plan.DefaultOptions())
	if r.AlreadyDone != 1 || again.Count(plan.AlterConfigs) != 1 {
		t.Fatalf("another creator's config is not left for the verification to find: %+v %s", r, again.Text())
	}
}

// A cluster that accepts a change but does not keep it must fail the run: the verification is what tells.
func TestARunThatDoesNotConvergeFails(t *testing.T) {
	f := &ignoringAlters{Fake: cluster.NewFake(map[string]string{"cleanup.policy": "delete"})}
	f.Put("m.00", 2, 1, map[string]string{"cleanup.policy": "compact"})
	var s sleeps
	out, err := Run(context.Background(), f, desiredN(t, 1, false), plan.DefaultOptions(), opts(&s), false)
	if !errors.Is(err, ErrNotConverged) {
		t.Fatalf("err %v", err)
	}
	if out.Final.Count(plan.AlterConfigs) != 1 || !strings.Contains(err.Error(), "cleanup.policy") {
		t.Errorf("final %s, err %v", out.Final.Text(), err)
	}
}

type ignoringAlters struct{ *cluster.Fake }

func (i *ignoringAlters) AlterConfigs(context.Context, string, map[string]string) error { return nil }

func TestDryRunChangesNothing(t *testing.T) {
	f := cluster.NewFake(map[string]string{"cleanup.policy": "delete"})
	var s sleeps
	out, err := Run(context.Background(), f, desiredN(t, 3, false), plan.DefaultOptions(), opts(&s), true)
	if err != nil || out.Plan.Count(plan.Create) != 3 || len(f.Calls) != 0 {
		t.Fatalf("dry run: %v, %d creates planned, calls %v", err, out.Plan.Count(plan.Create), f.Calls)
	}
}

func TestPruneDeletesAndAGoneTopicIsAlreadyDone(t *testing.T) {
	f := cluster.NewFake(map[string]string{"cleanup.policy": "delete"})
	d := desiredN(t, 10, true)
	for _, n := range d.Names() {
		f.Put(n, 2, 1, map[string]string{"cleanup.policy": "delete"})
	}
	f.Put("m.gone1", 2, 1, nil)
	f.Put("m.gone2", 2, 1, nil)
	f.FailOn["delete:m.gone2"] = fmt.Errorf("x: %w", cluster.ErrUnknownTopic) // deleted by someone else meanwhile
	po := plan.DefaultOptions()
	po.Prune = true
	var s sleeps
	out, err := Run(context.Background(), f, d, po, opts(&s), false)
	if err != nil {
		t.Fatal(err)
	}
	if out.Result.Executed[plan.Delete] != 1 || out.Result.AlreadyDone != 1 || f.Has("m.gone1") {
		t.Fatalf("result %+v", out.Result)
	}
}

func TestAlterAndPartitionFailuresAreCollected(t *testing.T) {
	f := cluster.NewFake(map[string]string{"cleanup.policy": "delete"})
	f.Put("m.00", 1, 1, map[string]string{"cleanup.policy": "compact"})
	f.Put("m.01", 1, 1, map[string]string{"cleanup.policy": "delete"})
	f.FailAlways["alter:m.00"] = errors.New("POLICY_VIOLATION")
	f.FailAlways["partitions:m.01"] = errors.New("INVALID_PARTITIONS")
	var s sleeps
	out, err := Run(context.Background(), f, desiredN(t, 2, false), plan.DefaultOptions(), opts(&s), false)
	// m.00's alter fails, m.01's partitions fail; m.00's partitions and m.01's (unneeded) alter are fine
	if err == nil || len(out.Result.Failures) != 2 {
		t.Fatalf("failures %+v err %v", out.Result.Failures, err)
	}
	for _, want := range []string{"alter_configs m.00: POLICY_VIOLATION", "add_partitions m.01: INVALID_PARTITIONS"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error lacks %q: %v", want, err)
		}
	}
	if f.Partitions("m.00") != 2 {
		t.Error("m.00's partitions were not raised although its own action could succeed")
	}
}

func TestACancelledRunFailsTheRemainingActions(t *testing.T) {
	f := cluster.NewFake(map[string]string{"cleanup.policy": "delete"})
	ctx, cancel := context.WithCancel(context.Background())
	d := desiredN(t, 6, false)
	state, _ := f.Observe(ctx)
	p, _ := plan.Compute(d, state, plan.DefaultOptions())
	o := opts(&sleeps{})
	o.Sleep = func(context.Context, time.Duration) error { cancel(); return context.Canceled }
	r := Execute(ctx, f, p, o)
	if len(r.Failures) == 0 || !errors.Is(r.Failures[len(r.Failures)-1].Err, context.Canceled) {
		t.Fatalf("result %+v", r)
	}
	if f.Len() >= 6 {
		t.Errorf("all %d topics created after the cancellation", f.Len())
	}
}

func TestObserveFailuresAreReported(t *testing.T) {
	f := cluster.NewFake(nil)
	f.ObserveErr = errors.New("all brokers down")
	if _, err := Run(context.Background(), f, desiredN(t, 1, false), plan.DefaultOptions(), opts(&sleeps{}), false); err == nil ||
		!strings.Contains(err.Error(), "observing the cluster: all brokers down") {
		t.Fatalf("err %v", err)
	}
}

func TestAnEmptyPlanExecutesNothing(t *testing.T) {
	f := cluster.NewFake(map[string]string{"cleanup.policy": "delete"})
	d := desiredN(t, 2, false)
	for _, n := range d.Names() {
		f.Put(n, 2, 1, nil)
	}
	out, err := Run(context.Background(), f, d, plan.DefaultOptions(), opts(&sleeps{}), false)
	if err != nil || !out.Plan.Empty() || len(f.Calls) != 0 {
		t.Fatalf("%v %v", err, f.Calls)
	}
}

func TestResultErrListsAtMostTwentyFailures(t *testing.T) {
	var r Result
	for i := range 25 {
		r.Failures = append(r.Failures, Failure{Action: plan.Action{Kind: plan.Create, Topic: fmt.Sprint(i)}, Err: errors.New("x")})
	}
	if msg := r.Err().Error(); !strings.Contains(msg, "25 of the plan's actions failed") || !strings.Contains(msg, "and 5 more") {
		t.Errorf("%s", msg)
	}
}

func TestDefaultsFillZeroOptions(t *testing.T) {
	o := withDefaults(Options{Retries: -1})
	if o.BatchSize != 20 || o.Retries != 0 || o.Logger == nil || o.Sleep == nil || o.LeaderPoll <= 0 || o.LeaderTimeout <= 0 {
		t.Errorf("%+v", o)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleep(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Errorf("sleep on a cancelled context: %v", err)
	}
	if err := sleep(context.Background(), time.Millisecond); err != nil {
		t.Error(err)
	}
}

// A request that reached a stalled broker fails retriably; the observation is tried again rather than failing the run.
func TestARetriableObserveFailureIsRetried(t *testing.T) {
	f := cluster.NewFake(map[string]string{"cleanup.policy": "delete"})
	f.ObserveErrs = []error{&cluster.RetriableError{Err: errors.New("dial tcp: i/o timeout")}, &cluster.RetriableError{Err: errors.New("i/o timeout")}}
	var s sleeps
	out, err := Run(context.Background(), f, desiredN(t, 1, false), plan.DefaultOptions(), opts(&s), false)
	if err != nil || out.Result.Executed[plan.Create] != 1 {
		t.Fatalf("%v %+v", err, out.Result)
	}
	if s.count(100*time.Millisecond) != 1 || s.count(200*time.Millisecond) != 1 {
		t.Errorf("observe backoffs %v", s.all)
	}
	f.ObserveErrs = []error{nil, &cluster.RetriableError{Err: errors.New("x")}}
	f.ObserveErrs = f.ObserveErrs[1:]
	if _, err := Run(context.Background(), f, desiredN(t, 2, false), plan.DefaultOptions(), opts(&s), false); err != nil {
		t.Fatalf("a retriable failure of the verifying observation: %v", err)
	}
}

func TestOutcomeNamesTheFailedStageAndCountsRetries(t *testing.T) {
	f := cluster.NewFake(nil)
	f.ObserveErr = errors.New("down")
	out, _ := Run(context.Background(), f, desiredN(t, 1, false), plan.DefaultOptions(), opts(&sleeps{}), false)
	if out.Stage != StageObserve {
		t.Errorf("stage %q", out.Stage)
	}
	f = cluster.NewFake(map[string]string{"cleanup.policy": "delete"})
	f.FailAlways["create:m.00"] = errors.New("no")
	out, _ = Run(context.Background(), f, desiredN(t, 1, false), plan.DefaultOptions(), opts(&sleeps{}), false)
	if out.Stage != StageExecute {
		t.Errorf("stage %q", out.Stage)
	}
	ig := &ignoringAlters{Fake: cluster.NewFake(map[string]string{"cleanup.policy": "delete"})}
	ig.Put("m.00", 2, 1, map[string]string{"cleanup.policy": "compact"})
	out, _ = Run(context.Background(), ig, desiredN(t, 1, false), plan.DefaultOptions(), opts(&sleeps{}), false)
	if out.Stage != StageVerify {
		t.Errorf("stage %q", out.Stage)
	}
	if _, err := Run(context.Background(), cluster.NewFake(nil), desiredN(t, 0, false), plan.DefaultOptions(), opts(&sleeps{}), false); err == nil {
		t.Error("empty file accepted")
	}
	g := cluster.NewFake(map[string]string{"cleanup.policy": "delete"})
	g.FailOn["create:m.00"] = &cluster.RetriableError{Err: errors.New("x")}
	g.LeaderDelay = 2
	out, err := Run(context.Background(), g, desiredN(t, 1, false), plan.DefaultOptions(), opts(&sleeps{}), false)
	if err != nil || out.Stage != "" || out.Stats.Retries != 1 || out.Stats.LeaderWait <= 0 {
		t.Errorf("%v stage %q stats %+v", err, out.Stage, out.Stats)
	}
}
