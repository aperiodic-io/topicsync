// Package retire deletes one topic on purpose, the only way topicsync deletes a topic in production.
//
// Retiring is a manual command, never part of `apply`, and it refuses unless every guard passes:
//   - the topic is no longer in the desired-state file (remove it there first, so that apply would not recreate it);
//   - the topic is not protected (internal topics, the file's protected patterns);
//   - no consumer group has a member assigned to it now;
//   - every consumer group that committed on it has read it to the end (lag 0), so nothing unread is lost;
//   - its newest record is older than the idle threshold, and its end offsets do not move over a wait: record
//     timestamps are the producer's to set (event time, a backfill), so they alone cannot prove nothing writes it;
//   - the caller confirmed with the topic's exact name.
package retire

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aperiodic-io/topicsync/internal/cluster"
	"github.com/aperiodic-io/topicsync/spec"
)

// Options are a retirement's checks.
type Options struct {
	// Confirm must equal the topic's name for the topic to be deleted; anything else is a dry run.
	Confirm string
	// MinIdle is how old the newest record must be.
	MinIdle time.Duration
	// WriteCheck is how long the end offsets are watched for a write; 0 skips the check.
	WriteCheck time.Duration
	// Sleep waits; tests replace it.
	Sleep func(ctx context.Context, d time.Duration) error
	// Now is the current time; tests replace it.
	Now func() time.Time
}

// Refusal is a guard that did not pass.
type Refusal struct {
	Reasons []string
}

func (r *Refusal) Error() string {
	return "refusing to retire the topic:\n  - " + strings.Join(r.Reasons, "\n  - ")
}

// Report is what a retirement found and did.
type Report struct {
	Topic   string
	Groups  []cluster.GroupUse
	Newest  time.Time
	Empty   bool
	Deleted bool
	// DryRun is set when every guard passed but Confirm did not name the topic.
	DryRun bool
}

// ErrNotFound is the error for a topic that does not exist.
var ErrNotFound = errors.New("no such topic on the cluster")

// Retire checks every guard and, if all pass and the name is confirmed, deletes the topic.
func Retire(ctx context.Context, c cluster.Cluster, d *spec.Desired, topic string, o Options) (Report, error) {
	if o.Now == nil {
		o.Now = time.Now
	}
	rep := Report{Topic: topic}
	if o.Sleep == nil {
		o.Sleep = func(ctx context.Context, d time.Duration) error {
			t := time.NewTimer(d)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
				return nil
			}
		}
	}
	state, err := c.Observe(ctx, nil)
	if err != nil {
		return rep, fmt.Errorf("observing the cluster: %w", err)
	}
	t, exists := state.Topics[topic]
	if !exists {
		return rep, fmt.Errorf("%s: %w", topic, ErrNotFound)
	}

	var reasons []string
	if _, listed := d.Topics[topic]; listed {
		reasons = append(reasons, "it is still in the desired-state file: remove it there first, or apply would create it again")
	}
	if t.Internal || d.IsProtected(topic) {
		reasons = append(reasons, "it is protected (an internal topic, or matched by a protected pattern)")
	}
	groups, err := c.GroupUses(ctx, topic)
	if err != nil {
		return rep, fmt.Errorf("listing the consumer groups that use it: %w", err)
	}
	rep.Groups = groups
	for _, g := range groups {
		if g.ActiveMembers > 0 {
			reasons = append(reasons, fmt.Sprintf("consumer group %s has %d active member(s) on it (state %s)", g.Group, g.ActiveMembers, g.State))
		}
		if g.Committed && g.Lag > 0 {
			reasons = append(reasons, fmt.Sprintf("consumer group %s has not read it to the end (lag %d): its records would be lost unread", g.Group, g.Lag))
		}
	}
	newest, has, err := c.NewestRecordTime(ctx, topic)
	if err != nil {
		return rep, fmt.Errorf("reading its newest record: %w", err)
	}
	rep.Newest, rep.Empty = newest, !has
	if has {
		if age := o.Now().Sub(newest); age < o.MinIdle {
			reasons = append(reasons, fmt.Sprintf("its newest record is %s old, less than --min-idle %s: something may still write it", age.Round(time.Second), o.MinIdle))
		}
	}
	if o.WriteCheck > 0 && len(reasons) == 0 {
		before, err := c.EndOffsets(ctx, topic)
		if err != nil {
			return rep, fmt.Errorf("reading its end offsets: %w", err)
		}
		if err := o.Sleep(ctx, o.WriteCheck); err != nil {
			return rep, err
		}
		after, err := c.EndOffsets(ctx, topic)
		if err != nil {
			return rep, fmt.Errorf("reading its end offsets: %w", err)
		}
		for p, end := range after {
			if end != before[p] {
				reasons = append(reasons, fmt.Sprintf("partition %d grew from offset %d to %d within %s: something writes it", p, before[p], end, o.WriteCheck))
			}
		}
		if len(after) != len(before) {
			reasons = append(reasons, fmt.Sprintf("its partition count changed from %d to %d during the write check", len(before), len(after)))
		}
	}
	if len(reasons) > 0 {
		return rep, &Refusal{Reasons: reasons}
	}
	if o.Confirm != topic {
		rep.DryRun = true
		return rep, nil
	}
	if err := c.DeleteTopic(ctx, topic); err != nil {
		return rep, fmt.Errorf("deleting %s: %w", topic, err)
	}
	rep.Deleted = true
	return rep, nil
}
