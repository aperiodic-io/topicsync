package retire

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aperiodic-io/topicsync/internal/cluster"
	"github.com/aperiodic-io/topicsync/spec"
)

var now = time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)

func setup(t *testing.T) (*cluster.Fake, *spec.Desired) {
	t.Helper()
	f := cluster.NewFake(nil)
	f.Put("old.topic", 1, 3, nil)
	f.Put("listed", 1, 3, nil)
	f.Put("_schemas", 1, 3, nil)
	f.Put("keep.me", 1, 3, nil)
	f.Newest["old.topic"] = now.Add(-48 * time.Hour)
	d, err := spec.File{APIVersion: spec.APIVersion, Cluster: "c", Defaults: spec.Defaults{Partitions: 1, ReplicationFactor: 3},
		Topics: []spec.Topic{{Name: "listed"}}, Protected: []string{`^keep\.`}}.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	return f, d
}

func o(confirm string) Options {
	return Options{Confirm: confirm, MinIdle: time.Hour, Now: func() time.Time { return now }}
}

func refusal(t *testing.T, err error, want string) {
	t.Helper()
	var r *Refusal
	if !errors.As(err, &r) {
		t.Fatalf("err %v is not a refusal", err)
	}
	if !strings.Contains(r.Error(), want) {
		t.Errorf("refusal %q lacks %q", r.Error(), want)
	}
}

func TestEveryGuardPassingWithoutConfirmIsADryRun(t *testing.T) {
	f, d := setup(t)
	rep, err := Retire(context.Background(), f, d, "old.topic", o(""))
	if err != nil || !rep.DryRun || rep.Deleted || !f.Has("old.topic") {
		t.Fatalf("rep %+v err %v", rep, err)
	}
	rep, err = Retire(context.Background(), f, d, "old.topic", o("old.topi"))
	if err != nil || !rep.DryRun || !f.Has("old.topic") {
		t.Fatalf("a confirmation that is not the exact name deleted it: %+v %v", rep, err)
	}
}

func TestConfirmedItDeletes(t *testing.T) {
	f, d := setup(t)
	rep, err := Retire(context.Background(), f, d, "old.topic", o("old.topic"))
	if err != nil || !rep.Deleted || f.Has("old.topic") {
		t.Fatalf("rep %+v err %v", rep, err)
	}
}

func TestAListedTopicIsRefused(t *testing.T) {
	f, d := setup(t)
	_, err := Retire(context.Background(), f, d, "listed", o("listed"))
	refusal(t, err, "still in the desired-state file")
	if !f.Has("listed") {
		t.Fatal("deleted")
	}
}

func TestProtectedTopicsAreRefused(t *testing.T) {
	f, d := setup(t)
	for _, n := range []string{"_schemas", "keep.me"} {
		_, err := Retire(context.Background(), f, d, n, o(n))
		refusal(t, err, "protected")
		if !f.Has(n) {
			t.Fatalf("%s deleted", n)
		}
	}
}

func TestAnActiveConsumerIsRefused(t *testing.T) {
	f, d := setup(t)
	f.Groups["old.topic"] = []cluster.GroupUse{{Group: "archiver", State: "Stable", ActiveMembers: 2, Committed: true}}
	_, err := Retire(context.Background(), f, d, "old.topic", o("old.topic"))
	refusal(t, err, "consumer group archiver has 2 active member(s)")
}

// A group that has not read the topic to the end would lose those records: an archiver behind, for instance.
func TestALaggingGroupIsRefused(t *testing.T) {
	f, d := setup(t)
	f.Groups["old.topic"] = []cluster.GroupUse{{Group: "archiver", State: "Empty", Committed: true, Lag: 12}}
	_, err := Retire(context.Background(), f, d, "old.topic", o("old.topic"))
	refusal(t, err, "lag 12")
	f.Groups["old.topic"] = []cluster.GroupUse{{Group: "archiver", State: "Empty", Committed: true, Lag: 0}}
	if _, err := Retire(context.Background(), f, d, "old.topic", o("")); err != nil {
		t.Fatalf("a group read to the end is refused: %v", err)
	}
}

func TestARecentWriteIsRefused(t *testing.T) {
	f, d := setup(t)
	f.Newest["old.topic"] = now.Add(-10 * time.Minute)
	_, err := Retire(context.Background(), f, d, "old.topic", o("old.topic"))
	refusal(t, err, "less than --min-idle 1h0m0s")
	if !f.Has("old.topic") {
		t.Fatal("deleted")
	}
}

func TestAnEmptyTopicPassesTheIdleGuard(t *testing.T) {
	f, d := setup(t)
	delete(f.Newest, "old.topic")
	rep, err := Retire(context.Background(), f, d, "old.topic", o(""))
	if err != nil || !rep.Empty || !rep.DryRun {
		t.Fatalf("%+v %v", rep, err)
	}
}

func TestEveryFailingGuardIsListed(t *testing.T) {
	f, d := setup(t)
	f.Groups["listed"] = []cluster.GroupUse{{Group: "g", ActiveMembers: 1, Committed: true, Lag: 3}}
	f.Newest["listed"] = now
	_, err := Retire(context.Background(), f, d, "listed", o("listed"))
	var r *Refusal
	if !errors.As(err, &r) || len(r.Reasons) != 4 {
		t.Fatalf("%v", err)
	}
}

func TestErrorsArePropagated(t *testing.T) {
	f, d := setup(t)
	if _, err := Retire(context.Background(), f, d, "nope", o("nope")); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing topic: %v", err)
	}
	f.FailOn["groups:old.topic"] = errors.New("coordinator down")
	if _, err := Retire(context.Background(), f, d, "old.topic", o("old.topic")); err == nil || !strings.Contains(err.Error(), "coordinator down") || !f.Has("old.topic") {
		t.Errorf("groups error: %v", err)
	}
	f.FailOn["newest:old.topic"] = errors.New("fetch failed")
	if _, err := Retire(context.Background(), f, d, "old.topic", o("old.topic")); err == nil || !f.Has("old.topic") {
		t.Errorf("newest error: %v", err)
	}
	f.FailOn["delete:old.topic"] = errors.New("TOPIC_DELETION_DISABLED")
	if _, err := Retire(context.Background(), f, d, "old.topic", o("old.topic")); err == nil || !strings.Contains(err.Error(), "TOPIC_DELETION_DISABLED") {
		t.Errorf("delete error: %v", err)
	}
	f.ObserveErr = errors.New("down")
	if _, err := Retire(context.Background(), f, d, "old.topic", o("old.topic")); err == nil {
		t.Error("observe error swallowed")
	}
}

func TestNowDefaultsToTheClock(t *testing.T) {
	f, d := setup(t)
	f.Newest["old.topic"] = time.Now().Add(-2 * time.Hour)
	if _, err := Retire(context.Background(), f, d, "old.topic", Options{MinIdle: time.Hour}); err != nil {
		t.Fatal(err)
	}
}

// Record timestamps are the producer's: a topic stamped with event time looks idle while it is written. End offsets
// that move during the write check refuse the retirement.
func TestATopicStillWrittenIsRefusedWhateverItsTimestamps(t *testing.T) {
	f, d := setup(t)
	f.Ends["old.topic"] = []map[int32]int64{{0: 100}, {0: 103}}
	opts := o("old.topic")
	opts.WriteCheck = time.Minute
	var waited time.Duration
	opts.Sleep = func(_ context.Context, d time.Duration) error { waited = d; return nil }
	_, err := Retire(context.Background(), f, d, "old.topic", opts)
	refusal(t, err, "partition 0 grew from offset 100 to 103")
	if waited != time.Minute || !f.Has("old.topic") {
		t.Fatalf("waited %s, exists %v", waited, f.Has("old.topic"))
	}
	f.Ends["old.topic"] = []map[int32]int64{{0: 103}, {0: 103}}
	if rep, err := Retire(context.Background(), f, d, "old.topic", opts); err != nil || !rep.Deleted {
		t.Fatalf("still end offsets: %+v %v", rep, err)
	}
}

func TestEndOffsetErrorsDuringTheWriteCheckAreErrors(t *testing.T) {
	f, d := setup(t)
	f.FailOn["ends:old.topic"] = errors.New("KAFKA_STORAGE_ERROR")
	opts := o("old.topic")
	opts.WriteCheck = time.Second
	opts.Sleep = func(context.Context, time.Duration) error { return nil }
	if _, err := Retire(context.Background(), f, d, "old.topic", opts); err == nil || !f.Has("old.topic") {
		t.Fatalf("an end-offset error let it through: %v", err)
	}
}
