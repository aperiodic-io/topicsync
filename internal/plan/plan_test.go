package plan

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/aperiodic-io/topicsync/internal/cluster"
	"github.com/aperiodic-io/topicsync/spec"
)

func desired(t *testing.T, prune bool, topics ...spec.Topic) *spec.Desired {
	t.Helper()
	f := spec.File{
		APIVersion: spec.APIVersion, Cluster: "c",
		Defaults: spec.Defaults{Partitions: 1, ReplicationFactor: 3, Configs: map[string]string{"cleanup.policy": "delete"}},
		Topics:   topics,
		Prune:    spec.PruneRule{Enabled: prune, Patterns: []string{`^m\..*$`}},
	}
	if !prune {
		f.Prune.Patterns = nil
	}
	d, err := f.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func observe(t *testing.T, f *cluster.Fake) cluster.State {
	t.Helper()
	s, err := f.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func kinds(p *Plan) string {
	var parts []string
	for _, a := range p.Actions {
		parts = append(parts, string(a.Kind)+":"+a.Topic)
	}
	return strings.Join(parts, " ")
}

func TestMissingTopicsAreCreatedWithTheirResolvedSettings(t *testing.T) {
	d := desired(t, false, spec.Topic{Name: "b", Partitions: 4, Configs: map[string]string{"retention.ms": "1000"}}, spec.Topic{Name: "a"})
	p, err := Compute(d, cluster.State{Topics: map[string]cluster.TopicState{}}, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if kinds(p) != "create:a create:b" {
		t.Fatalf("actions %s", kinds(p))
	}
	b := p.Actions[1]
	if b.Partitions != 4 || b.ReplicationFactor != 3 || b.Configs["retention.ms"] != "1000" || b.Configs["cleanup.policy"] != "delete" {
		t.Errorf("create b: %+v", b)
	}
}

// Only the keys the file names are managed: a key set by hand on the cluster, or a broker default the file does
// not name, is never in the plan.
func TestOnlyNamedConfigKeysAreCompared(t *testing.T) {
	f := cluster.NewFake(map[string]string{"cleanup.policy": "delete", "retention.ms": "604800000", "compression.type": "producer"})
	f.Put("a", 1, 3, map[string]string{"retention.ms": "1", "max.message.bytes": "999"})
	d := desired(t, false, spec.Topic{Name: "a", Configs: map[string]string{"retention.ms": "2"}})
	p, err := Compute(d, observe(t, f), DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if kinds(p) != "alter_configs:a" {
		t.Fatalf("actions %s", kinds(p))
	}
	ch := p.Actions[0].Changes
	if len(ch) != 1 || ch[0].Key != "retention.ms" || ch[0].From != "1" || ch[0].FromSource != "topic" || ch[0].To != "2" {
		t.Errorf("changes %+v: only retention.ms, from its topic value 1 to 2", ch)
	}
}

// A value equal to the desired one is not set again, even when it comes from a default rather than the topic: setting
// it would be a no-op write on every run.
func TestAValueEqualToADefaultIsNotSetAgain(t *testing.T) {
	f := cluster.NewFake(map[string]string{"cleanup.policy": "delete"})
	f.Put("a", 1, 3, nil)
	p, err := Compute(desired(t, false, spec.Topic{Name: "a"}), observe(t, f), DefaultOptions())
	if err != nil || !p.Empty() || p.Unchanged != 1 {
		t.Fatalf("plan %s, err %v, unchanged %d", kinds(p), err, p.Unchanged)
	}
}

func TestAKeyTheBrokerDoesNotReportIsSet(t *testing.T) {
	f := cluster.NewFake(nil)
	f.Put("a", 1, 3, nil)
	p, err := Compute(desired(t, false, spec.Topic{Name: "a"}), observe(t, f), DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if kinds(p) != "alter_configs:a" || p.Actions[0].Changes[0].FromSource != "absent" {
		t.Fatalf("plan %+v", p.Actions)
	}
}

func TestSensitiveConfigsAreReportedNotSet(t *testing.T) {
	s := cluster.State{Topics: map[string]cluster.TopicState{"a": {Name: "a", Partitions: 1, ReplicationFactor: 3, Configs: map[string]cluster.ConfigValue{
		"cleanup.policy": {Value: "delete"}, "secret": {Sensitive: true},
	}}}}
	p, err := Compute(desired(t, false, spec.Topic{Name: "a", Configs: map[string]string{"secret": "x"}}), s, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if !p.Empty() || p.WarningCount(SensitiveConfig) != 1 {
		t.Fatalf("actions %s warnings %+v", kinds(p), p.Warnings)
	}
}

// Kafka can add partitions but not remove them; and topicsync does not reassign replicas. Both differences are
// reported, never acted on.
func TestPartitionsGrowButNeverShrinkAndReplicationIsOnlyReported(t *testing.T) {
	f := cluster.NewFake(map[string]string{"cleanup.policy": "delete"})
	f.Put("grow", 2, 3, nil)
	f.Put("shrink", 8, 3, nil)
	f.Put("rf", 1, 1, nil)
	d := desired(t, false, spec.Topic{Name: "grow", Partitions: 5}, spec.Topic{Name: "shrink", Partitions: 4}, spec.Topic{Name: "rf"})
	p, err := Compute(d, observe(t, f), DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if kinds(p) != "add_partitions:grow" || p.Actions[0].FromPartitions != 2 || p.Actions[0].ToPartitions != 5 {
		t.Fatalf("actions %+v", p.Actions)
	}
	if p.WarningCount(PartitionsAboveDesired) != 1 || p.WarningCount(ReplicationFactorDiffers) != 1 {
		t.Errorf("warnings %+v", p.Warnings)
	}
	if p.Unchanged != 2 {
		t.Errorf("unchanged %d: shrink and rf change nothing", p.Unchanged)
	}
}

// Without both opt-ins nothing is deleted: a topic that is not listed is left alone. This is the property that
// keeps production's data safe from a truncated or wrong file.
func TestNothingIsDeletedWithoutBothOptIns(t *testing.T) {
	f := cluster.NewFake(map[string]string{"cleanup.policy": "delete"})
	for i := range 10 {
		f.Put(fmt.Sprintf("m.%d", i), 1, 3, nil)
	}
	f.Put("keep", 1, 3, nil)
	listed := spec.Topic{Name: "keep"}

	// prune disabled in the file: no delete, no warning
	p, err := Compute(desired(t, false, listed), observe(t, f), DefaultOptions())
	if err != nil || p.Count(Delete) != 0 || p.WarningCount(PruneNotRequested) != 0 {
		t.Fatalf("file without prune: %s %v", kinds(p), err)
	}
	// prune enabled in the file, not requested: warnings only
	p, err = Compute(desired(t, true, listed), observe(t, f), DefaultOptions())
	if err != nil || p.Count(Delete) != 0 || p.WarningCount(PruneNotRequested) != 10 {
		t.Fatalf("not requested: %s, %d warnings, %v", kinds(p), p.WarningCount(PruneNotRequested), err)
	}
	// requested but disabled in the file: refused
	o := DefaultOptions()
	o.Prune = true
	if _, err := Compute(desired(t, false, listed), observe(t, f), o); !errors.Is(err, ErrPruneNotEnabled) {
		t.Fatalf("requested without the file's opt-in: %v", err)
	}
}

func TestPruneDeletesOnlyMatchingUnlistedUnprotected(t *testing.T) {
	f := cluster.NewFake(map[string]string{"cleanup.policy": "delete"})
	for i := range 8 {
		f.Put(fmt.Sprintf("m.%d", i), 1, 3, nil)
	}
	f.Put("other", 1, 3, nil)       // matches no pattern
	f.Put("_m.internal", 1, 3, nil) // protected
	var listed []spec.Topic
	for i := range 7 {
		listed = append(listed, spec.Topic{Name: fmt.Sprintf("m.%d", i)})
	}
	o := DefaultOptions()
	o.Prune = true
	p, err := Compute(desired(t, true, listed...), observe(t, f), o)
	if err != nil {
		t.Fatal(err)
	}
	if kinds(p) != "delete:m.7" {
		t.Fatalf("actions %s: only m.7", kinds(p))
	}
}

// A truncated file makes every matching topic look prunable: the caps refuse such a run instead of deleting.
func TestPruneCapsRefuseALargeDeletion(t *testing.T) {
	f := cluster.NewFake(map[string]string{"cleanup.policy": "delete"})
	for i := range 100 {
		f.Put(fmt.Sprintf("m.%03d", i), 1, 3, nil)
	}
	keep := []spec.Topic{{Name: "m.000"}}
	o := DefaultOptions()
	o.Prune = true

	_, err := Compute(desired(t, true, keep...), observe(t, f), o)
	var pl *PruneLimitError
	if !errors.As(err, &pl) || pl.Deletes != 99 || pl.Matching != 100 {
		t.Fatalf("99 of 100 deletes: %v", err)
	}
	if !strings.Contains(err.Error(), "m.001") {
		t.Errorf("the error names no topic: %v", err)
	}

	// under the count cap, over the fraction cap
	var many []spec.Topic
	for i := range 70 {
		many = append(many, spec.Topic{Name: fmt.Sprintf("m.%03d", i)})
	}
	o.PruneMaxDeletes = 50
	o.PruneMaxFraction = 0.25
	if _, err := Compute(desired(t, true, many...), observe(t, f), o); !errors.As(err, &pl) {
		t.Fatalf("30 of 100 (30%% > 25%%): %v", err)
	}
	o.PruneMaxFraction = 0.30
	p, err := Compute(desired(t, true, many...), observe(t, f), o)
	if err != nil || p.Count(Delete) != 30 {
		t.Fatalf("30 of 100 at a 30%% cap: %v, %d deletes", err, p.Count(Delete))
	}
	o.PruneMaxDeletes = 0
	if _, err := Compute(desired(t, true, many...), observe(t, f), o); !errors.As(err, &pl) {
		t.Fatalf("max deletes 0: %v", err)
	}
}

func TestAnEmptyFileIsRefusedUnlessAllowed(t *testing.T) {
	d := desired(t, false)
	if _, err := Compute(d, cluster.State{}, DefaultOptions()); !errors.Is(err, ErrEmpty) {
		t.Fatalf("empty: %v", err)
	}
	o := DefaultOptions()
	o.AllowEmpty = true
	if p, err := Compute(d, cluster.State{}, o); err != nil || !p.Empty() {
		t.Fatalf("allowed: %v", err)
	}
}

func TestActionsRunInOrderCreateAlterPartitionsDelete(t *testing.T) {
	f := cluster.NewFake(map[string]string{"cleanup.policy": "delete"})
	f.Put("m.alter", 1, 3, map[string]string{"cleanup.policy": "compact"})
	f.Put("m.grow", 1, 3, nil)
	for i := range 10 {
		f.Put(fmt.Sprintf("m.keep%d", i), 1, 3, nil)
	}
	f.Put("m.gone", 1, 3, nil)
	topics := []spec.Topic{{Name: "m.new"}, {Name: "m.alter"}, {Name: "m.grow", Partitions: 2}}
	for i := range 10 {
		topics = append(topics, spec.Topic{Name: fmt.Sprintf("m.keep%d", i)})
	}
	o := DefaultOptions()
	o.Prune = true
	p, err := Compute(desired(t, true, topics...), observe(t, f), o)
	if err != nil {
		t.Fatal(err)
	}
	if got := kinds(p); got != "create:m.new alter_configs:m.alter add_partitions:m.grow delete:m.gone" {
		t.Fatalf("order %s", got)
	}
	text := p.Text()
	for _, want := range []string{"+ create m.new", "~ alter m.alter: cleanup.policy \"compact\" (topic) -> \"delete\"", "~ partitions m.grow: 1 -> 2", "- delete m.gone", "1 to create, 1 to alter, 1 to repartition, 1 to delete"} {
		if !strings.Contains(text, want) {
			t.Errorf("Text() lacks %q:\n%s", want, text)
		}
	}
}

func TestInternalTopicsAreNeverPrunedEvenIfMatched(t *testing.T) {
	s := cluster.State{Topics: map[string]cluster.TopicState{"m.x": {Name: "m.x", Internal: true, Partitions: 1, ReplicationFactor: 3}}}
	o := DefaultOptions()
	o.Prune = true
	p, err := Compute(desired(t, true, spec.Topic{Name: "m.keep"}), s, o)
	if err != nil || p.Count(Delete) != 0 {
		t.Fatalf("%s %v", kinds(p), err)
	}
}

// Property: for any desired state and any cluster, applying the plan and planning again leaves nothing to do; no
// delete appears without both opt-ins; a delete only ever hits a matching, unlisted, unprotected topic; and no config
// key outside the file is ever changed.
func TestPlanThenApplyConverges(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	keys := []string{"cleanup.policy", "retention.ms", "retention.bytes", "segment.bytes", "max.message.bytes"}
	vals := []string{"1", "2", "delete", "compact", "1000"}
	for iter := range 2000 {
		f := cluster.NewFake(map[string]string{"cleanup.policy": "delete", "retention.ms": "604800000"})
		names := []string{"m.a", "m.b", "m.c", "m.d", "x.a", "x.b", "_m.int", "m.e", "m.f", "m.g"}
		// a random cluster
		for _, n := range names {
			if r.IntN(2) == 0 {
				continue
			}
			c := map[string]string{}
			for _, k := range keys {
				if r.IntN(3) == 0 {
					c[k] = vals[r.IntN(len(vals))]
				}
			}
			f.Put(n, int32(1+r.IntN(4)), int16(1+r.IntN(3)), c)
		}
		before := observe(t, f)
		// a random desired state
		var topics []spec.Topic
		for _, n := range names[:8] {
			if r.IntN(2) == 0 || strings.HasPrefix(n, "_") {
				continue
			}
			tp := spec.Topic{Name: n, Partitions: int32(1 + r.IntN(4)), Configs: map[string]string{}}
			for _, k := range keys[:3] {
				if r.IntN(2) == 0 {
					tp.Configs[k] = vals[r.IntN(len(vals))]
				}
			}
			topics = append(topics, tp)
		}
		if len(topics) == 0 {
			topics = append(topics, spec.Topic{Name: "m.a"})
		}
		pruneFile, pruneRun := r.IntN(2) == 0, r.IntN(2) == 0
		d := desired(t, pruneFile, topics...)
		o := Options{Prune: pruneRun && pruneFile, PruneMaxDeletes: 100, PruneMaxFraction: 1}
		p, err := Compute(d, before, o)
		if err != nil {
			t.Fatalf("iter %d: %v", iter, err)
		}
		for _, a := range p.Actions {
			if a.Kind == Delete {
				if !o.Prune {
					t.Fatalf("iter %d: delete %s without both opt-ins", iter, a.Topic)
				}
				if !d.Prunable(a.Topic) || before.Topics[a.Topic].Internal {
					t.Fatalf("iter %d: delete of %s, which is not prunable", iter, a.Topic)
				}
			}
		}
		applyToFake(t, f, p)
		after := observe(t, f)
		again, err := Compute(d, after, Options{PruneMaxDeletes: 100, PruneMaxFraction: 1})
		if err != nil {
			t.Fatal(err)
		}
		if !again.Empty() {
			t.Fatalf("iter %d: not converged, left %s", iter, kinds(again))
		}
		// an oracle independent of the planner: every listed topic exists with at least its partitions and every
		// desired config value (the planner checking itself cannot catch a planner that plans too little)
		for name, dt := range d.Topics {
			a, ok := after.Topics[name]
			if !ok {
				t.Fatalf("iter %d: listed topic %s missing after applying", iter, name)
			}
			if a.Partitions < dt.Partitions {
				t.Fatalf("iter %d: %s has %d partitions, %d desired", iter, name, a.Partitions, dt.Partitions)
			}
			for k, v := range dt.Configs {
				if a.Configs[k].Value != v {
					t.Fatalf("iter %d: %s %s = %q, %q desired", iter, name, k, a.Configs[k].Value, v)
				}
			}
		}
		// keys outside the file never change on an existing topic
		for name, b := range before.Topics {
			a, ok := after.Topics[name]
			if !ok {
				continue
			}
			want := map[string]bool{}
			if dt, listed := d.Topics[name]; listed {
				for k := range dt.Configs {
					want[k] = true
				}
			}
			for k, bv := range b.Configs {
				if !want[k] && a.Configs[k] != bv {
					t.Fatalf("iter %d: %s's unmanaged key %s changed from %+v to %+v", iter, name, k, bv, a.Configs[k])
				}
			}
		}
	}
}

func applyToFake(t *testing.T, f *cluster.Fake, p *Plan) {
	t.Helper()
	ctx := context.Background()
	for _, a := range p.Actions {
		var err error
		switch a.Kind {
		case Create:
			err = f.CreateTopic(ctx, spec.DesiredTopic{Name: a.Topic, Partitions: a.Partitions, ReplicationFactor: a.ReplicationFactor, Configs: a.Configs})
		case AlterConfigs:
			set := map[string]string{}
			for _, c := range a.Changes {
				set[c.Key] = c.To
			}
			err = f.AlterConfigs(ctx, a.Topic, set)
		case AddPartitions:
			err = f.SetPartitions(ctx, a.Topic, a.ToPartitions)
		case Delete:
			err = f.DeleteTopic(ctx, a.Topic)
		}
		if err != nil {
			t.Fatalf("%s %s: %v", a.Kind, a.Topic, err)
		}
	}
}

func TestUnmanagedCountsUnlistedTopicsButNotInternalOrProtected(t *testing.T) {
	f := cluster.NewFake(map[string]string{"cleanup.policy": "delete"})
	f.Put("m.listed", 1, 3, nil)
	f.Put("stray.a", 1, 3, nil)
	f.Put("stray.b", 1, 3, nil)
	f.Put("_schemas", 1, 3, nil)
	p, err := Compute(desired(t, false, spec.Topic{Name: "m.listed"}), observe(t, f), DefaultOptions())
	if err != nil || p.Unmanaged != 2 {
		t.Fatalf("unmanaged %d err %v", p.Unmanaged, err)
	}
}
