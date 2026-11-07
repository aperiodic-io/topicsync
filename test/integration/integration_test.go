//go:build integration

// Integration tests: the Kafka adapter and full runs against a real broker in Docker. The broker comes from the
// environment (see internal/testbroker): CI runs this package once per supported broker and version.
package integration

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aperiodic-io/topicsync/internal/apply"
	"github.com/aperiodic-io/topicsync/internal/cluster"
	"github.com/aperiodic-io/topicsync/internal/kafka"
	"github.com/aperiodic-io/topicsync/internal/plan"
	"github.com/aperiodic-io/topicsync/internal/retire"
	"github.com/aperiodic-io/topicsync/internal/testbroker"
	"github.com/aperiodic-io/topicsync/spec"
)

func connect(t *testing.T, b *testbroker.Cluster) *kafka.Cluster {
	t.Helper()
	c, err := kafka.New(kafka.Config{Brokers: b.Addrs, RequestTimeout: 20 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func desired(t *testing.T, rf int16, topics ...spec.Topic) *spec.Desired {
	t.Helper()
	d, err := spec.File{APIVersion: spec.APIVersion, Cluster: "it",
		Defaults: spec.Defaults{Partitions: 1, ReplicationFactor: rf, Configs: map[string]string{"cleanup.policy": "delete"}},
		Topics:   topics}.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func eopts() apply.Options {
	o := apply.DefaultOptions()
	o.BatchPause = 100 * time.Millisecond
	o.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return o
}

func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	return c
}

func TestObserveReportsTopicsConfigsAndTheirSources(t *testing.T) {
	b := testbroker.Start(t)
	c := connect(t, b)
	if err := c.CreateTopic(ctx(t), spec.DesiredTopic{Name: "obs.a", Partitions: 3, ReplicationFactor: 1,
		Configs: map[string]string{"retention.ms": "3600000", "cleanup.policy": "delete"}}); err != nil {
		t.Fatal(err)
	}
	s, err := c.Observe(ctx(t))
	if err != nil {
		t.Fatal(err)
	}
	a, ok := s.Topics["obs.a"]
	if !ok {
		t.Fatalf("obs.a missing from %d topics", len(s.Topics))
	}
	if a.Partitions != 3 || a.ReplicationFactor != 1 {
		t.Errorf("partitions %d rf %d", a.Partitions, a.ReplicationFactor)
	}
	if v := a.Configs["retention.ms"]; v.Value != "3600000" || v.Source != cluster.SourceDynamicTopic {
		t.Errorf("retention.ms %+v: set on the topic", v)
	}
	// a config nobody set comes from a default
	if v, ok := a.Configs["max.message.bytes"]; !ok || v.Source != cluster.SourceDefault {
		t.Errorf("max.message.bytes %+v (present %v): a default", v, ok)
	}
}

func TestCreateTwiceIsTopicExistsAndDeleteTwiceIsUnknown(t *testing.T) {
	b := testbroker.Start(t)
	c := connect(t, b)
	tp := spec.DesiredTopic{Name: "twice", Partitions: 1, ReplicationFactor: 1}
	if err := c.CreateTopic(ctx(t), tp); err != nil {
		t.Fatal(err)
	}
	if err := c.CreateTopic(ctx(t), tp); !errors.Is(err, cluster.ErrTopicExists) {
		t.Errorf("second create: %v", err)
	}
	if err := c.DeleteTopic(ctx(t), "twice"); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteTopic(ctx(t), "twice"); !errors.Is(err, cluster.ErrUnknownTopic) {
		t.Errorf("second delete: %v", err)
	}
}

// The property that makes topicsync safe to run over topics others configured: only the keys the file names change.
func TestApplyChangesOnlyTheNamedKeys(t *testing.T) {
	b := testbroker.Start(t)
	c := connect(t, b)
	if err := c.CreateTopic(ctx(t), spec.DesiredTopic{Name: "keys", Partitions: 1, ReplicationFactor: 1,
		Configs: map[string]string{"retention.ms": "1000", "max.message.bytes": "2000000", "cleanup.policy": "delete"}}); err != nil {
		t.Fatal(err)
	}
	d := desired(t, 1, spec.Topic{Name: "keys", Configs: map[string]string{"retention.ms": "7200000"}})
	out, err := apply.Run(ctx(t), c, d, plan.DefaultOptions(), eopts(), false)
	if err != nil {
		t.Fatal(err)
	}
	if out.Result.Executed[plan.AlterConfigs] != 1 {
		t.Fatalf("result %+v", out.Result)
	}
	s, _ := c.Observe(ctx(t))
	k := s.Topics["keys"]
	if k.Configs["retention.ms"].Value != "7200000" {
		t.Errorf("retention.ms %+v", k.Configs["retention.ms"])
	}
	if v := k.Configs["max.message.bytes"]; v.Value != "2000000" || v.Source != cluster.SourceDynamicTopic {
		t.Errorf("an unnamed key set by hand changed: %+v", v)
	}
}

func TestPartitionsGrowAndCannotShrink(t *testing.T) {
	b := testbroker.Start(t)
	c := connect(t, b)
	if err := c.CreateTopic(ctx(t), spec.DesiredTopic{Name: "parts", Partitions: 2, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	out, err := apply.Run(ctx(t), c, desired(t, 1, spec.Topic{Name: "parts", Partitions: 5}), plan.DefaultOptions(), eopts(), false)
	if err != nil || out.Result.Executed[plan.AddPartitions] != 1 {
		t.Fatalf("%v %+v", err, out.Result)
	}
	if err := c.SetPartitions(ctx(t), "parts", 3); err == nil {
		t.Error("the broker lowered a partition count")
	}
	p, err := apply.Run(ctx(t), c, desired(t, 1, spec.Topic{Name: "parts", Partitions: 4}), plan.DefaultOptions(), eopts(), false)
	if err != nil || !p.Plan.Empty() || p.Plan.WarningCount(plan.PartitionsAboveDesired) != 1 {
		t.Fatalf("%v %s", err, p.Plan.Text())
	}
}

// Creating hundreds of topics is paced and waits for leaders; the second run finds nothing to do.
func TestManyTopicsArePacedConvergeAndAreIdempotent(t *testing.T) {
	b := testbroker.Start(t)
	c := connect(t, b)
	var topics []spec.Topic
	for i := range 150 {
		topics = append(topics, spec.Topic{Name: fmt.Sprintf("bulk.v1.x.%03d", i), Configs: map[string]string{"retention.bytes": fmt.Sprint(1000000 + i)}})
	}
	d := desired(t, 1, topics...)
	o := eopts()
	o.BatchSize = 20
	start := time.Now()
	out, err := apply.Run(ctx(t), c, d, plan.DefaultOptions(), o, false)
	if err != nil {
		t.Fatal(err)
	}
	if out.Result.Executed[plan.Create] != 150 || !out.Final.Empty() {
		t.Fatalf("created %d, final %s", out.Result.Executed[plan.Create], out.Final.Text())
	}
	t.Logf("150 topics in %s", time.Since(start))
	ready, err := c.LeadersReady(ctx(t), d.Names())
	if err != nil || !ready {
		t.Errorf("leaders ready %v %v", ready, err)
	}
	again, err := apply.Run(ctx(t), c, d, plan.DefaultOptions(), o, false)
	if err != nil || !again.Plan.Empty() || again.Plan.Unchanged != 150 {
		t.Fatalf("second run: %v %s", err, again.Plan.Text())
	}
}

// Two runs at once (a Job and a CronJob overlapping) both succeed: creates that race find the topic there.
func TestConcurrentRunsConverge(t *testing.T) {
	b := testbroker.Start(t)
	var topics []spec.Topic
	for i := range 40 {
		topics = append(topics, spec.Topic{Name: fmt.Sprintf("race.%02d", i)})
	}
	d := desired(t, 1, topics...)
	var wg sync.WaitGroup
	errs := make([]error, 3)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = apply.Run(ctx(t), connect(t, b), d, plan.DefaultOptions(), eopts(), false)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("run %d: %v", i, err)
		}
	}
}

func TestABrokerRefusalFailsThatTopicOnly(t *testing.T) {
	b := testbroker.Start(t)
	c := connect(t, b)
	d := desired(t, 1, spec.Topic{Name: "bad", Configs: map[string]string{"cleanup.policy": "nonsense"}}, spec.Topic{Name: "good"})
	out, err := apply.Run(ctx(t), c, d, plan.DefaultOptions(), eopts(), false)
	if err == nil || !strings.Contains(err.Error(), "create bad") {
		t.Fatalf("err %v", err)
	}
	s, _ := c.Observe(ctx(t))
	if _, ok := s.Topics["good"]; !ok {
		t.Error("the good topic was not created")
	}
	if len(out.Result.Failures) != 1 {
		t.Errorf("failures %+v", out.Result.Failures)
	}
}

func TestAReplicationFactorAboveTheBrokersFails(t *testing.T) {
	b := testbroker.Start(t)
	c := connect(t, b)
	_, err := apply.Run(ctx(t), c, desired(t, 3, spec.Topic{Name: "rf3"}), plan.DefaultOptions(), eopts(), false)
	if err == nil {
		t.Fatal("rf 3 on one broker succeeded")
	}
	if cluster.IsRetriable(errors.Unwrap(err)) {
		t.Logf("note: the broker reports it retriable: %v", err)
	}
}

func produce(t *testing.T, b *testbroker.Cluster, topic string, n int, ts time.Time) {
	t.Helper()
	cl := b.Client(kgo.DefaultProduceTopic(topic), kgo.RecordPartitioner(kgo.RoundRobinPartitioner()))
	for i := range n {
		r := &kgo.Record{Value: []byte(fmt.Sprint(i)), Timestamp: ts.Add(time.Duration(i) * time.Second)}
		if err := cl.ProduceSync(ctx(t), r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNewestRecordTime(t *testing.T) {
	b := testbroker.Start(t)
	c := connect(t, b)
	if err := c.CreateTopic(ctx(t), spec.DesiredTopic{Name: "ts", Partitions: 3, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	if _, has, err := c.NewestRecordTime(ctx(t), "ts"); err != nil || has {
		t.Fatalf("empty topic: has %v err %v", has, err)
	}
	base := time.Now().Add(-3 * time.Hour).Truncate(time.Millisecond)
	produce(t, b, "ts", 30, base)
	got, has, err := c.NewestRecordTime(ctx(t), "ts")
	want := base.Add(29 * time.Second)
	if err != nil || !has || !got.Equal(want) {
		t.Fatalf("newest %v has %v err %v, want %v", got, has, err, want)
	}
}

// What retire's guards rest on: a member assigned the topic, and a group's lag, read from the broker.
func TestGroupUsesSeeMembersAndLag(t *testing.T) {
	b := testbroker.Start(t)
	c := connect(t, b)
	if err := c.CreateTopic(ctx(t), spec.DesiredTopic{Name: "grp", Partitions: 2, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	produce(t, b, "grp", 20, time.Now())
	consumer := b.Client(kgo.ConsumerGroup("reader"), kgo.ConsumeTopics("grp"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit())
	// read some, commit some
	var seen int
	for seen < 5 {
		f := consumer.PollRecords(ctx(t), 5)
		seen += f.NumRecords()
		if err := consumer.CommitRecords(ctx(t), f.Records()[:1]...); err != nil {
			t.Fatal(err)
		}
	}
	var uses []cluster.GroupUse
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		uses, err = c.GroupUses(ctx(t), "grp")
		if err != nil {
			t.Fatal(err)
		}
		if len(uses) == 1 && uses[0].ActiveMembers == 1 {
			break
		}
		time.Sleep(time.Second)
	}
	if len(uses) != 1 || uses[0].Group != "reader" || uses[0].ActiveMembers != 1 || !uses[0].Committed || uses[0].Lag <= 0 {
		t.Fatalf("uses %+v", uses)
	}
	d := desired(t, 1, spec.Topic{Name: "other"})
	_, err := retire.Retire(ctx(t), c, d, "grp", retire.Options{Confirm: "grp", MinIdle: 0})
	var refusal *retire.Refusal
	if !errors.As(err, &refusal) {
		t.Fatalf("retire of a topic being read: %v", err)
	}
	consumer.Close()
	// a group read to the end and gone: retire passes
	end := b.Client(kgo.ConsumerGroup("reader"), kgo.ConsumeTopics("grp"), kgo.DisableAutoCommit())
	for {
		f := end.PollRecords(ctx(t), 100)
		if err := end.CommitRecords(ctx(t), f.Records()...); err != nil {
			t.Fatal(err)
		}
		uses, _ = c.GroupUses(ctx(t), "grp")
		if len(uses) == 1 && uses[0].Lag == 0 {
			break
		}
	}
	end.Close()
	deadline = time.Now().Add(60 * time.Second)
	for {
		_, err = retire.Retire(ctx(t), c, d, "grp", retire.Options{Confirm: "grp", MinIdle: 0})
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Second) // the group's member leaving takes a moment
	}
	if err != nil {
		t.Fatalf("retire of a topic read to the end, nobody on it: %v", err)
	}
	s, _ := c.Observe(ctx(t))
	if _, ok := s.Topics["grp"]; ok {
		t.Error("grp still exists after retire")
	}
}

func TestPruneDeletesOnRealBrokers(t *testing.T) {
	b := testbroker.Start(t)
	c := connect(t, b)
	for i := range 12 {
		if err := c.CreateTopic(ctx(t), spec.DesiredTopic{Name: fmt.Sprintf("m.v1.x.%02d", i), Partitions: 1, ReplicationFactor: 1}); err != nil {
			t.Fatal(err)
		}
	}
	f := spec.File{APIVersion: spec.APIVersion, Cluster: "it", Defaults: spec.Defaults{Partitions: 1, ReplicationFactor: 1},
		Prune: spec.PruneRule{Enabled: true, Patterns: []string{`^m\.v1\..*$`}}}
	for i := range 10 {
		f.Topics = append(f.Topics, spec.Topic{Name: fmt.Sprintf("m.v1.x.%02d", i)})
	}
	d, err := f.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	po := plan.DefaultOptions()
	po.Prune = true
	out, err := apply.Run(ctx(t), c, d, po, eopts(), false)
	if err != nil || out.Result.Executed[plan.Delete] != 2 {
		t.Fatalf("%v %+v", err, out.Result)
	}
	s, _ := c.Observe(ctx(t))
	for _, n := range []string{"m.v1.x.10", "m.v1.x.11"} {
		if _, ok := s.Topics[n]; ok {
			t.Errorf("%s survived the prune", n)
		}
	}
	if _, ok := s.Topics["m.v1.x.00"]; !ok {
		t.Error("a listed topic was pruned")
	}
}

// Regression: kadm's Metadata answers from franz-go's metadata cache, which listed no topic at all after a request the
// broker refused (INVALID_REQUEST on lowering partitions): every listed topic then looked missing. The adapter asks
// the broker directly.
func TestObserveStaysTruthfulAfterABrokerRefusal(t *testing.T) {
	b := testbroker.Start(t)
	c := connect(t, b)
	if err := c.CreateTopic(ctx(t), spec.DesiredTopic{Name: "truth", Partitions: 2, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := apply.Run(ctx(t), c, desired(t, 1, spec.Topic{Name: "truth", Partitions: 5}), plan.DefaultOptions(), eopts(), false); err != nil {
		t.Fatal(err)
	}
	if err := c.SetPartitions(ctx(t), "truth", 3); err == nil {
		t.Fatal("the broker lowered partitions")
	}
	for i := range 3 {
		s, err := c.Observe(ctx(t))
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := s.Topics["truth"]; !ok {
			t.Fatalf("observation %d after the refusal lists %d topics, without truth", i, len(s.Topics))
		}
	}
}

func TestLeadersReadyForAnUnknownTopicIsFalse(t *testing.T) {
	b := testbroker.Start(t)
	c := connect(t, b)
	ready, err := c.LeadersReady(ctx(t), []string{"does.not.exist"})
	if err != nil || ready {
		t.Fatalf("ready %v err %v", ready, err)
	}
}

func TestUnreachableBrokersAreARetriableFailure(t *testing.T) {
	c, err := kafka.New(kafka.Config{Brokers: []string{"127.0.0.1:1"}, RequestTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.Observe(ctx(t))
	if err == nil || !cluster.IsRetriable(err) {
		t.Fatalf("observe of a closed port: %v (retriable %v)", err, cluster.IsRetriable(err))
	}
}
