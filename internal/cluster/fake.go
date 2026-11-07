package cluster

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aperiodic-io/topicsync/spec"
)

// Fake is an in-memory Cluster for tests. Its zero value is not usable: call NewFake.
type Fake struct {
	mu sync.Mutex
	// Topics is the cluster's state; tests may read and edit it under Lock/Unlock.
	topics map[string]*fakeTopic
	// Defaults are the configs every topic reports with SourceDefault unless set on it.
	Defaults map[string]string
	// Brokers caps the replication factor a topic can be created with (0: no cap).
	Brokers int
	// FailOn makes the named operation on the named topic fail with the given error, once per entry ("create:t").
	FailOn map[string]error
	// FailAlways makes the named operation on the named topic fail every time.
	FailAlways map[string]error
	// LeaderDelay is how many LeadersReady calls a newly created topic (or new partitions) stays leaderless for.
	LeaderDelay int
	// ObserveErr makes Observe fail.
	ObserveErr error
	// ObserveErrs are returned by the next Observe calls, one each, before ObserveErr.
	ObserveErrs []error
	// Groups is each topic's consumer group uses.
	Groups map[string][]GroupUse
	// Newest is each topic's newest record time; absent: empty.
	Newest map[string]time.Time
	// Ends is each topic's end offsets, a sequence: each EndOffsets call takes the next (the last one repeats).
	Ends map[string][]map[int32]int64
	// Unreported are config keys the broker accepts but never reports (Redpanda: unclean.leader.election.enable).
	Unreported map[string]bool
	// Calls records every mutating call in order ("create:t", "alter:t:k=v,...", "partitions:t:3", "delete:t").
	Calls []string
}

type fakeTopic struct {
	partitions  int32
	rf          int16
	configs     map[string]string // set on the topic
	internal    bool
	leaderlessN int
}

// NewFake returns an empty Fake with the given broker defaults.
func NewFake(defaults map[string]string) *Fake {
	if defaults == nil {
		defaults = map[string]string{}
	}
	return &Fake{
		topics:     map[string]*fakeTopic{},
		Defaults:   defaults,
		FailOn:     map[string]error{},
		FailAlways: map[string]error{},
		Groups:     map[string][]GroupUse{},
		Newest:     map[string]time.Time{},
		Ends:       map[string][]map[int32]int64{},
		Unreported: map[string]bool{},
	}
}

// Put adds or replaces a topic as if created outside topicsync.
func (f *Fake) Put(name string, partitions int32, rf int16, configs map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := map[string]string{}
	for k, v := range configs {
		c[k] = v
	}
	f.topics[name] = &fakeTopic{partitions: partitions, rf: rf, configs: c, internal: len(name) > 0 && name[0] == '_'}
}

// Has reports whether a topic exists.
func (f *Fake) Has(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.topics[name]
	return ok
}

// TopicConfig returns a topic's effective config value.
func (f *Fake) TopicConfig(name, key string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.topics[name]
	if !ok {
		return "", false
	}
	if v, ok := t.configs[key]; ok {
		return v, true
	}
	v, ok := f.Defaults[key]
	return v, ok
}

// Partitions returns a topic's partition count.
func (f *Fake) Partitions(name string) int32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if t, ok := f.topics[name]; ok {
		return t.partitions
	}
	return 0
}

// Len returns the number of topics.
func (f *Fake) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.topics)
}

// CallsOf returns the recorded calls with the given prefix ("create:", "delete:").
func (f *Fake) CallsOf(prefix string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, c := range f.Calls {
		if len(c) >= len(prefix) && c[:len(prefix)] == prefix {
			out = append(out, c)
		}
	}
	return out
}

func (f *Fake) fail(op, topic string) error {
	key := op + ":" + topic
	if err, ok := f.FailAlways[key]; ok {
		return err
	}
	if err, ok := f.FailOn[key]; ok {
		delete(f.FailOn, key)
		return err
	}
	return nil
}

// Observe implements Cluster.
func (f *Fake) Observe(ctx context.Context, configsOf []string) (State, error) {
	if err := ctx.Err(); err != nil {
		return State{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.ObserveErrs) > 0 {
		err := f.ObserveErrs[0]
		f.ObserveErrs = f.ObserveErrs[1:]
		return State{}, err
	}
	if f.ObserveErr != nil {
		return State{}, f.ObserveErr
	}
	describe := map[string]bool{}
	for _, n := range configsOf {
		describe[n] = true
	}
	s := State{Topics: make(map[string]TopicState, len(f.topics))}
	for name, t := range f.topics {
		configs := map[string]ConfigValue{}
		if describe[name] {
			for k, v := range f.Defaults {
				configs[k] = ConfigValue{Value: v, Source: SourceDefault}
			}
			for k, v := range t.configs {
				if !f.Unreported[k] {
					configs[k] = ConfigValue{Value: normalize(k, v), Source: SourceDynamicTopic}
				}
			}
		}
		s.Topics[name] = TopicState{Name: name, Partitions: t.partitions, ReplicationFactor: t.rf, Configs: configs, Internal: t.internal}
	}
	return s, nil
}

// CreateTopic implements Cluster.
func (f *Fake) CreateTopic(ctx context.Context, t spec.DesiredTopic) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, "create:"+t.Name)
	if err := f.fail("create", t.Name); err != nil {
		return err
	}
	if _, ok := f.topics[t.Name]; ok {
		return fmt.Errorf("%s: %w", t.Name, ErrTopicExists)
	}
	if f.Brokers > 0 && int(t.ReplicationFactor) > f.Brokers {
		return fmt.Errorf("%s: replication factor %d is more than the %d brokers", t.Name, t.ReplicationFactor, f.Brokers)
	}
	c := map[string]string{}
	for k, v := range t.Configs {
		c[k] = v
	}
	f.topics[t.Name] = &fakeTopic{partitions: t.Partitions, rf: t.ReplicationFactor, configs: c, leaderlessN: f.LeaderDelay}
	return nil
}

// AlterConfigs implements Cluster.
func (f *Fake) AlterConfigs(ctx context.Context, topic string, set map[string]string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	call := "alter:" + topic + ":"
	for i, k := range keys {
		if i > 0 {
			call += ","
		}
		call += k + "=" + set[k]
	}
	f.Calls = append(f.Calls, call)
	if err := f.fail("alter", topic); err != nil {
		return err
	}
	t, ok := f.topics[topic]
	if !ok {
		return fmt.Errorf("%s: %w", topic, ErrUnknownTopic)
	}
	for k, v := range set {
		t.configs[k] = v
	}
	return nil
}

// SetPartitions implements Cluster.
func (f *Fake) SetPartitions(ctx context.Context, topic string, n int32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, fmt.Sprintf("partitions:%s:%d", topic, n))
	if err := f.fail("partitions", topic); err != nil {
		return err
	}
	t, ok := f.topics[topic]
	if !ok {
		return fmt.Errorf("%s: %w", topic, ErrUnknownTopic)
	}
	if n <= t.partitions {
		// as brokers do: INVALID_PARTITIONS for a count that is not an increase, also an equal one
		return fmt.Errorf("%s: INVALID_PARTITIONS: %d is not more than the current %d", topic, n, t.partitions)
	}
	t.partitions = n
	t.leaderlessN = f.LeaderDelay
	return nil
}

// DeleteTopic implements Cluster.
func (f *Fake) DeleteTopic(ctx context.Context, topic string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, "delete:"+topic)
	if err := f.fail("delete", topic); err != nil {
		return err
	}
	if _, ok := f.topics[topic]; !ok {
		return fmt.Errorf("%s: %w", topic, ErrUnknownTopic)
	}
	delete(f.topics, topic)
	return nil
}

// LeadersReady implements Cluster.
func (f *Fake) LeadersReady(ctx context.Context, partitions map[string]int32) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	ready := true
	for name, want := range partitions {
		t, ok := f.topics[name]
		if !ok || t.partitions < want {
			ready = false
			continue
		}
		if t.leaderlessN > 0 {
			t.leaderlessN--
			ready = false
		}
	}
	return ready, nil
}

// GroupUses implements Cluster.
func (f *Fake) GroupUses(ctx context.Context, topic string) ([]GroupUse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.fail("groups", topic); err != nil {
		return nil, err
	}
	return append([]GroupUse(nil), f.Groups[topic]...), nil
}

// NewestRecordTime implements Cluster.
func (f *Fake) NewestRecordTime(ctx context.Context, topic string) (time.Time, bool, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.fail("newest", topic); err != nil {
		return time.Time{}, false, err
	}
	t, ok := f.Newest[topic]
	return t, ok, nil
}

// EndOffsets implements Cluster.
func (f *Fake) EndOffsets(ctx context.Context, topic string) (map[int32]int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.fail("ends", topic); err != nil {
		return nil, err
	}
	seq := f.Ends[topic]
	if len(seq) == 0 {
		return map[int32]int64{0: 0}, nil
	}
	e := seq[0]
	if len(seq) > 1 {
		f.Ends[topic] = seq[1:]
	}
	return e, nil
}

// normalize reports a value as brokers do: list values sorted (Redpanda reports "delete,compact" as
// "compact,delete"), booleans lower case (Kafka reports "False" as "false").
func normalize(key, v string) string {
	switch strings.ToLower(v) {
	case "true", "false":
		return strings.ToLower(v)
	}
	if key == "cleanup.policy" && strings.Contains(v, ",") {
		parts := strings.Split(v, ",")
		sort.Strings(parts)
		return strings.Join(parts, ",")
	}
	return v
}

// Close implements Cluster.
func (f *Fake) Close() {}

var _ Cluster = (*Fake)(nil)

// Names returns every topic's name, for an Observe that describes them all.
func (f *Fake) Names() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.topics))
	for n := range f.topics {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
