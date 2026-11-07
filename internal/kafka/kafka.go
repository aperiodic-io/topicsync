// Package kafka implements cluster.Cluster over the Kafka admin API (franz-go's kadm). It works with any broker that
// speaks the Kafka protocol: Redpanda and Apache Kafka are tested.
package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/aperiodic-io/topicsync/internal/cluster"
	"github.com/aperiodic-io/topicsync/spec"
)

// Config is how to reach a cluster.
type Config struct {
	Brokers  []string
	ClientID string
	// RequestTimeout bounds each admin request.
	RequestTimeout time.Duration
	// DescribeChunk is how many topics one DescribeConfigs request asks for.
	DescribeChunk int

	TLS                   bool
	TLSCAFile             string
	TLSServerName         string
	TLSInsecureSkipVerify bool

	// SASLMechanism is PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512; empty for none.
	SASLMechanism string
	SASLUser      string
	SASLPassword  string
}

// Cluster is a cluster.Cluster over the Kafka admin API.
type Cluster struct {
	cfg  Config
	opts []kgo.Opt
	cl   *kgo.Client
	adm  *kadm.Client
}

// New connects lazily: the first request dials the brokers.
func New(cfg Config) (*Cluster, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("no brokers given")
	}
	if cfg.ClientID == "" {
		cfg.ClientID = "topicsync"
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	if cfg.DescribeChunk <= 0 {
		cfg.DescribeChunk = 100
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.RequestTimeoutOverhead(cfg.RequestTimeout),
		kgo.RetryTimeout(cfg.RequestTimeout),
		kgo.DialTimeout(10 * time.Second),
	}
	if cfg.TLS {
		tc := &tls.Config{ServerName: cfg.TLSServerName, InsecureSkipVerify: cfg.TLSInsecureSkipVerify, MinVersion: tls.VersionTLS12} //nolint:gosec // opt-in flag
		if cfg.TLSCAFile != "" {
			pem, err := os.ReadFile(cfg.TLSCAFile)
			if err != nil {
				return nil, fmt.Errorf("reading the TLS CA file: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("no certificate in the TLS CA file %s", cfg.TLSCAFile)
			}
			tc.RootCAs = pool
		}
		opts = append(opts, kgo.DialTLSConfig(tc))
	}
	switch cfg.SASLMechanism {
	case "":
	case "PLAIN":
		opts = append(opts, kgo.SASL(plain.Auth{User: cfg.SASLUser, Pass: cfg.SASLPassword}.AsMechanism()))
	case "SCRAM-SHA-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: cfg.SASLUser, Pass: cfg.SASLPassword}.AsSha256Mechanism()))
	case "SCRAM-SHA-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: cfg.SASLUser, Pass: cfg.SASLPassword}.AsSha512Mechanism()))
	default:
		return nil, fmt.Errorf("unknown SASL mechanism %q (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)", cfg.SASLMechanism)
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Cluster{cfg: cfg, opts: opts, cl: cl, adm: kadm.NewClient(cl)}, nil
}

// Close implements cluster.Cluster.
func (c *Cluster) Close() { c.cl.Close() }

func (c *Cluster) ctx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.cfg.RequestTimeout)
}

// classify marks transport failures and retriable broker errors retriable, and maps the two errors topicsync
// handles itself.
func classify(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, kerr.TopicAlreadyExists):
		return fmt.Errorf("%w: %w", cluster.ErrTopicExists, err)
	case errors.Is(err, kerr.UnknownTopicOrPartition), errors.Is(err, kerr.UnknownTopicID):
		return fmt.Errorf("%w: %w", cluster.ErrUnknownTopic, err)
	case errors.Is(err, context.Canceled), errors.Is(err, kgo.ErrClientClosed):
		return err
	}
	var ke *kerr.Error
	if errors.As(err, &ke) {
		if ke.Retriable {
			return &cluster.RetriableError{Err: err}
		}
		return err // the broker refused: an invalid config, a policy, authorization
	}
	// no broker answer at all: a dial, I/O or timeout failure, worth another try
	return &cluster.RetriableError{Err: err}
}

func withMessage(err error, msg string) error {
	if err == nil || msg == "" {
		return err
	}
	return fmt.Errorf("%w (%s)", err, msg)
}

// Observe implements cluster.Cluster.
func (c *Cluster) Observe(ctx context.Context) (cluster.State, error) {
	rctx, cancel := c.ctx(ctx)
	defer cancel()
	details, err := c.metadata(rctx, nil)
	if err != nil {
		return cluster.State{}, fmt.Errorf("listing topics: %w", classify(err))
	}
	s := cluster.State{Topics: make(map[string]cluster.TopicState, len(details))}
	var names []string
	for _, d := range details {
		name := d.topic
		if d.err != nil {
			if errors.Is(d.err, kerr.UnknownTopicOrPartition) {
				continue // deleted while listing
			}
			return cluster.State{}, fmt.Errorf("listing topic %s: %w", name, classify(d.err))
		}
		t := cluster.TopicState{Name: name, Partitions: int32(len(d.partitions)), //nolint:gosec // a partition count fits in int32 (the protocol's own type)
			Internal: d.internal, Configs: map[string]cluster.ConfigValue{}}
		rf := -1
		for _, p := range d.partitions {
			if rf < 0 || len(p.Replicas) < rf {
				rf = len(p.Replicas)
			}
		}
		if rf < 0 {
			rf = 0
		}
		t.ReplicationFactor = int16(rf)
		s.Topics[name] = t
		names = append(names, name)
	}
	sort.Strings(names)
	for start := 0; start < len(names); start += c.cfg.DescribeChunk {
		chunk := names[start:min(start+c.cfg.DescribeChunk, len(names))]
		dctx, dcancel := c.ctx(ctx)
		rcs, err := c.adm.DescribeTopicConfigs(dctx, chunk...)
		dcancel()
		if err != nil {
			return cluster.State{}, fmt.Errorf("describing topic configs: %w", classify(err))
		}
		for _, rc := range rcs {
			if rc.Err != nil {
				if errors.Is(rc.Err, kerr.UnknownTopicOrPartition) {
					delete(s.Topics, rc.Name)
					continue
				}
				return cluster.State{}, fmt.Errorf("describing configs of %s: %w", rc.Name, classify(withMessage(rc.Err, rc.ErrMessage)))
			}
			t, ok := s.Topics[rc.Name]
			if !ok {
				continue
			}
			for _, cfg := range rc.Configs {
				t.Configs[cfg.Key] = cluster.ConfigValue{Value: cfg.MaybeValue(), Source: source(cfg.Source), Sensitive: cfg.Sensitive}
			}
		}
	}
	return s, nil
}

func source(s kmsg.ConfigSource) cluster.Source {
	switch s {
	case kmsg.ConfigSourceDynamicTopicConfig:
		return cluster.SourceDynamicTopic
	case kmsg.ConfigSourceDefaultConfig, kmsg.ConfigSourceStaticBrokerConfig, kmsg.ConfigSourceDynamicBrokerConfig,
		kmsg.ConfigSourceDynamicDefaultBrokerConfig:
		return cluster.SourceDefault
	default:
		return cluster.SourceUnknown
	}
}

// CreateTopic implements cluster.Cluster.
func (c *Cluster) CreateTopic(ctx context.Context, t spec.DesiredTopic) error {
	rctx, cancel := c.ctx(ctx)
	defer cancel()
	configs := make(map[string]*string, len(t.Configs))
	for k, v := range t.Configs {
		configs[k] = &v
	}
	resp, err := c.adm.CreateTopic(rctx, t.Partitions, t.ReplicationFactor, configs, t.Name)
	if err != nil {
		return classify(err)
	}
	return classify(withMessage(resp.Err, resp.ErrMessage))
}

// AlterConfigs implements cluster.Cluster.
func (c *Cluster) AlterConfigs(ctx context.Context, topic string, set map[string]string) error {
	rctx, cancel := c.ctx(ctx)
	defer cancel()
	alters := make([]kadm.AlterConfig, 0, len(set))
	for k, v := range set {
		alters = append(alters, kadm.AlterConfig{Op: kadm.SetConfig, Name: k, Value: &v})
	}
	sort.Slice(alters, func(i, j int) bool { return alters[i].Name < alters[j].Name })
	resps, err := c.adm.AlterTopicConfigs(rctx, alters, topic)
	if err != nil {
		return classify(err)
	}
	for _, r := range resps {
		if r.Err != nil {
			return classify(withMessage(r.Err, r.ErrMessage))
		}
	}
	return nil
}

// SetPartitions implements cluster.Cluster.
func (c *Cluster) SetPartitions(ctx context.Context, topic string, n int32) error {
	rctx, cancel := c.ctx(ctx)
	defer cancel()
	resps, err := c.adm.UpdatePartitions(rctx, int(n), topic)
	if err != nil {
		return classify(err)
	}
	for _, r := range resps {
		if r.Err != nil {
			return classify(withMessage(r.Err, r.ErrMessage))
		}
	}
	return nil
}

// DeleteTopic implements cluster.Cluster.
func (c *Cluster) DeleteTopic(ctx context.Context, topic string) error {
	rctx, cancel := c.ctx(ctx)
	defer cancel()
	resps, err := c.adm.DeleteTopics(rctx, topic)
	if err != nil {
		return classify(err)
	}
	for _, r := range resps {
		if r.Err != nil {
			return classify(withMessage(r.Err, r.ErrMessage))
		}
	}
	return nil
}

// LeadersReady implements cluster.Cluster. A topic the metadata does not show yet counts as not ready.
func (c *Cluster) LeadersReady(ctx context.Context, topics []string) (bool, error) {
	rctx, cancel := c.ctx(ctx)
	defer cancel()
	if len(topics) == 0 {
		return true, nil
	}
	md, err := c.metadata(rctx, topics)
	if err != nil {
		return false, classify(err)
	}
	byName := make(map[string]topicMeta, len(md))
	for _, d := range md {
		byName[d.topic] = d
	}
	for _, name := range topics {
		d, ok := byName[name]
		if !ok || d.err != nil || len(d.partitions) == 0 {
			return false, nil
		}
		for _, p := range d.partitions {
			if p.ErrorCode != 0 || p.Leader < 0 {
				return false, nil
			}
		}
	}
	return true, nil
}

// topicMeta is one topic of a Metadata response.
type topicMeta struct {
	topic      string
	internal   bool
	partitions []kmsg.MetadataResponseTopicPartition
	err        error
}

// metadata asks a broker for the metadata of the given topics, or of every topic if topics is nil. It never uses
// the client's metadata cache: topicsync's decisions rest on what the cluster says now, and a cached view (franz-go
// answers kadm's Metadata from a cache) was seen to list no topic at all after a request the broker refused.
func (c *Cluster) metadata(ctx context.Context, topics []string) ([]topicMeta, error) {
	req := kmsg.NewPtrMetadataRequest()
	for _, t := range topics {
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic = kmsg.StringPtr(t)
		req.Topics = append(req.Topics, rt)
	}
	resp, err := req.RequestWith(ctx, c.cl)
	if err != nil {
		return nil, err
	}
	out := make([]topicMeta, 0, len(resp.Topics))
	for _, t := range resp.Topics {
		if t.Topic == nil {
			continue
		}
		out = append(out, topicMeta{topic: *t.Topic, internal: t.IsInternal, partitions: t.Partitions, err: kerr.ErrorForCode(t.ErrorCode)})
	}
	return out, nil
}

// GroupUses implements cluster.Cluster.
func (c *Cluster) GroupUses(ctx context.Context, topic string) ([]cluster.GroupUse, error) {
	rctx, cancel := c.ctx(ctx)
	defer cancel()
	listed, err := c.adm.ListGroups(rctx)
	if err != nil {
		return nil, fmt.Errorf("listing groups: %w", classify(err))
	}
	groups := listed.Groups()
	if len(groups) == 0 {
		return nil, nil
	}
	described, err := c.adm.DescribeGroups(rctx, groups...)
	if err != nil {
		return nil, fmt.Errorf("describing groups: %w", classify(err))
	}
	starts, err := c.adm.ListStartOffsets(rctx, topic)
	if err != nil {
		return nil, fmt.Errorf("listing start offsets: %w", classify(err))
	}
	ends, err := c.adm.ListEndOffsets(rctx, topic)
	if err != nil {
		return nil, fmt.Errorf("listing end offsets: %w", classify(err))
	}
	var uses []cluster.GroupUse
	for _, g := range groups {
		u := cluster.GroupUse{Group: g}
		if dg, ok := described[g]; ok {
			if dg.Err != nil {
				return nil, fmt.Errorf("describing group %s: %w", g, classify(dg.Err))
			}
			u.State = dg.State
			for _, m := range dg.Members {
				if memberUses(m, topic) {
					u.ActiveMembers++
				}
			}
		}
		offsets, err := c.adm.FetchOffsets(rctx, g)
		if err != nil {
			return nil, fmt.Errorf("fetching offsets of group %s: %w", g, classify(err))
		}
		var lag int64
		for p, end := range ends[topic] {
			if end.Err != nil {
				return nil, fmt.Errorf("end offset of %s/%d: %w", topic, p, classify(end.Err))
			}
			committed, ok := offsets.Lookup(topic, p)
			if ok && committed.Err == nil && committed.At >= 0 {
				u.Committed = true
				lag += max(0, end.Offset-committed.At)
				continue
			}
			lag += max(0, end.Offset-starts[topic][p].Offset)
		}
		u.Lag = lag
		if u.Committed || u.ActiveMembers > 0 {
			uses = append(uses, u)
		}
	}
	return uses, nil
}

func memberUses(m kadm.DescribedGroupMember, topic string) bool {
	if a, ok := m.Assigned.AsConsumer(); ok {
		for _, t := range a.Topics {
			if t.Topic == topic {
				return true
			}
		}
	}
	if j, ok := m.Join.AsConsumer(); ok {
		for _, t := range j.Topics {
			if t == topic {
				return true
			}
		}
	}
	return false
}

// NewestRecordTime implements cluster.Cluster: it reads the last few records of each non-empty partition and returns
// the newest timestamp among them.
func (c *Cluster) NewestRecordTime(ctx context.Context, topic string) (time.Time, bool, error) {
	rctx, cancel := c.ctx(ctx)
	defer cancel()
	starts, err := c.adm.ListStartOffsets(rctx, topic)
	if err != nil {
		return time.Time{}, false, classify(err)
	}
	ends, err := c.adm.ListEndOffsets(rctx, topic)
	if err != nil {
		return time.Time{}, false, classify(err)
	}
	const tail = 5
	assign := map[int32]kgo.Offset{}
	want := map[int32]int64{} // partition -> last offset to read
	for p, end := range ends[topic] {
		start := starts[topic][p].Offset
		if end.Err != nil || end.Offset <= start {
			continue
		}
		assign[p] = kgo.NewOffset().At(max(start, end.Offset-tail))
		want[p] = end.Offset - 1
	}
	if len(assign) == 0 {
		return time.Time{}, false, nil
	}
	opts := append(append([]kgo.Opt{}, c.opts...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: assign}),
		kgo.FetchIsolationLevel(kgo.ReadUncommitted()))
	consumer, err := kgo.NewClient(opts...)
	if err != nil {
		return time.Time{}, false, err
	}
	defer consumer.Close()
	var newest time.Time
	got := false
	for len(want) > 0 {
		fetches := consumer.PollFetches(rctx)
		if rctx.Err() != nil {
			break
		}
		fetches.EachError(func(_ string, _ int32, e error) { err = e })
		if err != nil {
			return time.Time{}, false, classify(err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if !got || r.Timestamp.After(newest) {
				newest, got = r.Timestamp, true
			}
			if last, ok := want[r.Partition]; ok && r.Offset >= last {
				delete(want, r.Partition)
			}
		})
	}
	if !got {
		// non-empty partitions whose tail holds only control records (transaction markers): no timestamp to read
		if len(want) > 0 && rctx.Err() != nil {
			return time.Time{}, false, fmt.Errorf("reading the newest records of %s: %w", topic, classify(rctx.Err()))
		}
		return time.Time{}, false, nil
	}
	return newest, true, nil
}

var _ cluster.Cluster = (*Cluster)(nil)
