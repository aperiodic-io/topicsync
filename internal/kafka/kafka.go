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
	"slices"
	"sort"
	"strings"
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
func (c *Cluster) Observe(ctx context.Context, configsOf []string) (cluster.State, error) {
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
	}
	for _, name := range configsOf {
		if _, ok := s.Topics[name]; ok {
			names = append(names, name)
		}
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
	if err == nil {
		for _, r := range resps {
			if r.Err != nil {
				err = withMessage(r.Err, r.ErrMessage)
			}
		}
	}
	if err == nil {
		return nil
	}
	// a retry of an increase that took effect although its answer was lost: brokers refuse an equal count
	// (INVALID_PARTITIONS); if the topic already has n partitions, the increase is done
	if md, merr := c.metadata(rctx, []string{topic}); merr == nil && len(md) == 1 && md[0].err == nil && int32(len(md[0].partitions)) >= n { //nolint:gosec // fits
		return nil
	}
	return classify(err)
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

// LeadersReady implements cluster.Cluster. A topic the metadata does not show yet, or with fewer partitions than
// asked, counts as not ready.
func (c *Cluster) LeadersReady(ctx context.Context, partitions map[string]int32) (bool, error) {
	rctx, cancel := c.ctx(ctx)
	defer cancel()
	if len(partitions) == 0 {
		return true, nil
	}
	topics := make([]string, 0, len(partitions))
	for t := range partitions {
		topics = append(topics, t)
	}
	md, err := c.metadata(rctx, topics)
	if err != nil {
		return false, classify(err)
	}
	byName := make(map[string]topicMeta, len(md))
	for _, d := range md {
		byName[d.topic] = d
	}
	for name, want := range partitions {
		d, ok := byName[name]
		if !ok || d.err != nil || len(d.partitions) == 0 || int32(len(d.partitions)) < want { //nolint:gosec // partition counts fit
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
	starts, ends, err := c.offsets(rctx, topic)
	if err != nil {
		return nil, err
	}
	var uses []cluster.GroupUse
	for _, g := range groups {
		u := cluster.GroupUse{Group: g}
		dg, ok := described[g]
		switch {
		case ok && dg.Err == nil && (dg.State != "Dead" || len(dg.Members) > 0):
			u.State = dg.State
			for _, m := range dg.Members {
				if memberUses(m, topic) {
					u.ActiveMembers++
				}
			}
		case ok && dg.Err != nil && !errors.Is(dg.Err, kerr.GroupIDNotFound):
			return nil, fmt.Errorf("describing group %s: %w", g, classify(dg.Err))
		default:
			// not a classic group: a consumer group of the new protocol (KIP-848, Kafka 4) is "not found" (or, on older
			// brokers, "Dead" with no members) to DescribeGroups, so ask ConsumerGroupDescribe; an error there is an
			// error, so an active consumer can never go unseen
			cg, err := c.adm.DescribeConsumerGroups(rctx, g)
			if err != nil {
				if dg.State == "Dead" && dg.Err == nil && errors.Is(err, kerr.UnsupportedVersion) {
					u.State = "Dead" // a broker without the new protocol: a Dead classic group has no member
					break
				}
				return nil, fmt.Errorf("describing group %s as a consumer group: %w", g, classify(err))
			}
			d, ok := cg[g]
			if !ok {
				return nil, fmt.Errorf("describing group %s as a consumer group: no answer", g)
			}
			if d.Err != nil && !errors.Is(d.Err, kerr.GroupIDNotFound) {
				return nil, fmt.Errorf("describing group %s as a consumer group: %w", g, classify(d.Err))
			}
			u.State = d.State
			for _, m := range d.Members {
				if _, assigned := m.Assignment[topic]; assigned || slices.Contains(m.SubscribedTopics, topic) {
					u.ActiveMembers++
				}
			}
		}
		offsets, err := c.adm.FetchOffsets(rctx, g)
		if err != nil {
			return nil, fmt.Errorf("fetching offsets of group %s: %w", g, classify(err))
		}
		var lag int64
		for p, end := range ends {
			committed, ok := offsets.Lookup(topic, p)
			if ok && committed.Err != nil {
				return nil, fmt.Errorf("committed offset of group %s on %s/%d: %w", g, topic, p, classify(committed.Err))
			}
			if ok && committed.At >= 0 {
				u.Committed = true
				lag += max(0, end-committed.At)
				continue
			}
			lag += max(0, end-starts[p])
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

// offsets returns each partition's start and end offsets. Any partition's error, or a partition the metadata shows
// but the answer lacks, is an error: a missing answer must never read as an empty partition.
func (c *Cluster) offsets(ctx context.Context, topic string) (starts, ends map[int32]int64, err error) {
	md, err := c.metadata(ctx, []string{topic})
	if err != nil {
		return nil, nil, classify(err)
	}
	if len(md) != 1 || md[0].err != nil {
		if len(md) == 1 {
			return nil, nil, classify(md[0].err)
		}
		return nil, nil, fmt.Errorf("%s: %w", topic, cluster.ErrUnknownTopic)
	}
	want := len(md[0].partitions)
	so, err := c.adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, nil, classify(err)
	}
	eo, err := c.adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, nil, classify(err)
	}
	starts, ends = map[int32]int64{}, map[int32]int64{}
	for p, o := range so[topic] {
		if o.Err != nil {
			return nil, nil, fmt.Errorf("start offset of %s/%d: %w", topic, p, classify(o.Err))
		}
		starts[p] = o.Offset
	}
	for p, o := range eo[topic] {
		if o.Err != nil {
			return nil, nil, fmt.Errorf("end offset of %s/%d: %w", topic, p, classify(o.Err))
		}
		ends[p] = o.Offset
	}
	if len(starts) != want || len(ends) != want {
		return nil, nil, fmt.Errorf("%s has %d partitions, but offsets came back for %d (start) and %d (end)", topic, want, len(starts), len(ends))
	}
	return starts, ends, nil
}

// EndOffsets implements cluster.Cluster.
func (c *Cluster) EndOffsets(ctx context.Context, topic string) (map[int32]int64, error) {
	rctx, cancel := c.ctx(ctx)
	defer cancel()
	_, ends, err := c.offsets(rctx, topic)
	return ends, err
}

// NewestRecordTime implements cluster.Cluster: it reads the last records of each non-empty partition and returns the
// newest timestamp among them. A partition it cannot read to its end within the request timeout (an unavailable
// leader; a tail of transaction markers it cannot see past) is an error, never a guess: retire rests on this.
func (c *Cluster) NewestRecordTime(ctx context.Context, topic string) (time.Time, bool, error) {
	rctx, cancel := c.ctx(ctx)
	defer cancel()
	starts, ends, err := c.offsets(rctx, topic)
	if err != nil {
		return time.Time{}, false, err
	}
	const tail = 5
	assign := map[int32]kgo.Offset{}
	want := map[int32]int64{} // partition -> last offset to read
	for p, end := range ends {
		if end <= starts[p] {
			continue
		}
		assign[p] = kgo.NewOffset().At(max(starts[p], end-tail))
		want[p] = end - 1
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
		var ferr error
		fetches.EachError(func(_ string, _ int32, e error) { ferr = e })
		if ferr != nil {
			return time.Time{}, false, classify(ferr)
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
	if len(want) > 0 {
		var parts []string
		for p := range want {
			parts = append(parts, fmt.Sprint(p))
		}
		sort.Strings(parts)
		return time.Time{}, false, fmt.Errorf("could not read %s's partitions %s to their end within %s (an unavailable leader, or transaction markers at the tail): its newest record is unknown",
			topic, strings.Join(parts, ","), c.cfg.RequestTimeout)
	}
	return newest, got, nil
}

var _ cluster.Cluster = (*Cluster)(nil)
