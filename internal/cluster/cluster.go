// Package cluster is what topicsync sees of a Kafka cluster, and the operations it may run against one. The planner
// works on State alone; the executor calls a Cluster. The kafka package implements Cluster over the Kafka admin
// API; Fake implements it in memory for tests.
package cluster

import (
	"context"
	"errors"
	"time"

	"github.com/aperiodic-io/topicsync/spec"
)

// Source is where a topic's config value comes from.
type Source int

const (
	// SourceUnknown is a source the broker did not say, or one topicsync has no name for.
	SourceUnknown Source = iota
	// SourceDynamicTopic is a value set on the topic itself.
	SourceDynamicTopic
	// SourceDefault is the broker's or cluster's default (static, dynamic or built in).
	SourceDefault
)

// ConfigValue is a topic config as the cluster reports it.
type ConfigValue struct {
	Value     string
	Source    Source
	Sensitive bool
}

// TopicState is a topic as the cluster reports it.
type TopicState struct {
	Name string
	// Partitions is the topic's partition count.
	Partitions int32
	// ReplicationFactor is the smallest replica count among its partitions: what the topic can still lose.
	ReplicationFactor int16
	Configs           map[string]ConfigValue
	Internal          bool
}

// State is every topic of a cluster.
type State struct {
	Topics map[string]TopicState
}

// GroupUse is how one consumer group uses a topic.
type GroupUse struct {
	Group string
	// State is the group's state as the broker reports it (Stable, Empty, ...).
	State string
	// ActiveMembers is how many members have the topic assigned (or subscribed) now.
	ActiveMembers int
	// Lag is the sum over the topic's partitions of the end offset less the group's committed offset; partitions the
	// group never committed count as their whole end offset.
	Lag int64
	// Committed is whether the group has committed an offset on the topic at all.
	Committed bool
}

// Cluster is what topicsync does to a cluster. Every method is safe to retry: creating a topic that exists returns
// ErrTopicExists, deleting one that does not returns ErrUnknownTopic.
type Cluster interface {
	// Observe returns every topic of the cluster, internal ones included, with every config.
	Observe(ctx context.Context) (State, error)
	// CreateTopic creates a topic with its partitions, replication factor and configs.
	CreateTopic(ctx context.Context, t spec.DesiredTopic) error
	// AlterConfigs sets the given configs of a topic, incrementally: no other config of the topic changes.
	AlterConfigs(ctx context.Context, topic string, set map[string]string) error
	// SetPartitions raises a topic's partition count to n.
	SetPartitions(ctx context.Context, topic string, n int32) error
	// DeleteTopic deletes a topic and its data.
	DeleteTopic(ctx context.Context, topic string) error
	// LeadersReady reports whether every partition of every given topic has a leader: until it does, a broker can
	// count as unready (a readiness gate on leaderless partitions), so creation is paced on it.
	LeadersReady(ctx context.Context, topics []string) (bool, error)
	// GroupUses returns every consumer group that is subscribed to or has committed offsets on a topic.
	GroupUses(ctx context.Context, topic string) ([]GroupUse, error)
	// NewestRecordTime returns the timestamp of the newest record of a topic across its partitions, and false if the
	// topic holds no record.
	NewestRecordTime(ctx context.Context, topic string) (time.Time, bool, error)
	// Close releases the cluster's connections.
	Close()
}

var (
	// ErrTopicExists is CreateTopic's error for a topic that is already there.
	ErrTopicExists = errors.New("topic already exists")
	// ErrUnknownTopic is the error for a topic that is not there.
	ErrUnknownTopic = errors.New("unknown topic")
)

// RetriableError marks an error worth retrying: a broker that is not the controller yet, a request that timed out,
// a leader election in progress.
type RetriableError struct{ Err error }

func (e *RetriableError) Error() string { return e.Err.Error() }
func (e *RetriableError) Unwrap() error { return e.Err }

// IsRetriable reports whether err is marked retriable.
func IsRetriable(err error) bool {
	var r *RetriableError
	return errors.As(err, &r)
}
