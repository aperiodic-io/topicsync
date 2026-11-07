package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/aperiodic-io/topicsync/internal/cluster"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		err                  error
		retriable            bool
		exists, unknownTopic bool
	}{
		{kerr.TopicAlreadyExists, false, true, false},
		{fmt.Errorf("wrapped: %w", kerr.TopicAlreadyExists), false, true, false},
		{kerr.UnknownTopicOrPartition, false, false, true},
		{kerr.NotController, true, false, false},
		{kerr.RequestTimedOut, true, false, false},
		{kerr.LeaderNotAvailable, true, false, false},
		{kerr.InvalidConfig, false, false, false},
		{kerr.PolicyViolation, false, false, false},
		{kerr.TopicAuthorizationFailed, false, false, false},
		{errors.New("dial tcp: connection refused"), true, false, false},
		{context.DeadlineExceeded, true, false, false},
		{context.Canceled, false, false, false},
		{kgo.ErrClientClosed, false, false, false},
	}
	for _, c := range cases {
		got := classify(c.err)
		if cluster.IsRetriable(got) != c.retriable {
			t.Errorf("%v: retriable %v, want %v", c.err, cluster.IsRetriable(got), c.retriable)
		}
		if errors.Is(got, cluster.ErrTopicExists) != c.exists {
			t.Errorf("%v: ErrTopicExists %v", c.err, !c.exists)
		}
		if errors.Is(got, cluster.ErrUnknownTopic) != c.unknownTopic {
			t.Errorf("%v: ErrUnknownTopic %v", c.err, !c.unknownTopic)
		}
	}
	if classify(nil) != nil {
		t.Error("classify(nil) != nil")
	}
}

func TestSource(t *testing.T) {
	for s, want := range map[kmsg.ConfigSource]cluster.Source{
		kmsg.ConfigSourceDynamicTopicConfig:         cluster.SourceDynamicTopic,
		kmsg.ConfigSourceDefaultConfig:              cluster.SourceDefault,
		kmsg.ConfigSourceStaticBrokerConfig:         cluster.SourceDefault,
		kmsg.ConfigSourceDynamicBrokerConfig:        cluster.SourceDefault,
		kmsg.ConfigSourceDynamicDefaultBrokerConfig: cluster.SourceDefault,
		kmsg.ConfigSourceUnknown:                    cluster.SourceUnknown,
	} {
		if got := source(s); got != want {
			t.Errorf("source(%v) = %v, want %v", s, got, want)
		}
	}
}

func TestNewValidatesItsConfig(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Error("no brokers accepted")
	}
	if _, err := New(Config{Brokers: []string{"b:9092"}, SASLMechanism: "GSSAPI"}); err == nil {
		t.Error("unknown SASL mechanism accepted")
	}
	if _, err := New(Config{Brokers: []string{"b:9092"}, TLS: true, TLSCAFile: "/nonexistent"}); err == nil {
		t.Error("missing CA file accepted")
	}
	notPEM := t.TempDir() + "/ca.pem"
	if err := os.WriteFile(notPEM, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{Brokers: []string{"b:9092"}, TLS: true, TLSCAFile: notPEM}); err == nil {
		t.Error("a CA file without a certificate accepted")
	}
	for _, m := range []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"} {
		c, err := New(Config{Brokers: []string{"b:9092"}, SASLMechanism: m, SASLUser: "u", SASLPassword: "p", TLS: true})
		if err != nil {
			t.Errorf("%s: %v", m, err)
			continue
		}
		c.Close()
	}
}

func TestWithMessage(t *testing.T) {
	if withMessage(nil, "x") != nil {
		t.Error("nil error gained a message")
	}
	base := errors.New("INVALID_CONFIG")
	if got := withMessage(base, "retention.ms must be >= -1"); !errors.Is(got, base) || got.Error() != "INVALID_CONFIG (retention.ms must be >= -1)" {
		t.Errorf("%v", got)
	}
	if got := withMessage(base, ""); !errors.Is(got, base) || got.Error() != base.Error() {
		t.Error("empty message changed the error")
	}
}
