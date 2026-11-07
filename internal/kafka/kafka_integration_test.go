//go:build integration

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/aperiodic-io/topicsync/internal/testbroker"
	"github.com/aperiodic-io/topicsync/spec"
)

// Regression: kadm's Metadata answers from franz-go's metadata cache, which listed no topic at all after a request the
// broker refused (INVALID_REQUEST on lowering partitions): every listed topic then looked missing. The adapter asks
// the broker directly. The refused request is sent on the adapter's own client, as it was when it happened.
func TestObserveStaysTruthfulAfterABrokerRefusal(t *testing.T) {
	b := testbroker.Start(t)
	c, err := New(Config{Brokers: b.Addrs, RequestTimeout: 20 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	if err := c.CreateTopic(ctx, spec.DesiredTopic{Name: "truth", Partitions: 2, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	if err := c.SetPartitions(ctx, "truth", 5); err != nil {
		t.Fatal(err)
	}
	if ready, err := c.LeadersReady(ctx, map[string]int32{"truth": 5}); err != nil || !ready {
		t.Logf("leaders %v %v", ready, err)
	}
	if _, err := c.adm.UpdatePartitions(ctx, 3, "truth"); err != nil {
		t.Logf("lowering: %v", err)
	}
	for i := range 3 {
		s, err := c.Observe(ctx, []string{"truth"})
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := s.Topics["truth"]; !ok {
			t.Fatalf("observation %d after the refusal lists %d topics, without truth", i, len(s.Topics))
		}
	}
}
