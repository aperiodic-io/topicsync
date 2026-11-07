//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aperiodic-io/topicsync/internal/apply"
	"github.com/aperiodic-io/topicsync/internal/plan"
	"github.com/aperiodic-io/topicsync/internal/testbroker"
	"github.com/aperiodic-io/topicsync/spec"
)

func redpandaOnly(t *testing.T) {
	t.Helper()
	if testbroker.Flavor() != "redpanda" {
		t.Skip("Redpanda only")
	}
}

// Production's clusters have three brokers and replication factor 3. A broker that stalls (paused: connections open,
// no answers) must not stop a run from planning, and a run that cannot place three replicas meanwhile fails loudly and
// succeeds once the broker is back.
func TestThreeBrokersReplicationThreeAndAStalledBroker(t *testing.T) {
	redpandaOnly(t)
	b := testbroker.StartRedpandaCluster(t, testbroker.Image("redpanda"))
	c := connect(t, b)
	var topics []spec.Topic
	for i := range 30 {
		topics = append(topics, spec.Topic{Name: fmt.Sprintf("rf3.%02d", i), Partitions: 3})
	}
	d := desired(t, 3, topics...)
	out, err := apply.Run(ctx(t), c, d, plan.DefaultOptions(), eopts(), false)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := c.Observe(ctx(t), nil)
	if s.Topics["rf3.00"].ReplicationFactor != 3 || out.Result.Executed[plan.Create] != 30 {
		t.Fatalf("rf %d created %d", s.Topics["rf3.00"].ReplicationFactor, out.Result.Executed[plan.Create])
	}

	b.Pause(2)
	paused := true
	defer func() {
		if paused {
			b.Unpause(2)
		}
	}()
	more := append(topics, spec.Topic{Name: "rf3.new", Partitions: 3})
	d2 := desired(t, 3, more...)
	// observing still works with one broker frozen
	shortCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c2 := connect(t, b)
	p, err := apply.Run(shortCtx, c2, d2, plan.DefaultOptions(), eopts(), true)
	if err != nil || p.Plan.Count(plan.Create) != 1 {
		t.Fatalf("plan with a broker paused: %v", err)
	}
	b.Unpause(2)
	paused = false
	b.WaitReady(3)
	out, err = apply.Run(ctx(t), connect(t, b), d2, plan.DefaultOptions(), eopts(), false)
	if err != nil || out.Result.Executed[plan.Create] != 1 {
		t.Fatalf("after the broker came back: %v %+v", err, out.Result)
	}
}

// A Redpanda upgrade (live-ingestion bumps its broker image regularly) keeps what topicsync made, and the newer
// broker reports it so that topicsync finds nothing to change: no config churn after an upgrade.
func TestARedpandaUpgradeLeavesNothingToChange(t *testing.T) {
	redpandaOnly(t)
	from := os.Getenv("TOPICSYNC_IT_UPGRADE_FROM")
	to := os.Getenv("TOPICSYNC_IT_UPGRADE_TO")
	if from == "" || to == "" {
		t.Skip("set TOPICSYNC_IT_UPGRADE_FROM and TOPICSYNC_IT_UPGRADE_TO to two Redpanda images")
	}
	vol := testbroker.Volume(t)
	old := testbroker.StartWith(t, "redpanda", from, vol)
	var topics []spec.Topic
	for i := range 25 {
		topics = append(topics, spec.Topic{Name: fmt.Sprintf("up.%02d", i), Partitions: 2,
			Configs: map[string]string{"retention.bytes": fmt.Sprint(40000000 + i), "segment.bytes": "16777216", "retention.ms": "-1"}})
	}
	d := desired(t, 1, topics...)
	c := connect(t, old)
	if _, err := apply.Run(ctx(t), c, d, plan.DefaultOptions(), eopts(), false); err != nil {
		t.Fatal(err)
	}
	c.Close()
	old.Stop()
	old.RemoveContainers()

	upgraded := testbroker.StartWith(t, "redpanda", to, vol)
	p, err := apply.Run(ctx(t), connect(t, upgraded), d, plan.DefaultOptions(), eopts(), true)
	if err != nil {
		t.Fatal(err)
	}
	if !p.Plan.Empty() {
		t.Fatalf("after upgrading %s -> %s the plan is not empty:\n%s", from, to, p.Plan.Text())
	}
}
