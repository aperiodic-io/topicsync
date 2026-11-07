// Package testbroker starts real Kafka brokers in Docker for integration and acceptance tests: Redpanda (one node or
// three) or Apache Kafka in KRaft mode. Each listens on a free host port it advertises as 127.0.0.1:<port>.
//
// The image and flavor come from the environment, so that CI runs the same tests against every broker it supports:
//
//	TOPICSYNC_IT_FLAVOR = redpanda (default) | kafka
//	TOPICSYNC_IT_IMAGE  = the image (default: a Redpanda or Apache Kafka release)
//
// Containers are removed when the test ends (KEEP_BROKERS=1 keeps them for debugging).
package testbroker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Default images, the brokers live-ingestion runs and the current Apache Kafka release.
const (
	DefaultRedpandaImage = "docker.redpanda.com/redpandadata/redpanda:v26.1.6"
	DefaultKafkaImage    = "apache/kafka:4.1.0"
)

// Cluster is one or more running brokers.
type Cluster struct {
	t          testing.TB
	Flavor     string
	Image      string
	Addrs      []string
	Containers []string
	Network    string
	Volumes    []string
}

// Flavor returns the flavor the environment selects.
func Flavor() string {
	if f := os.Getenv("TOPICSYNC_IT_FLAVOR"); f != "" {
		return f
	}
	return "redpanda"
}

// Image returns the image the environment selects for a flavor.
func Image(flavor string) string {
	if i := os.Getenv("TOPICSYNC_IT_IMAGE"); i != "" && flavor == Flavor() {
		return i
	}
	if flavor == "kafka" {
		return DefaultKafkaImage
	}
	return DefaultRedpandaImage
}

// Brokers returns the comma-separated seed list.
func (c *Cluster) Brokers() string { return strings.Join(c.Addrs, ",") }

// Start starts a single broker of the environment's flavor.
func Start(t testing.TB) *Cluster {
	t.Helper()
	return StartWith(t, Flavor(), Image(Flavor()), "")
}

// StartWith starts a single broker of a flavor and image; volume, if not empty, is a Docker volume kept across
// restarts (an upgrade test starts a newer image on it).
func StartWith(t testing.TB, flavor, image, volume string) *Cluster {
	t.Helper()
	requireDocker(t)
	c := &Cluster{t: t, Flavor: flavor, Image: image}
	port := freePort(t)
	name := "topicsync-it-" + suffix()
	var args []string
	switch flavor {
	case "redpanda":
		args = []string{"run", "-d", "--name", name, "-p", fmt.Sprintf("127.0.0.1:%d:%d", port, port)}
		if volume != "" {
			args = append(args, "-v", volume+":/var/lib/redpanda/data")
		}
		args = append(args, image, "redpanda", "start", "--mode", "dev-container", "--smp", "1", "--memory", "2G", "--overprovisioned",
			"--node-id", "0", "--kafka-addr", fmt.Sprintf("PLAINTEXT://0.0.0.0:%d", port),
			"--advertise-kafka-addr", fmt.Sprintf("PLAINTEXT://127.0.0.1:%d", port),
			"--set", "redpanda.auto_create_topics_enabled=false")
	case "kafka":
		args = []string{"run", "-d", "--name", name, "-p", fmt.Sprintf("127.0.0.1:%d:%d", port, port),
			"-e", "KAFKA_NODE_ID=1", "-e", "KAFKA_PROCESS_ROLES=broker,controller",
			"-e", fmt.Sprintf("KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:%d,CONTROLLER://0.0.0.0:9093", port),
			"-e", fmt.Sprintf("KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:%d", port),
			"-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
			"-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
			"-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
			"-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1", "-e", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
			"-e", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1", "-e", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
			"-e", "KAFKA_AUTO_CREATE_TOPICS_ENABLE=false", "-e", "KAFKA_HEAP_OPTS=-Xmx512m"}
		if volume != "" {
			args = append(args, "-v", volume+":/var/lib/kafka/data")
		}
		args = append(args, image)
	default:
		t.Fatalf("unknown flavor %q", flavor)
	}
	// cleanup first: a container that is created but fails to start must not leak
	c.Containers = append(c.Containers, name)
	t.Cleanup(c.remove)
	docker(t, args...)
	c.Addrs = append(c.Addrs, fmt.Sprintf("127.0.0.1:%d", port))
	c.WaitReady(1)
	return c
}

// StartRedpandaCluster starts a three-node Redpanda cluster on a Docker network of its own.
func StartRedpandaCluster(t testing.TB, image string) *Cluster {
	t.Helper()
	requireDocker(t)
	c := &Cluster{t: t, Flavor: "redpanda", Image: image, Network: "topicsync-it-net-" + suffix()}
	docker(t, "network", "create", c.Network)
	t.Cleanup(c.remove)
	var names []string
	for i := range 3 {
		names = append(names, fmt.Sprintf("%s-rp%d", c.Network, i))
	}
	for i, name := range names {
		port := freePort(t)
		args := []string{"run", "-d", "--name", name, "--network", c.Network, "--network-alias", name, "-p", fmt.Sprintf("127.0.0.1:%d:%d", port, port),
			image, "redpanda", "start", "--smp", "1", "--memory", "1G", "--overprovisioned", "--node-id", fmt.Sprint(i),
			"--kafka-addr", fmt.Sprintf("PLAINTEXT://0.0.0.0:%d", port), "--advertise-kafka-addr", fmt.Sprintf("PLAINTEXT://127.0.0.1:%d", port),
			"--rpc-addr", "0.0.0.0:33145", "--advertise-rpc-addr", name + ":33145", "--seeds", names[0] + ":33145",
			"--check=false", "--set", "redpanda.auto_create_topics_enabled=false"}
		c.Containers = append(c.Containers, name) // removed by the cleanup registered above, even if it fails to start
		docker(t, args...)
		c.Addrs = append(c.Addrs, fmt.Sprintf("127.0.0.1:%d", port))
	}
	c.WaitReady(3)
	return c
}

// WaitReady waits until n brokers are in the metadata and a topic can be created and deleted.
func (c *Cluster) WaitReady(n int) {
	c.t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	var last error
	for time.Now().Before(deadline) {
		if last = c.probe(n); last == nil {
			return
		}
		time.Sleep(time.Second)
	}
	c.dumpLogs()
	c.t.Fatalf("brokers %v not ready: %v", c.Addrs, last)
}

func (c *Cluster) probe(n int) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.Addrs...), kgo.DialTimeout(2*time.Second), kgo.RequestTimeoutOverhead(5*time.Second), kgo.RetryTimeout(5*time.Second))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	md, err := adm.Metadata(ctx)
	if err != nil {
		return err
	}
	if len(md.Brokers) < n {
		return fmt.Errorf("%d of %d brokers in the metadata", len(md.Brokers), n)
	}
	probe := "topicsync-it-probe-" + suffix()
	r, err := adm.CreateTopic(ctx, 1, int16(min(n, 3)), nil, probe)
	if err != nil {
		return err
	}
	if r.Err != nil {
		return r.Err
	}
	_, err = adm.DeleteTopics(ctx, probe)
	return err
}

// Admin returns an admin client to the cluster, closed when the test ends.
func (c *Cluster) Admin() *kadm.Client {
	c.t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.Addrs...))
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(cl.Close)
	return kadm.NewClient(cl)
}

// Client returns a client with extra options, closed when the test ends.
func (c *Cluster) Client(opts ...kgo.Opt) *kgo.Client {
	c.t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(c.Addrs...)}, opts...)...)
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(cl.Close)
	return cl
}

// Exec runs a command in the first container (rpk for Redpanda, the kafka scripts for Kafka).
func (c *Cluster) Exec(args ...string) string {
	c.t.Helper()
	return docker(c.t, append([]string{"exec", c.Containers[0]}, args...)...)
}

// Pause freezes container i (its connections stay open, as a stalled broker's).
func (c *Cluster) Pause(i int) { docker(c.t, "pause", c.Containers[i]) }

// Unpause resumes container i.
func (c *Cluster) Unpause(i int) { docker(c.t, "unpause", c.Containers[i]) }

// Stop stops every container, keeping their volumes.
func (c *Cluster) Stop() {
	for _, n := range c.Containers {
		docker(c.t, "stop", "-t", "30", n)
	}
}

func (c *Cluster) remove() {
	if os.Getenv("KEEP_BROKERS") == "1" {
		c.t.Logf("keeping brokers %v (KEEP_BROKERS=1)", c.Containers)
		return
	}
	if c.t.Failed() {
		c.dumpLogs()
	}
	for _, n := range c.Containers {
		_ = exec.Command("docker", "rm", "-f", "-v", n).Run()
	}
	if c.Network != "" {
		_ = exec.Command("docker", "network", "rm", c.Network).Run()
	}
}

func (c *Cluster) dumpLogs() {
	for _, n := range c.Containers {
		out, _ := exec.Command("docker", "logs", "--tail", "60", n).CombinedOutput()
		c.t.Logf("--- last log lines of %s:\n%s", n, out)
	}
}

// Volume creates a Docker volume removed when the test ends.
func Volume(t testing.TB) string {
	t.Helper()
	name := "topicsync-it-vol-" + suffix()
	docker(t, "volume", "create", name)
	t.Cleanup(func() { _ = exec.Command("docker", "volume", "rm", "-f", name).Run() })
	return name
}

// RemoveContainers removes the cluster's containers now, keeping named volumes (for an upgrade test's restart).
func (c *Cluster) RemoveContainers() {
	for _, n := range c.Containers {
		docker(c.t, "rm", "-f", "-v", n) // -v drops anonymous volumes only; the named data volume stays
	}
	c.Containers = nil
}

func docker(t testing.TB, args ...string) string {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func requireDocker(t testing.TB) {
	t.Helper()
	if err := exec.Command("docker", "version").Run(); err != nil {
		t.Skipf("docker is not available: %v", err)
	}
}

func freePort(t testing.TB) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func suffix() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
