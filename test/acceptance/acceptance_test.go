//go:build acceptance

// Acceptance tests: the topicsync binary, built once, run as a Job runs it, against a real broker in Docker.
//
// TOPICSYNC_PREVIOUS_BIN, if set, is the previous release's binary: the upgrade and rollback scenarios check that the
// two agree on a cluster (CI builds main's head for a pull request, the latest tag for a release).
package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aperiodic-io/topicsync/internal/testbroker"
	"github.com/aperiodic-io/topicsync/spec"
)

var bin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "topicsync-acceptance")
	if err != nil {
		panic(err)
	}
	bin = filepath.Join(dir, "topicsync")
	build := exec.Command("go", "build", "-ldflags", "-X github.com/aperiodic-io/topicsync/internal/cli.Version=acceptance", "-o", bin, "../../cmd/topicsync")
	build.Stdout, build.Stderr = os.Stdout, os.Stderr
	if err := build.Run(); err != nil {
		panic(err)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

type result struct {
	code           int
	stdout, stderr string
}

func run(t *testing.T, binary string, env []string, args ...string) result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	cmd.Env = append(os.Environ(), env...)
	err := cmd.Run()
	code := 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("running %s: %v", binary, err)
	}
	return result{code, out.String(), errb.String()}
}

func (r result) expect(t *testing.T, code int) result {
	t.Helper()
	if r.code != code {
		t.Fatalf("exit %d, want %d\nstdout:\n%s\nstderr:\n%s", r.code, code, r.stdout, r.stderr)
	}
	return r
}

// liveIngestionLike is a file shaped like live-ingestion's metrics-out file: metric topics per metric, exchange and
// interval with retention by bytes, and raw topics with their own partitions.
func liveIngestionLike(prune bool, extra ...spec.Topic) spec.File {
	f := spec.File{
		APIVersion: spec.APIVersion, Cluster: "redpanda-metrics-out",
		Defaults:  spec.Defaults{Partitions: 1, ReplicationFactor: 1, Configs: map[string]string{"cleanup.policy": "delete"}},
		Protected: []string{`^brrrrr\.leases$`},
	}
	for _, m := range []string{"ohlcv", "trade_size", "l1_price"} {
		for _, x := range []string{"binance-futures", "okx-perps", "hyperliquid-perps"} {
			for i, iv := range []string{"15s", "1m", "1h", "1d"} {
				f.Topics = append(f.Topics, spec.Topic{Name: fmt.Sprintf("%s.v1.%s.%s", m, x, iv),
					Configs: map[string]string{"retention.bytes": fmt.Sprint(40316198 + i), "segment.bytes": "16777216"}})
			}
		}
	}
	f.Topics = append(f.Topics, spec.Topic{Name: "raw.trades.binance-futures", Partitions: 3, Configs: map[string]string{"retention.bytes": "1879048192"}})
	f.Topics = append(f.Topics, extra...)
	if prune {
		f.Prune = spec.PruneRule{Enabled: true, Patterns: []string{`^[a-z0-9_]+\.v[0-9]+\.[a-z0-9-]+\.[0-9]+[smhd]$`}}
	}
	return f
}

func writeSpec(t *testing.T, f spec.File) string {
	t.Helper()
	b, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "topics.json")
	if err := os.WriteFile(p, b, 0o644); err != nil { // readable by the image's non-root user
		t.Fatal(err)
	}
	return p
}

// currentBrokers is the running test's broker list (tests run one at a time).
var currentBrokers []string

func admin(t *testing.T, b *testbroker.Cluster) *kadm.Client {
	currentBrokers = b.Addrs
	return b.Admin()
}

func configOf(t *testing.T, adm *kadm.Client, topic, key string) string {
	t.Helper()
	rcs, err := adm.DescribeTopicConfigs(context.Background(), topic)
	if err != nil {
		t.Fatal(err)
	}
	for _, rc := range rcs {
		for _, c := range rc.Configs {
			if c.Key == key {
				return c.MaybeValue()
			}
		}
	}
	return ""
}

func setConfig(t *testing.T, adm *kadm.Client, topic, key, value string) {
	t.Helper()
	if _, err := adm.AlterTopicConfigs(context.Background(), []kadm.AlterConfig{{Op: kadm.SetConfig, Name: key, Value: &value}}, topic); err != nil {
		t.Fatal(err)
	}
}

// topics lists the cluster's topics through a fresh client each time: kadm's listing answers from franz-go's metadata
// cache, which can show a deleted topic for a while.
func topics(t *testing.T, adm *kadm.Client) map[string]bool {
	t.Helper()
	_ = adm
	cl, err := kgo.NewClient(kgo.SeedBrokers(currentBrokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	td, err := kadm.NewClient(cl).ListTopics(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for n := range td {
		out[n] = true
	}
	return out
}

func TestApplyFromScratchIsIdempotentAndPlanAgrees(t *testing.T) {
	b := testbroker.Start(t)
	p := writeSpec(t, liveIngestionLike(false))
	r := run(t, bin, nil, "apply", "--spec", p, "--brokers", b.Brokers(), "--batch-size", "10", "--batch-pause", "100ms").expect(t, 0)
	if !strings.Contains(r.stdout, "37 to create") || !strings.Contains(r.stderr, "applied: the cluster matches the desired state") {
		t.Fatalf("%s\n%s", r.stdout, r.stderr)
	}
	adm := admin(t, b)
	if got := configOf(t, adm, "ohlcv.v1.okx-perps.1h", "retention.bytes"); got != "40316200" {
		t.Errorf("retention.bytes %q", got)
	}
	r = run(t, bin, nil, "apply", "--spec", p, "--brokers", b.Brokers()).expect(t, 0)
	if !strings.Contains(r.stdout, "0 to create, 0 to alter, 0 to repartition, 0 to delete, 37 unchanged") {
		t.Errorf("second apply:\n%s", r.stdout)
	}
	run(t, bin, nil, "plan", "--spec", p, "--brokers", b.Brokers(), "--detailed-exitcode").expect(t, 0)
}

// Drift made by hand is found by plan (exit 3) and undone by apply; a key nobody manages, set by hand, survives.
func TestDriftIsFoundAndCorrectedAndUnmanagedKeysSurvive(t *testing.T) {
	b := testbroker.Start(t)
	p := writeSpec(t, liveIngestionLike(false))
	run(t, bin, nil, "apply", "--spec", p, "--brokers", b.Brokers()).expect(t, 0)
	adm := admin(t, b)
	setConfig(t, adm, "trade_size.v1.okx-perps.1m", "retention.bytes", "1")
	setConfig(t, adm, "trade_size.v1.okx-perps.1m", "max.message.bytes", "2000000")
	r := run(t, bin, nil, "plan", "--spec", p, "--brokers", b.Brokers(), "--detailed-exitcode").expect(t, 3)
	if !strings.Contains(r.stdout, `~ alter trade_size.v1.okx-perps.1m: retention.bytes "1" (topic) -> "40316199"`) {
		t.Errorf("%s", r.stdout)
	}
	run(t, bin, nil, "apply", "--spec", p, "--brokers", b.Brokers()).expect(t, 0)
	if got := configOf(t, adm, "trade_size.v1.okx-perps.1m", "retention.bytes"); got != "40316199" {
		t.Errorf("retention.bytes %q after apply", got)
	}
	if got := configOf(t, adm, "trade_size.v1.okx-perps.1m", "max.message.bytes"); got != "2000000" {
		t.Errorf("an unmanaged key was reset: max.message.bytes %q", got)
	}
}

// The migration from Topic resources: topics already made by another tool (the Redpanda operator, rpk) with the
// file's settings are adopted with no change at all.
func TestTopicsMadeByAnotherToolAreAdoptedWithoutChange(t *testing.T) {
	b := testbroker.Start(t)
	f := liveIngestionLike(false)
	adm := admin(t, b)
	d, err := f.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range d.Names() {
		dt := d.Topics[n]
		cfg := map[string]*string{}
		for k, v := range dt.Configs {
			cfg[k] = &v
		}
		if b.Flavor == "redpanda" && strings.HasPrefix(n, "ohlcv.") {
			args := []string{"rpk", "topic", "create", n, "-p", fmt.Sprint(dt.Partitions), "-r", fmt.Sprint(dt.ReplicationFactor)}
			for k, v := range dt.Configs {
				args = append(args, "-c", k+"="+v)
			}
			b.Exec(args...)
			continue
		}
		if r, err := adm.CreateTopic(context.Background(), dt.Partitions, dt.ReplicationFactor, cfg, n); err != nil || r.Err != nil {
			t.Fatalf("%s: %v %v", n, err, r.Err)
		}
	}
	r := run(t, bin, nil, "plan", "--spec", writeSpec(t, f), "--brokers", b.Brokers(), "--detailed-exitcode").expect(t, 0)
	if !strings.Contains(r.stdout, "0 to create, 0 to alter") {
		t.Errorf("%s", r.stdout)
	}
}

func TestInvalidFilesExitTwoAndTouchNothing(t *testing.T) {
	b := testbroker.Start(t)
	dir := t.TempDir()
	for name, content := range map[string]string{
		"misspelt.json": `{"apiVersion": "topicsync.aperiodic.io/v1", "cluster": "c", "defaults": {"replicationfactor": 1}, "topics": [{"name": "a"}]}`,
		"badname.json":  `{"apiVersion": "topicsync.aperiodic.io/v1", "cluster": "c", "defaults": {"partitions": 1, "replicationFactor": 1}, "topics": [{"name": "a b"}]}`,
		"notjson.json":  `topics: []`,
	} {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		r := run(t, bin, nil, "apply", "--spec", p, "--brokers", b.Brokers()).expect(t, 2)
		if !strings.Contains(r.stderr, "invalid desired-state file") {
			t.Errorf("%s: %s", name, r.stderr)
		}
	}
	if n := len(topics(t, admin(t, b))); n != 0 {
		t.Errorf("%d topics after invalid runs", n)
	}
}

func TestUnreachableBrokersExitOneWithinTheTimeout(t *testing.T) {
	p := writeSpec(t, liveIngestionLike(false))
	start := time.Now()
	r := run(t, bin, nil, "apply", "--spec", p, "--brokers", "127.0.0.1:1", "--request-timeout", "2s", "--timeout", "20s", "--retries", "1").expect(t, 1)
	if !strings.Contains(r.stderr, "observing the cluster") {
		t.Errorf("%s", r.stderr)
	}
	if time.Since(start) > 40*time.Second {
		t.Errorf("took %s", time.Since(start))
	}
}

func TestPruneOnlyWithBothOptInsAndWithinItsCaps(t *testing.T) {
	b := testbroker.Start(t)
	adm := admin(t, b)
	stale := []spec.Topic{{Name: "ohlcv.v1.old-exchange.1m"}, {Name: "ohlcv.v1.old-exchange.1h"}}
	run(t, bin, nil, "apply", "--spec", writeSpec(t, liveIngestionLike(true, stale...)), "--brokers", b.Brokers()).expect(t, 0)
	if r, err := adm.CreateTopic(context.Background(), 1, 1, nil, "brrrrr.leases"); err != nil || r.Err != nil {
		t.Fatal(err, r.Err)
	}
	if r, err := adm.CreateTopic(context.Background(), 1, 1, nil, "someone.elses"); err != nil || r.Err != nil {
		t.Fatal(err, r.Err)
	}
	p := writeSpec(t, liveIngestionLike(true))
	run(t, bin, nil, "apply", "--spec", p, "--brokers", b.Brokers()).expect(t, 0)
	if !topics(t, adm)["ohlcv.v1.old-exchange.1m"] {
		t.Fatal("pruned without --prune")
	}
	noprune := writeSpec(t, liveIngestionLike(false))
	run(t, bin, nil, "apply", "--spec", noprune, "--brokers", b.Brokers(), "--prune").expect(t, 1)
	run(t, bin, nil, "apply", "--spec", p, "--brokers", b.Brokers(), "--prune").expect(t, 0)
	got := topics(t, adm)
	if got["ohlcv.v1.old-exchange.1m"] || got["ohlcv.v1.old-exchange.1h"] {
		t.Error("stale topics survived the prune")
	}
	if !got["brrrrr.leases"] || !got["someone.elses"] || !got["raw.trades.binance-futures"] {
		t.Error("prune deleted a protected, unmatched or listed topic")
	}
	// a truncated file: refused by the caps, nothing deleted
	short := liveIngestionLike(true)
	short.Topics = short.Topics[:3]
	r := run(t, bin, nil, "apply", "--spec", writeSpec(t, short), "--brokers", b.Brokers(), "--prune").expect(t, 1)
	if !strings.Contains(r.stderr, "refusing to prune") {
		t.Errorf("%s", r.stderr)
	}
	if len(topics(t, adm)) < 37 {
		t.Error("the refused prune deleted topics")
	}
}

func TestRetireEndToEnd(t *testing.T) {
	b := testbroker.Start(t)
	adm := admin(t, b)
	gone := spec.Topic{Name: "ohlcv.v1.retired.1m"}
	run(t, bin, nil, "apply", "--spec", writeSpec(t, liveIngestionLike(false, gone)), "--brokers", b.Brokers()).expect(t, 0)
	p := writeSpec(t, liveIngestionLike(false))
	cl := b.Client(kgo.DefaultProduceTopic(gone.Name))
	if err := cl.ProduceSync(context.Background(), &kgo.Record{Value: []byte("x"), Timestamp: time.Now().Add(-3 * time.Hour)}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	reader := b.Client(kgo.ConsumerGroup("archiver"), kgo.ConsumeTopics(gone.Name), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit())
	f := reader.PollRecords(context.Background(), 1)
	if err := reader.CommitRecords(context.Background(), f.Records()...); err != nil {
		t.Fatal(err)
	}
	r := run(t, bin, nil, "retire", "--spec", p, "--brokers", b.Brokers(), "--topic", gone.Name, "--confirm", gone.Name).expect(t, 1)
	if !strings.Contains(r.stderr, "active member") {
		t.Fatalf("a topic being read was not refused: %s", r.stderr)
	}
	reader.Close()
	deadline := time.Now().Add(60 * time.Second)
	for {
		r = run(t, bin, nil, "retire", "--spec", p, "--brokers", b.Brokers(), "--topic", gone.Name)
		if r.code == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Second)
	}
	r.expect(t, 0)
	if !strings.Contains(r.stdout, "run again with --confirm") || !topics(t, adm)[gone.Name] {
		t.Fatalf("the dry run: %s", r.stdout)
	}
	run(t, bin, nil, "retire", "--spec", p, "--brokers", b.Brokers(), "--topic", gone.Name, "--confirm", gone.Name).expect(t, 0)
	if topics(t, adm)[gone.Name] {
		t.Fatal("retired topic still there")
	}
}

// SIGTERM (a Job's activeDeadlineSeconds, a drain) stops a run promptly with a failure, and the next run finishes the
// job: no topic is left in a state the next run cannot fix.
func TestSIGTERMStopsARunAndTheNextOneConverges(t *testing.T) {
	b := testbroker.Start(t)
	var extra []spec.Topic
	for i := range 120 {
		extra = append(extra, spec.Topic{Name: fmt.Sprintf("bulk.v1.x.%03d", i)})
	}
	p := writeSpec(t, liveIngestionLike(false, extra...))
	cmd := exec.Command(bin, "apply", "--spec", p, "--brokers", b.Brokers(), "--batch-size", "5", "--batch-pause", "1s")
	var errb bytes.Buffer
	cmd.Stderr = &errb
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(4 * time.Second)
	sent := time.Now()
	_ = cmd.Process.Signal(syscall.SIGTERM)
	err := cmd.Wait()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 1 {
		t.Fatalf("exit after SIGTERM: %v\n%s", err, errb.String())
	}
	if time.Since(sent) > 15*time.Second {
		t.Errorf("took %s to stop", time.Since(sent))
	}
	if n := len(topics(t, admin(t, b))); n == 0 || n >= 157 {
		t.Errorf("%d topics: the run did not stop part-way", n)
	}
	r := run(t, bin, nil, "apply", "--spec", p, "--brokers", b.Brokers())
	if r.code != 0 {
		t.Logf("interrupted run's log:\n%s", errb.String())
	}
	r.expect(t, 0)
	run(t, bin, nil, "plan", "--spec", p, "--brokers", b.Brokers(), "--detailed-exitcode").expect(t, 0)
}

func TestMetricsArePushedAndLogsAreJSON(t *testing.T) {
	b := testbroker.Start(t)
	var mu sync.Mutex
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(body))
		mu.Unlock()
	}))
	defer srv.Close()
	p := writeSpec(t, liveIngestionLike(false))
	r := run(t, bin, []string{"TOPICSYNC_METRICS_PUSH_URL=" + srv.URL + "/api/v1/import/prometheus"},
		"apply", "--spec", p, "--brokers", b.Brokers(), "--log-format", "json").expect(t, 0)
	for _, line := range strings.Split(strings.TrimSpace(r.stderr), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("non-JSON log line %q", line)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 1 || !strings.Contains(bodies[0], `topicsync_last_run_success{kafka_cluster="redpanda-metrics-out",mode="apply"} 1`) ||
		!strings.Contains(bodies[0], `topicsync_executed_actions{kafka_cluster="redpanda-metrics-out",mode="apply",kind="create"} 37`) ||
		!strings.Contains(bodies[0], `version="acceptance"`) {
		t.Fatalf("%v", bodies)
	}
}

// Upgrade and rollback: the previous release and this build agree on a cluster, in both directions, so that a
// rollout of a new topicsync (or a rollback) changes nothing by itself.
func TestUpgradeAndRollbackAgree(t *testing.T) {
	prev := os.Getenv("TOPICSYNC_PREVIOUS_BIN")
	if prev == "" {
		t.Skip("TOPICSYNC_PREVIOUS_BIN is not set")
	}
	run(t, prev, nil, "version").expect(t, 0)
	for _, dir := range []struct{ first, then string }{{prev, bin}, {bin, prev}} {
		b := testbroker.Start(t)
		p := writeSpec(t, liveIngestionLike(true))
		run(t, dir.first, nil, "apply", "--spec", p, "--brokers", b.Brokers()).expect(t, 0)
		r := run(t, dir.then, nil, "plan", "--spec", p, "--brokers", b.Brokers(), "--detailed-exitcode")
		if r.code != 0 {
			t.Fatalf("%s applied, %s plans changes (exit %d):\n%s\n%s", dir.first, dir.then, r.code, r.stdout, r.stderr)
		}
		h1 := strings.TrimSpace(run(t, dir.first, nil, "hash", "--spec", p).expect(t, 0).stdout)
		h2 := strings.TrimSpace(run(t, dir.then, nil, "hash", "--spec", p).expect(t, 0).stdout)
		if h1 != h2 {
			t.Errorf("the two builds hash the same file differently: %s %s (every Job would rerun)", h1, h2)
		}
	}
}

// The container image, if given, runs as the Job runs it.
func TestTheImageRuns(t *testing.T) {
	image := os.Getenv("TOPICSYNC_IMAGE")
	if image == "" {
		t.Skip("TOPICSYNC_IMAGE is not set")
	}
	b := testbroker.Start(t)
	p := writeSpec(t, liveIngestionLike(false))
	out, err := exec.Command("docker", "run", "--rm", "--network", "host", "-v", p+":/etc/topicsync/topics.json:ro", image,
		"apply", "--spec", "/etc/topicsync/topics.json", "--brokers", b.Brokers()).CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if !strings.Contains(string(out), "applied") {
		t.Errorf("%s", out)
	}
}
