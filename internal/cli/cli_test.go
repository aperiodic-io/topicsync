package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aperiodic-io/topicsync/internal/cluster"
	"github.com/aperiodic-io/topicsync/internal/kafka"
	"github.com/aperiodic-io/topicsync/internal/plan"
)

const specJSON = `{
  "apiVersion": "topicsync.aperiodic.io/v1",
  "cluster": "metrics",
  "defaults": {"partitions": 1, "replicationFactor": 1, "configs": {"cleanup.policy": "delete"}},
  "topics": [{"name": "m.a"}, {"name": "m.b", "partitions": 2}],
  "prune": {"enabled": true, "patterns": ["^m\\..*$"]}
}`

type harness struct {
	t        *testing.T
	fake     *cluster.Fake
	stdout   bytes.Buffer
	stderr   bytes.Buffer
	env      map[string]string
	got      kafka.Config
	connErr  error
	specPath string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{t: t, fake: cluster.NewFake(map[string]string{"cleanup.policy": "delete"}), env: map[string]string{}}
	h.specPath = h.write("topics.json", specJSON)
	return h
}

func (h *harness) write(name, content string) string {
	p := filepath.Join(h.t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		h.t.Fatal(err)
	}
	return p
}

func (h *harness) run(args ...string) int {
	h.stdout.Reset()
	h.stderr.Reset()
	return Run(context.Background(), args, Env{
		Stdout: &h.stdout, Stderr: &h.stderr,
		Getenv: func(k string) string { return h.env[k] },
		NewCluster: func(c kafka.Config) (cluster.Cluster, error) {
			h.got = c
			if h.connErr != nil {
				return nil, h.connErr
			}
			return h.fake, nil
		},
		Now:   func() time.Time { return time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC) },
		Sleep: func(ctx context.Context, _ time.Duration) error { return ctx.Err() },
	})
}

func (h *harness) expect(code, want int) {
	h.t.Helper()
	if code != want {
		h.t.Fatalf("exit %d, want %d\nstdout:\n%s\nstderr:\n%s", code, want, h.stdout.String(), h.stderr.String())
	}
}

func TestValidateAndHash(t *testing.T) {
	h := newHarness(t)
	h.expect(h.run("validate", "--spec", h.specPath), ExitOK)
	if !strings.Contains(h.stdout.String(), "valid: cluster metrics, 2 topics, prune enabled, hash ") {
		t.Errorf("%s", h.stdout.String())
	}
	h.expect(h.run("hash", "--spec", h.specPath), ExitOK)
	if len(strings.TrimSpace(h.stdout.String())) != 64 {
		t.Errorf("hash %q", h.stdout.String())
	}
	bad := h.write("bad.json", `{"apiVersion": "x"}`)
	h.expect(h.run("validate", "--spec", bad), ExitUsage)
	if !strings.Contains(h.stderr.String(), "invalid desired-state file") {
		t.Errorf("%s", h.stderr.String())
	}
	h.expect(h.run("validate"), ExitUsage)
	h.expect(h.run("validate", "--spec", h.specPath, "extra"), ExitUsage)
	h.expect(h.run("validate", "--nope"), ExitUsage)
}

func TestPlanShowsChangesAndDetailedExitCode(t *testing.T) {
	h := newHarness(t)
	h.expect(h.run("plan", "--spec", h.specPath, "--brokers", "b1:9092, b2:9092"), ExitOK)
	if !strings.Contains(h.stdout.String(), "+ create m.a") || !strings.Contains(h.stdout.String(), "2 to create") {
		t.Errorf("%s", h.stdout.String())
	}
	if len(h.got.Brokers) != 2 || h.got.Brokers[1] != "b2:9092" {
		t.Errorf("brokers %v", h.got.Brokers)
	}
	if h.fake.Len() != 0 {
		t.Fatal("plan changed the cluster")
	}
	h.expect(h.run("plan", "--spec", h.specPath, "--brokers", "b", "--detailed-exitcode"), ExitDrift)

	h.fake.Put("m.a", 1, 1, nil)
	h.fake.Put("m.b", 2, 1, nil)
	h.expect(h.run("plan", "--spec", h.specPath, "--brokers", "b", "--detailed-exitcode"), ExitOK)
}

func TestPlanJSON(t *testing.T) {
	h := newHarness(t)
	h.expect(h.run("plan", "--spec", h.specPath, "--brokers", "b", "--output", "json"), ExitOK)
	var p plan.Plan
	if err := json.Unmarshal(h.stdout.Bytes(), &p); err != nil {
		t.Fatalf("%v\n%s", err, h.stdout.String())
	}
	if p.Count(plan.Create) != 2 || p.Cluster != "metrics" || len(p.Hash) != 64 {
		t.Errorf("%+v", p)
	}
	h.expect(h.run("plan", "--spec", h.specPath, "--brokers", "b", "--output", "yaml"), ExitUsage)
}

func TestApplyCreatesAndIsIdempotent(t *testing.T) {
	h := newHarness(t)
	h.expect(h.run("apply", "--spec", h.specPath, "--brokers", "b"), ExitOK)
	if h.fake.Len() != 2 || h.fake.Partitions("m.b") != 2 {
		t.Fatalf("cluster has %d topics", h.fake.Len())
	}
	if !strings.Contains(h.stderr.String(), "applied: the cluster matches the desired state") {
		t.Errorf("log: %s", h.stderr.String())
	}
	calls := len(h.fake.Calls)
	h.expect(h.run("apply", "--spec", h.specPath, "--brokers", "b"), ExitOK)
	if len(h.fake.Calls) != calls {
		t.Errorf("a second apply changed something: %v", h.fake.Calls[calls:])
	}
}

func TestApplyDryRunChangesNothing(t *testing.T) {
	h := newHarness(t)
	h.expect(h.run("apply", "--spec", h.specPath, "--brokers", "b", "--dry-run"), ExitOK)
	if h.fake.Len() != 0 {
		t.Fatal("dry run created topics")
	}
}

func TestApplyFailureExitsOne(t *testing.T) {
	h := newHarness(t)
	h.fake.FailAlways["create:m.a"] = errors.New("POLICY_VIOLATION")
	h.expect(h.run("apply", "--spec", h.specPath, "--brokers", "b", "--log-format", "json"), ExitFailure)
	var sawError bool
	for _, line := range strings.Split(strings.TrimSpace(h.stderr.String()), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("non-JSON log line %q", line)
		}
		if m["level"] == "ERROR" && strings.Contains(m["error"].(string), "POLICY_VIOLATION") {
			sawError = true
		}
	}
	if !sawError {
		t.Errorf("no error log: %s", h.stderr.String())
	}
}

func TestPruneNeedsTheFlag(t *testing.T) {
	h := newHarness(t)
	for _, n := range []string{"m.a", "m.b", "m.c", "m.d", "m.e", "m.f", "m.g", "m.h"} {
		h.fake.Put(n, 2, 1, nil)
	}
	h.fake.Put("m.gone", 1, 1, nil)
	h.expect(h.run("apply", "--spec", h.specPath, "--brokers", "b"), ExitOK)
	if !h.fake.Has("m.gone") {
		t.Fatal("pruned without --prune")
	}
	// 7 prunable of 9 matching: over the default 25% cap
	h.expect(h.run("apply", "--spec", h.specPath, "--brokers", "b", "--prune"), ExitFailure)
	if !strings.Contains(h.stderr.String(), "refusing to prune 7 topics") || !h.fake.Has("m.gone") {
		t.Fatalf("%s", h.stderr.String())
	}
	h.expect(h.run("apply", "--spec", h.specPath, "--brokers", "b", "--prune", "--prune-max-fraction", "1"), ExitOK)
	if h.fake.Has("m.gone") || !h.fake.Has("m.a") {
		t.Fatal("prune did not delete exactly the unlisted topics")
	}
}

func TestPruneWithAFileThatDoesNotEnableItFails(t *testing.T) {
	h := newHarness(t)
	p := h.write("noprune.json", strings.Replace(specJSON, `"enabled": true`, `"enabled": false`, 1))
	h.expect(h.run("apply", "--spec", p, "--brokers", "b", "--prune"), ExitFailure)
	if !strings.Contains(h.stderr.String(), "does not enable pruning") {
		t.Errorf("%s", h.stderr.String())
	}
}

func TestUsageErrors(t *testing.T) {
	h := newHarness(t)
	for _, args := range [][]string{
		{},
		{"frobnicate"},
		{"plan"},
		{"plan", "--spec", h.specPath},
		{"plan", "--spec", h.specPath, "--brokers", "b", "stray"},
		{"plan", "--spec", h.specPath, "--brokers", "b", "--log-format", "xml"},
		{"plan", "--spec", h.specPath, "--brokers", "b", "--log-level", "loud"},
		{"plan", "--spec", h.specPath, "--brokers", "b", "--prune-max-fraction", "0"},
		{"plan", "--spec", h.specPath, "--brokers", "b", "--prune-max-deletes", "-1"},
		{"plan", "--spec", h.specPath + ".missing", "--brokers", "b"},
		{"plan", "--spec", h.specPath, "--brokers", "b", "--sasl-mechanism", "PLAIN"},
		{"retire", "--spec", h.specPath, "--brokers", "b"},
	} {
		if code := h.run(args...); code != ExitUsage {
			t.Errorf("%v: exit %d, want %d\n%s", args, code, ExitUsage, h.stderr.String())
		}
	}
}

func TestVersionAndHelp(t *testing.T) {
	h := newHarness(t)
	h.expect(h.run("version"), ExitOK)
	if !strings.HasPrefix(h.stdout.String(), "topicsync ") {
		t.Error(h.stdout.String())
	}
	h.expect(h.run("help"), ExitOK)
	if !strings.Contains(h.stdout.String(), "Exit codes") {
		t.Error(h.stdout.String())
	}
}

func TestBrokersAndCredentialsFromTheEnvironment(t *testing.T) {
	h := newHarness(t)
	h.env["TOPICSYNC_BROKERS"] = "env1:9092,env2:9092"
	h.env["TOPICSYNC_SASL_USER"] = "u"
	h.env["TOPICSYNC_SASL_PASSWORD"] = "p"
	h.expect(h.run("plan", "--spec", h.specPath, "--sasl-mechanism", "scram-sha-512"), ExitOK)
	if len(h.got.Brokers) != 2 || h.got.SASLMechanism != "SCRAM-SHA-512" || h.got.SASLUser != "u" || h.got.SASLPassword != "p" {
		t.Errorf("%+v", h.got)
	}
	pw := h.write("pw", "from-file\n")
	h.expect(h.run("plan", "--spec", h.specPath, "--sasl-mechanism", "PLAIN", "--sasl-password-file", pw, "--tls-ca", "/ca.pem"), ExitOK)
	if h.got.SASLPassword != "from-file" || !h.got.TLS || h.got.TLSCAFile != "/ca.pem" {
		t.Errorf("%+v", h.got)
	}
	// a Secret that is not mounted is the environment's failure, reported under the config stage, not a usage error
	h.expect(h.run("plan", "--spec", h.specPath, "--sasl-mechanism", "PLAIN", "--sasl-password-file", pw+".missing"), ExitFailure)
	// credentials without a mechanism would connect unauthenticated
	delete(h.env, "TOPICSYNC_SASL_PASSWORD")
	h.expect(h.run("plan", "--spec", h.specPath, "--sasl-password-file", pw), ExitUsage)
}

func TestAConnectionFailureExitsOne(t *testing.T) {
	h := newHarness(t)
	h.connErr = errors.New("dial refused")
	h.expect(h.run("plan", "--spec", h.specPath, "--brokers", "b"), ExitFailure)
	h.connErr = nil
	h.fake.ObserveErr = errors.New("all brokers down")
	h.expect(h.run("apply", "--spec", h.specPath, "--brokers", "b"), ExitFailure)
}

func TestRetire(t *testing.T) {
	h := newHarness(t)
	h.fake.Put("old.x", 1, 1, nil)
	h.fake.Newest["old.x"] = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	h.fake.Groups["old.x"] = []cluster.GroupUse{{Group: "arch", State: "Empty", Committed: true}}
	h.expect(h.run("retire", "--spec", h.specPath, "--brokers", "b", "--topic", "old.x"), ExitOK)
	if !h.fake.Has("old.x") || !strings.Contains(h.stdout.String(), "run again with --confirm old.x") || !strings.Contains(h.stdout.String(), "group arch") {
		t.Fatalf("%s", h.stdout.String())
	}
	h.expect(h.run("retire", "--spec", h.specPath, "--brokers", "b", "--topic", "old.x", "--confirm", "old.x"), ExitOK)
	if h.fake.Has("old.x") || !strings.Contains(h.stdout.String(), "deleted old.x") {
		t.Fatalf("%s", h.stdout.String())
	}
	h.fake.Put("m.a", 1, 1, nil)
	h.expect(h.run("retire", "--spec", h.specPath, "--brokers", "b", "--topic", "m.a", "--confirm", "m.a"), ExitFailure)
	if !h.fake.Has("m.a") || !strings.Contains(h.stderr.String(), "still in the desired-state file") {
		t.Fatalf("%s", h.stderr.String())
	}
	h.fake.Put("empty.x", 1, 1, nil)
	h.expect(h.run("retire", "--spec", h.specPath, "--brokers", "b", "--topic", "empty.x"), ExitOK)
	if !strings.Contains(h.stdout.String(), "empty.x holds no record") {
		t.Error(h.stdout.String())
	}
}

// The run's metrics are pushed whatever its outcome, and a push that fails does not change the run's exit code.
func TestMetricsArePushedAndWrittenWhateverTheOutcome(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(b))
		mu.Unlock()
	}))
	defer srv.Close()
	h := newHarness(t)
	file := filepath.Join(t.TempDir(), "m.prom")
	h.env["TOPICSYNC_METRICS_PUSH_URL"] = srv.URL
	h.expect(h.run("apply", "--spec", h.specPath, "--brokers", "b", "--metrics-file", file), ExitOK)
	h.fake.FailAlways["alter:m.a"] = errors.New("x")
	h.fake.Put("m.a", 1, 1, map[string]string{"cleanup.policy": "compact"})
	h.expect(h.run("apply", "--spec", h.specPath, "--brokers", "b"), ExitFailure)
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 2 {
		t.Fatalf("%d pushes", len(bodies))
	}
	if !strings.Contains(bodies[0], `topicsync_last_run_success{kafka_cluster="metrics",mode="apply"} 1`) ||
		!strings.Contains(bodies[0], `topicsync_executed_actions{kafka_cluster="metrics",mode="apply",kind="create"} 2`) {
		t.Errorf("first push:\n%s", bodies[0])
	}
	if !strings.Contains(bodies[1], `topicsync_last_run_success{kafka_cluster="metrics",mode="apply"} 0`) ||
		!strings.Contains(bodies[1], `topicsync_failed_actions{kafka_cluster="metrics",mode="apply"} 1`) {
		t.Errorf("second push:\n%s", bodies[1])
	}
	b, err := os.ReadFile(file)
	if err != nil || !strings.Contains(string(b), "topicsync_last_run_success") {
		t.Errorf("metrics file: %v", err)
	}

	h.env["TOPICSYNC_METRICS_PUSH_URL"] = "http://127.0.0.1:1/unreachable"
	delete(h.fake.FailAlways, "alter:m.a")
	h.expect(h.run("apply", "--spec", h.specPath, "--brokers", "b"), ExitOK)
	if !strings.Contains(h.stderr.String(), "pushing metrics (the run's result stands)") {
		t.Errorf("%s", h.stderr.String())
	}
}

func TestAnEmptyFileNeedsAllowEmpty(t *testing.T) {
	h := newHarness(t)
	empty := h.write("empty.json", `{"apiVersion": "topicsync.aperiodic.io/v1", "cluster": "c", "topics": []}`)
	h.expect(h.run("apply", "--spec", empty, "--brokers", "b"), ExitFailure)
	h.expect(h.run("apply", "--spec", empty, "--brokers", "b", "--allow-empty"), ExitOK)
}

func TestDefaultEnvIsWired(t *testing.T) {
	e := DefaultEnv()
	if e.Stdout == nil || e.NewCluster == nil || e.Getenv == nil {
		t.Fatal("DefaultEnv is incomplete")
	}
	c, err := e.NewCluster(kafka.Config{Brokers: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
}

func pushServer(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(b))
		mu.Unlock()
	}))
	t.Cleanup(srv.Close)
	return srv, func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), bodies...) }
}

// A Job whose file cannot be read, or whose cluster cannot be reached, still reports, under its cluster label, with
// the stage it failed at: that is what the alerts key on.
func TestSpecAndConnectFailuresReportTheirStage(t *testing.T) {
	srv, bodies := pushServer(t)
	h := newHarness(t)
	h.env["TOPICSYNC_METRICS_PUSH_URL"] = srv.URL
	bad := h.write("bad.json", `{"apiVersion": "nope"}`)
	h.expect(h.run("apply", "--spec", bad, "--brokers", "b", "--cluster-label", "metrics-out"), ExitUsage)
	h.connErr = errors.New("dial refused")
	h.expect(h.run("apply", "--spec", h.specPath, "--brokers", "b", "--cluster-label", "metrics-out"), ExitFailure)
	got := bodies()
	if len(got) != 2 {
		t.Fatalf("%d pushes", len(got))
	}
	for i, stage := range []string{"spec", "connect"} {
		want := `topicsync_last_run_failed_stage{kafka_cluster="metrics-out",mode="apply",stage="` + stage + `"} 1`
		if !strings.Contains(got[i], want) || !strings.Contains(got[i], `topicsync_last_run_success{kafka_cluster="metrics-out",mode="apply"} 0`) {
			t.Errorf("push %d lacks %q:\n%s", i, want, got[i])
		}
	}
}

func TestPruneRefusalAndDriftAreReported(t *testing.T) {
	srv, bodies := pushServer(t)
	h := newHarness(t)
	h.env["TOPICSYNC_METRICS_PUSH_URL"] = srv.URL
	for _, n := range []string{"m.c", "m.d", "m.e", "m.f"} {
		h.fake.Put(n, 1, 1, nil)
	}
	h.expect(h.run("apply", "--spec", h.specPath, "--brokers", "b", "--prune"), ExitFailure)
	h.expect(h.run("plan", "--spec", h.specPath, "--brokers", "b"), ExitOK)
	got := bodies()
	if !strings.Contains(got[0], `topicsync_prune_refused{kafka_cluster="metrics",mode="apply"} 1`) ||
		!strings.Contains(got[0], `topicsync_last_run_failed_stage{kafka_cluster="metrics",mode="apply",stage="plan"} 1`) {
		t.Errorf("refused prune:\n%s", got[0])
	}
	for _, want := range []string{
		`topicsync_topic_drift{kafka_cluster="metrics",mode="plan",topic="m.a",kind="create"} 1`,
		`topicsync_unmanaged_topics{kafka_cluster="metrics",mode="plan"} 4`,
		`topicsync_topic_warning{kafka_cluster="metrics",mode="plan",topic="m.c",kind="prune_not_requested"} 1`,
		`topicsync_spec_info{kafka_cluster="metrics",mode="plan",spec_hash="`,
	} {
		if !strings.Contains(got[1], want) {
			t.Errorf("plan push lacks %q", want)
		}
	}
}

func TestConfigFailuresAndDryRunsReportTheirOwnLabels(t *testing.T) {
	srv, bodies := pushServer(t)
	h := newHarness(t)
	h.env["TOPICSYNC_METRICS_PUSH_URL"] = srv.URL
	h.env["TOPICSYNC_SASL_USER"] = "u"
	h.expect(h.run("apply", "--spec", h.specPath, "--brokers", "b", "--sasl-mechanism", "SCRAM-SHA-512", "--sasl-password-file", "/nonexistent",
		"--cluster-label", "metrics-out"), ExitFailure)
	delete(h.env, "TOPICSYNC_SASL_USER")
	h.expect(h.run("apply", "--spec", h.specPath, "--brokers", "b", "--dry-run"), ExitOK)
	got := bodies()
	if len(got) != 2 {
		t.Fatalf("%d pushes", len(got))
	}
	if !strings.Contains(got[0], `topicsync_last_run_failed_stage{kafka_cluster="metrics-out",mode="apply",stage="config"} 1`) {
		t.Errorf("config failure:\n%s", got[0])
	}
	if !strings.Contains(got[1], `topicsync_last_run_success{kafka_cluster="metrics",mode="dry-run"} 1`) || strings.Contains(got[1], `mode="apply"`) {
		t.Errorf("a dry run reports as an apply:\n%s", got[1])
	}
}
