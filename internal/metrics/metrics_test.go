package metrics

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aperiodic-io/topicsync/internal/plan"
)

func sampleRun() Run {
	p := &plan.Plan{Actions: []plan.Action{{Kind: plan.Create, Topic: "a"}, {Kind: plan.Create, Topic: "b"}, {Kind: plan.AlterConfigs, Topic: "c"}},
		Warnings: []plan.Warning{{Kind: plan.ReplicationFactorDiffers, Topic: "d"}}}
	return Run{Cluster: "metrics-out", Mode: "apply", Version: "v1.2.3", Start: time.Unix(1794040000, 0), Duration: 1500 * time.Millisecond,
		Success: true, Desired: 600, Observed: 612, Planned: p, Executed: map[plan.Kind]int{plan.Create: 2, plan.AlterConfigs: 1},
		Pending: &plan.Plan{}}
}

func TestTextIsPrometheusExposition(t *testing.T) {
	text := sampleRun().Text()
	for _, want := range []string{
		`# TYPE topicsync_last_run_success gauge`,
		`topicsync_last_run_success{kafka_cluster="metrics-out",mode="apply"} 1`,
		`topicsync_last_run_timestamp_seconds{kafka_cluster="metrics-out",mode="apply"} 1794040000`,
		`topicsync_last_run_duration_seconds{kafka_cluster="metrics-out",mode="apply"} 1.5`,
		`topicsync_desired_topics{kafka_cluster="metrics-out",mode="apply"} 600`,
		`topicsync_planned_actions{kafka_cluster="metrics-out",mode="apply",kind="create"} 2`,
		`topicsync_planned_actions{kafka_cluster="metrics-out",mode="apply",kind="delete"} 0`,
		`topicsync_executed_actions{kafka_cluster="metrics-out",mode="apply",kind="alter_configs"} 1`,
		`topicsync_pending_actions{kafka_cluster="metrics-out",mode="apply",kind="create"} 0`,
		`topicsync_warnings{kafka_cluster="metrics-out",mode="apply",kind="replication_factor_differs"} 1`,
		`topicsync_build_info{kafka_cluster="metrics-out",mode="apply",version="v1.2.3"} 1`,
	} {
		if !strings.Contains(text, want+"\n") {
			t.Errorf("missing line %q in:\n%s", want, text)
		}
	}
	// every sample line parses as name{labels} value
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		if i := strings.Index(line, "} "); i < 0 || !strings.Contains(line[:i], "{") {
			t.Errorf("malformed sample %q", line)
		}
	}
	if sampleRun().Text() != text {
		t.Error("Text is not deterministic")
	}
}

// A run that failed before planning measured no drift: it reports its failure, and none of the plan's families, so
// that a drift alert does not resolve on a run that saw nothing.
func TestAFailedRunWithoutAPlanReportsNoPlanFamilies(t *testing.T) {
	r := Run{Cluster: "c", Mode: "plan", Start: time.Unix(1, 0), FailedStage: "observe"}
	text := r.Text()
	if !strings.Contains(text, `topicsync_last_run_success{kafka_cluster="c",mode="plan"} 0`) ||
		!strings.Contains(text, `topicsync_last_run_failed_stage{kafka_cluster="c",mode="plan",stage="observe"} 1`) {
		t.Errorf("%s", text)
	}
	for _, absent := range []string{"topicsync_planned_actions{", "topicsync_pending_actions{", "topicsync_unmanaged_topics{", "topicsync_observed_topics{", "topicsync_warnings{", "topicsync_topic_drift{"} {
		if strings.Contains(text, absent) {
			t.Errorf("an unmeasured family %s is reported", absent)
		}
	}
	// an apply whose verification failed: the plan before, but no pending counts
	r = Run{Cluster: "c", Mode: "apply", Planned: &plan.Plan{}, FailedStage: "verify"}
	if text := r.Text(); strings.Contains(text, "topicsync_pending_actions{") || !strings.Contains(text, "topicsync_planned_actions{") {
		t.Errorf("verify failure:\n%s", text)
	}
}

// Prometheus' text format escapes only backslash, double quote and newline; a tab stays a tab.
func TestLabelValuesAreEscapedAsPrometheusExpects(t *testing.T) {
	r := Run{Cluster: "a\"b\\c\nd\te", Mode: "plan"}
	if !strings.Contains(r.Text(), `kafka_cluster="a\"b\\c\nd`+"\t"+`e"`) {
		t.Errorf("%s", r.Text())
	}
}

// One series per topic and kind, even with two sensitive keys on a topic: a Pushgateway refuses duplicates.
func TestPerTopicSeriesAreUnique(t *testing.T) {
	r := Run{Cluster: "c", Mode: "plan", Planned: &plan.Plan{Warnings: []plan.Warning{
		{Kind: plan.SensitiveConfig, Topic: "t"}, {Kind: plan.SensitiveConfig, Topic: "t"}}}}
	if n := strings.Count(r.Text(), "topicsync_topic_warning{"); n != 1 {
		t.Errorf("%d series for one topic and kind", n)
	}
}

func TestPushPostsTheText(t *testing.T) {
	var got, ctype string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got, ctype = string(b), r.Header.Get("Content-Type")
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	if err := Push(context.Background(), nil, srv.URL+"/api/v1/import/prometheus", "x 1\n"); err != nil {
		t.Fatal(err)
	}
	if got != "x 1\n" || !strings.HasPrefix(ctype, "text/plain") {
		t.Errorf("body %q type %q", got, ctype)
	}
}

// A Pushgateway group is replaced whole (PUT), so a topic that stopped drifting loses its series; other endpoints
// (VictoriaMetrics' import) get a POST.
func TestPushUsesPUTForAPushgatewayGroup(t *testing.T) {
	var method string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { method = r.Method }))
	defer srv.Close()
	if err := Push(context.Background(), nil, srv.URL+"/metrics/job/topicsync/kafka_cluster/x", "x 1\n"); err != nil || method != http.MethodPut {
		t.Errorf("pushgateway: %s %v", method, err)
	}
	if err := Push(context.Background(), nil, srv.URL+"/api/v1/import/prometheus", "x 1\n"); err != nil || method != http.MethodPost {
		t.Errorf("import: %s %v", method, err)
	}
}

func TestPushErrorsHideThePassword(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "no", http.StatusUnauthorized) }))
	defer srv.Close()
	u := strings.Replace(srv.URL, "http://", "http://user:s3cret@", 1)
	for _, target := range []string{u, "http://user:s3cret@127.0.0.1:1/x"} {
		err := Push(context.Background(), nil, target, "x 1\n")
		if err == nil || strings.Contains(err.Error(), "s3cret") {
			t.Errorf("error %v shows the password", err)
		}
	}
}

func TestPushReportsAnErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "cannot parse", http.StatusBadRequest)
	}))
	defer srv.Close()
	err := Push(context.Background(), srv.Client(), srv.URL, "x 1\n")
	if err == nil || !strings.Contains(err.Error(), "400") || !strings.Contains(err.Error(), "cannot parse") {
		t.Fatalf("err %v", err)
	}
	if err := Push(context.Background(), nil, "http://127.0.0.1:1/x", "x"); err == nil {
		t.Error("push to a closed port succeeded")
	}
	if err := Push(context.Background(), nil, "::bad", "x"); err == nil {
		t.Error("push to a bad URL succeeded")
	}
}

func TestWriteFileReplacesAtomically(t *testing.T) {
	dir := t.TempDir()
	p := dir + "/topicsync.prom"
	if err := WriteFile(p, "a 1\n"); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(p, "a 2\n"); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(p)
	if string(b) != "a 2\n" {
		t.Errorf("%q", b)
	}
	if st, _ := os.Stat(p); st.Mode().Perm() != 0o644 {
		t.Errorf("mode %v: a textfile collector running as another user cannot read it", st.Mode().Perm())
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Errorf("leftover temporary files: %v", entries)
	}
	if err := WriteFile(dir+"/missing/x.prom", "a"); err == nil {
		t.Error("write into a missing directory succeeded")
	}
}

func TestStagesSpecHashUnmanagedAndTimings(t *testing.T) {
	r := sampleRun()
	r.Success, r.FailedStage, r.SpecHash, r.Unmanaged, r.Retries = false, "execute", "abc123", 7, 4
	r.ObserveDuration, r.ExecuteDuration, r.LeaderWait = 2*time.Second, 3*time.Second, 500*time.Millisecond
	r.PruneRefused = true
	text := r.Text()
	for _, want := range []string{
		`topicsync_last_run_failed_stage{kafka_cluster="metrics-out",mode="apply",stage="execute"} 1`,
		`topicsync_last_run_failed_stage{kafka_cluster="metrics-out",mode="apply",stage="spec"} 0`,
		`topicsync_spec_info{kafka_cluster="metrics-out",mode="apply",spec_hash="abc123"} 1`,
		`topicsync_unmanaged_topics{kafka_cluster="metrics-out",mode="apply"} 7`,
		`topicsync_retries{kafka_cluster="metrics-out",mode="apply"} 4`,
		`topicsync_prune_refused{kafka_cluster="metrics-out",mode="apply"} 1`,
		`topicsync_stage_duration_seconds{kafka_cluster="metrics-out",mode="apply",stage="execute"} 3`,
		`topicsync_stage_duration_seconds{kafka_cluster="metrics-out",mode="apply",stage="leader_wait"} 0.5`,
	} {
		if !strings.Contains(text, want+"\n") {
			t.Errorf("missing %q", want)
		}
	}
	for _, st := range Stages {
		if !strings.Contains(text, `stage="`+st+`"}`) {
			t.Errorf("no series for stage %s", st)
		}
	}
}

// Per-topic series name what drifts, for drifting topics only: a converged cluster reports none, so their number stays
// bounded by the drift, and a wrong file cannot report more than MaxTopicSeries.
func TestPerTopicSeriesOnlyForDriftAndCapped(t *testing.T) {
	r := sampleRun() // apply, converged: Pending empty
	if strings.Contains(r.Text(), "topicsync_topic_drift{") {
		t.Error("a converged apply reports drifting topics")
	}
	if !strings.Contains(r.Text(), `topicsync_topic_warning{kafka_cluster="metrics-out",mode="apply",topic="d",kind="replication_factor_differs"} 1`) {
		t.Errorf("no warning series:\n%s", r.Text())
	}
	r.Pending = &plan.Plan{Actions: []plan.Action{{Kind: plan.AlterConfigs, Topic: "stuck"}}}
	if !strings.Contains(r.Text(), `topicsync_topic_drift{kafka_cluster="metrics-out",mode="apply",topic="stuck",kind="alter_configs"} 1`) {
		t.Errorf("no drift series for a pending action:\n%s", r.Text())
	}
	// a plan-only run reports its plan as the drift
	p := Run{Cluster: "c", Mode: "plan", Planned: &plan.Plan{Actions: []plan.Action{{Kind: plan.Create, Topic: "new"}}}}
	if !strings.Contains(p.Text(), `topicsync_topic_drift{kafka_cluster="c",mode="plan",topic="new",kind="create"} 1`) {
		t.Errorf("%s", p.Text())
	}
	big := &plan.Plan{}
	for i := range MaxTopicSeries + 50 {
		big.Actions = append(big.Actions, plan.Action{Kind: plan.Create, Topic: strings.Repeat("x", 3) + string(rune('a'+i%26)) + time.Duration(i).String()})
	}
	p.Planned = big
	text := p.Text()
	if n := strings.Count(text, "topicsync_topic_drift{"); n != MaxTopicSeries {
		t.Errorf("%d drift series, cap %d", n, MaxTopicSeries)
	}
	if !strings.Contains(text, `topicsync_topic_series_truncated{kafka_cluster="c",mode="plan"} 50`) {
		t.Error("truncation not reported")
	}
}

func TestRedactionAndUnwrapping(t *testing.T) {
	if got := redact("http://u:p@h/x"); strings.Contains(got, "p@") && !strings.Contains(got, "xxxxx") {
		t.Errorf("redact %q", got)
	}
	if got := redact("http://[::1"); got != "(an unparseable URL)" {
		t.Errorf("redact of a bad URL: %q", got)
	}
	plain := io.EOF
	if unwrapURLError(plain) != plain {
		t.Error("unwrap changed a non-URL error")
	}
}

func TestWriteFileFailsWhenItCannotRename(t *testing.T) {
	dir := t.TempDir()
	target := dir + "/is-a-dir"
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target+"/x", []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(target, "a 1\n"); err == nil {
		t.Error("renaming over a non-empty directory succeeded")
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".topicsync-") {
			t.Errorf("leftover temporary file %s", e.Name())
		}
	}
}
