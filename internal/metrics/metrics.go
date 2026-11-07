// Package metrics reports a run as Prometheus metrics. A run is short-lived (a Kubernetes Job or CronJob), so instead
// of being scraped it pushes its metrics once, in the Prometheus text format, to an endpoint that accepts it
// (VictoriaMetrics' /api/v1/import/prometheus, or a Pushgateway's /metrics/job/<job>), or writes them to a file.
package metrics

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aperiodic-io/topicsync/internal/plan"
)

// Run is what one run reports.
type Run struct {
	Cluster  string
	Mode     string // plan, apply, retire
	Version  string
	Start    time.Time
	Duration time.Duration
	Success  bool
	Desired  int
	Observed int
	// Planned is the plan before executing (for plan mode, the drift).
	Planned *plan.Plan
	// Executed counts executed actions by kind.
	Executed map[plan.Kind]int
	Failed   int
	// Pending is the plan left after executing (empty when converged).
	Pending *plan.Plan

	// SpecHash is the desired-state file's hash: which version of the file the run applied.
	SpecHash string
	// FailedStage is where a failed run failed (spec, connect, observe, plan, execute, verify, retire); empty on success.
	FailedStage string
	// PruneRefused is set when the prune caps refused the run.
	PruneRefused bool
	// Unmanaged counts the cluster's topics the file does not list (internal and protected ones aside).
	Unmanaged int
	// Retries counts retries of retriable errors.
	Retries int
	// Stage durations.
	ObserveDuration, ExecuteDuration, VerifyDuration, LeaderWait time.Duration
}

// Stages are every value of FailedStage, so that each has a series (0 or 1) and an alert can name it.
var Stages = []string{"spec", "connect", "observe", "plan", "execute", "verify", "retire"}

// MaxTopicSeries caps the per-topic series (drift and warnings) one run reports, keeping a run that finds the whole
// cluster different (a wrong file) from flooding the metrics store. topicsync_topic_series_truncated says when it did.
const MaxTopicSeries = 200

type sample struct {
	labels string
	value  float64
}

type family struct {
	name, help, typ string
	samples         []sample
}

// Text renders the run in the Prometheus text exposition format, families and samples in a stable order.
func (r Run) Text() string {
	base := fmt.Sprintf(`kafka_cluster=%q,mode=%q`, escape(r.Cluster), escape(r.Mode))
	b2f := func(b bool) float64 {
		if b {
			return 1
		}
		return 0
	}
	fams := []family{
		{"topicsync_build_info", "The topicsync build that ran.", "gauge", []sample{{fmt.Sprintf(`%s,version=%q`, base, escape(r.Version)), 1}}},
		{"topicsync_last_run_timestamp_seconds", "When the run started, in Unix seconds.", "gauge", []sample{{base, float64(r.Start.Unix())}}},
		{"topicsync_last_run_duration_seconds", "How long the run took.", "gauge", []sample{{base, r.Duration.Seconds()}}},
		{"topicsync_last_run_success", "1 if the run succeeded (for apply: converged), 0 otherwise.", "gauge", []sample{{base, b2f(r.Success)}}},
		{"topicsync_desired_topics", "Topics in the desired-state file.", "gauge", []sample{{base, float64(r.Desired)}}},
		{"topicsync_observed_topics", "Topics on the cluster, internal ones included.", "gauge", []sample{{base, float64(r.Observed)}}},
		{"topicsync_failed_actions", "Actions of the run that failed.", "gauge", []sample{{base, float64(r.Failed)}}},
	}
	perKind := func(name, help string, count func(plan.Kind) int) family {
		f := family{name: name, help: help, typ: "gauge"}
		for _, k := range plan.Kinds {
			f.samples = append(f.samples, sample{fmt.Sprintf(`%s,kind=%q`, base, k), float64(count(k))})
		}
		return f
	}
	fams = append(fams,
		perKind("topicsync_planned_actions", "Actions the plan held before executing: for a plan-only run, the drift from the desired state.",
			func(k plan.Kind) int { return countOf(r.Planned, k) }),
		perKind("topicsync_executed_actions", "Actions the run executed.",
			func(k plan.Kind) int { return r.Executed[k] }),
		perKind("topicsync_pending_actions", "Actions still needed after the run: non-zero means the cluster differs from the desired state.",
			func(k plan.Kind) int { return countOf(r.Pending, k) }),
	)
	stage := family{name: "topicsync_last_run_failed_stage", help: "1 for the stage where the last run failed, 0 for the others.", typ: "gauge"}
	for _, st := range Stages {
		stage.samples = append(stage.samples, sample{fmt.Sprintf(`%s,stage=%q`, base, st), b2f(r.FailedStage == st)})
	}
	fams = append(fams, stage,
		family{"topicsync_spec_info", "The desired-state file the run read, by hash.", "gauge", []sample{{fmt.Sprintf(`%s,spec_hash=%q`, base, escape(r.SpecHash)), 1}}},
		family{"topicsync_prune_refused", "1 if the prune caps refused the run (is the desired-state file complete?).", "gauge", []sample{{base, b2f(r.PruneRefused)}}},
		family{"topicsync_unmanaged_topics", "Topics on the cluster the file does not list, internal and protected ones aside.", "gauge", []sample{{base, float64(r.Unmanaged)}}},
		family{"topicsync_retries", "Retries of retriable errors during the run.", "gauge", []sample{{base, float64(r.Retries)}}},
		family{"topicsync_stage_duration_seconds", "How long each stage of the run took.", "gauge", []sample{
			{base + `,stage="observe"`, r.ObserveDuration.Seconds()},
			{base + `,stage="execute"`, r.ExecuteDuration.Seconds()},
			{base + `,stage="verify"`, r.VerifyDuration.Seconds()},
			{base + `,stage="leader_wait"`, r.LeaderWait.Seconds()},
		}},
	)
	drift := r.Pending
	if r.Mode != "apply" || drift == nil {
		drift = r.Planned
	}
	topicDrift := family{name: "topicsync_topic_drift", help: "1 for each topic and action still needed after the run (the plan, for a plan-only run); only drifting topics have a series.", typ: "gauge"}
	topicWarn := family{name: "topicsync_topic_warning", help: "1 for each topic and warning of the plan; only topics with a warning have a series.", typ: "gauge"}
	truncated := 0
	if drift != nil {
		for _, a := range drift.Actions {
			if len(topicDrift.samples) >= MaxTopicSeries {
				truncated++
				continue
			}
			topicDrift.samples = append(topicDrift.samples, sample{fmt.Sprintf(`%s,topic=%q,kind=%q`, base, escape(a.Topic), a.Kind), 1})
		}
	}
	if r.Planned != nil {
		for _, w := range r.Planned.Warnings {
			if len(topicWarn.samples) >= MaxTopicSeries {
				truncated++
				continue
			}
			topicWarn.samples = append(topicWarn.samples, sample{fmt.Sprintf(`%s,topic=%q,kind=%q`, base, escape(w.Topic), w.Kind), 1})
		}
	}
	fams = append(fams, family{"topicsync_topic_series_truncated", "Per-topic series left out of this run's report (over MaxTopicSeries).", "gauge", []sample{{base, float64(truncated)}}})
	for _, f := range []family{topicDrift, topicWarn} {
		if len(f.samples) > 0 {
			fams = append(fams, f)
		}
	}
	warnings := family{name: "topicsync_warnings", help: "Warnings of the plan, by kind.", typ: "gauge"}
	for _, k := range []plan.WarningKind{plan.PartitionsAboveDesired, plan.ReplicationFactorDiffers, plan.SensitiveConfig, plan.PruneNotRequested} {
		n := 0
		if r.Planned != nil {
			n = r.Planned.WarningCount(k)
		}
		warnings.samples = append(warnings.samples, sample{fmt.Sprintf(`%s,kind=%q`, base, k), float64(n)})
	}
	fams = append(fams, warnings)

	var b strings.Builder
	sort.SliceStable(fams, func(i, j int) bool { return fams[i].name < fams[j].name })
	for _, f := range fams {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", f.name, f.help, f.name, f.typ)
		for _, s := range f.samples {
			fmt.Fprintf(&b, "%s{%s} %s\n", f.name, s.labels, formatFloat(s.value))
		}
	}
	return b.String()
}

func countOf(p *plan.Plan, k plan.Kind) int {
	if p == nil {
		return 0
	}
	return p.Count(k)
}

func formatFloat(v float64) string {
	if v == float64(int64(v)) {
		return fmt.Sprintf("%d", int64(v))
	}
	return fmt.Sprintf("%g", v)
}

func escape(s string) string {
	// %q already escapes quotes and backslashes; newlines are not valid in a label value
	return strings.ReplaceAll(s, "\n", " ")
}

// Push POSTs the metrics to url. Any 2xx status is success.
func Push(ctx context.Context, client *http.Client, url, body string) error {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBufferString(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain; version=0.0.4")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("pushing metrics to %s: %s: %s", url, resp.Status, strings.TrimSpace(string(msg)))
	}
	return nil
}

// WriteFile writes the metrics to path atomically (a temporary file renamed over it), as a textfile collector
// expects.
func WriteFile(path, body string) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".topicsync-*.prom")
	if err != nil {
		return err
	}
	if _, err := tmp.WriteString(body); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), path)
}
