// Package metrics reports a run as Prometheus metrics. A run is short-lived (a Kubernetes Job or CronJob), so instead
// of being scraped it pushes its metrics once, in the Prometheus text format, to an endpoint that accepts it
// (VictoriaMetrics' /api/v1/import/prometheus, or a Pushgateway's /metrics/job/<job>), or writes them to a file.
package metrics

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
var Stages = []string{"spec", "config", "connect", "observe", "plan", "execute", "verify", "retire"}

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
//
// What a run did not measure is left out rather than reported as zero: a run that failed before planning (no
// plan) reports no planned, pending or warning counts, no observed or unmanaged topics, no drift; a run whose
// verification failed (no final plan) reports no pending counts. A zero would resolve drift alerts on a run that
// saw nothing. Alerts on these families must go with topicsync_last_run_success.
func (r Run) Text() string {
	base := "kafka_cluster=" + label(r.Cluster) + ",mode=" + label(r.Mode)
	b2f := func(b bool) float64 {
		if b {
			return 1
		}
		return 0
	}
	one := func(name, help string, v float64) family {
		return family{name, help, "gauge", []sample{{base, v}}}
	}
	fams := []family{
		{"topicsync_build_info", "The topicsync build that ran.", "gauge", []sample{{base + ",version=" + label(r.Version), 1}}},
		one("topicsync_last_run_timestamp_seconds", "When the run started, in Unix seconds.", float64(r.Start.Unix())),
		one("topicsync_last_run_duration_seconds", "How long the run took.", r.Duration.Seconds()),
		one("topicsync_last_run_success", "1 if the run succeeded (for apply: converged), 0 otherwise.", b2f(r.Success)),
		one("topicsync_desired_topics", "Topics in the desired-state file.", float64(r.Desired)),
		one("topicsync_failed_actions", "Actions of the run that failed.", float64(r.Failed)),
		one("topicsync_prune_refused", "1 if the prune caps refused the run (is the desired-state file complete?).", b2f(r.PruneRefused)),
		one("topicsync_retries", "Retries of retriable errors during the run.", float64(r.Retries)),
		{"topicsync_stage_duration_seconds", "How long each stage of the run took.", "gauge", []sample{
			{base + `,stage="observe"`, r.ObserveDuration.Seconds()},
			{base + `,stage="execute"`, r.ExecuteDuration.Seconds()},
			{base + `,stage="verify"`, r.VerifyDuration.Seconds()},
			{base + `,stage="leader_wait"`, r.LeaderWait.Seconds()},
		}},
	}
	if r.SpecHash != "" {
		fams = append(fams, family{"topicsync_spec_info", "The desired-state file the run read, by hash.", "gauge", []sample{{base + ",spec_hash=" + label(r.SpecHash), 1}}})
	}
	stage := family{name: "topicsync_last_run_failed_stage", help: "1 for the stage where the last run failed, 0 for the others.", typ: "gauge"}
	for _, st := range Stages {
		stage.samples = append(stage.samples, sample{base + ",stage=" + label(st), b2f(r.FailedStage == st)})
	}
	fams = append(fams, stage)
	perKind := func(name, help string, count func(plan.Kind) int) family {
		f := family{name: name, help: help, typ: "gauge"}
		for _, k := range plan.Kinds {
			f.samples = append(f.samples, sample{base + ",kind=" + label(string(k)), float64(count(k))})
		}
		return f
	}
	fams = append(fams, perKind("topicsync_executed_actions", "Actions the run executed.", func(k plan.Kind) int { return r.Executed[k] }))

	truncated := 0
	if r.Planned != nil {
		fams = append(fams,
			one("topicsync_observed_topics", "Topics on the cluster, internal ones included.", float64(r.Observed)),
			one("topicsync_unmanaged_topics", "Topics on the cluster the file does not list, internal and protected ones aside.", float64(r.Unmanaged)),
			perKind("topicsync_planned_actions", "Actions the plan held before executing: for a plan-only run, the drift from the desired state.",
				func(k plan.Kind) int { return r.Planned.Count(k) }),
		)
		warnings := family{name: "topicsync_warnings", help: "Warnings of the plan, by kind.", typ: "gauge"}
		for _, k := range []plan.WarningKind{plan.PartitionsAboveDesired, plan.ReplicationFactorDiffers, plan.SensitiveConfig, plan.PruneNotRequested, plan.UnreportedConfig} {
			warnings.samples = append(warnings.samples, sample{base + ",kind=" + label(string(k)), float64(r.Planned.WarningCount(k))})
		}
		fams = append(fams, warnings)
		// one series per topic and kind, even if a topic has several warnings of a kind (two sensitive keys): a
		// Pushgateway refuses a push with duplicate series
		topicWarn := family{name: "topicsync_topic_warning", help: "1 for each topic and kind of warning of the plan.", typ: "gauge"}
		seen := map[string]bool{}
		for _, w := range r.Planned.Warnings {
			key := w.Topic + "\x00" + string(w.Kind)
			if seen[key] {
				continue
			}
			seen[key] = true
			if len(topicWarn.samples) >= MaxTopicSeries {
				truncated++
				continue
			}
			topicWarn.samples = append(topicWarn.samples, sample{base + ",topic=" + label(w.Topic) + ",kind=" + label(string(w.Kind)), 1})
		}
		fams = append(fams, topicWarn)
	}
	// the drift: what is still needed after the run (the plan itself, for a plan-only run)
	drift := r.Pending
	if r.Mode != "apply" {
		drift = r.Planned
	}
	if drift != nil {
		fams = append(fams, perKind("topicsync_pending_actions", "Actions still needed after the run: non-zero means the cluster differs from the desired state.",
			func(k plan.Kind) int { return drift.Count(k) }))
		topicDrift := family{name: "topicsync_topic_drift", help: "1 for each topic and kind of action still needed after the run.", typ: "gauge"}
		seen := map[string]bool{}
		for _, a := range drift.Actions {
			key := a.Topic + "\x00" + string(a.Kind)
			if seen[key] {
				continue
			}
			seen[key] = true
			if len(topicDrift.samples) >= MaxTopicSeries {
				truncated++
				continue
			}
			topicDrift.samples = append(topicDrift.samples, sample{base + ",topic=" + label(a.Topic) + ",kind=" + label(string(a.Kind)), 1})
		}
		fams = append(fams, topicDrift)
	}
	fams = append(fams, one("topicsync_topic_series_truncated", "Per-topic series left out of this run's report (over MaxTopicSeries).", float64(truncated)))

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

func formatFloat(v float64) string {
	if v == float64(int64(v)) {
		return fmt.Sprintf("%d", int64(v))
	}
	return fmt.Sprintf("%g", v)
}

// label renders a label value in the Prometheus text format: only backslash, double quote and newline are escaped
// (Go's %q would also escape tabs and non-ASCII bytes as \t and \x.., which Prometheus parsers reject).
func label(s string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s) + `"`
}

// Push POSTs the metrics to url. Any 2xx status is success.
func Push(ctx context.Context, client *http.Client, rawURL, body string) error {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	// a Pushgateway's POST replaces only the metric names it receives, so a topic that stopped drifting would keep its
	// old series: PUT replaces the whole group
	method := http.MethodPost
	if strings.Contains(rawURL, "/metrics/job/") {
		method = http.MethodPut
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, bytes.NewBufferString(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain; version=0.0.4")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("pushing metrics to %s: %w", redact(rawURL), unwrapURLError(err))
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("pushing metrics to %s: %s: %s", redact(rawURL), resp.Status, strings.TrimSpace(string(msg)))
	}
	return nil
}

// redact hides a password in a URL's user info.
func redact(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "(an unparseable URL)"
	}
	return u.Redacted()
}

// unwrapURLError drops net/http's *url.Error wrapper, whose message repeats the URL with its password.
func unwrapURLError(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
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
	// readable by a textfile collector running as another user
	if err := os.Chmod(tmp.Name(), 0o644); err != nil { //nolint:gosec // metrics, not secrets
		_ = os.Remove(tmp.Name())
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	return nil
}
