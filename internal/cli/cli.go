// Package cli is topicsync's command line. Run takes its dependencies as arguments so that tests can drive every
// command against a fake cluster.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/aperiodic-io/topicsync/internal/apply"
	"github.com/aperiodic-io/topicsync/internal/cluster"
	"github.com/aperiodic-io/topicsync/internal/kafka"
	"github.com/aperiodic-io/topicsync/internal/metrics"
	"github.com/aperiodic-io/topicsync/internal/plan"
	"github.com/aperiodic-io/topicsync/internal/retire"
	"github.com/aperiodic-io/topicsync/spec"
)

// Exit codes.
const (
	ExitOK      = 0
	ExitFailure = 1
	ExitUsage   = 2
	// ExitDrift is plan's exit code with --detailed-exitcode when the cluster differs from the desired state.
	ExitDrift = 3
)

// Version is set at build time (-ldflags "-X github.com/aperiodic-io/topicsync/internal/cli.Version=...").
var Version = "dev"

// Env is how the CLI reaches the outside world.
type Env struct {
	Stdout, Stderr io.Writer
	Getenv         func(string) string
	// NewCluster connects to a cluster; tests return a fake.
	NewCluster func(kafka.Config) (cluster.Cluster, error)
	// Now is the current time.
	Now func() time.Time
	// Sleep is apply's wait; tests make it instant.
	Sleep func(ctx context.Context, d time.Duration) error
}

// DefaultEnv is the real world.
func DefaultEnv() Env {
	return Env{
		Stdout: os.Stdout, Stderr: os.Stderr, Getenv: os.Getenv,
		NewCluster: func(c kafka.Config) (cluster.Cluster, error) { return kafka.New(c) },
		Now:        time.Now,
	}
}

const usage = `topicsync keeps a Kafka cluster's topics as a desired-state file says.

Usage:
  topicsync validate --spec FILE              check a file, print its summary and hash
  topicsync hash --spec FILE                  print the hash of a file's resolved state
  topicsync plan --spec FILE --brokers HOSTS  show what apply would change
  topicsync apply --spec FILE --brokers HOSTS create, alter and repartition topics (and prune, with --prune)
  topicsync retire --spec FILE --brokers HOSTS --topic NAME [--confirm NAME]
                                              delete one topic that is no longer listed, after checking nothing uses it
  topicsync version

Run "topicsync COMMAND -h" for a command's flags. Exit codes: 0 success, 1 failure, 2 usage or invalid file,
3 drift found (plan --detailed-exitcode).
`

// Run runs one command and returns its exit code.
func Run(ctx context.Context, args []string, env Env) int {
	if env.Now == nil {
		env.Now = time.Now
	}
	if env.Getenv == nil {
		env.Getenv = func(string) string { return "" }
	}
	if len(args) == 0 {
		fmt.Fprint(env.Stderr, usage)
		return ExitUsage
	}
	switch args[0] {
	case "version", "--version", "-v":
		fmt.Fprintf(env.Stdout, "topicsync %s\n", Version)
		return ExitOK
	case "help", "-h", "--help":
		fmt.Fprint(env.Stdout, usage)
		return ExitOK
	case "validate", "hash":
		return runOffline(args[0], args[1:], env)
	case "plan", "apply", "retire":
		return runOnline(ctx, args[0], args[1:], env)
	default:
		fmt.Fprintf(env.Stderr, "unknown command %q\n\n%s", args[0], usage)
		return ExitUsage
	}
}

func runOffline(cmd string, args []string, env Env) int {
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(env.Stderr)
	specPath := fs.String("spec", "", "the desired-state file (required)")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	if *specPath == "" || fs.NArg() > 0 {
		fmt.Fprintf(env.Stderr, "%s: --spec FILE is required, and nothing else\n", cmd)
		return ExitUsage
	}
	d, err := spec.Load(*specPath)
	if err != nil {
		fmt.Fprintln(env.Stderr, err)
		return ExitUsage
	}
	if cmd == "hash" {
		fmt.Fprintln(env.Stdout, d.Hash)
		return ExitOK
	}
	fmt.Fprintf(env.Stdout, "valid: cluster %s, %d topics, prune %s, hash %s\n", d.Cluster, len(d.Topics), onOff(d.Prune.Enabled), d.Hash)
	return ExitOK
}

func onOff(b bool) string {
	if b {
		return "enabled"
	}
	return "disabled"
}

type common struct {
	spec, brokers, output, logFormat, logLevel string
	timeout, requestTimeout                    time.Duration
	tls, tlsInsecure                           bool
	tlsCA, tlsServerName                       string
	saslMechanism, saslUser, saslPasswordFile  string
	metricsPushURL, metricsFile, clusterLabel  string
}

func (c *common) register(fs *flag.FlagSet, env Env) {
	fs.StringVar(&c.spec, "spec", "", "the desired-state file (required)")
	fs.StringVar(&c.brokers, "brokers", env.Getenv("TOPICSYNC_BROKERS"), "comma-separated seed brokers, host:port (or $TOPICSYNC_BROKERS)")
	fs.StringVar(&c.output, "output", "text", "plan output: text or json")
	fs.StringVar(&c.logFormat, "log-format", "text", "log format: text or json")
	fs.StringVar(&c.logLevel, "log-level", "info", "log level: debug, info, warn, error")
	fs.DurationVar(&c.timeout, "timeout", 15*time.Minute, "the whole run's deadline")
	fs.DurationVar(&c.requestTimeout, "request-timeout", 30*time.Second, "each Kafka request's deadline")
	fs.BoolVar(&c.tls, "tls", false, "connect with TLS")
	fs.StringVar(&c.tlsCA, "tls-ca", "", "PEM file of the CAs to trust (default: the system's)")
	fs.StringVar(&c.tlsServerName, "tls-server-name", "", "the server name to verify (default: the broker's host)")
	fs.BoolVar(&c.tlsInsecure, "tls-insecure-skip-verify", false, "do not verify the brokers' certificates")
	fs.StringVar(&c.saslMechanism, "sasl-mechanism", "", "PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512 (default: none)")
	fs.StringVar(&c.saslUser, "sasl-user", env.Getenv("TOPICSYNC_SASL_USER"), "SASL user (or $TOPICSYNC_SASL_USER)")
	fs.StringVar(&c.saslPasswordFile, "sasl-password-file", "", "file holding the SASL password (or $TOPICSYNC_SASL_PASSWORD)")
	fs.StringVar(&c.metricsPushURL, "metrics-push-url", env.Getenv("TOPICSYNC_METRICS_PUSH_URL"),
		"POST the run's metrics in the Prometheus text format here (VictoriaMetrics /api/v1/import/prometheus, a Pushgateway)")
	fs.StringVar(&c.metricsFile, "metrics-file", "", "write the run's metrics to this file (textfile collector)")
	fs.StringVar(&c.clusterLabel, "cluster-label", "", "the metrics' cluster label (default: the file's cluster); set it so that a run whose file cannot be read still reports under its cluster")
}

func (c *common) logger(w io.Writer) (*slog.Logger, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(c.logLevel)); err != nil {
		return nil, fmt.Errorf("--log-level %q: %w", c.logLevel, err)
	}
	opts := &slog.HandlerOptions{Level: level}
	switch c.logFormat {
	case "text":
		return slog.New(slog.NewTextHandler(w, opts)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(w, opts)), nil
	}
	return nil, fmt.Errorf("--log-format %q: text or json", c.logFormat)
}

func (c *common) kafkaConfig(env Env) (kafka.Config, error) {
	var brokers []string
	for _, b := range strings.Split(c.brokers, ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	if len(brokers) == 0 {
		return kafka.Config{}, errors.New("--brokers (or $TOPICSYNC_BROKERS) is required")
	}
	cfg := kafka.Config{
		Brokers: brokers, RequestTimeout: c.requestTimeout, TLS: c.tls || c.tlsCA != "" || c.tlsInsecure,
		TLSCAFile: c.tlsCA, TLSServerName: c.tlsServerName, TLSInsecureSkipVerify: c.tlsInsecure,
		SASLMechanism: strings.ToUpper(c.saslMechanism), SASLUser: c.saslUser,
	}
	if cfg.SASLMechanism == "" && (c.saslUser != "" || c.saslPasswordFile != "") {
		return kafka.Config{}, errors.New("a SASL user or password without --sasl-mechanism: say which mechanism, or the connection would be unauthenticated")
	}
	if cfg.SASLMechanism != "" {
		pw := env.Getenv("TOPICSYNC_SASL_PASSWORD")
		if c.saslPasswordFile != "" {
			b, err := os.ReadFile(c.saslPasswordFile)
			if err != nil {
				return kafka.Config{}, &configError{fmt.Errorf("reading --sasl-password-file: %w", err)}
			}
			pw = strings.TrimRight(string(b), "\r\n")
		}
		if cfg.SASLUser == "" || pw == "" {
			return kafka.Config{}, errors.New("--sasl-mechanism needs a user and a password")
		}
		cfg.SASLPassword = pw
	}
	return cfg, nil
}

// configError is a failure of the environment (a Secret not mounted), not of the command line: exit 1, reported.
type configError struct{ error }

func (e *configError) Unwrap() error { return e.error }

func runOnline(ctx context.Context, cmd string, args []string, env Env) int {
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(env.Stderr)
	var c common
	c.register(fs, env)
	po := plan.DefaultOptions()
	eo := apply.DefaultOptions()
	var (
		prune, allowEmpty, dryRun, detailed bool
		topic, confirm                      string
		minIdle, writeCheck                 time.Duration
	)
	if cmd == "plan" || cmd == "apply" {
		fs.BoolVar(&prune, "prune", false, "delete topics the file no longer lists (default false: they are never deleted). Needs the file's "+
			"prune.enabled too, and deletes only names matching its prune patterns, within the caps below")
		fs.IntVar(&po.PruneMaxDeletes, "prune-max-deletes", po.PruneMaxDeletes, "refuse a run that would prune more topics than this")
		fs.Float64Var(&po.PruneMaxFraction, "prune-max-fraction", po.PruneMaxFraction,
			"refuse a run that would prune more than this share of the topics matching a prune pattern")
		fs.BoolVar(&allowEmpty, "allow-empty", false, "accept a file that lists no topic")
	}
	if cmd == "plan" {
		fs.BoolVar(&detailed, "detailed-exitcode", false, "exit 3 if the cluster differs from the desired state")
	}
	if cmd == "apply" {
		fs.BoolVar(&dryRun, "dry-run", false, "plan only, as the plan command")
		fs.IntVar(&eo.BatchSize, "batch-size", eo.BatchSize, "topics created (or repartitioned) before waiting for their partition leaders")
		fs.DurationVar(&eo.BatchPause, "batch-pause", eo.BatchPause, "pause between batches")
		fs.DurationVar(&eo.LeaderTimeout, "leader-timeout", eo.LeaderTimeout, "how long a batch may wait for its partition leaders")
		fs.IntVar(&eo.Retries, "retries", eo.Retries, "retries of a retriable error")
	}
	if cmd == "retire" {
		fs.StringVar(&topic, "topic", "", "the topic to retire (required)")
		fs.StringVar(&confirm, "confirm", "", "repeat the topic's name to delete it; without it, only the checks run")
		fs.DurationVar(&minIdle, "min-idle", time.Hour, "how old the topic's newest record must be")
		fs.DurationVar(&writeCheck, "write-check", 30*time.Second, "how long the topic's end offsets must stay still (record timestamps alone can lie)")
	}
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(env.Stderr, "%s: unexpected arguments %v\n", cmd, fs.Args())
		return ExitUsage
	}
	if c.spec == "" {
		fmt.Fprintf(env.Stderr, "%s: --spec FILE is required\n", cmd)
		return ExitUsage
	}
	if cmd == "retire" && topic == "" {
		fmt.Fprintln(env.Stderr, "retire: --topic NAME is required")
		return ExitUsage
	}
	if c.output != "text" && c.output != "json" {
		fmt.Fprintf(env.Stderr, "--output %q: text or json\n", c.output)
		return ExitUsage
	}
	if po.PruneMaxDeletes < 0 || po.PruneMaxFraction <= 0 || po.PruneMaxFraction > 1 {
		fmt.Fprintln(env.Stderr, "--prune-max-deletes must be >= 0 and --prune-max-fraction in (0, 1]")
		return ExitUsage
	}
	log, err := c.logger(env.Stderr)
	if err != nil {
		fmt.Fprintln(env.Stderr, err)
		return ExitUsage
	}
	start := env.Now()
	mode := cmd
	if dryRun {
		mode = "dry-run" // not an apply: a dry run must not satisfy "no successful apply" alerts
	}
	run := metrics.Run{Cluster: c.clusterLabel, Mode: mode, Version: Version, Start: start}
	report := func(code int) int {
		run.Duration = env.Now().Sub(start)
		c.reportMetrics(ctx, run, log)
		return code
	}
	kc, err := c.kafkaConfig(env)
	var ce *configError
	switch {
	case errors.As(err, &ce):
		log.Error("configuration", "error", err)
		run.FailedStage = "config"
		return report(ExitFailure)
	case err != nil:
		fmt.Fprintln(env.Stderr, err)
		return ExitUsage
	}
	if kc.SASLMechanism == "PLAIN" && !kc.TLS {
		log.Warn("SASL PLAIN without TLS sends the password in clear text")
	}
	d, err := spec.Load(c.spec)
	if err != nil {
		fmt.Fprintln(env.Stderr, err)
		run.FailedStage = "spec"
		return report(ExitUsage)
	}
	if run.Cluster == "" {
		run.Cluster = d.Cluster
	}
	run.Desired, run.SpecHash = len(d.Topics), d.Hash
	po.Prune, po.AllowEmpty = prune, allowEmpty
	eo.Logger = log
	if env.Sleep != nil {
		eo.Sleep = env.Sleep
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	cl, err := env.NewCluster(kc)
	if err != nil {
		log.Error("connecting", "error", err)
		run.FailedStage = "connect"
		return report(ExitFailure)
	}
	defer cl.Close()
	log = log.With("cluster", d.Cluster)

	var code int
	switch cmd {
	case "plan", "apply":
		code = c.planOrApply(ctx, cmd, cl, d, po, eo, dryRun, detailed, log, env, &run)
	case "retire":
		code = c.retire(ctx, cl, d, topic, confirm, minIdle, writeCheck, log, env, &run)
	}
	return report(code)
}

func (c *common) planOrApply(ctx context.Context, cmd string, cl cluster.Cluster, d *spec.Desired, po plan.Options, eo apply.Options,
	dryRun, detailed bool, log *slog.Logger, env Env, run *metrics.Run) int {
	planOnly := cmd == "plan" || dryRun
	out, err := apply.Run(ctx, cl, d, po, eo, planOnly)
	run.Retries, run.LeaderWait = out.Stats.Retries, out.Stats.LeaderWait
	run.ObserveDuration, run.ExecuteDuration, run.VerifyDuration = out.ObserveDuration, out.ExecuteDuration, out.VerifyDuration
	run.FailedStage = string(out.Stage)
	var pl *plan.PruneLimitError
	run.PruneRefused = errors.As(err, &pl)
	if out.Plan != nil {
		run.Planned, run.Observed, run.Unmanaged = out.Plan, out.Plan.Observed, out.Plan.Unmanaged
		run.Executed, run.Failed, run.Pending = out.Result.Executed, len(out.Result.Failures), out.Final
		c.printPlan(env.Stdout, out.Plan)
	}
	switch {
	case err == nil && planOnly:
		run.Success = true
		log.Info("planned", "create", out.Plan.Count(plan.Create), "alter", out.Plan.Count(plan.AlterConfigs),
			"repartition", out.Plan.Count(plan.AddPartitions), "delete", out.Plan.Count(plan.Delete), "warnings", len(out.Plan.Warnings), "spec", d.Hash)
		if detailed && !out.Plan.Empty() {
			return ExitDrift
		}
		return ExitOK
	case err == nil:
		run.Success = true
		log.Info("applied: the cluster matches the desired state", "created", out.Result.Executed[plan.Create],
			"altered", out.Result.Executed[plan.AlterConfigs], "repartitioned", out.Result.Executed[plan.AddPartitions],
			"deleted", out.Result.Executed[plan.Delete], "alreadyDone", out.Result.AlreadyDone, "spec", d.Hash)
		return ExitOK
	default:
		log.Error(cmd+" failed", "error", err)
		return ExitFailure
	}
}

func (c *common) printPlan(w io.Writer, p *plan.Plan) {
	if c.output == "json" {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(p)
		return
	}
	fmt.Fprint(w, p.Text())
}

func (c *common) retire(ctx context.Context, cl cluster.Cluster, d *spec.Desired, topic, confirm string, minIdle, writeCheck time.Duration,
	log *slog.Logger, env Env, run *metrics.Run) int {
	rep, err := retire.Retire(ctx, cl, d, topic, retire.Options{Confirm: confirm, MinIdle: minIdle, WriteCheck: writeCheck, Now: env.Now, Sleep: env.Sleep})
	for _, g := range rep.Groups {
		fmt.Fprintf(env.Stdout, "group %s: state %s, %d active member(s), lag %d, committed %v\n", g.Group, g.State, g.ActiveMembers, g.Lag, g.Committed)
	}
	if rep.Empty {
		fmt.Fprintf(env.Stdout, "%s holds no record\n", topic)
	} else if !rep.Newest.IsZero() {
		fmt.Fprintf(env.Stdout, "%s: newest record %s (%s ago)\n", topic, rep.Newest.UTC().Format(time.RFC3339), env.Now().Sub(rep.Newest).Round(time.Second))
	}
	switch {
	case err != nil:
		log.Error("retire refused or failed", "topic", topic, "error", err)
		run.FailedStage = "retire"
		return ExitFailure
	case rep.DryRun:
		run.Success = true
		fmt.Fprintf(env.Stdout, "every check passed: run again with --confirm %s to delete %s and its data\n", topic, topic)
		return ExitOK
	default:
		run.Success = true
		run.Executed = map[plan.Kind]int{plan.Delete: 1}
		log.Warn("retired: topic and data deleted", "topic", topic)
		fmt.Fprintf(env.Stdout, "deleted %s\n", topic)
		return ExitOK
	}
}

func (c *common) reportMetrics(ctx context.Context, run metrics.Run, log *slog.Logger) {
	if c.metricsPushURL == "" && c.metricsFile == "" {
		return
	}
	body := run.Text()
	if c.metricsFile != "" {
		if err := metrics.WriteFile(c.metricsFile, body); err != nil {
			log.Warn("writing metrics", "file", c.metricsFile, "error", err)
		}
	}
	if c.metricsPushURL != "" {
		// the run's own deadline may be spent: give the push a few seconds of its own
		pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if err := metrics.Push(pctx, nil, c.metricsPushURL, body); err != nil {
			log.Warn("pushing metrics (the run's result stands)", "error", err)
		}
	}
}
