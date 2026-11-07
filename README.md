# topicsync

topicsync keeps a Kafka cluster's topics as a desired-state file says: it creates missing topics, sets the config keys
the file names, raises partition counts, and reports what it will not change. It **never deletes a topic** unless two
separate opt-ins say so (and then only within caps), and it retires a single topic only through a guarded manual
command.

It replaces Kubernetes `Topic` resources (one object per topic, reconciled by an operator, whose deletion deletes the
topic's data) with one reviewed file per cluster and a short-lived Job. It works with Redpanda and Apache Kafka (both
tested in CI), over the Kafka protocol only.

## Usage

```
topicsync validate --spec topics.json                      # check a file; print its summary and hash
topicsync hash     --spec topics.json                      # print the hash of its resolved state (names the Job)
topicsync plan     --spec topics.json --brokers b:9092 [--detailed-exitcode] [--output json]
topicsync apply    --spec topics.json --brokers b:9092 [--prune] [--dry-run]
topicsync retire   --spec topics.json --brokers b:9092 --topic NAME [--confirm NAME] [--min-idle 1h]
topicsync version
```

Exit codes: `0` success, `1` failure (the cluster refused something, a run did not converge, a guard refused),
`2` usage or invalid file, `3` drift found (`plan --detailed-exitcode`).

Connection: `--brokers` (or `$TOPICSYNC_BROKERS`), `--tls`, `--tls-ca`, `--tls-server-name`, `--sasl-mechanism`
(PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) with `--sasl-user`/`$TOPICSYNC_SASL_USER` and `--sasl-password-file`/
`$TOPICSYNC_SASL_PASSWORD`. Run `topicsync <command> -h` for every flag.

## The desired-state file

```json
{
  "apiVersion": "topicsync.aperiodic.io/v1",
  "cluster": "redpanda-metrics-out",
  "defaults": {"partitions": 1, "replicationFactor": 3, "configs": {"cleanup.policy": "delete"}},
  "topics": [
    {"name": "ohlcv.v1.binance-futures.1m", "configs": {"retention.bytes": "40316198", "segment.bytes": "16777216"}},
    {"name": "raw.trades.binance-futures", "partitions": 6}
  ],
  "protected": ["^brrrrr\\.leases$"],
  "prune": {"enabled": false, "patterns": []}
}
```

- A topic's partitions, replication factor and configs fall back to `defaults`; its own config keys override the
  defaults' key by key.
- **Only the config keys a topic ends up with are managed.** Any other config on the cluster (set by hand, or a broker
  default) is never touched.
- Unknown or misspelt keys are refused, exactly spelt (Go's JSON decoder alone would accept `replicationfactor`).
  Every problem in a file is reported at once.
- `protected` patterns (matched as written: a pattern that matches more protects more), and every name starting with
  `_`, are never deleted.
- Internal topics (`__consumer_offsets`, `_schemas`, `_redpanda.*`: every name starting with `_`) cannot be listed: the
  file's defaults would otherwise apply to them (`cleanup.policy=delete` on a compacted `_schemas`).
- Duplicate keys are refused (a reviewer reads the first, JSON decoders keep the last), and so are empty or `null`
  config values (leave the key out to keep the broker's).
- The **hash** (`topicsync hash`) is a SHA-256 of the resolved state, not of the bytes: reordering or reformatting the
  file (or listing its patterns in another order) does not change it; any change to a topic, a default, the protected
  or prune rules does. Name the Kubernetes Job after it, so it runs once per change. Files written for v1 keep their
  hash in every later release (`spec/compat_test.go`).

## What `apply` does

1. Observes every topic, and the configs of the topics the file lists, straight from the brokers. It never reads a
   client-side metadata cache: a cached view was seen to list no topic at all after a refused request (see
   `TestObserveStaysTruthfulAfterABrokerRefusal`). Configs of topics the file does not list are not read, so one
   unrelated topic's error cannot fail a run. Transport failures are retried.
2. Plans:
   - a missing topic is **created**;
   - a named config key whose value differs is **set** (incremental AlterConfigs);
   - fewer partitions than desired are **raised**;
   - more partitions than desired, or another replication factor, are only **reported** (Kafka cannot shrink a topic;
     topicsync does not reassign replicas);
   - a sensitive config cannot be compared, so it is reported, not set;
   - a config the broker does not report for a topic (it ignores or does not know the key; Redpanda does not report
     `unclean.leader.election.enable`) is reported, not set, so that it is not planned again on every run;
   - values are compared as brokers normalize them: booleans in any case (`False` = `false`), list values in any order
     (`delete,compact` = `compact,delete`).
3. Executes:
   - **Creation is paced:** in batches (`--batch-size`, default 20), each batch waiting until its partitions (and, for a
     partition increase, the new ones) have leaders (`--leader-timeout`), then `--batch-pause`. A broker with a
     readiness gate on leaderless partitions would otherwise flap its readiness under hundreds of creates.
   - **Retries:** errors the broker marks retriable (and transport failures) are retried with exponential backoff
     (`--retries`).
   - **Partial failures:** one topic's failure (an invalid config, a policy) does not stop the others; the run fails at
     the end, listing every failure.
   - A topic created by someone else in between counts as done, and is then verified. A partition increase whose answer
     was lost and is retried (brokers refuse the now-equal count) counts as done.
4. **Verifies:** it observes again and plans again, up to 5 times 5 s apart (a broker that has not caught up with an
   alter reports the old value for a moment). The run succeeds only if nothing is left to do (`not converged`
   otherwise), so a cluster that silently ignores a config cannot pass.

SIGTERM ends a run promptly. Every Kafka request is atomic per topic, so a run cut short leaves nothing the next run
cannot finish (`TestSIGTERMStopsARunAndTheNextOneConverges`).

## Deleting topics

**By default topicsync deletes nothing.** A topic removed from the file stays on the cluster, data and all: it is
reported as unmanaged (`topicsync_unmanaged_topics`), never deleted. Deleting needs one of the two paths below, each
an explicit decision.

**Prune** (off unless both the file and the command turn it on) deletes topics the file does not list, and only when
all of these hold:

- the file says `"prune": {"enabled": true, "patterns": [...]}` (a file without a `prune` section, or with
  `"enabled": false`, never prunes); each pattern is anchored (`^...$`) and matched against whole names only, so a
  top-level `|` cannot widen it;
- the command says `--prune`;
- the topic matches a prune pattern, matches no protected pattern, and is not internal;
- the run deletes no more than `--prune-max-deletes` (50) topics and no more than `--prune-max-fraction` (25%) of the
  topics matching a pattern. Over either cap the whole run is refused. Caps catch a truncated or wrong file.

`--prune` with a file that does not enable it is an error, not a silent no-op. Use prune where losing a topic is
cheap (a staging cluster that tracks branches); leave it off in production.

**Retire** deletes one topic on purpose, the production way. It refuses unless:

- the topic is no longer in the file;
- it is not protected;
- no consumer group has a member on it;
- every group that committed on it has read it to the end (lag 0);
- its newest record is older than `--min-idle` (1 h), read from every partition (a partition it cannot read to its
  end, or offsets returned with an error, refuse: never "empty");
- its end offsets do not move for `--write-check` (30 s): record timestamps are the producer's to set (event time, a
  backfill), so a topic stamped in the past can still be written.

Consumer groups of both protocols are seen (classic, and Kafka 4's KIP-848). A reader outside any consumer group
(manual assignment, offsets kept elsewhere) is not: only the write check covers its producer side. Without
`--confirm <exact name>` it only runs the checks.

## Metrics

A run is short-lived, so it pushes its metrics once, in the Prometheus text format, to `--metrics-push-url` (or
`$TOPICSYNC_METRICS_PUSH_URL`): VictoriaMetrics' `/api/v1/import/prometheus` (POST), or a Pushgateway group
`/metrics/job/<job>/...` (PUT, which replaces the whole group, so a topic that stopped drifting loses its series). It
can also write them to `--metrics-file` (a textfile collector; mode 0644). A push failure is logged, its URL's password
redacted, and never changes the run's result.

Every series carries `kafka_cluster` (the file's `cluster`, or `--cluster-label`, so that a run whose file cannot be
read still reports under its cluster) and `mode` (`plan`, `apply`, `dry-run`, `retire`).

**What a run did not measure is left out, not reported as zero:** a run that failed before planning has no planned,
pending or warning counts, no observed or unmanaged topics and no drift series; one whose verification failed has no
pending counts. Gate drift alerts on `topicsync_last_run_success`.

| Metric | Meaning |
|---|---|
| `topicsync_last_run_success` | 1 if the run succeeded (apply: converged) |
| `topicsync_last_run_timestamp_seconds` | when it started |
| `topicsync_last_run_duration_seconds` | how long it took |
| `topicsync_last_run_failed_stage{stage}` | 1 for the stage it failed at: `spec`, `config` (e.g. an unreadable password file), `connect`, `observe`, `plan`, `execute`, `verify`, `retire` |
| `topicsync_stage_duration_seconds{stage}` | `observe`, `execute`, `verify`, `leader_wait` |
| `topicsync_planned_actions{kind}` | the plan before executing; for `plan` runs, the **drift** |
| `topicsync_executed_actions{kind}` | what the run did |
| `topicsync_pending_actions{kind}` | what is still needed after the run (non-zero: the cluster differs) |
| `topicsync_failed_actions` | actions that failed |
| `topicsync_warnings{kind}` | `partitions_above_desired`, `replication_factor_differs`, `sensitive_config`, `prune_not_requested`, `unreported_config` |
| `topicsync_topic_drift{topic,kind}` | one series per topic still drifting (only drifting topics) |
| `topicsync_topic_warning{topic,kind}` | one series per topic with a warning |
| `topicsync_topic_series_truncated` | per-topic series left out above 200 per run (a wrong file cannot flood the store) |
| `topicsync_desired_topics`, `topicsync_observed_topics` | counts |
| `topicsync_unmanaged_topics` | topics on the cluster the file does not list (internal and protected aside) |
| `topicsync_prune_refused` | 1 if the prune caps refused the run |
| `topicsync_retries` | retries of retriable errors |
| `topicsync_spec_info{spec_hash}` | which version of the file the run read |
| `topicsync_build_info{version}` | which topicsync ran |

Suggested alerts (live-ingestion's are in its `vmalert` rules): a failed run; no successful run for twice the schedule;
drift that persists across runs; a refused prune; unmanaged topics appearing; a slow or retry-heavy run.

## Running it in Kubernetes

Per cluster:

- **A ConfigMap** with the file.
- **A Job** named `topicsync-<cluster>-<first 10 hex of the hash>`, running `apply`, with `backoffLimit: 0`.
  - Flux (kstatus) treats a Complete Job as Ready and a Failed one as failed, so a layer that `dependsOn` the topic
    layer with `wait: true` starts writers only after their topics exist.
  - A new file is a new Job, and the layer prunes the old one. Only a layer that holds **no** Topic resources may
    prune: a pruned Topic resource makes the Redpanda operator delete its topic. Never enable Flux pruning on a layer
    that still applies Topic resources (live-ingestion keeps its production Topic layers `prune: false` for good).
- **A CronJob** running `plan` (drift report; alert on its metrics) or `apply` (drift correction) on a schedule. Leave
  `--detailed-exitcode` out of CronJobs: its exit 3 on drift fails the Job, and retries it.

The image is `ghcr.io/aperiodic-io/topicsync:<commit>` (also `:main`, and `:<tag>` for releases). It's distroless and
non-root, and works with a read-only root filesystem; see `test/e2e/manifests/job.yaml`.

## Migrating from the Redpanda operator's Topic resources

`scripts/migrate-from-redpanda-operator.sh` hands a namespace's topics over without deleting one. Deleting a Topic
resource makes the operator delete its topic, and the operator puts the finalizer back on every reconcile, so the
script removes the resources only while nothing can act on them:

1. It checks every argument before changing anything (`--timeout` is whole seconds).
2. It refuses unless every Topic resource's topic is listed in the topicsync file (`--spec`), so none becomes
   unmanaged; and unless the Flux Kustomization that applies the Topics and the operator's Flux HelmRelease are
   suspended or absent, so neither re-creates them. A lookup error is never read as "absent".
3. It snapshots the Topic resources and the operator's replica count into a new, empty `--snapshot-dir`.
4. It scales the operator to 0 and waits until it has 0 replicas and no pod.
5. One Topic at a time: it checks the operator is still stopped, clears the finalizer, checks it is gone, deletes.
6. It checks no Topic resource is left, and scales the operator back.

Any error stops it at once with the operator left stopped and the state and the recovery command printed; a rerun
with the operator at 0 needs `--operator-replicas N`. `scripts/migrate-from-redpanda-operator_test.sh` drives every
failure path against a fake kubectl (an apiserver error at each check, the operator coming back mid-way, a failed
patch, a rerun). Before it, run `topicsync plan --detailed-exitcode` until it exits 0. `test/e2e/kind.sh` rehearses the
happy path against the real operator, with the negative control: deleting a Topic resource with the operator running
does delete the topic. It does not exercise Flux or the failure paths, which the script's own test covers.

## Tests

| Suite | Command | What |
|---|---|---|
| unit | `go test -race ./...` | spec, planner (property tests checked against independent oracles, prune patterns and caps included), executor, retire guards, metrics, CLI on a fake cluster; the Kafka adapter against franz-go's in-process broker (kfake), with injected per-partition errors |
| scripts | `scripts/migrate-from-redpanda-operator_test.sh` | the migration script against a fake kubectl, every failure path |
| fuzz | `go test -fuzz FuzzParse ./spec` | the parser never panics; an accepted file's resolved form hashes the same |
| integration | `go test -tags integration ./test/integration` | the Kafka adapter and full runs against a real broker: configs and sources, only named keys, create races, partitions, groups and lag, newest record, paced bulk creation, concurrent runs, broker refusals, 3-broker RF 3 with a stalled broker, a Redpanda upgrade |
| acceptance | `go test -tags acceptance ./test/acceptance` | the built binary as a Job runs it: idempotence, drift, adoption of topics made by another tool, invalid files, unreachable brokers, prune and its caps, retire, SIGTERM, metrics, upgrade and rollback against the previous build, the image |
| end-to-end | `test/e2e/kind.sh` | kind + Redpanda + the Redpanda operator: the migration, Jobs (adopt, change, plan, invalid file) |

Integration and acceptance tests start brokers in Docker. `TOPICSYNC_IT_FLAVOR` (`redpanda` | `kafka`) and
`TOPICSYNC_IT_IMAGE` select the broker; CI runs Redpanda 26.1 and 26.2 and Apache Kafka 3.9 and 4.1. Each step of a
release is gated on all of them.
