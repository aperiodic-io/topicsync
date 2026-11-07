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
- `protected` patterns, and every name starting with `_` (`__consumer_offsets`, `_schemas`, `_redpanda.*`), are never
  deleted.
- The **hash** (`topicsync hash`) is a SHA-256 of the resolved state, not of the bytes: reordering or reformatting the
  file does not change it; any change to a topic, a default, the protected or prune rules does. Name the Kubernetes Job
  after it, so it runs once per change.

## What `apply` does

1. Observes every topic and every config, straight from the brokers. It never reads a client-side metadata cache: a
   cached view was seen to list no topic at all after a refused request (see `TestObserveStaysTruthfulAfterABrokerRefusal`).
2. Plans:
   - a missing topic is **created**;
   - a named config key whose value differs is **set** (incremental AlterConfigs);
   - fewer partitions than desired are **raised**;
   - more partitions than desired, or another replication factor, are only **reported** (Kafka cannot shrink a topic;
     topicsync does not reassign replicas);
   - a sensitive config cannot be compared, so it is reported, not set.
3. Executes:
   - **Creation is paced:** in batches (`--batch-size`, default 20), each batch waiting until its partitions have
     leaders (`--leader-timeout`), then `--batch-pause`. A broker with a readiness gate on leaderless partitions would
     otherwise flap its readiness under hundreds of creates.
   - **Retries:** errors the broker marks retriable (and transport failures) are retried with exponential backoff
     (`--retries`).
   - **Partial failures:** one topic's failure (an invalid config, a policy) does not stop the others; the run fails at
     the end, listing every failure.
   - A topic created by someone else in between counts as done, and is then verified.
4. **Verifies:** it observes again and plans again. The run succeeds only if nothing is left to do (`not converged`
   otherwise), so a cluster that silently ignores a config cannot pass.

SIGTERM ends a run promptly. Every Kafka request is atomic per topic, so a run cut short leaves nothing the next run
cannot finish (`TestSIGTERMStopsARunAndTheNextOneConverges`).

## Deleting topics

**Prune** deletes topics the file does not list, and only when all of these hold:

- the file says `"prune": {"enabled": true, "patterns": [...]}`, with each pattern anchored (`^...$`);
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
- its newest record is older than `--min-idle` (1 h).

Without `--confirm <exact name>` it only runs the checks.

## Metrics

A run is short-lived, so it pushes its metrics once, in the Prometheus text format, to `--metrics-push-url` (or
`$TOPICSYNC_METRICS_PUSH_URL`): VictoriaMetrics' `/api/v1/import/prometheus` or a Pushgateway's `/metrics/job/<job>`.
It can also write them to `--metrics-file` (textfile collector). A push failure is logged and never changes the run's
result.

Every series carries `kafka_cluster` (the file's `cluster`, or `--cluster-label`, so that a run whose file cannot be read still reports
under its cluster) and `mode` (`plan`, `apply`, `retire`).

| Metric | Meaning |
|---|---|
| `topicsync_last_run_success` | 1 if the run succeeded (apply: converged) |
| `topicsync_last_run_timestamp_seconds` | when it started |
| `topicsync_last_run_duration_seconds` | how long it took |
| `topicsync_last_run_failed_stage{stage}` | 1 for the stage it failed at: `spec`, `connect`, `observe`, `plan`, `execute`, `verify`, `retire` |
| `topicsync_stage_duration_seconds{stage}` | `observe`, `execute`, `verify`, `leader_wait` |
| `topicsync_planned_actions{kind}` | the plan before executing; for `plan` runs, the **drift** |
| `topicsync_executed_actions{kind}` | what the run did |
| `topicsync_pending_actions{kind}` | what is still needed after the run (non-zero: the cluster differs) |
| `topicsync_failed_actions` | actions that failed |
| `topicsync_warnings{kind}` | `partitions_above_desired`, `replication_factor_differs`, `sensitive_config`, `prune_not_requested` |
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
  - A new file is a new Job, and the layer prunes the old one (it holds no topics, so it may prune).
- **A CronJob** running `plan --detailed-exitcode` (drift report) or `apply` (drift correction) on a schedule, pushing
  metrics.

The image is `ghcr.io/aperiodic-io/topicsync:<commit>` (also `:main`, and `:<tag>` for releases). It's distroless and
non-root, and works with a read-only root filesystem; see `test/e2e/manifests/job.yaml`.

## Migrating from the Redpanda operator's Topic resources

`scripts/migrate-from-redpanda-operator.sh` hands a namespace's topics over without deleting one:

1. It refuses unless the Flux Kustomization that applies the Topics is suspended (or gone).
2. It snapshots the Topic resources.
3. It scales the operator to 0 and waits until no operator pod is left.
4. It clears each Topic's finalizer, then deletes the resource.
5. It checks none is left, and scales the operator back.

Before it, run topicsync's `plan --detailed-exitcode` until it exits 0 against the cluster: the file then says exactly
what the resources made. `test/e2e/kind.sh` rehearses all of it against the real operator. It includes the negative
control: deleting a Topic resource with the operator running does delete the topic.

## Tests

| Suite | Command | What |
|---|---|---|
| unit | `go test -race ./...` | spec, planner (with property tests checked against an independent oracle), executor, retire guards, metrics, CLI; fake cluster |
| fuzz | `go test -fuzz FuzzParse ./spec` | the parser never panics; an accepted file's resolved form hashes the same |
| integration | `go test -tags integration ./test/integration` | the Kafka adapter and full runs against a real broker: configs and sources, only named keys, create races, partitions, groups and lag, newest record, paced bulk creation, concurrent runs, broker refusals, 3-broker RF 3 with a stalled broker, a Redpanda upgrade |
| acceptance | `go test -tags acceptance ./test/acceptance` | the built binary as a Job runs it: idempotence, drift, adoption of topics made by another tool, invalid files, unreachable brokers, prune and its caps, retire, SIGTERM, metrics, upgrade and rollback against the previous build, the image |
| end-to-end | `test/e2e/kind.sh` | kind + Redpanda + the Redpanda operator: the migration, Jobs (adopt, change, plan, invalid file) |

Integration and acceptance tests start brokers in Docker. `TOPICSYNC_IT_FLAVOR` (`redpanda` | `kafka`) and
`TOPICSYNC_IT_IMAGE` select the broker; CI runs Redpanda 26.1 and 26.2 and Apache Kafka 3.9 and 4.1. Each step of a
release is gated on all of them.
