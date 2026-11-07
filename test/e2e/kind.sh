#!/usr/bin/env bash
# End-to-end test in a kind cluster: topicsync as a Kubernetes Job, and the migration from the Redpanda operator's
# Topic resources to topicsync without losing a topic or a record.
#
#   1. Redpanda (one broker) and the Redpanda operator; four Topic resources made into topics by the operator, with data.
#   2. Negative control: deleting a Topic resource with the operator running deletes the Kafka topic. This is the hazard
#      the migration avoids; if it did not happen, the rest of the test would prove nothing.
#   3. The migration script: the three remaining topics survive, their high watermarks unchanged, also after the operator
#      is back for a minute.
#   4. A topicsync Job over the same topics: completes, finds nothing to change.
#   5. A Job with a changed file: creates and alters; checked with rpk.
#   6. A Job with an invalid file: fails (so the Flux layer waiting on it fails).
#
# Needs docker, kind, kubectl, helm, jq. KEEP_CLUSTER=1 keeps the cluster.
set -euo pipefail
cd "$(dirname "$0")"
CLUSTER=${CLUSTER:-topicsync-e2e}
IMAGE=${TOPICSYNC_IMAGE:-topicsync:e2e}
REDPANDA_IMAGE=${REDPANDA_IMAGE:-docker.redpanda.com/redpandadata/redpanda:v26.1.6}
OPERATOR_VERSION=${OPERATOR_VERSION:-26.1.10}
KCTX="kind-$CLUSTER"
k() { kubectl --context "$KCTX" "$@"; }
rpk() { k -n redpanda exec redpanda-0 -- rpk "$@"; }
log() { echo "[$(date -u +%T)] $*"; }
fail() { echo "FAIL: $*" >&2; dump; exit 1; }
dump() {
  k -n redpanda get pods,jobs,topics.cluster.redpanda.com 2>/dev/null || true
  for j in $(k -n redpanda get jobs -o name 2>/dev/null); do echo "--- $j"; k -n redpanda logs "$j" --tail 40 2>/dev/null || true; done
}
created=0
cleanup() { [[ "${KEEP_CLUSTER:-0}" == 1 || $created == 0 ]] || kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true; }
trap cleanup EXIT

hw() { # topic -> sum of its partitions' high watermarks (the last column of rpk's table)
  rpk topic describe "$1" -p | awk 'NR>1 {s+=$NF} END {print s+0}'
}
has_topic() { # an rpk error fails the test: it must never read as "the topic is gone"
  local list
  list=$(rpk topic list) || fail "rpk topic list failed"
  awk 'NR>1 {print $1}' <<<"$list" | grep -qx "$1"
}
config() { rpk topic describe "$1" -c | awk -v k="$2" '$1==k {print $2}'; }

job() { # name configmap args... -> applies the Job
  local name=$1 cm=$2; shift 2
  local args; args=$(printf '"%s", ' "$@"); args=${args%, }
  sed -e "s|JOB_NAME|$name|" -e "s|CONFIGMAP|$cm|" -e "s|TOPICSYNC_IMAGE|$IMAGE|" -e "s|\[ARGS, |[$args, |" manifests/job.yaml | k apply -f - >/dev/null
}
spec_cm() { k -n redpanda create configmap "$1" --from-file=topics.json="$2" --dry-run=client -o yaml | k apply -f - >/dev/null; }

log "kind cluster $CLUSTER"
if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  kind create cluster --name "$CLUSTER" --wait 120s
  created=1 # only a cluster this run made is deleted at the end
fi
if [[ "$IMAGE" == "topicsync:e2e" ]]; then
  log "building $IMAGE"
  docker build -q -t "$IMAGE" ../.. >/dev/null
fi
kind load docker-image "$IMAGE" --name "$CLUSTER"

log "Redpanda $REDPANDA_IMAGE"
k create namespace redpanda --dry-run=client -o yaml | k apply -f - >/dev/null
sed "s|REDPANDA_IMAGE|$REDPANDA_IMAGE|" manifests/redpanda.yaml | k apply -f - >/dev/null
k -n redpanda rollout status statefulset/redpanda --timeout=300s

log "Redpanda operator $OPERATOR_VERSION"
helm repo add redpanda https://charts.redpanda.com >/dev/null 2>&1 || true
helm repo update redpanda >/dev/null
helm --kube-context "$KCTX" upgrade --install redpanda-operator redpanda/operator --version "$OPERATOR_VERSION" -n redpanda \
  --set crds.enabled=true --wait --timeout 5m >/dev/null
k -n redpanda apply -f manifests/topics.yaml >/dev/null
for t in ohlcv.v1.e2e.1m ohlcv.v1.e2e.1h raw.trades.e2e e2e.deleteme; do
  k -n redpanda wait --for=condition=Ready "topic.cluster.redpanda.com/$t" --timeout=180s >/dev/null || fail "Topic $t not Ready"
done
for t in ohlcv.v1.e2e.1m ohlcv.v1.e2e.1h raw.trades.e2e; do
  seq 1 50 | k -n redpanda exec -i redpanda-0 -- rpk topic produce "$t" >/dev/null
done
declare -A before
for t in ohlcv.v1.e2e.1m ohlcv.v1.e2e.1h raw.trades.e2e; do
  before[$t]=$(hw "$t")
  [[ "${before[$t]}" -ge 50 ]] || fail "$t holds ${before[$t]} records, 50 produced: the survival check below would prove nothing"
done
log "topics made by the operator, with data: ${before[*]}"

log "negative control: deleting a Topic resource with the operator running"
k -n redpanda delete topic.cluster.redpanda.com e2e.deleteme --timeout=120s >/dev/null
for _ in $(seq 1 30); do has_topic e2e.deleteme || break; sleep 2; done
has_topic e2e.deleteme && fail "the operator did not delete the Kafka topic: the hazard is not reproduced, the test proves nothing"
log "  the operator deleted the Kafka topic e2e.deleteme (as expected)"

log "migration"
../../scripts/migrate-from-redpanda-operator.sh --namespace redpanda --operator-deployment redpanda-operator \
  --spec spec-adopt.json --snapshot-dir "$(mktemp -d)/snapshot" --timeout 180
[[ "$(k -n redpanda get topics.cluster.redpanda.com -o name | wc -l)" == 0 ]] || fail "Topic resources left"
k -n redpanda rollout status deployment/redpanda-operator --timeout=180s >/dev/null
log "  operator back; waiting a minute for it to act on anything it still knew about"
sleep 60
for t in ohlcv.v1.e2e.1m ohlcv.v1.e2e.1h raw.trades.e2e; do
  has_topic "$t" || fail "$t was deleted by the migration"
  [[ "$(hw "$t")" == "${before[$t]}" ]] || fail "$t high watermark $(hw "$t"), was ${before[$t]}: records lost"
done
log "  all three topics and their records survived"

log "topicsync Job over the adopted topics"
spec_cm topics-adopt spec-adopt.json
job topicsync-adopt topics-adopt apply
k -n redpanda wait --for=condition=Complete job/topicsync-adopt --timeout=300s >/dev/null || fail "adopt Job did not complete"
k -n redpanda logs job/topicsync-adopt | grep -q "0 to create, 0 to alter, 0 to repartition, 0 to delete, 3 unchanged" ||
  fail "the adopt Job changed something: $(k -n redpanda logs job/topicsync-adopt)"
log "  nothing to change"

log "topicsync Job with a changed file"
spec_cm topics-change spec-change.json
job topicsync-change topics-change apply
k -n redpanda wait --for=condition=Complete job/topicsync-change --timeout=300s >/dev/null || fail "change Job did not complete"
has_topic ohlcv.v1.e2e.1d || fail "ohlcv.v1.e2e.1d not created"
[[ "$(config ohlcv.v1.e2e.1m retention.bytes)" == 50000000 ]] || fail "retention.bytes not altered: $(config ohlcv.v1.e2e.1m retention.bytes)"
[[ "$(hw ohlcv.v1.e2e.1m)" == "${before[ohlcv.v1.e2e.1m]}" ]] || fail "records lost on alter"
log "  created and altered"

log "topicsync plan Job: no drift"
job topicsync-plan topics-change plan --detailed-exitcode
k -n redpanda wait --for=condition=Complete job/topicsync-plan --timeout=300s >/dev/null || fail "plan Job reports drift"

log "topicsync Job with an invalid file"
printf '{"apiVersion": "topicsync.aperiodic.io/v1", "cluster": "e2e", "topics": [{"name": "bad name"}]}' > /tmp/topicsync-bad.json
spec_cm topics-bad /tmp/topicsync-bad.json
job topicsync-bad topics-bad apply
k -n redpanda wait --for=condition=Failed job/topicsync-bad --timeout=300s >/dev/null || fail "the invalid file's Job did not fail"
log "  failed, as it must"

log "PASS"
