#!/usr/bin/env bash
# Hand a namespace's Kafka topics over from the Redpanda operator's Topic resources to topicsync, without deleting a
# single topic.
#
# Deleting a Topic resource makes the operator delete the Kafka topic and its data (its finalizer), and the operator
# puts the finalizer back on every reconcile. So this script removes the resources only while nothing can act on them:
#   1. checks every argument before changing anything;
#   2. refuses unless every Topic resource's Kafka topic is listed in the topicsync file (--spec), so that none of
#      them becomes unmanaged, and unless whatever applies the Topics (a Flux Kustomization) and whatever manages the
#      operator (a Flux HelmRelease) are suspended or absent, so that neither re-creates them;
#   3. snapshots every Topic resource and the operator's replica count into a new --snapshot-dir;
#   4. scales the operator to 0 and waits until none of its pods is left;
#   5. per Topic: checks again that the operator has 0 replicas and no pod, clears the finalizer, checks it is gone,
#      then deletes the resource;
#   6. checks that no Topic resource is left (also none stuck deleting), and scales the operator back.
# Any error stops the script at once, with the operator left stopped and the state printed: it never reads an error as
# "gone" or "stopped". The brokers keep running throughout.
#
# Usage:
#   migrate-from-redpanda-operator.sh --namespace NS --operator-deployment NAME --spec topics.json
#       [--operator-namespace NS] [--operator-helmrelease NAME] [--selector LABELS] [--snapshot-dir DIR]
#       [--timeout SECONDS] [--operator-replicas N] [--dry-run]
set -euo pipefail

ns="" opdeploy="" opns="" ophr="" selector="" snapdir="" spec="" timeout=300 dry=0 replicas_override=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace) ns="$2"; shift 2 ;;
    --operator-deployment) opdeploy="$2"; shift 2 ;;
    --operator-namespace) opns="$2"; shift 2 ;;
    --operator-helmrelease) ophr="$2"; shift 2 ;;
    --selector) selector="$2"; shift 2 ;;
    --snapshot-dir) snapdir="$2"; shift 2 ;;
    --spec) spec="$2"; shift 2 ;;
    --timeout) timeout="$2"; shift 2 ;;
    --operator-replicas) replicas_override="$2"; shift 2 ;;
    --dry-run) dry=1; shift ;;
    -h|--help) sed -n '2,26p' "$0"; exit 0 ;;
    *) echo "unknown argument $1" >&2; exit 2 ;;
  esac
done
die() { echo "refusing: $*" >&2; exit 2; }
[[ -n "$ns" && -n "$opdeploy" && -n "$spec" ]] || die "--namespace, --operator-deployment and --spec are required"
[[ "$timeout" =~ ^[0-9]+$ ]] || die "--timeout is a whole number of seconds, not \"$timeout\""
[[ -z "$replicas_override" || "$replicas_override" =~ ^[1-9][0-9]*$ ]] || die "--operator-replicas is a positive number"
[[ -r "$spec" ]] || die "cannot read --spec $spec"
for tool in kubectl jq; do command -v "$tool" >/dev/null || die "$tool is not installed"; done
opns="${opns:-$ns}"
ophr="${ophr:-$opdeploy}"
snapdir="${snapdir:-./topic-snapshot-$ns-$(date -u +%Y%m%dT%H%M%SZ)}"
if [[ -e "$snapdir" ]] && [[ -n "$(ls -A "$snapdir")" ]]; then
  die "the snapshot dir $snapdir is not empty: an earlier run's snapshot would be overwritten"
fi
sel=(); [[ -n "$selector" ]] && sel=(-l "$selector")
kt=(kubectl -n "$ns")
log() { echo "[$(date -u +%T)] $*"; }
run() { if [[ $dry == 1 ]]; then echo "DRY-RUN: $*"; else "$@"; fi; }

stage="checking"
replicas=""
on_error() {
  local rc=$?
  report_failure "$rc"
  exit "$rc"
}
report_failure() {
  local rc=$1
  echo "FAILED (exit $rc) while $stage." >&2
  if [[ -n "$replicas" && "$stage" != "checking" ]]; then
    echo "The operator $opns/$opdeploy is left at 0 replicas (it had $replicas, saved in $snapdir/operator-replicas)." >&2
    echo "Do NOT scale it back while any Topic resource still exists without its finalizer and the deletion is unfinished:" >&2
    echo "  kubectl -n $ns get topics.cluster.redpanda.com" >&2
    echo "Once none is left (or all still have their finalizer): kubectl -n $opns scale deployment $opdeploy --replicas=$replicas" >&2
  fi
}
# fatal stops the script where errexit does not apply (inside a condition: if, until, &&)
fatal() {
  echo "$*" >&2
  report_failure 1
  exit 1
}
trap on_error ERR

# 1-2. everything checked before anything changes
mkdir -p "$snapdir"
"${kt[@]}" get topics.cluster.redpanda.com "${sel[@]}" -o json > "$snapdir/topics.json"
mapfile -t names < <(jq -r '.items[].metadata.name' "$snapdir/topics.json")
log "${#names[@]} Topic resources in $ns${selector:+ ($selector)}, snapshot in $snapdir/topics.json"
[[ ${#names[@]} -gt 0 ]] || { log "nothing to do"; exit 0; }

mapfile -t unlisted < <(jq -r --slurpfile spec "$spec" '
  ($spec[0].topics | map(.name)) as $listed
  | .items[] | (.spec.overwriteTopicName // .metadata.name) as $t | select(($listed | index($t)) == null) | $t' "$snapdir/topics.json")
if [[ ${#unlisted[@]} -gt 0 ]]; then
  die "${#unlisted[@]} Topic resources are not in $spec, and would become unmanaged: ${unlisted[*]:0:10}"
fi
log "every Topic resource's topic is in $spec"

mapfile -t owners < <(jq -r '.items[].metadata.labels // {} | select(."kustomize.toolkit.fluxcd.io/name") |
  ."kustomize.toolkit.fluxcd.io/namespace" + "/" + ."kustomize.toolkit.fluxcd.io/name"' "$snapdir/topics.json" | sort -u)
for o in "${owners[@]}"; do
  [[ -z "$o" ]] && continue
  # only NotFound counts as gone: any other error stops the script
  out=$(kubectl -n "${o%%/*}" get kustomizations.kustomize.toolkit.fluxcd.io "${o#*/}" --ignore-not-found -o jsonpath='{.metadata.name} {.spec.suspend}')
  if [[ -n "$out" && "$out" != *" true" ]]; then
    die "Flux Kustomization $o applies these Topics and is not suspended (flux suspend kustomization ${o#*/} -n ${o%%/*})"
  fi
  if [[ -n "$out" ]]; then log "Flux Kustomization $o: suspended"; else log "Flux Kustomization $o: gone"; fi
done

if kubectl api-resources --api-group=helm.toolkit.fluxcd.io -o name | grep -qx 'helmreleases.helm.toolkit.fluxcd.io'; then
  out=$(kubectl -n "$opns" get helmreleases.helm.toolkit.fluxcd.io "$ophr" --ignore-not-found -o jsonpath='{.metadata.name} {.spec.suspend}')
  if [[ -n "$out" && "$out" != *" true" ]]; then
    die "the operator's HelmRelease $opns/$ophr is not suspended: an upgrade would restore its replicas mid-way (flux suspend helmrelease $ophr -n $opns)"
  fi
  if [[ -n "$out" ]]; then log "operator HelmRelease $opns/$ophr: suspended"; else log "operator HelmRelease $opns/$ophr: absent"; fi
fi

replicas=$(kubectl -n "$opns" get deployment "$opdeploy" -o jsonpath='{.spec.replicas}')
if [[ "$replicas" == 0 ]]; then
  [[ -n "$replicas_override" ]] || die "the operator has 0 replicas already (an earlier run stopped part-way?): pass --operator-replicas N, its count to restore"
  replicas="$replicas_override"
fi
podsel=$(kubectl -n "$opns" get deployment "$opdeploy" -o json | jq -r '.spec.selector.matchLabels | to_entries | map("\(.key)=\(.value)") | join(",")')
[[ -n "$podsel" ]] || die "the operator's Deployment has no matchLabels selector"
echo "$replicas" > "$snapdir/operator-replicas"
log "operator $opns/$opdeploy: $replicas replica(s) to restore, pods $podsel"

operator_stopped() { # strict: an error is an error, never "no pod" (it runs in conditions, where errexit is off)
  local r pods
  r=$(kubectl -n "$opns" get deployment "$opdeploy" -o jsonpath='{.spec.replicas}') || fatal "reading the operator's replicas failed"
  pods=$(kubectl -n "$opns" get pods -l "$podsel" -o name) || fatal "listing the operator's pods failed"
  [[ "$r" == 0 && -z "$pods" ]]
}

# 3-4. stop the operator
stage="stopping the operator"
run kubectl -n "$opns" scale deployment "$opdeploy" --replicas=0
if [[ $dry == 0 ]]; then
  deadline=$((SECONDS + timeout))
  until operator_stopped; do
    if (( SECONDS > deadline )); then
      echo "the operator still has pods after ${timeout}s" >&2
      false
    fi
    sleep 2
  done
  log "operator stopped: 0 replicas, no pod"
fi

# 5. one Topic at a time, the operator checked before each
stage="removing Topic resources"
for n in "${names[@]}"; do
  if [[ $dry == 0 ]]; then
    operator_stopped || { echo "the operator came back (replicas or pods) before $n" >&2; false; }
  fi
  run "${kt[@]}" patch topics.cluster.redpanda.com "$n" --type merge -p '{"metadata":{"finalizers":null}}' >/dev/null
  if [[ $dry == 0 ]]; then
    left=$("${kt[@]}" get topics.cluster.redpanda.com "$n" -o jsonpath='{.metadata.finalizers}')
    [[ -z "$left" ]] || { echo "$n has finalizers again ($left): something reconciles it" >&2; false; }
  fi
  run "${kt[@]}" delete topics.cluster.redpanda.com "$n" --wait=true --timeout=60s >/dev/null
done
log "${#names[@]} Topic resources removed"

# 6. none left, then the operator back
stage="checking that no Topic resource is left"
if [[ $dry == 0 ]]; then
  left=$("${kt[@]}" get topics.cluster.redpanda.com "${sel[@]}" -o name | wc -l)
  [[ "$left" == 0 ]] || { echo "$left Topic resources are left" >&2; false; }
fi
stage="restoring the operator"
run kubectl -n "$opns" scale deployment "$opdeploy" --replicas="$replicas"
trap - ERR
log "done: ${#names[@]} Topic resources removed, the operator back at $replicas replica(s); the Kafka topics were not touched"
