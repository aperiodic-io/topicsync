#!/usr/bin/env bash
# Hand a namespace's Kafka topics over from the Redpanda operator's Topic resources to topicsync, without deleting a
# single topic.
#
# Deleting a Topic resource makes the operator delete the Kafka topic and its data (its finalizer). This script removes
# the resources so that nothing deletes a topic:
#   1. refuses unless whatever applies the Topics (a Flux Kustomization) is suspended or gone, so nothing re-creates them;
#   2. snapshots every Topic resource (JSON) into --snapshot-dir;
#   3. scales the operator to 0 and waits until none of its pods is left: no operator, nobody to act on a finalizer;
#   4. clears each Topic's finalizers, then deletes it;
#   5. checks that no Topic resource is left, and scales the operator back.
# The brokers keep running throughout; the operator also manages the cluster's own resources, which simply wait.
#
# Usage:
#   migrate-from-redpanda-operator.sh --namespace NS --operator-deployment NAME [--operator-namespace NS]
#                                     [--selector LABELS] [--snapshot-dir DIR] [--timeout 300] [--dry-run]
#
# Verify the topics before and after with `topicsync plan --detailed-exitcode` (0: nothing to change) or rpk.
set -euo pipefail

ns="" opdeploy="" opns="" selector="" snapdir="" timeout=300 dry=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace) ns="$2"; shift 2 ;;
    --operator-deployment) opdeploy="$2"; shift 2 ;;
    --operator-namespace) opns="$2"; shift 2 ;;
    --selector) selector="$2"; shift 2 ;;
    --snapshot-dir) snapdir="$2"; shift 2 ;;
    --timeout) timeout="$2"; shift 2 ;;
    --dry-run) dry=1; shift ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "unknown argument $1" >&2; exit 2 ;;
  esac
done
[[ -n "$ns" && -n "$opdeploy" ]] || { echo "--namespace and --operator-deployment are required" >&2; exit 2; }
opns="${opns:-$ns}"
snapdir="${snapdir:-./topic-snapshot-$ns-$(date -u +%Y%m%dT%H%M%SZ)}"
sel=(); [[ -n "$selector" ]] && sel=(-l "$selector")
kt=(kubectl -n "$ns")
log() { echo "[$(date -u +%T)] $*"; }
run() { if [[ $dry == 1 ]]; then echo "DRY-RUN: $*"; else "$@"; fi; }

mkdir -p "$snapdir"
"${kt[@]}" get topics.cluster.redpanda.com "${sel[@]}" -o json > "$snapdir/topics.json"
mapfile -t names < <(jq -r '.items[].metadata.name' "$snapdir/topics.json")
log "${#names[@]} Topic resources in $ns${selector:+ ($selector)}, snapshot in $snapdir/topics.json"
[[ ${#names[@]} -gt 0 ]] || { log "nothing to do"; exit 0; }

# 1. nothing may re-apply them
mapfile -t owners < <(jq -r '.items[].metadata.labels // {} | select(."kustomize.toolkit.fluxcd.io/name") |
  ."kustomize.toolkit.fluxcd.io/namespace" + "/" + ."kustomize.toolkit.fluxcd.io/name"' "$snapdir/topics.json" | sort -u)
for o in "${owners[@]}"; do
  [[ -z "$o" ]] && continue
  suspended=$(kubectl -n "${o%%/*}" get kustomizations.kustomize.toolkit.fluxcd.io "${o#*/}" -o jsonpath='{.spec.suspend}' 2>/dev/null || echo gone)
  if [[ "$suspended" != "true" && "$suspended" != "gone" ]]; then
    echo "refusing: Flux Kustomization $o applies these Topics and is not suspended (flux suspend kustomization ${o#*/} -n ${o%%/*})" >&2
    exit 1
  fi
  log "Flux Kustomization $o: ${suspended/true/suspended}"
done

# 2. the operator's replicas, to restore
replicas=$(kubectl -n "$opns" get deployment "$opdeploy" -o jsonpath='{.spec.replicas}')
podsel=$(kubectl -n "$opns" get deployment "$opdeploy" -o json | jq -r '.spec.selector.matchLabels | to_entries | map("\(.key)=\(.value)") | join(",")')
echo "$replicas" > "$snapdir/operator-replicas"
log "operator $opns/$opdeploy: $replicas replica(s), pods $podsel"

# 3. stop the operator, wait until none of its pods is left
run kubectl -n "$opns" scale deployment "$opdeploy" --replicas=0
if [[ $dry == 0 ]]; then
  deadline=$((SECONDS + timeout))
  while [[ -n "$(kubectl -n "$opns" get pods -l "$podsel" -o name 2>/dev/null)" ]]; do
    if (( SECONDS > deadline )); then
      echo "refusing to go on: operator pods still exist after ${timeout}s; scaling it back to $replicas" >&2
      kubectl -n "$opns" scale deployment "$opdeploy" --replicas="$replicas"
      exit 1
    fi
    sleep 2
  done
  log "operator stopped: no pod left"
fi

# 4. finalizers off, then delete
for n in "${names[@]}"; do
  run "${kt[@]}" patch topics.cluster.redpanda.com "$n" --type merge -p '{"metadata":{"finalizers":null}}' >/dev/null
done
log "finalizers cleared"
for n in "${names[@]}"; do
  run "${kt[@]}" delete topics.cluster.redpanda.com "$n" --wait=true --timeout=60s >/dev/null
done

# 5. none left, operator back
if [[ $dry == 0 ]]; then
  left=$("${kt[@]}" get topics.cluster.redpanda.com "${sel[@]}" -o name | wc -l)
  if [[ "$left" != 0 ]]; then
    echo "$left Topic resources are left; the operator stays at 0 replicas: investigate before scaling it back" >&2
    exit 1
  fi
fi
run kubectl -n "$opns" scale deployment "$opdeploy" --replicas="$replicas"
log "done: ${#names[@]} Topic resources removed, the operator back at $replicas replica(s); the Kafka topics were not touched"
