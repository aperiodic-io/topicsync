#!/usr/bin/env bash
# Tests of migrate-from-redpanda-operator.sh against a fake kubectl that models the Topic resources, their
# finalizers and the operator: deleting a Topic that still has its finalizer while the operator runs is recorded as
# DATA LOSS (the operator would delete the Kafka topic). Every scenario asserts no data loss.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
script="$here/migrate-from-redpanda-operator.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/bin"

cat >"$tmp/bin/kubectl" <<'FAKE'
#!/usr/bin/env bash
# state in $FAKE: topics/<name> (content: "fin" or ""), replicas, pods, ks_suspend, hr_suspend, hr_crd, log, loss
set -uo pipefail
st="$FAKE"
echo "$*" >>"$st/log"
if [[ -n "${FAKE_FAIL:-}" ]] && [[ "$*" =~ $FAKE_FAIL ]]; then echo "Error from server: etcdserver: request timed out" >&2; exit 1; fi
args=("$@")
[[ "${args[0]}" == "-n" ]] && args=("${args[@]:2}")
case "${args[*]}" in
  "api-resources --api-group=helm.toolkit.fluxcd.io -o name")
    [[ -f "$st/hr_crd" ]] && echo helmreleases.helm.toolkit.fluxcd.io; exit 0 ;;
  "get topics.cluster.redpanda.com -o json")
    printf '{"items":['; first=1
    for f in "$st"/topics/*; do [[ -e "$f" ]] || continue; n=$(basename "$f")
      [[ $first == 1 ]] || printf ','; first=0
      printf '{"metadata":{"name":"%s","labels":{"kustomize.toolkit.fluxcd.io/name":"metrics-topics","kustomize.toolkit.fluxcd.io/namespace":"flux-system"}},"spec":{"overwriteTopicName":"%s"}}' "$n" "${n//-/_}"
    done; printf ']}\n'; exit 0 ;;
  "get topics.cluster.redpanda.com -o name")
    for f in "$st"/topics/*; do [[ -e "$f" ]] && echo "topic.cluster.redpanda.com/$(basename "$f")"; done; exit 0 ;;
  get\ kustomizations*)
    [[ -f "$st/ks_suspend" ]] && echo -n "metrics-topics $(cat "$st/ks_suspend")"; exit 0 ;;
  get\ helmreleases*)
    [[ -f "$st/hr_suspend" ]] && echo -n "redpanda-operator $(cat "$st/hr_suspend")"; exit 0 ;;
  "get deployment redpanda-operator -o jsonpath={.spec.replicas}") cat "$st/replicas"; exit 0 ;;
  "get deployment redpanda-operator -o json") echo '{"spec":{"selector":{"matchLabels":{"app":"operator"}}}}'; exit 0 ;;
  "get pods -l app=operator -o name")
    for _ in $(seq 1 "$(cat "$st/pods")"); do echo pod/operator-x; done; exit 0 ;;
  scale\ deployment\ redpanda-operator\ --replicas=*)
    r="${args[3]#--replicas=}"; echo "$r" >"$st/replicas"; echo "$r" >"$st/pods"; exit 0 ;;
  patch\ topics.cluster.redpanda.com\ *)
    n="${args[2]}"; : >"$st/topics/$n"
    patches=$(( $(cat "$st/patches" 2>/dev/null || echo 0) + 1 )); echo $patches >"$st/patches"
    if [[ -n "${FAKE_OPERATOR_BACK_AFTER:-}" && $patches -ge $FAKE_OPERATOR_BACK_AFTER ]]; then echo 1 >"$st/replicas"; echo 1 >"$st/pods"; fi
    exit 0 ;;
  get\ topics.cluster.redpanda.com\ *\ -o\ jsonpath=\{.metadata.finalizers\})
    [[ "$(cat "$st/topics/${args[2]}")" == fin ]] && echo -n '["operator.redpanda.com/finalizer"]'; exit 0 ;;
  delete\ topics.cluster.redpanda.com\ *)
    n="${args[2]}"
    if [[ "$(cat "$st/topics/$n")" == fin && "$(cat "$st/pods")" -gt 0 ]]; then echo "$n" >>"$st/loss"; fi
    rm -f "$st/topics/$n"; exit 0 ;;
esac
echo "fake kubectl: unhandled: $*" >&2; exit 99
FAKE
chmod +x "$tmp/bin/kubectl"

fail() { echo "FAIL: $*" >&2; exit 1; }
setup() { # fresh state: 3 topics with finalizers, operator at 1, Kustomization and HelmRelease suspended
  st="$tmp/st-$1"; rm -rf "$st"; mkdir -p "$st/topics"; : >"$st/log"
  for t in ohlcv-v1-a-1m ohlcv-v1-a-1h raw-trades-a; do echo fin >"$st/topics/$t"; done
  echo 1 >"$st/replicas"; echo 1 >"$st/pods"; echo true >"$st/ks_suspend"; echo true >"$st/hr_suspend"; touch "$st/hr_crd"
  cat >"$st/spec.json" <<'EOF'
{"apiVersion": "topicsync.aperiodic.io/v1", "cluster": "c", "topics": [{"name": "ohlcv_v1_a_1m"}, {"name": "ohlcv_v1_a_1h"}, {"name": "raw_trades_a"}]}
EOF
}
migrate() { # scenario args... -> exit code
  local s="$1"; shift
  FAKE="$tmp/st-$s" PATH="$tmp/bin:$PATH" "$script" --namespace live-metrics --operator-deployment redpanda-operator \
    --spec "$tmp/st-$s/spec.json" --snapshot-dir "$tmp/snap-$s" --timeout 5 "$@" >"$tmp/out-$s" 2>&1 && return 0 || return $?
}
no_loss() { [[ ! -s "$tmp/st-$1/loss" ]] || fail "$1: DATA LOSS: $(cat "$tmp/st-$1/loss")"; }
no_deletes() { ! grep -q ' delete ' "$tmp/st-$1/log" || fail "$1: deleted although it should have refused"; }

# the happy path: every resource gone, no data loss, the operator back at 1
setup ok; migrate ok || fail "ok: exit $? $(cat "$tmp/out-ok")"
no_loss ok
[[ -z "$(ls "$tmp/st-ok/topics")" ]] || fail "ok: resources left"
[[ "$(cat "$tmp/st-ok/replicas")" == 1 ]] || fail "ok: operator not restored"

# --timeout 5m (kubectl style) is refused before anything changes (it used to skip the wait for the operator)
setup tm; set +e; migrate tm --timeout 5m; rc=$?; set -e
[[ $rc == 2 ]] || fail "tm: exit $rc"; no_deletes tm
! grep -q ' scale ' "$tmp/st-tm/log" || fail "tm: scaled the operator before refusing"

# an apiserver error while waiting for the operator's pods is not "no pod left"
setup pods; set +e; FAKE_FAIL='get pods' migrate pods; rc=$?; set -e
[[ $rc != 0 ]] || fail "pods: an error read as success"; no_deletes pods; no_loss pods
grep -q "left at 0 replicas" "$tmp/out-pods" || fail "pods: no restore instructions: $(cat "$tmp/out-pods")"

# an apiserver error reading the Kustomization is not "gone"
setup ks; set +e; FAKE_FAIL='get kustomizations' migrate ks; rc=$?; set -e
[[ $rc != 0 ]] || fail "ks: an error read as gone"; no_deletes ks

# an unsuspended Kustomization, or HelmRelease, refuses
setup ks2; echo false >"$tmp/st-ks2/ks_suspend"; set +e; migrate ks2; rc=$?; set -e
[[ $rc == 2 ]] || fail "ks2: exit $rc"; no_deletes ks2
setup hr; echo false >"$tmp/st-hr/hr_suspend"; set +e; migrate hr; rc=$?; set -e
[[ $rc == 2 ]] || fail "hr: exit $rc"; no_deletes hr
grep -q "HelmRelease" "$tmp/out-hr" || fail "hr: $(cat "$tmp/out-hr")"

# a Topic whose topic the file does not list refuses (it would become unmanaged)
setup un; echo '{"apiVersion": "topicsync.aperiodic.io/v1", "cluster": "c", "topics": [{"name": "ohlcv_v1_a_1m"}]}' >"$tmp/st-un/spec.json"
set +e; migrate un; rc=$?; set -e
[[ $rc == 2 ]] || fail "un: exit $rc"; no_deletes un

# the operator comes back mid-way (an upgrade): the script stops before the next delete, and no data is lost
setup back; set +e; FAKE_OPERATOR_BACK_AFTER=2 migrate back; rc=$?; set -e
[[ $rc != 0 ]] || fail "back: went on with the operator running"; no_loss back
grep -q "came back\|finalizers again" "$tmp/out-back" || fail "back: $(cat "$tmp/out-back")"

# a partial failure leaves the operator stopped; a rerun refuses without --operator-replicas, and a non-empty
# snapshot dir is refused
setup part; set +e; FAKE_FAIL='patch topics.cluster.redpanda.com raw-trades-a' migrate part; rc=$?; set -e
[[ $rc != 0 ]] || fail "part: a failed patch exited 0"; no_loss part
[[ "$(cat "$tmp/st-part/replicas")" == 0 ]] || fail "part: the operator was restored with Topics half-handled"
set +e; migrate part; rc=$?; set -e
if [[ $rc != 2 ]] || ! grep -q "not empty" "$tmp/out-part"; then fail "part rerun into the same snapshot dir: exit $rc $(cat "$tmp/out-part")"; fi
rm -rf "$tmp/snap-part"; set +e; migrate part; rc=$?; set -e
if [[ $rc != 2 ]] || ! grep -q "operator-replicas" "$tmp/out-part"; then fail "part rerun with the operator at 0: exit $rc $(cat "$tmp/out-part")"; fi
rm -rf "$tmp/snap-part"; migrate part --operator-replicas 1 || fail "part rerun with --operator-replicas: $(cat "$tmp/out-part")"
no_loss part; [[ "$(cat "$tmp/st-part/replicas")" == 1 ]] || fail "part: not restored to 1"

# --dry-run changes nothing
setup dry; migrate dry --dry-run || fail "dry: $(cat "$tmp/out-dry")"
! grep -qE ' (scale|patch|delete) ' "$tmp/st-dry/log" || fail "dry: changed something"

echo "migrate-from-redpanda-operator_test: ok"
