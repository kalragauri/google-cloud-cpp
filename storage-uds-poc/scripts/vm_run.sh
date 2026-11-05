#!/bin/bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# VM job: runs S0 probes or the S4 benchmark matrix (plan rev 4 §6-§8) and
# uploads every CSV, summary JSON, Cloud Monitoring JSON, path report, and log
# to gs://$OPS/results/$RUN/.
#
#   PHASE=probes    S0 probes (quickstart on regional & zonal + Monitoring check)
#   PHASE=smoke     short-duration smoke run of every workload x comparator
#   PHASE=full      prescribed >=3 min / 5 min measured matrix (W1-W4 zonal,
#                   plus regional when ALLOW_REGIONAL_BIDI=1; W5 zonal; W6)
#   PHASE=regional  W1-W4 on the regional bucket only, for a bucket whose
#                   project is allow-listed for BidiReadObject (row C2)
#   PHASE=wt        worker-thread sensitivity: W3 and W4 on zonal for the two
#                   UDS comparators at WORKER_THREADS=N (N > 0)
#
# Enforces plan §7.1 comparison rules:
#   * Ops Agent active before starting (row D3).
#   * One comparator running at a time; lingering daemons stopped between cells
#     so idle server RSS never leaks into another comparator's VM memory window.
#   * Rotating comparator order across workloads (`native`, `uds-sidecar`,
#     `uds-inproc`, row C8).
#   * `verify_path.py` gates every cell (Cloudpath only, row C1).
#   * `monitoring.py` queries Cloud Monitoring CPU and Ops Agent memory per
#     workload window (rows D1/D2).
#   * Regional BidiReadObject is gated per bucket-owning project; any phase
#     that reads bidi on the regional bucket first checks the gate and aborts
#     with a clear message instead of producing 15 PERMISSION_DENIED cells.
set -uo pipefail

OPS="${OPS:?OPS bucket must be set}"
PHASE="${PHASE:-smoke}"
REGIONAL="${REGIONAL:-}"
ZONAL="${ZONAL:-}"
BUCKET_PREFIX="${BUCKET_PREFIX:-}"
ALLOW_REGIONAL_BIDI="${ALLOW_REGIONAL_BIDI:-0}"
WORKER_THREADS="${WORKER_THREADS:-0}"
POC="${POC:-$HOME/poc/src/storage-uds-poc}"
BUILD="$POC/cmake-out"
RUN="${RUN:-$PHASE-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT="$HOME/poc/results/$RUN"

case "$PHASE" in
  probes | smoke | full)
    : "${REGIONAL:?REGIONAL bucket must be set}"
    : "${ZONAL:?ZONAL bucket must be set}"
    : "${BUCKET_PREFIX:?BUCKET_PREFIX must be set}"
    ;;
  regional)
    : "${REGIONAL:?REGIONAL bucket must be set}"
    ;;
  wt)
    : "${ZONAL:?ZONAL bucket must be set}"
    if (( WORKER_THREADS <= 0 )); then
      echo "FATAL: PHASE=wt requires WORKER_THREADS > 0" >&2
      exit 1
    fi
    ;;
  *)
    echo "FATAL: unknown PHASE=$PHASE" >&2
    exit 1
    ;;
esac

mkdir -p "$OUT"
cd "$OUT"

export GCS_CORE_SERVER="$BUILD/gcs-core-server"
BENCH="$BUILD/bench/uds_poc_bench"
VERIFY="$POC/scripts/verify_path.py"
MON="$POC/scripts/monitoring.py"

# Row D3: verify Google Cloud Ops Agent is active before measuring.
if ! systemctl is-active --quiet google-cloud-ops-agent; then
  echo "FATAL: google-cloud-ops-agent is not active" >&2
  exit 1
fi

md() {
  curl -fsS -H Metadata-Flavor:Google "http://169.254.169.254/computeMetadata/v1/$1"
}
PROJECT="$(md project/project-id)"
INSTANCE_ID="$(md instance/id)"
ZONE="$(md instance/zone | awk -F/ '{print $NF}')"
MACHINE="$(md instance/machine-type | awk -F/ '{print $NF}')"
RUST_DIR="$HOME/poc/rust"
[[ ! -d "$RUST_DIR" ]] && RUST_DIR="$BUILD/_deps/google_cloud_rust-src"
RUST_SHA="$(git -C "$RUST_DIR" rev-parse HEAD 2>/dev/null || echo unknown)"
CPP_SHA="$(git -C "$POC/.." rev-parse HEAD 2>/dev/null || echo unknown)"

{
  echo "host=$(hostname) project=$PROJECT instance_id=$INSTANCE_ID zone=$ZONE machine=$MACHINE cpus=$(nproc)"
  echo "phase=$PHASE regional=$REGIONAL zonal=$ZONAL bucket_prefix=$BUCKET_PREFIX worker_threads=$WORKER_THREADS start=$(date -u +%FT%TZ)"
  echo "ops_agent=$(systemctl is-active google-cloud-ops-agent)"
  echo "cpp_sha=$CPP_SHA rust_sha=$RUST_SHA"
  echo "kernel=$(uname -r) glibc=$(ldd --version | head -1 | awk '{print $NF}')"
} | tee run-info.txt
gcloud storage cp --quiet run-info.txt "gs://$OPS/results/$RUN/" >/dev/null 2>&1 || true

stop_daemon() {
  pkill -x gcs-core-server 2>/dev/null || true
  for _ in $(seq 1 50); do
    pgrep -x gcs-core-server >/dev/null 2>&1 || break
    sleep 0.05
  done
}

# Aborts unless the service admits BidiReadObject on the regional bucket. The
# quickstart prints a "GAP  BidiReadObject" line when the bucket-owning project
# is not on the allow list; everything else in that run still passes, so the
# exit code alone cannot be used.
require_regional_bidi() {
  local gate_log="gate-quickstart-inproc-regional.log"
  stop_daemon
  "$BUILD/quickstart_inproc" --project="$PROJECT" "$REGIONAL" >"$gate_log" 2>&1
  local rc=$?
  gcloud storage cp --quiet "$gate_log" "gs://$OPS/results/$RUN/" >/dev/null 2>&1 || true
  if (( rc != 0 )) || [[ "$(cat "$gate_log")" == *"GAP  BidiReadObject"* ]]; then
    echo "FATAL: BidiReadObject is not admitted on gs://$REGIONAL (rc=$rc);" \
      "see $gate_log" >&2
    exit 1
  fi
  echo "gate: BidiReadObject admitted on gs://$REGIONAL"
}

rotate() {
  local n=$1
  shift
  local a=("$@")
  local k=$((n % ${#a[@]}))
  echo "${a[@]:k}" "${a[@]:0:k}"
}

FAILED=0

if [[ "$PHASE" == "probes" ]]; then
  PROBE_START="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  stop_daemon
  python3 "$VERIFY" "probe-quickstart-regional.path.json" -- \
    "$BUILD/quickstart" --project="$PROJECT" "$REGIONAL" | tee probe-quickstart-regional.log || FAILED=$((FAILED + 1))
  python3 "$VERIFY" "probe-quickstart-zonal.path.json" -- \
    "$BUILD/quickstart" --zonal "$ZONAL" | tee probe-quickstart-zonal.log || FAILED=$((FAILED + 1))
  stop_daemon
  python3 "$VERIFY" "probe-quickstart-inproc-regional.path.json" -- \
    "$BUILD/quickstart_inproc" --project="$PROJECT" "$REGIONAL" | tee probe-quickstart-inproc-regional.log || FAILED=$((FAILED + 1))
  python3 "$VERIFY" "probe-quickstart-inproc-zonal.path.json" -- \
    "$BUILD/quickstart_inproc" --zonal "$ZONAL" | tee probe-quickstart-inproc-zonal.log || FAILED=$((FAILED + 1))
  sleep 90
  PROBE_END="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  python3 "$MON" --project="$PROJECT" --instance-id="$INSTANCE_ID" \
    --start="$PROBE_START" --end="$PROBE_END" --wait-secs=240 \
    --out="probe-monitoring.json" || FAILED=$((FAILED + 1))
  echo "end=$(date -u +%FT%TZ) failed_runs=$FAILED" | tee -a run-info.txt
  gcloud storage cp --quiet "$OUT"/* "gs://$OPS/results/$RUN/" >/dev/null 2>&1 || true
  exit $((FAILED > 0 ? 1 : 0))
fi

if [[ "$PHASE" == "smoke" ]]; then
  WARMUP=1
  DUR_SHORT=3
  DUR_LONG=4
  THREADS_48=4
else
  WARMUP=10
  DUR_SHORT=180
  DUR_LONG=300
  THREADS_48=48
fi

SUMMARY_FILES=()

run_cell() {
  local cell=$1
  local mode=$2
  shift 2
  local name="${cell}-${mode}"
  echo "######## $name ($(date -u +%FT%TZ))"
  stop_daemon
  python3 "$VERIFY" "$name.path.json" -- \
    "$BENCH" --mode="$mode" \
    --warmup-secs="$WARMUP" \
    --worker-threads="$WORKER_THREADS" \
    --out="$name.csv" \
    --summary-json="$name.summary.json" \
    "$@" >"$name.log" 2>&1
  local rc=$?
  stop_daemon
  echo "  exit=$rc $(grep '^SUMMARY ' "$name.log" || tail -n 1 "$name.log")"
  if (( rc != 0 )); then
    FAILED=$((FAILED + 1))
  else
    SUMMARY_FILES+=("$name")
  fi
  gcloud storage cp --quiet "$name".* "gs://$OPS/results/$RUN/" >/dev/null 2>&1 || true
}

COMPS=(native uds-sidecar uds-inproc)
k=0

# --- PHASE=wt: worker-thread sensitivity on the UDS comparators only ---------
# Native has no Tokio pool, so it is not a comparator here; the base run (same
# cells at the default WORKER_THREADS=0) is the reference for
# `analyze.py --sensitivity`.
if [[ "$PHASE" == "wt" ]]; then
  for mode in uds-sidecar uds-inproc; do
    run_cell "random8k-zonal" "$mode" \
      --workload=random8k --bucket="$ZONAL" --bucket-type=zonal \
      --duration-secs="$DUR_SHORT"
  done
  for mode in uds-sidecar uds-inproc; do
    run_cell "bidi-read-48t-zonal" "$mode" \
      --workload=bidi-read-48t --bucket="$ZONAL" --bucket-type=zonal \
      --threads="$THREADS_48" --duration-secs="$DUR_LONG"
  done
fi

# --- W1-W3 on zonal (and regional when allow-listed) --------------------------
BIDI_TARGETS=()
if [[ "$PHASE" == "regional" ]]; then
  require_regional_bidi
  BIDI_TARGETS=("regional:$REGIONAL")
elif [[ "$PHASE" != "wt" ]]; then
  BIDI_TARGETS=("zonal:$ZONAL")
  if [[ "$ALLOW_REGIONAL_BIDI" == "1" ]]; then
    require_regional_bidi
    BIDI_TARGETS+=("regional:$REGIONAL")
  fi
fi

for target in "${BIDI_TARGETS[@]}"; do
  btype="${target%%:*}"
  bname="${target#*:}"
  for w in open read50x2x4-serial read50x2x4-concurrent random8k; do
    for mode in $(rotate "$k" "${COMPS[@]}"); do
      run_cell "${w}-${btype}" "$mode" \
        --workload="$w" --bucket="$bname" --bucket-type="$btype" \
        --duration-secs="$DUR_SHORT"
    done
    k=$((k + 1))
  done

  # --- W4: 48-thread BidiRead 16 MiB ranges of 200 MiB object -----------------
  for mode in $(rotate "$k" "${COMPS[@]}"); do
    run_cell "bidi-read-48t-${btype}" "$mode" \
      --workload=bidi-read-48t --bucket="$bname" --bucket-type="$btype" \
      --threads="$THREADS_48" --duration-secs="$DUR_LONG"
  done
  k=$((k + 1))
done

if [[ "$PHASE" == "smoke" || "$PHASE" == "full" ]]; then
  # --- W5: 48-thread appendable BidiWrite 200 MB object + Finalize (zonal) ----
  for mode in $(rotate "$k" "${COMPS[@]}"); do
    run_cell "bidi-write-48t-zonal" "$mode" \
      --workload=bidi-write-48t --bucket="$ZONAL" --bucket-type=zonal \
      --threads="$THREADS_48" --duration-secs="$DUR_LONG"
  done
  k=$((k + 1))

  # --- W6: ListObjects (100 items, regional) & ListBuckets (100 items) --------
  for mode in $(rotate "$k" "${COMPS[@]}"); do
    run_cell "list-objects-100-regional" "$mode" \
      --workload=list-objects-100 --bucket="$REGIONAL" --bucket-type=regional \
      --duration-secs="$DUR_SHORT"
  done
  k=$((k + 1))

  for mode in $(rotate "$k" "${COMPS[@]}"); do
    run_cell "list-buckets-100-project" "$mode" \
      --workload=list-buckets-100 --project="$PROJECT" \
      --bucket-prefix="$BUCKET_PREFIX" --duration-secs="$DUR_SHORT"
  done
  k=$((k + 1))
fi

# --- Query Cloud Monitoring CPU & Ops Agent memory per cell window ------------
# Wait for the final cell's 60 s Monitoring alignment bucket to close and flush
# (`compute.googleapis.com/instance/cpu/utilization` has up to ~240 s lag).
sleep "${MONITORING_FLUSH_SECS:-240}"
echo "######## Querying Cloud Monitoring for ${#SUMMARY_FILES[@]} windows"
for name in "${SUMMARY_FILES[@]}"; do
  if ! python3 "$MON" \
      --project="$PROJECT" \
      --instance-id="$INSTANCE_ID" \
      --summary-json="$name.summary.json" \
      --wait-secs=240 \
      --out="$name.monitoring.json"; then
    echo "  monitoring FAILED for $name" >&2
    FAILED=$((FAILED + 1))
  else
    gcloud storage cp --quiet "$name.monitoring.json" "gs://$OPS/results/$RUN/" >/dev/null 2>&1 || true
  fi
done

echo "end=$(date -u +%FT%TZ) failed_runs=$FAILED" | tee -a run-info.txt
gcloud storage cp --quiet run-info.txt "gs://$OPS/results/$RUN/" >/dev/null 2>&1 || true
echo "RUN=$RUN FAILED_RUNS=$FAILED"
exit $((FAILED > 0 ? 1 : 0))
