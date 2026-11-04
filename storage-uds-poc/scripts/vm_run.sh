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

# VM job: runs the benchmark matrix (plan §7.2) and uploads every CSV, log
# and path report to gs://$OPS/results/$RUN/.
#
#   PHASE=smoke       2 iterations of every cell, default flags first
#   PHASE=full        the measured matrix
#   PHASE=supplement  supplementary Cloudpath pass (uds-ch4, inproc-ch4,
#                     pure-rust-unbuffered, pure-upload THROUGHPUT-WRITE)
#   ITER              iterations per cell in full/supplement (default 200)
#
# Rules enforced here: implementations run one at a time, in rotating order
# per cell; every run goes through verify_path.py (Cloudpath only); the
# sidecar, the in-process core and the pure-Rust core all use 4 gRPC
# subchannels, like native. `inproc` (core on threads of the benchmark
# process) runs next to `uds` (core in a sidecar process) in every cell with
# the same core arguments, so the pair isolates the process boundary.
set -uo pipefail
OPS="${OPS:-kalragauri-uds-poc-ops}"
PHASE="${PHASE:-smoke}"
ITER="${ITER:-200}"
REGIONAL="${REGIONAL:-kalragauri-uds-poc-regional}"
ZONAL="${ZONAL:-kalragauri-uds-poc-zonal}"
POC="$HOME/poc/src/storage-uds-poc"
RUN="${RUN:-$PHASE-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT="$HOME/poc/results/$RUN"
mkdir -p "$OUT"
cd "$OUT"

export GCS_CORE_SIDECAR="$POC/sidecar/target/release/gcs-core-sidecar"
UDS="$POC/cmake-out/bench/ssb_uds"
INPROC="$POC/cmake-out/bench/ssb_inproc"
NATIVE="$POC/cmake-out/bench/ssb_native"
RUST="$POC/sidecar/target/release/gcs-core-bench"
# Scripts can be updated without rebuilding: fetch the current copies.
gcloud storage cp --quiet "gs://$OPS/scripts/verify_path.py" "$POC/scripts/verify_path.py" >/dev/null 2>&1 || true
VERIFY="$POC/scripts/verify_path.py"
SIDECAR_ARGS=(--sidecar-arg=--grpc-subchannels=4)

md() { curl -fsS -H Metadata-Flavor:Google "http://metadata/computeMetadata/v1/instance/$1" | awk -F/ '{print $NF}'; }
{
  echo "host=$(hostname) zone=$(md zone) machine=$(md machine-type) cpus=$(nproc)"
  echo "phase=$PHASE iterations=$ITER regional=$REGIONAL zonal=$ZONAL start=$(date -u +%FT%TZ)"
  echo "kernel=$(uname -r) glibc=$(ldd --version | head -1 | awk '{print $NF}')"
} | tee run-info.txt

FAILED=0
# run NAME BINARY ARGS... : one verified run; CSV is NAME.csv, log NAME.log.
run() {
  local name=$1; shift
  echo "######## $name"
  python3 "$VERIFY" "$name.path.json" -- "$@" --out="$name.csv" >"$name.log" 2>&1
  local rc=$?
  local bad
  bad=$(python3 -c 'import csv,sys; print(sum(1 for r in csv.DictReader(open(sys.argv[1])) if r["ok"]=="0"))' "$name.csv" 2>/dev/null || echo "?")
  echo "  exit=$rc failed_rows=$bad; $(tail -1 "$name.log")"
  (( rc != 0 )) && FAILED=$((FAILED + 1))
  gcloud storage cp --quiet "$name".* "gs://$OPS/results/$RUN/" >/dev/null 2>&1
}

# run_impl PREFIX IMPL ARGS... : one verified run of implementation IMPL
# (uds, uds-ch4, inproc, inproc-ch4, rust, rust-unbuffered, native,
# native-<sync>) named PREFIX-IMPL. The one place that maps an implementation
# to its binary and flags, so every cell runs each implementation the same way.
run_impl() {
  local prefix=$1 impl=$2; shift 2
  case $impl in
    uds)             run "$prefix-$impl" "$UDS" "$@" "${SIDECAR_ARGS[@]}" ;;
    uds-ch4)         run "$prefix-$impl" "$UDS" "$@" "${SIDECAR_ARGS[@]}" --channels=4 --label=ch4 ;;
    inproc)          run "$prefix-$impl" "$INPROC" "$@" "${SIDECAR_ARGS[@]}" ;;
    inproc-ch4)      run "$prefix-$impl" "$INPROC" "$@" "${SIDECAR_ARGS[@]}" --channels=4 --label=ch4 ;;
    rust)            run "$prefix-$impl" "$RUST" "$@" ;;
    rust-unbuffered) run "$prefix-$impl" "$RUST" "$@" --unbuffered --label=unbuffered ;;
    native)          run "$prefix-$impl" "$NATIVE" "$@" --sync=grpc-cfe ;;
    native-*)        run "$prefix-$impl" "$NATIVE" "$@" --sync="${impl#native-}" ;;
    *) echo "run_impl: unknown implementation $impl" >&2; FAILED=$((FAILED + 1)) ;;
  esac
}

# rotate N ITEMS... : prints ITEMS rotated left by N (ordering-bias control).
rotate() { local n=$1; shift; local a=("$@"); local k=$((n % ${#a[@]})); echo "${a[@]:k}" "${a[@]:0:k}"; }

if [[ "$PHASE" == "smoke" ]]; then
  ITER=2
  SIZES=(100000)
  # Default flags first (no overrides), then the configured variants.
  run smoke-default-uds "$UDS" --bucket="$REGIONAL" --iterations=$ITER
  run smoke-default-inproc "$INPROC" --bucket="$REGIONAL" --iterations=$ITER
  run smoke-default-native "$NATIVE" --bucket="$REGIONAL" --iterations=$ITER
  run smoke-default-rust "$RUST" --bucket="$REGIONAL" --iterations=$ITER
elif [[ "$PHASE" == "supplement" ]]; then
  SIZES=(100000000)
else
  SIZES=(100000 2097152 100000000)
fi

# --- Regional · JSON · ReadObject/WriteObject (w1r3) -------------------------
k=0
for size in "${SIZES[@]}"; do
  if [[ "$PHASE" == "supplement" ]]; then
    w1r3_impls=(uds inproc native-json-h1 rust rust-unbuffered)
  elif (( size == 100000000 )); then
    w1r3_impls=(uds inproc native-json-h1 native-json-default native-grpc-cfe rust rust-unbuffered)
  else
    w1r3_impls=(uds inproc native-json-h1 native-json-default native-grpc-cfe rust)
  fi
  for impl in $(rotate $k "${w1r3_impls[@]}"); do
    common=(--bucket="$REGIONAL" --workload=w1r3 --iterations="$ITER" --object-sizes="$size")
    run_impl "w1r3-$size" "$impl" "${common[@]}"
  done
  k=$((k + 1))
done

if [[ "$PHASE" != "supplement" ]]; then
  # --- ListObjects (gRPC; native JSON list as labelled context) --------------
  LIST_ITER=100; [[ $PHASE == smoke ]] && LIST_ITER=2
  for bucket_type in regional zonal; do
    bucket=$REGIONAL; [[ $bucket_type == zonal ]] && bucket=$ZONAL
    for impl in $(rotate $k uds inproc native-grpc-cfe rust native-json-h1); do
      common=(--bucket="$bucket" --bucket-type=$bucket_type --workload=list --iterations="$LIST_ITER")
      run_impl "list-$bucket_type" "$impl" "${common[@]}"
    done
    k=$((k + 1))
  done

  # --- gRPC bidi: Rapid Workload + appendable --------------------------------
  # Regional: BidiReadObject is not enabled for the regional bucket in this
  # project (PERMISSION_DENIED); 1 iteration per impl records the evidence.
  for bucket_type in zonal regional; do
    bucket=$ZONAL; it=$ITER
    if [[ $bucket_type == regional ]]; then bucket=$REGIONAL; it=1; fi
    for w in open random8k read50x2x4 appendable; do
      [[ $bucket_type == regional && $w == appendable ]] && continue
      witer=$it; [[ $w == read50x2x4 && $PHASE == full && $bucket_type == zonal ]] && witer=$(( ITER / 2 ))
      for impl in $(rotate $k uds inproc native rust); do
        common=(--bucket="$bucket" --bucket-type=$bucket_type --workload=$w --iterations="$witer" --object-sizes="$(IFS=,; echo "${SIZES[*]}")")
        run_impl "$w-$bucket_type" "$impl" "${common[@]}"
      done
      k=$((k + 1))
    done
  done
fi

# --- Throughput (concurrent 100 MB transfers) ------------------------------------
TP_ITER="${TP_ITER:-5}"; [[ $PHASE == smoke ]] && TP_ITER=1
for workers in 8 32; do
  [[ $PHASE == smoke && $workers == 32 ]] && continue
  for op in read write; do
    # Regional JSON
    if [[ "$op" == "write" ]]; then
      tp_json_impls=(uds uds-ch4 inproc inproc-ch4 native-json-h1 rust rust-unbuffered)
    else
      tp_json_impls=(uds uds-ch4 inproc inproc-ch4 native-json-h1 rust)
    fi
    for impl in $(rotate $k "${tp_json_impls[@]}"); do
      common=(--bucket="$REGIONAL" --workload=throughput --throughput-op=$op --workers=$workers --iterations=$TP_ITER --object-sizes=100000000)
      run_impl "tp-json-$op-w$workers" "$impl" "${common[@]}"
    done
    # Zonal bidi
    for impl in $(rotate $k uds uds-ch4 inproc inproc-ch4 native rust); do
      common=(--bucket="$ZONAL" --bucket-type=zonal --workload=throughput --throughput-op=$op --workers=$workers --iterations=$TP_ITER --object-sizes=100000000)
      run_impl "tp-bidi-$op-w$workers" "$impl" "${common[@]}"
    done
    k=$((k + 1))
  done
done

if [[ "$PHASE" != "supplement" ]]; then
  # --- Hop floor: synthetic core (no GCS), both boundaries ----------------------
  # Same chunk × channels sweep for the sidecar and the in-process core; the
  # abstract-socket link runs at the default chunk only, as a control for the
  # channel type (descriptor "direct" channel vs. the resolver-based channel
  # `uds` uses).
  SYN_ITER=50; SYN_TP_ITER=10
  [[ $PHASE == smoke ]] && SYN_ITER=2 && SYN_TP_ITER=1
  for chunk in 262144 1048576 4194304; do
    for channels in 1 4; do
      for impl in $(rotate $k uds inproc); do
        bin=$UDS; [[ $impl == inproc ]] && bin=$INPROC
        run "synthetic-w1r3-c$chunk-ch$channels-$impl" "$bin" --bucket=synthetic --workload=w1r3 \
          --iterations=$SYN_ITER --object-sizes=100000,2097152,100000000 --label=synthetic \
          --channels=$channels --chunk-size=$chunk --sidecar-arg=--synthetic --sidecar-arg=--chunk-size=$chunk
        run "synthetic-tp-c$chunk-ch$channels-$impl" "$bin" --bucket=synthetic --workload=throughput \
          --workers=32 --iterations=$SYN_TP_ITER --object-sizes=100000000 --label=synthetic \
          --channels=$channels --chunk-size=$chunk --sidecar-arg=--synthetic --sidecar-arg=--chunk-size=$chunk
      done
      k=$((k + 1))
    done
  done
  for channels in 1 4; do
    run "synthetic-w1r3-c1048576-ch$channels-inproc-abstract" "$INPROC" --bucket=synthetic --workload=w1r3 \
      --iterations=$SYN_ITER --object-sizes=100000,2097152,100000000 --label=synthetic-abstract \
      --channels=$channels --chunk-size=1048576 --sidecar-arg=--synthetic --sidecar-arg=--chunk-size=1048576 \
      --sidecar-arg=--link=abstract
    run "synthetic-tp-c1048576-ch$channels-inproc-abstract" "$INPROC" --bucket=synthetic --workload=throughput \
      --workers=32 --iterations=$SYN_TP_ITER --object-sizes=100000000 --label=synthetic-abstract \
      --channels=$channels --chunk-size=1048576 --sidecar-arg=--synthetic --sidecar-arg=--chunk-size=1048576 \
      --sidecar-arg=--link=abstract
  done
fi

echo "end=$(date -u +%FT%TZ) failed_runs=$FAILED" | tee -a run-info.txt
gcloud storage cp --quiet run-info.txt "gs://$OPS/results/$RUN/" >/dev/null 2>&1
echo "RUN=$RUN FAILED_RUNS=$FAILED"
