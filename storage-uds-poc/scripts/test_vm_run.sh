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

# Control-flow test for vm_run.sh. Stubs every VM-only dependency (gcloud,
# metadata curl, systemctl, the bench, the quickstart gate, verify_path.py,
# monitoring.py) and asserts, per PHASE, which cells are scheduled and with
# which flags, that the regional BidiReadObject gate aborts on the GAP marker
# and on a failing quickstart, and that the per-phase variable checks reject
# bad input. No network, no GCS, runs in a few seconds.
#
#   bash scripts/test_vm_run.sh
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HARNESS="$(mktemp -d)"
trap 'rm -rf "$HARNESS"' EXIT
POC="$HARNESS/poc/src/storage-uds-poc"
mkdir -p "$POC/scripts" "$POC/cmake-out/bench" "$HARNESS/bin" "$HARNESS/home"
cp "$HERE/vm_run.sh" "$POC/scripts/vm_run.sh"

cat > "$HARNESS/bin/gcloud" <<'EOF'
#!/bin/bash
exit 0
EOF
cat > "$HARNESS/bin/curl" <<'EOF'
#!/bin/bash
case "$*" in
  *project/project-id) echo "stub-project" ;;
  *instance/id) echo "123" ;;
  *instance/zone) echo "projects/1/zones/us-central1-b" ;;
  *machine-type) echo "projects/1/machineTypes/c4-standard-192" ;;
esac
EOF
cat > "$HARNESS/bin/systemctl" <<'EOF'
#!/bin/bash
[[ "$*" == *--quiet* ]] || echo active
exit 0
EOF
cat > "$POC/cmake-out/gcs-core-server" <<'EOF'
#!/bin/bash
exit 0
EOF
# GATE_RESULT selects the quickstart outcome: ok (default), gap (BidiReadObject
# not enabled for the project, exit 0), or fail (quickstart itself failed).
cat > "$POC/cmake-out/quickstart_inproc" <<'EOF'
#!/bin/bash
case "${GATE_RESULT:-ok}" in
  gap) echo "  GAP  BidiReadObject not enabled for this bucket: PERMISSION_DENIED" ;;
  fail) echo "BuildClients failed: UNAVAILABLE"; exit 1 ;;
  *) echo "  ok   AsyncClient::Open: " ;;
esac
echo PASS
EOF
# The bench stub echoes its flags so the test can assert them per cell.
cat > "$POC/cmake-out/bench/uds_poc_bench" <<'EOF'
#!/bin/bash
summary=""
for a in "$@"; do
  case "$a" in
    --summary-json=*) summary="${a#*=}" ;;
  esac
done
[[ -n "$summary" ]] && echo '{}' > "$summary"
echo "SUMMARY $*"
EOF
cat > "$POC/scripts/verify_path.py" <<'EOF'
import subprocess, sys
i = sys.argv.index("--")
sys.exit(subprocess.call(sys.argv[i + 1:]))
EOF
cat > "$POC/scripts/monitoring.py" <<'EOF'
import sys
for a in sys.argv:
    if a.startswith("--out="):
        open(a[len("--out="):], "w").write("{}")
EOF
chmod +x "$HARNESS/bin/"* "$POC/cmake-out/gcs-core-server" \
  "$POC/cmake-out/quickstart_inproc" "$POC/cmake-out/bench/uds_poc_bench"

FAILURES=0
LAST_LOG=""

# run_phase LABEL VAR=VALUE... runs vm_run.sh with the stubs and the given
# environment; the log is kept in $LAST_LOG for the assertions.
run_phase() {
  local label=$1
  shift
  LAST_LOG="$HARNESS/$label.log"
  (
    export PATH="$HARNESS/bin:$PATH" HOME="$HARNESS/home" POC="$POC" \
      OPS="stub-ops" MONITORING_FLUSH_SECS=0 RUN="$label" "$@"
    bash "$POC/scripts/vm_run.sh"
  ) >"$LAST_LOG" 2>&1
  return $?
}

count_cells() {
  python3 - "$LAST_LOG" <<'PY'
import sys
print(sum(1 for l in open(sys.argv[1]) if l.startswith("######## ") and "Querying" not in l))
PY
}

count_flag() {
  python3 - "$LAST_LOG" "$1" <<'PY'
import sys
print(sum(1 for l in open(sys.argv[1]) if l.startswith("  exit=") and sys.argv[2] in l))
PY
}

expect() {
  local label=$1 what=$2 got=$3 want=$4
  if [[ "$got" == "$want" ]]; then
    echo "ok   [$label] $what = $want"
  else
    echo "FAIL [$label] $what: got '$got', want '$want'" >&2
    FAILURES=$((FAILURES + 1))
  fi
}

expect_log() {
  local label=$1 needle=$2
  if [[ "$(cat "$LAST_LOG")" == *"$needle"* ]]; then
    echo "ok   [$label] log contains: $needle"
  else
    echo "FAIL [$label] log lacks: $needle" >&2
    FAILURES=$((FAILURES + 1))
  fi
}

# --- Variable validation -------------------------------------------------------
run_phase wt-zero env PHASE=wt ZONAL=zb WORKER_THREADS=0; rc=$?
expect wt-zero rc "$rc" 1
expect_log wt-zero "requires WORKER_THREADS > 0"
run_phase wt-text env PHASE=wt ZONAL=zb WORKER_THREADS=abc; rc=$?
expect wt-text rc "$rc" 1
expect_log wt-text "non-negative integer"
run_phase wt-space env PHASE=wt ZONAL=zb WORKER_THREADS="1 2"; rc=$?
expect wt-space rc "$rc" 1
run_phase flush-text env PHASE=wt ZONAL=zb WORKER_THREADS=16 MONITORING_FLUSH_SECS=abc; rc=$?
expect flush-text rc "$rc" 1
expect_log flush-text "MONITORING_FLUSH_SECS must be"
run_phase bad-phase env PHASE=bogus; rc=$?
expect bad-phase rc "$rc" 1
expect_log bad-phase "unknown PHASE=bogus"
run_phase probes-missing env PHASE=probes REGIONAL=rb; rc=$?
expect probes-missing rc "$rc" 1
expect_log probes-missing "PHASE=probes requires REGIONAL and ZONAL"
run_phase full-missing env PHASE=full REGIONAL=rb ZONAL=zb; rc=$?
expect full-missing rc "$rc" 1
expect_log full-missing "requires REGIONAL, ZONAL, and BUCKET_PREFIX"

# --- Phase cell counts and flags -------------------------------------------------
run_phase wt16 env PHASE=wt ZONAL=zb WORKER_THREADS=16; rc=$?
expect wt16 rc "$rc" 0
expect wt16 cells "$(count_cells)" 4
expect wt16 "cells with --worker-threads=16" "$(count_flag '--worker-threads=16')" 4
expect wt16 "native cells" "$(count_flag '--mode=native')" 0
expect wt16 "W4 cells at 300 s with 48 threads" \
  "$(count_flag '--workload=bidi-read-48t --bucket=zb --bucket-type=zonal --threads=48 --duration-secs=300')" 2
expect wt16 "W3 cells at 180 s" \
  "$(count_flag '--workload=random8k --bucket=zb --bucket-type=zonal --duration-secs=180')" 2

run_phase regional-ok env PHASE=regional REGIONAL=rb; rc=$?
expect regional-ok rc "$rc" 0
expect_log regional-ok "gate: BidiReadObject admitted on gs://rb"
expect regional-ok cells "$(count_cells)" 15
expect regional-ok "regional cells" "$(count_flag '--bucket-type=regional')" 15
expect regional-ok "default worker threads" "$(count_flag '--worker-threads=0')" 15

run_phase full env PHASE=full REGIONAL=rb ZONAL=zb BUCKET_PREFIX=p-; rc=$?
expect full rc "$rc" 0
expect full cells "$(count_cells)" 24
expect full "zonal W1-W5 cells (6 workloads x 3 comparators)" "$(count_flag '--bucket-type=zonal')" 18
expect full "list-buckets cells" "$(count_flag '--bucket-prefix=p-')" 3
# The wt cells must be flag-identical to their base cells (same loop).
expect full "W4 base cells" \
  "$(count_flag '--workload=bidi-read-48t --bucket=zb --bucket-type=zonal --threads=48 --duration-secs=300')" 3

run_phase full-allow env PHASE=full REGIONAL=rb ZONAL=zb BUCKET_PREFIX=p- ALLOW_REGIONAL_BIDI=1; rc=$?
expect full-allow rc "$rc" 0
expect full-allow cells "$(count_cells)" 39

run_phase smoke env PHASE=smoke REGIONAL=rb ZONAL=zb BUCKET_PREFIX=p-; rc=$?
expect smoke rc "$rc" 0
expect smoke cells "$(count_cells)" 24
expect smoke "short durations" "$(count_flag '--duration-secs=3')" 18

# --- WORKLOADS filter ------------------------------------------------------------
# mode_order CELL prints the comparator order of CELL's cells in $LAST_LOG.
mode_order() {
  python3 - "$LAST_LOG" "$1" <<'PY'
import sys
cell = sys.argv[2]
modes = []
for l in open(sys.argv[1]):
    if l.startswith("######## " + cell + "-"):
        modes.append(l.split()[1][len(cell) + 1:])
print(",".join(modes))
PY
}
PARITY=read50x2x4-serial,read50x2x4-concurrent,bidi-read-48t
run_phase full env PHASE=full REGIONAL=rb ZONAL=zb BUCKET_PREFIX=p-
FULL_W2A_ORDER="$(mode_order read50x2x4-serial-zonal)"
FULL_W4_ORDER="$(mode_order bidi-read-48t-zonal)"
run_phase full-filtered env PHASE=full REGIONAL=rb ZONAL=zb BUCKET_PREFIX=p- WORKLOADS="$PARITY"; rc=$?
expect full-filtered rc "$rc" 0
expect full-filtered cells "$(count_cells)" 9
expect full-filtered "W5/W6 cells" "$(count_flag '--workload=bidi-write-48t')$(count_flag '--workload=list-')" 00
expect full-filtered "W1/W3 cells" "$(count_flag '--workload=open ')$(count_flag '--workload=random8k')" 00
# Skipped workloads still advance the rotation, so each re-run cell keeps the
# comparator order it had in the full matrix.
expect full-filtered "W2a comparator order" "$(mode_order read50x2x4-serial-zonal)" "$FULL_W2A_ORDER"
expect full-filtered "W4 comparator order" "$(mode_order bidi-read-48t-zonal)" "$FULL_W4_ORDER"
expect_log full-filtered "workloads=$PARITY"
run_phase regional-filtered env PHASE=regional REGIONAL=rb WORKLOADS="$PARITY"; rc=$?
expect regional-filtered rc "$rc" 0
expect regional-filtered cells "$(count_cells)" 9
expect regional-filtered "regional cells" "$(count_flag '--bucket-type=regional')" 9
run_phase filter-unknown env PHASE=full REGIONAL=rb ZONAL=zb BUCKET_PREFIX=p- WORKLOADS=open,bogus; rc=$?
expect filter-unknown rc "$rc" 1
expect filter-unknown cells "$(count_cells)" 0
expect_log filter-unknown "unknown workload 'bogus'"
run_phase filter-empty-item env PHASE=full REGIONAL=rb ZONAL=zb BUCKET_PREFIX=p- WORKLOADS=open,,random8k; rc=$?
expect filter-empty-item rc "$rc" 1

# --- Regional gate ---------------------------------------------------------------
run_phase regional-gap env PHASE=regional REGIONAL=rb GATE_RESULT=gap; rc=$?
expect regional-gap rc "$rc" 1
expect regional-gap cells "$(count_cells)" 0
expect_log regional-gap "BidiReadObject not admitted on gs://rb (PERMISSION_DENIED)"
expect_log regional-gap "gate=FAILED"
run_phase regional-fail env PHASE=regional REGIONAL=rb GATE_RESULT=fail; rc=$?
expect regional-fail rc "$rc" 1
expect_log regional-fail "gate quickstart failed (exit 1)"
run_phase full-allow-gap env PHASE=full REGIONAL=rb ZONAL=zb BUCKET_PREFIX=p- ALLOW_REGIONAL_BIDI=1 GATE_RESULT=gap; rc=$?
expect full-allow-gap rc "$rc" 1
expect full-allow-gap cells "$(count_cells)" 0

if (( FAILURES > 0 )); then
  echo "test_vm_run: $FAILURES failure(s)" >&2
  exit 1
fi
echo "test_vm_run: all checks passed"
