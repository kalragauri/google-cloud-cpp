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

# Pins the parent-crash cleanup contract:
# In 1:1 child mode (`--child` -> `SidecarMode::kChild`), `gcs-core-server` is
# spawned from a long-lived helper thread with `--pdeathsig=true --idle-secs=0`.
# Sending `SIGKILL` to the parent process must cause the child `gcs-core-server`
# to exit within <= 1 s (no orphan server left behind).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="${BUILD_DIR:-$POC_DIR/cmake-out}"
COLD_START="${COLD_START:-$BUILD_DIR/cold_start}"
export GCS_CORE_SERVER="${GCS_CORE_SERVER:-$BUILD_DIR/gcs-core-server}"

TMP="$(mktemp -d)"
PARENT_PID=""
CORE_PID=""
cleanup() {
  [[ -n "$PARENT_PID" ]] && kill -9 "$PARENT_PID" 2>/dev/null || true
  [[ -n "$CORE_PID" ]] && kill -9 "$CORE_PID" 2>/dev/null || true
  rm -rf "$TMP"
}
trap cleanup EXIT

OUT="$TMP/cold_start.out"
"$COLD_START" \
  --child \
  --hold-ms=30000 \
  --socket-path="$TMP/child.sock" \
  --sidecar-arg=--endpoint=http://127.0.0.1:1 \
  --sidecar-arg=--shm-size=0 \
  --sidecar-arg=--grpc-subchannels=1 \
  --sidecar-arg=--worker-threads=2 >"$OUT" &
PARENT_PID=$!

for _ in $(seq 1 100); do
  if grep -q '^core_pid=' "$OUT" 2>/dev/null; then
    break
  fi
  sleep 0.05
done

CORE_PID="$(awk -F= '/^core_pid=/{print $2}' "$OUT")"
if [[ -z "$CORE_PID" || "$CORE_PID" -le 0 ]]; then
  echo "FAIL: cold_start did not report core_pid; output:" >&2
  cat "$OUT" >&2
  exit 1
fi
if [[ "$CORE_PID" -eq "$PARENT_PID" ]]; then
  echo "FAIL: expected separate child PID, got parent PID $PARENT_PID" >&2
  exit 1
fi
if ! kill -0 "$CORE_PID" 2>/dev/null; then
  echo "FAIL: child PID $CORE_PID is not alive before parent SIGKILL" >&2
  exit 1
fi

T0_NS="$(date +%s%N)"
kill -9 "$PARENT_PID"
wait "$PARENT_PID" 2>/dev/null || true
PARENT_PID=""

DEADLINE_NS=$((T0_NS + 1000000000))
while kill -0 "$CORE_PID" 2>/dev/null; do
  if (( $(date +%s%N) >= DEADLINE_NS )); then
    echo "FAIL: child gcs-core-server (pid=$CORE_PID) survived > 1 s after parent SIGKILL" >&2
    exit 1
  fi
  sleep 0.01
done
ELAPSED_MS=$(( ($(date +%s%N) - T0_NS) / 1000000 ))
CORE_PID=""
echo "PASS: child gcs-core-server exited in ${ELAPSED_MS} ms (<= 1000 ms) after parent SIGKILL"
