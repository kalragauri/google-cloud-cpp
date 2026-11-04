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

# Pins input-matrix row B9 (plan rev 4 §8):
# 1. `gcs-core-server` binds its Unix domain socket with mode `0600`.
# 2. Even if filesystem mode bits are bypassed (e.g. `chmod 0666` or an abstract
#    socket), a client running under another UID (`nobody`) is rejected by the
#    server's `SO_PEERCRED` interceptor with `PERMISSION_DENIED`.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="${BUILD_DIR:-$POC_DIR/cmake-out}"
COLD_START="${COLD_START:-$BUILD_DIR/cold_start}"
SERVER="${GCS_CORE_SERVER:-$BUILD_DIR/gcs-core-server}"
REQUIRE_SUDO="${REQUIRE_SUDO:-0}"

TMP="$(mktemp -d)"
chmod 0755 "$TMP"
SRV_PID=""
cleanup() {
  [[ -n "$SRV_PID" ]] && kill -9 "$SRV_PID" 2>/dev/null || true
  rm -rf "$TMP"
}
trap cleanup EXIT

SOCK="$TMP/peer.sock"
SRV_OUT="$TMP/srv.out"
"$SERVER" \
  --socket="$SOCK" \
  --idle-secs=60 \
  --endpoint=http://127.0.0.1:1 \
  --shm-size=0 \
  --grpc-subchannels=1 \
  --worker-threads=2 >"$SRV_OUT" 2>&1 &
SRV_PID=$!

for _ in $(seq 1 100); do
  if grep -q '^READY ' "$SRV_OUT" 2>/dev/null; then
    break
  fi
  sleep 0.05
done

if ! grep -q '^READY ' "$SRV_OUT"; then
  echo "FAIL: gcs-core-server did not become READY; log:" >&2
  cat "$SRV_OUT" >&2
  exit 1
fi

MODE="$(stat -c '%a' "$SOCK")"
if [[ "$MODE" != "600" ]]; then
  echo "FAIL: expected socket mode 600, got $MODE" >&2
  exit 1
fi

if ! sudo -n -u nobody true 2>/dev/null; then
  if [[ "$REQUIRE_SUDO" == "1" ]]; then
    echo "FAIL: REQUIRE_SUDO=1 set, but sudo -n -u nobody failed" >&2
    exit 1
  fi
  echo "SKIP_CROSS_UID: socket mode verified ($MODE); passwordless sudo unavailable on this host"
  exit 0
fi

# Layer 1: 0600 filesystem permissions reject `connect()` from UID `nobody`.
if sudo -n -u nobody python3 - "$SOCK" <<'PY' 2>"$TMP/connect.err"
import socket, sys
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.connect(sys.argv[1])
PY
then
  echo "FAIL: UID nobody unexpectedly connected to 0600 socket" >&2
  exit 1
fi

# Layer 2: relax filesystem bits to 0666 so `connect()` reaches gRPC, and prove
# `SO_PEERCRED` rejects UID `nobody` with PERMISSION_DENIED. Copy the binaries
# into $TMP (0755) so `nobody` does not need traverse permission on $HOME.
chmod 0666 "$SOCK"
cp "$COLD_START" "$TMP/cold_start"
cp "$SERVER" "$TMP/gcs-core-server"
chmod 0755 "$TMP/cold_start" "$TMP/gcs-core-server"
set +e
ERR_MSG="$(sudo -n -u nobody env GCS_CORE_SERVER="$TMP/gcs-core-server" "$TMP/cold_start" --socket-path="$SOCK" 2>&1)"
RC=$?
set -e
if [[ "$RC" -eq 0 ]]; then
  echo "FAIL: UID nobody succeeded against gcs-core-server; output: $ERR_MSG" >&2
  exit 1
fi
if [[ "$ERR_MSG" != *"PERMISSION_DENIED"* ]]; then
  echo "FAIL: expected PERMISSION_DENIED from SO_PEERCRED, got (rc=$RC): $ERR_MSG" >&2
  exit 1
fi
echo "PASS: mode 0600 + SO_PEERCRED rejected cross-UID client with PERMISSION_DENIED"
