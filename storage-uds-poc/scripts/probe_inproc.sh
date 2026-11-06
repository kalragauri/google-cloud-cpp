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

# VM job: packaging, build-time, cold-start, and customer-environment deployment
# probes:
#   1. Raw & stripped binary and archive sizes + shared-library (`ldd`) diff.
#   2. Incremental one-file rebuild wall times (C++ TU, Rust `service.rs`,
#      `gcscore.proto`).
#   3. Cold `Client::Create()` latency (n=30) and `VmRSS`/`VmHWM`/`Threads`
#      footprint across daemon, child (`--child`), and in-process modes.
#   4. Customer-environment deployment probes: `noexec` tmpfs, read-only
#      filesystem (filesystem UDS vs `XDG_RUNTIME_DIR` vs `unix-abstract:`
#      in-process), container PID-1 namespace (`unshare --pid`), >107-byte
#      socket path (B8), stale socket / regular file (B7), parent `SIGKILL`
#      orphan cleanup (B4), and cross-UID `SO_PEERCRED` rejection (B9).
set -uo pipefail

export PATH="$HOME/.cargo/bin:$PATH"
OPS="${OPS:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC="${POC:-$(dirname "$SCRIPT_DIR")}"
BUILD="${BUILD:-$POC/cmake-out}"
RUN="${RUN:-packaging-probes-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT="${OUT:-$HOME/poc/results/$RUN}"
mkdir -p "$OUT"

RUST_DIR="$HOME/poc/rust"
[[ ! -d "$RUST_DIR" ]] && RUST_DIR="$BUILD/_deps/google_cloud_rust-src"
export PROTOC="$BUILD/vcpkg_installed/x64-linux/tools/protobuf/protoc"
export GCS_CORE_SERVER="$BUILD/gcs-core-server"

TMP="$(mktemp -d)"
cleanup() {
  sudo -n umount "$TMP/noexec" 2>/dev/null || true
  sudo -n umount "$TMP/ro_mnt" 2>/dev/null || true
  pkill -x gcs-core-server 2>/dev/null || true
  rm -rf "$TMP"
}
trap cleanup EXIT

echo "=== 1. Binary and archive sizes (bytes; stripped = strip -s) ==="
for f in "$BUILD/gcs-core-server" \
         "$BUILD/quickstart" "$BUILD/quickstart_inproc" \
         "$BUILD/cold_start" "$BUILD/cold_start_inproc" \
         "$BUILD/bench/uds_poc_bench"; do
  [[ -f "$f" ]] || continue
  name="$(basename "$f")"
  raw="$(stat -c '%s' "$f")"
  cp "$f" "$TMP/$name" && strip -s "$TMP/$name"
  stripped="$(stat -c '%s' "$TMP/$name")"
  printf "%-22s raw=%12d (%7.2f MiB)  stripped=%12d (%7.2f MiB)\n" \
    "$name" "$raw" "$(awk "BEGIN{print $raw/1048576}")" \
    "$stripped" "$(awk "BEGIN{print $stripped/1048576}")"
done

for lib in "$BUILD/cargo-target/release/libstorage_grpc_uds.a" \
           "$BUILD/libgcs_core.a" "$BUILD/libgcs_core_inproc.a" \
           "$BUILD/libgcs_core_storage.a" "$BUILD/libgcs_core_storage_inproc.a" \
           "$BUILD/libgcs_core_protos.a"; do
  [[ -f "$lib" ]] || continue
  raw="$(stat -c '%s' "$lib")"
  printf "%-30s raw=%12d (%7.2f MiB)\n" \
    "$(basename "$lib")" "$raw" "$(awk "BEGIN{print $raw/1048576}")"
done

echo "--- ldd diff: quickstart vs quickstart_inproc ---"
diff <(ldd "$BUILD/quickstart" | awk '{print $1}' | sort) \
     <(ldd "$BUILD/quickstart_inproc" | awk '{print $1}' | sort) \
  && echo "same shared libraries"

echo ""
echo "=== 2. Cold Client::Create() in a fresh process (n=30) & footprint ==="
pkill -x gcs-core-server 2>/dev/null || true
python3 - "$BUILD/cold_start" "$BUILD/cold_start_inproc" "$TMP" <<'PY'
import os
import subprocess
import sys

cold_sidecar, cold_inproc, tmp = sys.argv[1], sys.argv[2], sys.argv[3]
base_args = [
    "--sidecar-arg=--endpoint=http://127.0.0.1:1",
    "--sidecar-arg=--shm-size=0",
    "--sidecar-arg=--grpc-subchannels=4",
]
configs = [
    ("sidecar-child-default", cold_sidecar, ["--child", "--channels=4", *base_args]),
    ("sidecar-child-wt8", cold_sidecar, ["--child", "--channels=4", *base_args, "--sidecar-arg=--worker-threads=8"]),
    ("sidecar-daemon-warm", cold_sidecar, [f"--socket-path={tmp}/warm-daemon.sock", "--channels=4", *base_args]),
    ("inproc-default", cold_inproc, ["--channels=4", *base_args]),
    ("inproc-wt8", cold_inproc, ["--channels=4", *base_args, "--sidecar-arg=--worker-threads=8"]),
]

for label, binary, args in configs:
    if label == "sidecar-daemon-warm":
        # Prime the daemon once so the 30 samples measure warm attach latency;
        # cold spawn is already measured by sidecar-child-*.
        subprocess.run([binary, *args], capture_output=True, text=True, check=True)
    times, footprint = [], None
    for i in range(30):
        p = subprocess.run([binary, *args], capture_output=True, text=True, timeout=60)
        if p.returncode != 0:
            print(f"[{label}] iter {i} failed: {p.stderr.strip()[:200]}")
            break
        lines = p.stdout.splitlines()
        times.append(float(lines[0].split("=", 1)[1]))
        if footprint is None:
            footprint = " | ".join(line.strip() for line in lines[1:])
    if not times:
        continue
    times.sort()
    q = lambda f: times[min(len(times) - 1, int(f * (len(times) - 1) + 0.5))]
    print(
        f"[{label}] create_ms: n={len(times)} min={times[0]:.2f} p50={q(0.5):.2f} "
        f"p90={q(0.9):.2f} p99={q(0.99):.2f} max={times[-1]:.2f}"
    )
    print(f"[{label}] {footprint}")
PY
pkill -x gcs-core-server 2>/dev/null || true

echo ""
echo "=== 3. Deployment environment probes ==="
HERMETIC_ARGS=(
  --sidecar-arg=--endpoint=http://127.0.0.1:1
  --sidecar-arg=--shm-size=0
  --sidecar-arg=--grpc-subchannels=1
  --sidecar-arg=--worker-threads=2
)

echo "--- 3a. UDS path > 107 bytes ---"
LONG_SOCK="$TMP/$(python3 - <<'PY'
print("a" * 115 + ".sock")
PY
)"
if "$BUILD/cold_start" --child --socket-path="$LONG_SOCK" "${HERMETIC_ARGS[@]}" 2>"$TMP/long.err"; then
  echo "FAIL: >107B socket unexpectedly succeeded"
else
  echo "PASS (>107B rejected): $(cat "$TMP/long.err")"
fi

echo "--- 3b. Stale socket & regular non-socket file at socket path ---"
python3 - "$TMP/stale.sock" <<'PY'
import socket, sys
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.bind(sys.argv[1])
s.close()
PY
"$BUILD/cold_start" --child --socket-path="$TMP/stale.sock" "${HERMETIC_ARGS[@]}" >/dev/null \
  && echo "PASS: stale socket unlinked and replaced cleanly"
echo "not-a-socket" >"$TMP/regular.file"
if "$BUILD/cold_start" --child --socket-path="$TMP/regular.file" "${HERMETIC_ARGS[@]}" 2>"$TMP/reg.err"; then
  echo "FAIL: regular file unexpectedly overwritten"
else
  echo "PASS (regular file preserved): $(head -n 1 "$TMP/reg.err")"
fi

echo "--- 3c. Parent SIGKILL orphan cleanup & SO_PEERCRED ---"
bash "$POC/scripts/test_parent_crash.sh"
bash "$POC/scripts/test_peer_cred.sh"

if sudo -n true 2>/dev/null; then
  echo "--- 3d. noexec /tmp mount probe ---"
  mkdir -p "$TMP/noexec"
  sudo -n mount -t tmpfs -o size=64m,noexec,mode=1777 tmpfs "$TMP/noexec"
  cp "$BUILD/gcs-core-server" "$TMP/noexec/gcs-core-server"
  chmod 0755 "$TMP/noexec/gcs-core-server"
  if GCS_CORE_SERVER="$TMP/noexec/gcs-core-server" \
      "$BUILD/cold_start" --child --socket-path="$TMP/noexec/fail.sock" "${HERMETIC_ARGS[@]}" 2>"$TMP/noexec.err"; then
    echo "FAIL: binary on noexec tmpfs unexpectedly executed"
  else
    echo "PASS (binary on noexec rejected): $(head -n 1 "$TMP/noexec.err")"
  fi
  "$BUILD/cold_start" --child --socket-path="$TMP/noexec/ok.sock" "${HERMETIC_ARGS[@]}" >/dev/null \
    && echo "PASS: socket on noexec tmpfs works when binary is on exec mount"
  sudo -n umount "$TMP/noexec"

  echo "--- 3e. Read-only filesystem probe (sidecar vs unix-abstract inproc) ---"
  mkdir -p "$TMP/ro_src" "$TMP/ro_mnt"
  chmod 0777 "$TMP/ro_src"
  sudo -n mount --bind "$TMP/ro_src" "$TMP/ro_mnt"
  sudo -n mount -o remount,ro,bind "$TMP/ro_mnt"
  if "$BUILD/cold_start" --child --socket-path="$TMP/ro_mnt/ro.sock" "${HERMETIC_ARGS[@]}" 2>"$TMP/ro.err"; then
    echo "FAIL: filesystem socket on read-only mount unexpectedly succeeded"
  else
    echo "PASS (sidecar on RO FS without writable runtime dir fails cleanly): $(head -n 1 "$TMP/ro.err")"
  fi
  XDG_RUNTIME_DIR="$TMP/ro_mnt" "$BUILD/cold_start_inproc" "${HERMETIC_ARGS[@]}" >/dev/null \
    && echo "PASS: in-process variant (unix-abstract:) succeeds with read-only XDG_RUNTIME_DIR"
  sudo -n umount "$TMP/ro_mnt"

  echo "--- 3f. Container PID-1 namespace probe (unshare --pid --fork) ---"
  sudo -n unshare --pid --fork --mount-proc \
    su - "$(id -un)" -c "GCS_CORE_SERVER='$BUILD/gcs-core-server' '$BUILD/cold_start' --child --socket-path='$TMP/pid1.sock' ${HERMETIC_ARGS[*]}" \
    | head -n 2 && echo "PASS: child sidecar works inside PID namespace"
else
  echo "SKIP 3d-3f: passwordless sudo unavailable on this host"
fi

echo ""
echo "=== 4. Incremental 1-file rebuild times (cpus=$(nproc)) ==="
measure() {
  local label=$1
  shift
  local t0 t1
  t0=$(date +%s.%N)
  "$@" >/dev/null 2>&1
  t1=$(date +%s.%N)
  awk "BEGIN{printf \"%s=%.3f s\n\", \"$label\", $t1 - $t0}"
}
touch_probe() {
  cp "$1" "$TMP/$(basename "$1").bak"
  echo "// incremental build probe" >>"$1"
}
restore_probe() {
  mv "$TMP/$(basename "$1").bak" "$1"
}

cmake --build "$BUILD" -j"$(nproc)" --target quickstart quickstart_inproc >/dev/null 2>&1

touch_probe "$POC/cpp/src/client.cc"
measure "cpp_client_cc_to_quickstart_sidecar" cmake --build "$BUILD" -j"$(nproc)" --target quickstart
restore_probe "$POC/cpp/src/client.cc"
cmake --build "$BUILD" -j"$(nproc)" --target quickstart quickstart_inproc >/dev/null 2>&1

touch_probe "$POC/cpp/src/client.cc"
measure "cpp_client_cc_to_quickstart_inproc" cmake --build "$BUILD" -j"$(nproc)" --target quickstart_inproc
restore_probe "$POC/cpp/src/client.cc"
cmake --build "$BUILD" -j"$(nproc)" --target quickstart quickstart_inproc >/dev/null 2>&1

if [[ -f "$RUST_DIR/src/storage/grpc-uds/src/service.rs" ]]; then
  touch_probe "$RUST_DIR/src/storage/grpc-uds/src/service.rs"
  measure "rust_service_rs_to_quickstart_sidecar" cmake --build "$BUILD" -j"$(nproc)" --target gcs_core_rust quickstart
  restore_probe "$RUST_DIR/src/storage/grpc-uds/src/service.rs"
  cmake --build "$BUILD" -j"$(nproc)" --target quickstart quickstart_inproc >/dev/null 2>&1

  touch_probe "$RUST_DIR/src/storage/grpc-uds/src/service.rs"
  measure "rust_service_rs_to_quickstart_inproc" cmake --build "$BUILD" -j"$(nproc)" --target quickstart_inproc
  restore_probe "$RUST_DIR/src/storage/grpc-uds/src/service.rs"
  cmake --build "$BUILD" -j"$(nproc)" --target quickstart quickstart_inproc >/dev/null 2>&1

  touch_probe "$RUST_DIR/src/storage/grpc-uds/proto/gcscore.proto"
  measure "proto_gcscore_to_quickstart_sidecar_and_inproc" cmake --build "$BUILD" -j"$(nproc)" --target gcs_core_rust quickstart quickstart_inproc
  restore_probe "$RUST_DIR/src/storage/grpc-uds/proto/gcscore.proto"
  cmake --build "$BUILD" -j"$(nproc)" --target quickstart quickstart_inproc >/dev/null 2>&1
fi

echo "PROBES_DONE run=$RUN"
if [[ -n "$OPS" ]]; then
  gcloud storage cp --quiet "$OUT"/* "gs://$OPS/results/$RUN/" >/dev/null 2>&1 || true
fi
