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

# VM job: packaging and lifecycle probes for both boundaries, measured the
# same way (plan §6.4): stripped binary sizes, one-file rebuild times, and the
# cold start of `Client::Create()` in a fresh process with the footprint right
# after. Output goes to stdout (the job runner uploads the log).
#
# It rebuilds the benchmark binaries: run it only as its own job, never while
# vm_run.sh is measuring (the job runner executes jobs one at a time).
#   OPS   ops bucket; results are also copied to gs://$OPS/results/$RUN/
set -uo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
OPS="${OPS:-kalragauri-uds-poc-ops}"
POC="$HOME/poc/src/storage-uds-poc"
BUILD="$POC/cmake-out"
RUN="${RUN:-inproc-probes-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT="$HOME/poc/results/$RUN"
mkdir -p "$OUT"
export PROTOC="$BUILD/vcpkg_installed/x64-linux/tools/protobuf/protoc"
export GCS_CORE_SIDECAR="$POC/sidecar/target/release/gcs-core-sidecar"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "=== 1. Binary and archive sizes (bytes; stripped = strip -s) ==="
for f in "$POC/sidecar/target/release/gcs-core-sidecar" "$POC/sidecar/target/release/gcs-core-bench" \
         "$BUILD/bench/ssb_uds" "$BUILD/bench/ssb_inproc" "$BUILD/bench/ssb_native" \
         "$BUILD/quickstart" "$BUILD/quickstart_inproc" "$BUILD/cold_start" "$BUILD/cold_start_inproc"; do
  name="$(basename "$f")"
  raw="$(stat -c '%s' "$f")"
  cp "$f" "$TMP/$name" && strip -s "$TMP/$name"
  stripped="$(stat -c '%s' "$TMP/$name")"
  printf "%-22s raw=%12d (%7.2f MiB)  stripped=%12d (%7.2f MiB)\n" "$name" "$raw" \
    "$(awk "BEGIN{print $raw/1048576}")" "$stripped" "$(awk "BEGIN{print $stripped/1048576}")"
done
for lib in "$POC/sidecar/target/release/libgcs_core.a" "$BUILD/libgcs_core.a" "$BUILD/libgcs_core_inproc.a" "$BUILD/libgcs_core_protos.a"; do
  [[ -f "$lib" ]] || continue
  raw="$(stat -c '%s' "$lib")"
  printf "%-22s raw=%12d (%7.2f MiB)\n" "$(basename "$lib")" "$raw" "$(awk "BEGIN{print $raw/1048576}")"
done
echo "--- ldd ssb_inproc (the in-process binary must not need anything the uds one does not) ---"
diff <(ldd "$BUILD/bench/ssb_uds" | awk '{print $1}' | sort) <(ldd "$BUILD/bench/ssb_inproc" | awk '{print $1}' | sort) && echo "same shared libraries"

echo ""
echo "=== 2. One-file rebuild times (1-line comment touch; cpus=$(nproc)) ==="
measure() { # measure LABEL CMD... : wall seconds of CMD
  local label=$1; shift
  local t0 t1; t0=$(date +%s.%N); "$@" >/dev/null 2>&1; t1=$(date +%s.%N)
  awk "BEGIN{printf \"%s=%.3f s\n\", \"$label\", $t1 - $t0}"
}
touch_probe() { # touch_probe FILE : appends a comment line, restores on return via RESTORE
  cp "$1" "$TMP/$(basename "$1").bak"; echo "// incremental build probe" >> "$1"
}
restore() { mv "$TMP/$(basename "$1").bak" "$1"; }
cmake --build "$BUILD" -j"$(nproc)" --target ssb_uds ssb_inproc >/dev/null 2>&1

touch_probe "$POC/cpp/src/client.cc"
measure "cpp_client_cc_to_ssb_uds" cmake --build "$BUILD" -j"$(nproc)" --target ssb_uds
measure "cpp_client_cc_to_ssb_inproc" cmake --build "$BUILD" -j"$(nproc)" --target ssb_inproc
restore "$POC/cpp/src/client.cc"
cmake --build "$BUILD" -j"$(nproc)" --target ssb_uds ssb_inproc >/dev/null 2>&1

# Each Rust-side measurement starts from a fully built tree with one file
# touched, so neither variant reuses compilation the other just did (the lib
# target builds rlib and staticlib together; measuring one variant right after
# the other would time little more than a link).
rebuild_all() {
  (cd "$POC/sidecar" && cargo build --release >/dev/null 2>&1)
  cmake --build "$BUILD" -j"$(nproc)" --target ssb_uds ssb_inproc >/dev/null 2>&1
}
for variant in sidecar inproc; do
  touch_probe "$POC/sidecar/src/service.rs"
  if [[ $variant == sidecar ]]; then
    measure "rust_service_rs_to_sidecar_binary" bash -c "cd '$POC/sidecar' && cargo build --release --bin gcs-core-sidecar"
  else
    measure "rust_service_rs_to_ssb_inproc" cmake --build "$BUILD" -j"$(nproc)" --target ssb_inproc
  fi
  restore "$POC/sidecar/src/service.rs"
  rebuild_all
done
for variant in sidecar inproc; do
  touch_probe "$POC/proto/gcs/core/v0/storage_core.proto"
  if [[ $variant == sidecar ]]; then
    measure "proto_to_ssb_uds_and_sidecar" bash -c "cmake --build '$BUILD' -j$(nproc) --target ssb_uds && cd '$POC/sidecar' && cargo build --release --bin gcs-core-sidecar"
  else
    measure "proto_to_ssb_inproc" cmake --build "$BUILD" -j"$(nproc)" --target ssb_inproc
  fi
  restore "$POC/proto/gcs/core/v0/storage_core.proto"
  rebuild_all
done

echo ""
echo "=== 3. Cold Client::Create() in a fresh process, n=30 per row, and footprint after ==="
python3 - "$BUILD/cold_start" "$BUILD/cold_start_inproc" << 'PY'
import subprocess, sys, statistics
uds, inproc = sys.argv[1], sys.argv[2]
CONFIGS = [
    ("synthetic-default", ["--sidecar-arg=--synthetic"]),
    ("synthetic-wt8", ["--sidecar-arg=--synthetic", "--sidecar-arg=--worker-threads=8"]),
    ("cloudpath-ch4-default", ["--sidecar-arg=--grpc-subchannels=4"]),
    ("cloudpath-ch4-wt8", ["--sidecar-arg=--grpc-subchannels=4", "--sidecar-arg=--worker-threads=8"]),
]
for variant, binary in (("uds", uds), ("inproc", inproc)):
    for label, args in CONFIGS:
        times, footprint = [], None
        for i in range(30):
            p = subprocess.run([binary, *args], capture_output=True, text=True, timeout=60)
            if p.returncode != 0:
                print(f"[{variant}/{label}] iteration {i} failed: {p.stderr.strip()[:200]}")
                break
            lines = p.stdout.splitlines()
            times.append(float(lines[0].split("=", 1)[1]))
            if footprint is None:
                footprint = " | ".join(l.strip() for l in lines[1:])
        if not times:
            continue
        times.sort()
        q = lambda f: times[min(len(times) - 1, int(f * (len(times) - 1) + 0.5))]
        print(f"[{variant}/{label}] create_ms: n={len(times)} min={times[0]:.2f} p50={q(0.5):.2f} "
              f"p90={q(0.9):.2f} p99={q(0.99):.2f} max={times[-1]:.2f}")
        print(f"[{variant}/{label}] {footprint}")
PY

echo ""
echo "=== 4. Unit tests on the VM ==="
(cd "$POC/sidecar" && cargo test --release 2>&1 | grep -E "^test result|FAILED") || true
"$BUILD/gcs_core_test" 2>&1 | tail -3
"$BUILD/gcs_core_inproc_test" 2>&1 | tail -3

echo "RUN=$RUN"
gcloud storage cp --quiet "$OUT"/* "gs://$OPS/results/$RUN/" >/dev/null 2>&1 || true
