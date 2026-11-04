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

# VM job: install toolchains, unpack sources, build the shared Rust server and
# C++ adapters/benchmarks, and run hermetic unit & lifecycle tests.
#   OPS       ops bucket (set by poc-runner)
#   SRC_TGZ   GCS object path of the google-cloud-cpp source tarball
#   RUST_TGZ  optional GCS object path of a google-cloud-rust tarball; when
#             unset, CMake FetchContent clones GCS_CORE_RUST_REPO at
#             GCS_CORE_RUST_REF (plan rev 4 §3.4).
set -euxo pipefail
OPS="${OPS:?OPS bucket must be set}"
SRC_TGZ="${SRC_TGZ:-src/google-cloud-cpp-uds-poc.tgz}"
RUST_TGZ="${RUST_TGZ:-}"
ROOT="$HOME/poc"
mkdir -p "$ROOT"

# --- toolchains (idempotent) ---------------------------------------------------
sudo apt-get update -y
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  build-essential cmake ninja-build git curl zip unzip tar pkg-config \
  python3 linux-libc-dev iproute2 time ca-certificates
if [[ ! -x "$HOME/.cargo/bin/cargo" ]]; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup-init.sh
  sh /tmp/rustup-init.sh -y --profile minimal --default-toolchain stable --no-modify-path
fi
export PATH="$HOME/.cargo/bin:$PATH"
if [[ ! -d "$HOME/vcpkg" ]]; then
  git clone --quiet https://github.com/microsoft/vcpkg.git "$HOME/vcpkg"
  "$HOME/vcpkg/bootstrap-vcpkg.sh" -disableMetrics
fi

# --- sources -------------------------------------------------------------------
rm -rf "$ROOT/src" && mkdir -p "$ROOT/src"
gcloud storage cp "gs://$OPS/$SRC_TGZ" "$ROOT/src.tgz"
tar -C "$ROOT/src" -xzf "$ROOT/src.tgz"
POC="$ROOT/src/storage-uds-poc"

CMAKE_EXTRA_ARGS=()
if [[ -n "$RUST_TGZ" ]]; then
  rm -rf "$ROOT/rust" && mkdir -p "$ROOT/rust"
  gcloud storage cp "gs://$OPS/$RUST_TGZ" "$ROOT/rust.tgz"
  tar -C "$ROOT/rust" -xzf "$ROOT/rust.tgz"
  CMAKE_EXTRA_ARGS+=("-DFETCHCONTENT_SOURCE_DIR_GOOGLE_CLOUD_RUST=$ROOT/rust")
fi
if [[ -n "${GCS_CORE_RUST_REPO:-}" ]]; then
  CMAKE_EXTRA_ARGS+=("-DGCS_CORE_RUST_REPO=${GCS_CORE_RUST_REPO}")
fi
if [[ -n "${GCS_CORE_RUST_REF:-}" ]]; then
  CMAKE_EXTRA_ARGS+=("-DGCS_CORE_RUST_REF=${GCS_CORE_RUST_REF}")
fi

# --- configure & build ---------------------------------------------------------
/usr/bin/time -f 'cmake configure wall=%es' cmake -S "$POC" -B "$POC/cmake-out" -GNinja \
  -DCMAKE_BUILD_TYPE=Release \
  -DGCS_UDS_POC_BUILD_BENCH=ON \
  -DCMAKE_TOOLCHAIN_FILE="$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake" \
  "${CMAKE_EXTRA_ARGS[@]}"

/usr/bin/time -f 'cmake+cargo build wall=%es' cmake --build "$POC/cmake-out" -j"$(nproc)"

# --- hermetic unit, Python, and lifecycle tests --------------------------------
RUST_DIR="$ROOT/rust"
if [[ ! -d "$RUST_DIR" ]]; then
  RUST_DIR="$POC/cmake-out/_deps/google_cloud_rust-src"
fi
export PROTOC="$POC/cmake-out/vcpkg_installed/x64-linux/tools/protobuf/protoc"
(cd "$RUST_DIR" && RUSTFLAGS="--cfg google_cloud_unstable_storage_bidi" \
  cargo test --release -p storage-grpc-uds)

ctest --test-dir "$POC/cmake-out" --output-on-failure
python3 "$POC/scripts/test_monitoring.py" -v
python3 "$POC/scripts/test_verify_path.py" -v
bash "$POC/scripts/test_parent_crash.sh"
REQUIRE_SUDO=1 bash "$POC/scripts/test_peer_cred.sh"

ls -lh "$POC/cmake-out/gcs-core-server" \
  "$POC/cmake-out/cargo-target/release/libstorage_grpc_uds.a" \
  "$POC/cmake-out/quickstart" \
  "$POC/cmake-out/quickstart_inproc" \
  "$POC/cmake-out/bench/uds_poc_bench"
echo BUILD_OK
