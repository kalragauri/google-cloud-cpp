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

# VM job: install toolchains, fetch the source tarball, build everything.
# Submitted through the GCS job runner (see vm_startup.sh); runs as `poc`.
#   OPS     ops bucket (job runner sets it in the environment of the job)
#   SRC_TGZ object name of the source tarball in the ops bucket
set -euxo pipefail
OPS="${OPS:-kalragauri-uds-poc-ops}"
SRC_TGZ="${SRC_TGZ:-src/google-cloud-cpp-uds-poc.tgz}"
ROOT="$HOME/poc"
mkdir -p "$ROOT"

# --- toolchains (idempotent) ---------------------------------------------------
sudo apt-get update -y
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  build-essential cmake ninja-build git curl zip unzip tar pkg-config \
  python3 linux-libc-dev iproute2 time
if [[ ! -x "$HOME/.cargo/bin/cargo" ]]; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup-init.sh
  sh /tmp/rustup-init.sh -y --profile minimal --default-toolchain 1.99 --no-modify-path
fi
if [[ ! -d "$HOME/vcpkg" ]]; then
  git clone --quiet https://github.com/microsoft/vcpkg.git "$HOME/vcpkg"
  "$HOME/vcpkg/bootstrap-vcpkg.sh" -disableMetrics
fi

# --- source ---------------------------------------------------------------------
rm -rf "$ROOT/src" && mkdir -p "$ROOT/src"
gcloud storage cp "gs://$OPS/$SRC_TGZ" "$ROOT/src.tgz"
tar -C "$ROOT/src" -xzf "$ROOT/src.tgz"
POC="$ROOT/src/storage-uds-poc"

# --- C++ (wrapper, tests, quickstart, ssb_uds, ssb_native) -----------------------
/usr/bin/time -f 'cpp configure wall=%es' cmake -S "$POC" -B "$POC/cmake-out" -GNinja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE="$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake"
/usr/bin/time -f 'cpp build wall=%es' cmake --build "$POC/cmake-out" -j"$(nproc)" \
  --target gcs_core gcs_core_test quickstart ssb_uds ssb_native

# --- Rust (sidecar + pure-rust bench); protoc from the vcpkg tree ---------------
export PROTOC="$POC/cmake-out/vcpkg_installed/x64-linux/tools/protobuf/protoc"
(cd "$POC/sidecar" && /usr/bin/time -f 'rust build wall=%es' \
  "$HOME/.cargo/bin/cargo" build --release)

# --- tests ------------------------------------------------------------------------
(cd "$POC/sidecar" && "$HOME/.cargo/bin/cargo" test --release)
GCS_CORE_SIDECAR="$POC/sidecar/target/release/gcs-core-sidecar" "$POC/cmake-out/gcs_core_test"

ls -la "$POC/cmake-out/bench/ssb_uds" "$POC/cmake-out/bench/ssb_native" \
  "$POC/sidecar/target/release/gcs-core-sidecar" "$POC/sidecar/target/release/gcs-core-bench"
echo BUILD_OK
