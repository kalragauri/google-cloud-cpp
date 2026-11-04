# GCS SDK consolidation POC: C++ over the shared Rust gRPC-over-UDS server

> **Proof of concept, not for upstream.** This directory is not referenced by
> the repository's root `CMakeLists.txt` and makes **zero edits** to
> `google/cloud/storage/`. It evaluates exposing unmodified
> `google::cloud::storage::Client` and `google::cloud::storage::AsyncClient`
> backed by the shared Rust server (`storage-grpc-uds` in `google-cloud-rust`,
> contract `gcscore.v1.GcsCore`) in **two hostings that differ only in the
> boundary**: a multiprocess daemon/child reached over a filesystem Unix domain
> socket (`gcs_core_storage`) and Tokio threads inside the application reached
> over an abstract Unix socket (`gcs_core_storage_inproc`).

```
 your C++ app
     │  unmodified google::cloud::storage::Client & google::cloud::storage::AsyncClient
     ▼
 public SDK adapters (cpp/src/storage_connection.cc, async_connection.cc)
     │  gcs_core::Client — framing, status mapping; backend seam (core_backend.h)
     │    sidecar daemon (default): unix:/run/user/<uid>/gcs-core-daemon.sock → shared gcs-core-server
     │    sidecar child (--child):  unix:/run/user/<uid>/gcs-core-<pid>.sock  → 1:1 gcs-core-server
     │    in-process:               unix-abstract:gcs-core-<pid>-<n>          → Tokio threads "gcs-core-inproc"
     ▼
 storage-grpc-uds (google-cloud-rust, fetched by CMake; binary + staticlib)
     │  JSON (read/write) · gRPC StorageControl (list/delete) · gRPC bidi (open/append)
     ▼
 Cloud Storage
```

## Where the server comes from

CMake fetches `google-cloud-rust` (`GCS_CORE_RUST_REPO`, default the
`kalragauri` fork) at `GCS_CORE_RUST_REF` (branch `grpc-uds-cpp` or pinned
commit SHA: `googleapis/google-cloud-rust` PR #7350 plus our four follow-up
commits) and builds `storage-grpc-uds` with `cargo`. Nothing from the server is
copied into this tree. For local iteration on the server, point CMake at a
worktree:
`-DFETCHCONTENT_SOURCE_DIR_GOOGLE_CLOUD_RUST=$HOME/google-cloud-rust-grpc-uds-cpp`.

## Layout

| Path | What it is |
| --- | --- |
| `cpp/include/gcs_core/storage_client.h` | Public factories `MakeUdsStorageClient` and `MakeUdsAsyncClient` returning unmodified `google::cloud::storage::Client` and `AsyncClient` |
| `cpp/include/gcs_core/client.h`, `cpp/src/` | Thin `gcscore.v1.GcsCore` gRPC wrapper + `StorageConnection` / `AsyncConnection` adapters; `core_backend.h` is the seam between `sidecar_backend.cc` and `inproc_backend.cc` |
| `cpp/tests/` | `adapter_fake_test.cc` (33 public-SDK adapter tests against fake `GcsCore`), `client_fake_test.cc`, `client_common_test.cc`, `client_sidecar_test.cc`, `client_inproc_test.cc`, `sidecar_process_test.cc` |
| `cpp/examples/quickstart.cc`, `cold_start.cc` | Live E2E check via public `Client` / `AsyncClient`; cold `Client::Create()` probe |
| `bench/uds_poc_bench.cc` | Unified benchmark binary driving `--mode={native,uds-sidecar,uds-inproc}` across the 6 POC workloads (`open`, `read50x2x4-serial`, `read50x2x4-concurrent`, `random8k`, `bidi-read-48t`, `bidi-write-48t`, `list-objects-100`, `list-buckets-100`) |
| `scripts/` | VM provisioning (`vm_startup.sh`, `vm_build.sh`), benchmark runner (`vm_run.sh`), path gate (`verify_path.py`), Cloud Monitoring collector (`monitoring.py`), packaging/deployment probes (`probe_inproc.sh`, `test_parent_crash.sh`, `test_peer_cred.sh`), and analyzer (`analyze.py`) |

## Build and test (workstation)

Prerequisites: Rust (`rustup`), C++17 toolchain, Ninja, CMake >= 3.22, `~/vcpkg`.

```bash
cd storage-uds-poc
cmake -S . -B cmake-out -GNinja -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE=$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake
cmake --build cmake-out -j"$(nproc)"
ctest --test-dir cmake-out --output-on-failure

# Live E2E check against regional STANDARD and zonal RAPID buckets:
./cmake-out/quickstart REGIONAL_BUCKET
./cmake-out/quickstart --zonal ZONAL_BUCKET
./cmake-out/quickstart_inproc REGIONAL_BUCKET
./cmake-out/quickstart_inproc --zonal ZONAL_BUCKET
```

## Operational notes

* **Sidecar modes (`SidecarMode::kDaemon` vs `SidecarMode::kChild`).**
  `SidecarMode::kDaemon` (default) attaches to or flock-spawns a per-UID shared
  `gcs-core-server` (`--pdeathsig=false --idle-secs=300`) at
  `$XDG_RUNTIME_DIR/gcs-core-daemon.sock` (fallback `/tmp/gcs-core-<uid>/`).
  `SidecarMode::kChild` spawns a 1:1 child (`--pdeathsig=true --idle-secs=0`)
  from a dedicated process-lifetime helper thread so Linux's thread-bound
  `PR_SET_PDEATHSIG` delivers `SIGKILL` if the parent crashes.
* **In-process variant (`gcs_core_storage_inproc`).** Links
  `libstorage_grpc_uds.a` into the host binary and binds an abstract Unix
  socket (`unix-abstract:gcs-core-<pid>-<n>`), avoiding filesystem socket paths
  while keeping the identical gRPC wire contract.
* **Credentials.** Both sides resolve Application Default Credentials
  independently; in-process credential resolution runs inside the host process,
  whereas custom C++ `google::cloud::Credentials` callbacks cannot cross the
  `gcscore.v1` boundary (`StorageCredentialsOption` is rejected with
  `kInvalidArgument`).
* **`fork()` safety.** Consistent with gRPC C++ and Tokio multi-threaded
  runtimes, calling `fork()` without `exec()` after creating a `Client` or
  `AsyncClient` (in either sidecar or in-process mode) is unsupported: neither
  gRPC completion queues nor embedded Tokio worker pools reinitialize across
  `fork()`. Construct clients in worker processes *after* forking.
