# GCS SDK consolidation POC: C++ wrapper over a Rust core via gRPC-over-UDS

> **Proof of concept, not for upstream.** This directory is not referenced by
> the repository's build. It answers one question: can C++ users get Cloud
> Storage through the Rust `google-cloud-storage` crate running in a sidecar
> process, reached over gRPC on a Unix domain socket, and what does it cost
> compared with native `google-cloud-cpp` on the same network path?

```
 your C++ app
     │  gcs_core::Client / ObjectReader / ObjectWriter / ObjectDescriptor / AppendableWriter
     ▼
 thin C++ wrapper (cpp/)     — no storage logic: framing, status mapping, sidecar spawner
     │  gRPC over UDS: gcs.core.v0.StorageCore (proto/)
     ▼
 gcs-core-sidecar (sidecar/) — tonic server + google-cloud-storage 1.21.0 (crates.io)
     │  JSON over HTTP/1.1 (read/write) · gRPC StorageControl (list/delete) · gRPC bidi (open/append)
     ▼
 Cloud Storage — Cloudpath only (the Rust crate has no DirectPath)
```

## Layout

| Path | What it is |
| --- | --- |
| `proto/gcs/core/v0/storage_core.proto` | Wire API between wrapper and sidecar (stateless; handles only for bidi descriptors) |
| `sidecar/` | Rust binary `gcs-core-sidecar`, plus `gcs-core-bench` (the "pure-rust" comparator: same crate calls, in-process) |
| `sidecar/.cargo/config.toml` | `--cfg google_cloud_unstable_storage_bidi` (appendable writes are gated behind this rustc cfg) |
| `cpp/include/gcs_core/client.h`, `cpp/src/` | The thin C++ wrapper and the sidecar process manager |
| `cpp/tests/` | gtests against the real sidecar in `--synthetic` mode |
| `cpp/examples/quickstart.cc` | End-to-end check against real regional and zonal buckets |
| `bench/` | `ssb_uds` and `ssb_native`: the same workloads (`ssb_workloads.h`) on the wrapper and on native google-cloud-cpp |
| `scripts/` | VM job runner, build/run scripts, Cloudpath verifier, analysis |

## Build (workstation)

Prerequisites: Rust 1.99 (`rustup`), the C++ toolchain used for google-cloud-cpp, `~/vcpkg`.

```bash
cd storage-uds-poc
cmake -S . -B cmake-out -GNinja -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE=$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake
cmake --build cmake-out
# tonic-prost-build needs protoc; reuse the one vcpkg just built.
(cd sidecar && PROTOC=$PWD/../cmake-out/vcpkg_installed/x64-linux/tools/protobuf/protoc \
   cargo build --release)
export GCS_CORE_SIDECAR=$PWD/sidecar/target/release/gcs-core-sidecar
ctest --test-dir cmake-out --output-on-failure      # wrapper tests (synthetic sidecar)
(cd sidecar && cargo test --release)                # sidecar unit tests
./cmake-out/quickstart REGIONAL_BUCKET              # JSON + list + bidi read
./cmake-out/quickstart --zonal ZONAL_BUCKET         # appendable write + bidi read
```

The sidecar uses Application Default Credentials.

## Benchmarks

Run on a GCE VM in the buckets' zone (`scripts/vm_*.sh`; the VM is driven through
a GCS job queue, see `scripts/vm_startup.sh`). Every run is wrapped in
`scripts/verify_path.py`, which fails the run if any GCS connection used a
DirectPath address. Comparison rules (same network path, same protocol family,
HTTP/1.1 for JSON, 8 MiB resumable chunks, 4 gRPC channels, one measurement
method for both processes) are in the implementation plan.

```bash
./cmake-out/bench/ssb_uds    --bucket=B --workload=w1r3 --object-sizes=100000000 --iterations=200 \
    --sidecar-arg=--grpc-subchannels=4
./cmake-out/bench/ssb_native --bucket=B --workload=w1r3 --object-sizes=100000000 --iterations=200 --sync=json-h1
./sidecar/target/release/gcs-core-bench --bucket=B --workload=w1r3 --object-sizes=100000000 --iterations=200
python3 scripts/analyze.py RESULTS_DIR > summary.md
```

Workloads: `w1r3` (JSON), `list` (gRPC), `open` / `random8k` / `read50x2x4` /
`appendable` (gRPC bidi; use `--bucket-type=zonal` for zonal buckets),
`throughput` (concurrent 100 MB transfers). `ssb_uds --sidecar-arg=--synthetic`
measures the wrapper↔sidecar hop without GCS.

## How the wrapper works

* **Sidecar lifetime.** The first `Client` for a given `ClientOptions` key
  (`sidecar_path`, `socket_path`, `sidecar_args`, `channels`) spawns the
  sidecar (`posix_spawn`) and passes it the read end of a pipe as
  `--parent-fd=3`. The sidecar exits when that pipe closes, so it never
  outlives the application, even after `SIGKILL`. `Client` instances with
  matching keys share one sidecar (carrying `chunk_size_hint` per `Client`);
  the last one stops it.
* **Discovery.** `ClientOptions::sidecar_path`, else `$GCS_CORE_SIDECAR`, else
  next to the executable, else `PATH`.
* **Socket.** `$XDG_RUNTIME_DIR/gcs-core-<pid>.sock`, else a 0700 directory
  `/tmp/gcs-core-<uid>/`; the socket is 0600 and the sidecar also checks the
  peer uid (`SO_PEERCRED`).
* **Errors.** gRPC status codes equal `google::cloud::StatusCode`s; the
  sidecar maps crate errors (RPC status, HTTP status for JSON) to them.
* **Crash recovery.** If a call fails with `UNAVAILABLE` because the sidecar
  died, the wrapper starts a new one; the next call succeeds.
* **Credentials.** The sidecar's own ADC. C++ credential objects cannot cross
  the process boundary.
