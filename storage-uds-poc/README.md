# GCS SDK consolidation POC: C++ wrapper over a Rust core — sidecar process vs. in-process threads

> **Proof of concept, not for upstream.** This directory is not referenced by
> the repository's build. It answers one question: can C++ users get Cloud
> Storage through the Rust `google-cloud-storage` crate behind a thin gRPC
> wrapper, and what does it cost compared with native `google-cloud-cpp` on the
> same network path? The core is built in **two hostings that differ only in
> the boundary**: a sidecar *process* reached over a Unix domain socket
> (`gcs_core`, `ssb_uds`), and tokio *threads inside the application* reached
> over socket pairs (`gcs_core_inproc`, `ssb_inproc`). Everything else — the
> proto, the Rust service, the wrapper above the backend seam, the benchmark
> workloads — is the same code, so `inproc` against `uds` isolates the cost of
> the process boundary.

```
 your C++ app
     │  gcs_core::Client / ObjectReader / ObjectWriter / ObjectDescriptor / AppendableWriter
     ▼
 thin C++ wrapper (cpp/)     — no storage logic: framing, status mapping; backend seam (core_backend.h)
     │  gRPC: gcs.core.v0.StorageCore (proto/)
     │    sidecar variant:    unix:/run/user/<uid>/gcs-core-<pid>.sock  →  gcs-core-sidecar process
     │    in-process variant: socketpair(AF_UNIX) per channel          →  tokio threads "gcs-core-N"
     ▼
 Rust core (sidecar/, crate `gcs_core`: rlib for the sidecar binary, staticlib for the C++ app)
     │  tonic server + google-cloud-storage 1.21.0 (crates.io)
     │  JSON over HTTP/1.1 (read/write) · gRPC StorageControl (list/delete) · gRPC bidi (open/append)
     ▼
 Cloud Storage — Cloudpath only (the Rust crate has no DirectPath)
```

## Layout

| Path | What it is |
| --- | --- |
| `proto/gcs/core/v0/storage_core.proto` | Wire API between wrapper and core (stateless; handles only for bidi descriptors) |
| `sidecar/` | Rust crate `gcs_core`: `service.rs` (the API on the crate), `inproc.rs` (C ABI hosting it on threads), `main.rs` (the `gcs-core-sidecar` process), `gcs-core-bench` (the "pure-rust" comparator: same crate calls, no wrapper) |
| `sidecar/.cargo/config.toml` | `--cfg google_cloud_unstable_storage_bidi` (appendable writes are gated behind this rustc cfg) |
| `cpp/include/gcs_core/client.h`, `cpp/src/` | The thin C++ wrapper; `core_backend.h` is the seam, `sidecar_backend.cc` and `inproc_backend.cc` the two sides of it |
| `cpp/tests/` | gtests against the real core in `--synthetic` mode: `client_common_test.cc` runs in both variants' test binaries, the `_sidecar_`/`_inproc_` files cover what only one variant has |
| `cpp/examples/quickstart.cc`, `cold_start.cc` | End-to-end check against real buckets; cold `Client::Create()` probe — both built per variant |
| `bench/` | `ssb_uds`, `ssb_inproc` (one workload body, `gcs_core_target.h`) and `ssb_native`: the same workloads (`ssb_workloads.h`) on the two wrapper hostings and on native google-cloud-cpp |
| `scripts/` | VM job runner, build/run scripts, Cloudpath verifier, analysis, packaging probes |

## Build (workstation)

Prerequisites: Rust 1.99 (`rustup`), the C++ toolchain used for google-cloud-cpp, `~/vcpkg`.

```bash
cd storage-uds-poc
cmake -S . -B cmake-out -GNinja -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE=$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake
cmake --build cmake-out        # runs `cargo build --release` too (sidecar, bench, libgcs_core.a)
export GCS_CORE_SIDECAR=$PWD/sidecar/target/release/gcs-core-sidecar
ctest --test-dir cmake-out --output-on-failure      # gcs_core_test (sidecar) + gcs_core_inproc_test
(cd sidecar && PROTOC=$PWD/../cmake-out/vcpkg_installed/x64-linux/tools/protobuf/protoc \
   cargo test --release)                            # core unit tests
./cmake-out/quickstart REGIONAL_BUCKET              # JSON + list + bidi read (sidecar)
./cmake-out/quickstart_inproc --zonal ZONAL_BUCKET  # appendable write + bidi read (in-process)
```

Both variants use Application Default Credentials; in-process the lookup runs
inside the application. `-DGCS_UDS_POC_BUILD_INPROC=OFF` builds the sidecar
variant alone.

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
./cmake-out/bench/ssb_inproc --bucket=B --workload=w1r3 --object-sizes=100000000 --iterations=200 \
    --sidecar-arg=--grpc-subchannels=4          # same core arguments; add --sidecar-arg=--link=abstract for the control
./cmake-out/bench/ssb_native --bucket=B --workload=w1r3 --object-sizes=100000000 --iterations=200 --sync=json-h1
./sidecar/target/release/gcs-core-bench --bucket=B --workload=w1r3 --object-sizes=100000000 --iterations=200
python3 scripts/analyze.py RESULTS_DIR > summary.md
```

Workloads: `w1r3` (JSON), `list` (gRPC), `open` / `random8k` / `read50x2x4` /
`appendable` (gRPC bidi; use `--bucket-type=zonal` for zonal buckets),
`throughput` (concurrent 100 MB transfers). `--sidecar-arg=--synthetic` on
either wrapper binary measures the wrapper↔core hop without GCS.

CSV columns are the same for every binary. For `ssb_uds`, `*_peer` is the
sidecar process. For `ssb_inproc` there is one process: `*_peer` is the core's
threads (their CPU from per-thread clocks, their allocations from a thread-local
tag in `alloc_counter.cc`) and `*_app` the rest, so `app + peer` is the whole
process either way; RSS stays process-wide in `*_app`.

## How the wrapper works

Shared by both variants: the first `Client` for a given options key starts a
core and `Client`s with matching keys share it (carrying `chunk_size_hint` per
`Client`); the last one stops it. gRPC status codes equal
`google::cloud::StatusCode`s; the core maps crate errors (RPC status, HTTP
status for JSON) to them. If a call fails with `UNAVAILABLE` because the core
died, the wrapper starts a new one and the next call succeeds; handles from the
old instance are rejected (per-instance epoch).

### In-process variant (`gcs_core_inproc`)

* **Hosting.** `libgcs_core.a` is linked into the application. `Client::Create`
  calls `gcs_core_inproc_start` (`cpp/src/gcs_core_inproc.h`), which parses the
  same core flags the sidecar takes (minus `--socket`/`--parent-fd`), starts a
  tokio runtime whose threads are named `gcs-core-<n>-<m>`, builds the core
  clients (ADC lookup, on the calling thread) and serves the same tonic
  service. `Create` returns once the core serves.
* **Link.** `--link=socketpair` (default): one `socketpair(AF_UNIX)` per channel,
  the client end wrapped with `grpc::CreateCustomInsecureChannelFromFd`; nothing
  outside the process can connect. `--link=abstract`: an abstract-namespace
  socket with a random name and the same peer-uid check as the sidecar, served
  to a normal `unix-abstract:` channel — a control for the channel type.
* **Lifetime.** The last `Client` drops its channels, then the core drains and
  the runtime shuts down, each bounded by 2 s. `ClientOptions::sidecar_path`
  and `socket_path` are rejected. `GetSidecarVersion().pid` and
  `Client::sidecar_pid()` are this process; `GetSidecarStats()` describes this
  process and reports `allocated_bytes = 0`; `Client::core_thread_cpu_us()`
  gives the core threads' CPU.
* **What a C++ host has to know.** A Rust binary ignores SIGPIPE; a C++ program
  usually does not, and the core's vectored writes can raise it when a peer end
  is already closed. The library blocks SIGPIPE on its own threads (and on the
  caller's thread while it runs futures there) so the write fails with `EPIPE`
  instead; dispositions are never changed. The core never calls `exit()`,
  installs no signal handlers, and catches panics at every entry point. After a
  `fork()` the child has no core threads: do not use a `Client` there.

### Sidecar variant (`gcs_core`)

* **Sidecar lifetime.** `Client::Create` spawns the sidecar (`posix_spawn`)
  and passes it the read end of a pipe as `--parent-fd=3`. The sidecar exits
  when that pipe closes, so it never outlives the application, even after
  `SIGKILL`.
* **Discovery.** `ClientOptions::sidecar_path`, else `$GCS_CORE_SIDECAR`, else
  next to the executable, else `PATH`.
* **Socket.** `$XDG_RUNTIME_DIR/gcs-core-<pid>.sock`, else a 0700 directory
  `/tmp/gcs-core-<uid>/`; the socket is 0600 and the sidecar also checks the
  peer uid (`SO_PEERCRED`).
* **Credentials.** The sidecar's own ADC. C++ credential objects cannot cross
  the process boundary.
