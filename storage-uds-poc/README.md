# GCS SDK consolidation POC: C++ over the shared Rust gRPC-over-UDS server

> **Proof of concept, not for upstream.** This directory is not referenced by
> the repository's build. It answers one question: can C++ users get Cloud
> Storage through the shared Rust server (`storage-grpc-uds` in
> google-cloud-rust, service `gcscore.v1.GcsCore`) behind a thin gRPC wrapper,
> and what does it cost compared with native `google-cloud-cpp`? The server is
> used in **two hostings that differ only in the boundary**: a child *process*
> reached over a Unix domain socket (`gcs_core`), and Tokio *threads inside the
> application* reached over an abstract Unix socket (`gcs_core_inproc`).
> Everything above the backend seam is the same code in both.

```
 your C++ app
     │  gcs_core::Client / ObjectReader / ObjectWriter / ObjectDescriptor / AppendableWriter
     ▼
 thin C++ wrapper (cpp/)     — no storage logic: framing, status mapping; backend seam (core_backend.h)
     │  gRPC: gcscore.v1.GcsCore (proto from the server crate)
     │    sidecar variant:    unix:/run/user/<uid>/gcs-core-<pid>.sock  →  gcs-core-server child process
     │    in-process variant: unix-abstract:gcs-core-<pid>-<n>          →  Tokio threads "gcs-core-inproc"
     ▼
 storage-grpc-uds (google-cloud-rust, fetched by CMake; binary + staticlib)
     │  JSON (read/write) · gRPC StorageControl (list/delete) · gRPC bidi (open/append)
     ▼
 Cloud Storage
```

## Where the server comes from

CMake fetches google-cloud-rust (`GCS_CORE_RUST_REPO`, default the
`kalragauri` fork) at `GCS_CORE_RUST_REF` (default branch `grpc-uds-cpp`:
googleapis/google-cloud-rust PR #7350 plus our follow-up commits) and builds
`storage-grpc-uds` with cargo. Nothing from the server is copied into this
tree. For local iteration on the server, point CMake at a worktree:
`-DFETCHCONTENT_SOURCE_DIR_GOOGLE_CLOUD_RUST=$HOME/google-cloud-rust-grpc-uds-cpp`.

## Layout

| Path | What it is |
| --- | --- |
| `cpp/include/gcs_core/client.h`, `cpp/src/` | The thin C++ wrapper; `core_backend.h` is the seam, `sidecar_backend.cc` and `inproc_backend.cc` the two sides of it |
| `cpp/tests/` | `client_common_test.cc` (both variants, real server without GCS access: lifecycle and validation), `_sidecar_`/`_inproc_`/`sidecar_process_` files for one variant, `client_fake_test.cc` (request/response mapping against a fake `GcsCore`) |
| `cpp/examples/quickstart.cc`, `cold_start.cc` | End-to-end check against real buckets; cold `Client::Create()` probe — both built per variant |
| `bench/`, `scripts/` | Phase-1 harness, off by default (`GCS_UDS_POC_BUILD_BENCH`) until it is ported to the plan's workloads |

## Build (workstation)

Prerequisites: Rust 1.99 (`rustup`), the C++ toolchain used for google-cloud-cpp, `~/vcpkg`.

```bash
cd storage-uds-poc
cmake -S . -B cmake-out -GNinja -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE=$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake
cmake --build cmake-out        # runs cargo too; copies gcs-core-server into cmake-out/
ctest --test-dir cmake-out --output-on-failure
./cmake-out/quickstart REGIONAL_BUCKET              # JSON + list + delete + bidi read (sidecar)
./cmake-out/quickstart_inproc --zonal ZONAL_BUCKET  # appendable write + bidi read (in-process)
```

Both variants use Application Default Credentials; in-process the lookup runs
inside the application. `-DGCS_UDS_POC_BUILD_INPROC=OFF` builds the sidecar
variant alone.

## How the wrapper works

Shared by both variants: the first `Client` for a given options key starts a
server and `Client`s with matching keys share it (carrying `chunk_size_hint`
per `Client`); the last one stops it. Readiness is a `Ping` on every channel.
If a call fails with `UNAVAILABLE` because the server died, the wrapper starts
a new one and the next call succeeds. Descriptor handles from the old server
are rejected rather than sent to the new one, whose handle numbers restart at 1.
`ClientOptions::sidecar_args` takes `gcs-core-server` flags; `--socket`,
`--pdeathsig` and `--tcp` are owned by the wrapper and rejected.

### In-process variant (`gcs_core_inproc`)

* **Hosting.** `libstorage_grpc_uds.a` is linked into the application.
  `Client::Create` calls `gcs_core_start_inproc_with_args` with a fresh
  abstract socket name and the caller's flags; it returns once the upstream
  clients are built and the socket is bound, or with the reason it could not.
* **Lifetime.** The last `Client` drops its channels, then
  `gcs_core_stop_inproc` stops the server. There is no abort entry point, so a
  test "crash" is an orderly stop. `ClientOptions::sidecar_path` and
  `socket_path` are rejected. `Client::sidecar_pid()` is this process;
  `Client::core_thread_cpu_us()` gives the server threads' CPU.

### Sidecar variant (`gcs_core`)

* **Lifetime.** `Client::Create` spawns `gcs-core-server --pdeathsig=true
  --idle-secs=0` (`posix_spawn`) from a process-lifetime helper thread:
  Linux ties `PR_SET_PDEATHSIG` to the spawning thread, so spawning from a
  short-lived caller thread would stop the server when that thread exits.
  Stop is SIGTERM (the server drains and removes its socket), then SIGKILL.
* **Discovery.** `ClientOptions::sidecar_path`, else `$GCS_CORE_SERVER`, else
  next to the executable, else `PATH`.
* **Socket.** `$XDG_RUNTIME_DIR/gcs-core-<pid>.sock`, else a 0700 directory
  `/tmp/gcs-core-<uid>/`; the server also checks the peer uid (`SO_PEERCRED`).
* **Credentials.** The server's own ADC. C++ credential objects cannot cross
  the process boundary.
