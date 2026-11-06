# C++ on the Shared Rust Core via gRPC over UDS — POC Report

**Scope:** Evaluates running the unmodified public `google::cloud::storage::Client` and `google::cloud::storage::AsyncClient` over the shared Rust gRPC-over-UDS server (`storage-grpc-uds`, wire contract `gcscore.v1.GcsCore`) in both **multiprocess sidecar** (`uds-sidecar`) and **in-process Tokio** (`uds-inproc`) hostings against the native `google-cloud-cpp` gRPC client over **Cloudpath** (`storage.googleapis.com:443`), on a `c4-standard-192` VM in `us-central1-b` against buckets in `us-west1`.

**Revisions under test:**
- **C++ (`google-cloud-cpp`, branch `grpc-uds-cpp`):** benchmark binary built at `bb95000affdb3be71c34ba53636f47b29de973a8` for the `Open`, random `8 KiB`, `BidiWrite` and list cells, and at `21871eec65ea1b6c17b98a0b10f2a86c30bf6753` for the `50 × 2 MiB` and 48-thread `BidiRead` cells (the later build opens the read session outside the timed operation, as the workload definitions require). The recorded run info names the same code by its commit ids before a documentation-only history rewrite, `36919ee423` and `c7e65b2b6a`; **zero files modified under `google/cloud/`**.
- **Rust (`google-cloud-rust`, branch `grpc-uds-cpp`):** `e432ad1de767f8b8094de70fcb3113ccba67310d` = [googleapis/google-cloud-rust#7350](https://github.com/googleapis/google-cloud-rust/pull/7350) plus 5 server commits ([googleapis/google-cloud-rust#7390](https://github.com/googleapis/google-cloud-rust/pull/7390)).

> [!NOTE]
> All **46 published benchmark cells** (24 zonal + list cells, 15 Standard Regional cells plus a repeat of the 3 regional 48-thread `BidiRead` cells, and 4 Tokio worker-thread sensitivity cells) and all packaging and customer-environment probes completed with **`0` RPC errors**, **`0` DirectPath sockets** ([verify_path.py](scripts/verify_path.py)), and **100% complete Cloud Monitoring windows** ([monitoring.py](scripts/monitoring.py)).

---

## 1. Executive Summary

1. **Full public SDK compatibility with zero edits to `google/cloud/storage/`:** Both `uds-sidecar` (per-UID daemon or `1:1` child over a `0600` filesystem Unix socket) and `uds-inproc` (background Tokio runtime inside the C++ process over a Linux `unix-abstract:` socket) implement `storage::internal::StorageConnection` and `storage_experimental::AsyncConnection` behind the unchanged public `Client` and `AsyncClient` APIs. Covered operations: JSON `ReadObject`, `WriteObject`, and `InsertObject`; gRPC `ListObjects`, `ListBucketsForProject`, and `DeleteObject`; bidirectional `BidiReadObject` (`AsyncClient::Open` / `ObjectDescriptor::Read`); and appendable `BidiWriteObject` (`StartAppendableObjectUpload`).
2. **Single-stream reads and lists are at parity; the 48-thread streaming read is slower over UDS, at equal connection counts (`4` vs `4`):**
   - **`Open`, `50 × 2 MiB`, random `8 KiB`, lists:** Standard Regional `Open` P50 `53.7 ms` native vs `53.9 ms` / `54.8 ms` UDS; `50 × 2 MiB` serial within `-1.1%` to `+4.2%` P50 on both buckets and concurrent within `-7.0%` to `+1.9%` regionally; random `8 KiB` within `-1.7%` to `+4.9%` P50; lists within `-4.0%` to `+2.0%` P50 despite `StorageControl` in `google-cloud-rust` using 1 HTTP/2 connection versus 4 natively. Zonal concurrent `50 × 2 MiB` is faster over UDS (`-23.9%` / `-16.0%` P50; one run, cause not verified).
   - **Zonal `Open`** favors UDS (`1.69 s` native vs `661.0 ms` / `623.7 ms`) because `google-cloud-cpp` sleeps uniformly in `[1.0 s, 2.0 s]` on the initial Cloudpath zonal redirect whereas `google-cloud-rust` uses `[0.0 s, 1.0 s]` full jitter. This is a once-per-session cost; with sessions reused, zonal `50 × 2 MiB` serial is at parity.
   - **48-thread `16 MiB` streaming read (`5 min`):** with one long-lived session per worker, native C++ reaches **`1,108.0 MB/s` zonal** and **`1,043–1,063 MB/s` regional**. Zonal UDS is **`-23.0%` (sidecar, `852.9 MB/s`)** and **`-15.8%` (in-process, `932.5 MB/s`)**. Regionally (two runs) one UDS hosting per run was capped at `155–253 MB/s` with all 48 workers in lockstep while the other reached `682–761 MB/s` (`-27%` to `-36%`); native was stable. CPU per byte is flat (`3.21–3.89 ns/byte` across all nine cells), so the gap is in how streams reach the network, not CPU. The suspected cause — the Rust client's balancer concentrating long-lived streams on one of its 4 connections — is unverified and tracked as a follow-up action item.
3. **Appendable writes (`BidiWriteObject`, 48 threads × 200 MB) regress in throughput, CPU, and memory:** Throughput drops **`-7.1%` (`1,335.3` → `1,240.1 MB/s`, sidecar)** and **`-28.3%` (`957.5 MB/s`, in-process)**, total CPU per byte rises **2.8×** (`0.97` → `2.78` / `2.71 ns/byte`), and peak resident memory reaches **`8,703 MiB` (`uds-sidecar` server, plus `1,321 MiB` app)** and **`9,532 MiB` (`uds-inproc`)** versus **`392 MiB` native** (`2.40%` / `2.18%` vs `0.95%` of VM RAM). Native C++ reuses one shared external `absl::Cord`, whereas the UDS path copies chunks into `WriteRequest` protobufs and allocates per-chunk `Bytes` plus `32 MiB` per-stream `ReplayBuffer` queues across 48 streams and 192 Tokio worker threads / glibc arenas.
4. **Tokio's 1-worker-per-vCPU default (`192` workers here) is footprint overhead without throughput gain:** At equal durations (`180 s` on `8 KiB` reads, `300 s` on 48-thread `16 MiB` reads, both arms on the same harness build), `--worker-threads=16` matches the 192-worker default within run-to-run noise (`+0.8%` to `+3.1%` `8 KiB` ops/s; `+2.1%` sidecar / `-2.5%` in-process streaming read MB/s) while cutting server threads from `193 → 17` (sidecar) and `260 → 84` (in-process) and idle `VmSize` from `12.4 GiB → 0.6 GiB`.
5. **Packaging, startup, and build costs are material for C++ users:** Stripped `gcs-core-server` is **`30.49 MiB`**; linking the server in-process adds **`+28.73 MiB`** stripped (`61.83 MiB` vs `33.10 MiB`, from a `122.81 MiB` static archive). Cold `Client::Create()` takes **`5.8 ms`** attaching to a warm daemon, **`14.8–22.2 ms`** in-process, and **`98.2–102.9 ms`** spawning a child sidecar. Source builds add `cargo` + `rustc >= 1.91` + `protoc` alongside CMake/vcpkg; on 192 vCPUs, 1-file incremental rebuilds take **`47.9 s` (Rust)** and **`62.6 s` (`gcscore.proto`)** versus **`5.3 s` (C++)**.
6. **Linux deployment probes pass (`7/7`); Standard Regional `BidiReadObject` is enabled per bucket project:** Probes pass on `noexec` `/tmp`, read-only filesystems (via `unix-abstract:`), container PID namespaces, `>107`-byte socket paths, stale sockets vs regular files, `13 ms` child exit after parent `SIGKILL` (`PR_SET_PDEATHSIG`), and `0600` + `SO_PEERCRED` cross-UID rejection. Regional `BidiReadObject` is currently enabled per bucket-owning project (`PERMISSION_DENIED` on other projects; measured here on an enabled test project).

---

## 2. Experimental Setup and Methodology

| Parameter | Configuration |
|---|---|
| **Compute VM** | `kalragauri-uds-poc-c4`, `c4-standard-192` (192 vCPUs, 744 GiB RAM), `us-central1-b` (cross-region from `us-west1` buckets to enforce Cloudpath), Ubuntu 24.04 (`7.0.0-1014-gcp`, glibc `2.39`), gVNIC Tier-1 egress, Google Cloud Ops Agent active |
| **Buckets (`us-west1`)** | **Zonal (`RAPID`, HNS):** `gs://kalragauri-uds-poc-usw1-zonal` (`us-west1-a`) for W1–W5.<br>**Standard Regional (`STANDARD`, non-HNS):** `gs://kalragauri-uds-poc-vp-usw1` (`us-west1`, owned by a test project with regional `BidiReadObject` enabled) for regional W1–W4; `gs://kalragauri-uds-poc-usw1` (`us-west1`) for W6a `ListObjects`; 100 `gs://kalragauri-uds-list-000..099` buckets for W6b `ListBuckets` (deleted after the run). |
| **Comparators (`--mode`)** | **`native`:** `google::cloud::storage` gRPC `Client` / `AsyncClient` pinned to `storage.googleapis.com` (DirectPath disabled).<br>**`uds-sidecar`:** unmodified `Client` / `AsyncClient` → `gcs_core_storage` → `gcs-core-server` daemon over a `unix:` filesystem socket.<br>**`uds-inproc`:** unmodified `Client` / `AsyncClient` → `gcs_core_storage_inproc` → background Tokio runtime inside the C++ process over `unix-abstract:`. |
| **Cloudpath connections** | **Pinned to `4` on both sides** (`GrpcNumChannelsOption(4)` natively; `--grpc-subchannels=4` → `Storage::builder().with_grpc_subchannel_count(4)` in the Rust server for data-plane and bidi RPCs). Left unpinned on a 192-vCPU VM, both SDKs default to 1 connection per vCPU (`192`).<br>**Exception on W6 (`StorageControl`):** `google-cloud-gax` ([grpc.rs](https://github.com/googleapis/google-cloud-rust/blob/18d8711442/src/gax-internal/src/grpc.rs#L627-L635)) exposes no subchannel-count setter on generated `StorageControl`, leaving the Rust server at **1 HTTP/2 connection vs 4 natively** (`1` vs `4–7` observed Cloudpath peers) — a conservative asymmetry against UDS.<br>**Local UDS channels:** `4` round-robin gRPC channels between the C++ adapter and the local Rust server (`--uds-channels=4`), independent of upstream Cloudpath connections. |
| **Tokio worker threads** | Default = `1` per available vCPU (`192` workers); sensitivity pass run at `--worker-threads=16`. |
| **Durations & isolation** | `180 s` for W1–W3 and W6; `300 s` for W4 and W5; `10 s` unrecorded warm-up before every window. One comparator runs at a time, comparator order rotates across workloads to avoid time-of-day bias, and `gcs-core-server` is stopped between cells. |
| **Verification gates** | 1. **Path gate:** [verify_path.py](scripts/verify_path.py) inspects open sockets during every cell (`verified == true`, Cloudpath peers only, `0` DirectPath sockets).<br>2. **Cloud Monitoring gate:** [monitoring.py](scripts/monitoring.py) queries `compute.googleapis.com/instance/cpu/utilization` (`60 s` mean) and `agent.googleapis.com/memory/percent_used{state="used"}` (`60 s` max) across each cell's `[window_start_utc, window_end_utc]` after ingestion flush. |
| **Credentials** | Application Default Credentials (ADC) resolved independently on each side of the UDS boundary. |

Committed summaries and probe logs live in `storage-uds-poc/results/` ([rev4-c4-20261105](results/rev4-c4-20261105/)); tables are generated directly by [analyze.py](scripts/analyze.py) (`--doc-tables` and `--sensitivity`).

---

## 3. End-to-End Functionality

### 3.1 Public C++ SDK Surface (Unmodified Veneer API)

| POC API | Unmodified `google::cloud::storage` Method | Over UDS (`uds-sidecar` & `uds-inproc`) |
|---|---|---|
| `BidiReadObject` in gRPC | `AsyncClient::Open(BucketName, object)` → `ObjectDescriptor`; `ObjectDescriptor::Read(offset, limit)` / `ReadFromOffset` | ✅ Zonal; ✅ Standard Regional when enabled for the bucket's project |
| `BidiWriteObject` in gRPC | `AsyncClient::StartAppendableObjectUpload(BucketName, object)` → `AsyncWriter::Write` / `Flush` / `Finalize` | ✅ Zonal (appendable objects); regular-bucket `BidiWriteObject` is not implemented in `google-cloud-rust` |
| `ReadObject` in JSON | `Client::ReadObject(bucket, object)` → `ObjectReadStream` (full and ranged) | ✅ Verified end-to-end via `quickstart` (not in the gRPC benchmark matrix) |
| `WriteObject` in JSON | `Client::WriteObject(bucket, object)` → `ObjectWriteStream`; `Client::InsertObject` | ✅ Verified end-to-end via `quickstart` (not in the gRPC benchmark matrix) |
| `ListObject` in gRPC | `Client::ListObjects(bucket)` → `ListObjectsReader` | ✅ Benchmarked (W6a) |
| `ListBucket` in gRPC | `Client::ListBucketsForProject(project)` → `ListBucketsReader` | ✅ Benchmarked (W6b) |
| `DeleteObject` in gRPC | `Client::DeleteObject`, `AsyncClient::DeleteObject` | ✅ Added to `gcscore.v1.GcsCore` in follow-up commit `48a58e09f` |

**Verification coverage:**
- **Live Cloudpath probes** ([02-probes.log](results/rev4-c4-20261105/02-probes.log)): `quickstart` and `quickstart_inproc` against both Standard Regional and Zonal buckets verify JSON single-shot upload, resumable upload, full and ranged reads with `x-goog-hash` CRC32C headers, gRPC `ListObjects` / `ListBucketsForProject` / `DeleteObject`, status mapping (`NOT_FOUND`, `FAILED_PRECONDITION`), zonal appendable upload (`Write` + `Flush` + `Finalize` with `x-uds-transport-used == GRPC`), zonal `BidiReadObject` full and ranged reads, and expected rejection of non-appendable JSON uploads on zonal buckets.
- **Hermetic test suites:** `4/4` C++ `ctest` binaries (including 33 public-SDK adapter tests in [adapter_fake_test.cc](cpp/tests/adapter_fake_test.cc) against an in-process fake `GcsCore`), `13/13` Rust unit and integration tests, and `20` Python unit tests plus offline runner control-flow checks ([test_vm_run.sh](scripts/test_vm_run.sh)).

### 3.2 Known Functional Gaps Across the `gcscore.v1` Boundary

- **`AsyncWriter::Flush` and `Query` persistence reporting:** `gcscore.v1.WriteObject` is currently client-streaming rather than bidirectional, so `Flush` and `Query` report bytes accepted by the local UDS stream rather than service-acknowledged `persisted_size` (upstream bidirectional write work in [googleapis/google-cloud-rust#7357](https://github.com/googleapis/google-cloud-rust/pull/7357)).
- **Regular-bucket `BidiWriteObject`:** `google-cloud-storage` (Rust) exposes `BidiWriteObject` only via `open_appendable_object` on zonal buckets; W5 on Standard Regional buckets is therefore N/A.
- **In-process C++ policy and credential callbacks:** Custom `google::cloud::Credentials` (`UnifiedCredentialsOption`, `GrpcCredentialOption` → rejected with `kUnimplemented`), custom C++ `RetryPolicy` / `BackoffPolicy` / `IdempotencyPolicy` subclasses, and caller OpenTelemetry spans do not cross the protobuf socket boundary.
- **`fork()` without `exec()` after client construction:** Unsupported in both hostings (neither gRPC C++ completion queues nor multi-threaded Tokio runtimes reinitialize across `fork()`).

---

## 4. Benchmark Results

**Conventions:** `% Diff = (UDS - Native) / Native`. Negative `% Diff` on latency, CPU, and peak memory favors UDS; positive `% Diff` on throughput favors UDS. Latency rows report per-operation wall time at the outermost public C++ SDK call, except **W4 (`bidi-read-48t`)**, which reports **per-`16 MiB`-range** `ObjectDescriptor::Read` latency (13 ranges per `200 MiB` object: twelve `16 MiB` ranges and a final `8 MiB` range). Per-object W4 latencies and full secondary tables are in [full-summary.md](results/rev5-c4-20261106/full-summary.md), [regional-summary.md](results/rev5-c4-20261106/regional-summary.md), and [regional-w4-run2-summary.md](results/rev5-c4-20261106/regional-w4-run2-summary.md); each lists the run and binary every cell came from.

### 4.1 Zonal Bucket (`us-west1-a`, `RAPID`) — Workloads W1–W5

Source: [full-doc-tables.md](results/rev5-c4-20261106/full-doc-tables.md) (`18` zonal cells here plus `6` control-plane list cells in [section 4.3](#43-control-plane-listing--workloads-w6a-and-w6b) = `24` cells, `0` errors).

| Workload | Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---|---:|---:|---:|---:|---:|
| **W1** Open `200 MiB` (`AsyncClient::Open`, `180 s`) | Latency P50 / P90 / P99 | 1.69 s / 2.05 s / 2.22 s | 661.0 ms / 1.01 s / 1.11 s | -60.9% / -51.0% / -50.0% | 623.7 ms / 1.04 s / 1.11 s | -63.1% / -49.6% / -50.0% |
| | Throughput (`ops/s`) | 0.58 | 1.55 | +168.5% | 1.57 | +171.8% |
| | VM CPU avg / Peak Mem | 0.26% / 2.08% | 0.33% / 1.28% | +30.8% / -38.7% | 0.32% / 1.06% | +24.1% / -49.2% |
| | Ops (`ok / err`) | 104 / 0 | 280 / 0 | | 284 / 0 | |
| **W2a** `50 × 2 MiB` @ `4 MiB` stride, serial (`180 s`) † | Latency P50 / P90 / P99 | 4.29 s / 4.34 s / 4.40 s | 4.27 s / 4.34 s / 4.56 s | -0.4% / -0.0% / +3.6% | 4.24 s / 4.30 s / 4.37 s | -1.1% / -0.9% / -0.8% |
| | Throughput (`MB/s`) | 24.5 | 24.5 | -0.0% | 24.7 | +0.7% |
| | VM CPU avg / Peak Mem | 0.37% / 0.85% | 0.61% / 0.86% | +64.2% / +1.9% | 0.36% / 0.86% | -4.3% / +2.3% |
| | Ops (`ok / err`) | 43 / 0 | 43 / 0 | | 43 / 0 | |
| **W2b** `50 × 2 MiB` @ `4 MiB` stride, concurrent (`180 s`) † | Latency P50 / P90 / P99 | 3.79 s / 4.04 s / 4.47 s | 2.88 s / 2.98 s / 3.11 s | -23.9% / -26.3% / -30.4% | 3.18 s / 3.36 s / 3.37 s | -16.0% / -17.0% / -24.6% |
| | Throughput (`MB/s`) | 27.4 | 36.2 | +31.9% | 32.9 | +20.0% |
| | VM CPU avg / Peak Mem | 0.40% / 0.87% | 0.69% / 0.95% | +74.9% / +9.3% | 0.36% / 0.86% | -9.4% / -0.2% |
| | Ops (`ok / err`) | 48 / 0 | 63 / 0 | | 57 / 0 | |
| **W3** Random `8 KiB` on open `100 MiB` (`180 s`) | Latency P50 / P90 / P99 | 35.4 ms / 36.2 ms / 38.0 ms | 37.1 ms / 39.6 ms / 42.3 ms | +4.9% / +9.5% / +11.3% | 36.0 ms / 39.0 ms / 45.5 ms | +1.8% / +7.8% / +19.7% |
| | Throughput (`ops/s`) | 28.15 | 26.59 | -5.6% | 27.22 | -3.3% |
| | VM CPU avg / Peak Mem | 0.32% / 0.80% | 0.35% / 0.82% | +9.5% / +2.1% | 0.42% / 0.82% | +28.5% / +2.1% |
| | Ops (`ok / err`) | 5,068 / 0 | 4,786 / 0 | | 4,899 / 0 | |
| **W4** `BidiRead` `16 MiB` chunks, 48 threads (`300 s`) † | Range P50 / P90 / P99 | 565.7 ms / 1.08 s / 1.27 s | 886.8 ms / 1.45 s / 2.33 s | +56.8% / +35.0% / +84.1% | 798.9 ms / 1.25 s / 1.50 s | +41.2% / +16.1% / +18.4% |
| | Throughput (`MB/s`) | **1,108.0** | 852.9 | -23.0% | 932.5 | -15.8% |
| | VM CPU avg / Peak Mem | 2.59% / 0.87% | 2.39% / 1.01% | -7.9% / +15.3% | 2.73% / 1.01% | +5.3% / +15.3% |
| | Objects (`ok / err`) | 1,565 / 0 | 1,200 / 0 | | 1,313 / 0 | |
| **W5** `BidiWrite` `200 MB` objects, 48 threads (`300 s`) | Object P50 / P90 / P99 | 3.98 s / 15.42 s / 28.83 s | 6.28 s / 14.90 s / 21.08 s | +57.6% / -3.4% / -26.9% | 6.65 s / 22.40 s / 33.68 s | +67.0% / +45.3% / +16.8% |
| | Throughput (`MB/s`) | 1,335.3 | 1,240.1 | -7.1% | 957.5 | -28.3% |
| | VM CPU avg / Peak Mem | 1.29% / 0.95% | 2.27% / 2.40% | +75.8% / +152.6% | 1.80% / 2.18% | +39.8% / +129.5% |
| | Objects (`ok / err`) | 2,050 / 0 | 1,913 / 0 | | 1,482 / 0 | |

### 4.2 Standard Regional Bucket (`us-west1`, `STANDARD`, Non-HNS) — Workloads W1–W4

Source: [regional-doc-tables.md](results/rev5-c4-20261106/regional-doc-tables.md) and, for the W4 repeat, [regional-w4-run2-doc-tables.md](results/rev5-c4-20261106/regional-w4-run2-doc-tables.md) (`18` cells on `gs://kalragauri-uds-poc-vp-usw1`, `0` errors).

| Workload | Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---|---:|---:|---:|---:|---:|
| **W1** Open `200 MiB` (`AsyncClient::Open`, `180 s`) | Latency P50 / P90 / P99 | 53.7 ms / 61.9 ms / 85.6 ms | 53.9 ms / 62.4 ms / 133.6 ms | +0.4% / +0.8% / +56.0% | 54.8 ms / 64.4 ms / 133.4 ms | +2.0% / +4.0% / +55.8% |
| | Throughput (`ops/s`) | 18.09 | 17.70 | -2.2% | 17.29 | -4.4% |
| | VM CPU avg / Peak Mem | 0.30% / 0.81% | 0.35% / 0.81% | +17.7% / +0.9% | 0.33% / 0.83% | +8.2% / +2.7% |
| | Ops (`ok / err`) | 3,256 / 0 | 3,186 / 0 | | 3,113 / 0 | |
| **W2a** `50 × 2 MiB` @ `4 MiB` stride, serial (`180 s`) † | Latency P50 / P90 / P99 | 4.63 s / 4.70 s / 4.82 s | 4.60 s / 4.71 s / 5.69 s | -0.7% / +0.1% / +18.0% | 4.83 s / 4.89 s / 4.98 s | +4.2% / +3.9% / +3.3% |
| | Throughput (`MB/s`) | 22.6 | 22.6 | +0.1% | 21.8 | -3.5% |
| | VM CPU avg / Peak Mem | 0.37% / 0.87% | 0.35% / 0.85% | -4.4% / -2.1% | 0.35% / 0.85% | -3.6% / -1.6% |
| | Ops (`ok / err`) | 39 / 0 | 39 / 0 | | 38 / 0 | |
| **W2b** `50 × 2 MiB` @ `4 MiB` stride, concurrent (`180 s`) † | Latency P50 / P90 / P99 | 3.34 s / 3.41 s / 3.46 s | 3.40 s / 3.45 s / 3.49 s | +1.9% / +1.3% / +1.0% | 3.10 s / 3.41 s / 3.51 s | -7.0% / -0.0% / +1.5% |
| | Throughput (`MB/s`) | 31.3 | 30.9 | -1.2% | 33.2 | +6.1% |
| | VM CPU avg / Peak Mem | 0.40% / 0.85% | 0.78% / 1.02% | +93.1% / +20.2% | 0.37% / 0.87% | -7.0% / +2.3% |
| | Ops (`ok / err`) | 54 / 0 | 54 / 0 | | 57 / 0 | |
| **W3** Random `8 KiB` on open `100 MiB` (`180 s`) | Latency P50 / P90 / P99 | 41.3 ms / 46.7 ms / 57.4 ms | 40.6 ms / 43.5 ms / 48.4 ms | -1.7% / -6.9% / -15.7% | 43.1 ms / 47.7 ms / 54.6 ms | +4.4% / +2.1% / -4.7% |
| | Throughput (`ops/s`) | 23.54 | 24.28 | +3.2% | 22.97 | -2.4% |
| | VM CPU avg / Peak Mem | 0.33% / 0.83% | 0.35% / 0.83% | +8.6% / -0.3% | 0.49% / 0.95% | +49.6% / +14.6% |
| | Ops (`ok / err`) | 4,237 / 0 | 4,371 / 0 | | 4,135 / 0 | |
| **W4** `BidiRead` `16 MiB` chunks, 48 threads (`300 s`) † — **run 1** | Range P50 / P90 / P99 | 767.7 ms / 959.3 ms / 1.20 s | 611.8 ms / 2.47 s / 2.98 s | -20.3% / +157.1% / +147.4% | 3.12 s / 3.45 s / 3.89 s ‡ | +307.0% / +259.2% / +222.8% |
| | Throughput (`MB/s`) | **1,062.9** | 681.6 | -35.9% | 253.3 ‡ | -76.2% |
| | VM CPU avg / Peak Mem | 2.58% / 1.05% | 2.02% / 1.09% | -21.8% / +3.4% | 1.24% / 1.11% | -52.0% / +4.9% |
| | Objects (`ok / err`) | 1,500 / 0 | 952 / 0 | | 336 / 0 | |
| **W4** — **run 2** (same cells, repeated 25 min later) | Range P50 / P90 / P99 | 779.5 ms / 1.09 s / 1.34 s | 5.01 s / 5.78 s / 7.46 s ‡ | +543.1% / +432.2% / +456.9% | 591.9 ms / 1.84 s / 2.00 s | -24.1% / +69.0% / +49.4% |
| | Throughput (`MB/s`) | **1,043.0** | 155.5 ‡ | -85.1% | 761.4 | -27.0% |
| | VM CPU avg / Peak Mem | 2.39% / 0.88% | 0.87% / 1.04% | -63.7% / +18.9% | 2.17% / 1.04% | -9.2% / +18.3% |
| | Objects (`ok / err`) | 1,471 / 0 | 192 / 0 | | 1,065 / 0 | |

† Binary `21871eec65`: each worker opens its `ObjectDescriptor` once, outside the timed operation, and W4 checks the deadline before every range; every such cell recorded `measured_opens = 0`. Unmarked rows: binary `bb95000aff`.
‡ Throughput cap: all 48 workers advance in lockstep (each moved the same `1.57 GB` in run 1 / `0.97 GB` in run 2), i.e. one shared bottleneck. It hit one UDS hosting per run (a different one each time) and never native; see the 48-thread `BidiRead` root-cause item in [section 4.5](#45-root-cause-analysis).

### 4.3 Control-Plane Listing — Workloads W6a and W6b

Source: [full-doc-tables.md](results/rev4-c4-20261105/full-doc-tables.md) (`6` cells, `0` errors).

| Workload | Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---|---:|---:|---:|---:|---:|
| **W6a** `ListObjects`, 100 items (`us-west1` `STANDARD`, `180 s`) | Latency P50 / P90 / P99 | 60.2 ms / 70.3 ms / 86.5 ms | 57.9 ms / 65.0 ms / 126.8 ms | -3.8% / -7.5% / +46.6% | 57.7 ms / 64.7 ms / 115.3 ms | -4.0% / -7.9% / +33.3% |
| | Throughput (`ops/s`) | 16.03 | 16.65 | +3.9% | 16.82 | +4.9% |
| | VM CPU avg / Peak Mem | 0.51% / 2.22% | 0.35% / 0.86% | -31.9% / -61.3% | 0.32% / 0.84% | -37.7% / -62.1% |
| | Ops (`ok / err`) | 2,886 / 0 | 2,998 / 0 | | 3,028 / 0 | |
| **W6b** `ListBuckets`, 100 items (project-scoped, `180 s`) | Latency P50 / P90 / P99 | 79.0 ms / 91.9 ms / 119.6 ms | 80.5 ms / 95.7 ms / 122.8 ms | +2.0% / +4.2% / +2.7% | 77.5 ms / 91.9 ms / 114.0 ms | -1.9% / +0.1% / -4.6% |
| | Throughput (`ops/s`) | 12.32 | 12.14 | -1.5% | 12.57 | +2.0% |
| | VM CPU avg / Peak Mem | 0.29% / 0.83% | 0.34% / 0.83% | +17.1% / -0.6% | 0.31% / 0.83% | +6.6% / +0.0% |
| | Ops (`ok / err`) | 2,218 / 0 | 2,185 / 0 | | 2,263 / 0 | |

### 4.4 Process-Level CPU and Peak RSS Attribution (`getrusage`, `VmHWM`, Server `Stats` RPC)

Total CPU sums user + kernel seconds across the C++ application and the Rust server (for `uds-inproc`, `/proc/self/task/<tid>/stat` separates the embedded Tokio threads from the C++ threads inside the single OS process):

| Cell & Metric | Native C++ | `uds-sidecar` (C++ app + Rust server) | `uds-inproc` (C++ + Tokio in one process) |
|---|---:|---:|---:|
| **Zonal W4 (`BidiRead` 48t)** — CPU `ns/byte` | **3.35** (`1,118.3 s / 333.4 GB`) | **3.52** (`223.4 + 680.3 s / 256.8 GB`) | **3.51** (`233.8 + 750.7 s / 280.3 GB`) |
| **Zonal W4 (`BidiRead` 48t)** — Peak RSS | `250.2 MiB` | `342.0 MiB` app + `889.6 MiB` server | `1,365.8 MiB` |
| **Regional W4 (`BidiRead` 48t)** — CPU `ns/byte`, run 1 / run 2 | **3.21** / **3.22** (`1,025.6 s / 319.5 GB`; `1,010.6 s / 313.5 GB`) | **3.61** / **3.74** (`183.8 + 557.0 s / 205.0 GB`; `49.6 + 126.5 s / 47.1 GB`) | **3.89** / **3.51** (`82.9 + 214.0 s / 76.4 GB`; `190.9 + 611.3 s / 228.8 GB`) |
| **Regional W4 (`BidiRead` 48t)** — Peak RSS, run 1 / run 2 | `230.5` / `240.7 MiB` | `330.7` / `335.4 MiB` app + `1,187.9` / `940.8 MiB` server | `1,464.5` / `1,633.0 MiB` |
| **Zonal W5 (`BidiWrite` 48t)** — CPU `ns/byte` | **0.97** (`396.4 s / 410.0 GB`) | **2.78** (`493.1 + 571.8 s / 382.6 GB`) | **2.71** (`360.0 + 443.0 s / 296.4 GB`) |
| **Zonal W5 (`BidiWrite` 48t)** — Peak RSS | `391.6 MiB` | `1,320.8 MiB` app + **`8,703.3 MiB` server** | **`9,532.1 MiB`** |
| **W2b (`50 × 2 MiB` concurrent, zonal / regional)** — CPU `ns/byte` | `3.58` / `3.71` | `2.37` / `2.62` | `2.13` / `2.68` |
| **W3 (`8 KiB` random, zonal / regional)** — CPU `µs/op` | `0.79` / `0.84` | `0.94` / `0.95` | `0.97` / `0.94` |
| **Server threads (at default 192 workers)** | — (`27–115` app threads) | `193` server (`+ 27–68` app) | `216–260` total |

### 4.5 Root-Cause Analysis

1. **Zonal `Open` (`W1`: `1.69 s` native vs `661.0 ms` / `623.7 ms` UDS) — verified in SDK source:**
   - Over Cloudpath (`storage.googleapis.com`), an initial `BidiReadObject` request for a zonal bucket carries no `routing_token` and receives `ABORTED` with `BidiReadObjectRedirectedError` (`routing_token` + `read_handle`). Both SDKs route that initial redirect through their generic `ExponentialBackoff` sleep before dialing the redirected stream:
     - **`google-cloud-cpp`:** `AsyncConnectionImpl` ([connection_impl.cc](../google/cloud/storage/internal/async/connection_impl.cc#L197-L220)) applies `ApplyRedirectErrors` and enters `AsyncRetryLoop` with 3-argument `ExponentialBackoffPolicy(1s, 5min, 2.0)` ([backoff_policy.h](../google/cloud/internal/backoff_policy.h#L129-L133), [backoff_policy.cc](../google/cloud/internal/backoff_policy.cc#L49-L53)), which samples uniformly from **`[1.0 s, 2.0 s]`** (mean `1.50 s`, P99 `1.99 s`) + 2 Cloudpath RTTs → **`1.69 s` P50 / `2.22 s` P99**.
     - **`google-cloud-rust`:** `Connector::connect` ([connector.rs](https://github.com/googleapis/google-cloud-rust/blob/18d8711442/src/storage/src/storage/bidi/connector.rs#L83-L110)) uses `ExponentialBackoffBuilder::new().with_initial_delay(Duration::from_secs(1))` ([backoff_policy.rs](https://github.com/googleapis/google-cloud-rust/blob/18d8711442/src/storage/src/backoff_policy.rs#L26-L33)), which samples full jitter from **`[0.0 s, 1.0 s]`** (mean `0.50 s`, P99 `0.99 s`) + 2 Cloudpath RTTs → **`661.0 ms` / `623.7 ms` P50, `1.11 s` P99**.
   - This is a per-stream setup cost, paid once per `ObjectDescriptor` and not per read, which is why zonal serial `W2a` (descriptor opened outside the timed operation) is at parity: `4.29 s` native vs `4.27 s` / `4.24 s`, within `±1.1%`.

2. **48-thread `BidiRead` (`W4`) — native C++ leads with long-lived sessions; the Rust core intermittently caps at one connection's worth of throughput:**
   - **Session lifetime matters:** re-opening the `ObjectDescriptor` for every `200 MiB` object costs native C++ the zonal redirect backoff each time (`1.69 s` P50 per open, about `19%` of an object's time). With one long-lived descriptor per worker, native reaches `1,108.0 MB/s` zonal and `1,043–1,063 MB/s` regional with a per-range P99 of `1.2–1.3 s`.
   - **Zonal (one run):** native `1,108.0 MB/s` vs `852.9 MB/s` (`-23.0%`, sidecar) / `932.5 MB/s` (`-15.8%`, in-process), with higher UDS per-range latency at every percentile (`+16%` to `+84%`).
   - **Regional (two runs, ‡):** native was stable (`1,062.9` and `1,043.0 MB/s`). In each run one UDS hosting ran normally (`681.6` sidecar in run 1, `761.4 MB/s` in-process in run 2) and the other was capped (`253.3 MB/s` in-process in run 1, `155.5 MB/s` sidecar in run 2). In the capped cells all 48 workers advance in lockstep — each moved the same bytes, and completions arrive as batches of exactly 48 objects (`336 MB/s` per 30 s bucket) — which is the signature of all streams sharing one bottleneck. In the uncapped UDS cells only 15–16 of 48 workers ran fast, while native had all 48 fast in both runs.
   - **CPU per byte stays flat** across all nine `W4` cells (`3.21–3.89 ns/byte`; process-attribution table in [section 4.4](#44-process-level-cpu-and-peak-rss-attribution-getrusage-vmhwm-server-stats-rpc)): the UDS hop does not add CPU; the throughput gap is about how streams reach the network, not CPU.
   - *Unverified hypothesis:* the Rust core spreads requests over its 4 Cloudpath connections with tonic's `Channel::balance_channel` ([grpc.rs](https://github.com/googleapis/google-cloud-rust/blob/18d8711442/src/gax-internal/src/grpc.rs#L627-L635)), which only selects among endpoints that are already connected; 48 long-lived streams opened in one burst during warm-up can then concentrate on whichever connection came up first and stay there for the whole window. google-cloud-cpp assigns each call round-robin to one of its 4 channels. The collected artifacts have no per-connection byte counts, so this is not confirmed; it is the first follow-up action item in [section 6.3](#63-follow-up-action-items).

3. **Zonal concurrent `50 × 2 MiB` (`W2b`) is faster over UDS (`-23.9%` / `-16.0%` P50) — observed, cause not verified:** both sides reuse one descriptor per worker and issue the same 50 concurrent ranges; the regional bucket shows parity (`+1.9%` / `-7.0%`), and native spends more CPU per byte (`3.58` vs `2.37` / `2.13 ns/byte` zonal). One run per bucket; treat the size of the zonal gap as unconfirmed.

4. **48-thread `BidiWrite` (`W5`) — 2.8× CPU per byte and `8.7–9.5 GiB` peak RSS (verified in source):**
   - Native C++ slices one shared `200 MB` zero-copy `absl::MakeCordFromExternal` payload across all 48 threads ([uds_poc_bench.cc](bench/uds_poc_bench.cc#L481-L498)), spending `0.97 ns/byte` and peaking at `391.6 MiB` RSS.
   - Over UDS, C++ materializes `std::string` chunks and copies them into `gcscore::v1::WriteRequest` protobuf messages ([SplitPayloadChunks](cpp/src/async_connection.cc#L600-L631): `1.29 ns/byte` app CPU, `1,320.8 MiB` app peak RSS), while the Rust server decodes each chunk into fresh heap `Bytes`, coalesces `2 MiB` frames, queues up to `8 MiB` in the foreground channel ([transport.rs](https://github.com/googleapis/google-cloud-rust/blob/18d8711442/src/storage/src/storage/bidi_write/transport.rs#L36-L44)), and retains unacknowledged chunks in a `32 MiB` per-stream `ReplayBuffer` ([replay_buffer.rs](https://github.com/googleapis/google-cloud-rust/blob/18d8711442/src/storage/src/storage/bidi_write/replay_buffer.rs#L27)) until a watermark `state_lookup` response trims them ([worker.rs](https://github.com/googleapis/google-cloud-rust/blob/18d8711442/src/storage/src/storage/bidi_write/worker.rs#L163-L171)) (`1.49 ns/byte` server CPU). Across 48 concurrent streams (`~2.0 GiB` of live per-stream queues plus `h2`/TLS send buffers) allocated and freed across 192 Tokio worker threads and glibc per-thread malloc arenas, resident memory peaks at **`8,703.3 MiB` (`uds-sidecar` server)** and **`9,532.1 MiB` (`uds-inproc`)**, or `2.40%` / `2.18%` of VM RAM versus `0.95%` native.

5. **Connection count:** both comparators are pinned to **4 Cloudpath connections** (`GrpcNumChannelsOption(4)` natively, `--grpc-subchannels=4` in the Rust server). Both SDKs default to one connection per vCPU (`192` on this VM), so absolute throughput here is lower than a default-configured client would reach; the comparison between comparators is like-for-like.

### 4.6 Tokio Worker-Thread Sensitivity (`--worker-threads=16` vs Default `192`)

Measured at identical durations (`180 s` for W3, `300 s` for W4) on `gs://kalragauri-uds-poc-usw1-zonal` ([wt16-sensitivity.md](results/rev4-c4-20261105/wt16-sensitivity.md), `4` cells, `0` errors):

| Cell | Comparator | Latency P50 / P90 / P99 (`192` → `16` workers) | Latency % Diff | Throughput (`192` → `16` workers) | VM CPU avg | Server Threads |
|---|---|---|---:|---|---:|---:|
| **W3** Random `8 KiB` (`per op`) | `uds-sidecar` | `37.1 / 39.6 / 42.3 ms` → `37.0 / 39.1 / 40.1 ms` | `-0.3% / -1.2% / -5.2%` | `26.59` → `26.79 ops/s` (`+0.8%`) | `0.35%` → `0.35%` | `193` → **`17`** |
| **W3** Random `8 KiB` (`per op`) | `uds-inproc` | `36.0 / 39.0 / 45.5 ms` → `35.4 / 36.3 / 39.4 ms` | `-1.7% / -6.9% / -13.3%` | `27.22` → `28.06 ops/s` (`+3.1%`) | `0.42%` → `0.60%` | `216` → **`40`** |
| **W4** `BidiRead` 48t (`per range`) | `uds-sidecar` | `554.0 ms / 4.76 s / 6.55 s` → `576.9 ms / 5.05 s / 6.81 s` | `+4.1% / +6.1% / +4.0%` | `457.5` → `467.2 MB/s` (`+2.1%`) | `1.56%` → `1.60%` | `193` → **`17`** |
| **W4** `BidiRead` 48t (`per range`) | `uds-inproc` | `591.5 ms / 1.96 s / 3.48 s` → `609.1 ms / 2.06 s / 3.55 s` | `+3.0% / +4.9% / +1.9%` | `774.2` → `754.9 MB/s` (`-2.5%`) | `2.25%` → `1.99%` | `260` → **`84`** |

**Takeaway:** Sizing Tokio to 16 workers matches the 192-worker default within run-to-run noise (`-2.5%` to `+3.1%` throughput, `-13.3%` to `+6.1%` latency) while reducing server threads by **11.4×** in sidecar mode (`193 → 17`) and **3.1×** in-process (`260 → 84`). *(Note: an initial `180 s` W4 sensitivity run had appeared `-13.5%` / `-33.0%` slower solely because post-deadline drain occupies a larger fraction of a `180 s` window than a `300 s` base window; re-running at the matching `300 s` duration closed the gap. Both arms used binary `bb95000aff`, which re-opened a descriptor per W4 object, so the W4 values here differ from the W4 rows above; the comparison between the two arms is like-for-like.)*

---

## 5. Packaging, Footprint, Build Cost, and Customer-Environment Probes

Source: [packaging-probes.log](results/rev4-c4-20261105/packaging-probes.log) (Linux x86-64, GCC 13, Rust release profile, gRPC `1.84`, static vcpkg dependencies).

### 5.1 Binary and Archive Sizes

| Artifact | Unstripped | Stripped (`strip -s`) | Notes |
|---|---:|---:|---|
| `gcs-core-server` (standalone Rust daemon) | `42.57 MiB` | **`30.49 MiB`** | Shipped alongside the C++ binary in `uds-sidecar` mode |
| `quickstart` (sidecar-mode C++ app) | `44.07 MiB` | `33.10 MiB` | Baseline C++ app linking `google-cloud-cpp` + UDS stub |
| `quickstart_inproc` (in-process C++ app) | `89.37 MiB` | **`61.83 MiB`** | **`+28.73 MiB` stripped** vs `quickstart`; identical `ldd` shared-lib set |
| `cold_start` / `cold_start_inproc` | `29.36` / `74.67 MiB` | `22.81` / `51.56 MiB` | **`+28.75 MiB` stripped** in-process delta |
| `libstorage_grpc_uds.a` (Rust `staticlib`) | `122.81 MiB` | — | Linked into `*_inproc` targets |
| C++ UDS adapter archives (`libgcs_core*.a`) | `0.41–1.62 MiB` | — | `libgcs_core_protos.a` is `1.35 MiB` |

### 5.2 Cold `Client::Create()` Latency (`n = 30` Fresh Processes) and Idle Footprint

| Hosting Mode | Cold `Create()` P50 | Cold `Create()` P99 | Idle App Footprint | Idle Server Footprint |
|---|---:|---:|---|---|
| `uds-sidecar` (`1:1` child, default 192 workers) | `102.9 ms` | `123.9 ms` | `1.37 GiB` VmSize, `16.3 MiB` RSS, `20` threads | `12.41 GiB` VmSize, `18.0 MiB` RSS, `193` threads |
| `uds-sidecar` (`1:1` child, `--worker-threads=8`) | `98.2 ms` | `124.8 ms` | `1.37 GiB` VmSize, `16.1 MiB` RSS, `20` threads | `563.0 MiB` VmSize, `15.3 MiB` RSS, `9` threads |
| `uds-sidecar` (attach to warm per-UID daemon) | **`5.8 ms`** | **`6.6 ms`** | `1.29 GiB` VmSize, `16.3 MiB` RSS, `19` threads | Shared across processes (`18.0 MiB` RSS) |
| `uds-inproc` (default 192 workers) | `22.2 ms` | `23.5 ms` | `13.76 GiB` VmSize, `34.4 MiB` RSS, `212` threads | Embedded in app process |
| `uds-inproc` (`--worker-threads=8`) | **`14.8 ms`** | **`16.7 ms`** | `1.90 GiB` VmSize, `31.3 MiB` RSS, `28` threads | Embedded in app process |

### 5.3 Full and Incremental Build Times (`c4-standard-192`, `-j192`)

| Build Step | Wall Time |
|---|---:|
| Cold CMake configure + vcpkg source build of 18 C++ ports (`grpc`, `protobuf`, `openssl`, `abseil`, `curl`, `crc32c`, ...) | `345.9 s` |
| Cold `cargo build --release` (`storage-grpc-uds`, 276 crates) + CMake Ninja build (`-j192`) | `107.9 s` |
| 1-file incremental rebuild: C++ [client.cc](cpp/src/client.cc) → `quickstart` / `quickstart_inproc` | **`5.3 s` / `6.5 s`** |
| 1-file incremental rebuild: Rust [service.rs](https://github.com/googleapis/google-cloud-rust/pull/7350) → `quickstart` / `quickstart_inproc` | **`47.9 s` / `49.4 s`** |
| 1-file incremental rebuild: wire contract `gcscore.proto` → both `quickstart` and `quickstart_inproc` | **`62.6 s`** |

### 5.4 Customer-Environment Deployment Probes (`7/7` PASS)

| Probe | Environment / Failure Scenario | Observed Result |
|---|---|---|
| **Socket path limit** | `sockaddr_un::sun_path` > 107 bytes (`140`-byte path) | **PASS:** Fails fast with `INVALID_ARGUMENT: socket path is 140 bytes; the limit is 107` |
| **Stale socket vs regular file** | Dead daemon's leftover socket file; non-socket regular file at socket path | **PASS:** Stale socket unlinked under `.lock` `flock` and replaced; regular file preserved with `FAILED_PRECONDITION` |
| **Parent crash (`SIGKILL`)** | `1:1` child sidecar (`--pdeathsig=true`) when parent receives `SIGKILL` | **PASS:** Child `gcs-core-server` exits in **`13 ms`** (`<= 1,000 ms` gate) via `PR_SET_PDEATHSIG(SIGTERM)` |
| **Multi-tenant UID isolation** | Different OS UID connecting to `gcs-core-server` socket | **PASS:** Mode `0600` + `SO_PEERCRED` peer-UID check rejects cross-UID client with `PERMISSION_DENIED` |
| **`noexec` `/tmp` mount** | Server binary on `noexec` tmpfs vs socket file on `noexec` tmpfs | **PASS:** Binary on `noexec` rejected cleanly (`FAILED_PRECONDITION`); socket on `noexec` works when binary is on an executable mount |
| **Read-only filesystem** | Read-only `$XDG_RUNTIME_DIR` without writable socket directory | **PASS:** Filesystem sidecar reports `ReadOnlyFilesystem` cleanly; **`uds-inproc` (`unix-abstract:`) succeeds with zero filesystem writes** |
| **Container PID namespace** | `unshare --pid --fork` container namespace | **PASS:** Child sidecar spawns cleanly (`core_pid=10`) and serves RPCs in `11.5 ms` |

---

## 6. Maintenance, Debuggability, and Architectural Constraints

### 6.1 Maintenance and Debuggability (Observed in This POC)

- **Dual build graph in every developer and CI workflow:** Building from source requires CMake + Ninja + vcpkg/Conan **plus** `cargo` (`rustc >= 1.91`) and `protoc`. Neither vcpkg nor Conan packages `gcs-core-server` today, so CMake invokes `cargo` via `FetchContent`, increasing incremental Rust/proto edit-compile cycles by **9–12×** over pure C++ (`47.9–62.6 s` vs `5.3 s`).
- **Cross-repository coordination cost:** Even for this POC, **5 server commits** on top of [googleapis/google-cloud-rust#7350](https://github.com/googleapis/google-cloud-rust/pull/7350) were required before the C++ adapters could size the Tokio pool (`--worker-threads`), pass flags in-process, delete objects (`DeleteObject`), avoid startup races (listening only after `Storage`/`StorageControl` initialize), and safely recover stale sockets ([googleapis/google-cloud-rust#7390](https://github.com/googleapis/google-cloud-rust/pull/7390)). Every new Storage feature touches three layers across two repositories (`google-cloud-rust` service → `gcscore.v1` proto → `google-cloud-cpp` adapter).
- **Split observability and cross-runtime debugging:** `GOOGLE_CLOUD_CPP_ENABLE_TRACING` and `GRPC_TRACE` terminate at the UDS boundary while `RUST_LOG` covers the server side; attributing CPU and memory in sidecar mode required adding a dedicated `Stats` RPC to `gcscore.v1`. During smoke runs, diagnosing an exit-time `double free or corruption` crash (`exit=250`) required `gdb` backtraces on the VM to isolate static OpenSSL `3.6.2`'s `atexit` (`OPENSSL_cleanup`) racing gRPC `PosixEventEngine` background threads (resolved via `OPENSSL_init_crypto(OPENSSL_INIT_NO_ATEXIT, nullptr)` in [uds_poc_bench.cc](bench/uds_poc_bench.cc#L1002-L1013)).

### 6.2 Architectural Constraints and Open Upstream Items

1. **Standard Regional `BidiReadObject` is enabled per project:** On projects where it is not enabled (such as `kalragauri-sdk-test`), regional `BidiReadObject` fails with `PERMISSION_DENIED: The BidiReadObject RPC is not yet available for general use, or for project ...`. Zonal buckets are unaffected, and enabled projects succeed as measured in the Standard Regional benchmark table ([section 4.2](#42-standard-regional-bucket-us-west1-standard-non-hns--workloads-w1w4)).
2. **Cloudpath only (no DirectPath in the Rust core yet):** `google-cloud-rust` does not yet support DirectPath; C++ production workloads that rely on DirectPath cannot migrate until the Rust core reaches parity.
3. **Write-path memory and persistence acks:** High-concurrency appendable uploads use `8.7–9.5 GiB` peak RSS vs `392 MiB` natively and `2.8×` CPU/byte, and `AsyncWriter::Flush` / `Query` cannot report server-persisted byte offsets until bidirectional write acks land ([googleapis/google-cloud-rust#7357](https://github.com/googleapis/google-cloud-rust/pull/7357)).
4. **Regular-bucket `BidiWriteObject`:** Not yet implemented in `google-cloud-storage` (Rust).
5. **Default Tokio thread count scales with host vCPUs (`192` on `c4-standard-192`):** Reserves `12.4 GiB` of virtual address space for thread stacks with zero throughput benefit over 16 workers in the Tokio worker-thread sensitivity runs ([section 4.6](#46-tokio-worker-thread-sensitivity---worker-threads16-vs-default-192)); `gcs-core-server` should cap its default worker pool (e.g. `min(num_cpus, 16)`).
6. **Linux-only transport primitives and unwired shared-memory plane:** `AF_UNIX` `SO_PEERCRED`, `PR_SET_PDEATHSIG`, `unix-abstract:`, and `memfd` + `SCM_RIGHTS` (`ReadObjectShm`) are Linux-specific; macOS and Windows need platform equivalents, and `ReadObjectShm` is not yet wired into `ReadPayload` in the C++ adapters.
7. **48 long-lived read streams can be capped at one connection's throughput in the Rust core (cause unverified):** in each of the two regional 48-thread `BidiRead` runs one UDS hosting moved all 48 streams in lockstep at `155–253 MB/s` while native C++ on the same 4 connections reached `1,043–1,063 MB/s`. Applications that keep many `ObjectDescriptor`s open would see this as an intermittent throughput ceiling.

### 6.3 Follow-Up Action Items

1. **Confirm or rule out the single-connection cap on 48 long-lived streams (constraint 7):** re-run the regional 48-thread `BidiRead` for `uds-sidecar` and `uds-inproc` while sampling per-socket bytes on the 4 Cloudpath connections every 10 s (`ss -tin` on the server process), and once with the warm-up opens staggered. If the streams concentrate on one connection, the fix belongs in `google-cloud-rust` (`Channel::balance_channel` selecting only already-connected endpoints, [grpc.rs](https://github.com/googleapis/google-cloud-rust/blob/18d8711442/src/gax-internal/src/grpc.rs#L627-L635)): wait for all subchannels before serving, or assign long-lived streams round-robin. Owner: C++ UDS POC (kalragauri). Cost: about 15 minutes of VM time.
2. **Repeat zonal concurrent `50 × 2 MiB`** once to confirm whether the `-16%` to `-24%` UDS advantage holds (one run so far; regional shows parity).

---

## 7. Reproduction

```bash
# 1. Workstation build & hermetic test suite
cd storage-uds-poc
cmake -S . -B cmake-out -GNinja -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE=$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake
cmake --build cmake-out -j"$(nproc)"
ctest --test-dir cmake-out --output-on-failure

# 2. GCE c4-standard-192 benchmark phases (after scripts/vm_build.sh)
PHASE=probes|smoke|full|regional|wt \
  OPS=<ops-bucket/prefix> REGIONAL=<std-regional-bucket> ZONAL=<rapid-zonal-bucket> \
  BUCKET_PREFIX=<list-bucket-prefix> WORKER_THREADS=<0|16> \
  [WORKLOADS=read50x2x4-serial,read50x2x4-concurrent,bidi-read-48t] \
  bash scripts/vm_run.sh

# 3. Render summary, per-workload, and sensitivity tables
python3 scripts/analyze.py RESULTS_DIR
python3 scripts/analyze.py RESULTS_DIR --doc-tables
python3 scripts/analyze.py WT_RESULTS_DIR --sensitivity FULL_RESULTS_DIR
python3 scripts/analyze.py RERUN_DIR --base RESULTS_DIR [--doc-tables]
```

Committed outputs: [full-summary.md](results/rev5-c4-20261106/full-summary.md), [full-doc-tables.md](results/rev5-c4-20261106/full-doc-tables.md), [regional-summary.md](results/rev5-c4-20261106/regional-summary.md), [regional-doc-tables.md](results/rev5-c4-20261106/regional-doc-tables.md), [regional-w4-run2-doc-tables.md](results/rev5-c4-20261106/regional-w4-run2-doc-tables.md), and the smoke checks of the `21871eec65` build in [parity-smoke.log](results/rev5-c4-20261106/parity-smoke.log). From the `bb95000aff` run: [wt16-sensitivity.md](results/rev4-c4-20261105/wt16-sensitivity.md), [packaging-probes.log](results/rev4-c4-20261105/packaging-probes.log), and [02-probes.log](results/rev4-c4-20261105/02-probes.log). Raw per-op CSVs, `.summary.json`, `.monitoring.json`, and `.path.json` artifacts are archived in `gs://kalragauri-uds-poc-ops/c4/results/`.
