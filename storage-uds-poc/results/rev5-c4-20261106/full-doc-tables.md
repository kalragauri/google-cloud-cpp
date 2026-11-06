# Per-workload tables (`full-parity`)

Cells re-measured in `full-parity` replace the same cells of `full`: `bidi-read-48t-zonal`, `read50x2x4-concurrent-zonal`, `read50x2x4-serial-zonal`. All other cells come from `full`.

Base run (`full`):

```text
host=kalragauri-uds-poc-c4 project=kalragauri-sdk-test instance_id=2425645790610435742 zone=us-central1-b machine=c4-standard-192 cpus=192
phase=full regional=kalragauri-uds-poc-usw1 zonal=kalragauri-uds-poc-usw1-zonal bucket_prefix=kalragauri-uds-list- worker_threads=0 start=2026-11-05T05:55:44Z
ops_agent=active
cpp_sha=36919ee42327802f1d1824b5e01a11031d0335f7 rust_sha=e432ad1de767f8b8094de70fcb3113ccba67310d
kernel=7.0.0-1014-gcp glibc=2.39
end=2026-11-05T07:32:43Z failed_runs=0
```

Overlay run (`full-parity`):

```text
host=kalragauri-uds-poc-c4 project=kalragauri-sdk-test instance_id=2425645790610435742 zone=us-central1-b machine=c4-standard-192 cpus=192
phase=full regional=kalragauri-uds-poc-usw1 zonal=kalragauri-uds-poc-usw1-zonal bucket_prefix=kalragauri-uds-list- worker_threads=0 monitoring_flush_secs=240 workloads=read50x2x4-serial,read50x2x4-concurrent,bidi-read-48t start=2026-11-06T06:48:46Z
ops_agent=active
cpp_sha=c7e65b2b6a423541767cf3d55d444bb0a97f9788 rust_sha=e432ad1de767f8b8094de70fcb3113ccba67310d
kernel=7.0.0-1014-gcp glibc=2.39
end=2026-11-06T07:28:27Z failed_runs=0
```

`% Diff` is `(UDS - Native) / Native`; negative latency, CPU, and memory deltas and positive throughput deltas favor the UDS comparator. Latency rows name their basis: `per op` is one benchmark operation; `per range` (W4) is one `ObjectDescriptor::Read` range, 13 per 200 MiB object (twelve of 16 MiB and a final 8 MiB range). `‡` marks a Monitoring series with fewer points than the window has 60 s buckets.

### W1 — Open a 200 MiB object (`AsyncClient::Open`)

Bucket: Zonal (RAPID) `gs://kalragauri-uds-poc-usw1-zonal` · cell `open-zonal`

| Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---:|---:|---:|---:|---:|
| Latency P50 (per op) | 1.69 s | 661.0 ms | -60.9% | 623.7 ms | -63.1% |
| Latency P90 (per op) | 2.05 s | 1.01 s | -51.0% | 1.04 s | -49.6% |
| Latency P99 (per op) | 2.22 s | 1.11 s | -50.0% | 1.11 s | -50.0% |
| Throughput | 0.58 ops/s | 1.55 ops/s | +168.5% | 1.57 ops/s | +171.8% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 0.26% | 0.33% | +30.8% | 0.32% | +24.1% |
| Peak Memory (`memory/percent_used`, max) | 2.08% | 1.28% | -38.7% | 1.06% | -49.2% |
| Ops (ok / err) | 104 / 0 | 280 / 0 |  | 284 / 0 |  |

### W2a — 50 × 2 MiB ranges at a 4 MiB stride, serial

Bucket: Zonal (RAPID) `gs://kalragauri-uds-poc-usw1-zonal` · cell `read50x2x4-serial-zonal`

| Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---:|---:|---:|---:|---:|
| Latency P50 (per op) | 4.29 s | 4.27 s | -0.4% | 4.24 s | -1.1% |
| Latency P90 (per op) | 4.34 s | 4.34 s | -0.0% | 4.30 s | -0.9% |
| Latency P99 (per op) | 4.40 s | 4.56 s | +3.6% | 4.37 s | -0.8% |
| Throughput | 24.5 MB/s | 24.5 MB/s | -0.0% | 24.7 MB/s | +0.7% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 0.37% | 0.61% | +64.2% | 0.36% | -4.3% |
| Peak Memory (`memory/percent_used`, max) | 0.85% | 0.86% | +1.9% | 0.86% | +2.3% |
| Ops (ok / err) | 43 / 0 | 43 / 0 |  | 43 / 0 |  |

### W2b — 50 × 2 MiB ranges at a 4 MiB stride, concurrent

Bucket: Zonal (RAPID) `gs://kalragauri-uds-poc-usw1-zonal` · cell `read50x2x4-concurrent-zonal`

| Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---:|---:|---:|---:|---:|
| Latency P50 (per op) | 3.79 s | 2.88 s | -23.9% | 3.18 s | -16.0% |
| Latency P90 (per op) | 4.04 s | 2.98 s | -26.3% | 3.36 s | -17.0% |
| Latency P99 (per op) | 4.47 s | 3.11 s | -30.4% | 3.37 s | -24.6% |
| Throughput | 27.4 MB/s | 36.2 MB/s | +31.9% | 32.9 MB/s | +20.0% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 0.40% | 0.69% | +74.9% | 0.36% | -9.4% |
| Peak Memory (`memory/percent_used`, max) | 0.87% | 0.95% | +9.3% | 0.86% | -0.2% |
| Ops (ok / err) | 48 / 0 | 63 / 0 |  | 57 / 0 |  |

### W3 — Random 8 KiB ranges on an open 100 MiB object

Bucket: Zonal (RAPID) `gs://kalragauri-uds-poc-usw1-zonal` · cell `random8k-zonal`

| Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---:|---:|---:|---:|---:|
| Latency P50 (per op) | 35.4 ms | 37.1 ms | +4.9% | 36.0 ms | +1.8% |
| Latency P90 (per op) | 36.2 ms | 39.6 ms | +9.5% | 39.0 ms | +7.8% |
| Latency P99 (per op) | 38.0 ms | 42.3 ms | +11.3% | 45.5 ms | +19.7% |
| Throughput | 28.15 ops/s | 26.59 ops/s | -5.6% | 27.22 ops/s | -3.3% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 0.32% | 0.35% | +9.5% | 0.42% | +28.5% |
| Peak Memory (`memory/percent_used`, max) | 0.80% | 0.82% | +2.1% | 0.82% | +2.1% |
| Ops (ok / err) | 5068 / 0 | 4786 / 0 |  | 4899 / 0 |  |

### W4 — Read 16 MiB chunks of a 200 MiB object via BidiRead, 48 threads, 5 min

Bucket: Zonal (RAPID) `gs://kalragauri-uds-poc-usw1-zonal` · cell `bidi-read-48t-zonal`

| Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---:|---:|---:|---:|---:|
| Latency P50 (per range) | 565.7 ms | 886.8 ms | +56.8% | 798.9 ms | +41.2% |
| Latency P90 (per range) | 1.08 s | 1.45 s | +35.0% | 1.25 s | +16.1% |
| Latency P99 (per range) | 1.27 s | 2.33 s | +84.1% | 1.50 s | +18.4% |
| Throughput | 1,108.0 MB/s | 852.9 MB/s | -23.0% | 932.5 MB/s | -15.8% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 2.59% | 2.39% | -7.9% | 2.73% | +5.3% |
| Peak Memory (`memory/percent_used`, max) | 0.87% | 1.01% | +15.3% | 1.01% | +15.3% |
| Ops (ok / err) | 1565 / 0 | 1200 / 0 |  | 1313 / 0 |  |

### W5 — Write 200 MB objects via BidiWrite, 48 threads, 5 min

Bucket: Zonal (RAPID) `gs://kalragauri-uds-poc-usw1-zonal` · cell `bidi-write-48t-zonal`

| Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---:|---:|---:|---:|---:|
| Latency P50 (per op) | 3.98 s | 6.28 s | +57.6% | 6.65 s | +67.0% |
| Latency P90 (per op) | 15.42 s | 14.90 s | -3.4% | 22.40 s | +45.3% |
| Latency P99 (per op) | 28.83 s | 21.08 s | -26.9% | 33.68 s | +16.8% |
| Throughput | 1,335.3 MB/s | 1,240.1 MB/s | -7.1% | 957.5 MB/s | -28.3% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 1.29% | 2.27% | +75.8% | 1.80% | +39.8% |
| Peak Memory (`memory/percent_used`, max) | 0.95% | 2.40% | +152.6% | 2.18% | +129.5% |
| Ops (ok / err) | 2050 / 0 | 1913 / 0 |  | 1482 / 0 |  |

### W6a — ListObjects, 100 items

Bucket: Standard Regional `gs://kalragauri-uds-poc-usw1` · cell `list-objects-100-regional`

| Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---:|---:|---:|---:|---:|
| Latency P50 (per op) | 60.2 ms | 57.9 ms | -3.8% | 57.7 ms | -4.0% |
| Latency P90 (per op) | 70.3 ms | 65.0 ms | -7.5% | 64.7 ms | -7.9% |
| Latency P99 (per op) | 86.5 ms | 126.8 ms | +46.6% | 115.3 ms | +33.3% |
| Throughput | 16.03 ops/s | 16.65 ops/s | +3.9% | 16.82 ops/s | +4.9% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 0.51% | 0.35% | -31.9% | 0.32% | -37.7% |
| Peak Memory (`memory/percent_used`, max) | 2.22% | 0.86% | -61.3% | 0.84% | -62.1% |
| Ops (ok / err) | 2886 / 0 | 2998 / 0 |  | 3028 / 0 |  |

### W6b — ListBuckets, 100 items, project-scoped

Bucket: Project-scoped · cell `list-buckets-100-project`

| Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---:|---:|---:|---:|---:|
| Latency P50 (per op) | 79.0 ms | 80.5 ms | +2.0% | 77.5 ms | -1.9% |
| Latency P90 (per op) | 91.9 ms | 95.7 ms | +4.2% | 91.9 ms | +0.1% |
| Latency P99 (per op) | 119.6 ms | 122.8 ms | +2.7% | 114.0 ms | -4.6% |
| Throughput | 12.32 ops/s | 12.14 ops/s | -1.5% | 12.57 ops/s | +2.0% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 0.29% | 0.34% | +17.1% | 0.31% | +6.6% |
| Peak Memory (`memory/percent_used`, max) | 0.83% | 0.83% | -0.6% | 0.83% | +0.0% |
| Ops (ok / err) | 2218 / 0 | 2185 / 0 |  | 2263 / 0 |  |
