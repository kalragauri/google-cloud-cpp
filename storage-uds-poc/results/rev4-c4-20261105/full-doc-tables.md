# Worklog tables (`full`)

`% Diff` is `(UDS - Native) / Native`; negative latency, CPU, and memory deltas and positive throughput deltas favor the UDS comparator. W4 latency is per 16 MiB range.

### W1 — Open a 200 MiB object (`AsyncClient::Open`)

Bucket: Zonal (`us-west1-a`, RAPID) · cell `open-zonal`

| Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---:|---:|---:|---:|---:|
| Latency P50 | 1.69 s | 661.0 ms | -60.9% | 623.7 ms | -63.1% |
| Latency P90 | 2.05 s | 1.01 s | -51.0% | 1.04 s | -49.6% |
| Latency P99 | 2.22 s | 1.11 s | -50.0% | 1.11 s | -50.0% |
| Throughput | 0.58 ops/s | 1.55 ops/s | +168.5% | 1.57 ops/s | +171.8% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 0.26% | 0.33% | +30.8% | 0.32% | +24.1% |
| Peak Memory (`memory/percent_used`, max) | 2.08% | 1.28% | -38.7% | 1.06% | -49.2% |
| Ops (ok / err) | 104 / 0 | 280 / 0 |  | 284 / 0 |  |

### W2a — 50 × 2 MiB ranges at a 4 MiB stride, serial

Bucket: Zonal (`us-west1-a`, RAPID) · cell `read50x2x4-serial-zonal`

| Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---:|---:|---:|---:|---:|
| Latency P50 | 6.32 s | 5.60 s | -11.3% | 5.39 s | -14.7% |
| Latency P90 | 6.60 s | 6.11 s | -7.4% | 5.96 s | -9.7% |
| Latency P99 | 7.43 s | 6.51 s | -12.4% | 6.24 s | -16.0% |
| Throughput | 16.6 MB/s | 18.6 MB/s | +11.9% | 19.4 MB/s | +17.0% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 0.35% | 0.37% | +7.6% | 0.35% | -0.3% |
| Peak Memory (`memory/percent_used`, max) | 0.83% | 0.90% | +7.6% | 0.83% | -0.1% |
| Ops (ok / err) | 29 / 0 | 32 / 0 |  | 34 / 0 |  |

### W2b — 50 × 2 MiB ranges at a 4 MiB stride, concurrent

Bucket: Zonal (`us-west1-a`, RAPID) · cell `read50x2x4-concurrent-zonal`

| Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---:|---:|---:|---:|---:|
| Latency P50 | 4.82 s | 4.14 s | -14.2% | 4.12 s | -14.6% |
| Latency P90 | 5.57 s | 4.60 s | -17.4% | 4.56 s | -18.2% |
| Latency P99 | 5.83 s | 4.88 s | -16.4% | 4.60 s | -21.1% |
| Throughput | 21.2 MB/s | 25.4 MB/s | +19.9% | 25.5 MB/s | +20.5% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 0.35% | 0.38% | +7.1% | 0.34% | -3.5% |
| Peak Memory (`memory/percent_used`, max) | 0.81% | 0.83% | +2.5% | 0.83% | +2.6% |
| Ops (ok / err) | 37 / 0 | 44 / 0 |  | 44 / 0 |  |

### W3 — Random 8 KiB ranges on an open 100 MiB object

Bucket: Zonal (`us-west1-a`, RAPID) · cell `random8k-zonal`

| Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---:|---:|---:|---:|---:|
| Latency P50 | 35.4 ms | 37.1 ms | +4.9% | 36.0 ms | +1.8% |
| Latency P90 | 36.2 ms | 39.6 ms | +9.5% | 39.0 ms | +7.8% |
| Latency P99 | 38.0 ms | 42.3 ms | +11.3% | 45.5 ms | +19.7% |
| Throughput | 28.15 ops/s | 26.59 ops/s | -5.6% | 27.22 ops/s | -3.3% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 0.32% | 0.35% | +9.5% | 0.42% | +28.5% |
| Peak Memory (`memory/percent_used`, max) | 0.80% | 0.82% | +2.1% | 0.82% | +2.1% |
| Ops (ok / err) | 5068 / 0 | 4786 / 0 |  | 4899 / 0 |  |

### W4 — Read 16 MiB chunks of a 200 MiB object via BidiRead, 48 threads, 5 min

Bucket: Zonal (`us-west1-a`, RAPID) · cell `bidi-read-48t-zonal`

| Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---:|---:|---:|---:|---:|
| Latency P50 | 554.3 ms | 554.0 ms | -0.0% | 591.5 ms | +6.7% |
| Latency P90 | 3.84 s | 4.76 s | +24.0% | 1.96 s | -48.8% |
| Latency P99 | 5.12 s | 6.55 s | +28.0% | 3.48 s | -31.9% |
| Throughput | 538.0 MB/s | 457.5 MB/s | -15.0% | 774.2 MB/s | +43.9% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 1.85% | 1.56% | -15.9% | 2.25% | +21.5% |
| Peak Memory (`memory/percent_used`, max) | 0.86% | 0.94% | +9.0% | 0.95% | +10.2% |
| Ops (ok / err) | 819 / 0 | 718 / 0 |  | 1147 / 0 |  |

### W5 — Write 200 MB objects via BidiWrite, 48 threads, 5 min

Bucket: Zonal (`us-west1-a`, RAPID) · cell `bidi-write-48t-zonal`

| Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---:|---:|---:|---:|---:|
| Latency P50 | 3.98 s | 6.28 s | +57.6% | 6.65 s | +67.0% |
| Latency P90 | 15.42 s | 14.90 s | -3.4% | 22.40 s | +45.3% |
| Latency P99 | 28.83 s | 21.08 s | -26.9% | 33.68 s | +16.8% |
| Throughput | 1,335.3 MB/s | 1,240.1 MB/s | -7.1% | 957.5 MB/s | -28.3% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 1.29% | 2.27% | +75.8% | 1.80% | +39.8% |
| Peak Memory (`memory/percent_used`, max) | 0.95% | 2.40% | +152.6% | 2.18% | +129.5% |
| Ops (ok / err) | 2050 / 0 | 1913 / 0 |  | 1482 / 0 |  |

### W6a — ListObjects, 100 items

Bucket: Standard Regional (`us-west1`) · cell `list-objects-100-regional`

| Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---:|---:|---:|---:|---:|
| Latency P50 | 60.2 ms | 57.9 ms | -3.8% | 57.7 ms | -4.0% |
| Latency P90 | 70.3 ms | 65.0 ms | -7.5% | 64.7 ms | -7.9% |
| Latency P99 | 86.5 ms | 126.8 ms | +46.6% | 115.3 ms | +33.3% |
| Throughput | 16.03 ops/s | 16.65 ops/s | +3.9% | 16.82 ops/s | +4.9% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 0.51% | 0.35% | -31.9% | 0.32% | -37.7% |
| Peak Memory (`memory/percent_used`, max) | 2.22% | 0.86% | -61.3% | 0.84% | -62.1% |
| Ops (ok / err) | 2886 / 0 | 2998 / 0 |  | 3028 / 0 |  |

### W6b — ListBuckets, 100 items, project-scoped

Bucket: Project-scoped · cell `list-buckets-100-project`

| Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---:|---:|---:|---:|---:|
| Latency P50 | 79.0 ms | 80.5 ms | +2.0% | 77.5 ms | -1.9% |
| Latency P90 | 91.9 ms | 95.7 ms | +4.2% | 91.9 ms | +0.1% |
| Latency P99 | 119.6 ms | 122.8 ms | +2.7% | 114.0 ms | -4.6% |
| Throughput | 12.32 ops/s | 12.14 ops/s | -1.5% | 12.57 ops/s | +2.0% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 0.29% | 0.34% | +17.1% | 0.31% | +6.6% |
| Peak Memory (`memory/percent_used`, max) | 0.83% | 0.83% | -0.6% | 0.83% | +0.0% |
| Ops (ok / err) | 2218 / 0 | 2185 / 0 |  | 2263 / 0 |  |
