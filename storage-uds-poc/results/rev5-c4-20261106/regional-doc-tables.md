# Worklog tables (`regional-parity`)

Cells re-measured in `regional-parity` replace the same cells of `regional`: `bidi-read-48t-regional`, `read50x2x4-concurrent-regional`, `read50x2x4-serial-regional`. All other cells come from `regional`.

Base run (`regional`):

```text
host=kalragauri-uds-poc-c4 project=kalragauri-sdk-test instance_id=2425645790610435742 zone=us-central1-b machine=c4-standard-192 cpus=192
phase=regional regional=kalragauri-uds-poc-vp-usw1 bucket_project=<test project with regional BidiReadObject enabled> worker_threads=0 start=2026-11-05T07:51:24Z
ops_agent=active
cpp_sha=36919ee42327802f1d1824b5e01a11031d0335f7 rust_sha=e432ad1de767f8b8094de70fcb3113ccba67310d
kernel=7.0.0-1014-gcp glibc=2.39
end=2026-11-05T08:52:31Z failed_runs=0
```

Overlay run (`regional-parity`):

```text
host=kalragauri-uds-poc-c4 project=kalragauri-sdk-test instance_id=2425645790610435742 zone=us-central1-b machine=c4-standard-192 cpus=192
phase=regional regional=kalragauri-uds-poc-vp-usw1 zonal= bucket_prefix= worker_threads=0 monitoring_flush_secs=240 workloads=read50x2x4-serial,read50x2x4-concurrent,bidi-read-48t start=2026-11-06T07:28:29Z
ops_agent=active
cpp_sha=c7e65b2b6a423541767cf3d55d444bb0a97f9788 rust_sha=e432ad1de767f8b8094de70fcb3113ccba67310d
kernel=7.0.0-1014-gcp glibc=2.39
end=2026-11-06T08:08:11Z failed_runs=0
```

`% Diff` is `(UDS - Native) / Native`; negative latency, CPU, and memory deltas and positive throughput deltas favor the UDS comparator. Latency rows name their basis: `per op` is one benchmark operation; `per range` (W4) is one `ObjectDescriptor::Read` range, 13 per 200 MiB object (twelve of 16 MiB and a final 8 MiB range). `‡` marks a Monitoring series with fewer points than the window has 60 s buckets.

### W1 — Open a 200 MiB object (`AsyncClient::Open`)

Bucket: Standard Regional `gs://kalragauri-uds-poc-vp-usw1` · cell `open-regional`

| Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---:|---:|---:|---:|---:|
| Latency P50 (per op) | 53.7 ms | 53.9 ms | +0.4% | 54.8 ms | +2.0% |
| Latency P90 (per op) | 61.9 ms | 62.4 ms | +0.8% | 64.4 ms | +4.0% |
| Latency P99 (per op) | 85.6 ms | 133.6 ms | +56.0% | 133.4 ms | +55.8% |
| Throughput | 18.09 ops/s | 17.70 ops/s | -2.2% | 17.29 ops/s | -4.4% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 0.30% | 0.35% | +17.7% | 0.33% | +8.2% |
| Peak Memory (`memory/percent_used`, max) | 0.81% | 0.81% | +0.9% | 0.83% | +2.7% |
| Ops (ok / err) | 3256 / 0 | 3186 / 0 |  | 3113 / 0 |  |

### W2a — 50 × 2 MiB ranges at a 4 MiB stride, serial

Bucket: Standard Regional `gs://kalragauri-uds-poc-vp-usw1` · cell `read50x2x4-serial-regional`

| Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---:|---:|---:|---:|---:|
| Latency P50 (per op) | 4.63 s | 4.60 s | -0.7% | 4.83 s | +4.2% |
| Latency P90 (per op) | 4.70 s | 4.71 s | +0.1% | 4.89 s | +3.9% |
| Latency P99 (per op) | 4.82 s | 5.69 s | +18.0% | 4.98 s | +3.3% |
| Throughput | 22.6 MB/s | 22.6 MB/s | +0.1% | 21.8 MB/s | -3.5% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 0.37% | 0.35% | -4.4% | 0.35% | -3.6% |
| Peak Memory (`memory/percent_used`, max) | 0.87% | 0.85% | -2.1% | 0.85% | -1.6% |
| Ops (ok / err) | 39 / 0 | 39 / 0 |  | 38 / 0 |  |

### W2b — 50 × 2 MiB ranges at a 4 MiB stride, concurrent

Bucket: Standard Regional `gs://kalragauri-uds-poc-vp-usw1` · cell `read50x2x4-concurrent-regional`

| Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---:|---:|---:|---:|---:|
| Latency P50 (per op) | 3.34 s | 3.40 s | +1.9% | 3.10 s | -7.0% |
| Latency P90 (per op) | 3.41 s | 3.45 s | +1.3% | 3.41 s | -0.0% |
| Latency P99 (per op) | 3.46 s | 3.49 s | +1.0% | 3.51 s | +1.5% |
| Throughput | 31.3 MB/s | 30.9 MB/s | -1.2% | 33.2 MB/s | +6.1% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 0.40% | 0.78% | +93.1% | 0.37% | -7.0% |
| Peak Memory (`memory/percent_used`, max) | 0.85% | 1.02% | +20.2% | 0.87% | +2.3% |
| Ops (ok / err) | 54 / 0 | 54 / 0 |  | 57 / 0 |  |

### W3 — Random 8 KiB ranges on an open 100 MiB object

Bucket: Standard Regional `gs://kalragauri-uds-poc-vp-usw1` · cell `random8k-regional`

| Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---:|---:|---:|---:|---:|
| Latency P50 (per op) | 41.3 ms | 40.6 ms | -1.7% | 43.1 ms | +4.4% |
| Latency P90 (per op) | 46.7 ms | 43.5 ms | -6.9% | 47.7 ms | +2.1% |
| Latency P99 (per op) | 57.4 ms | 48.4 ms | -15.7% | 54.6 ms | -4.7% |
| Throughput | 23.54 ops/s | 24.28 ops/s | +3.2% | 22.97 ops/s | -2.4% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 0.33% | 0.35% | +8.6% | 0.49% | +49.6% |
| Peak Memory (`memory/percent_used`, max) | 0.83% | 0.83% | -0.3% | 0.95% | +14.6% |
| Ops (ok / err) | 4237 / 0 | 4371 / 0 |  | 4135 / 0 |  |

### W4 — Read 16 MiB chunks of a 200 MiB object via BidiRead, 48 threads, 5 min

Bucket: Standard Regional `gs://kalragauri-uds-poc-vp-usw1` · cell `bidi-read-48t-regional`

| Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---:|---:|---:|---:|---:|
| Latency P50 (per range) | 767.7 ms | 611.8 ms | -20.3% | 3.12 s | +307.0% |
| Latency P90 (per range) | 959.3 ms | 2.47 s | +157.1% | 3.45 s | +259.2% |
| Latency P99 (per range) | 1.20 s | 2.98 s | +147.4% | 3.89 s | +222.8% |
| Throughput | 1,062.9 MB/s | 681.6 MB/s | -35.9% | 253.3 MB/s | -76.2% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 2.58% | 2.02% | -21.8% | 1.24% | -52.0% |
| Peak Memory (`memory/percent_used`, max) | 1.05% | 1.09% | +3.4% | 1.11% | +4.9% |
| Ops (ok / err) | 1500 / 0 | 952 / 0 |  | 336 / 0 |  |
