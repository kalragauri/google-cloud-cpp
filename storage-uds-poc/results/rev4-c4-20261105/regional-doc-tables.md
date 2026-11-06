# Per-workload tables (`regional`)

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
| Latency P50 (per op) | 5.21 s | 5.20 s | -0.2% | 5.22 s | +0.2% |
| Latency P90 (per op) | 5.69 s | 5.88 s | +3.4% | 5.66 s | -0.5% |
| Latency P99 (per op) | 5.96 s | 7.73 s | +29.7% | 8.37 s | +40.5% |
| Throughput | 20.0 MB/s | 19.7 MB/s | -1.7% | 19.7 MB/s | -1.4% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 0.35% | 0.38% | +10.7% | 0.35% | +0.9% |
| Peak Memory (`memory/percent_used`, max) | 0.81% | 0.83% | +1.9% | 0.82% | +1.1% |
| Ops (ok / err) | 35 / 0 | 34 / 0 |  | 34 / 0 |  |

### W2b — 50 × 2 MiB ranges at a 4 MiB stride, concurrent

Bucket: Standard Regional `gs://kalragauri-uds-poc-vp-usw1` · cell `read50x2x4-concurrent-regional`

| Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---:|---:|---:|---:|---:|
| Latency P50 (per op) | 3.63 s | 3.70 s | +1.8% | 3.74 s | +2.8% |
| Latency P90 (per op) | 3.96 s | 3.91 s | -1.2% | 3.95 s | -0.2% |
| Latency P99 (per op) | 4.28 s | 4.07 s | -4.9% | 4.56 s | +6.6% |
| Throughput | 28.9 MB/s | 28.5 MB/s | -1.3% | 28.3 MB/s | -2.0% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 0.39% | 0.39% | +0.5% | 0.36% | -7.8% |
| Peak Memory (`memory/percent_used`, max) | 0.84% | 0.84% | +0.0% | 0.84% | -0.4% |
| Ops (ok / err) | 50 / 0 | 50 / 0 |  | 49 / 0 |  |

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
| Latency P50 (per range) | 1.12 s | 594.2 ms | -47.1% | 601.4 ms | -46.5% |
| Latency P90 (per range) | 6.44 s | 2.66 s | -58.8% | 2.15 s | -66.6% |
| Latency P99 (per range) | 7.41 s | 4.23 s | -42.9% | 3.54 s | -52.2% |
| Throughput | 292.8 MB/s | 643.3 MB/s | +119.7% | 756.1 MB/s | +158.3% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 1.16% | 1.88% | +62.7% | 2.23% | +92.7% |
| Peak Memory (`memory/percent_used`, max) | 0.83% | 1.01% | +20.9% | 0.96% | +15.5% |
| Ops (ok / err) | 465 / 0 | 960 / 0 |  | 1131 / 0 |  |
