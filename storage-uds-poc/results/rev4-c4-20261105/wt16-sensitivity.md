# Worker-thread sensitivity (`wt16` vs `full`)

`% Diff` is `(worker-thread run - base) / base`. Latency rows name their basis (`per op`, or `per range` for W4: 13 ranges per 200 MiB object, the last one 8 MiB). `‡` marks an incomplete Monitoring series.

| Cell | Comparator | Metric | Base | Worker-thread run | % Diff |
|---|---|---|---:|---:|---:|
| `random8k-zonal` | `uds-sidecar` | Latency P50 (per op) | 37.1 ms | 37.0 ms | -0.3% |
| `random8k-zonal` | `uds-sidecar` | Latency P90 (per op) | 39.6 ms | 39.1 ms | -1.2% |
| `random8k-zonal` | `uds-sidecar` | Latency P99 (per op) | 42.3 ms | 40.1 ms | -5.2% |
| `random8k-zonal` | `uds-sidecar` | Throughput | 26.59 ops/s | 26.79 ops/s | +0.8% |
| `random8k-zonal` | `uds-sidecar` | CPU Utilization (`instance/cpu/utilization`, avg) | 0.35% | 0.35% | -0.1% |
| `random8k-zonal` | `uds-sidecar` | Peak Memory (`memory/percent_used`, max) | 0.82% | 0.84% | +2.0% |
| `random8k-zonal` | `uds-sidecar` | Ops (ok / err) | 4786 / 0 | 4823 / 0 |  |
| `random8k-zonal` | `uds-sidecar` | Server threads | 193 | 17 | |
| `random8k-zonal` | `uds-sidecar` | `--worker-threads` | 0 | 16 | |
| `random8k-zonal` | `uds-inproc` | Latency P50 (per op) | 36.0 ms | 35.4 ms | -1.7% |
| `random8k-zonal` | `uds-inproc` | Latency P90 (per op) | 39.0 ms | 36.3 ms | -6.9% |
| `random8k-zonal` | `uds-inproc` | Latency P99 (per op) | 45.5 ms | 39.4 ms | -13.3% |
| `random8k-zonal` | `uds-inproc` | Throughput | 27.22 ops/s | 28.06 ops/s | +3.1% |
| `random8k-zonal` | `uds-inproc` | CPU Utilization (`instance/cpu/utilization`, avg) | 0.42% | 0.60% | +43.6% |
| `random8k-zonal` | `uds-inproc` | Peak Memory (`memory/percent_used`, max) | 0.82% | 0.92% | +12.1% |
| `random8k-zonal` | `uds-inproc` | Ops (ok / err) | 4899 / 0 | 5052 / 0 |  |
| `random8k-zonal` | `uds-inproc` | Server threads | 216 | 40 | |
| `random8k-zonal` | `uds-inproc` | `--worker-threads` | 0 | 16 | |
| `bidi-read-48t-zonal` | `uds-sidecar` | Latency P50 (per range) | 554.0 ms | 576.9 ms | +4.1% |
| `bidi-read-48t-zonal` | `uds-sidecar` | Latency P90 (per range) | 4.76 s | 5.05 s | +6.1% |
| `bidi-read-48t-zonal` | `uds-sidecar` | Latency P99 (per range) | 6.55 s | 6.81 s | +4.0% |
| `bidi-read-48t-zonal` | `uds-sidecar` | Throughput | 457.5 MB/s | 467.2 MB/s | +2.1% |
| `bidi-read-48t-zonal` | `uds-sidecar` | CPU Utilization (`instance/cpu/utilization`, avg) | 1.56% | 1.60% | +2.5% |
| `bidi-read-48t-zonal` | `uds-sidecar` | Peak Memory (`memory/percent_used`, max) | 0.94% | 0.99% | +5.1% |
| `bidi-read-48t-zonal` | `uds-sidecar` | Ops (ok / err) | 718 / 0 | 707 / 0 |  |
| `bidi-read-48t-zonal` | `uds-sidecar` | Server threads | 193 | 17 | |
| `bidi-read-48t-zonal` | `uds-sidecar` | `--worker-threads` | 0 | 16 | |
| `bidi-read-48t-zonal` | `uds-inproc` | Latency P50 (per range) | 591.5 ms | 609.1 ms | +3.0% |
| `bidi-read-48t-zonal` | `uds-inproc` | Latency P90 (per range) | 1.96 s | 2.06 s | +4.9% |
| `bidi-read-48t-zonal` | `uds-inproc` | Latency P99 (per range) | 3.48 s | 3.55 s | +1.9% |
| `bidi-read-48t-zonal` | `uds-inproc` | Throughput | 774.2 MB/s | 754.9 MB/s | -2.5% |
| `bidi-read-48t-zonal` | `uds-inproc` | CPU Utilization (`instance/cpu/utilization`, avg) | 2.25% | 1.99% | -11.5% |
| `bidi-read-48t-zonal` | `uds-inproc` | Peak Memory (`memory/percent_used`, max) | 0.95% | 0.95% | -0.6% |
| `bidi-read-48t-zonal` | `uds-inproc` | Ops (ok / err) | 1147 / 0 | 1110 / 0 |  |
| `bidi-read-48t-zonal` | `uds-inproc` | Server threads | 260 | 84 | |
| `bidi-read-48t-zonal` | `uds-inproc` | `--worker-threads` | 0 | 16 | |
