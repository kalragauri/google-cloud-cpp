# Per-workload tables (`regional-parity-r2`)

`% Diff` is `(UDS - Native) / Native`; negative latency, CPU, and memory deltas and positive throughput deltas favor the UDS comparator. Latency rows name their basis: `per op` is one benchmark operation; `per range` (W4) is one `ObjectDescriptor::Read` range, 13 per 200 MiB object (twelve of 16 MiB and a final 8 MiB range). `‡` marks a Monitoring series with fewer points than the window has 60 s buckets.

### W4 — Read 16 MiB chunks of a 200 MiB object via BidiRead, 48 threads, 5 min

Bucket: Standard Regional `gs://kalragauri-uds-poc-vp-usw1` · cell `bidi-read-48t-regional`

| Metric | Native C++ | `uds-sidecar` | % Diff | `uds-inproc` | % Diff |
|---|---:|---:|---:|---:|---:|
| Latency P50 (per range) | 779.5 ms | 5.01 s | +543.1% | 591.9 ms | -24.1% |
| Latency P90 (per range) | 1.09 s | 5.78 s | +432.2% | 1.84 s | +69.0% |
| Latency P99 (per range) | 1.34 s | 7.46 s | +456.9% | 2.00 s | +49.4% |
| Throughput | 1,043.0 MB/s | 155.5 MB/s | -85.1% | 761.4 MB/s | -27.0% |
| CPU Utilization (`instance/cpu/utilization`, avg) | 2.39% | 0.87% | -63.7% | 2.17% | -9.2% |
| Peak Memory (`memory/percent_used`, max) | 0.88% | 1.04% | +18.9% | 1.04% | +18.3% |
| Ops (ok / err) | 1471 / 0 | 192 / 0 |  | 1065 / 0 |  |
