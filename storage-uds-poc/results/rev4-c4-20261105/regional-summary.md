# Benchmark Summary (`regional`)

```text
host=kalragauri-uds-poc-c4 project=kalragauri-sdk-test instance_id=2425645790610435742 zone=us-central1-b machine=c4-standard-192 cpus=192
phase=regional regional=kalragauri-uds-poc-vp-usw1 bucket_project=<test project with regional BidiReadObject enabled> worker_threads=0 start=2026-11-05T07:51:24Z
ops_agent=active
cpp_sha=36919ee42327802f1d1824b5e01a11031d0335f7 rust_sha=e432ad1de767f8b8094de70fcb3113ccba67310d
kernel=7.0.0-1014-gcp glibc=2.39
end=2026-11-05T08:52:31Z failed_runs=0
```

## Primary Metrics (Latency, Throughput, Cloud Monitoring VM CPU & Peak Memory)

| Cell | Comparator | Path | Ops (ok/err) | P50 (ms) | P90 (ms) | P99 (ms) | P50 vs native | MB/s | MiB/s | VM CPU avg % | VM CPU max % | VM Peak Mem % |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `bidi-read-48t-regional` | `native` | Cloudpath | 465/0 | 17329.29 | 78597.77 | 86706.98 | 1.00x | 292.77 | 279.21 | 1.16% | 1.57% | 0.83% |
| `bidi-read-48t-regional` | `uds-sidecar` | Cloudpath | 960/0 | 7605.67 | 32918.98 | 48253.19 | 0.44x | 643.25 | 613.45 | 1.88% | 2.29% | 1.01% |
| `bidi-read-48t-regional` | `uds-inproc` | Cloudpath | 1131/0 | 7687.54 | 27643.91 | 41777.23 | 0.44x | 756.10 | 721.07 | 2.23% | 2.45% | 0.96% |
| `open-regional` | `native` | Cloudpath | 3256/0 | 53.75 | 61.94 | 85.62 | 1.00x | 0.00 | 0.00 | 0.30% | 0.34% | 0.81% |
| `open-regional` | `uds-sidecar` | Cloudpath | 3186/0 | 53.94 | 62.42 | 133.61 | 1.00x | 0.00 | 0.00 | 0.35% | 0.36% | 0.81% |
| `open-regional` | `uds-inproc` | Cloudpath | 3113/0 | 54.80 | 64.44 | 133.37 | 1.02x | 0.00 | 0.00 | 0.33% | 0.38% | 0.83% |
| `random8k-regional` | `native` | Cloudpath | 4237/0 | 41.31 | 46.70 | 57.36 | 1.00x | 0.19 | 0.18 | 0.33% | 0.38% | 0.83% |
| `random8k-regional` | `uds-sidecar` | Cloudpath | 4371/0 | 40.62 | 43.47 | 48.37 | 0.98x | 0.20 | 0.19 | 0.35% | 0.38% | 0.83% |
| `random8k-regional` | `uds-inproc` | Cloudpath | 4135/0 | 43.11 | 47.68 | 54.64 | 1.04x | 0.19 | 0.18 | 0.49% | 0.93% | 0.95% |
| `read50x2x4-concurrent-regional` | `native` | Cloudpath | 50/0 | 3634.96 | 3957.63 | 4279.64 | 1.00x | 28.91 | 27.57 | 0.39% | 0.40% | 0.84% |
| `read50x2x4-concurrent-regional` | `uds-sidecar` | Cloudpath | 50/0 | 3701.27 | 3908.16 | 4070.01 | 1.02x | 28.55 | 27.23 | 0.39% | 0.41% | 0.84% |
| `read50x2x4-concurrent-regional` | `uds-inproc` | Cloudpath | 49/0 | 3735.80 | 3951.27 | 4560.04 | 1.03x | 28.34 | 27.03 | 0.36% | 0.40% | 0.84% |
| `read50x2x4-serial-regional` | `native` | Cloudpath | 35/0 | 5211.30 | 5687.29 | 5960.85 | 1.00x | 19.99 | 19.07 | 0.35% | 0.35% | 0.81% |
| `read50x2x4-serial-regional` | `uds-sidecar` | Cloudpath | 34/0 | 5202.30 | 5880.88 | 7728.41 | 1.00x | 19.66 | 18.75 | 0.38% | 0.39% | 0.83% |
| `read50x2x4-serial-regional` | `uds-inproc` | Cloudpath | 34/0 | 5219.89 | 5658.62 | 8374.71 | 1.00x | 19.71 | 18.80 | 0.35% | 0.38% | 0.82% |

## Secondary Process Attribution (`getrusage` CPU & `VmHWM` Peak RSS)

| Cell | Comparator | App CPU (s) | App Peak RSS (MiB) | Server CPU (s) | Server Peak RSS (MiB) | Server Threads |
|---|---|---:|---:|---:|---:|---:|
| `bidi-read-48t-regional` | `native` | 335.47 | 195.2 | n/a | n/a | n/a |
| `bidi-read-48t-regional` | `uds-sidecar` | 174.17 | 352.6 | 517.31 | 904.7 | 193 |
| `bidi-read-48t-regional` | `uds-inproc` | 191.07 | 1311.0 | 611.82 | shared | 260 |
| `open-regional` | `native` | 5.80 | 41.4 | n/a | n/a | n/a |
| `open-regional` | `uds-sidecar` | 2.69 | 23.2 | 1.32 | 23.7 | 193 |
| `open-regional` | `uds-inproc` | 2.70 | 45.7 | 1.30 | shared | 216 |
| `random8k-regional` | `native` | 3.54 | 42.1 | n/a | n/a | n/a |
| `random8k-regional` | `uds-sidecar` | 3.02 | 26.5 | 1.16 | 22.6 | 193 |
| `random8k-regional` | `uds-inproc` | 2.85 | 47.3 | 1.02 | shared | 216 |
| `read50x2x4-concurrent-regional` | `native` | 19.74 | 47.0 | n/a | n/a | n/a |
| `read50x2x4-concurrent-regional` | `uds-sidecar` | 4.52 | 127.8 | 8.13 | 83.8 | 193 |
| `read50x2x4-concurrent-regional` | `uds-inproc` | 4.29 | 241.9 | 7.84 | shared | 216 |
| `read50x2x4-serial-regional` | `native` | 12.23 | 46.4 | n/a | n/a | n/a |
| `read50x2x4-serial-regional` | `uds-sidecar` | 3.84 | 136.4 | 6.49 | 45.2 | 193 |
| `read50x2x4-serial-regional` | `uds-inproc` | 3.56 | 190.5 | 6.18 | shared | 216 |

## Per-Range Latency (W4 Streaming Read)

| Cell | Comparator | Ranges | Range P50 (ms) | Range P90 (ms) | Range P99 (ms) |
|---|---|---:|---:|---:|---:|
| `bidi-read-48t-regional` | `native` | 6045 | 1123.96 | 6441.62 | 7407.74 |
| `bidi-read-48t-regional` | `uds-sidecar` | 12480 | 594.25 | 2655.89 | 4232.37 |
| `bidi-read-48t-regional` | `uds-inproc` | 14703 | 601.44 | 2151.19 | 3538.14 |
