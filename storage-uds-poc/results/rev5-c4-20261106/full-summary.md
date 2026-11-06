# Benchmark Summary (`full-parity`)

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

## Primary Metrics (Latency, Throughput, Cloud Monitoring VM CPU & Peak Memory)

| Cell | Comparator | Path | Ops (ok/err) | P50 (ms) | P90 (ms) | P99 (ms) | P50 vs native | MB/s | MiB/s | VM CPU avg % | VM CPU max % | VM Peak Mem % |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `bidi-read-48t-zonal` | `native` | Cloudpath | 1565/0 | 7059.55 | 13716.67 | 15145.03 | 1.00x | 1107.98 | 1056.65 | 2.59% | 3.07% | 0.87% |
| `bidi-read-48t-zonal` | `uds-sidecar` | Cloudpath | 1200/0 | 11360.65 | 18473.00 | 26511.34 | 1.61x | 852.85 | 813.34 | 2.39% | 2.66% | 1.01% |
| `bidi-read-48t-zonal` | `uds-inproc` | Cloudpath | 1313/0 | 10158.12 | 15745.95 | 18877.13 | 1.44x | 932.53 | 889.33 | 2.73% | 2.83% | 1.01% |
| `bidi-write-48t-zonal` | `native` | Cloudpath | 2050/0 | 3983.38 | 15420.11 | 28833.76 | 1.00x | 1335.29 | 1273.43 | 1.29% | 1.45% | 0.95% |
| `bidi-write-48t-zonal` | `uds-sidecar` | Cloudpath | 1913/0 | 6278.68 | 14899.05 | 21081.78 | 1.58x | 1240.13 | 1182.68 | 2.27% | 2.84% | 2.40% |
| `bidi-write-48t-zonal` | `uds-inproc` | Cloudpath | 1482/0 | 6652.52 | 22398.91 | 33681.97 | 1.67x | 957.49 | 913.14 | 1.80% | 2.17% | 2.18% |
| `list-buckets-100-project` | `native` | Cloudpath | 2218/0 | 78.95 | 91.86 | 119.56 | 1.00x | 0.00 | 0.00 | 0.29% | 0.32% | 0.83% |
| `list-buckets-100-project` | `uds-sidecar` | Cloudpath | 2185/0 | 80.50 | 95.70 | 122.84 | 1.02x | 0.00 | 0.00 | 0.34% | 0.35% | 0.83% |
| `list-buckets-100-project` | `uds-inproc` | Cloudpath | 2263/0 | 77.46 | 91.91 | 114.01 | 0.98x | 0.00 | 0.00 | 0.31% | 0.33% | 0.83% |
| `list-objects-100-regional` | `native` | Cloudpath | 2886/0 | 60.17 | 70.26 | 86.48 | 1.00x | 0.00 | 0.00 | 0.51% | 1.12% | 2.22% |
| `list-objects-100-regional` | `uds-sidecar` | Cloudpath | 2998/0 | 57.89 | 64.98 | 126.76 | 0.96x | 0.00 | 0.00 | 0.35% | 0.36% | 0.86% |
| `list-objects-100-regional` | `uds-inproc` | Cloudpath | 3028/0 | 57.75 | 64.68 | 115.26 | 0.96x | 0.00 | 0.00 | 0.32% | 0.34% | 0.84% |
| `open-zonal` | `native` | Cloudpath | 104/0 | 1690.26 | 2054.86 | 2219.07 | 1.00x | 0.00 | 0.00 | 0.26% | 0.31% | 2.08% |
| `open-zonal` | `uds-sidecar` | Cloudpath | 280/0 | 660.95 | 1007.68 | 1109.38 | 0.39x | 0.00 | 0.00 | 0.33% | 0.35% | 1.28% |
| `open-zonal` | `uds-inproc` | Cloudpath | 284/0 | 623.67 | 1036.24 | 1110.30 | 0.37x | 0.00 | 0.00 | 0.32% | 0.35% | 1.06% |
| `random8k-zonal` | `native` | Cloudpath | 5068/0 | 35.37 | 36.20 | 38.00 | 1.00x | 0.23 | 0.22 | 0.32% | 0.34% | 0.80% |
| `random8k-zonal` | `uds-sidecar` | Cloudpath | 4786/0 | 37.09 | 39.63 | 42.30 | 1.05x | 0.22 | 0.21 | 0.35% | 0.37% | 0.82% |
| `random8k-zonal` | `uds-inproc` | Cloudpath | 4899/0 | 36.00 | 39.03 | 45.46 | 1.02x | 0.22 | 0.21 | 0.42% | 0.67% | 0.82% |
| `read50x2x4-concurrent-zonal` | `native` | Cloudpath | 48/0 | 3787.29 | 4042.23 | 4466.52 | 1.00x | 27.42 | 26.15 | 0.40% | 0.41% | 0.87% |
| `read50x2x4-concurrent-zonal` | `uds-sidecar` | Cloudpath | 63/0 | 2883.76 | 2978.62 | 3109.22 | 0.76x | 36.18 | 34.51 | 0.69% | 1.41% | 0.95% |
| `read50x2x4-concurrent-zonal` | `uds-inproc` | Cloudpath | 57/0 | 3181.09 | 3357.05 | 3369.59 | 0.84x | 32.92 | 31.39 | 0.36% | 0.37% | 0.86% |
| `read50x2x4-serial-zonal` | `native` | Cloudpath | 43/0 | 4287.16 | 4344.84 | 4402.89 | 1.00x | 24.49 | 23.36 | 0.37% | 0.38% | 0.85% |
| `read50x2x4-serial-zonal` | `uds-sidecar` | Cloudpath | 43/0 | 4269.19 | 4344.40 | 4562.20 | 1.00x | 24.48 | 23.35 | 0.61% | 1.47% | 0.86% |
| `read50x2x4-serial-zonal` | `uds-inproc` | Cloudpath | 43/0 | 4240.33 | 4303.86 | 4366.49 | 0.99x | 24.65 | 23.51 | 0.36% | 0.36% | 0.86% |

## Secondary Process Attribution (`getrusage` CPU & `VmHWM` Peak RSS)

| Cell | Comparator | App CPU (s) | App Peak RSS (MiB) | Server CPU (s) | Server Peak RSS (MiB) | Server Threads |
|---|---|---:|---:|---:|---:|---:|
| `bidi-read-48t-zonal` | `native` | 1118.32 | 250.2 | n/a | n/a | n/a |
| `bidi-read-48t-zonal` | `uds-sidecar` | 223.36 | 342.0 | 680.29 | 889.6 | 193 |
| `bidi-read-48t-zonal` | `uds-inproc` | 233.80 | 1365.8 | 750.73 | shared | 260 |
| `bidi-write-48t-zonal` | `native` | 396.43 | 391.6 | n/a | n/a | n/a |
| `bidi-write-48t-zonal` | `uds-sidecar` | 493.14 | 1320.8 | 571.80 | 8703.3 | 193 |
| `bidi-write-48t-zonal` | `uds-inproc` | 360.01 | 9532.1 | 442.96 | shared | 260 |
| `list-buckets-100-project` | `native` | 2.57 | 37.8 | n/a | n/a | n/a |
| `list-buckets-100-project` | `uds-sidecar` | 1.73 | 23.9 | 1.34 | 22.2 | 193 |
| `list-buckets-100-project` | `uds-inproc` | 1.69 | 45.6 | 1.37 | shared | 216 |
| `list-objects-100-regional` | `native` | 3.54 | 38.5 | n/a | n/a | n/a |
| `list-objects-100-regional` | `uds-sidecar` | 1.99 | 25.3 | 1.63 | 22.0 | 193 |
| `list-objects-100-regional` | `uds-inproc` | 2.13 | 45.9 | 1.55 | shared | 216 |
| `open-zonal` | `native` | 1.33 | 36.9 | n/a | n/a | n/a |
| `open-zonal` | `uds-sidecar` | 0.99 | 22.1 | 0.23 | 23.2 | 193 |
| `open-zonal` | `uds-inproc` | 0.99 | 44.5 | 0.20 | shared | 216 |
| `random8k-zonal` | `native` | 4.01 | 39.5 | n/a | n/a | n/a |
| `random8k-zonal` | `uds-sidecar` | 3.25 | 25.9 | 1.24 | 23.1 | 193 |
| `random8k-zonal` | `uds-inproc` | 3.40 | 48.0 | 1.35 | shared | 216 |
| `read50x2x4-concurrent-zonal` | `native` | 18.01 | 47.5 | n/a | n/a | n/a |
| `read50x2x4-concurrent-zonal` | `uds-sidecar` | 5.01 | 144.4 | 10.64 | 87.9 | 193 |
| `read50x2x4-concurrent-zonal` | `uds-inproc` | 4.27 | 231.0 | 8.44 | shared | 216 |
| `read50x2x4-serial-zonal` | `native` | 16.55 | 46.0 | n/a | n/a | n/a |
| `read50x2x4-serial-zonal` | `uds-sidecar` | 4.24 | 127.5 | 8.51 | 33.7 | 193 |
| `read50x2x4-serial-zonal` | `uds-inproc` | 3.93 | 157.9 | 6.97 | shared | 216 |

## Per-Range Latency (W4 Streaming Read)

| Cell | Comparator | Ranges | Range P50 (ms) | Range P90 (ms) | Range P99 (ms) |
|---|---|---:|---:|---:|---:|
| `bidi-read-48t-zonal` | `native` | 20652 | 565.69 | 1077.15 | 1267.16 |
| `bidi-read-48t-zonal` | `uds-sidecar` | 15908 | 886.84 | 1454.47 | 2333.04 |
| `bidi-read-48t-zonal` | `uds-inproc` | 17364 | 798.86 | 1250.24 | 1500.70 |
