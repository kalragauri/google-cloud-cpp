# Benchmark Summary (`regional-parity`)

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

## Primary Metrics (Latency, Throughput, Cloud Monitoring VM CPU & Peak Memory)

| Cell | Comparator | Path | Ops (ok/err) | P50 (ms) | P90 (ms) | P99 (ms) | P50 vs native | MB/s | MiB/s | VM CPU avg % | VM CPU max % | VM Peak Mem % |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `bidi-read-48t-regional` | `native` | Cloudpath | 1500/0 | 10135.63 | 11926.72 | 13538.91 | 1.00x | 1062.94 | 1013.70 | 2.58% | 2.88% | 1.05% |
| `bidi-read-48t-regional` | `uds-sidecar` | Cloudpath | 952/0 | 7600.81 | 30902.79 | 37412.45 | 0.75x | 681.59 | 650.01 | 2.02% | 2.24% | 1.09% |
| `bidi-read-48t-regional` | `uds-inproc` | Cloudpath | 336/0 | 39759.11 | 42181.91 | 42370.70 | 3.92x | 253.33 | 241.59 | 1.24% | 1.57% | 1.11% |
| `open-regional` | `native` | Cloudpath | 3256/0 | 53.75 | 61.94 | 85.62 | 1.00x | 0.00 | 0.00 | 0.30% | 0.34% | 0.81% |
| `open-regional` | `uds-sidecar` | Cloudpath | 3186/0 | 53.94 | 62.42 | 133.61 | 1.00x | 0.00 | 0.00 | 0.35% | 0.36% | 0.81% |
| `open-regional` | `uds-inproc` | Cloudpath | 3113/0 | 54.80 | 64.44 | 133.37 | 1.02x | 0.00 | 0.00 | 0.33% | 0.38% | 0.83% |
| `random8k-regional` | `native` | Cloudpath | 4237/0 | 41.31 | 46.70 | 57.36 | 1.00x | 0.19 | 0.18 | 0.33% | 0.38% | 0.83% |
| `random8k-regional` | `uds-sidecar` | Cloudpath | 4371/0 | 40.62 | 43.47 | 48.37 | 0.98x | 0.20 | 0.19 | 0.35% | 0.38% | 0.83% |
| `random8k-regional` | `uds-inproc` | Cloudpath | 4135/0 | 43.11 | 47.68 | 54.64 | 1.04x | 0.19 | 0.18 | 0.49% | 0.93% | 0.95% |
| `read50x2x4-concurrent-regional` | `native` | Cloudpath | 54/0 | 3335.28 | 3406.09 | 3456.17 | 1.00x | 31.27 | 29.83 | 0.40% | 0.41% | 0.85% |
| `read50x2x4-concurrent-regional` | `uds-sidecar` | Cloudpath | 54/0 | 3400.06 | 3451.60 | 3489.79 | 1.02x | 30.91 | 29.48 | 0.78% | 1.70% | 1.02% |
| `read50x2x4-concurrent-regional` | `uds-inproc` | Cloudpath | 57/0 | 3103.33 | 3405.62 | 3508.78 | 0.93x | 33.20 | 31.66 | 0.37% | 0.39% | 0.87% |
| `read50x2x4-serial-regional` | `native` | Cloudpath | 39/0 | 4633.63 | 4702.54 | 4820.47 | 1.00x | 22.57 | 21.53 | 0.37% | 0.37% | 0.87% |
| `read50x2x4-serial-regional` | `uds-sidecar` | Cloudpath | 39/0 | 4600.70 | 4706.60 | 5688.68 | 0.99x | 22.60 | 21.55 | 0.35% | 0.39% | 0.85% |
| `read50x2x4-serial-regional` | `uds-inproc` | Cloudpath | 38/0 | 4828.24 | 4886.98 | 4981.82 | 1.04x | 21.78 | 20.77 | 0.35% | 0.36% | 0.85% |

## Secondary Process Attribution (`getrusage` CPU & `VmHWM` Peak RSS)

| Cell | Comparator | App CPU (s) | App Peak RSS (MiB) | Server CPU (s) | Server Peak RSS (MiB) | Server Threads |
|---|---|---:|---:|---:|---:|---:|
| `bidi-read-48t-regional` | `native` | 1025.64 | 230.5 | n/a | n/a | n/a |
| `bidi-read-48t-regional` | `uds-sidecar` | 183.76 | 330.7 | 557.03 | 1187.9 | 193 |
| `bidi-read-48t-regional` | `uds-inproc` | 82.94 | 1464.5 | 214.01 | shared | 260 |
| `open-regional` | `native` | 5.80 | 41.4 | n/a | n/a | n/a |
| `open-regional` | `uds-sidecar` | 2.69 | 23.2 | 1.32 | 23.7 | 193 |
| `open-regional` | `uds-inproc` | 2.70 | 45.7 | 1.30 | shared | 216 |
| `random8k-regional` | `native` | 3.54 | 42.1 | n/a | n/a | n/a |
| `random8k-regional` | `uds-sidecar` | 3.02 | 26.5 | 1.16 | 22.6 | 193 |
| `random8k-regional` | `uds-inproc` | 2.85 | 47.3 | 1.02 | shared | 216 |
| `read50x2x4-concurrent-regional` | `native` | 20.98 | 47.7 | n/a | n/a | n/a |
| `read50x2x4-concurrent-regional` | `uds-sidecar` | 5.47 | 120.3 | 9.38 | 94.7 | 193 |
| `read50x2x4-concurrent-regional` | `uds-inproc` | 5.29 | 263.3 | 10.72 | shared | 216 |
| `read50x2x4-serial-regional` | `native` | 15.31 | 44.8 | n/a | n/a | n/a |
| `read50x2x4-serial-regional` | `uds-sidecar` | 4.24 | 118.4 | 6.74 | 36.2 | 193 |
| `read50x2x4-serial-regional` | `uds-inproc` | 4.33 | 142.7 | 6.65 | shared | 216 |

## Per-Range Latency (W4 Streaming Read)

| Cell | Comparator | Ranges | Range P50 (ms) | Range P90 (ms) | Range P99 (ms) |
|---|---|---:|---:|---:|---:|
| `bidi-read-48t-regional` | `native` | 19794 | 767.66 | 959.27 | 1204.97 |
| `bidi-read-48t-regional` | `uds-sidecar` | 12694 | 611.82 | 2465.84 | 2981.26 |
| `bidi-read-48t-regional` | `uds-inproc` | 4719 | 3124.06 | 3445.63 | 3889.54 |
