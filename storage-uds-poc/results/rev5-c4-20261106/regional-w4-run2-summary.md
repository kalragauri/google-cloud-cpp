# Benchmark Summary (`regional-parity-r2`)

```text
host=kalragauri-uds-poc-c4 project=kalragauri-sdk-test instance_id=2425645790610435742 zone=us-central1-b machine=c4-standard-192 cpus=192
phase=regional regional=kalragauri-uds-poc-vp-usw1 zonal= bucket_prefix= worker_threads=0 monitoring_flush_secs=240 workloads=bidi-read-48t start=2026-11-06T08:13:11Z
ops_agent=active
cpp_sha=c7e65b2b6a423541767cf3d55d444bb0a97f9788 rust_sha=e432ad1de767f8b8094de70fcb3113ccba67310d
kernel=7.0.0-1014-gcp glibc=2.39
end=2026-11-06T08:33:11Z failed_runs=0
```

## Primary Metrics (Latency, Throughput, Cloud Monitoring VM CPU & Peak Memory)

| Cell | Comparator | Path | Ops (ok/err) | P50 (ms) | P90 (ms) | P99 (ms) | P50 vs native | MB/s | MiB/s | VM CPU avg % | VM CPU max % | VM Peak Mem % |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `bidi-read-48t-regional` | `native` | Cloudpath | 1471/0 | 10304.24 | 13800.00 | 15762.61 | 1.00x | 1043.05 | 994.73 | 2.39% | 2.91% | 0.88% |
| `bidi-read-48t-regional` | `uds-sidecar` | Cloudpath | 192/0 | 64945.03 | 67509.65 | 67525.58 | 6.30x | 155.53 | 148.33 | 0.87% | 1.29% | 1.04% |
| `bidi-read-48t-regional` | `uds-inproc` | Cloudpath | 1065/0 | 7356.99 | 22923.56 | 23447.59 | 0.71x | 761.36 | 726.09 | 2.17% | 2.41% | 1.04% |

## Secondary Process Attribution (`getrusage` CPU & `VmHWM` Peak RSS)

| Cell | Comparator | App CPU (s) | App Peak RSS (MiB) | Server CPU (s) | Server Peak RSS (MiB) | Server Threads |
|---|---|---:|---:|---:|---:|---:|
| `bidi-read-48t-regional` | `native` | 1010.59 | 240.7 | n/a | n/a | n/a |
| `bidi-read-48t-regional` | `uds-sidecar` | 49.64 | 335.4 | 126.47 | 940.8 | 193 |
| `bidi-read-48t-regional` | `uds-inproc` | 190.93 | 1633.0 | 611.26 | shared | 260 |

## Per-Range Latency (W4 Streaming Read)

| Cell | Comparator | Ranges | Range P50 (ms) | Range P90 (ms) | Range P99 (ms) |
|---|---|---:|---:|---:|---:|
| `bidi-read-48t-regional` | `native` | 19423 | 779.51 | 1086.14 | 1338.97 |
| `bidi-read-48t-regional` | `uds-sidecar` | 2901 | 5013.14 | 5780.32 | 7456.74 |
| `bidi-read-48t-regional` | `uds-inproc` | 14171 | 591.91 | 1835.73 | 1999.95 |
