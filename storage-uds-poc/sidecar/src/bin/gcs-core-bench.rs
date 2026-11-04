// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! `gcs-core-bench`: the same workloads as `bench/ssb_workloads.h`, run on
//! the Rust core in-process ("pure-rust" comparator). It makes the same crate
//! calls as the sidecar, so `uds` minus `pure-rust` is the cost of the
//! wrapper + UDS hop. Measurement matches the C++ harness: getrusage(SELF),
//! interposed malloc, /proc RSS. CSV schema is identical (peer columns 0).

#[path = "../malloc_interpose.rs"]
mod malloc_interpose;

use gcs_core::stats;

use bytes::Bytes;
use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::model_ext::ReadRange;
use google_cloud_storage::object_descriptor::ObjectDescriptor;
use std::fmt::Write as _;
use std::io::Write as _;
use std::sync::Arc;
use std::time::Instant;

const KIB: i64 = 1024;
const MIB: i64 = KIB * KIB;

/// Bit-for-bit the SplitMix64 in ssb_workloads.h.
struct SplitMix64(u64);
impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }
}

#[derive(Clone)]
struct Args {
    bucket: String,
    workload: String,
    zonal: bool,
    iterations: i32,
    warmup: i32,
    workers: i32,
    list_objects: i32,
    throughput_write: bool,
    object_sizes: Vec<i64>,
    out: String,
    prefix: String,
    label: String,
    grpc_subchannels: usize,
    unbuffered: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut a = Args {
        bucket: String::new(),
        workload: "w1r3".into(),
        zonal: false,
        iterations: 100,
        warmup: 1,
        workers: 8,
        list_objects: 1000,
        throughput_write: false,
        object_sizes: vec![100_000, 2 * MIB, 100_000_000],
        out: String::new(),
        prefix: "gcs-uds-poc/bench/".into(),
        label: String::new(),
        grpc_subchannels: 4,
        unbuffered: false,
    };
    for arg in std::env::args().skip(1) {
        let (k, v) = arg.split_once('=').unwrap_or((arg.as_str(), ""));
        let num = |v: &str| v.parse::<i64>().map_err(|_| format!("{k} needs a number"));
        match k {
            "--bucket" => a.bucket = v.into(),
            "--workload" => a.workload = v.into(),
            "--bucket-type" => match v {
                "regional" => a.zonal = false,
                "zonal" => a.zonal = true,
                _ => return Err("--bucket-type must be regional or zonal".into()),
            },
            "--iterations" => a.iterations = num(v)? as i32,
            "--warmup" => a.warmup = num(v)? as i32,
            "--workers" => a.workers = num(v)? as i32,
            "--list-objects" => a.list_objects = num(v)? as i32,
            "--throughput-op" => match v {
                "read" => a.throughput_write = false,
                "write" => a.throughput_write = true,
                _ => return Err("--throughput-op must be read or write".into()),
            },
            "--object-sizes" => {
                if v.is_empty() {
                    return Err("--object-sizes must not be empty".into());
                }
                a.object_sizes = v.split(',').map(num).collect::<Result<_, _>>()?;
            }
            "--out" => a.out = v.into(),
            "--prefix" => a.prefix = v.into(),
            "--label" => a.label = v.into(),
            "--grpc-subchannels" => {
                let n = num(v)?;
                if n < 1 {
                    return Err("--grpc-subchannels must be >= 1".into());
                }
                a.grpc_subchannels = n as usize;
            }
            "--unbuffered" => a.unbuffered = true,
            _ => return Err(format!("unknown flag {arg}")),
        }
    }
    if a.bucket.is_empty() {
        return Err("--bucket is required".into());
    }
    if a.iterations < 1 || a.warmup < 0 || a.workers < 1 || a.list_objects < 1 {
        return Err(
            "--iterations, --workers, and --list-objects must be >= 1 and --warmup >= 0".into(),
        );
    }
    if a.object_sizes.is_empty() || a.object_sizes.iter().any(|&sz| sz <= 0) {
        return Err("--object-sizes must be a non-empty list of positive sizes".into());
    }
    Ok(a)
}

fn bucket_name(b: &str) -> String {
    format!("projects/_/buckets/{b}")
}

fn range(offset: i64, length: i64) -> ReadRange {
    if length == 0 {
        ReadRange::offset(offset as u64)
    } else {
        ReadRange::segment(offset as u64, length as u64)
    }
}

type OpResult = Result<i64, String>;

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

struct Core {
    storage: Storage,
    control: StorageControl,
    unbuffered: bool,
}

impl Core {
    async fn json_upload(
        &self,
        bucket: &str,
        object: &str,
        data: Bytes,
        resumable: bool,
    ) -> OpResult {
        let size = data.len() as i64;
        // Default (`!unbuffered`) matches the sidecar's `send_buffered()` call
        // on a non-seekable UDS stream; `--unbuffered` uses `send_unbuffered()`
        // on seekable in-memory `Bytes`.
        let threshold = if resumable { 0_usize } else { usize::MAX };
        let builder = self
            .storage
            .write_object(bucket_name(bucket), object, data)
            .with_resumable_upload_threshold(threshold);
        if self.unbuffered {
            builder.send_unbuffered().await.map_err(err)?;
        } else {
            builder.send_buffered().await.map_err(err)?;
        }
        Ok(size)
    }

    async fn json_read(&self, bucket: &str, object: &str) -> OpResult {
        let mut r = self
            .storage
            .read_object(bucket_name(bucket), object)
            .send()
            .await
            .map_err(err)?;
        let mut n = 0_i64;
        while let Some(chunk) = r.next().await {
            n += chunk.map_err(err)?.len() as i64;
        }
        Ok(n)
    }

    async fn list(&self, bucket: &str, prefix: &str, page_size: i32) -> OpResult {
        let mut token = String::new();
        let mut n = 0_i64;
        loop {
            let mut b = self
                .control
                .list_objects()
                .set_parent(bucket_name(bucket))
                .set_prefix(prefix)
                .set_page_size(page_size);
            if !token.is_empty() {
                b = b.set_page_token(token);
            }
            let page = b.send().await.map_err(err)?;
            n += page.objects.len() as i64;
            token = page.next_page_token;
            if token.is_empty() {
                return Ok(n);
            }
        }
    }

    async fn delete(&self, bucket: &str, object: &str) -> OpResult {
        self.control
            .delete_object()
            .set_bucket(bucket_name(bucket))
            .set_object(object)
            .send()
            .await
            .map_err(err)?;
        Ok(0)
    }

    async fn appendable_upload(&self, bucket: &str, object: &str, data: Bytes) -> OpResult {
        let size = data.len() as i64;
        let mut w = self
            .storage
            .open_appendable_object(bucket_name(bucket), object)
            .send()
            .await
            .map_err(err)?;
        w.append(data).await.map_err(err)?;
        w.finalize().await.map_err(err)?;
        Ok(size)
    }

    async fn open(&self, bucket: &str, object: &str) -> Result<ObjectDescriptor, String> {
        self.storage
            .open_object(bucket_name(bucket), object)
            .send()
            .await
            .map_err(err)
    }
}

async fn read_ranges(d: &ObjectDescriptor, ranges: &[(i64, i64)]) -> OpResult {
    let reads = ranges.iter().map(|&(o, l)| async move {
        let mut r = d.read_range(range(o, l)).await;
        let mut n = 0_i64;
        while let Some(chunk) = r.next().await {
            n += chunk.map_err(err)?.len() as i64;
        }
        Ok::<i64, String>(n)
    });
    let mut total = 0;
    for r in futures::future::join_all(reads).await {
        total += r?;
    }
    Ok(total)
}

/// One measured operation kept for the end-of-run summary.
struct Row {
    workload: String,
    op: String,
    size: i64,
    wall_ms: f64,
    cpu_ms: f64,
    bytes: i64,
    ok: bool,
}

struct Recorder {
    impl_name: String,
    labels: String,
    file: std::fs::File,
    rows: Vec<Row>,
}

impl Recorder {
    #[allow(clippy::too_many_arguments)]
    fn add(
        &mut self,
        workload: &str,
        iteration: i32,
        size: i64,
        op: &str,
        wall_ms: f64,
        cpu_ms: f64,
        bytes: i64,
        error: &str,
        alloc: u64,
        rss_kib: u64,
    ) {
        let e = error
            .chars()
            .take(160)
            .collect::<String>()
            .replace([',', '\n'], ";");
        let ok = error.is_empty();
        let _ = writeln!(
            self.file,
            "{},{workload},{iteration},{size},{op},{wall_ms:.3},{cpu_ms:.3},0.000,{cpu_ms:.3},{bytes},{},{e},{alloc},0,{rss_kib},0,{}",
            self.impl_name, ok as i32, self.labels
        );
        if !ok {
            eprintln!("  ERROR {workload} #{iteration} {op}: {error}");
        }
        if iteration >= 0 {
            self.rows.push(Row {
                workload: workload.to_string(),
                op: op.to_string(),
                size,
                wall_ms,
                cpu_ms,
                bytes,
                ok,
            });
        }
    }

    fn summary(&self) {
        let mut groups: std::collections::BTreeMap<(String, String, i64), Vec<&Row>> =
            Default::default();
        for r in &self.rows {
            groups
                .entry((r.workload.clone(), r.op.clone(), r.size))
                .or_default()
                .push(r);
        }
        let pct = |mut v: Vec<f64>, p: f64| {
            if v.is_empty() {
                return 0.0;
            }
            v.sort_by(|a, b| a.partial_cmp(b).unwrap());
            v[((p / 100.0 * (v.len() - 1) as f64) + 0.5) as usize]
        };
        println!("\nSUMMARY impl={} labels={}", self.impl_name, self.labels);
        for ((w, op, size), rows) in groups {
            let ok: Vec<_> = rows.iter().filter(|r| r.ok).collect();
            let lat: Vec<f64> = ok.iter().map(|r| r.wall_ms).collect();
            let cpu: Vec<f64> = ok.iter().map(|r| r.cpu_ms).collect();
            let tput: Vec<f64> = ok
                .iter()
                .filter(|r| r.bytes > 0)
                .map(|r| (r.bytes as f64 / 1048576.0) / (r.wall_ms / 1000.0))
                .collect();
            let mut line = String::new();
            let _ = write!(
                line,
                "{w:<11}{op:<14}{size:>11}{:>5}{:>5}{:>10.2}{:>10.2}{:>10.2}{:>10.2}{:>11.2}",
                rows.len(),
                rows.len() - ok.len(),
                pct(lat.clone(), 50.0),
                pct(lat.clone(), 90.0),
                pct(lat, 99.0),
                pct(tput, 50.0),
                pct(cpu, 50.0)
            );
            println!("{line}");
        }
    }
}

struct Usage {
    start: Instant,
    cpu: stats::Snapshot,
}
impl Usage {
    fn new() -> Self {
        Self {
            cpu: stats::snapshot(),
            start: Instant::now(),
        }
    }
    fn stop(&self) -> (f64, f64, u64, u64) {
        let wall = self.start.elapsed().as_secs_f64() * 1000.0;
        let now = stats::snapshot();
        let cpu = ((now.user_cpu_us + now.system_cpu_us)
            - (self.cpu.user_cpu_us + self.cpu.system_cpu_us)) as f64
            / 1000.0;
        (
            wall,
            cpu,
            now.allocated_bytes - self.cpu.allocated_bytes,
            now.rss_bytes / 1024,
        )
    }
}

async fn run(a: Args) -> Result<(), String> {
    let storage = Storage::builder()
        .with_grpc_subchannel_count(a.grpc_subchannels)
        .build()
        .await
        .map_err(err)?;
    let control = StorageControl::builder().build().await.map_err(err)?;
    let core = Arc::new(Core {
        storage,
        control,
        unbuffered: a.unbuffered,
    });
    let impl_name = if a.label.is_empty() {
        "pure-rust".to_string()
    } else {
        format!("pure-rust-{}", a.label)
    };
    let upload_mode = if a.unbuffered {
        ";upload=unbuffered"
    } else {
        ""
    };
    let labels = format!(
        "transport=pure-rust;json=http1.1;list=grpc-cfe;bidi=grpc-cfe;channels={};crate=1.21.0{upload_mode}",
        a.grpc_subchannels
    );
    let out = if a.out.is_empty() {
        format!("{impl_name}-{}.csv", a.workload)
    } else {
        a.out.clone()
    };
    let mut file = std::fs::File::create(&out).map_err(err)?;
    let _ = writeln!(
        file,
        "impl,workload,iteration,object_size,op,latency_ms,cpu_ms_app,cpu_ms_peer,cpu_ms_total,bytes,ok,error,alloc_bytes_app,alloc_bytes_peer,rss_kib_app,rss_kib_peer,labels"
    );
    let mut rec = Recorder {
        impl_name: impl_name.clone(),
        labels,
        file,
        rows: vec![],
    };
    println!(
        "impl={impl_name} bucket={} workload={} iterations={} csv={out}",
        a.bucket, a.workload, a.iterations
    );

    let max =
        a.object_sizes
            .iter()
            .copied()
            .max()
            .unwrap_or(0)
            .max(if a.workload == "read50x2x4" {
                200 * MIB
            } else {
                0
            });
    let mut g = SplitMix64(7);
    let mut buf = vec![0_u8; max as usize];
    for chunk in buf.as_chunks_mut::<8>().0 {
        *chunk = g.next().to_ne_bytes();
    }
    let data = Bytes::from(buf);
    let run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let name = |w: &str, i: i32| format!("{}{impl_name}/{run_id}/{w}-{i}", a.prefix);
    let mut rng = SplitMix64(42);
    let whole = Usage::new();
    let w = a.workload.clone();

    macro_rules! measure {
        ($i:expr, $size:expr, $op:expr, $body:expr) => {{
            let u = Usage::new();
            let r: OpResult = $body.await;
            let (wall, cpu, alloc, rss) = u.stop();
            let (bytes, e) = match &r {
                Ok(b) => (*b, String::new()),
                Err(e) => (0, e.clone()),
            };
            if $i >= 0 {
                rec.add(&w, $i, $size, $op, wall, cpu, bytes, &e, alloc, rss);
            }
            // A plain bool (not `Result::is_ok`'s #[must_use]) so call
            // sites that ignore the outcome need no `let _ =`.
            let ok: bool = r.is_ok();
            ok
        }};
    }

    match w.as_str() {
        "w1r3" => {
            for i in -a.warmup..a.iterations {
                let size = a.object_sizes[rng.below(a.object_sizes.len() as u64) as usize];
                let resumable = i.rem_euclid(2) == 1;
                let obj = name("w1r3", i);
                let payload = data.slice(0..size as usize);
                if !measure!(
                    i,
                    size,
                    if resumable {
                        "RESUMABLE"
                    } else {
                        "SINGLE-SHOT"
                    },
                    core.json_upload(&a.bucket, &obj, payload, resumable)
                ) {
                    continue;
                }
                for op in ["READ[0]", "READ[1]", "READ[2]"] {
                    measure!(i, size, op, async {
                        let n = core.json_read(&a.bucket, &obj).await?;
                        if n != size {
                            Err(format!("short read {n}"))
                        } else {
                            Ok(n)
                        }
                    });
                }
                measure!(i, size, "DELETE", core.delete(&a.bucket, &obj));
            }
        }
        "list" => {
            let prefix = format!("{}/", name("list", 0));
            println!("  list setup: creating {} objects", a.list_objects);
            let small = data.slice(0..16);
            let sem = Arc::new(tokio::sync::Semaphore::new(16));
            let mut tasks = vec![];
            for j in 0..a.list_objects {
                let (core, sem, small, obj, bucket, zonal) = (
                    core.clone(),
                    sem.clone(),
                    small.clone(),
                    format!("{prefix}{j}"),
                    a.bucket.clone(),
                    a.zonal,
                );
                tasks.push(tokio::spawn(async move {
                    let _p = sem.acquire().await;
                    let r = if zonal {
                        core.appendable_upload(&bucket, &obj, small).await
                    } else {
                        core.json_upload(&bucket, &obj, small, false).await
                    };
                    if let Err(e) = r {
                        eprintln!("  setup {obj}: {e}");
                    }
                }));
            }
            futures::future::join_all(tasks).await;
            for i in -a.warmup..a.iterations {
                for page in [1000, 100] {
                    let n = a.list_objects as i64;
                    measure!(i, n, &format!("LIST-P{page}"), async {
                        let got = core.list(&a.bucket, &prefix, page).await?;
                        if got != n {
                            Err(format!("listed {got}"))
                        } else {
                            Ok(got)
                        }
                    });
                }
            }
            let mut tasks = vec![];
            for j in 0..a.list_objects {
                let (core, sem, obj, bucket) = (
                    core.clone(),
                    sem.clone(),
                    format!("{prefix}{j}"),
                    a.bucket.clone(),
                );
                tasks.push(tokio::spawn(async move {
                    let _p = sem.acquire().await;
                    let _ = core.delete(&bucket, &obj).await;
                }));
            }
            futures::future::join_all(tasks).await;
        }
        "throughput" => {
            let size = a.object_sizes.iter().copied().max().unwrap();
            let src = name("throughput-src", 0);
            if !a.throughput_write {
                let r = if a.zonal {
                    core.appendable_upload(&a.bucket, &src, data.slice(0..size as usize))
                        .await
                } else {
                    core.json_upload(
                        &a.bucket,
                        &src,
                        data.slice(0..size as usize),
                        size >= 16 * MIB,
                    )
                    .await
                };
                r.map_err(|e| format!("throughput setup: {e}"))?;
            }
            let bidi = a.zonal || a.label.contains("bidi");
            let u = Usage::new();
            let mut tasks = vec![];
            for worker in 0..a.workers {
                let (core, a2, src, data) = (core.clone(), a.clone(), src.clone(), data.clone());
                let impl_name2 = impl_name.clone();
                tasks.push(tokio::spawn(async move {
                    let (mut bytes, mut errors) = (0_i64, 0_i32);
                    for i in 0..a2.iterations {
                        let r = if a2.throughput_write {
                            let obj = format!(
                                "{}{impl_name2}/{run_id}/throughput-{}",
                                a2.prefix,
                                worker * 100_000 + i
                            );
                            let p = data.slice(0..size as usize);
                            if a2.zonal {
                                core.appendable_upload(&a2.bucket, &obj, p).await
                            } else {
                                core.json_upload(&a2.bucket, &obj, p, size >= 16 * MIB)
                                    .await
                            }
                        } else if bidi {
                            match core.open(&a2.bucket, &src).await {
                                Ok(d) => read_ranges(&d, &[(0, 0)]).await,
                                Err(e) => Err(e),
                            }
                        } else {
                            core.json_read(&a2.bucket, &src).await
                        };
                        match r {
                            Ok(n) => bytes += n,
                            Err(e) => {
                                errors += 1;
                                eprintln!("  throughput worker {worker}: {e}");
                            }
                        }
                    }
                    (bytes, errors)
                }));
            }
            let (mut bytes, mut errors) = (0_i64, 0_i32);
            for t in futures::future::join_all(tasks).await {
                let (b, e) = t.map_err(err)?;
                bytes += b;
                errors += e;
            }
            let (wall, cpu, alloc, _) = u.stop();
            let peak_rss_kib = stats::snapshot().peak_rss_bytes / 1024;
            let e = if errors == 0 {
                String::new()
            } else {
                format!("{errors} errors")
            };
            let op = if a.throughput_write {
                "THROUGHPUT-WRITE"
            } else {
                "THROUGHPUT-READ"
            };
            rec.add(&w, 0, size, op, wall, cpu, bytes, &e, alloc, peak_rss_kib);
            println!(
                "  throughput: {} workers, {:.1} MiB/s",
                a.workers,
                (bytes as f64 / 1048576.0) / (wall / 1000.0)
            );
            // Clean up written objects after all THROUGHPUT-WRITE timing, CPU,
            // allocation, and peak-RSS snapshots have been recorded.
            if a.throughput_write {
                let sem = Arc::new(tokio::sync::Semaphore::new(16));
                let mut cleanup = vec![];
                for worker in 0..a.workers {
                    for i in 0..a.iterations {
                        let (core, sem, obj, bucket) = (
                            core.clone(),
                            sem.clone(),
                            name("throughput", worker * 100_000 + i),
                            a.bucket.clone(),
                        );
                        cleanup.push(tokio::spawn(async move {
                            let _p = sem.acquire().await;
                            let _ = core.delete(&bucket, &obj).await;
                        }));
                    }
                }
                futures::future::join_all(cleanup).await;
            } else {
                let _ = core.delete(&a.bucket, &src).await;
            }
        }
        "open" | "random8k" | "read50x2x4" | "appendable" => {
            for i in -a.warmup..a.iterations {
                let size = if w == "read50x2x4" {
                    200 * MIB
                } else {
                    a.object_sizes[rng.below(a.object_sizes.len() as u64) as usize]
                };
                let obj = name(&w, i);
                let random_offset = rng.below((size - 8 * KIB).max(0) as u64 + 1) as i64;
                let payload = data.slice(0..size as usize);
                let up_op = if a.zonal { "APPENDABLE" } else { "UPLOAD" };
                let up = async {
                    if a.zonal {
                        core.appendable_upload(&a.bucket, &obj, payload).await
                    } else {
                        core.json_upload(&a.bucket, &obj, payload, size >= 16 * MIB)
                            .await
                    }
                };
                if !measure!(i, size, up_op, up) {
                    continue;
                }
                match w.as_str() {
                    "open" => {
                        measure!(i, size, "open", async {
                            core.open(&a.bucket, &obj).await.map(|_| 0)
                        });
                    }
                    "random8k" => {
                        let len = (8 * KIB).min(size);
                        measure!(i, size, "random8k", async {
                            let d = core.open(&a.bucket, &obj).await?;
                            let n = read_ranges(&d, &[(random_offset, len)]).await?;
                            if n != len {
                                Err("short read".to_string())
                            } else {
                                Ok(n)
                            }
                        });
                    }
                    "read50x2x4" => {
                        let ranges: Vec<(i64, i64)> =
                            (0..50).map(|j| (j * 4 * MIB, 2 * MIB)).collect();
                        measure!(i, size, "read50x2x4", async {
                            let d = core.open(&a.bucket, &obj).await?;
                            let n = read_ranges(&d, &ranges).await?;
                            if n != 100 * MIB {
                                Err(format!("short read {n}"))
                            } else {
                                Ok(n)
                            }
                        });
                    }
                    _ => {
                        measure!(i, size, "READ-BIDI", async {
                            let d = core.open(&a.bucket, &obj).await?;
                            let n = read_ranges(&d, &[(0, 0)]).await?;
                            if n != size {
                                Err(format!("short read {n}"))
                            } else {
                                Ok(n)
                            }
                        });
                    }
                }
                measure!(i, size, "DELETE", core.delete(&a.bucket, &obj));
            }
        }
        other => return Err(format!("unknown workload {other}")),
    }
    let (wall, cpu, alloc, _) = whole.stop();
    let peak = stats::snapshot().peak_rss_bytes / 1024;
    rec.add(&w, -1, 0, "RUN_TOTAL", wall, cpu, 0, "", alloc, peak);
    let _ = rec.file.flush();
    rec.summary();
    Ok(())
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("gcs-core-bench: {e}");
            return std::process::ExitCode::from(2);
        }
    };
    match run(args).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("gcs-core-bench: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
