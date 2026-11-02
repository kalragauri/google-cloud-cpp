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

//! `gcs.core.v0.StorageCore` implemented on the `google-cloud-storage` crate.
//!
//! Every RPC is a thin translation: the crate does retries, resumable-upload
//! state, checksum validation and connection management. In `--synthetic`
//! mode the same RPCs serve zero-filled data without touching GCS, so the
//! wrapper ↔ sidecar hop can be measured on its own.

use crate::{convert, pb, stats};
use bytes::{Bytes, BytesMut};
use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::model_ext::ReadRange;
use google_cloud_storage::object_descriptor::ObjectDescriptor;
use google_cloud_storage::streaming_source::{SizeHint, StreamingSource};
use pb::storage_core_server::StorageCore;
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::{Notify, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

/// Must match the `=` pin in Cargo.toml; reported by GetVersion.
pub const STORAGE_CRATE_VERSION: &str = "1.21.0";
/// Upper bound for a `data` message. The wrapper raises its receive limit
/// above this; larger hints are clamped rather than rejected.
pub const MAX_CHUNK: usize = 16 << 20;
/// Messages buffered per response stream before the producer waits for the
/// wrapper (backpressure from the UDS connection into the core).
const STREAM_DEPTH: usize = 4;

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub default_chunk: usize,
    /// Coalesce the HTTP body's small chunks into `default_chunk`-sized
    /// messages (one copy in the sidecar, far fewer gRPC messages).
    pub coalesce_reads: bool,
}

pub struct Clients {
    pub storage: Storage,
    pub control: StorageControl,
}

enum Descriptor {
    Real(ObjectDescriptor),
    Synthetic { size: u64 },
}

pub struct Service {
    clients: Option<Clients>,
    config: Config,
    descriptors: Mutex<HashMap<u64, Arc<Descriptor>>>,
    handle_epoch: u64,
    next_handle: AtomicU64,
    /// Object sizes "written" in synthetic mode, so later reads see them.
    synthetic_sizes: Mutex<HashMap<(String, String), u64>>,
    zeros: Bytes,
    shutdown: Arc<Notify>,
}

/// Non-zero upper 32 bits identifying one `Service` instance. Mixing `(pid,
/// timestamp)` with a per-process monotonic counter keeps handles distinct
/// both across respawned sidecar child processes and across in-process tests.
fn next_handle_epoch() -> u64 {
    static BASE: OnceLock<u32> = OnceLock::new();
    static SEQ: AtomicU32 = AtomicU32::new(0);
    let base = *BASE.get_or_init(|| {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let mut z = nanos ^ u64::from(std::process::id()).rotate_left(32);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        (z ^ (z >> 31)) as u32
    });
    loop {
        let upper = base.wrapping_add(SEQ.fetch_add(1, Ordering::Relaxed));
        if upper != 0 {
            return u64::from(upper) << 32;
        }
    }
}

impl Service {
    pub fn new(clients: Option<Clients>, config: Config, shutdown: Arc<Notify>) -> Self {
        Self {
            clients,
            config,
            descriptors: Mutex::new(HashMap::new()),
            handle_epoch: next_handle_epoch(),
            next_handle: AtomicU64::new(0),
            synthetic_sizes: Mutex::new(HashMap::new()),
            zeros: Bytes::from(vec![0_u8; MAX_CHUNK]),
            shutdown,
        }
    }

    fn chunk_size(&self, hint: u32) -> usize {
        match hint as usize {
            0 => self.config.default_chunk,
            n => n.min(MAX_CHUNK),
        }
    }

    /// Size of a synthetic object: what an earlier synthetic write recorded,
    /// else a decimal suffix of the name (`.../100000000`), else NOT_FOUND.
    fn synthetic_size(&self, bucket: &str, object: &str) -> Result<u64, Status> {
        let key = (convert::bucket_id(bucket).to_string(), object.to_string());
        if let Some(size) = self.synthetic_sizes.lock().unwrap().get(&key) {
            return Ok(*size);
        }
        object
            .rsplit(['/', '-'])
            .next()
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| {
                Status::not_found(format!("synthetic object {bucket}/{object} not found"))
            })
    }

    fn descriptor(&self, handle: u64) -> Result<Arc<Descriptor>, Status> {
        if (handle & 0xffff_ffff_0000_0000) != self.handle_epoch {
            return Err(Status::failed_precondition(format!(
                "stale object handle {handle:#x} from a previous sidecar instance"
            )));
        }
        self.descriptors
            .lock()
            .unwrap()
            .get(&handle)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("unknown object handle {handle:#x}")))
    }
}

/// Byte range `[start, end)` of a synthetic object selected by `range`.
fn synthetic_span(size: u64, range: Option<&pb::ReadRange>) -> Result<(u64, u64), Status> {
    let Some(r) = range else { return Ok((0, size)) };
    convert::read_range(Some(r))?;
    let span = if r.offset < 0 {
        (size.saturating_sub(r.offset.unsigned_abs()), size)
    } else {
        let start = r.offset as u64;
        if start > size || (start == size && size > 0) {
            return Err(Status::out_of_range(format!(
                "offset {start} >= object size {size}"
            )));
        }
        let end = if r.limit == 0 {
            size
        } else {
            (start + r.limit as u64).min(size)
        };
        (start, end)
    };
    Ok(span)
}

fn synthetic_metadata(bucket: &str, object: &str, size: u64) -> pb::ObjectMetadata {
    pb::ObjectMetadata {
        bucket: convert::bucket_id(bucket).to_string(),
        name: object.to_string(),
        generation: 1,
        metageneration: 1,
        size,
        storage_class: "SYNTHETIC".to_string(),
        ..Default::default()
    }
}

/// Sends `bytes` as one or more messages of at most `chunk` bytes.
///
/// With `pending`, small inputs accumulate until a full chunk is available.
/// Oversized inputs are split with `Bytes::split_to`, which shares the
/// buffer instead of copying. Returns false once the receiver is gone (the
/// wrapper cancelled), so the producer stops pulling from the core.
async fn forward<T>(
    tx: &mpsc::Sender<Result<T, Status>>,
    mut bytes: Bytes,
    chunk: usize,
    pending: Option<&mut BytesMut>,
    make: &impl Fn(Bytes) -> T,
) -> bool {
    match pending {
        Some(pending) if !pending.is_empty() || bytes.len() < chunk => {
            pending.extend_from_slice(&bytes);
            while pending.len() >= chunk {
                let head = pending.split_to(chunk).freeze();
                if tx.send(Ok(make(head))).await.is_err() {
                    return false;
                }
            }
            true
        }
        _ => {
            while !bytes.is_empty() {
                let head = bytes.split_to(chunk.min(bytes.len()));
                if tx.send(Ok(make(head))).await.is_err() {
                    return false;
                }
            }
            true
        }
    }
}

/// Streams `[start, end)` of zeros as `chunk`-sized slices of one shared
/// buffer: no per-message allocation, so synthetic numbers are hop-only.
async fn send_zeros<T>(
    tx: &mpsc::Sender<Result<T, Status>>,
    zeros: &Bytes,
    start: u64,
    end: u64,
    chunk: usize,
    make: &impl Fn(Bytes) -> T,
) {
    let mut remaining = end.saturating_sub(start) as usize;
    while remaining > 0 {
        let n = remaining.min(chunk).min(zeros.len());
        if tx.send(Ok(make(zeros.slice(0..n)))).await.is_err() {
            return;
        }
        remaining -= n;
    }
}

/// Adapts the wrapper's client-streaming request into the crate's upload
/// source, so uploaded bytes go from the UDS connection to the HTTP body
/// without being collected first.
struct RequestSource {
    first: Option<Bytes>,
    // `Streaming` is `Send` but not `Sync`, and `send_buffered` requires
    // `Sync`. Only `get_mut` is ever used, so the mutex is never contended.
    stream: Mutex<Streaming<pb::WriteObjectRequest>>,
    size_hint: Option<u64>,
}

impl StreamingSource for RequestSource {
    type Error = Status;

    async fn next(&mut self) -> Option<Result<Bytes, Self::Error>> {
        if let Some(first) = self.first.take() {
            return Some(Ok(first));
        }
        let stream = self.stream.get_mut().expect("never locked");
        loop {
            match stream.message().await {
                Ok(Some(m)) if m.data.is_empty() => continue,
                Ok(Some(m)) => return Some(Ok(m.data)),
                Ok(None) => return None,
                Err(status) => return Some(Err(status)),
            }
        }
    }

    async fn size_hint(&self) -> Result<SizeHint, Self::Error> {
        Ok(match self.size_hint {
            Some(n) => SizeHint::with_exact(n),
            None => SizeHint::new(),
        })
    }
}

fn data_response(data: Bytes) -> pb::ReadObjectResponse {
    pb::ReadObjectResponse {
        metadata: None,
        data,
    }
}

#[tonic::async_trait]
impl StorageCore for Service {
    async fn get_version(
        &self,
        _: Request<pb::GetVersionRequest>,
    ) -> Result<Response<pb::GetVersionResponse>, Status> {
        Ok(Response::new(pb::GetVersionResponse {
            sidecar_version: env!("CARGO_PKG_VERSION").to_string(),
            storage_crate_version: STORAGE_CRATE_VERSION.to_string(),
            pid: std::process::id(),
            bidi_enabled: cfg!(google_cloud_unstable_storage_bidi),
            synthetic: self.clients.is_none(),
        }))
    }

    async fn get_stats(
        &self,
        _: Request<pb::GetStatsRequest>,
    ) -> Result<Response<pb::GetStatsResponse>, Status> {
        let s = stats::snapshot();
        Ok(Response::new(pb::GetStatsResponse {
            user_cpu_us: s.user_cpu_us,
            system_cpu_us: s.system_cpu_us,
            rss_bytes: s.rss_bytes,
            peak_rss_bytes: s.peak_rss_bytes,
            allocated_bytes: s.allocated_bytes,
            threads: s.threads,
        }))
    }

    async fn shutdown(
        &self,
        _: Request<pb::ShutdownRequest>,
    ) -> Result<Response<pb::ShutdownResponse>, Status> {
        // Graceful: the server finishes in-flight RPCs (including this one).
        self.shutdown.notify_one();
        Ok(Response::new(pb::ShutdownResponse {}))
    }

    type ReadObjectStream = ReceiverStream<Result<pb::ReadObjectResponse, Status>>;

    async fn read_object(
        &self,
        request: Request<pb::ReadObjectRequest>,
    ) -> Result<Response<Self::ReadObjectStream>, Status> {
        let r = request.into_inner();
        if r.generation < 0 {
            return Err(Status::invalid_argument("generation must be >= 0"));
        }
        let range = convert::read_range(r.range.as_ref())?;
        let chunk = self.chunk_size(r.chunk_size_hint);
        let (tx, rx) = mpsc::channel(STREAM_DEPTH);

        let Some(clients) = &self.clients else {
            let size = self.synthetic_size(&r.bucket, &r.object)?;
            let (start, end) = synthetic_span(size, r.range.as_ref())?;
            let metadata = synthetic_metadata(&r.bucket, &r.object, size);
            let zeros = self.zeros.clone();
            tokio::spawn(async move {
                let first = pb::ReadObjectResponse {
                    metadata: Some(metadata),
                    data: Bytes::new(),
                };
                if tx.send(Ok(first)).await.is_ok() {
                    send_zeros(&tx, &zeros, start, end, chunk, &data_response).await;
                }
            });
            return Ok(Response::new(ReceiverStream::new(rx)));
        };

        let mut builder = clients
            .storage
            .read_object(convert::bucket_name(&r.bucket), &r.object)
            .set_read_range(range);
        if r.generation > 0 {
            builder = builder.set_generation(r.generation);
        }
        let mut response = builder.send().await.map_err(|e| convert::to_status(&e))?;
        let metadata = convert::from_highlights(&r.bucket, &r.object, &response.object());
        let coalesce = self.config.coalesce_reads;
        tokio::spawn(async move {
            let first = pb::ReadObjectResponse {
                metadata: Some(metadata),
                data: Bytes::new(),
            };
            if tx.send(Ok(first)).await.is_err() {
                return;
            }
            let mut pending = BytesMut::new();
            while let Some(item) = response.next().await {
                let bytes = match item {
                    Ok(b) => b,
                    Err(e) => {
                        let _ = tx.send(Err(convert::to_status(&e))).await;
                        return;
                    }
                };
                let pending = coalesce.then_some(&mut pending);
                if !forward(&tx, bytes, chunk, pending, &data_response).await {
                    return;
                }
            }
            if !pending.is_empty() {
                let _ = tx.send(Ok(data_response(pending.freeze()))).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn write_object(
        &self,
        request: Request<Streaming<pb::WriteObjectRequest>>,
    ) -> Result<Response<pb::WriteObjectResponse>, Status> {
        let mut stream = request.into_inner();
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("empty WriteObject stream"))?;
        let spec = first.spec.ok_or_else(|| {
            Status::invalid_argument("the first WriteObject message must carry spec")
        })?;
        let mode = pb::write_spec::UploadMode::try_from(spec.upload_mode).map_err(|_| {
            Status::invalid_argument(format!("unknown upload_mode {}", spec.upload_mode))
        })?;
        if mode == pb::write_spec::UploadMode::SingleShot && spec.size_hint.is_none() {
            // The crate picks single-shot only when the exact size is below the
            // threshold; without a size it always goes resumable.
            return Err(Status::invalid_argument(
                "UPLOAD_MODE_SINGLE_SHOT requires size_hint",
            ));
        }

        let Some(clients) = &self.clients else {
            let mut size = first.data.len() as u64;
            while let Some(m) = stream.message().await? {
                size += m.data.len() as u64;
            }
            self.synthetic_sizes.lock().unwrap().insert(
                (
                    convert::bucket_id(&spec.bucket).to_string(),
                    spec.object.clone(),
                ),
                size,
            );
            let metadata = synthetic_metadata(&spec.bucket, &spec.object, size);
            return Ok(Response::new(pb::WriteObjectResponse {
                metadata: Some(metadata),
            }));
        };
        let source = RequestSource {
            first: (!first.data.is_empty()).then_some(first.data),
            stream: Mutex::new(stream),
            size_hint: spec.size_hint,
        };
        let mut builder =
            clients
                .storage
                .write_object(convert::bucket_name(&spec.bucket), &spec.object, source);
        if let Some(g) = spec.if_generation_match {
            builder = builder.set_if_generation_match(g);
        }
        if !spec.content_type.is_empty() {
            builder = builder.set_content_type(&spec.content_type);
        }
        builder = match mode {
            pb::write_spec::UploadMode::Auto => builder,
            pb::write_spec::UploadMode::SingleShot => {
                builder.with_resumable_upload_threshold(usize::MAX)
            }
            pb::write_spec::UploadMode::Resumable => {
                builder.with_resumable_upload_threshold(0_usize)
            }
        };
        let object = builder
            .send_buffered()
            .await
            .map_err(|e| convert::to_status(&e))?;
        Ok(Response::new(pb::WriteObjectResponse {
            metadata: Some(convert::from_object(&object)),
        }))
    }

    async fn list_objects(
        &self,
        request: Request<pb::ListObjectsRequest>,
    ) -> Result<Response<pb::ListObjectsResponse>, Status> {
        let r = request.into_inner();
        if r.page_size < 0 {
            return Err(Status::invalid_argument("page_size must be >= 0"));
        }
        let Some(clients) = &self.clients else {
            let bucket = convert::bucket_id(&r.bucket);
            let mut objects = Vec::new();
            let mut prefixes = BTreeSet::new();
            {
                let map = self.synthetic_sizes.lock().unwrap();
                for ((b, name), &size) in map.iter() {
                    if b != bucket || !name.starts_with(&r.prefix) {
                        continue;
                    }
                    if !r.delimiter.is_empty() {
                        let rest = &name[r.prefix.len()..];
                        if let Some(pos) = rest.find(&r.delimiter) {
                            prefixes.insert(format!("{}{}{}", r.prefix, &rest[..pos], r.delimiter));
                            continue;
                        }
                    }
                    objects.push((name.clone(), size));
                }
            }
            objects.sort_by(|a, b| a.0.cmp(&b.0));
            let remaining: Vec<(String, u64)> = objects
                .into_iter()
                .filter(|(name, _)| {
                    r.page_token.is_empty() || name.as_str() > r.page_token.as_str()
                })
                .collect();
            let limit = if r.page_size == 0 {
                remaining.len()
            } else {
                (r.page_size as usize).min(remaining.len())
            };
            let next_page_token = if limit < remaining.len() {
                remaining[limit - 1].0.clone()
            } else {
                String::new()
            };
            let page_objects = remaining[..limit]
                .iter()
                .map(|(name, size)| synthetic_metadata(bucket, name, *size))
                .collect();
            let page_prefixes = if r.page_token.is_empty() {
                prefixes.into_iter().collect()
            } else {
                Vec::new()
            };
            return Ok(Response::new(pb::ListObjectsResponse {
                objects: page_objects,
                prefixes: page_prefixes,
                next_page_token,
            }));
        };
        let mut builder = clients
            .control
            .list_objects()
            .set_parent(convert::bucket_name(&r.bucket));
        if !r.prefix.is_empty() {
            builder = builder.set_prefix(r.prefix);
        }
        if !r.delimiter.is_empty() {
            builder = builder.set_delimiter(r.delimiter);
        }
        if r.page_size > 0 {
            builder = builder.set_page_size(r.page_size);
        }
        if !r.page_token.is_empty() {
            builder = builder.set_page_token(r.page_token);
        }
        let response = builder.send().await.map_err(|e| convert::to_status(&e))?;
        Ok(Response::new(pb::ListObjectsResponse {
            objects: response.objects.iter().map(convert::from_object).collect(),
            prefixes: response.prefixes,
            next_page_token: response.next_page_token,
        }))
    }

    async fn delete_object(
        &self,
        request: Request<pb::DeleteObjectRequest>,
    ) -> Result<Response<pb::DeleteObjectResponse>, Status> {
        let r = request.into_inner();
        if r.generation < 0 {
            return Err(Status::invalid_argument("generation must be >= 0"));
        }
        let Some(clients) = &self.clients else {
            self.synthetic_sizes
                .lock()
                .unwrap()
                .remove(&(convert::bucket_id(&r.bucket).to_string(), r.object));
            return Ok(Response::new(pb::DeleteObjectResponse {}));
        };
        let mut builder = clients
            .control
            .delete_object()
            .set_bucket(convert::bucket_name(&r.bucket))
            .set_object(r.object);
        if r.generation > 0 {
            builder = builder.set_generation(r.generation);
        }
        builder.send().await.map_err(|e| convert::to_status(&e))?;
        Ok(Response::new(pb::DeleteObjectResponse {}))
    }

    async fn open_object(
        &self,
        request: Request<pb::OpenObjectRequest>,
    ) -> Result<Response<pb::OpenObjectResponse>, Status> {
        let r = request.into_inner();
        if r.generation < 0 {
            return Err(Status::invalid_argument("generation must be >= 0"));
        }
        let (descriptor, metadata) = match &self.clients {
            None => {
                let size = self.synthetic_size(&r.bucket, &r.object)?;
                (
                    Descriptor::Synthetic { size },
                    synthetic_metadata(&r.bucket, &r.object, size),
                )
            }
            Some(clients) => {
                let mut builder = clients
                    .storage
                    .open_object(convert::bucket_name(&r.bucket), &r.object);
                if r.generation > 0 {
                    builder = builder.set_generation(r.generation);
                }
                let d = builder.send().await.map_err(|e| convert::to_status(&e))?;
                let metadata = convert::from_object(&d.object());
                (Descriptor::Real(d), metadata)
            }
        };
        let seq = (self.next_handle.fetch_add(1, Ordering::Relaxed) + 1) & 0xffff_ffff;
        let handle = self.handle_epoch | seq;
        self.descriptors
            .lock()
            .unwrap()
            .insert(handle, Arc::new(descriptor));
        Ok(Response::new(pb::OpenObjectResponse {
            handle,
            metadata: Some(metadata),
        }))
    }

    type ReadRangesStream = ReceiverStream<Result<pb::ReadRangesResponse, Status>>;

    async fn read_ranges(
        &self,
        request: Request<pb::ReadRangesRequest>,
    ) -> Result<Response<Self::ReadRangesStream>, Status> {
        let r = request.into_inner();
        let descriptor = self.descriptor(r.handle)?;
        // Validate every range before starting any read.
        let ranges: Vec<ReadRange> = r
            .ranges
            .iter()
            .map(|x| convert::read_range(Some(x)))
            .collect::<Result<_, _>>()?;
        let chunk = self.chunk_size(r.chunk_size_hint);
        let (tx, rx) = mpsc::channel(STREAM_DEPTH.max(r.ranges.len()));
        for (index, (range, wire)) in ranges.into_iter().zip(r.ranges.iter()).enumerate() {
            let tx = tx.clone();
            let descriptor = descriptor.clone();
            let index = index as u32;
            let make = move |data| pb::ReadRangesResponse {
                range_index: index,
                data,
            };
            match &*descriptor {
                Descriptor::Synthetic { size } => {
                    let (start, end) = synthetic_span(*size, Some(wire))?;
                    let zeros = self.zeros.clone();
                    tokio::spawn(
                        async move { send_zeros(&tx, &zeros, start, end, chunk, &make).await },
                    );
                }
                Descriptor::Real(_) => {
                    tokio::spawn(async move {
                        let Descriptor::Real(d) = &*descriptor else {
                            unreachable!()
                        };
                        let mut response = d.read_range(range).await;
                        while let Some(item) = response.next().await {
                            match item {
                                Ok(bytes) => {
                                    if !forward(&tx, bytes, chunk, None, &make).await {
                                        return;
                                    }
                                }
                                Err(e) => {
                                    let _ = tx.send(Err(convert::to_status(&e))).await;
                                    return;
                                }
                            }
                        }
                    });
                }
            }
        }
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn close_object(
        &self,
        request: Request<pb::CloseObjectRequest>,
    ) -> Result<Response<pb::CloseObjectResponse>, Status> {
        // Dropping the last reference closes the BidiReadObject stream; reads
        // still in flight keep their own reference until they finish.
        self.descriptors
            .lock()
            .unwrap()
            .remove(&request.into_inner().handle);
        Ok(Response::new(pb::CloseObjectResponse {}))
    }

    async fn appendable_write(
        &self,
        request: Request<Streaming<pb::AppendableWriteRequest>>,
    ) -> Result<Response<pb::AppendableWriteResponse>, Status> {
        let mut stream = request.into_inner();
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("empty AppendableWrite stream"))?;
        let spec = first.spec.clone().ok_or_else(|| {
            Status::invalid_argument("the first AppendableWrite message must carry spec")
        })?;
        validate_appendable_spec(&spec)?;

        let Some(clients) = &self.clients else {
            let mut size = first.data.len() as u64;
            let mut finalize = first.finalize;
            while let Some(m) = stream.message().await? {
                validate_appendable_chunk(finalize, &m)?;
                size += m.data.len() as u64;
                finalize = m.finalize;
            }
            self.synthetic_sizes.lock().unwrap().insert(
                (
                    convert::bucket_id(&spec.bucket).to_string(),
                    spec.object.clone(),
                ),
                size,
            );
            return Ok(Response::new(pb::AppendableWriteResponse {
                persisted_size: size as i64,
                metadata: finalize.then(|| synthetic_metadata(&spec.bucket, &spec.object, size)),
            }));
        };
        appendable_write_real(clients, spec, first, stream).await
    }
}

fn validate_appendable_spec(spec: &pb::WriteSpec) -> Result<(), Status> {
    let mode = pb::write_spec::UploadMode::try_from(spec.upload_mode).map_err(|_| {
        Status::invalid_argument(format!("unknown upload_mode {}", spec.upload_mode))
    })?;
    if mode != pb::write_spec::UploadMode::Auto {
        return Err(Status::invalid_argument(
            "AppendableWrite only supports UPLOAD_MODE_AUTO",
        ));
    }
    if spec.size_hint.is_some() {
        return Err(Status::invalid_argument(
            "AppendableWrite does not support size_hint",
        ));
    }
    Ok(())
}

fn validate_appendable_chunk(
    prev_finalized: bool,
    msg: &pb::AppendableWriteRequest,
) -> Result<(), Status> {
    if prev_finalized {
        return Err(Status::invalid_argument(
            "AppendableWrite finalize may only be set on the last message",
        ));
    }
    if msg.spec.is_some() {
        return Err(Status::invalid_argument(
            "only the first AppendableWrite message may carry spec",
        ));
    }
    Ok(())
}

#[cfg(google_cloud_unstable_storage_bidi)]
async fn appendable_write_real(
    clients: &Clients,
    spec: pb::WriteSpec,
    first: pb::AppendableWriteRequest,
    mut stream: Streaming<pb::AppendableWriteRequest>,
) -> Result<Response<pb::AppendableWriteResponse>, Status> {
    let to_status = |e: google_cloud_storage::Error| convert::to_status(&e);
    let mut builder = clients
        .storage
        .open_appendable_object(convert::bucket_name(&spec.bucket), &spec.object);
    if let Some(g) = spec.if_generation_match {
        builder = builder.set_if_generation_match(g);
    }
    if !spec.content_type.is_empty() {
        builder = builder.set_content_type(&spec.content_type);
    }
    let mut writer = builder.send().await.map_err(to_status)?;
    let mut finalize = first.finalize;
    if !first.data.is_empty() {
        writer.append(first.data).await.map_err(to_status)?;
    }
    while let Some(m) = stream.message().await? {
        validate_appendable_chunk(finalize, &m)?;
        if !m.data.is_empty() {
            writer.append(m.data).await.map_err(to_status)?;
        }
        finalize = m.finalize;
    }
    let response = if finalize {
        let object = writer.finalize().await.map_err(to_status)?;
        pb::AppendableWriteResponse {
            persisted_size: object.size,
            metadata: Some(convert::from_object(&object)),
        }
    } else {
        let persisted_size = writer.close().await.map_err(to_status)?;
        pb::AppendableWriteResponse {
            persisted_size,
            metadata: None,
        }
    };
    Ok(Response::new(response))
}

#[cfg(not(google_cloud_unstable_storage_bidi))]
async fn appendable_write_real(
    _: &Clients,
    _: pb::WriteSpec,
    _: pb::AppendableWriteRequest,
    _: Streaming<pb::AppendableWriteRequest>,
) -> Result<Response<pb::AppendableWriteResponse>, Status> {
    Err(Status::unimplemented(
        "sidecar built without --cfg google_cloud_unstable_storage_bidi; appendable writes are unavailable",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(offset: i64, limit: i64) -> pb::ReadRange {
        pb::ReadRange { offset, limit }
    }

    /// Synthetic spans follow the same range semantics as the core: whole
    /// object, offset-to-end, bounded segment clamped at EOF, tail.
    #[test]
    fn synthetic_span_matches_range_semantics() {
        let cases = [
            (None, (0, 100)),
            (Some(range(0, 0)), (0, 100)),
            (Some(range(10, 0)), (10, 100)),
            (Some(range(10, 20)), (10, 30)),
            (Some(range(90, 20)), (90, 100)),
            (Some(range(-30, 0)), (70, 100)),
            (Some(range(-300, 0)), (0, 100)),
        ];
        for (input, want) in cases {
            assert_eq!(
                synthetic_span(100, input.as_ref()).unwrap(),
                want,
                "input={input:?}"
            );
        }
    }

    /// Reading at or past EOF of a non-empty object is OUT_OF_RANGE, like the
    /// JSON API's 416; a zero-byte object can still be read whole.
    #[test]
    fn synthetic_span_past_eof() {
        assert_eq!(
            synthetic_span(100, Some(&range(100, 0)))
                .unwrap_err()
                .code(),
            tonic::Code::OutOfRange
        );
        assert_eq!(
            synthetic_span(100, Some(&range(101, 0)))
                .unwrap_err()
                .code(),
            tonic::Code::OutOfRange
        );
        assert_eq!(synthetic_span(0, None).unwrap(), (0, 0));
    }

    async fn collect(mut rx: mpsc::Receiver<Result<Bytes, Status>>) -> Vec<usize> {
        let mut sizes = Vec::new();
        while let Some(Ok(b)) = rx.recv().await {
            sizes.push(b.len());
        }
        sizes
    }

    /// Without coalescing, oversized inputs are split into chunk-sized
    /// messages and small inputs pass through unchanged.
    #[tokio::test]
    async fn forward_splits_without_coalescing() {
        let (tx, rx) = mpsc::channel(16);
        assert!(forward(&tx, Bytes::from(vec![1; 10]), 4, None, &|b| b).await);
        assert!(forward(&tx, Bytes::from(vec![1; 3]), 4, None, &|b| b).await);
        drop(tx);
        assert_eq!(collect(rx).await, vec![4, 4, 2, 3]);
    }

    /// With coalescing, small inputs accumulate into full chunks and the
    /// caller flushes the remainder; no bytes are lost or duplicated.
    #[tokio::test]
    async fn forward_coalesces_small_inputs() {
        let (tx, rx) = mpsc::channel(16);
        let mut pending = BytesMut::new();
        for n in [3, 3, 3, 9] {
            assert!(forward(&tx, Bytes::from(vec![1; n]), 4, Some(&mut pending), &|b| b).await);
        }
        let rest = pending.len();
        drop(tx);
        let sent = collect(rx).await;
        assert_eq!(sent, vec![4, 4, 4, 4]);
        assert_eq!(sent.iter().sum::<usize>() + rest, 18);
    }

    /// A dropped receiver (wrapper cancelled) stops the producer.
    #[tokio::test]
    async fn forward_stops_when_receiver_dropped() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        assert!(!forward(&tx, Bytes::from(vec![1; 8]), 4, None, &|b| b).await);
    }

    /// Synthetic zeros cover exactly the requested span in chunk-sized
    /// slices.
    #[tokio::test]
    async fn send_zeros_covers_span() {
        let (tx, rx) = mpsc::channel(16);
        let zeros = Bytes::from(vec![0; 8]);
        send_zeros(&tx, &zeros, 5, 25, 8, &|b| b).await;
        drop(tx);
        assert_eq!(collect(rx).await, vec![8, 8, 4]);
    }

    fn test_service() -> Service {
        Service::new(
            None,
            Config {
                default_chunk: 4096,
                coalesce_reads: true,
            },
            Arc::new(Notify::new()),
        )
    }

    /// Handles from an earlier sidecar instance carry a different upper-32-bit
    /// epoch, so ReadRanges rejects them with FAILED_PRECONDITION and CloseObject
    /// cannot close a fresh handle in the new instance.
    #[tokio::test]
    async fn handle_epoch_isolates_instances() {
        let s1 = test_service();
        let s2 = test_service();
        let h1 = s1
            .open_object(Request::new(pb::OpenObjectRequest {
                bucket: "b".into(),
                object: "sized/1000".into(),
                generation: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .handle;
        let h2 = s2
            .open_object(Request::new(pb::OpenObjectRequest {
                bucket: "b".into(),
                object: "sized/7777".into(),
                generation: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .handle;
        assert_ne!(h1, h2);
        let err = match s2
            .read_ranges(Request::new(pb::ReadRangesRequest {
                handle: h1,
                ranges: vec![range(0, 0)],
                chunk_size_hint: 0,
            }))
            .await
        {
            Ok(_) => panic!("expected stale handle to fail"),
            Err(e) => e,
        };
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);

        s2.close_object(Request::new(pb::CloseObjectRequest { handle: h1 }))
            .await
            .unwrap();
        assert!(s2.descriptor(h2).is_ok());
    }

    /// Synthetic ListObjects filters by prefix, supports delimiter prefixes,
    /// and paginates deterministically.
    #[tokio::test]
    async fn synthetic_list_objects_paginates_and_filters() {
        let svc = test_service();
        {
            let mut map = svc.synthetic_sizes.lock().unwrap();
            map.insert(("b".into(), "p/a".into()), 1);
            map.insert(("b".into(), "p/b".into()), 2);
            map.insert(("b".into(), "p/sub/c".into()), 3);
            map.insert(("b".into(), "other/z".into()), 4);
        }
        let p1 = svc
            .list_objects(Request::new(pb::ListObjectsRequest {
                bucket: "projects/_/buckets/b".into(),
                prefix: "p/".into(),
                delimiter: "/".into(),
                page_size: 1,
                page_token: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(p1.objects.len(), 1);
        assert_eq!(p1.objects[0].name, "p/a");
        assert_eq!(p1.prefixes, vec!["p/sub/"]);
        assert!(!p1.next_page_token.is_empty());

        let p2 = svc
            .list_objects(Request::new(pb::ListObjectsRequest {
                bucket: "b".into(),
                prefix: "p/".into(),
                delimiter: "/".into(),
                page_size: 1,
                page_token: p1.next_page_token,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(p2.objects.len(), 1);
        assert_eq!(p2.objects[0].name, "p/b");
        assert!(p2.next_page_token.is_empty());
    }

    /// AppendableWrite validates WriteSpec upload_mode/size_hint and rejects
    /// trailing messages after finalize or duplicate spec fields.
    #[test]
    fn appendable_write_validates_spec_and_finalize() {
        let mut spec = pb::WriteSpec {
            bucket: "b".into(),
            object: "o".into(),
            ..Default::default()
        };
        assert!(validate_appendable_spec(&spec).is_ok());

        spec.upload_mode = pb::write_spec::UploadMode::SingleShot as i32;
        assert_eq!(
            validate_appendable_spec(&spec).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        spec.upload_mode = 99;
        assert_eq!(
            validate_appendable_spec(&spec).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        spec.upload_mode = pb::write_spec::UploadMode::Auto as i32;
        spec.size_hint = Some(16);
        assert_eq!(
            validate_appendable_spec(&spec).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );

        let chunk = pb::AppendableWriteRequest::default();
        assert!(validate_appendable_chunk(false, &chunk).is_ok());
        assert_eq!(
            validate_appendable_chunk(true, &chunk).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        let with_spec = pb::AppendableWriteRequest {
            spec: Some(pb::WriteSpec::default()),
            ..Default::default()
        };
        assert_eq!(
            validate_appendable_chunk(false, &with_spec)
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
}
