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

//! Conversions between the `gcs.core.v0` wire types and the storage crate.

use crate::pb;
use google_cloud_storage::model::Object;
use google_cloud_storage::model_ext::{ObjectHighlights, ReadRange};
use tonic::{Code, Status};

const BUCKET_PREFIX: &str = "projects/_/buckets/";

/// The storage crate addresses buckets by resource name.
pub fn bucket_name(bucket: &str) -> String {
    if bucket.starts_with("projects/") {
        bucket.to_string()
    } else {
        format!("{BUCKET_PREFIX}{bucket}")
    }
}

/// The wire API uses bare bucket ids, like the C++ client.
pub fn bucket_id(name: &str) -> &str {
    name.strip_prefix(BUCKET_PREFIX).unwrap_or(name)
}

/// Maps the wire `ReadRange` onto the crate's range type.
///
/// `limit == 0` means "to the end"; a negative offset is a tail read and must
/// not carry a limit (the JSON API cannot express "last N bytes, first M").
pub fn read_range(range: Option<&pb::ReadRange>) -> Result<ReadRange, Status> {
    let Some(r) = range else {
        return Ok(ReadRange::all());
    };
    match (r.offset, r.limit) {
        (_, limit) if limit < 0 => Err(Status::invalid_argument("range.limit must be >= 0")),
        (i64::MIN, _) => Err(Status::invalid_argument("range.offset overflow")),
        (offset, 0) if offset < 0 => Ok(ReadRange::tail(offset.unsigned_abs())),
        (offset, _) if offset < 0 => Err(Status::invalid_argument(
            "a negative range.offset (tail read) requires range.limit == 0",
        )),
        (0, 0) => Ok(ReadRange::all()),
        (offset, 0) => Ok(ReadRange::offset(offset as u64)),
        (offset, limit) => {
            // `google-cloud-storage` 1.21.0 (`read_object.rs:634`) formats
            // `bytes={o}-{}", o + l - 1` in signed `i64`.
            if offset.checked_add(limit).is_none() {
                return Err(Status::invalid_argument(
                    "range.offset + range.limit overflows i64::MAX",
                ));
            }
            Ok(ReadRange::segment(offset as u64, limit as u64))
        }
    }
}

pub fn from_object(o: &Object) -> pb::ObjectMetadata {
    let (crc32c, md5_hash) = match &o.checksums {
        Some(c) => (c.crc32c, c.md5_hash.clone()),
        None => (None, bytes::Bytes::new()),
    };
    pb::ObjectMetadata {
        bucket: bucket_id(&o.bucket).to_string(),
        name: o.name.clone(),
        generation: o.generation,
        metageneration: o.metageneration,
        size: o.size.max(0) as u64,
        content_type: o.content_type.clone(),
        crc32c,
        md5_hash,
        etag: o.etag.clone(),
        storage_class: o.storage_class.clone(),
    }
}

/// `read_object` returns only highlights; bucket and name come from the
/// request.
pub fn from_highlights(bucket: &str, object: &str, h: &ObjectHighlights) -> pb::ObjectMetadata {
    let (crc32c, md5_hash) = match &h.checksums {
        Some(c) => (c.crc32c, c.md5_hash.clone()),
        None => (None, bytes::Bytes::new()),
    };
    pb::ObjectMetadata {
        bucket: bucket_id(bucket).to_string(),
        name: object.to_string(),
        generation: h.generation,
        metageneration: h.metageneration,
        size: h.size.max(0) as u64,
        content_type: h.content_type.clone(),
        crc32c,
        md5_hash,
        etag: h.etag.clone(),
        storage_class: h.storage_class.clone(),
    }
}

/// Maps a storage-crate error to the status the wrapper surfaces 1:1.
///
/// gRPC-path errors carry an RPC status; JSON-path errors carry an HTTP code
/// (mapped as in `google-cloud-cpp`'s `MapHttpCodeToStatus`); checksum
/// mismatches become `DATA_LOSS`; transport failures become `UNAVAILABLE`.
pub fn to_status(e: &google_cloud_storage::Error) -> Status {
    let message = e.to_string();
    if let Some(s) = e.status() {
        let code = match (s.code, e.http_status_code()) {
            // JSON errors arrive as a service status whose code is UNKNOWN
            // (the body has no gRPC code) plus the HTTP status, which is the
            // meaningful part.
            (google_cloud_gax::error::rpc::Code::Unknown, Some(http)) => http_to_code(http),
            (code, _) => Code::from_i32(code as i32),
        };
        return Status::new(code, format!("{}: {message}", s.message));
    }
    if is_checksum_mismatch(e) {
        return Status::data_loss(message);
    }
    if let Some(http) = e.http_status_code() {
        return Status::new(http_to_code(http), message);
    }
    let code = if e.is_timeout() {
        Code::DeadlineExceeded
    } else if e.is_authentication() {
        Code::Unauthenticated
    } else if e.is_connect() || e.is_io() || e.is_transport() {
        Code::Unavailable
    } else {
        Code::Unknown
    };
    Status::new(code, message)
}

fn is_checksum_mismatch(e: &google_cloud_storage::Error) -> bool {
    let mut source = std::error::Error::source(e);
    while let Some(s) = source {
        if let Some(google_cloud_storage::error::ReadError::ChecksumMismatch(_)) =
            s.downcast_ref::<google_cloud_storage::error::ReadError>()
        {
            return true;
        }
        source = s.source();
    }
    false
}

pub fn http_to_code(http: u16) -> Code {
    match http {
        400 => Code::InvalidArgument,
        401 => Code::Unauthenticated,
        403 => Code::PermissionDenied,
        404 => Code::NotFound,
        408 => Code::DeadlineExceeded,
        409 => Code::Aborted,
        412 => Code::FailedPrecondition,
        416 => Code::OutOfRange,
        429 => Code::ResourceExhausted,
        499 => Code::Cancelled,
        501 => Code::Unimplemented,
        502..=504 => Code::Unavailable,
        500..=599 => Code::Internal,
        _ => Code::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(offset: i64, limit: i64) -> pb::ReadRange {
        pb::ReadRange { offset, limit }
    }

    /// Every legal wire range maps to the crate range with the same meaning;
    /// `{0,0}` and an absent range are both "whole object".
    #[test]
    fn read_range_maps_legal_inputs() {
        let cases = [
            (None, ReadRange::all()),
            (Some(range(0, 0)), ReadRange::all()),
            (Some(range(10, 0)), ReadRange::offset(10)),
            (Some(range(10, 5)), ReadRange::segment(10, 5)),
            (Some(range(0, 5)), ReadRange::segment(0, 5)),
            (Some(range(-7, 0)), ReadRange::tail(7)),
        ];
        for (input, want) in cases {
            let got = read_range(input.as_ref()).unwrap();
            assert_eq!(format!("{got:?}"), format!("{want:?}"), "input={input:?}");
        }
    }

    /// A tail read with a limit, or a negative limit, cannot be expressed and
    /// is rejected before any request is sent.
    #[test]
    fn read_range_rejects_illegal_inputs() {
        for input in [range(-7, 1), range(0, -1), range(5, -3)] {
            let err = read_range(Some(&input)).unwrap_err();
            assert_eq!(err.code(), Code::InvalidArgument, "input={input:?}");
        }
    }

    /// Bucket ids gain the resource prefix once; full names pass through.
    #[test]
    fn bucket_name_round_trip() {
        assert_eq!(bucket_name("b"), "projects/_/buckets/b");
        assert_eq!(bucket_name("projects/_/buckets/b"), "projects/_/buckets/b");
        assert_eq!(bucket_id("projects/_/buckets/b"), "b");
        assert_eq!(bucket_id("b"), "b");
    }

    /// HTTP codes the JSON path returns map to the codes the C++ client uses
    /// for the same responses.
    #[test]
    fn http_codes_map_like_cpp() {
        let cases = [
            (404, Code::NotFound),
            (412, Code::FailedPrecondition),
            (403, Code::PermissionDenied),
            (416, Code::OutOfRange),
            (429, Code::ResourceExhausted),
            (503, Code::Unavailable),
            (500, Code::Internal),
            (302, Code::Unknown),
        ];
        for (http, want) in cases {
            assert_eq!(http_to_code(http), want, "http={http}");
        }
    }

    /// Metadata conversion strips the bucket prefix and carries checksums;
    /// a negative size (never expected) is clamped instead of wrapping.
    #[test]
    fn from_object_converts_fields() {
        let o = Object::new()
            .set_bucket("projects/_/buckets/b")
            .set_name("o")
            .set_generation(3)
            .set_size(-1)
            .set_checksums(google_cloud_storage::model::ObjectChecksums::new().set_crc32c(42_u32));
        let m = from_object(&o);
        assert_eq!(m.bucket, "b");
        assert_eq!(m.name, "o");
        assert_eq!(m.generation, 3);
        assert_eq!(m.size, 0);
        assert_eq!(m.crc32c, Some(42));
    }

    fn json_error(
        code: google_cloud_gax::error::rpc::Code,
        http: Option<u16>,
    ) -> google_cloud_storage::Error {
        let status = google_cloud_gax::error::rpc::Status::default()
            .set_code(code)
            .set_message("from service");
        google_cloud_storage::Error::service_with_http_metadata(status, http, None)
    }

    /// The JSON path reports e.g. a 412 as a service status with code
    /// UNKNOWN plus the HTTP code; the HTTP code must win so preconditions
    /// surface as FAILED_PRECONDITION (seen against a real bucket).
    #[test]
    fn json_unknown_status_uses_http_code() {
        use google_cloud_gax::error::rpc::Code as RpcCode;
        let cases = [
            (RpcCode::Unknown, Some(412), Code::FailedPrecondition),
            (RpcCode::Unknown, Some(404), Code::NotFound),
            (RpcCode::Unknown, None, Code::Unknown),
            (RpcCode::NotFound, Some(404), Code::NotFound),
            // A specific RPC code is kept even if the HTTP code disagrees.
            (RpcCode::PermissionDenied, Some(400), Code::PermissionDenied),
        ];
        for (rpc, http, want) in cases {
            assert_eq!(
                to_status(&json_error(rpc, http)).code(),
                want,
                "rpc={rpc:?} http={http:?}"
            );
        }
    }

    #[test]
    fn i64_min_tail_offset_is_rejected() {
        let r = pb::ReadRange {
            offset: i64::MIN,
            limit: 0,
        };
        assert_eq!(
            read_range(Some(&r)).unwrap_err().code(),
            Code::InvalidArgument
        );
    }

    /// Segment ranges where `offset + limit` would overflow signed `i64`
    /// (`read_object.rs` computes `o + l - 1` in `i64`) are rejected, while
    /// `offset + limit == i64::MAX` succeeds.
    #[test]
    fn segment_end_overflow_is_rejected() {
        assert!(read_range(Some(&range(0, i64::MAX))).is_ok());
        assert!(read_range(Some(&range(1, i64::MAX - 1))).is_ok());
        for bad in [range(1, i64::MAX), range(10, i64::MAX), range(i64::MAX, 1)] {
            assert_eq!(
                read_range(Some(&bad)).unwrap_err().code(),
                Code::InvalidArgument,
                "bad={bad:?}"
            );
        }
    }
}
