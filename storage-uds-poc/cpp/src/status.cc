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

#include "status.h"

namespace gcs_core::internal {

namespace pb = ::gcscore::v1;

static_assert(static_cast<int>(grpc::StatusCode::UNAUTHENTICATED) ==
                  static_cast<int>(google::cloud::StatusCode::kUnauthenticated),
              "gRPC and google::cloud status codes must share numbering");
static_assert(static_cast<int>(grpc::StatusCode::DATA_LOSS) ==
                  static_cast<int>(google::cloud::StatusCode::kDataLoss),
              "gRPC and google::cloud status codes must share numbering");

Status FromGrpc(grpc::Status const& status) {
  if (status.ok()) return {};
  return Status(static_cast<google::cloud::StatusCode>(status.error_code()),
                status.error_message());
}

ObjectMetadata FromProto(pb::ObjectMeta const& m, std::string const& bucket,
                         std::string const& object) {
  ObjectMetadata out;
  out.bucket = bucket;
  out.name = object;
  out.generation = m.generation();
  out.metageneration = m.metageneration();
  out.size = static_cast<std::uint64_t>(m.size());
  out.content_type = m.content_type();
  out.content_encoding = m.content_encoding();
  // The server reports 0 when the object has no CRC32C; a real CRC32C of 0 is
  // indistinguishable and treated as absent.
  if (m.crc32c() != 0) out.crc32c = m.crc32c();
  return out;
}

ObjectMetadata FromProto(pb::ListedObject const& m, std::string const& bucket) {
  ObjectMetadata out;
  out.bucket = bucket;
  out.name = m.name();
  out.generation = m.generation();
  out.metageneration = m.metageneration();
  out.size = static_cast<std::uint64_t>(m.size());
  out.content_type = m.content_type();
  out.storage_class = m.storage_class();
  if (m.crc32c() != 0) out.crc32c = m.crc32c();
  out.updated = std::chrono::system_clock::time_point(
      std::chrono::seconds(m.updated_unix_secs()));
  return out;
}

BucketMetadata FromProto(pb::ListedBucket const& m) {
  BucketMetadata out;
  out.name = m.name();
  out.location = m.location();
  out.storage_class = m.storage_class();
  out.metageneration = m.metageneration();
  out.created = std::chrono::system_clock::time_point(
      std::chrono::seconds(m.created_unix_secs()));
  return out;
}

pb::WriteHeader ToWriteHeader(WriteOptions const& options,
                              std::string const& bucket,
                              std::string const& object,
                              pb::Transport transport) {
  pb::WriteHeader header;
  header.set_bucket(bucket);
  header.set_object(object);
  header.set_content_type(options.content_type);
  if (options.if_generation_match) {
    header.set_if_generation_match(*options.if_generation_match);
  }
  // `known_size` selects the core's upload strategy: set, the crate uploads
  // in one request when the size is below its resumable threshold; unset, it
  // streams a resumable upload with bounded memory.
  switch (options.mode) {
    case UploadMode::kAuto:
    case UploadMode::kSingleShot:
      if (options.size_hint) {
        header.set_known_size(static_cast<std::int64_t>(*options.size_hint));
      }
      break;
    case UploadMode::kResumable:
      break;
  }
  // `chunk_size` is the crate's resumable buffer (0 leaves its 8 MiB default,
  // the same as the native baseline), not the size of the messages we send.
  header.set_chunk_size(options.resumable_buffer_size);
  header.set_transport(transport);
  return header;
}

std::pair<std::int64_t, std::int64_t> ToOffsetLength(ReadRange range) {
  return {range.offset, range.limit == 0 ? -1 : range.limit};
}

}  // namespace gcs_core::internal
