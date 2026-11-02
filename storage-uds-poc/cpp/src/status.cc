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

namespace v0 = ::gcs::core::v0;

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

ObjectMetadata FromProto(v0::ObjectMetadata const& m) {
  ObjectMetadata out;
  out.bucket = m.bucket();
  out.name = m.name();
  out.generation = m.generation();
  out.metageneration = m.metageneration();
  out.size = m.size();
  out.content_type = m.content_type();
  if (m.has_crc32c()) out.crc32c = m.crc32c();
  out.md5_hash = m.md5_hash();
  out.etag = m.etag();
  out.storage_class = m.storage_class();
  return out;
}

void ToProto(WriteOptions const& options, std::string const& bucket,
             std::string const& object, v0::WriteSpec& spec) {
  spec.set_bucket(bucket);
  spec.set_object(object);
  if (options.size_hint) spec.set_size_hint(*options.size_hint);
  spec.set_content_type(options.content_type);
  if (options.if_generation_match) {
    spec.set_if_generation_match(*options.if_generation_match);
  }
  switch (options.mode) {
    case UploadMode::kAuto:
      spec.set_upload_mode(v0::WriteSpec::UPLOAD_MODE_AUTO);
      break;
    case UploadMode::kSingleShot:
      spec.set_upload_mode(v0::WriteSpec::UPLOAD_MODE_SINGLE_SHOT);
      break;
    case UploadMode::kResumable:
      spec.set_upload_mode(v0::WriteSpec::UPLOAD_MODE_RESUMABLE);
      break;
  }
}

}  // namespace gcs_core::internal
