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

#ifndef GCS_UDS_POC_CPP_SRC_STATUS_H
#define GCS_UDS_POC_CPP_SRC_STATUS_H

#include "gcs_core/client.h"
#include "gcscore.pb.h"
#include <grpcpp/support/status.h>

namespace gcs_core::internal {

/// gRPC status codes and google::cloud::StatusCode share numbering, so the
/// server's status is surfaced unchanged.
Status FromGrpc(grpc::Status const& status);

/// `gcscore.v1` metadata does not repeat the bucket and object name; the
/// caller supplies them from its request.
ObjectMetadata FromProto(::gcscore::v1::ObjectMeta const& m,
                         std::string const& bucket, std::string const& object);
ObjectMetadata FromProto(::gcscore::v1::ListedObject const& m,
                         std::string const& bucket);
BucketMetadata FromProto(::gcscore::v1::ListedBucket const& m);

/// The first message of a `WriteObject` stream for `options`.
::gcscore::v1::WriteHeader ToWriteHeader(WriteOptions const& options,
                                         std::string const& bucket,
                                         std::string const& object,
                                         ::gcscore::v1::Transport transport);

/// `ReadRange` → `(offset, length)` as the server reads them: `length < 0`
/// reads to the end, a negative `offset` reads the last `-offset` bytes.
std::pair<std::int64_t, std::int64_t> ToOffsetLength(ReadRange range);

}  // namespace gcs_core::internal

#endif  // GCS_UDS_POC_CPP_SRC_STATUS_H
