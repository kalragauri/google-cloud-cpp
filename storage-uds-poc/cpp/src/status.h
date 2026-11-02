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

#include "gcs/core/v0/storage_core.pb.h"
#include "gcs_core/client.h"
#include <grpcpp/support/status.h>

namespace gcs_core::internal {

/// gRPC status codes and google::cloud::StatusCode share numbering, so the
/// sidecar's status is surfaced unchanged.
Status FromGrpc(grpc::Status const& status);

ObjectMetadata FromProto(::gcs::core::v0::ObjectMetadata const& m);

void ToProto(WriteOptions const& options, std::string const& bucket,
             std::string const& object, ::gcs::core::v0::WriteSpec& spec);

}  // namespace gcs_core::internal

#endif  // GCS_UDS_POC_CPP_SRC_STATUS_H
