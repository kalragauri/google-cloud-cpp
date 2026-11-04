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

#ifndef GCS_UDS_POC_CPP_INCLUDE_GCS_CORE_STORAGE_CLIENT_H
#define GCS_UDS_POC_CPP_INCLUDE_GCS_CORE_STORAGE_CLIENT_H

#include "google/cloud/storage/async/client.h"
#include "google/cloud/storage/async/connection.h"
#include "google/cloud/storage/client.h"
#include "google/cloud/storage/internal/storage_connection.h"
#include "google/cloud/options.h"
#include "google/cloud/status_or.h"
#include "gcs_core/client.h"
#include <memory>

/**
 * Public `google-cloud-cpp` SDK adapters over the shared Rust gRPC-over-UDS
 * server (`gcscore.v1.GcsCore`).
 *
 * Applications and benchmarks use unmodified `google::cloud::storage::Client`
 * and `google::cloud::storage::AsyncClient` types constructed via these
 * factories without modifying any `google/cloud/storage/` SDK source files:
 *
 * - `MakeUdsStorageClient` builds an undecorated `storage::Client` backed by
 *   `UdsStorageConnection` (`ReadObject`, `InsertObject`, `WriteObject`,
 *   `ListObjects`, `ListBuckets`, `DeleteObject`; remaining operations return
 *   `kUnimplemented`). SDK-side retry and checksum decorators are bypassed
 *   because retries, resumable-upload state, CRC32C validation, and ADC
 *   authentication live in the shared Rust server.
 * - `MakeUdsAsyncClient` builds a `storage::AsyncClient` backed by
 *   `UdsAsyncConnection` (`Open` -> `ObjectDescriptor::Read`,
 *   `StartAppendableObjectUpload` -> `AsyncWriter`, `DeleteObject`; remaining
 *   operations return `kUnimplemented`).
 */
namespace gcs_core {

/// Default UDS client options for the public SDK adapters: 4 independent
/// HTTP/2 connections over UDS (`channels = 4`) and shared per-UID daemon mode
/// when running out-of-process.
ClientOptions DefaultAdapterClientOptions();

/// Builds a `StorageConnection` backed by the shared Rust gRPC-over-UDS server.
StatusOr<std::shared_ptr<google::cloud::storage::internal::StorageConnection>>
MakeUdsStorageConnection(
    ClientOptions const& core_options = DefaultAdapterClientOptions(),
    google::cloud::Options options = {});

/// Builds an unmodified `google::cloud::storage::Client` backed by
/// `MakeUdsStorageConnection` without SDK-side retry or logging decorations.
StatusOr<google::cloud::storage::Client> MakeUdsStorageClient(
    ClientOptions const& core_options = DefaultAdapterClientOptions(),
    google::cloud::Options options = {});

/// Builds an `AsyncConnection` backed by the shared Rust gRPC-over-UDS server.
StatusOr<std::shared_ptr<google::cloud::storage::AsyncConnection>>
MakeUdsAsyncConnection(
    ClientOptions const& core_options = DefaultAdapterClientOptions(),
    google::cloud::Options options = {});

/// Builds an unmodified `google::cloud::storage::AsyncClient` backed by
/// `MakeUdsAsyncConnection`.
StatusOr<google::cloud::storage::AsyncClient> MakeUdsAsyncClient(
    ClientOptions const& core_options = DefaultAdapterClientOptions(),
    google::cloud::Options options = {});

}  // namespace gcs_core

#endif  // GCS_UDS_POC_CPP_INCLUDE_GCS_CORE_STORAGE_CLIENT_H
