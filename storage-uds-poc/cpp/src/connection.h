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

#ifndef GCS_UDS_POC_CPP_SRC_CONNECTION_H
#define GCS_UDS_POC_CPP_SRC_CONNECTION_H

#include "absl/strings/cord.h"
#include "core_backend.h"
#include "gcs_core/client.h"
#include "gcscore.grpc.pb.h"
#include "status.h"
#include <grpcpp/grpcpp.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace gcs_core {
namespace internal {

/// Default and maximum size of one data message sent to the server. Uploads
/// are framed into messages of this size; the server accepts messages up to
/// 64 MiB.
inline constexpr std::uint32_t kDefaultChunkSize = 1024 * 1024;
inline constexpr std::uint32_t kMaxChunkSize = 16 * 1024 * 1024;
/// Bound on best-effort `CloseObject` during descriptor destruction so a hung
/// or stopped server cannot stall stack unwinding indefinitely.
inline constexpr std::chrono::milliseconds kDescriptorCloseTimeout{500};

constexpr std::uint32_t WriteChunkSize(std::uint32_t chunk_size_hint) {
  return chunk_size_hint == 0 ? kDefaultChunkSize
                              : std::min(chunk_size_hint, kMaxChunkSize);
}

/// `ReadChunk.data` is a vector of slices so the server can forward network
/// buffers without concatenating them; join them into one Cord.
absl::Cord JoinSlices(::gcscore::v1::ReadChunk const& chunk);

/// Splits `data` into messages of at most `chunk` bytes. `Cord::Subcord`
/// shares the underlying buffers, so no bytes are copied here.
template <typename Fn>
bool ForEachChunk(absl::Cord const& data, std::size_t chunk, Fn&& fn) {
  for (std::size_t offset = 0; offset < data.size(); offset += chunk) {
    if (!fn(data.Subcord(offset, chunk))) return false;
  }
  return true;
}

/// The stubs of one running server generation. Replaced as a whole on
/// restart, so a call in flight keeps using the set it started with.
struct StubSet {
  std::uint64_t generation = 0;
  std::vector<std::unique_ptr<::gcscore::v1::GcsCore::Stub>> stubs;
};

/// One running server (whatever the backend) plus the gRPC channels to it,
/// shared by clients whose backends have the same sharing key.
class Connection {
 public:
  static StatusOr<std::shared_ptr<Connection>> GetOrCreate(
      ClientOptions const& options);

  ~Connection();

  /// Round-robin over the channels. `stubs_mu_` only guards copying the
  /// `shared_ptr` and is never held across an RPC.
  ::gcscore::v1::GcsCore::Stub& Stub(std::shared_ptr<StubSet>& keepalive);

  /// Called after a failed RPC. If the server generation that served this
  /// RPC died, start a new one once so the next call succeeds; concurrent
  /// callers failing on the same generation observe the already-bumped
  /// `generation_` and do not restart a second time.
  Status Check(Status status, std::uint64_t failed_generation);

  /// Dispatches one unary RPC on a round-robin channel and checks for crash
  /// restart on `UNAVAILABLE`.
  template <typename Request, typename Response>
  Status Unary(grpc::Status (::gcscore::v1::GcsCore::Stub::*method)(
                   grpc::ClientContext*, Request const&, Response*),
               Request const& request, Response& response) {
    return Unary(method, request, response, nullptr);
  }

  /// Like the 3-argument `Unary`, and writes the generation that served the
  /// call into `*served_by` when non-null.
  template <typename Request, typename Response>
  Status Unary(grpc::Status (::gcscore::v1::GcsCore::Stub::*method)(
                   grpc::ClientContext*, Request const&, Response*),
               Request const& request, Response& response,
               std::uint64_t* served_by) {
    std::shared_ptr<StubSet> stubs;
    grpc::ClientContext ctx;
    Status s = FromGrpc((Stub(stubs).*method)(&ctx, request, &response));
    if (served_by != nullptr) *served_by = stubs->generation;
    if (!s.ok()) return Check(std::move(s), stubs->generation);
    return {};
  }

  /// Like `Stub()`, but only while `generation` is still the running server.
  /// Server-side handles are small counters that restart at 1, so a handle
  /// from a server that has since been restarted would name an unrelated
  /// object in the new one; callers holding such a handle get nullptr.
  ::gcscore::v1::GcsCore::Stub* StubFor(std::uint64_t generation,
                                        std::shared_ptr<StubSet>& keepalive);

  int pid() const;
  std::optional<std::uint64_t> CoreThreadCpuMicros() const;
  void KillForTesting();

 private:
  Connection(std::unique_ptr<CoreBackend> backend,
             std::chrono::milliseconds readiness_timeout);

  std::shared_ptr<StubSet> CurrentStubs() const;
  Status StartLocked();
  Status WaitUntilReadyLocked(StubSet const& set);

  std::unique_ptr<CoreBackend> const backend_;
  std::chrono::milliseconds const readiness_timeout_;
  mutable std::mutex mu_;
  std::uint64_t generation_ = 0;
  mutable std::mutex stubs_mu_;
  std::shared_ptr<StubSet> stubs_;
  std::atomic<std::size_t> next_{0};
};

/// Synchronous server-streaming `ReadObject` helper.
class ReaderImpl {
 public:
  ReaderImpl(std::shared_ptr<Connection> conn, std::string bucket,
             std::string object);
  ~ReaderImpl();

  Status Start(::gcscore::v1::ReadObjectRequest const& request);
  ObjectMetadata const& metadata() const { return metadata_; }
  StatusOr<std::optional<absl::Cord>> Next();

 private:
  std::shared_ptr<Connection> conn_;
  std::string bucket_;
  std::string object_;
  std::shared_ptr<StubSet> stubs_;
  grpc::ClientContext context_;
  std::unique_ptr<grpc::ClientReader<::gcscore::v1::ReadChunk>> reader_;
  ObjectMetadata metadata_;
  std::optional<absl::Cord> pending_;
  bool done_ = false;
};

/// Synchronous client-streaming `WriteObject` helper: a header message, then
/// data messages. Cancels on destruction if `Finish()` has not completed so
/// the server aborts the upload instead of committing a partial object.
class WriterImpl {
 public:
  WriterImpl(std::shared_ptr<Connection> conn,
             ::gcscore::v1::WriteHeader header, std::uint32_t chunk_size_hint);
  ~WriterImpl();

  std::string const& bucket() const { return header_.bucket(); }
  std::string const& object() const { return header_.object(); }

  Status Write(absl::Cord const& data);
  StatusOr<::gcscore::v1::WriteObjectResponse> Finish();

 private:
  bool SendHeader();

  std::shared_ptr<Connection> conn_;
  std::shared_ptr<StubSet> stubs_;
  ::gcscore::v1::WriteHeader header_;
  std::size_t write_chunk_size_;
  grpc::ClientContext context_;
  ::gcscore::v1::WriteObjectResponse response_;
  std::unique_ptr<grpc::ClientWriter<::gcscore::v1::WriteRequest>> writer_;
  bool header_sent_ = false;
  bool finished_ = false;
};

}  // namespace internal
}  // namespace gcs_core

#endif  // GCS_UDS_POC_CPP_SRC_CONNECTION_H
