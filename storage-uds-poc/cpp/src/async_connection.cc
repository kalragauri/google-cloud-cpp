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

#include "google/cloud/storage/async/connection.h"
#include "google/cloud/storage/async/idempotency_policy.h"
#include "google/cloud/storage/async/object_descriptor_connection.h"
#include "google/cloud/storage/async/options.h"
#include "google/cloud/storage/async/reader_connection.h"
#include "google/cloud/storage/async/resume_policy.h"
#include "google/cloud/storage/async/retry_policy.h"
#include "google/cloud/storage/async/rewriter_connection.h"
#include "google/cloud/storage/async/writer_connection.h"
#include "google/cloud/storage/internal/async/read_payload_impl.h"
#include "google/cloud/storage/internal/async/write_payload_impl.h"
#include "google/cloud/storage/internal/grpc/bucket_name.h"
#include "google/cloud/storage/options.h"
#include "google/cloud/background_threads.h"
#include "google/cloud/completion_queue.h"
#include "google/cloud/credentials.h"
#include "google/cloud/future.h"
#include "google/cloud/grpc_options.h"
#include "google/cloud/internal/async_streaming_read_rpc_impl.h"
#include "google/cloud/internal/async_streaming_write_rpc_impl.h"
#include "google/cloud/log.h"
#include "google/cloud/options.h"
#include "google/cloud/rpc_metadata.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "connection.h"
#include "gcs_core/storage_client.h"
#include "google/storage/v2/storage.pb.h"
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
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

namespace gcs_core {
namespace {

namespace pb = ::gcscore::v1;
namespace gcs = ::google::cloud::storage;
namespace gcs_internal = ::google::cloud::storage_internal;
using ::google::cloud::CompletionQueue;
using ::google::cloud::future;
using ::google::cloud::make_ready_future;
using ::google::cloud::Options;
using ::google::cloud::RpcMetadata;
using ::google::cloud::StatusCode;

constexpr std::int64_t kDefaultMaxRangeSize = 128LL * 1024 * 1024;

Status UnimplementedAsyncMethod(std::string_view method) {
  return Status(StatusCode::kUnimplemented,
                absl::StrCat(method,
                             " is not implemented by UdsAsyncConnection "
                             "(gcscore.v1.GcsCore does not expose it)"));
}

Status UnimplementedAsyncOption(std::string_view field,
                                std::string_view method) {
  return Status(StatusCode::kUnimplemented,
                absl::StrCat(field, " is not supported by ", method,
                             " over UDS (gcscore.v1.GcsCore)"));
}

StatusOr<Options> ValidateAndDefaultAsyncOptions(Options opts) {
  if (opts.has<google::cloud::UnifiedCredentialsOption>() ||
      opts.has<google::cloud::GrpcCredentialOption>()) {
    return Status(
        StatusCode::kUnimplemented,
        "UnifiedCredentialsOption / GrpcCredentialOption are not supported "
        "over UDS; the shared Rust server authenticates using Application "
        "Default Credentials");
  }
  if (opts.has<gcs::AsyncRetryPolicyOption>() ||
      opts.has<gcs::ResumePolicyOption>() ||
      opts.has<gcs::AsyncIdempotencyPolicyOption>() ||
      opts.has<gcs::RetryPolicyOption>() ||
      opts.has<gcs::BackoffPolicyOption>() ||
      opts.has<gcs::IdempotencyPolicyOption>()) {
    GCP_LOG(WARNING)
        << "UdsAsyncConnection ignores SDK retry, backoff, resume, and "
           "idempotency policies; retries are owned by the shared Rust server";
  }
  // `ObjectDescriptor::Read` unconditionally reads `MaximumRangeSizeOption`
  // from `ObjectDescriptorConnection::options()`.
  if (!opts.has<gcs::MaximumRangeSizeOption>()) {
    opts.set<gcs::MaximumRangeSizeOption>(kDefaultMaxRangeSize);
  }
  return opts;
}

google::storage::v2::Object ToV2Object(pb::ObjectMeta const& meta,
                                       std::string const& bucket_id,
                                       std::string const& object_name) {
  google::storage::v2::Object out;
  out.set_bucket(gcs_internal::GrpcBucketIdToName(bucket_id));
  out.set_name(object_name);
  out.set_size(meta.size());
  out.set_generation(meta.generation());
  out.set_metageneration(meta.metageneration());
  out.set_content_type(meta.content_type());
  out.set_content_encoding(meta.content_encoding());
  if (meta.crc32c() != 0) {
    out.mutable_checksums()->set_crc32c(meta.crc32c());
  }
  return out;
}

google::storage::v2::Object ToV2ObjectFromWrite(
    pb::WriteObjectResponse const& resp, std::string const& bucket_id,
    std::string const& object_name, std::string const& content_type) {
  google::storage::v2::Object out;
  out.set_bucket(gcs_internal::GrpcBucketIdToName(bucket_id));
  out.set_name(object_name);
  out.set_size(resp.size());
  out.set_generation(resp.generation());
  if (!content_type.empty()) out.set_content_type(content_type);
  if (resp.crc32c() != 0) {
    out.mutable_checksums()->set_crc32c(resp.crc32c());
  }
  return out;
}

class ErrorAsyncReaderConnection : public gcs::AsyncReaderConnection {
 public:
  explicit ErrorAsyncReaderConnection(Status status)
      : status_(std::move(status)) {}

  void Cancel() override {}
  future<ReadResponse> Read() override {
    return make_ready_future(ReadResponse(status_));
  }
  RpcMetadata GetRequestMetadata() override { return {}; }

 private:
  Status status_;
};

class UnimplementedAsyncRewriterConnection
    : public gcs::AsyncRewriterConnection {
 public:
  future<StatusOr<google::storage::v2::RewriteResponse>> Iterate() override {
    return make_ready_future(StatusOr<google::storage::v2::RewriteResponse>(
        UnimplementedAsyncMethod("RewriteObject")));
  }
};

struct ReaderState {
  using RpcType = google::cloud::internal::AsyncStreamingReadRpc<pb::ReadChunk>;

  ReaderState(std::shared_ptr<internal::Connection> c,
              std::shared_ptr<internal::StubSet> s, std::uint64_t gen,
              std::unique_ptr<RpcType> r, std::int64_t initial_offset)
      : conn(std::move(c)),
        stubs(std::move(s)),
        generation(gen),
        rpc(std::move(r)),
        current_offset(initial_offset) {}

  void Cancel() {
    RpcType* rpc_ptr = nullptr;
    {
      std::scoped_lock lk(mu);
      cancelled = true;
      rpc_ptr = rpc.get();
    }
    if (rpc_ptr != nullptr) rpc_ptr->Cancel();
  }

  std::shared_ptr<internal::Connection> conn;
  std::shared_ptr<internal::StubSet> stubs;
  std::uint64_t generation;
  std::unique_ptr<RpcType> rpc;

  std::mutex mu;
  std::int64_t current_offset;
  bool started = false;
  bool finished = false;
  bool cancelled = false;
};

future<gcs::AsyncReaderConnection::ReadResponse> FinishReaderStream(
    std::shared_ptr<ReaderState> state) {
  {
    std::scoped_lock lk(state->mu);
    state->finished = true;
  }
  auto* rpc_ptr = state->rpc.get();
  return rpc_ptr->Finish().then(
      [state = std::move(state)](
          future<Status> f) -> gcs::AsyncReaderConnection::ReadResponse {
        Status s = f.get();
        bool was_cancelled = false;
        {
          std::scoped_lock lk(state->mu);
          was_cancelled = state->cancelled;
        }
        if (!s.ok()) {
          s = state->conn->Check(std::move(s), state->generation);
        }
        if (was_cancelled) {
          return gcs::AsyncReaderConnection::ReadResponse(Status(
              StatusCode::kCancelled,
              s.ok() ? std::string("ReadRange cancelled") : s.message()));
        }
        return gcs::AsyncReaderConnection::ReadResponse(std::move(s));
      });
}

future<gcs::AsyncReaderConnection::ReadResponse> DrainReaderToFinish(
    std::shared_ptr<ReaderState> state) {
  auto* rpc_ptr = state->rpc.get();
  return rpc_ptr->Read().then(
      [state = std::move(state)](future<std::optional<pb::ReadChunk>> f) mutable
          -> future<gcs::AsyncReaderConnection::ReadResponse> {
        if (f.get().has_value()) return DrainReaderToFinish(std::move(state));
        return FinishReaderStream(std::move(state));
      });
}

future<gcs::AsyncReaderConnection::ReadResponse> ReadNextChunk(
    std::shared_ptr<ReaderState> state) {
  auto* rpc_ptr = state->rpc.get();
  return rpc_ptr->Read().then(
      [state = std::move(state)](future<std::optional<pb::ReadChunk>> f) mutable
          -> future<gcs::AsyncReaderConnection::ReadResponse> {
        std::optional<pb::ReadChunk> chunk = f.get();
        if (!chunk.has_value()) {
          return FinishReaderStream(std::move(state));
        }
        bool is_cancelled = false;
        {
          std::scoped_lock lk(state->mu);
          is_cancelled = state->cancelled;
        }
        if (is_cancelled) {
          return DrainReaderToFinish(std::move(state));
        }
        absl::Cord data = internal::JoinSlices(*chunk);
        if (data.empty()) {
          return ReadNextChunk(std::move(state));
        }
        std::int64_t offset = 0;
        {
          std::scoped_lock lk(state->mu);
          offset = state->current_offset;
          state->current_offset += static_cast<std::int64_t>(data.size());
        }
        gcs::ReadPayload payload =
            gcs_internal::ReadPayloadImpl::Make(std::move(data));
        payload.set_offset(offset);
        return make_ready_future(
            gcs::AsyncReaderConnection::ReadResponse(std::move(payload)));
      });
}

void DrainAbandonedReader(std::shared_ptr<ReaderState> state) {
  auto* rpc_ptr = state->rpc.get();
  rpc_ptr->Read().then([state = std::move(state)](
                           future<std::optional<pb::ReadChunk>> f) mutable {
    if (f.get().has_value()) {
      DrainAbandonedReader(std::move(state));
      return;
    }
    auto* finish_rpc = state->rpc.get();
    finish_rpc->Finish().then([state = std::move(state)](future<Status>) {});
  });
}

class UdsObjectDescriptorConnection;

class UdsAsyncReaderConnection : public gcs::AsyncReaderConnection {
 public:
  UdsAsyncReaderConnection(
      std::weak_ptr<UdsObjectDescriptorConnection> descriptor,
      std::uint64_t reader_id, std::shared_ptr<ReaderState> state)
      : descriptor_(std::move(descriptor)),
        reader_id_(reader_id),
        state_(std::move(state)) {}

  ~UdsAsyncReaderConnection() override;

  void Cancel() override { state_->Cancel(); }

  future<ReadResponse> Read() override {
    bool need_start = false;
    {
      std::scoped_lock lk(state_->mu);
      if (state_->finished) {
        return make_ready_future(ReadResponse(
            Status(StatusCode::kCancelled, "ReadRange stream already closed")));
      }
      if (!state_->started) {
        if (state_->cancelled) {
          state_->finished = true;
          return make_ready_future(ReadResponse(
              Status(StatusCode::kCancelled, "ReadRange cancelled")));
        }
        state_->started = true;
        need_start = true;
      }
    }
    if (need_start) {
      std::shared_ptr<ReaderState> state = state_;
      auto* rpc_ptr = state->rpc.get();
      return rpc_ptr->Start().then(
          [state = std::move(state)](
              future<bool> f) mutable -> future<ReadResponse> {
            if (!f.get()) return FinishReaderStream(std::move(state));
            return ReadNextChunk(std::move(state));
          });
    }
    return ReadNextChunk(state_);
  }

  RpcMetadata GetRequestMetadata() override {
    return state_->rpc->GetRequestMetadata();
  }

 private:
  std::weak_ptr<UdsObjectDescriptorConnection> descriptor_;
  std::uint64_t reader_id_;
  std::shared_ptr<ReaderState> state_;
};

class UdsObjectDescriptorConnection
    : public gcs::ObjectDescriptorConnection,
      public std::enable_shared_from_this<UdsObjectDescriptorConnection> {
 public:
  UdsObjectDescriptorConnection(std::shared_ptr<internal::Connection> conn,
                                CompletionQueue cq, std::uint64_t generation,
                                std::uint64_t handle,
                                google::storage::v2::Object metadata,
                                std::uint32_t chunk_size_hint, Options options)
      : conn_(std::move(conn)),
        cq_(std::move(cq)),
        generation_(generation),
        handle_(handle),
        metadata_(std::move(metadata)),
        chunk_size_hint_(chunk_size_hint),
        options_(std::move(options)) {}

  ~UdsObjectDescriptorConnection() override {
    std::vector<std::shared_ptr<ReaderState>> active;
    {
      std::scoped_lock lk(mu_);
      closed_ = true;
      active.reserve(active_readers_.size());
      for (auto const& [id, weak] : active_readers_) {
        if (std::shared_ptr<ReaderState> s = weak.lock()) {
          active.push_back(std::move(s));
        }
      }
      active_readers_.clear();
    }
    // Cancel outside `mu_` so a reader unregistration never contends with
    // descriptor destruction.
    for (std::shared_ptr<ReaderState> const& reader : active) {
      reader->Cancel();
    }

    std::shared_ptr<internal::StubSet> stubs;
    pb::GcsCore::Stub* stub = conn_->StubFor(generation_, stubs);
    if (stub == nullptr) return;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() +
                     internal::kDescriptorCloseTimeout);
    pb::CloseObjectRequest req;
    req.set_handle(handle_);
    pb::Empty resp;
    (void)stub->CloseObject(&ctx, req, &resp);
  }

  Options options() const override { return options_; }

  std::optional<google::storage::v2::Object> metadata() const override {
    return metadata_;
  }

  void MakeSubsequentStream() override {}

  bool IsOpen() const override {
    std::scoped_lock lk(mu_);
    if (closed_) return false;
    std::shared_ptr<internal::StubSet> stubs;
    return conn_->StubFor(generation_, stubs) != nullptr;
  }

  std::unique_ptr<gcs::AsyncReaderConnection> Read(ReadParams p) override {
    if (p.start < 0 && p.length != 0) {
      return std::make_unique<ErrorAsyncReaderConnection>(
          Status(StatusCode::kInvalidArgument,
                 "ReadLast (negative start) requires length == 0"));
    }
    if (p.start >= 0 && p.length < 0) {
      return std::make_unique<ErrorAsyncReaderConnection>(
          Status(StatusCode::kInvalidArgument, "read length must be >= 0"));
    }

    {
      std::scoped_lock lk(mu_);
      if (closed_) {
        return std::make_unique<ErrorAsyncReaderConnection>(
            Status(StatusCode::kCancelled, "ObjectDescriptor is closed"));
      }
    }

    std::shared_ptr<internal::StubSet> stubs;
    pb::GcsCore::Stub* stub = conn_->StubFor(generation_, stubs);
    if (stub == nullptr) {
      return std::make_unique<ErrorAsyncReaderConnection>(
          Status(StatusCode::kFailedPrecondition,
                 "the server that opened this descriptor has restarted; open "
                 "the object again"));
    }

    pb::ReadRangeRequest req;
    req.set_handle(handle_);
    req.set_offset(p.start);
    req.set_length(p.length <= 0 ? -1 : p.length);
    req.set_chunk_size(chunk_size_hint_);

    std::unique_ptr<ReaderState::RpcType> rpc =
        google::cloud::internal::MakeStreamingReadRpc<pb::ReadRangeRequest,
                                                      pb::ReadChunk>(
            cq_, std::make_shared<grpc::ClientContext>(),
            google::cloud::internal::MakeImmutableOptions(options_), req,
            [stub](grpc::ClientContext* ctx, pb::ReadRangeRequest const& r,
                   grpc::CompletionQueue* gq) {
              return stub->PrepareAsyncReadRange(ctx, r, gq);
            });

    std::int64_t const initial_offset =
        p.start >= 0 ? p.start
                     : std::max<std::int64_t>(0, metadata_.size() + p.start);
    auto state = std::make_shared<ReaderState>(
        conn_, std::move(stubs), generation_, std::move(rpc), initial_offset);

    std::uint64_t reader_id = 0;
    bool was_closed = false;
    {
      std::scoped_lock lk(mu_);
      reader_id = ++next_reader_id_;
      was_closed = closed_;
      if (!was_closed) {
        active_readers_.emplace(reader_id, state);
      }
    }
    if (was_closed) {
      state->Cancel();
    }
    return std::make_unique<UdsAsyncReaderConnection>(
        weak_from_this(), reader_id, std::move(state));
  }

  void UnregisterReader(std::uint64_t reader_id) {
    std::scoped_lock lk(mu_);
    active_readers_.erase(reader_id);
  }

 private:
  std::shared_ptr<internal::Connection> conn_;
  CompletionQueue cq_;
  std::uint64_t generation_;
  std::uint64_t handle_;
  google::storage::v2::Object metadata_;
  std::uint32_t chunk_size_hint_;
  Options options_;

  mutable std::mutex mu_;
  bool closed_ = false;
  std::uint64_t next_reader_id_ = 0;
  std::unordered_map<std::uint64_t, std::weak_ptr<ReaderState>> active_readers_;
};

UdsAsyncReaderConnection::~UdsAsyncReaderConnection() {
  if (std::shared_ptr<UdsObjectDescriptorConnection> desc =
          descriptor_.lock()) {
    desc->UnregisterReader(reader_id_);
  }
  bool need_drain = false;
  {
    std::scoped_lock lk(state_->mu);
    if (state_->started && !state_->finished) {
      state_->cancelled = true;
      need_drain = true;
    }
  }
  if (need_drain) {
    state_->rpc->Cancel();
    DrainAbandonedReader(std::move(state_));
  }
}

struct WriterState {
  using RpcType =
      google::cloud::internal::AsyncStreamingWriteRpc<pb::WriteRequest,
                                                      pb::WriteObjectResponse>;

  WriterState(std::shared_ptr<internal::Connection> c,
              std::shared_ptr<internal::StubSet> s, std::uint64_t gen,
              std::unique_ptr<RpcType> r, std::string id, std::string b,
              std::string o, std::string ct, std::size_t chunk_sz)
      : conn(std::move(c)),
        stubs(std::move(s)),
        generation(gen),
        rpc(std::move(r)),
        upload_id(std::move(id)),
        bucket_id(std::move(b)),
        object(std::move(o)),
        content_type(std::move(ct)),
        write_chunk_size(chunk_sz) {}

  std::shared_ptr<internal::Connection> conn;
  std::shared_ptr<internal::StubSet> stubs;
  std::uint64_t generation;
  std::unique_ptr<RpcType> rpc;
  std::string upload_id;
  std::string bucket_id;
  std::string object;
  std::string content_type;
  std::size_t write_chunk_size;

  std::mutex mu;
  std::int64_t bytes_accepted = 0;
  std::variant<std::int64_t, google::storage::v2::Object> persisted_state{
      std::int64_t{0}};
  std::optional<Status> terminal_error;
  std::string transport_used;
  bool started = false;
  bool finished = false;
  bool cancelled = false;
};

future<Status> FinishBrokenWriter(std::shared_ptr<WriterState> state,
                                  std::string_view context_msg) {
  {
    std::scoped_lock lk(state->mu);
    if (state->finished) {
      return make_ready_future(state->terminal_error.value_or(
          Status(StatusCode::kCancelled, "upload stream already closed")));
    }
    state->finished = true;
  }
  std::string fallback_msg(context_msg);
  auto* rpc_ptr = state->rpc.get();
  return rpc_ptr->Finish().then(
      [state = std::move(state), fallback_msg = std::move(fallback_msg)](
          future<StatusOr<pb::WriteObjectResponse>> f) -> Status {
        StatusOr<pb::WriteObjectResponse> resp = f.get();
        Status s = resp.ok() ? Status(StatusCode::kCancelled, fallback_msg)
                             : state->conn->Check(std::move(resp).status(),
                                                  state->generation);
        {
          std::scoped_lock lk(state->mu);
          state->terminal_error = s;
        }
        return s;
      });
}

std::vector<std::string> SplitPayloadChunks(gcs::WritePayload const& payload,
                                            std::size_t max_chunk_bytes) {
  absl::Cord const cord = gcs_internal::WritePayloadImpl::GetImpl(payload);
  std::vector<std::string> chunks;
  if (cord.empty()) return chunks;
  chunks.reserve((cord.size() + max_chunk_bytes - 1) / max_chunk_bytes);
  internal::ForEachChunk(cord, max_chunk_bytes, [&](absl::Cord const& sub) {
    chunks.emplace_back(sub);
    return true;
  });
  return chunks;
}

future<Status> WriteChunkSequence(
    std::shared_ptr<WriterState> state,
    std::shared_ptr<std::vector<std::string>> chunks, std::size_t index,
    std::int64_t total_bytes, bool update_persisted_on_done) {
  if (index == chunks->size()) {
    {
      std::scoped_lock lk(state->mu);
      state->bytes_accepted += total_bytes;
      if (update_persisted_on_done) {
        state->persisted_state = state->bytes_accepted;
      }
    }
    return make_ready_future(Status{});
  }

  pb::WriteRequest req;
  req.set_data((*chunks)[index]);
  auto* rpc_ptr = state->rpc.get();
  return rpc_ptr->Write(req, grpc::WriteOptions{})
      .then([state = std::move(state), chunks = std::move(chunks), index,
             total_bytes, update_persisted_on_done](
                future<bool> f) mutable -> future<Status> {
        if (!f.get()) {
          return FinishBrokenWriter(
              std::move(state), "UDS WriteObject stream closed during Write");
        }
        return WriteChunkSequence(std::move(state), std::move(chunks),
                                  index + 1, total_bytes,
                                  update_persisted_on_done);
      });
}

class UdsAsyncWriterConnection : public gcs::AsyncWriterConnection {
 public:
  explicit UdsAsyncWriterConnection(std::shared_ptr<WriterState> state)
      : state_(std::move(state)) {}

  ~UdsAsyncWriterConnection() override {
    bool need_cancel_and_finish = false;
    {
      std::scoped_lock lk(state_->mu);
      if (state_->started && !state_->finished) {
        state_->finished = true;
        state_->cancelled = true;
        need_cancel_and_finish = true;
      }
    }
    if (need_cancel_and_finish) {
      state_->rpc->Cancel();
      auto* rpc_ptr = state_->rpc.get();
      rpc_ptr->Finish().then([state = std::move(state_)](
                                 future<StatusOr<pb::WriteObjectResponse>>) {});
    }
  }

  void Cancel() override {
    WriterState::RpcType* rpc_ptr = nullptr;
    {
      std::scoped_lock lk(state_->mu);
      state_->cancelled = true;
      rpc_ptr = state_->rpc.get();
    }
    if (rpc_ptr != nullptr) rpc_ptr->Cancel();
  }

  std::string UploadId() const override { return state_->upload_id; }

  std::variant<std::int64_t, google::storage::v2::Object> PersistedState()
      const override {
    std::scoped_lock lk(state_->mu);
    return state_->persisted_state;
  }

  future<Status> Write(gcs::WritePayload payload) override {
    return SendPayload(std::move(payload), /*update_persisted=*/false);
  }

  future<Status> Flush(gcs::WritePayload payload) override {
    return SendPayload(std::move(payload), /*update_persisted=*/true);
  }

  future<Status> Close(gcs::WritePayload /*payload*/) override {
    return make_ready_future(Status(
        StatusCode::kUnimplemented,
        "AsyncWriter::Close without Finalize is not supported over UDS; the "
        "shared server finalizes appendable uploads when the stream ends"));
  }

  future<StatusOr<std::int64_t>> Query() override {
    std::scoped_lock lk(state_->mu);
    if (state_->terminal_error.has_value()) {
      return make_ready_future(StatusOr<std::int64_t>(*state_->terminal_error));
    }
    if (state_->cancelled) {
      return make_ready_future(StatusOr<std::int64_t>(
          Status(StatusCode::kCancelled, "upload stream cancelled")));
    }
    state_->persisted_state = state_->bytes_accepted;
    return make_ready_future(StatusOr<std::int64_t>(state_->bytes_accepted));
  }

  future<StatusOr<google::storage::v2::Object>> Finalize(
      gcs::WritePayload payload) override {
    std::shared_ptr<WriterState> state = state_;
    return SendPayload(std::move(payload), /*update_persisted=*/false)
        .then([state = std::move(state)](future<Status> write_f) mutable
                  -> future<StatusOr<google::storage::v2::Object>> {
          if (Status s = write_f.get(); !s.ok()) {
            return make_ready_future(
                StatusOr<google::storage::v2::Object>(std::move(s)));
          }
          {
            std::scoped_lock lk(state->mu);
            if (state->finished) {
              return make_ready_future(StatusOr<google::storage::v2::Object>(
                  state->terminal_error.value_or(Status(
                      StatusCode::kFailedPrecondition,
                      "Finalize called on an already closed upload stream"))));
            }
            state->finished = true;
          }
          auto* rpc_ptr = state->rpc.get();
          return rpc_ptr->WritesDone().then(
              [state = std::move(state)](future<bool>) mutable
                  -> future<StatusOr<google::storage::v2::Object>> {
                auto* finish_rpc = state->rpc.get();
                return finish_rpc->Finish().then(
                    [state = std::move(state)](
                        future<StatusOr<pb::WriteObjectResponse>> f)
                        -> StatusOr<google::storage::v2::Object> {
                      StatusOr<pb::WriteObjectResponse> resp = f.get();
                      if (!resp) {
                        Status s = state->conn->Check(std::move(resp).status(),
                                                      state->generation);
                        std::scoped_lock lk(state->mu);
                        state->terminal_error = s;
                        return s;
                      }
                      bool const json_fallback =
                          resp->transport_used() == pb::JSON;
                      if (json_fallback) {
                        GCP_LOG(WARNING)
                            << "StartAppendableObjectUpload on gs://"
                            << state->bucket_id << "/" << state->object
                            << " completed via the shared Rust server's JSON "
                               "regular-object fallback instead of gRPC "
                               "BidiWriteObject";
                      }
                      google::storage::v2::Object obj = ToV2ObjectFromWrite(
                          *resp, state->bucket_id, state->object,
                          state->content_type);
                      {
                        std::scoped_lock lk(state->mu);
                        state->transport_used = json_fallback ? "JSON" : "GRPC";
                        state->persisted_state = obj;
                      }
                      return obj;
                    });
              });
        });
  }

  RpcMetadata GetRequestMetadata() override {
    RpcMetadata meta = state_->rpc->GetRequestMetadata();
    std::scoped_lock lk(state_->mu);
    if (!state_->transport_used.empty()) {
      meta.headers.emplace("x-uds-transport-used", state_->transport_used);
    }
    return meta;
  }

  std::optional<google::storage::v2::BidiWriteHandle> WriteHandle()
      const override {
    return std::nullopt;
  }

 private:
  future<Status> SendPayload(gcs::WritePayload const& payload,
                             bool update_persisted) {
    {
      std::scoped_lock lk(state_->mu);
      if (state_->terminal_error.has_value()) {
        return make_ready_future(*state_->terminal_error);
      }
      if (state_->cancelled || state_->finished) {
        return make_ready_future(
            Status(StatusCode::kCancelled, "upload stream closed"));
      }
    }
    auto const total_bytes = static_cast<std::int64_t>(payload.size());
    auto chunks = std::make_shared<std::vector<std::string>>(
        SplitPayloadChunks(payload, state_->write_chunk_size));
    return WriteChunkSequence(state_, std::move(chunks), 0, total_bytes,
                              update_persisted);
  }

  std::shared_ptr<WriterState> state_;
};

class UdsAsyncConnection : public gcs::AsyncConnection {
 public:
  UdsAsyncConnection(std::shared_ptr<internal::Connection> conn,
                     std::uint32_t chunk_size_hint, Options options)
      : conn_(std::move(conn)),
        chunk_size_hint_(chunk_size_hint),
        options_(std::move(options)),
        background_(
            google::cloud::internal::MakeBackgroundThreadsFactory(options_)()) {
  }

  Options options() const override { return options_; }

  future<StatusOr<std::shared_ptr<gcs::ObjectDescriptorConnection>>> Open(
      OpenParams p) override {
    google::storage::v2::BidiReadObjectSpec const& spec = p.read_spec;
    if (spec.has_if_generation_not_match()) {
      return make_ready_future(
          StatusOr<std::shared_ptr<gcs::ObjectDescriptorConnection>>(
              UnimplementedAsyncOption("if_generation_not_match", "Open")));
    }
    if (spec.has_if_metageneration_match()) {
      return make_ready_future(
          StatusOr<std::shared_ptr<gcs::ObjectDescriptorConnection>>(
              UnimplementedAsyncOption("if_metageneration_match", "Open")));
    }
    if (spec.has_if_metageneration_not_match()) {
      return make_ready_future(
          StatusOr<std::shared_ptr<gcs::ObjectDescriptorConnection>>(
              UnimplementedAsyncOption("if_metageneration_not_match", "Open")));
    }
    if (spec.has_if_generation_match()) {
      return make_ready_future(
          StatusOr<std::shared_ptr<gcs::ObjectDescriptorConnection>>(
              UnimplementedAsyncOption("if_generation_match", "Open")));
    }

    std::string bucket_id = gcs_internal::GrpcBucketNameToId(spec.bucket());
    std::string object_name = spec.object();
    pb::OpenObjectRequest req;
    req.set_bucket(bucket_id);
    req.set_object(object_name);
    req.set_generation(spec.generation());

    std::shared_ptr<internal::StubSet> stubs;
    pb::GcsCore::Stub& stub_ref = conn_->Stub(stubs);
    std::uint64_t const generation = stubs->generation;
    CompletionQueue cq = background_->cq();
    std::shared_ptr<internal::Connection> conn = conn_;
    std::uint32_t const chunk_size_hint = chunk_size_hint_;
    Options call_options = std::move(p.options);
    if (!call_options.has<gcs::MaximumRangeSizeOption>()) {
      call_options.set<gcs::MaximumRangeSizeOption>(kDefaultMaxRangeSize);
    }

    return cq
        .MakeUnaryRpc(
            [stub = static_cast<pb::GcsCore::StubInterface*>(&stub_ref),
             stubs = std::move(stubs)](grpc::ClientContext* ctx,
                                       pb::OpenObjectRequest const& r,
                                       grpc::CompletionQueue* gq) {
              return stub->AsyncOpenObject(ctx, r, gq);
            },
            req, std::make_unique<grpc::ClientContext>())
        .then(
            [conn = std::move(conn), cq, generation,
             bucket_id = std::move(bucket_id),
             object_name = std::move(object_name), chunk_size_hint,
             call_options = std::move(call_options)](
                future<StatusOr<pb::OpenObjectResponse>> f) mutable
                -> StatusOr<std::shared_ptr<gcs::ObjectDescriptorConnection>> {
              StatusOr<pb::OpenObjectResponse> resp = f.get();
              if (!resp) {
                return conn->Check(std::move(resp).status(), generation);
              }
              google::storage::v2::Object meta =
                  ToV2Object(resp->meta(), bucket_id, object_name);
              return std::shared_ptr<gcs::ObjectDescriptorConnection>(
                  std::make_shared<UdsObjectDescriptorConnection>(
                      std::move(conn), std::move(cq), generation,
                      resp->handle(), std::move(meta), chunk_size_hint,
                      std::move(call_options)));
            });
  }

  future<StatusOr<std::unique_ptr<gcs::AsyncWriterConnection>>>
  StartAppendableObjectUpload(AppendableUploadParams p) override {
    if (p.request.has_append_object_spec()) {
      return make_ready_future(
          StatusOr<std::unique_ptr<gcs::AsyncWriterConnection>>(
              UnimplementedAsyncOption("append_object_spec",
                                       "StartAppendableObjectUpload")));
    }
    if (!p.request.has_write_object_spec()) {
      return make_ready_future(
          StatusOr<std::unique_ptr<gcs::AsyncWriterConnection>>(Status(
              StatusCode::kInvalidArgument,
              "StartAppendableObjectUpload requires write_object_spec")));
    }
    google::storage::v2::WriteObjectSpec const& spec =
        p.request.write_object_spec();
    if (spec.has_if_generation_not_match()) {
      return make_ready_future(
          StatusOr<std::unique_ptr<gcs::AsyncWriterConnection>>(
              UnimplementedAsyncOption("if_generation_not_match",
                                       "StartAppendableObjectUpload")));
    }
    if (spec.has_if_metageneration_match()) {
      return make_ready_future(
          StatusOr<std::unique_ptr<gcs::AsyncWriterConnection>>(
              UnimplementedAsyncOption("if_metageneration_match",
                                       "StartAppendableObjectUpload")));
    }
    if (spec.has_if_metageneration_not_match()) {
      return make_ready_future(
          StatusOr<std::unique_ptr<gcs::AsyncWriterConnection>>(
              UnimplementedAsyncOption("if_metageneration_not_match",
                                       "StartAppendableObjectUpload")));
    }
    if (p.request.has_common_object_request_params()) {
      return make_ready_future(
          StatusOr<std::unique_ptr<gcs::AsyncWriterConnection>>(
              UnimplementedAsyncOption("common_object_request_params",
                                       "StartAppendableObjectUpload")));
    }

    std::string bucket_id =
        gcs_internal::GrpcBucketNameToId(spec.resource().bucket());
    std::string object_name = spec.resource().name();
    std::string content_type = spec.resource().content_type();

    pb::WriteHeader header;
    header.set_bucket(bucket_id);
    header.set_object(object_name);
    header.set_content_type(content_type);
    if (spec.has_if_generation_match()) {
      header.set_if_generation_match(spec.if_generation_match());
    }
    header.set_transport(pb::GRPC);

    std::shared_ptr<internal::StubSet> stubs;
    pb::GcsCore::Stub* stub = &conn_->Stub(stubs);
    std::uint64_t const generation = stubs->generation;
    std::unique_ptr<WriterState::RpcType> rpc =
        google::cloud::internal::MakeStreamingWriteRpc<pb::WriteRequest,
                                                       pb::WriteObjectResponse>(
            background_->cq(), std::make_shared<grpc::ClientContext>(),
            google::cloud::internal::MakeImmutableOptions(p.options),
            [stub](grpc::ClientContext* ctx, pb::WriteObjectResponse* resp,
                   grpc::CompletionQueue* gq) {
              return stub->PrepareAsyncWriteObject(ctx, resp, gq);
            });

    std::string upload_id = absl::StrCat("uds-appendable-", ++next_upload_id_);
    auto state = std::make_shared<WriterState>(
        conn_, std::move(stubs), generation, std::move(rpc),
        std::move(upload_id), std::move(bucket_id), std::move(object_name),
        std::move(content_type), internal::WriteChunkSize(chunk_size_hint_));

    auto* rpc_ptr = state->rpc.get();
    return rpc_ptr->Start().then(
        [state = std::move(state),
         header = std::move(header)](future<bool> start_f) mutable
            -> future<StatusOr<std::unique_ptr<gcs::AsyncWriterConnection>>> {
          {
            std::scoped_lock lk(state->mu);
            state->started = true;
          }
          if (!start_f.get()) {
            return FinishBrokenWriter(state,
                                      "failed to start UDS WriteObject stream")
                .then([](future<Status> s)
                          -> StatusOr<
                              std::unique_ptr<gcs::AsyncWriterConnection>> {
                  return s.get();
                });
          }
          pb::WriteRequest req;
          *req.mutable_header() = std::move(header);
          auto* write_rpc = state->rpc.get();
          return write_rpc->Write(req, grpc::WriteOptions{})
              .then([state = std::move(state)](future<bool> write_f) mutable
                        -> future<StatusOr<
                            std::unique_ptr<gcs::AsyncWriterConnection>>> {
                if (!write_f.get()) {
                  return FinishBrokenWriter(state,
                                            "failed to write UDS WriteHeader")
                      .then(
                          [](future<Status> s)
                              -> StatusOr<
                                  std::unique_ptr<gcs::AsyncWriterConnection>> {
                            return s.get();
                          });
                }
                return make_ready_future(
                    StatusOr<std::unique_ptr<gcs::AsyncWriterConnection>>(
                        std::make_unique<UdsAsyncWriterConnection>(
                            std::move(state))));
              });
        });
  }

  future<Status> DeleteObject(DeleteObjectParams p) override {
    google::storage::v2::DeleteObjectRequest const& r = p.request;
    if (r.has_if_generation_not_match()) {
      return make_ready_future(
          UnimplementedAsyncOption("if_generation_not_match", "DeleteObject"));
    }
    if (r.has_if_metageneration_match()) {
      return make_ready_future(
          UnimplementedAsyncOption("if_metageneration_match", "DeleteObject"));
    }
    if (r.has_if_metageneration_not_match()) {
      return make_ready_future(UnimplementedAsyncOption(
          "if_metageneration_not_match", "DeleteObject"));
    }

    pb::DeleteObjectRequest req;
    req.set_bucket(gcs_internal::GrpcBucketNameToId(r.bucket()));
    req.set_object(r.object());
    req.set_generation(r.generation());
    if (r.has_if_generation_match()) {
      req.set_if_generation_match(r.if_generation_match());
    }

    std::shared_ptr<internal::StubSet> stubs;
    pb::GcsCore::Stub& stub_ref = conn_->Stub(stubs);
    std::uint64_t const generation = stubs->generation;
    std::shared_ptr<internal::Connection> conn = conn_;

    return background_->cq()
        .MakeUnaryRpc(
            [stub = static_cast<pb::GcsCore::StubInterface*>(&stub_ref),
             stubs = std::move(stubs)](grpc::ClientContext* ctx,
                                       pb::DeleteObjectRequest const& q,
                                       grpc::CompletionQueue* gq) {
              return stub->AsyncDeleteObject(ctx, q, gq);
            },
            req, std::make_unique<grpc::ClientContext>())
        .then([conn = std::move(conn),
               generation](future<StatusOr<pb::Empty>> f) -> Status {
          StatusOr<pb::Empty> resp = f.get();
          if (!resp) {
            return conn->Check(std::move(resp).status(), generation);
          }
          return {};
        });
  }

  // --- Unimplemented AsyncConnection methods (pinned in A23) -----------------
  future<StatusOr<google::storage::v2::Bucket>> GetBucket(
      GetBucketParams) override {
    return make_ready_future(StatusOr<google::storage::v2::Bucket>(
        UnimplementedAsyncMethod("GetBucket")));
  }
  future<StatusOr<google::storage::v2::Object>> InsertObject(
      InsertObjectParams) override {
    return make_ready_future(StatusOr<google::storage::v2::Object>(
        UnimplementedAsyncMethod("InsertObject")));
  }
  future<StatusOr<std::unique_ptr<gcs::AsyncReaderConnection>>> ReadObject(
      ReadObjectParams) override {
    return make_ready_future(
        StatusOr<std::unique_ptr<gcs::AsyncReaderConnection>>(
            UnimplementedAsyncMethod("ReadObject")));
  }
  future<StatusOr<gcs::ReadPayload>> ReadObjectRange(
      ReadObjectParams) override {
    return make_ready_future(StatusOr<gcs::ReadPayload>(
        UnimplementedAsyncMethod("ReadObjectRange")));
  }
  future<StatusOr<std::unique_ptr<gcs::AsyncWriterConnection>>>
  ResumeAppendableObjectUpload(AppendableUploadParams) override {
    return make_ready_future(
        StatusOr<std::unique_ptr<gcs::AsyncWriterConnection>>(
            UnimplementedAsyncMethod("ResumeAppendableObjectUpload")));
  }
  future<StatusOr<std::unique_ptr<gcs::AsyncWriterConnection>>>
  StartUnbufferedUpload(UploadParams) override {
    return make_ready_future(
        StatusOr<std::unique_ptr<gcs::AsyncWriterConnection>>(
            UnimplementedAsyncMethod("StartUnbufferedUpload")));
  }
  future<StatusOr<std::unique_ptr<gcs::AsyncWriterConnection>>>
  StartBufferedUpload(UploadParams) override {
    return make_ready_future(
        StatusOr<std::unique_ptr<gcs::AsyncWriterConnection>>(
            UnimplementedAsyncMethod("StartBufferedUpload")));
  }
  future<StatusOr<std::unique_ptr<gcs::AsyncWriterConnection>>>
  ResumeUnbufferedUpload(ResumeUploadParams) override {
    return make_ready_future(
        StatusOr<std::unique_ptr<gcs::AsyncWriterConnection>>(
            UnimplementedAsyncMethod("ResumeUnbufferedUpload")));
  }
  future<StatusOr<std::unique_ptr<gcs::AsyncWriterConnection>>>
  ResumeBufferedUpload(ResumeUploadParams) override {
    return make_ready_future(
        StatusOr<std::unique_ptr<gcs::AsyncWriterConnection>>(
            UnimplementedAsyncMethod("ResumeBufferedUpload")));
  }
  future<StatusOr<google::storage::v2::Object>> ComposeObject(
      ComposeObjectParams) override {
    return make_ready_future(StatusOr<google::storage::v2::Object>(
        UnimplementedAsyncMethod("ComposeObject")));
  }
  std::shared_ptr<gcs::AsyncRewriterConnection> RewriteObject(
      RewriteObjectParams) override {
    return std::make_shared<UnimplementedAsyncRewriterConnection>();
  }

 private:
  std::shared_ptr<internal::Connection> conn_;
  std::uint32_t chunk_size_hint_;
  Options options_;
  std::atomic<std::uint64_t> next_upload_id_{0};
  // Declared last so it is destroyed first, draining CQ callbacks before
  // `conn_` tears down the UDS channels.
  std::unique_ptr<google::cloud::BackgroundThreads> background_;
};

}  // namespace

StatusOr<std::shared_ptr<gcs::AsyncConnection>> MakeUdsAsyncConnection(
    ClientOptions const& core_options, Options options) {
  StatusOr<Options> validated =
      ValidateAndDefaultAsyncOptions(std::move(options));
  if (!validated) return std::move(validated).status();
  StatusOr<std::shared_ptr<internal::Connection>> conn =
      internal::Connection::GetOrCreate(core_options);
  if (!conn) return std::move(conn).status();
  return std::shared_ptr<gcs::AsyncConnection>(
      std::make_shared<UdsAsyncConnection>(*std::move(conn),
                                           core_options.chunk_size_hint,
                                           *std::move(validated)));
}

StatusOr<gcs::AsyncClient> MakeUdsAsyncClient(ClientOptions const& core_options,
                                              Options options) {
  StatusOr<std::shared_ptr<gcs::AsyncConnection>> conn =
      MakeUdsAsyncConnection(core_options, std::move(options));
  if (!conn) return std::move(conn).status();
  return gcs::AsyncClient(*std::move(conn));
}

}  // namespace gcs_core
