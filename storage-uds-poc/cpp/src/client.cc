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

#include "gcs_core/client.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "core_backend.h"
#include "gcscore.grpc.pb.h"
#include "status.h"
#include <grpcpp/grpcpp.h>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <mutex>
#include <thread>

namespace gcs_core {
namespace internal {

namespace pb = ::gcscore::v1;
using ::google::cloud::StatusCode;

namespace {

/// Default and maximum size of one data message sent to the server. Uploads
/// are framed into messages of this size; the server accepts messages up to
/// 64 MiB.
constexpr std::uint32_t kDefaultChunkSize = 1024 * 1024;
constexpr std::uint32_t kMaxChunkSize = 16 * 1024 * 1024;
/// Bound on best-effort `CloseObject` during `~ObjectDescriptor()` so a hung
/// or stopped server cannot stall stack unwinding indefinitely.
constexpr std::chrono::milliseconds kDescriptorCloseTimeout{500};

constexpr std::uint32_t WriteChunkSize(std::uint32_t chunk_size_hint) {
  return chunk_size_hint == 0 ? kDefaultChunkSize
                              : std::min(chunk_size_hint, kMaxChunkSize);
}

/// `ReadChunk.data` is a vector of slices so the server can forward network
/// buffers without concatenating them; join them into one Cord.
absl::Cord JoinSlices(pb::ReadChunk const& chunk) {
  absl::Cord out;
  for (std::string const& slice : chunk.data()) out.Append(slice);
  return out;
}

}  // namespace

grpc::ChannelArguments LocalChannelArguments(int channel_index) {
  grpc::ChannelArguments args;
  args.SetMaxReceiveMessageSize(-1);
  args.SetMaxSendMessageSize(-1);
  // Distinct arguments plus a local subchannel pool give each channel its
  // own connection instead of sharing one.
  args.SetInt(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL, 1);
  args.SetInt("gcs_core.channel_index", channel_index);
  // For `unix:` targets gRPC uses the socket path as `:authority`, which the
  // server's HTTP/2 stack (hyper/h2) rejects with RST_STREAM(PROTOCOL_ERROR).
  // Any valid host name works, and the same one keeps both variants
  // identical.
  args.SetString(GRPC_ARG_DEFAULT_AUTHORITY, "localhost");
  // The first connect usually races the server creating its socket. gRPC's
  // default 1 s reconnect backoff would then dominate startup; a local socket
  // needs only milliseconds between attempts.
  args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 5);
  args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 5);
  args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 100);
  return args;
}

Status ValidateServerFlags(std::vector<std::string> const& args) {
  for (std::string const& arg : args) {
    for (std::string_view flag : {"--socket", "--pdeathsig", "--tcp"}) {
      if (arg == flag || absl::StartsWith(arg, absl::StrCat(flag, "="))) {
        return Status(StatusCode::kInvalidArgument,
                      absl::StrCat(flag,
                                   " is set by the wrapper or not "
                                   "supported; remove it from "
                                   "ClientOptions::sidecar_args"));
      }
    }
  }
  return {};
}

/// The stubs of one running server generation. Replaced as a whole on
/// restart, so a call in flight keeps using the set it started with.
struct StubSet {
  std::uint64_t generation = 0;
  std::vector<std::unique_ptr<pb::GcsCore::Stub>> stubs;
};

/// One running server (whatever the backend) plus the gRPC channels to it,
/// shared by `Client` instances whose backends have the same sharing key.
class Connection {
 public:
  static StatusOr<std::shared_ptr<Connection>> GetOrCreate(
      ClientOptions const& options) {
    if (options.channels < 1) {
      return Status(StatusCode::kInvalidArgument, "channels must be >= 1");
    }
    if (options.readiness_timeout <= std::chrono::milliseconds::zero()) {
      return Status(StatusCode::kInvalidArgument,
                    "readiness_timeout must be > 0");
    }

    static std::mutex mu;
    static std::vector<std::weak_ptr<Connection>> active;
    std::scoped_lock lk(mu);

    std::vector<std::shared_ptr<Connection>> live;
    active.erase(std::remove_if(active.begin(), active.end(),
                                [&](std::weak_ptr<Connection> const& w) {
                                  std::shared_ptr<Connection> c = w.lock();
                                  if (!c) return true;
                                  live.push_back(std::move(c));
                                  return false;
                                }),
                 active.end());

    std::vector<std::string> sockets_in_use;
    for (std::shared_ptr<Connection> const& c : live) {
      if (!c->backend_->SocketPath().empty()) {
        sockets_in_use.push_back(c->backend_->SocketPath());
      }
    }
    StatusOr<std::unique_ptr<CoreBackend>> backend =
        MakeCoreBackend(options, sockets_in_use);
    if (!backend) return std::move(backend).status();
    for (std::shared_ptr<Connection> const& c : live) {
      if (c->backend_->SharingKey() == (*backend)->SharingKey()) return c;
      if (!(*backend)->SocketPath().empty() &&
          c->backend_->SocketPath() == (*backend)->SocketPath()) {
        return Status(
            StatusCode::kFailedPrecondition,
            absl::StrCat("socket path ", (*backend)->SocketPath(),
                         " is already in use by a server with different "
                         "ClientOptions"));
      }
    }

    auto conn = std::shared_ptr<Connection>(
        new Connection(*std::move(backend), options.readiness_timeout));
    std::scoped_lock clk(conn->mu_);
    if (Status s = conn->StartLocked(); !s.ok()) return s;
    active.push_back(conn);
    return conn;
  }

  ~Connection() {
    std::scoped_lock lk(mu_);
    // Our channels first: the server drains open connections before it
    // stops, and no caller can hold a stub once the last Client is gone.
    {
      std::scoped_lock slk(stubs_mu_);
      stubs_.reset();
    }
    backend_->Stop();
  }

  /// Round-robin over the channels. `stubs_mu_` only guards copying the
  /// `shared_ptr` and is never held across an RPC.
  pb::GcsCore::Stub& Stub(std::shared_ptr<StubSet>& keepalive) {
    keepalive = CurrentStubs();
    std::size_t const i = next_.fetch_add(1, std::memory_order_relaxed);
    return *keepalive->stubs[i % keepalive->stubs.size()];
  }

  /// Called after a failed RPC. If the server generation that served this
  /// RPC died, start a new one once so the next call succeeds; concurrent
  /// callers failing on the same generation observe the already-bumped
  /// `generation_` and do not restart a second time.
  Status Check(Status status, std::uint64_t failed_generation) {
    if (status.code() != StatusCode::kUnavailable) return status;
    std::scoped_lock lk(mu_);
    if (failed_generation != generation_) {
      return Status(
          StatusCode::kUnavailable,
          absl::StrCat("server restarted after failure: ", status.message()));
    }
    if (backend_->Exited()) {
      std::string const why = backend_->ExitDescription();
      // The stubs stay in place until StartLocked replaces them, so a
      // concurrent caller never observes an empty set.
      backend_->Stop();
      if (Status s = StartLocked(); !s.ok()) return s;
      return Status(
          StatusCode::kUnavailable,
          absl::StrCat(backend_->Describe(), " ", why,
                       "; restarted it for the next call: ", status.message()));
    }
    return status;
  }

  /// Dispatches one unary RPC on a round-robin channel and checks for crash
  /// restart on `UNAVAILABLE`.
  template <typename Request, typename Response>
  Status Unary(grpc::Status (pb::GcsCore::Stub::*method)(grpc::ClientContext*,
                                                         Request const&,
                                                         Response*),
               Request const& request, Response& response) {
    return Unary(method, request, response, nullptr);
  }

  /// Like the 3-argument `Unary`, and writes the generation that served the
  /// call into `*served_by` when non-null.
  template <typename Request, typename Response>
  Status Unary(grpc::Status (pb::GcsCore::Stub::*method)(grpc::ClientContext*,
                                                         Request const&,
                                                         Response*),
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
  pb::GcsCore::Stub* StubFor(std::uint64_t generation,
                             std::shared_ptr<StubSet>& keepalive) {
    keepalive = CurrentStubs();
    if (!keepalive || keepalive->generation != generation) return nullptr;
    std::size_t const i = next_.fetch_add(1, std::memory_order_relaxed);
    return keepalive->stubs[i % keepalive->stubs.size()].get();
  }

  int pid() const {
    std::scoped_lock lk(mu_);
    return backend_->pid();
  }

  std::optional<std::uint64_t> CoreThreadCpuMicros() const {
    std::scoped_lock lk(mu_);
    return backend_->CoreThreadCpuMicros();
  }

  void KillForTesting() {
    std::scoped_lock lk(mu_);
    backend_->Kill();
  }

 private:
  Connection(std::unique_ptr<CoreBackend> backend,
             std::chrono::milliseconds readiness_timeout)
      : backend_(std::move(backend)), readiness_timeout_(readiness_timeout) {}

  std::shared_ptr<StubSet> CurrentStubs() const {
    std::scoped_lock lk(stubs_mu_);
    return stubs_;
  }

  Status StartLocked() {
    StatusOr<std::vector<std::shared_ptr<grpc::Channel>>> channels =
        backend_->Start();
    if (!channels) return std::move(channels).status();
    auto set = std::make_shared<StubSet>();
    set->generation = ++generation_;
    for (std::shared_ptr<grpc::Channel>& channel : *channels) {
      set->stubs.push_back(pb::GcsCore::NewStub(std::move(channel)));
    }
    {
      std::scoped_lock slk(stubs_mu_);
      stubs_ = set;
    }
    return WaitUntilReadyLocked(*set);
  }

  Status WaitUntilReadyLocked(StubSet const& set) {
    std::chrono::steady_clock::time_point const deadline =
        std::chrono::steady_clock::now() + readiness_timeout_;
    Status last;
    while (std::chrono::steady_clock::now() < deadline) {
      if (backend_->Exited()) {
        return Status(
            StatusCode::kFailedPrecondition,
            absl::StrCat(backend_->Describe(), " ", backend_->ExitDescription(),
                         " before becoming ready"));
      }
      // Every channel must be connected, so the first measured call does not
      // pay connection setup.
      bool all_ready = true;
      for (std::unique_ptr<pb::GcsCore::Stub> const& stub : set.stubs) {
        grpc::ClientContext ctx;
        ctx.set_wait_for_ready(true);
        ctx.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(200));
        pb::PingRequest request;
        pb::PingResponse response;
        Status s = FromGrpc(stub->Ping(&ctx, request, &response));
        if (!s.ok()) {
          last = std::move(s);
          all_ready = false;
          break;
        }
      }
      if (all_ready) {
        backend_->OnReady();
        return {};
      }
    }
    backend_->Kill();
    return Status(
        StatusCode::kDeadlineExceeded,
        absl::StrCat(backend_->Describe(), " did not become ready within ",
                     readiness_timeout_.count(),
                     "ms; last error: ", last.message()));
  }

  std::unique_ptr<CoreBackend> const backend_;
  std::chrono::milliseconds const readiness_timeout_;
  mutable std::mutex mu_;
  std::uint64_t generation_ = 0;
  mutable std::mutex stubs_mu_;
  std::shared_ptr<StubSet> stubs_;
  std::atomic<std::size_t> next_{0};
};

void KillCoreForTesting(Client const& client) {
  client.conn_->KillForTesting();
}

namespace {
/// Splits `data` into messages of at most `chunk` bytes. `Cord::Subcord`
/// shares the underlying buffers, so no bytes are copied here.
template <typename Fn>
bool ForEachChunk(absl::Cord const& data, std::size_t chunk, Fn&& fn) {
  for (std::size_t offset = 0; offset < data.size(); offset += chunk) {
    if (!fn(data.Subcord(offset, chunk))) return false;
  }
  return true;
}
}  // namespace

class ReaderImpl {
 public:
  ReaderImpl(std::shared_ptr<Connection> conn, std::string bucket,
             std::string object)
      : conn_(std::move(conn)),
        bucket_(std::move(bucket)),
        object_(std::move(object)) {}
  ~ReaderImpl() {
    if (reader_ && !done_) {
      context_.TryCancel();
      pb::ReadChunk discard;
      while (reader_->Read(&discard)) {
      }
      (void)reader_->Finish();
    }
  }

  Status Start(pb::ReadObjectRequest const& request) {
    reader_ = conn_->Stub(stubs_).ReadObject(&context_, request);
    pb::ReadChunk first;
    if (!reader_->Read(&first)) {
      done_ = true;
      Status s = FromGrpc(reader_->Finish());
      if (s.ok()) {
        return Status(StatusCode::kInternal,
                      "server closed ReadObject without metadata");
      }
      return conn_->Check(std::move(s), stubs_->generation);
    }
    metadata_ = FromProto(first.meta(), bucket_, object_);
    absl::Cord data = JoinSlices(first);
    if (!data.empty()) pending_ = std::move(data);
    return {};
  }

  ObjectMetadata const& metadata() const { return metadata_; }

  StatusOr<std::optional<absl::Cord>> Next() {
    if (pending_) {
      std::optional<absl::Cord> out = std::move(pending_);
      pending_.reset();
      return out;
    }
    if (done_) return std::optional<absl::Cord>{};
    pb::ReadChunk message;
    while (reader_->Read(&message)) {
      absl::Cord data = JoinSlices(message);
      if (!data.empty()) return std::optional<absl::Cord>(std::move(data));
    }
    done_ = true;
    Status s = FromGrpc(reader_->Finish());
    if (!s.ok()) return conn_->Check(std::move(s), stubs_->generation);
    return std::optional<absl::Cord>{};
  }

 private:
  std::shared_ptr<Connection> conn_;
  std::string bucket_;
  std::string object_;
  std::shared_ptr<StubSet> stubs_;
  grpc::ClientContext context_;
  std::unique_ptr<grpc::ClientReader<pb::ReadChunk>> reader_;
  ObjectMetadata metadata_;
  std::optional<absl::Cord> pending_;
  bool done_ = false;
};

/// One client-streaming `WriteObject`: a header message, then data messages.
/// Used for regular and appendable uploads; the header's transport selects
/// which one the server performs.
class WriterImpl {
 public:
  WriterImpl(std::shared_ptr<Connection> conn, pb::WriteHeader header,
             std::uint32_t chunk_size_hint)
      : conn_(std::move(conn)),
        header_(std::move(header)),
        write_chunk_size_(WriteChunkSize(chunk_size_hint)) {
    writer_ = conn_->Stub(stubs_).WriteObject(&context_, &response_);
  }
  ~WriterImpl() {
    // Cancelling makes the server fail the upload instead of finalizing a
    // partial object.
    if (writer_ && !finished_) {
      context_.TryCancel();
      (void)writer_->Finish();
    }
  }

  std::string const& bucket() const { return header_.bucket(); }
  std::string const& object() const { return header_.object(); }

  Status Write(absl::Cord const& data) {
    if (finished_) {
      return Status(StatusCode::kFailedPrecondition, "upload already closed");
    }
    bool const ok =
        SendHeader() &&
        ForEachChunk(data, write_chunk_size_, [&](absl::Cord const& chunk) {
          pb::WriteRequest request;
          request.set_data(std::string(chunk));
          return writer_->Write(request);
        });
    // A failed Write means the stream is broken; Finish reports why.
    if (!ok) return Finish().status();
    return {};
  }

  StatusOr<pb::WriteObjectResponse> Finish() {
    if (finished_) {
      return Status(StatusCode::kFailedPrecondition, "upload already closed");
    }
    (void)SendHeader();
    (void)writer_->WritesDone();
    finished_ = true;
    Status s = FromGrpc(writer_->Finish());
    if (!s.ok()) return conn_->Check(std::move(s), stubs_->generation);
    return response_;
  }

 private:
  bool SendHeader() {
    if (header_sent_) return true;
    header_sent_ = true;
    pb::WriteRequest request;
    *request.mutable_header() = header_;
    return writer_->Write(request);
  }

  std::shared_ptr<Connection> conn_;
  std::shared_ptr<StubSet> stubs_;
  pb::WriteHeader header_;
  std::size_t write_chunk_size_;
  grpc::ClientContext context_;
  pb::WriteObjectResponse response_;
  std::unique_ptr<grpc::ClientWriter<pb::WriteRequest>> writer_;
  bool header_sent_ = false;
  bool finished_ = false;
};

namespace {
ObjectMetadata FromWriteResponse(pb::WriteObjectResponse const& r,
                                 std::string const& bucket,
                                 std::string const& object) {
  ObjectMetadata out;
  out.bucket = bucket;
  out.name = object;
  out.generation = r.generation();
  out.size = static_cast<std::uint64_t>(r.size());
  if (r.crc32c() != 0) out.crc32c = r.crc32c();
  return out;
}
}  // namespace

}  // namespace internal

namespace pb = ::gcscore::v1;
using ::google::cloud::StatusCode;
using internal::FromGrpc;
using internal::FromProto;

// --- ObjectReader
// -------------------------------------------------------------
ObjectReader::ObjectReader(std::unique_ptr<internal::ReaderImpl> impl)
    : impl_(std::move(impl)) {}
ObjectReader::ObjectReader(ObjectReader&&) noexcept = default;
ObjectReader& ObjectReader::operator=(ObjectReader&&) noexcept = default;
ObjectReader::~ObjectReader() = default;
ObjectMetadata const& ObjectReader::metadata() const {
  return impl_->metadata();
}
StatusOr<std::optional<absl::Cord>> ObjectReader::Next() {
  return impl_->Next();
}

// --- ObjectWriter
// -------------------------------------------------------------
ObjectWriter::ObjectWriter(std::unique_ptr<internal::WriterImpl> impl)
    : impl_(std::move(impl)) {}
ObjectWriter::ObjectWriter(ObjectWriter&&) noexcept = default;
ObjectWriter& ObjectWriter::operator=(ObjectWriter&&) noexcept = default;
ObjectWriter::~ObjectWriter() = default;
Status ObjectWriter::Write(absl::Cord const& data) {
  return impl_->Write(data);
}
StatusOr<ObjectMetadata> ObjectWriter::Close() {
  StatusOr<pb::WriteObjectResponse> response = impl_->Finish();
  if (!response) return std::move(response).status();
  return internal::FromWriteResponse(*response, impl_->bucket(),
                                     impl_->object());
}

// --- AppendableWriter
// -----------------------------------------------------------
AppendableWriter::AppendableWriter(std::unique_ptr<internal::WriterImpl> impl)
    : impl_(std::move(impl)) {}
AppendableWriter::AppendableWriter(AppendableWriter&&) noexcept = default;
AppendableWriter& AppendableWriter::operator=(AppendableWriter&&) noexcept =
    default;
AppendableWriter::~AppendableWriter() = default;
Status AppendableWriter::Write(absl::Cord const& data) {
  return impl_->Write(data);
}
StatusOr<ObjectMetadata> AppendableWriter::Finalize(bool allow_json_fallback) {
  StatusOr<pb::WriteObjectResponse> response = impl_->Finish();
  if (!response) return std::move(response).status();
  bool const json_fallback = response->transport_used() != pb::GRPC;
  // Unless explicitly requested, a JSON fallback means the caller measured or
  // relied on something other than an appendable BidiWriteObject upload.
  if (json_fallback && !allow_json_fallback) {
    return Status(StatusCode::kFailedPrecondition,
                  absl::StrCat("server wrote gs://", impl_->bucket(), "/",
                               impl_->object(),
                               " as a regular object over JSON instead of an "
                               "appendable BidiWriteObject upload"));
  }
  ObjectMetadata meta =
      internal::FromWriteResponse(*response, impl_->bucket(), impl_->object());
  meta.used_json_fallback = json_fallback;
  return meta;
}
// NOLINTNEXTLINE(readability-convert-member-functions-to-static): public API
StatusOr<std::int64_t> AppendableWriter::Close() {
  return Status(StatusCode::kUnimplemented,
                "the shared server finalizes appendable uploads when the "
                "stream ends; leaving an object appendable is not supported");
}

// --- ObjectDescriptor
// -----------------------------------------------------------
ObjectDescriptor::ObjectDescriptor(std::shared_ptr<internal::Connection> conn,
                                   std::uint64_t generation,
                                   std::uint64_t handle,
                                   ObjectMetadata metadata,
                                   std::uint32_t chunk_size_hint)
    : conn_(std::move(conn)),
      generation_(generation),
      handle_(handle),
      metadata_(std::move(metadata)),
      chunk_size_hint_(chunk_size_hint) {}
ObjectDescriptor::ObjectDescriptor(ObjectDescriptor&& rhs) noexcept
    : conn_(std::move(rhs.conn_)),
      generation_(rhs.generation_),
      handle_(rhs.handle_),
      metadata_(std::move(rhs.metadata_)),
      chunk_size_hint_(rhs.chunk_size_hint_) {}
ObjectDescriptor& ObjectDescriptor::operator=(ObjectDescriptor&& rhs) noexcept {
  ObjectDescriptor tmp(std::move(rhs));
  std::swap(conn_, tmp.conn_);
  std::swap(generation_, tmp.generation_);
  std::swap(handle_, tmp.handle_);
  std::swap(metadata_, tmp.metadata_);
  std::swap(chunk_size_hint_, tmp.chunk_size_hint_);
  return *this;
}
ObjectDescriptor::~ObjectDescriptor() {
  if (!conn_) return;
  std::shared_ptr<internal::StubSet> stubs;
  pb::GcsCore::Stub* stub = conn_->StubFor(generation_, stubs);
  // The server that issued the handle is gone and took the handle with it.
  if (stub == nullptr) return;
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() +
                   internal::kDescriptorCloseTimeout);
  pb::CloseObjectRequest request;
  request.set_handle(handle_);
  pb::Empty response;
  (void)stub->CloseObject(&ctx, request, &response);
}
ObjectMetadata const& ObjectDescriptor::metadata() const { return metadata_; }

namespace {

/// One in-flight `ReadRange` RPC of `ObjectDescriptor::ReadRanges`.
struct RangeCall {
  grpc::ClientContext context;
  pb::ReadChunk message;
  grpc::Status status;
  std::unique_ptr<grpc::ClientAsyncReader<pb::ReadChunk>> reader;
};

enum class RangeEvent : std::uintptr_t { kStarted = 0, kRead = 1, kDone = 2 };
constexpr std::uintptr_t kRangeEventBits = 2;

void* RangeTag(std::size_t index, RangeEvent event) {
  // gRPC completion-queue tags are opaque pointers; packing the range index
  // and event into one avoids allocating a tag per event.
  // NOLINTNEXTLINE(performance-no-int-to-ptr)
  return reinterpret_cast<void*>((index << kRangeEventBits) |
                                 static_cast<std::uintptr_t>(event));
}

}  // namespace

Status ObjectDescriptor::ReadRanges(
    std::vector<ReadRange> const& ranges,
    std::function<void(std::size_t, absl::Cord const&)> const& on_data) {
  if (ranges.empty()) return {};
  std::shared_ptr<internal::StubSet> stubs;
  pb::GcsCore::Stub* stub = conn_->StubFor(generation_, stubs);
  if (stub == nullptr) {
    return Status(StatusCode::kFailedPrecondition,
                  "the server that opened this descriptor has restarted; "
                  "open the object again");
  }
  // One completion queue drives all ranges from the calling thread, so
  // `on_data` runs here without a thread per range.
  grpc::CompletionQueue cq;
  std::vector<std::unique_ptr<RangeCall>> calls;
  calls.reserve(ranges.size());
  for (std::size_t i = 0; i != ranges.size(); ++i) {
    auto call = std::make_unique<RangeCall>();
    auto const [offset, length] = internal::ToOffsetLength(ranges[i]);
    pb::ReadRangeRequest request;
    request.set_handle(handle_);
    request.set_offset(offset);
    request.set_length(length);
    request.set_chunk_size(chunk_size_hint_);
    call->reader = stub->PrepareAsyncReadRange(&call->context, request, &cq);
    call->reader->StartCall(RangeTag(i, RangeEvent::kStarted));
    calls.push_back(std::move(call));
  }

  Status first_error;
  std::size_t remaining = calls.size();
  void* tag = nullptr;
  bool ok = false;
  while (remaining != 0 && cq.Next(&tag, &ok)) {
    auto const bits = reinterpret_cast<std::uintptr_t>(tag);
    std::size_t const index = bits >> kRangeEventBits;
    auto const event =
        static_cast<RangeEvent>(bits & ((1U << kRangeEventBits) - 1));
    RangeCall& call = *calls[index];
    switch (event) {
      case RangeEvent::kStarted:
      case RangeEvent::kRead:
        if (event == RangeEvent::kRead && ok) {
          on_data(index, internal::JoinSlices(call.message));
        }
        if (ok) {
          call.reader->Read(&call.message, RangeTag(index, RangeEvent::kRead));
        } else {
          call.reader->Finish(&call.status, RangeTag(index, RangeEvent::kDone));
        }
        break;
      case RangeEvent::kDone:
        --remaining;
        if (!call.status.ok() && first_error.ok()) {
          first_error = FromGrpc(call.status);
          // The result is an error either way; stop the other ranges early.
          for (std::unique_ptr<RangeCall> const& c : calls) {
            c->context.TryCancel();
          }
        }
        break;
    }
  }
  cq.Shutdown();
  while (cq.Next(&tag, &ok)) {
  }
  if (first_error.ok()) return {};
  return conn_->Check(std::move(first_error), generation_);
}

// --- Client
// ---------------------------------------------------------------------
Client::Client(std::shared_ptr<internal::Connection> conn,
               std::uint32_t chunk_size_hint)
    : conn_(std::move(conn)), chunk_size_hint_(chunk_size_hint) {}

StatusOr<Client> Client::Create(ClientOptions const& options) {
  StatusOr<std::shared_ptr<internal::Connection>> conn =
      internal::Connection::GetOrCreate(options);
  if (!conn) return std::move(conn).status();
  return Client(*std::move(conn), options.chunk_size_hint);
}

StatusOr<ObjectReader> Client::ReadObject(
    std::string const& bucket, std::string const& object, ReadRange range,
    std::int64_t generation, std::optional<std::int64_t> if_generation_match) {
  auto const [offset, length] = internal::ToOffsetLength(range);
  pb::ReadObjectRequest request;
  request.set_bucket(bucket);
  request.set_object(object);
  request.set_offset(offset);
  request.set_length(length);
  request.set_generation(generation);
  if (if_generation_match.has_value()) {
    request.set_if_generation_match(*if_generation_match);
  }
  request.set_transport(pb::JSON);
  request.set_chunk_size(chunk_size_hint_);
  auto impl = std::make_unique<internal::ReaderImpl>(conn_, bucket, object);
  if (Status s = impl->Start(request); !s.ok()) return s;
  return ObjectReader(std::move(impl));
}

StatusOr<ObjectWriter> Client::WriteObject(std::string const& bucket,
                                           std::string const& object,
                                           WriteOptions const& options) {
  if (options.mode == UploadMode::kSingleShot && !options.size_hint) {
    return Status(StatusCode::kInvalidArgument,
                  "UploadMode::kSingleShot requires size_hint");
  }
  return ObjectWriter(std::make_unique<internal::WriterImpl>(
      conn_, internal::ToWriteHeader(options, bucket, object, pb::JSON),
      chunk_size_hint_));
}

StatusOr<ObjectMetadata> Client::InsertObject(std::string const& bucket,
                                              std::string const& object,
                                              absl::Cord const& data,
                                              WriteOptions options) {
  options.size_hint = data.size();
  StatusOr<ObjectWriter> writer = WriteObject(bucket, object, options);
  if (!writer) return std::move(writer).status();
  if (Status s = writer->Write(data); !s.ok()) return s;
  return writer->Close();
}

StatusOr<ListObjectsPage> Client::ListObjects(
    std::string const& bucket, ListObjectsRequest const& request) {
  pb::ListObjectsRequest proto;
  proto.set_bucket(bucket);
  proto.set_prefix(request.prefix);
  proto.set_delimiter(request.delimiter);
  proto.set_start_offset(request.start_offset);
  proto.set_end_offset(request.end_offset);
  proto.set_versions(request.versions);
  proto.set_page_size(request.page_size);
  proto.set_page_token(request.page_token);
  pb::ListObjectsResponse response;
  if (Status s = conn_->Unary(&pb::GcsCore::Stub::ListObjects, proto, response);
      !s.ok()) {
    return s;
  }
  ListObjectsPage page;
  page.objects.reserve(response.items_size());
  for (pb::ListedObject const& o : response.items()) {
    page.objects.push_back(FromProto(o, bucket));
  }
  page.prefixes.assign(response.prefixes().begin(), response.prefixes().end());
  page.next_page_token = response.next_page_token();
  return page;
}

StatusOr<ListBucketsPage> Client::ListBuckets(
    std::string const& project, ListBucketsRequest const& request) {
  pb::ListBucketsRequest proto;
  proto.set_project(project);
  proto.set_prefix(request.prefix);
  proto.set_page_size(request.page_size);
  proto.set_page_token(request.page_token);
  pb::ListBucketsResponse response;
  if (Status s = conn_->Unary(&pb::GcsCore::Stub::ListBuckets, proto, response);
      !s.ok()) {
    return s;
  }
  ListBucketsPage page;
  page.buckets.reserve(response.items_size());
  for (pb::ListedBucket const& b : response.items()) {
    page.buckets.push_back(FromProto(b));
  }
  page.next_page_token = response.next_page_token();
  return page;
}

Status Client::DeleteObject(std::string const& bucket,
                            std::string const& object, std::int64_t generation,
                            std::optional<std::int64_t> if_generation_match) {
  pb::DeleteObjectRequest request;
  request.set_bucket(bucket);
  request.set_object(object);
  request.set_generation(generation);
  if (if_generation_match.has_value()) {
    request.set_if_generation_match(*if_generation_match);
  }
  pb::Empty response;
  return conn_->Unary(&pb::GcsCore::Stub::DeleteObject, request, response);
}

StatusOr<ObjectDescriptor> Client::OpenObject(std::string const& bucket,
                                              std::string const& object,
                                              std::int64_t generation) {
  pb::OpenObjectRequest request;
  request.set_bucket(bucket);
  request.set_object(object);
  request.set_generation(generation);
  pb::OpenObjectResponse response;
  std::uint64_t served_by = 0;
  if (Status s = conn_->Unary(&pb::GcsCore::Stub::OpenObject, request, response,
                              &served_by);
      !s.ok()) {
    return s;
  }
  return ObjectDescriptor(conn_, served_by, response.handle(),
                          FromProto(response.meta(), bucket, object),
                          chunk_size_hint_);
}

StatusOr<AppendableWriter> Client::StartAppendableUpload(
    std::string const& bucket, std::string const& object,
    WriteOptions const& options) {
  if (options.mode != UploadMode::kAuto) {
    return Status(StatusCode::kInvalidArgument,
                  "StartAppendableUpload only supports UploadMode::kAuto");
  }
  if (options.size_hint.has_value()) {
    return Status(StatusCode::kInvalidArgument,
                  "StartAppendableUpload does not support size_hint");
  }
  return AppendableWriter(std::make_unique<internal::WriterImpl>(
      conn_, internal::ToWriteHeader(options, bucket, object, pb::GRPC),
      chunk_size_hint_));
}

namespace {
StatusOr<pb::StatsResponse> Stats(internal::Connection& conn,
                                  bool reset_peak_rss) {
  pb::StatsRequest request;
  request.set_reset_peak_rss(reset_peak_rss);
  pb::StatsResponse response;
  if (Status s = conn.Unary(&pb::GcsCore::Stub::Stats, request, response);
      !s.ok()) {
    return s;
  }
  return response;
}

std::uint64_t SecondsToMicros(double seconds) {
  return static_cast<std::uint64_t>(seconds * 1e6);
}
}  // namespace

StatusOr<SidecarVersion> Client::GetSidecarVersion() {
  StatusOr<pb::StatsResponse> stats = Stats(*conn_, /*reset_peak_rss=*/false);
  if (!stats) return std::move(stats).status();
  SidecarVersion v;
  v.pid = static_cast<std::uint32_t>(stats->pid());
  return v;
}

StatusOr<SidecarStats> Client::GetSidecarStats(bool reset_peak_rss) {
  StatusOr<pb::StatsResponse> stats = Stats(*conn_, reset_peak_rss);
  if (!stats) return std::move(stats).status();
  SidecarStats out;
  out.user_cpu_us = SecondsToMicros(stats->cpu_user_secs());
  out.system_cpu_us = SecondsToMicros(stats->cpu_sys_secs());
  out.rss_bytes = static_cast<std::uint64_t>(stats->rss_bytes());
  out.peak_rss_bytes = static_cast<std::uint64_t>(stats->peak_rss_bytes());
  out.threads = static_cast<std::uint32_t>(stats->threads());
  return out;
}

int Client::sidecar_pid() const { return conn_->pid(); }

std::optional<std::uint64_t> Client::core_thread_cpu_us() const {
  return conn_->CoreThreadCpuMicros();
}

}  // namespace gcs_core
