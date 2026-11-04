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
#include "absl/strings/str_cat.h"
#include "core_backend.h"
#include "gcs/core/v0/storage_core.grpc.pb.h"
#include "status.h"
#include <grpcpp/grpcpp.h>
#include <algorithm>
#include <atomic>
#include <mutex>
#include <thread>

namespace gcs_core {
namespace internal {

namespace v0 = ::gcs::core::v0;
using ::google::cloud::StatusCode;

namespace {

/// Default and maximum size of one data message sent to the core. Uploads
/// are framed into messages of this size, matching `service::MAX_CHUNK`
/// (16 MiB) inside the core's 32 MiB gRPC message envelope.
constexpr std::uint32_t kDefaultChunkSize = 1024 * 1024;
constexpr std::uint32_t kMaxChunkSize = 16 * 1024 * 1024;
/// Bound on best-effort `CloseObject` during `~ObjectDescriptor()` so a hung
/// or stopped core cannot stall stack unwinding indefinitely.
constexpr std::chrono::milliseconds kDescriptorCloseTimeout{500};

constexpr std::uint32_t WriteChunkSize(std::uint32_t chunk_size_hint) {
  return chunk_size_hint == 0 ? kDefaultChunkSize
                              : std::min(chunk_size_hint, kMaxChunkSize);
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
  // core's HTTP/2 stack (hyper/h2) rejects with RST_STREAM(PROTOCOL_ERROR);
  // channels created from a descriptor default to `test.authority`. Any
  // valid host name works, and the same one keeps both variants identical.
  args.SetString(GRPC_ARG_DEFAULT_AUTHORITY, "localhost");
  // The first connect usually races the sidecar creating its socket. gRPC's
  // default 1 s reconnect backoff would then dominate startup; a local socket
  // needs only milliseconds between attempts. (A descriptor channel never
  // reconnects; the arguments are harmless there.)
  args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 5);
  args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 5);
  args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 100);
  return args;
}

/// The stubs of one running core generation. Replaced as a whole on
/// restart, so a call in flight keeps using the set it started with.
struct StubSet {
  std::uint64_t generation = 0;
  std::vector<std::unique_ptr<v0::StorageCore::Stub>> stubs;
};

/// One running core (whatever the backend) plus the gRPC channels to it,
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
                         " is already in use by a sidecar with different "
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
    // Our channels first: an in-process core drains open connections before
    // it stops, and no caller can hold a stub once the last Client is gone.
    {
      std::scoped_lock slk(stubs_mu_);
      stubs_.reset();
    }
    backend_->Stop();
  }

  /// Round-robin over the channels. `stubs_mu_` only guards copying the
  /// `shared_ptr` and is never held across an RPC.
  v0::StorageCore::Stub& Stub(std::shared_ptr<StubSet>& keepalive) {
    keepalive = CurrentStubs();
    std::size_t const i = next_.fetch_add(1, std::memory_order_relaxed);
    return *keepalive->stubs[i % keepalive->stubs.size()];
  }

  /// Called after a failed RPC. If the core generation that served this RPC
  /// died, start a new one once so the next call succeeds; concurrent
  /// callers failing on the same generation observe the already-bumped
  /// `generation_` and do not restart a second time.
  Status Check(Status status, std::uint64_t failed_generation) {
    if (status.code() != StatusCode::kUnavailable) return status;
    std::scoped_lock lk(mu_);
    if (failed_generation != generation_) {
      return Status(
          StatusCode::kUnavailable,
          absl::StrCat("core restarted after failure: ", status.message()));
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
  Status Unary(grpc::Status (v0::StorageCore::Stub::*method)(
                   grpc::ClientContext*, Request const&, Response*),
               Request const& request, Response& response) {
    std::shared_ptr<StubSet> stubs;
    grpc::ClientContext ctx;
    Status s = FromGrpc((Stub(stubs).*method)(&ctx, request, &response));
    if (!s.ok()) return Check(std::move(s), stubs->generation);
    return {};
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
      set->stubs.push_back(v0::StorageCore::NewStub(std::move(channel)));
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
      for (std::unique_ptr<v0::StorageCore::Stub> const& stub : set.stubs) {
        grpc::ClientContext ctx;
        ctx.set_wait_for_ready(true);
        ctx.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(200));
        v0::GetVersionRequest request;
        v0::GetVersionResponse response;
        Status s = FromGrpc(stub->GetVersion(&ctx, request, &response));
        if (!s.ok()) {
          last = std::move(s);
          all_ready = false;
          break;
        }
      }
      if (all_ready) return {};
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
  explicit ReaderImpl(std::shared_ptr<Connection> conn)
      : conn_(std::move(conn)) {}
  ~ReaderImpl() {
    if (reader_ && !done_) {
      context_.TryCancel();
      v0::ReadObjectResponse discard;
      while (reader_->Read(&discard)) {
      }
      (void)reader_->Finish();
    }
  }

  Status Start(v0::ReadObjectRequest const& request) {
    reader_ = conn_->Stub(stubs_).ReadObject(&context_, request);
    v0::ReadObjectResponse first;
    if (!reader_->Read(&first)) {
      done_ = true;
      Status s = FromGrpc(reader_->Finish());
      if (s.ok()) {
        return Status(StatusCode::kInternal,
                      "core closed ReadObject without metadata");
      }
      return conn_->Check(std::move(s), stubs_->generation);
    }
    metadata_ = FromProto(first.metadata());
    if (!first.data().empty()) pending_ = absl::Cord(first.data());
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
    v0::ReadObjectResponse message;
    while (reader_->Read(&message)) {
      if (!message.data().empty()) {
        return std::optional<absl::Cord>(absl::Cord(message.data()));
      }
    }
    done_ = true;
    Status s = FromGrpc(reader_->Finish());
    if (!s.ok()) return conn_->Check(std::move(s), stubs_->generation);
    return std::optional<absl::Cord>{};
  }

 private:
  std::shared_ptr<Connection> conn_;
  std::shared_ptr<StubSet> stubs_;
  grpc::ClientContext context_;
  std::unique_ptr<grpc::ClientReader<v0::ReadObjectResponse>> reader_;
  ObjectMetadata metadata_;
  std::optional<absl::Cord> pending_;
  bool done_ = false;
};

/// Shared shape of the two client-streaming uploads.
template <typename Request, typename Response>
class UploadStream {
 public:
  using StartFn = std::unique_ptr<grpc::ClientWriter<Request>> (
      v0::StorageCore::Stub::*)(grpc::ClientContext*, Response*);

  UploadStream(std::shared_ptr<Connection> conn, v0::WriteSpec spec,
               std::uint32_t chunk_size_hint, StartFn start)
      : conn_(std::move(conn)),
        spec_(std::move(spec)),
        write_chunk_size_(WriteChunkSize(chunk_size_hint)) {
    writer_ = (conn_->Stub(stubs_).*start)(&context_, &response_);
  }
  ~UploadStream() {
    if (writer_ && !finished_) {
      context_.TryCancel();
      (void)writer_->Finish();
    }
  }

  Status Write(absl::Cord const& data) {
    if (finished_) {
      return Status(StatusCode::kFailedPrecondition, "upload already closed");
    }
    bool const ok =
        ForEachChunk(data, write_chunk_size_, [&](absl::Cord const& chunk) {
          Request request;
          AttachSpec(request);
          request.set_data(chunk);
          return writer_->Write(request);
        });
    // A failed Write means the stream is broken; Finish reports why.
    if (!ok) return Finish().status();
    return {};
  }

  StatusOr<Response> Finish() { return FinishImpl(Request{}, false); }

  /// Sends `last` as the final message (carrying flags such as `finalize`).
  StatusOr<Response> FinishWithLast(Request last) {
    return FinishImpl(std::move(last), true);
  }

 private:
  StatusOr<Response> FinishImpl(Request last, bool send_last) {
    if (finished_) {
      return Status(StatusCode::kFailedPrecondition, "upload already closed");
    }
    if (send_last || !spec_sent_) {
      AttachSpec(last);
      (void)writer_->Write(last);
    }
    (void)writer_->WritesDone();
    finished_ = true;
    Status s = FromGrpc(writer_->Finish());
    if (!s.ok()) return conn_->Check(std::move(s), stubs_->generation);
    return response_;
  }

  void AttachSpec(Request& request) {
    if (spec_sent_) return;
    *request.mutable_spec() = spec_;
    spec_sent_ = true;
  }

  std::shared_ptr<Connection> conn_;
  std::shared_ptr<StubSet> stubs_;
  v0::WriteSpec spec_;
  std::size_t write_chunk_size_;
  grpc::ClientContext context_;
  Response response_;
  std::unique_ptr<grpc::ClientWriter<Request>> writer_;
  bool spec_sent_ = false;
  bool finished_ = false;
};

class WriterImpl
    : public UploadStream<v0::WriteObjectRequest, v0::WriteObjectResponse> {
 public:
  WriterImpl(std::shared_ptr<Connection> conn, v0::WriteSpec spec,
             std::uint32_t chunk_size_hint)
      : UploadStream(std::move(conn), std::move(spec), chunk_size_hint,
                     &v0::StorageCore::Stub::WriteObject) {}
};

class AppendableWriterImpl : public UploadStream<v0::AppendableWriteRequest,
                                                 v0::AppendableWriteResponse> {
 public:
  AppendableWriterImpl(std::shared_ptr<Connection> conn, v0::WriteSpec spec,
                       std::uint32_t chunk_size_hint)
      : UploadStream(std::move(conn), std::move(spec), chunk_size_hint,
                     &v0::StorageCore::Stub::AppendableWrite) {}
};

}  // namespace internal

namespace v0 = ::gcs::core::v0;
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
  StatusOr<v0::WriteObjectResponse> response = impl_->Finish();
  if (!response) return std::move(response).status();
  return FromProto(response->metadata());
}

// --- AppendableWriter
// -----------------------------------------------------------
AppendableWriter::AppendableWriter(
    std::unique_ptr<internal::AppendableWriterImpl> impl)
    : impl_(std::move(impl)) {}
AppendableWriter::AppendableWriter(AppendableWriter&&) noexcept = default;
AppendableWriter& AppendableWriter::operator=(AppendableWriter&&) noexcept =
    default;
AppendableWriter::~AppendableWriter() = default;
Status AppendableWriter::Write(absl::Cord const& data) {
  return impl_->Write(data);
}
StatusOr<ObjectMetadata> AppendableWriter::Finalize() {
  v0::AppendableWriteRequest last;
  last.set_finalize(true);
  StatusOr<v0::AppendableWriteResponse> response =
      impl_->FinishWithLast(std::move(last));
  if (!response) return std::move(response).status();
  return FromProto(response->metadata());
}
StatusOr<std::int64_t> AppendableWriter::Close() {
  StatusOr<v0::AppendableWriteResponse> response = impl_->Finish();
  if (!response) return std::move(response).status();
  return response->persisted_size();
}

// --- ObjectDescriptor
// -----------------------------------------------------------
ObjectDescriptor::ObjectDescriptor(std::shared_ptr<internal::Connection> conn,
                                   std::uint64_t handle,
                                   ObjectMetadata metadata,
                                   std::uint32_t chunk_size_hint)
    : conn_(std::move(conn)),
      handle_(handle),
      metadata_(std::move(metadata)),
      chunk_size_hint_(chunk_size_hint) {}
ObjectDescriptor::ObjectDescriptor(ObjectDescriptor&& rhs) noexcept
    : conn_(std::move(rhs.conn_)),
      handle_(rhs.handle_),
      metadata_(std::move(rhs.metadata_)),
      chunk_size_hint_(rhs.chunk_size_hint_) {}
ObjectDescriptor& ObjectDescriptor::operator=(ObjectDescriptor&& rhs) noexcept {
  ObjectDescriptor tmp(std::move(rhs));
  std::swap(conn_, tmp.conn_);
  std::swap(handle_, tmp.handle_);
  std::swap(metadata_, tmp.metadata_);
  std::swap(chunk_size_hint_, tmp.chunk_size_hint_);
  return *this;
}
ObjectDescriptor::~ObjectDescriptor() {
  if (!conn_) return;
  std::shared_ptr<internal::StubSet> stubs;
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() +
                   internal::kDescriptorCloseTimeout);
  v0::CloseObjectRequest request;
  request.set_handle(handle_);
  v0::CloseObjectResponse response;
  (void)conn_->Stub(stubs).CloseObject(&ctx, request, &response);
}
ObjectMetadata const& ObjectDescriptor::metadata() const { return metadata_; }

Status ObjectDescriptor::ReadRanges(
    std::vector<ReadRange> const& ranges,
    std::function<void(std::size_t, absl::Cord const&)> const& on_data) {
  v0::ReadRangesRequest request;
  request.set_handle(handle_);
  request.set_chunk_size_hint(chunk_size_hint_);
  for (ReadRange const& r : ranges) {
    v0::ReadRange& p = *request.add_ranges();
    p.set_offset(r.offset);
    p.set_limit(r.limit);
  }
  std::shared_ptr<internal::StubSet> stubs;
  grpc::ClientContext ctx;
  std::unique_ptr<grpc::ClientReader<v0::ReadRangesResponse>> reader =
      conn_->Stub(stubs).ReadRanges(&ctx, request);
  v0::ReadRangesResponse message;
  while (reader->Read(&message)) {
    on_data(message.range_index(), absl::Cord(message.data()));
  }
  return conn_->Check(FromGrpc(reader->Finish()), stubs->generation);
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

StatusOr<ObjectReader> Client::ReadObject(std::string const& bucket,
                                          std::string const& object,
                                          ReadRange range,
                                          std::int64_t generation) {
  v0::ReadObjectRequest request;
  request.set_bucket(bucket);
  request.set_object(object);
  request.set_generation(generation);
  request.mutable_range()->set_offset(range.offset);
  request.mutable_range()->set_limit(range.limit);
  request.set_chunk_size_hint(chunk_size_hint_);
  auto impl = std::make_unique<internal::ReaderImpl>(conn_);
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
  v0::WriteSpec spec;
  internal::ToProto(options, bucket, object, spec);
  return ObjectWriter(std::make_unique<internal::WriterImpl>(
      conn_, std::move(spec), chunk_size_hint_));
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
  v0::ListObjectsRequest proto;
  proto.set_bucket(bucket);
  proto.set_prefix(request.prefix);
  proto.set_delimiter(request.delimiter);
  proto.set_page_size(request.page_size);
  proto.set_page_token(request.page_token);
  v0::ListObjectsResponse response;
  if (Status s =
          conn_->Unary(&v0::StorageCore::Stub::ListObjects, proto, response);
      !s.ok()) {
    return s;
  }
  ListObjectsPage page;
  page.objects.reserve(response.objects_size());
  for (v0::ObjectMetadata const& o : response.objects()) {
    page.objects.push_back(FromProto(o));
  }
  page.prefixes.assign(response.prefixes().begin(), response.prefixes().end());
  page.next_page_token = response.next_page_token();
  return page;
}

Status Client::DeleteObject(std::string const& bucket,
                            std::string const& object,
                            std::int64_t generation) {
  v0::DeleteObjectRequest request;
  request.set_bucket(bucket);
  request.set_object(object);
  request.set_generation(generation);
  v0::DeleteObjectResponse response;
  return conn_->Unary(&v0::StorageCore::Stub::DeleteObject, request, response);
}

StatusOr<ObjectDescriptor> Client::OpenObject(std::string const& bucket,
                                              std::string const& object,
                                              std::int64_t generation) {
  v0::OpenObjectRequest request;
  request.set_bucket(bucket);
  request.set_object(object);
  request.set_generation(generation);
  v0::OpenObjectResponse response;
  if (Status s =
          conn_->Unary(&v0::StorageCore::Stub::OpenObject, request, response);
      !s.ok()) {
    return s;
  }
  return ObjectDescriptor(conn_, response.handle(),
                          FromProto(response.metadata()), chunk_size_hint_);
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
  v0::WriteSpec spec;
  internal::ToProto(options, bucket, object, spec);
  return AppendableWriter(std::make_unique<internal::AppendableWriterImpl>(
      conn_, std::move(spec), chunk_size_hint_));
}

StatusOr<SidecarVersion> Client::GetSidecarVersion() {
  v0::GetVersionRequest request;
  v0::GetVersionResponse response;
  if (Status s =
          conn_->Unary(&v0::StorageCore::Stub::GetVersion, request, response);
      !s.ok()) {
    return s;
  }
  SidecarVersion v;
  v.sidecar_version = response.sidecar_version();
  v.storage_crate_version = response.storage_crate_version();
  v.pid = response.pid();
  v.bidi_enabled = response.bidi_enabled();
  v.synthetic = response.synthetic();
  return v;
}

StatusOr<SidecarStats> Client::GetSidecarStats() {
  v0::GetStatsRequest request;
  v0::GetStatsResponse response;
  if (Status s =
          conn_->Unary(&v0::StorageCore::Stub::GetStats, request, response);
      !s.ok()) {
    return s;
  }
  SidecarStats out;
  out.user_cpu_us = response.user_cpu_us();
  out.system_cpu_us = response.system_cpu_us();
  out.rss_bytes = response.rss_bytes();
  out.peak_rss_bytes = response.peak_rss_bytes();
  out.allocated_bytes = response.allocated_bytes();
  out.threads = response.threads();
  return out;
}

int Client::sidecar_pid() const { return conn_->pid(); }

std::optional<std::uint64_t> Client::core_thread_cpu_us() const {
  return conn_->CoreThreadCpuMicros();
}

}  // namespace gcs_core
