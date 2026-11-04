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

// Unit tests for `UdsStorageConnection` (`storage::Client`) and
// `UdsAsyncConnection` (`storage::AsyncClient`) against an in-memory fake
// `gcscore.v1.GcsCore` gRPC-over-UDS server, pinning input-matrix rows A1–A27.

#include "google/cloud/storage/async/client.h"
#include "google/cloud/storage/async/retry_policy.h"
#include "google/cloud/storage/async/writer_connection.h"
#include "google/cloud/storage/client.h"
#include "google/cloud/storage/download_options.h"
#include "google/cloud/storage/hashing_options.h"
#include "google/cloud/storage/internal/grpc/object_metadata_parser.h"
#include "google/cloud/storage/internal/hash_function.h"
#include "google/cloud/storage/options.h"
#include "google/cloud/storage/retry_policy.h"
#include "google/cloud/storage/upload_options.h"
#include "google/cloud/credentials.h"
#include "google/cloud/grpc_options.h"
#include "google/cloud/log.h"
#include "absl/strings/cord.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "gcs_core/storage_client.h"
#include "gcscore.grpc.pb.h"
#include "test_support.h"
#include <gmock/gmock.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>
#include <unistd.h>

namespace gcs_core {
namespace {

namespace pb = ::gcscore::v1;
namespace gcs = ::google::cloud::storage;
namespace gcs_internal = ::google::cloud::storage::internal;
using ::gcs_core::testing::IsOk;
using ::gcs_core::testing::StatusIs;
using ::google::cloud::Options;
using ::google::cloud::Status;
using ::google::cloud::StatusCode;
using ::google::cloud::StatusOr;
using ::testing::AllOf;
using ::testing::Contains;
using ::testing::ElementsAre;
using ::testing::Ge;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::Pair;
using ::testing::Property;
using ::testing::SizeIs;

MATCHER_P2(StatusIsWithMessage, code, substr,
           "status has expected code and message substring") {
  Status const s = ::gcs_core::testing::AsStatus(arg);
  if (s.code() == code && absl::StrContains(s.message(), substr)) {
    return true;
  }
  *result_listener << s;
  return false;
}

class CapturingLogBackend final : public google::cloud::LogBackend {
 public:
  void Process(google::cloud::LogRecord const& record) override {
    std::scoped_lock lk(mu_);
    messages_.emplace_back(record.severity, record.message);
  }
  void ProcessWithOwnership(google::cloud::LogRecord record) override {
    Process(record);
  }

  std::vector<std::pair<google::cloud::Severity, std::string>> Messages()
      const {
    std::scoped_lock lk(mu_);
    return messages_;
  }

 private:
  mutable std::mutex mu_;
  std::vector<std::pair<google::cloud::Severity, std::string>> messages_;
};

class ScopedLogCapture {
 public:
  ScopedLogCapture()
      : backend_(std::make_shared<CapturingLogBackend>()),
        prev_severity_(google::cloud::LogSink::Instance().minimum_severity()),
        id_(google::cloud::LogSink::Instance().AddBackend(backend_)) {
    google::cloud::LogSink::Instance().set_minimum_severity(
        google::cloud::Severity::GCP_LS_WARNING);
  }
  ~ScopedLogCapture() {
    google::cloud::LogSink::Instance().RemoveBackend(id_);
    google::cloud::LogSink::Instance().set_minimum_severity(prev_severity_);
  }

  std::vector<std::pair<google::cloud::Severity, std::string>> Messages()
      const {
    return backend_->Messages();
  }

 private:
  std::shared_ptr<CapturingLogBackend> backend_;
  google::cloud::Severity prev_severity_;
  google::cloud::LogSink::BackendId id_;
};

class FakeCore final : public pb::GcsCore::Service {
 public:
  grpc::Status Ping(grpc::ServerContext*, pb::PingRequest const* request,
                    pb::PingResponse* response) override {
    std::scoped_lock lk(mu);
    if (!ping_error.ok()) return ping_error;
    response->set_payload(request->payload());
    return grpc::Status::OK;
  }

  grpc::Status ReadObject(grpc::ServerContext*,
                          pb::ReadObjectRequest const* request,
                          grpc::ServerWriter<pb::ReadChunk>* writer) override {
    std::string full_data;
    {
      std::scoped_lock lk(mu);
      read_requests.push_back(*request);
      if (!error.ok()) return error;
      auto it = objects.find(request->object());
      if (it == objects.end()) {
        return {grpc::StatusCode::NOT_FOUND, "no such object"};
      }
      full_data = it->second;
    }

    auto const total_size = static_cast<std::int64_t>(full_data.size());
    std::int64_t start = 0;
    std::int64_t len = total_size;
    if (request->offset() < 0) {
      start = std::max<std::int64_t>(0, total_size + request->offset());
      len = total_size - start;
    } else {
      if (request->offset() > total_size ||
          (total_size > 0 && request->offset() == total_size)) {
        return {grpc::StatusCode::OUT_OF_RANGE, "offset past EOF"};
      }
      start = request->offset();
      len = request->length() < 0
                ? (total_size - start)
                : std::min<std::int64_t>(request->length(), total_size - start);
    }

    std::string const slice = full_data.substr(static_cast<std::size_t>(start),
                                               static_cast<std::size_t>(len));
    pb::ReadChunk first;
    first.mutable_meta()->set_size(total_size);
    first.mutable_meta()->set_generation(77);
    first.mutable_meta()->set_metageneration(3);
    first.mutable_meta()->set_content_type("text/plain");
    first.mutable_meta()->set_content_encoding("gzip");
    first.mutable_meta()->set_crc32c(0xdeadbeef);
    writer->Write(first);

    for (std::size_t offset = 0; offset < slice.size(); offset += 10) {
      pb::ReadChunk chunk;
      std::string const part = slice.substr(offset, 10);
      chunk.add_data(part.substr(0, 5));
      chunk.add_data(part.substr(std::min<std::size_t>(5, part.size())));
      writer->Write(chunk);
    }
    return grpc::Status::OK;
  }

  grpc::Status WriteObject(grpc::ServerContext* ctx,
                           grpc::ServerReader<pb::WriteRequest>* reader,
                           pb::WriteObjectResponse* response) override {
    pb::WriteRequest message;
    std::vector<pb::WriteRequest> messages;
    std::string data;
    while (reader->Read(&message)) {
      if (message.has_data()) data += message.data();
      messages.push_back(message);
    }
    std::scoped_lock lk(mu);
    if (ctx->IsCancelled()) {
      ++aborted_writes;
      cv.notify_all();
      return {grpc::StatusCode::CANCELLED, "client cancelled upload stream"};
    }
    writes.push_back(messages);
    if (!error.ok()) return error;
    if (messages.empty() || !messages.front().has_header()) {
      return {grpc::StatusCode::INVALID_ARGUMENT,
              "first message must be header"};
    }
    pb::WriteHeader const& hdr = messages.front().header();
    if (hdr.has_if_generation_match() && hdr.if_generation_match() == 0 &&
        objects.count(hdr.object()) > 0) {
      return {grpc::StatusCode::FAILED_PRECONDITION,
              "ifGenerationMatch=0 failed: object exists"};
    }
    objects[hdr.object()] = data;
    response->set_size(static_cast<std::int64_t>(data.size()));
    response->set_generation(99);
    response->set_crc32c(data.empty() ? 0 : 0x12345678);
    pb::Transport const used =
        override_transport_used.value_or(hdr.transport());
    response->set_transport_used(used);
    return grpc::Status::OK;
  }

  grpc::Status ListObjects(grpc::ServerContext*,
                           pb::ListObjectsRequest const* request,
                           pb::ListObjectsResponse* response) override {
    std::scoped_lock lk(mu);
    list_objects_requests.push_back(*request);
    if (!error.ok()) return error;
    if (request->page_token().empty()) {
      pb::ListedObject& a = *response->add_items();
      a.set_name("p/obj-1");
      a.set_size(12);
      a.set_generation(11);
      a.set_metageneration(2);
      a.set_content_type("application/octet-stream");
      a.set_storage_class("STANDARD");
      a.set_updated_unix_secs(1700000000);  // date -u -d @1700000000
      a.set_crc32c(0xabcdef01);
      response->add_prefixes("p/sub/");
      response->set_next_page_token("page-2");
    } else {
      pb::ListedObject& b = *response->add_items();
      b.set_name("p/obj-2");
      b.set_size(34);
      b.set_generation(12);
      b.set_metageneration(1);
      b.set_content_type("text/plain");
      b.set_storage_class("STANDARD");
      b.set_updated_unix_secs(1700000100);  // date -u -d @1700000100
    }
    return grpc::Status::OK;
  }

  grpc::Status ListBuckets(grpc::ServerContext*,
                           pb::ListBucketsRequest const* request,
                           pb::ListBucketsResponse* response) override {
    std::scoped_lock lk(mu);
    list_buckets_requests.push_back(*request);
    if (!error.ok()) return error;
    if (request->page_token().empty()) {
      pb::ListedBucket& b1 = *response->add_items();
      b1.set_name("b-one");
      b1.set_location("US-WEST1");
      b1.set_storage_class("STANDARD");
      b1.set_metageneration(4);
      b1.set_created_unix_secs(1600000000);  // date -u -d @1600000000
      response->set_next_page_token("bpage-2");
    } else {
      pb::ListedBucket& b2 = *response->add_items();
      b2.set_name("b-two");
      b2.set_location("US-WEST1");
      b2.set_storage_class("RAPID");
      b2.set_metageneration(1);
      b2.set_created_unix_secs(1600000200);  // date -u -d @1600000200
    }
    return grpc::Status::OK;
  }

  grpc::Status DeleteObject(grpc::ServerContext*,
                            pb::DeleteObjectRequest const* request,
                            pb::Empty*) override {
    std::scoped_lock lk(mu);
    delete_requests.push_back(*request);
    if (!error.ok()) return error;
    return objects.erase(request->object()) == 1
               ? grpc::Status::OK
               : grpc::Status(grpc::StatusCode::NOT_FOUND, "no such object");
  }

  grpc::Status OpenObject(grpc::ServerContext*,
                          pb::OpenObjectRequest const* request,
                          pb::OpenObjectResponse* response) override {
    std::scoped_lock lk(mu);
    open_requests.push_back(*request);
    if (!error.ok()) return error;
    auto it = objects.find(request->object());
    if (it == objects.end()) {
      return {grpc::StatusCode::NOT_FOUND, "no such object"};
    }
    response->set_handle(++next_handle);
    open_handles[response->handle()] = it->second;
    response->mutable_meta()->set_size(
        static_cast<std::int64_t>(it->second.size()));
    response->mutable_meta()->set_generation(55);
    response->mutable_meta()->set_metageneration(2);
    response->mutable_meta()->set_content_type("application/octet-stream");
    response->mutable_meta()->set_crc32c(0x11223344);
    return grpc::Status::OK;
  }

  grpc::Status ReadRange(grpc::ServerContext* ctx,
                         pb::ReadRangeRequest const* request,
                         grpc::ServerWriter<pb::ReadChunk>* writer) override {
    bool should_block = false;
    int barrier_target = 0;
    std::string data;
    {
      std::scoped_lock lk(mu);
      range_requests.push_back(*request);
      if (!error.ok()) return error;
      should_block = block_read_ranges;
      barrier_target = concurrent_range_barrier;
      if (should_block) {
        ++entered_blocked_reads;
        cv.notify_all();
      }
      if (barrier_target > 0) {
        ++entered_barrier_reads;
        if (entered_barrier_reads >= barrier_target) {
          barrier_released = true;
        }
        cv.notify_all();
      }
      auto it = open_handles.find(request->handle());
      if (it == open_handles.end()) {
        return {grpc::StatusCode::NOT_FOUND, "no such handle"};
      }
      data = it->second;
    }

    if (barrier_target > 0) {
      std::unique_lock<std::mutex> lk(mu);
      cv.wait_for(lk, std::chrono::seconds(2),
                  [&] { return barrier_released || ctx->IsCancelled(); });
    }

    if (should_block) {
      while (!ctx->IsCancelled()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
      }
      return {grpc::StatusCode::CANCELLED, "cancelled while blocked"};
    }

    auto const total_size = static_cast<std::int64_t>(data.size());
    std::int64_t start = 0;
    std::int64_t len = total_size;
    if (request->offset() < 0) {
      start = std::max<std::int64_t>(0, total_size + request->offset());
      len = total_size - start;
    } else {
      if (request->offset() > total_size) {
        return {grpc::StatusCode::OUT_OF_RANGE, "offset past EOF"};
      }
      start = request->offset();
      len = request->length() < 0
                ? (total_size - start)
                : std::min<std::int64_t>(request->length(), total_size - start);
    }
    std::string const range = data.substr(static_cast<std::size_t>(start),
                                          static_cast<std::size_t>(len));
    for (std::size_t i = 0; i < range.size(); i += 17) {
      pb::ReadChunk chunk;
      chunk.add_data(range.substr(i, 17));
      writer->Write(chunk);
    }
    return grpc::Status::OK;
  }

  grpc::Status CloseObject(grpc::ServerContext*,
                           pb::CloseObjectRequest const* request,
                           pb::Empty*) override {
    std::scoped_lock lk(mu);
    closed_handles.push_back(request->handle());
    open_handles.erase(request->handle());
    return grpc::Status::OK;
  }

  std::mutex mu;
  std::condition_variable cv;
  grpc::Status ping_error = grpc::Status::OK;
  grpc::Status error = grpc::Status::OK;
  std::optional<pb::Transport> override_transport_used;
  bool block_read_ranges = false;
  int entered_blocked_reads = 0;
  int concurrent_range_barrier = 0;
  int entered_barrier_reads = 0;
  bool barrier_released = false;
  int aborted_writes = 0;

  std::map<std::string, std::string> objects;
  std::uint64_t next_handle = 0;
  std::map<std::uint64_t, std::string> open_handles;
  std::vector<pb::ReadObjectRequest> read_requests;
  std::vector<std::vector<pb::WriteRequest>> writes;
  std::vector<pb::ListObjectsRequest> list_objects_requests;
  std::vector<pb::ListBucketsRequest> list_buckets_requests;
  std::vector<pb::DeleteObjectRequest> delete_requests;
  std::vector<pb::OpenObjectRequest> open_requests;
  std::vector<pb::ReadRangeRequest> range_requests;
  std::vector<std::uint64_t> closed_handles;
};

class AdapterFakeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    static std::atomic<int> seq{0};
    socket_ = "/tmp/gcs-uds-adapter-fake-" + std::to_string(::getpid()) + "-" +
              std::to_string(++seq) + ".sock";
    grpc::ServerBuilder builder;
    builder.AddListeningPort("unix:" + socket_,
                             grpc::InsecureServerCredentials());
    builder.RegisterService(&fake_);
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);
  }

  void TearDown() override {
    if (server_) server_->Shutdown();
    (void)std::remove(socket_.c_str());
  }

  ClientOptions CoreOpts(int channels = 4) const {
    ClientOptions opts = DefaultAdapterClientOptions();
    opts.socket_path = socket_;
    opts.channels = channels;
    return opts;
  }

  FakeCore fake_;
  std::string socket_;
  std::unique_ptr<grpc::Server> server_;
};

StatusOr<std::string> ReadAllAsync(
    std::pair<gcs::AsyncReader, gcs::AsyncToken> stream,
    std::vector<std::int64_t>* offsets = nullptr) {
  auto [reader, token] = std::move(stream);
  std::string out;
  while (token.valid()) {
    StatusOr<std::pair<gcs::ReadPayload, gcs::AsyncToken>> step =
        reader.Read(std::move(token)).get();
    if (!step) return std::move(step).status();
    if (offsets != nullptr) offsets->push_back(step->first.offset());
    absl::AppendCordToString(step->first.cord_contents(), &out);
    token = std::move(step->second);
  }
  return out;
}

/// @test Matrix row A1: `Client::ReadObject` streams whole object contents and
/// sets `DisableCrc32cChecksum` default (`ChecksumAlgorithm::kNone`).
TEST_F(AdapterFakeTest, ReadWhole) {
  std::string const want = "0123456789abcdefghijKLMNOP";
  fake_.objects["obj.txt"] = want;
  StatusOr<gcs::Client> client = MakeUdsStorageClient(CoreOpts());
  ASSERT_THAT(client, IsOk());

  gcs::ObjectReadStream stream = client->ReadObject("my-bucket", "obj.txt");
  std::string const got(std::istreambuf_iterator<char>(stream), {});
  ASSERT_THAT(stream.status(), IsOk());
  EXPECT_EQ(got, want);

  ASSERT_THAT(fake_.read_requests, SizeIs(1));
  pb::ReadObjectRequest const& req = fake_.read_requests.front();
  EXPECT_EQ(req.bucket(), "my-bucket");
  EXPECT_EQ(req.object(), "obj.txt");
  EXPECT_EQ(req.offset(), 0);
  EXPECT_EQ(req.length(), -1);
  EXPECT_EQ(req.transport(), pb::JSON);
}

/// @test Matrix row A1: `ObjectReadStream` exposes metadata and HTTP headers
/// from the first `ReadChunk.meta`.
TEST_F(AdapterFakeTest, ReadMetadataHeaders) {
  fake_.objects["meta.txt"] = "payload";
  StatusOr<gcs::Client> client = MakeUdsStorageClient(CoreOpts());
  ASSERT_THAT(client, IsOk());

  gcs::ObjectReadStream stream = client->ReadObject("my-bucket", "meta.txt");
  std::string const got(std::istreambuf_iterator<char>(stream), {});
  ASSERT_THAT(stream.status(), IsOk());
  EXPECT_EQ(got, "payload");
  EXPECT_EQ(stream.generation(), std::optional<std::int64_t>(77));
  EXPECT_EQ(stream.metageneration(), std::optional<std::int64_t>(3));
  EXPECT_EQ(stream.size(), std::optional<std::uint64_t>(7));
  std::string const want_crc =
      google::cloud::storage_internal::Crc32cFromProto(0xdeadbeef);
  EXPECT_THAT(stream.headers(),
              AllOf(Contains(Pair("content-type", "text/plain")),
                    Contains(Pair("content-encoding", "gzip")),
                    Contains(Pair("x-goog-hash", "crc32c=" + want_crc))));
}

/// @test Matrix row A2: `ReadRange`, `ReadFromOffset`, `ReadLast`, combined
/// `ReadRange`+`ReadFromOffset`, and invalid range bounds.
TEST_F(AdapterFakeTest, ReadRanges) {
  fake_.objects["alpha"] = "0123456789abcdef";
  StatusOr<gcs::Client> client = MakeUdsStorageClient(CoreOpts());
  ASSERT_THAT(client, IsOk());

  {
    gcs::ObjectReadStream s =
        client->ReadObject("bkt", "alpha", gcs::ReadRange(4, 10));
    std::string const got(std::istreambuf_iterator<char>(s), {});
    ASSERT_THAT(s.status(), IsOk());
    EXPECT_EQ(got, "456789");
    EXPECT_EQ(fake_.read_requests.back().offset(), 4);
    EXPECT_EQ(fake_.read_requests.back().length(), 6);
  }
  {
    gcs::ObjectReadStream s =
        client->ReadObject("bkt", "alpha", gcs::ReadFromOffset(11));
    std::string const got(std::istreambuf_iterator<char>(s), {});
    ASSERT_THAT(s.status(), IsOk());
    EXPECT_EQ(got, "bcdef");
    EXPECT_EQ(fake_.read_requests.back().offset(), 11);
    EXPECT_EQ(fake_.read_requests.back().length(), -1);
  }
  {
    gcs::ObjectReadStream s =
        client->ReadObject("bkt", "alpha", gcs::ReadLast(4));
    std::string const got(std::istreambuf_iterator<char>(s), {});
    ASSERT_THAT(s.status(), IsOk());
    EXPECT_EQ(got, "cdef");
    EXPECT_EQ(fake_.read_requests.back().offset(), -4);
    EXPECT_EQ(fake_.read_requests.back().length(), -1);
  }
  {
    gcs::ObjectReadStream s = client->ReadObject(
        "bkt", "alpha", gcs::ReadRange(2, 12), gcs::ReadFromOffset(6));
    std::string const got(std::istreambuf_iterator<char>(s), {});
    ASSERT_THAT(s.status(), IsOk());
    EXPECT_EQ(got, "6789ab");
    EXPECT_EQ(fake_.read_requests.back().offset(), 6);
    EXPECT_EQ(fake_.read_requests.back().length(), 6);
  }

  // Empty or inverted ranges must fail before hitting the server (where
  // `length == 0` means unbounded read in `google.storage.v2`).
  std::size_t const requests_before = fake_.read_requests.size();
  for (gcs::ReadRange const bad :
       {gcs::ReadRange(5, 5), gcs::ReadRange(8, 3), gcs::ReadRange(-1, 5)}) {
    gcs::ObjectReadStream s = client->ReadObject("bkt", "alpha", bad);
    (void)std::string(std::istreambuf_iterator<char>(s), {});
    EXPECT_THAT(s.status(), StatusIs(StatusCode::kInvalidArgument));
  }
  {
    gcs::ObjectReadStream s =
        client->ReadObject("bkt", "alpha", gcs::ReadLast(0));
    (void)std::string(std::istreambuf_iterator<char>(s), {});
    EXPECT_THAT(s.status(), StatusIs(StatusCode::kInvalidArgument));
  }
  {
    gcs::ObjectReadStream s = client->ReadObject(
        "bkt", "alpha", gcs::ReadLast(4), gcs::ReadFromOffset(1));
    (void)std::string(std::istreambuf_iterator<char>(s), {});
    EXPECT_THAT(s.status(), StatusIs(StatusCode::kInvalidArgument));
  }
  EXPECT_EQ(fake_.read_requests.size(), requests_before);
}

/// @test Matrix row A3: reading past EOF surfaces `kOutOfRange`.
TEST_F(AdapterFakeTest, ReadPastEof) {
  fake_.objects["short.bin"] = "abcd";
  StatusOr<gcs::Client> client = MakeUdsStorageClient(CoreOpts());
  ASSERT_THAT(client, IsOk());

  gcs::ObjectReadStream s =
      client->ReadObject("bkt", "short.bin", gcs::ReadFromOffset(100));
  (void)std::string(std::istreambuf_iterator<char>(s), {});
  EXPECT_THAT(s.status(), StatusIs(StatusCode::kOutOfRange));
}

/// @test Matrix row A3: reading a zero-byte object returns empty body and
/// preserves metadata.
TEST_F(AdapterFakeTest, ZeroByteObject) {
  fake_.objects["empty.bin"] = "";
  StatusOr<gcs::Client> client = MakeUdsStorageClient(CoreOpts());
  ASSERT_THAT(client, IsOk());

  gcs::ObjectReadStream s = client->ReadObject("bkt", "empty.bin");
  std::string const got(std::istreambuf_iterator<char>(s), {});
  ASSERT_THAT(s.status(), IsOk());
  EXPECT_THAT(got, IsEmpty());
  EXPECT_EQ(s.size(), std::optional<std::uint64_t>(0));
  EXPECT_EQ(s.generation(), std::optional<std::int64_t>(77));
}

/// @test Matrix row A4: `Generation` and `IfGenerationMatch` presence on reads
/// (unset leaves `has_if_generation_match()` false; `0` sets it explicitly).
TEST_F(AdapterFakeTest, ReadPreconditions) {
  fake_.objects["p.bin"] = "xyz";
  StatusOr<gcs::Client> client = MakeUdsStorageClient(CoreOpts());
  ASSERT_THAT(client, IsOk());

  {
    gcs::ObjectReadStream s = client->ReadObject("bkt", "p.bin");
    (void)std::string(std::istreambuf_iterator<char>(s), {});
    ASSERT_THAT(s.status(), IsOk());
    EXPECT_EQ(fake_.read_requests.back().generation(), 0);
    EXPECT_FALSE(fake_.read_requests.back().has_if_generation_match());
  }
  {
    gcs::ObjectReadStream s = client->ReadObject(
        "bkt", "p.bin", gcs::Generation(12), gcs::IfGenerationMatch(0));
    (void)std::string(std::istreambuf_iterator<char>(s), {});
    ASSERT_THAT(s.status(), IsOk());
    EXPECT_EQ(fake_.read_requests.back().generation(), 12);
    EXPECT_TRUE(fake_.read_requests.back().has_if_generation_match());
    EXPECT_EQ(fake_.read_requests.back().if_generation_match(), 0);
  }
  {
    gcs::ObjectReadStream s =
        client->ReadObject("bkt", "p.bin", gcs::IfGenerationNotMatch(1));
    (void)std::string(std::istreambuf_iterator<char>(s), {});
    EXPECT_THAT(s.status(), StatusIs(StatusCode::kUnimplemented));
  }
}

/// @test Matrix row A5: `Client::InsertObject` sends `known_size = size` over
/// `WriteObject` with `JSON` transport.
TEST_F(AdapterFakeTest, InsertObject) {
  StatusOr<gcs::Client> client = MakeUdsStorageClient(CoreOpts());
  ASSERT_THAT(client, IsOk());

  std::string const body = "single-shot-payload";
  StatusOr<gcs::ObjectMetadata> meta =
      client->InsertObject("my-bucket", "ins.bin", body);
  ASSERT_THAT(meta, IsOk());
  EXPECT_EQ(meta->bucket(), "my-bucket");
  EXPECT_EQ(meta->name(), "ins.bin");
  EXPECT_EQ(meta->size(), body.size());
  EXPECT_EQ(meta->generation(), 99);
  EXPECT_EQ(meta->crc32c(),
            google::cloud::storage_internal::Crc32cFromProto(0x12345678));

  ASSERT_THAT(fake_.writes, SizeIs(1));
  pb::WriteHeader const& hdr = fake_.writes.front().front().header();
  EXPECT_TRUE(hdr.has_known_size());
  EXPECT_EQ(hdr.known_size(), static_cast<std::int64_t>(body.size()));
  EXPECT_EQ(hdr.transport(), pb::JSON);
  EXPECT_EQ(fake_.objects["ins.bin"], body);
}

/// @test Matrix row A6: `Client::WriteObject` (< 8 MiB) leaves `known_size`
/// unset, forwards `UploadBufferSizeOption` in `chunk_size`, and finalizes on
/// `Close()`.
TEST_F(AdapterFakeTest, WriteSmall) {
  StatusOr<gcs::Client> client = MakeUdsStorageClient(CoreOpts());
  ASSERT_THAT(client, IsOk());

  std::string const body(64 * 1024, 's');
  gcs::ObjectWriteStream stream = client->WriteObject("my-bucket", "small.bin");
  stream.write(body.data(), static_cast<std::streamsize>(body.size()));
  stream.Close();
  ASSERT_THAT(stream.metadata(), IsOk());
  EXPECT_EQ(stream.metadata()->size(), body.size());

  ASSERT_THAT(fake_.writes, SizeIs(1));
  pb::WriteHeader const& hdr = fake_.writes.front().front().header();
  EXPECT_FALSE(hdr.has_known_size());
  EXPECT_EQ(hdr.chunk_size(), 8U * 1024 * 1024);
  EXPECT_EQ(hdr.transport(), pb::JSON);
  EXPECT_EQ(fake_.objects["small.bin"], body);
}

/// @test Matrix row A7: multi-chunk `Client::WriteObject` streams across
/// `UploadBufferSizeOption` boundaries and reassembles cleanly.
TEST_F(AdapterFakeTest, WriteLarge) {
  StatusOr<gcs::Client> client = MakeUdsStorageClient(
      CoreOpts(), Options{}.set<gcs::UploadBufferSizeOption>(256 * 1024));
  ASSERT_THAT(client, IsOk());

  std::string body(600 * 1024, '\0');
  for (std::size_t i = 0; i < body.size(); ++i) {
    body[i] = static_cast<char>('a' + (i % 26));
  }
  gcs::ObjectWriteStream stream = client->WriteObject("my-bucket", "multi.bin");
  stream.write(body.data(), static_cast<std::streamsize>(body.size()));
  stream.Close();
  ASSERT_THAT(stream.metadata(), IsOk());
  EXPECT_EQ(stream.metadata()->size(), body.size());
  EXPECT_EQ(fake_.objects["multi.bin"], body);
  ASSERT_THAT(fake_.writes, SizeIs(1));
  EXPECT_EQ(fake_.writes.front().front().header().chunk_size(), 256U * 1024);
}

/// @test Matrix row A7: non-final `UploadChunk` returns `committed_size = bytes
/// accepted` with `payload == nullopt`; final `UploadChunk` returns metadata.
TEST_F(AdapterFakeTest, CommittedSizeSemantics) {
  StatusOr<std::shared_ptr<gcs_internal::StorageConnection>> conn =
      MakeUdsStorageConnection(CoreOpts());
  ASSERT_THAT(conn, IsOk());

  gcs_internal::ResumableUploadRequest create_req("bkt", "chunks.bin");
  StatusOr<gcs_internal::CreateResumableUploadResponse> created =
      (*conn)->CreateResumableUpload(create_req);
  ASSERT_THAT(created, IsOk());

  std::string const chunk1(256 * 1024, 'x');
  gcs_internal::UploadChunkRequest req1(
      created->upload_id, 0,
      gcs_internal::ConstBufferSequence{
          gcs_internal::ConstBuffer(chunk1.data(), chunk1.size())},
      gcs_internal::CreateNullHashFunction());
  StatusOr<gcs_internal::QueryResumableUploadResponse> r1 =
      (*conn)->UploadChunk(req1);
  ASSERT_THAT(r1, IsOk());
  EXPECT_EQ(r1->committed_size, std::optional<std::uint64_t>(chunk1.size()));
  EXPECT_FALSE(r1->payload.has_value());

  std::string const chunk2 = "tail";
  gcs_internal::UploadChunkRequest req2(
      created->upload_id, chunk1.size(),
      gcs_internal::ConstBufferSequence{
          gcs_internal::ConstBuffer(chunk2.data(), chunk2.size())},
      gcs_internal::CreateNullHashFunction(), gcs_internal::HashValues{});
  StatusOr<gcs_internal::QueryResumableUploadResponse> r2 =
      (*conn)->UploadChunk(req2);
  ASSERT_THAT(r2, IsOk());
  ASSERT_TRUE(r2->payload.has_value());
  EXPECT_EQ(r2->payload->size(), chunk1.size() + chunk2.size());
}

/// @test Matrix row A8: `UploadContentLength(n)` sets `WriteHeader.known_size`.
TEST_F(AdapterFakeTest, KnownSize) {
  StatusOr<gcs::Client> client = MakeUdsStorageClient(CoreOpts());
  ASSERT_THAT(client, IsOk());

  std::string const body = "exact-length-body";
  gcs::ObjectWriteStream stream = client->WriteObject(
      "bkt", "known.bin", gcs::UploadContentLength(body.size()));
  stream.write(body.data(), static_cast<std::streamsize>(body.size()));
  stream.Close();
  ASSERT_THAT(stream.metadata(), IsOk());
  ASSERT_THAT(fake_.writes, SizeIs(1));
  pb::WriteHeader const& hdr = fake_.writes.front().front().header();
  EXPECT_TRUE(hdr.has_known_size());
  EXPECT_EQ(hdr.known_size(), static_cast<std::int64_t>(body.size()));
}

/// @test Matrix row A9: `IfGenerationMatch(0)` sets `if_generation_match = 0`
/// and fails with `kFailedPrecondition` when the object already exists.
TEST_F(AdapterFakeTest, WritePrecondition) {
  fake_.objects["exists.bin"] = "v1";
  StatusOr<gcs::Client> client = MakeUdsStorageClient(CoreOpts());
  ASSERT_THAT(client, IsOk());

  gcs::ObjectWriteStream stream =
      client->WriteObject("bkt", "exists.bin", gcs::IfGenerationMatch(0));
  stream << "v2";
  stream.Close();
  EXPECT_THAT(stream.metadata(), StatusIs(StatusCode::kFailedPrecondition));
  ASSERT_THAT(fake_.writes, SizeIs(1));
  pb::WriteHeader const& hdr = fake_.writes.front().front().header();
  EXPECT_TRUE(hdr.has_if_generation_match());
  EXPECT_EQ(hdr.if_generation_match(), 0);
  EXPECT_EQ(fake_.objects["exists.bin"], "v1");
}

/// @test Matrix row A10: resuming a prior upload session (`Restore...` /
/// `QueryResumableUpload`) returns `kUnimplemented`.
TEST_F(AdapterFakeTest, ResumeUnsupported) {
  StatusOr<std::shared_ptr<gcs_internal::StorageConnection>> conn =
      MakeUdsStorageConnection(CoreOpts());
  ASSERT_THAT(conn, IsOk());

  EXPECT_THAT((*conn)->QueryResumableUpload(
                  gcs_internal::QueryResumableUploadRequest("prior-id")),
              StatusIs(StatusCode::kUnimplemented));

  gcs_internal::ResumableUploadRequest req("bkt", "obj");
  req.set_option(gcs::UseResumableUploadSession("prior-id"));
  EXPECT_THAT((*conn)->CreateResumableUpload(req),
              StatusIs(StatusCode::kUnimplemented));

  gcs::Client client =
      gcs_internal::ClientImplDetails::CreateWithoutDecorations(*conn);
  gcs::ObjectWriteStream stream = client.WriteObject(
      "bkt", "obj", gcs::RestoreResumableUploadSession("prior-id"));
  EXPECT_THAT(stream.last_status(), StatusIs(StatusCode::kUnimplemented));
}

/// @test Matrix row A11: `ContentType` and `WithObjectMetadata` populate
/// `WriteHeader.content_type`.
TEST_F(AdapterFakeTest, ContentType) {
  StatusOr<gcs::Client> client = MakeUdsStorageClient(CoreOpts());
  ASSERT_THAT(client, IsOk());

  ASSERT_THAT(client->InsertObject("bkt", "a.json", "{}",
                                   gcs::ContentType("application/json")),
              IsOk());
  EXPECT_EQ(fake_.writes.back().front().header().content_type(),
            "application/json");

  gcs::ObjectWriteStream s = client->WriteObject(
      "bkt", "b.csv",
      gcs::WithObjectMetadata(
          gcs::ObjectMetadata().set_content_type("text/csv")));
  s << "a,b\n";
  s.Close();
  ASSERT_THAT(s.metadata(), IsOk());
  EXPECT_EQ(s.metadata()->content_type(), "text/csv");
  EXPECT_EQ(fake_.writes.back().front().header().content_type(), "text/csv");
}

/// @test Matrix row A12: an upload cancelled via `AutoFinalizeDisabled()` +
/// `Client::DeleteResumableUpload` on a live `Client`, or via connection
/// destruction after `Suspend()`, cancels the UDS stream and never commits an
/// object.
TEST_F(AdapterFakeTest, AbandonedUpload) {
  StatusOr<gcs::Client> live_client = MakeUdsStorageClient(
      CoreOpts(), Options{}.set<gcs::UploadBufferSizeOption>(256 * 1024));
  ASSERT_THAT(live_client, IsOk());
  std::string session_id;
  {
    gcs::ObjectWriteStream stream = live_client->WriteObject(
        "bkt", "abandoned-1", gcs::AutoFinalizeDisabled());
    session_id = stream.resumable_session_id();
    ASSERT_FALSE(session_id.empty());
    std::string const part(300 * 1024, 'p');
    stream.write(part.data(), static_cast<std::streamsize>(part.size()));
    // Destroying `stream` with `AutoFinalizeDisabled()` leaves the upload
    // unfinalized until `DeleteResumableUpload` cancels it on `live_client`.
  }
  ASSERT_THAT(live_client->DeleteResumableUpload(session_id), IsOk());
  {
    std::unique_lock<std::mutex> lk(fake_.mu);
    ASSERT_TRUE(fake_.cv.wait_for(lk, std::chrono::seconds(2),
                                  [&] { return fake_.aborted_writes >= 1; }));
  }

  {
    StatusOr<gcs::Client> temp_client = MakeUdsStorageClient(
        CoreOpts(), Options{}.set<gcs::UploadBufferSizeOption>(256 * 1024));
    ASSERT_THAT(temp_client, IsOk());
    gcs::ObjectWriteStream stream =
        temp_client->WriteObject("bkt", "abandoned-2");
    std::string const part(300 * 1024, 'q');
    stream.write(part.data(), static_cast<std::streamsize>(part.size()));
    std::move(stream).Suspend();
    // Destroy `temp_client` (and its `UdsStorageConnection`) without `Close()`.
  }

  std::unique_lock<std::mutex> lk(fake_.mu);
  ASSERT_TRUE(fake_.cv.wait_for(lk, std::chrono::seconds(2),
                                [&] { return fake_.aborted_writes >= 2; }));
  EXPECT_EQ(fake_.objects.count("abandoned-1"), 0U);
  EXPECT_EQ(fake_.objects.count("abandoned-2"), 0U);
}

/// @test Matrix row A13: `Client::ListObjects` iterates pages and forwards
/// filter options.
TEST_F(AdapterFakeTest, ListObjectsPages) {
  StatusOr<gcs::Client> client = MakeUdsStorageClient(CoreOpts());
  ASSERT_THAT(client, IsOk());

  std::vector<gcs::ObjectMetadata> items;
  for (StatusOr<gcs::ObjectMetadata> const& item :
       client->ListObjects("my-bucket", gcs::Prefix("p/"), gcs::Delimiter("/"),
                           gcs::MaxResults(10), gcs::StartOffset("p/a"),
                           gcs::EndOffset("p/z"), gcs::Versions(true))) {
    ASSERT_THAT(item, IsOk());
    items.push_back(*item);
  }

  ASSERT_THAT(items, SizeIs(2));
  EXPECT_THAT(items,
              ElementsAre(Property(&gcs::ObjectMetadata::name, "p/obj-1"),
                          Property(&gcs::ObjectMetadata::name, "p/obj-2")));
  ASSERT_THAT(fake_.list_objects_requests, SizeIs(2));
  pb::ListObjectsRequest const& r0 = fake_.list_objects_requests[0];
  EXPECT_EQ(r0.bucket(), "my-bucket");
  EXPECT_EQ(r0.prefix(), "p/");
  EXPECT_EQ(r0.delimiter(), "/");
  EXPECT_EQ(r0.page_size(), 10);
  EXPECT_EQ(r0.start_offset(), "p/a");
  EXPECT_EQ(r0.end_offset(), "p/z");
  EXPECT_TRUE(r0.versions());
  EXPECT_THAT(r0.page_token(), IsEmpty());
  EXPECT_EQ(fake_.list_objects_requests[1].page_token(), "page-2");
}

/// @test Matrix row A13: `ListedObject` maps its subset of fields to
/// `storage::ObjectMetadata`.
TEST_F(AdapterFakeTest, ListObjectsFieldsSubset) {
  StatusOr<gcs::Client> client = MakeUdsStorageClient(CoreOpts());
  ASSERT_THAT(client, IsOk());

  auto reader = client->ListObjects("my-bucket");
  auto it = reader.begin();
  ASSERT_NE(it, reader.end());
  ASSERT_THAT(*it, IsOk());
  gcs::ObjectMetadata const& m = **it;
  EXPECT_EQ(m.bucket(), "my-bucket");
  EXPECT_EQ(m.name(), "p/obj-1");
  EXPECT_EQ(m.size(), 12U);
  EXPECT_EQ(m.generation(), 11);
  EXPECT_EQ(m.metageneration(), 2);
  EXPECT_EQ(m.content_type(), "application/octet-stream");
  EXPECT_EQ(m.storage_class(), "STANDARD");
  EXPECT_EQ(m.updated(), std::chrono::system_clock::from_time_t(1700000000));
  EXPECT_EQ(m.crc32c(),
            google::cloud::storage_internal::Crc32cFromProto(0xabcdef01));
}

/// @test Matrix row A14: `Client::ListBucketsForProject` and `ListBuckets`
/// iterate pages and map `BucketMetadata`.
TEST_F(AdapterFakeTest, ListBucketsPages) {
  StatusOr<gcs::Client> client = MakeUdsStorageClient(
      CoreOpts(), Options{}.set<gcs::ProjectIdOption>("proj-opt"));
  ASSERT_THAT(client, IsOk());

  std::vector<gcs::BucketMetadata> buckets;
  for (StatusOr<gcs::BucketMetadata> const& b : client->ListBucketsForProject(
           "proj-explicit", gcs::Prefix("b-"), gcs::MaxResults(25))) {
    ASSERT_THAT(b, IsOk());
    buckets.push_back(*b);
  }
  ASSERT_THAT(buckets, SizeIs(2));
  EXPECT_EQ(buckets[0].name(), "b-one");
  EXPECT_EQ(buckets[0].location(), "US-WEST1");
  EXPECT_EQ(buckets[0].storage_class(), "STANDARD");
  EXPECT_EQ(buckets[0].metageneration(), 4);
  EXPECT_EQ(buckets[1].name(), "b-two");
  EXPECT_EQ(buckets[1].storage_class(), "RAPID");

  ASSERT_THAT(fake_.list_buckets_requests, SizeIs(2));
  EXPECT_EQ(fake_.list_buckets_requests[0].project(), "proj-explicit");
  EXPECT_EQ(fake_.list_buckets_requests[0].prefix(), "b-");
  EXPECT_EQ(fake_.list_buckets_requests[0].page_size(), 25);
  EXPECT_EQ(fake_.list_buckets_requests[1].page_token(), "bpage-2");

  // `Client::ListBuckets()` picks up `ProjectIdOption`.
  auto default_reader = client->ListBuckets();
  ASSERT_THAT(*default_reader.begin(), IsOk());
  EXPECT_EQ(fake_.list_buckets_requests[2].project(), "proj-opt");
}

/// @test Patch P3: `Client::DeleteObject` forwards generation and precondition
/// over UDS.
TEST_F(AdapterFakeTest, DeleteObject) {
  fake_.objects["victim"] = "bye";
  StatusOr<gcs::Client> client = MakeUdsStorageClient(CoreOpts());
  ASSERT_THAT(client, IsOk());

  ASSERT_THAT(client->DeleteObject("bkt", "victim", gcs::Generation(7),
                                   gcs::IfGenerationMatch(7)),
              IsOk());
  ASSERT_THAT(fake_.delete_requests, SizeIs(1));
  EXPECT_EQ(fake_.delete_requests[0].bucket(), "bkt");
  EXPECT_EQ(fake_.delete_requests[0].object(), "victim");
  EXPECT_EQ(fake_.delete_requests[0].generation(), 7);
  EXPECT_TRUE(fake_.delete_requests[0].has_if_generation_match());
  EXPECT_EQ(fake_.delete_requests[0].if_generation_match(), 7);

  EXPECT_THAT(client->DeleteObject("bkt", "victim"),
              StatusIs(StatusCode::kNotFound));
}

/// @test Matrix row A15: unimplemented `StorageConnection` methods fail with
/// `kUnimplemented` and name the method.
TEST_F(AdapterFakeTest, UnimplementedMethods) {
  StatusOr<gcs::Client> client = MakeUdsStorageClient(CoreOpts());
  ASSERT_THAT(client, IsOk());

  struct Case {
    std::string_view method;
    Status status;
  };
  std::vector<Case> const cases = {
      {"GetObjectMetadata", client->GetObjectMetadata("b", "o").status()},
      {"CopyObject", client->CopyObject("b", "s", "b", "d").status()},
      {"PatchObject",
       client->PatchObject("b", "o", gcs::ObjectMetadataPatchBuilder{})
           .status()},
      {"UpdateObject",
       client->UpdateObject("b", "o", gcs::ObjectMetadata{}).status()},
      {"ComposeObject",
       client->ComposeObject("b", {gcs::ComposeSourceObject{"s", {}, {}}}, "d")
           .status()},
      {"RewriteObject",
       client->RewriteObjectBlocking("b", "s", "b", "d").status()},
      {"MoveObject", client->MoveObject("b", "s", "d").status()},
      {"RestoreObject", client->RestoreObject("b", "o", 1).status()},
      {"CreateBucket",
       client->CreateBucketForProject("b", "p", gcs::BucketMetadata{})
           .status()},
      {"GetBucketMetadata", client->GetBucketMetadata("b").status()},
      {"DeleteBucket", client->DeleteBucket("b")},
      {"UpdateBucket",
       client->UpdateBucket("b", gcs::BucketMetadata{}).status()},
      {"PatchBucket",
       client->PatchBucket("b", gcs::BucketMetadataPatchBuilder{}).status()},
      {"ListBucketAcl", client->ListBucketAcl("b").status()},
      {"ListObjectAcl", client->ListObjectAcl("b", "o").status()},
      {"ListDefaultObjectAcl", client->ListDefaultObjectAcl("b").status()},
      {"GetServiceAccount", client->GetServiceAccountForProject("p").status()},
      {"CreateNotification",
       client->CreateNotification("b", "topic", gcs::NotificationMetadata{})
           .status()},
  };

  for (Case const& c : cases) {
    EXPECT_THAT(c.status,
                StatusIsWithMessage(StatusCode::kUnimplemented, c.method))
        << "method=" << c.method;
  }
}

/// @test Matrix row A16: `AsyncClient::Open` returns an `ObjectDescriptor`
/// populated with `projects/_/buckets/<bucket>` v2 metadata.
TEST_F(AdapterFakeTest, AsyncOpen) {
  fake_.objects["open.bin"] = "0123456789";
  StatusOr<gcs::AsyncClient> client = MakeUdsAsyncClient(CoreOpts());
  ASSERT_THAT(client, IsOk());

  StatusOr<gcs::ObjectDescriptor> desc =
      client->Open(gcs::BucketName("my-bucket"), "open.bin").get();
  ASSERT_THAT(desc, IsOk());
  ASSERT_TRUE(desc->metadata().has_value());
  EXPECT_EQ(desc->metadata()->bucket(), "projects/_/buckets/my-bucket");
  EXPECT_EQ(desc->metadata()->name(), "open.bin");
  EXPECT_EQ(desc->metadata()->size(), 10);
  EXPECT_EQ(desc->metadata()->generation(), 55);
  EXPECT_EQ(desc->metadata()->metageneration(), 2);
  EXPECT_EQ(desc->metadata()->checksums().crc32c(), 0x11223344U);
  ASSERT_THAT(fake_.open_requests, SizeIs(1));
  EXPECT_EQ(fake_.open_requests[0].bucket(), "my-bucket");
}

/// @test Matrix row A17: 50 concurrent `ObjectDescriptor::Read` calls fan out
/// over the 4 UDS channels and return exact ranges and offsets.
TEST_F(AdapterFakeTest, AsyncConcurrentRanges) {
  // Spin up a server capped at 1 concurrent HTTP/2 stream per UDS connection,
  // with a 4-stream barrier in `ReadRange`: the barrier can only release if
  // `UdsObjectDescriptorConnection::Read` dispatches across 4 distinct UDS
  // connections.
  std::string const capped_sock =
      "/tmp/gcs-uds-adapter-capped-" + std::to_string(::getpid()) + ".sock";
  FakeCore capped_fake;
  std::string body(200 * 1024, '\0');
  for (std::size_t i = 0; i < body.size(); ++i) {
    body[i] = static_cast<char>('A' + ((i / 64) % 26));
  }
  capped_fake.objects["big.bin"] = body;
  capped_fake.concurrent_range_barrier = 4;

  grpc::ServerBuilder builder;
  builder.AddChannelArgument(GRPC_ARG_MAX_CONCURRENT_STREAMS, 1);
  builder.AddListeningPort("unix:" + capped_sock,
                           grpc::InsecureServerCredentials());
  builder.RegisterService(&capped_fake);
  std::unique_ptr<grpc::Server> capped_server = builder.BuildAndStart();
  ASSERT_NE(capped_server, nullptr);

  ClientOptions opts = DefaultAdapterClientOptions();
  opts.socket_path = capped_sock;
  opts.channels = 4;
  StatusOr<gcs::AsyncClient> client = MakeUdsAsyncClient(opts);
  ASSERT_THAT(client, IsOk());

  StatusOr<gcs::ObjectDescriptor> desc =
      client->Open(gcs::BucketName("my-bucket"), "big.bin").get();
  ASSERT_THAT(desc, IsOk());

  constexpr int kNumReads = 50;
  std::vector<std::thread> workers;
  std::vector<StatusOr<std::string>> results(kNumReads);
  workers.reserve(kNumReads);
  for (int i = 0; i < kNumReads; ++i) {
    auto stream = desc->Read(static_cast<std::int64_t>(i) * 4096, 2048);
    workers.emplace_back([&, i, s = std::move(stream)]() mutable {
      results[static_cast<std::size_t>(i)] = ReadAllAsync(std::move(s));
    });
  }
  for (std::thread& t : workers) t.join();

  for (int i = 0; i < kNumReads; ++i) {
    auto const idx = static_cast<std::size_t>(i);
    ASSERT_THAT(results[idx], IsOk());
    EXPECT_EQ(*results[idx], body.substr(idx * 4096, 2048));
  }
  {
    std::scoped_lock lk(capped_fake.mu);
    EXPECT_TRUE(capped_fake.barrier_released);
  }
  capped_fake.concurrent_range_barrier = 0;

  // Verify `ReadFromOffset` and `ReadLast` on `ObjectDescriptor`.
  StatusOr<std::string> tail =
      ReadAllAsync(desc->ReadFromOffset((200 * 1024) - 100));
  ASSERT_THAT(tail, IsOk());
  EXPECT_EQ(*tail, body.substr(body.size() - 100));

  std::vector<std::int64_t> last_offsets;
  StatusOr<std::string> suffix =
      ReadAllAsync(desc->ReadLast(64), &last_offsets);
  ASSERT_THAT(suffix, IsOk());
  EXPECT_EQ(*suffix, body.substr(body.size() - 64));
  ASSERT_THAT(last_offsets,
              Contains(static_cast<std::int64_t>(body.size() - 64)));

  desc = gcs::ObjectDescriptor{};
  client = Status(StatusCode::kCancelled, "done");
  capped_server->Shutdown();
  (void)std::remove(capped_sock.c_str());
}

/// @test Matrix row A18: destroying `ObjectDescriptor` while a `ReadRange`
/// call is in flight sends `CloseObject` and resolves pending reads with
/// `kCancelled`.
TEST_F(AdapterFakeTest, AsyncCloseWithPendingReads) {
  fake_.objects["hang.bin"] = std::string(1024, 'z');
  fake_.block_read_ranges = true;
  StatusOr<gcs::AsyncClient> client = MakeUdsAsyncClient(CoreOpts());
  ASSERT_THAT(client, IsOk());

  StatusOr<gcs::ObjectDescriptor> desc =
      client->Open(gcs::BucketName("my-bucket"), "hang.bin").get();
  ASSERT_THAT(desc, IsOk());

  auto [reader, token] = desc->Read(0, 128);
  google::cloud::future<StatusOr<std::pair<gcs::ReadPayload, gcs::AsyncToken>>>
      pending = reader.Read(std::move(token));

  {
    std::unique_lock<std::mutex> lk(fake_.mu);
    ASSERT_TRUE(fake_.cv.wait_for(lk, std::chrono::seconds(2), [&] {
      return fake_.entered_blocked_reads >= 1;
    }));
  }

  // Resetting `desc` drops the last strong reference to
  // `UdsObjectDescriptorConnection`, cancelling active readers and calling
  // `CloseObject`.
  desc = gcs::ObjectDescriptor{};

  StatusOr<std::pair<gcs::ReadPayload, gcs::AsyncToken>> out = pending.get();
  EXPECT_THAT(out, StatusIs(StatusCode::kCancelled));
  std::scoped_lock lk(fake_.mu);
  EXPECT_THAT(fake_.closed_handles, ElementsAre(1U));
}

/// @test Matrix row A19: `Open` passes through `kPermissionDenied` unchanged.
TEST_F(AdapterFakeTest, AsyncOpenPermissionDenied) {
  fake_.error = grpc::Status(
      grpc::StatusCode::PERMISSION_DENIED,
      "BidiReadObject is not yet available for general use, or for project");
  StatusOr<gcs::AsyncClient> client = MakeUdsAsyncClient(CoreOpts());
  ASSERT_THAT(client, IsOk());

  StatusOr<gcs::ObjectDescriptor> desc =
      client->Open(gcs::BucketName("reg-bkt"), "obj").get();
  EXPECT_THAT(desc, StatusIsWithMessage(StatusCode::kPermissionDenied,
                                        "not yet available"));
}

/// @test Matrix row A20 + P3: `StartAppendableObjectUpload` streams data with
/// `transport = GRPC` and finalizes to `google::storage::v2::Object`.
TEST_F(AdapterFakeTest, AsyncAppendableWriteFinalize) {
  StatusOr<gcs::AsyncClient> client = MakeUdsAsyncClient(CoreOpts());
  ASSERT_THAT(client, IsOk());

  StatusOr<std::pair<gcs::AsyncWriter, gcs::AsyncToken>> started =
      client
          ->StartAppendableObjectUpload(gcs::BucketName("zonal-bkt"), "app.bin")
          .get();
  ASSERT_THAT(started, IsOk());
  auto [writer, token] = *std::move(started);

  for (std::string_view part : {"chunk-1|", "chunk-2|"}) {
    StatusOr<gcs::AsyncToken> next =
        writer.Write(std::move(token), gcs::WritePayload(std::string(part)))
            .get();
    ASSERT_THAT(next, IsOk());
    token = *std::move(next);
  }
  StatusOr<google::storage::v2::Object> obj =
      writer.Finalize(std::move(token), gcs::WritePayload("chunk-3")).get();
  ASSERT_THAT(obj, IsOk());
  EXPECT_EQ(obj->bucket(), "projects/_/buckets/zonal-bkt");
  EXPECT_EQ(obj->name(), "app.bin");
  EXPECT_EQ(obj->size(), 23);
  EXPECT_EQ(obj->generation(), 99);
  EXPECT_THAT(writer.GetRequestMetadata().headers,
              Contains(Pair("x-uds-transport-used", "GRPC")));

  ASSERT_THAT(fake_.writes, SizeIs(1));
  pb::WriteHeader const& hdr = fake_.writes.front().front().header();
  EXPECT_EQ(hdr.bucket(), "zonal-bkt");
  EXPECT_EQ(hdr.transport(), pb::GRPC);
  EXPECT_EQ(fake_.objects["app.bin"], "chunk-1|chunk-2|chunk-3");

  ASSERT_THAT(
      client->DeleteObject(gcs::BucketName("zonal-bkt"), "app.bin").get(),
      IsOk());
  EXPECT_EQ(fake_.objects.count("app.bin"), 0U);
}

/// @test Matrix row A21: `Flush` and `Query` report bytes accepted by the UDS
/// server, and `Close()` without `Finalize()` returns `kUnimplemented`.
TEST_F(AdapterFakeTest, AsyncFlushQuerySemantics) {
  StatusOr<std::shared_ptr<gcs::AsyncConnection>> conn =
      MakeUdsAsyncConnection(CoreOpts());
  ASSERT_THAT(conn, IsOk());
  gcs::AsyncClient client(*conn);

  StatusOr<std::pair<gcs::AsyncWriter, gcs::AsyncToken>> started =
      client.StartAppendableObjectUpload(gcs::BucketName("zonal"), "f.bin")
          .get();
  ASSERT_THAT(started, IsOk());
  auto [writer, token] = *std::move(started);

  StatusOr<gcs::AsyncToken> next =
      writer.Write(std::move(token), gcs::WritePayload("12345")).get();
  ASSERT_THAT(next, IsOk());
  ASSERT_THAT(writer.Flush().get(), IsOk());
  EXPECT_EQ(std::get<std::int64_t>(writer.PersistedState()), 5);

  StatusOr<google::storage::v2::Object> fin =
      writer.Finalize(*std::move(next)).get();
  ASSERT_THAT(fin, IsOk());
  EXPECT_TRUE(std::holds_alternative<google::storage::v2::Object>(
      writer.PersistedState()));

  // `AsyncWriter::Close()` consumes the writer and invokes
  // `AsyncWriterConnection::Close`, which returns `kUnimplemented` over UDS.
  StatusOr<std::pair<gcs::AsyncWriter, gcs::AsyncToken>> unclosed =
      client.StartAppendableObjectUpload(gcs::BucketName("zonal"), "close.bin")
          .get();
  ASSERT_THAT(unclosed, IsOk());
  EXPECT_THAT(unclosed->first.Close().get(),
              StatusIs(StatusCode::kUnimplemented));

  // Also exercise `AsyncWriterConnection::Flush` + `Query` directly.
  gcs::AsyncConnection::AppendableUploadParams raw_params;
  auto* spec = raw_params.request.mutable_write_object_spec();
  spec->mutable_resource()->set_bucket("projects/_/buckets/zonal");
  spec->mutable_resource()->set_name("q.bin");
  StatusOr<std::unique_ptr<gcs::AsyncWriterConnection>> raw_writer =
      (*conn)->StartAppendableObjectUpload(std::move(raw_params)).get();
  ASSERT_THAT(raw_writer, IsOk());
  ASSERT_THAT((*raw_writer)->Flush(gcs::WritePayload("abcdefg")).get(), IsOk());
  StatusOr<std::int64_t> queried = (*raw_writer)->Query().get();
  ASSERT_THAT(queried, IsOk());
  EXPECT_EQ(*queried, 7);
  ASSERT_THAT((*raw_writer)->Finalize(gcs::WritePayload("")).get(), IsOk());
}

/// @test Matrix row A22: when the server falls back to JSON on an appendable
/// upload (`transport_used = JSON`), `Finalize` logs a warning and records
/// `x-uds-transport-used: JSON`.
TEST_F(AdapterFakeTest, AsyncAppendableOnRegional) {
  fake_.override_transport_used = pb::JSON;
  ScopedLogCapture logs;
  StatusOr<gcs::AsyncClient> client = MakeUdsAsyncClient(CoreOpts());
  ASSERT_THAT(client, IsOk());

  StatusOr<std::pair<gcs::AsyncWriter, gcs::AsyncToken>> started =
      client
          ->StartAppendableObjectUpload(gcs::BucketName("regional-bkt"),
                                        "fallback.bin")
          .get();
  ASSERT_THAT(started, IsOk());
  auto [writer, token] = *std::move(started);
  StatusOr<google::storage::v2::Object> obj =
      writer.Finalize(std::move(token), gcs::WritePayload("abc")).get();
  ASSERT_THAT(obj, IsOk());
  EXPECT_THAT(writer.GetRequestMetadata().headers,
              Contains(Pair("x-uds-transport-used", "JSON")));
  EXPECT_THAT(logs.Messages(),
              Contains(Pair(google::cloud::Severity::GCP_LS_WARNING,
                            HasSubstr("JSON regular-object fallback"))));
}

/// @test Matrix row A23: regular-object BidiWrite and other non-UDS
/// `AsyncClient` operations return `kUnimplemented`.
TEST_F(AdapterFakeTest, AsyncRegularBidiWriteUnimplemented) {
  StatusOr<gcs::AsyncClient> client = MakeUdsAsyncClient(CoreOpts());
  ASSERT_THAT(client, IsOk());

  gcs::BucketName const b("my-bucket");
  EXPECT_THAT(
      client->StartUnbufferedUpload(b, "o").get(),
      StatusIsWithMessage(StatusCode::kUnimplemented, "StartUnbufferedUpload"));
  EXPECT_THAT(
      client->StartBufferedUpload(b, "o").get(),
      StatusIsWithMessage(StatusCode::kUnimplemented, "StartBufferedUpload"));
  EXPECT_THAT(client->ResumeUnbufferedUpload("id").get(),
              StatusIsWithMessage(StatusCode::kUnimplemented,
                                  "ResumeUnbufferedUpload"));
  EXPECT_THAT(
      client->ResumeBufferedUpload("id").get(),
      StatusIsWithMessage(StatusCode::kUnimplemented, "ResumeBufferedUpload"));
  EXPECT_THAT(client->ResumeAppendableObjectUpload(b, "o", 1).get(),
              StatusIsWithMessage(StatusCode::kUnimplemented,
                                  "ResumeAppendableObjectUpload"));
  EXPECT_THAT(client->InsertObject(b, "o", "data").get(),
              StatusIsWithMessage(StatusCode::kUnimplemented, "InsertObject"));
  EXPECT_THAT(client->ReadObject(b, "o").get(),
              StatusIsWithMessage(StatusCode::kUnimplemented, "ReadObject"));
  EXPECT_THAT(
      client->ReadObjectRange(b, "o", 0, 10).get(),
      StatusIsWithMessage(StatusCode::kUnimplemented, "ReadObjectRange"));
  EXPECT_THAT(client->GetBucket(b).get(),
              StatusIsWithMessage(StatusCode::kUnimplemented, "GetBucket"));
  EXPECT_THAT(client->ComposeObject(b, "dest", {}).get(),
              StatusIsWithMessage(StatusCode::kUnimplemented, "ComposeObject"));
  auto [rewriter, rtok] = client->StartRewrite(b, "src", b, "dst");
  EXPECT_THAT(rewriter.Iterate(std::move(rtok)).get(),
              StatusIsWithMessage(StatusCode::kUnimplemented, "RewriteObject"));
}

/// @test Matrix row A24: `UnifiedCredentialsOption` and `GrpcCredentialOption`
/// are rejected at factory construction time.
TEST_F(AdapterFakeTest, CredentialsRejected) {
  Options const unified =
      Options{}.set<google::cloud::UnifiedCredentialsOption>(
          google::cloud::MakeInsecureCredentials());
  Options const legacy = Options{}.set<google::cloud::GrpcCredentialOption>(
      grpc::InsecureChannelCredentials());

  EXPECT_THAT(MakeUdsStorageClient(CoreOpts(), unified),
              StatusIs(StatusCode::kUnimplemented));
  EXPECT_THAT(MakeUdsStorageClient(CoreOpts(), legacy),
              StatusIs(StatusCode::kUnimplemented));
  EXPECT_THAT(MakeUdsAsyncClient(CoreOpts(), unified),
              StatusIs(StatusCode::kUnimplemented));
  EXPECT_THAT(MakeUdsAsyncClient(CoreOpts(), legacy),
              StatusIs(StatusCode::kUnimplemented));
}

/// @test Matrix row A25: SDK retry/backoff policies log a warning and
/// `MakeUdsStorageClient` installs no retry decorator.
TEST_F(AdapterFakeTest, PoliciesIgnoredWarning) {
  ScopedLogCapture logs;
  StatusOr<std::shared_ptr<gcs_internal::StorageConnection>> sync_conn =
      MakeUdsStorageConnection(
          CoreOpts(), Options{}.set<gcs::RetryPolicyOption>(
                          gcs::LimitedErrorCountRetryPolicy(3).clone()));
  ASSERT_THAT(sync_conn, IsOk());
  EXPECT_THAT((*sync_conn)->InspectStackStructure(),
              ElementsAre("UdsStorageConnection"));

  StatusOr<std::shared_ptr<gcs::AsyncConnection>> async_conn =
      MakeUdsAsyncConnection(
          CoreOpts(), Options{}.set<gcs::AsyncRetryPolicyOption>(
                          gcs::LimitedErrorCountAsyncRetryPolicy(3).clone()));
  ASSERT_THAT(async_conn, IsOk());
  EXPECT_THAT(logs.Messages(), SizeIs(Ge(2U)));
}

/// @test Matrix row A26: default checksum options disable redundant C++
/// hashing, and both `ChecksumValidationOption` and deprecated per-request
/// `DisableCrc32cChecksum` / `DisableMD5Hash` work.
TEST_F(AdapterFakeTest, HashOptions) {
  fake_.objects["h.bin"] = "hash-test";
  StatusOr<std::shared_ptr<gcs_internal::StorageConnection>> conn =
      MakeUdsStorageConnection(CoreOpts());
  ASSERT_THAT(conn, IsOk());
  EXPECT_EQ((*conn)->options().get<gcs::DownloadChecksumValidationOption>(),
            gcs::ChecksumAlgorithm::kNone);
  EXPECT_EQ((*conn)->options().get<gcs::UploadChecksumValidationOption>(),
            gcs::ChecksumAlgorithm::kNone);

  StatusOr<gcs::Client> explicit_client = MakeUdsStorageClient(
      CoreOpts(), Options{}
                      .set<gcs::DownloadChecksumValidationOption>(
                          gcs::ChecksumAlgorithm::kNone)
                      .set<gcs::UploadChecksumValidationOption>(
                          gcs::ChecksumAlgorithm::kNone));
  ASSERT_THAT(explicit_client, IsOk());
  {
    gcs::ObjectReadStream rs = explicit_client->ReadObject("bkt", "h.bin");
    std::string const got(std::istreambuf_iterator<char>(rs), {});
    ASSERT_THAT(rs.status(), IsOk());
    EXPECT_EQ(got, "hash-test");
  }

  gcs::Client client =
      gcs_internal::ClientImplDetails::CreateWithoutDecorations(*conn);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
  gcs::ObjectReadStream rs =
      client.ReadObject("bkt", "h.bin", gcs::DisableCrc32cChecksum(true),
                        gcs::DisableMD5Hash(true));
  std::string const got(std::istreambuf_iterator<char>(rs), {});
  ASSERT_THAT(rs.status(), IsOk());
  EXPECT_EQ(got, "hash-test");

  gcs::ObjectWriteStream ws =
      client.WriteObject("bkt", "h-out.bin", gcs::DisableCrc32cChecksum(true),
                         gcs::DisableMD5Hash(true));
#pragma GCC diagnostic pop
  ws << "written";
  ws.Close();
  ASSERT_THAT(ws.metadata(), IsOk());
}

/// @test Matrix row A27: 404 / 412 / 403 / 429 gRPC codes surface unchanged
/// through both `Client` and `AsyncClient`.
TEST_F(AdapterFakeTest, ErrorCodes) {
  struct CodePair {
    grpc::StatusCode wire;
    StatusCode sdk;
  };
  std::vector<CodePair> const pairs = {
      {grpc::StatusCode::NOT_FOUND, StatusCode::kNotFound},
      {grpc::StatusCode::FAILED_PRECONDITION, StatusCode::kFailedPrecondition},
      {grpc::StatusCode::PERMISSION_DENIED, StatusCode::kPermissionDenied},
      {grpc::StatusCode::RESOURCE_EXHAUSTED, StatusCode::kResourceExhausted},
  };

  StatusOr<gcs::Client> sync_client = MakeUdsStorageClient(CoreOpts());
  StatusOr<gcs::AsyncClient> async_client = MakeUdsAsyncClient(CoreOpts());
  ASSERT_THAT(sync_client, IsOk());
  ASSERT_THAT(async_client, IsOk());

  for (CodePair const& p : pairs) {
    {
      std::scoped_lock lk(fake_.mu);
      fake_.error = grpc::Status(p.wire, "injected error");
    }
    EXPECT_THAT(sync_client->InsertObject("b", "o", "x"), StatusIs(p.sdk));
    gcs::ObjectReadStream rs = sync_client->ReadObject("b", "o");
    (void)std::string(std::istreambuf_iterator<char>(rs), {});
    EXPECT_THAT(rs.status(), StatusIs(p.sdk));
    EXPECT_THAT(async_client->Open(gcs::BucketName("b"), "o").get(),
                StatusIs(p.sdk));
  }
}

/// @test Matrix row B9: a `SO_PEERCRED` `PERMISSION_DENIED` rejection on `Ping`
/// fails fast with `kPermissionDenied` instead of waiting out
/// `readiness_timeout` (`kDeadlineExceeded`).
TEST_F(AdapterFakeTest, PeerCredentialPermissionDeniedFailsFast) {
  {
    std::scoped_lock lk(fake_.mu);
    fake_.ping_error = {grpc::StatusCode::PERMISSION_DENIED,
                        "peer uid 65534 does not match server uid 1000"};
  }
  ClientOptions opts = CoreOpts();
  opts.readiness_timeout = std::chrono::milliseconds(5000);
  auto const t0 = std::chrono::steady_clock::now();
  StatusOr<gcs::Client> sync_client = MakeUdsStorageClient(opts);
  auto const elapsed = std::chrono::steady_clock::now() - t0;
  EXPECT_THAT(sync_client, StatusIsWithMessage(StatusCode::kPermissionDenied,
                                               "peer uid 65534"));
  EXPECT_LT(elapsed, std::chrono::seconds(2));
}

/// @test Boundary validation and unsupported option guards fail explicitly
/// (`kInvalidArgument` for out-of-range integers, `kUnimplemented` for MD5 /
/// precomputed checksums / non-`content_type` `WithObjectMetadata`) instead of
/// silently truncating or dropping caller options.
TEST_F(AdapterFakeTest, BoundaryAndUnsupportedOptions) {
  fake_.objects["p.bin"] = "xyz";
  StatusOr<gcs::Client> sync_client = MakeUdsStorageClient(CoreOpts());
  StatusOr<gcs::AsyncClient> async_client = MakeUdsAsyncClient(CoreOpts());
  ASSERT_THAT(sync_client, IsOk());
  ASSERT_THAT(async_client, IsOk());

  // Negative Generation and IfGenerationMatch are rejected across sync + async.
  {
    gcs::ObjectReadStream s =
        sync_client->ReadObject("bkt", "p.bin", gcs::Generation(-1));
    (void)std::string(std::istreambuf_iterator<char>(s), {});
    EXPECT_THAT(s.status(), StatusIs(StatusCode::kInvalidArgument));
  }
  {
    gcs::ObjectReadStream s =
        sync_client->ReadObject("bkt", "p.bin", gcs::IfGenerationMatch(-1));
    (void)std::string(std::istreambuf_iterator<char>(s), {});
    EXPECT_THAT(s.status(), StatusIs(StatusCode::kInvalidArgument));
  }
  EXPECT_THAT(
      sync_client->InsertObject("bkt", "x", "body", gcs::IfGenerationMatch(-1)),
      StatusIs(StatusCode::kInvalidArgument));
  {
    gcs::ObjectWriteStream ws =
        sync_client->WriteObject("bkt", "x", gcs::IfGenerationMatch(-1));
    EXPECT_THAT(ws.last_status(), StatusIs(StatusCode::kInvalidArgument));
  }
  EXPECT_THAT(sync_client->DeleteObject("bkt", "p.bin", gcs::Generation(-1)),
              StatusIs(StatusCode::kInvalidArgument));
  EXPECT_THAT(
      sync_client->DeleteObject("bkt", "p.bin", gcs::IfGenerationMatch(-1)),
      StatusIs(StatusCode::kInvalidArgument));

  {
    google::storage::v2::BidiReadObjectSpec spec;
    spec.set_bucket("projects/_/buckets/bkt");
    spec.set_object("p.bin");
    spec.set_generation(-1);
    EXPECT_THAT(async_client->Open(spec).get(),
                StatusIs(StatusCode::kInvalidArgument));
  }
  {
    google::storage::v2::BidiReadObjectSpec spec;
    spec.set_bucket("projects/_/buckets/bkt");
    spec.set_object("p.bin");
    spec.set_if_generation_match(0);
    // `gcscore.v1.OpenObjectRequest` does not expose `if_generation_match`.
    EXPECT_THAT(async_client->Open(spec).get(),
                StatusIs(StatusCode::kUnimplemented));
  }
  {
    google::storage::v2::BidiWriteObjectRequest req;
    auto* wspec = req.mutable_write_object_spec();
    wspec->mutable_resource()->set_bucket("projects/_/buckets/bkt");
    wspec->mutable_resource()->set_name("x");
    wspec->set_if_generation_match(-1);
    EXPECT_THAT(async_client->StartAppendableObjectUpload(req).get(),
                StatusIs(StatusCode::kInvalidArgument));
  }
  EXPECT_THAT(
      async_client->DeleteObject(gcs::BucketName("bkt"), "p.bin", -1).get(),
      StatusIs(StatusCode::kInvalidArgument));
  {
    google::storage::v2::DeleteObjectRequest req;
    req.set_bucket("projects/_/buckets/bkt");
    req.set_object("p.bin");
    req.set_if_generation_match(-1);
    EXPECT_THAT(async_client->DeleteObject(req).get(),
                StatusIs(StatusCode::kInvalidArgument));
  }

  // Negative MaxResults and UploadContentLength > INT64_MAX fail cleanly.
  {
    auto reader = sync_client->ListObjects("bkt", gcs::MaxResults(-1));
    EXPECT_THAT(*reader.begin(), StatusIs(StatusCode::kInvalidArgument));
  }
  {
    auto reader =
        sync_client->ListBucketsForProject("proj", gcs::MaxResults(-1));
    EXPECT_THAT(*reader.begin(), StatusIs(StatusCode::kInvalidArgument));
  }
  {
    constexpr std::uint64_t kTooLarge =
        static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) +
        1ULL;
    gcs::ObjectWriteStream ws = sync_client->WriteObject(
        "bkt", "x", gcs::UploadContentLength(kTooLarge));
    EXPECT_THAT(ws.last_status(), StatusIs(StatusCode::kInvalidArgument));
  }

  // Unsupported checksums and non-content_type ObjectMetadata fields return
  // kUnimplemented rather than silently dropping integrity/metadata settings.
  EXPECT_THAT(
      MakeUdsStorageClient(CoreOpts(),
                           Options{}.set<gcs::UploadChecksumValidationOption>(
                               gcs::ChecksumAlgorithm::kMD5)),
      StatusIs(StatusCode::kUnimplemented));
  EXPECT_THAT(
      MakeUdsStorageClient(CoreOpts(),
                           Options{}.set<gcs::DownloadChecksumValidationOption>(
                               gcs::ChecksumAlgorithm::kMD5)),
      StatusIs(StatusCode::kUnimplemented));
  EXPECT_THAT(MakeUdsStorageClient(
                  CoreOpts(), Options{}.set<gcs::PrecomputedChecksumsOption>(
                                  gcs::PrecomputedChecksums{"AAAAAA==", ""})),
              StatusIs(StatusCode::kUnimplemented));

  EXPECT_THAT(sync_client->InsertObject(
                  "bkt", "x", "body",
                  gcs::WithObjectMetadata(
                      gcs::ObjectMetadata().set_cache_control("no-cache"))),
              StatusIs(StatusCode::kUnimplemented));

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
  EXPECT_THAT(sync_client->InsertObject("bkt", "x", "body",
                                        gcs::Crc32cChecksumValue("AAAAAA==")),
              StatusIs(StatusCode::kUnimplemented));
  EXPECT_THAT(
      sync_client->InsertObject("bkt", "x", "body",
                                gcs::MD5HashValue("1B2M2Y8AsgTpgAmY7PhCfg==")),
      StatusIs(StatusCode::kUnimplemented));
  EXPECT_THAT(
      sync_client->InsertObject("bkt", "x", "body", gcs::EnableMD5Hash()),
      StatusIs(StatusCode::kUnimplemented));
#pragma GCC diagnostic pop
}

}  // namespace
}  // namespace gcs_core
