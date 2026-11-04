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

// The wrapper against a fake `gcscore.v1.GcsCore` server: what the wrapper
// sends for each public call (request fields, header-first framing, chunk
// sizes, transport) and how it maps every response field and error back.
// Server behavior itself is covered by the server's own tests.

#include "gcs_core/client.h"
#include "gcscore.grpc.pb.h"
#include "test_support.h"
#include <gmock/gmock.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

namespace gcs_core {
namespace {

namespace pb = ::gcscore::v1;
using ::gcs_core::testing::IsOk;
using ::gcs_core::testing::StatusIs;
using ::google::cloud::StatusCode;
using ::testing::AllOf;
using ::testing::Each;
using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::Le;
using ::testing::Property;
using ::testing::SizeIs;

/// Object bytes the fake serves: `name` -> contents.
using Objects = std::map<std::string, std::string>;

/// Records what the wrapper sent and replies from an in-memory store.
/// `error` (when not OK) is returned by every data RPC instead.
class FakeCore final : public pb::GcsCore::Service {
 public:
  grpc::Status Ping(grpc::ServerContext*, pb::PingRequest const* request,
                    pb::PingResponse* response) override {
    response->set_payload(request->payload());
    return grpc::Status::OK;
  }

  grpc::Status Stats(grpc::ServerContext*, pb::StatsRequest const* request,
                     pb::StatsResponse* response) override {
    {
      std::scoped_lock lk(mu);
      stats_requests.push_back(*request);
    }
    response->set_pid(4242);
    response->set_cpu_user_secs(1.5);
    response->set_cpu_sys_secs(0.25);
    response->set_rss_bytes(1000);
    response->set_peak_rss_bytes(2000);
    response->set_threads(7);
    return grpc::Status::OK;
  }

  grpc::Status ReadObject(grpc::ServerContext*,
                          pb::ReadObjectRequest const* request,
                          grpc::ServerWriter<pb::ReadChunk>* writer) override {
    std::string data;
    {
      std::scoped_lock lk(mu);
      read_requests.push_back(*request);
      if (!error.ok()) return error;
      auto it = objects.find(request->object());
      if (it == objects.end()) {
        return {grpc::StatusCode::NOT_FOUND, "no such object"};
      }
      data = it->second;
    }
    // Metadata alone first, then the bytes as two slices per message, the
    // shape the server produces from coalesced network buffers.
    pb::ReadChunk first;
    first.mutable_meta()->set_size(static_cast<std::int64_t>(data.size()));
    first.mutable_meta()->set_generation(77);
    first.mutable_meta()->set_metageneration(3);
    first.mutable_meta()->set_content_type("text/plain");
    first.mutable_meta()->set_content_encoding("gzip");
    first.mutable_meta()->set_crc32c(0xdeadbeef);
    writer->Write(first);
    for (std::size_t offset = 0; offset < data.size(); offset += 10) {
      pb::ReadChunk chunk;
      std::string const part = data.substr(offset, 10);
      chunk.add_data(part.substr(0, 5));
      chunk.add_data(part.substr(std::min<std::size_t>(5, part.size())));
      writer->Write(chunk);
    }
    return grpc::Status::OK;
  }

  grpc::Status WriteObject(grpc::ServerContext*,
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
    writes.push_back(messages);
    if (!error.ok()) return error;
    if (messages.empty() || !messages.front().has_header()) {
      return {grpc::StatusCode::INVALID_ARGUMENT,
              "first message must be header"};
    }
    objects[messages.front().header().object()] = data;
    response->set_size(static_cast<std::int64_t>(data.size()));
    response->set_generation(99);
    response->set_crc32c(data.empty() ? 0 : 0x1234);
    response->set_transport_used(transport_used);
    return grpc::Status::OK;
  }

  grpc::Status ListObjects(grpc::ServerContext*,
                           pb::ListObjectsRequest const* request,
                           pb::ListObjectsResponse* response) override {
    std::scoped_lock lk(mu);
    list_objects_requests.push_back(*request);
    if (!error.ok()) return error;
    pb::ListedObject& a = *response->add_items();
    a.set_name("p/a");
    a.set_size(5);
    a.set_generation(11);
    a.set_metageneration(2);
    a.set_content_type("application/octet-stream");
    a.set_storage_class("STANDARD");
    a.set_updated_unix_secs(1700000000);  // date -u -d @1700000000
    a.set_crc32c(0);
    response->add_prefixes("p/dir/");
    response->set_next_page_token("next");
    return grpc::Status::OK;
  }

  grpc::Status ListBuckets(grpc::ServerContext*,
                           pb::ListBucketsRequest const* request,
                           pb::ListBucketsResponse* response) override {
    std::scoped_lock lk(mu);
    list_buckets_requests.push_back(*request);
    if (!error.ok()) return error;
    pb::ListedBucket& b = *response->add_items();
    b.set_name("bucket-1");
    b.set_location("US-WEST1");
    b.set_storage_class("STANDARD");
    b.set_metageneration(4);
    b.set_created_unix_secs(1600000000);  // date -u -d @1600000000
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
    if (!error.ok()) return error;
    auto it = objects.find(request->object());
    if (it == objects.end()) {
      return {grpc::StatusCode::NOT_FOUND, "no such object"};
    }
    response->set_handle(++next_handle);
    open_handles[response->handle()] = it->second;
    response->mutable_meta()->set_size(
        static_cast<std::int64_t>(it->second.size()));
    response->mutable_meta()->set_generation(5);
    return grpc::Status::OK;
  }

  grpc::Status ReadRange(grpc::ServerContext*,
                         pb::ReadRangeRequest const* request,
                         grpc::ServerWriter<pb::ReadChunk>* writer) override {
    std::string data;
    {
      std::scoped_lock lk(mu);
      range_requests.push_back(*request);
      if (request->offset() == fail_range_offset) {
        return {grpc::StatusCode::OUT_OF_RANGE, "bad range"};
      }
      auto it = open_handles.find(request->handle());
      if (it == open_handles.end()) {
        return {grpc::StatusCode::NOT_FOUND, "no such handle"};
      }
      data = it->second;
    }
    auto const offset = static_cast<std::size_t>(request->offset());
    std::size_t const length =
        request->length() < 0 ? data.size() - offset
                              : static_cast<std::size_t>(request->length());
    std::string const range = data.substr(offset, length);
    for (std::size_t i = 0; i < range.size(); i += 3) {
      pb::ReadChunk chunk;
      chunk.add_data(range.substr(i, 3));
      writer->Write(chunk);
    }
    return grpc::Status::OK;
  }

  grpc::Status CloseObject(grpc::ServerContext*,
                           pb::CloseObjectRequest const* request,
                           pb::Empty*) override {
    std::this_thread::sleep_for(close_delay);
    std::scoped_lock lk(mu);
    closed_handles.push_back(request->handle());
    open_handles.erase(request->handle());
    return grpc::Status::OK;
  }

  std::mutex mu;
  grpc::Status error = grpc::Status::OK;
  pb::Transport transport_used = pb::JSON;
  std::int64_t fail_range_offset = -1000;
  std::chrono::milliseconds close_delay{0};
  Objects objects;
  std::uint64_t next_handle = 0;
  std::map<std::uint64_t, std::string> open_handles;
  std::vector<pb::ReadObjectRequest> read_requests;
  std::vector<std::vector<pb::WriteRequest>> writes;
  std::vector<pb::ListObjectsRequest> list_objects_requests;
  std::vector<pb::ListBucketsRequest> list_buckets_requests;
  std::vector<pb::DeleteObjectRequest> delete_requests;
  std::vector<pb::ReadRangeRequest> range_requests;
  std::vector<pb::StatsRequest> stats_requests;
  std::vector<std::uint64_t> closed_handles;
};

/// Runs a `FakeCore` on a Unix socket in a private temporary directory and
/// creates wrapper clients connected to it.
class FakeCoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char dir_template[] = "/tmp/gcs-core-fake-XXXXXX";
    ASSERT_NE(::mkdtemp(dir_template), nullptr);
    dir_ = dir_template;
    // A distinct path per test: the wrapper shares connections by socket.
    static std::atomic<int> seq{0};
    socket_ = dir_ + "/fake-" + std::to_string(++seq) + ".sock";
    grpc::ServerBuilder builder;
    builder.AddListeningPort("unix:" + socket_,
                             grpc::InsecureServerCredentials());
    builder.RegisterService(&fake_);
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);
  }

  void TearDown() override {
    if (server_) server_->Shutdown();
    ::unlink(socket_.c_str());
    ::rmdir(dir_.c_str());
  }

  StatusOr<Client> MakeClient(std::uint32_t chunk_size_hint = 0,
                              int channels = 1) {
    ClientOptions options;
    options.socket_path = socket_;
    options.chunk_size_hint = chunk_size_hint;
    options.channels = channels;
    return Client::Create(options);
  }

  std::string dir_;
  std::string socket_;
  FakeCore fake_;
  std::unique_ptr<grpc::Server> server_;
};

std::string ReadAll(ObjectReader& reader) {
  std::string out;
  for (;;) {
    StatusOr<std::optional<absl::Cord>> chunk = reader.Next();
    if (!chunk || !*chunk) return out;
    out += std::string(**chunk);
  }
}

/// @test ReadObject maps the first message's metadata (adding the bucket and
/// object from the request) and joins multi-slice messages in order.
TEST_F(FakeCoreTest, ReadObjectMetadataAndSlices) {
  fake_.objects["obj"] = "0123456789abcdefghijklmnopqrstuvw";
  StatusOr<Client> client = MakeClient();
  ASSERT_THAT(client, IsOk());
  StatusOr<ObjectReader> reader = client->ReadObject("bkt", "obj");
  ASSERT_THAT(reader, IsOk());
  ObjectMetadata const& m = reader->metadata();
  EXPECT_EQ(m.bucket, "bkt");
  EXPECT_EQ(m.name, "obj");
  EXPECT_EQ(m.size, 33U);
  EXPECT_EQ(m.generation, 77);
  EXPECT_EQ(m.metageneration, 3);
  EXPECT_EQ(m.content_type, "text/plain");
  EXPECT_EQ(m.content_encoding, "gzip");
  EXPECT_EQ(m.crc32c, std::optional<std::uint32_t>(0xdeadbeef));
  EXPECT_EQ(ReadAll(*reader), "0123456789abcdefghijklmnopqrstuvw");
}

/// @test ReadObject forwards `if_generation_match` when set (including 0) and
/// leaves it unset otherwise.
TEST_F(FakeCoreTest, ReadObjectPreconditionPresence) {
  fake_.objects["obj"] = "abc";
  StatusOr<Client> client = MakeClient();
  ASSERT_THAT(client, IsOk());
  ASSERT_THAT(client->ReadObject("bkt", "obj", {}, 0, std::nullopt), IsOk());
  ASSERT_THAT(client->ReadObject("bkt", "obj", {}, 0, 0), IsOk());
  ASSERT_THAT(client->ReadObject("bkt", "obj", {}, 5, 42), IsOk());
  ASSERT_THAT(fake_.read_requests, SizeIs(3));
  EXPECT_FALSE(fake_.read_requests[0].has_if_generation_match());
  ASSERT_TRUE(fake_.read_requests[1].has_if_generation_match());
  EXPECT_EQ(fake_.read_requests[1].if_generation_match(), 0);
  ASSERT_TRUE(fake_.read_requests[2].has_if_generation_match());
  EXPECT_EQ(fake_.read_requests[2].if_generation_match(), 42);
}

/// @test A zero-byte object yields metadata and an immediate end of data.
TEST_F(FakeCoreTest, ReadEmptyObject) {
  fake_.objects["empty"] = "";
  StatusOr<Client> client = MakeClient();
  ASSERT_THAT(client, IsOk());
  StatusOr<ObjectReader> reader = client->ReadObject("bkt", "empty");
  ASSERT_THAT(reader, IsOk());
  EXPECT_EQ(reader->metadata().size, 0U);
  StatusOr<std::optional<absl::Cord>> next = reader->Next();
  ASSERT_THAT(next, IsOk());
  EXPECT_FALSE(next->has_value());
}

struct RangeMapping {
  char const* name;
  ReadRange range;
  std::int64_t offset;
  std::int64_t length;
};

class ReadRangeMappingTest
    : public FakeCoreTest,
      public ::testing::WithParamInterface<RangeMapping> {};

/// @test `ReadRange{offset, limit}` reaches the server as `(offset, length)`
/// with `limit == 0` meaning "to the end" (`length = -1`; the server reads
/// `length = 0` the same way, but -1 is its documented spelling), always over
/// JSON, with the client's chunk-size hint.
TEST_P(ReadRangeMappingTest, RequestFields) {
  RangeMapping const& c = GetParam();
  fake_.objects["obj"] = std::string(100, 'x');
  StatusOr<Client> client = MakeClient(/*chunk_size_hint=*/4096);
  ASSERT_THAT(client, IsOk());
  StatusOr<ObjectReader> reader =
      client->ReadObject("bkt", "obj", c.range, /*generation=*/12);
  ASSERT_THAT(reader, IsOk());
  ASSERT_THAT(fake_.read_requests, SizeIs(1));
  pb::ReadObjectRequest const& r = fake_.read_requests.front();
  EXPECT_EQ(r.bucket(), "bkt");
  EXPECT_EQ(r.object(), "obj");
  EXPECT_EQ(r.offset(), c.offset);
  EXPECT_EQ(r.length(), c.length);
  EXPECT_EQ(r.generation(), 12);
  EXPECT_FALSE(r.has_if_generation_match());
  EXPECT_EQ(r.transport(), pb::JSON);
  EXPECT_EQ(r.chunk_size(), 4096U);
}

INSTANTIATE_TEST_SUITE_P(
    Ranges, ReadRangeMappingTest,
    ::testing::Values(RangeMapping{"Full", {0, 0}, 0, -1},
                      RangeMapping{"OffsetToEnd", {100, 0}, 100, -1},
                      RangeMapping{"Segment", {100, 50}, 100, 50},
                      RangeMapping{"Tail", {-30, 0}, -30, -1}),
    [](::testing::TestParamInfo<RangeMapping> const& info) {
      return info.param.name;
    });

/// @test Server errors on the first message surface with the same code.
TEST_F(FakeCoreTest, ReadObjectErrorCode) {
  StatusOr<Client> client = MakeClient();
  ASSERT_THAT(client, IsOk());
  EXPECT_THAT(client->ReadObject("bkt", "missing"),
              StatusIs(StatusCode::kNotFound));
  fake_.error = grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "412");
  fake_.objects["obj"] = "x";
  EXPECT_THAT(client->ReadObject("bkt", "obj"),
              StatusIs(StatusCode::kFailedPrecondition));
}

/// @test An upload sends the header alone first, then data in messages no
/// larger than the chunk-size hint, and maps the response.
TEST_F(FakeCoreTest, WriteHeaderFirstThenBoundedChunks) {
  StatusOr<Client> client = MakeClient(/*chunk_size_hint=*/4096);
  ASSERT_THAT(client, IsOk());
  WriteOptions options;
  options.content_type = "application/x-test";
  StatusOr<ObjectWriter> writer = client->WriteObject("bkt", "up", options);
  ASSERT_THAT(writer, IsOk());
  ASSERT_THAT(writer->Write(absl::Cord(std::string(6000, 'a'))), IsOk());
  ASSERT_THAT(writer->Write(absl::Cord(std::string(4000, 'b'))), IsOk());
  StatusOr<ObjectMetadata> meta = writer->Close();
  ASSERT_THAT(meta, IsOk());
  EXPECT_EQ(meta->bucket, "bkt");
  EXPECT_EQ(meta->name, "up");
  EXPECT_EQ(meta->size, 10000U);
  EXPECT_EQ(meta->generation, 99);
  EXPECT_EQ(meta->crc32c, std::optional<std::uint32_t>(0x1234));

  ASSERT_THAT(fake_.writes, SizeIs(1));
  std::vector<pb::WriteRequest> const& messages = fake_.writes.front();
  ASSERT_FALSE(messages.empty());
  ASSERT_TRUE(messages.front().has_header());
  EXPECT_EQ(messages.front().header().bucket(), "bkt");
  EXPECT_EQ(messages.front().header().object(), "up");
  EXPECT_EQ(messages.front().header().content_type(), "application/x-test");
  EXPECT_EQ(messages.front().header().transport(), pb::JSON);
  EXPECT_EQ(messages.front().header().chunk_size(), 0U)
      << "the resumable buffer stays at the crate default";
  std::vector<std::size_t> sizes;
  for (std::size_t i = 1; i < messages.size(); ++i) {
    ASSERT_TRUE(messages[i].has_data()) << "message " << i;
    sizes.push_back(messages[i].data().size());
  }
  EXPECT_THAT(sizes, Each(Le(4096U)));
  EXPECT_THAT(sizes, ElementsAre(4096, 1904, 4000));
}

/// @test Closing an upload without data still sends the header, creating an
/// empty object.
TEST_F(FakeCoreTest, EmptyUploadSendsHeader) {
  StatusOr<Client> client = MakeClient();
  ASSERT_THAT(client, IsOk());
  StatusOr<ObjectWriter> writer = client->WriteObject("bkt", "empty");
  ASSERT_THAT(writer, IsOk());
  StatusOr<ObjectMetadata> meta = writer->Close();
  ASSERT_THAT(meta, IsOk());
  EXPECT_EQ(meta->size, 0U);
  EXPECT_FALSE(meta->crc32c.has_value()) << "0 from the server means absent";
  ASSERT_THAT(fake_.writes, SizeIs(1));
  ASSERT_THAT(fake_.writes.front(), SizeIs(1));
  EXPECT_TRUE(fake_.writes.front().front().has_header());
}

struct UploadModeCase {
  char const* name;
  UploadMode mode;
  std::optional<std::uint64_t> size_hint;
  std::optional<std::int64_t> want_known_size;
};

class UploadModeTest : public FakeCoreTest,
                       public ::testing::WithParamInterface<UploadModeCase> {};

/// @test The upload mode decides whether the size reaches the server as
/// `known_size` (which lets the crate upload in one request): passed for
/// kAuto and kSingleShot when known, withheld for kResumable.
TEST_P(UploadModeTest, KnownSize) {
  UploadModeCase const& c = GetParam();
  StatusOr<Client> client = MakeClient();
  ASSERT_THAT(client, IsOk());
  WriteOptions options;
  options.mode = c.mode;
  options.size_hint = c.size_hint;
  StatusOr<ObjectWriter> writer = client->WriteObject("bkt", "o", options);
  ASSERT_THAT(writer, IsOk());
  ASSERT_THAT(writer->Write(absl::Cord("abc")), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());
  ASSERT_THAT(fake_.writes, SizeIs(1));
  pb::WriteHeader const& h = fake_.writes.front().front().header();
  std::optional<std::int64_t> const sent =
      h.has_known_size() ? std::optional<std::int64_t>(h.known_size())
                         : std::nullopt;
  EXPECT_EQ(sent, c.want_known_size);
}

INSTANTIATE_TEST_SUITE_P(
    Modes, UploadModeTest,
    ::testing::Values(UploadModeCase{"AutoWithSize", UploadMode::kAuto, 3, 3},
                      UploadModeCase{"AutoWithoutSize", UploadMode::kAuto,
                                     std::nullopt, std::nullopt},
                      UploadModeCase{"SingleShot", UploadMode::kSingleShot, 3,
                                     3},
                      UploadModeCase{"ResumableWithSize",
                                     UploadMode::kResumable, 3, std::nullopt}),
    [](::testing::TestParamInfo<UploadModeCase> const& info) {
      return info.param.name;
    });

/// @test `if_generation_match` is sent only when set, and 0 ("must not
/// exist") is sent as a present 0 rather than dropped.
TEST_F(FakeCoreTest, GenerationPreconditionPresence) {
  StatusOr<Client> client = MakeClient();
  ASSERT_THAT(client, IsOk());
  WriteOptions none;
  ASSERT_THAT(client->InsertObject("bkt", "a", absl::Cord("1"), none), IsOk());
  WriteOptions zero;
  zero.if_generation_match = 0;
  zero.resumable_buffer_size = 4 * 1024 * 1024;
  ASSERT_THAT(client->InsertObject("bkt", "b", absl::Cord("1"), zero), IsOk());
  ASSERT_THAT(fake_.writes, SizeIs(2));
  EXPECT_FALSE(fake_.writes[0].front().header().has_if_generation_match());
  EXPECT_EQ(fake_.writes[0].front().header().chunk_size(), 0U);
  ASSERT_TRUE(fake_.writes[1].front().header().has_if_generation_match());
  EXPECT_EQ(fake_.writes[1].front().header().if_generation_match(), 0);
  EXPECT_EQ(fake_.writes[1].front().header().chunk_size(), 4U * 1024 * 1024);
  EXPECT_EQ(fake_.writes[1].front().header().known_size(), 1)
      << "InsertObject passes the data size";
}

/// @test Appendable uploads request the gRPC transport, succeed only when
/// the server reports it was used, and cannot be left unfinalized.
TEST_F(FakeCoreTest, AppendableRequiresGrpcTransport) {
  StatusOr<Client> client = MakeClient();
  ASSERT_THAT(client, IsOk());

  fake_.transport_used = pb::GRPC;
  StatusOr<AppendableWriter> ok = client->StartAppendableUpload("bkt", "app");
  ASSERT_THAT(ok, IsOk());
  ASSERT_THAT(ok->Write(absl::Cord(std::string(10, 'q'))), IsOk());
  StatusOr<ObjectMetadata> meta = ok->Finalize();
  ASSERT_THAT(meta, IsOk());
  EXPECT_EQ(meta->size, 10U);
  EXPECT_EQ(fake_.writes.back().front().header().transport(), pb::GRPC);

  fake_.transport_used = pb::JSON;
  StatusOr<AppendableWriter> fallback =
      client->StartAppendableUpload("bkt", "regional");
  ASSERT_THAT(fallback, IsOk());
  ASSERT_THAT(fallback->Write(absl::Cord("x")), IsOk());
  EXPECT_THAT(fallback->Finalize(), StatusIs(StatusCode::kFailedPrecondition));

  StatusOr<AppendableWriter> allowed_fallback =
      client->StartAppendableUpload("bkt", "regional-allowed");
  ASSERT_THAT(allowed_fallback, IsOk());
  ASSERT_THAT(allowed_fallback->Write(absl::Cord("xy")), IsOk());
  StatusOr<ObjectMetadata> fallback_meta =
      allowed_fallback->Finalize(/*allow_json_fallback=*/true);
  ASSERT_THAT(fallback_meta, IsOk());
  EXPECT_EQ(fallback_meta->size, 2U);
  EXPECT_TRUE(fallback_meta->used_json_fallback);

  StatusOr<AppendableWriter> open = client->StartAppendableUpload("bkt", "o");
  ASSERT_THAT(open, IsOk());
  EXPECT_THAT(open->Close(), StatusIs(StatusCode::kUnimplemented));
}

/// @test ListObjects forwards every request field and maps every listed
/// field, filling in the bucket; a CRC32C of 0 is reported as absent.
TEST_F(FakeCoreTest, ListObjectsFields) {
  StatusOr<Client> client = MakeClient();
  ASSERT_THAT(client, IsOk());
  ListObjectsRequest request;
  request.prefix = "p/";
  request.delimiter = "/";
  request.start_offset = "p/a";
  request.end_offset = "p/z";
  request.versions = true;
  request.page_size = 100;
  request.page_token = "tok";
  StatusOr<ListObjectsPage> page = client->ListObjects("bkt", request);
  ASSERT_THAT(page, IsOk());

  ASSERT_THAT(fake_.list_objects_requests, SizeIs(1));
  pb::ListObjectsRequest const& r = fake_.list_objects_requests.front();
  EXPECT_EQ(r.bucket(), "bkt");
  EXPECT_EQ(r.prefix(), "p/");
  EXPECT_EQ(r.delimiter(), "/");
  EXPECT_EQ(r.start_offset(), "p/a");
  EXPECT_EQ(r.end_offset(), "p/z");
  EXPECT_TRUE(r.versions());
  EXPECT_EQ(r.page_size(), 100);
  EXPECT_EQ(r.page_token(), "tok");

  ASSERT_THAT(page->objects, SizeIs(1));
  ObjectMetadata const& o = page->objects.front();
  EXPECT_EQ(o.bucket, "bkt");
  EXPECT_EQ(o.name, "p/a");
  EXPECT_EQ(o.size, 5U);
  EXPECT_EQ(o.generation, 11);
  EXPECT_EQ(o.metageneration, 2);
  EXPECT_EQ(o.content_type, "application/octet-stream");
  EXPECT_EQ(o.storage_class, "STANDARD");
  EXPECT_EQ(o.updated, std::chrono::system_clock::time_point(
                           std::chrono::seconds(1700000000)));
  EXPECT_FALSE(o.crc32c.has_value());
  EXPECT_THAT(page->prefixes, ElementsAre("p/dir/"));
  EXPECT_EQ(page->next_page_token, "next");
}

/// @test ListBuckets forwards the project and paging fields and maps every
/// bucket field.
TEST_F(FakeCoreTest, ListBucketsFields) {
  StatusOr<Client> client = MakeClient();
  ASSERT_THAT(client, IsOk());
  ListBucketsRequest request;
  request.prefix = "b";
  request.page_size = 100;
  request.page_token = "tok";
  StatusOr<ListBucketsPage> page = client->ListBuckets("my-project", request);
  ASSERT_THAT(page, IsOk());
  ASSERT_THAT(fake_.list_buckets_requests, SizeIs(1));
  pb::ListBucketsRequest const& r = fake_.list_buckets_requests.front();
  EXPECT_EQ(r.project(), "my-project");
  EXPECT_EQ(r.prefix(), "b");
  EXPECT_EQ(r.page_size(), 100);
  EXPECT_EQ(r.page_token(), "tok");
  ASSERT_THAT(page->buckets, SizeIs(1));
  BucketMetadata const& b = page->buckets.front();
  EXPECT_EQ(b.name, "bucket-1");
  EXPECT_EQ(b.location, "US-WEST1");
  EXPECT_EQ(b.storage_class, "STANDARD");
  EXPECT_EQ(b.metageneration, 4);
  EXPECT_EQ(b.created, std::chrono::system_clock::time_point(
                           std::chrono::seconds(1600000000)));
  EXPECT_TRUE(page->next_page_token.empty());
}

/// @test DeleteObject forwards bucket, object and generation, and surfaces
/// NOT_FOUND.
TEST_F(FakeCoreTest, DeleteObject) {
  fake_.objects["gone"] = "x";
  fake_.objects["guarded"] = "y";
  StatusOr<Client> client = MakeClient();
  ASSERT_THAT(client, IsOk());
  ASSERT_THAT(client->DeleteObject("bkt", "gone", 8), IsOk());
  ASSERT_THAT(client->DeleteObject("bkt", "guarded", 0, 0), IsOk());
  EXPECT_THAT(client->DeleteObject("bkt", "gone"),
              StatusIs(StatusCode::kNotFound));
  ASSERT_THAT(fake_.delete_requests, SizeIs(3));
  EXPECT_EQ(fake_.delete_requests[0].bucket(), "bkt");
  EXPECT_EQ(fake_.delete_requests[0].object(), "gone");
  EXPECT_EQ(fake_.delete_requests[0].generation(), 8);
  EXPECT_FALSE(fake_.delete_requests[0].has_if_generation_match());
  ASSERT_TRUE(fake_.delete_requests[1].has_if_generation_match());
  EXPECT_EQ(fake_.delete_requests[1].if_generation_match(), 0);
}

/// @test ReadRanges issues one ReadRange per range on the descriptor's handle
/// and delivers each range's bytes, in order, tagged with its index; the
/// descriptor's destructor closes the handle.
TEST_F(FakeCoreTest, DescriptorReadRanges) {
  fake_.objects["obj"] = "abcdefghijklmnopqrstuvwxyz";
  StatusOr<Client> client = MakeClient(/*chunk_size_hint=*/0, /*channels=*/2);
  ASSERT_THAT(client, IsOk());
  std::uint64_t handle = 0;
  {
    StatusOr<ObjectDescriptor> descriptor = client->OpenObject("bkt", "obj");
    ASSERT_THAT(descriptor, IsOk());
    EXPECT_EQ(descriptor->metadata().bucket, "bkt");
    EXPECT_EQ(descriptor->metadata().name, "obj");
    EXPECT_EQ(descriptor->metadata().size, 26U);
    handle = fake_.next_handle;
    std::vector<std::string> got(3);
    Status s = descriptor->ReadRanges(
        {{0, 10}, {20, 0}, {5, 7}},
        [&](std::size_t i, absl::Cord const& c) { got[i] += std::string(c); });
    ASSERT_THAT(s, IsOk());
    EXPECT_THAT(got, ElementsAre("abcdefghij", "uvwxyz", "fghijkl"));
    EXPECT_THAT(fake_.range_requests,
                AllOf(SizeIs(3),
                      Each(Property(&pb::ReadRangeRequest::handle, handle))));
  }
  EXPECT_THAT(fake_.closed_handles, ElementsAre(handle));
}

/// @test If one range fails, ReadRanges returns that error after every range
/// has finished (none is left running).
TEST_F(FakeCoreTest, DescriptorReadRangesError) {
  fake_.objects["obj"] = std::string(100, 'z');
  fake_.fail_range_offset = 50;
  StatusOr<Client> client = MakeClient();
  ASSERT_THAT(client, IsOk());
  StatusOr<ObjectDescriptor> descriptor = client->OpenObject("bkt", "obj");
  ASSERT_THAT(descriptor, IsOk());
  Status s = descriptor->ReadRanges({{0, 10}, {50, 10}, {60, 10}},
                                    [](std::size_t, absl::Cord const&) {});
  EXPECT_THAT(s, StatusIs(StatusCode::kOutOfRange));
  EXPECT_THAT(descriptor->ReadRanges({}, [](std::size_t, absl::Cord const&) {}),
              IsOk())
      << "no ranges is a no-op";
}

/// @test A descriptor from a server that has since been restarted is
/// rejected instead of reaching the new server: the shared server numbers
/// handles from 1, so the stale handle would name the new server's object.
/// Its destructor does not close the new server's handle either.
TEST_F(FakeCoreTest, StaleDescriptorRejectedAfterRestart) {
  fake_.objects["old"] = "stale-bytes";
  fake_.objects["new"] = "fresh";
  StatusOr<Client> client = MakeClient();
  ASSERT_THAT(client, IsOk());
  std::optional<ObjectDescriptor> stale;
  {
    StatusOr<ObjectDescriptor> d = client->OpenObject("bkt", "old");
    ASSERT_THAT(d, IsOk());
    stale.emplace(*std::move(d));
  }

  // The backend reports the server gone and the next call fails UNAVAILABLE,
  // which is how the wrapper learns of a crash and restarts the server.
  internal::KillCoreForTesting(*client);
  fake_.error = grpc::Status(grpc::StatusCode::UNAVAILABLE, "server died");
  EXPECT_THAT(client->OpenObject("bkt", "new"),
              StatusIs(StatusCode::kUnavailable));
  fake_.error = grpc::Status::OK;
  // A restarted server starts numbering handles again.
  fake_.next_handle = 0;
  fake_.open_handles.clear();

  StatusOr<ObjectDescriptor> fresh = client->OpenObject("bkt", "new");
  ASSERT_THAT(fresh, IsOk());
  ASSERT_EQ(fake_.next_handle, 1U) << "the fresh handle aliases the stale one";

  std::string stale_bytes;
  EXPECT_THAT(stale->ReadRanges({{0, 0}},
                                [&](std::size_t, absl::Cord const& c) {
                                  stale_bytes += std::string(c);
                                }),
              StatusIs(StatusCode::kFailedPrecondition));
  EXPECT_EQ(stale_bytes, "");
  stale.reset();
  EXPECT_THAT(fake_.closed_handles, IsEmpty());

  std::string fresh_bytes;
  ASSERT_THAT(fresh->ReadRanges({{0, 0}},
                                [&](std::size_t, absl::Cord const& c) {
                                  fresh_bytes += std::string(c);
                                }),
              IsOk());
  EXPECT_EQ(fresh_bytes, "fresh");
}

/// @test The descriptor's best-effort CloseObject is bounded by a deadline,
/// so a server that stops answering cannot stall the destructor (and with it
/// stack unwinding) indefinitely.
TEST_F(FakeCoreTest, DescriptorCloseIsBounded) {
  fake_.objects["obj"] = "x";
  fake_.close_delay = std::chrono::milliseconds(3000);
  StatusOr<Client> client = MakeClient();
  ASSERT_THAT(client, IsOk());
  StatusOr<ObjectDescriptor> descriptor = client->OpenObject("bkt", "obj");
  ASSERT_THAT(descriptor, IsOk());
  auto const start = std::chrono::steady_clock::now();
  {
    ObjectDescriptor closing = *std::move(descriptor);
  }
  EXPECT_LT(std::chrono::steady_clock::now() - start,
            std::chrono::milliseconds(1500));
}

/// @test Server stats are converted from seconds to microseconds; the shared
/// server does not count allocations.
TEST_F(FakeCoreTest, StatsAndVersion) {
  StatusOr<Client> client = MakeClient();
  ASSERT_THAT(client, IsOk());
  StatusOr<SidecarStats> stats = client->GetSidecarStats();
  ASSERT_THAT(stats, IsOk());
  EXPECT_EQ(stats->user_cpu_us, 1'500'000U);
  EXPECT_EQ(stats->system_cpu_us, 250'000U);
  EXPECT_EQ(stats->rss_bytes, 1000U);
  EXPECT_EQ(stats->peak_rss_bytes, 2000U);
  EXPECT_EQ(stats->threads, 7U);
  EXPECT_EQ(stats->allocated_bytes, 0U);
  StatusOr<SidecarVersion> version = client->GetSidecarVersion();
  ASSERT_THAT(version, IsOk());
  EXPECT_EQ(version->pid, 4242U);
  ASSERT_THAT(client->GetSidecarStats(/*reset_peak_rss=*/true), IsOk());
  ASSERT_THAT(fake_.stats_requests, SizeIs(3));
  EXPECT_FALSE(fake_.stats_requests[0].reset_peak_rss());
  EXPECT_FALSE(fake_.stats_requests[1].reset_peak_rss());
  EXPECT_TRUE(fake_.stats_requests[2].reset_peak_rss());
}

}  // namespace
}  // namespace gcs_core
