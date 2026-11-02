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

// Wrapper tests against the real sidecar in --synthetic mode: the full
// wrapper -> UDS -> sidecar path runs, without external GCS traffic.
// `GCS_CORE_SIDECAR` points at the built sidecar (set by CMake/ctest).

#include "gcs_core/client.h"
#include "gcs/core/v0/storage_core.grpc.pb.h"
#include <gmock/gmock.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <sys/stat.h>
#include <unistd.h>

namespace gcs_core {
namespace {

using ::google::cloud::StatusCode;
using ::testing::ElementsAre;
using ::testing::HasSubstr;

Status AsStatus(Status const& s) { return s; }
template <typename T>
Status const& AsStatus(StatusOr<T> const& s) {
  return s.status();
}

MATCHER(IsOk, "status is OK") {
  Status const s = AsStatus(arg);
  if (s.ok()) return true;
  *result_listener << s;
  return false;
}

MATCHER_P(StatusIs, code, "status has expected code") {
  Status const s = AsStatus(arg);
  if (s.code() == code) return true;
  *result_listener << s;
  return false;
}

ClientOptions SyntheticOptions() {
  ClientOptions options;
  options.sidecar_args = {"--synthetic"};
  return options;
}

StatusOr<std::uint64_t> ReadSize(ObjectReader& reader,
                                 std::size_t* max_chunk = nullptr) {
  std::uint64_t total = 0;
  for (;;) {
    StatusOr<std::optional<absl::Cord>> chunk = reader.Next();
    if (!chunk.ok()) return std::move(chunk).status();
    if (!*chunk) return total;
    if (max_chunk != nullptr) {
      *max_chunk = std::max(*max_chunk, (*chunk)->size());
    }
    total += (*chunk)->size();
  }
}

bool ProcessAlive(int pid) { return pid > 0 && ::kill(pid, 0) == 0; }

void WaitForExit(int pid) {
  std::chrono::steady_clock::time_point const deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (ProcessAlive(pid) && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

std::string WriteSleepScript(std::string const& tag) {
  std::string path =
      "/tmp/gcs-core-sleep-" + tag + "-" + std::to_string(::getpid()) + ".sh";
  {
    std::ofstream out(path);
    out << "#!/bin/sh\nsleep 10\n";
  }
  ::chmod(path.c_str(), 0700);
  return path;
}

/// @test The sidecar reports synthetic mode and the pinned crate version,
/// proving the wrapper reached the process it spawned.
TEST(ClientTest, VersionFromSyntheticSidecar) {
  StatusOr<Client> client = Client::Create(SyntheticOptions());
  ASSERT_THAT(client, IsOk());
  StatusOr<SidecarVersion> version = client->GetSidecarVersion();
  ASSERT_THAT(version, IsOk());
  EXPECT_TRUE(version->synthetic);
  EXPECT_EQ(version->storage_crate_version, "1.21.0");
  EXPECT_EQ(static_cast<int>(version->pid), client->sidecar_pid());
}

class WriteThenReadSizeTest : public ::testing::TestWithParam<std::uint64_t> {};

/// @test A written object reads back with its exact size, covering 0 bytes and
/// payloads spanning multiple 1 MiB UDS chunks.
TEST_P(WriteThenReadSizeTest, RoundTripsExactByteCount) {
  std::uint64_t const size = GetParam();
  StatusOr<Client> client = Client::Create(SyntheticOptions());
  ASSERT_THAT(client, IsOk());
  std::string const object = "obj-" + std::to_string(size) + "-x";
  StatusOr<ObjectMetadata> meta =
      client->InsertObject("b", object, absl::Cord(std::string(size, 'a')));
  ASSERT_THAT(meta, IsOk());
  EXPECT_EQ(meta->size, size);
  StatusOr<ObjectReader> reader = client->ReadObject("b", object);
  ASSERT_THAT(reader, IsOk());
  EXPECT_EQ(reader->metadata().size, size);
  StatusOr<std::uint64_t> got = ReadSize(*reader);
  ASSERT_THAT(got, IsOk());
  EXPECT_EQ(*got, size);
}

INSTANTIATE_TEST_SUITE_P(Sizes, WriteThenReadSizeTest,
                         ::testing::Values(0ULL, 1ULL, 100'000ULL,
                                           5'000'001ULL));

struct RangeCase {
  char const* name;
  ReadRange range;
  std::uint64_t want;
};

class ReadRangeCaseTest : public ::testing::TestWithParam<RangeCase> {};

/// @test Valid ReadRange forms (whole object, offset-to-end, bounded segment,
/// clamped segment, negative tail offset) return the core's byte spans.
TEST_P(ReadRangeCaseTest, ValidRangeSpan) {
  RangeCase const& c = GetParam();
  StatusOr<Client> client = Client::Create(SyntheticOptions());
  ASSERT_THAT(client, IsOk());
  StatusOr<ObjectReader> reader =
      client->ReadObject("b", "sized/1000", c.range);
  ASSERT_THAT(reader, IsOk());
  StatusOr<std::uint64_t> got = ReadSize(*reader);
  ASSERT_THAT(got, IsOk());
  EXPECT_EQ(*got, c.want);
}

INSTANTIATE_TEST_SUITE_P(
    Ranges, ReadRangeCaseTest,
    ::testing::Values(RangeCase{"Full", {0, 0}, 1000},
                      RangeCase{"OffsetToEnd", {100, 0}, 900},
                      RangeCase{"Segment", {100, 50}, 50},
                      RangeCase{"ClampedAtEof", {990, 50}, 10},
                      RangeCase{"Tail", {-30, 0}, 30}),
    [](::testing::TestParamInfo<RangeCase> const& info) {
      return info.param.name;
    });

/// @test Out-of-range and malformed tail ranges surface OUT_OF_RANGE and
/// INVALID_ARGUMENT from the sidecar.
TEST(ClientTest, InvalidReadRanges) {
  StatusOr<Client> client = Client::Create(SyntheticOptions());
  ASSERT_THAT(client, IsOk());
  EXPECT_THAT(client->ReadObject("b", "sized/1000", {1000, 0}),
              StatusIs(StatusCode::kOutOfRange));
  EXPECT_THAT(client->ReadObject("b", "sized/1000", {-5, 1}),
              StatusIs(StatusCode::kInvalidArgument));
}

/// @test Errors from the sidecar surface with the same status code.
TEST(ClientTest, MissingObjectIsNotFound) {
  StatusOr<Client> client = Client::Create(SyntheticOptions());
  ASSERT_THAT(client, IsOk());
  EXPECT_THAT(client->ReadObject("b", "no-such-object"),
              StatusIs(StatusCode::kNotFound));
}

/// @test Single-shot uploads need size_hint up front; the wrapper rejects the
/// request before contacting the sidecar.
TEST(ClientTest, SingleShotRequiresSize) {
  StatusOr<Client> client = Client::Create(SyntheticOptions());
  ASSERT_THAT(client, IsOk());
  WriteOptions options;
  options.mode = UploadMode::kSingleShot;
  EXPECT_THAT(client->WriteObject("b", "o", options),
              StatusIs(StatusCode::kInvalidArgument));
}

/// @test Streaming writes across multiple Write() calls concatenate cleanly.
TEST(ClientTest, StreamingWrite) {
  StatusOr<Client> client = Client::Create(SyntheticOptions());
  ASSERT_THAT(client, IsOk());
  StatusOr<ObjectWriter> writer = client->WriteObject("b", "streamed-x");
  ASSERT_THAT(writer, IsOk());
  absl::Cord const part(std::string(3 * 1024 * 1024, 'z'));
  ASSERT_THAT(writer->Write(part), IsOk());
  ASSERT_THAT(writer->Write(part), IsOk());
  ASSERT_THAT(writer->Write(part), IsOk());
  StatusOr<ObjectMetadata> meta = writer->Close();
  ASSERT_THAT(meta, IsOk());
  EXPECT_EQ(meta->size, 9U * 1024 * 1024);
}

/// @test ListObjects paginates matching objects and DeleteObject removes them.
TEST(ClientTest, ListObjectsAndDeleteObject) {
  StatusOr<Client> client = Client::Create(SyntheticOptions());
  ASSERT_THAT(client, IsOk());
  ASSERT_THAT(client->InsertObject("list-b", "p/a-x", absl::Cord("1")), IsOk());
  ASSERT_THAT(client->InsertObject("list-b", "p/b-x", absl::Cord("22")),
              IsOk());
  ASSERT_THAT(client->InsertObject("list-b", "p/c-x", absl::Cord("333")),
              IsOk());
  ASSERT_THAT(client->InsertObject("list-b", "other/z-x", absl::Cord("4")),
              IsOk());

  ListObjectsRequest req;
  req.prefix = "p/";
  req.page_size = 2;
  StatusOr<ListObjectsPage> page1 = client->ListObjects("list-b", req);
  ASSERT_THAT(page1, IsOk());
  ASSERT_EQ(page1->objects.size(), 2U);
  EXPECT_EQ(page1->objects[0].name, "p/a-x");
  EXPECT_EQ(page1->objects[1].name, "p/b-x");
  EXPECT_FALSE(page1->next_page_token.empty());

  req.page_token = page1->next_page_token;
  StatusOr<ListObjectsPage> page2 = client->ListObjects("list-b", req);
  ASSERT_THAT(page2, IsOk());
  ASSERT_EQ(page2->objects.size(), 1U);
  EXPECT_EQ(page2->objects[0].name, "p/c-x");
  EXPECT_TRUE(page2->next_page_token.empty());

  ASSERT_THAT(client->DeleteObject("list-b", "p/b-x"), IsOk());
  EXPECT_THAT(client->ReadObject("list-b", "p/b-x"),
              StatusIs(StatusCode::kNotFound));
}

/// @test Concurrent ranges on one descriptor arrive tagged with their index
/// and complete.
TEST(ClientTest, DescriptorReadRanges) {
  StatusOr<Client> client = Client::Create(SyntheticOptions());
  ASSERT_THAT(client, IsOk());
  StatusOr<ObjectDescriptor> descriptor =
      client->OpenObject("b", "sized/10000000");
  ASSERT_THAT(descriptor, IsOk());
  EXPECT_EQ(descriptor->metadata().size, 10'000'000U);
  std::vector<std::uint64_t> got(3);
  Status s = descriptor->ReadRanges(
      {{0, 10}, {1'000'000, 3'000'000}, {-7, 0}},
      [&](std::size_t i, absl::Cord const& c) { got[i] += c.size(); });
  ASSERT_THAT(s, IsOk());
  EXPECT_THAT(got, ElementsAre(10, 3'000'000, 7));
}

/// @test Appendable uploads finalize with the total size; Close() leaves the
/// object unfinalized and reports the persisted size.
TEST(ClientTest, AppendableWrite) {
  StatusOr<Client> client = Client::Create(SyntheticOptions());
  ASSERT_THAT(client, IsOk());
  StatusOr<AppendableWriter> writer =
      client->StartAppendableUpload("b", "appendable-x");
  ASSERT_THAT(writer, IsOk());
  ASSERT_THAT(writer->Write(absl::Cord(std::string(4096, 'q'))), IsOk());
  StatusOr<ObjectMetadata> meta = writer->Finalize();
  ASSERT_THAT(meta, IsOk());
  EXPECT_EQ(meta->size, 4096U);

  StatusOr<AppendableWriter> open =
      client->StartAppendableUpload("b", "appendable-y");
  ASSERT_THAT(open, IsOk());
  ASSERT_THAT(open->Write(absl::Cord(std::string(10, 'q'))), IsOk());
  StatusOr<std::int64_t> persisted = open->Close();
  ASSERT_THAT(persisted, IsOk());
  EXPECT_EQ(*persisted, 10);
}

/// @test AppendableWrite rejects non-auto UploadMode, size_hint, and
/// non-terminal finalize messages across both wrapper and sidecar.
TEST(ClientTest, AppendableWriteValidatesSpecAndFinalize) {
  std::string const socket =
      "/tmp/gcs-core-append-val-" + std::to_string(::getpid()) + ".sock";
  ClientOptions client_opts = SyntheticOptions();
  client_opts.socket_path = socket;
  StatusOr<Client> client = Client::Create(client_opts);
  ASSERT_THAT(client, IsOk());

  WriteOptions single_shot;
  single_shot.mode = UploadMode::kSingleShot;
  single_shot.size_hint = 16;
  EXPECT_THAT(client->StartAppendableUpload("b", "bad-mode", single_shot),
              StatusIs(StatusCode::kInvalidArgument));

  WriteOptions with_size;
  with_size.size_hint = 16;
  EXPECT_THAT(client->StartAppendableUpload("b", "bad-size", with_size),
              StatusIs(StatusCode::kInvalidArgument));

  // Also probe the sidecar directly over UDS to verify wire-level validation of
  // upload_mode and non-terminal finalize=true.
  grpc::ChannelArguments args;
  args.SetString(GRPC_ARG_DEFAULT_AUTHORITY, "localhost");
  std::shared_ptr<grpc::Channel> ch = grpc::CreateCustomChannel(
      "unix:" + socket, grpc::InsecureChannelCredentials(), args);
  std::unique_ptr<::gcs::core::v0::StorageCore::Stub> stub =
      ::gcs::core::v0::StorageCore::NewStub(ch);

  {
    grpc::ClientContext ctx;
    ::gcs::core::v0::AppendableWriteResponse resp;
    std::unique_ptr<grpc::ClientWriter<::gcs::core::v0::AppendableWriteRequest>>
        w = stub->AppendableWrite(&ctx, &resp);
    ::gcs::core::v0::AppendableWriteRequest req;
    req.mutable_spec()->set_bucket("b");
    req.mutable_spec()->set_object("wire-bad-mode");
    req.mutable_spec()->set_upload_mode(
        ::gcs::core::v0::WriteSpec::UPLOAD_MODE_SINGLE_SHOT);
    (void)w->Write(req);
    (void)w->WritesDone();
    EXPECT_EQ(w->Finish().error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  }

  {
    grpc::ClientContext ctx;
    ::gcs::core::v0::AppendableWriteResponse resp;
    std::unique_ptr<grpc::ClientWriter<::gcs::core::v0::AppendableWriteRequest>>
        w = stub->AppendableWrite(&ctx, &resp);
    ::gcs::core::v0::AppendableWriteRequest first;
    first.mutable_spec()->set_bucket("b");
    first.mutable_spec()->set_object("wire-early-finalize");
    first.set_data(absl::Cord("hello"));
    first.set_finalize(true);
    (void)w->Write(first);
    ::gcs::core::v0::AppendableWriteRequest trailing;
    trailing.set_data(absl::Cord("world"));
    (void)w->Write(trailing);
    (void)w->WritesDone();
    EXPECT_EQ(w->Finish().error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  }
}

/// @test Clients with matching sidecar options share one sidecar while each
/// Client preserves its own per-RPC chunk_size_hint.
TEST(ClientTest, ChunkSizeHintIsPerClientSharingSidecar) {
  ClientOptions default_chunks = SyntheticOptions();
  ClientOptions small_chunks = SyntheticOptions();
  small_chunks.chunk_size_hint = 4096;

  StatusOr<Client> a = Client::Create(default_chunks);
  StatusOr<Client> b = Client::Create(small_chunks);
  ASSERT_THAT(a, IsOk());
  ASSERT_THAT(b, IsOk());
  EXPECT_EQ(a->sidecar_pid(), b->sidecar_pid());

  StatusOr<ObjectReader> ra = a->ReadObject("b", "sized/65536");
  StatusOr<ObjectReader> rb = b->ReadObject("b", "sized/65536");
  ASSERT_THAT(ra, IsOk());
  ASSERT_THAT(rb, IsOk());

  std::size_t max_a = 0;
  std::size_t max_b = 0;
  ASSERT_THAT(ReadSize(*ra, &max_a), IsOk());
  ASSERT_THAT(ReadSize(*rb, &max_b), IsOk());
  EXPECT_EQ(max_a, 65536U);
  EXPECT_EQ(max_b, 4096U);
}

/// @test Clients with distinct sidecar_args do not silently drop options onto
/// an existing Connection, and conflicting explicit socket_paths are rejected.
TEST(ClientTest, DifferentOptionsUseSeparateSidecars) {
  ClientOptions base = SyntheticOptions();
  ClientOptions custom_sidecar = SyntheticOptions();
  custom_sidecar.sidecar_args = {"--synthetic", "--chunk-size=4096"};

  StatusOr<Client> a = Client::Create(base);
  StatusOr<Client> b = Client::Create(custom_sidecar);
  ASSERT_THAT(a, IsOk());
  ASSERT_THAT(b, IsOk());
  EXPECT_NE(a->sidecar_pid(), b->sidecar_pid());

  StatusOr<ObjectReader> rb = b->ReadObject("b", "sized/65536");
  ASSERT_THAT(rb, IsOk());
  std::size_t max_b = 0;
  ASSERT_THAT(ReadSize(*rb, &max_b), IsOk());
  EXPECT_EQ(max_b, 4096U);

  std::string const explicit_sock =
      "/tmp/gcs-core-conflict-" + std::to_string(::getpid()) + ".sock";
  ClientOptions c1 = SyntheticOptions();
  c1.socket_path = explicit_sock;
  StatusOr<Client> pinned = Client::Create(c1);
  ASSERT_THAT(pinned, IsOk());

  ClientOptions c2 = custom_sidecar;
  c2.socket_path = explicit_sock;
  EXPECT_THAT(Client::Create(c2), StatusIs(StatusCode::kFailedPrecondition));
}

/// @test Destroying the last Client stops the sidecar and removes its socket.
TEST(ClientTest, TeardownStopsSidecar) {
  int pid = 0;
  std::string const socket =
      "/tmp/gcs-core-teardown-" + std::to_string(::getpid()) + ".sock";
  {
    ClientOptions options = SyntheticOptions();
    options.socket_path = socket;
    StatusOr<Client> client = Client::Create(options);
    ASSERT_THAT(client, IsOk());
    pid = client->sidecar_pid();
    struct stat st{};
    EXPECT_EQ(::stat(socket.c_str(), &st), 0);
    EXPECT_TRUE(ProcessAlive(pid));
  }
  EXPECT_FALSE(ProcessAlive(pid));
  struct stat st{};
  EXPECT_NE(::stat(socket.c_str(), &st), 0);
}

/// @test If the sidecar dies, the next failing call reports UNAVAILABLE and
/// restarts it, so the call after that succeeds.
TEST(ClientTest, RespawnAfterCrash) {
  StatusOr<Client> client = Client::Create(SyntheticOptions());
  ASSERT_THAT(client, IsOk());
  int const first = client->sidecar_pid();
  ASSERT_EQ(::kill(first, SIGKILL), 0);
  WaitForExit(first);
  EXPECT_THAT(client->GetSidecarVersion(), StatusIs(StatusCode::kUnavailable));
  StatusOr<SidecarVersion> again = client->GetSidecarVersion();
  ASSERT_THAT(again, IsOk());
  EXPECT_NE(client->sidecar_pid(), first);
}

/// @test An ObjectDescriptor opened before a sidecar crash cannot alias a new
/// handle after respawn, nor can its destructor close the new handle.
TEST(ClientTest, StaleDescriptorRejectedAfterRespawn) {
  StatusOr<Client> client = Client::Create(SyntheticOptions());
  ASSERT_THAT(client, IsOk());

  std::optional<ObjectDescriptor> stale;
  {
    StatusOr<ObjectDescriptor> d1 = client->OpenObject("b", "sized/1000");
    ASSERT_THAT(d1, IsOk());
    stale.emplace(*std::move(d1));
  }

  int const first_pid = client->sidecar_pid();
  ASSERT_EQ(::kill(first_pid, SIGKILL), 0);
  WaitForExit(first_pid);
  EXPECT_THAT(client->GetSidecarVersion(), StatusIs(StatusCode::kUnavailable));

  StatusOr<ObjectDescriptor> fresh = client->OpenObject("b", "sized/7777");
  ASSERT_THAT(fresh, IsOk());
  EXPECT_NE(client->sidecar_pid(), first_pid);

  // Reading on the pre-crash descriptor must fail rather than reading 7777 B.
  std::uint64_t stale_bytes = 0;
  Status stale_read = stale->ReadRanges(
      {{0, 0}},
      [&](std::size_t, absl::Cord const& c) { stale_bytes += c.size(); });
  EXPECT_THAT(stale_read, StatusIs(StatusCode::kFailedPrecondition));
  EXPECT_EQ(stale_bytes, 0U);

  // Destroying the stale descriptor must not close `fresh` in the new sidecar.
  stale.reset();
  std::uint64_t fresh_bytes = 0;
  Status fresh_read = fresh->ReadRanges(
      {{0, 0}},
      [&](std::size_t, absl::Cord const& c) { fresh_bytes += c.size(); });
  ASSERT_THAT(fresh_read, IsOk());
  EXPECT_EQ(fresh_bytes, 7777U);
}

/// @test Destroying an ObjectDescriptor while its sidecar is stopped is bounded
/// by CloseObject's deadline rather than blocking indefinitely.
TEST(ClientTest, DestructorBoundsCloseWhenSidecarStopped) {
  ClientOptions options = SyntheticOptions();
  options.socket_path =
      "/tmp/gcs-core-sigstop-" + std::to_string(::getpid()) + ".sock";
  StatusOr<Client> client = Client::Create(options);
  ASSERT_THAT(client, IsOk());
  StatusOr<ObjectDescriptor> descriptor = client->OpenObject("b", "sized/1024");
  ASSERT_THAT(descriptor, IsOk());

  int const pid = client->sidecar_pid();
  ASSERT_EQ(::kill(pid, SIGSTOP), 0);

  std::mutex mu;
  std::condition_variable cv;
  bool done = false;
  std::thread watchdog([&] {
    std::unique_lock<std::mutex> lk(mu);
    if (!cv.wait_for(lk, std::chrono::milliseconds(1800),
                     [&] { return done; })) {
      ::kill(pid, SIGCONT);
    }
  });

  std::chrono::steady_clock::time_point const start =
      std::chrono::steady_clock::now();
  {
    ObjectDescriptor closing = *std::move(descriptor);
  }
  std::chrono::steady_clock::duration const elapsed =
      std::chrono::steady_clock::now() - start;

  {
    std::scoped_lock lk(mu);
    done = true;
  }
  ::kill(pid, SIGCONT);
  cv.notify_all();
  watchdog.join();

  EXPECT_LT(elapsed, std::chrono::milliseconds(1200));
}

/// @test Killing the sidecar mid-stream surfaces UNAVAILABLE on Next() and
/// automatically recovers the client for subsequent reads.
TEST(ClientTest, SidecarCrashMidRead) {
  StatusOr<Client> client = Client::Create(SyntheticOptions());
  ASSERT_THAT(client, IsOk());
  StatusOr<ObjectReader> reader = client->ReadObject("b", "sized/50000000");
  ASSERT_THAT(reader, IsOk());
  StatusOr<std::optional<absl::Cord>> first_chunk = reader->Next();
  ASSERT_THAT(first_chunk, IsOk());
  ASSERT_TRUE(first_chunk->has_value());

  int const pid = client->sidecar_pid();
  ASSERT_EQ(::kill(pid, SIGKILL), 0);
  WaitForExit(pid);

  StatusOr<std::uint64_t> rest = ReadSize(*reader);
  EXPECT_THAT(rest, StatusIs(StatusCode::kUnavailable));

  StatusOr<ObjectReader> recovered = client->ReadObject("b", "sized/1024");
  ASSERT_THAT(recovered, IsOk());
  StatusOr<std::uint64_t> got = ReadSize(*recovered);
  ASSERT_THAT(got, IsOk());
  EXPECT_EQ(*got, 1024U);
}

/// @test A sidecar process that never binds its socket times out cleanly and
/// is terminated rather than leaked.
TEST(ClientTest, ReadinessTimeout) {
  std::string const script = WriteSleepScript("timeout");
  ClientOptions options;
  options.sidecar_path = script;
  options.socket_path =
      "/tmp/gcs-core-never-ready-" + std::to_string(::getpid()) + ".sock";
  options.readiness_timeout = std::chrono::milliseconds(150);

  StatusOr<Client> client = Client::Create(options);
  ::unlink(script.c_str());
  EXPECT_THAT(client, StatusIs(StatusCode::kDeadlineExceeded));
}

/// @test Invalid options fail Create() without starting anything, even while a
/// valid Client is alive.
TEST(ClientTest, InvalidOptions) {
  ClientOptions zero_channels = SyntheticOptions();
  zero_channels.channels = 0;
  EXPECT_THAT(Client::Create(zero_channels),
              StatusIs(StatusCode::kInvalidArgument));

  ClientOptions zero_timeout = SyntheticOptions();
  zero_timeout.readiness_timeout = std::chrono::milliseconds(0);
  EXPECT_THAT(Client::Create(zero_timeout),
              StatusIs(StatusCode::kInvalidArgument));

  ClientOptions neg_timeout = SyntheticOptions();
  neg_timeout.readiness_timeout = std::chrono::milliseconds(-5);
  EXPECT_THAT(Client::Create(neg_timeout),
              StatusIs(StatusCode::kInvalidArgument));

  ClientOptions long_socket = SyntheticOptions();
  long_socket.socket_path = "/tmp/" + std::string(200, 's');
  EXPECT_THAT(Client::Create(long_socket),
              StatusIs(StatusCode::kInvalidArgument));

  std::string const regular_file =
      "/tmp/gcs-core-client-reg-" + std::to_string(::getpid()) + ".txt";
  {
    std::ofstream out(regular_file);
    out << "keep";
  }
  ClientOptions regular_socket = SyntheticOptions();
  regular_socket.socket_path = regular_file;
  EXPECT_THAT(Client::Create(regular_socket),
              StatusIs(StatusCode::kFailedPrecondition));
  struct stat reg_st{};
  EXPECT_EQ(::lstat(regular_file.c_str(), &reg_st), 0);
  ::unlink(regular_file.c_str());

  ClientOptions bad_binary = SyntheticOptions();
  bad_binary.sidecar_path = "/nonexistent/gcs-core-sidecar";
  EXPECT_THAT(Client::Create(bad_binary),
              StatusIs(StatusCode::kFailedPrecondition));

  StatusOr<Client> live = Client::Create(SyntheticOptions());
  ASSERT_THAT(live, IsOk());
  EXPECT_THAT(Client::Create(zero_channels),
              StatusIs(StatusCode::kInvalidArgument));
  EXPECT_THAT(Client::Create(long_socket),
              StatusIs(StatusCode::kInvalidArgument));
  EXPECT_THAT(Client::Create(bad_binary),
              StatusIs(StatusCode::kFailedPrecondition));
}

/// @test A sidecar that exits immediately (bad flag) is reported with its
/// exit status instead of waiting for the readiness timeout.
TEST(ClientTest, SidecarExitsBeforeReady) {
  ClientOptions options;
  options.sidecar_args = {"--no-such-flag"};
  StatusOr<Client> client = Client::Create(options);
  EXPECT_THAT(client, StatusIs(StatusCode::kFailedPrecondition));
  EXPECT_THAT(client.status().message(), HasSubstr("exited with status 2"));
}

/// @test With multiple UDS channels, round-robin calls all succeed.
TEST(ClientTest, MultipleChannels) {
  ClientOptions options = SyntheticOptions();
  options.channels = 4;
  StatusOr<Client> client = Client::Create(options);
  ASSERT_THAT(client, IsOk());
  ASSERT_THAT(client->GetSidecarStats(), IsOk());
  ASSERT_THAT(client->GetSidecarStats(), IsOk());
  ASSERT_THAT(client->GetSidecarStats(), IsOk());
  ASSERT_THAT(client->GetSidecarStats(), IsOk());
  ASSERT_THAT(client->GetSidecarStats(), IsOk());
}

/// @test GetSidecarStats reports live process counters that grow with work.
TEST(ClientTest, SidecarStatsGrow) {
  StatusOr<Client> client = Client::Create(SyntheticOptions());
  ASSERT_THAT(client, IsOk());
  StatusOr<SidecarStats> before = client->GetSidecarStats();
  ASSERT_THAT(before, IsOk());
  StatusOr<ObjectReader> reader = client->ReadObject("b", "sized/100000000");
  ASSERT_THAT(reader, IsOk());
  StatusOr<std::uint64_t> got = ReadSize(*reader);
  ASSERT_THAT(got, IsOk());
  EXPECT_EQ(*got, 100'000'000U);
  StatusOr<SidecarStats> after = client->GetSidecarStats();
  ASSERT_THAT(after, IsOk());
  EXPECT_GT(after->rss_bytes, 0U);
  EXPECT_GE(after->user_cpu_us + after->system_cpu_us,
            before->user_cpu_us + before->system_cpu_us);
  EXPECT_GT(after->allocated_bytes, before->allocated_bytes);
}

}  // namespace
}  // namespace gcs_core
