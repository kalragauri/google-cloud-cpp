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

// Wrapper tests that hold for both variants, against the real core in
// --synthetic mode: the full wrapper -> gRPC -> core path runs, without
// external GCS traffic. Compiled once per variant; see test_support.h.

#include "gcs_core/client.h"
#include "test_support.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace gcs_core {
namespace {

using ::gcs_core::testing::CrashCore;
using ::gcs_core::testing::IsOk;
using ::gcs_core::testing::kInProcess;
using ::gcs_core::testing::ReadSize;
using ::gcs_core::testing::StatusIs;
using ::gcs_core::testing::SyntheticOptions;
using ::google::cloud::StatusCode;
using ::testing::ElementsAre;

/// @test The core reports synthetic mode, the pinned crate version and the
/// pid of the process serving it.
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

/// @test StartAppendableUpload rejects non-auto UploadMode and size_hint in
/// the wrapper, before any RPC.
TEST(ClientTest, AppendableWriteValidatesSpec) {
  StatusOr<Client> client = Client::Create(SyntheticOptions());
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
}

/// @test Clients with matching core options share one core while each
/// Client preserves its own per-RPC chunk_size_hint.
TEST(ClientTest, ChunkSizeHintIsPerClientSharingCore) {
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

/// @test If the core dies, the next failing call reports UNAVAILABLE and
/// restarts it, so the call after that succeeds (a sidecar gets a new pid;
/// an in-process core keeps this process's pid).
TEST(ClientTest, RestartAfterCrash) {
  StatusOr<Client> client = Client::Create(SyntheticOptions());
  ASSERT_THAT(client, IsOk());
  int const first = client->sidecar_pid();
  CrashCore(*client);
  EXPECT_THAT(client->GetSidecarVersion(), StatusIs(StatusCode::kUnavailable));
  StatusOr<SidecarVersion> again = client->GetSidecarVersion();
  ASSERT_THAT(again, IsOk());
  if (kInProcess) {
    EXPECT_EQ(client->sidecar_pid(), first);
  } else {
    EXPECT_NE(client->sidecar_pid(), first);
  }
}

/// @test An ObjectDescriptor opened before a core crash cannot alias a new
/// handle after the restart, nor can its destructor close the new handle.
TEST(ClientTest, StaleDescriptorRejectedAfterRestart) {
  StatusOr<Client> client = Client::Create(SyntheticOptions());
  ASSERT_THAT(client, IsOk());

  std::optional<ObjectDescriptor> stale;
  {
    StatusOr<ObjectDescriptor> d1 = client->OpenObject("b", "sized/1000");
    ASSERT_THAT(d1, IsOk());
    stale.emplace(*std::move(d1));
  }

  CrashCore(*client);
  EXPECT_THAT(client->GetSidecarVersion(), StatusIs(StatusCode::kUnavailable));

  StatusOr<ObjectDescriptor> fresh = client->OpenObject("b", "sized/7777");
  ASSERT_THAT(fresh, IsOk());

  // Reading on the pre-crash descriptor must fail rather than reading 7777 B.
  std::uint64_t stale_bytes = 0;
  Status stale_read = stale->ReadRanges(
      {{0, 0}},
      [&](std::size_t, absl::Cord const& c) { stale_bytes += c.size(); });
  EXPECT_THAT(stale_read, StatusIs(StatusCode::kFailedPrecondition));
  EXPECT_EQ(stale_bytes, 0U);

  // Destroying the stale descriptor must not close `fresh` in the new core.
  stale.reset();
  std::uint64_t fresh_bytes = 0;
  Status fresh_read = fresh->ReadRanges(
      {{0, 0}},
      [&](std::size_t, absl::Cord const& c) { fresh_bytes += c.size(); });
  ASSERT_THAT(fresh_read, IsOk());
  EXPECT_EQ(fresh_bytes, 7777U);
}

/// @test A core crash mid-stream surfaces UNAVAILABLE on Next() and
/// automatically recovers the client for subsequent reads.
TEST(ClientTest, CoreCrashMidRead) {
  StatusOr<Client> client = Client::Create(SyntheticOptions());
  ASSERT_THAT(client, IsOk());
  StatusOr<ObjectReader> reader = client->ReadObject("b", "sized/50000000");
  ASSERT_THAT(reader, IsOk());
  StatusOr<std::optional<absl::Cord>> first_chunk = reader->Next();
  ASSERT_THAT(first_chunk, IsOk());
  ASSERT_TRUE(first_chunk->has_value());

  CrashCore(*client);

  StatusOr<std::uint64_t> rest = ReadSize(*reader);
  EXPECT_THAT(rest, StatusIs(StatusCode::kUnavailable));

  StatusOr<ObjectReader> recovered = client->ReadObject("b", "sized/1024");
  ASSERT_THAT(recovered, IsOk());
  StatusOr<std::uint64_t> got = ReadSize(*recovered);
  ASSERT_THAT(got, IsOk());
  EXPECT_EQ(*got, 1024U);
}

/// @test Options every variant validates fail Create() without starting
/// anything, even while a valid Client is alive.
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

  StatusOr<Client> live = Client::Create(SyntheticOptions());
  ASSERT_THAT(live, IsOk());
  EXPECT_THAT(Client::Create(zero_channels),
              StatusIs(StatusCode::kInvalidArgument));
}

/// @test With multiple channels, round-robin calls all succeed.
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
/// Allocation counting is a sidecar feature: in-process the core allocates
/// through this program's malloc and reports 0.
TEST(ClientTest, StatsGrow) {
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
  if (kInProcess) {
    EXPECT_EQ(after->allocated_bytes, 0U);
  } else {
    EXPECT_GT(after->allocated_bytes, before->allocated_bytes);
  }
}

}  // namespace
}  // namespace gcs_core
