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

// Wrapper tests that hold for both variants, against the real shared server
// with no GCS access (see `ServerOptions()`): start, readiness, restart after a
// crash, option validation and the lifecycle RPCs. Compiled once per variant;
// see test_support.h.

#include "gcs_core/client.h"
#include "test_support.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>
#include <unistd.h>

namespace gcs_core {
namespace {

using ::gcs_core::testing::CrashCore;
using ::gcs_core::testing::IsOk;
using ::gcs_core::testing::kInProcess;
using ::gcs_core::testing::ServerOptions;
using ::gcs_core::testing::StatusIs;
using ::google::cloud::StatusCode;
using ::testing::HasSubstr;

/// @test The server reports the pid of the process serving it: the sidecar
/// child, or this process in-process.
TEST(ClientTest, VersionReportsServingPid) {
  StatusOr<Client> client = Client::Create(ServerOptions());
  ASSERT_THAT(client, IsOk());
  StatusOr<SidecarVersion> version = client->GetSidecarVersion();
  ASSERT_THAT(version, IsOk());
  EXPECT_EQ(static_cast<int>(version->pid), client->sidecar_pid());
  if (kInProcess) {
    EXPECT_EQ(client->sidecar_pid(), ::getpid());
  } else {
    EXPECT_NE(client->sidecar_pid(), ::getpid());
  }
}

/// @test If the core dies, the next failing call reports UNAVAILABLE and
/// restarts it, so the call after that succeeds (a sidecar gets a new pid;
/// an in-process core keeps this process's pid).
TEST(ClientTest, RestartAfterCrash) {
  StatusOr<Client> client = Client::Create(ServerOptions());
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

/// @test Options every variant validates fail Create() without starting
/// anything, even while a valid Client is alive.
TEST(ClientTest, InvalidOptions) {
  ClientOptions zero_channels = ServerOptions();
  zero_channels.channels = 0;
  EXPECT_THAT(Client::Create(zero_channels),
              StatusIs(StatusCode::kInvalidArgument));

  ClientOptions zero_timeout = ServerOptions();
  zero_timeout.readiness_timeout = std::chrono::milliseconds(0);
  EXPECT_THAT(Client::Create(zero_timeout),
              StatusIs(StatusCode::kInvalidArgument));

  ClientOptions neg_timeout = ServerOptions();
  neg_timeout.readiness_timeout = std::chrono::milliseconds(-5);
  EXPECT_THAT(Client::Create(neg_timeout),
              StatusIs(StatusCode::kInvalidArgument));

  StatusOr<Client> live = Client::Create(ServerOptions());
  ASSERT_THAT(live, IsOk());
  EXPECT_THAT(Client::Create(zero_channels),
              StatusIs(StatusCode::kInvalidArgument));
}

/// @test With multiple channels, round-robin calls all succeed.
TEST(ClientTest, MultipleChannels) {
  ClientOptions options = ServerOptions();
  options.channels = 4;
  StatusOr<Client> client = Client::Create(options);
  ASSERT_THAT(client, IsOk());
  ASSERT_THAT(client->GetSidecarStats(), IsOk());
  ASSERT_THAT(client->GetSidecarStats(), IsOk());
  ASSERT_THAT(client->GetSidecarStats(), IsOk());
  ASSERT_THAT(client->GetSidecarStats(), IsOk());
  ASSERT_THAT(client->GetSidecarStats(), IsOk());
}

/// @test GetSidecarStats reports live counters of the serving process: CPU
/// never goes backwards and RSS is non-zero. The shared server does not count
/// allocations, so `allocated_bytes` is always 0.
TEST(ClientTest, StatsAreLive) {
  StatusOr<Client> client = Client::Create(ServerOptions());
  ASSERT_THAT(client, IsOk());
  StatusOr<SidecarStats> before = client->GetSidecarStats();
  ASSERT_THAT(before, IsOk());
  StatusOr<SidecarStats> after = client->GetSidecarStats();
  ASSERT_THAT(after, IsOk());
  EXPECT_GT(after->rss_bytes, 0U);
  EXPECT_GT(after->threads, 0U);
  EXPECT_GE(after->user_cpu_us + after->system_cpu_us,
            before->user_cpu_us + before->system_cpu_us);
  EXPECT_EQ(after->allocated_bytes, 0U);
}

class ReservedFlagTest : public ::testing::TestWithParam<char const*> {};

/// @test Flags the wrapper owns (the socket, child mode, and the TCP listener
/// that would bypass the UDS boundary) are rejected in both variants before
/// anything starts, rather than conflicting with the wrapper's own values.
TEST_P(ReservedFlagTest, RejectedBeforeStart) {
  ClientOptions options = ServerOptions();
  options.sidecar_args.emplace_back(GetParam());
  StatusOr<Client> client = Client::Create(options);
  EXPECT_THAT(client, StatusIs(StatusCode::kInvalidArgument));
  std::string const flag(GetParam());
  EXPECT_THAT(client.status().message(),
              HasSubstr(flag.substr(0, flag.find('='))));
}

INSTANTIATE_TEST_SUITE_P(Flags, ReservedFlagTest,
                         ::testing::Values("--socket=/tmp/x.sock", "--socket",
                                           "--pdeathsig=false", "--pdeathsig",
                                           "--tcp=127.0.0.1:0", "--tcp"));

/// @test An unknown server flag fails Create(): the sidecar exits with clap's
/// usage status before binding, and the in-process parser reports the flag.
TEST(ClientTest, UnknownFlagFailsCreate) {
  ClientOptions options = ServerOptions();
  options.sidecar_args.emplace_back("--no-such-flag");
  StatusOr<Client> client = Client::Create(options);
  if (kInProcess) {
    EXPECT_THAT(client, StatusIs(StatusCode::kInvalidArgument));
    EXPECT_THAT(client.status().message(), HasSubstr("--no-such-flag"));
  } else {
    EXPECT_THAT(client, StatusIs(StatusCode::kFailedPrecondition));
    EXPECT_THAT(client.status().message(), HasSubstr("exited with status 2"));
  }
}

}  // namespace
}  // namespace gcs_core
