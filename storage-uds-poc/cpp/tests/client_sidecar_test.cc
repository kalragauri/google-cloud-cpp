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

// Tests of the sidecar (process-boundary) variant only: socket paths,
// binary discovery and the child's exit paths.
// Everything that holds for both variants is in client_common_test.cc.

#include "gcs_core/client.h"
#include "test_support.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <chrono>
#include <fstream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

namespace gcs_core {
namespace {

using ::gcs_core::testing::IsOk;
using ::gcs_core::testing::ProcessAlive;
using ::gcs_core::testing::ServerOptions;
using ::gcs_core::testing::StatusIs;
using ::google::cloud::StatusCode;

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

/// @test Clients with distinct sidecar_args get their own server rather than
/// silently dropping options onto an existing one, and a second
/// configuration cannot claim a socket path already in use.
TEST(SidecarClientTest, DifferentOptionsUseSeparateSidecars) {
  ClientOptions base = ServerOptions();
  ClientOptions custom = ServerOptions();
  custom.sidecar_args.emplace_back("--chunk-size=4096");

  StatusOr<Client> a = Client::Create(base);
  StatusOr<Client> b = Client::Create(custom);
  ASSERT_THAT(a, IsOk());
  ASSERT_THAT(b, IsOk());
  EXPECT_NE(a->sidecar_pid(), b->sidecar_pid());

  std::string const explicit_sock =
      "/tmp/gcs-core-conflict-" + std::to_string(::getpid()) + ".sock";
  ClientOptions c1 = ServerOptions();
  c1.socket_path = explicit_sock;
  StatusOr<Client> pinned = Client::Create(c1);
  ASSERT_THAT(pinned, IsOk());

  ClientOptions c2 = custom;
  c2.socket_path = explicit_sock;
  EXPECT_THAT(Client::Create(c2), StatusIs(StatusCode::kFailedPrecondition));
}

/// @test Destroying the last Client stops the sidecar and removes its socket.
TEST(SidecarClientTest, TeardownStopsSidecar) {
  int pid = 0;
  std::string const socket =
      "/tmp/gcs-core-teardown-" + std::to_string(::getpid()) + ".sock";
  {
    ClientOptions options = ServerOptions();
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

/// @test A sidecar process that never binds its socket times out cleanly and
/// is terminated rather than leaked.
TEST(SidecarClientTest, ReadinessTimeout) {
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

/// @test Socket and binary options only the sidecar variant has fail Create()
/// with the matching code and never touch a regular file at the socket path.
TEST(SidecarClientTest, InvalidSocketAndBinaryOptions) {
  ClientOptions long_socket = ServerOptions();
  long_socket.socket_path = "/tmp/" + std::string(200, 's');
  EXPECT_THAT(Client::Create(long_socket),
              StatusIs(StatusCode::kInvalidArgument));

  std::string const regular_file =
      "/tmp/gcs-core-client-reg-" + std::to_string(::getpid()) + ".txt";
  {
    std::ofstream out(regular_file);
    out << "keep";
  }
  ClientOptions regular_socket = ServerOptions();
  regular_socket.socket_path = regular_file;
  EXPECT_THAT(Client::Create(regular_socket),
              StatusIs(StatusCode::kFailedPrecondition));
  struct stat reg_st{};
  EXPECT_EQ(::lstat(regular_file.c_str(), &reg_st), 0);
  ::unlink(regular_file.c_str());

  ClientOptions bad_binary = ServerOptions();
  bad_binary.sidecar_path = "/nonexistent/gcs-core-server";
  EXPECT_THAT(Client::Create(bad_binary),
              StatusIs(StatusCode::kFailedPrecondition));

  StatusOr<Client> live = Client::Create(ServerOptions());
  ASSERT_THAT(live, IsOk());
  EXPECT_THAT(Client::Create(long_socket),
              StatusIs(StatusCode::kInvalidArgument));
  EXPECT_THAT(Client::Create(bad_binary),
              StatusIs(StatusCode::kFailedPrecondition));
}

}  // namespace
}  // namespace gcs_core
