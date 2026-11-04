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
// binary discovery, the child's exit paths and the wire probe over `unix:`.
// Everything that holds for both variants is in client_common_test.cc.

#include "gcs/core/v0/storage_core.grpc.pb.h"
#include "gcs_core/client.h"
#include "test_support.h"
#include <gmock/gmock.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>
#include <sys/stat.h>
#include <unistd.h>

namespace gcs_core {
namespace {

using ::gcs_core::testing::IsOk;
using ::gcs_core::testing::ProcessAlive;
using ::gcs_core::testing::ReadSize;
using ::gcs_core::testing::StatusIs;
using ::gcs_core::testing::SyntheticOptions;
using ::google::cloud::StatusCode;
using ::testing::HasSubstr;

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

/// @test The core validates upload_mode and a non-terminal finalize=true at
/// the wire level; probed directly over the sidecar's socket.
TEST(SidecarClientTest, AppendableWriteWireValidation) {
  std::string const socket =
      "/tmp/gcs-core-append-val-" + std::to_string(::getpid()) + ".sock";
  ClientOptions client_opts = SyntheticOptions();
  client_opts.socket_path = socket;
  StatusOr<Client> client = Client::Create(client_opts);
  ASSERT_THAT(client, IsOk());

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

/// @test Clients with distinct sidecar_args do not silently drop options onto
/// an existing Connection, and conflicting explicit socket_paths are rejected.
TEST(SidecarClientTest, DifferentOptionsUseSeparateSidecars) {
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
TEST(SidecarClientTest, TeardownStopsSidecar) {
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

/// @test Destroying an ObjectDescriptor while its sidecar is stopped is bounded
/// by CloseObject's deadline rather than blocking indefinitely.
TEST(SidecarClientTest, DestructorBoundsCloseWhenSidecarStopped) {
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

/// @test A sidecar that exits immediately (bad flag) is reported with its
/// exit status instead of waiting for the readiness timeout.
TEST(SidecarClientTest, SidecarExitsBeforeReady) {
  ClientOptions options;
  options.sidecar_args = {"--no-such-flag"};
  StatusOr<Client> client = Client::Create(options);
  EXPECT_THAT(client, StatusIs(StatusCode::kFailedPrecondition));
  EXPECT_THAT(client.status().message(), HasSubstr("exited with status 2"));
}

/// @test Socket and binary options only the sidecar variant has fail Create()
/// with the matching code and never touch a regular file at the socket path.
TEST(SidecarClientTest, InvalidSocketAndBinaryOptions) {
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
  EXPECT_THAT(Client::Create(long_socket),
              StatusIs(StatusCode::kInvalidArgument));
  EXPECT_THAT(Client::Create(bad_binary),
              StatusIs(StatusCode::kFailedPrecondition));
}

}  // namespace
}  // namespace gcs_core
