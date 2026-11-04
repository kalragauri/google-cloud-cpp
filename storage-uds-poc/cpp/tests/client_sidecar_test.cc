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

// Tests of the sidecar (process-boundary) variant only: daemon vs child
// lifecycle, cross-process daemon sharing and concurrent startup races, parent
// crash cleanup, socket paths, and binary discovery. Everything that holds for
// both variants is in client_common_test.cc.

#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "gcs_core/client.h"
#include "sidecar_process.h"
#include "test_support.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>
#include <spawn.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

namespace gcs_core {
namespace {

using ::gcs_core::testing::IsOk;
using ::gcs_core::testing::ProcessAlive;
using ::gcs_core::testing::ServerOptions;
using ::gcs_core::testing::StatusIs;
using ::gcs_core::testing::WaitForExit;
using ::google::cloud::StatusCode;
using ::testing::Each;
using ::testing::Gt;

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

std::string ColdStartBinary() {
  char const* env = std::getenv("GCS_CORE_COLD_START");
  return env != nullptr ? env : "";
}

std::string UniqueSocket(std::string_view tag) {
  return "/tmp/gcs-core-" + std::string(tag) + "-" +
         std::to_string(::getpid()) + ".sock";
}

ClientOptions DaemonOptions(std::string const& socket_path) {
  ClientOptions options = ServerOptions();
  options.sidecar_mode = SidecarMode::kDaemon;
  options.socket_path = socket_path;
  return options;
}

/// Ensures any daemon listening on `socket_` at test end is terminated and its
/// socket files are cleaned up.
class ScopedDaemonCleanup {
 public:
  explicit ScopedDaemonCleanup(std::string socket)
      : socket_(std::move(socket)) {}
  ~ScopedDaemonCleanup() {
    if (std::optional<pid_t> pid = internal::ProbeSocketListener(socket_)) {
      ::kill(*pid, SIGTERM);
      WaitForExit(*pid);
      if (ProcessAlive(*pid)) {
        ::kill(*pid, SIGKILL);
        WaitForExit(*pid);
      }
      int status = 0;
      (void)::waitpid(*pid, &status, WNOHANG);
    }
    internal::RemoveSocketFiles(socket_);
  }

 private:
  std::string socket_;
};

/// One spawned `cold_start` helper process with its stdout captured on a pipe.
struct SpawnedColdStart {
  pid_t pid = 0;
  int stdout_fd = -1;
};

SpawnedColdStart StartColdStart(std::vector<std::string> const& extra_flags) {
  std::string const bin = ColdStartBinary();
  int pipefd[2] = {-1, -1};
  if (bin.empty() || ::pipe(pipefd) != 0) return {};

  std::vector<std::string> args = {
      bin,
      "--sidecar-arg=--endpoint=http://127.0.0.1:1",
      "--sidecar-arg=--shm-size=0",
      "--sidecar-arg=--grpc-subchannels=1",
      "--sidecar-arg=--worker-threads=1",
  };
  args.insert(args.end(), extra_flags.begin(), extra_flags.end());
  std::vector<char*> argv;
  argv.reserve(args.size() + 1);
  for (std::string& a : args) argv.push_back(a.data());
  argv.push_back(nullptr);

  posix_spawn_file_actions_t actions;
  ::posix_spawn_file_actions_init(&actions);
  ::posix_spawn_file_actions_addclose(&actions, pipefd[0]);
  ::posix_spawn_file_actions_adddup2(&actions, pipefd[1], STDOUT_FILENO);
  ::posix_spawn_file_actions_addclose(&actions, pipefd[1]);

  pid_t pid = 0;
  int const rc =
      ::posix_spawn(&pid, bin.c_str(), &actions, nullptr, argv.data(), environ);
  ::posix_spawn_file_actions_destroy(&actions);
  ::close(pipefd[1]);
  if (rc != 0) {
    ::close(pipefd[0]);
    return {};
  }
  return {pid, pipefd[0]};
}

int ParseCorePid(std::string const& output) {
  std::istringstream lines(output);
  std::string line;
  while (std::getline(lines, line)) {
    constexpr std::string_view kPrefix = "core_pid=";
    if (absl::StartsWith(line, kPrefix)) {
      int pid = 0;
      if (absl::SimpleAtoi(std::string_view(line).substr(kPrefix.size()),
                           &pid)) {
        return pid;
      }
    }
  }
  return 0;
}

struct ColdStartResult {
  int exit_code = -1;
  int core_pid = 0;
  std::string stdout_text;
};

ColdStartResult FinishColdStart(SpawnedColdStart proc) {
  ColdStartResult out;
  char buf[512];
  for (;;) {
    ssize_t const n = ::read(proc.stdout_fd, buf, sizeof(buf));
    if (n <= 0) break;
    out.stdout_text.append(buf, static_cast<std::size_t>(n));
  }
  ::close(proc.stdout_fd);
  int status = 0;
  while (::waitpid(proc.pid, &status, 0) < 0 && errno == EINTR) {
  }
  if (WIFEXITED(status)) out.exit_code = WEXITSTATUS(status);
  out.core_pid = ParseCorePid(out.stdout_text);
  return out;
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

  std::string const explicit_sock = UniqueSocket("conflict");
  ClientOptions c1 = ServerOptions();
  c1.socket_path = explicit_sock;
  StatusOr<Client> pinned = Client::Create(c1);
  ASSERT_THAT(pinned, IsOk());

  ClientOptions c2 = custom;
  c2.socket_path = explicit_sock;
  EXPECT_THAT(Client::Create(c2), StatusIs(StatusCode::kFailedPrecondition));
}

/// @test In 1:1 child mode, destroying the last Client stops the sidecar and
/// removes its socket.
TEST(SidecarClientTest, TeardownStopsSidecar) {
  int pid = 0;
  std::string const socket = UniqueSocket("teardown");
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

/// @test In default daemon mode, the first Client spawns the daemon and leaves
/// it running when the last Client in this process is destroyed (matrix B1).
TEST(SidecarClientTest, DaemonSpawnSurvivesClientTeardown) {
  std::string const socket = UniqueSocket("daemon-survives");
  ScopedDaemonCleanup cleanup(socket);
  int daemon_pid = 0;
  {
    StatusOr<Client> client = Client::Create(DaemonOptions(socket));
    ASSERT_THAT(client, IsOk());
    daemon_pid = client->sidecar_pid();
    EXPECT_GT(daemon_pid, 0);
  }
  EXPECT_EQ(internal::ProbeSocketListener(socket), daemon_pid);

  // Re-creating a Client in the same process connects to the running daemon
  // without spawning another.
  StatusOr<Client> second = Client::Create(DaemonOptions(socket));
  ASSERT_THAT(second, IsOk());
  EXPECT_EQ(second->sidecar_pid(), daemon_pid);
}

/// @test Two separate client processes with the same UID and socket share one
/// daemon instance without spawning a second server (matrix B2).
TEST(SidecarClientTest, DaemonSharedAcrossProcesses) {
  ASSERT_FALSE(ColdStartBinary().empty()) << "GCS_CORE_COLD_START must be set";
  std::string const socket = UniqueSocket("daemon-shared");
  ScopedDaemonCleanup cleanup(socket);

  ColdStartResult const first =
      FinishColdStart(StartColdStart({"--socket-path=" + socket}));
  ASSERT_EQ(first.exit_code, 0) << first.stdout_text;
  ASSERT_GT(first.core_pid, 0);
  EXPECT_EQ(internal::ProbeSocketListener(socket), first.core_pid);

  ColdStartResult const second =
      FinishColdStart(StartColdStart({"--socket-path=" + socket}));
  ASSERT_EQ(second.exit_code, 0) << second.stdout_text;
  EXPECT_EQ(second.core_pid, first.core_pid);
}

/// @test Simultaneous processes starting against an unbound daemon socket race
/// cleanly: losers detect the winner and connect to it instead of failing
/// Create() or unlinking the winner's socket (matrix B3).
TEST(SidecarClientTest, DaemonConcurrentStartupRace) {
  ASSERT_FALSE(ColdStartBinary().empty()) << "GCS_CORE_COLD_START must be set";
  std::string const socket = UniqueSocket("daemon-race");
  ScopedDaemonCleanup cleanup(socket);

  std::vector<SpawnedColdStart> procs;
  procs.reserve(4);
  for (int i = 0; i != 4; ++i) {
    SpawnedColdStart p = StartColdStart({"--socket-path=" + socket});
    ASSERT_GT(p.pid, 0);
    procs.push_back(p);
  }
  std::vector<int> exit_codes;
  std::vector<int> core_pids;
  for (SpawnedColdStart p : procs) {
    ColdStartResult r = FinishColdStart(p);
    exit_codes.push_back(r.exit_code);
    core_pids.push_back(r.core_pid);
  }
  EXPECT_THAT(exit_codes, Each(0));
  ASSERT_THAT(core_pids, Each(Gt(0)));
  EXPECT_THAT(core_pids, Each(core_pids.front()));
}

/// @test In 1:1 child mode, SIGKILL to the parent process causes the kernel's
/// PR_SET_PDEATHSIG(SIGTERM) to terminate the child server within 1 s
/// (matrix B4).
TEST(SidecarClientTest, ChildExitsOnParentSigkill) {
  ASSERT_FALSE(ColdStartBinary().empty()) << "GCS_CORE_COLD_START must be set";
  std::string const socket = UniqueSocket("parent-sigkill");
  ScopedDaemonCleanup cleanup(socket);

  SpawnedColdStart parent =
      StartColdStart({"--child", "--socket-path=" + socket, "--hold-ms=30000"});
  ASSERT_GT(parent.pid, 0);

  // Read stdout until the parent prints `core_pid=...`, proving
  // `Client::Create` finished and the child server armed PR_SET_PDEATHSIG on
  // `SpawnerThread`.
  std::string output;
  int child_pid = 0;
  char ch = '\0';
  while (::read(parent.stdout_fd, &ch, 1) == 1) {
    output.push_back(ch);
    if (ch == '\n') {
      child_pid = ParseCorePid(output);
      if (child_pid > 0) break;
    }
  }
  ::close(parent.stdout_fd);
  ASSERT_GT(child_pid, 0) << output;
  ASSERT_TRUE(ProcessAlive(child_pid));

  std::chrono::steady_clock::time_point const killed_at =
      std::chrono::steady_clock::now();
  ASSERT_EQ(::kill(parent.pid, SIGKILL), 0);
  int status = 0;
  while (::waitpid(parent.pid, &status, 0) < 0 && errno == EINTR) {
  }

  std::chrono::steady_clock::time_point const deadline =
      killed_at + std::chrono::seconds(1);
  while (ProcessAlive(child_pid) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_FALSE(ProcessAlive(child_pid))
      << "child server should exit within 1 s of parent SIGKILL";
}

/// @test When an adopted daemon crashes (SIGKILL), the next RPC reports
/// UNAVAILABLE and spawns a replacement daemon so subsequent calls succeed
/// (matrix B6).
TEST(SidecarClientTest, AdoptedDaemonRespawnsAfterCrash) {
  std::string const socket = UniqueSocket("adopted-crash");
  ScopedDaemonCleanup cleanup(socket);

  int first_pid = 0;
  {
    StatusOr<Client> spawner = Client::Create(DaemonOptions(socket));
    ASSERT_THAT(spawner, IsOk());
    first_pid = spawner->sidecar_pid();
  }
  // A fresh Client adopts `first_pid` via `ProbeSocketListener`
  // (`external_pid_`).
  StatusOr<Client> adopter = Client::Create(DaemonOptions(socket));
  ASSERT_THAT(adopter, IsOk());
  ASSERT_EQ(adopter->sidecar_pid(), first_pid);

  ASSERT_EQ(::kill(first_pid, SIGKILL), 0);
  WaitForExit(first_pid);

  EXPECT_THAT(adopter->GetSidecarVersion(), StatusIs(StatusCode::kUnavailable));
  StatusOr<SidecarVersion> recovered = adopter->GetSidecarVersion();
  ASSERT_THAT(recovered, IsOk());
  EXPECT_NE(adopter->sidecar_pid(), first_pid);
  EXPECT_EQ(static_cast<int>(recovered->pid), adopter->sidecar_pid());
}

/// @test A stale Unix socket file left on disk by an earlier crashed daemon is
/// replaced cleanly on spawn (matrix B7).
TEST(SidecarClientTest, StaleSocketRemovedOnSpawn) {
  std::string const socket = UniqueSocket("stale-sock");
  ScopedDaemonCleanup cleanup(socket);

  int const fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
  ASSERT_GE(fd, 0);
  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  std::memcpy(addr.sun_path, socket.data(), socket.size());
  ASSERT_EQ(::bind(fd, reinterpret_cast<sockaddr const*>(&addr), sizeof(addr)),
            0);
  ::close(fd);
  struct stat st{};
  ASSERT_EQ(::lstat(socket.c_str(), &st), 0);
  ASSERT_TRUE(S_ISSOCK(st.st_mode));

  StatusOr<Client> client = Client::Create(DaemonOptions(socket));
  ASSERT_THAT(client, IsOk());
  EXPECT_THAT(client->GetSidecarVersion(), IsOk());
}

/// @test A sidecar process that never binds its socket times out cleanly and
/// is terminated rather than leaked.
TEST(SidecarClientTest, ReadinessTimeout) {
  std::string const script = WriteSleepScript("timeout");
  ClientOptions options;
  options.sidecar_mode = SidecarMode::kChild;
  options.sidecar_path = script;
  options.socket_path = UniqueSocket("never-ready");
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
