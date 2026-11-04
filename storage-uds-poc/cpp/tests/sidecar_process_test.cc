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

#include "sidecar_process.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <fstream>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

namespace gcs_core::internal {
namespace {

using ::google::cloud::Status;
using ::google::cloud::StatusCode;
using ::google::cloud::StatusOr;
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

/// Sets an environment variable for the lifetime of the object.
class ScopedEnv {
 public:
  ScopedEnv(std::string_view name, std::optional<std::string> value)
      : name_(name) {
    if (char const* old = std::getenv(name_.c_str())) old_ = old;
    if (value) {
      ::setenv(name_.c_str(), value->c_str(), 1);
    } else {
      ::unsetenv(name_.c_str());
    }
  }
  ~ScopedEnv() {
    if (old_) {
      ::setenv(name_.c_str(), old_->c_str(), 1);
    } else {
      ::unsetenv(name_.c_str());
    }
  }

 private:
  std::string name_;
  std::optional<std::string> old_;
};

std::string BuiltSidecar() {
  std::string const name(kSidecarEnvVar);
  char const* env = std::getenv(name.c_str());
  return env != nullptr ? env : "";
}

std::string SelfDirectory() {
  std::string buf(4096, '\0');
  ssize_t const n = ::readlink("/proc/self/exe", buf.data(), buf.size() - 1);
  if (n <= 0) return {};
  buf.resize(static_cast<std::size_t>(n));
  std::size_t const slash = buf.rfind('/');
  return slash == std::string::npos ? std::string{} : buf.substr(0, slash);
}

void WaitForFile(std::string const& path) {
  struct stat st{};
  std::chrono::steady_clock::time_point const deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (::stat(path.c_str(), &st) != 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

/// @test An explicitly configured binary wins over the environment and a
/// non-executable configured path is an error, never a silent fallback.
TEST(SidecarProcessTest, ResolveConfiguredPath) {
  std::string const sidecar = BuiltSidecar();
  ASSERT_FALSE(sidecar.empty()) << "GCS_CORE_SERVER must point at the build";
  ScopedEnv env(kSidecarEnvVar, "/nonexistent/from-env");
  StatusOr<std::string> resolved = ResolveSidecarPath(sidecar);
  ASSERT_THAT(resolved, IsOk());
  EXPECT_EQ(*resolved, sidecar);

  StatusOr<std::string> bad = ResolveSidecarPath("/etc/hostname");
  EXPECT_THAT(bad, StatusIs(StatusCode::kFailedPrecondition));
  EXPECT_THAT(bad.status().message(), HasSubstr("/etc/hostname"));
}

/// @test With nothing configured, the environment variable is used, and an
/// invalid value is reported naming the variable.
TEST(SidecarProcessTest, ResolveFromEnvironment) {
  std::string const sidecar = BuiltSidecar();
  {
    ScopedEnv env(kSidecarEnvVar, sidecar);
    StatusOr<std::string> resolved = ResolveSidecarPath("");
    ASSERT_THAT(resolved, IsOk());
    EXPECT_EQ(*resolved, sidecar);
  }
  ScopedEnv env(kSidecarEnvVar, "/nonexistent/gcs-core-server");
  StatusOr<std::string> bad = ResolveSidecarPath("");
  EXPECT_THAT(bad, StatusIs(StatusCode::kFailedPrecondition));
  EXPECT_THAT(bad.status().message(), HasSubstr(std::string(kSidecarEnvVar)));
}

/// @test Without configuration or environment, a sibling binary next to the
/// running executable wins over PATH.
TEST(SidecarProcessTest, ResolveSiblingNextToExecutable) {
  std::string const sidecar = BuiltSidecar();
  std::string const exe_dir = SelfDirectory();
  ASSERT_FALSE(exe_dir.empty());
  std::string const sibling = exe_dir + "/" + std::string(kSidecarBinaryName);
  // The build copies the server next to the test binaries; only plant (and
  // later remove) a sibling when it is not already there, so the test never
  // deletes the build's own server.
  bool const planted = sibling != sidecar;
  if (planted) {
    ::unlink(sibling.c_str());
    ASSERT_EQ(::symlink(sidecar.c_str(), sibling.c_str()), 0);
  }

  ScopedEnv env(kSidecarEnvVar, std::nullopt);
  ScopedEnv path("PATH", "/nonexistent");
  StatusOr<std::string> resolved = ResolveSidecarPath("");
  if (planted) ::unlink(sibling.c_str());

  ASSERT_THAT(resolved, IsOk());
  EXPECT_EQ(*resolved, sibling);
}

/// @test Without configuration, environment, or sibling, PATH is searched.
TEST(SidecarProcessTest, ResolveFromPath) {
  std::string const sidecar = BuiltSidecar();
  std::string const dir = sidecar.substr(0, sidecar.rfind('/'));
  ScopedEnv env(kSidecarEnvVar, std::nullopt);
  ScopedEnv path("PATH", "/nonexistent:" + dir);
  StatusOr<std::string> resolved = ResolveSidecarPath("");
  ASSERT_THAT(resolved, IsOk());
  EXPECT_EQ(*resolved, dir + "/" + std::string(kSidecarBinaryName));
}

/// @test The default socket lives under XDG_RUNTIME_DIR when set, else in a
/// private per-uid directory under /tmp.
TEST(SidecarProcessTest, DefaultSocketPath) {
  {
    ScopedEnv env("XDG_RUNTIME_DIR", "/run/user/1234");
    StatusOr<std::string> path = DefaultSocketPath();
    ASSERT_THAT(path, IsOk());
    EXPECT_EQ(*path, "/run/user/1234/gcs-core-" + std::to_string(::getpid()) +
                         ".sock");
  }
  ScopedEnv env("XDG_RUNTIME_DIR", std::nullopt);
  StatusOr<std::string> path = DefaultSocketPath();
  ASSERT_THAT(path, IsOk());
  std::string const dir = "/tmp/gcs-core-" + std::to_string(::getuid());
  EXPECT_EQ(*path, dir + "/" + std::to_string(::getpid()) + ".sock");
  struct stat st{};
  ASSERT_EQ(::stat(dir.c_str(), &st), 0);
  EXPECT_EQ(st.st_mode & 0777, 0700U);
}

/// @test Socket paths longer than sun_path, or empty, are rejected.
TEST(SidecarProcessTest, ValidateSocketPath) {
  EXPECT_THAT(ValidateSocketPath(std::string(kMaxSocketPathLength, 'a')),
              IsOk());
  EXPECT_THAT(ValidateSocketPath(std::string(kMaxSocketPathLength + 1, 'a')),
              StatusIs(StatusCode::kInvalidArgument));
  EXPECT_THAT(ValidateSocketPath(""), StatusIs(StatusCode::kInvalidArgument));
}

/// Server flags for a hermetic child: no GCS endpoint, no shared memory.
std::vector<std::string> HermeticArgs() {
  return {"--endpoint=http://127.0.0.1:1", "--shm-size=0",
          "--grpc-subchannels=1", "--worker-threads=1"};
}

std::string TestSocket(std::string_view tag) {
  return "/tmp/gcs-core-" + std::string(tag) + "-" +
         std::to_string(::getpid()) + ".sock";
}

/// @test Terminate() sends SIGTERM first; the server drains, exits 0 well
/// within the grace period and removes its own socket.
TEST(SidecarProcessTest, TerminateIsGraceful) {
  std::string const socket = TestSocket("graceful");
  StatusOr<std::unique_ptr<SidecarProcess>> process =
      SidecarProcess::Spawn(BuiltSidecar(), socket, HermeticArgs());
  ASSERT_THAT(process, IsOk());
  WaitForFile(socket);
  struct stat st{};
  ASSERT_EQ(::stat(socket.c_str(), &st), 0);
  std::chrono::steady_clock::time_point const start =
      std::chrono::steady_clock::now();
  (*process)->Terminate(std::chrono::seconds(2));
  std::chrono::steady_clock::duration const elapsed =
      std::chrono::steady_clock::now() - start;
  EXPECT_TRUE((*process)->Exited());
  EXPECT_THAT((*process)->ExitDescription(), HasSubstr("exited with status 0"));
  EXPECT_LT(elapsed, std::chrono::seconds(1));
  EXPECT_NE(::stat(socket.c_str(), &st), 0) << "socket should be removed";
}

/// @test Kill() is SIGKILL, as in a crash: no drain, and the wrapper still
/// removes the socket the server could not.
TEST(SidecarProcessTest, KillIsImmediate) {
  std::string const socket = TestSocket("kill");
  StatusOr<std::unique_ptr<SidecarProcess>> process =
      SidecarProcess::Spawn(BuiltSidecar(), socket, HermeticArgs());
  ASSERT_THAT(process, IsOk());
  WaitForFile(socket);
  (*process)->Kill();
  EXPECT_TRUE((*process)->Exited());
  EXPECT_THAT((*process)->ExitDescription(), HasSubstr("killed by signal 9"));
  struct stat st{};
  EXPECT_NE(::stat(socket.c_str(), &st), 0) << "socket should be removed";
}

/// @test The child asks for PR_SET_PDEATHSIG, which Linux ties to the
/// spawning *thread*. Spawning from a thread that then exits must not take
/// the server down with it, because Spawn() forks from a long-lived helper.
TEST(SidecarProcessTest, SurvivesSpawningThreadExit) {
  std::string const socket = TestSocket("thread-exit");
  StatusOr<std::unique_ptr<SidecarProcess>> process =
      Status(StatusCode::kUnknown, "not spawned");
  // The thread waits for the socket before exiting: the server arms
  // PR_SET_PDEATHSIG during startup, and a thread that is already gone by
  // then would never trigger it.
  std::thread([&] {
    process = SidecarProcess::Spawn(BuiltSidecar(), socket, HermeticArgs());
    WaitForFile(socket);
  }).join();
  ASSERT_THAT(process, IsOk());
  // Long enough for a SIGTERM from the kernel to have been delivered and
  // acted on: the server exits within milliseconds of SIGTERM.
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  EXPECT_FALSE((*process)->Exited()) << (*process)->ExitDescription();
  struct stat st{};
  EXPECT_EQ(::stat(socket.c_str(), &st), 0);
}

/// @test Pre-existing regular files and symlinks at the socket path are
/// rejected up front and never unlinked during process teardown.
TEST(SidecarProcessTest, RegularFileSocketPathPreserved) {
  std::string const regular =
      "/tmp/gcs-core-regular-" + std::to_string(::getpid()) + ".txt";
  std::string const link =
      "/tmp/gcs-core-link-" + std::to_string(::getpid()) + ".sock";
  ::unlink(regular.c_str());
  ::unlink(link.c_str());
  {
    std::ofstream out(regular);
    out << "keep";
  }
  ASSERT_EQ(::symlink(regular.c_str(), link.c_str()), 0);

  EXPECT_THAT(ValidateSocketPath(regular),
              StatusIs(StatusCode::kFailedPrecondition));
  EXPECT_THAT(ValidateSocketPath(link),
              StatusIs(StatusCode::kFailedPrecondition));
  EXPECT_THAT(SidecarProcess::Spawn(BuiltSidecar(), regular, HermeticArgs()),
              StatusIs(StatusCode::kFailedPrecondition));

  struct stat st{};
  EXPECT_EQ(::lstat(regular.c_str(), &st), 0);
  EXPECT_EQ(::lstat(link.c_str(), &st), 0);
  ::unlink(link.c_str());
  ::unlink(regular.c_str());
}

/// @test If the host process reaps the child externally (waitpid ECHILD),
/// Exited() still transitions to true and Terminate() does not stall.
TEST(SidecarProcessTest, ExternalWaitpidReapDoesNotStallTerminate) {
  std::string const socket =
      "/tmp/gcs-core-reap-" + std::to_string(::getpid()) + ".sock";
  StatusOr<std::unique_ptr<SidecarProcess>> process =
      SidecarProcess::Spawn("/bin/true", socket, {});
  ASSERT_THAT(process, IsOk());

  int raw_status = 0;
  ASSERT_EQ(::waitpid((*process)->pid(), &raw_status, 0), (*process)->pid());

  EXPECT_TRUE((*process)->Exited());
  EXPECT_THAT((*process)->ExitDescription(), HasSubstr("reaped externally"));
  std::chrono::steady_clock::time_point const start =
      std::chrono::steady_clock::now();
  (*process)->Terminate(std::chrono::milliseconds(100));
  std::chrono::steady_clock::duration const elapsed =
      std::chrono::steady_clock::now() - start;
  EXPECT_LT(elapsed, std::chrono::milliseconds(100));
}

}  // namespace
}  // namespace gcs_core::internal
