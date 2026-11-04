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

// Tests of the in-process (thread-boundary) variant only: option validation,
// core sharing and teardown by thread count, process-wide stats and CPU
// attribution. Everything that holds for both variants
// is in client_common_test.cc.

#include "absl/strings/match.h"
#include "gcs_core/client.h"
#include "test_support.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

namespace gcs_core {
namespace {

using ::gcs_core::testing::IsOk;
using ::gcs_core::testing::ServerOptions;
using ::gcs_core::testing::StatusIs;
using ::google::cloud::StatusCode;
using ::testing::AllOf;
using ::testing::HasSubstr;

/// Threads of this process whose name starts with `gcs-core-`, the prefix the
/// core gives its tokio workers. gtest runs tests sequentially, so the count
/// belongs to the cores the current test created.
int CoreThreads() {
  int n = 0;
  for (auto const& task :
       std::filesystem::directory_iterator("/proc/self/task")) {
    std::ifstream comm(task.path() / "comm");
    std::string name;
    std::getline(comm, name);
    if (absl::StartsWith(name, "gcs-core-")) ++n;
  }
  return n;
}

void WaitForCoreThreads(int expected) {
  std::chrono::steady_clock::time_point const deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (CoreThreads() != expected &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

/// A process-wide counter from /proc/self/status, e.g. "Threads:".
std::int64_t StatusField(char const* key) {
  std::ifstream status("/proc/self/status");
  std::string line;
  while (std::getline(status, line)) {
    if (absl::StartsWith(line, key)) {
      return std::stoll(line.substr(std::strlen(key)));
    }
  }
  return -1;
}

/// @test The sidecar lifecycle mode has no meaning in-process and `kChild` is
/// rejected rather than silently ignored.
TEST(InprocClientTest, RejectsSidecarMode) {
  ClientOptions options = ServerOptions();
  options.sidecar_mode = SidecarMode::kChild;
  StatusOr<Client> client = Client::Create(options);
  EXPECT_THAT(client, StatusIs(StatusCode::kInvalidArgument));
  EXPECT_THAT(client.status().message(), HasSubstr("sidecar_mode"));
}

/// @test The sidecar binary option has no meaning in-process and is rejected
/// rather than silently ignored.
TEST(InprocClientTest, RejectsSidecarPath) {
  ClientOptions options = ServerOptions();
  options.sidecar_path = "/bin/true";
  StatusOr<Client> client = Client::Create(options);
  EXPECT_THAT(client, StatusIs(StatusCode::kInvalidArgument));
  EXPECT_THAT(client.status().message(), HasSubstr("sidecar_path"));
}

/// @test The socket path option has no meaning in-process and is rejected.
TEST(InprocClientTest, RejectsSocketPath) {
  ClientOptions options = ServerOptions();
  options.socket_path = "/tmp/gcs-core-unused.sock";
  StatusOr<Client> client = Client::Create(options);
  EXPECT_THAT(client, StatusIs(StatusCode::kInvalidArgument));
  EXPECT_THAT(client.status().message(), HasSubstr("socket_path"));
}

/// @test Server discovery is not consulted: a bogus `GCS_CORE_SERVER` does
/// not stop an in-process core from starting.
TEST(InprocClientTest, IgnoresSidecarDiscovery) {
  ::setenv("GCS_CORE_SERVER", "/nonexistent/gcs-core-server", 1);
  StatusOr<Client> client = Client::Create(ServerOptions());
  ::unsetenv("GCS_CORE_SERVER");
  ASSERT_THAT(client, IsOk());
  EXPECT_THAT(client->GetSidecarVersion(), IsOk());
}

/// @test A rejected flag (reserved or unknown) fails Create() before any
/// core thread starts, so a bad configuration leaves nothing running.
TEST(InprocClientTest, RejectedFlagsStartNoThreads) {
  WaitForCoreThreads(0);
  ClientOptions reserved = ServerOptions();
  reserved.sidecar_args.emplace_back("--tcp=127.0.0.1:0");
  EXPECT_THAT(Client::Create(reserved), StatusIs(StatusCode::kInvalidArgument));
  ClientOptions unknown = ServerOptions();
  unknown.sidecar_args.emplace_back("--no-such-flag");
  EXPECT_THAT(Client::Create(unknown), StatusIs(StatusCode::kInvalidArgument));
  EXPECT_EQ(CoreThreads(), 0);
}

/// @test Clients with equal options share one core: the second Create()
/// starts no threads.
TEST(InprocClientTest, SharedCoreAcrossEqualOptions) {
  StatusOr<Client> a = Client::Create(ServerOptions());
  ASSERT_THAT(a, IsOk());
  int const with_one = CoreThreads();
  EXPECT_GT(with_one, 0);
  StatusOr<Client> b = Client::Create(ServerOptions());
  ASSERT_THAT(b, IsOk());
  EXPECT_EQ(CoreThreads(), with_one);
  EXPECT_THAT(b->GetSidecarVersion(), IsOk());
}

/// @test Different core options start a second core (own threads) rather
/// than silently reusing the first one's configuration.
TEST(InprocClientTest, DifferentOptionsUseSeparateCores) {
  StatusOr<Client> a = Client::Create(ServerOptions());
  ASSERT_THAT(a, IsOk());
  int const with_one = CoreThreads();
  ClientOptions small = ServerOptions();
  small.sidecar_args.emplace_back("--chunk-size=4096");
  StatusOr<Client> b = Client::Create(small);
  ASSERT_THAT(b, IsOk());
  EXPECT_GT(CoreThreads(), with_one);
  EXPECT_THAT(b->GetSidecarVersion(), IsOk());
}

/// @test Concurrent Create() calls with equal options end up on one core.
TEST(InprocClientTest, ConcurrentCreateSharesRuntime) {
  WaitForCoreThreads(0);
  int single = 0;
  {
    StatusOr<Client> one = Client::Create(ServerOptions());
    ASSERT_THAT(one, IsOk());
    single = CoreThreads();
    ASSERT_GE(single, 2);
  }
  WaitForCoreThreads(0);
  std::mutex mu;
  std::vector<Client> clients;
  std::vector<std::thread> threads;
  for (int i = 0; i != 8; ++i) {
    threads.emplace_back([&] {
      StatusOr<Client> c = Client::Create(ServerOptions());
      ASSERT_THAT(c, IsOk());
      ASSERT_THAT(c->GetSidecarVersion(), IsOk());
      std::scoped_lock lk(mu);
      clients.push_back(*std::move(c));
    });
  }
  for (std::thread& t : threads) t.join();
  ASSERT_EQ(clients.size(), 8U);
  // Tokio's `spawn_blocking` pool (used while loading ADC in `prepare_service`)
  // may keep 1 or 2 idle blocking threads alongside the 2 worker threads, so a
  // single runtime has `single` +/- 1 threads while two runtimes have >= 2 * 2
  // worker threads + 2 supervisor/blocking threads (>= 6).
  EXPECT_LE(CoreThreads(), single + 1);
  clients.erase(clients.begin() + 1, clients.end());
  EXPECT_GT(CoreThreads(), 0);
  clients.clear();
  WaitForCoreThreads(0);
  EXPECT_EQ(CoreThreads(), 0);
}

/// @test Destroying the last Client stops the core: its threads are gone.
TEST(InprocClientTest, TeardownStopsCoreThreads) {
  WaitForCoreThreads(0);
  ASSERT_EQ(CoreThreads(), 0);
  {
    StatusOr<Client> client = Client::Create(ServerOptions());
    ASSERT_THAT(client, IsOk());
    EXPECT_GT(CoreThreads(), 0);
  }
  WaitForCoreThreads(0);
  EXPECT_EQ(CoreThreads(), 0);
}

/// @test The core reports this process's pid, and so does the Client.
TEST(InprocClientTest, VersionReportsOwnPid) {
  StatusOr<Client> client = Client::Create(ServerOptions());
  ASSERT_THAT(client, IsOk());
  StatusOr<SidecarVersion> version = client->GetSidecarVersion();
  ASSERT_THAT(version, IsOk());
  EXPECT_EQ(static_cast<int>(version->pid), ::getpid());
  EXPECT_EQ(client->sidecar_pid(), ::getpid());
}

/// @test GetStats describes this process: live RSS, the process's own thread
/// count, and no allocation count (the core allocates through this program's
/// malloc, which the core's counter never sees).
TEST(InprocClientTest, StatsAreProcessWide) {
  StatusOr<Client> client = Client::Create(ServerOptions());
  ASSERT_THAT(client, IsOk());
  StatusOr<SidecarStats> stats = client->GetSidecarStats();
  ASSERT_THAT(stats, IsOk());
  EXPECT_GT(stats->rss_bytes, 0U);
  EXPECT_EQ(stats->allocated_bytes, 0U);
  std::int64_t const threads = StatusField("Threads:");
  EXPECT_GE(static_cast<std::int64_t>(stats->threads), threads - 2);
  EXPECT_LE(static_cast<std::int64_t>(stats->threads), threads + 2);
}

/// @test The core serves when Create() returns (the C ABI starts it
/// synchronously), so even a short readiness timeout succeeds — the sidecar
/// variant would need its spawn time on top.
TEST(InprocClientTest, ReadyWhenStartReturns) {
  for (int i = 0; i != 5; ++i) {
    ClientOptions options = ServerOptions();
    options.readiness_timeout = std::chrono::milliseconds(50);
    StatusOr<Client> client = Client::Create(options);
    ASSERT_THAT(client, IsOk()) << "attempt " << i;
    EXPECT_THAT(client->GetSidecarVersion(), IsOk());
  }
}

/// @test A core that cannot build its clients (here: Application Default
/// Credentials point at a missing file) fails Create() with
/// FAILED_PRECONDITION carrying the reason, and leaves no core thread.
TEST(InprocClientTest, StartFailureLeavesNoThreads) {
  WaitForCoreThreads(0);
  ASSERT_EQ(CoreThreads(), 0);
  char const* old = std::getenv("GOOGLE_APPLICATION_CREDENTIALS");
  std::string const saved = old == nullptr ? std::string() : old;
  ::setenv("GOOGLE_APPLICATION_CREDENTIALS",
           "/nonexistent/gcs-core-test-adc.json", 1);
  StatusOr<Client> client = Client::Create(ServerOptions());
  if (old == nullptr) {
    ::unsetenv("GOOGLE_APPLICATION_CREDENTIALS");
  } else {
    ::setenv("GOOGLE_APPLICATION_CREDENTIALS", saved.c_str(), 1);
  }
  EXPECT_THAT(client, StatusIs(StatusCode::kFailedPrecondition));
  EXPECT_THAT(client.status().message(),
              AllOf(HasSubstr("building the storage clients"),
                    HasSubstr("GOOGLE_APPLICATION_CREDENTIALS")));
  WaitForCoreThreads(0);
  EXPECT_EQ(CoreThreads(), 0);
}

/// @test Core-thread CPU is reported in-process and grows with work.
TEST(InprocClientTest, CoreThreadCpuGrowsWithWork) {
  StatusOr<Client> client = Client::Create(ServerOptions());
  ASSERT_THAT(client, IsOk());
  std::optional<std::uint64_t> const before = client->core_thread_cpu_us();
  ASSERT_TRUE(before.has_value());
  // Each Stats call reads /proc on a core thread; enough of them to move
  // the counter past the scheduler's clock-tick granularity.
  for (int i = 0; i != 2000; ++i)
    ASSERT_THAT(client->GetSidecarStats(), IsOk());
  std::optional<std::uint64_t> const after = client->core_thread_cpu_us();
  ASSERT_TRUE(after.has_value());
  EXPECT_GT(*after, *before);
}

}  // namespace
}  // namespace gcs_core
