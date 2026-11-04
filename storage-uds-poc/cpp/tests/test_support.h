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

#ifndef GCS_UDS_POC_CPP_TESTS_TEST_SUPPORT_H
#define GCS_UDS_POC_CPP_TESTS_TEST_SUPPORT_H

#include "gcs_core/client.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <optional>
#include <string>
#include <thread>
#include <unistd.h>

// Shared by the test binaries of both library variants. `client_common_test.cc`
// is compiled once per variant; `GCS_CORE_TEST_INPROC` selects the in-process
// expectations where the two legitimately differ.

namespace gcs_core {
namespace testing {

#ifdef GCS_CORE_TEST_INPROC
constexpr bool kInProcess = true;
#else
constexpr bool kInProcess = false;
#endif

inline Status AsStatus(Status const& s) { return s; }
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

/// A real `gcs-core-server` that never reaches GCS: the endpoint points at a
/// closed local port, so these tests exercise only the lifecycle RPCs (`Ping`,
/// `Stats`) and stay hermetic. Data-plane mapping is covered against a fake
/// `GcsCore` (client_fake_test.cc) and end to end by the quickstart. The
/// thread and subchannel counts keep each test's core small; shared memory
/// is off because the wrapper does not use it.
inline ClientOptions ServerOptions() {
  ClientOptions options;
  if (!kInProcess) options.sidecar_mode = SidecarMode::kChild;
  options.sidecar_args = {"--endpoint=http://127.0.0.1:1", "--shm-size=0",
                          "--grpc-subchannels=1", "--worker-threads=2"};
  return options;
}

inline bool ProcessAlive(int pid) { return pid > 0 && ::kill(pid, 0) == 0; }

inline void WaitForExit(int pid) {
  std::chrono::steady_clock::time_point const deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (ProcessAlive(pid) && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

/// Crashes the core serving `client` from outside the wrapper and returns
/// once it is gone. Sidecar: SIGKILL to the child, so the wrapper discovers
/// the death through `waitpid` exactly as in production. In-process there is
/// no outside and the server has no abort entry point, so the test hook stops
/// the core; the wrapper then sees its channels fail as if the core died.
inline void CrashCore(Client const& client) {
  int const pid = client.sidecar_pid();
  if (kInProcess) {
    internal::KillCoreForTesting(client);
    return;
  }
  ::kill(pid, SIGKILL);
  WaitForExit(pid);
}

}  // namespace testing
}  // namespace gcs_core

#endif  // GCS_UDS_POC_CPP_TESTS_TEST_SUPPORT_H
