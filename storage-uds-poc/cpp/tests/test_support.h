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

/// A core that serves deterministic data without GCS; small thread count so
/// the suite does not start dozens of workers per test in-process.
inline ClientOptions SyntheticOptions() {
  ClientOptions options;
  options.sidecar_args = {"--synthetic"};
  if (kInProcess) options.sidecar_args.emplace_back("--worker-threads=2");
  return options;
}

inline StatusOr<std::uint64_t> ReadSize(ObjectReader& reader,
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
/// no outside: the core is aborted through the test hook (no frames reach the
/// channels), which is what the wrapper would see if the core died.
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
