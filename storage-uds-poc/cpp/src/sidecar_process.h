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

#ifndef GCS_UDS_POC_CPP_SRC_SIDECAR_PROCESS_H
#define GCS_UDS_POC_CPP_SRC_SIDECAR_PROCESS_H

#include "google/cloud/status_or.h"
#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <sys/types.h>

namespace gcs_core::internal {

/// Name of the sidecar binary looked up next to the executable and on PATH.
inline constexpr std::string_view kSidecarBinaryName = "gcs-core-sidecar";
/// Environment variable that overrides sidecar discovery.
inline constexpr std::string_view kSidecarEnvVar = "GCS_CORE_SIDECAR";
/// `sizeof(sockaddr_un::sun_path)` minus the terminating NUL.
inline constexpr std::size_t kMaxSocketPathLength = 107;

/// Resolves the sidecar binary: `configured`, else `$GCS_CORE_SIDECAR`, else
/// next to the running executable, else `PATH`. An explicitly configured or
/// env-provided path that is not executable is an error (no fallback), so a
/// typo never silently runs a different binary.
google::cloud::StatusOr<std::string> ResolveSidecarPath(
    std::string const& configured);

/// Default socket path, creating its 0700 parent directory if needed.
google::cloud::StatusOr<std::string> DefaultSocketPath();

/// Rejects empty paths, paths longer than `sun_path`, and existing non-sockets.
google::cloud::Status ValidateSocketPath(std::string const& path);

/// Owns one sidecar child process.
///
/// The child inherits the read end of a pipe as `--parent-fd=3`; the write
/// end stays in this process (close-on-exec). When this process exits for
/// any reason the kernel closes it, the sidecar reads EOF and shuts down.
class SidecarProcess {
 public:
  static google::cloud::StatusOr<std::unique_ptr<SidecarProcess>> Spawn(
      std::string const& binary, std::string const& socket_path,
      std::vector<std::string> const& extra_args);

  SidecarProcess(SidecarProcess const&) = delete;
  SidecarProcess& operator=(SidecarProcess const&) = delete;
  /// Calls `Terminate()` with the default grace period.
  ~SidecarProcess();

  pid_t pid() const { return pid_; }
  std::string const& socket_path() const { return socket_path_; }

  /// Non-blocking. Reaps the child if it has exited.
  bool Exited();
  /// Exit description once `Exited()` returned true.
  std::string ExitDescription() const;

  /// Closes the lifetime pipe, waits up to 2 s (or `grace`), then SIGTERM,
  /// then SIGKILL; removes the socket file. Idempotent.
  void Terminate();
  void Terminate(std::chrono::milliseconds grace);

 private:
  SidecarProcess(pid_t pid, int lifetime_fd, std::string socket_path);
  bool WaitFor(std::chrono::milliseconds timeout);

  pid_t pid_;
  int lifetime_fd_;
  std::string socket_path_;
  std::optional<int> wait_status_;
  bool reaped_externally_ = false;
};

}  // namespace gcs_core::internal

#endif  // GCS_UDS_POC_CPP_SRC_SIDECAR_PROCESS_H
