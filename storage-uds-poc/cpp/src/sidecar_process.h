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

/// Name of the shared server binary (`storage-grpc-uds` in google-cloud-rust)
/// looked up next to the executable and on PATH.
inline constexpr std::string_view kSidecarBinaryName = "gcs-core-server";
/// Environment variable that overrides server discovery.
inline constexpr std::string_view kSidecarEnvVar = "GCS_CORE_SERVER";
/// `sizeof(sockaddr_un::sun_path)` minus the terminating NUL.
inline constexpr std::size_t kMaxSocketPathLength = 107;

/// Resolves the server binary: `configured`, else `$GCS_CORE_SERVER`, else
/// next to the running executable, else `PATH`. An explicitly configured or
/// env-provided path that is not executable is an error (no fallback), so a
/// typo never silently runs a different binary.
google::cloud::StatusOr<std::string> ResolveSidecarPath(
    std::string const& configured);

/// Default socket path, creating its 0700 parent directory if needed.
google::cloud::StatusOr<std::string> DefaultSocketPath();

/// Rejects empty paths, paths longer than `sun_path`, and existing non-sockets.
google::cloud::Status ValidateSocketPath(std::string const& path);

/// Owns one `gcs-core-server` child process in its 1:1 child mode
/// (`--pdeathsig=true --idle-secs=0`).
///
/// The server asks the kernel for SIGTERM when its parent goes away
/// (`PR_SET_PDEATHSIG`). Linux ties that to the *thread* that spawned the
/// child, not the process, so `Spawn()` runs `posix_spawn` on a helper thread
/// that lives as long as this process; spawning from a short-lived caller
/// thread would kill the server when that thread ends.
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

  /// SIGTERM (the server drains and exits), waits up to 2 s (or `grace`),
  /// then SIGKILL; removes leftover socket files. Idempotent.
  void Terminate();
  void Terminate(std::chrono::milliseconds grace);
  /// SIGKILL without a grace period, as a crash would; removes leftover
  /// socket files. Idempotent.
  void Kill();

 private:
  SidecarProcess(pid_t pid, std::string socket_path);
  bool WaitFor(std::chrono::milliseconds timeout);
  void RemoveSocketFiles() const;

  pid_t pid_;
  std::string socket_path_;
  std::optional<int> wait_status_;
  bool reaped_externally_ = false;
};

}  // namespace gcs_core::internal

#endif  // GCS_UDS_POC_CPP_SRC_SIDECAR_PROCESS_H
