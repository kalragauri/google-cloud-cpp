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
#include "gcs_core/client.h"
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

/// Resolves the server binary: @p configured, else `$GCS_CORE_SERVER`, else
/// next to the running executable, else `PATH`. An explicitly configured or
/// env-provided path that is not executable is an error (no fallback), so a
/// typo never silently runs a different binary.
google::cloud::StatusOr<std::string> ResolveSidecarPath(
    std::string const& configured);

/// Default socket path for @p mode, creating its 0700 parent directory if
/// needed.
google::cloud::StatusOr<std::string> DefaultSocketPath(SidecarMode mode);

/// Rejects empty paths, paths longer than `sun_path`, and existing non-sockets.
google::cloud::Status ValidateSocketPath(std::string const& path);

/// Connects to @p socket_path and reads `SO_PEERCRED`. Returns the peer PID
/// when a server owned by our UID is actively accepting connections on @p
/// socket_path, or `std::nullopt` when the socket is absent, stale, or owned by
/// another UID.
std::optional<pid_t> ProbeSocketListener(std::string const& socket_path);

/// Removes @p socket_path and `@p socket_path.shm` only when they are Unix
/// sockets with no live listener.
void RemoveSocketFiles(std::string const& socket_path);

/// Owns one `gcs-core-server` child process in either shared daemon mode
/// (`--pdeathsig=false --idle-secs=300`) or 1:1 child mode
/// (`--pdeathsig=true --idle-secs=0`).
///
/// In 1:1 child mode the server asks the kernel for SIGTERM when its parent
/// goes away (`PR_SET_PDEATHSIG`). Linux ties that to the *thread* that
/// spawned the child, not the process, so `Spawn()` runs `posix_spawn` on a
/// helper thread that lives as long as this process; spawning from a
/// short-lived caller thread would kill the server when that thread ends.
class SidecarProcess {
 public:
  static google::cloud::StatusOr<std::unique_ptr<SidecarProcess>> Spawn(
      std::string const& binary, std::string const& socket_path,
      SidecarMode mode, std::vector<std::string> const& extra_args);

  SidecarProcess(SidecarProcess const&) = delete;
  SidecarProcess& operator=(SidecarProcess&&) = delete;
  SidecarProcess& operator=(SidecarProcess const&) = delete;
  /// Calls `Terminate()` unless `Detach()` was called.
  ~SidecarProcess();

  pid_t pid() const { return pid_; }
  std::string const& socket_path() const { return socket_path_; }

  /// Non-blocking. Reaps the child if it has exited.
  bool Exited();
  /// Process exit code when `Exited()` returned true via normal exit (`_exit` /
  /// `return`), or `std::nullopt` while running, killed by a signal, or reaped
  /// externally.
  std::optional<int> ExitCode() const;
  /// Exit description once `Exited()` returned true.
  std::string ExitDescription() const;

  /// Releases ownership so `~SidecarProcess()` leaves the server running and
  /// leaves its socket untouched. If the child has not exited yet, a detached
  /// reaper waits on its PID so it never lingers as a zombie when it later
  /// stops.
  void Detach();

  /// SIGTERM (the server drains and exits), waits up to 2 s (or @p grace),
  /// then SIGKILL; removes leftover socket files. Idempotent; no-op after
  /// `Detach()`.
  void Terminate();
  void Terminate(std::chrono::milliseconds grace);
  /// SIGKILL without a grace period, as a crash would; removes leftover
  /// socket files. Idempotent.
  void Kill();

 private:
  SidecarProcess(pid_t pid, std::string socket_path);
  bool WaitFor(std::chrono::milliseconds timeout);

  pid_t pid_;
  std::string socket_path_;
  std::optional<int> wait_status_;
  bool reaped_externally_ = false;
  bool detached_ = false;
};

}  // namespace gcs_core::internal

#endif  // GCS_UDS_POC_CPP_SRC_SIDECAR_PROCESS_H
