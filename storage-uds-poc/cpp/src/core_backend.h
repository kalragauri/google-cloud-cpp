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

#ifndef GCS_UDS_POC_CPP_SRC_CORE_BACKEND_H
#define GCS_UDS_POC_CPP_SRC_CORE_BACKEND_H

#include "gcs_core/client.h"
#include <grpcpp/grpcpp.h>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace gcs_core {
namespace internal {

/**
 * Where the server runs: a child process (`sidecar_backend.cc`) or Tokio
 * threads inside this process (`inproc_backend.cc`).
 *
 * `Connection` owns the stubs, the readiness poll and the crash-restart
 * policy and asks the backend only to start, stop and describe the core, so
 * everything above this seam — and therefore everything a benchmark measures
 * apart from the boundary itself — is the same code in both variants.
 *
 * One backend per `Connection`. `Start()` may be called again after `Stop()`
 * or `Kill()`: that is how a crashed core is restarted. Not thread-safe;
 * `Connection` serializes calls under its own mutex.
 */
class CoreBackend {
 public:
  virtual ~CoreBackend() = default;

  /// `Client`s whose backends have equal keys share one `Connection`.
  virtual std::string const& SharingKey() const = 0;
  /// Socket path this backend binds, so two configurations cannot claim the
  /// same path; empty when the backend binds none.
  virtual std::string const& SocketPath() const = 0;

  /// Starts the core and returns one channel per configured channel. The
  /// channels may still be connecting; the caller polls `Ping`.
  virtual StatusOr<std::vector<std::shared_ptr<grpc::Channel>>> Start() = 0;
  /// Called once `Ping` succeeds on every channel, so a daemon backend whose
  /// spawned child lost a startup race can adopt the winning listener PID.
  virtual void OnReady() {}
  /// True once the core stopped on its own after `Start()`.
  virtual bool Exited() = 0;
  /// Why the core stopped; meaningful once `Exited()` is true.
  virtual std::string ExitDescription() = 0;
  /// Orderly, bounded stop.
  virtual void Stop() = 0;
  /// Stop without a grace period (readiness timeout, a failed start, test
  /// crash hooks). The sidecar gets SIGKILL, as in a crash. The in-process
  /// server has no abort entry point, so it gets the same orderly stop as
  /// `Stop()`; peers still see their connections close.
  virtual void Kill() = 0;

  /// Pid of the process running the core.
  virtual int pid() const = 0;
  /// For messages: `sidecar /path/to/gcs-core-server`, `in-process server`.
  virtual std::string Describe() const = 0;
  /// CPU time consumed by the core's threads when they live in this process;
  /// `nullopt` for a separate process (its `GetStats` covers that).
  virtual std::optional<std::uint64_t> CoreThreadCpuMicros() const = 0;
};

/// Validates the backend-specific `ClientOptions` and builds the backend
/// without starting it. `sockets_in_use` are the socket paths of the live
/// connections, so a default path can avoid them. Defined once per library
/// variant (`gcs_core` → sidecar, `gcs_core_inproc` → in-process).
StatusOr<std::unique_ptr<CoreBackend>> MakeCoreBackend(
    ClientOptions const& options,
    std::vector<std::string> const& sockets_in_use);

/// Channel arguments for a connection to the core on this host. Shared by
/// both backends so a benchmark row differs only by the boundary.
grpc::ChannelArguments LocalChannelArguments(int channel_index);

/// Rejects `ClientOptions::sidecar_args` the wrapper sets itself
/// (`--socket`, `--pdeathsig`) or that would expose the server beyond this
/// host (`--tcp`), in either `--flag value` or `--flag=value` form.
Status ValidateServerFlags(std::vector<std::string> const& args);

}  // namespace internal
}  // namespace gcs_core

#endif  // GCS_UDS_POC_CPP_SRC_CORE_BACKEND_H
