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

// The thread-boundary backend: the shared server runs on Tokio threads
// inside this process (`libstorage_grpc_uds.a`, `gcs_core_uds.h`) and listens
// on an abstract-namespace Unix socket, so nothing appears on the filesystem
// and the channels are the same gRPC-over-UDS channels the sidecar uses.

#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "core_backend.h"
#include "gcs_core_uds.h"
#include <atomic>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <unistd.h>

namespace gcs_core {
namespace internal {
namespace {

using ::google::cloud::StatusCode;

/// The server names its runtime threads this (15 bytes, the kernel limit),
/// which is how their CPU time is found among this process's threads.
constexpr std::string_view kCoreThreadName = "gcs-core-inproc";

/// User + system CPU of this process's threads named `kCoreThreadName`, in
/// microseconds, from `/proc/self/task/<tid>/stat` (fields 14 and 15, clock
/// ticks).
std::uint64_t CoreThreadCpuMicrosNow() {
  static std::int64_t const kTicksPerSecond = ::sysconf(_SC_CLK_TCK);
  std::uint64_t ticks = 0;
  std::error_code ec;
  for (std::filesystem::directory_entry const& task :
       std::filesystem::directory_iterator("/proc/self/task", ec)) {
    std::ifstream comm(task.path() / "comm");
    std::string name;
    std::getline(comm, name);
    if (name != kCoreThreadName) continue;
    std::ifstream stat_file(task.path() / "stat");
    std::stringstream buffer;
    buffer << stat_file.rdbuf();
    std::string const stat = buffer.str();
    // `comm` (field 2) may contain spaces; the fields after it start past the
    // last ')'.
    std::size_t const close = stat.rfind(')');
    if (close == std::string::npos) continue;
    std::vector<std::string_view> fields =
        absl::StrSplit(std::string_view(stat).substr(close + 2), ' ');
    // fields[0] is field 3 (state), so utime/stime are fields[11]/fields[12].
    std::uint64_t utime = 0;
    std::uint64_t stime = 0;
    if (fields.size() > 12 && absl::SimpleAtoi(fields[11], &utime) &&
        absl::SimpleAtoi(fields[12], &stime)) {
      ticks += utime + stime;
    }
  }
  if (kTicksPerSecond <= 0) return 0;
  return ticks * 1000000 / static_cast<std::uint64_t>(kTicksPerSecond);
}

class InProcessBackend : public CoreBackend {
 public:
  InProcessBackend(std::vector<std::string> args, int channels)
      : args_(std::move(args)),
        channels_(channels),
        key_(absl::StrCat("inproc\x1f", channels_, "\x1f",
                          absl::StrJoin(args_, "\x1e"))) {}

  ~InProcessBackend() override { Stop(); }

  std::string const& SharingKey() const override { return key_; }
  std::string const& SocketPath() const override {
    static std::string const kNone;
    return kNone;
  }

  StatusOr<std::vector<std::shared_ptr<grpc::Channel>>> Start() override {
    // A fresh name per start: a restarted server never meets connections
    // that were meant for the previous one.
    static std::atomic<std::uint64_t> next_seq{0};
    name_ = absl::StrCat("gcs-core-", ::getpid(), "-", ++next_seq);
    std::string const socket = absl::StrCat("@", name_);
    std::vector<std::string> flags = {"--socket", socket};
    flags.insert(flags.end(), args_.begin(), args_.end());
    std::vector<char const*> argv;
    argv.reserve(flags.size());
    for (std::string const& f : flags) argv.push_back(f.c_str());
    if (gcs_core_start_inproc_with_args(static_cast<int>(argv.size()),
                                        argv.data()) != 0) {
      std::string why(gcs_core_inproc_last_error(nullptr, 0) + 1, '\0');
      why.resize(gcs_core_inproc_last_error(why.data(), why.size()));
      name_.clear();
      // Flag errors are the caller's; everything else is a failure to bring
      // the server up.
      StatusCode const code = absl::StrContains(why, "flags")
                                  ? StatusCode::kInvalidArgument
                                  : StatusCode::kFailedPrecondition;
      return Status(code, absl::StrCat("in-process server: ", why));
    }
    running_ = true;
    std::vector<std::shared_ptr<grpc::Channel>> channels;
    channels.reserve(static_cast<std::size_t>(channels_));
    for (int i = 0; i != channels_; ++i) {
      channels.push_back(grpc::CreateCustomChannel(
          absl::StrCat("unix-abstract:", name_),
          grpc::InsecureChannelCredentials(), LocalChannelArguments(i)));
    }
    return channels;
  }

  /// The server lives as long as its runtime thread; there is no crash that
  /// leaves this process running, so it only "exits" when stopped.
  bool Exited() override { return !running_; }
  std::string ExitDescription() override {
    return running_ ? "running" : "stopped";
  }

  void Stop() override {
    if (!running_) return;
    retired_cpu_us_ += CoreThreadCpuMicrosNow();
    (void)gcs_core_stop_inproc(absl::StrCat("@", name_).c_str());
    running_ = false;
  }

  /// The shared server has no abort entry point; stopping drains in-flight
  /// RPCs, which ends their streams with an error just as a crash would.
  void Kill() override { Stop(); }

  int pid() const override { return ::getpid(); }
  std::string Describe() const override { return "in-process server"; }

  std::optional<std::uint64_t> CoreThreadCpuMicros() const override {
    return retired_cpu_us_ + (running_ ? CoreThreadCpuMicrosNow() : 0);
  }

 private:
  std::vector<std::string> const args_;
  int const channels_;
  std::string const key_;
  std::string name_;
  bool running_ = false;
  /// CPU of earlier instances (before a restart), so a delta across a
  /// restart never goes backwards.
  std::uint64_t retired_cpu_us_ = 0;
};

}  // namespace

StatusOr<std::unique_ptr<CoreBackend>> MakeInprocBackend(
    ClientOptions const& options,
    std::vector<std::string> const& /*sockets_in_use*/) {
  if (options.hosting == HostingMode::kSidecar) {
    return Status(StatusCode::kInvalidArgument,
                  "ClientOptions::hosting is kSidecar, which is not available "
                  "in the in-process-only library");
  }
  if (options.sidecar_mode != SidecarMode::kDaemon) {
    return Status(StatusCode::kInvalidArgument,
                  "ClientOptions::sidecar_mode is not used by the in-process "
                  "server (it runs inside this process)");
  }
  if (!options.sidecar_path.empty()) {
    return Status(StatusCode::kInvalidArgument,
                  "ClientOptions::sidecar_path is not used by the in-process "
                  "server (it is linked into this program)");
  }
  if (!options.socket_path.empty()) {
    return Status(StatusCode::kInvalidArgument,
                  "ClientOptions::socket_path is not used by the in-process "
                  "server (it binds an abstract socket)");
  }
  if (Status s = ValidateServerFlags(options.sidecar_args); !s.ok()) return s;
  return std::unique_ptr<CoreBackend>(
      new InProcessBackend(options.sidecar_args, options.channels));
}

}  // namespace internal
}  // namespace gcs_core
