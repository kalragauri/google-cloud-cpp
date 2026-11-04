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

// The thread-boundary backend: the core runs on tokio threads inside this
// process (`libgcs_core.a`, see `gcs_core_inproc.h`), and each channel is
// either one end of a `socketpair` or a connection to an abstract-namespace
// socket, depending on the core's `--link` argument.

#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "core_backend.h"
#include "gcs_core/inproc.h"
#include "gcs_core_inproc.h"
#include <grpcpp/create_channel_posix.h>
#include <mutex>
#include <unistd.h>

namespace gcs_core {
namespace internal {
namespace {

using ::google::cloud::StatusCode;

/// How long `Stop()` lets the server drain in-flight RPCs and the runtime
/// wind down, each.
constexpr std::uint64_t kStopGraceMs = 2000;

std::mutex& HooksMutex() {
  static std::mutex mu;
  return mu;
}

inproc::ThreadHooks& GlobalHooks() {
  static inproc::ThreadHooks hooks;
  return hooks;
}

/// Reads a string-returning ABI function into a std::string, growing the
/// buffer if the first call reports a longer text.
template <typename Fn>
std::string ReadString(Fn&& fn) {
  std::string out(256, '\0');
  std::size_t n = fn(out.data(), out.size());
  if (n >= out.size()) {
    out.assign(n + 1, '\0');
    n = fn(out.data(), out.size());
  }
  out.resize(n);
  return out;
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
    std::vector<char const*> argv;
    argv.reserve(args_.size());
    for (std::string const& a : args_) argv.push_back(a.c_str());
    GcsCoreThreadHooks hooks{};
    {
      std::scoped_lock lk(HooksMutex());
      hooks.on_start = GlobalHooks().on_start;
      hooks.on_stop = GlobalHooks().on_stop;
      hooks.data = GlobalHooks().data;
    }
    core_ = gcs_core_inproc_start(argv.data(), argv.size(),
                                  static_cast<std::size_t>(channels_), &hooks);
    if (core_ == nullptr) {
      std::string const why = ReadString(gcs_core_inproc_last_error);
      // Kind 1 is an argument error (nothing was started), everything else a
      // failure to bring the core up — the same two codes the sidecar
      // variant uses for a bad flag and for a child that exits early.
      StatusCode const code = gcs_core_inproc_last_error_kind() == 1
                                  ? StatusCode::kInvalidArgument
                                  : StatusCode::kFailedPrecondition;
      return Status(code, absl::StrCat("in-process core: ", why));
    }
    std::vector<std::shared_ptr<grpc::Channel>> channels;
    channels.reserve(static_cast<std::size_t>(channels_));
    if (gcs_core_inproc_link_kind(core_) == 0) {
      for (int i = 0; i != channels_; ++i) {
        int const fd =
            gcs_core_inproc_take_channel_fd(core_, static_cast<std::size_t>(i));
        if (fd < 0) {
          Kill();
          return Status(StatusCode::kInternal,
                        absl::StrCat("in-process core handed out no socket "
                                     "for channel ",
                                     i));
        }
        // gRPC owns `fd` from here and closes it with the channel.
        channels.push_back(grpc::CreateCustomInsecureChannelFromFd(
            "gcs-core-inproc", fd, LocalChannelArguments(i)));
      }
    } else {
      std::string const target =
          "unix-abstract:" + ReadString([&](char* buf, std::size_t cap) {
            return gcs_core_inproc_abstract_name(core_, buf, cap);
          });
      for (int i = 0; i != channels_; ++i) {
        channels.push_back(grpc::CreateCustomChannel(
            target, grpc::InsecureChannelCredentials(),
            LocalChannelArguments(i)));
      }
    }
    return channels;
  }

  bool Exited() override {
    return core_ == nullptr || gcs_core_inproc_alive(core_) == 0;
  }

  std::string ExitDescription() override {
    if (core_ == nullptr) return "not running";
    return ReadString([&](char* buf, std::size_t cap) {
      return gcs_core_inproc_status(core_, buf, cap);
    });
  }

  void Stop() override {
    if (core_ == nullptr) return;
    retired_cpu_us_ += gcs_core_inproc_core_cpu_us(core_);
    gcs_core_inproc_stop(core_, kStopGraceMs);
    core_ = nullptr;
  }

  void Kill() override {
    if (core_ == nullptr) return;
    retired_cpu_us_ += gcs_core_inproc_core_cpu_us(core_);
    gcs_core_inproc_kill(core_);
    core_ = nullptr;
  }

  int pid() const override { return ::getpid(); }
  std::string Describe() const override { return "in-process core"; }

  std::optional<std::uint64_t> CoreThreadCpuMicros() const override {
    std::uint64_t const live =
        core_ == nullptr ? 0 : gcs_core_inproc_core_cpu_us(core_);
    return retired_cpu_us_ + live;
  }

 private:
  std::vector<std::string> const args_;
  int const channels_;
  std::string const key_;
  GcsCoreInproc* core_ = nullptr;
  /// CPU of earlier instances of this backend (before a restart), so a delta
  /// across a restart never goes backwards.
  std::uint64_t retired_cpu_us_ = 0;
};

}  // namespace

StatusOr<std::unique_ptr<CoreBackend>> MakeCoreBackend(
    ClientOptions const& options,
    std::vector<std::string> const& /*sockets_in_use*/) {
  if (!options.sidecar_path.empty()) {
    return Status(StatusCode::kInvalidArgument,
                  "ClientOptions::sidecar_path is not used by the in-process "
                  "core (the core is linked into this program)");
  }
  if (!options.socket_path.empty()) {
    return Status(StatusCode::kInvalidArgument,
                  "ClientOptions::socket_path is not used by the in-process "
                  "core (channels are socket pairs inside this process)");
  }
  return std::unique_ptr<CoreBackend>(
      new InProcessBackend(options.sidecar_args, options.channels));
}

}  // namespace internal

namespace inproc {

void SetThreadHooks(ThreadHooks hooks) {
  std::scoped_lock lk(internal::HooksMutex());
  internal::GlobalHooks() = hooks;
}

}  // namespace inproc
}  // namespace gcs_core
