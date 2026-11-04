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

// The process-boundary backend: the shared `gcs-core-server` runs in a
// separate OS process — by default as a per-UID shared daemon (`kDaemon`), or
// as a 1:1 owned child (`kChild`) — and the channels are `unix:` sockets to it.

#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "core_backend.h"
#include "sidecar_process.h"
#include <algorithm>
#include <chrono>
#include <csignal>
#include <string_view>
#include <thread>
#include <sys/stat.h>

namespace gcs_core {
namespace internal {
namespace {

std::string_view ModeKey(SidecarMode mode) {
  switch (mode) {
    case SidecarMode::kDaemon:
      return "daemon";
    case SidecarMode::kChild:
      return "child";
  }
  return "unknown";
}

std::string SuffixSocketPath(std::string const& base, std::uint64_t seq) {
  constexpr std::string_view kSockSuffix = ".sock";
  if (base.size() > kSockSuffix.size() &&
      std::string_view(base).substr(base.size() - kSockSuffix.size()) ==
          kSockSuffix) {
    return absl::StrCat(base.substr(0, base.size() - kSockSuffix.size()), "-",
                        seq, kSockSuffix);
  }
  return absl::StrCat(base, "-", seq);
}

class SidecarBackend : public CoreBackend {
 public:
  SidecarBackend(SidecarMode mode, std::string binary, std::string socket,
                 std::string const& configured_socket,
                 std::vector<std::string> args, int channels)
      : mode_(mode),
        binary_(std::move(binary)),
        socket_(std::move(socket)),
        args_(std::move(args)),
        channels_(channels),
        // Configured (not chosen) socket: Clients that leave it empty share
        // one sidecar even though its default path may carry a sequence suffix.
        key_(absl::StrCat("sidecar\x1f", ModeKey(mode_), "\x1f", binary_,
                          "\x1f", configured_socket, "\x1f", channels_, "\x1f",
                          absl::StrJoin(args_, "\x1e"))) {}

  std::string const& SharingKey() const override { return key_; }
  std::string const& SocketPath() const override { return socket_; }

  StatusOr<std::vector<std::shared_ptr<grpc::Channel>>> Start() override {
    process_.reset();
    external_pid_.reset();
    exit_description_ = "not running";

    if (mode_ == SidecarMode::kDaemon) {
      if (std::optional<pid_t> existing = ProbeSocketListener(socket_)) {
        external_pid_ = existing;
      }
    }
    if (!external_pid_.has_value()) {
      StatusOr<std::unique_ptr<SidecarProcess>> process =
          SidecarProcess::Spawn(binary_, socket_, mode_, args_);
      if (!process) return std::move(process).status();
      process_ = *std::move(process);
    }

    std::vector<std::shared_ptr<grpc::Channel>> channels;
    channels.reserve(static_cast<std::size_t>(channels_));
    for (int i = 0; i != channels_; ++i) {
      channels.push_back(grpc::CreateCustomChannel(
          "unix:" + socket_, grpc::InsecureChannelCredentials(),
          LocalChannelArguments(i)));
    }
    return channels;
  }

  void OnReady() override {
    if (mode_ != SidecarMode::kDaemon || !process_) return;
    if (std::optional<pid_t> listener = ProbeSocketListener(socket_);
        listener.has_value() && *listener != process_->pid()) {
      // Another process won the startup race on `socket_` while `Ping`
      // connected to the winner; detach our loser child and adopt the winner.
      process_->Detach();
      process_.reset();
      external_pid_ = listener;
    }
  }

  bool Exited() override {
    if (external_pid_.has_value()) {
      if (ProbeSocketListener(socket_) == external_pid_) return false;
      exit_description_ =
          absl::StrCat("daemon pid ", *external_pid_, " stopped listening");
      return true;
    }
    if (!process_) return true;
    if (!process_->Exited()) return false;
    // When two processes spawn a daemon simultaneously on the same socket, the
    // loser exits with status 1 ("another server is already listening on ...").
    // Adopt the winner rather than failing Create().
    if (mode_ == SidecarMode::kDaemon && process_->ExitCode() == 1) {
      for (int attempt = 0; attempt != 10; ++attempt) {
        struct stat st{};
        if (::lstat(socket_.c_str(), &st) != 0) break;
        if (std::optional<pid_t> peer = ProbeSocketListener(socket_);
            peer.has_value() && *peer != process_->pid()) {
          process_->Detach();
          process_.reset();
          external_pid_ = peer;
          return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    }
    exit_description_ = process_->ExitDescription();
    return true;
  }

  std::string ExitDescription() override { return exit_description_; }

  void Stop() override {
    switch (mode_) {
      case SidecarMode::kDaemon:
        if (process_) {
          process_->Detach();
          process_.reset();
        }
        external_pid_.reset();
        break;
      case SidecarMode::kChild:
        if (process_) {
          process_->Terminate();
          process_.reset();
        }
        external_pid_.reset();
        break;
    }
  }

  void Kill() override {
    if (process_) {
      process_->Kill();
      exit_description_ = process_->ExitDescription();
      process_.reset();
      return;
    }
    if (external_pid_.has_value()) {
      pid_t const target = *external_pid_;
      ::kill(target, SIGKILL);
      std::chrono::steady_clock::time_point const deadline =
          std::chrono::steady_clock::now() + std::chrono::seconds(2);
      while (ProbeSocketListener(socket_) == target &&
             std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
      }
      RemoveSocketFiles(socket_);
      exit_description_ = absl::StrCat("daemon pid ", target, " killed");
      external_pid_.reset();
    }
  }

  int pid() const override {
    if (process_) return process_->pid();
    if (external_pid_.has_value()) return *external_pid_;
    return 0;
  }

  std::string Describe() const override {
    return absl::StrCat("sidecar ", binary_);
  }

  std::optional<std::uint64_t> CoreThreadCpuMicros() const override {
    return std::nullopt;
  }

 private:
  SidecarMode const mode_;
  std::string const binary_;
  std::string const socket_;
  std::vector<std::string> const args_;
  int const channels_;
  std::string const key_;
  std::unique_ptr<SidecarProcess> process_;
  std::optional<pid_t> external_pid_;
  std::string exit_description_ = "not running";
};

}  // namespace

StatusOr<std::unique_ptr<CoreBackend>> MakeCoreBackend(
    ClientOptions const& options,
    std::vector<std::string> const& sockets_in_use) {
  StatusOr<std::string> binary = ResolveSidecarPath(options.sidecar_path);
  if (!binary) return std::move(binary).status();
  if (Status s = ValidateServerFlags(options.sidecar_args); !s.ok()) return s;
  std::string socket;
  if (!options.socket_path.empty()) {
    socket = options.socket_path;
  } else {
    StatusOr<std::string> base = DefaultSocketPath(options.sidecar_mode);
    if (!base) return std::move(base).status();
    // Only the first default-path sidecar of this process gets the plain path;
    // further distinct configurations get a sequence suffix.
    static std::uint64_t next_socket_seq = 0;
    socket = *base;
    auto in_use = [&](std::string const& candidate) {
      return std::find(sockets_in_use.begin(), sockets_in_use.end(),
                       candidate) != sockets_in_use.end();
    };
    while (in_use(socket)) socket = SuffixSocketPath(*base, ++next_socket_seq);
  }
  if (Status s = ValidateSocketPath(socket); !s.ok()) return s;
  return std::unique_ptr<CoreBackend>(new SidecarBackend(
      options.sidecar_mode, *std::move(binary), std::move(socket),
      options.socket_path, options.sidecar_args, options.channels));
}

}  // namespace internal
}  // namespace gcs_core
