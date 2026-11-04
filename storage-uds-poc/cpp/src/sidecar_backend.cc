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

// The process-boundary backend: the shared `gcs-core-server` runs as a child
// process in its 1:1 mode and the channels are `unix:` sockets to it.

#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "core_backend.h"
#include "sidecar_process.h"
#include <algorithm>
#include <string_view>

namespace gcs_core {
namespace internal {
namespace {

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
  SidecarBackend(std::string binary, std::string socket,
                 std::string const& configured_socket,
                 std::vector<std::string> args, int channels)
      : binary_(std::move(binary)),
        socket_(std::move(socket)),
        args_(std::move(args)),
        channels_(channels),
        // Configured (not chosen) socket: Clients that leave it empty share
        // one sidecar even though its default path carries a sequence suffix.
        key_(absl::StrCat("sidecar\x1f", binary_, "\x1f", configured_socket,
                          "\x1f", channels_, "\x1f",
                          absl::StrJoin(args_, "\x1e"))) {}

  std::string const& SharingKey() const override { return key_; }
  std::string const& SocketPath() const override { return socket_; }

  StatusOr<std::vector<std::shared_ptr<grpc::Channel>>> Start() override {
    StatusOr<std::unique_ptr<SidecarProcess>> process =
        SidecarProcess::Spawn(binary_, socket_, args_);
    if (!process) return std::move(process).status();
    process_ = *std::move(process);
    std::vector<std::shared_ptr<grpc::Channel>> channels;
    channels.reserve(static_cast<std::size_t>(channels_));
    for (int i = 0; i != channels_; ++i) {
      channels.push_back(grpc::CreateCustomChannel(
          "unix:" + socket_, grpc::InsecureChannelCredentials(),
          LocalChannelArguments(i)));
    }
    return channels;
  }

  bool Exited() override { return !process_ || process_->Exited(); }

  std::string ExitDescription() override {
    return process_ ? process_->ExitDescription() : "not running";
  }

  void Stop() override {
    if (!process_) return;
    process_->Terminate();
    process_.reset();
  }

  void Kill() override {
    if (!process_) return;
    process_->Kill();
    process_.reset();
  }

  int pid() const override { return process_ ? process_->pid() : 0; }

  std::string Describe() const override {
    return absl::StrCat("sidecar ", binary_);
  }

  std::optional<std::uint64_t> CoreThreadCpuMicros() const override {
    return std::nullopt;
  }

 private:
  std::string const binary_;
  std::string const socket_;
  std::vector<std::string> const args_;
  int const channels_;
  std::string const key_;
  std::unique_ptr<SidecarProcess> process_;
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
    StatusOr<std::string> base = DefaultSocketPath();
    if (!base) return std::move(base).status();
    // Only the first default-path sidecar of this process gets the plain
    // `<pid>.sock`; further configurations get a sequence suffix.
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
      *std::move(binary), std::move(socket), options.socket_path,
      options.sidecar_args, options.channels));
}

}  // namespace internal
}  // namespace gcs_core
