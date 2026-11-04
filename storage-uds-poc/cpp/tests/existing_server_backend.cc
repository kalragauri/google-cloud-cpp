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

// A third backend for the wrapper's unit tests: connects to a server that is
// already listening on `ClientOptions::socket_path` (a fake `GcsCore`
// started by the test), so the wrapper's framing and mapping can be tested
// without starting `gcs-core-server` or reaching GCS.

#include "absl/strings/str_cat.h"
#include "core_backend.h"
#include <unistd.h>

namespace gcs_core {
namespace internal {
namespace {

class ExistingServerBackend : public CoreBackend {
 public:
  ExistingServerBackend(std::string socket, int channels)
      : socket_(std::move(socket)),
        channels_(channels),
        key_(absl::StrCat("existing\x1f", socket_, "\x1f", channels_)) {}

  std::string const& SharingKey() const override { return key_; }
  std::string const& SocketPath() const override { return socket_; }

  StatusOr<std::vector<std::shared_ptr<grpc::Channel>>> Start() override {
    exited_ = false;
    std::vector<std::shared_ptr<grpc::Channel>> channels;
    for (int i = 0; i != channels_; ++i) {
      channels.push_back(grpc::CreateCustomChannel(
          "unix:" + socket_, grpc::InsecureChannelCredentials(),
          LocalChannelArguments(i)));
    }
    return channels;
  }
  // The server belongs to the test and keeps running; `Kill()` only marks
  // this backend exited, so a test can drive the wrapper's restart path by
  // making the fake fail a call with UNAVAILABLE.
  bool Exited() override { return exited_; }
  std::string ExitDescription() override { return "killed for testing"; }
  void Stop() override {}
  void Kill() override { exited_ = true; }
  int pid() const override { return ::getpid(); }
  std::string Describe() const override { return "test server"; }
  std::optional<std::uint64_t> CoreThreadCpuMicros() const override {
    return std::nullopt;
  }

 private:
  std::string const socket_;
  int const channels_;
  std::string const key_;
  bool exited_ = false;
};

}  // namespace

StatusOr<std::unique_ptr<CoreBackend>> MakeCoreBackend(
    ClientOptions const& options, std::vector<std::string> const&) {
  return std::unique_ptr<CoreBackend>(
      new ExistingServerBackend(options.socket_path, options.channels));
}

}  // namespace internal
}  // namespace gcs_core
