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

// Cold start of the wrapper in a fresh process: how long `Client::Create()`
// takes until the core serves, and the footprint right after. Built once per
// variant (`cold_start`, `cold_start_inproc`) so a sidecar spawn and an
// in-process start are measured through the same API call.
//
//   cold_start [--channels=N] [--socket-path=PATH] [--child] [--hold-ms=N]
//              [--sidecar-arg=ARG]...
//
// Prints `create_ms=<ms>`, `core_pid=<pid>`, and the VmRSS/VmHWM/VmSize/Threads
// lines of this process (`self`) and, when the core is another process, of that
// process (`core`).

#include "absl/strings/match.h"
#include "gcs_core/client.h"
#include <chrono>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <unistd.h>

namespace {

void PrintStatus(char const* who, int pid) {
  std::ifstream status("/proc/" + std::to_string(pid) + "/status");
  std::string line;
  while (std::getline(status, line)) {
    if (absl::StartsWith(line, "VmRSS:") || absl::StartsWith(line, "VmHWM:") ||
        absl::StartsWith(line, "VmSize:") ||
        absl::StartsWith(line, "Threads:")) {
      std::cout << who << " " << line << "\n";
    }
  }
}

}  // namespace

int main(int argc, char* argv[]) {
  gcs_core::ClientOptions options;
  int hold_ms = 0;
  for (int i = 1; i < argc; ++i) {
    std::string const arg = argv[i];
    std::size_t const eq = arg.find('=');
    std::string const key = arg.substr(0, eq);
    std::string const val =
        eq == std::string::npos ? std::string() : arg.substr(eq + 1);
    if (key == "--channels") {
      options.channels = std::stoi(val);
    } else if (key == "--socket-path") {
      options.socket_path = val;
    } else if (arg == "--child") {
      options.sidecar_mode = gcs_core::SidecarMode::kChild;
    } else if (key == "--hold-ms") {
      hold_ms = std::stoi(val);
    } else if (key == "--sidecar-arg") {
      options.sidecar_args.push_back(val);
    } else {
      std::cerr << "usage: cold_start [--channels=N] [--socket-path=PATH] "
                   "[--child] [--hold-ms=N] [--sidecar-arg=ARG]...\n";
      return 2;
    }
  }
  std::chrono::steady_clock::time_point const t0 =
      std::chrono::steady_clock::now();
  gcs_core::StatusOr<gcs_core::Client> client =
      gcs_core::Client::Create(options);
  std::chrono::steady_clock::time_point const t1 =
      std::chrono::steady_clock::now();
  if (!client) {
    std::cerr << "Client::Create: " << client.status() << "\n";
    return 1;
  }
  std::cout << "create_ms="
            << std::chrono::duration<double, std::milli>(t1 - t0).count()
            << "\n";
  std::cout << "core_pid=" << client->sidecar_pid() << "\n";
  PrintStatus("self", ::getpid());
  if (client->sidecar_pid() != ::getpid()) {
    PrintStatus("core", client->sidecar_pid());
  }
  std::cout.flush();
  if (hold_ms > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(hold_ms));
  }
  return 0;
}
