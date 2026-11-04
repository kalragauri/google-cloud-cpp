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

// Benchmark target: the thin C++ wrapper → UDS → Rust core sidecar process.
// The workloads are in gcs_core_target.h; this file only names the variant
// and reads the sidecar's counters over GetStats.
//
// Extra flags: --channels=N (UDS connections), --chunk-size=BYTES (read/write
// message size hint), --sidecar-arg=ARG (repeatable, passed through).

#include "gcs_core/client.h"
#include "gcs_core_target.h"
#include "ssb_workloads.h"

namespace {

class Uds : public gcs_core_bench::WrapperTarget {
 public:
  Uds(gcs_core::Client client, std::string labels)
      : WrapperTarget(std::move(client)), labels_(std::move(labels)) {}

  std::string Name() const override { return "uds"; }
  std::string Labels() const override { return labels_; }

  /// The sidecar's own counters (another process): the harness adds them to
  /// this process's as `peer`.
  std::optional<ssb::ProcessSnapshot> PeerSnapshot() override {
    gcs_core::StatusOr<gcs_core::SidecarStats> s = client_.GetSidecarStats();
    if (!s) return std::nullopt;
    return ssb::ProcessSnapshot{
        static_cast<double>(s->user_cpu_us + s->system_cpu_us),
        s->allocated_bytes, static_cast<std::int64_t>(s->rss_bytes / 1024),
        static_cast<std::int64_t>(s->peak_rss_bytes / 1024)};
  }

 private:
  std::string labels_;
};

}  // namespace

int main(int argc, char* argv[]) {
  ssb::Args args;
  std::vector<std::string> const rest = ssb::ParseArgs(argc, argv, args);
  gcs_core::ClientOptions options;
  if (!gcs_core_bench::ParseWrapperFlags(rest, options)) return 2;
  gcs_core::StatusOr<gcs_core::Client> client =
      gcs_core::Client::Create(options);
  if (!client) {
    std::cerr << "Client::Create: " << client.status() << "\n";
    return 1;
  }
  std::string const labels =
      gcs_core_bench::WrapperLabels("uds", options, *client);
  Uds target(*std::move(client), labels);
  return ssb::Runner(target, args).Run();
}
