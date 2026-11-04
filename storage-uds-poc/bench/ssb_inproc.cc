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

// Benchmark target: the thin C++ wrapper → socket pair → Rust core on tokio
// threads inside this process. The workloads are in gcs_core_target.h; this
// file names the variant and splits this process's counters into an
// application part and a core part.
//
// One process means one `getrusage` and one `malloc` counter. The core's
// share is attributed by thread: the core's tokio threads are tagged through
// `gcs_core::inproc::SetThreadHooks`, their CPU is summed from per-thread
// clocks by the core, and the malloc interposer counts bytes requested on a
// tagged thread separately. The runner subtracts that share from the process
// (`PeerSharesProcess()`), so `app + peer` is still the whole process, exactly
// as `app + sidecar` is for `ssb_uds`, and reads it outside the measured
// window like the sidecar's GetStats. RSS has no per-thread meaning and stays
// process-wide in the `app` column.
//
// Extra flags: --channels=N (socket pairs), --chunk-size=BYTES (read/write
// message size hint), --sidecar-arg=ARG (repeatable, passed to the core; e.g.
// --sidecar-arg=--link=abstract).

#include "alloc_counter.h"
#include "gcs_core/client.h"
#include "gcs_core/inproc.h"
#include "gcs_core_target.h"
#include "ssb_workloads.h"

namespace {

class Inproc : public gcs_core_bench::WrapperTarget {
 public:
  Inproc(gcs_core::Client client, std::string labels)
      : WrapperTarget(std::move(client)), labels_(std::move(labels)) {}

  std::string Name() const override { return "inproc"; }
  std::string Labels() const override { return labels_; }

  /// The core's share of this process: CPU of its threads, bytes requested
  /// on its threads. No RSS of its own.
  std::optional<ssb::ProcessSnapshot> PeerSnapshot() override {
    std::optional<std::uint64_t> const cpu = client_.core_thread_cpu_us();
    if (!cpu) return std::nullopt;
    return ssb::ProcessSnapshot{static_cast<double>(*cpu),
                                alloc_counter::core_bytes(), 0, 0};
  }

  bool PeerSharesProcess() const override { return true; }

 private:
  std::string labels_;
};

}  // namespace

int main(int argc, char* argv[]) {
  ssb::Args args;
  std::vector<std::string> const rest = ssb::ParseArgs(argc, argv, args);
  gcs_core::ClientOptions options;
  if (!gcs_core_bench::ParseWrapperFlags(rest, options)) return 2;

  // Tag the core's threads before the core exists; both hooks only flip a
  // thread-local, so they are safe on any thread at any time.
  gcs_core::inproc::ThreadHooks hooks;
  hooks.on_start = [](void*) { alloc_counter::MarkCoreThread(true); };
  hooks.on_stop = [](void*) { alloc_counter::MarkCoreThread(false); };
  gcs_core::inproc::SetThreadHooks(hooks);

  gcs_core::StatusOr<gcs_core::Client> client =
      gcs_core::Client::Create(options);
  if (!client) {
    std::cerr << "Client::Create: " << client.status() << "\n";
    return 1;
  }
  // In-process the core's GetStats describes this very process; the harness
  // must never add it as a peer, and the core's own allocation counter must
  // be idle (this binary's interposer counts for everyone).
  if (gcs_core::StatusOr<gcs_core::SidecarStats> stats =
          client->GetSidecarStats();
      !stats || stats->allocated_bytes != 0) {
    std::cerr << "unexpected core stats in-process: "
              << (stats ? "allocated_bytes != 0"
                        : google::cloud::StatusCodeToString(
                              stats.status().code()))
              << "\n";
    return 1;
  }
  if (!client->core_thread_cpu_us()) {
    std::cerr << "core_thread_cpu_us unavailable: not an in-process core\n";
    return 1;
  }
  std::string labels =
      gcs_core_bench::WrapperLabels("inproc", options, *client);
  bool const abstract_link =
      std::any_of(options.sidecar_args.begin(), options.sidecar_args.end(),
                  [](std::string const& a) { return a == "--link=abstract"; });
  labels += abstract_link ? ";link=abstract" : ";link=socketpair";
  Inproc target(*std::move(client), labels);
  return ssb::Runner(target, args).Run();
}
