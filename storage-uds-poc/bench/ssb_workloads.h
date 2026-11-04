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

// Workloads shared by every implementation (uds wrapper, native C++), so each
// runs the exact same operations in the same order.
//
// Derived from the C++ FFI POC harness (v-pratap/google-cloud-rust
// examples/cpp-wrapper-poc/cpp/bench/ssb_workloads.h), which re-implements the
// storage-shared-benchmarking workloads. Changes:
//   * two-process measurement: an implementation may expose a "peer" process
//     (the sidecar) whose CPU, allocations and RSS are recorded alongside the
//     benchmark process's, read outside the timed window;
//   * workloads for the doc's grid: w1r3 (JSON), list (gRPC), open /
//     random8k / read50x2x4 (gRPC bidi), appendable (zonal), throughput;
//   * unsupported operations are recorded as rows with error "UNSUPPORTED"
//     instead of being skipped, so gaps are visible in the data.
//
//   w1r3       : upload(size) SINGLE-SHOT or RESUMABLE ; READ[0..2] ; DELETE
//   list       : (setup N objects) ; LIST-P1000 ; LIST-P100 per iteration
//   open       : upload ; OPEN ; DELETE
//   random8k   : upload ; OPEN + one 8 KiB read at a random offset ; DELETE
//   read50x2x4 : upload(200 MiB) ; OPEN + 50 concurrent 2 MiB reads, 4 MiB
//                stride ; DELETE
//   appendable : APPENDABLE upload(size) ; DELETE
//   throughput : W workers; each iteration reads (or writes) one object of
//                the largest size; reports aggregate MiB/s
//
// Object sizes default to SSB's {100 KB, 2 MiB, 100 MB}, picked per iteration
// from a fixed-seed PRNG so every implementation sees the same sequence.

#ifndef GCS_UDS_POC_BENCH_SSB_WORKLOADS_H
#define GCS_UDS_POC_BENCH_SSB_WORKLOADS_H

#include "absl/strings/match.h"
#include "alloc_counter.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>
#include <sys/resource.h>

namespace ssb {

constexpr std::int64_t kKB = 1000;
constexpr std::int64_t kMB = kKB * kKB;
constexpr std::int64_t kKiB = 1024;
constexpr std::int64_t kMiB = kKiB * kKiB;
inline constexpr std::string_view kUnsupported = "UNSUPPORTED";

/// Cumulative counters of one process.
struct ProcessSnapshot {
  double cpu_us = 0;
  std::uint64_t alloc_bytes = 0;
  std::int64_t rss_kib = 0;
  std::int64_t peak_rss_kib = 0;
};

inline double SelfCpuUs() {
  rusage ru{};
  getrusage(RUSAGE_SELF, &ru);
  return (static_cast<double>(ru.ru_utime.tv_sec) * 1e6) +
         static_cast<double>(ru.ru_utime.tv_usec) +
         (static_cast<double>(ru.ru_stime.tv_sec) * 1e6) +
         static_cast<double>(ru.ru_stime.tv_usec);
}

inline ProcessSnapshot SelfSnapshot() {
  return {SelfCpuUs(), alloc_counter::malloc_bytes(), alloc_counter::rss_kib(),
          alloc_counter::peak_rss_kib()};
}

using Ranges = std::vector<std::pair<std::int64_t, std::int64_t>>;

/// SplitMix64: a generator simple enough to reproduce bit-for-bit in the
/// pure-Rust comparator, so every implementation sees the same object sizes
/// and read offsets (std::uniform_int_distribution is not portable).
class SplitMix64 {
 public:
  explicit SplitMix64(std::uint64_t seed) : state_(seed) {}
  void Seed(std::uint64_t seed) { state_ = seed; }
  std::uint64_t Next() {
    std::uint64_t z = (state_ += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
  }
  /// Uniform-enough value in [0, n) for benchmark purposes.
  std::uint64_t Below(std::uint64_t n) { return n == 0 ? 0 : Next() % n; }

 private:
  std::uint64_t state_;
};

/// What each implementation provides. Errors are strings (empty == OK);
/// return `kUnsupported` when the implementation cannot serve the operation.
class Target {
 public:
  virtual ~Target() = default;
  virtual std::string Name() const = 0;
  /// Free-form `key=value;...` describing the configuration (CSV `labels`).
  virtual std::string Labels() const = 0;
  /// True when PeerSnapshot() reports a share of *this* process (a core on
  /// threads of the benchmark) rather than another process (a sidecar). The
  /// runner then reports `app = process - peer`, so `app + peer` is the whole
  /// process either way.
  virtual bool PeerSharesProcess() const { return false; }
  /// Counters of a second process doing work for this implementation.
  virtual std::optional<ProcessSnapshot> PeerSnapshot() { return std::nullopt; }

  // JSON path.
  virtual std::string JsonUpload(std::string const& bucket,
                                 std::string const& object, char const* data,
                                 std::int64_t size, bool resumable) = 0;
  virtual std::string JsonRead(std::string const& bucket,
                               std::string const& object,
                               std::int64_t* bytes) = 0;
  // List and delete (protocol depends on the implementation; see Labels()).
  virtual std::string List(std::string const& bucket, std::string const& prefix,
                           int page_size, std::int64_t* count) = 0;
  virtual std::string Delete(std::string const& bucket,
                             std::string const& object) = 0;
  // gRPC bidi path. Open/Read/ReadConcurrent use one "current" descriptor and
  // are only called from one thread; BidiReadAll is thread-safe.
  virtual std::string AppendableUpload(std::string const& bucket,
                                       std::string const& object,
                                       char const* data, std::int64_t size) = 0;
  virtual std::string Open(std::string const& bucket,
                           std::string const& object) = 0;
  virtual std::string Read(std::int64_t offset, std::int64_t length,
                           std::int64_t* bytes) = 0;
  virtual std::string ReadConcurrent(Ranges const& ranges,
                                     std::int64_t* bytes) = 0;
  virtual void CloseDescriptor() = 0;
  virtual std::string BidiReadAll(std::string const& bucket,
                                  std::string const& object,
                                  std::int64_t* bytes) = 0;
};

struct Args {
  std::string bucket;
  std::string workload = "w1r3";
  std::string bucket_type = "regional";  // regional | zonal
  int iterations = 100;
  int warmup = 1;
  int workers = 8;
  int list_objects = 1000;
  std::string throughput_op = "read";  // read | write
  std::vector<std::int64_t> object_sizes = {100 * kKB, 2 * kMiB, 100 * kMB};
  std::string out;
  std::string prefix = "gcs-uds-poc/bench/";
  std::string label;
};

inline std::vector<std::int64_t> ParseSizes(std::string const& s) {
  std::vector<std::int64_t> r;
  if (s.empty()) return r;
  std::stringstream ss(s);
  std::string tok;
  while (std::getline(ss, tok, ',')) {
    if (tok.empty()) return {};
    r.push_back(std::stoll(tok));
  }
  return r;
}

/// Parses the shared flags; flags it does not know are returned for the
/// implementation-specific parser.
inline std::vector<std::string> ParseArgs(int argc, char* argv[], Args& a) {
  std::vector<std::string> rest;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    std::size_t const eq = arg.find('=');
    std::string const key = arg.substr(0, eq);
    std::string const val =
        eq == std::string::npos ? std::string() : arg.substr(eq + 1);
    if (key == "--bucket")
      a.bucket = val;
    else if (key == "--workload")
      a.workload = val;
    else if (key == "--bucket-type")
      a.bucket_type = val;
    else if (key == "--iterations")
      a.iterations = std::stoi(val);
    else if (key == "--warmup")
      a.warmup = std::stoi(val);
    else if (key == "--workers")
      a.workers = std::stoi(val);
    else if (key == "--list-objects")
      a.list_objects = std::stoi(val);
    else if (key == "--throughput-op")
      a.throughput_op = val;
    else if (key == "--object-sizes")
      a.object_sizes = ParseSizes(val);
    else if (key == "--out")
      a.out = val;
    else if (key == "--prefix")
      a.prefix = val;
    else if (key == "--label")
      a.label = val;
    else
      rest.push_back(arg);
  }
  if (a.bucket.empty()) {
    std::cerr
        << "--bucket is required\n"
           "flags: --bucket=B --workload=w1r3|list|open|random8k|"
           "read50x2x4|appendable|throughput --bucket-type=regional|zonal "
           "[--iterations=100] [--warmup=1] [--workers=8] "
           "[--list-objects=1000] [--throughput-op=read|write] "
           "[--object-sizes=100000,2097152,100000000] [--out=f.csv] "
           "[--prefix=P] [--label=L]\n";
    std::exit(2);
  }
  if (a.bucket_type != "regional" && a.bucket_type != "zonal") {
    std::cerr << "--bucket-type must be regional or zonal\n";
    std::exit(2);
  }
  if (a.throughput_op != "read" && a.throughput_op != "write") {
    std::cerr << "--throughput-op must be read or write\n";
    std::exit(2);
  }
  if (a.iterations < 1 || a.warmup < 0 || a.workers < 1 || a.list_objects < 1) {
    std::cerr << "--iterations, --workers, and --list-objects must be >= 1 "
                 "and --warmup >= 0\n";
    std::exit(2);
  }
  if (a.object_sizes.empty() ||
      std::any_of(a.object_sizes.begin(), a.object_sizes.end(),
                  [](std::int64_t sz) { return sz <= 0; })) {
    std::cerr << "--object-sizes must be a non-empty list of positive sizes\n";
    std::exit(2);
  }
  return rest;
}

using Clock = std::chrono::steady_clock;

/// One measured operation: wall time and benchmark-process counters between
/// construction and Stop(); peer counters around the whole window.
struct Sample {
  Sample(std::string workload, std::string op, int iteration,
         std::int64_t object_size = 0, std::int64_t bytes = 0)
      : workload(std::move(workload)),
        op(std::move(op)),
        iteration(iteration),
        object_size(object_size),
        bytes(bytes) {}

  std::string workload;
  std::string op;
  int iteration = 0;
  std::int64_t object_size = 0;
  std::int64_t bytes = 0;
  double wall_ms = 0;
  double cpu_ms_app = 0;
  double cpu_ms_peer = 0;
  bool ok = true;
  std::string error;
  std::uint64_t alloc_app = 0;
  std::uint64_t alloc_peer = 0;
  std::int64_t rss_kib_app = 0;
  std::int64_t rss_kib_peer = 0;
};

/// Fills the app/peer counters of `s` from snapshots taken in the order
/// `peer_before, self_before, <work>, self_after, peer_after`: the peer reads
/// (an RPC to a sidecar, or a walk over core-thread clocks in-process) run
/// outside the [self_before, self_after] window, so their own cost is never
/// charged to the work. When the peer shares this process, its deltas are
/// subtracted from the process deltas; allocations are clamped at 0 because
/// the peer window is slightly wider than the process window.
inline void Attribute(Sample& s, ProcessSnapshot const& self_before,
                      ProcessSnapshot const& self_after,
                      std::optional<ProcessSnapshot> const& peer_before,
                      std::optional<ProcessSnapshot> const& peer_after,
                      bool peer_shares_process, bool peak_rss) {
  double self_cpu_us = self_after.cpu_us - self_before.cpu_us;
  auto self_alloc = static_cast<std::int64_t>(self_after.alloc_bytes -
                                              self_before.alloc_bytes);
  s.rss_kib_app = peak_rss ? self_after.peak_rss_kib : self_after.rss_kib;
  if (peer_before && peer_after) {
    double const peer_cpu_us = peer_after->cpu_us - peer_before->cpu_us;
    auto const peer_alloc = static_cast<std::int64_t>(peer_after->alloc_bytes -
                                                      peer_before->alloc_bytes);
    s.cpu_ms_peer = peer_cpu_us / 1000.0;
    s.alloc_peer =
        static_cast<std::uint64_t>(std::max<std::int64_t>(0, peer_alloc));
    s.rss_kib_peer = peak_rss ? peer_after->peak_rss_kib : peer_after->rss_kib;
    if (peer_shares_process) {
      self_cpu_us = std::max(0.0, self_cpu_us - peer_cpu_us);
      self_alloc -= peer_alloc;
    }
  }
  s.cpu_ms_app = self_cpu_us / 1000.0;
  s.alloc_app =
      static_cast<std::uint64_t>(std::max<std::int64_t>(0, self_alloc));
}

class Recorder {
 public:
  Recorder(std::string impl, std::string labels, std::string const& path)
      : impl_(std::move(impl)), labels_(std::move(labels)), csv_(path) {
    csv_ << "impl,workload,iteration,object_size,op,latency_ms,cpu_ms_app,"
            "cpu_ms_peer,cpu_ms_total,bytes,ok,error,alloc_bytes_app,"
            "alloc_bytes_peer,rss_kib_app,rss_kib_peer,labels\n";
  }
  void Add(Sample s) {
    std::scoped_lock lk(mu_);
    std::string err = s.error.substr(0, 160);
    std::replace(err.begin(), err.end(), ',', ';');
    std::replace(err.begin(), err.end(), '\n', ' ');
    csv_ << impl_ << ',' << s.workload << ',' << s.iteration << ','
         << s.object_size << ',' << s.op << ',' << std::fixed
         << std::setprecision(3) << s.wall_ms << ',' << s.cpu_ms_app << ','
         << s.cpu_ms_peer << ',' << s.cpu_ms_app + s.cpu_ms_peer << ','
         << s.bytes << ',' << (s.ok ? 1 : 0) << ',' << err << ',' << s.alloc_app
         << ',' << s.alloc_peer << ',' << s.rss_kib_app << ',' << s.rss_kib_peer
         << ',' << labels_ << '\n';
    csv_.flush();
    if (!s.ok && s.error != kUnsupported) {
      std::cerr << "  ERROR " << s.workload << " #" << s.iteration << " "
                << s.op << ": " << s.error << "\n";
    }
    samples_.push_back(std::move(s));
  }

  void PrintSummary() const {
    using Key = std::tuple<std::string, std::string, std::int64_t>;
    std::map<Key, std::vector<Sample const*>> groups;
    for (Sample const& s : samples_) {
      if (s.iteration >= 0)
        groups[{s.workload, s.op, s.object_size}].push_back(&s);
    }
    std::cout << "\nSUMMARY impl=" << impl_ << " labels=" << labels_ << "\n"
              << std::left << std::setw(11) << "workload" << std::setw(14)
              << "op" << std::right << std::setw(11) << "size" << std::setw(5)
              << "n" << std::setw(5) << "err" << std::setw(10) << "p50ms"
              << std::setw(10) << "p90ms" << std::setw(10) << "p99ms"
              << std::setw(10) << "MiB/s50" << std::setw(11) << "cpuApp50"
              << std::setw(11) << "cpuPeer50" << "\n";
    for (auto const& [k, v] : groups) {
      std::vector<double> lat;
      std::vector<double> app;
      std::vector<double> peer;
      std::vector<double> tput;
      int errors = 0;
      for (Sample const* s : v) {
        if (!s->ok) {
          ++errors;
          continue;
        }
        lat.push_back(s->wall_ms);
        app.push_back(s->cpu_ms_app);
        peer.push_back(s->cpu_ms_peer);
        if (s->bytes > 0 && s->wall_ms > 0) {
          tput.push_back((static_cast<double>(s->bytes) / 1048576.0) /
                         (s->wall_ms / 1000.0));
        }
      }
      std::cout << std::left << std::setw(11) << std::get<0>(k) << std::setw(14)
                << std::get<1>(k) << std::right << std::setw(11)
                << std::get<2>(k) << std::setw(5) << v.size() << std::setw(5)
                << errors << std::fixed << std::setprecision(2) << std::setw(10)
                << Pct(lat, 50) << std::setw(10) << Pct(lat, 90)
                << std::setw(10) << Pct(lat, 99) << std::setw(10)
                << Pct(tput, 50) << std::setw(11) << Pct(app, 50)
                << std::setw(11) << Pct(peer, 50) << "\n";
    }
  }

  static double Pct(std::vector<double> v, double p) {
    if (v.empty()) return 0;
    std::sort(v.begin(), v.end());
    auto const idx = static_cast<std::size_t>(std::max(
        0.0, std::round((p / 100.0) * static_cast<double>(v.size() - 1))));
    return v[std::min(idx, v.size() - 1)];
  }

 private:
  std::string impl_;
  std::string labels_;
  std::mutex mu_;
  std::ofstream csv_;
  std::vector<Sample> samples_;
};

class Runner {
 public:
  Runner(Target& t, Args a)
      : t_(t),
        a_(std::move(a)),
        gen_(42),
        impl_(t.Name() + (a_.label.empty() ? "" : "-" + a_.label)),
        run_id_(std::to_string(
            std::chrono::system_clock::now().time_since_epoch().count())) {
    std::int64_t max_size =
        *std::max_element(a_.object_sizes.begin(), a_.object_sizes.end());
    if (a_.workload == "read50x2x4")
      max_size = std::max<std::int64_t>(max_size, 200 * kMiB);
    data_.resize(static_cast<std::size_t>(max_size));
    SplitMix64 g(7);
    for (std::size_t i = 0; i + 8 <= data_.size(); i += 8) {
      std::uint64_t const v = g.Next();
      std::memcpy(&data_[i], &v, 8);
    }
  }

  int Run() {
    std::string const csv =
        a_.out.empty() ? impl_ + "-" + a_.workload + ".csv" : a_.out;
    Recorder rec(impl_, t_.Labels(), csv);
    std::cout << "impl=" << impl_ << " labels=" << t_.Labels()
              << " bucket=" << a_.bucket << " workload=" << a_.workload
              << " iterations=" << a_.iterations << " csv=" << csv << "\n";
    gen_.Seed(42);
    std::optional<ProcessSnapshot> const peer_start = t_.PeerSnapshot();
    ProcessSnapshot const self_start = SelfSnapshot();
    Clock::time_point const t0 = Clock::now();
    if (a_.workload == "list") {
      ListWorkload(rec);
    } else if (a_.workload == "throughput") {
      ThroughputWorkload(rec);
    } else {
      for (int i = -a_.warmup; i < a_.iterations; ++i) Iteration(i, rec);
    }
    // Whole-run rows (iteration = -1): totals and RSS for both processes.
    Sample total{a_.workload, "RUN_TOTAL", -1};
    total.wall_ms =
        std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
    ProcessSnapshot const self_end = SelfSnapshot();
    std::optional<ProcessSnapshot> const peer_end = t_.PeerSnapshot();
    Attribute(total, self_start, self_end, peer_start, peer_end,
              t_.PeerSharesProcess(), /*peak_rss=*/true);
    rec.Add(total);
    rec.PrintSummary();
    return 0;
  }

 private:
  std::string ObjectName(std::string const& w, int i) const {
    return a_.prefix + impl_ + "/" + run_id_ + "/" + w + "-" +
           std::to_string(i);
  }

  std::int64_t PickSize() {
    return a_.object_sizes[gen_.Below(a_.object_sizes.size())];
  }

  /// Times `f`. Peer counters are read before the window starts and after it
  /// ends, so the snapshot RPC itself is not inside the measured latency.
  template <typename F>
  bool Measure(Recorder& rec, int i, std::int64_t size, std::string const& op,
               F&& f) {
    std::optional<ProcessSnapshot> const peer_before = t_.PeerSnapshot();
    ProcessSnapshot const self_before = SelfSnapshot();
    Clock::time_point const start = Clock::now();
    std::int64_t bytes = 0;
    std::string const err = f(&bytes);
    Clock::duration const wall = Clock::now() - start;
    ProcessSnapshot const self_after = SelfSnapshot();
    std::optional<ProcessSnapshot> const peer_after = t_.PeerSnapshot();
    Sample s{a_.workload, op, i, size, err.empty() ? bytes : 0};
    s.wall_ms = std::chrono::duration<double, std::milli>(wall).count();
    Attribute(s, self_before, self_after, peer_before, peer_after,
              t_.PeerSharesProcess(), /*peak_rss=*/false);
    s.ok = err.empty();
    s.error = err;
    if (i >= 0) rec.Add(std::move(s));  // warm-up iterations are not recorded
    return err.empty();
  }

  bool Zonal() const { return a_.bucket_type == "zonal"; }

  /// Regional buckets take regular uploads; zonal buckets only appendable.
  std::string UploadForBidi(std::string const& name, std::int64_t size) {
    return Zonal() ? t_.AppendableUpload(a_.bucket, name, data_.data(), size)
                   : t_.JsonUpload(a_.bucket, name, data_.data(), size,
                                   size >= 16 * kMiB);
  }

  void Iteration(int i, Recorder& rec) {
    std::string const& w = a_.workload;
    if (w == "w1r3") return W1r3Iteration(i, rec);

    std::int64_t const size = (w == "read50x2x4") ? 200 * kMiB : PickSize();
    std::string const name = ObjectName(w, i);
    auto const random_offset = static_cast<std::int64_t>(
        gen_.Below(static_cast<std::uint64_t>(
                       std::max<std::int64_t>(0, size - (8 * kKiB))) +
                   1));
    char const* upload_op = Zonal() ? "APPENDABLE" : "UPLOAD";

    if (!Measure(rec, i, size, upload_op, [&](std::int64_t* b) {
          *b = size;
          return UploadForBidi(name, size);
        })) {
      return;
    }
    if (w == "open") {
      Measure(rec, i, size, "open",
              [&](std::int64_t*) { return t_.Open(a_.bucket, name); });
      t_.CloseDescriptor();
    } else if (w == "random8k") {
      std::int64_t const len = std::min<std::int64_t>(8 * kKiB, size);
      Measure(rec, i, size, "random8k", [&](std::int64_t* b) {
        if (std::string e = t_.Open(a_.bucket, name); !e.empty()) return e;
        std::string e = t_.Read(random_offset, len, b);
        if (e.empty() && *b != len) e = "short read";
        return e;
      });
      t_.CloseDescriptor();
    } else if (w == "read50x2x4") {
      Ranges ranges;
      ranges.reserve(50);
      for (int j = 0; j < 50; ++j) {
        ranges.emplace_back(static_cast<std::int64_t>(j) * 4 * kMiB, 2 * kMiB);
      }
      Measure(rec, i, size, "read50x2x4", [&](std::int64_t* b) {
        if (std::string e = t_.Open(a_.bucket, name); !e.empty()) return e;
        std::string e = t_.ReadConcurrent(ranges, b);
        if (e.empty() && *b != 50 * 2 * kMiB)
          e = "short read " + std::to_string(*b);
        return e;
      });
      t_.CloseDescriptor();
    } else if (w == "appendable") {
      Measure(rec, i, size, "READ-BIDI", [&](std::int64_t* b) {
        std::string e = t_.BidiReadAll(a_.bucket, name, b);
        if (e.empty() && *b != size) e = "short read " + std::to_string(*b);
        return e;
      });
    }
    Measure(rec, i, size, "DELETE",
            [&](std::int64_t*) { return t_.Delete(a_.bucket, name); });
  }

  // w1r3: SSB sync worker: SINGLE-SHOT (InsertObject) or RESUMABLE
  // (WriteObject) picked per iteration; READ[0..2] full reads; DELETE.
  void W1r3Iteration(int i, Recorder& rec) {
    std::int64_t const size = PickSize();
    // Alternate by iteration so both upload kinds get the same sample count
    // in every implementation.
    bool const resumable = ((i % 2) + 2) % 2 == 1;
    char const* upload_op = resumable ? "RESUMABLE" : "SINGLE-SHOT";
    std::string const name = ObjectName("w1r3", i);
    if (!Measure(rec, i, size, upload_op, [&](std::int64_t* b) {
          *b = size;
          return t_.JsonUpload(a_.bucket, name, data_.data(), size, resumable);
        })) {
      return;
    }
    for (char const* op : {"READ[0]", "READ[1]", "READ[2]"}) {
      Measure(rec, i, size, op, [&](std::int64_t* b) {
        std::string e = t_.JsonRead(a_.bucket, name, b);
        if (e.empty() && *b != size) e = "short read " + std::to_string(*b);
        return e;
      });
    }
    Measure(rec, i, size, "DELETE",
            [&](std::int64_t*) { return t_.Delete(a_.bucket, name); });
  }

  // list: one-time setup of N small objects (not measured), then per
  // iteration a full listing with page size 1000 and with page size 100.
  void ListWorkload(Recorder& rec) {
    std::string const prefix = ObjectName("list", 0) + "/";
    std::cout << "  list setup: creating " << a_.list_objects << " objects\n";
    std::vector<std::thread> setup;
    setup.reserve(16);
    std::atomic<int> next{0};
    for (int w = 0; w < 16; ++w) {
      setup.emplace_back([&] {
        for (int j = next++; j < a_.list_objects; j = next++) {
          std::string const name = prefix + std::to_string(j);
          std::string const e =
              Zonal() ? t_.AppendableUpload(a_.bucket, name, data_.data(), 16)
                      : t_.JsonUpload(a_.bucket, name, data_.data(), 16, false);
          if (!e.empty()) std::cerr << "  setup " << name << ": " << e << "\n";
        }
      });
    }
    for (std::thread& t : setup) t.join();
    for (int i = -a_.warmup; i < a_.iterations; ++i) {
      for (int page : {1000, 100}) {
        Measure(rec, i, a_.list_objects, "LIST-P" + std::to_string(page),
                [&](std::int64_t* b) {
                  std::string e = t_.List(a_.bucket, prefix, page, b);
                  if (e.empty() && *b != a_.list_objects) {
                    e = "listed " + std::to_string(*b);
                  }
                  return e;
                });
      }
    }
    std::cout << "  list cleanup\n";
    std::vector<std::thread> cleanup;
    cleanup.reserve(16);
    next = 0;
    for (int w = 0; w < 16; ++w) {
      cleanup.emplace_back([&] {
        for (int j = next++; j < a_.list_objects; j = next++) {
          (void)t_.Delete(a_.bucket, prefix + std::to_string(j));
        }
      });
    }
    for (std::thread& t : cleanup) t.join();
  }

  // throughput: W concurrent workers, each doing `iterations` reads (or
  // writes) of the largest object size; one THROUGHPUT row with aggregate
  // bytes, wall time and both processes' CPU over the whole interval.
  void ThroughputWorkload(Recorder& rec) {
    std::int64_t const size =
        *std::max_element(a_.object_sizes.begin(), a_.object_sizes.end());
    bool const write = a_.throughput_op == "write";
    std::string const source = ObjectName("throughput-src", 0);
    if (!write) {
      std::string const e = UploadForBidi(source, size);
      if (!e.empty()) {
        std::cerr << "  throughput setup failed: " << e << "\n";
        return;
      }
    }
    auto run_once = [&](int worker, int i, std::int64_t* b) -> std::string {
      if (write) {
        std::string const name =
            ObjectName("throughput", (worker * 100000) + i);
        *b = size;
        return UploadForBidi(name, size);
      }
      return Zonal() || absl::StrContains(a_.label, "bidi")
                 ? t_.BidiReadAll(a_.bucket, source, b)
                 : t_.JsonRead(a_.bucket, source, b);
    };
    std::atomic<std::int64_t> total_bytes{0};
    std::atomic<int> errors{0};
    std::optional<ProcessSnapshot> const peer_before = t_.PeerSnapshot();
    ProcessSnapshot const self_before = SelfSnapshot();
    Clock::time_point const start = Clock::now();
    std::vector<std::thread> workers;
    workers.reserve(static_cast<std::size_t>(std::max(0, a_.workers)));
    for (int w = 0; w < a_.workers; ++w) {
      workers.emplace_back([&, w] {
        for (int i = 0; i < a_.iterations; ++i) {
          std::int64_t bytes = 0;
          std::string const e = run_once(w, i, &bytes);
          if (!e.empty()) {
            ++errors;
            std::cerr << "  throughput worker " << w << ": " << e << "\n";
          } else {
            total_bytes += bytes;
          }
        }
      });
    }
    for (std::thread& t : workers) t.join();
    Clock::duration const wall = Clock::now() - start;
    ProcessSnapshot const self_after = SelfSnapshot();
    std::optional<ProcessSnapshot> const peer_after = t_.PeerSnapshot();
    Sample s{a_.workload, write ? "THROUGHPUT-WRITE" : "THROUGHPUT-READ", 0,
             size, total_bytes.load()};
    s.wall_ms = std::chrono::duration<double, std::milli>(wall).count();
    Attribute(s, self_before, self_after, peer_before, peer_after,
              t_.PeerSharesProcess(), /*peak_rss=*/true);
    s.ok = errors.load() == 0;
    if (!s.ok) s.error = std::to_string(errors.load()) + " errors";
    rec.Add(std::move(s));
    std::cout << "  throughput: " << a_.workers << " workers, "
              << (static_cast<double>(total_bytes.load()) / 1048576.0) /
                     std::chrono::duration<double>(wall).count()
              << " MiB/s\n";
    // Clean up written objects after all THROUGHPUT-WRITE timing, CPU,
    // allocation, and peak-RSS snapshots have been recorded.
    if (write) {
      int const total_objects = a_.workers * a_.iterations;
      std::atomic<int> next{0};
      int const cleanup_threads = std::min(16, std::max(1, total_objects));
      std::vector<std::thread> cleanup;
      cleanup.reserve(static_cast<std::size_t>(cleanup_threads));
      for (int c = 0; c < cleanup_threads; ++c) {
        cleanup.emplace_back([&] {
          for (int idx = next++; idx < total_objects; idx = next++) {
            int const w = idx / a_.iterations;
            int const it = idx % a_.iterations;
            (void)t_.Delete(a_.bucket,
                            ObjectName("throughput", (w * 100000) + it));
          }
        });
      }
      for (std::thread& th : cleanup) th.join();
    } else {
      (void)t_.Delete(a_.bucket, source);
    }
  }

  Target& t_;
  Args a_;
  SplitMix64 gen_;
  std::string impl_;
  std::string run_id_;
  std::vector<char> data_;
};

}  // namespace ssb

#endif  // GCS_UDS_POC_BENCH_SSB_WORKLOADS_H
