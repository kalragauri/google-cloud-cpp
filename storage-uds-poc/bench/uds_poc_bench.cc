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

// Unified benchmark harness for the GCS SDK Consolidation UDS POC (plan rev 4).
//
// Drives all three comparators (`native`, `uds-sidecar`, `uds-inproc`) through
// the unmodified public `google::cloud::storage::Client` and
// `google::cloud::storage::AsyncClient` interfaces across the six prescribed
// Cloudpath workloads (W1-W6):
//   * W1 `open`: `AsyncClient::Open` on a pre-staged 200 MiB object (>=3 min)
//   * W2 `read50x2x4-serial` / `read50x2x4-concurrent`: 50 x 2 MiB ranges at a
//     4 MiB stride on a pre-staged 200 MiB object (>=3 min)
//   * W3 `random8k`: random 8 KiB ranges on an already-open 100 MiB object
//     (>=3 min)
//   * W4 `bidi-read-48t`: 48 threads reading a pre-staged 200 MiB object in
//     16 MiB `ObjectDescriptor::Read` ranges (5 min)
//   * W5 `bidi-write-48t`: 48 threads writing and finalizing 200 MB appendable
//     objects via `StartAppendableObjectUpload` (5 min)
//   * W6 `list-objects-100` / `list-buckets-100`: 100-item `ListObjects` and
//     `ListBucketsForProject` over gRPC (>=3 min)

#include "google/cloud/storage/async/client.h"
#include "google/cloud/storage/client.h"
#include "google/cloud/storage/grpc_plugin.h"
#include "google/cloud/storage/options.h"
#include "google/cloud/common_options.h"
#include "google/cloud/grpc_options.h"
#include "google/cloud/options.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/strings/cord.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "gcs_core/client.h"
#include "gcs_core/storage_client.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>
#include <sys/resource.h>
#include <unistd.h>

namespace {

namespace gc = ::google::cloud;
namespace gcs = ::google::cloud::storage;
using SteadyClock = std::chrono::steady_clock;
using SystemClock = std::chrono::system_clock;
using ReadStep = std::pair<gcs::ReadPayload, gcs::AsyncToken>;

constexpr std::int64_t kKiB = 1024;
constexpr std::int64_t kMiB = 1024 * kKiB;
constexpr std::int64_t kMB = 1000 * 1000;
constexpr std::string_view kCloudpathEndpoint = "storage.googleapis.com";
constexpr std::size_t kRestUploadBuffer = 8 * 1024 * 1024;

enum class ComparatorMode { kNative, kUdsSidecar, kUdsInproc };

enum class WorkloadKind {
  kOpen,
  kRead50x2x4Serial,
  kRead50x2x4Concurrent,
  kRandom8k,
  kBidiRead48t,
  kBidiWrite48t,
  kListObjects100,
  kListBuckets100,
};

struct Config {
  ComparatorMode mode = ComparatorMode::kNative;
  std::string mode_name;
  WorkloadKind workload = WorkloadKind::kOpen;
  std::string workload_name;
  std::string bucket;
  std::string bucket_type;
  std::string project;
  std::string bucket_prefix = "kalragauri-uds-list-";
  std::string prefix = "gcs-uds-poc/rev4/";
  double duration_secs = -1.0;
  double warmup_secs = 10.0;
  int max_ops = 0;
  int threads = 0;
  std::int64_t object_size = 0;
  std::int64_t range_size = 0;
  std::int64_t write_chunk_size = 16 * kMiB;
  int list_count = 100;
  int uds_channels = 4;
  int grpc_subchannels = 4;
  int cq_threads = 0;
  std::uint32_t chunk_size_hint = 0;
  bool child_mode = false;
  std::string socket_path;
  std::vector<std::string> sidecar_args;
  std::string out_csv;
  std::string summary_json;
};

/// Deterministic SplitMix64 PRNG so all comparators read identical offsets in
/// `random8k` regardless of standard-library implementation (plan §7.1 R4).
class SplitMix64 {
 public:
  explicit SplitMix64(std::uint64_t seed) : state_(seed) {}

  std::uint64_t Next() {
    std::uint64_t z = (state_ += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
  }

  std::uint64_t Below(std::uint64_t bound) {
    return bound == 0 ? 0 : Next() % bound;
  }

 private:
  std::uint64_t state_;
};

std::string FormatUtcIso8601(SystemClock::time_point tp) {
  auto const ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      tp.time_since_epoch()) %
                  1000;
  std::time_t const secs = SystemClock::to_time_t(tp);
  std::tm utc_tm{};
  gmtime_r(&secs, &utc_tm);
  char buf[96];
  std::snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d.%03lldZ",
                utc_tm.tm_year + 1900, utc_tm.tm_mon + 1, utc_tm.tm_mday,
                utc_tm.tm_hour, utc_tm.tm_min, utc_tm.tm_sec,
                static_cast<long long>(ms.count()));
  return std::string(buf);
}

std::string FormatStatus(gc::Status const& s) {
  if (s.ok()) return {};
  return absl::StrCat(gc::StatusCodeToString(s.code()), ": ", s.message());
}

std::string JsonEscape(std::string_view input) {
  std::string out;
  out.reserve(input.size() + 8);
  for (unsigned char const c : input) {
    switch (c) {
      case '"':
        out += "\\\"";
        break;
      case '\\':
        out += "\\\\";
        break;
      case '\b':
        out += "\\b";
        break;
      case '\f':
        out += "\\f";
        break;
      case '\n':
        out += "\\n";
        break;
      case '\r':
        out += "\\r";
        break;
      case '\t':
        out += "\\t";
        break;
      default:
        if (c < 0x20) {
          char hex[7];
          std::snprintf(hex, sizeof(hex), "\\u%04x", c);
          out += hex;
        } else {
          out.push_back(static_cast<char>(c));
        }
    }
  }
  return out;
}

std::string CsvEscape(std::string_view input) {
  if (input.find_first_of(",\"\n\r") == std::string_view::npos) {
    return std::string(input);
  }
  std::string out = "\"";
  for (char const c : input) {
    if (c == '"') out.push_back('"');
    out.push_back(c);
  }
  out.push_back('"');
  return out;
}

struct ProcMetrics {
  double user_secs = 0.0;
  double sys_secs = 0.0;
  std::int64_t rss_kib = 0;
  std::int64_t peak_rss_kib = 0;
  int threads = 0;
};

void ResetSelfPeakRss() {
  // Linux `clear_refs` value 5 resets VmHWM to current VmRSS so peak memory
  // reflects the timed window rather than one-time dataset staging.
  std::ofstream clear_refs("/proc/self/clear_refs");
  clear_refs << "5";
}

ProcMetrics ReadSelfMetrics() {
  ProcMetrics m;
  rusage ru{};
  if (::getrusage(RUSAGE_SELF, &ru) == 0) {
    m.user_secs = static_cast<double>(ru.ru_utime.tv_sec) +
                  (static_cast<double>(ru.ru_utime.tv_usec) * 1e-6);
    m.sys_secs = static_cast<double>(ru.ru_stime.tv_sec) +
                 (static_cast<double>(ru.ru_stime.tv_usec) * 1e-6);
  }
  std::ifstream status("/proc/self/status");
  std::string line;
  while (std::getline(status, line)) {
    std::istringstream iss(line);
    std::string key;
    std::int64_t val = 0;
    if (!(iss >> key >> val)) continue;
    if (key == "VmRSS:") m.rss_kib = val;
    if (key == "VmHWM:") m.peak_rss_kib = val;
    if (key == "Threads:") m.threads = static_cast<int>(val);
  }
  return m;
}

struct LatencySummary {
  std::size_t count = 0;
  double min_us = 0.0;
  double p50_us = 0.0;
  double p90_us = 0.0;
  double p99_us = 0.0;
  double max_us = 0.0;
  double mean_us = 0.0;
};

LatencySummary SummarizeLatencies(std::vector<double> values) {
  LatencySummary s;
  if (values.empty()) return s;
  std::sort(values.begin(), values.end());
  s.count = values.size();
  s.min_us = values.front();
  s.max_us = values.back();
  double const sum = std::accumulate(values.begin(), values.end(), 0.0);
  s.mean_us = sum / static_cast<double>(values.size());
  auto pick = [&](double q) {
    double const rank = q * static_cast<double>(values.size() - 1);
    auto const idx = static_cast<std::size_t>(std::llround(rank));
    return values[std::min(idx, values.size() - 1)];
  };
  s.p50_us = pick(0.50);
  s.p90_us = pick(0.90);
  s.p99_us = pick(0.99);
  return s;
}

struct SampleRow {
  std::string ts_utc;
  int thread_id = 0;
  std::int64_t seq = 0;
  std::string op;
  std::int64_t bytes = 0;
  double latency_us = 0.0;
  bool ok = true;
  std::string error;
};

class Recorder {
 public:
  void RecordSample(SampleRow row) {
    std::scoped_lock lk(mu_);
    if (row.ok) {
      ++ops_ok_;
      total_bytes_ += row.bytes;
      latencies_us_.push_back(row.latency_us);
    } else {
      ++ops_err_;
      if (first_error_.empty()) first_error_ = row.error;
    }
    rows_.push_back(std::move(row));
  }

  void RecordRangeLatency(double latency_us) {
    std::scoped_lock lk(mu_);
    range_latencies_us_.push_back(latency_us);
  }

  std::int64_t total_ops() const {
    std::scoped_lock lk(mu_);
    return ops_ok_ + ops_err_;
  }

  std::int64_t ops_ok() const {
    std::scoped_lock lk(mu_);
    return ops_ok_;
  }

  std::int64_t ops_err() const {
    std::scoped_lock lk(mu_);
    return ops_err_;
  }

  std::int64_t total_bytes() const {
    std::scoped_lock lk(mu_);
    return total_bytes_;
  }

  std::string first_error() const {
    std::scoped_lock lk(mu_);
    return first_error_;
  }

  LatencySummary ObjectSummary() const {
    std::scoped_lock lk(mu_);
    return SummarizeLatencies(latencies_us_);
  }

  LatencySummary RangeSummary() const {
    std::scoped_lock lk(mu_);
    return SummarizeLatencies(range_latencies_us_);
  }

  bool WriteCsv(std::string const& path, Config const& cfg) const {
    if (path.empty()) return true;
    std::ofstream out(path);
    if (!out.is_open()) return false;
    out << "ts_utc,mode,workload,bucket,thread_id,seq,op,bytes,latency_us,ok,"
           "error\n";
    out << std::fixed << std::setprecision(2);
    std::scoped_lock lk(mu_);
    for (SampleRow const& r : rows_) {
      out << r.ts_utc << "," << cfg.mode_name << "," << cfg.workload_name << ","
          << CsvEscape(cfg.bucket) << "," << r.thread_id << "," << r.seq << ","
          << r.op << "," << r.bytes << "," << r.latency_us << ","
          << (r.ok ? 1 : 0) << "," << CsvEscape(r.error) << "\n";
    }
    return out.good();
  }

 private:
  mutable std::mutex mu_;
  std::int64_t ops_ok_ = 0;
  std::int64_t ops_err_ = 0;
  std::int64_t total_bytes_ = 0;
  std::string first_error_;
  std::vector<double> latencies_us_;
  std::vector<double> range_latencies_us_;
  std::vector<SampleRow> rows_;
};

/// Holds the public SDK clients for the selected comparator plus an optional
/// control handle to the UDS server for `Stats(reset_peak_rss)` snapshots.
struct Clients {
  std::optional<gcs::Client> sync_client;
  std::optional<gcs::Client> native_rest_setup_client;
  std::optional<gcs::AsyncClient> async_client;
  std::optional<gcs_core::Client> core_control;
};

bool HasFlagPrefix(std::vector<std::string> const& args,
                   std::string_view prefix) {
  std::string const prefix_eq = absl::StrCat(prefix, "=");
  return std::any_of(args.begin(), args.end(), [&](std::string const& arg) {
    return arg == prefix || absl::StartsWith(arg, prefix_eq);
  });
}

gc::StatusOr<Clients> BuildClients(Config const& cfg) {
  Clients out;
  auto const cq_pool = static_cast<std::size_t>(cfg.cq_threads);
  if (cfg.mode == ComparatorMode::kNative) {
    // Explicit Cloudpath endpoint prevents `DefaultOptionsGrpc` from selecting
    // DirectPath (`google-c2p:///`) on GCE, matching the Rust core (R1).
    gc::Options grpc_opts =
        gc::Options{}
            .set<gc::EndpointOption>(std::string(kCloudpathEndpoint))
            .set<gc::GrpcNumChannelsOption>(cfg.grpc_subchannels)
            .set<gc::GrpcBackgroundThreadPoolSizeOption>(cq_pool);
    out.sync_client.emplace(gcs::MakeGrpcClient(grpc_opts));
    out.async_client.emplace(gcs::AsyncClient(grpc_opts));
    out.native_rest_setup_client.emplace(gcs::Client(
        gc::Options{}.set<gcs::UploadBufferSizeOption>(kRestUploadBuffer)));
    return out;
  }

  gcs_core::ClientOptions core_opts = gcs_core::DefaultAdapterClientOptions();
  core_opts.hosting = (cfg.mode == ComparatorMode::kUdsSidecar)
                          ? gcs_core::HostingMode::kSidecar
                          : gcs_core::HostingMode::kInProcess;
  if (cfg.mode == ComparatorMode::kUdsSidecar && cfg.child_mode) {
    core_opts.sidecar_mode = gcs_core::SidecarMode::kChild;
  }
  core_opts.channels = cfg.uds_channels;
  core_opts.chunk_size_hint = cfg.chunk_size_hint;
  core_opts.socket_path = cfg.socket_path;
  core_opts.sidecar_args = cfg.sidecar_args;
  if (!HasFlagPrefix(core_opts.sidecar_args, "--grpc-subchannels")) {
    core_opts.sidecar_args.push_back(
        absl::StrCat("--grpc-subchannels=", cfg.grpc_subchannels));
  }

  gc::StatusOr<gcs_core::Client> ctrl = gcs_core::Client::Create(core_opts);
  if (!ctrl) return std::move(ctrl).status();
  out.core_control.emplace(*std::move(ctrl));

  gc::StatusOr<gcs::Client> sync_c = gcs_core::MakeUdsStorageClient(
      core_opts,
      gc::Options{}.set<gcs::UploadBufferSizeOption>(kRestUploadBuffer));
  if (!sync_c) return std::move(sync_c).status();
  out.sync_client.emplace(*std::move(sync_c));

  gc::StatusOr<gcs::AsyncClient> async_c = gcs_core::MakeUdsAsyncClient(
      core_opts,
      gc::Options{}.set<gc::GrpcBackgroundThreadPoolSizeOption>(cq_pool));
  if (!async_c) return std::move(async_c).status();
  out.async_client.emplace(*std::move(async_c));
  return out;
}

std::shared_ptr<std::vector<char>> MakeDeterministicPayload(std::int64_t size) {
  auto buf =
      std::make_shared<std::vector<char>>(static_cast<std::size_t>(size));
  SplitMix64 prng(0x5eedcafeULL);
  std::size_t offset = 0;
  while (offset + sizeof(std::uint64_t) <= buf->size()) {
    std::uint64_t const word = prng.Next();
    std::copy_n(reinterpret_cast<char const*>(&word), sizeof(word),
                buf->data() + offset);
    offset += sizeof(word);
  }
  if (offset < buf->size()) {
    std::uint64_t const tail = prng.Next();
    std::copy_n(reinterpret_cast<char const*>(&tail), buf->size() - offset,
                buf->data() + offset);
  }
  return buf;
}

std::vector<absl::Cord> SliceIntoCords(
    std::shared_ptr<std::vector<char>> const& buf, std::int64_t slice_bytes) {
  auto const total = static_cast<std::int64_t>(buf->size());
  std::int64_t const step = slice_bytes > 0 ? slice_bytes : total;
  std::vector<absl::Cord> cords;
  if (total == 0) {
    cords.emplace_back();
    return cords;
  }
  for (std::int64_t off = 0; off < total; off += step) {
    std::int64_t const len = std::min<std::int64_t>(step, total - off);
    std::shared_ptr<std::vector<char>> keepalive = buf;
    cords.push_back(absl::MakeCordFromExternal(
        absl::string_view(buf->data() + off, static_cast<std::size_t>(len)),
        [keepalive = std::move(keepalive)] {}));
  }
  return cords;
}

gc::Status DrainAsyncReader(gcs::AsyncReader reader, gcs::AsyncToken token,
                            std::int64_t* bytes_out) {
  std::int64_t total = 0;
  while (token.valid()) {
    gc::StatusOr<ReadStep> step = reader.Read(std::move(token)).get();
    if (!step) return std::move(step).status();
    total += static_cast<std::int64_t>(step->first.size());
    token = std::move(step->second);
  }
  *bytes_out = total;
  return {};
}

void DrainAsyncReaderCallback(
    gcs::AsyncReader reader, gcs::AsyncToken token,
    std::shared_ptr<std::atomic<std::int64_t>> bytes_accum,
    std::function<void(gc::Status)> on_done) {
  gc::future<gc::StatusOr<ReadStep>> pending = reader.Read(std::move(token));
  std::move(pending).then(
      [reader = std::move(reader), bytes_accum = std::move(bytes_accum),
       on_done =
           std::move(on_done)](gc::future<gc::StatusOr<ReadStep>> f) mutable {
        gc::StatusOr<ReadStep> step = f.get();
        if (!step) {
          on_done(std::move(step).status());
          return;
        }
        *bytes_accum += static_cast<std::int64_t>(step->first.size());
        if (step->second.valid()) {
          DrainAsyncReaderCallback(std::move(reader), std::move(step->second),
                                   std::move(bytes_accum), std::move(on_done));
        } else {
          on_done(gc::Status{});
        }
      });
}

gc::Status UploadAppendable(gcs::AsyncClient& async_client,
                            std::string const& bucket,
                            std::string const& object,
                            std::vector<absl::Cord> const& chunks,
                            std::int64_t expected_size) {
  gc::StatusOr<std::pair<gcs::AsyncWriter, gcs::AsyncToken>> started =
      async_client.StartAppendableObjectUpload(gcs::BucketName(bucket), object)
          .get();
  if (!started) return std::move(started).status();
  auto [writer, token] = *std::move(started);
  for (absl::Cord const& chunk : chunks) {
    gc::StatusOr<gcs::AsyncToken> next =
        writer.Write(std::move(token), gcs::WritePayload(chunk)).get();
    if (!next) return std::move(next).status();
    token = *std::move(next);
  }
  gc::StatusOr<google::storage::v2::Object> meta =
      writer.Finalize(std::move(token)).get();
  if (!meta) return std::move(meta).status();
  if (meta->size() != expected_size) {
    return gc::Status(gc::StatusCode::kDataLoss,
                      absl::StrCat("appendable upload size mismatch: got ",
                                   meta->size(), " want ", expected_size));
  }
  return {};
}

/// Ensures `object_name` exists in `bucket` with `size` bytes before warmup,
/// reusing an existing staged object so consecutive comparators do not
/// redundantly upload 200 MiB over Cloudpath.
gc::Status EnsureStagedObject(Clients& clients, Config const& cfg,
                              std::string const& object_name,
                              std::int64_t size) {
  for (gc::StatusOr<gcs::ObjectMetadata>& item :
       clients.sync_client->ListObjects(cfg.bucket, gcs::Prefix(object_name),
                                        gcs::MaxResults(10))) {
    if (!item) return std::move(item).status();
    if (item->name() == object_name &&
        static_cast<std::int64_t>(item->size()) == size) {
      return {};
    }
  }
  std::shared_ptr<std::vector<char>> payload = MakeDeterministicPayload(size);
  if (cfg.bucket_type == "zonal") {
    std::vector<absl::Cord> chunks = SliceIntoCords(payload, 16 * kMiB);
    return UploadAppendable(*clients.async_client, cfg.bucket, object_name,
                            chunks, size);
  }
  gcs::Client& uploader = clients.native_rest_setup_client.has_value()
                              ? *clients.native_rest_setup_client
                              : *clients.sync_client;
  gcs::ObjectWriteStream os = uploader.WriteObject(cfg.bucket, object_name);
  os.write(payload->data(), static_cast<std::streamsize>(size));
  os.Close();
  return os.metadata().status();
}

gc::Status EnsureListDataset(Clients& clients, Config const& cfg,
                             std::string const& list_prefix) {
  int existing = 0;
  for (gc::StatusOr<gcs::ObjectMetadata>& item :
       clients.sync_client->ListObjects(cfg.bucket, gcs::Prefix(list_prefix),
                                        gcs::MaxResults(cfg.list_count + 10))) {
    if (!item) return std::move(item).status();
    ++existing;
  }
  if (existing == cfg.list_count) return {};

  std::vector<absl::Cord> tiny = {absl::Cord("x")};
  for (int i = 0; i < cfg.list_count; ++i) {
    char suffix[16];
    std::snprintf(suffix, sizeof(suffix), "obj-%04d", i);
    std::string const name = absl::StrCat(list_prefix, suffix);
    if (cfg.bucket_type == "zonal") {
      gc::Status s =
          UploadAppendable(*clients.async_client, cfg.bucket, name, tiny, 1);
      if (!s.ok()) return s;
    } else {
      gcs::Client& uploader = clients.native_rest_setup_client.has_value()
                                  ? *clients.native_rest_setup_client
                                  : *clients.sync_client;
      gc::StatusOr<gcs::ObjectMetadata> meta =
          uploader.InsertObject(cfg.bucket, name, "x");
      if (!meta) return std::move(meta).status();
    }
  }
  return {};
}

void DeleteObjectsConcurrently(gcs::AsyncClient& async_client,
                               std::string const& bucket,
                               std::vector<std::string> const& objects,
                               int worker_count) {
  if (objects.empty()) return;
  std::atomic<std::size_t> next_idx{0};
  int const nthreads = std::max(
      1, std::min<int>(worker_count, static_cast<int>(objects.size())));
  std::vector<std::thread> pool;
  pool.reserve(static_cast<std::size_t>(nthreads));
  for (int t = 0; t < nthreads; ++t) {
    pool.emplace_back([&] {
      while (true) {
        std::size_t const idx =
            next_idx.fetch_add(1, std::memory_order_relaxed);
        if (idx >= objects.size()) break;
        (void)async_client.DeleteObject(gcs::BucketName(bucket), objects[idx])
            .get();
      }
    });
  }
  for (std::thread& t : pool) t.join();
}

void WriteSummaryJson(std::ostream& out, Config const& cfg,
                      std::string const& window_start_utc,
                      std::string const& window_end_utc,
                      double window_wall_secs, Recorder const& rec,
                      ProcMetrics const& app_start, ProcMetrics const& app_end,
                      std::optional<gcs_core::SidecarStats> const& srv_start,
                      std::optional<gcs_core::SidecarStats> const& srv_end,
                      std::optional<std::uint64_t> core_inproc_cpu_start_us,
                      std::optional<std::uint64_t> core_inproc_cpu_end_us,
                      int server_pid) {
  LatencySummary const obj_lat = rec.ObjectSummary();
  LatencySummary const rng_lat = rec.RangeSummary();
  std::int64_t const bytes = rec.total_bytes();
  double const mb_s =
      window_wall_secs > 0.0
          ? (static_cast<double>(bytes) / 1e6) / window_wall_secs
          : 0.0;
  double const mib_s =
      window_wall_secs > 0.0
          ? (static_cast<double>(bytes) / static_cast<double>(kMiB)) /
                window_wall_secs
          : 0.0;

  double const proc_user_secs =
      std::max(0.0, app_end.user_secs - app_start.user_secs);
  double const proc_sys_secs =
      std::max(0.0, app_end.sys_secs - app_start.sys_secs);
  double const proc_total_secs = proc_user_secs + proc_sys_secs;
  double const proc_ns_per_byte =
      bytes > 0 ? (proc_total_secs * 1e9) / static_cast<double>(bytes) : 0.0;

  out << std::fixed << std::setprecision(4);
  out << "{\n"
      << R"(  "mode": ")" << JsonEscape(cfg.mode_name) << "\",\n"
      << R"(  "workload": ")" << JsonEscape(cfg.workload_name) << "\",\n"
      << R"(  "bucket": ")" << JsonEscape(cfg.bucket) << "\",\n"
      << R"(  "bucket_type": ")" << JsonEscape(cfg.bucket_type) << "\",\n"
      << R"(  "project": ")" << JsonEscape(cfg.project) << "\",\n"
      << "  \"threads\": " << cfg.threads << ",\n"
      << "  \"uds_channels\": " << cfg.uds_channels << ",\n"
      << "  \"grpc_subchannels\": " << cfg.grpc_subchannels << ",\n"
      << "  \"cq_threads\": " << cfg.cq_threads << ",\n"
      << "  \"object_size\": " << cfg.object_size << ",\n"
      << "  \"range_size\": " << cfg.range_size << ",\n"
      << R"(  "window_start_utc": ")" << window_start_utc << "\",\n"
      << R"(  "window_end_utc": ")" << window_end_utc << "\",\n"
      << "  \"window_wall_secs\": " << window_wall_secs << ",\n"
      << "  \"ops_ok\": " << rec.ops_ok() << ",\n"
      << "  \"ops_err\": " << rec.ops_err() << ",\n"
      << R"(  "first_error": ")" << JsonEscape(rec.first_error()) << "\",\n"
      << "  \"total_bytes\": " << bytes << ",\n"
      << "  \"throughput_mb_s\": " << mb_s << ",\n"
      << "  \"throughput_mib_s\": " << mib_s << ",\n"
      << "  \"latency_us\": {\n"
      << "    \"count\": " << obj_lat.count << ",\n"
      << "    \"min\": " << obj_lat.min_us << ",\n"
      << "    \"p50\": " << obj_lat.p50_us << ",\n"
      << "    \"p90\": " << obj_lat.p90_us << ",\n"
      << "    \"p99\": " << obj_lat.p99_us << ",\n"
      << "    \"max\": " << obj_lat.max_us << ",\n"
      << "    \"mean\": " << obj_lat.mean_us << "\n"
      << "  },\n"
      << "  \"range_latency_us\": {\n"
      << "    \"count\": " << rng_lat.count << ",\n"
      << "    \"min\": " << rng_lat.min_us << ",\n"
      << "    \"p50\": " << rng_lat.p50_us << ",\n"
      << "    \"p90\": " << rng_lat.p90_us << ",\n"
      << "    \"p99\": " << rng_lat.p99_us << ",\n"
      << "    \"max\": " << rng_lat.max_us << ",\n"
      << "    \"mean\": " << rng_lat.mean_us << "\n"
      << "  },\n"
      << "  \"app_process\": {\n"
      << "    \"pid\": " << ::getpid() << ",\n"
      << "    \"cpu_user_secs\": " << proc_user_secs << ",\n"
      << "    \"cpu_sys_secs\": " << proc_sys_secs << ",\n"
      << "    \"cpu_total_secs\": " << proc_total_secs << ",\n"
      << "    \"cpu_ns_per_byte\": " << proc_ns_per_byte << ",\n"
      << "    \"rss_kib\": " << app_end.rss_kib << ",\n"
      << "    \"peak_rss_kib\": " << app_end.peak_rss_kib << ",\n"
      << "    \"threads\": " << app_end.threads << "\n"
      << "  }";

  if (srv_start.has_value() && srv_end.has_value()) {
    bool const shares_proc = (server_pid == ::getpid());
    double srv_cpu_secs = 0.0;
    if (shares_proc && core_inproc_cpu_start_us.has_value() &&
        core_inproc_cpu_end_us.has_value() &&
        *core_inproc_cpu_end_us >= *core_inproc_cpu_start_us) {
      srv_cpu_secs = static_cast<double>(*core_inproc_cpu_end_us -
                                         *core_inproc_cpu_start_us) *
                     1e-6;
    } else {
      std::uint64_t const start_us =
          srv_start->user_cpu_us + srv_start->system_cpu_us;
      std::uint64_t const end_us =
          srv_end->user_cpu_us + srv_end->system_cpu_us;
      srv_cpu_secs = end_us >= start_us
                         ? static_cast<double>(end_us - start_us) * 1e-6
                         : 0.0;
    }
    double const srv_ns_per_byte =
        bytes > 0 ? (srv_cpu_secs * 1e9) / static_cast<double>(bytes) : 0.0;
    out << ",\n  \"server_process\": {\n"
        << "    \"pid\": " << server_pid << ",\n"
        << "    \"shares_app_process\": " << (shares_proc ? "true" : "false")
        << ",\n"
        << "    \"cpu_total_secs\": " << srv_cpu_secs << ",\n"
        << "    \"cpu_ns_per_byte\": " << srv_ns_per_byte << ",\n"
        << "    \"rss_bytes\": " << srv_end->rss_bytes << ",\n"
        << "    \"peak_rss_bytes\": " << srv_end->peak_rss_bytes << ",\n"
        << "    \"threads\": " << srv_end->threads << "\n"
        << "  }\n";
  } else {
    out << ",\n  \"server_process\": null\n";
  }
  out << "}\n";
}

bool ParseFlags(int argc, char* argv[], Config& cfg) {
  for (int i = 1; i < argc; ++i) {
    std::string const arg = argv[i];
    std::size_t const eq = arg.find('=');
    std::string const key = arg.substr(0, eq);
    std::string const val =
        eq == std::string::npos ? std::string() : arg.substr(eq + 1);
    if (key == "--mode") {
      cfg.mode_name = val;
    } else if (key == "--workload") {
      cfg.workload_name = val;
    } else if (key == "--bucket") {
      cfg.bucket = val;
    } else if (key == "--bucket-type") {
      cfg.bucket_type = val;
    } else if (key == "--project") {
      cfg.project = val;
    } else if (key == "--bucket-prefix") {
      cfg.bucket_prefix = val;
    } else if (key == "--prefix") {
      cfg.prefix = val;
    } else if (key == "--duration-secs") {
      cfg.duration_secs = std::stod(val);
    } else if (key == "--warmup-secs") {
      cfg.warmup_secs = std::stod(val);
    } else if (key == "--max-ops") {
      cfg.max_ops = std::stoi(val);
    } else if (key == "--threads") {
      cfg.threads = std::stoi(val);
    } else if (key == "--object-size") {
      cfg.object_size = std::stoll(val);
    } else if (key == "--range-size") {
      cfg.range_size = std::stoll(val);
    } else if (key == "--write-chunk-size") {
      cfg.write_chunk_size = std::stoll(val);
    } else if (key == "--list-count") {
      cfg.list_count = std::stoi(val);
    } else if (key == "--uds-channels") {
      cfg.uds_channels = std::stoi(val);
    } else if (key == "--grpc-subchannels") {
      cfg.grpc_subchannels = std::stoi(val);
    } else if (key == "--cq-threads") {
      cfg.cq_threads = std::stoi(val);
    } else if (key == "--chunk-size-hint") {
      cfg.chunk_size_hint = static_cast<std::uint32_t>(std::stoul(val));
    } else if (arg == "--child-mode") {
      cfg.child_mode = true;
    } else if (key == "--socket-path") {
      cfg.socket_path = val;
    } else if (key == "--sidecar-arg") {
      cfg.sidecar_args.push_back(val);
    } else if (key == "--out") {
      cfg.out_csv = val;
    } else if (key == "--summary-json") {
      cfg.summary_json = val;
    } else {
      std::cerr << "unknown flag: " << arg << "\n";
      return false;
    }
  }

  if (cfg.mode_name == "native") {
    cfg.mode = ComparatorMode::kNative;
  } else if (cfg.mode_name == "uds-sidecar") {
    cfg.mode = ComparatorMode::kUdsSidecar;
  } else if (cfg.mode_name == "uds-inproc") {
    cfg.mode = ComparatorMode::kUdsInproc;
  } else {
    std::cerr << "--mode must be native, uds-sidecar, or uds-inproc\n";
    return false;
  }

  if (cfg.workload_name == "open") {
    cfg.workload = WorkloadKind::kOpen;
  } else if (cfg.workload_name == "read50x2x4-serial") {
    cfg.workload = WorkloadKind::kRead50x2x4Serial;
  } else if (cfg.workload_name == "read50x2x4-concurrent") {
    cfg.workload = WorkloadKind::kRead50x2x4Concurrent;
  } else if (cfg.workload_name == "random8k") {
    cfg.workload = WorkloadKind::kRandom8k;
  } else if (cfg.workload_name == "bidi-read-48t") {
    cfg.workload = WorkloadKind::kBidiRead48t;
  } else if (cfg.workload_name == "bidi-write-48t") {
    cfg.workload = WorkloadKind::kBidiWrite48t;
  } else if (cfg.workload_name == "list-objects-100") {
    cfg.workload = WorkloadKind::kListObjects100;
  } else if (cfg.workload_name == "list-buckets-100") {
    cfg.workload = WorkloadKind::kListBuckets100;
  } else {
    std::cerr << "--workload must be open, read50x2x4-serial, "
                 "read50x2x4-concurrent, random8k, bidi-read-48t, "
                 "bidi-write-48t, list-objects-100, or list-buckets-100\n";
    return false;
  }

  bool const is_48t = (cfg.workload == WorkloadKind::kBidiRead48t ||
                       cfg.workload == WorkloadKind::kBidiWrite48t);
  if (cfg.duration_secs < 0.0) {
    cfg.duration_secs = is_48t ? 300.0 : 180.0;
  }
  if (cfg.threads <= 0) {
    cfg.threads = is_48t ? 48 : 1;
  }
  if (cfg.cq_threads <= 0) {
    cfg.cq_threads = std::max(4, cfg.threads);
  }
  if (cfg.bucket_type.empty()) {
    cfg.bucket_type =
        (cfg.workload == WorkloadKind::kListObjects100) ? "regional" : "zonal";
  }
  if (cfg.bucket_type != "regional" && cfg.bucket_type != "zonal") {
    std::cerr << "--bucket-type must be regional or zonal\n";
    return false;
  }
  if (cfg.object_size <= 0) {
    switch (cfg.workload) {
      case WorkloadKind::kOpen:
      case WorkloadKind::kRead50x2x4Serial:
      case WorkloadKind::kRead50x2x4Concurrent:
      case WorkloadKind::kBidiRead48t:
        cfg.object_size = 200 * kMiB;
        break;
      case WorkloadKind::kRandom8k:
        cfg.object_size = 100 * kMiB;
        break;
      case WorkloadKind::kBidiWrite48t:
        cfg.object_size = 200 * kMB;
        break;
      case WorkloadKind::kListObjects100:
      case WorkloadKind::kListBuckets100:
        cfg.object_size = 0;
        break;
    }
  }
  if (cfg.range_size <= 0) {
    switch (cfg.workload) {
      case WorkloadKind::kRead50x2x4Serial:
      case WorkloadKind::kRead50x2x4Concurrent:
        cfg.range_size = 2 * kMiB;
        break;
      case WorkloadKind::kRandom8k:
        cfg.range_size = 8 * kKiB;
        break;
      case WorkloadKind::kBidiRead48t:
        cfg.range_size = 16 * kMiB;
        break;
      case WorkloadKind::kOpen:
      case WorkloadKind::kBidiWrite48t:
      case WorkloadKind::kListObjects100:
      case WorkloadKind::kListBuckets100:
        cfg.range_size = 0;
        break;
    }
  }
  if (cfg.workload == WorkloadKind::kListBuckets100) {
    if (cfg.project.empty()) {
      std::cerr << "--project is required for list-buckets-100\n";
      return false;
    }
  } else if (cfg.bucket.empty()) {
    std::cerr << "--bucket is required\n";
    return false;
  }
  if (cfg.uds_channels <= 0 || cfg.grpc_subchannels <= 0 ||
      cfg.write_chunk_size <= 0 || cfg.list_count <= 0) {
    std::cerr << "--uds-channels, --grpc-subchannels, --write-chunk-size, and "
                 "--list-count must be positive\n";
    return false;
  }
  if ((cfg.workload == WorkloadKind::kRead50x2x4Serial ||
       cfg.workload == WorkloadKind::kRead50x2x4Concurrent) &&
      (cfg.range_size <= 0 ||
       cfg.object_size < (49 * 4 * kMiB) + cfg.range_size)) {
    std::cerr << "read50x2x4 workloads require --range-size > 0 and "
                 "--object-size >= 196 MiB + range_size\n";
    return false;
  }
  if ((cfg.workload == WorkloadKind::kRandom8k ||
       cfg.workload == WorkloadKind::kBidiRead48t) &&
      (cfg.range_size <= 0 || cfg.object_size < cfg.range_size)) {
    std::cerr << "range read workloads require 0 < --range-size <= "
                 "--object-size\n";
    return false;
  }
  return true;
}

}  // namespace

int main(int argc, char* argv[]) {
  Config cfg;
  if (!ParseFlags(argc, argv, cfg)) return 2;

  gc::StatusOr<Clients> built = BuildClients(cfg);
  if (!built) {
    std::cerr << "BuildClients failed: " << built.status() << "\n";
    return 1;
  }
  Clients clients = *std::move(built);

  // --- Dataset setup (outside warmup and timed window, plan §5) --------------
  std::string const staged_object =
      absl::StrCat(cfg.prefix, "staged-", cfg.object_size, "B.bin");
  std::string const list_prefix = absl::StrCat(cfg.prefix, "list100/");
  std::shared_ptr<std::vector<char>> write_buf;
  std::vector<absl::Cord> write_chunks;

  switch (cfg.workload) {
    case WorkloadKind::kOpen:
    case WorkloadKind::kRead50x2x4Serial:
    case WorkloadKind::kRead50x2x4Concurrent:
    case WorkloadKind::kRandom8k:
    case WorkloadKind::kBidiRead48t: {
      gc::Status s =
          EnsureStagedObject(clients, cfg, staged_object, cfg.object_size);
      if (!s.ok()) {
        std::cerr << "EnsureStagedObject failed: " << s << "\n";
        return 1;
      }
      break;
    }
    case WorkloadKind::kBidiWrite48t:
      write_buf = MakeDeterministicPayload(cfg.object_size);
      write_chunks = SliceIntoCords(write_buf, cfg.write_chunk_size);
      break;
    case WorkloadKind::kListObjects100: {
      gc::Status s = EnsureListDataset(clients, cfg, list_prefix);
      if (!s.ok()) {
        std::cerr << "EnsureListDataset failed: " << s << "\n";
        return 1;
      }
      break;
    }
    case WorkloadKind::kListBuckets100:
      break;
  }

  std::string const run_nonce =
      absl::StrCat(::getpid(), "-",
                   std::chrono::duration_cast<std::chrono::milliseconds>(
                       SystemClock::now().time_since_epoch())
                       .count());
  std::mutex created_mu;
  std::vector<std::string> created_objects;

  // W3 keeps a single open ObjectDescriptor across the run ("random 8 KiB
  // ranges from an open 100 MiB object", plan §5).
  std::optional<gcs::ObjectDescriptor> w3_descriptor;
  gc::Status w3_open_error;
  if (cfg.workload == WorkloadKind::kRandom8k) {
    gc::StatusOr<gcs::ObjectDescriptor> opened =
        clients.async_client->Open(gcs::BucketName(cfg.bucket), staged_object)
            .get();
    if (!opened) {
      w3_open_error = std::move(opened).status();
    } else {
      w3_descriptor.emplace(*std::move(opened));
    }
  }

  auto run_phase = [&](double phase_secs, bool record, Recorder* rec) {
    if (phase_secs <= 0.0) return;
    SteadyClock::time_point const deadline =
        SteadyClock::now() + std::chrono::duration_cast<SteadyClock::duration>(
                                 std::chrono::duration<double>(phase_secs));
    std::atomic<std::int64_t> seq_gen{0};

    auto worker_fn = [&](int tid) {
      SplitMix64 rng(42ULL + (static_cast<std::uint64_t>(tid) * 0x9e3779b9ULL));
      while (SteadyClock::now() < deadline) {
        std::int64_t const seq =
            seq_gen.fetch_add(1, std::memory_order_relaxed);
        if (record && cfg.max_ops > 0 && seq >= cfg.max_ops) break;
        if (!record && cfg.max_ops > 0 && seq >= 1) break;

        SystemClock::time_point const ts_wall = SystemClock::now();
        SteadyClock::time_point const t0 = SteadyClock::now();
        std::int64_t op_bytes = 0;
        std::string op_name;
        gc::Status status;

        switch (cfg.workload) {
          case WorkloadKind::kOpen: {
            op_name = "OPEN";
            gc::StatusOr<gcs::ObjectDescriptor> d =
                clients.async_client
                    ->Open(gcs::BucketName(cfg.bucket), staged_object)
                    .get();
            if (!d) {
              status = std::move(d).status();
            } else if (!d->metadata().has_value() ||
                       d->metadata()->size() != cfg.object_size) {
              status = gc::Status(gc::StatusCode::kDataLoss,
                                  "unexpected ObjectDescriptor metadata size");
            }
            break;
          }
          case WorkloadKind::kRead50x2x4Serial: {
            op_name = "READ50X2X4_SERIAL";
            gc::StatusOr<gcs::ObjectDescriptor> d =
                clients.async_client
                    ->Open(gcs::BucketName(cfg.bucket), staged_object)
                    .get();
            if (!d) {
              status = std::move(d).status();
              break;
            }
            for (int r = 0; r < 50; ++r) {
              std::int64_t const off = static_cast<std::int64_t>(r) * 4 * kMiB;
              auto [reader, token] = d->Read(off, cfg.range_size);
              std::int64_t got = 0;
              status =
                  DrainAsyncReader(std::move(reader), std::move(token), &got);
              if (!status.ok()) break;
              if (got != cfg.range_size) {
                status = gc::Status(gc::StatusCode::kDataLoss,
                                    "short read in read50x2x4-serial");
                break;
              }
              op_bytes += got;
            }
            break;
          }
          case WorkloadKind::kRead50x2x4Concurrent: {
            op_name = "READ50X2X4_CONCURRENT";
            gc::StatusOr<gcs::ObjectDescriptor> d =
                clients.async_client
                    ->Open(gcs::BucketName(cfg.bucket), staged_object)
                    .get();
            if (!d) {
              status = std::move(d).status();
              break;
            }
            struct BatchState {
              std::mutex mu;
              std::condition_variable cv;
              int completed = 0;
              gc::Status first_err;
            };
            auto batch = std::make_shared<BatchState>();
            auto bytes_acc = std::make_shared<std::atomic<std::int64_t>>(0);
            for (int r = 0; r < 50; ++r) {
              std::int64_t const off = static_cast<std::int64_t>(r) * 4 * kMiB;
              auto [reader, token] = d->Read(off, cfg.range_size);
              DrainAsyncReaderCallback(std::move(reader), std::move(token),
                                       bytes_acc, [batch](gc::Status s) {
                                         std::scoped_lock lk(batch->mu);
                                         if (!s.ok() && batch->first_err.ok()) {
                                           batch->first_err = std::move(s);
                                         }
                                         ++batch->completed;
                                         batch->cv.notify_one();
                                       });
            }
            {
              std::unique_lock<std::mutex> lk(batch->mu);
              if (!batch->cv.wait_for(lk, std::chrono::seconds(180),
                                      [&] { return batch->completed == 50; })) {
                status = gc::Status(gc::StatusCode::kDeadlineExceeded,
                                    "read50x2x4-concurrent timed out");
              } else {
                status = batch->first_err;
              }
            }
            op_bytes = bytes_acc->load();
            if (status.ok() && op_bytes != 50 * cfg.range_size) {
              status = gc::Status(gc::StatusCode::kDataLoss,
                                  "short read in read50x2x4-concurrent");
            }
            break;
          }
          case WorkloadKind::kRandom8k: {
            op_name = "RANDOM8K";
            if (!w3_descriptor.has_value()) {
              status = w3_open_error;
              break;
            }
            std::uint64_t const max_off = static_cast<std::uint64_t>(
                std::max<std::int64_t>(0, cfg.object_size - cfg.range_size));
            auto const off = static_cast<std::int64_t>(rng.Below(max_off + 1));
            auto [reader, token] = w3_descriptor->Read(off, cfg.range_size);
            status = DrainAsyncReader(std::move(reader), std::move(token),
                                      &op_bytes);
            if (status.ok() && op_bytes != cfg.range_size) {
              status =
                  gc::Status(gc::StatusCode::kDataLoss, "short 8 KiB read");
            }
            break;
          }
          case WorkloadKind::kBidiRead48t: {
            op_name = "BIDI_READ_OBJECT";
            gc::StatusOr<gcs::ObjectDescriptor> d =
                clients.async_client
                    ->Open(gcs::BucketName(cfg.bucket), staged_object)
                    .get();
            if (!d) {
              status = std::move(d).status();
              break;
            }
            for (std::int64_t off = 0; off < cfg.object_size;
                 off += cfg.range_size) {
              std::int64_t const len =
                  std::min<std::int64_t>(cfg.range_size, cfg.object_size - off);
              SteadyClock::time_point const tr0 = SteadyClock::now();
              auto [reader, token] = d->Read(off, len);
              std::int64_t got = 0;
              status =
                  DrainAsyncReader(std::move(reader), std::move(token), &got);
              SteadyClock::time_point const tr1 = SteadyClock::now();
              if (!status.ok()) break;
              if (got != len) {
                status = gc::Status(gc::StatusCode::kDataLoss,
                                    "short range read in bidi-read-48t");
                break;
              }
              op_bytes += got;
              if (record && rec != nullptr) {
                rec->RecordRangeLatency(
                    std::chrono::duration<double, std::micro>(tr1 - tr0)
                        .count());
              }
            }
            break;
          }
          case WorkloadKind::kBidiWrite48t: {
            op_name = "BIDI_WRITE_OBJECT";
            std::string const obj_name =
                absl::StrCat(cfg.prefix, "w5/", run_nonce, "/t", tid, "-",
                             (record ? "m" : "w"), seq);
            {
              std::scoped_lock lk(created_mu);
              created_objects.push_back(obj_name);
            }
            status = UploadAppendable(*clients.async_client, cfg.bucket,
                                      obj_name, write_chunks, cfg.object_size);
            if (status.ok()) op_bytes = cfg.object_size;
            break;
          }
          case WorkloadKind::kListObjects100: {
            op_name = "LIST_OBJECTS_100";
            int count = 0;
            for (gc::StatusOr<gcs::ObjectMetadata>& item :
                 clients.sync_client->ListObjects(
                     cfg.bucket, gcs::Prefix(list_prefix),
                     gcs::MaxResults(cfg.list_count))) {
              if (!item) {
                status = std::move(item).status();
                break;
              }
              ++count;
            }
            if (status.ok() && count != cfg.list_count) {
              status =
                  gc::Status(gc::StatusCode::kDataLoss,
                             absl::StrCat("ListObjects returned ", count,
                                          " items; expected ", cfg.list_count));
            }
            break;
          }
          case WorkloadKind::kListBuckets100: {
            op_name = "LIST_BUCKETS_100";
            int count = 0;
            for (gc::StatusOr<gcs::BucketMetadata>& item :
                 clients.sync_client->ListBucketsForProject(
                     cfg.project, gcs::Prefix(cfg.bucket_prefix),
                     gcs::MaxResults(cfg.list_count))) {
              if (!item) {
                status = std::move(item).status();
                break;
              }
              ++count;
            }
            if (status.ok() && count != cfg.list_count) {
              status = gc::Status(
                  gc::StatusCode::kDataLoss,
                  absl::StrCat("ListBucketsForProject returned ", count,
                               " buckets; expected ", cfg.list_count));
            }
            break;
          }
        }

        SteadyClock::time_point const t1 = SteadyClock::now();
        if (record && rec != nullptr) {
          SampleRow row;
          row.ts_utc = FormatUtcIso8601(ts_wall);
          row.thread_id = tid;
          row.seq = seq;
          row.op = std::move(op_name);
          row.bytes = status.ok() ? op_bytes : 0;
          row.latency_us =
              std::chrono::duration<double, std::micro>(t1 - t0).count();
          row.ok = status.ok();
          row.error = FormatStatus(status);
          rec->RecordSample(std::move(row));
        }
      }
    };

    std::vector<std::thread> workers;
    workers.reserve(static_cast<std::size_t>(cfg.threads));
    for (int t = 0; t < cfg.threads; ++t) {
      workers.emplace_back(worker_fn, t);
    }
    for (std::thread& w : workers) w.join();
  };

  // --- Warm-up (excluded from CSV, latency summary, and Monitoring window) ---
  if (w3_open_error.ok()) {
    run_phase(cfg.warmup_secs, /*record=*/false, nullptr);
  }

  // --- Timed & monitored measurement window ----------------------------------
  ResetSelfPeakRss();
  std::optional<gcs_core::SidecarStats> srv_start;
  std::optional<std::uint64_t> inproc_cpu_start_us;
  int server_pid = 0;
  if (clients.core_control.has_value()) {
    server_pid = clients.core_control->sidecar_pid();
    inproc_cpu_start_us = clients.core_control->core_thread_cpu_us();
    gc::StatusOr<gcs_core::SidecarStats> st =
        clients.core_control->GetSidecarStats(/*reset_peak_rss=*/true);
    if (st) srv_start = *st;
  }
  ProcMetrics const app_start = ReadSelfMetrics();
  SystemClock::time_point const win_start_sys = SystemClock::now();
  SteadyClock::time_point const win_start_steady = SteadyClock::now();

  Recorder recorder;
  run_phase(cfg.duration_secs, /*record=*/true, &recorder);

  SteadyClock::time_point const win_end_steady = SteadyClock::now();
  SystemClock::time_point const win_end_sys = SystemClock::now();
  ProcMetrics const app_end = ReadSelfMetrics();
  std::optional<gcs_core::SidecarStats> srv_end;
  std::optional<std::uint64_t> inproc_cpu_end_us;
  if (clients.core_control.has_value()) {
    server_pid = clients.core_control->sidecar_pid();
    inproc_cpu_end_us = clients.core_control->core_thread_cpu_us();
    gc::StatusOr<gcs_core::SidecarStats> st =
        clients.core_control->GetSidecarStats(/*reset_peak_rss=*/false);
    if (st) srv_end = *st;
  }
  w3_descriptor.reset();

  // --- Post-window cleanup (outside timed window and Stats snapshot) ---------
  if (!created_objects.empty()) {
    DeleteObjectsConcurrently(*clients.async_client, cfg.bucket,
                              created_objects, cfg.threads);
  }

  double const wall_secs =
      std::chrono::duration<double>(win_end_steady - win_start_steady).count();
  std::string const win_start_utc = FormatUtcIso8601(win_start_sys);
  std::string const win_end_utc = FormatUtcIso8601(win_end_sys);

  if (!recorder.WriteCsv(cfg.out_csv, cfg)) {
    std::cerr << "Failed to write CSV: " << cfg.out_csv << "\n";
    return 1;
  }

  std::ostringstream json_buf;
  WriteSummaryJson(json_buf, cfg, win_start_utc, win_end_utc, wall_secs,
                   recorder, app_start, app_end, srv_start, srv_end,
                   inproc_cpu_start_us, inproc_cpu_end_us, server_pid);
  if (!cfg.summary_json.empty()) {
    std::ofstream jf(cfg.summary_json);
    if (!jf.is_open()) {
      std::cerr << "Failed to write summary JSON: " << cfg.summary_json << "\n";
      return 1;
    }
    jf << json_buf.str();
  }

  LatencySummary const obj = recorder.ObjectSummary();
  std::cout << std::fixed << std::setprecision(2)
            << "SUMMARY mode=" << cfg.mode_name
            << " workload=" << cfg.workload_name << " window=[" << win_start_utc
            << "," << win_end_utc << "]"
            << " wall_s=" << wall_secs << " ok=" << recorder.ops_ok()
            << " err=" << recorder.ops_err() << " p50_us=" << obj.p50_us
            << " p90_us=" << obj.p90_us << " p99_us=" << obj.p99_us << " mb_s="
            << (wall_secs > 0.0
                    ? (static_cast<double>(recorder.total_bytes()) / 1e6) /
                          wall_secs
                    : 0.0)
            << " mib_s="
            << (wall_secs > 0.0 ? (static_cast<double>(recorder.total_bytes()) /
                                   static_cast<double>(kMiB)) /
                                      wall_secs
                                : 0.0)
            << "\n";
  if (recorder.ops_err() > 0) {
    std::cerr << "first_error: " << recorder.first_error() << "\n";
  }
  return recorder.ops_ok() > 0 ? 0 : 1;
}
