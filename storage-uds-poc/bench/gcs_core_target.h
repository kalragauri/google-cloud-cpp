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

#ifndef GCS_UDS_POC_BENCH_GCS_CORE_TARGET_H
#define GCS_UDS_POC_BENCH_GCS_CORE_TARGET_H

// The workload body shared by the two wrapper benchmarks (`ssb_uds` over the
// sidecar, `ssb_inproc` over the in-process core). Both drive the same
// `gcs_core::Client` API, so the only per-variant code is how a binary names
// itself and where it reads the core's counters from.
//
//   JSON reads/writes : core read_object / write_object (HTTP/1.1 JSON)
//   list, delete      : core StorageControl (gRPC, Cloudpath)
//   bidi              : core open_object / appendable writer (gRPC bidi)

#include "gcs_core/client.h"
#include "ssb_workloads.h"
#include <iostream>
#include <optional>
#include <string>
#include <vector>

namespace gcs_core_bench {

inline std::string Err(gcs_core::Status const& s) {
  return s.ok()
             ? std::string()
             : google::cloud::StatusCodeToString(s.code()) + ": " + s.message();
}

inline absl::Cord Bytes(char const* data, std::int64_t size) {
  // The benchmark payload outlives every call, so the Cord can reference it
  // without copying; the wrapper/gRPC copy it into messages exactly once.
  return absl::MakeCordFromExternal(
      absl::string_view(data, static_cast<std::size_t>(size)), [] {});
}

/// Flags both wrapper benchmarks accept after the common `ssb::Args`:
/// `--channels=N`, `--chunk-size=BYTES`, `--sidecar-arg=ARG` (repeatable,
/// passed to the core; `--core-arg=ARG` is the same thing). Returns false
/// after printing the offending flag.
inline bool ParseWrapperFlags(std::vector<std::string> const& rest,
                              gcs_core::ClientOptions& options) {
  for (std::string const& arg : rest) {
    std::size_t const eq = arg.find('=');
    std::string const key = arg.substr(0, eq);
    std::string const val =
        eq == std::string::npos ? std::string() : arg.substr(eq + 1);
    if (key == "--channels") {
      options.channels = std::stoi(val);
    } else if (key == "--chunk-size") {
      options.chunk_size_hint = static_cast<std::uint32_t>(std::stoul(val));
    } else if (key == "--sidecar-arg" || key == "--core-arg") {
      options.sidecar_args.push_back(val);
    } else {
      std::cerr << "unknown flag " << arg
                << " (wrapper flags: --channels=N --chunk-size=BYTES "
                   "--sidecar-arg=ARG)\n";
      return false;
    }
  }
  return true;
}

/// `key=value;...` describing one wrapper configuration, shared by both
/// binaries so rows of the two transports line up column for column.
inline std::string WrapperLabels(std::string const& transport,
                                 gcs_core::ClientOptions const& options,
                                 gcs_core::Client& client) {
  std::string labels = "transport=" + transport +
                       ";json=http1.1;list=grpc-cfe;bidi=grpc-cfe;channels=" +
                       std::to_string(options.channels) +
                       ";chunk=" + std::to_string(options.chunk_size_hint);
  if (gcs_core::StatusOr<gcs_core::SidecarVersion> version =
          client.GetSidecarVersion();
      version) {
    labels += ";crate=" + version->storage_crate_version +
              (version->synthetic ? ";synthetic=1" : "");
  }
  for (std::string const& a : options.sidecar_args) labels += ";core" + a;
  return labels;
}

class WrapperTarget : public ssb::Target {
 public:
  explicit WrapperTarget(gcs_core::Client client)
      : client_(std::move(client)) {}

  std::string JsonUpload(std::string const& bucket, std::string const& object,
                         char const* data, std::int64_t size,
                         bool resumable) override {
    gcs_core::WriteOptions options;
    options.mode = resumable ? gcs_core::UploadMode::kResumable
                             : gcs_core::UploadMode::kSingleShot;
    return Err(client_.InsertObject(bucket, object, Bytes(data, size), options)
                   .status());
  }

  std::string JsonRead(std::string const& bucket, std::string const& object,
                       std::int64_t* bytes) override {
    gcs_core::StatusOr<gcs_core::ObjectReader> reader =
        client_.ReadObject(bucket, object);
    if (!reader) return Err(reader.status());
    std::int64_t got = 0;
    for (;;) {
      gcs_core::StatusOr<std::optional<absl::Cord>> chunk = reader->Next();
      if (!chunk) return Err(chunk.status());
      if (!*chunk) break;
      got += static_cast<std::int64_t>((*chunk)->size());
    }
    *bytes = got;
    return {};
  }

  std::string List(std::string const& bucket, std::string const& prefix,
                   int page_size, std::int64_t* count) override {
    gcs_core::ListObjectsRequest request;
    request.prefix = prefix;
    request.page_size = page_size;
    std::int64_t n = 0;
    do {
      gcs_core::StatusOr<gcs_core::ListObjectsPage> page =
          client_.ListObjects(bucket, request);
      if (!page) return Err(page.status());
      n += static_cast<std::int64_t>(page->objects.size());
      request.page_token = page->next_page_token;
    } while (!request.page_token.empty());
    *count = n;
    return {};
  }

  std::string Delete(std::string const& bucket,
                     std::string const& object) override {
    return Err(client_.DeleteObject(bucket, object));
  }

  std::string AppendableUpload(std::string const& bucket,
                               std::string const& object, char const* data,
                               std::int64_t size) override {
    gcs_core::StatusOr<gcs_core::AppendableWriter> writer =
        client_.StartAppendableUpload(bucket, object);
    if (!writer) return Err(writer.status());
    if (gcs_core::Status s = writer->Write(Bytes(data, size)); !s.ok()) {
      return Err(s);
    }
    return Err(writer->Finalize().status());
  }

  std::string Open(std::string const& bucket,
                   std::string const& object) override {
    gcs_core::StatusOr<gcs_core::ObjectDescriptor> d =
        client_.OpenObject(bucket, object);
    if (!d) return Err(d.status());
    descriptor_.emplace(*std::move(d));
    return {};
  }

  std::string Read(std::int64_t offset, std::int64_t length,
                   std::int64_t* bytes) override {
    return ReadConcurrent({{offset, length}}, bytes);
  }

  /// All ranges in one ReadRanges call: the core reads them concurrently on
  /// one BidiReadObject stream (as native issues them concurrently on one
  /// descriptor).
  std::string ReadConcurrent(ssb::Ranges const& ranges,
                             std::int64_t* bytes) override {
    std::vector<gcs_core::ReadRange> wire;
    wire.reserve(ranges.size());
    for (auto const& [off, len] : ranges) wire.push_back({off, len});
    std::int64_t got = 0;
    gcs_core::Status s =
        descriptor_->ReadRanges(wire, [&](std::size_t, absl::Cord const& c) {
          got += static_cast<std::int64_t>(c.size());
        });
    *bytes = got;
    return Err(s);
  }

  void CloseDescriptor() override { descriptor_.reset(); }

  std::string BidiReadAll(std::string const& bucket, std::string const& object,
                          std::int64_t* bytes) override {
    gcs_core::StatusOr<gcs_core::ObjectDescriptor> d =
        client_.OpenObject(bucket, object);
    if (!d) return Err(d.status());
    std::int64_t got = 0;
    gcs_core::Status s =
        d->ReadRanges({{0, 0}}, [&](std::size_t, absl::Cord const& c) {
          got += static_cast<std::int64_t>(c.size());
        });
    *bytes = got;
    return Err(s);
  }

 protected:
  gcs_core::Client client_;

 private:
  std::optional<gcs_core::ObjectDescriptor> descriptor_;
};

}  // namespace gcs_core_bench

#endif  // GCS_UDS_POC_BENCH_GCS_CORE_TARGET_H
