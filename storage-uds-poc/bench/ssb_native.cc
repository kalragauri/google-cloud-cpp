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

// Benchmark target: native google-cloud-cpp, always Cloudpath.
//
//   --sync=json-h1       storage::Client, REST, HTTP/1.1 (primary JSON row:
//                        same HTTP version as the Rust core)
//   --sync=json-default  storage::Client, REST, libcurl's default HTTP
//                        version ("customer default" context row)
//   --sync=grpc-cfe      MakeGrpcClient on storage.googleapis.com (gRPC list;
//                        sync gRPC read/write as labelled context rows)
//   bidi workloads       storage::AsyncClient on storage.googleapis.com
//
// Every gRPC client sets EndpointOption("storage.googleapis.com"): without
// it the library selects DirectPath on GCE, which the Rust core cannot use.

#include "google/cloud/storage/async/client.h"
#include "google/cloud/storage/client.h"
#include "google/cloud/storage/grpc_plugin.h"
#include "google/cloud/storage/options.h"
#include "google/cloud/common_options.h"
#include "google/cloud/grpc_options.h"
#include "absl/strings/match.h"
#include "ssb_workloads.h"
#include <condition_variable>
#include <functional>
#include <string_view>

namespace {

namespace gc = ::google::cloud;
namespace gcs = ::google::cloud::storage;
using ReadStep = std::pair<gcs::ReadPayload, gcs::AsyncToken>;

constexpr std::string_view kCloudpathEndpoint = "storage.googleapis.com";
constexpr int kGrpcChannels = 4;
/// Same resumable chunk as the Rust core's default
/// (RESUMABLE_UPLOAD_TARGET_CHUNK).
constexpr std::size_t kUploadBuffer = 8 * 1024 * 1024;

std::string Err(gc::Status const& s) {
  return s.ok() ? std::string()
                : gc::StatusCodeToString(s.code()) + ": " + s.message();
}

absl::Cord Bytes(char const* data, std::int64_t size) {
  return absl::MakeCordFromExternal(
      absl::string_view(data, static_cast<std::size_t>(size)), [] {});
}

gc::Options GrpcCloudpath() {
  return gc::Options{}
      .set<gc::EndpointOption>(std::string(kCloudpathEndpoint))
      .set<gc::GrpcNumChannelsOption>(kGrpcChannels);
}

// SSB read50x2x4_worker.cc ReadUntilDone, plus a byte counter.
void ReadUntilDone(gcs::AsyncReader reader, gcs::AsyncToken token,
                   std::shared_ptr<std::atomic<std::int64_t>> bytes,
                   std::function<void(gc::Status)> on_done) {
  gc::future<gc::StatusOr<ReadStep>> pending = reader.Read(std::move(token));
  std::move(pending).then([reader = std::move(reader), bytes = std::move(bytes),
                           on_done = std::move(on_done)](
                              gc::future<gc::StatusOr<ReadStep>> f) mutable {
    gc::StatusOr<ReadStep> result = f.get();
    if (!result.ok()) return on_done(result.status());
    ReadStep pair = *std::move(result);
    *bytes += static_cast<std::int64_t>(pair.first.size());
    if (pair.second.valid()) {
      ReadUntilDone(std::move(reader), std::move(pair.second), std::move(bytes),
                    std::move(on_done));
    } else {
      on_done(gc::Status());
    }
  });
}

std::string DrainReader(gcs::AsyncReader reader, gcs::AsyncToken token,
                        std::int64_t* bytes) {
  std::int64_t got = 0;
  while (token.valid()) {
    gc::StatusOr<ReadStep> r = reader.Read(std::move(token)).get();
    if (!r) return Err(r.status());
    got += static_cast<std::int64_t>(r->first.size());
    token = std::move(r->second);
  }
  *bytes = got;
  return {};
}

class Native : public ssb::Target {
 public:
  explicit Native(std::string sync) : sync_mode_(std::move(sync)) {
    if (sync_mode_ == "grpc-cfe") {
      sync_.emplace(gcs::MakeGrpcClient(
          GrpcCloudpath().set<gcs::UploadBufferSizeOption>(kUploadBuffer)));
    } else if (sync_mode_ == "json-h1") {
      sync_.emplace(gcs::Client(
          gc::Options{}
              .set<gcs::UploadBufferSizeOption>(kUploadBuffer)
              .set<gc::storage_experimental::HttpVersionOption>("1.1")));
    } else {
      sync_.emplace(gcs::Client(
          gc::Options{}.set<gcs::UploadBufferSizeOption>(kUploadBuffer)));
    }
  }

  std::string Name() const override { return "native-" + sync_mode_; }
  std::string Labels() const override {
    char const* http = sync_mode_ == "json-h1" ? "http1.1" : "libcurl-default";
    return std::string("transport=native;sync=") + sync_mode_ +
           ";json=" + http +
           ";grpc=cfe;channels=" + std::to_string(kGrpcChannels) +
           ";upload_buffer=8MiB;sdk=" + gc::version_string();
  }

  std::string JsonUpload(std::string const& bucket, std::string const& object,
                         char const* data, std::int64_t size,
                         bool resumable) override {
    if (!resumable) {
      gc::StatusOr<gcs::ObjectMetadata> r = sync_->InsertObject(
          bucket, object,
          std::string_view{data, static_cast<std::size_t>(size)});
      return Err(r.status());
    }
    gcs::ObjectWriteStream os = sync_->WriteObject(bucket, object);
    os.write(data, size);
    os.Close();
    return Err(os.metadata().status());
  }

  std::string JsonRead(std::string const& bucket, std::string const& object,
                       std::int64_t* bytes) override {
    gcs::ObjectReadStream is = sync_->ReadObject(bucket, object);
    std::vector<char> discard(2 * 1024 * 1024);  // as SSB read_object()
    std::int64_t got = 0;
    while (true) {
      is.read(discard.data(), static_cast<std::streamsize>(discard.size()));
      got += is.gcount();
      if (!is.good()) break;
    }
    *bytes = got;
    return Err(is.status());
  }

  std::string List(std::string const& bucket, std::string const& prefix,
                   int page_size, std::int64_t* count) override {
    std::int64_t n = 0;
    for (gc::StatusOr<gcs::ObjectMetadata>& o : sync_->ListObjects(
             bucket, gcs::Prefix(prefix), gcs::MaxResults(page_size))) {
      if (!o) return Err(o.status());
      ++n;
    }
    *count = n;
    return {};
  }

  std::string Delete(std::string const& bucket,
                     std::string const& object) override {
    return Err(sync_->DeleteObject(bucket, object));
  }

  std::string AppendableUpload(std::string const& bucket,
                               std::string const& object, char const* data,
                               std::int64_t size) override {
    gc::StatusOr<std::pair<gcs::AsyncWriter, gcs::AsyncToken>> start =
        Async()
            .StartAppendableObjectUpload(gcs::BucketName(bucket), object)
            .get();
    if (!start) return Err(start.status());
    auto [writer, token] = *std::move(start);
    gc::StatusOr<gcs::AsyncToken> w =
        writer.Write(std::move(token), gcs::WritePayload(Bytes(data, size)))
            .get();
    if (!w) return Err(w.status());
    return Err(writer.Finalize(*std::move(w)).get().status());
  }

  std::string Open(std::string const& bucket,
                   std::string const& object) override {
    gc::StatusOr<gcs::ObjectDescriptor> d =
        Async().Open(gcs::BucketName(bucket), object).get();
    if (!d) return Err(d.status());
    desc_.emplace(*std::move(d));
    return {};
  }

  std::string Read(std::int64_t offset, std::int64_t length,
                   std::int64_t* bytes) override {
    auto [reader, token] = length == 0 ? desc_->ReadFromOffset(offset)
                                       : desc_->Read(offset, length);
    return DrainReader(std::move(reader), std::move(token), bytes);
  }

  std::string ReadConcurrent(ssb::Ranges const& ranges,
                             std::int64_t* bytes) override {
    struct State {
      std::mutex mu;
      std::condition_variable cv;
      int done = 0;
      std::string first_error;
    };
    auto state = std::make_shared<State>();
    auto counter = std::make_shared<std::atomic<std::int64_t>>(0);
    int const n = static_cast<int>(ranges.size());
    for (auto const& [off, len] : ranges) {
      auto [reader, token] = desc_->Read(off, len);
      ReadUntilDone(std::move(reader), std::move(token), counter,
                    [state](gc::Status const& s) {
                      std::scoped_lock lk(state->mu);
                      if (!s.ok() && state->first_error.empty()) {
                        state->first_error = Err(s);
                      }
                      ++state->done;
                      state->cv.notify_one();
                    });
    }
    std::unique_lock<std::mutex> lk(state->mu);
    if (!state->cv.wait_for(lk, std::chrono::seconds(120),
                            [&] { return state->done == n; })) {
      return "Download timeout";
    }
    *bytes = counter->load();
    return state->first_error;
  }

  void CloseDescriptor() override { desc_.reset(); }

  std::string BidiReadAll(std::string const& bucket, std::string const& object,
                          std::int64_t* bytes) override {
    gc::StatusOr<gcs::ObjectDescriptor> d =
        Async().Open(gcs::BucketName(bucket), object).get();
    if (!d) return Err(d.status());
    auto [reader, token] = d->ReadFromOffset(0);
    return DrainReader(std::move(reader), std::move(token), bytes);
  }

 private:
  /// Created on first use, so JSON-only runs carry no gRPC background
  /// threads in their CPU numbers.
  gcs::AsyncClient& Async() {
    std::call_once(async_once_, [this] { async_.emplace(GrpcCloudpath()); });
    return *async_;
  }

  std::string sync_mode_;
  std::optional<gcs::Client> sync_;
  std::once_flag async_once_;
  std::optional<gcs::AsyncClient> async_;
  std::optional<gcs::ObjectDescriptor> desc_;
};

}  // namespace

int main(int argc, char* argv[]) {
  ssb::Args args;
  std::vector<std::string> const rest = ssb::ParseArgs(argc, argv, args);
  std::string sync = "json-h1";
  for (std::string const& arg : rest) {
    if (absl::StartsWith(arg, "--sync=")) {
      sync = arg.substr(7);
    } else {
      std::cerr << "unknown flag " << arg
                << " (native flags: --sync=json-h1|json-default|grpc-cfe)\n";
      return 2;
    }
  }
  if (sync != "json-h1" && sync != "json-default" && sync != "grpc-cfe") {
    std::cerr << "--sync must be json-h1, json-default or grpc-cfe\n";
    return 2;
  }
  Native target(sync);
  return ssb::Runner(target, args).Run();
}
