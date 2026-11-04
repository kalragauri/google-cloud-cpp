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

// End-to-end check of the public `google-cloud-cpp` SDK clients
// (`google::cloud::storage::Client` and `google::cloud::storage::AsyncClient`)
// backed by the shared Rust gRPC-over-UDS server against real buckets.
//
//   quickstart [--project=PROJECT] REGIONAL_BUCKET
//   quickstart --zonal ZONAL_BUCKET
//
// Prints PASS and exits 0 when every step behaves as expected.

#include "google/cloud/storage/async/client.h"
#include "google/cloud/storage/client.h"
#include "absl/strings/cord.h"
#include "absl/strings/match.h"
#include "gcs_core/client.h"
#include "gcs_core/storage_client.h"
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <iterator>
#include <random>
#include <string>
#include <string_view>
#include <utility>

namespace {

namespace gcs = ::google::cloud::storage;
using ::google::cloud::Status;
using ::google::cloud::StatusCode;
using ::google::cloud::StatusOr;

int failures = 0;

void Check(bool ok, std::string const& what) {
  std::cout << (ok ? "  ok   " : "  FAIL ") << what << "\n";
  if (!ok) ++failures;
}

std::string RandomBytes(std::size_t n) {
  std::mt19937_64 gen(42);
  std::string s(n, '\0');
  for (char& c : s) c = static_cast<char>(gen());
  return s;
}

StatusOr<std::string> DrainAsyncReader(
    std::pair<gcs::AsyncReader, gcs::AsyncToken> stream) {
  auto [reader, token] = std::move(stream);
  std::string out;
  while (token.valid()) {
    StatusOr<std::pair<gcs::ReadPayload, gcs::AsyncToken>> step =
        reader.Read(std::move(token)).get();
    if (!step) return std::move(step).status();
    absl::AppendCordToString(step->first.cord_contents(), &out);
    token = std::move(step->second);
  }
  return out;
}

std::string ObjectName(std::string_view suffix) {
  std::chrono::system_clock::rep const now =
      std::chrono::system_clock::now().time_since_epoch().count();
  return "gcs-uds-poc/quickstart/" + std::to_string(now) + "/" +
         std::string(suffix);
}

void Regional(gcs::Client& client, gcs::AsyncClient& async_client,
              std::string const& bucket, std::string const& project) {
  std::string const data = RandomBytes((3 * 1024 * 1024) + 17);
  std::string const name = ObjectName("regional.bin");

  std::cout << "single-shot upload via storage::Client::InsertObject (JSON)\n";
  StatusOr<gcs::ObjectMetadata> meta =
      client.InsertObject(bucket, name, data, gcs::IfGenerationMatch(0),
                          gcs::ContentType("application/octet-stream"));
  Check(meta.ok(), "InsertObject: " + meta.status().message());
  if (!meta) return;
  Check(meta->size() == data.size(), "uploaded size matches");

  std::cout << "resumable upload via storage::Client::WriteObject (JSON)\n";
  gcs::ObjectWriteStream writer =
      client.WriteObject(bucket, name + ".resumable");
  writer.write(data.data(), static_cast<std::streamsize>(data.size()));
  writer.Close();
  StatusOr<gcs::ObjectMetadata> rmeta = writer.metadata();
  Check(rmeta.ok() && rmeta->size() == data.size(),
        "WriteObject Close: " + rmeta.status().message());

  std::cout << "full read via storage::Client::ReadObject (JSON)\n";
  gcs::ObjectReadStream reader = client.ReadObject(bucket, name);
  std::string const got(std::istreambuf_iterator<char>(reader), {});
  Check(reader.status().ok(), "ReadObject: " + reader.status().message());
  Check(got == data, "read back == written");
  Check(reader.headers().count("x-goog-hash") > 0,
        "headers carry x-goog-hash crc32c");

  std::cout << "ranged read via storage::Client::ReadObject (JSON)\n";
  gcs::ObjectReadStream ranged =
      client.ReadObject(bucket, name, gcs::ReadRange(1000, 5096));
  std::string const slice(std::istreambuf_iterator<char>(ranged), {});
  Check(ranged.status().ok() && slice == data.substr(1000, 4096),
        "segment [1000, 5096): " + ranged.status().message());

  std::cout << "list objects via storage::Client::ListObjects (gRPC)\n";
  std::size_t object_count = 0;
  Status list_status;
  for (StatusOr<gcs::ObjectMetadata> const& item :
       client.ListObjects(bucket, gcs::Prefix(name))) {
    if (!item) {
      list_status = item.status();
      break;
    }
    ++object_count;
  }
  Check(list_status.ok() && object_count == 2,
        "ListObjects finds both objects: " + list_status.message());

  if (!project.empty()) {
    std::cout << "list buckets via storage::Client::ListBucketsForProject "
                 "(gRPC)\n";
    std::size_t bucket_count = 0;
    Status bucket_status;
    for (StatusOr<gcs::BucketMetadata> const& b :
         client.ListBucketsForProject(project, gcs::MaxResults(5))) {
      if (!b) {
        bucket_status = b.status();
        break;
      }
      if (++bucket_count >= 5) break;
    }
    Check(bucket_status.ok() && bucket_count >= 1,
          "ListBucketsForProject: " + bucket_status.message());
  }

  std::cout << "bidi read via storage::AsyncClient::Open (gRPC)\n";
  StatusOr<gcs::ObjectDescriptor> descriptor =
      async_client.Open(gcs::BucketName(bucket), name).get();
  // BidiReadObject is allow-listed per project/bucket by the service; report
  // that as an environment gap rather than an adapter failure.
  if (!descriptor &&
      descriptor.status().code() == StatusCode::kPermissionDenied &&
      absl::StrContains(descriptor.status().message(), "not yet available")) {
    std::cout << "  GAP  BidiReadObject not enabled for this bucket: "
              << descriptor.status().message().substr(0, 140) << "\n";
  } else {
    Check(descriptor.ok(),
          "AsyncClient::Open: " + descriptor.status().message());
  }
  if (descriptor) {
    auto r0 = descriptor->Read(0, 1024);
    auto r1 = descriptor->Read(2048, 1024);
    StatusOr<std::string> p0 = DrainAsyncReader(std::move(r0));
    StatusOr<std::string> p1 = DrainAsyncReader(std::move(r1));
    Check(p0.ok() && p1.ok(), "ObjectDescriptor::Read x2");
    if (p0 && p1) {
      Check(*p0 == data.substr(0, 1024) && *p1 == data.substr(2048, 1024),
            "concurrent ranges match");
    }
  }

  std::cout << "errors\n";
  gcs::ObjectReadStream missing =
      client.ReadObject(bucket, name + ".does-not-exist");
  (void)std::string(std::istreambuf_iterator<char>(missing), {});
  Check(missing.status().code() == StatusCode::kNotFound,
        "missing object -> NOT_FOUND (" + missing.status().message() + ")");
  StatusOr<gcs::ObjectMetadata> precondition =
      client.InsertObject(bucket, name, "x", gcs::IfGenerationMatch(0));
  Check(precondition.status().code() == StatusCode::kFailedPrecondition,
        "ifGenerationMatch=0 on existing -> FAILED_PRECONDITION (" +
            precondition.status().message() + ")");

  std::cout << "delete via storage::Client::DeleteObject (gRPC)\n";
  Check(client.DeleteObject(bucket, name).ok(), "DeleteObject");
  Check(client.DeleteObject(bucket, name + ".resumable").ok(),
        "DeleteObject resumable");
}

void Zonal(gcs::Client& client, gcs::AsyncClient& async_client,
           std::string const& bucket) {
  std::string const data = RandomBytes((5 * 1024 * 1024) + 3);
  std::string const name = ObjectName("zonal.bin");

  std::cout << "appendable upload via AsyncClient::StartAppendableObjectUpload "
               "(gRPC BidiWriteObject)\n";
  StatusOr<std::pair<gcs::AsyncWriter, gcs::AsyncToken>> started =
      async_client.StartAppendableObjectUpload(gcs::BucketName(bucket), name)
          .get();
  Check(started.ok(),
        "StartAppendableObjectUpload: " + started.status().message());
  if (!started) return;
  auto [writer, token] = *std::move(started);
  std::size_t const split = 2 * 1024 * 1024;
  StatusOr<gcs::AsyncToken> next_tok =
      writer.Write(std::move(token), gcs::WritePayload(data.substr(0, split)))
          .get();
  Check(next_tok.ok(), "AsyncWriter::Write: " + next_tok.status().message());
  if (!next_tok) return;
  Status flush_status = writer.Flush().get();
  Check(flush_status.ok(), "AsyncWriter::Flush: " + flush_status.message());
  StatusOr<google::storage::v2::Object> obj =
      writer
          .Finalize(*std::move(next_tok), gcs::WritePayload(data.substr(split)))
          .get();
  Check(obj.ok() && static_cast<std::size_t>(obj->size()) == data.size(),
        "AsyncWriter::Finalize: " + obj.status().message());
  auto const& headers = writer.GetRequestMetadata().headers;
  auto const transport_it = headers.find("x-uds-transport-used");
  Check(transport_it != headers.end() && transport_it->second == "GRPC",
        "x-uds-transport-used == GRPC");

  std::cout << "bidi read via AsyncClient::Open (gRPC BidiReadObject)\n";
  StatusOr<gcs::ObjectDescriptor> descriptor =
      async_client.Open(gcs::BucketName(bucket), name).get();
  Check(descriptor.ok(), "AsyncClient::Open: " + descriptor.status().message());
  if (descriptor) {
    StatusOr<std::string> whole =
        DrainAsyncReader(descriptor->ReadFromOffset(0));
    Check(whole.ok() && *whole == data,
          "full read via ObjectDescriptor::ReadFromOffset: " +
              whole.status().message());
    StatusOr<std::string> ranged =
        DrainAsyncReader(descriptor->Read(1000, 4096));
    Check(
        ranged.ok() && *ranged == data.substr(1000, 4096),
        "ranged read via ObjectDescriptor::Read: " + ranged.status().message());
  }

  std::cout << "regular JSON upload is rejected on a zonal bucket\n";
  StatusOr<gcs::ObjectMetadata> rejected =
      client.InsertObject(bucket, name + ".regular", "x");
  Check(!rejected.ok(),
        "InsertObject on zonal fails (" + rejected.status().message() + ")");

  std::cout << "list + delete (gRPC)\n";
  std::size_t count = 0;
  for (StatusOr<gcs::ObjectMetadata> const& item :
       client.ListObjects(bucket, gcs::Prefix(name))) {
    if (item) ++count;
  }
  Check(count == 1, "ListObjects finds zonal object");
  Status del = async_client.DeleteObject(gcs::BucketName(bucket), name).get();
  Check(del.ok(), "AsyncClient::DeleteObject: " + del.message());
}

}  // namespace

int main(int argc, char* argv[]) {
  bool zonal = false;
  std::string bucket;
  std::string project;
  if (char const* env_proj = std::getenv("GOOGLE_CLOUD_PROJECT");
      env_proj != nullptr) {
    project = env_proj;
  }
  for (int i = 1; i < argc; ++i) {
    std::string const arg = argv[i];
    if (arg == "--zonal") {
      zonal = true;
    } else if (absl::StartsWith(arg, "--project=")) {
      project = arg.substr(std::string_view("--project=").size());
    } else {
      bucket = arg;
    }
  }
  if (bucket.empty()) {
    std::cerr << "usage: quickstart [--zonal] [--project=PROJECT] BUCKET\n";
    return 2;
  }

  gcs_core::ClientOptions const core_options =
      gcs_core::DefaultAdapterClientOptions();
  StatusOr<gcs_core::Client> control = gcs_core::Client::Create(core_options);
  if (!control) {
    std::cerr << "Client::Create: " << control.status() << "\n";
    return 1;
  }
  StatusOr<gcs::Client> client = gcs_core::MakeUdsStorageClient(core_options);
  if (!client) {
    std::cerr << "MakeUdsStorageClient: " << client.status() << "\n";
    return 1;
  }
  StatusOr<gcs::AsyncClient> async_client =
      gcs_core::MakeUdsAsyncClient(core_options);
  if (!async_client) {
    std::cerr << "MakeUdsAsyncClient: " << async_client.status() << "\n";
    return 1;
  }

  StatusOr<gcs_core::SidecarVersion> version = control->GetSidecarVersion();
  if (version) std::cout << "server pid=" << version->pid << "\n";
  if (zonal) {
    Zonal(*client, *async_client, bucket);
  } else {
    Regional(*client, *async_client, bucket, project);
  }
  std::cout << (failures == 0 ? "PASS\n" : "FAIL\n");
  return failures == 0 ? 0 : 1;
}
