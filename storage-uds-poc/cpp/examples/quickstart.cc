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

// End-to-end check of the wrapper against real buckets.
//
//   quickstart REGIONAL_BUCKET            # JSON read/write + gRPC list/delete
//                                         # + gRPC bidi read
//   quickstart --zonal ZONAL_BUCKET       # appendable write + bidi read
//
// Prints PASS and exits 0 when every step behaves as expected.

#include "absl/strings/match.h"
#include "gcs_core/client.h"
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <random>
#include <string>
#include <vector>

namespace {

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

std::string ReadAllAppend(gcs_core::ObjectReader& reader,
                          gcs_core::Status& status) {
  std::string out;
  for (;;) {
    gcs_core::StatusOr<std::optional<absl::Cord>> chunk = reader.Next();
    if (!chunk) {
      status = chunk.status();
      return out;
    }
    if (!*chunk) return out;
    absl::AppendCordToString(**chunk, &out);
  }
}

std::string ObjectName(std::string const& suffix) {
  std::chrono::system_clock::rep const now =
      std::chrono::system_clock::now().time_since_epoch().count();
  return "gcs-uds-poc/quickstart/" + std::to_string(now) + "/" + suffix;
}

void Regional(gcs_core::Client& client, std::string const& bucket) {
  std::string const data = RandomBytes((3 * 1024 * 1024) + 17);
  std::string const name = ObjectName("regional.bin");

  std::cout << "single-shot upload (JSON)\n";
  gcs_core::WriteOptions single;
  single.mode = gcs_core::UploadMode::kSingleShot;
  single.if_generation_match = 0;
  gcs_core::StatusOr<gcs_core::ObjectMetadata> meta =
      client.InsertObject(bucket, name, absl::Cord(data), single);
  Check(meta.ok(), "InsertObject: " + meta.status().message());
  if (!meta) return;
  Check(meta->size == data.size(), "uploaded size matches");

  std::cout << "resumable upload (JSON)\n";
  gcs_core::WriteOptions resumable;
  resumable.mode = gcs_core::UploadMode::kResumable;
  gcs_core::StatusOr<gcs_core::ObjectWriter> writer =
      client.WriteObject(bucket, name + ".resumable", resumable);
  Check(writer.ok(), "WriteObject");
  if (writer) {
    Check(writer->Write(absl::Cord(data)).ok(), "Write");
    gcs_core::StatusOr<gcs_core::ObjectMetadata> rmeta = writer->Close();
    Check(rmeta.ok() && rmeta->size == data.size(),
          "resumable Close: " + rmeta.status().message());
  }

  std::cout << "full read (JSON)\n";
  gcs_core::StatusOr<gcs_core::ObjectReader> reader =
      client.ReadObject(bucket, name);
  Check(reader.ok(), "ReadObject: " + reader.status().message());
  if (reader) {
    gcs_core::Status status;
    std::string const got = ReadAllAppend(*reader, status);
    Check(status.ok(), "read status: " + status.message());
    Check(got == data, "read back == written");
    Check(reader->metadata().crc32c.has_value(), "metadata carries crc32c");
  }

  std::cout << "ranged read (JSON)\n";
  gcs_core::StatusOr<gcs_core::ObjectReader> ranged =
      client.ReadObject(bucket, name, {1000, 4096});
  if (ranged) {
    gcs_core::Status status;
    std::string const got = ReadAllAppend(*ranged, status);
    Check(status.ok() && got == data.substr(1000, 4096),
          "segment [1000, 5096)");
  } else {
    Check(false, "ranged ReadObject: " + ranged.status().message());
  }

  std::cout << "list (gRPC)\n";
  gcs_core::ListObjectsRequest list;
  list.prefix = name;
  gcs_core::StatusOr<gcs_core::ListObjectsPage> page =
      client.ListObjects(bucket, list);
  Check(page.ok() && page->objects.size() == 2,
        "ListObjects finds both objects: " + page.status().message());

  std::cout << "bidi read (gRPC BidiReadObject)\n";
  gcs_core::StatusOr<gcs_core::ObjectDescriptor> descriptor =
      client.OpenObject(bucket, name);
  // BidiReadObject is allow-listed per project/bucket by the service; report
  // that as an environment gap rather than a wrapper failure.
  if (!descriptor &&
      descriptor.status().code() ==
          google::cloud::StatusCode::kPermissionDenied &&
      absl::StrContains(descriptor.status().message(), "not yet available")) {
    std::cout << "  GAP  BidiReadObject not enabled for this bucket: "
              << descriptor.status().message().substr(0, 140) << "\n";
  } else {
    Check(descriptor.ok(), "OpenObject: " + descriptor.status().message());
  }
  if (descriptor) {
    std::vector<std::string> parts(2);
    gcs_core::Status s = descriptor->ReadRanges(
        {{0, 1024}, {2048, 1024}}, [&](std::size_t i, absl::Cord const& c) {
          absl::AppendCordToString(c, &parts[i]);
        });
    Check(s.ok(), "ReadRanges: " + s.message());
    Check(
        parts[0] == data.substr(0, 1024) && parts[1] == data.substr(2048, 1024),
        "ranges match");
  }

  std::cout << "errors\n";
  gcs_core::StatusOr<gcs_core::ObjectReader> missing =
      client.ReadObject(bucket, name + ".does-not-exist");
  Check(!missing.ok() &&
            missing.status().code() == google::cloud::StatusCode::kNotFound,
        "missing object -> NOT_FOUND (" + missing.status().message() + ")");
  gcs_core::StatusOr<gcs_core::ObjectMetadata> precondition =
      client.InsertObject(bucket, name, absl::Cord("x"), single);
  Check(
      !precondition.ok() && precondition.status().code() ==
                                google::cloud::StatusCode::kFailedPrecondition,
      "ifGenerationMatch=0 on existing -> FAILED_PRECONDITION (" +
          precondition.status().message() + ")");

  std::cout << "delete (gRPC)\n";
  Check(client.DeleteObject(bucket, name).ok(), "DeleteObject");
  Check(client.DeleteObject(bucket, name + ".resumable").ok(),
        "DeleteObject resumable");
}

void Zonal(gcs_core::Client& client, std::string const& bucket) {
  std::string const data = RandomBytes((5 * 1024 * 1024) + 3);
  std::string const name = ObjectName("zonal.bin");

  std::cout << "appendable upload (gRPC BidiWriteObject)\n";
  gcs_core::StatusOr<gcs_core::AppendableWriter> writer =
      client.StartAppendableUpload(bucket, name);
  Check(writer.ok(), "StartAppendableUpload");
  if (!writer) return;
  Check(writer->Write(absl::Cord(data)).ok(), "Write");
  gcs_core::StatusOr<gcs_core::ObjectMetadata> meta = writer->Finalize();
  Check(meta.ok() && meta->size == data.size(),
        "Finalize: " + meta.status().message());

  std::cout << "bidi read (gRPC BidiReadObject)\n";
  gcs_core::StatusOr<gcs_core::ObjectDescriptor> descriptor =
      client.OpenObject(bucket, name);
  // BidiReadObject is allow-listed per project/bucket by the service; report
  // that as an environment gap rather than a wrapper failure.
  if (!descriptor &&
      descriptor.status().code() ==
          google::cloud::StatusCode::kPermissionDenied &&
      absl::StrContains(descriptor.status().message(), "not yet available")) {
    std::cout << "  GAP  BidiReadObject not enabled for this bucket: "
              << descriptor.status().message().substr(0, 140) << "\n";
  } else {
    Check(descriptor.ok(), "OpenObject: " + descriptor.status().message());
  }
  if (descriptor) {
    std::string whole;
    gcs_core::Status s =
        descriptor->ReadRanges({{0, 0}}, [&](std::size_t, absl::Cord const& c) {
          absl::AppendCordToString(c, &whole);
        });
    Check(s.ok() && whole == data, "full read via ReadRanges: " + s.message());
  }

  std::cout << "regular upload is rejected on a zonal bucket\n";
  gcs_core::WriteOptions single;
  single.mode = gcs_core::UploadMode::kSingleShot;
  gcs_core::StatusOr<gcs_core::ObjectMetadata> rejected =
      client.InsertObject(bucket, name + ".regular", absl::Cord("x"), single);
  Check(!rejected.ok(),
        "InsertObject on zonal fails (" + rejected.status().message() + ")");

  std::cout << "list + delete (gRPC)\n";
  gcs_core::ListObjectsRequest list;
  list.prefix = name;
  gcs_core::StatusOr<gcs_core::ListObjectsPage> page =
      client.ListObjects(bucket, list);
  Check(page.ok() && page->objects.size() == 1,
        "ListObjects: " + page.status().message());
  Check(client.DeleteObject(bucket, name).ok(), "DeleteObject");
}

}  // namespace

int main(int argc, char* argv[]) {
  bool zonal = false;
  std::string bucket;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--zonal") {
      zonal = true;
    } else {
      bucket = arg;
    }
  }
  if (bucket.empty()) {
    std::cerr << "usage: quickstart [--zonal] BUCKET\n";
    return 2;
  }
  gcs_core::StatusOr<gcs_core::Client> client = gcs_core::Client::Create();
  if (!client) {
    std::cerr << "Client::Create: " << client.status() << "\n";
    return 1;
  }
  gcs_core::StatusOr<gcs_core::SidecarVersion> version =
      client->GetSidecarVersion();
  if (version) {
    std::cout << "sidecar pid=" << version->pid
              << " crate=" << version->storage_crate_version
              << " bidi=" << version->bidi_enabled << "\n";
  }
  if (zonal) {
    Zonal(*client, bucket);
  } else {
    Regional(*client, bucket);
  }
  std::cout << (failures == 0 ? "PASS\n" : "FAIL\n");
  return failures == 0 ? 0 : 1;
}
