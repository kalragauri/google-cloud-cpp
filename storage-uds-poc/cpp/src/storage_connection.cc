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

#include "google/cloud/storage/internal/storage_connection.h"
#include "google/cloud/storage/download_options.h"
#include "google/cloud/storage/hashing_options.h"
#include "google/cloud/storage/internal/bucket_requests.h"
#include "google/cloud/storage/internal/grpc/object_metadata_parser.h"
#include "google/cloud/storage/internal/http_response.h"
#include "google/cloud/storage/internal/object_read_source.h"
#include "google/cloud/storage/internal/object_requests.h"
#include "google/cloud/storage/options.h"
#include "google/cloud/storage/upload_options.h"
#include "google/cloud/storage/well_known_headers.h"
#include "google/cloud/storage/well_known_parameters.h"
#include "google/cloud/credentials.h"
#include "google/cloud/grpc_options.h"
#include "google/cloud/internal/getenv.h"
#include "google/cloud/log.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "connection.h"
#include "gcs_core/storage_client.h"
#include "status.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace gcs_core {
namespace {

namespace pb = ::gcscore::v1;
namespace gcs = ::google::cloud::storage;
namespace gcs_internal = ::google::cloud::storage::internal;
using ::google::cloud::Options;
using ::google::cloud::StatusCode;

constexpr std::size_t kDefaultUploadBufferSize = 8 * 1024 * 1024;
constexpr std::size_t kDefaultDownloadBufferSize = 1024 * 1024;
constexpr std::size_t kDefaultMaxSimpleUploadSize = 20 * 1024 * 1024;
// Bounds in-memory upload sessions on a long-lived `Client` when callers
// abandon `Suspend()` / `AutoFinalizeDisabled()` streams without calling
// `DeleteResumableUpload`: sessions idle longer than `kAbandonedUploadIdleTtl`
// are reaped on `CreateResumableUpload`, and exceeding `kMaxActiveUploads`
// concurrent live sessions returns `kResourceExhausted` rather than evicting an
// active upload.
constexpr std::size_t kMaxActiveUploads = 1024;
constexpr std::chrono::minutes kAbandonedUploadIdleTtl{10};

Status UnimplementedMethod(std::string_view method) {
  return Status(StatusCode::kUnimplemented,
                absl::StrCat(method,
                             " is not implemented by UdsStorageConnection "
                             "(gcscore.v1.GcsCore does not expose it)"));
}

Status UnimplementedOption(std::string_view option, std::string_view method) {
  return Status(StatusCode::kUnimplemented,
                absl::StrCat(option, " is not supported by ", method,
                             " over UDS (gcscore.v1.GcsCore)"));
}

#include "google/cloud/internal/disable_deprecation_warnings.inc"
template <typename Request>
Status CheckDeprecatedHashOptions(Request const& request,
                                  std::string_view method) {
  if (request.template HasOption<gcs::Crc32cChecksumValue>()) {
    return UnimplementedOption("Crc32cChecksumValue", method);
  }
  if (request.template HasOption<gcs::MD5HashValue>()) {
    return UnimplementedOption("MD5HashValue", method);
  }
  if (request.template HasOption<gcs::DisableMD5Hash>() &&
      !request.template GetOption<gcs::DisableMD5Hash>().value()) {
    return UnimplementedOption("EnableMD5Hash", method);
  }
  return {};
}
#include "google/cloud/internal/diagnostics_pop.inc"

Status CheckChecksumAlgorithmOptions(Options const& opts,
                                     std::string_view scope) {
  if (opts.has<gcs::PrecomputedChecksumsOption>()) {
    gcs::PrecomputedChecksums const& p =
        opts.get<gcs::PrecomputedChecksumsOption>();
    if (!p.crc32c.empty() || !p.md5.empty()) {
      return UnimplementedOption("PrecomputedChecksumsOption", scope);
    }
  }
  if (opts.has<gcs::UploadChecksumValidationOption>()) {
    gcs::ChecksumAlgorithm const alg =
        opts.get<gcs::UploadChecksumValidationOption>();
    if (alg == gcs::ChecksumAlgorithm::kMD5 ||
        alg == gcs::ChecksumAlgorithm::kCrc32cAndMD5) {
      return UnimplementedOption("UploadChecksumValidationOption(MD5)", scope);
    }
  }
  if (opts.has<gcs::DownloadChecksumValidationOption>()) {
    gcs::ChecksumAlgorithm const alg =
        opts.get<gcs::DownloadChecksumValidationOption>();
    if (alg == gcs::ChecksumAlgorithm::kMD5 ||
        alg == gcs::ChecksumAlgorithm::kCrc32cAndMD5) {
      return UnimplementedOption("DownloadChecksumValidationOption(MD5)",
                                 scope);
    }
  }
  return {};
}

template <typename Request>
Status CheckCommonPreconditions(Request const& request,
                                std::string_view method) {
  if (request.template HasOption<gcs::IfGenerationNotMatch>()) {
    return UnimplementedOption("IfGenerationNotMatch", method);
  }
  if (request.template HasOption<gcs::IfMetagenerationMatch>()) {
    return UnimplementedOption("IfMetagenerationMatch", method);
  }
  if (request.template HasOption<gcs::IfMetagenerationNotMatch>()) {
    return UnimplementedOption("IfMetagenerationNotMatch", method);
  }
  if (request.template HasOption<gcs::IfGenerationMatch>() &&
      request.template GetOption<gcs::IfGenerationMatch>().value() < 0) {
    return Status(StatusCode::kInvalidArgument,
                  absl::StrCat(method, ": IfGenerationMatch must be >= 0"));
  }
  return {};
}

template <typename Request>
Status CheckWriteOptions(Request const& request, std::string_view method) {
  if (Status s = CheckCommonPreconditions(request, method); !s.ok()) return s;
  if (Status s = CheckDeprecatedHashOptions(request, method); !s.ok()) return s;
  if (Status s = CheckChecksumAlgorithmOptions(
          google::cloud::internal::CurrentOptions(), method);
      !s.ok()) {
    return s;
  }
  if (request.template HasOption<gcs::EncryptionKey>()) {
    return UnimplementedOption("EncryptionKey", method);
  }
  if (request.template HasOption<gcs::KmsKeyName>()) {
    return UnimplementedOption("KmsKeyName", method);
  }
  return {};
}

template <typename Request>
StatusOr<std::string> ExtractContentType(Request const& request,
                                         std::string_view method) {
  std::string content_type;
  if (request.template HasOption<gcs::WithObjectMetadata>()) {
    auto const opt = request.template GetOption<gcs::WithObjectMetadata>();
    gcs::ObjectMetadata const& meta = opt.value();
    gcs::ObjectMetadata only_content_type;
    if (!meta.content_type().empty()) {
      only_content_type.set_content_type(meta.content_type());
    }
    if (meta != only_content_type) {
      return UnimplementedOption(
          "WithObjectMetadata (fields other than content_type)", method);
    }
    content_type = meta.content_type();
  }
  if (request.template HasOption<gcs::ContentType>()) {
    content_type = request.template GetOption<gcs::ContentType>().value();
  }
  return content_type;
}

StatusOr<Options> ValidateAndDefaultStorageOptions(Options opts) {
  if (opts.has<google::cloud::UnifiedCredentialsOption>() ||
      opts.has<google::cloud::GrpcCredentialOption>()) {
    return Status(
        StatusCode::kUnimplemented,
        "UnifiedCredentialsOption / GrpcCredentialOption are not supported "
        "over UDS; the shared Rust server authenticates using Application "
        "Default Credentials");
  }
  if (Status s = CheckChecksumAlgorithmOptions(opts, "MakeUdsStorageClient");
      !s.ok()) {
    return s;
  }
  if (opts.has<gcs::RetryPolicyOption>() ||
      opts.has<gcs::BackoffPolicyOption>() ||
      opts.has<gcs::IdempotencyPolicyOption>()) {
    GCP_LOG(WARNING)
        << "UdsStorageConnection ignores SDK RetryPolicyOption, "
           "BackoffPolicyOption, and IdempotencyPolicyOption; retries and "
           "resumable state are owned by the shared Rust server";
  }
  // Apply stream-buffer and checksum defaults directly rather than calling
  // `DefaultOptionsWithCredentials`, which would initialize native REST/curl
  // credentials in the C++ process.
  if (!opts.has<gcs::UploadBufferSizeOption>()) {
    opts.set<gcs::UploadBufferSizeOption>(kDefaultUploadBufferSize);
  }
  if (!opts.has<gcs::DownloadBufferSizeOption>()) {
    opts.set<gcs::DownloadBufferSizeOption>(kDefaultDownloadBufferSize);
  }
  if (!opts.has<gcs::MaximumSimpleUploadSizeOption>()) {
    opts.set<gcs::MaximumSimpleUploadSizeOption>(kDefaultMaxSimpleUploadSize);
  }
  // The Rust core validates CRC32C on the wire; disabling C++ streambuf
  // hashing avoids hashing every byte a second time in the wrapper process.
  if (!opts.has<gcs::DownloadChecksumValidationOption>()) {
    opts.set<gcs::DownloadChecksumValidationOption>(
        gcs::ChecksumAlgorithm::kNone);
  }
  if (!opts.has<gcs::UploadChecksumValidationOption>()) {
    opts.set<gcs::UploadChecksumValidationOption>(
        gcs::ChecksumAlgorithm::kNone);
  }
  if (!opts.has<gcs::RestEndpointOption>()) {
    opts.set<gcs::RestEndpointOption>("https://storage.googleapis.com");
  }
  if (!opts.has<gcs::ProjectIdOption>()) {
    if (std::optional<std::string> env =
            google::cloud::internal::GetEnv("GOOGLE_CLOUD_PROJECT");
        env.has_value() && !env->empty()) {
      opts.set<gcs::ProjectIdOption>(*std::move(env));
    }
  }
  return opts;
}

gcs::ObjectMetadata ToSdkObjectMetadata(pb::WriteObjectResponse const& r,
                                        std::string const& bucket,
                                        std::string const& object,
                                        std::string const& content_type) {
  gcs::ObjectMetadata meta;
  meta.set_bucket(bucket);
  meta.set_name(object);
  meta.set_generation(r.generation());
  meta.set_size(static_cast<std::uint64_t>(r.size()));
  if (!content_type.empty()) meta.set_content_type(content_type);
  if (r.crc32c() != 0) {
    meta.set_crc32c(
        google::cloud::storage_internal::Crc32cFromProto(r.crc32c()));
  }
  return meta;
}

gcs::ObjectMetadata ToSdkListedObjectMetadata(pb::ListedObject const& item,
                                              std::string const& bucket) {
  ObjectMetadata const core = internal::FromProto(item, bucket);
  gcs::ObjectMetadata meta;
  meta.set_bucket(core.bucket);
  meta.set_name(core.name);
  meta.set_generation(core.generation);
  meta.set_metageneration(core.metageneration);
  meta.set_size(core.size);
  meta.set_content_type(core.content_type);
  meta.set_storage_class(core.storage_class);
  meta.set_updated(core.updated);
  if (core.crc32c.has_value()) {
    meta.set_crc32c(
        google::cloud::storage_internal::Crc32cFromProto(*core.crc32c));
  }
  return meta;
}

gcs::BucketMetadata ToSdkBucketMetadata(pb::ListedBucket const& item) {
  BucketMetadata const core = internal::FromProto(item);
  gcs::BucketMetadata meta;
  meta.set_id(core.name);
  meta.set_name(core.name);
  meta.set_location(core.location);
  meta.set_storage_class(core.storage_class);
  meta.set_metageneration(core.metageneration);
  meta.set_time_created(core.created);
  return meta;
}

std::size_t CopyFromCord(absl::Cord& cord, char* dst, std::size_t max_bytes) {
  std::size_t const to_copy = std::min(max_bytes, cord.size());
  std::size_t copied = 0;
  for (std::string_view const chunk : cord.Chunks()) {
    if (copied == to_copy) break;
    std::size_t const n = std::min(to_copy - copied, chunk.size());
    std::memcpy(dst + copied, chunk.data(), n);
    copied += n;
  }
  cord.RemovePrefix(copied);
  return copied;
}

class UdsObjectReadSource : public gcs_internal::ObjectReadSource {
 public:
  explicit UdsObjectReadSource(std::unique_ptr<internal::ReaderImpl> reader)
      : reader_(std::move(reader)) {}

  bool IsOpen() const override { return open_; }

  StatusOr<gcs_internal::HttpResponse> Close() override {
    open_ = false;
    leftover_.Clear();
    reader_.reset();
    return gcs_internal::HttpResponse{
        gcs_internal::HttpStatusCode::kOk, {}, {}};
  }

  StatusOr<gcs_internal::ReadSourceResult> Read(char* buf,
                                                std::size_t n) override {
    if (!open_) {
      return gcs_internal::ReadSourceResult{
          0, gcs_internal::HttpResponse{
                 gcs_internal::HttpStatusCode::kOk, {}, {}}};
    }

    gcs_internal::ReadSourceResult result{};
    if (first_read_) {
      first_read_ = false;
      ObjectMetadata const& meta = reader_->metadata();
      result.generation = meta.generation;
      result.metageneration = meta.metageneration;
      result.size = meta.size;
      if (meta.crc32c.has_value()) {
        std::string const crc32c =
            google::cloud::storage_internal::Crc32cFromProto(*meta.crc32c);
        result.hashes.crc32c = crc32c;
        result.response.headers.emplace("x-goog-hash", "crc32c=" + crc32c);
      }
      if (!meta.content_type.empty()) {
        result.response.headers.emplace("content-type", meta.content_type);
      }
      if (!meta.content_encoding.empty()) {
        result.response.headers.emplace("content-encoding",
                                        meta.content_encoding);
      }
    }

    std::size_t copied = 0;
    while (copied < n) {
      if (leftover_.empty()) {
        StatusOr<std::optional<absl::Cord>> next = reader_->Next();
        if (!next) {
          open_ = false;
          reader_.reset();
          return std::move(next).status();
        }
        if (!next->has_value()) {
          open_ = false;
          reader_.reset();
          break;
        }
        leftover_ = *std::move(*next);
      }
      copied += CopyFromCord(leftover_, buf + copied, n - copied);
    }

    result.bytes_received = copied;
    result.response.status_code = open_
                                      ? gcs_internal::HttpStatusCode::kContinue
                                      : gcs_internal::HttpStatusCode::kOk;
    return result;
  }

 private:
  std::unique_ptr<internal::ReaderImpl> reader_;
  absl::Cord leftover_;
  bool first_read_ = true;
  bool open_ = true;
};

StatusOr<std::pair<std::int64_t, std::int64_t>> ParseReadRange(
    gcs_internal::ReadObjectRangeRequest const& request) {
  bool const has_range = request.HasOption<gcs::ReadRange>();
  bool const has_offset = request.HasOption<gcs::ReadFromOffset>();
  bool const has_last = request.HasOption<gcs::ReadLast>();

  if (has_last && (has_range || has_offset)) {
    return Status(
        StatusCode::kInvalidArgument,
        "ReadLast cannot be combined with ReadRange or ReadFromOffset");
  }
  if (has_last) {
    std::int64_t const last = request.GetOption<gcs::ReadLast>().value();
    if (last <= 0) {
      return Status(StatusCode::kInvalidArgument, "ReadLast must be > 0");
    }
    return std::pair<std::int64_t, std::int64_t>{-last, -1};
  }
  if (has_range) {
    gcs::ReadRangeData const range =
        request.GetOption<gcs::ReadRange>().value();
    if (range.begin < 0 || range.end < 0) {
      return Status(StatusCode::kInvalidArgument,
                    "ReadRange bounds must be >= 0");
    }
    std::int64_t begin = range.begin;
    if (has_offset) {
      std::int64_t const offset =
          request.GetOption<gcs::ReadFromOffset>().value();
      if (offset < 0) {
        return Status(StatusCode::kInvalidArgument,
                      "ReadFromOffset must be >= 0");
      }
      begin = std::max(begin, offset);
    }
    // `gcscore.v1` maps `length == 0` via `ReadRange::segment(offset, 0)` to
    // `read_limit = 0` (unbounded), and HTTP Range headers cannot express an
    // empty `[b, b)` interval either; reject `begin >= end` explicitly.
    if (begin >= range.end) {
      return Status(StatusCode::kInvalidArgument,
                    "ReadRange requires begin < end");
    }
    return std::pair<std::int64_t, std::int64_t>{begin, range.end - begin};
  }
  if (has_offset) {
    std::int64_t const offset =
        request.GetOption<gcs::ReadFromOffset>().value();
    if (offset < 0) {
      return Status(StatusCode::kInvalidArgument,
                    "ReadFromOffset must be >= 0");
    }
    return std::pair<std::int64_t, std::int64_t>{offset, -1};
  }
  return std::pair<std::int64_t, std::int64_t>{0, -1};
}

class UdsStorageConnection : public gcs_internal::StorageConnection {
 public:
  UdsStorageConnection(std::shared_ptr<internal::Connection> conn,
                       std::uint32_t chunk_size_hint, Options options)
      : conn_(std::move(conn)),
        chunk_size_hint_(chunk_size_hint),
        options_(std::move(options)) {}

  Options options() const override { return options_; }

  StatusOr<std::unique_ptr<gcs_internal::ObjectReadSource>> ReadObject(
      gcs_internal::ReadObjectRangeRequest const& request) override {
    if (Status s = CheckCommonPreconditions(request, "ReadObject"); !s.ok()) {
      return s;
    }
    if (Status s = CheckDeprecatedHashOptions(request, "ReadObject"); !s.ok()) {
      return s;
    }
    if (Status s = CheckChecksumAlgorithmOptions(
            google::cloud::internal::CurrentOptions(), "ReadObject");
        !s.ok()) {
      return s;
    }
    if (request.HasOption<gcs::EncryptionKey>()) {
      return UnimplementedOption("EncryptionKey", "ReadObject");
    }
    if (request.HasOption<gcs::Generation>() &&
        request.GetOption<gcs::Generation>().value() < 0) {
      return Status(StatusCode::kInvalidArgument,
                    "ReadObject: Generation must be >= 0");
    }
    StatusOr<std::pair<std::int64_t, std::int64_t>> range =
        ParseReadRange(request);
    if (!range) return std::move(range).status();

    pb::ReadObjectRequest proto;
    proto.set_bucket(request.bucket_name());
    proto.set_object(request.object_name());
    proto.set_offset(range->first);
    proto.set_length(range->second);
    if (request.HasOption<gcs::Generation>()) {
      proto.set_generation(request.GetOption<gcs::Generation>().value());
    }
    if (request.HasOption<gcs::IfGenerationMatch>()) {
      proto.set_if_generation_match(
          request.GetOption<gcs::IfGenerationMatch>().value());
    }
    proto.set_transport(pb::JSON);
    proto.set_chunk_size(chunk_size_hint_);

    auto reader = std::make_unique<internal::ReaderImpl>(
        conn_, request.bucket_name(), request.object_name());
    if (Status s = reader->Start(proto); !s.ok()) return s;
    return std::unique_ptr<gcs_internal::ObjectReadSource>(
        std::make_unique<UdsObjectReadSource>(std::move(reader)));
  }

  StatusOr<gcs::ObjectMetadata> InsertObjectMedia(
      gcs_internal::InsertObjectMediaRequest const& request) override {
    if (Status s = CheckWriteOptions(request, "InsertObjectMedia"); !s.ok()) {
      return s;
    }
    StatusOr<std::string> content_type =
        ExtractContentType(request, "InsertObjectMedia");
    if (!content_type) return std::move(content_type).status();

    std::string_view slice(request.payload().data(), request.payload().size());
    if (request.HasOption<gcs::UploadFromOffset>()) {
      std::uint64_t const off =
          request.GetOption<gcs::UploadFromOffset>().value();
      if (off > slice.size()) {
        return Status(StatusCode::kInvalidArgument,
                      "UploadFromOffset exceeds payload size");
      }
      slice.remove_prefix(static_cast<std::size_t>(off));
    }
    if (request.HasOption<gcs::UploadLimit>()) {
      std::uint64_t const lim = request.GetOption<gcs::UploadLimit>().value();
      if (lim < slice.size()) {
        slice = slice.substr(0, static_cast<std::size_t>(lim));
      }
    }

    pb::WriteHeader header;
    header.set_bucket(request.bucket_name());
    header.set_object(request.object_name());
    header.set_content_type(*content_type);
    if (request.HasOption<gcs::IfGenerationMatch>()) {
      header.set_if_generation_match(
          request.GetOption<gcs::IfGenerationMatch>().value());
    }
    header.set_known_size(static_cast<std::int64_t>(slice.size()));
    header.set_transport(pb::JSON);

    internal::WriterImpl writer(conn_, std::move(header), chunk_size_hint_);
    if (!slice.empty()) {
      if (Status s = writer.Write(absl::Cord(slice)); !s.ok()) return s;
    }
    StatusOr<pb::WriteObjectResponse> resp = writer.Finish();
    if (!resp) return std::move(resp).status();
    return ToSdkObjectMetadata(*resp, request.bucket_name(),
                               request.object_name(), *content_type);
  }

  StatusOr<gcs_internal::CreateResumableUploadResponse> CreateResumableUpload(
      gcs_internal::ResumableUploadRequest const& request) override {
    if (request.HasOption<gcs::UseResumableUploadSession>() &&
        !request.GetOption<gcs::UseResumableUploadSession>().value().empty()) {
      return Status(
          StatusCode::kUnimplemented,
          "UseResumableUploadSession with a prior session ID is not supported "
          "over UDS; resumable state lives inside the active UDS stream");
    }
    if (Status s = CheckWriteOptions(request, "CreateResumableUpload");
        !s.ok()) {
      return s;
    }
    StatusOr<std::string> content_type =
        ExtractContentType(request, "CreateResumableUpload");
    if (!content_type) return std::move(content_type).status();

    std::size_t const buffer_size =
        request.HasOption<gcs::UploadBufferSize>()
            ? request.GetOption<gcs::UploadBufferSize>().value()
            : options_.get<gcs::UploadBufferSizeOption>();
    auto const clamped_buffer =
        static_cast<std::uint32_t>(std::min<std::size_t>(
            buffer_size, std::numeric_limits<std::uint32_t>::max()));

    pb::WriteHeader header;
    header.set_bucket(request.bucket_name());
    header.set_object(request.object_name());
    header.set_content_type(*content_type);
    if (request.HasOption<gcs::IfGenerationMatch>()) {
      header.set_if_generation_match(
          request.GetOption<gcs::IfGenerationMatch>().value());
    }
    // Leave `known_size` unset unless the caller declared `UploadContentLength`
    // so the Rust server streams a resumable upload with bounded memory.
    if (request.HasOption<gcs::UploadContentLength>()) {
      std::uint64_t const declared_len =
          request.GetOption<gcs::UploadContentLength>().value();
      if (declared_len > static_cast<std::uint64_t>(
                             std::numeric_limits<std::int64_t>::max())) {
        return Status(StatusCode::kInvalidArgument,
                      "CreateResumableUpload: UploadContentLength exceeds "
                      "INT64_MAX");
      }
      header.set_known_size(static_cast<std::int64_t>(declared_len));
    }
    header.set_chunk_size(clamped_buffer);
    header.set_transport(pb::JSON);

    auto const now = std::chrono::steady_clock::now();
    auto session = std::make_shared<UploadSession>();
    session->bucket = request.bucket_name();
    session->object = request.object_name();
    session->content_type = *content_type;
    session->writer = std::make_unique<internal::WriterImpl>(
        conn_, std::move(header), chunk_size_hint_);
    session->last_activity = now;

    std::string const id = absl::StrCat("uds-upload-", ++next_upload_id_);
    std::vector<std::shared_ptr<UploadSession>> evicted;
    bool at_capacity = false;
    {
      std::scoped_lock lk(uploads_mu_);
      for (auto it = uploads_.begin(); it != uploads_.end();) {
        if (now - it->second->last_activity > kAbandonedUploadIdleTtl) {
          evicted.push_back(std::move(it->second));
          it = uploads_.erase(it);
        } else {
          ++it;
        }
      }
      if (uploads_.size() >= kMaxActiveUploads) {
        at_capacity = true;
      } else {
        uploads_.emplace(id, std::move(session));
      }
    }
    evicted.clear();
    session.reset();
    if (at_capacity) {
      return Status(
          StatusCode::kResourceExhausted,
          "CreateResumableUpload: active upload session limit (1024) reached; "
          "finalize or DeleteResumableUpload abandoned streams");
    }
    return gcs_internal::CreateResumableUploadResponse{id};
  }

  StatusOr<gcs_internal::QueryResumableUploadResponse> QueryResumableUpload(
      gcs_internal::QueryResumableUploadRequest const& /*request*/) override {
    // `Client::WriteObject` routes `RestoreResumableUploadSession(id)` through
    // `CreateOrResume` -> `QueryResumableUpload`; cross-stream/cross-process
    // resume is not supported because the Rust server holds upload state inside
    // the single `WriteObject` client stream.
    return Status(
        StatusCode::kUnimplemented,
        "QueryResumableUpload / resuming a prior upload session is not "
        "supported over UDS");
  }

  StatusOr<gcs_internal::EmptyResponse> DeleteResumableUpload(
      gcs_internal::DeleteResumableUploadRequest const& request) override {
    std::shared_ptr<UploadSession> removed;
    {
      std::scoped_lock lk(uploads_mu_);
      auto it = uploads_.find(request.upload_session_url());
      if (it == uploads_.end()) {
        return Status(StatusCode::kNotFound, "unknown upload session");
      }
      removed = std::move(it->second);
      uploads_.erase(it);
    }
    // Destroying `removed->writer` outside `uploads_mu_` cancels the UDS stream
    // (`TryCancel()`), causing the server to abort the upload without
    // committing a partial object.
    removed.reset();
    return gcs_internal::EmptyResponse{};
  }

  StatusOr<gcs_internal::QueryResumableUploadResponse> UploadChunk(
      gcs_internal::UploadChunkRequest const& request) override {
    std::shared_ptr<UploadSession> session;
    {
      std::scoped_lock lk(uploads_mu_);
      auto it = uploads_.find(request.upload_session_url());
      if (it == uploads_.end()) {
        return Status(StatusCode::kNotFound, "unknown upload session");
      }
      it->second->last_activity = std::chrono::steady_clock::now();
      session = it->second;
    }

    std::scoped_lock slk(session->mu);
    if (request.offset() != session->bytes_accepted) {
      return Status(StatusCode::kInvalidArgument,
                    absl::StrCat("UploadChunk offset ", request.offset(),
                                 " does not match bytes accepted ",
                                 session->bytes_accepted));
    }

    absl::Cord data;
    for (auto const& buf : request.payload()) {
      if (!buf.empty()) {
        data.Append(std::string_view(buf.data(), buf.size()));
      }
    }
    if (!data.empty()) {
      if (Status s = session->writer->Write(data); !s.ok()) {
        EraseUploadSession(request.upload_session_url());
        return s;
      }
      session->bytes_accepted += data.size();
    }

    if (!request.last_chunk()) {
      return gcs_internal::QueryResumableUploadResponse{session->bytes_accepted,
                                                        std::nullopt};
    }

    StatusOr<pb::WriteObjectResponse> resp = session->writer->Finish();
    EraseUploadSession(request.upload_session_url());
    if (!resp) return std::move(resp).status();
    gcs::ObjectMetadata meta = ToSdkObjectMetadata(
        *resp, session->bucket, session->object, session->content_type);
    return gcs_internal::QueryResumableUploadResponse{
        static_cast<std::uint64_t>(resp->size()), std::move(meta)};
  }

  StatusOr<gcs_internal::ListObjectsResponse> ListObjects(
      gcs_internal::ListObjectsRequest const& request) override {
    if (request.HasOption<gcs::MatchGlob>()) {
      return UnimplementedOption("MatchGlob", "ListObjects");
    }
    if (request.HasOption<gcs::SoftDeleted>()) {
      return UnimplementedOption("SoftDeleted", "ListObjects");
    }
    if (request.HasOption<gcs::IncludeTrailingDelimiter>()) {
      return UnimplementedOption("IncludeTrailingDelimiter", "ListObjects");
    }
    if (request.HasOption<gcs::IncludeFoldersAsPrefixes>()) {
      return UnimplementedOption("IncludeFoldersAsPrefixes", "ListObjects");
    }

    pb::ListObjectsRequest proto;
    proto.set_bucket(request.bucket_name());
    proto.set_page_token(request.page_token());
    if (request.HasOption<gcs::Prefix>()) {
      proto.set_prefix(request.GetOption<gcs::Prefix>().value());
    }
    if (request.HasOption<gcs::Delimiter>()) {
      proto.set_delimiter(request.GetOption<gcs::Delimiter>().value());
    }
    if (request.HasOption<gcs::StartOffset>()) {
      proto.set_start_offset(request.GetOption<gcs::StartOffset>().value());
    }
    if (request.HasOption<gcs::EndOffset>()) {
      proto.set_end_offset(request.GetOption<gcs::EndOffset>().value());
    }
    if (request.HasOption<gcs::Versions>()) {
      proto.set_versions(request.GetOption<gcs::Versions>().value());
    }
    if (request.HasOption<gcs::MaxResults>()) {
      std::int64_t const max_results =
          request.GetOption<gcs::MaxResults>().value();
      if (max_results < 0) {
        return Status(StatusCode::kInvalidArgument,
                      "ListObjects: MaxResults must be >= 0");
      }
      proto.set_page_size(static_cast<std::int32_t>(std::min<std::int64_t>(
          max_results, std::numeric_limits<std::int32_t>::max())));
    }

    pb::ListObjectsResponse response;
    if (Status s =
            conn_->Unary(&pb::GcsCore::Stub::ListObjects, proto, response);
        !s.ok()) {
      return s;
    }

    gcs_internal::ListObjectsResponse out;
    out.next_page_token = response.next_page_token();
    out.prefixes.assign(response.prefixes().begin(), response.prefixes().end());
    out.items.reserve(static_cast<std::size_t>(response.items_size()));
    for (pb::ListedObject const& item : response.items()) {
      out.items.push_back(
          ToSdkListedObjectMetadata(item, request.bucket_name()));
    }
    return out;
  }

  StatusOr<gcs_internal::ListBucketsResponse> ListBuckets(
      gcs_internal::ListBucketsRequest const& request) override {
    if (request.project_id().empty()) {
      return Status(StatusCode::kInvalidArgument,
                    "ListBuckets requires a non-empty project ID");
    }
    pb::ListBucketsRequest proto;
    proto.set_project(request.project_id());
    proto.set_page_token(request.page_token());
    if (request.HasOption<gcs::Prefix>()) {
      proto.set_prefix(request.GetOption<gcs::Prefix>().value());
    }
    if (request.HasOption<gcs::MaxResults>()) {
      std::int64_t const max_results =
          request.GetOption<gcs::MaxResults>().value();
      if (max_results < 0) {
        return Status(StatusCode::kInvalidArgument,
                      "ListBuckets: MaxResults must be >= 0");
      }
      proto.set_page_size(static_cast<std::int32_t>(std::min<std::int64_t>(
          max_results, std::numeric_limits<std::int32_t>::max())));
    }

    pb::ListBucketsResponse response;
    if (Status s =
            conn_->Unary(&pb::GcsCore::Stub::ListBuckets, proto, response);
        !s.ok()) {
      return s;
    }

    gcs_internal::ListBucketsResponse out;
    out.next_page_token = response.next_page_token();
    out.items.reserve(static_cast<std::size_t>(response.items_size()));
    for (pb::ListedBucket const& item : response.items()) {
      out.items.push_back(ToSdkBucketMetadata(item));
    }
    return out;
  }

  StatusOr<gcs_internal::EmptyResponse> DeleteObject(
      gcs_internal::DeleteObjectRequest const& request) override {
    if (Status s = CheckCommonPreconditions(request, "DeleteObject"); !s.ok()) {
      return s;
    }
    pb::DeleteObjectRequest proto;
    proto.set_bucket(request.bucket_name());
    proto.set_object(request.object_name());
    if (request.HasOption<gcs::Generation>()) {
      std::int64_t const gen = request.GetOption<gcs::Generation>().value();
      if (gen < 0) {
        return Status(StatusCode::kInvalidArgument,
                      "DeleteObject: Generation must be >= 0");
      }
      proto.set_generation(gen);
    }
    if (request.HasOption<gcs::IfGenerationMatch>()) {
      proto.set_if_generation_match(
          request.GetOption<gcs::IfGenerationMatch>().value());
    }
    pb::Empty response;
    if (Status s =
            conn_->Unary(&pb::GcsCore::Stub::DeleteObject, proto, response);
        !s.ok()) {
      return s;
    }
    return gcs_internal::EmptyResponse{};
  }

  // --- Unimplemented operations (table-driven test coverage in A15) ----------
  StatusOr<gcs::BucketMetadata> CreateBucket(
      gcs_internal::CreateBucketRequest const&) override {
    return UnimplementedMethod("CreateBucket");
  }
  StatusOr<gcs::BucketMetadata> GetBucketMetadata(
      gcs_internal::GetBucketMetadataRequest const&) override {
    return UnimplementedMethod("GetBucketMetadata");
  }
  StatusOr<gcs_internal::EmptyResponse> DeleteBucket(
      gcs_internal::DeleteBucketRequest const&) override {
    return UnimplementedMethod("DeleteBucket");
  }
  StatusOr<gcs::BucketMetadata> UpdateBucket(
      gcs_internal::UpdateBucketRequest const&) override {
    return UnimplementedMethod("UpdateBucket");
  }
  StatusOr<gcs::BucketMetadata> PatchBucket(
      gcs_internal::PatchBucketRequest const&) override {
    return UnimplementedMethod("PatchBucket");
  }
  StatusOr<gcs::NativeIamPolicy> GetNativeBucketIamPolicy(
      gcs_internal::GetBucketIamPolicyRequest const&) override {
    return UnimplementedMethod("GetNativeBucketIamPolicy");
  }
  StatusOr<gcs::NativeIamPolicy> SetNativeBucketIamPolicy(
      gcs_internal::SetNativeBucketIamPolicyRequest const&) override {
    return UnimplementedMethod("SetNativeBucketIamPolicy");
  }
  StatusOr<gcs_internal::TestBucketIamPermissionsResponse>
  TestBucketIamPermissions(
      gcs_internal::TestBucketIamPermissionsRequest const&) override {
    return UnimplementedMethod("TestBucketIamPermissions");
  }
  StatusOr<gcs::BucketMetadata> LockBucketRetentionPolicy(
      gcs_internal::LockBucketRetentionPolicyRequest const&) override {
    return UnimplementedMethod("LockBucketRetentionPolicy");
  }

  StatusOr<gcs::ObjectMetadata> CopyObject(
      gcs_internal::CopyObjectRequest const&) override {
    return UnimplementedMethod("CopyObject");
  }
  StatusOr<gcs::ObjectMetadata> GetObjectMetadata(
      gcs_internal::GetObjectMetadataRequest const&) override {
    return UnimplementedMethod("GetObjectMetadata");
  }
  StatusOr<gcs::ObjectMetadata> UpdateObject(
      gcs_internal::UpdateObjectRequest const&) override {
    return UnimplementedMethod("UpdateObject");
  }
  StatusOr<gcs::ObjectMetadata> MoveObject(
      gcs_internal::MoveObjectRequest const&) override {
    return UnimplementedMethod("MoveObject");
  }
  StatusOr<gcs::ObjectMetadata> PatchObject(
      gcs_internal::PatchObjectRequest const&) override {
    return UnimplementedMethod("PatchObject");
  }
  StatusOr<gcs::ObjectMetadata> ComposeObject(
      gcs_internal::ComposeObjectRequest const&) override {
    return UnimplementedMethod("ComposeObject");
  }
  StatusOr<gcs_internal::RewriteObjectResponse> RewriteObject(
      gcs_internal::RewriteObjectRequest const&) override {
    return UnimplementedMethod("RewriteObject");
  }
  StatusOr<gcs::ObjectMetadata> RestoreObject(
      gcs_internal::RestoreObjectRequest const&) override {
    return UnimplementedMethod("RestoreObject");
  }
  StatusOr<std::unique_ptr<std::string>> UploadFileSimple(
      std::string const&, std::size_t,
      gcs_internal::InsertObjectMediaRequest&) override {
    return UnimplementedMethod("UploadFileSimple");
  }
  StatusOr<std::unique_ptr<std::istream>> UploadFileResumable(
      std::string const&, gcs_internal::ResumableUploadRequest&) override {
    return UnimplementedMethod("UploadFileResumable");
  }
  Status DownloadStreamToFile(
      gcs::ObjectReadStream&&, std::string const&,
      gcs_internal::ReadObjectRangeRequest const&) override {
    return UnimplementedMethod("DownloadStreamToFile");
  }
  StatusOr<gcs::ObjectMetadata> ExecuteParallelUploadFile(
      std::vector<std::thread>,
      std::vector<gcs_internal::ParallelUploadFileShard>, bool) override {
    return UnimplementedMethod("ExecuteParallelUploadFile");
  }

  StatusOr<gcs_internal::ListBucketAclResponse> ListBucketAcl(
      gcs_internal::ListBucketAclRequest const&) override {
    return UnimplementedMethod("ListBucketAcl");
  }
  StatusOr<gcs::BucketAccessControl> CreateBucketAcl(
      gcs_internal::CreateBucketAclRequest const&) override {
    return UnimplementedMethod("CreateBucketAcl");
  }
  StatusOr<gcs_internal::EmptyResponse> DeleteBucketAcl(
      gcs_internal::DeleteBucketAclRequest const&) override {
    return UnimplementedMethod("DeleteBucketAcl");
  }
  StatusOr<gcs::BucketAccessControl> GetBucketAcl(
      gcs_internal::GetBucketAclRequest const&) override {
    return UnimplementedMethod("GetBucketAcl");
  }
  StatusOr<gcs::BucketAccessControl> UpdateBucketAcl(
      gcs_internal::UpdateBucketAclRequest const&) override {
    return UnimplementedMethod("UpdateBucketAcl");
  }
  StatusOr<gcs::BucketAccessControl> PatchBucketAcl(
      gcs_internal::PatchBucketAclRequest const&) override {
    return UnimplementedMethod("PatchBucketAcl");
  }

  StatusOr<gcs_internal::ListObjectAclResponse> ListObjectAcl(
      gcs_internal::ListObjectAclRequest const&) override {
    return UnimplementedMethod("ListObjectAcl");
  }
  StatusOr<gcs::ObjectAccessControl> CreateObjectAcl(
      gcs_internal::CreateObjectAclRequest const&) override {
    return UnimplementedMethod("CreateObjectAcl");
  }
  StatusOr<gcs_internal::EmptyResponse> DeleteObjectAcl(
      gcs_internal::DeleteObjectAclRequest const&) override {
    return UnimplementedMethod("DeleteObjectAcl");
  }
  StatusOr<gcs::ObjectAccessControl> GetObjectAcl(
      gcs_internal::GetObjectAclRequest const&) override {
    return UnimplementedMethod("GetObjectAcl");
  }
  StatusOr<gcs::ObjectAccessControl> UpdateObjectAcl(
      gcs_internal::UpdateObjectAclRequest const&) override {
    return UnimplementedMethod("UpdateObjectAcl");
  }
  StatusOr<gcs::ObjectAccessControl> PatchObjectAcl(
      gcs_internal::PatchObjectAclRequest const&) override {
    return UnimplementedMethod("PatchObjectAcl");
  }

  StatusOr<gcs_internal::ListDefaultObjectAclResponse> ListDefaultObjectAcl(
      gcs_internal::ListDefaultObjectAclRequest const&) override {
    return UnimplementedMethod("ListDefaultObjectAcl");
  }
  StatusOr<gcs::ObjectAccessControl> CreateDefaultObjectAcl(
      gcs_internal::CreateDefaultObjectAclRequest const&) override {
    return UnimplementedMethod("CreateDefaultObjectAcl");
  }
  StatusOr<gcs_internal::EmptyResponse> DeleteDefaultObjectAcl(
      gcs_internal::DeleteDefaultObjectAclRequest const&) override {
    return UnimplementedMethod("DeleteDefaultObjectAcl");
  }
  StatusOr<gcs::ObjectAccessControl> GetDefaultObjectAcl(
      gcs_internal::GetDefaultObjectAclRequest const&) override {
    return UnimplementedMethod("GetDefaultObjectAcl");
  }
  StatusOr<gcs::ObjectAccessControl> UpdateDefaultObjectAcl(
      gcs_internal::UpdateDefaultObjectAclRequest const&) override {
    return UnimplementedMethod("UpdateDefaultObjectAcl");
  }
  StatusOr<gcs::ObjectAccessControl> PatchDefaultObjectAcl(
      gcs_internal::PatchDefaultObjectAclRequest const&) override {
    return UnimplementedMethod("PatchDefaultObjectAcl");
  }

  StatusOr<gcs::ServiceAccount> GetServiceAccount(
      gcs_internal::GetProjectServiceAccountRequest const&) override {
    return UnimplementedMethod("GetServiceAccount");
  }
  StatusOr<gcs_internal::ListHmacKeysResponse> ListHmacKeys(
      gcs_internal::ListHmacKeysRequest const&) override {
    return UnimplementedMethod("ListHmacKeys");
  }
  StatusOr<gcs_internal::CreateHmacKeyResponse> CreateHmacKey(
      gcs_internal::CreateHmacKeyRequest const&) override {
    return UnimplementedMethod("CreateHmacKey");
  }
  StatusOr<gcs_internal::EmptyResponse> DeleteHmacKey(
      gcs_internal::DeleteHmacKeyRequest const&) override {
    return UnimplementedMethod("DeleteHmacKey");
  }
  StatusOr<gcs::HmacKeyMetadata> GetHmacKey(
      gcs_internal::GetHmacKeyRequest const&) override {
    return UnimplementedMethod("GetHmacKey");
  }
  StatusOr<gcs::HmacKeyMetadata> UpdateHmacKey(
      gcs_internal::UpdateHmacKeyRequest const&) override {
    return UnimplementedMethod("UpdateHmacKey");
  }
  StatusOr<gcs_internal::SignBlobResponse> SignBlob(
      gcs_internal::SignBlobRequest const&) override {
    return UnimplementedMethod("SignBlob");
  }

  StatusOr<gcs_internal::ListNotificationsResponse> ListNotifications(
      gcs_internal::ListNotificationsRequest const&) override {
    return UnimplementedMethod("ListNotifications");
  }
  StatusOr<gcs::NotificationMetadata> CreateNotification(
      gcs_internal::CreateNotificationRequest const&) override {
    return UnimplementedMethod("CreateNotification");
  }
  StatusOr<gcs::NotificationMetadata> GetNotification(
      gcs_internal::GetNotificationRequest const&) override {
    return UnimplementedMethod("GetNotification");
  }
  StatusOr<gcs_internal::EmptyResponse> DeleteNotification(
      gcs_internal::DeleteNotificationRequest const&) override {
    return UnimplementedMethod("DeleteNotification");
  }

  std::vector<std::string> InspectStackStructure() const override {
    return {"UdsStorageConnection"};
  }

 private:
  struct UploadSession {
    std::mutex mu;
    std::string bucket;
    std::string object;
    std::string content_type;
    std::unique_ptr<internal::WriterImpl> writer;
    std::uint64_t bytes_accepted = 0;
    std::chrono::steady_clock::time_point last_activity;
  };

  void EraseUploadSession(std::string const& id) {
    std::shared_ptr<UploadSession> removed;
    {
      std::scoped_lock lk(uploads_mu_);
      auto it = uploads_.find(id);
      if (it != uploads_.end()) {
        removed = std::move(it->second);
        uploads_.erase(it);
      }
    }
    removed.reset();
  }

  std::shared_ptr<internal::Connection> conn_;
  std::uint32_t chunk_size_hint_;
  Options options_;
  std::atomic<std::uint64_t> next_upload_id_{0};
  std::mutex uploads_mu_;
  std::unordered_map<std::string, std::shared_ptr<UploadSession>> uploads_;
};

}  // namespace

ClientOptions DefaultAdapterClientOptions() {
  ClientOptions opts;
  opts.channels = 4;
  return opts;
}

StatusOr<std::shared_ptr<gcs_internal::StorageConnection>>
MakeUdsStorageConnection(ClientOptions const& core_options, Options options) {
  StatusOr<Options> validated =
      ValidateAndDefaultStorageOptions(std::move(options));
  if (!validated) return std::move(validated).status();
  StatusOr<std::shared_ptr<internal::Connection>> conn =
      internal::Connection::GetOrCreate(core_options);
  if (!conn) return std::move(conn).status();
  return std::shared_ptr<gcs_internal::StorageConnection>(
      std::make_shared<UdsStorageConnection>(*std::move(conn),
                                             core_options.chunk_size_hint,
                                             *std::move(validated)));
}

StatusOr<gcs::Client> MakeUdsStorageClient(ClientOptions const& core_options,
                                           Options options) {
  StatusOr<std::shared_ptr<gcs_internal::StorageConnection>> conn =
      MakeUdsStorageConnection(core_options, std::move(options));
  if (!conn) return std::move(conn).status();
  return gcs_internal::ClientImplDetails::CreateWithoutDecorations(
      *std::move(conn));
}

}  // namespace gcs_core
