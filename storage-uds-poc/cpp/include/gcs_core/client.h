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

#ifndef GCS_UDS_POC_CPP_INCLUDE_GCS_CORE_CLIENT_H
#define GCS_UDS_POC_CPP_INCLUDE_GCS_CORE_CLIENT_H

#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/strings/cord.h"
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

/**
 * Thin C++ wrapper over the Rust `core-sdk` (POC).
 *
 * The wrapper contains no storage logic: every call is forwarded over gRPC to
 * the core, which runs the Rust `google-cloud-storage` crate. Retries,
 * resumable-upload state, checksum validation and credentials (Application
 * Default Credentials) all live in the core. Statuses are returned exactly as
 * the core reports them.
 *
 * The same header serves two libraries that differ only in where the core
 * runs: `gcs_core` starts it as the `gcs-core-sidecar` process and talks to
 * it over a Unix domain socket; `gcs_core_inproc` links it in and runs it on
 * threads of this process. Fields and functions that only make sense for one
 * of them say so.
 */
namespace gcs_core {

using ::google::cloud::Status;
using ::google::cloud::StatusOr;

class Client;

namespace internal {
class Connection;
class ReaderImpl;
class WriterImpl;
class AppendableWriterImpl;
/// Test hook: terminates the core serving `client` without draining, as a
/// crash would; the next failing call restarts it.
void KillCoreForTesting(Client const& client);
}  // namespace internal

struct ObjectMetadata {
  std::string bucket;
  std::string name;
  std::int64_t generation = 0;
  std::int64_t metageneration = 0;
  std::uint64_t size = 0;
  std::string content_type;
  std::optional<std::uint32_t> crc32c;
  std::string md5_hash;
  std::string etag;
  std::string storage_class;
};

/// `limit == 0` reads to the end. A negative `offset` reads the last
/// `-offset` bytes and requires `limit == 0`.
struct ReadRange {
  std::int64_t offset = 0;
  std::int64_t limit = 0;
};

enum class UploadMode { kAuto, kSingleShot, kResumable };

struct WriteOptions {
  /// Total size if known. Required for `UploadMode::kSingleShot`.
  std::optional<std::uint64_t> size_hint;
  std::string content_type;
  std::optional<std::int64_t> if_generation_match;
  UploadMode mode = UploadMode::kAuto;
};

struct ListObjectsRequest {
  std::string prefix;
  std::string delimiter;
  std::int32_t page_size = 0;
  std::string page_token;
};

struct ListObjectsPage {
  std::vector<ObjectMetadata> objects;
  std::vector<std::string> prefixes;
  std::string next_page_token;
};

struct SidecarVersion {
  std::string sidecar_version;
  std::string storage_crate_version;
  /// Pid of the process serving the core: the sidecar, or this process.
  std::uint32_t pid = 0;
  bool bidi_enabled = false;
  bool synthetic = false;
};

/// Cumulative counters of the process serving the core; diff two snapshots.
/// In-process they describe this process itself and `allocated_bytes` is 0
/// (the core's allocations go through the application's own `malloc`).
struct SidecarStats {
  std::uint64_t user_cpu_us = 0;
  std::uint64_t system_cpu_us = 0;
  std::uint64_t rss_bytes = 0;
  std::uint64_t peak_rss_bytes = 0;
  std::uint64_t allocated_bytes = 0;
  std::uint32_t threads = 0;
};

struct ClientOptions {
  /// Sidecar binary. Empty: `$GCS_CORE_SIDECAR`, then `gcs-core-sidecar`
  /// next to the running executable, then `gcs-core-sidecar` on `PATH`.
  /// Sidecar variant only; the in-process variant rejects a non-empty value.
  std::string sidecar_path;
  /// Socket path. Empty: `$XDG_RUNTIME_DIR/gcs-core-<pid>.sock`, else
  /// `/tmp/gcs-core-<uid>/<pid>.sock`. Sidecar variant only; the in-process
  /// variant rejects a non-empty value.
  std::string socket_path;
  /// Extra core arguments, e.g. `--synthetic` or `--chunk-size=N`. Both
  /// variants accept the same core flags; the in-process variant adds
  /// `--link=socketpair|abstract` and rejects `--socket`/`--parent-fd`.
  std::vector<std::string> sidecar_args;
  /// Number of independent HTTP/2 connections to the core.
  int channels = 1;
  /// Upper bound for read and write chunk messages (clamped to 16 MiB); 0 lets
  /// the sidecar choose on reads and uses 1 MiB on writes.
  std::uint32_t chunk_size_hint = 0;
  /// Must be > 0. Shared connections retain the first caller's timeout for
  /// subsequent crash restarts.
  std::chrono::milliseconds readiness_timeout{5000};
};

/// Streams one object. Obtained from `Client::ReadObject`.
class ObjectReader {
 public:
  ObjectReader(ObjectReader&&) noexcept;
  ObjectReader& operator=(ObjectReader&&) noexcept;
  ~ObjectReader();

  ObjectMetadata const& metadata() const;
  /// Next chunk of data; an empty optional at the end of the object.
  StatusOr<std::optional<absl::Cord>> Next();

 private:
  friend class Client;
  explicit ObjectReader(std::unique_ptr<internal::ReaderImpl> impl);
  std::unique_ptr<internal::ReaderImpl> impl_;
};

/// Uploads one object. Obtained from `Client::WriteObject`; `Close()`
/// finalizes it. Destroying an unclosed writer cancels the upload.
class ObjectWriter {
 public:
  ObjectWriter(ObjectWriter&&) noexcept;
  ObjectWriter& operator=(ObjectWriter&&) noexcept;
  ~ObjectWriter();

  Status Write(absl::Cord const& data);
  StatusOr<ObjectMetadata> Close();

 private:
  friend class Client;
  explicit ObjectWriter(std::unique_ptr<internal::WriterImpl> impl);
  std::unique_ptr<internal::WriterImpl> impl_;
};

/// Appendable (zonal bucket) upload over the core's BidiWriteObject path.
class AppendableWriter {
 public:
  AppendableWriter(AppendableWriter&&) noexcept;
  AppendableWriter& operator=(AppendableWriter&&) noexcept;
  ~AppendableWriter();

  Status Write(absl::Cord const& data);
  /// Finalizes the object.
  StatusOr<ObjectMetadata> Finalize();
  /// Leaves the object appendable; returns the persisted size.
  StatusOr<std::int64_t> Close();

 private:
  friend class Client;
  explicit AppendableWriter(
      std::unique_ptr<internal::AppendableWriterImpl> impl);
  std::unique_ptr<internal::AppendableWriterImpl> impl_;
};

/// An open object for concurrent ranged reads over the core's
/// BidiReadObject path. Closes the descriptor on destruction.
class ObjectDescriptor {
 public:
  ObjectDescriptor(ObjectDescriptor&&) noexcept;
  ObjectDescriptor& operator=(ObjectDescriptor&&) noexcept;
  ~ObjectDescriptor();

  ObjectMetadata const& metadata() const;
  /// Reads all `ranges` concurrently. `on_data(index, chunk)` is called on
  /// the calling thread, possibly interleaving ranges, in order within each
  /// range. Safe to call from several threads at once.
  Status ReadRanges(
      std::vector<ReadRange> const& ranges,
      std::function<void(std::size_t, absl::Cord const&)> const& on_data);

 private:
  friend class Client;
  ObjectDescriptor(std::shared_ptr<internal::Connection> conn,
                   std::uint64_t handle, ObjectMetadata metadata,
                   std::uint32_t chunk_size_hint);
  std::shared_ptr<internal::Connection> conn_;
  std::uint64_t handle_ = 0;
  ObjectMetadata metadata_;
  std::uint32_t chunk_size_hint_ = 0;
};

class Client {
 public:
  /// Starts (or reuses) a core matching `(sidecar_path, socket_path,
  /// sidecar_args, channels)` and waits until it serves.
  static StatusOr<Client> Create(ClientOptions const& options = {});

  StatusOr<ObjectReader> ReadObject(std::string const& bucket,
                                    std::string const& object,
                                    ReadRange range = {},
                                    std::int64_t generation = 0);
  StatusOr<ObjectWriter> WriteObject(std::string const& bucket,
                                     std::string const& object,
                                     WriteOptions const& options = {});
  /// Uploads `data` with one call; sets `size_hint` from `data`.
  StatusOr<ObjectMetadata> InsertObject(std::string const& bucket,
                                        std::string const& object,
                                        absl::Cord const& data,
                                        WriteOptions options = {});
  StatusOr<ListObjectsPage> ListObjects(std::string const& bucket,
                                        ListObjectsRequest const& request);
  Status DeleteObject(std::string const& bucket, std::string const& object,
                      std::int64_t generation = 0);
  StatusOr<ObjectDescriptor> OpenObject(std::string const& bucket,
                                        std::string const& object,
                                        std::int64_t generation = 0);
  StatusOr<AppendableWriter> StartAppendableUpload(
      std::string const& bucket, std::string const& object,
      WriteOptions const& options = {});

  StatusOr<SidecarVersion> GetSidecarVersion();
  StatusOr<SidecarStats> GetSidecarStats();
  /// Pid of the process serving this client's core (this process when the
  /// core runs in-process).
  int sidecar_pid() const;
  /// CPU time (user + system, microseconds) consumed so far by the core's
  /// threads when they run in this process; `nullopt` for a sidecar, whose
  /// CPU is in `GetSidecarStats()`. Carried across crash restarts.
  std::optional<std::uint64_t> core_thread_cpu_us() const;

 private:
  friend void internal::KillCoreForTesting(Client const& client);
  Client(std::shared_ptr<internal::Connection> conn,
         std::uint32_t chunk_size_hint);
  std::shared_ptr<internal::Connection> conn_;
  std::uint32_t chunk_size_hint_ = 0;
};

}  // namespace gcs_core

#endif  // GCS_UDS_POC_CPP_INCLUDE_GCS_CORE_CLIENT_H
