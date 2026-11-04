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
 * Thin C++ wrapper over the shared Rust gRPC-over-UDS server (POC).
 *
 * The wrapper contains no storage logic: every call is forwarded over gRPC
 * (`gcscore.v1.GcsCore`) to `storage-grpc-uds`, which runs the Rust
 * `google-cloud-storage` crate. Retries, resumable-upload state, checksum
 * validation and credentials (Application Default Credentials) all live in
 * the server. Statuses are returned exactly as the server reports them.
 *
 * The same header serves two libraries that differ only in where the server
 * runs: `gcs_core` starts `gcs-core-server` as a child process and talks to
 * it over a Unix domain socket; `gcs_core_inproc` links the server's static
 * library and runs it on threads of this process, reached over an
 * abstract-namespace Unix socket. Fields and functions that only make sense
 * for one of them say so.
 */
namespace gcs_core {

using ::google::cloud::Status;
using ::google::cloud::StatusOr;

class Client;

namespace internal {
class Connection;
class ReaderImpl;
class WriterImpl;
/// Test hook: terminates the server serving `client` without draining, as a
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
  std::string content_encoding;
  std::optional<std::uint32_t> crc32c;
  /// Only set by `ListObjects`; other calls leave it empty.
  std::string storage_class;
  /// Only set by `ListObjects` (second resolution).
  std::chrono::system_clock::time_point updated;
  /// True when `AppendableWriter::Finalize(true)` completed over JSON fallback.
  bool used_json_fallback = false;
};

struct BucketMetadata {
  std::string name;
  std::string location;
  std::string storage_class;
  std::int64_t metageneration = 0;
  /// Second resolution.
  std::chrono::system_clock::time_point created;
};

/// `limit == 0` reads to the end. A negative `offset` reads the last
/// `-offset` bytes and requires `limit == 0`.
struct ReadRange {
  std::int64_t offset = 0;
  std::int64_t limit = 0;
};

/// How the server uploads. `kAuto` and `kSingleShot` pass `size_hint` to the
/// server (single request below the crate's 16 MiB resumable threshold);
/// `kResumable` withholds it, so the server streams a resumable upload.
enum class UploadMode { kAuto, kSingleShot, kResumable };

struct WriteOptions {
  /// Total size if known. Required for `UploadMode::kSingleShot`.
  std::optional<std::uint64_t> size_hint;
  std::string content_type;
  std::optional<std::int64_t> if_generation_match;
  /// Resumable upload flush buffer size on the server
  /// (`WriteHeader.chunk_size`); 0 leaves the crate's 8 MiB default.
  std::uint32_t resumable_buffer_size = 0;
  UploadMode mode = UploadMode::kAuto;
};

struct ListObjectsRequest {
  std::string prefix;
  std::string delimiter;
  std::string start_offset;
  std::string end_offset;
  bool versions = false;
  std::int32_t page_size = 0;
  std::string page_token;
};

struct ListObjectsPage {
  std::vector<ObjectMetadata> objects;
  std::vector<std::string> prefixes;
  std::string next_page_token;
};

struct ListBucketsRequest {
  std::string prefix;
  std::int32_t page_size = 0;
  std::string page_token;
};

struct ListBucketsPage {
  std::vector<BucketMetadata> buckets;
  std::string next_page_token;
};

struct SidecarVersion {
  /// Pid of the process serving the core: the sidecar, or this process.
  std::uint32_t pid = 0;
};

/// Cumulative counters of the process serving the core; diff two snapshots.
/// In-process they describe this process itself. `allocated_bytes` is always
/// 0: the shared server does not count allocations.
struct SidecarStats {
  std::uint64_t user_cpu_us = 0;
  std::uint64_t system_cpu_us = 0;
  std::uint64_t rss_bytes = 0;
  std::uint64_t peak_rss_bytes = 0;
  std::uint64_t allocated_bytes = 0;
  std::uint32_t threads = 0;
};

/// Where the shared Rust server runs when using a combined multi-backend build
/// (`gcs_core_combined` / `gcs_core_storage_combined`). Single-backend
/// libraries (`gcs_core` and `gcs_core_inproc`) default to their own backend
/// when `ClientOptions::hosting` is unset and reject the other value with
/// `kInvalidArgument`.
enum class HostingMode {
  /// Separate OS process (`gcs-core-server`) over a filesystem `unix:` socket.
  kSidecar,
  /// Tokio runtime threads inside this process (`libstorage_grpc_uds.a`) over
  /// an abstract-namespace `unix-abstract:` socket.
  kInProcess,
};

/// How `gcs_core` (the multiprocess variant) runs `gcs-core-server`.
enum class SidecarMode {
  /// Shared per-UID daemon (`--idle-secs=300` unless `--idle-secs` is in
  /// `sidecar_args`, `--pdeathsig=false`): connects to an already-running
  /// server on the socket if one is listening, otherwise spawns one and leaves
  /// it running when this process's last `Client` is destroyed.
  kDaemon,
  /// 1:1 child owned by this process (`--pdeathsig=true`, `--idle-secs=0`):
  /// spawned on a per-process socket and terminated when the last `Client` is
  /// destroyed (or by the kernel when this process exits).
  kChild,
};

struct ClientOptions {
  /// Selects the hosting backend when linked against a combined library;
  /// `std::nullopt` uses the library's default (`kSidecar` in `gcs_core` and
  /// `gcs_core_combined`, `kInProcess` in `gcs_core_inproc`).
  std::optional<HostingMode> hosting;
  /// Multiprocess lifecycle mode. Defaults to the shared per-UID daemon; the
  /// in-process variant rejects `SidecarMode::kChild`.
  SidecarMode sidecar_mode = SidecarMode::kDaemon;
  /// Server binary. Empty: `$GCS_CORE_SERVER`, then `gcs-core-server` next to
  /// the running executable, then `gcs-core-server` on `PATH`. Sidecar
  /// variant only; the in-process variant rejects a non-empty value.
  std::string sidecar_path;
  /// Socket path. Empty in `kDaemon` mode: `$XDG_RUNTIME_DIR/gcs-core.sock`,
  /// else `/tmp/gcs-core-<uid>/gcs-core.sock`. Empty in `kChild` mode:
  /// `$XDG_RUNTIME_DIR/gcs-core-<pid>.sock`, else
  /// `/tmp/gcs-core-<uid>/<pid>.sock`. Sidecar variant only; the in-process
  /// variant rejects a non-empty value (it binds an abstract socket).
  std::string socket_path;
  /// Extra server flags as for `gcs-core-server`, e.g.
  /// `--grpc-subchannels=4` or `--worker-threads=8`. Both variants accept the
  /// same flags; `--socket`, `--pdeathsig` and `--tcp` are set by the wrapper
  /// or rejected.
  std::vector<std::string> sidecar_args;
  /// Number of independent HTTP/2 connections to the core.
  int channels = 1;
  /// Upper bound for read and write chunk messages (clamped to 16 MiB); 0 lets
  /// the server choose on reads and uses 1 MiB on writes.
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

/// Appendable (zonal bucket) upload over the server's BidiWriteObject path.
class AppendableWriter {
 public:
  AppendableWriter(AppendableWriter&&) noexcept;
  AppendableWriter& operator=(AppendableWriter&&) noexcept;
  ~AppendableWriter();

  Status Write(absl::Cord const& data);
  /// Finalizes the object. When @p allow_json_fallback is false, fails with
  /// `kFailedPrecondition` if the server wrote it over JSON instead of
  /// `BidiWriteObject` (it does so when the bucket rejects appendable objects
  /// or when built without the unstable bidi cfg; the regular object already
  /// exists by then). When @p allow_json_fallback is true, returns the metadata
  /// with `used_json_fallback = true`.
  StatusOr<ObjectMetadata> Finalize(bool allow_json_fallback = false);
  /// Would leave the object appendable. Always `kUnimplemented`: the shared
  /// server's `WriteObject` finalizes when the stream ends.
  StatusOr<std::int64_t> Close();

 private:
  friend class Client;
  explicit AppendableWriter(std::unique_ptr<internal::WriterImpl> impl);
  std::unique_ptr<internal::WriterImpl> impl_;
};

/// An open object for concurrent ranged reads over the server's
/// BidiReadObject path. Closes the descriptor on destruction.
class ObjectDescriptor {
 public:
  ObjectDescriptor(ObjectDescriptor&&) noexcept;
  ObjectDescriptor& operator=(ObjectDescriptor&&) noexcept;
  ~ObjectDescriptor();

  ObjectMetadata const& metadata() const;
  /// Reads all `ranges` concurrently (one `ReadRange` RPC each).
  /// `on_data(index, chunk)` is called on the calling thread, possibly
  /// interleaving ranges, in order within each range. Safe to call from
  /// several threads at once.
  Status ReadRanges(
      std::vector<ReadRange> const& ranges,
      std::function<void(std::size_t, absl::Cord const&)> const& on_data);

 private:
  friend class Client;
  ObjectDescriptor(std::shared_ptr<internal::Connection> conn,
                   std::uint64_t generation, std::uint64_t handle,
                   ObjectMetadata metadata, std::uint32_t chunk_size_hint);
  std::shared_ptr<internal::Connection> conn_;
  /// The server generation that issued `handle_`; see `Connection::StubFor`.
  std::uint64_t generation_ = 0;
  std::uint64_t handle_ = 0;
  ObjectMetadata metadata_;
  std::uint32_t chunk_size_hint_ = 0;
};

class Client {
 public:
  /// Starts (or reuses) a core matching `(sidecar_mode, sidecar_path,
  /// socket_path, sidecar_args, channels)` and waits until it serves.
  static StatusOr<Client> Create(ClientOptions const& options = {});

  StatusOr<ObjectReader> ReadObject(
      std::string const& bucket, std::string const& object,
      ReadRange range = {}, std::int64_t generation = 0,
      std::optional<std::int64_t> if_generation_match = std::nullopt);
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
  /// `project` is a project id or number, with or without `projects/`.
  StatusOr<ListBucketsPage> ListBuckets(std::string const& project,
                                        ListBucketsRequest const& request);
  Status DeleteObject(
      std::string const& bucket, std::string const& object,
      std::int64_t generation = 0,
      std::optional<std::int64_t> if_generation_match = std::nullopt);
  StatusOr<ObjectDescriptor> OpenObject(std::string const& bucket,
                                        std::string const& object,
                                        std::int64_t generation = 0);
  StatusOr<AppendableWriter> StartAppendableUpload(
      std::string const& bucket, std::string const& object,
      WriteOptions const& options = {});

  StatusOr<SidecarVersion> GetSidecarVersion();
  StatusOr<SidecarStats> GetSidecarStats(bool reset_peak_rss = false);
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
