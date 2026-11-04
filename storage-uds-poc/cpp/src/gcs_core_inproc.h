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

#ifndef GCS_UDS_POC_CPP_SRC_GCS_CORE_INPROC_H
#define GCS_UDS_POC_CPP_SRC_GCS_CORE_INPROC_H

#include <stddef.h>
#include <stdint.h>

/**
 * C ABI of `libgcs_core.a` (Rust, `sidecar/src/inproc.rs`): the core hosted
 * on tokio threads inside this process. Hand-written to match the `#[repr(C)]`
 * and `extern "C"` declarations there; keep the two in sync.
 *
 * Lifecycle: `gcs_core_inproc_start` → (take the channel descriptors or the
 * abstract socket name, create gRPC channels) → RPCs → `gcs_core_inproc_stop`.
 * The library never calls `exit()`, never changes signal dispositions, and
 * catches panics at every entry point.
 */
#ifdef __cplusplus
extern "C" {
#endif

// A C header: `typedef` keeps it valid for C callers as well as C++.
// NOLINTBEGIN(modernize-use-using)

/// Opaque handle to one hosted core.
typedef struct GcsCoreInproc GcsCoreInproc;

/// Callbacks run on every core thread right after it starts and right before
/// it exits, with `data` passed back unchanged. Either callback may be null.
/// Must stay valid until `gcs_core_inproc_stop` has returned.
typedef struct GcsCoreThreadHooks {
  void (*on_start)(void* data);
  void (*on_stop)(void* data);
  void* data;
} GcsCoreThreadHooks;

// NOLINTEND(modernize-use-using)

/// `argv[0..argc)`: core arguments as for `gcs-core-sidecar` minus the
/// process-only flags (`--socket`, `--parent-fd`); plus `--link=socketpair`
/// (default) or `--link=abstract`. `channels`: connections the host will
/// open (>= 1). `hooks` may be null. Returns null on failure; see
/// `gcs_core_inproc_last_error`. Blocks until the core serves (credential
/// lookup and client construction happen on the calling thread).
GcsCoreInproc* gcs_core_inproc_start(char const* const* argv, size_t argc,
                                     size_t channels,
                                     GcsCoreThreadHooks const* hooks);

/// Why the last `gcs_core_inproc_start` on this thread failed. Copies the
/// NUL-terminated message into `buf` (truncated to `cap - 1`) and returns the
/// full message length.
size_t gcs_core_inproc_last_error(char* buf, size_t cap);
/// 1: invalid arguments (bad or process-only flags); 2: the core could not be
/// started (runtime, credentials, clients, sockets).
int gcs_core_inproc_last_error_kind(void);

/// 0 for `socketpair`, 1 for `abstract`.
int gcs_core_inproc_link_kind(GcsCoreInproc const* core);
/// Transfers ownership of channel `index`'s client socket to the caller
/// (exactly once per index); -1 if unavailable. `socketpair` link only.
int gcs_core_inproc_take_channel_fd(GcsCoreInproc* core, size_t index);
/// The abstract socket name without its leading NUL, for a
/// `unix-abstract:<name>` gRPC target. Same copy semantics as
/// `gcs_core_inproc_last_error`. `abstract` link only (0 otherwise).
size_t gcs_core_inproc_abstract_name(GcsCoreInproc const* core, char* buf,
                                     size_t cap);

/// 1 while the server runs; 0 once it stopped (Shutdown RPC, failure, panic).
int gcs_core_inproc_alive(GcsCoreInproc const* core);
/// `running`, or why the server stopped. Same copy semantics as above.
size_t gcs_core_inproc_status(GcsCoreInproc const* core, char* buf, size_t cap);
/// Cumulative CPU time (user + system, microseconds) of the core's threads.
uint64_t gcs_core_inproc_core_cpu_us(GcsCoreInproc const* core);

/// Kills the core without draining — what a crash looks like to its peers —
/// and frees `core`. For tests of the restart path. Null is accepted.
void gcs_core_inproc_kill(GcsCoreInproc* core);

/// Stops the server (draining in-flight RPCs) and the runtime, each bounded
/// by `grace_ms`, and frees `core`. Returns 1 if the server stopped within the
/// bound, else 0; the runtime is shut down either way. Drop the gRPC channels
/// first so the drain is not waiting on them. Null is accepted (no-op, 1).
int gcs_core_inproc_stop(GcsCoreInproc* core, uint64_t grace_ms);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // GCS_UDS_POC_CPP_SRC_GCS_CORE_INPROC_H
