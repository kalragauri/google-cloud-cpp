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

#ifndef GCS_UDS_POC_CPP_INCLUDE_GCS_CORE_INPROC_H
#define GCS_UDS_POC_CPP_INCLUDE_GCS_CORE_INPROC_H

/**
 * Extras of the in-process variant (`gcs_core_inproc`), where the Rust core
 * runs on tokio threads inside the application. Not provided by the sidecar
 * variant.
 */
namespace gcs_core {
namespace inproc {

/// Callbacks run on each core thread right after it starts and right before
/// it exits, with `data` passed back unchanged. A benchmark uses them to tag
/// core threads in a thread-local so allocations can be attributed to the
/// core. Either callback may be null.
struct ThreadHooks {
  void (*on_start)(void* data) = nullptr;
  void (*on_stop)(void* data) = nullptr;
  void* data = nullptr;
};

/// Installs `hooks` for cores started afterwards (not for a core already
/// shared by live `Client`s). The callbacks and `data` must stay valid until
/// the last `Client` using them is destroyed.
void SetThreadHooks(ThreadHooks hooks);

}  // namespace inproc
}  // namespace gcs_core

#endif  // GCS_UDS_POC_CPP_INCLUDE_GCS_CORE_INPROC_H
