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

// Copied unchanged in behavior from the C++ FFI POC harness
// (v-pratap/google-cloud-rust examples/cpp-wrapper-poc/cpp/bench), so both
// POCs count allocations the same way.

// Process-wide heap allocation counters for the benchmarks.
//
// * malloc_bytes(): every heap allocation in the process. malloc/calloc/
//   realloc/memalign/posix_memalign/aligned_alloc are interposed here and
//   forwarded to glibc's __libc_* implementations. Rust's global allocator
//   (System) and C++ operator new both end up in malloc, so this counts the
//   Rust core, gRPC C-core and C++ code alike. This is the fair metric for
//   comparing the Rust-wrapped and native clients.
// * new_bytes(): only C++ `operator new`, which is exactly what
//   storage-shared-benchmarking's `memory` metric counts (it overrides
//   operator new). Use this to compare native C++ with the SSB dashboards.

#ifndef GCS_UDS_POC_BENCH_ALLOC_COUNTER_H
#define GCS_UDS_POC_BENCH_ALLOC_COUNTER_H

#include <cstdint>

namespace alloc_counter {
std::uint64_t malloc_bytes();
std::uint64_t malloc_calls();
std::uint64_t new_bytes();
/// Current / peak resident set size of the process in KiB (/proc/self/status).
std::int64_t rss_kib();
std::int64_t peak_rss_kib();
}  // namespace alloc_counter

#endif  // GCS_UDS_POC_BENCH_ALLOC_COUNTER_H
