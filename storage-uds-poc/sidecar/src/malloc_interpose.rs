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

//! Process-wide allocation counting for the two Rust *binaries*
//! (`gcs-core-sidecar`, `gcs-core-bench`), included with `#[path]`.
//!
//! The libc allocation functions are interposed (as the C++ harness does),
//! rather than counting at Rust's `GlobalAlloc`: the core also allocates from
//! C code (aws-lc via `OPENSSL_malloc`), which never passes through Rust's
//! allocator. Rust's default `System` allocator calls these symbols, so Rust
//! allocations are counted too, exactly once.
//!
//! This module is deliberately not part of `libgcs_core`: when the core is
//! linked into a C++ application, that application's own interposer (the
//! benchmark's `alloc_counter.cc`) counts for the whole process, and a second
//! strong definition of `malloc` would be a link error.

use gcs_core::stats::record_allocation as count;

// glibc's real implementations; exported with a stable ABI since 2.2.5.
unsafe extern "C" {
    fn __libc_malloc(size: usize) -> *mut libc::c_void;
    fn __libc_calloc(n: usize, size: usize) -> *mut libc::c_void;
    fn __libc_realloc(p: *mut libc::c_void, size: usize) -> *mut libc::c_void;
    fn __libc_memalign(align: usize, size: usize) -> *mut libc::c_void;
    fn __libc_free(p: *mut libc::c_void);
}

// SAFETY (all functions below): each forwards to the glibc implementation
// with the same contract; only a counter is updated, which allocates nothing.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn malloc(size: usize) -> *mut libc::c_void {
    count(size);
    unsafe { __libc_malloc(size) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn calloc(n: usize, size: usize) -> *mut libc::c_void {
    count(n.saturating_mul(size));
    unsafe { __libc_calloc(n, size) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn realloc(p: *mut libc::c_void, size: usize) -> *mut libc::c_void {
    count(size);
    unsafe { __libc_realloc(p, size) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn free(p: *mut libc::c_void) {
    unsafe { __libc_free(p) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn memalign(align: usize, size: usize) -> *mut libc::c_void {
    count(size);
    unsafe { __libc_memalign(align, size) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn aligned_alloc(align: usize, size: usize) -> *mut libc::c_void {
    count(size);
    unsafe { __libc_memalign(align, size) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn posix_memalign(
    out: *mut *mut libc::c_void,
    align: usize,
    size: usize,
) -> libc::c_int {
    count(size);
    let p = unsafe { __libc_memalign(align, size) };
    if p.is_null() {
        return libc::ENOMEM;
    }
    unsafe { *out = p };
    0
}

#[cfg(test)]
mod tests {
    /// Rust heap allocations go through the interposed malloc, so a 1 MiB
    /// Vec moves the counter by at least 1 MiB.
    #[test]
    fn rust_allocations_are_counted() {
        let before = gcs_core::stats::snapshot().allocated_bytes;
        let v = std::hint::black_box(vec![1_u8; 1 << 20]);
        let after = gcs_core::stats::snapshot().allocated_bytes;
        assert!(after - before >= 1 << 20, "delta={}", after - before);
        drop(v);
    }
}
