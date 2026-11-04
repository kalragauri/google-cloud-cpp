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

//! Process-level counters reported by `GetStats`.
//!
//! The benchmark measures the sidecar with the same method it uses for the
//! application process (getrusage + RSS + bytes allocated), so the two can be
//! summed. Tokio task poll time is deliberately not used: it misses CPU spent
//! outside polled futures (TLS, hyper, the allocator, kernel time).

use std::sync::atomic::{AtomicU64, Ordering};

/// Cumulative bytes requested from malloc and friends, never decremented, so
/// a delta between two snapshots is the allocation volume of the work in
/// between. Fed by the binaries' `malloc_interpose` module; inside
/// `libgcs_core` linked into a C++ program nothing feeds it and it stays 0 —
/// the host's own interposer then counts for the whole process.
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

/// Records `n` requested bytes. Must not allocate: it runs inside `malloc`.
#[inline]
pub fn record_allocation(n: usize) {
    ALLOCATED_BYTES.fetch_add(n as u64, Ordering::Relaxed);
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Snapshot {
    pub user_cpu_us: u64,
    pub system_cpu_us: u64,
    pub rss_bytes: u64,
    pub peak_rss_bytes: u64,
    pub allocated_bytes: u64,
    pub threads: u32,
}

pub fn snapshot() -> Snapshot {
    let (user_cpu_us, system_cpu_us) = rusage_self_us();
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let proc = parse_proc_status(&status);
    Snapshot {
        user_cpu_us,
        system_cpu_us,
        rss_bytes: proc.rss_bytes,
        peak_rss_bytes: proc.peak_rss_bytes,
        allocated_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
        threads: proc.threads,
    }
}

fn rusage_self_us() -> (u64, u64) {
    let mut ru = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: `ru` is a valid, writable rusage; RUSAGE_SELF is always valid.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, ru.as_mut_ptr()) };
    if rc != 0 {
        return (0, 0);
    }
    // SAFETY: getrusage succeeded, so the struct is initialized.
    let ru = unsafe { ru.assume_init() };
    let us = |tv: libc::timeval| tv.tv_sec as u64 * 1_000_000 + tv.tv_usec as u64;
    (us(ru.ru_utime), us(ru.ru_stime))
}

#[derive(Debug, Default, PartialEq, Eq)]
struct ProcStatus {
    rss_bytes: u64,
    peak_rss_bytes: u64,
    threads: u32,
}

/// Parses the `VmRSS`, `VmHWM` (kB) and `Threads` lines of /proc/self/status.
fn parse_proc_status(text: &str) -> ProcStatus {
    let mut out = ProcStatus::default();
    for line in text.lines() {
        let mut parts = line.split_whitespace();
        let (Some(key), Some(value)) = (parts.next(), parts.next()) else {
            continue;
        };
        let Ok(value) = value.parse::<u64>() else {
            continue;
        };
        match key {
            "VmRSS:" => out.rss_bytes = value * 1024,
            "VmHWM:" => out.peak_rss_bytes = value * 1024,
            "Threads:" => out.threads = value as u32,
            _ => {}
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies the kB → bytes conversion and that unrelated or malformed
    /// lines are ignored.
    #[test]
    fn parse_proc_status_extracts_rss_hwm_threads() {
        let text =
            "Name:\tsidecar\nVmHWM:\t  2048 kB\nVmRSS:\t  1024 kB\nThreads:\t7\nVmLck:\tbad kB\n";
        assert_eq!(
            parse_proc_status(text),
            ProcStatus {
                rss_bytes: 1024 * 1024,
                peak_rss_bytes: 2048 * 1024,
                threads: 7
            }
        );
    }

    /// An empty or unreadable status file yields zeros rather than an error,
    /// so GetStats never fails because of /proc.
    #[test]
    fn parse_proc_status_empty_is_zero() {
        assert_eq!(parse_proc_status(""), ProcStatus::default());
    }

    /// The live snapshot must report this test process's own threads and a
    /// non-zero RSS.
    #[test]
    fn snapshot_reports_live_values() {
        let s = snapshot();
        assert!(s.rss_bytes > 0);
        assert!(s.threads >= 1);
    }
}
