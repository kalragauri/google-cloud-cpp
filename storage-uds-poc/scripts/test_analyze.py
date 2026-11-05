#!/usr/bin/env python3
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Unit tests for analyze.py."""

import json
import os
import tempfile
import unittest

import analyze


def _make_summary(
    mode: str,
    p50_us: float,
    range_count: int = 0,
    shares_app: bool = False,
) -> dict[str, object]:
  server = None
  if mode != "native":
    server = {
        "pid": 100,
        "shares_app_process": shares_app,
        "cpu_total_secs": 3.0,
        "cpu_ns_per_byte": 1.5,
        "rss_bytes": 40 * 1024 * 1024,
        "peak_rss_bytes": 50 * 1024 * 1024,
        "threads": 12,
    }
  return {
      "mode": mode,
      "workload": "bidi-read-48t",
      "ops_ok": 10,
      "ops_err": 0,
      "throughput_mb_s": 500.0,
      "throughput_mib_s": 476.84,
      "latency_us": {"count": 10, "p50": p50_us, "p90": p50_us * 1.2, "p99": p50_us * 1.5},
      "range_latency_us": {
          "count": range_count,
          "p50": 5000.0,
          "p90": 7000.0,
          "p99": 9000.0,
      },
      "app_process": {
          "cpu_total_secs": 10.0,
          "peak_rss_kib": 102400,
      },
      "server_process": server,
  }


class AnalyzeTest(unittest.TestCase):
  """Verifies cell discovery, ratio computation, CPU attribution, and W4 range table."""

  def test_discover_and_render_markdown(self) -> None:
    with tempfile.TemporaryDirectory() as tmp:
      for mode, p50, shares in (
          ("native", 10000.0, False),
          ("uds-sidecar", 12500.0, False),
          ("uds-inproc", 11000.0, True),
      ):
        stem = os.path.join(tmp, f"w4-bidi-read-48t-zonal-{mode}")
        with open(f"{stem}.summary.json", "w", encoding="utf-8") as fh:
          json.dump(_make_summary(mode, p50, range_count=20, shares_app=shares), fh)
        with open(f"{stem}.monitoring.json", "w", encoding="utf-8") as fh:
          json.dump(
              {
                  "cpu_utilization": {"avg_percent": 42.5, "max_percent": 55.0},
                  "memory_percent_used": {"max_percent": 12.3},
              },
              fh,
          )
        with open(f"{stem}.path.json", "w", encoding="utf-8") as fh:
          json.dump({"verified": True}, fh)

      cells = analyze.discover_cells(tmp)
      self.assertIn("w4-bidi-read-48t-zonal", cells)
      md = analyze.render_markdown(tmp, cells)
      # P50 ratio vs native: 12500 / 10000 = 1.25x, 11000 / 10000 = 1.10x.
      self.assertIn("| `w4-bidi-read-48t-zonal` | `uds-sidecar` | Cloudpath | 10/0 | 12.50 |", md)
      self.assertIn("| 1.25x |", md)
      self.assertIn("| 1.10x |", md)
      # In-process subtracts server_process CPU (10.0 - 3.0 = 7.00s) and marks
      # server Peak RSS as "shared" so host+core RSS is not double-counted.
      self.assertIn("| `w4-bidi-read-48t-zonal` | `uds-inproc` | 7.00 | 100.0 | 3.00 | shared | 12 |", md)
      self.assertIn("## Per-Range Latency (W4 Streaming Read)", md)

  def test_pct_diff_sign_and_zero_base(self) -> None:
    self.assertEqual(analyze.pct_diff(125.0, 100.0), "+25.0%")
    self.assertEqual(analyze.pct_diff(90.0, 100.0), "-10.0%")
    self.assertEqual(analyze.pct_diff(1.0, 0.0), "n/a")
    self.assertEqual(analyze.pct_diff(1.0, -1.0), "n/a")

  def test_doc_throughput_unit_is_a_property_of_the_workload(self) -> None:
    # W3 moves 8 KiB per op and is rate-bound: ops/s even though bytes flow.
    small = {"workload": "random8k", "window_wall_secs": 10.0, "ops_ok": 500,
             "total_bytes": 500 * 8192, "throughput_mb_s": 0.41}
    self.assertEqual(analyze.doc_throughput(small), (50.0, "ops/s"))
    # W4 is bandwidth-bound: MB/s, and still MB/s for an empty cell so a failed
    # comparator cannot flip the row's unit.
    bulk = {"workload": "bidi-read-48t", "window_wall_secs": 10.0, "ops_ok": 10,
            "total_bytes": 10 * 16 * 1024 * 1024, "throughput_mb_s": 16.8}
    self.assertEqual(analyze.doc_throughput(bulk), (16.8, "MB/s"))
    empty_bulk = dict(bulk, ops_ok=0, total_bytes=0, throughput_mb_s=0.0)
    self.assertEqual(analyze.doc_throughput(empty_bulk), (0.0, "MB/s"))
    # Metadata-only (W1/W6): ops/s, and an empty window does not divide by 0.
    meta = {"workload": "open", "window_wall_secs": 0.0, "ops_ok": 3,
            "total_bytes": 0, "throughput_mb_s": 0.0}
    self.assertEqual(analyze.doc_throughput(meta), (0.0, "ops/s"))

  def test_latency_basis_is_decided_once_per_cell(self) -> None:
    w4_native = {"summary": _make_summary("native", 10000.0, range_count=20)}
    w4_uds = {"summary": _make_summary("uds-sidecar", 12000.0, range_count=20)}
    self.assertEqual(analyze.latency_basis([w4_native, w4_uds]), "per range")
    # One comparator without ranges (failed or old cell) demotes the whole
    # cell to per-op so the % Diff never mixes the two distributions.
    w4_no_ranges = {"summary": _make_summary("uds-inproc", 11000.0,
                                            range_count=0)}
    self.assertEqual(analyze.latency_basis([w4_native, w4_uds, w4_no_ranges]),
                     "per op")
    other = {"summary": dict(_make_summary("native", 10000.0, range_count=20),
                             workload="random8k")}
    self.assertEqual(analyze.latency_basis([other]), "per op")
    self.assertEqual(analyze.latency_basis([]), "per op")
    self.assertEqual(
        analyze.doc_latency(w4_native["summary"], "per range")["p50"], 5000.0)
    self.assertEqual(
        analyze.doc_latency(w4_native["summary"], "per op")["p50"], 10000.0)

  def test_split_cell_name_and_title_order(self) -> None:
    self.assertEqual(analyze.split_cell_name("bidi-read-48t-zonal"),
                     ("bidi-read-48t", "zonal"))
    self.assertEqual(analyze.split_cell_name("list-buckets-100-project"),
                     ("list-buckets-100", "project"))
    cells = ["list-objects-100-regional", "open-zonal", "bidi-write-48t-zonal"]
    self.assertEqual(sorted(cells, key=analyze.workload_order),
                     ["open-zonal", "bidi-write-48t-zonal",
                      "list-objects-100-regional"])

  def test_monitoring_completeness_marker(self) -> None:
    mon = {
        "aligned_window": {"start_utc": "2026-11-05T06:46:00Z",
                           "end_utc": "2026-11-05T06:52:00Z"},
        "cpu_utilization": {"point_count": 6, "avg_percent": 1.5},
        "memory_percent_used": {"point_count": 5, "max_percent": 0.9},
    }
    self.assertEqual(analyze.monitoring_expected_points(mon), 6)
    self.assertEqual(
        analyze.monitoring_value(mon, "cpu_utilization", "avg_percent"),
        (1.5, True))
    # One bucket short: the value is still reported but marked.
    self.assertEqual(
        analyze.monitoring_value(mon, "memory_percent_used", "max_percent"),
        (0.9, False))
    self.assertEqual(analyze.percent_display(0.9, False), "0.90%‡")
    self.assertEqual(analyze.percent_display(None, True), "n/a")
    self.assertEqual(
        analyze.monitoring_value(None, "cpu_utilization", "avg_percent"),
        (None, True))

  def _write_w4_cell(self, directory: str, mode: str, range_p50: float,
                     mb_s: float, cpu_avg: float | None,
                     worker_threads: int = 0) -> None:
    stem = os.path.join(directory, f"bidi-read-48t-zonal-{mode}")
    summary = _make_summary(mode, 10000.0, range_count=20)
    summary["range_latency_us"] = {"count": 20, "p50": range_p50,
                                   "p90": range_p50 * 2, "p99": range_p50 * 3}
    summary["window_wall_secs"] = 10.0
    summary["total_bytes"] = 10 * 16 * 1024 * 1024
    summary["throughput_mb_s"] = mb_s
    summary["worker_threads"] = worker_threads
    summary["bucket"] = "example-zonal"
    with open(f"{stem}.summary.json", "w", encoding="utf-8") as fh:
      json.dump(summary, fh)
    if cpu_avg is not None:
      with open(f"{stem}.monitoring.json", "w", encoding="utf-8") as fh:
        json.dump({"aligned_window": {"start_utc": "2026-11-05T06:46:00Z",
                                      "end_utc": "2026-11-05T06:48:00Z"},
                   "cpu_utilization": {"point_count": 2,
                                       "avg_percent": cpu_avg,
                                       "max_percent": cpu_avg + 1.0},
                   "memory_percent_used": {"point_count": 2,
                                           "max_percent": 1.0}}, fh)

  def test_render_doc_tables_signed_deltas_and_missing_monitoring(self) -> None:
    with tempfile.TemporaryDirectory() as tmp:
      self._write_w4_cell(tmp, "native", range_p50=4000.0, mb_s=500.0,
                          cpu_avg=2.0)
      self._write_w4_cell(tmp, "uds-sidecar", range_p50=5000.0, mb_s=400.0,
                          cpu_avg=1.0)
      self._write_w4_cell(tmp, "uds-inproc", range_p50=3000.0, mb_s=600.0,
                          cpu_avg=None)
      cells = analyze.discover_cells(tmp)
      doc = analyze.render_doc_tables(tmp, cells)
      self.assertIn("### W4 — Read 16 MiB chunks", doc)
      self.assertIn("Bucket: Zonal (RAPID) `gs://example-zonal` · cell "
                    "`bidi-read-48t-zonal`", doc)
      self.assertIn("| Metric | Native C++ | `uds-sidecar` | % Diff | "
                    "`uds-inproc` | % Diff |", doc)
      # Signed deltas in both directions pin the (UDS - Native) / Native
      # argument order; the label carries the per-range basis.
      self.assertIn("| Latency P50 (per range) | 4.0 ms | 5.0 ms | +25.0% | "
                    "3.0 ms | -25.0% |", doc)
      self.assertIn("| Throughput | 500.0 MB/s | 400.0 MB/s | -20.0% | "
                    "600.0 MB/s | +20.0% |", doc)
      # A comparator without monitoring.json renders n/a and no delta.
      self.assertIn("| CPU Utilization (`instance/cpu/utilization`, avg) | "
                    "2.00% | 1.00% | -50.0% | n/a |  |", doc)
      self.assertIn("| Ops (ok / err) | 10 / 0 | 10 / 0 |  | 10 / 0 |  |", doc)

  def test_render_doc_tables_without_native_cell(self) -> None:
    with tempfile.TemporaryDirectory() as tmp:
      self._write_w4_cell(tmp, "uds-sidecar", range_p50=5000.0, mb_s=400.0,
                          cpu_avg=1.0)
      doc = analyze.render_doc_tables(tmp, analyze.discover_cells(tmp))
      self.assertIn("(no native cell in this results directory; skipped)", doc)
      self.assertNotIn("| Metric |", doc)

  def test_render_sensitivity_deltas_and_missing_base(self) -> None:
    with tempfile.TemporaryDirectory() as base, \
        tempfile.TemporaryDirectory() as wt:
      self._write_w4_cell(base, "uds-sidecar", range_p50=4000.0, mb_s=500.0,
                          cpu_avg=2.0, worker_threads=0)
      self._write_w4_cell(wt, "uds-sidecar", range_p50=5000.0, mb_s=400.0,
                          cpu_avg=1.0, worker_threads=16)
      self._write_w4_cell(wt, "uds-inproc", range_p50=3000.0, mb_s=600.0,
                          cpu_avg=1.0, worker_threads=16)
      sens = analyze.render_sensitivity(base, analyze.discover_cells(base),
                                        wt, analyze.discover_cells(wt))
      # (worker-thread run - base) / base, signed.
      self.assertIn("| `bidi-read-48t-zonal` | `uds-sidecar` | Latency P50 "
                    "(per range) | 4.0 ms | 5.0 ms | +25.0% |", sens)
      self.assertIn("| `bidi-read-48t-zonal` | `uds-sidecar` | Throughput | "
                    "500.0 MB/s | 400.0 MB/s | -20.0% |", sens)
      self.assertIn("| `bidi-read-48t-zonal` | `uds-sidecar` | Server threads "
                    "| 12 | 12 | |", sens)
      self.assertIn("| `bidi-read-48t-zonal` | `uds-sidecar` | "
                    "`--worker-threads` | 0 | 16 | |", sens)
      # A worker-thread cell with no counterpart in the base run is reported,
      # not silently dropped, and native never appears.
      self.assertIn("| `bidi-read-48t-zonal` | `uds-inproc` | (no base cell) "
                    "| | | |", sens)
      self.assertNotIn("| `native` |", sens)

  def test_main_rejects_combined_modes_and_empty_base_dir(self) -> None:
    with tempfile.TemporaryDirectory() as tmp:
      with self.assertRaises(SystemExit):
        analyze.main([tmp, "--doc-tables", "--sensitivity", tmp])
      with self.assertRaises(SystemExit):
        analyze.main([tmp, "--sensitivity", ""])


if __name__ == "__main__":
  unittest.main()
