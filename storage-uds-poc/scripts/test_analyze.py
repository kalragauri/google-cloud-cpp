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

  def test_doc_throughput_unit_by_bytes_per_op(self) -> None:
    # 8 KiB per op (W3) is rate-bound: report ops/s even though bytes flow.
    small = {"window_wall_secs": 10.0, "ops_ok": 500, "total_bytes": 500 * 8192,
             "throughput_mb_s": 0.41}
    self.assertEqual(analyze.doc_throughput(small), (50.0, "ops/s"))
    # 16 MiB per op (W4) is bandwidth-bound: report MB/s.
    bulk = {"window_wall_secs": 10.0, "ops_ok": 10,
            "total_bytes": 10 * 16 * 1024 * 1024, "throughput_mb_s": 16.8}
    self.assertEqual(analyze.doc_throughput(bulk), (16.8, "MB/s"))
    # Metadata-only (W1/W6): ops/s, and an empty window does not divide by 0.
    meta = {"window_wall_secs": 0.0, "ops_ok": 3, "total_bytes": 0,
            "throughput_mb_s": 0.0}
    self.assertEqual(analyze.doc_throughput(meta), (0.0, "ops/s"))

  def test_doc_latency_uses_per_range_only_for_w4(self) -> None:
    w4 = _make_summary("native", 10000.0, range_count=20)
    self.assertEqual(analyze.doc_latency(w4)["p50"], 5000.0)
    w4_no_ranges = _make_summary("native", 10000.0, range_count=0)
    self.assertEqual(analyze.doc_latency(w4_no_ranges)["p50"], 10000.0)
    other = dict(_make_summary("native", 10000.0, range_count=20))
    other["workload"] = "random8k"
    self.assertEqual(analyze.doc_latency(other)["p50"], 10000.0)

  def test_split_cell_name_and_title_order(self) -> None:
    self.assertEqual(analyze.split_cell_name("bidi-read-48t-zonal"),
                     ("bidi-read-48t", "zonal"))
    self.assertEqual(analyze.split_cell_name("list-buckets-100-project"),
                     ("list-buckets-100", "project"))
    cells = ["list-objects-100-regional", "open-zonal", "bidi-write-48t-zonal"]
    self.assertEqual(sorted(cells, key=analyze.workload_order),
                     ["open-zonal", "bidi-write-48t-zonal",
                      "list-objects-100-regional"])

  def test_render_doc_tables_and_sensitivity(self) -> None:
    with tempfile.TemporaryDirectory() as tmp:
      for mode, p50 in (("native", 10000.0), ("uds-sidecar", 12500.0),
                        ("uds-inproc", 11000.0)):
        stem = os.path.join(tmp, f"bidi-read-48t-zonal-{mode}")
        summary = _make_summary(mode, p50, range_count=20)
        summary["window_wall_secs"] = 10.0
        summary["total_bytes"] = 10 * 16 * 1024 * 1024
        summary["worker_threads"] = 0
        with open(f"{stem}.summary.json", "w", encoding="utf-8") as fh:
          json.dump(summary, fh)
        with open(f"{stem}.monitoring.json", "w", encoding="utf-8") as fh:
          json.dump({"cpu_utilization": {"avg_percent": 2.0,
                                         "max_percent": 3.0},
                     "memory_percent_used": {"max_percent": 1.0}}, fh)
      cells = analyze.discover_cells(tmp)
      doc = analyze.render_doc_tables(tmp, cells)
      self.assertIn("### W4 — Read 16 MiB chunks", doc)
      self.assertIn("| Metric | Native C++ | `uds-sidecar` | % Diff | "
                    "`uds-inproc` | % Diff |", doc)
      # W4 reports the per-range P50 (5.0 ms for every mode -> +0.0%).
      self.assertIn(
          "| Latency P50 | 5.0 ms | 5.0 ms | +0.0% | 5.0 ms | +0.0% |", doc)
      self.assertIn("| Throughput | 500.0 MB/s | 500.0 MB/s | +0.0% |", doc)
      # Comparing a directory against itself yields all-zero deltas and keeps
      # the server-thread and worker-thread rows for the UDS comparators only.
      sens = analyze.render_sensitivity(tmp, cells, tmp, cells)
      self.assertIn("| `bidi-read-48t-zonal` | `uds-sidecar` | Latency P50 | "
                    "5.0 ms | 5.0 ms | +0.0% |", sens)
      self.assertIn("| `bidi-read-48t-zonal` | `uds-inproc` | Server threads "
                    "| 12 | 12 | |", sens)
      self.assertNotIn("| `native` |", sens)


if __name__ == "__main__":
  unittest.main()
