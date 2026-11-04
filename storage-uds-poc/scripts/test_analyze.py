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


if __name__ == "__main__":
  unittest.main()
