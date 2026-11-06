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
"""Unit tests for monitoring.py."""

import json
import os
import pathlib
import sys
import tempfile
from typing import Any, Mapping
import unittest
from unittest import mock
import urllib.parse

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import monitoring  # noqa: E402


RECORDED_CPU_PAGE_1: dict[str, Any] = {
    "timeSeries": [
        {
            "metric": {"type": "compute.googleapis.com/instance/cpu/utilization"},
            "resource": {
                "type": "gce_instance",
                "labels": {
                    "project_id": "kalragauri-sdk-test",
                    "instance_id": "987654321012345678",
                    "zone": "us-central1-b",
                },
            },
            "metricKind": "GAUGE",
            "valueType": "DOUBLE",
            "points": [
                {
                    "interval": {
                        "startTime": "2026-11-04T12:04:00Z",
                        "endTime": "2026-11-04T12:04:00Z",
                    },
                    "value": {"doubleValue": 0.42},
                },
                {
                    "interval": {
                        "startTime": "2026-11-04T12:02:00Z",
                        "endTime": "2026-11-04T12:02:00Z",
                    },
                    "value": {"doubleValue": 0.18},
                },
            ],
        }
    ],
    "nextPageToken": "cpu-page-2",
    "unit": "10^2.%",
}

RECORDED_CPU_PAGE_2: dict[str, Any] = {
    "timeSeries": [
        {
            "metric": {"type": "compute.googleapis.com/instance/cpu/utilization"},
            "points": [
                {
                    "interval": {
                        "startTime": "2026-11-04T12:03:00Z",
                        "endTime": "2026-11-04T12:03:00Z",
                    },
                    "value": {"doubleValue": 0.30},
                }
            ],
        }
    ]
}

RECORDED_MEMORY_RESPONSE: dict[str, Any] = {
    "timeSeries": [
        {
            "metric": {
                "type": "agent.googleapis.com/memory/percent_used",
                "labels": {"state": "used"},
            },
            "resource": {
                "type": "gce_instance",
                "labels": {
                    "project_id": "kalragauri-sdk-test",
                    "instance_id": "987654321012345678",
                    "zone": "us-central1-b",
                },
            },
            "metricKind": "GAUGE",
            "valueType": "DOUBLE",
            "points": [
                {
                    "interval": {
                        "startTime": "2026-11-04T12:04:00Z",
                        "endTime": "2026-11-04T12:04:00Z",
                    },
                    "value": {"doubleValue": 6.8},
                },
                {
                    "interval": {
                        "startTime": "2026-11-04T12:03:00Z",
                        "endTime": "2026-11-04T12:03:00Z",
                    },
                    "value": {"doubleValue": 7.5},
                },
                {
                    "interval": {
                        "startTime": "2026-11-04T12:02:00Z",
                        "endTime": "2026-11-04T12:02:00Z",
                    },
                    "value": {"doubleValue": 5.2},
                },
            ],
        }
    ],
    "unit": "%",
}


class FakeMonitoringBackend:
  """Deterministic recorded-response backend capturing requested query params."""

  def __init__(self, empty_memory: bool = False) -> None:
    self.urls: list[str] = []
    self._empty_memory = empty_memory

  def __call__(self, url: str) -> Mapping[str, Any]:
    self.urls.append(url)
    parsed = urllib.parse.urlparse(url)
    params = urllib.parse.parse_qs(parsed.query)
    flt = params["filter"][0]
    page = params.get("pageToken", [""])[0]
    if monitoring.CPU_METRIC in flt:
      return RECORDED_CPU_PAGE_2 if page == "cpu-page-2" else RECORDED_CPU_PAGE_1
    if monitoring.MEMORY_METRIC in flt:
      return {} if self._empty_memory else RECORDED_MEMORY_RESPONSE
    raise AssertionError(f"unexpected filter: {flt}")


class MonitoringTest(unittest.TestCase):

  def test_pad_window_expands_partial_minutes_outward(self) -> None:
    # A 3m18s window starting mid-minute expands to [12:01:00Z, 12:05:00Z] so
    # every 60 s ALIGN_MEAN bucket overlapping the run is captured.
    start = monitoring.parse_utc_timestamp("2026-11-04T12:01:23.456Z")
    end = monitoring.parse_utc_timestamp("2026-11-04T12:04:41.789Z")
    padded_start, padded_end = monitoring.pad_window_to_minutes(start, end)
    self.assertEqual(
        monitoring.format_utc_timestamp(padded_start), "2026-11-04T12:01:00Z"
    )
    self.assertEqual(
        monitoring.format_utc_timestamp(padded_end), "2026-11-04T12:05:00Z"
    )

  def test_pad_window_preserves_exact_minute_boundaries(self) -> None:
    # Exact whole-minute timestamps do not gain an extra trailing minute unless
    # start == end.
    start = monitoring.parse_utc_timestamp("2026-11-04T12:01:00Z")
    end = monitoring.parse_utc_timestamp("2026-11-04T12:04:00Z")
    padded_start, padded_end = monitoring.pad_window_to_minutes(start, end)
    self.assertEqual(
        monitoring.format_utc_timestamp(padded_start), "2026-11-04T12:01:00Z"
    )
    self.assertEqual(
        monitoring.format_utc_timestamp(padded_end), "2026-11-04T12:04:00Z"
    )

  def test_query_window_metrics_aggregates_cpu_and_memory(self) -> None:
    # Pins the query shape: instance_id + state="used" filters, 60s ALIGN_MEAN,
    # pagination across nextPageToken, chronological ordering, CPU avg/max, and
    # peak memory percent. The window ends on a minute boundary so the three
    # recorded points (12:02, 12:03, 12:04) fill the padded window exactly.
    backend = FakeMonitoringBackend()
    start = monitoring.parse_utc_timestamp("2026-11-04T12:01:12Z")
    end = monitoring.parse_utc_timestamp("2026-11-04T12:04:00Z")

    report = monitoring.query_window_metrics(
        backend,
        project="kalragauri-sdk-test",
        instance_id="987654321012345678",
        start=start,
        end=end,
    )

    self.assertEqual(len(backend.urls), 3)
    first_qs = urllib.parse.parse_qs(urllib.parse.urlparse(backend.urls[0]).query)
    self.assertEqual(first_qs["aggregation.alignmentPeriod"], ["60s"])
    self.assertEqual(first_qs["aggregation.perSeriesAligner"], ["ALIGN_MEAN"])
    self.assertEqual(first_qs["interval.startTime"], ["2026-11-04T12:01:00Z"])
    self.assertEqual(first_qs["interval.endTime"], ["2026-11-04T12:04:00Z"])
    self.assertIn(
        'resource.labels.instance_id = "987654321012345678"',
        first_qs["filter"][0],
    )

    mem_qs = urllib.parse.parse_qs(urllib.parse.urlparse(backend.urls[2]).query)
    self.assertIn('metric.labels.state = "used"', mem_qs["filter"][0])

    self.assertEqual(report["aligned_window"]["expected_points"], 3)
    cpu = report["cpu_utilization"]
    self.assertEqual(cpu["point_count"], 3)
    self.assertAlmostEqual(cpu["avg_fraction"], 0.30, places=6)
    self.assertAlmostEqual(cpu["max_fraction"], 0.42, places=6)
    self.assertAlmostEqual(cpu["avg_percent"], 30.0, places=4)
    self.assertAlmostEqual(cpu["max_percent"], 42.0, places=4)
    self.assertEqual(
        [p["end_utc"] for p in cpu["points"]],
        [
            "2026-11-04T12:02:00Z",
            "2026-11-04T12:03:00Z",
            "2026-11-04T12:04:00Z",
        ],
    )

    mem = report["memory_percent_used"]
    self.assertEqual(mem["point_count"], 3)
    self.assertAlmostEqual(mem["max_percent"], 7.5, places=6)
    self.assertAlmostEqual(mem["avg_percent"], 6.5, places=6)

  def test_incomplete_series_raises_with_partial_report(self) -> None:
    # A window that pads to 12:05 expects 4 buckets, but the recorded series
    # stops at 12:04 (the late-flush case): the query must signal the shortfall
    # so the CLI keeps polling, and carry the partial report for the deadline
    # fallback.
    backend = FakeMonitoringBackend()
    start = monitoring.parse_utc_timestamp("2026-11-04T12:01:12Z")
    end = monitoring.parse_utc_timestamp("2026-11-04T12:04:49Z")
    with self.assertRaisesRegex(monitoring.IncompleteSeriesError,
                                "cpu 3/4, memory 3/4") as ctx:
      monitoring.query_window_metrics(
          backend,
          project="kalragauri-sdk-test",
          instance_id="987654321012345678",
          start=start,
          end=end,
      )
    partial = ctx.exception.report
    self.assertEqual(partial["aligned_window"]["expected_points"], 4)
    self.assertEqual(partial["cpu_utilization"]["point_count"], 3)
    self.assertIsInstance(ctx.exception, monitoring.MonitoringError)

  def test_empty_series_raises_instead_of_reporting_zero(self) -> None:
    # Missing Ops Agent or ingestion lag must fail loudly rather than writing a
    # misleading 0% memory figure.
    backend = FakeMonitoringBackend(empty_memory=True)
    start = monitoring.parse_utc_timestamp("2026-11-04T12:01:12Z")
    end = monitoring.parse_utc_timestamp("2026-11-04T12:04:49Z")
    with self.assertRaisesRegex(monitoring.MonitoringError, "empty timeSeries"):
      monitoring.query_window_metrics(
          backend,
          project="kalragauri-sdk-test",
          instance_id="987654321012345678",
          start=start,
          end=end,
      )

  def test_cli_reads_summary_json_and_writes_output_file(self) -> None:
    # End-to-end CLI flow reading window_start_utc/window_end_utc from a bench
    # summary file in a real temporary directory. The recorded series is one
    # bucket short of this window and `--wait-secs` defaults to 0, so the CLI
    # must write the partial report (marked by `expected_points`) rather than
    # fail the cell.
    backend = FakeMonitoringBackend()
    with tempfile.TemporaryDirectory() as tmpdir:
      summary_path = os.path.join(tmpdir, "cell.summary.json")
      out_path = os.path.join(tmpdir, "cell.monitoring.json")
      with open(summary_path, "w", encoding="utf-8") as fh:
        json.dump(
            {
                "window_start_utc": "2026-11-04T12:01:05Z",
                "window_end_utc": "2026-11-04T12:04:55Z",
            },
            fh,
        )
      with mock.patch.object(
          monitoring,
          "make_http_json_fetcher",
          autospec=True,
          return_value=backend,
      ):
        rc = monitoring.main([
            "--project=kalragauri-sdk-test",
            "--instance-id=987654321012345678",
            f"--summary-json={summary_path}",
            f"--out={out_path}",
            "--access-token=fake-token",
        ])
      self.assertEqual(rc, 0)
      with open(out_path, "r", encoding="utf-8") as fh:
        written = json.load(fh)
      self.assertEqual(written["aligned_window"]["expected_points"], 4)
      self.assertEqual(written["cpu_utilization"]["point_count"], 3)
      self.assertAlmostEqual(
          written["cpu_utilization"]["max_percent"], 42.0, places=4
      )
      self.assertAlmostEqual(
          written["memory_percent_used"]["max_percent"], 7.5, places=4
      )


if __name__ == "__main__":
  unittest.main()
