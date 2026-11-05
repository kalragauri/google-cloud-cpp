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
"""Queries Cloud Monitoring VM CPU and Ops Agent memory over a workload window.

Implements plan rev 4 §8 rows D1 and D2:
  * CPU:    `compute.googleapis.com/instance/cpu/utilization`
            filtered by `resource.labels.instance_id`, 60 s `ALIGN_MEAN`,
            reports `avg` and `max` (both raw [0, 1] fraction and percent).
  * Memory: `agent.googleapis.com/memory/percent_used`
            filtered by `metric.labels.state = "used"` and
            `resource.labels.instance_id`, 60 s `ALIGN_MEAN`, reports `max`
            (and `avg`) in percent.
"""

import argparse
import datetime
import json
import subprocess
import sys
import time
from typing import Any, Callable, Mapping
import urllib.error
import urllib.parse
import urllib.request

CPU_METRIC = "compute.googleapis.com/instance/cpu/utilization"
MEMORY_METRIC = "agent.googleapis.com/memory/percent_used"
MONITORING_API = "https://monitoring.googleapis.com/v3"
METADATA_ROOT = "http://169.254.169.254/computeMetadata/v1"


class MonitoringError(RuntimeError):
  """Raised when Cloud Monitoring returns an error or an empty time series."""


class IncompleteSeriesError(MonitoringError):
  """Raised when a series has fewer 60 s points than the window has buckets.

  GCE publishes `instance/cpu/utilization` once a minute and makes a sample
  visible up to ~240 s later, so the last bucket of a window can be missing
  for several minutes after the workload ends. The caller retries until its
  deadline and, if the series is still short, keeps the partial report
  (which `analyze.py` marks) rather than dropping the cell.
  """

  def __init__(self, message: str, report: Mapping[str, Any]):
    super().__init__(message)
    self.report = report


JsonFetcher = Callable[[str], Mapping[str, Any]]


def parse_utc_timestamp(value: str) -> datetime.datetime:
  """Parses an ISO-8601 UTC timestamp (`...Z` or explicit offset) to UTC."""
  normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
  dt = datetime.datetime.fromisoformat(normalized)
  if dt.tzinfo is None:
    raise ValueError(f"timestamp must include a timezone: {value!r}")
  return dt.astimezone(datetime.timezone.utc)


def format_utc_timestamp(dt: datetime.datetime) -> str:
  """Formats a UTC datetime as `YYYY-MM-DDTHH:MM:SSZ`."""
  utc = dt.astimezone(datetime.timezone.utc).replace(microsecond=0)
  return utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def pad_window_to_minutes(
    start: datetime.datetime, end: datetime.datetime
) -> tuple[datetime.datetime, datetime.datetime]:
  """Pads `[start, end]` outward to whole-minute boundaries (`[floor, ceil]`)."""
  if end < start:
    raise ValueError(f"end ({end.isoformat()}) precedes start ({start.isoformat()})")
  utc_start = start.astimezone(datetime.timezone.utc)
  utc_end = end.astimezone(datetime.timezone.utc)
  padded_start = utc_start.replace(second=0, microsecond=0)
  padded_end = utc_end.replace(second=0, microsecond=0)
  if utc_end > padded_end:
    padded_end += datetime.timedelta(minutes=1)
  if padded_end <= padded_start:
    padded_end = padded_start + datetime.timedelta(minutes=1)
  return padded_start, padded_end


def build_timeseries_url(
    project: str,
    metric_filter: str,
    start: datetime.datetime,
    end: datetime.datetime,
    page_token: str = "",
) -> str:
  """Builds a `projects.timeSeries.list` URL with 60 s `ALIGN_MEAN`."""
  params: dict[str, str] = {
      "filter": metric_filter,
      "interval.startTime": format_utc_timestamp(start),
      "interval.endTime": format_utc_timestamp(end),
      "aggregation.alignmentPeriod": "60s",
      "aggregation.perSeriesAligner": "ALIGN_MEAN",
  }
  if page_token:
    params["pageToken"] = page_token
  encoded_project = urllib.parse.quote(project, safe="")
  return (
      f"{MONITORING_API}/projects/{encoded_project}/timeSeries?"
      f"{urllib.parse.urlencode(params)}"
  )


def extract_point_values(payload: Mapping[str, Any]) -> list[tuple[str, float]]:
  """Extracts `(endTime, doubleValue)` pairs sorted chronologically."""
  series_list = payload.get("timeSeries", [])
  points: list[tuple[str, float]] = []
  for series in series_list:
    for point in series.get("points", []):
      end_time = point["interval"]["endTime"]
      val_obj = point["value"]
      if "doubleValue" in val_obj:
        raw = float(val_obj["doubleValue"])
      elif "int64Value" in val_obj:
        raw = float(val_obj["int64Value"])
      else:
        raise MonitoringError(f"unsupported point value payload: {val_obj!r}")
      points.append((end_time, raw))
  points.sort(key=lambda item: item[0])
  return points


def fetch_series_points(
    fetcher: JsonFetcher,
    project: str,
    metric_filter: str,
    start: datetime.datetime,
    end: datetime.datetime,
) -> list[tuple[str, float]]:
  """Fetches all pages of aligned points for `metric_filter` in `[start, end]`."""
  all_points: list[tuple[str, float]] = []
  page_token = ""
  while True:
    url = build_timeseries_url(project, metric_filter, start, end, page_token)
    payload = fetcher(url)
    all_points.extend(extract_point_values(payload))
    next_token = str(payload.get("nextPageToken", ""))
    if not next_token:
      break
    page_token = next_token
  all_points.sort(key=lambda item: item[0])
  if not all_points:
    raise MonitoringError(
        f"empty timeSeries for filter={metric_filter!r} in "
        f"[{format_utc_timestamp(start)}, {format_utc_timestamp(end)}]"
    )
  return all_points


def query_window_metrics(
    fetcher: JsonFetcher,
    project: str,
    instance_id: str,
    start: datetime.datetime,
    end: datetime.datetime,
) -> dict[str, Any]:
  """Queries both VM CPU utilization and Ops Agent used memory percentage."""
  padded_start, padded_end = pad_window_to_minutes(start, end)
  cpu_filter = (
      f'metric.type = "{CPU_METRIC}" AND '
      f'resource.labels.instance_id = "{instance_id}"'
  )
  mem_filter = (
      f'metric.type = "{MEMORY_METRIC}" AND '
      'metric.labels.state = "used" AND '
      f'resource.labels.instance_id = "{instance_id}"'
  )
  cpu_points = fetch_series_points(
      fetcher, project, cpu_filter, padded_start, padded_end
  )
  mem_points = fetch_series_points(
      fetcher, project, mem_filter, padded_start, padded_end
  )
  cpu_vals = [v for _, v in cpu_points]
  mem_vals = [v for _, v in mem_points]
  cpu_avg = sum(cpu_vals) / len(cpu_vals)
  cpu_max = max(cpu_vals)
  mem_avg = sum(mem_vals) / len(mem_vals)
  mem_max = max(mem_vals)
  expected_points = int((padded_end - padded_start).total_seconds() // 60)
  report = {
      "project": project,
      "instance_id": instance_id,
      "raw_window": {
          "start_utc": format_utc_timestamp(start),
          "end_utc": format_utc_timestamp(end),
      },
      "aligned_window": {
          "start_utc": format_utc_timestamp(padded_start),
          "end_utc": format_utc_timestamp(padded_end),
          "alignment_period": "60s",
          "per_series_aligner": "ALIGN_MEAN",
          "expected_points": expected_points,
      },
      "cpu_utilization": {
          "metric": CPU_METRIC,
          "point_count": len(cpu_points),
          "avg_fraction": cpu_avg,
          "max_fraction": cpu_max,
          "avg_percent": cpu_avg * 100.0,
          "max_percent": cpu_max * 100.0,
          "points": [
              {"end_utc": ts, "fraction": val, "percent": val * 100.0}
              for ts, val in cpu_points
          ],
      },
      "memory_percent_used": {
          "metric": MEMORY_METRIC,
          "state": "used",
          "point_count": len(mem_points),
          "avg_percent": mem_avg,
          "max_percent": mem_max,
          "points": [
              {"end_utc": ts, "percent": val} for ts, val in mem_points
          ],
      },
  }
  if len(cpu_points) < expected_points or len(mem_points) < expected_points:
    raise IncompleteSeriesError(
        f"incomplete series: cpu {len(cpu_points)}/{expected_points}, "
        f"memory {len(mem_points)}/{expected_points} points in "
        f"[{format_utc_timestamp(padded_start)}, "
        f"{format_utc_timestamp(padded_end)}]",
        report,
    )
  return report


def fetch_metadata(path: str) -> str:
  """Reads one attribute from the GCE metadata server."""
  req = urllib.request.Request(
      f"{METADATA_ROOT}/{path}", headers={"Metadata-Flavor": "Google"}
  )
  with urllib.request.urlopen(req, timeout=5) as resp:
    return resp.read().decode("utf-8").strip()


def resolve_access_token(explicit_token: str) -> str:
  """Obtains an OAuth2 bearer token from `--access-token`, GCE MDS, or gcloud."""
  if explicit_token:
    return explicit_token
  try:
    raw_token = fetch_metadata("instance/service-accounts/default/token")
  except urllib.error.URLError:
    proc = subprocess.run(
        ["gcloud", "auth", "print-access-token"],
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip()
  token_payload = json.loads(raw_token)
  return str(token_payload["access_token"])


class BearerJsonClient:
  """Bearer-authenticated HTTP JSON client for Cloud Monitoring REST calls."""

  def __init__(self, token: str) -> None:
    self._auth = f"Bearer {token}"

  def __call__(self, url: str) -> Mapping[str, Any]:
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": self._auth,
            "Accept": "application/json",
        },
    )
    try:
      with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as err:
      detail = err.read().decode("utf-8", errors="replace")
      raise MonitoringError(
          f"HTTP {err.code} from Cloud Monitoring ({url}): {detail}"
      ) from err
    return json.loads(body)


def make_http_json_fetcher(access_token: str) -> JsonFetcher:
  """Creates a bearer-authenticated JSON GET helper for Cloud Monitoring."""
  return BearerJsonClient(access_token)


def load_window_from_summary(summary_path: str) -> tuple[str, str]:
  """Reads `window_start_utc` and `window_end_utc` from a bench summary JSON."""
  with open(summary_path, "r", encoding="utf-8") as fh:
    data = json.load(fh)
  return str(data["window_start_utc"]), str(data["window_end_utc"])


def main(argv: list[str] | None = None) -> int:
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument("--project", default="", help="GCP project ID")
  parser.add_argument("--instance-id", default="", help="Numeric GCE instance ID")
  parser.add_argument("--start", default="", help="Window start ISO-8601 UTC")
  parser.add_argument("--end", default="", help="Window end ISO-8601 UTC")
  parser.add_argument(
      "--summary-json",
      default="",
      help="Benchmark summary JSON containing window_start_utc/window_end_utc",
  )
  parser.add_argument("--out", default="", help="Output JSON file (stdout if unset)")
  parser.add_argument(
      "--wait-secs",
      type=int,
      default=0,
      help="Retry up to N seconds while waiting for 60s Monitoring points to flush",
  )
  parser.add_argument("--access-token", default="", help="Explicit OAuth2 token")
  args = parser.parse_args(argv)

  start_str, end_str = args.start, args.end
  if args.summary_json:
    s_start, s_end = load_window_from_summary(args.summary_json)
    start_str = start_str or s_start
    end_str = end_str or s_end
  if not start_str or not end_str:
    parser.error("provide --start and --end (or --summary-json)")

  start_dt = parse_utc_timestamp(start_str)
  end_dt = parse_utc_timestamp(end_str)
  project = args.project or fetch_metadata("project/project-id")
  instance_id = args.instance_id or fetch_metadata("instance/id")
  deadline = time.monotonic() + max(0, args.wait_secs)

  while True:
    token = resolve_access_token(args.access_token)
    fetcher = make_http_json_fetcher(token)
    try:
      report = query_window_metrics(
          fetcher, project, instance_id, start_dt, end_dt
      )
      break
    except IncompleteSeriesError as err:
      if time.monotonic() >= deadline:
        # Keep the partial report: `analyze.py` marks series whose
        # `point_count` is below `expected_points`, which is more useful than
        # a missing cell.
        print(f"monitoring: {err}; writing partial report", file=sys.stderr)
        report = err.report
        break
      time.sleep(15)
    except MonitoringError as err:
      if time.monotonic() >= deadline:
        print(f"monitoring: {err}", file=sys.stderr)
        return 1
      time.sleep(15)

  serialized = json.dumps(report, indent=2) + "\n"
  if args.out:
    with open(args.out, "w", encoding="utf-8") as fh:
      fh.write(serialized)
  else:
    sys.stdout.write(serialized)
  return 0


if __name__ == "__main__":
  sys.exit(main())
