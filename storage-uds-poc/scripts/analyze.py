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
"""Summarizes a rev-4 benchmark results directory into Markdown tables.

Usage: analyze.py RESULTS_DIR > summary.md

Reads `<cell>-<mode>.summary.json`, `<cell>-<mode>.monitoring.json` (when
present), and `<cell>-<mode>.path.json` for every cell in `RESULTS_DIR`, checks
Cloudpath verification (`verified == True` and `0` DirectPath peers), and
renders side-by-side comparison tables across `native`, `uds-sidecar`, and
`uds-inproc` (plan rev 4 §7-§8).
"""

import glob
import json
import os
import sys
from typing import Any, Mapping, Sequence

MODES = ("native", "uds-sidecar", "uds-inproc")


def load_json_if_exists(path: str) -> Mapping[str, Any] | None:
  if not os.path.exists(path):
    return None
  with open(path, "r", encoding="utf-8") as fh:
    return json.load(fh)


def discover_cells(results_dir: str) -> dict[str, dict[str, dict[str, Any]]]:
  """Groups summary, monitoring, and path reports by `(cell_prefix, mode)`."""
  cells: dict[str, dict[str, dict[str, Any]]] = {}
  pattern = os.path.join(results_dir, "*.summary.json")
  for summary_path in sorted(glob.glob(pattern)):
    stem = os.path.basename(summary_path)[: -len(".summary.json")]
    matched_mode = ""
    cell_name = ""
    for mode in MODES:
      suffix = f"-{mode}"
      if stem.endswith(suffix):
        matched_mode = mode
        cell_name = stem[: -len(suffix)]
        break
    if not matched_mode:
      continue
    with open(summary_path, "r", encoding="utf-8") as fh:
      summary = json.load(fh)
    mon = load_json_if_exists(os.path.join(results_dir, f"{stem}.monitoring.json"))
    path_rep = load_json_if_exists(os.path.join(results_dir, f"{stem}.path.json"))
    cells.setdefault(cell_name, {})[matched_mode] = {
        "summary": summary,
        "monitoring": mon,
        "path": path_rep,
    }
  return cells


def format_ms(us: float) -> str:
  return f"{us / 1000.0:.2f}"


def format_ratio(num: float, den: float) -> str:
  if den <= 0.0:
    return "n/a"
  return f"{num / den:.2f}x"


def render_markdown(
    results_dir: str, cells: Mapping[str, Mapping[str, Mapping[str, Any]]]
) -> str:
  lines: list[str] = []
  info_path = os.path.join(results_dir, "run-info.txt")
  lines.append(f"# Benchmark Summary (`{os.path.basename(os.path.abspath(results_dir))}`)")
  lines.append("")
  if os.path.exists(info_path):
    with open(info_path, "r", encoding="utf-8") as fh:
      info = fh.read().strip()
    lines.append("```text")
    lines.append(info)
    lines.append("```")
    lines.append("")

  lines.append("## Primary Metrics (Latency, Throughput, Cloud Monitoring VM CPU & Peak Memory)")
  lines.append("")
  lines.append(
      "| Cell | Comparator | Path | Ops (ok/err) | P50 (ms) | P90 (ms) | P99 (ms) "
      "| P50 vs native | MB/s | MiB/s | VM CPU avg % | VM CPU max % | VM Peak Mem % |"
  )
  lines.append("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")

  for cell_name, by_mode in cells.items():
    native_p50 = 0.0
    if "native" in by_mode:
      native_p50 = float(by_mode["native"]["summary"]["latency_us"]["p50"])
    for mode in MODES:
      if mode not in by_mode:
        continue
      entry = by_mode[mode]
      s = entry["summary"]
      lat = s["latency_us"]
      mon = entry["monitoring"]
      path_rep = entry["path"]
      verified = bool(path_rep and path_rep.get("verified"))
      path_str = "Cloudpath" if verified else ("UNVERIFIED" if path_rep else "n/a")
      cpu_avg = (
          f"{mon['cpu_utilization']['avg_percent']:.2f}%"
          if mon
          else "n/a"
      )
      cpu_max = (
          f"{mon['cpu_utilization']['max_percent']:.2f}%"
          if mon
          else "n/a"
      )
      mem_max = (
          f"{mon['memory_percent_used']['max_percent']:.2f}%"
          if mon
          else "n/a"
      )
      ratio = "1.00x" if mode == "native" else format_ratio(float(lat["p50"]), native_p50)
      lines.append(
          f"| `{cell_name}` | `{mode}` | {path_str} | "
          f"{s['ops_ok']}/{s['ops_err']} | "
          f"{format_ms(float(lat['p50']))} | {format_ms(float(lat['p90']))} | "
          f"{format_ms(float(lat['p99']))} | {ratio} | "
          f"{float(s['throughput_mb_s']):.2f} | {float(s['throughput_mib_s']):.2f} | "
          f"{cpu_avg} | {cpu_max} | {mem_max} |"
      )

  lines.append("")
  lines.append("## Secondary Process Attribution (`getrusage` CPU & `VmHWM` Peak RSS)")
  lines.append("")
  lines.append(
      "| Cell | Comparator | App CPU (s) | App Peak RSS (MiB) | "
      "Server CPU (s) | Server Peak RSS (MiB) | Server Threads |"
  )
  lines.append("|---|---|---:|---:|---:|---:|---:|")

  for cell_name, by_mode in cells.items():
    for mode in MODES:
      if mode not in by_mode:
        continue
      s = by_mode[mode]["summary"]
      app = s["app_process"]
      srv = s.get("server_process")
      app_peak_mib = float(app["peak_rss_kib"]) / 1024.0
      if srv:
        srv_cpu = float(srv["cpu_total_secs"])
        srv_peak_mib = float(srv["peak_rss_bytes"]) / (1024.0 * 1024.0)
        srv_threads = str(srv["threads"])
        app_cpu = float(app["cpu_total_secs"])
        if srv.get("shares_app_process"):
          app_cpu = max(0.0, app_cpu - srv_cpu)
          srv_peak_str = "shared"
        else:
          srv_peak_str = f"{srv_peak_mib:.1f}"
        lines.append(
            f"| `{cell_name}` | `{mode}` | {app_cpu:.2f} | {app_peak_mib:.1f} | "
            f"{srv_cpu:.2f} | {srv_peak_str} | {srv_threads} |"
        )
      else:
        lines.append(
            f"| `{cell_name}` | `{mode}` | {float(app['cpu_total_secs']):.2f} | "
            f"{app_peak_mib:.1f} | n/a | n/a | n/a |"
        )

  has_range = any(
      int(by_mode[m]["summary"].get("range_latency_us", {}).get("count", 0)) > 0
      for by_mode in cells.values()
      for m in by_mode
  )
  if has_range:
    lines.append("")
    lines.append("## Per-Range Latency (W4 Streaming Read)")
    lines.append("")
    lines.append(
        "| Cell | Comparator | Ranges | Range P50 (ms) | Range P90 (ms) | Range P99 (ms) |"
    )
    lines.append("|---|---|---:|---:|---:|---:|")
    for cell_name, by_mode in cells.items():
      for mode in MODES:
        if mode not in by_mode:
          continue
        rlat = by_mode[mode]["summary"].get("range_latency_us", {})
        if int(rlat.get("count", 0)) <= 0:
          continue
        lines.append(
            f"| `{cell_name}` | `{mode}` | {rlat['count']} | "
            f"{format_ms(float(rlat['p50']))} | {format_ms(float(rlat['p90']))} | "
            f"{format_ms(float(rlat['p99']))} |"
        )

  lines.append("")
  return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
  args = list(sys.argv[1:] if argv is None else argv)
  if len(args) != 1:
    print(__doc__, file=sys.stderr)
    return 2
  cells = discover_cells(args[0])
  if not cells:
    print(f"analyze: no *.summary.json files found in {args[0]!r}", file=sys.stderr)
    return 1
  sys.stdout.write(render_markdown(args[0], cells))
  return 0


if __name__ == "__main__":
  sys.exit(main())
