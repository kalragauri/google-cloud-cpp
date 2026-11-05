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

Usage:
  analyze.py RESULTS_DIR > summary.md
  analyze.py RESULTS_DIR --doc-tables > doc_tables.md
  analyze.py WT_RESULTS_DIR --sensitivity BASE_RESULTS_DIR > sensitivity.md

Reads `<cell>-<mode>.summary.json`, `<cell>-<mode>.monitoring.json` (when
present), and `<cell>-<mode>.path.json` for every cell in `RESULTS_DIR`, checks
Cloudpath verification (`verified == True` and `0` DirectPath peers), and
renders side-by-side comparison tables across `native`, `uds-sidecar`, and
`uds-inproc` (plan rev 4 §7-§8).

`--doc-tables` renders one table per workload and bucket type in the layout of
the consolidation worklog ("Benchmarking Results"): Native | uds-sidecar |
% Diff | uds-inproc | % Diff for latency (P50 / P90 / P99), throughput (MB/s,
or ops/s for metadata-only workloads), Cloud Monitoring CPU and peak memory.
W4 (`bidi-read-48t`) reports the per-16 MiB-range latency there, which is the
quantity the other language columns report; the per-object latency stays in
the primary table.

`--sensitivity BASE_DIR` compares the UDS comparators of `WT_RESULTS_DIR` (a
`--worker-threads=N` run) against the same cells in `BASE_DIR` (default worker
threads).
"""

import argparse
import glob
import json
import os
import sys
from typing import Any, Mapping, Sequence

MODES = ("native", "uds-sidecar", "uds-inproc")
UDS_MODES = ("uds-sidecar", "uds-inproc")

# Worklog section titles, keyed by the workload prefix of the cell name.
WORKLOAD_TITLES = (
    ("open", "W1 — Open a 200 MiB object (`AsyncClient::Open`)"),
    ("read50x2x4-serial",
     "W2a — 50 × 2 MiB ranges at a 4 MiB stride, serial"),
    ("read50x2x4-concurrent",
     "W2b — 50 × 2 MiB ranges at a 4 MiB stride, concurrent"),
    ("random8k", "W3 — Random 8 KiB ranges on an open 100 MiB object"),
    ("bidi-read-48t",
     "W4 — Read 16 MiB chunks of a 200 MiB object via BidiRead, 48 threads, "
     "5 min"),
    ("bidi-write-48t",
     "W5 — Write 200 MB objects via BidiWrite, 48 threads, 5 min"),
    ("list-objects-100", "W6a — ListObjects, 100 items"),
    ("list-buckets-100", "W6b — ListBuckets, 100 items, project-scoped"),
)

BUCKET_LABELS = {
    "zonal": "Zonal (`us-west1-a`, RAPID)",
    "regional": "Standard Regional (`us-west1`)",
    "project": "Project-scoped",
}


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


def split_cell_name(cell_name: str) -> tuple[str, str]:
  """Splits `<workload>-<bucket type>` into its two parts.

  Cell names are built by `vm_run.sh` as `${workload}-${btype}` where the
  bucket type is the last dash-separated token (`zonal`, `regional`, or
  `project` for the bucket listing).
  """
  workload, _, bucket_type = cell_name.rpartition("-")
  return workload, bucket_type


def workload_title(workload: str) -> str:
  for prefix, title in WORKLOAD_TITLES:
    if workload == prefix:
      return title
  return workload


def workload_order(cell_name: str) -> tuple[int, str]:
  workload, _ = split_cell_name(cell_name)
  for idx, (prefix, _) in enumerate(WORKLOAD_TITLES):
    if workload == prefix:
      return idx, cell_name
  return len(WORKLOAD_TITLES), cell_name


def format_latency(us: float) -> str:
  if us >= 1e6:
    return f"{us / 1e6:.2f} s"
  return f"{us / 1e3:.1f} ms"


def pct_diff(value: float, base: float) -> str:
  if base <= 0.0:
    return "n/a"
  return f"{(value - base) / base * 100.0:+.1f}%"


def doc_latency(summary: Mapping[str, Any]) -> Mapping[str, Any]:
  """Latency distribution to report in the worklog table.

  W4 streams a 200 MiB object as 16 MiB ranges; the other language columns
  report the per-range latency, so the per-range distribution is used when
  present. Every other workload reports its per-operation latency.
  """
  rlat = summary.get("range_latency_us", {})
  if (summary.get("workload") == "bidi-read-48t" and
      int(rlat.get("count", 0)) > 0):
    return rlat
  return summary["latency_us"]


def doc_throughput(summary: Mapping[str, Any]) -> tuple[float, str]:
  """Returns `(value, label)`: MB/s for bulk-data workloads, ops/s otherwise.

  Operations that move less than 1 MiB each (`open`, the 8 KiB random reads,
  and the listings) are rate-bound, not bandwidth-bound, so ops/s is the
  figure the worklog asks for ("MB/s or operations per second").
  """
  wall = float(summary["window_wall_secs"])
  ops_ok = int(summary["ops_ok"])
  bytes_per_op = int(summary["total_bytes"]) / max(ops_ok, 1)
  if bytes_per_op >= 1024 * 1024:
    return float(summary["throughput_mb_s"]), "MB/s"
  ops = float(ops_ok) / wall if wall > 0.0 else 0.0
  return ops, "ops/s"


def monitoring_value(mon: Mapping[str, Any] | None, series: str,
                     field: str) -> float | None:
  if not mon:
    return None
  return float(mon[series][field])


def doc_metric_rows(
    entry: Mapping[str, Any]) -> list[tuple[str, str, float | None, bool]]:
  """Rows `(metric, display, numeric value for % diff, lower is better)`."""
  s = entry["summary"]
  lat = doc_latency(s)
  tput, unit = doc_throughput(s)
  cpu = monitoring_value(entry["monitoring"], "cpu_utilization", "avg_percent")
  mem = monitoring_value(entry["monitoring"], "memory_percent_used",
                         "max_percent")
  rows: list[tuple[str, str, float | None, bool]] = []
  for pct in ("p50", "p90", "p99"):
    rows.append((f"Latency {pct.upper()}", format_latency(float(lat[pct])),
                 float(lat[pct]), True))
  if unit == "MB/s":
    tput_display = f"{tput:,.1f} {unit}"
  else:
    tput_display = f"{tput:,.2f} {unit}"
  rows.append(("Throughput", tput_display, tput, False))
  rows.append(("CPU Utilization (`instance/cpu/utilization`, avg)",
               "n/a" if cpu is None else f"{cpu:.2f}%", cpu, True))
  rows.append(("Peak Memory (`memory/percent_used`, max)",
               "n/a" if mem is None else f"{mem:.2f}%", mem, True))
  rows.append(("Ops (ok / err)", f"{s['ops_ok']} / {s['ops_err']}", None,
               False))
  return rows


def render_doc_tables(
    results_dir: str, cells: Mapping[str, Mapping[str, Mapping[str, Any]]]
) -> str:
  """Renders the worklog "Benchmarking Results" tables for one results dir."""
  lines: list[str] = []
  lines.append(
      f"# Worklog tables (`{os.path.basename(os.path.abspath(results_dir))}`)")
  lines.append("")
  lines.append(
      "`% Diff` is `(UDS - Native) / Native`; negative latency, CPU, and "
      "memory deltas and positive throughput deltas favor the UDS comparator. "
      "W4 latency is per 16 MiB range.")
  for cell_name in sorted(cells, key=workload_order):
    by_mode = cells[cell_name]
    if "native" not in by_mode:
      continue
    workload, bucket_type = split_cell_name(cell_name)
    lines.append("")
    lines.append(f"### {workload_title(workload)}")
    lines.append("")
    lines.append(
        f"Bucket: {BUCKET_LABELS.get(bucket_type, bucket_type)} · cell "
        f"`{cell_name}`")
    lines.append("")
    header = "| Metric | Native C++ |"
    sep = "|---|---:|"
    present_uds = [m for m in UDS_MODES if m in by_mode]
    for mode in present_uds:
      header += f" `{mode}` | % Diff |"
      sep += "---:|---:|"
    lines.append(header)
    lines.append(sep)
    native_rows = doc_metric_rows(by_mode["native"])
    uds_rows = {m: doc_metric_rows(by_mode[m]) for m in present_uds}
    for idx, (metric, native_display, native_value,
              _) in enumerate(native_rows):
      row = f"| {metric} | {native_display} |"
      for mode in present_uds:
        _, display, value, _ = uds_rows[mode][idx]
        diff = ("" if native_value is None or value is None else
                pct_diff(value, native_value))
        row += f" {display} | {diff} |"
      lines.append(row)
  lines.append("")
  return "\n".join(lines)


def render_sensitivity(
    base_dir: str, base_cells: Mapping[str, Mapping[str, Mapping[str, Any]]],
    wt_dir: str, wt_cells: Mapping[str, Mapping[str, Mapping[str, Any]]]
) -> str:
  """Compares UDS comparators of a `--worker-threads=N` run to the base run."""
  lines: list[str] = []
  wt_name = os.path.basename(os.path.abspath(wt_dir))
  base_name = os.path.basename(os.path.abspath(base_dir))
  lines.append(f"# Worker-thread sensitivity (`{wt_name}` vs `{base_name}`)")
  lines.append("")
  lines.append(
      "| Cell | Comparator | Metric | Base | Worker-thread run | % Diff |")
  lines.append("|---|---|---|---:|---:|---:|")
  for cell_name in sorted(wt_cells, key=workload_order):
    for mode in UDS_MODES:
      if mode not in wt_cells[cell_name]:
        continue
      if cell_name not in base_cells or mode not in base_cells[cell_name]:
        lines.append(f"| `{cell_name}` | `{mode}` | (no base cell) | | | |")
        continue
      base_entry = base_cells[cell_name][mode]
      wt_entry = wt_cells[cell_name][mode]
      base_rows = doc_metric_rows(base_entry)
      wt_rows = doc_metric_rows(wt_entry)
      for (metric, base_display, base_value, _), (_, wt_display, wt_value,
                                                  _) in zip(base_rows, wt_rows):
        diff = ("" if base_value is None or wt_value is None else
                pct_diff(wt_value, base_value))
        lines.append(f"| `{cell_name}` | `{mode}` | {metric} | "
                     f"{base_display} | {wt_display} | {diff} |")
      base_srv = base_entry["summary"].get("server_process") or {}
      wt_srv = wt_entry["summary"].get("server_process") or {}
      lines.append(f"| `{cell_name}` | `{mode}` | Server threads | "
                   f"{base_srv.get('threads', 'n/a')} | "
                   f"{wt_srv.get('threads', 'n/a')} | |")
      base_wt = base_entry["summary"].get("worker_threads", "n/a")
      wt_wt = wt_entry["summary"].get("worker_threads", "n/a")
      lines.append(f"| `{cell_name}` | `{mode}` | `--worker-threads` | "
                   f"{base_wt} | {wt_wt} | |")
  lines.append("")
  return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
  parser = argparse.ArgumentParser(
      description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
  parser.add_argument("results_dir")
  parser.add_argument("--doc-tables", action="store_true",
                      help="render the worklog 'Benchmarking Results' layout")
  parser.add_argument("--sensitivity", metavar="BASE_DIR",
                      help="compare UDS comparators of RESULTS_DIR against "
                      "the same cells in BASE_DIR")
  args = parser.parse_args(argv)

  cells = discover_cells(args.results_dir)
  if not cells:
    print(f"analyze: no *.summary.json files found in {args.results_dir!r}",
          file=sys.stderr)
    return 1
  if args.sensitivity:
    base_cells = discover_cells(args.sensitivity)
    if not base_cells:
      print(f"analyze: no *.summary.json files found in {args.sensitivity!r}",
            file=sys.stderr)
      return 1
    sys.stdout.write(render_sensitivity(args.sensitivity, base_cells,
                                        args.results_dir, cells))
    return 0
  if args.doc_tables:
    sys.stdout.write(render_doc_tables(args.results_dir, cells))
    return 0
  sys.stdout.write(render_markdown(args.results_dir, cells))
  return 0


if __name__ == "__main__":
  sys.exit(main())
