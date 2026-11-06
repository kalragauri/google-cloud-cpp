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
  analyze.py RERUN_DIR --base BASE_RESULTS_DIR [--doc-tables] > merged.md

Reads `<cell>-<mode>.summary.json`, `<cell>-<mode>.monitoring.json` (when
present), and `<cell>-<mode>.path.json` for every cell in `RESULTS_DIR`, checks
Cloudpath verification (`verified == True` and `0` DirectPath peers), and
renders side-by-side comparison tables across `native`, `uds-sidecar`, and
`uds-inproc`.

`--doc-tables` renders one side-by-side table per workload and bucket type:
Native | uds-sidecar |
% Diff | uds-inproc | % Diff for latency (P50 / P90 / P99), throughput (MB/s
for the bulk-data workloads, ops/s for the rate-bound ones), Cloud Monitoring
CPU and peak memory. The latency basis is decided once per cell and named in
the row label: W4 (`bidi-read-48t`) reports the per-range latency (13 ranges
per 200 MiB object: twelve of 16 MiB and a final 8 MiB range), which is the
quantity the other language columns report; every other workload reports
per-operation latency, and the primary table keeps per-object latency for W4.
Monitoring values whose window is missing 60 s buckets are marked with `‡`.

`--sensitivity BASE_DIR` compares the UDS comparators of `WT_RESULTS_DIR` (a
`--worker-threads=N` run) against the same cells in `BASE_DIR` (default worker
threads). The two modes are mutually exclusive.

`--base BASE_DIR` renders `BASE_DIR` with every cell that `RESULTS_DIR` also
contains replaced, whole, by the `RESULTS_DIR` measurement (a targeted re-run),
and lists which run each cell came from together with both `run-info.txt`
blocks. It combines with the summary and `--doc-tables`, not `--sensitivity`.
"""

import argparse
import datetime
import glob
import json
import os
import sys
from typing import Any, Mapping, Sequence

MODES = ("native", "uds-sidecar", "uds-inproc")
UDS_MODES = ("uds-sidecar", "uds-inproc")

# Section titles, keyed by the workload prefix of the cell name.
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

# Throughput unit per workload. Bulk-data workloads are bandwidth-bound and
# report MB/s; the others move < 1 MiB per operation and report ops/s. Keyed by
# workload so every comparator in a cell uses the same unit regardless of what
# it managed to move.
BULK_DATA_WORKLOADS = frozenset(
    {"read50x2x4-serial", "read50x2x4-concurrent", "bidi-read-48t",
     "bidi-write-48t"})

# Bucket locations are not in the summary JSON; the report states them.
BUCKET_LABELS = {
    "zonal": "Zonal (RAPID)",
    "regional": "Standard Regional",
    "project": "Project-scoped",
}

MONITORING_INCOMPLETE_MARK = "‡"


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


def read_run_info(results_dir: str) -> str | None:
  info_path = os.path.join(results_dir, "run-info.txt")
  if not os.path.exists(info_path):
    return None
  with open(info_path, "r", encoding="utf-8") as fh:
    return fh.read().strip()


def overlay_cells(
    base_cells: Mapping[str, Mapping[str, Mapping[str, Any]]],
    new_cells: Mapping[str, Mapping[str, Mapping[str, Any]]],
) -> dict[str, Mapping[str, Mapping[str, Any]]]:
  """Returns `base_cells` with every cell present in `new_cells` replaced.

  A cell is replaced as a whole (all comparators), never merged per mode: a
  table must not mix comparators measured by different harness revisions.
  Replaced cells keep their position; cells only in `new_cells` are appended.
  """
  merged: dict[str, Mapping[str, Mapping[str, Any]]] = dict(base_cells)
  merged.update(new_cells)
  return merged


def render_provenance(base_dir: str, overlay_dir: str,
                      overlaid: Sequence[str]) -> list[str]:
  """Lists which run each cell came from, with both `run-info.txt` blocks."""
  base_name = os.path.basename(os.path.abspath(base_dir))
  overlay_name = os.path.basename(os.path.abspath(overlay_dir))
  lines = [
      f"Cells re-measured in `{overlay_name}` replace the same cells of "
      f"`{base_name}`: " + ", ".join(f"`{c}`" for c in sorted(overlaid)) +
      f". All other cells come from `{base_name}`.",
      "",
  ]
  for label, directory in (("Base run", base_dir), ("Overlay run", overlay_dir)):
    info = read_run_info(directory)
    if info is None:
      continue
    lines += [f"{label} (`{os.path.basename(os.path.abspath(directory))}`):",
              "", "```text", info, "```", ""]
  return lines


def render_markdown(
    results_dir: str, cells: Mapping[str, Mapping[str, Mapping[str, Any]]],
    provenance: Sequence[str] = ()
) -> str:
  lines: list[str] = []
  lines.append(f"# Benchmark Summary (`{os.path.basename(os.path.abspath(results_dir))}`)")
  lines.append("")
  info = read_run_info(results_dir)
  if provenance:
    lines.extend(provenance)
  elif info is not None:
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


def has_range_latency(summary: Mapping[str, Any]) -> bool:
  return (summary.get("workload") == "bidi-read-48t" and
          int(summary.get("range_latency_us", {}).get("count", 0)) > 0)


def latency_basis(entries: Sequence[Mapping[str, Any]]) -> str:
  """Decides, once per cell, which latency distribution a table row reports.

  W4 streams each 200 MiB object as 13 ranges (twelve of 16 MiB, one of 8 MiB);
  the other language columns report the per-range latency, so W4 uses it when
  *every* comparator in the cell recorded ranges. Mixing a per-range value for
  one comparator with a per-object value for another would make the `% Diff`
  meaningless, so a cell where any comparator lacks ranges falls back to
  per-operation latency for all of them.
  """
  if entries and all(has_range_latency(e["summary"]) for e in entries):
    return "per range"
  return "per op"


def doc_latency(summary: Mapping[str, Any], basis: str) -> Mapping[str, Any]:
  if basis == "per range":
    return summary["range_latency_us"]
  return summary["latency_us"]


def doc_throughput(summary: Mapping[str, Any]) -> tuple[float, str]:
  """Returns `(value, unit)`: MB/s for bulk-data workloads, ops/s otherwise.

  The unit is a property of the workload (`BULK_DATA_WORKLOADS`), not of what
  a comparator happened to move, so a failed or empty cell cannot flip a row
  from MB/s to ops/s.
  """
  wall = float(summary["window_wall_secs"])
  if summary.get("workload") in BULK_DATA_WORKLOADS:
    return float(summary["throughput_mb_s"]), "MB/s"
  ops = float(summary["ops_ok"]) / wall if wall > 0.0 else 0.0
  return ops, "ops/s"


def monitoring_expected_points(mon: Mapping[str, Any]) -> int | None:
  """Number of 60 s buckets the aligned window should contain, or None."""
  window = mon.get("aligned_window") or {}
  try:
    start = datetime.datetime.fromisoformat(
        str(window["start_utc"]).replace("Z", "+00:00"))
    end = datetime.datetime.fromisoformat(
        str(window["end_utc"]).replace("Z", "+00:00"))
  except (KeyError, ValueError):
    return None
  return max(0, int((end - start).total_seconds() // 60))


def monitoring_value(mon: Mapping[str, Any] | None, series: str,
                     field: str) -> tuple[float | None, bool]:
  """Returns `(value, complete)`; `complete` is False when the series has fewer
  points than the aligned window has 60 s buckets (a late Monitoring flush)."""
  if not mon or series not in mon:
    return None, True
  expected = monitoring_expected_points(mon)
  count = int(mon[series].get("point_count", 0))
  complete = expected is None or count >= expected
  return float(mon[series][field]), complete


def percent_display(value: float | None, complete: bool) -> str:
  if value is None:
    return "n/a"
  return f"{value:.2f}%" + ("" if complete else MONITORING_INCOMPLETE_MARK)


def doc_metric_rows(entry: Mapping[str, Any],
                    basis: str) -> list[tuple[str, str, float | None]]:
  """Rows `(metric, display, numeric value for % diff)` for one comparator."""
  s = entry["summary"]
  lat = doc_latency(s, basis)
  tput, unit = doc_throughput(s)
  cpu, cpu_ok = monitoring_value(entry["monitoring"], "cpu_utilization",
                                 "avg_percent")
  mem, mem_ok = monitoring_value(entry["monitoring"], "memory_percent_used",
                                 "max_percent")
  rows: list[tuple[str, str, float | None]] = []
  for pct in ("p50", "p90", "p99"):
    rows.append((f"Latency {pct.upper()} ({basis})",
                 format_latency(float(lat[pct])), float(lat[pct])))
  if unit == "MB/s":
    tput_display = f"{tput:,.1f} {unit}"
  else:
    tput_display = f"{tput:,.2f} {unit}"
  rows.append(("Throughput", tput_display, tput))
  rows.append(("CPU Utilization (`instance/cpu/utilization`, avg)",
               percent_display(cpu, cpu_ok), cpu))
  rows.append(("Peak Memory (`memory/percent_used`, max)",
               percent_display(mem, mem_ok), mem))
  rows.append(("Ops (ok / err)", f"{s['ops_ok']} / {s['ops_err']}", None))
  return rows


def render_doc_tables(
    results_dir: str, cells: Mapping[str, Mapping[str, Mapping[str, Any]]],
    provenance: Sequence[str] = ()
) -> str:
  """Renders the side-by-side per-workload tables for one results dir."""
  lines: list[str] = []
  lines.append(
      f"# Per-workload tables (`{os.path.basename(os.path.abspath(results_dir))}`)")
  lines.append("")
  if provenance:
    lines.extend(provenance)
  lines.append(
      "`% Diff` is `(UDS - Native) / Native`; negative latency, CPU, and "
      "memory deltas and positive throughput deltas favor the UDS comparator. "
      "Latency rows name their basis: `per op` is one benchmark operation; "
      "`per range` (W4) is one `ObjectDescriptor::Read` range, 13 per 200 MiB "
      "object (twelve of 16 MiB and a final 8 MiB range). "
      f"`{MONITORING_INCOMPLETE_MARK}` marks a Monitoring series with fewer "
      "points than the window has 60 s buckets.")
  for cell_name in sorted(cells, key=workload_order):
    by_mode = cells[cell_name]
    workload, bucket_type = split_cell_name(cell_name)
    lines.append("")
    lines.append(f"### {workload_title(workload)}")
    lines.append("")
    bucket = str(by_mode[next(iter(by_mode))]["summary"].get("bucket", ""))
    bucket_note = f" `gs://{bucket}`" if bucket else ""
    lines.append(
        f"Bucket: {BUCKET_LABELS.get(bucket_type, bucket_type)}{bucket_note} "
        f"· cell `{cell_name}`")
    lines.append("")
    if "native" not in by_mode:
      lines.append("(no native cell in this results directory; skipped)")
      continue
    present_uds = [m for m in UDS_MODES if m in by_mode]
    basis = latency_basis([by_mode[m] for m in ["native", *present_uds]])
    header = "| Metric | Native C++ |"
    sep = "|---|---:|"
    for mode in present_uds:
      header += f" `{mode}` | % Diff |"
      sep += "---:|---:|"
    lines.append(header)
    lines.append(sep)
    native_rows = doc_metric_rows(by_mode["native"], basis)
    uds_rows = {m: doc_metric_rows(by_mode[m], basis) for m in present_uds}
    for idx, (metric, native_display, native_value) in enumerate(native_rows):
      row = f"| {metric} | {native_display} |"
      for mode in present_uds:
        _, display, value = uds_rows[mode][idx]
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
      "`% Diff` is `(worker-thread run - base) / base`. Latency rows name "
      "their basis (`per op`, or `per range` for W4: 13 ranges per 200 MiB "
      f"object, the last one 8 MiB). `{MONITORING_INCOMPLETE_MARK}` marks an "
      "incomplete Monitoring series.")
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
      basis = latency_basis([base_entry, wt_entry])
      base_rows = doc_metric_rows(base_entry, basis)
      wt_rows = doc_metric_rows(wt_entry, basis)
      for (metric, base_display, base_value), (_, wt_display,
                                               wt_value) in zip(base_rows,
                                                                wt_rows):
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
  modes = parser.add_mutually_exclusive_group()
  modes.add_argument("--doc-tables", action="store_true",
                     help="render side-by-side per-workload comparison tables")
  modes.add_argument("--sensitivity", metavar="BASE_DIR",
                     help="compare UDS comparators of RESULTS_DIR against "
                     "the same cells in BASE_DIR")
  parser.add_argument("--base", metavar="BASE_DIR",
                      help="render BASE_DIR with every cell present in "
                      "RESULTS_DIR replaced by the RESULTS_DIR measurement")
  args = parser.parse_args(argv)
  if args.sensitivity is not None and not args.sensitivity:
    parser.error("--sensitivity requires a non-empty BASE_DIR")
  if args.base is not None and not args.base:
    parser.error("--base requires a non-empty BASE_DIR")
  if args.base and args.sensitivity:
    parser.error("--base cannot be combined with --sensitivity")

  cells = discover_cells(args.results_dir)
  if not cells:
    print(f"analyze: no *.summary.json files found in {args.results_dir!r}",
          file=sys.stderr)
    return 1
  provenance: list[str] = []
  if args.base:
    base_cells = discover_cells(args.base)
    if not base_cells:
      print(f"analyze: no *.summary.json files found in {args.base!r}",
            file=sys.stderr)
      return 1
    provenance = render_provenance(args.base, args.results_dir, list(cells))
    cells = overlay_cells(base_cells, cells)
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
    sys.stdout.write(render_doc_tables(args.results_dir, cells, provenance))
    return 0
  sys.stdout.write(render_markdown(args.results_dir, cells, provenance))
  return 0


if __name__ == "__main__":
  sys.exit(main())
