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
"""Summarizes a results directory into the plan's comparison tables.

Usage: analyze.py RESULTS_DIR > summary.md

Ratios are computed only inside a comparison pair (same network path and
protocol family, plan §7.2). Rows whose runs failed path verification are
excluded and listed. Units follow the ACv2 benchmark spec: latency ms,
CPU ns/byte (app + sidecar), allocations bytes/byte, peak RSS MiB.
"""

import collections
import csv
import glob
import json
import os
import statistics
import sys

# (title, workload, ops, POC impls, baseline impl, pure-rust impls, context impls)
CELLS = [
    (
        "Regional · JSON · WriteObject",
        "w1r3",
        ["SINGLE-SHOT", "RESUMABLE"],
        ["uds", "uds-ch4"],
        "native-json-h1",
        ["pure-rust", "pure-rust-unbuffered"],
        ["native-json-default", "native-grpc-cfe"],
    ),
    (
        "Regional · JSON · ReadObject",
        "w1r3",
        ["READ[0]", "READ[1]", "READ[2]"],
        ["uds", "uds-ch4"],
        "native-json-h1",
        ["pure-rust", "pure-rust-unbuffered"],
        ["native-json-default", "native-grpc-cfe"],
    ),
    (
        "Regional · gRPC · ListObjects",
        "list",
        ["LIST-P1000", "LIST-P100"],
        ["uds"],
        "native-grpc-cfe",
        ["pure-rust"],
        ["native-json-h1"],
    ),
    (
        "Zone · gRPC · ListObjects",
        "list",
        ["LIST-P1000", "LIST-P100"],
        ["uds"],
        "native-grpc-cfe",
        ["pure-rust"],
        ["native-json-h1"],
    ),
    (
        "Zone · gRPC · ReadObject (open)",
        "open",
        ["open"],
        ["uds"],
        "native-grpc-cfe",
        ["pure-rust"],
        [],
    ),
    (
        "Zone · gRPC · ReadObject (random8k)",
        "random8k",
        ["random8k"],
        ["uds"],
        "native-grpc-cfe",
        ["pure-rust"],
        [],
    ),
    (
        "Zone · gRPC · ReadObject (read50x2x4)",
        "read50x2x4",
        ["read50x2x4"],
        ["uds"],
        "native-grpc-cfe",
        ["pure-rust"],
        [],
    ),
    (
        "Zone · gRPC · ReadObject (full, bidi)",
        "appendable",
        ["READ-BIDI"],
        ["uds"],
        "native-grpc-cfe",
        ["pure-rust"],
        [],
    ),
    (
        "Zone · gRPC · WriteObject (appendable)",
        "appendable",
        ["APPENDABLE"],
        ["uds"],
        "native-grpc-cfe",
        ["pure-rust"],
        [],
    ),
]


def pct(values, p):
    if not values:
        return float("nan")
    v = sorted(values)
    return v[min(len(v) - 1, int(p / 100 * (len(v) - 1) + 0.5))]


def parse_labels(raw):
    out = {}
    for item in raw.split(";"):
        if "=" in item:
            k, v = item.split("=", 1)
            out[k] = v
    return out


def load(results):
    rows, bad_runs = [], []
    for path in sorted(glob.glob(os.path.join(results, "*.csv"))):
        base = path[:-4]
        report = base + ".path.json"
        verified = True
        if os.path.exists(report):
            with open(report) as f:
                r = json.load(f)
            if r.get("directpath"):
                verified = False
                bad_runs.append((os.path.basename(base), "DirectPath peers: %s" % r["directpath"]))
            elif r.get("verified") is False:
                verified = False
                bad_runs.append((os.path.basename(base), "unverified: no GCS peer observed"))
        run = os.path.basename(base)
        bucket_type = "zonal" if "-zonal-" in run or "bidi" in run else "regional"
        with open(path) as f:
            for row in csv.DictReader(f):
                row["run"], row["verified"], row["bucket_type"] = run, verified, bucket_type
                rows.append(row)
    return rows, bad_runs


def impl_key(row):
    # ssb_native names itself native-<sync>; bidi runs use AsyncClient but are
    # launched with --sync=grpc-cfe, so they share that name.
    return row["impl"]


def summarize_rows(rows):
    ok = [r for r in rows if r["ok"] == "1"]
    lat = [float(r["latency_ms"]) for r in ok]
    size = [max(1, int(r["bytes"]) or int(r["object_size"])) for r in ok]
    cpu = [float(r["cpu_ms_total"]) * 1e6 / s for r, s in zip(ok, size)]
    cpu_app = [float(r["cpu_ms_app"]) * 1e6 / s for r, s in zip(ok, size)]
    cpu_peer = [float(r["cpu_ms_peer"]) * 1e6 / s for r, s in zip(ok, size)]
    alloc = [(int(r["alloc_bytes_app"]) + int(r["alloc_bytes_peer"])) / s for r, s in zip(ok, size)]
    alloc_app = [int(r["alloc_bytes_app"]) / s for r, s in zip(ok, size)]
    alloc_peer = [int(r["alloc_bytes_peer"]) / s for r, s in zip(ok, size)]
    tput = [
        int(r["bytes"]) / 1048576 / (float(r["latency_ms"]) / 1000)
        for r in ok
        if int(r["bytes"]) > 0 and float(r["latency_ms"]) > 0
    ]
    return {
        "n": len(rows),
        "err": len(rows) - len(ok),
        "p50": pct(lat, 50),
        "p90": pct(lat, 90),
        "p99": pct(lat, 99),
        "cpu": statistics.median(cpu) if cpu else float("nan"),
        "cpu_app": statistics.median(cpu_app) if cpu_app else float("nan"),
        "cpu_peer": statistics.median(cpu_peer) if cpu_peer else float("nan"),
        "alloc": statistics.median(alloc) if alloc else float("nan"),
        "alloc_app": statistics.median(alloc_app) if alloc_app else float("nan"),
        "alloc_peer": statistics.median(alloc_peer) if alloc_peer else float("nan"),
        "tput": statistics.median(tput) if tput else float("nan"),
        "errors": collections.Counter(r["error"][:90] for r in rows if r["ok"] != "1"),
    }


def fmt(x, digits=2):
    return "–" if x != x else f"{x:.{digits}f}"


def ratio(a, b):
    return "–" if a != a or b != b or b == 0 else f"{a / b:.2f}×"


def main():
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    rows, bad_runs = load(sys.argv[1])
    rows = [r for r in rows if r["verified"] and int(r["iteration"]) >= 0]
    by = collections.defaultdict(list)
    for r in rows:
        by[(r["bucket_type"], r["workload"], r["op"], int(r["object_size"]), impl_key(r))].append(r)

    print("# UDS POC results\n")
    print(f"Source: `{sys.argv[1]}`. Latency ms (p50/p90/p99); CPU and allocations are per-row"
          " medians per byte transferred (app + sidecar); ratios are impl ÷ baseline within one cell.\n")
    for title, workload, ops, pocs, base, rusts, context in CELLS:
        bucket_type = "zonal" if title.startswith("Zone") else "regional"
        sizes = sorted({k[3] for k in by if k[0] == bucket_type and k[1] == workload and k[2] in ops and k[4] != "uds-synthetic"})
        if not sizes:
            continue
        print(f"## {title}\n")
        print("| op | size | impl | n | err | p50 | p90 | p99 | MiB/s | CPU ns/B (app) | alloc B/B | p50 vs base | CPU vs base |")
        print("|---|--:|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|")
        for op in ops:
            for size in sizes:
                b = by.get((bucket_type, workload, op, size, base))
                bstats = summarize_rows(b) if b else None
                for impl in pocs + [base] + rusts + context:
                    rs = by.get((bucket_type, workload, op, size, impl))
                    if not rs:
                        continue
                    s = summarize_rows(rs)
                    vs_p50 = ratio(s["p50"], bstats["p50"]) if bstats and impl != base else ""
                    vs_cpu = ratio(s["cpu"], bstats["cpu"]) if bstats and impl != base else ""
                    label = impl + (" *(context)*" if impl in context else "")
                    print(f"| {op} | {size} | {label} | {s['n']} | {s['err']} | {fmt(s['p50'])} | {fmt(s['p90'])} |"
                          f" {fmt(s['p99'])} | {fmt(s['tput'], 1)} | {fmt(s['cpu'])} ({fmt(s['cpu_app'])}) |"
                          f" {fmt(s['alloc'])} | {vs_p50} | {vs_cpu} |")
        print()

    syn_w1r3 = collections.defaultdict(list)
    for r in rows:
        if r["impl"] == "uds-synthetic" and r["workload"] == "w1r3":
            lbl = parse_labels(r["labels"])
            chunk_kib = int(lbl.get("chunk", "0")) // 1024
            channels = int(lbl.get("channels", "1"))
            syn_w1r3[(chunk_kib, channels, r["op"], int(r["object_size"]))].append(r)
    if syn_w1r3:
        print("## Synthetic hop floor (single-stream `--synthetic`, median CPU & alloc)\n")
        print("| chunk KiB | ch | op | size | n | p50 | p90 | p99 | MiB/s | CPU ns/B (app, sidecar) | alloc B/B (app, sidecar) |")
        print("|--:|--:|---|--:|--:|--:|--:|--:|--:|--:|--:|")
        for (chunk_kib, channels, op, size), rs in sorted(syn_w1r3.items()):
            if channels != 1 or op not in ("READ[0]", "SINGLE-SHOT"):
                continue
            s = summarize_rows(rs)
            print(
                f"| {chunk_kib} | {channels} | {op} | {size} | {s['n']} | {fmt(s['p50'], 3)} |"
                f" {fmt(s['p90'], 3)} | {fmt(s['p99'], 3)} | {fmt(s['tput'], 1)} |"
                f" {fmt(s['cpu'])} ({fmt(s['cpu_app'])}, {fmt(s['cpu_peer'])}) |"
                f" {fmt(s['alloc'])} ({fmt(s['alloc_app'])}, {fmt(s['alloc_peer'])}) |"
            )
        print()

    print("## Throughput (concurrent 100 MB transfers)\n")
    print("| run | impl | MiB/s | CPU ms app | CPU ms sidecar | peak RSS MiB app | peak RSS MiB sidecar | ok |")
    print("|---|---|--:|--:|--:|--:|--:|--:|")
    for r in rows:
        if r["workload"] == "throughput" and r["op"].startswith("THROUGHPUT"):
            tput = int(r["bytes"]) / 1048576 / (float(r["latency_ms"]) / 1000) if float(r["latency_ms"]) else 0
            rss_app = int(r["rss_kib_app"]) / 1024
            rss_peer = int(r["rss_kib_peer"]) / 1024
            print(f"| {r['run']} | {r['impl']} | {tput:.1f} | {float(r['cpu_ms_app']):.0f} | {float(r['cpu_ms_peer']):.0f} |"
                  f" {rss_app:.0f} | {rss_peer:.0f} | {r['ok']} |")
    print()

    print("## Whole-run totals (peak RSS)\n")
    print("| run | impl | wall s | CPU s app | CPU s sidecar | peak RSS MiB app | peak RSS MiB sidecar |")
    print("|---|---|--:|--:|--:|--:|--:|")
    for path in sorted(glob.glob(os.path.join(sys.argv[1], "*.csv"))):
        with open(path) as f:
            for r in csv.DictReader(f):
                if r["op"] == "RUN_TOTAL":
                    print(f"| {os.path.basename(path)[:-4]} | {r['impl']} | {float(r['latency_ms'])/1000:.1f} |"
                          f" {float(r['cpu_ms_app'])/1000:.1f} | {float(r['cpu_ms_peer'])/1000:.1f} |"
                          f" {int(r['rss_kib_app'])/1024:.0f} | {int(r['rss_kib_peer'])/1024:.0f} |")
    print()

    print("## Gaps and errors observed\n")
    errs = collections.Counter()
    for r in rows:
        if r["ok"] != "1":
            errs[(r["run"].rsplit("-", 1)[0], r["impl"], r["op"], r["error"][:120])] += 1
    for (run, impl, op, e), n in sorted(errs.items()):
        print(f"- `{run}` {impl} {op} ×{n}: {e}")
    if bad_runs:
        print("\n## Runs excluded by the Cloudpath check\n")
        for run, peers in bad_runs:
            print(f"- `{run}`: {peers}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
