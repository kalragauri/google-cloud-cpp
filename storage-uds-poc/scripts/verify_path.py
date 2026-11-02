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
"""Runs a benchmark and proves every GCS connection it made was Cloudpath.

Usage: verify_path.py REPORT.json -- COMMAND [ARGS...]

While COMMAND runs, samples `ss -tnpH` every 50 ms and records the remote
endpoints of TCP connections owned by COMMAND or any descendant (the
sidecar). After COMMAND finishes it exits non-zero if any :443 peer is in a
DirectPath range (3), or if a run that talks to GCS showed no peer at all
(4: unverified, e.g. connections shorter than the sampling interval), so a
run can never silently mix or hide network paths. Runs against the
synthetic sidecar make no GCS connections and need no peers.
"""

import ipaddress
import json
import re
import subprocess
import sys
import time

# Google DirectPath backends (google-c2p / ALTS) are addressed from these
# ranges; Cloudpath uses public storage.googleapis.com VIPs.
DIRECTPATH = [ipaddress.ip_network("34.126.0.0/18"),
              ipaddress.ip_network("2001:4860:8040::/42")]


def descendants(root):
    pids = {root}
    try:
        out = subprocess.run(["ps", "-e", "-o", "pid=,ppid="], capture_output=True,
                             text=True, check=True).stdout
    except subprocess.CalledProcessError:
        return pids
    children = {}
    for line in out.split("\n"):
        parts = line.split()
        if len(parts) == 2:
            children.setdefault(int(parts[1]), []).append(int(parts[0]))
    stack = [root]
    while stack:
        for c in children.get(stack.pop(), []):
            if c not in pids:
                pids.add(c)
                stack.append(c)
    return pids


def peers(pids):
    out = subprocess.run(["ss", "-tnpH"], capture_output=True, text=True).stdout
    found = set()
    for line in out.split("\n"):
        owners = {int(p) for p in re.findall(r"pid=(\d+)", line)}
        if not owners & pids:
            continue
        cols = line.split()
        if len(cols) >= 5:
            found.add(cols[4])  # remote address:port
    return found


def classify(peer):
    host, _, port = peer.rpartition(":")
    host = host.strip("[]")
    try:
        ip = ipaddress.ip_address(host.split("%")[0])
    except ValueError:
        return "other"
    if port != "443":
        return "other"
    if any(ip in net for net in DIRECTPATH):
        return "directpath"
    return "cloudpath"


def main():
    if len(sys.argv) < 4 or sys.argv[2] != "--":
        print(__doc__, file=sys.stderr)
        return 2
    report_path, cmd = sys.argv[1], sys.argv[3:]
    proc = subprocess.Popen(cmd)
    seen = set()
    while proc.poll() is None:
        seen |= peers(descendants(proc.pid))
        time.sleep(0.05)
    by_kind = {}
    for p in sorted(seen):
        by_kind.setdefault(classify(p), []).append(p)
    synthetic = any("--synthetic" in a for a in cmd)
    verified = not by_kind.get("directpath") and (synthetic or bool(by_kind.get("cloudpath")))
    report = {"command": cmd, "exit_code": proc.returncode, "synthetic": synthetic,
              "verified": verified,
              "cloudpath": by_kind.get("cloudpath", []),
              "directpath": by_kind.get("directpath", []),
              "other": by_kind.get("other", [])}
    with open(report_path, "w") as f:
        json.dump(report, f, indent=1)
    if report["directpath"]:
        print(f"verify_path: DIRECTPATH peers seen: {report['directpath']}", file=sys.stderr)
        return 3
    if not verified:
        print("verify_path: UNVERIFIED: no GCS peer observed", file=sys.stderr)
        return proc.returncode or 4
    print(f"verify_path: {len(report['cloudpath'])} Cloudpath peers, 0 DirectPath")
    return proc.returncode


if __name__ == "__main__":
    sys.exit(main())
