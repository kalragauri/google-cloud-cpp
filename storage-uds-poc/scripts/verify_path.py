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

While COMMAND runs, samples `ss -tnpH` and records the remote endpoints of TCP
connections owned by COMMAND, any descendant, and (when running the out-of-proc
sidecar/daemon comparator) any same-UID `gcs-core-server` daemon process.
Exits non-zero if any :443 peer is in a DirectPath range (3), or if a run that
talks to GCS showed no peer at all (4: unverified).
"""

import ipaddress
import json
import os
import pathlib
import re
import subprocess
import sys
import time
from typing import Sequence

# Google DirectPath backends (google-c2p / ALTS) are addressed from these
# ranges; Cloudpath uses public storage.googleapis.com VIPs.
DIRECTPATH = [
    ipaddress.ip_network("34.126.0.0/18"),
    ipaddress.ip_network("2001:4860:8040::/42"),
]
SERVER_COMM = "gcs-core-server"


def uses_external_daemon(cmd: Sequence[str]) -> bool:
  """Returns True when COMMAND may route GCS traffic via a detached daemon."""
  if not cmd:
    return False
  exe_name = pathlib.Path(cmd[0]).name
  if exe_name.endswith("_inproc") or exe_name == "ssb_native":
    return False
  for arg in cmd[1:]:
    if arg in ("--mode=native", "--mode=uds-inproc"):
      return False
  return True


def descendants(root: int) -> set[int]:
  """Returns `root` and all recursive child PIDs currently in `ps`."""
  pids = {root}
  out = subprocess.run(
      ["ps", "-e", "-o", "pid=,ppid="],
      capture_output=True,
      text=True,
      check=True,
  ).stdout
  children: dict[int, list[int]] = {}
  for line in out.split("\n"):
    parts = line.split()
    if len(parts) == 2:
      children.setdefault(int(parts[1]), []).append(int(parts[0]))
  stack = [root]
  while stack:
    for child in children.get(stack.pop(), []):
      if child not in pids:
        pids.add(child)
        stack.append(child)
  return pids


def same_uid_server_pids(uid: int) -> set[int]:
  """Returns PIDs of `gcs-core-server` processes owned by `uid`."""
  proc = subprocess.run(
      ["ps", "-u", str(uid), "-o", "pid=,comm="],
      capture_output=True,
      text=True,
      check=False,
  )
  if proc.returncode != 0:
    return set()
  pids: set[int] = set()
  for line in proc.stdout.split("\n"):
    parts = line.split(None, 1)
    if len(parts) == 2 and parts[1].strip() == SERVER_COMM:
      pids.add(int(parts[0]))
  return pids


def tracked_pids(root: int, include_daemon: bool, uid: int) -> set[int]:
  """Returns all PIDs whose TCP peers belong to the benchmark under test."""
  pids = set(descendants(root))
  if include_daemon:
    return pids | same_uid_server_pids(uid)
  return pids


def parse_peers_from_ss(ss_output: str, pids: set[int]) -> set[str]:
  """Extracts remote `address:port` endpoints owned by any PID in `pids`."""
  found: set[str] = set()
  for line in ss_output.split("\n"):
    owners = {int(p) for p in re.findall(r"pid=(\d+)", line)}
    if not owners & pids:
      continue
    cols = line.split()
    if len(cols) >= 5:
      found.add(cols[4])
  return found


def peers(pids: set[int]) -> set[str]:
  """Samples `ss -tnpH` once for remote endpoints owned by `pids`."""
  out = subprocess.run(
      ["ss", "-tnpH"], capture_output=True, text=True, check=True
  ).stdout
  return parse_peers_from_ss(out, pids)


def classify(peer: str) -> str:
  """Classifies `host:port` into `cloudpath`, `directpath`, or `other`."""
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


def main(argv: Sequence[str] | None = None) -> int:
  args = list(sys.argv[1:] if argv is None else argv)
  if len(args) < 3 or args[1] != "--":
    print(__doc__, file=sys.stderr)
    return 2
  report_path, cmd = args[0], args[2:]
  include_daemon = uses_external_daemon(cmd)
  uid = os.getuid()
  proc = subprocess.Popen(cmd)
  seen: set[str] = set()
  samples = 0
  while proc.poll() is None:
    seen |= peers(tracked_pids(proc.pid, include_daemon, uid))
    samples += 1
    time.sleep(0.01 if samples < 20 else 0.05)
  if include_daemon:
    seen |= peers(same_uid_server_pids(uid))

  by_kind: dict[str, list[str]] = {}
  for peer in sorted(seen):
    by_kind.setdefault(classify(peer), []).append(peer)
  synthetic = any("--synthetic" in a for a in cmd)
  verified = not by_kind.get("directpath") and (
      synthetic or bool(by_kind.get("cloudpath"))
  )
  report = {
      "command": cmd,
      "exit_code": proc.returncode,
      "include_daemon": include_daemon,
      "synthetic": synthetic,
      "verified": verified,
      "cloudpath": by_kind.get("cloudpath", []),
      "directpath": by_kind.get("directpath", []),
      "other": by_kind.get("other", []),
  }
  with open(report_path, "w", encoding="utf-8") as fh:
    json.dump(report, fh, indent=2)
  if report["directpath"]:
    print(
        f"verify_path: DIRECTPATH peers seen: {report['directpath']}",
        file=sys.stderr,
    )
    return 3
  if not verified:
    print("verify_path: UNVERIFIED: no GCS peer observed", file=sys.stderr)
    return proc.returncode or 4
  print(
      f"verify_path: {len(report['cloudpath'])} Cloudpath peers, 0 DirectPath"
  )
  return proc.returncode


if __name__ == "__main__":
  sys.exit(main())
