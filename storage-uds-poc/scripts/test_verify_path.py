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
"""Unit tests for verify_path.py (plan rev 4 §8 row C1)."""

import pathlib
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import verify_path  # noqa: E402


class VerifyPathTest(unittest.TestCase):

  def test_uses_external_daemon_excludes_native_and_inproc(self) -> None:
    # Native and in-process comparators own their own TCP sockets; excluding
    # any idle gcs-core-server daemon prevents a false positive if the client
    # under test failed to connect.
    self.assertTrue(
        verify_path.uses_external_daemon(
            ["/tmp/uds_poc_bench", "--mode=uds-sidecar", "--workload=open"]
        )
    )
    self.assertFalse(
        verify_path.uses_external_daemon(
            ["/tmp/uds_poc_bench", "--mode=native", "--workload=open"]
        )
    )
    self.assertFalse(
        verify_path.uses_external_daemon(
            ["/tmp/uds_poc_bench", "--mode=uds-inproc", "--workload=open"]
        )
    )
    self.assertFalse(
        verify_path.uses_external_daemon(["/tmp/quickstart_inproc", "bucket"])
    )

  def test_classify_distinguishes_cloudpath_directpath_and_other(self) -> None:
    self.assertEqual(verify_path.classify("142.250.189.202:443"), "cloudpath")
    self.assertEqual(
        verify_path.classify("[2607:f8b0:4005:80b::200a]:443"), "cloudpath"
    )
    self.assertEqual(verify_path.classify("34.126.12.34:443"), "directpath")
    self.assertEqual(
        verify_path.classify("[2001:4860:8040::1]:443"), "directpath"
    )
    self.assertEqual(verify_path.classify("169.254.169.254:80"), "other")

  def test_parse_peers_matches_daemon_pid_when_included(self) -> None:
    ss_sample = (
        "ESTAB 0 0 10.128.0.5:51234 142.250.189.202:443 "
        'users:(("gcs-core-server",pid=4242,fd=11))\n'
        "ESTAB 0 0 10.128.0.5:51235 34.126.1.2:443 "
        'users:(("unrelated",pid=9999,fd=4))\n'
    )
    self.assertEqual(
        verify_path.parse_peers_from_ss(ss_sample, {1000, 4242}),
        {"142.250.189.202:443"},
    )
    self.assertEqual(verify_path.parse_peers_from_ss(ss_sample, {1000}), set())

  def test_tracked_pids_merges_daemon_only_when_requested(self) -> None:
    with (
        mock.patch.object(
            verify_path, "descendants", autospec=True, return_value={100, 101}
        ),
        mock.patch.object(
            verify_path,
            "same_uid_server_pids",
            autospec=True,
            return_value={4242},
        ),
    ):
      self.assertEqual(
          verify_path.tracked_pids(100, include_daemon=True, uid=1000),
          {100, 101, 4242},
      )
      self.assertEqual(
          verify_path.tracked_pids(100, include_daemon=False, uid=1000),
          {100, 101},
      )


if __name__ == "__main__":
  unittest.main()
