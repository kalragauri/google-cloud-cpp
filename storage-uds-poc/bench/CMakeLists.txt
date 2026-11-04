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

# Single unified benchmark binary (`uds_poc_bench`) driving all three
# comparators (`--mode=native`, `--mode=uds-sidecar`, `--mode=uds-inproc`)
# through identical public `google::cloud::storage::Client` and
# `google::cloud::storage::AsyncClient` workload loops.
add_executable(uds_poc_bench uds_poc_bench.cc)
target_link_libraries(
    uds_poc_bench
    PRIVATE absl::cord absl::strings gcs_core_storage_combined
            google-cloud-cpp::storage google-cloud-cpp::storage_grpc)
gcs_uds_poc_warnings(uds_poc_bench)
