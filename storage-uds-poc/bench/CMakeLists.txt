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

# Three binaries sharing one workload definition. Each links only what it
# measures, so no process carries another variant's libraries or threads:
# `ssb_uds` has no Rust code, `ssb_inproc` has no sidecar spawner.
add_executable(ssb_uds alloc_counter.cc ssb_uds.cc)
target_link_libraries(ssb_uds PRIVATE gcs_core)
gcs_uds_poc_warnings(ssb_uds)

if (GCS_UDS_POC_BUILD_INPROC)
    add_executable(ssb_inproc alloc_counter.cc ssb_inproc.cc)
    target_link_libraries(ssb_inproc PRIVATE gcs_core_inproc)
    gcs_uds_poc_warnings(ssb_inproc)
endif ()

add_executable(ssb_native alloc_counter.cc ssb_native.cc)
target_link_libraries(ssb_native PRIVATE absl::strings
                                         google-cloud-cpp::storage_grpc)
gcs_uds_poc_warnings(ssb_native)
