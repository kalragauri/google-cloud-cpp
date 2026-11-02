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

# GCS SDK consolidation POC: thin C++ wrapper over a Rust core-sdk sidecar
# (gRPC over a Unix domain socket), plus the native google-cloud-cpp baseline.
#
# This is a standalone project. It is NOT referenced by the repository's root
# CMakeLists.txt, so it never affects the library build or its CI.
#
# Configure (dependencies come from google-cloud-cpp's own vcpkg.json, so the
# binary cache is shared with regular google-cloud-cpp builds):
#
#   cmake -S storage-uds-poc -B storage-uds-poc/cmake-out -GNinja \
#     -DCMAKE_BUILD_TYPE=Release \
#     -DCMAKE_TOOLCHAIN_FILE=$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake

cmake_minimum_required(VERSION 3.22)

set(GCS_UDS_POC_GOOGLE_CLOUD_CPP_DIR "${CMAKE_CURRENT_LIST_DIR}/.."
    CACHE PATH "google-cloud-cpp checkout used for the baseline and common")
set(VCPKG_MANIFEST_DIR "${GCS_UDS_POC_GOOGLE_CLOUD_CPP_DIR}" CACHE PATH "")
set(VCPKG_OVERLAY_PORTS
    "${GCS_UDS_POC_GOOGLE_CLOUD_CPP_DIR}/ci/gha/builds/vcpkg-overlays"
    CACHE STRING "")

project(gcs_uds_poc LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(GCS_UDS_POC_BUILD_TESTS "Build the wrapper unit tests" ON)

# google-cloud-cpp from this checkout: `common` for the wrapper, `storage` and
# `storage_grpc` for the native baseline. EXCLUDE_FROM_ALL builds only what is
# linked below; its own tests and examples are off.
set(GOOGLE_CLOUD_CPP_ENABLE "storage;storage_grpc" CACHE STRING "")
set(BUILD_TESTING OFF CACHE BOOL "")
set(GOOGLE_CLOUD_CPP_WITH_MOCKS OFF CACHE BOOL "")
set(GOOGLE_CLOUD_CPP_ENABLE_EXAMPLES OFF CACHE BOOL "")
set(GOOGLE_CLOUD_CPP_ENABLE_WERROR OFF CACHE BOOL "")
add_subdirectory(${GCS_UDS_POC_GOOGLE_CLOUD_CPP_DIR} google-cloud-cpp
                 EXCLUDE_FROM_ALL)

find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(absl CONFIG REQUIRED)

# --- gcs.core.v0 protos -----------------------------------------------------
set(GCS_UDS_POC_PROTO_DIR "${CMAKE_CURRENT_LIST_DIR}/proto")
set(GCS_UDS_POC_PROTO "${GCS_UDS_POC_PROTO_DIR}/gcs/core/v0/storage_core.proto")
set(GCS_UDS_POC_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/gen")
set(GCS_UDS_POC_GEN_SRCS
    "${GCS_UDS_POC_GEN_DIR}/gcs/core/v0/storage_core.pb.cc"
    "${GCS_UDS_POC_GEN_DIR}/gcs/core/v0/storage_core.grpc.pb.cc")
add_custom_command(
    OUTPUT ${GCS_UDS_POC_GEN_SRCS}
           "${GCS_UDS_POC_GEN_DIR}/gcs/core/v0/storage_core.pb.h"
           "${GCS_UDS_POC_GEN_DIR}/gcs/core/v0/storage_core.grpc.pb.h"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${GCS_UDS_POC_GEN_DIR}"
    COMMAND
        $<TARGET_FILE:protobuf::protoc> --proto_path=${GCS_UDS_POC_PROTO_DIR}
        --cpp_out=${GCS_UDS_POC_GEN_DIR} --grpc_out=${GCS_UDS_POC_GEN_DIR}
        --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
        ${GCS_UDS_POC_PROTO}
    DEPENDS ${GCS_UDS_POC_PROTO} protobuf::protoc gRPC::grpc_cpp_plugin
    VERBATIM)
add_library(gcs_core_protos STATIC ${GCS_UDS_POC_GEN_SRCS})
target_include_directories(gcs_core_protos SYSTEM
                           PUBLIC "${GCS_UDS_POC_GEN_DIR}")
target_link_libraries(gcs_core_protos PUBLIC gRPC::grpc++
                                             protobuf::libprotobuf)

# --- the thin wrapper ---------------------------------------------------------
add_library(
    gcs_core
    cpp/include/gcs_core/client.h
    cpp/src/client.cc
    cpp/src/sidecar_process.cc
    cpp/src/sidecar_process.h
    cpp/src/status.cc
    cpp/src/status.h)
target_include_directories(gcs_core PUBLIC cpp/include PRIVATE cpp/src)
target_link_libraries(
    gcs_core
    PUBLIC absl::cord absl::status absl::strings google-cloud-cpp::common
    PRIVATE gRPC::grpc++ gcs_core_protos)

add_executable(quickstart cpp/examples/quickstart.cc)
target_link_libraries(quickstart PRIVATE gcs_core)

if (GCS_UDS_POC_BUILD_TESTS)
    find_package(GTest CONFIG REQUIRED)
    enable_testing()
    add_executable(gcs_core_test cpp/tests/client_test.cc
                                 cpp/tests/sidecar_process_test.cc)
    target_include_directories(gcs_core_test PRIVATE cpp/src)
    target_link_libraries(gcs_core_test PRIVATE GTest::gmock_main gcs_core
                                                gcs_core_protos)
    add_test(NAME gcs_core_test COMMAND gcs_core_test)
    # The tests start the real sidecar in --synthetic mode.
    set_tests_properties(
        gcs_core_test
        PROPERTIES
            ENVIRONMENT
            "GCS_CORE_SIDECAR=${CMAKE_CURRENT_LIST_DIR}/sidecar/target/release/gcs-core-sidecar"
    )
endif ()

# --- benchmarks -----------------------------------------------------------------
add_subdirectory(bench)
