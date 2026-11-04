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

# GCS SDK consolidation POC: thin C++ wrapper over the shared Rust
# gRPC-over-UDS server (`storage-grpc-uds` in google-cloud-rust), built in two
# variants that differ only in where the server runs — a child process reached
# over a Unix domain socket (`gcs_core`) or Tokio threads inside the
# application reached over an abstract Unix socket (`gcs_core_inproc`) — plus
# the native google-cloud-cpp baseline.
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

# The POC's own sources build warning-free; the google-cloud-cpp subdirectory
# and the generated protos keep their own flags.
function (gcs_uds_poc_warnings target)
    target_compile_options(${target} PRIVATE -Wall -Wextra -Werror)
endfunction ()

option(GCS_UDS_POC_BUILD_TESTS "Build the wrapper unit tests" ON)
option(GCS_UDS_POC_BUILD_INPROC "Build the in-process variant" ON)

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

# --- the shared Rust server (google-cloud-rust, storage-grpc-uds) -------------
# Every language's POC uses the same server: PR #7350's `storage-grpc-uds`
# crate plus the follow-up commits on our `grpc-uds-cpp` branch. Nothing is
# copied into this tree; the checkout is fetched and built by cargo. For local
# iteration point CMake at a worktree instead of fetching:
#   -DFETCHCONTENT_SOURCE_DIR_GOOGLE_CLOUD_RUST=$HOME/google-cloud-rust-grpc-uds-cpp
include(FetchContent)
set(GCS_CORE_RUST_REPO "https://github.com/kalragauri/google-cloud-rust.git"
    CACHE STRING "google-cloud-rust repository providing storage-grpc-uds")
set(GCS_CORE_RUST_REF "grpc-uds-cpp"
    CACHE STRING "Commit SHA (preferred) or branch of GCS_CORE_RUST_REPO")
FetchContent_Declare(
    google_cloud_rust
    GIT_REPOSITORY "${GCS_CORE_RUST_REPO}"
    GIT_TAG "${GCS_CORE_RUST_REF}"
    GIT_SHALLOW FALSE)
# The repository has no top-level CMakeLists.txt, so this only populates it.
FetchContent_MakeAvailable(google_cloud_rust)
set(GCS_CORE_RUST_CRATE_DIR "${google_cloud_rust_SOURCE_DIR}/src/storage/grpc-uds")

find_program(GCS_UDS_POC_CARGO cargo HINTS "$ENV{HOME}/.cargo/bin" REQUIRED)
set(GCS_CORE_CARGO_TARGET_DIR "${CMAKE_CURRENT_BINARY_DIR}/cargo-target")
set(GCS_CORE_SERVER_BINARY "${CMAKE_CURRENT_BINARY_DIR}/gcs-core-server")
set(GCS_CORE_RUST_STATICLIB
    "${GCS_CORE_CARGO_TARGET_DIR}/release/libstorage_grpc_uds.a")
# One cargo build produces the server binary (sidecar variant) and the static
# library (in-process variant). Appendable writes need the unstable bidi cfg.
# cargo tracks its own inputs, so the target runs every build and is a no-op
# when nothing changed.
add_custom_target(
    gcs_core_rust ALL
    COMMAND
        ${CMAKE_COMMAND} -E env "CARGO_TARGET_DIR=${GCS_CORE_CARGO_TARGET_DIR}"
        "RUSTFLAGS=--cfg google_cloud_unstable_storage_bidi"
        ${GCS_UDS_POC_CARGO} build --release --quiet -p storage-grpc-uds
    COMMAND
        ${CMAKE_COMMAND} -E copy_if_different
        "${GCS_CORE_CARGO_TARGET_DIR}/release/gcs-core-server"
        "${GCS_CORE_SERVER_BINARY}"
    WORKING_DIRECTORY "${google_cloud_rust_SOURCE_DIR}"
    BYPRODUCTS "${GCS_CORE_SERVER_BINARY}" "${GCS_CORE_RUST_STATICLIB}"
    COMMENT "cargo build --release -p storage-grpc-uds"
    VERBATIM)

# --- gcscore.v1 protos (the shared wire contract) ----------------------------
set(GCS_UDS_POC_PROTO_DIR "${GCS_CORE_RUST_CRATE_DIR}/proto")
set(GCS_UDS_POC_PROTO "${GCS_UDS_POC_PROTO_DIR}/gcscore.proto")
set(GCS_UDS_POC_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/gen")
set(GCS_UDS_POC_GEN_SRCS "${GCS_UDS_POC_GEN_DIR}/gcscore.pb.cc"
                         "${GCS_UDS_POC_GEN_DIR}/gcscore.grpc.pb.cc")
add_custom_command(
    OUTPUT ${GCS_UDS_POC_GEN_SRCS} "${GCS_UDS_POC_GEN_DIR}/gcscore.pb.h"
           "${GCS_UDS_POC_GEN_DIR}/gcscore.grpc.pb.h"
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

# --- the thin wrapper, sidecar variant -----------------------------------------
# Everything above the backend seam (`cpp/src/core_backend.h`) is the same
# source in both variants; only the backend translation unit differs.
set(GCS_CORE_WRAPPER_SOURCES cpp/include/gcs_core/client.h cpp/src/client.cc
                             cpp/src/core_backend.h cpp/src/status.cc
                             cpp/src/status.h)
add_library(gcs_core ${GCS_CORE_WRAPPER_SOURCES} cpp/src/sidecar_backend.cc
                     cpp/src/sidecar_process.cc cpp/src/sidecar_process.h)
target_include_directories(gcs_core PUBLIC cpp/include PRIVATE cpp/src)
target_link_libraries(
    gcs_core
    PUBLIC absl::cord absl::status absl::strings google-cloud-cpp::common
    PRIVATE gRPC::grpc++ gcs_core_protos)
gcs_uds_poc_warnings(gcs_core)

add_executable(quickstart cpp/examples/quickstart.cc)
target_link_libraries(quickstart PRIVATE gcs_core)
gcs_uds_poc_warnings(quickstart)
# Cold start of Client::Create() in a fresh process (packaging probes).
add_executable(cold_start cpp/examples/cold_start.cc)
target_link_libraries(cold_start PRIVATE gcs_core)
gcs_uds_poc_warnings(cold_start)

# --- the shared server as a static library, and the in-process variant --------
if (GCS_UDS_POC_BUILD_INPROC)
    add_library(gcs_core_rust_lib STATIC IMPORTED)
    set_target_properties(gcs_core_rust_lib PROPERTIES IMPORTED_LOCATION
                                                       "${GCS_CORE_RUST_STATICLIB}")
    target_include_directories(gcs_core_rust_lib
                               INTERFACE "${GCS_CORE_RUST_CRATE_DIR}/include")
    # From `rustc --print native-static-libs` for this crate.
    target_link_libraries(gcs_core_rust_lib INTERFACE gcc_s util rt pthread m
                                                      dl)
    add_dependencies(gcs_core_rust_lib gcs_core_rust)

    add_library(gcs_core_inproc ${GCS_CORE_WRAPPER_SOURCES}
                                cpp/src/inproc_backend.cc)
    target_include_directories(gcs_core_inproc PUBLIC cpp/include
                               PRIVATE cpp/src)
    target_link_libraries(
        gcs_core_inproc
        PUBLIC absl::cord absl::status absl::strings google-cloud-cpp::common
        PRIVATE gRPC::grpc++ gcs_core_protos gcs_core_rust_lib)
    gcs_uds_poc_warnings(gcs_core_inproc)

    add_executable(quickstart_inproc cpp/examples/quickstart.cc)
    target_link_libraries(quickstart_inproc PRIVATE gcs_core_inproc)
    gcs_uds_poc_warnings(quickstart_inproc)
    add_executable(cold_start_inproc cpp/examples/cold_start.cc)
    target_link_libraries(cold_start_inproc PRIVATE gcs_core_inproc)
    gcs_uds_poc_warnings(cold_start_inproc)
endif ()

if (GCS_UDS_POC_BUILD_TESTS)
    find_package(GTest CONFIG REQUIRED)
    enable_testing()
    # The shared tests are compiled once per variant; `test_support.h` picks
    # the few expectations that legitimately differ. They start the real
    # shared server without GCS access and cover its lifecycle only; the
    # data plane is covered by gcs_core_fake_test and, end to end, by the
    # quickstart against real buckets.
    add_executable(gcs_core_test cpp/tests/client_common_test.cc
                                 cpp/tests/client_sidecar_test.cc
                                 cpp/tests/sidecar_process_test.cc
                                 cpp/tests/test_support.h)
    target_include_directories(gcs_core_test PRIVATE cpp/src)
    target_link_libraries(gcs_core_test PRIVATE GTest::gmock_main gcs_core
                                                gcs_core_protos)
    gcs_uds_poc_warnings(gcs_core_test)
    add_dependencies(gcs_core_test gcs_core_rust)
    add_test(NAME gcs_core_test COMMAND gcs_core_test)
    set_tests_properties(
        gcs_core_test PROPERTIES ENVIRONMENT
                                 "GCS_CORE_SERVER=${GCS_CORE_SERVER_BINARY}")

    # The wrapper against a fake GcsCore: a third backend that connects to a
    # server the test already started, so no Rust server or GCS is involved.
    add_library(gcs_core_testing STATIC ${GCS_CORE_WRAPPER_SOURCES}
                                        cpp/tests/existing_server_backend.cc)
    target_include_directories(gcs_core_testing PUBLIC cpp/include
                               PRIVATE cpp/src)
    target_link_libraries(
        gcs_core_testing
        PUBLIC absl::cord absl::status absl::strings google-cloud-cpp::common
               gRPC::grpc++ gcs_core_protos)
    gcs_uds_poc_warnings(gcs_core_testing)
    add_executable(gcs_core_fake_test cpp/tests/client_fake_test.cc
                                      cpp/tests/test_support.h)
    target_link_libraries(gcs_core_fake_test PRIVATE GTest::gmock_main
                                                     gcs_core_testing)
    gcs_uds_poc_warnings(gcs_core_fake_test)
    add_test(NAME gcs_core_fake_test COMMAND gcs_core_fake_test)
    if (GCS_UDS_POC_BUILD_INPROC)
        add_executable(gcs_core_inproc_test cpp/tests/client_common_test.cc
                                            cpp/tests/client_inproc_test.cc
                                            cpp/tests/test_support.h)
        target_compile_definitions(gcs_core_inproc_test
                                   PRIVATE GCS_CORE_TEST_INPROC=1)
        target_include_directories(gcs_core_inproc_test PRIVATE cpp/src)
        target_link_libraries(gcs_core_inproc_test
                              PRIVATE GTest::gmock_main gcs_core_inproc
                                      gcs_core_protos)
        gcs_uds_poc_warnings(gcs_core_inproc_test)
        add_test(NAME gcs_core_inproc_test COMMAND gcs_core_inproc_test)
    endif ()
endif ()

# --- benchmarks -----------------------------------------------------------------
# The phase-1 harness measured the wrapper's own API; the worklog's workloads
# are measured through the unmodified google-cloud-cpp clients instead (plan
# rev 4, step S3), so it stays off until it is ported.
option(GCS_UDS_POC_BUILD_BENCH "Build the benchmark binaries" OFF)
if (GCS_UDS_POC_BUILD_BENCH)
    add_subdirectory(bench)
endif ()
