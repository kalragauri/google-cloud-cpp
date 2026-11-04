// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! `gcs_core`: the Rust core of the C++ POC, in two hostings.
//!
//! * `gcs-core-sidecar` (`src/main.rs`) serves [`service::Service`] from a
//!   separate process over a Unix domain socket (process boundary).
//! * `libgcs_core.a` exposes [`inproc`], a C ABI that serves the same
//!   service on tokio threads inside the calling process (thread boundary).
//!
//! Everything the wire API does — [`service`], [`convert`], the message and
//! flow-control settings in [`transport`] — is shared, so a benchmark of the
//! two hostings differs only in where the server threads live.

pub mod config;
pub mod convert;
pub mod inproc;
pub mod service;
pub mod stats;
pub mod transport;

pub mod pb {
    tonic::include_proto!("gcs.core.v0");
}
