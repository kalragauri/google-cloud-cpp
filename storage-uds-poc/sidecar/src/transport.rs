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

//! The tonic server shared by the sidecar process and the in-process core:
//! identical message limits and HTTP/2 windows, so a benchmark row differs
//! between the two only by where the server threads live.

use crate::config::Args;
use crate::pb::storage_core_server::StorageCoreServer;
use crate::service::{self, Service};
use tokio::net::UnixStream;
use tonic::transport::server::Router;

/// gRPC message limit on both directions; above `service::MAX_CHUNK` plus
/// protobuf framing.
pub const MAX_MESSAGE: usize = 2 * service::MAX_CHUNK;

/// The service router with the core's message and flow-control settings.
pub fn router(svc: Service, args: &Args) -> Router {
    let server = StorageCoreServer::new(svc)
        .max_decoding_message_size(MAX_MESSAGE)
        .max_encoding_message_size(MAX_MESSAGE);
    tonic::transport::Server::builder()
        .initial_stream_window_size(args.stream_window)
        .initial_connection_window_size(args.connection_window)
        .add_service(server)
}

/// Accept filter for named Unix sockets: only connections from this uid are
/// served. A filesystem socket is already 0600; the check also covers sockets
/// in directories with looser permissions and abstract-namespace sockets,
/// which have no permissions at all.
pub fn same_uid_only(conn: std::io::Result<UnixStream>) -> Option<std::io::Result<UnixStream>> {
    // SAFETY: getuid has no preconditions and cannot fail.
    let uid = unsafe { libc::getuid() };
    match conn {
        Ok(stream) => match stream.peer_cred() {
            Ok(cred) if cred.uid() == uid => Some(Ok(stream)),
            Ok(cred) => {
                eprintln!("gcs-core: rejected connection from uid {}", cred.uid());
                None
            }
            Err(e) => Some(Err(e)),
        },
        Err(e) => Some(Err(e)),
    }
}
