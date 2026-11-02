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

//! `gcs-core-sidecar`: serves `gcs.core.v0.StorageCore` on a Unix socket.
//!
//! Lifetime: exits on the `Shutdown` RPC, on SIGTERM/SIGINT, or when the
//! parent closes the pipe passed via `--parent-fd`, so a crashed parent
//! never leaves the sidecar behind.

mod convert;
mod service;
mod stats;

pub mod pb {
    tonic::include_proto!("gcs.core.v0");
}

use futures::StreamExt;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::UnixListener;
use tokio::sync::Notify;
use tokio_stream::wrappers::UnixListenerStream;

/// gRPC message limit on both directions; above `service::MAX_CHUNK` plus
/// protobuf framing.
const MAX_MESSAGE: usize = 2 * service::MAX_CHUNK;
/// HTTP/2 initial window bounds enforced by `hyper` / `h2` (`65,535`..=`2^31 - 1`).
const MIN_H2_WINDOW: u32 = 65_535;
const MAX_H2_WINDOW: u32 = 0x7fff_ffff;

#[derive(Debug, PartialEq)]
struct Args {
    socket: PathBuf,
    parent_fd: Option<i32>,
    synthetic: bool,
    chunk_size: usize,
    coalesce_reads: bool,
    grpc_subchannels: Option<usize>,
    worker_threads: Option<usize>,
    stream_window: u32,
    connection_window: u32,
}

const USAGE: &str = "usage: gcs-core-sidecar --socket=PATH [--parent-fd=N] [--synthetic] \
[--chunk-size=BYTES (1048576)] [--no-coalesce-reads] [--grpc-subchannels=N] \
[--worker-threads=N] [--stream-window=BYTES (8388608)] [--connection-window=BYTES (16777216)]";

fn parse_args(argv: impl IntoIterator<Item = String>) -> Result<Args, String> {
    let mut socket = None;
    let mut args = Args {
        socket: PathBuf::new(),
        parent_fd: None,
        synthetic: false,
        chunk_size: 1 << 20,
        coalesce_reads: true,
        grpc_subchannels: None,
        worker_threads: None,
        stream_window: 8 << 20,
        connection_window: 16 << 20,
    };
    fn num<T: std::str::FromStr>(key: &str, value: Option<&str>) -> Result<T, String> {
        value
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| format!("{key} needs a numeric value"))
    }
    for arg in argv {
        let (key, value) = match arg.split_once('=') {
            Some((k, v)) => (k.to_string(), Some(v.to_string())),
            None => (arg.clone(), None),
        };
        let value = value.as_deref();
        match key.as_str() {
            "--socket" => socket = value.map(PathBuf::from),
            "--parent-fd" => {
                let fd: i32 = num(&key, value)?;
                if fd <= 2 {
                    return Err("--parent-fd must be > 2".to_string());
                }
                args.parent_fd = Some(fd);
            }
            "--synthetic" => args.synthetic = true,
            "--chunk-size" => args.chunk_size = num(&key, value)?,
            "--no-coalesce-reads" => args.coalesce_reads = false,
            "--grpc-subchannels" => {
                let n: usize = num(&key, value)?;
                if n == 0 {
                    return Err("--grpc-subchannels must be >= 1".to_string());
                }
                args.grpc_subchannels = Some(n);
            }
            "--worker-threads" => {
                let n: usize = num(&key, value)?;
                if n == 0 {
                    return Err("--worker-threads must be >= 1".to_string());
                }
                args.worker_threads = Some(n);
            }
            "--stream-window" => args.stream_window = num(&key, value)?,
            "--connection-window" => args.connection_window = num(&key, value)?,
            _ => return Err(format!("unknown argument {arg}")),
        }
    }
    args.socket = socket
        .filter(|s| !s.as_os_str().is_empty())
        .ok_or("--socket is required")?;
    if args.chunk_size == 0 || args.chunk_size > service::MAX_CHUNK {
        return Err(format!(
            "--chunk-size must be in [1, {}]",
            service::MAX_CHUNK
        ));
    }
    for (flag, val) in [
        ("--stream-window", args.stream_window),
        ("--connection-window", args.connection_window),
    ] {
        if !(MIN_H2_WINDOW..=MAX_H2_WINDOW).contains(&val) {
            return Err(format!(
                "{flag} must be in [{MIN_H2_WINDOW}, {MAX_H2_WINDOW}]"
            ));
        }
    }
    Ok(args)
}

fn main() -> ExitCode {
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("gcs-core-sidecar: {e}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    if let Some(n) = args.worker_threads {
        builder.worker_threads(n);
    }
    let runtime = match builder.enable_all().build() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("gcs-core-sidecar: cannot start runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run(args)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("gcs-core-sidecar: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn make_clients(args: &Args) -> Result<service::Clients, Box<dyn std::error::Error>> {
    // Default endpoints only: the core is Cloudpath by construction.
    let mut storage = google_cloud_storage::client::Storage::builder();
    if let Some(n) = args.grpc_subchannels {
        storage = storage.with_grpc_subchannel_count(n);
    }
    let storage = storage.build().await?;
    let control = google_cloud_storage::client::StorageControl::builder()
        .build()
        .await?;
    Ok(service::Clients { storage, control })
}

async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let clients = if args.synthetic {
        None
    } else {
        Some(make_clients(&args).await?)
    };
    let shutdown = Arc::new(Notify::new());
    let config = service::Config {
        default_chunk: args.chunk_size,
        coalesce_reads: args.coalesce_reads,
    };
    let svc = service::Service::new(clients, config, shutdown.clone());

    let listener = bind(&args.socket)?;
    // SAFETY: getuid has no preconditions and cannot fail.
    let uid = unsafe { libc::getuid() };
    let incoming = UnixListenerStream::new(listener).filter_map(move |conn| async move {
        match conn {
            // The socket is 0600 already; the peer check also covers sockets
            // created in a directory with looser permissions.
            Ok(stream) => match stream.peer_cred() {
                Ok(cred) if cred.uid() == uid => Some(Ok(stream)),
                Ok(cred) => {
                    eprintln!(
                        "gcs-core-sidecar: rejected connection from uid {}",
                        cred.uid()
                    );
                    None
                }
                Err(e) => Some(Err(e)),
            },
            Err(e) => Some(Err(e)),
        }
    });

    if let Some(fd) = args.parent_fd {
        tokio::spawn(watch_parent(fd, shutdown.clone()));
    }
    tokio::spawn(watch_signals(shutdown.clone()));

    eprintln!(
        "gcs-core-sidecar: listening on {} pid={} synthetic={} bidi={}",
        args.socket.display(),
        std::process::id(),
        args.synthetic,
        cfg!(google_cloud_unstable_storage_bidi)
    );
    let server = pb::storage_core_server::StorageCoreServer::new(svc)
        .max_decoding_message_size(MAX_MESSAGE)
        .max_encoding_message_size(MAX_MESSAGE);
    let result = tonic::transport::Server::builder()
        .initial_stream_window_size(args.stream_window)
        .initial_connection_window_size(args.connection_window)
        .add_service(server)
        .serve_with_incoming_shutdown(Box::pin(incoming), async move { shutdown.notified().await })
        .await;
    let _ = std::fs::remove_file(&args.socket);
    result?;
    Ok(())
}

/// Binds the socket with mode 0600, replacing a stale socket file left by a
/// crashed sidecar but never deleting a regular file.
fn bind(path: &std::path::Path) -> std::io::Result<UnixListener> {
    use std::os::unix::fs::FileTypeExt;
    if let Ok(meta) = std::fs::symlink_metadata(path) {
        if !meta.file_type().is_socket() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("{} exists and is not a socket", path.display()),
            ));
        }
        std::fs::remove_file(path)?;
    }
    let listener = UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

/// Shuts down when the parent's end of the lifetime pipe closes (EOF), which
/// the kernel does even if the parent is SIGKILLed.
async fn watch_parent(fd: i32, shutdown: Arc<Notify>) {
    use std::os::fd::FromRawFd;
    // SAFETY: the parent passes the read end of a pipe it created for this
    // purpose; nothing else in this process uses the descriptor.
    let file = unsafe { std::fs::File::from_raw_fd(fd) };
    let mut file = tokio::fs::File::from_std(file);
    let mut buf = [0_u8; 64];
    while let Ok(n) = file.read(&mut buf).await {
        if n == 0 {
            break;
        }
    }
    eprintln!("gcs-core-sidecar: parent pipe closed, shutting down");
    shutdown.notify_one();
}

async fn watch_signals(shutdown: Arc<Notify>) {
    use tokio::signal::unix::{SignalKind, signal};
    let (Ok(mut term), Ok(mut int)) = (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
    ) else {
        return;
    };
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
    shutdown.notify_one();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(v: &[&str]) -> Result<Args, String> {
        parse_args(v.iter().map(|s| s.to_string()))
    }

    /// Defaults apply when only the socket is given.
    #[test]
    fn parse_defaults() {
        let a = parse(&["--socket=/tmp/s.sock"]).unwrap();
        assert_eq!(a.socket, PathBuf::from("/tmp/s.sock"));
        assert_eq!(a.chunk_size, 1 << 20);
        assert!(a.coalesce_reads);
        assert!(!a.synthetic);
        assert_eq!(a.parent_fd, None);
    }

    /// Every flag is honored.
    #[test]
    fn parse_all_flags() {
        let a = parse(&[
            "--socket=/s",
            "--parent-fd=5",
            "--synthetic",
            "--chunk-size=4096",
            "--no-coalesce-reads",
            "--grpc-subchannels=4",
            "--worker-threads=8",
            "--stream-window=65535",
            "--connection-window=131072",
        ])
        .unwrap();
        assert_eq!(
            a,
            Args {
                socket: PathBuf::from("/s"),
                parent_fd: Some(5),
                synthetic: true,
                chunk_size: 4096,
                coalesce_reads: false,
                grpc_subchannels: Some(4),
                worker_threads: Some(8),
                stream_window: 65535,
                connection_window: 131072,
            }
        );
    }

    /// Missing/empty socket, unknown flags, non-numeric values, chunk sizes
    /// outside [1, MAX_CHUNK], and HTTP/2 flow-control windows outside
    /// [65_535, 0x7fff_ffff] are rejected.
    #[test]
    fn parse_rejects_bad_input() {
        let max_plus_one = format!("--chunk-size={}", service::MAX_CHUNK + 1);
        for bad in [
            vec![],
            vec!["--socket="],
            vec!["--socket=/s", "--bogus"],
            vec!["--socket=/s", "--parent-fd=x"],
            vec!["--socket=/s", "--parent-fd=-1"],
            vec!["--socket=/s", "--parent-fd=2"],
            vec!["--socket=/s", "--grpc-subchannels=0"],
            vec!["--socket=/s", "--worker-threads=0"],
            vec!["--socket=/s", "--chunk-size=0"],
            vec!["--socket=/s", max_plus_one.as_str()],
            vec!["--socket=/s", "--stream-window=0"],
            vec!["--socket=/s", "--stream-window=65534"],
            vec!["--socket=/s", "--stream-window=2147483648"],
            vec!["--socket=/s", "--connection-window=0"],
            vec!["--socket=/s", "--connection-window=65534"],
            vec!["--socket=/s", "--connection-window=2147483648"],
        ] {
            assert!(parse(&bad).is_err(), "{bad:?}");
        }
    }

    /// A stale socket file is replaced; a regular file at the path is never
    /// deleted.
    #[tokio::test]
    async fn bind_replaces_stale_socket_only() {
        let dir = std::env::temp_dir().join(format!("gcs-core-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let sock = dir.join("s.sock");
        drop(bind(&sock).unwrap());
        let listener = bind(&sock).unwrap();
        let mode = std::fs::metadata(&sock).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        drop(listener);
        let regular = dir.join("regular");
        std::fs::write(&regular, b"x").unwrap();
        assert!(bind(&regular).is_err());
        assert!(regular.exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
