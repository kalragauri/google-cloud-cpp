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
//!
//! The service, its configuration and the server settings come from the
//! `gcs_core` library, which the in-process variant links into C++ instead.

#[path = "malloc_interpose.rs"]
mod malloc_interpose;

use futures::StreamExt;
use gcs_core::config::{self, Args, Mode};
use gcs_core::{service, transport};
use std::os::unix::fs::PermissionsExt;
use std::process::ExitCode;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::UnixListener;
use tokio::sync::Notify;
use tokio_stream::wrappers::UnixListenerStream;

fn main() -> ExitCode {
    let args = match config::parse_args(Mode::Sidecar, std::env::args().skip(1)) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("gcs-core-sidecar: {e}\n{}", config::USAGE);
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

async fn run(args: Args) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let socket = args
        .socket
        .clone()
        .expect("parse_args requires --socket in sidecar mode");
    let clients = if args.synthetic {
        None
    } else {
        Some(service::Clients::build(args.grpc_subchannels).await?)
    };
    let shutdown = Arc::new(Notify::new());
    let svc = service::Service::new(clients, args.service_config(), shutdown.clone());

    let listener = bind(&socket)?;
    let incoming = UnixListenerStream::new(listener)
        .filter_map(|conn| async move { transport::same_uid_only(conn) });

    if let Some(fd) = args.parent_fd {
        tokio::spawn(watch_parent(fd, shutdown.clone()));
    }
    tokio::spawn(watch_signals(shutdown.clone()));

    eprintln!(
        "gcs-core-sidecar: listening on {} pid={} synthetic={} bidi={}",
        socket.display(),
        std::process::id(),
        args.synthetic,
        cfg!(google_cloud_unstable_storage_bidi)
    );
    let result = transport::router(svc, &args)
        .serve_with_incoming_shutdown(Box::pin(incoming), async move { shutdown.notified().await })
        .await;
    let _ = std::fs::remove_file(&socket);
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
