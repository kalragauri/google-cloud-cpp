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

//! Core configuration shared by the sidecar binary and the in-process
//! library. Both accept the same `--flag=value` arguments, so a benchmark can
//! pass one set of core options to either variant; the only differences are
//! the flags that describe the boundary itself (`--socket`, `--parent-fd`,
//! `--link`).

use crate::service;
use std::path::PathBuf;

/// How the core is hosted; decides which boundary flags are legal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// `gcs-core-sidecar`: a separate process listening on a Unix socket.
    Sidecar,
    /// `libgcs_core`: tokio threads inside the application process.
    InProcess,
}

/// In-process connection between the wrapper's gRPC channels and the core.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Link {
    /// One `socketpair(AF_UNIX)` per channel; no name, so nothing outside the
    /// process can connect.
    #[default]
    Socketpair,
    /// An abstract-namespace Unix socket with a random name, served exactly
    /// like the sidecar's socket (same peer-credential check). Used only as
    /// a control for the channel type, see the plan.
    Abstract,
}

#[derive(Debug, PartialEq)]
pub struct Args {
    /// Sidecar mode only: the filesystem socket to serve on.
    pub socket: Option<PathBuf>,
    /// Sidecar mode only: read end of the parent's lifetime pipe.
    pub parent_fd: Option<i32>,
    pub synthetic: bool,
    pub chunk_size: usize,
    pub coalesce_reads: bool,
    pub grpc_subchannels: Option<usize>,
    pub worker_threads: Option<usize>,
    pub stream_window: u32,
    pub connection_window: u32,
    /// In-process mode only.
    pub link: Link,
}

/// HTTP/2 initial window bounds enforced by `hyper` / `h2`
/// (`65,535`..=`2^31 - 1`).
pub const MIN_H2_WINDOW: u32 = 65_535;
pub const MAX_H2_WINDOW: u32 = 0x7fff_ffff;

pub const USAGE: &str = "usage: gcs-core-sidecar --socket=PATH [--parent-fd=N] [--synthetic] \
[--chunk-size=BYTES (1048576)] [--no-coalesce-reads] [--grpc-subchannels=N] \
[--worker-threads=N] [--stream-window=BYTES (8388608)] [--connection-window=BYTES (16777216)]";

pub fn parse_args(mode: Mode, argv: impl IntoIterator<Item = String>) -> Result<Args, String> {
    let mut args = Args {
        socket: None,
        parent_fd: None,
        synthetic: false,
        chunk_size: 1 << 20,
        coalesce_reads: true,
        grpc_subchannels: None,
        worker_threads: None,
        stream_window: 8 << 20,
        connection_window: 16 << 20,
        link: Link::Socketpair,
    };
    fn num<T: std::str::FromStr>(key: &str, value: Option<&str>) -> Result<T, String> {
        value
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| format!("{key} needs a numeric value"))
    }
    let process_only = |key: &str| -> Result<(), String> {
        match mode {
            Mode::Sidecar => Ok(()),
            Mode::InProcess => Err(format!("{key} is not supported by the in-process core")),
        }
    };
    for arg in argv {
        let (key, value) = match arg.split_once('=') {
            Some((k, v)) => (k.to_string(), Some(v.to_string())),
            None => (arg.clone(), None),
        };
        let value = value.as_deref();
        match key.as_str() {
            "--socket" => {
                process_only(&key)?;
                args.socket = value.filter(|v| !v.is_empty()).map(PathBuf::from);
            }
            "--parent-fd" => {
                process_only(&key)?;
                let fd: i32 = num(&key, value)?;
                if fd <= 2 {
                    return Err("--parent-fd must be > 2".to_string());
                }
                args.parent_fd = Some(fd);
            }
            "--link" => {
                if mode == Mode::Sidecar {
                    return Err("--link applies only to the in-process core".to_string());
                }
                args.link = match value {
                    Some("socketpair") => Link::Socketpair,
                    Some("abstract") => Link::Abstract,
                    _ => return Err("--link must be socketpair or abstract".to_string()),
                };
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
    if mode == Mode::Sidecar && args.socket.is_none() {
        return Err("--socket is required".to_string());
    }
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

impl Args {
    pub fn service_config(&self) -> service::Config {
        service::Config {
            default_chunk: self.chunk_size,
            coalesce_reads: self.coalesce_reads,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(mode: Mode, v: &[&str]) -> Result<Args, String> {
        parse_args(mode, v.iter().map(|s| s.to_string()))
    }

    /// Defaults apply when only the socket is given.
    #[test]
    fn parse_defaults() {
        let a = parse(Mode::Sidecar, &["--socket=/tmp/s.sock"]).unwrap();
        assert_eq!(a.socket, Some(PathBuf::from("/tmp/s.sock")));
        assert_eq!(a.chunk_size, 1 << 20);
        assert!(a.coalesce_reads);
        assert!(!a.synthetic);
        assert_eq!(a.parent_fd, None);
        assert_eq!(a.link, Link::Socketpair);
    }

    /// Every flag is honored.
    #[test]
    fn parse_all_flags() {
        let a = parse(
            Mode::Sidecar,
            &[
                "--socket=/s",
                "--parent-fd=5",
                "--synthetic",
                "--chunk-size=4096",
                "--no-coalesce-reads",
                "--grpc-subchannels=4",
                "--worker-threads=8",
                "--stream-window=65535",
                "--connection-window=131072",
            ],
        )
        .unwrap();
        assert_eq!(
            a,
            Args {
                socket: Some(PathBuf::from("/s")),
                parent_fd: Some(5),
                synthetic: true,
                chunk_size: 4096,
                coalesce_reads: false,
                grpc_subchannels: Some(4),
                worker_threads: Some(8),
                stream_window: 65535,
                connection_window: 131072,
                link: Link::Socketpair,
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
            vec!["--socket=/s", "--link=socketpair"],
        ] {
            assert!(parse(Mode::Sidecar, &bad).is_err(), "{bad:?}");
        }
    }

    /// In-process: no socket is needed, the core flags keep their meaning, and
    /// `--link` selects the connection mechanism.
    #[test]
    fn in_process_accepts_core_flags_and_link() {
        let a = parse(Mode::InProcess, &[]).unwrap();
        assert_eq!(a.socket, None);
        assert_eq!(a.link, Link::Socketpair);
        let a = parse(
            Mode::InProcess,
            &[
                "--synthetic",
                "--chunk-size=4096",
                "--link=abstract",
                "--worker-threads=2",
            ],
        )
        .unwrap();
        assert!(a.synthetic);
        assert_eq!(a.chunk_size, 4096);
        assert_eq!(a.link, Link::Abstract);
        assert_eq!(a.worker_threads, Some(2));
    }

    /// In-process: the flags that describe a separate process are errors that
    /// name the flag, and `--link` accepts only the two known mechanisms.
    #[test]
    fn in_process_rejects_process_only_flags() {
        for (bad, needle) in [
            (vec!["--socket=/s"], "--socket"),
            (vec!["--parent-fd=5"], "--parent-fd"),
            (vec!["--link=tcp"], "--link"),
            (vec!["--link"], "--link"),
            (vec!["--bogus"], "--bogus"),
            (vec!["--chunk-size=0"], "--chunk-size"),
        ] {
            let err = parse(Mode::InProcess, &bad).unwrap_err();
            assert!(err.contains(needle), "{bad:?}: {err}");
        }
    }
}
