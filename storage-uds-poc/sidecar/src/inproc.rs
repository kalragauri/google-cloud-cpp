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

//! The core hosted on tokio threads inside the calling process, behind a C
//! ABI (`gcs_core_inproc_*`, declared for C++ in `cpp/src/gcs_core_inproc.h`).
//!
//! The wrapper keeps its gRPC stubs; what changes against the sidecar is only
//! where the server lives and how the channels reach it:
//!
//! * [`Link::Socketpair`]: one `socketpair(AF_UNIX)` per channel. The server
//!   ends are served by tonic, the client ends are handed to the host exactly
//!   once (gRPC takes ownership of a descriptor it wraps). Nothing outside the
//!   process can connect, so no peer check is needed.
//! * [`Link::Abstract`]: an abstract-namespace socket with a random name,
//!   served like the sidecar's socket, peer-credential check included. It
//!   exists so the benchmark can tell a channel-type effect from a boundary
//!   effect.
//!
//! Rules the library keeps for its host: it never calls `exit`, never touches
//! signal dispositions, catches panics at every entry point, and bounds every
//! blocking wait in `stop` by the host's grace period.
//!
//! SIGPIPE: a Rust *binary* ignores it at startup, so the sidecar never sees
//! it; a C++ host usually leaves it fatal. The core's vectored writes go
//! through `writev`, which raises SIGPIPE when the peer end of a socket pair
//! is already closed (a channel torn down before the server's GOAWAY, say).
//! The library therefore blocks SIGPIPE on its own threads and on the caller's
//! thread while it runs futures there: a blocked SIGPIPE from a write stays
//! pending on that thread and the write returns `EPIPE`. Dispositions are
//! untouched, so the host's other threads behave as before.

use crate::config::{self, Args, Link, Mode};
use crate::service::{Clients, Service};
use crate::transport;
use futures::{FutureExt, Stream, StreamExt};
use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{CStr, c_char, c_int, c_void};
use std::os::fd::{FromRawFd, IntoRawFd, OwnedFd};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::UnixStream;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// Host callbacks run on each core thread right after it starts and right
/// before it exits, so the host can tag the thread (the benchmark sets a
/// thread-local flag that attributes allocations to the core). `data` is
/// passed back unchanged; both callbacks may be null.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct GcsCoreThreadHooks {
    pub on_start: Option<unsafe extern "C" fn(*mut c_void)>,
    pub on_stop: Option<unsafe extern "C" fn(*mut c_void)>,
    pub data: *mut c_void,
}

/// Locks ignoring poisoning: every guarded value here stays consistent even if
/// a holder panicked, and a C ABI call must not turn a panic elsewhere into an
/// abort of the host.
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

struct Hooks(GcsCoreThreadHooks);
// SAFETY: the host promises the callbacks and `data` are safe to use from any
// thread for as long as the core runs (the C++ backend keeps them alive until
// `gcs_core_inproc_stop` has returned).
unsafe impl Send for Hooks {}
unsafe impl Sync for Hooks {}

/// CPU time of the core's threads, so the benchmark can split process CPU
/// into an application part and a core part without an RPC.
///
/// Live threads are read through their per-thread CPU clocks; a thread moves
/// its final time into `retired_us` as it exits, under the same lock the
/// reader holds, so no thread's time is ever counted twice or skipped.
#[derive(Default)]
pub(crate) struct CpuTracker {
    live: Mutex<HashMap<libc::pid_t, libc::clockid_t>>,
    retired_us: AtomicU64,
}

impl CpuTracker {
    fn thread_started(&self) {
        let mut clock: libc::clockid_t = 0;
        // SAFETY: pthread_self() is always a valid thread; `clock` is a valid
        // out-pointer.
        if unsafe { libc::pthread_getcpuclockid(libc::pthread_self(), &mut clock) } == 0 {
            lock(&self.live).insert(gettid(), clock);
        }
    }

    fn thread_stopping(&self) {
        let own = clock_us(libc::CLOCK_THREAD_CPUTIME_ID).unwrap_or(0);
        let mut live = lock(&self.live);
        live.remove(&gettid());
        self.retired_us.fetch_add(own, Ordering::Relaxed);
    }

    /// Reads the clocks under the lock: no allocation on the benchmark's
    /// calling thread, and a thread cannot retire between being listed and
    /// being read (its `thread_stopping` waits for the lock).
    pub(crate) fn total_us(&self) -> u64 {
        let live = lock(&self.live);
        let live_us: u64 = live.values().copied().filter_map(clock_us).sum();
        self.retired_us.load(Ordering::Relaxed) + live_us
    }
}

/// Blocks SIGPIPE on the calling thread for the guard's lifetime and restores
/// the previous mask after. Used on core threads (for their whole life) and
/// on the caller's thread around `block_on`.
///
/// A write that hits a closed peer while SIGPIPE is blocked leaves a SIGPIPE
/// pending on that thread; unblocking would then deliver it and kill a host
/// that keeps the default disposition. So a SIGPIPE that became pending under
/// the guard is consumed before the old mask is restored. One that was
/// already pending before the guard is the host's and is left alone.
struct BlockSigpipe {
    previous: libc::sigset_t,
    was_pending: bool,
}

fn sigpipe_pending() -> bool {
    // SAFETY: sigpending writes into a valid local set.
    unsafe {
        let mut pending: libc::sigset_t = std::mem::zeroed();
        libc::sigpending(&mut pending);
        libc::sigismember(&pending, libc::SIGPIPE) == 1
    }
}

impl BlockSigpipe {
    fn new() -> Self {
        // SAFETY: zeroed sigset_t values are valid inputs for sigemptyset,
        // and the pointers are to live locals.
        unsafe {
            let mut block: libc::sigset_t = std::mem::zeroed();
            let mut previous: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&mut block);
            libc::sigaddset(&mut block, libc::SIGPIPE);
            libc::pthread_sigmask(libc::SIG_BLOCK, &block, &mut previous);
            Self {
                previous,
                was_pending: sigpipe_pending(),
            }
        }
    }
}

impl Drop for BlockSigpipe {
    fn drop(&mut self) {
        // SAFETY: sigtimedwait with a zero timeout only dequeues a signal that
        // is pending and blocked on this thread (or process-wide); the mask
        // restored is one obtained from pthread_sigmask on this thread.
        unsafe {
            if !self.was_pending && sigpipe_pending() {
                let mut set: libc::sigset_t = std::mem::zeroed();
                libc::sigemptyset(&mut set);
                libc::sigaddset(&mut set, libc::SIGPIPE);
                let zero = libc::timespec {
                    tv_sec: 0,
                    tv_nsec: 0,
                };
                libc::sigtimedwait(&set, std::ptr::null_mut(), &zero);
            }
            libc::pthread_sigmask(libc::SIG_SETMASK, &self.previous, std::ptr::null_mut());
        }
    }
}

fn gettid() -> libc::pid_t {
    // SAFETY: no preconditions.
    unsafe { libc::gettid() }
}

fn clock_us(clock: libc::clockid_t) -> Option<u64> {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid out-pointer; an invalid clock id (a thread that
    // has exited) is reported as an error, not undefined behavior.
    if unsafe { libc::clock_gettime(clock, &mut ts) } != 0 {
        return None;
    }
    Some(ts.tv_sec as u64 * 1_000_000 + ts.tv_nsec as u64 / 1_000)
}

enum LinkEnds {
    /// Client ends, each handed to the host once (`None` after the handover).
    Socketpair(Vec<Option<OwnedFd>>),
    Abstract(String),
}

/// Upper bound for a runtime to wind down in `kill` and on drop.
const STOP_GRACE: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, PartialEq, Eq)]
enum ServerStatus {
    Running,
    Stopped(String),
}

/// Why `start` failed, so the C++ side can map it to a status code the way
/// the sidecar's exit status is mapped today.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Bad or process-only arguments; nothing was started.
    InvalidArgument = 1,
    /// Runtime, credentials, clients or sockets could not be set up.
    Failed = 2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartError {
    pub kind: ErrorKind,
    pub message: String,
}

impl StartError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::InvalidArgument,
            message: message.into(),
        }
    }
    fn failed(message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Failed,
            message: message.into(),
        }
    }
}

/// One hosted core: a runtime, the server task and the link ends. Opaque to
/// the host, which only holds the pointer returned by `gcs_core_inproc_start`.
pub struct Inproc {
    /// Process-wide sequence number of this core; its threads are named
    /// `gcs-core-<instance>-<n>`, so tools (and tests) can tell cores apart.
    instance: usize,
    runtime: Option<tokio::runtime::Runtime>,
    shutdown: Arc<Notify>,
    server: Option<JoinHandle<()>>,
    status: Arc<Mutex<ServerStatus>>,
    ends: LinkEnds,
    pub(crate) cpu: Arc<CpuTracker>,
}

type Incoming = Pin<Box<dyn Stream<Item = std::io::Result<UnixStream>> + Send>>;

/// Builds the server side of the link and whatever the host needs to reach it.
/// Must run inside the runtime's context (`UnixStream::from_std` registers
/// with the reactor).
fn make_link(link: Link, channels: usize) -> Result<(Incoming, LinkEnds), StartError> {
    match link {
        Link::Socketpair => {
            let mut server_ends = Vec::with_capacity(channels);
            let mut client_ends = Vec::with_capacity(channels);
            for _ in 0..channels {
                let mut fds = [-1 as c_int; 2];
                // SAFETY: `fds` is a valid two-element out-array.
                let rc = unsafe {
                    libc::socketpair(
                        libc::AF_UNIX,
                        libc::SOCK_STREAM | libc::SOCK_CLOEXEC,
                        0,
                        fds.as_mut_ptr(),
                    )
                };
                if rc != 0 {
                    return Err(StartError::failed(format!(
                        "socketpair: {}",
                        std::io::Error::last_os_error()
                    )));
                }
                // SAFETY: both descriptors were just created and are owned by
                // nobody else.
                let (server, client) =
                    unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
                let server = std::os::unix::net::UnixStream::from(server);
                server
                    .set_nonblocking(true)
                    .map_err(|e| StartError::failed(format!("set_nonblocking: {e}")))?;
                let server = UnixStream::from_std(server)
                    .map_err(|e| StartError::failed(format!("register socket: {e}")))?;
                server_ends.push(server);
                client_ends.push(Some(client));
            }
            // tonic treats the end of the incoming stream as a shutdown of
            // every accepted connection, so the stream stays pending until
            // the shutdown signal fires.
            let incoming = futures::stream::iter(server_ends.into_iter().map(Ok))
                .chain(futures::stream::pending());
            Ok((Box::pin(incoming), LinkEnds::Socketpair(client_ends)))
        }
        Link::Abstract => {
            use std::os::linux::net::SocketAddrExt;
            let name = format!("gcs-core-{}-{}", std::process::id(), random_hex());
            let addr = std::os::unix::net::SocketAddr::from_abstract_name(name.as_bytes())
                .map_err(|e| StartError::failed(format!("abstract address: {e}")))?;
            let listener = std::os::unix::net::UnixListener::bind_addr(&addr)
                .map_err(|e| StartError::failed(format!("bind abstract socket: {e}")))?;
            listener
                .set_nonblocking(true)
                .map_err(|e| StartError::failed(format!("set_nonblocking: {e}")))?;
            let listener = tokio::net::UnixListener::from_std(listener)
                .map_err(|e| StartError::failed(format!("register listener: {e}")))?;
            let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener)
                .filter_map(|conn| async move { transport::same_uid_only(conn) });
            Ok((Box::pin(incoming), LinkEnds::Abstract(name)))
        }
    }
}

/// 128 random bits as hex for the abstract socket name; falls back to a
/// time-and-pid hash only if `/dev/urandom` cannot be read.
fn random_hex() -> String {
    use std::io::Read;
    let mut bytes = [0_u8; 16];
    let read_ok = std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut bytes))
        .is_ok();
    if !read_ok {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let mixed = nanos ^ (u128::from(std::process::id()) << 64);
        bytes = mixed.to_le_bytes();
    }
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

impl Inproc {
    /// Parses the core arguments, starts the runtime, builds the core clients
    /// (ADC lookup happens here, on the calling thread) and spawns the server.
    /// Returns only when the server is accepting, so the host's first RPC
    /// never pays for startup.
    pub fn start(
        argv: Vec<String>,
        channels: usize,
        hooks: Option<GcsCoreThreadHooks>,
    ) -> Result<Box<Inproc>, StartError> {
        if channels == 0 {
            return Err(StartError::invalid("channels must be >= 1"));
        }
        let args: Args = config::parse_args(Mode::InProcess, argv).map_err(StartError::invalid)?;
        static INSTANCES: AtomicUsize = AtomicUsize::new(0);
        let instance = INSTANCES.fetch_add(1, Ordering::Relaxed);
        let cpu = Arc::new(CpuTracker::default());
        let hooks = Arc::new(hooks.map(Hooks));
        let thread_seq = Arc::new(AtomicUsize::new(0));

        let mut builder = tokio::runtime::Builder::new_multi_thread();
        if let Some(n) = args.worker_threads {
            builder.worker_threads(n);
        }
        builder
            .thread_name_fn(move || {
                format!(
                    "gcs-core-{instance}-{}",
                    thread_seq.fetch_add(1, Ordering::Relaxed)
                )
            })
            .on_thread_start({
                let (cpu, hooks) = (cpu.clone(), hooks.clone());
                move || {
                    // For the thread's whole life; the mask dies with it.
                    std::mem::forget(BlockSigpipe::new());
                    cpu.thread_started();
                    if let Some(Hooks(h)) = hooks.as_ref()
                        && let Some(f) = h.on_start
                    {
                        // SAFETY: contract of `GcsCoreThreadHooks`.
                        unsafe { f(h.data) }
                    }
                }
            })
            .on_thread_stop({
                let (cpu, hooks) = (cpu.clone(), hooks.clone());
                move || {
                    if let Some(Hooks(h)) = hooks.as_ref()
                        && let Some(f) = h.on_stop
                    {
                        // SAFETY: contract of `GcsCoreThreadHooks`.
                        unsafe { f(h.data) }
                    }
                    cpu.thread_stopping();
                }
            })
            .enable_all();
        let runtime = builder
            .build()
            .map_err(|e| StartError::failed(format!("cannot start the core runtime: {e}")))?;

        // From here on a failure must not leave the runtime's threads behind,
        // and must not drop it unbounded either.
        let fail = |runtime: tokio::runtime::Runtime, e: StartError| {
            let _no_sigpipe = BlockSigpipe::new();
            runtime.shutdown_timeout(STOP_GRACE);
            Err(e)
        };
        let clients = if args.synthetic {
            None
        } else {
            let built = {
                let _no_sigpipe = BlockSigpipe::new();
                runtime.block_on(Clients::build(args.grpc_subchannels))
            };
            match built {
                Ok(clients) => Some(clients),
                Err(e) => {
                    return fail(
                        runtime,
                        StartError::failed(format!("cannot build the core clients: {e}")),
                    );
                }
            }
        };
        let shutdown = Arc::new(Notify::new());
        let svc = Service::new(clients, args.service_config(), shutdown.clone());
        let router = transport::router(svc, &args);
        let linked = {
            let _guard = runtime.enter();
            make_link(args.link, channels)
        };
        let (incoming, ends) = match linked {
            Ok(link) => link,
            Err(e) => return fail(runtime, e),
        };

        let status = Arc::new(Mutex::new(ServerStatus::Running));
        let serve = router.serve_with_incoming_shutdown(incoming, {
            let shutdown = shutdown.clone();
            async move { shutdown.notified().await }
        });
        let server = runtime.spawn({
            let status = status.clone();
            async move {
                let outcome = AssertUnwindSafe(serve).catch_unwind().await;
                *lock(&status) = ServerStatus::Stopped(match outcome {
                    Ok(Ok(())) => "core server stopped".to_string(),
                    Ok(Err(e)) => format!("core server failed: {e}"),
                    Err(p) => format!("core server panicked: {}", panic_message(&*p)),
                });
            }
        });
        Ok(Box::new(Inproc {
            instance,
            runtime: Some(runtime),
            shutdown,
            server: Some(server),
            status,
            ends,
            cpu,
        }))
    }

    /// Name prefix of this core's threads (`gcs-core-<instance>-`).
    pub fn thread_prefix(&self) -> String {
        format!("gcs-core-{}-", self.instance)
    }

    pub fn link(&self) -> Link {
        match self.ends {
            LinkEnds::Socketpair(_) => Link::Socketpair,
            LinkEnds::Abstract(_) => Link::Abstract,
        }
    }

    /// Hands the client end of channel `index` to the caller, once.
    pub fn take_channel_fd(&mut self, index: usize) -> Option<OwnedFd> {
        match &mut self.ends {
            LinkEnds::Socketpair(ends) => ends.get_mut(index).and_then(Option::take),
            LinkEnds::Abstract(_) => None,
        }
    }

    pub fn abstract_name(&self) -> Option<&str> {
        match &self.ends {
            LinkEnds::Abstract(name) => Some(name),
            LinkEnds::Socketpair(_) => None,
        }
    }

    /// True while the server task runs; false once it stopped for any reason
    /// (the `Shutdown` RPC, a failure, a panic).
    pub fn alive(&self) -> bool {
        *lock(&self.status) == ServerStatus::Running
    }

    pub fn status(&self) -> String {
        match &*lock(&self.status) {
            ServerStatus::Running => "running".to_string(),
            ServerStatus::Stopped(why) => why.clone(),
        }
    }

    pub fn core_cpu_us(&self) -> u64 {
        self.cpu.total_us()
    }

    #[cfg(test)]
    pub(crate) fn runtime(&self) -> &tokio::runtime::Runtime {
        self.runtime.as_ref().expect("runtime present until stop")
    }

    /// Stops the server (in-flight RPCs are drained, as with the `Shutdown`
    /// RPC), then the runtime, each bounded by `grace`. Returns whether the
    /// server stopped within the bound; the runtime is shut down either way.
    pub fn stop(mut self, grace: Duration) -> bool {
        // Client ends the host never took are open connections the drain would
        // wait for; closing them lets the server see EOF.
        if let LinkEnds::Socketpair(ends) = &mut self.ends {
            ends.clear();
        }
        self.shutdown.notify_one();
        let runtime = self.runtime.take().expect("stop is called once");
        let _no_sigpipe = BlockSigpipe::new();
        let clean = match self.server.take() {
            Some(server) => {
                runtime.block_on(async { tokio::time::timeout(grace, server).await.is_ok() })
            }
            None => true,
        };
        runtime.shutdown_timeout(grace);
        clean
    }
}

impl Inproc {
    /// Tears the core down the way a crash would: no `Shutdown`, no GOAWAY,
    /// no draining — the server task is aborted and the runtime dropped, so
    /// peers see their connections close mid-stream. For tests of the
    /// restart path; `stop` is the orderly way.
    pub fn kill(mut self) {
        if let LinkEnds::Socketpair(ends) = &mut self.ends {
            ends.clear();
        }
        if let Some(server) = self.server.take() {
            server.abort();
        }
        *lock(&self.status) = ServerStatus::Stopped("core killed".to_string());
        let runtime = self.runtime.take().expect("kill is called once");
        let _no_sigpipe = BlockSigpipe::new();
        // Crash semantics come from the abort above (no frames reach the
        // peers), not from a short wait: the bound only caps a stuck task, and
        // threads must be gone before the host's hook data is.
        runtime.shutdown_timeout(STOP_GRACE);
    }
}

impl Drop for Inproc {
    /// A host that forgets `stop` still gets a bounded shutdown rather than a
    /// runtime drop that may wait on blocking tasks forever.
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            self.shutdown.notify_one();
            let _no_sigpipe = BlockSigpipe::new();
            runtime.shutdown_timeout(STOP_GRACE);
        }
    }
}

// --- C ABI -------------------------------------------------------------------

thread_local! {
    static LAST_ERROR: RefCell<(c_int, String)> = const { RefCell::new((0, String::new())) };
}

fn set_last_error(kind: ErrorKind, message: String) {
    LAST_ERROR.with(|e| *e.borrow_mut() = (kind as c_int, message));
}

/// Copies `text` into `buf` (NUL-terminated, truncated to `cap - 1`) and
/// returns the full length of `text`, so a host can size a buffer and retry.
///
/// # Safety
/// `buf` must be valid for `cap` bytes, or `cap` must be 0.
unsafe fn copy_out(text: &str, buf: *mut c_char, cap: usize) -> usize {
    if cap > 0 && !buf.is_null() {
        let n = text.len().min(cap - 1);
        // SAFETY: `buf` is valid for `cap >= n + 1` bytes by the caller's
        // contract; the ranges do not overlap.
        unsafe {
            std::ptr::copy_nonoverlapping(text.as_ptr(), buf.cast::<u8>(), n);
            *buf.add(n) = 0;
        }
    }
    text.len()
}

/// Starts a core. `argv`/`argc` are core arguments as for the sidecar minus
/// the process-only flags (see `config::parse_args`); `channels` is the number
/// of connections the host will open; `hooks` may be null. Returns null on
/// failure, with the reason available from `gcs_core_inproc_last_error`.
///
/// # Safety
/// `argv` must point to `argc` valid NUL-terminated strings; `hooks`, if not
/// null, must point to a valid `GcsCoreThreadHooks` whose callbacks stay valid
/// until `gcs_core_inproc_stop` returns.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_core_inproc_start(
    argv: *const *const c_char,
    argc: usize,
    channels: usize,
    hooks: *const GcsCoreThreadHooks,
) -> *mut Inproc {
    let result = catch_unwind(AssertUnwindSafe(|| {
        let mut args = Vec::with_capacity(argc);
        for i in 0..argc {
            // SAFETY: `argv` has `argc` valid C strings by the caller's contract.
            let s = unsafe { CStr::from_ptr(*argv.add(i)) };
            args.push(s.to_string_lossy().into_owned());
        }
        // SAFETY: `hooks` is null or valid by the caller's contract.
        let hooks = unsafe { hooks.as_ref() }.copied();
        Inproc::start(args, channels, hooks)
    }));
    match result {
        Ok(Ok(core)) => Box::into_raw(core),
        Ok(Err(StartError { kind, message })) => {
            set_last_error(kind, message);
            std::ptr::null_mut()
        }
        Err(payload) => {
            set_last_error(
                ErrorKind::Failed,
                format!(
                    "panic in gcs_core_inproc_start: {}",
                    panic_message(&*payload)
                ),
            );
            std::ptr::null_mut()
        }
    }
}

/// The failure reason of the last `gcs_core_inproc_start` on this thread.
///
/// # Safety
/// See `copy_out`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_core_inproc_last_error(buf: *mut c_char, cap: usize) -> usize {
    LAST_ERROR.with(|e| unsafe { copy_out(&e.borrow().1, buf, cap) })
}

/// The kind of the last failure on this thread: 1 invalid arguments, 2 the
/// core could not be started, 0 none.
#[unsafe(no_mangle)]
pub extern "C" fn gcs_core_inproc_last_error_kind() -> c_int {
    LAST_ERROR.with(|e| e.borrow().0)
}

/// 0 for `socketpair`, 1 for `abstract`.
///
/// # Safety
/// `core` must be a live pointer from `gcs_core_inproc_start`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_core_inproc_link_kind(core: *const Inproc) -> c_int {
    match unsafe { &*core }.link() {
        Link::Socketpair => 0,
        Link::Abstract => 1,
    }
}

/// Transfers ownership of channel `index`'s client descriptor to the caller;
/// -1 if the index is out of range, the descriptor was already taken, or the
/// link is not `socketpair`.
///
/// # Safety
/// `core` must be a live pointer from `gcs_core_inproc_start`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_core_inproc_take_channel_fd(core: *mut Inproc, index: usize) -> c_int {
    unsafe { &mut *core }
        .take_channel_fd(index)
        .map_or(-1, IntoRawFd::into_raw_fd)
}

/// The abstract socket name (without the leading NUL byte); 0 for `socketpair`.
///
/// # Safety
/// `core` must be a live pointer from `gcs_core_inproc_start`; see `copy_out`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_core_inproc_abstract_name(
    core: *const Inproc,
    buf: *mut c_char,
    cap: usize,
) -> usize {
    let name = unsafe { &*core }.abstract_name().unwrap_or("");
    unsafe { copy_out(name, buf, cap) }
}

/// 1 while the server task runs, else 0.
///
/// # Safety
/// `core` must be a live pointer from `gcs_core_inproc_start`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_core_inproc_alive(core: *const Inproc) -> c_int {
    c_int::from(unsafe { &*core }.alive())
}

/// Human-readable server state (`running`, or why it stopped).
///
/// # Safety
/// `core` must be a live pointer from `gcs_core_inproc_start`; see `copy_out`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_core_inproc_status(
    core: *const Inproc,
    buf: *mut c_char,
    cap: usize,
) -> usize {
    let status = unsafe { &*core }.status();
    unsafe { copy_out(&status, buf, cap) }
}

/// Cumulative CPU time (user + system, microseconds) of the core's threads.
///
/// # Safety
/// `core` must be a live pointer from `gcs_core_inproc_start`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_core_inproc_core_cpu_us(core: *const Inproc) -> u64 {
    unsafe { &*core }.core_cpu_us()
}

/// Kills the core without draining (a simulated crash) and frees it; `core`
/// is invalid afterwards.
///
/// # Safety
/// `core` must be a live pointer from `gcs_core_inproc_start`, passed exactly
/// once. Null is accepted.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_core_inproc_kill(core: *mut Inproc) {
    if core.is_null() {
        return;
    }
    // SAFETY: ownership returns to us by the caller's contract.
    let core = unsafe { Box::from_raw(core) };
    let _ = catch_unwind(AssertUnwindSafe(|| (*core).kill()));
}

/// Stops the core and frees it; `core` is invalid afterwards. Returns 1 if the
/// server stopped within `grace_ms`, else 0 (the runtime is shut down anyway).
///
/// # Safety
/// `core` must be a live pointer from `gcs_core_inproc_start`, passed exactly
/// once. The host should drop its channels first so the server's graceful
/// drain is not waiting on them.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_core_inproc_stop(core: *mut Inproc, grace_ms: u64) -> c_int {
    if core.is_null() {
        return 1;
    }
    // SAFETY: ownership returns to us by the caller's contract.
    let core = unsafe { Box::from_raw(core) };
    match catch_unwind(AssertUnwindSafe(|| {
        (*core).stop(Duration::from_millis(grace_ms))
    })) {
        Ok(clean) => c_int::from(clean),
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::AsRawFd;
    use std::time::Instant;

    /// Threads of one core, by name prefix; robust against the other tests
    /// cargo runs in parallel in this process.
    fn core_threads(prefix: &str) -> usize {
        std::fs::read_dir("/proc/self/task")
            .map(|tasks| {
                tasks
                    .flatten()
                    .filter(|t| {
                        std::fs::read_to_string(t.path().join("comm"))
                            .map(|name| name.starts_with(prefix))
                            .unwrap_or(false)
                    })
                    .count()
            })
            .unwrap_or(0)
    }

    fn synthetic(extra: &[&str], channels: usize) -> Box<Inproc> {
        let mut argv = vec!["--synthetic".to_string(), "--worker-threads=2".to_string()];
        argv.extend(extra.iter().map(|s| s.to_string()));
        Inproc::start(argv, channels, None).expect("synthetic core starts")
    }

    /// Waits for the core's threads to be gone after `stop`; tokio joins its
    /// workers on shutdown, so this converges quickly.
    fn wait_for_core_threads_gone(prefix: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while core_threads(prefix) > 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(core_threads(prefix), 0, "core threads still alive");
    }

    /// The flags that describe a separate process, an unknown flag, and zero
    /// channels are rejected before any thread is started.
    #[test]
    fn start_rejects_bad_configuration() {
        for (argv, needle) in [
            (vec!["--socket=/tmp/x.sock"], "--socket"),
            (vec!["--parent-fd=5"], "--parent-fd"),
            (vec!["--bogus"], "--bogus"),
        ] {
            let err = Inproc::start(argv.iter().map(|s| s.to_string()).collect(), 1, None)
                .err()
                .expect("rejected");
            assert_eq!(err.kind, ErrorKind::InvalidArgument);
            assert!(err.message.contains(needle), "{argv:?}: {}", err.message);
        }
        let err = Inproc::start(vec!["--synthetic".to_string()], 0, None)
            .err()
            .unwrap();
        assert_eq!(err.kind, ErrorKind::InvalidArgument);
        assert!(err.message.contains("channels"), "{}", err.message);
    }

    /// `socketpair` yields one client socket per channel, each handed out once;
    /// stopping with no connection in flight is clean and leaves no thread.
    #[test]
    fn socketpair_link_hands_out_each_fd_once_and_stops_clean() {
        let mut core = synthetic(&[], 3);
        let prefix = core.thread_prefix();
        assert_eq!(core.link(), Link::Socketpair);
        assert!(core.alive());
        assert!(
            core_threads(&prefix) >= 2,
            "workers: {}",
            core_threads(&prefix)
        );
        let mut fds = Vec::new();
        for i in 0..3 {
            let fd = core.take_channel_fd(i).expect("fd present");
            let mut st = std::mem::MaybeUninit::<libc::stat>::zeroed();
            // SAFETY: valid fd and out-pointer.
            assert_eq!(unsafe { libc::fstat(fd.as_raw_fd(), st.as_mut_ptr()) }, 0);
            // SAFETY: fstat succeeded.
            let mode = unsafe { st.assume_init() }.st_mode;
            assert_eq!(mode & libc::S_IFMT, libc::S_IFSOCK);
            fds.push(fd);
        }
        assert!(core.take_channel_fd(0).is_none(), "second take must fail");
        assert!(core.take_channel_fd(3).is_none(), "out of range");
        assert!(core.abstract_name().is_none());
        drop(fds);
        assert!(core.stop(Duration::from_secs(2)));
        wait_for_core_threads_gone(&prefix);
    }

    /// A core whose client ends were never taken still stops clean: `stop`
    /// closes them so the server's drain is not waiting on the host.
    #[test]
    fn stop_is_clean_when_client_ends_were_never_taken() {
        let core = synthetic(&[], 2);
        let prefix = core.thread_prefix();
        let started = Instant::now();
        assert!(core.stop(Duration::from_secs(2)));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{:?}",
            started.elapsed()
        );
        wait_for_core_threads_gone(&prefix);
    }

    /// The abstract link has a name, accepts a same-uid connection, and stop
    /// stays within its bound even with that connection still open.
    #[test]
    fn abstract_link_accepts_connection_and_stop_is_bounded() {
        use std::os::linux::net::SocketAddrExt;
        let core = synthetic(&["--link=abstract"], 1);
        let prefix = core.thread_prefix();
        assert_eq!(core.link(), Link::Abstract);
        let name = core.abstract_name().expect("name").to_string();
        assert!(name.starts_with("gcs-core-"), "{name}");
        let addr = std::os::unix::net::SocketAddr::from_abstract_name(name.as_bytes()).unwrap();
        let client = std::os::unix::net::UnixStream::connect_addr(&addr).expect("connect");
        let started = Instant::now();
        core.stop(Duration::from_millis(500));
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "stop took {:?}",
            started.elapsed()
        );
        drop(client);
        wait_for_core_threads_gone(&prefix);
    }

    /// `kill` closes a connected peer's socket without a handshake and leaves
    /// no core thread behind.
    #[test]
    fn kill_closes_peers_abruptly() {
        use std::io::Read;
        let mut core = synthetic(&[], 1);
        let prefix = core.thread_prefix();
        let fd = core.take_channel_fd(0).expect("fd");
        let mut peer = std::os::unix::net::UnixStream::from(fd);
        peer.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        core.kill();
        let mut buf = [0_u8; 64];
        // EOF (0) or a reset: either way the read returns promptly.
        let started = Instant::now();
        let _ = peer.read(&mut buf);
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "{:?}",
            started.elapsed()
        );
        wait_for_core_threads_gone(&prefix);
    }

    /// A panic on a core thread is contained by the task boundary: the
    /// process survives, the server keeps running, and stop is clean.
    #[test]
    fn panic_on_core_thread_does_not_take_the_server_down() {
        let core = synthetic(&[], 1);
        let prefix = core.thread_prefix();
        let joined = core.runtime().block_on(
            core.runtime()
                .spawn(async { panic!("boom on a core thread") }),
        );
        assert!(joined.unwrap_err().is_panic());
        assert!(core.alive());
        assert_eq!(core.status(), "running");
        assert!(core.stop(Duration::from_secs(2)));
        wait_for_core_threads_gone(&prefix);
    }

    /// Core threads run with SIGPIPE blocked (a closed peer must yield EPIPE
    /// in a host that did not ignore SIGPIPE), and the caller's own mask is
    /// restored after start and stop.
    #[test]
    fn core_threads_block_sigpipe_and_caller_mask_is_restored() {
        fn sigpipe_blocked() -> bool {
            // SAFETY: querying the mask of the calling thread.
            unsafe {
                let mut set: libc::sigset_t = std::mem::zeroed();
                libc::pthread_sigmask(libc::SIG_BLOCK, std::ptr::null(), &mut set);
                libc::sigismember(&set, libc::SIGPIPE) == 1
            }
        }
        assert!(
            !sigpipe_blocked(),
            "test thread starts with SIGPIPE unblocked"
        );
        let core = synthetic(&[], 1);
        assert!(!sigpipe_blocked(), "caller's mask restored after start");
        let on_core = core
            .runtime()
            .block_on(core.runtime().spawn(async { sigpipe_blocked() }))
            .unwrap();
        assert!(on_core, "core thread has SIGPIPE blocked");
        core.stop(Duration::from_secs(2));
        assert!(!sigpipe_blocked(), "caller's mask restored after stop");
    }

    /// A SIGPIPE raised while the guard blocks it is consumed when the guard
    /// ends (otherwise unblocking would deliver it to a host that keeps the
    /// default disposition); one already pending before the guard is left
    /// for its owner. An outer guard keeps SIGPIPE blocked so "still pending"
    /// is observable without ever delivering it. Runs on its own thread
    /// because the mask and thread-directed signals are per thread.
    #[test]
    fn sigpipe_raised_under_the_guard_is_consumed() {
        fn raise_on_this_thread() {
            // SAFETY: sends SIGPIPE to the calling thread, where it is blocked.
            unsafe { libc::pthread_kill(libc::pthread_self(), libc::SIGPIPE) };
        }
        std::thread::spawn(|| {
            let _outer = BlockSigpipe::new();
            {
                let _inner = BlockSigpipe::new();
                raise_on_this_thread();
                assert!(sigpipe_pending());
            }
            assert!(!sigpipe_pending(), "raised under the guard: consumed");

            raise_on_this_thread();
            {
                let _inner = BlockSigpipe::new();
            }
            assert!(sigpipe_pending(), "pending before the guard: left alone");
            // Clean up the one we left, still under `_outer`.
            {
                let _consume = BlockSigpipe {
                    // SAFETY: an all-zero sigset_t is a valid (empty) set.
                    previous: unsafe {
                        let mut set: libc::sigset_t = std::mem::zeroed();
                        libc::pthread_sigmask(libc::SIG_BLOCK, std::ptr::null(), &mut set);
                        set
                    },
                    was_pending: false,
                };
            }
            assert!(!sigpipe_pending());
        })
        .join()
        .unwrap();
    }

    /// Starting and stopping a core leaves the process's signal dispositions
    /// alone (the sidecar binary installs its own handlers; the library must
    /// not).
    #[test]
    fn start_does_not_change_signal_dispositions() {
        fn handler(sig: c_int) -> usize {
            let mut act = std::mem::MaybeUninit::<libc::sigaction>::zeroed();
            // SAFETY: querying with a null `act` only reads the disposition.
            assert_eq!(
                unsafe { libc::sigaction(sig, std::ptr::null(), act.as_mut_ptr()) },
                0
            );
            // SAFETY: sigaction succeeded.
            unsafe { act.assume_init() }.sa_sigaction
        }
        let signals = [libc::SIGTERM, libc::SIGINT, libc::SIGPIPE, libc::SIGHUP];
        let before: Vec<usize> = signals.iter().map(|s| handler(*s)).collect();
        let core = synthetic(&[], 1);
        let during: Vec<usize> = signals.iter().map(|s| handler(*s)).collect();
        core.stop(Duration::from_secs(2));
        let after: Vec<usize> = signals.iter().map(|s| handler(*s)).collect();
        assert_eq!(before, during);
        assert_eq!(before, after);
    }

    /// Core CPU counts work done on core threads and not work done on the
    /// caller's thread.
    #[test]
    fn core_cpu_counts_core_threads_only() {
        fn spin(d: Duration) {
            let start = Instant::now();
            let mut x = 0_u64;
            while start.elapsed() < d {
                x = x
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                std::hint::black_box(x);
            }
        }
        let core = synthetic(&[], 1);
        let idle = core.core_cpu_us();
        spin(Duration::from_millis(60));
        let after_caller_spin = core.core_cpu_us();
        assert!(
            after_caller_spin - idle < 20_000,
            "caller work attributed to the core: {} us",
            after_caller_spin - idle
        );
        core.runtime()
            .block_on(
                core.runtime()
                    .spawn(async { spin(Duration::from_millis(60)) }),
            )
            .unwrap();
        let after_core_spin = core.core_cpu_us();
        assert!(
            after_core_spin - after_caller_spin >= 40_000,
            "core work not attributed: {} us",
            after_core_spin - after_caller_spin
        );
        assert!(core.stop(Duration::from_secs(2)));
    }

    /// Thread hooks run on every core thread, start and stop, with the host's
    /// data pointer.
    #[test]
    fn thread_hooks_run_on_core_threads() {
        static STARTS: AtomicU64 = AtomicU64::new(0);
        static STOPS: AtomicU64 = AtomicU64::new(0);
        unsafe extern "C" fn on_start(data: *mut c_void) {
            assert_eq!(data as usize, 0x1234);
            STARTS.fetch_add(1, Ordering::SeqCst);
        }
        unsafe extern "C" fn on_stop(data: *mut c_void) {
            assert_eq!(data as usize, 0x1234);
            STOPS.fetch_add(1, Ordering::SeqCst);
        }
        let hooks = GcsCoreThreadHooks {
            on_start: Some(on_start),
            on_stop: Some(on_stop),
            data: 0x1234 as *mut c_void,
        };
        let core = Inproc::start(
            vec!["--synthetic".to_string(), "--worker-threads=2".to_string()],
            1,
            Some(hooks),
        )
        .unwrap();
        assert!(core.stop(Duration::from_secs(2)));
        let deadline = Instant::now() + Duration::from_secs(5);
        while STOPS.load(Ordering::SeqCst) < STARTS.load(Ordering::SeqCst)
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            STARTS.load(Ordering::SeqCst) >= 2,
            "starts={}",
            STARTS.load(Ordering::SeqCst)
        );
        assert_eq!(STARTS.load(Ordering::SeqCst), STOPS.load(Ordering::SeqCst));
    }

    /// The C entry points: a rejected start returns null and the reason;
    /// status/alive/link/fd/stop round-trip through raw pointers.
    #[test]
    fn c_abi_round_trip() {
        let bad = std::ffi::CString::new("--socket=/x").unwrap();
        let argv = [bad.as_ptr()];
        // SAFETY: valid argv of length 1.
        let core = unsafe { gcs_core_inproc_start(argv.as_ptr(), 1, 1, std::ptr::null()) };
        assert!(core.is_null());
        let mut buf = vec![0 as c_char; 256];
        // SAFETY: buffer of the given capacity.
        let len = unsafe { gcs_core_inproc_last_error(buf.as_mut_ptr(), buf.len()) };
        let message = unsafe { CStr::from_ptr(buf.as_ptr()) }
            .to_string_lossy()
            .into_owned();
        assert_eq!(len, message.len());
        assert!(message.contains("--socket"), "{message}");
        assert_eq!(gcs_core_inproc_last_error_kind(), 1);

        let good = [
            std::ffi::CString::new("--synthetic").unwrap(),
            std::ffi::CString::new("--worker-threads=2").unwrap(),
        ];
        let argv: Vec<*const c_char> = good.iter().map(|s| s.as_ptr()).collect();
        // SAFETY: valid argv of length 2.
        let core = unsafe { gcs_core_inproc_start(argv.as_ptr(), argv.len(), 2, std::ptr::null()) };
        assert!(!core.is_null());
        // SAFETY: live core pointer.
        unsafe {
            assert_eq!(gcs_core_inproc_link_kind(core), 0);
            assert_eq!(gcs_core_inproc_alive(core), 1);
            let n = gcs_core_inproc_status(core, buf.as_mut_ptr(), buf.len());
            assert_eq!(CStr::from_ptr(buf.as_ptr()).to_str().unwrap(), "running");
            assert_eq!(n, "running".len());
            assert_eq!(
                gcs_core_inproc_abstract_name(core, buf.as_mut_ptr(), buf.len()),
                0
            );
            let fd0 = gcs_core_inproc_take_channel_fd(core, 0);
            let fd1 = gcs_core_inproc_take_channel_fd(core, 1);
            assert!(fd0 >= 0 && fd1 >= 0 && fd0 != fd1);
            assert_eq!(gcs_core_inproc_take_channel_fd(core, 0), -1);
            assert_eq!(gcs_core_inproc_take_channel_fd(core, 2), -1);
            libc::close(fd0);
            libc::close(fd1);
            // Truncation reports the full length and NUL-terminates.
            let mut tiny = [0 as c_char; 4];
            assert_eq!(
                gcs_core_inproc_status(core, tiny.as_mut_ptr(), tiny.len()),
                "running".len()
            );
            assert_eq!(CStr::from_ptr(tiny.as_ptr()).to_str().unwrap(), "run");
            assert_eq!(gcs_core_inproc_stop(core, 2_000), 1);
            assert_eq!(gcs_core_inproc_stop(std::ptr::null_mut(), 0), 1);
        }
    }
}
