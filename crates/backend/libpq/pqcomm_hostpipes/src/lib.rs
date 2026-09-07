//! pqcomm_hostpipes: the HOST-PIPES transport provider — a full postmaster
//! (N backend threads, one session each) whose every I/O object is a plain
//! file descriptor handed in by the host. No `socket()`, no `bind()`, no
//! `accept()`, no `listen()`, no unix socket file: exactly the surface
//! wasm32-wasip1(-threads) can support, proved natively over OS pipes.
//!
//! FOURTH consumer of the §2.4 transport-provider seam
//! (docs/design/dst-and-wasm.md), behind the SAME set-once slots:
//!
//!   1. socket      (native default)  be_secure::init_seams + pqcomm::init_socket_seams
//!   2. stdio       (--stdio-wire)    pqcomm_stdio::init_transport_seams
//!   3. sim-net     (--sim-net)       pqcomm_simnet::init_transport_seams
//!   4. host-pipes  (--host-pipes)    THIS crate
//!
//! Where the stdio provider serves ONE session over fds 0/1 and never runs
//! the postmaster, this provider serves MANY concurrent sessions under the
//! real `PostmasterMain`/`ServerLoop`, so it installs the postmaster half of
//! the seam too (`listen_server_port` / `accept_connection`).
//!
//! # The fd contract (the whole thing the wasm host has to honour)
//!
//! **Listener.** One host-owned fd, named by the environment variable
//! [`LISTEN_FD_ENV`] (`PGRUST_HOSTPIPES_LISTEN_FD`). `listen_server_port`
//! ignores hostname/port/unix_socket_dir entirely and returns that fd as the
//! single server fd. Natively it is the read end of an OS pipe; on wasm the
//! host backs it with a SharedArrayBuffer pipe. The postmaster waits on it
//! for readability (natively: epoll, as for any listen socket; on wasm: a
//! timed latch wait plus a zero-timeout `poll` probe — see ServerLoop).
//!
//! **Connection record.** The host announces each new connection by writing
//! exactly [`CONN_RECORD_LEN`] = 16 bytes to the listener fd, little-endian:
//!
//! ```text
//!   offset  size  field
//!   0       u32   magic      0x50475048 — the bytes "HPGP" in stream order
//!   4       i32   in_fd      server READS client->server bytes here
//!   8       i32   out_fd     server WRITES server->client bytes here
//!   12      i32   wake_fd    this session's WAKE token ring, or 0 for none
//! ```
//!
//! `wake_fd` is the slot the first cut of this record reserved. It is
//! OPTIONAL and 0 means "none" (fd 0 is stdin and can never be a wake ring),
//! so a host that predates it announces connections exactly as before. When
//! present it names a host-backed pipe that is BOTH ends at once — the
//! backend reads it and any thread writes it — which the backend adopts as
//! its waiter's wake pipe for the life of the session. See "Blocking
//! semantics" below: it is what makes a `SetLatch` from another backend
//! reach a session parked in a `read`.
//!
//! `accept_connection` reads exactly one record with a BLOCKING `read`,
//! looping over short reads and EINTR, and returns a `ClientSocket` whose
//! `sock` is `in_fd`.
//!
//! **The fd pair map.** `ClientSocket` carries ONE fd (`sock`) — C's accepted
//! socket is one bidirectional object and the whole backend path is typed
//! that way. A host-pipes connection is two unidirectional objects, so the
//! second fd travels in [`OUT_FDS`], a process-global `in_fd -> out_fd` map:
//! inserted by `accept_connection` (postmaster thread), read ONCE by
//! `pq_init` (the backend thread that inherited the ClientSocket) into that
//! backend's thread-local [`STATE`], and erased when the session's fds are
//! closed. The hot byte path never touches the map — it is a handoff
//! channel between two threads, not a per-write lookup.
//!
//! **Blocking semantics.** Both session fds stay in BLOCKING mode, exactly
//! as the stdio provider leaves fds 0/1, and the emulated-noblock arms
//! delegate to the shared `fdnb` crate (a zero-timeout `poll(2)`, which
//! works on pipes natively and reaches the wasm host's `poll_oneoff`).
//!
//! A BLOCKED byte op is where this transport used to differ from C in the
//! one way that a client can feel. C blocks in
//! `WaitEventSetWait(FeBeWaitSet, …)` on `WL_SOCKET_READABLE | WL_LATCH_SET`,
//! so a backend that sets an idle session's latch — an async NOTIFY, a
//! cancel, a fast shutdown — wakes it AT ONCE. This provider had no wait set
//! and polled the fd with a [`INTERRUPT_POLL_MS`] bound instead, which made
//! every latch-carried event arrive uniformly 0–100 ms late (measured: a
//! cross-session NOTIFY to an IDLE listener, median ~50 ms).
//!
//! [`wait_client_io`] restores C's property with the pieces this transport
//! has: a `poll(2)` over TWO fds — the session fd and this backend's WAKE fd
//! — is the wait set, and the waiter's FD-PARK mode is `WL_LATCH_SET`
//! (`SetLatch` from any thread then writes a token byte to that wake fd
//! instead of signalling a condvar the poll cannot see). The wake fd is
//! this thread's own `pipe(2)` natively (`waiter::ensure_wake_pipe`) and the
//! host-announced ring on wasm, where WASI p1 has no `pipe(2)`. The
//! [`INTERRUPT_POLL_MS`] poll remains as the FALLBACK for a connection with
//! no wake fd at all — a wasm host that announces `wake_fd = 0`, which is
//! every host built before the slot was filled in.
//!
//! Still no `FeBeWaitSet`: a `WaitEventSet` cannot hold these fds. The wasm
//! backend rejects every event with a real fd by construction (WASI p1 has
//! no sockets, and its wait is a futex park no host fd can reach), and a
//! pipe PAIR would need two socket positions where C's set has one, since
//! `ModifyWaitEvent` can change an event's mask but never its fd.
//!
//! **Close.** The backend closes BOTH fds at session end (`secure_close`,
//! also registered as an `on_proc_exit` callback by `pq_init`) and drops the
//! map entry; the client end of a pipe then sees EOF. The postmaster keeps
//! no copy of either fd (the child thread shares the fd table, so there is
//! nothing for it to close — C's post-fork close has no analogue here).
//!
//! Determinism ledger: the `libc::read/write/close` sites below are the
//! raw-IO surface OF this transport provider, the same standing as
//! be_secure's `libc::recv/send` rows and pqcomm_stdio's read/write rows —
//! they live BEHIND the §2.4 seam, so the sim consumer never executes them.

use std::cell::Cell;
use std::collections::HashMap;
use std::sync::LazyLock;

use elog::ereport;
use pgsync::Mutex;
use types_error::{ErrorLocation, PgResult, ERROR, LOG};
use types_startup::{ClientSocket, Port};

/// The environment variable naming the host-owned listener fd. Required
/// when `--host-pipes` selects this transport; the postmaster FATALs
/// without it (there is no other way to name a host-owned fd — WASI p1 has
/// no `socket()` and no name service).
pub const LISTEN_FD_ENV: &str = "PGRUST_HOSTPIPES_LISTEN_FD";

/// The environment variable naming a host-owned POSTMASTER WAKE fd, if the
/// host offers one. OPTIONAL: without it the postmaster falls back to the
/// timed accept probe it used before (ServerLoop's wasm arm). With it, the
/// named fd is a host-backed pipe that is BOTH ends at once — the guest
/// writes a token byte to it and reads the token back off it — and it is the
/// single object the postmaster blocks on:
///
///   * the HOST writes a byte after every connection record it puts on the
///     listener, and after closing the listener, so an accept no longer waits
///     for the next probe tick;
///   * a GUEST thread's `SetLatch` on the postmaster latch writes the same
///     byte, because the postmaster's waiter is in fd-park mode for the
///     duration of the block (`waiter::adopt_wake_pipe`) — the wake route
///     `pipe(2)` gives every native backend, handed in by the host instead
///     because WASI p1 has no `pipe(2)`.
///
/// This is the POSTMASTER's ring and nothing else: a session backend gets
/// its own through the connection record's `wake_fd` field. Sharing one
/// would be unsound in the one way that matters — both sides DRAIN, and a
/// backend that swallowed the token announcing a connection would leave the
/// postmaster asleep on it.
///
/// The bytes carry no information: they are wake tokens, drained and thrown
/// away. That is what makes the several-writers-one-reader arrangement sound
/// on a ring whose ordinary contract is single-producer.
pub const WAKE_FD_ENV: &str = "PGRUST_HOSTPIPES_WAKE_FD";

/// Connection-record magic: little-endian `0x50475048` = the bytes
/// `H P G P` in stream order (a host writing the ASCII tag byte-by-byte and
/// a host writing a LE u32 agree).
pub const CONN_RECORD_MAGIC: u32 = 0x5047_5048;

/// Fixed connection-record size, in bytes (magic, in_fd, out_fd, wake_fd).
pub const CONN_RECORD_LEN: usize = 16;

/// `in_fd -> (out_fd, wake_fd)` for every live host-pipes connection.
/// Written by the postmaster thread at accept, read by the backend thread at
/// `pq_init`, erased at close. See the crate docs ("The fd pair map"): this
/// exists because `ClientSocket` carries a single fd and a host-pipes
/// connection is a pipe pair plus (optionally) a wake ring.
static OUT_FDS: LazyLock<Mutex<HashMap<i32, (i32, i32)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// One host-pipes session's fds, as the backend thread holds them. The
/// host-pipes twin of pqcomm::socket's CLIENT_STATE (which is
/// (sock, noblock, ssl_in_use); TLS never runs on this transport, the fd is
/// a pair, and the wake fd is what a BLOCKED op parks on next to it).
#[derive(Clone, Copy)]
struct Conn {
    in_fd: i32,
    out_fd: i32,
    /// The fd a blocked op adds to its `poll` so a `SetLatch` ends the wait,
    /// or `PGINVALID_SOCKET` when this connection has no wake route (the
    /// [`INTERRUPT_POLL_MS`] fallback). Natively this thread's own `pipe(2)`
    /// (waiter-owned); on wasm the host ring named in the connection record.
    wake_fd: i32,
    /// True when `wake_fd` is a HOST fd this thread ADOPTED: released at
    /// close and never closed, because the host owns it. False for the
    /// waiter's own `pipe(2)`, which the waiter slot closes at thread exit.
    wake_adopted: bool,
    noblock: bool,
}

thread_local! {
    // Some(Conn) once this backend's pq_init ran.
    static STATE: Cell<Option<Conn>> = const { Cell::new(None) };
}

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report OUR source site (call site via track_caller).
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn errno() -> i32 {
    // SAFETY: __error returns this thread's valid errno location.
    unsafe { *libc::__error() }
}

#[cfg(all(unix, not(any(target_os = "macos", target_os = "ios"))))]
fn errno() -> i32 {
    // SAFETY: __errno_location returns this thread's valid errno location.
    unsafe { *libc::__errno_location() }
}

#[cfg(target_family = "wasm")]
fn errno() -> i32 {
    // wasi-libc's errno is thread-local storage exposed as __errno_location.
    // SAFETY: same contract as the unix arms.
    unsafe { *libc::__errno_location() }
}

/// The seam's `ssize_t` contract (be_secure's ssize_result twin):
/// `Ok(0)` is EOF, `Err(errno)` is the C `-1`.
fn ssize_result(n: isize, e: i32) -> Result<usize, i32> {
    if n >= 0 {
        Ok(n as usize)
    } else {
        Err(e)
    }
}

fn state() -> Option<Conn> {
    STATE.get()
}

/// How long a BLOCKED byte op parks before checking interrupts again, in ms
/// — on a connection with NO WAKE FD, which is the only place this is still
/// reached ([`wait_client_io`]).
///
/// This bound is the whole reason a wake-less blocking arm polls instead of
/// calling `read`/`write` straight: an uninterruptible block is a wedge. The
/// socket provider blocks in `WaitEventSetWait(FeBeWaitSet, ...)`, which the
/// latch wakes, and calls `ProcessClientRead/WriteInterrupt(true)` on every
/// wake — that is how a fast shutdown reaps an IDLE backend, how
/// authentication_timeout fires against a half-open connection, and how a
/// cancel lands. A pipe has no wait set and (measured) does NOT return EINTR
/// on the thread model's stop signal, so a plain blocking `read` parks a
/// session backend forever: PM_WAIT_BACKENDS then stalls the full 60 s to
/// the GL-GANGWEDGE watchdog, escalates to immediate shutdown, and the
/// postmaster force-exits with code 1 — a clean `SIGINT` turned into crash
/// recovery by one idle client. Polling with a bound restores the SAFETY
/// property at the cost of one wake per 100 ms per BLOCKED session — but it
/// only ever restores the WORST CASE of C's latency: with the poll alone,
/// everything the latch carries arrives uniformly 0–100 ms late. That is
/// what the wake fd removes.
const INTERRUPT_POLL_MS: i32 = 100;

/// `poll(fd, events, INTERRUPT_POLL_MS)`: >0 ready, 0 timeout, <0 error.
/// A poll ERROR reports ready (fdnb's N2 rule): the caller's read/write then
/// surfaces the real errno (EBADF/…) or EOF instead of spinning.
fn poll_bounded(fd: i32, events: i16) -> i32 {
    let mut pfd = libc::pollfd { fd, events, revents: 0 };
    // SAFETY: pfd is a valid single-entry pollfd for the duration of the call.
    let rc = unsafe { libc::poll(&mut pfd, 1, INTERRUPT_POLL_MS) };
    if rc < 0 && errno() != libc::EINTR {
        return 1; // N2: fall through and let the op surface the error
    }
    rc
}

/// `poll({fd, events}, {wake_fd, POLLIN}, timeout_ms)`. Returns
/// `(fd fired, wake_fd fired, rc)`. ANY revent on the session fd counts as
/// fired — POLLHUP/POLLERR mean the following read/write must run and
/// surface EOF or the errno, exactly as `poll_bounded`'s `rc > 0` did.
fn poll_pair(fd: i32, events: i16, wake_fd: i32, timeout_ms: i64) -> (bool, bool, i32) {
    let mut pfds = [
        libc::pollfd { fd, events, revents: 0 },
        libc::pollfd { fd: wake_fd, events: libc::POLLIN, revents: 0 },
    ];
    let t = timeout_ms.clamp(-1, i32::MAX as i64) as i32;
    // SAFETY: pfds is a valid 2-entry pollfd array for the duration of the call.
    let rc = unsafe { libc::poll(pfds.as_mut_ptr(), 2, t) };
    (pfds[0].revents != 0, pfds[1].revents != 0, rc)
}

/// STRICT zero-timeout readiness probe (ServerLoop's `fd_is_readable`): only
/// a genuine POLLIN counts, so a dead fd can never be read as "a token is
/// waiting" and turned into a BLOCKING read on the wasm host's pipes.
fn wake_fd_readable(fd: i32) -> bool {
    let mut pfd = libc::pollfd { fd, events: libc::POLLIN, revents: 0 };
    // SAFETY: pfd is a valid single-entry pollfd for the duration of the call.
    let rc = unsafe { libc::poll(&mut pfd, 1, 0) };
    rc > 0 && (pfd.revents & libc::POLLIN) != 0
}

/// Throw away every wake token queued on this backend's wake fd — the twin
/// of ServerLoop's `drain_wake_fd`, and bounded for the same reason: the
/// bytes carry no information, draining is only what keeps the ring from
/// filling and the next park from returning on a stale token, and a wake fd
/// that reports readable but yields nothing must not spin a backend.
///
/// Poll-gated on every lap because the wasm host's pipes are BLOCKING (the
/// native ones are `O_NONBLOCK` and would answer EAGAIN).
fn drain_wake_fd(fd: i32) {
    let mut buf = [0u8; 256];
    for _ in 0..64 {
        if !wake_fd_readable(fd) {
            return;
        }
        // SAFETY: buf is valid writable memory of the stated length.
        let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
        if n <= 0 {
            return;
        }
    }
}

/// ONE blocked lap on a session fd. `true` = attempt the I/O now (the fd
/// reported ready, or the poll failed and the op must surface the real
/// errno — fdnb's N2 rule); `false` = the caller runs
/// `ProcessClientRead/WriteInterrupt(true)` and comes back, exactly as C
/// does on every `FeBeWaitSet` wake that carried `WL_LATCH_SET`.
///
/// The latch half is `waiteventset::wait_loop`'s protocol and ServerLoop's
/// `pm_park_on_wake_fd`, verbatim: publish the waker into `Latch.waker`,
/// arm `maybe_sleeping`, RE-CHECK `is_set` (the Dekker arm — the Acquire
/// pairing lives in `latch::set_latch`), enter fd-park so a cross-thread
/// unpark writes the wake fd instead of signalling a condvar, block, leave
/// fd-park, disarm. A set latch is RESET here and reported as "not ready",
/// which is `be_secure::secure_read`'s `reset_latch_my_latch` + `continue`.
///
/// With no wake fd (`PGINVALID_SOCKET`) this degrades to the historical
/// [`INTERRUPT_POLL_MS`] poll: nothing else can end a park early, so the
/// bound IS the interrupt latency.
fn wait_client_io(fd: i32, events: i16, wake_fd: i32) -> bool {
    use std::sync::atomic::Ordering::{Release, SeqCst};

    if wake_fd < 0 {
        return poll_bounded(fd, events) > 0;
    }

    let handle = init_small::globals::MyLatch();
    let l = handle.map(latch::latch_ref);
    let mut fd_parked = false;
    if let Some(l) = l {
        if !l.is_set() {
            l.waker.store(waiter::current_handle().as_u64(), Release);
            l.set_maybe_sleeping(true);
        }
        if l.is_set() {
            // Already set: report it without blocking, as the native wait
            // loop does (it degrades the block to a zero-timeout poll).
            l.set_maybe_sleeping(false);
            reset_my_latch(handle);
            drain_wake_fd(wake_fd);
            return false;
        }
        fd_parked = waiter::begin_fd_park();
    }

    // How long this lap may block. Every route that matters ends it — bytes
    // on the session fd, a token on the wake fd — so the only bound left is
    // GL-RECWAKE-1's lost-wake backstop, the same one `wait_loop` and
    // `pm_park_on_wake_fd` apply to their fd-parked laps. `fd_parked ==
    // false` means a notification landed before we armed: degrade to a
    // zero-timeout probe rather than block on it.
    let mut block_ms: i64 = if fd_parked { -1 } else { 0 };
    if fd_parked {
        let cadence = waiter::recheck_cadence_ms();
        if cadence > 0 {
            block_ms = cadence;
        }
    }
    let (data_ready, wake_ready, rc) = poll_pair(fd, events, wake_fd, block_ms);

    let mut latch_fired = false;
    if let Some(l) = l {
        if fd_parked {
            waiter::end_fd_park();
        }
        if l.maybe_sleeping.load(SeqCst) != 0 {
            l.set_maybe_sleeping(false);
        }
        // The latch is authoritative whichever way the poll ended — the
        // native backends' post-`epoll_wait` test (epoll.rs, wasm_threads.rs).
        if l.is_set() {
            reset_my_latch(handle);
            latch_fired = true;
        }
    }
    if wake_ready {
        drain_wake_fd(wake_fd);
    }
    if latch_fired {
        return false; // the caller processes interrupts, then comes back
    }
    if rc < 0 && errno() != libc::EINTR {
        return true; // N2: let the op surface the error
    }
    data_ready
}

fn reset_my_latch(handle: Option<types_storage::latch::LatchHandle>) {
    if let Some(h) = handle {
        latch::ResetLatch(h);
    }
}

/// Resolve this backend's wake route, once, at `pq_init`.
///
/// `record_wake_fd` is the connection record's `wake_fd` (0 = the host
/// offered none). A host fd is ADOPTED into this thread's waiter slot — the
/// same door the postmaster uses for its own (`waiter::adopt_wake_pipe`),
/// both ends being one ring because the bytes are wake tokens and nothing
/// else. Without one we ask the waiter for its own `pipe(2)`, which is what
/// every native backend has and what WASI p1 cannot make (ENOSYS there).
///
/// Returns `(wake_fd, adopted)`; `PGINVALID_SOCKET` selects the
/// [`INTERRUPT_POLL_MS`] fallback.
fn adopt_wake_route(record_wake_fd: i32) -> (i32, bool) {
    if record_wake_fd > 0 && waiter::adopt_wake_pipe(record_wake_fd, record_wake_fd) {
        return (record_wake_fd, true);
    }
    match waiter::ensure_wake_pipe() {
        Ok(rfd) => (rfd, false),
        Err(_) => (types_core::PGINVALID_SOCKET, false),
    }
}

/// LOG the arrangement ONCE per process: the first session backend says how
/// blocked ops wait here, every later one is silent (one line per connection
/// would be noise on a busy postmaster). The postmaster logs its own wake
/// route the same way (`ServerLoop::pm_wake_fd`).
fn log_wake_route_once(wake_fd: i32) {
    use std::sync::atomic::{AtomicBool, Ordering};
    static SAID: AtomicBool = AtomicBool::new(false);
    if SAID.swap(true, Ordering::Relaxed) {
        return;
    }
    let _ = if wake_fd >= 0 {
        ereport(LOG)
            .errmsg(format!(
                "host-pipes sessions wait on their pipe and their wake fd \
                 (this one: {wake_fd}): a SetLatch ends a blocked read at once"
            ))
            .finish(loc("pq_init"))
    } else {
        ereport(LOG)
            .errmsg(format!(
                "host-pipes sessions have no wake fd: blocked reads poll every \
                 {INTERRUPT_POLL_MS}ms, so a latch wake lands up to that late"
            ))
            .errhint("The host names one per connection in the record's wake_fd field.")
            .finish(loc("pq_init"))
    };
}

// ---------------------------------------------------------------------------
// Postmaster half: listen + accept.
// ---------------------------------------------------------------------------

fn listen_fd_from_env() -> Option<i32> {
    fd_from_env(LISTEN_FD_ENV)
}

fn fd_from_env(name: &str) -> Option<i32> {
    std::env::var(name).ok()?.trim().parse::<i32>().ok().filter(|fd| *fd >= 0)
}

/// The host's postmaster-wake fd ([`WAKE_FD_ENV`]), or None when the host
/// offers none. Read by ServerLoop's wasm arm at first wait.
pub fn wake_fd() -> Option<i32> {
    fd_from_env(WAKE_FD_ENV)
}

/// `ListenServerPort` for this transport: there is nothing to bind. The host
/// already owns the listener; we adopt the fd it named in the environment
/// and ignore hostname/port/unix_socket_dir (PostmasterMain's host-pipes arm
/// passes None/0/None and never runs the listen_addresses /
/// unix_socket_directories loops).
fn listen_server_port(
    _hostname: Option<&str>,
    _port: u16,
    _unix_socket_dir: Option<&str>,
    listen_sockets: &mut Vec<i32>,
    max_listen: usize,
) -> PgResult<()> {
    let Some(fd) = listen_fd_from_env() else {
        return ereport(ERROR)
            .errmsg(format!(
                "the host-pipes transport requires {LISTEN_FD_ENV} to name the host's listener fd"
            ))
            .errhint("The host passes the listener as an inherited file descriptor.")
            .finish(loc("listen_server_port"));
    };
    if listen_sockets.len() >= max_listen {
        return ereport(ERROR)
            .errmsg_internal("host-pipes listener would exceed MAXLISTEN")
            .finish(loc("listen_server_port"));
    }
    listen_sockets.push(fd);
    let _ = ereport(LOG)
        .errmsg(format!("listening on host-pipes listener fd {fd}"))
        .finish(loc("listen_server_port"));
    Ok(())
}

/// The accept seam's STATUS_ERROR arm, in the socket provider's shape (the
/// real diagnosis was already logged at LOG, as C does).
fn accept_failed() -> Box<types_error::PgError> {
    Box::new(types_error::PgError::new(LOG, "could not accept new connection"))
}

/// The socket provider's 100 ms back-off after a failed accept: the
/// postmaster retries immediately on read-ready, and a listener that is
/// permanently ready (EOF) would otherwise spin.
fn sleep_after_accept_failure() {
    // std::thread::sleep, matching the socket provider's AcceptConnection
    // verbatim (this is postmaster-thread back-off, not a session wait).
    std::thread::sleep(std::time::Duration::from_millis(100));
}

/// `AcceptConnection` for this transport: read exactly one 16-byte
/// connection record off the listener fd. The read is BLOCKING and loops
/// over short reads and EINTR — the postmaster only calls this once the fd
/// reported readable, and a record is written by the host as one unit, but a
/// pipe grants no atomicity guarantee to a *reader*.
fn accept_connection(server_fd: i32) -> PgResult<ClientSocket> {
    let mut rec = [0u8; CONN_RECORD_LEN];
    let mut got = 0usize;
    while got < CONN_RECORD_LEN {
        // SAFETY: rec[got..] is valid writable memory of the passed length.
        let n = unsafe {
            libc::read(
                server_fd,
                rec.as_mut_ptr().add(got).cast(),
                CONN_RECORD_LEN - got,
            )
        };
        if n < 0 {
            let e = errno();
            if e == libc::EINTR {
                continue;
            }
            let _ = ereport(LOG)
                .with_saved_errno(e)
                .errmsg("could not read host-pipes connection record: %m")
                .finish(loc("accept_connection"));
            return Err(accept_failed());
        }
        if n == 0 {
            // EOF ON THE LISTENER IS THIS TRANSPORT'S STOP SIGNAL.
            //
            // The host owns the listener; the only way it ever reaches EOF is
            // that the host closed its write end, and the only thing that can
            // mean is "no further connection will be announced". On a socket
            // that state is unreachable (a listen socket has no writer), so C
            // has no arm for it; here it is the exact information a SIGINT
            // carries, and it arrives on a channel every host has — including
            // one with no signals at all (wasm: main_entry's `pqsignal` is a
            // documented no-op there, so a process signal can reach nothing).
            //
            // So: raise the SAME pending flags the SIGINT handler raises and
            // set the postmaster latch, through the seam that IS that handler
            // (postmaster_seams::signal_postmaster_fast_shutdown). The state
            // machine then runs the ordinary PM_STOP_BACKENDS ->
            // PM_WAIT_BACKENDS -> shutdown checkpoint -> exit(0) ceremony; no
            // second shutdown path exists and none is invented here. NOT
            // cfg'd to wasm on purpose: the native driver drives this exact
            // arm, which is what proves the wasm lane's shutdown is the real
            // one.
            //
            // Anti-spin: an EOF listener stays readable forever, so returning
            // Err on a still-accepting loop would burn a core. The shutdown
            // request IS the fix — process_pm_shutdown_request reaches
            // PM_STOP_BACKENDS, which calls ConfigurePostmasterWaitSet(false)
            // and deregisters the listener (ServerLoop also stops probing and
            // accepting the moment a shutdown is pending). Only when no
            // postmaster installed the seam is there nothing to stop the
            // spin, and then we fall back to the socket provider's back-off.
            let _ = ereport(LOG)
                .errmsg("host-pipes listener closed: fast shutdown requested")
                .finish(loc("accept_connection"));
            if postmaster_seams::signal_postmaster_fast_shutdown::is_installed() {
                postmaster_seams::signal_postmaster_fast_shutdown::call();
            } else {
                sleep_after_accept_failure();
            }
            return Err(accept_failed());
        }
        got += n as usize;
    }

    let magic = u32::from_le_bytes([rec[0], rec[1], rec[2], rec[3]]);
    let in_fd = i32::from_le_bytes([rec[4], rec[5], rec[6], rec[7]]);
    let out_fd = i32::from_le_bytes([rec[8], rec[9], rec[10], rec[11]]);
    // OPTIONAL, and 0 means "none" (fd 0 is stdin): a host that predates the
    // field announces connections unchanged and its backends take the
    // INTERRUPT_POLL_MS fallback. A NEGATIVE value is a desynchronised
    // stream, which the check below is terminal about.
    let wake_fd = i32::from_le_bytes([rec[12], rec[13], rec[14], rec[15]]);
    if magic != CONN_RECORD_MAGIC || in_fd < 0 || out_fd < 0 || wake_fd < 0 {
        // A desynchronised listener stream is unrecoverable (we cannot know
        // where the next record starts), so this is loud and terminal for
        // the connection, not a skipped byte.
        let _ = ereport(LOG)
            .errmsg_internal(format!(
                "invalid host-pipes connection record (magic {magic:#010x}, in_fd {in_fd}, \
                 out_fd {out_fd}, wake_fd {wake_fd})"
            ))
            .finish(loc("accept_connection"));
        sleep_after_accept_failure();
        return Err(accept_failed());
    }

    OUT_FDS.lock().unwrap_or_else(|e| e.into_inner()).insert(in_fd, (out_fd, wake_fd));

    Ok(ClientSocket {
        sock: in_fd,
        raddr: local_raddr(),
    })
}

/// The synthetic peer address every host-pipes connection carries: an
/// unnamed AF_UNIX address, which is what the peer factually is — a local
/// process the host handed us a pipe pair to. That choice is load-bearing
/// downstream, not cosmetic: `pg_getnameinfo_all` names it `[local]` (the
/// stdio provider hard-codes the same string), and `check_hba` matches it
/// against `local` pg_hba.conf lines instead of the `host` lines, which are
/// about IP peers this transport has none of.
fn local_raddr() -> ip::SockAddr {
    let mut sa = ip::SockAddr::zeroed();
    let fam = (ip::sys::AF_UNIX as u16).to_ne_bytes();
    sa.addr[0] = fam[0];
    sa.addr[1] = fam[1];
    // sizeof(sa_family_t): an unnamed AF_UNIX peer, exactly what accept(2)
    // reports for a socketpair end (empty sun_path -> remote_port "").
    sa.salen = 2;
    sa
}

// ---------------------------------------------------------------------------
// Per-connection half: init, bytes, close.
// ---------------------------------------------------------------------------

/// pq_init, host-pipes shape: the stdio provider's shape (no getsockname, no
/// keepalives, no fcntl, no FeBeWaitSet — none of them exist on a pipe pair)
/// on this connection's `in_fd`/`out_fd` instead of 0/1. This runs on the
/// BACKEND thread, and is where the accept-time `in_fd -> out_fd` handoff is
/// consumed into thread-local state.
fn pq_init(client_sock: &ClientSocket) -> PgResult<Port> {
    let in_fd = client_sock.sock;
    let pair = OUT_FDS.lock().unwrap_or_else(|e| e.into_inner()).get(&in_fd).copied();
    let Some((out_fd, record_wake_fd)) = pair else {
        return ereport(ERROR)
            .errmsg_internal(format!("no host-pipes output fd registered for input fd {in_fd}"))
            .finish(loc("pq_init"))
            .map(|()| unreachable!());
    };

    let port = Port::new(client_sock);
    pqcomm::pq_init_buffers()?;
    // The wake route is per THREAD (a waiter slot is), so it is claimed here,
    // on the backend thread that will do the blocking, and never at accept.
    let (wake_fd, wake_adopted) = adopt_wake_route(record_wake_fd);
    log_wake_route_once(wake_fd);
    STATE.set(Some(Conn { in_fd, out_fd, wake_fd, wake_adopted, noblock: false }));
    // The socket provider registers socket_close here for the same reason:
    // a session that ends any way at all (X, FATAL, SIGTERM) must release
    // its client I/O. Ours actually closes the fds — nothing else will, and
    // an unclosed write end is a client that waits forever.
    ipc_seams::on_proc_exit::call(hostpipes_close, 0);
    Ok(port)
}

/// secure_read over this connection's `in_fd`. Interrupt-processing shape is
/// the stdio provider's, itself be_secure::secure_read's minus the latch arm
/// (a pipe read has no wait set to fall back to).
pub fn secure_read(buf: &mut [u8]) -> PgResult<Result<usize, i32>> {
    postgres_seams::process_client_read_interrupt::call(false)?;

    let Some(conn) = state() else {
        return Ok(Err(libc::EBADF));
    };
    let (in_fd, noblock) = (conn.in_fd, conn.noblock);

    let (n, e) = loop {
        if !noblock && !wait_client_io(in_fd, libc::POLLIN, conn.wake_fd) {
            // Not readable yet (the latch fired, or a lost-wake recheck lap
            // expired): this is the BLOCKED state, so process interrupts
            // exactly as the socket provider does on every FeBeWaitSet wake,
            // then wait again.
            postgres_seams::process_client_read_interrupt::call(true)?;
            continue;
        }
        let (n, e) = if noblock {
            fdnb::read_noblock(in_fd, buf)
        } else {
            // Readable: this read cannot block (POLLIN means >= 1 byte or EOF).
            // SAFETY: buf is valid writable memory of buf.len() bytes.
            let n = unsafe { libc::read(in_fd, buf.as_mut_ptr().cast(), buf.len()) };
            (n as isize, errno())
        };
        if n < 0 && e == libc::EINTR {
            postgres_seams::process_client_read_interrupt::call(true)?;
            continue;
        }
        break (n, e);
    };

    postgres_seams::process_client_read_interrupt::call(false)?;

    Ok(ssize_result(n, e))
}

/// secure_write over this connection's `out_fd`. Partial writes are the
/// caller's loop (pqcomm::internal_flush_buffer), as with every provider.
pub fn secure_write(buf: &[u8]) -> PgResult<Result<usize, i32>> {
    postgres_seams::process_client_write_interrupt::call(false)?;

    let Some(conn) = state() else {
        return Ok(Err(libc::EBADF));
    };
    let (out_fd, noblock) = (conn.out_fd, conn.noblock);

    let (n, e) = loop {
        if !noblock && !wait_client_io(out_fd, libc::POLLOUT, conn.wake_fd) {
            postgres_seams::process_client_write_interrupt::call(true)?;
            continue;
        }
        let (n, e) = if noblock {
            fdnb::write_noblock(out_fd, buf)
        } else {
            // Capped at fdnb::NB_WRITE_CAP for the same reason the noblock
            // arm is (fdnb's N1): POLLOUT licenses only a BOUNDED write, and
            // a blocking write of more transfers what fits and then parks
            // uninterruptibly — the wedge this loop exists to avoid. Short
            // writes are the seam's normal vocabulary (the caller loops).
            let cap = buf.len().min(fdnb::NB_WRITE_CAP);
            // SAFETY: buf[..cap] is valid readable memory.
            let n = unsafe { libc::write(out_fd, buf.as_ptr().cast(), cap) };
            (n as isize, errno())
        };
        if n < 0 && e == libc::EINTR {
            postgres_seams::process_client_write_interrupt::call(true)?;
            continue;
        }
        break (n, e);
    };

    postgres_seams::process_client_write_interrupt::call(false)?;

    Ok(ssize_result(n, e))
}

fn set_port_noblock(noblock: bool) -> bool {
    // Mode is emulated (fdnb's zero-timeout poll), never an fcntl: the fds
    // stay blocking so a plain read IS the wait. Same as the stdio provider.
    let Some(conn) = state() else {
        // The socket provider's "no client connection" answer before pq_init.
        return false;
    };
    STATE.set(Some(Conn { noblock, ..conn }));
    true
}

/// TLS never runs on this transport, so close is not a TLS shutdown (as it
/// is for the socket provider) but the actual release of the connection's
/// two fds plus its map entry. Idempotent: the on_proc_exit callback and an
/// explicit call cannot double-close.
fn secure_close() {
    let Some(conn) = STATE.replace(None) else {
        return;
    };
    OUT_FDS.lock().unwrap_or_else(|e| e.into_inner()).remove(&conn.in_fd);
    if conn.wake_adopted {
        // Hand the HOST's wake ring back BEFORE the fds go: the waiter slot
        // closes whatever fds it holds when this thread exits, which is
        // right for a `pipe(2)` it made and wrong for a borrowed one. Not
        // closed here either — the ring belongs to the host, and a closed
        // one would report readable (EOF) forever to whoever it hands it to
        // next.
        waiter::release_adopted_wake_pipe();
    }
    // SAFETY: both fds belong to this session and are closed exactly once
    // (STATE was taken above, so a second call returns early).
    unsafe {
        libc::close(conn.in_fd);
        libc::close(conn.out_fd);
    }
}

fn hostpipes_close(_code: i32, _arg: usize) {
    secure_close();
}

// SwitchToSharedLatch/SwitchBackToLocalLatch repoint: no FeBeWaitSet on this
// transport (nothing parks on readiness), so there is nothing to repoint —
// the socket provider's impl likewise no-ops when the set is absent.
fn modify_fe_be_wait_set_latch(_latch: types_storage::latch::LatchHandle) -> PgResult<()> {
    Ok(())
}

/// True: this transport's listener is a HOST-OWNED fd, not something the
/// postmaster binds. PostmasterMain asks before running its
/// listen_addresses / unix_socket_directories loops — with host-pipes both
/// GUCs are empty by construction and the loops must not run (they would
/// create real sockets, which is the whole thing this transport exists to
/// avoid).
fn transport_owns_listener() -> bool {
    true
}

/// `transport_wake_fd` for this transport: the host's postmaster-wake fd, or
/// `PGINVALID_SOCKET` when the host named none (the postmaster then keeps
/// whatever wait its target already had). See [`WAKE_FD_ENV`].
fn transport_wake_fd() -> i32 {
    wake_fd().unwrap_or(types_core::PGINVALID_SOCKET)
}

/// Install the host-pipes provider into the transport seam slots — both
/// halves: the per-connection half (`pq_init`, `secure_*`) like the stdio
/// provider, AND the postmaster half (`listen_server_port`,
/// `accept_connection`) like the socket provider. Exactly one provider
/// installs per process (seam_core's install-twice panic enforces it).
pub fn init_transport_seams() {
    be_secure_seams::secure_read::set(secure_read);
    be_secure_seams::secure_write::set(secure_write);
    be_secure_seams::secure_close::set(secure_close);
    be_secure_seams::set_port_noblock::set(set_port_noblock);
    be_secure_seams::be_tls_get_certificate_hash::set(|| {
        // Reachable only from SCRAM channel binding with ssl_in_use, which
        // never becomes true on this transport.
        ereport(ERROR)
            .errmsg_internal("channel binding is not supported on the host-pipes transport")
            .finish(loc("be_tls_get_certificate_hash"))
            .map(|()| Vec::new())
    });
    pqcomm_seams::pq_init::set(pq_init);
    pqcomm_seams::modify_fe_be_wait_set_latch::set(modify_fe_be_wait_set_latch);
    pqcomm_seams::listen_server_port::set(listen_server_port);
    pqcomm_seams::accept_connection::set(accept_connection);
    pqcomm_seams::transport_owns_listener::set(transport_owns_listener);
    pqcomm_seams::transport_wake_fd::set(transport_wake_fd);
    // pq_check_connection is left VACANT, as the stdio provider leaves it:
    // it is the socket transport's WL_SOCKET_CLOSED poll, and the caller
    // (ProcessInterrupts' CLIENT_CONNECTION_CHECK arm) treats a vacant seam
    // as "connection alive". A dead pipe surfaces as EOF at the next read.
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The record magic is the ASCII tag "HPGP" in stream order — the
    /// property a host writing bytes and a host writing a LE u32 share.
    #[test]
    fn magic_is_hpgp_in_stream_order() {
        assert_eq!(&CONN_RECORD_MAGIC.to_le_bytes(), b"HPGP");
    }

    /// The synthetic peer address is an unnamed AF_UNIX address: what makes
    /// `pg_getnameinfo_all` say "[local]" and `check_hba` consult the
    /// `local` lines.
    #[test]
    fn synthetic_raddr_is_unnamed_af_unix() {
        let sa = local_raddr();
        assert_eq!(ip::sockaddr_family(&sa), ip::sys::AF_UNIX);
        assert_eq!(sa.salen, 2);
        assert!(sa.addr[2..].iter().all(|&b| b == 0));
        assert!(!ip::sockaddr_is_all_zeros(&sa));
    }

    /// The env var is the only channel that can name a host-owned fd, and a
    /// missing/garbage value must not silently degrade to fd 0 (stdin).
    #[test]
    fn listen_fd_env_parse() {
        // Not set / unparsable / negative all decline.
        assert_eq!(std::env::var(LISTEN_FD_ENV).ok().and_then(|v| v.parse::<i32>().ok()), None);
        assert_eq!("  3 ".trim().parse::<i32>().ok(), Some(3));
        assert_eq!("-1".parse::<i32>().ok().filter(|fd| *fd >= 0), None);
        assert_eq!("nope".parse::<i32>().ok(), None);
    }

    /// The wake fd is OPTIONAL and shares the listener fd's parse rules: a
    /// missing or garbage value declines (and the postmaster keeps whatever
    /// wait its target already had), never degrades to fd 0.
    #[test]
    fn wake_fd_env_is_optional_and_parsed_like_the_listener() {
        assert_eq!(std::env::var(WAKE_FD_ENV).ok(), None);
        assert_eq!(wake_fd(), None);
        assert_eq!(transport_wake_fd(), types_core::PGINVALID_SOCKET);
        assert_ne!(WAKE_FD_ENV, LISTEN_FD_ENV);
    }

    /// EOF on the listener is a SHUTDOWN, not a retry: the arm calls the
    /// fast-shutdown seam when one is installed. Pinned here as the seam
    /// contract (the flags it raises are pinned in postmaster's tests, and
    /// the whole path end-to-end by wasm/run-native-hostpipes.mjs).
    #[test]
    fn listener_eof_asks_for_a_fast_shutdown_through_the_seam() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static CALLS: AtomicUsize = AtomicUsize::new(0);
        // seam_core installs are set-once per process; this test binary has
        // no postmaster, so the slot is free.
        postmaster_seams::signal_postmaster_fast_shutdown::set(|| {
            CALLS.fetch_add(1, Ordering::Release);
        });
        assert!(postmaster_seams::signal_postmaster_fast_shutdown::is_installed());
        postmaster_seams::signal_postmaster_fast_shutdown::call();
        assert_eq!(CALLS.load(Ordering::Acquire), 1);
    }

    /// The 16-byte record decodes little-endian at the documented offsets,
    /// wake_fd included — the field the reserved slot became.
    #[test]
    fn record_layout_is_le_at_documented_offsets() {
        let mut rec = [0u8; CONN_RECORD_LEN];
        rec[0..4].copy_from_slice(&CONN_RECORD_MAGIC.to_le_bytes());
        rec[4..8].copy_from_slice(&7i32.to_le_bytes());
        rec[8..12].copy_from_slice(&9i32.to_le_bytes());
        rec[12..16].copy_from_slice(&11i32.to_le_bytes());
        assert_eq!(u32::from_le_bytes([rec[0], rec[1], rec[2], rec[3]]), CONN_RECORD_MAGIC);
        assert_eq!(i32::from_le_bytes([rec[4], rec[5], rec[6], rec[7]]), 7);
        assert_eq!(i32::from_le_bytes([rec[8], rec[9], rec[10], rec[11]]), 9);
        assert_eq!(i32::from_le_bytes([rec[12], rec[13], rec[14], rec[15]]), 11);
        assert_eq!(rec.len(), 16);
    }

    /// A host that predates the wake_fd field writes zero there, and zero
    /// means "no wake route" — never fd 0 (stdin), which is a real fd on
    /// every host this transport runs on.
    #[test]
    fn wake_fd_zero_means_no_wake_route() {
        let rec = [0u8; CONN_RECORD_LEN];
        let wake_fd = i32::from_le_bytes([rec[12], rec[13], rec[14], rec[15]]);
        assert_eq!(wake_fd, 0);
        // adopt_wake_route's guard is `> 0`, so 0 falls through to the
        // waiter's own pipe(2) (native) or to no route at all (wasm).
        assert!(!(wake_fd > 0));
    }

    /// The blocked-lap bound: with a wake fd the lap is the waiter's recheck
    /// cadence (a lost-wake backstop), and without one it is the poll bound
    /// — which is then the whole interrupt latency.
    #[test]
    fn interrupt_poll_is_the_wake_less_bound() {
        assert_eq!(INTERRUPT_POLL_MS, 100);
        assert!(waiter::recheck_cadence_ms() > INTERRUPT_POLL_MS as i64);
    }
}
