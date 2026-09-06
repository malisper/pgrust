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
//!   12      u32   reserved   must be 0 today; ignored (forward slot)
//! ```
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
//! as the stdio provider leaves fds 0/1: a blocking `read` IS the wait, and
//! the emulated-noblock arms delegate to the shared `fdnb` crate (a
//! zero-timeout `poll(2)`, which works on pipes natively and reaches the
//! wasm host's `poll_oneoff`). No `FeBeWaitSet` is created, so no socket
//! event is ever registered — the wasm wait-event backend rejects those by
//! construction.
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

/// Connection-record magic: little-endian `0x50475048` = the bytes
/// `H P G P` in stream order (a host writing the ASCII tag byte-by-byte and
/// a host writing a LE u32 agree).
pub const CONN_RECORD_MAGIC: u32 = 0x5047_5048;

/// Fixed connection-record size, in bytes (magic, in_fd, out_fd, reserved).
pub const CONN_RECORD_LEN: usize = 16;

/// `in_fd -> out_fd` for every live host-pipes connection. Written by the
/// postmaster thread at accept, read by the backend thread at `pq_init`,
/// erased at close. See the crate docs ("The fd pair map"): this exists
/// because `ClientSocket` carries a single fd and a pipe pair is two.
static OUT_FDS: LazyLock<Mutex<HashMap<i32, i32>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

thread_local! {
    // Some((in_fd, out_fd, noblock)) once this backend's pq_init ran — the
    // host-pipes twin of pqcomm::socket's CLIENT_STATE (which is
    // (sock, noblock, ssl_in_use); TLS never runs on this transport, and
    // the fd is a pair).
    static STATE: Cell<Option<(i32, i32, bool)>> = const { Cell::new(None) };
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

fn state() -> Option<(i32, i32, bool)> {
    STATE.get()
}

/// How long a BLOCKED byte op parks before checking interrupts again, in ms.
///
/// This bound is the whole reason the blocking arms below poll instead of
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
/// recovery by one idle client. Polling with a bound restores C's property
/// at the cost of one wake per 100 ms per BLOCKED session (an idle backend
/// only; nothing on the hot path polls at all).
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

// ---------------------------------------------------------------------------
// Postmaster half: listen + accept.
// ---------------------------------------------------------------------------

fn listen_fd_from_env() -> Option<i32> {
    std::env::var(LISTEN_FD_ENV).ok()?.trim().parse::<i32>().ok().filter(|fd| *fd >= 0)
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
            // The host closed the listener: C's accept() on a dead listen
            // socket is an error the postmaster logs and retries; there is
            // no retry that helps here, but the shape is the same (Err is
            // the caller's STATUS_ERROR arm). Sleep as the socket provider
            // does so a permanently-EOF listener cannot spin the postmaster
            // at 100% CPU (it stays readable forever).
            let _ = ereport(LOG)
                .errmsg("host-pipes listener reached end of file")
                .finish(loc("accept_connection"));
            sleep_after_accept_failure();
            return Err(accept_failed());
        }
        got += n as usize;
    }

    let magic = u32::from_le_bytes([rec[0], rec[1], rec[2], rec[3]]);
    let in_fd = i32::from_le_bytes([rec[4], rec[5], rec[6], rec[7]]);
    let out_fd = i32::from_le_bytes([rec[8], rec[9], rec[10], rec[11]]);
    if magic != CONN_RECORD_MAGIC || in_fd < 0 || out_fd < 0 {
        // A desynchronised listener stream is unrecoverable (we cannot know
        // where the next record starts), so this is loud and terminal for
        // the connection, not a skipped byte.
        let _ = ereport(LOG)
            .errmsg_internal(format!(
                "invalid host-pipes connection record (magic {magic:#010x}, in_fd {in_fd}, out_fd {out_fd})"
            ))
            .finish(loc("accept_connection"));
        sleep_after_accept_failure();
        return Err(accept_failed());
    }

    OUT_FDS.lock().unwrap_or_else(|e| e.into_inner()).insert(in_fd, out_fd);

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
    let out_fd = OUT_FDS.lock().unwrap_or_else(|e| e.into_inner()).get(&in_fd).copied();
    let Some(out_fd) = out_fd else {
        return ereport(ERROR)
            .errmsg_internal(format!("no host-pipes output fd registered for input fd {in_fd}"))
            .finish(loc("pq_init"))
            .map(|()| unreachable!());
    };

    let port = Port::new(client_sock);
    pqcomm::pq_init_buffers()?;
    STATE.set(Some((in_fd, out_fd, false)));
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

    let Some((in_fd, _, noblock)) = state() else {
        return Ok(Err(libc::EBADF));
    };

    let (n, e) = loop {
        if !noblock && poll_bounded(in_fd, libc::POLLIN) <= 0 {
            // Not readable yet (or a signal cut the poll short): this is the
            // BLOCKED state, so process interrupts exactly as the socket
            // provider does on every FeBeWaitSet wake, then wait again.
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

    let Some((_, out_fd, noblock)) = state() else {
        return Ok(Err(libc::EBADF));
    };

    let (n, e) = loop {
        if !noblock && poll_bounded(out_fd, libc::POLLOUT) <= 0 {
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
    let Some((in_fd, out_fd, _)) = state() else {
        // The socket provider's "no client connection" answer before pq_init.
        return false;
    };
    STATE.set(Some((in_fd, out_fd, noblock)));
    true
}

/// TLS never runs on this transport, so close is not a TLS shutdown (as it
/// is for the socket provider) but the actual release of the connection's
/// two fds plus its map entry. Idempotent: the on_proc_exit callback and an
/// explicit call cannot double-close.
fn secure_close() {
    let Some((in_fd, out_fd, _)) = STATE.replace(None) else {
        return;
    };
    OUT_FDS.lock().unwrap_or_else(|e| e.into_inner()).remove(&in_fd);
    // SAFETY: both fds belong to this session and are closed exactly once
    // (STATE was taken above, so a second call returns early).
    unsafe {
        libc::close(in_fd);
        libc::close(out_fd);
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

    /// The 16-byte record decodes little-endian at the documented offsets.
    #[test]
    fn record_layout_is_le_at_documented_offsets() {
        let mut rec = [0u8; CONN_RECORD_LEN];
        rec[0..4].copy_from_slice(&CONN_RECORD_MAGIC.to_le_bytes());
        rec[4..8].copy_from_slice(&7i32.to_le_bytes());
        rec[8..12].copy_from_slice(&9i32.to_le_bytes());
        assert_eq!(u32::from_le_bytes([rec[0], rec[1], rec[2], rec[3]]), CONN_RECORD_MAGIC);
        assert_eq!(i32::from_le_bytes([rec[4], rec[5], rec[6], rec[7]]), 7);
        assert_eq!(i32::from_le_bytes([rec[8], rec[9], rec[10], rec[11]]), 9);
        assert_eq!(rec.len(), 16);
    }
}
