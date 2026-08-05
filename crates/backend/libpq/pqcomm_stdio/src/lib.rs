//! pqcomm_stdio: the stdio transport provider — pgwire over the process's
//! stdin(0)/stdout(1) byte duplex.
//!
//! This is the SECOND consumer of the §2.4 transport-provider seam
//! (docs/design/dst-and-wasm.md). The seam itself is the existing set-once
//! slot pair the whole protocol path already flows through:
//!
//!   bytes in   pqcomm::pq_recvbuf / pq_getbyte_if_available
//!                -> be_secure_seams::secure_read
//!   bytes out  pqcomm::internal_flush_buffer /
//!              backend_startup::write_negotiation_byte
//!                -> be_secure_seams::secure_write
//!   session    backend bring-up -> pqcomm_seams::pq_init
//!   mode       pqcomm::socket_set_nonblocking
//!                -> be_secure_seams::set_port_noblock
//!
//! Providers (one indirection, N consumers — resolved ONCE at boot by
//! seams_init::init_all_with_transport, never per byte):
//!   1. socket (native default): be_secure::init_seams +
//!      pqcomm::init_socket_seams — libc::recv/send on the accepted fd,
//!      byte-identical to before this crate existed;
//!   2. stdio (this crate): --stdio-wire on any target; on wasm32-wasip1 the
//!      wasmtime host pumps protocol bytes through the preopened stdio pipes
//!      (WASI p1 has no socket(), so this IS the wasm client-server story);
//!      native --stdio-wire is the differential-comparison arm;
//!   3. sim-net (P4, future): an in-memory duplex pair under the sim
//!      scheduler installs into these SAME slots — deterministic delivery,
//!      no raw I/O — giving multi-connection scenarios replay identity.
//!
//! Determinism ledger: the libc::read/write sites below are the raw-IO
//! surface OF the transport provider, exactly like be_secure's libc::recv/
//! send rows — they live BEHIND the seam, so the sim consumer never executes
//! them. The noblock EMULATION (poll probe + capped write) lives in the
//! shared `fdnb` crate — the wasm-net-seam ledger's N1/N2 MUST-FIX items
//! are fixed THERE, once, with red-scenario tests (P4 sim-net increment).
//!
//! Single-threaded by construction: one process, one session (the wasm
//! lifecycle — signals/threads do not exist on WASI p1). Blocking semantics
//! come from the host pipe itself; the noblock mode is emulated with a
//! zero-timeout poll, so the be_secure wait-set machinery (FeBeWaitSet) is
//! never engaged and is deliberately not created.

use core::cell::Cell;

use elog::ereport;
use types_error::{ErrorLocation, PgResult, ERROR};
use types_startup::{ClientSocket, Port};

const STDIN_FD: i32 = 0;
const STDOUT_FD: i32 = 1;

thread_local! {
    // Some(noblock) once pq_init ran — the stdio twin of pqcomm::socket's
    // CLIENT_STATE (sock, noblock, ssl_in_use): there is no fd to remember
    // and TLS never runs on this transport, leaving only the noblock bit.
    static STATE: Cell<Option<bool>> = const { Cell::new(None) };
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

fn noblock() -> bool {
    STATE.get().unwrap_or(false)
}

/// secure_read over fd 0. Same interrupt-processing shape as the socket
/// provider's be_secure::secure_read: process-before, process-after, and a
/// blocked-flavored process on EINTR (the stdio stand-in for the latch arm —
/// a pipe read has no wait set to fall back to). The noblock arm delegates
/// to the shared fdnb emulation (N2-fixed readiness: a dead fd surfaces
/// EBADF/EOF instead of eternal EWOULDBLOCK).
pub fn secure_read(buf: &mut [u8]) -> PgResult<Result<usize, i32>> {
    postgres_seams::process_client_read_interrupt::call(false)?;

    let (n, e) = loop {
        let (n, e) = if noblock() {
            fdnb::read_noblock(STDIN_FD, buf)
        } else {
            // SAFETY: buf is valid writable memory of buf.len() bytes.
            let n = unsafe { libc::read(STDIN_FD, buf.as_mut_ptr().cast(), buf.len()) };
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

/// secure_write over fd 1. Partial writes are the caller's loop
/// (pqcomm::internal_flush_buffer), as with the socket provider. The
/// noblock arm delegates to the shared fdnb emulation — N1-fixed: the write
/// is capped at fdnb::NB_WRITE_CAP so POLLOUT actually licenses it (the old
/// poll-then-BLOCKING-write of the full unsent buffer could park forever,
/// making pq_flush_if_writable/pq_putmessage_noblock not non-blocking).
pub fn secure_write(buf: &[u8]) -> PgResult<Result<usize, i32>> {
    postgres_seams::process_client_write_interrupt::call(false)?;

    let (n, e) = loop {
        let (n, e) = if noblock() {
            fdnb::write_noblock(STDOUT_FD, buf)
        } else {
            // SAFETY: buf is valid readable memory of buf.len() bytes.
            let n = unsafe { libc::write(STDOUT_FD, buf.as_ptr().cast(), buf.len()) };
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
    if STATE.get().is_none() {
        // Mirrors the socket provider's "no client connection" answer before
        // pq_init.
        return false;
    }
    STATE.set(Some(noblock));
    true
}

// TLS never runs on the stdio transport; the stream stays plaintext for the
// whole session, so close is state-free (the host owns the pipes).
fn secure_close() {}

/// pq_init, stdio shape: no getsockname/keepalive/fcntl (there is no socket)
/// and no FeBeWaitSet (the byte arms above never park on readiness — blocking
/// comes from the host pipe, noblock from the zero-timeout poll). laddr and
/// raddr stay all-zeros = "client address unknown" (ip::sockaddr_is_all_zeros
/// vocabulary); the wire bring-up names the peer "[local]" itself.
fn pq_init(client_sock: &ClientSocket) -> PgResult<Port> {
    let port = Port::new(client_sock);
    pqcomm::pq_init_buffers()?;
    STATE.set(Some(false));
    Ok(port)
}

// SwitchToSharedLatch/SwitchBackToLocalLatch repoint: no FeBeWaitSet on this
// transport, so there is nothing to repoint (the socket provider's impl
// likewise no-ops when the set is absent).
fn modify_fe_be_wait_set_latch(_latch: types_storage::latch::LatchHandle) -> PgResult<()> {
    Ok(())
}

/// Install the stdio provider into the transport seam slots. Boot-time
/// counterpart of `be_secure::init_seams` + the transport half of
/// `pqcomm::init_socket_seams`; exactly one provider installs per process
/// (seam_core's install-twice panic enforces it).
pub fn init_transport_seams() {
    be_secure_seams::secure_read::set(secure_read);
    be_secure_seams::secure_write::set(secure_write);
    be_secure_seams::secure_close::set(secure_close);
    be_secure_seams::set_port_noblock::set(set_port_noblock);
    be_secure_seams::be_tls_get_certificate_hash::set(|| {
        // Reachable only from SCRAM channel binding with ssl_in_use, which
        // never becomes true on this transport.
        ereport(ERROR)
            .errmsg_internal("channel binding is not supported on the stdio transport")
            .finish(loc("be_tls_get_certificate_hash"))
            .map(|()| Vec::new())
    });
    pqcomm_seams::pq_init::set(pq_init);
    pqcomm_seams::modify_fe_be_wait_set_latch::set(modify_fe_be_wait_set_latch);
}
