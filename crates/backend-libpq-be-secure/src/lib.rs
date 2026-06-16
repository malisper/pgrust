//! Port of `src/backend/libpq/be-secure.c`.
//!
//! > functions related to setting up a secure connection to the frontend.
//! > Secure connections are expected to provide confidentiality, message
//! > integrity and endpoint authentication.
//!
//! This is the transport-neutral secure-session dispatcher. It owns the secure
//! read/write control flow (the blocking-retry loop, the `raw_buf` unread-buffer
//! consumption, the raw `recv`/`send`) and routes the encrypted arms to the
//! backends: the `#ifdef USE_SSL` arm into [`backend_libpq_be_secure_openssl`]
//! (`be-secure-openssl.c`, a direct dependency), and the `#ifdef ENABLE_GSS`
//! arm into `be-secure-gssapi.c` (unported — a loud panic, the dead arm in this
//! build). The cross-subsystem call-outs (tcop interrupt processing, the FeBe
//! wait-event machinery, the process latch reset) cross seams.
//!
//! ## Conditional compilation
//!
//! The C file is `#ifdef USE_SSL` / `#ifdef ENABLE_GSS` gated. This build is
//! non-`USE_SSL` and non-`ENABLE_GSS`, so [`USE_SSL`] and [`ENABLE_GSS`] are
//! `false` and the `#else` arms run. Both arms of every `#ifdef` are
//! transcribed as ordinary `if` branches on those `const bool`s so the full
//! logic is present and faithful; the compiler folds the dead `const`-false
//! arms exactly as `cpp` folds dead `#ifdef` arms.

#![allow(non_upper_case_globals)]
// The retry loops set `waitfor = 0` at the C-faithful init points; under the
// non-USE_SSL build those are immediately reassigned, which rustc would flag.
#![allow(unused_assignments)]

use std::sync::atomic::{AtomicBool, Ordering};

use backend_utils_error::ereport;
use types_error::{ErrorLocation, PgResult, DEBUG2, ERRCODE_ADMIN_SHUTDOWN, FATAL, LOG};
use types_net::{Port, SockError, SockResult};
use types_storage::waiteventset::{
    WL_LATCH_SET, WL_POSTMASTER_DEATH, WL_SOCKET_READABLE, WL_SOCKET_WRITEABLE,
};

use backend_libpq_be_secure_openssl as tls;

const SRCFILE: &str = "be-secure.c";
fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new(SRCFILE, 0, funcname)
}

/// `#ifdef USE_SSL` — this build does not link OpenSSL.
pub const USE_SSL: bool = false;
/// `#ifdef ENABLE_GSS` — this build does not link GSSAPI.
pub const ENABLE_GSS: bool = false;

/// `STATUS_ERROR` (c.h `#define STATUS_ERROR (-1)`).
pub const STATUS_ERROR: i32 = -1;
/// `STATUS_OK` (c.h `#define STATUS_OK (0)`).
pub const STATUS_OK: i32 = 0;

/// `WAIT_EVENT_CLIENT_READ` (utils/wait_event.h) — `PG_WAIT_CLIENT | 0`.
const WAIT_EVENT_CLIENT_READ: u32 = 0x06000000;
/// `WAIT_EVENT_CLIENT_WRITE` — `PG_WAIT_CLIENT | 1`.
const WAIT_EVENT_CLIENT_WRITE: u32 = 0x06000000 | 1;

/// `EWOULDBLOCK` / `EAGAIN` (POSIX defines them equal on the supported builds).
#[cfg(target_os = "macos")]
const EAGAIN: i32 = 35;
#[cfg(not(target_os = "macos"))]
const EAGAIN: i32 = 11;
const EWOULDBLOCK: i32 = EAGAIN;

/* ============================================================== *
 *  errno access for the direct recv/send calls.
 * ============================================================== */

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn errno_location() -> *mut i32 {
    unsafe { libc::__error() }
}
#[cfg(not(any(target_os = "macos", target_os = "ios")))]
fn errno_location() -> *mut i32 {
    unsafe { libc::__errno_location() }
}
fn errno() -> i32 {
    unsafe { *errno_location() }
}

/* ============================================================== *
 *  LoadedSSL — the postmaster's "SSL initialized" flag (be-secure
 *  owns the negotiation-disabled predicate that reads it).
 * ============================================================== */

static LOADED_SSL: AtomicBool = AtomicBool::new(false);

/// `LoadedSSL` — whether `secure_initialize` succeeded. In the C postmaster
/// this is a postmaster global set after `secure_initialize(true)`; the
/// `ssl_negotiation_disabled` predicate and the `ssl_supported` guard read it.
pub fn loaded_ssl() -> bool {
    LOADED_SSL.load(Ordering::Relaxed)
}

/* ------------------------------------------------------------ */
/*			 Procedures common to all secure sessions			*/
/* ------------------------------------------------------------ */

/// `int secure_initialize(bool isServerStart)` — initialize global context.
///
/// > If isServerStart is true, report any errors as FATAL (so we don't return).
/// > Otherwise, log errors at LOG level and return -1 to indicate trouble,
/// > preserving the old SSL state if any.  Returns 0 if OK.
pub fn secure_initialize(is_server_start: bool) -> PgResult<i32> {
    if USE_SSL {
        // `return be_tls_init(isServerStart);`
        let (min_v, max_v) = ssl_protocol_versions();
        let rc = match tls::be_tls_init(min_v, max_v, is_server_start)? {
            Ok(_loaded_ca) => 0,
            Err(()) => STATUS_ERROR,
        };
        // The postmaster sets `LoadedSSL = (secure_initialize(true) == 0)`.
        if rc == 0 {
            LOADED_SSL.store(true, Ordering::Relaxed);
        }
        Ok(rc)
    } else {
        Ok(0)
    }
}

/// `void secure_destroy(void)` — destroy global context, if any.
///
/// In the C postmaster each `secure_destroy()` in the SIGHUP-off path is paired
/// with `LoadedSSL = false`; the flag lives here, so the pairing is folded in.
pub fn secure_destroy() {
    if USE_SSL {
        tls::be_tls_destroy();
        LOADED_SSL.store(false, Ordering::Relaxed);
    }
}

/// `bool secure_loaded_verify_locations(void)` — whether the root CA store has
/// been loaded to verify certificates. The openssl backend tracks the flag
/// (`ssl_loaded_verify_locations`); be_tls_init's success result carries it,
/// stored alongside `LoadedSSL`. In a non-`USE_SSL` build, false.
pub fn secure_loaded_verify_locations() -> bool {
    if USE_SSL {
        ssl_loaded_verify_locations()
    } else {
        false
    }
}

/// `int secure_open_server(Port *port)` — attempt to negotiate secure session.
pub fn secure_open_server(port: &mut Port) -> PgResult<i32> {
    if USE_SSL {
        let mut r: i32 = 0;

        /* push unencrypted buffered data back through SSL setup */
        let len = backend_libpq_pqcomm_seams::pq_buffer_remaining_data::call();
        if len > 0 {
            // `char *buf = palloc(len);` — the pqcomm seam allocates the read
            // buffer in `mcx`; a transient context stands in for the C
            // CurrentMemoryContext, and the bytes are copied into the Port's
            // owned `raw_buf`.
            backend_libpq_pqcomm_seams::pq_startmsgread::call()?;
            // `if (pq_getbytes(buf, len) == EOF) return STATUS_ERROR;`
            let ctx = mcx::MemoryContext::new("secure_open_server");
            let bytes: Option<Vec<u8>> =
                match backend_libpq_pqcomm_seams::pq_getbytes::call(ctx.mcx(), len as usize)? {
                    Some(v) => Some(v.as_slice().to_vec()),
                    None => None,
                };
            let buf = match bytes {
                Some(b) => b,
                None => return Ok(STATUS_ERROR), /* shouldn't be possible */
            };
            backend_libpq_pqcomm_seams::pq_endmsgread::call();
            port.raw_buf = Some(buf);
            port.raw_buf_remaining = len;
            port.raw_buf_consumed = 0;
        }
        debug_assert_eq!(backend_libpq_pqcomm_seams::pq_buffer_remaining_data::call(), 0);

        // INJECTION_POINT("backend-ssl-startup", NULL): USE_INJECTION_POINTS is
        // not compiled in on this target (cf. backend_startup.c), so the
        // injection point is a no-op here.

        // r = be_tls_open_server(port);
        r = be_tls_open_server(port)?;

        if port.raw_buf_remaining > 0 {
            // This shouldn't be possible -- it would mean the client sent
            // encrypted data before we established a session key...
            ereport(LOG)
                .errmsg_internal(
                    "buffered unencrypted data remains after negotiating SSL connection",
                )
                .finish(loc("secure_open_server"))?;
            return Ok(STATUS_ERROR);
        }
        if port.raw_buf.is_some() {
            // pfree(port->raw_buf); port->raw_buf = NULL;
            port.raw_buf = None;
        }

        ereport(DEBUG2)
            .errmsg_internal(format!(
                "SSL connection from DN:\"{}\" CN:\"{}\"",
                port.peer_dn.as_deref().unwrap_or("(anonymous)"),
                port.peer_cn.as_deref().unwrap_or("(anonymous)"),
            ))
            .finish(loc("secure_open_server"))?;
        Ok(r)
    } else {
        Ok(0)
    }
}

/// `void secure_close(Port *port)` — close secure session.
pub fn secure_close(port: &mut Port) {
    if USE_SSL && port.ssl_in_use {
        tls::be_tls_close(port);
    }
}

/// `ssize_t secure_read(Port *port, void *ptr, size_t len)`.
///
/// The repo seam contract carries the `ssize_t` outcome as `SockResult`: `n > 0`
/// -> `Ok(n)`, `n == 0` (EOF) -> `Err(Eof)`, `n < 0` -> `Err(Errno(e))`.
pub fn secure_read(port: &mut Port, buf: &mut [u8]) -> PgResult<SockResult> {
    let mut n: isize;
    let mut waitfor: i32;
    // The single `errno` the C tests against: the raw/GSS arms leave the OS
    // syscall errno; the SSL arm's `be_tls_read` classifies its own and returns
    // it (the C `errno =` inside `be_tls_read`). Carried so the final
    // SockResult mapping sees the same value the C would read from `errno`.
    let mut last_arm_errno: i32 = 0;

    /* Deal with any already-pending interrupt condition. */
    backend_tcop_postgres_seams::process_client_read_interrupt::call(false)?;

    // retry:
    loop {
        waitfor = 0;
        let arm_errno: i32;
        if USE_SSL && port.ssl_in_use {
            let (io, data) = tls::be_tls_read(port, buf.len());
            n = io.n;
            waitfor = io.waitfor;
            arm_errno = io.errno;
            if io.n > 0 {
                let k = io.n as usize;
                buf[..k].copy_from_slice(&data[..k]);
            }
        } else if ENABLE_GSS && gss_enc(port) {
            n = be_gssapi_read(port, buf);
            waitfor = WL_SOCKET_READABLE as i32;
            arm_errno = errno();
        } else {
            n = secure_raw_read(port, buf);
            waitfor = WL_SOCKET_READABLE as i32;
            arm_errno = errno();
        }
        last_arm_errno = arm_errno;

        /* In blocking mode, wait until the socket is ready */
        if n < 0 && !port.noblock && would_block(arm_errno) {
            debug_assert!(waitfor != 0);

            // ModifyWaitEvent(FeBeWaitSet, FeBeWaitSetSocketPos, waitfor, NULL);
            backend_libpq_pqcomm_seams::modify_fe_be_wait_set_socket::call(waitfor as u32);

            // WaitEventSetWait(FeBeWaitSet, -1, &event, 1, WAIT_EVENT_CLIENT_READ);
            let (_nev, events) =
                backend_libpq_pqcomm_seams::wait_event_set_wait_fe_be::call(-1, WAIT_EVENT_CLIENT_READ);

            // Postmaster death -> exit.
            if events & WL_POSTMASTER_DEATH != 0 {
                return Err(ereport(FATAL)
                    .errcode(ERRCODE_ADMIN_SHUTDOWN)
                    .errmsg("terminating connection due to unexpected postmaster exit")
                    .into_error());
            }

            /* Handle interrupt. */
            if events & WL_LATCH_SET != 0 {
                backend_storage_ipc_latch_seams::reset_latch_my_latch::call();
                backend_tcop_postgres_seams::process_client_read_interrupt::call(true)?;
            }
            continue; // goto retry;
        }
        break;
    }

    /*
     * Process interrupts that happened during a successful (or non-blocking, or
     * hard-failed) read.
     */
    backend_tcop_postgres_seams::process_client_read_interrupt::call(false)?;

    Ok(ssize_to_sock_result(n, last_arm_errno))
}

/// `ssize_t secure_raw_read(Port *port, void *ptr, size_t len)`.
pub fn secure_raw_read(port: &mut Port, buf: &mut [u8]) -> isize {
    /* Read from the "unread" buffered data first. c.f. libpq-be.h */
    if port.raw_buf_remaining > 0 {
        let mut len = buf.len() as i64;
        if len > port.raw_buf_remaining {
            len = port.raw_buf_remaining;
        }
        let len_usize = len as usize;
        debug_assert!(port.raw_buf.is_some());
        let consumed = port.raw_buf_consumed as usize;
        if let Some(src) = port.raw_buf.as_ref() {
            buf[..len_usize].copy_from_slice(&src[consumed..consumed + len_usize]);
        }
        port.raw_buf_consumed += len;
        port.raw_buf_remaining -= len;
        return len as isize;
    }

    /*
     * Try to read from the socket without blocking. (WIN32 `pgwin32_noblock`
     * toggling is the dead WIN32 path; this is non-WIN32.)
     * n = recv(port->sock, ptr, len, 0);
     */
    unsafe { libc::recv(port.sock, buf.as_mut_ptr() as *mut libc::c_void, buf.len(), 0) as isize }
}

/// `ssize_t secure_write(Port *port, const void *ptr, size_t len)`.
pub fn secure_write(port: &mut Port, buf: &[u8]) -> PgResult<SockResult> {
    let mut n: isize;
    let mut waitfor: i32;
    let mut last_arm_errno: i32 = 0;

    /* Deal with any already-pending interrupt condition. */
    backend_tcop_postgres_seams::process_client_write_interrupt::call(false)?;

    // retry:
    loop {
        waitfor = 0;
        let arm_errno: i32;
        if USE_SSL && port.ssl_in_use {
            let io = tls::be_tls_write(port, buf);
            n = io.n;
            waitfor = io.waitfor;
            arm_errno = io.errno;
        } else if ENABLE_GSS && gss_enc(port) {
            n = be_gssapi_write(port, buf);
            waitfor = WL_SOCKET_WRITEABLE as i32;
            arm_errno = errno();
        } else {
            n = secure_raw_write(port, buf);
            waitfor = WL_SOCKET_WRITEABLE as i32;
            arm_errno = errno();
        }
        last_arm_errno = arm_errno;

        if n < 0 && !port.noblock && would_block(arm_errno) {
            debug_assert!(waitfor != 0);

            backend_libpq_pqcomm_seams::modify_fe_be_wait_set_socket::call(waitfor as u32);

            let (_nev, events) = backend_libpq_pqcomm_seams::wait_event_set_wait_fe_be::call(
                -1,
                WAIT_EVENT_CLIENT_WRITE,
            );

            // See comments in secure_read.
            if events & WL_POSTMASTER_DEATH != 0 {
                return Err(ereport(FATAL)
                    .errcode(ERRCODE_ADMIN_SHUTDOWN)
                    .errmsg("terminating connection due to unexpected postmaster exit")
                    .into_error());
            }

            /* Handle interrupt. */
            if events & WL_LATCH_SET != 0 {
                backend_storage_ipc_latch_seams::reset_latch_my_latch::call();
                backend_tcop_postgres_seams::process_client_write_interrupt::call(true)?;
            }
            continue; // goto retry;
        }
        break;
    }

    /*
     * Process interrupts that happened during a successful (or non-blocking, or
     * hard-failed) write.
     */
    backend_tcop_postgres_seams::process_client_write_interrupt::call(false)?;

    Ok(ssize_to_sock_result(n, last_arm_errno))
}

/// `ssize_t secure_raw_write(Port *port, const void *ptr, size_t len)`.
pub fn secure_raw_write(port: &mut Port, buf: &[u8]) -> isize {
    // (WIN32 `pgwin32_noblock` toggling is the dead WIN32 path; this is non-WIN32.)
    // n = send(port->sock, ptr, len, 0);
    unsafe { libc::send(port.sock, buf.as_ptr() as *const libc::c_void, buf.len(), 0) as isize }
}

/* ============================================================== *
 *  Local helpers
 * ============================================================== */

/// `errno == EWOULDBLOCK || errno == EAGAIN` after a failed transport step.
fn would_block(errno: i32) -> bool {
    errno == EWOULDBLOCK || errno == EAGAIN
}

/// Marshal a C `ssize_t` transport return into the repo `SockResult` contract.
/// `arm_errno` is the errno the arm that produced `n` left behind (the process
/// errno for raw/GSS, the classified errno for the SSL arm) — the single value
/// the C code reads from `errno` after a `-1` return.
fn ssize_to_sock_result(n: isize, arm_errno: i32) -> SockResult {
    if n > 0 {
        Ok(n as usize)
    } else if n == 0 {
        Err(SockError::Eof)
    } else {
        Err(SockError::Errno(arm_errno))
    }
}

/// `(ssl_min_protocol_version, ssl_max_protocol_version)` — the file-scope GUC
/// pair `be_tls_init` reads. Only consulted on the (dead) `USE_SSL` path.
fn ssl_protocol_versions() -> (i32, i32) {
    use backend_utils_misc_guc_tables::vars;
    (
        vars::ssl_min_protocol_version.read(),
        vars::ssl_max_protocol_version.read(),
    )
}

/// `ssl_loaded_verify_locations` — read back from the openssl backend's last
/// successful `be_tls_init` / `be_tls_destroy`. Only on the dead `USE_SSL` path.
fn ssl_loaded_verify_locations() -> bool {
    SSL_LOADED_VERIFY.load(Ordering::Relaxed)
}
static SSL_LOADED_VERIFY: AtomicBool = AtomicBool::new(false);

/// `port->gss && port->gss->enc` — whether GSS encryption is active. Dead in a
/// non-`ENABLE_GSS` build; the non-GSS build has no `gss` carrier, so false.
fn gss_enc(_port: &Port) -> bool {
    false
}

/// `be_tls_open_server(port)` — run the handshake and copy the negotiated facts
/// into the `Port`. The accept loop's between-step wait is
/// `WaitLatchOrSocket(NULL, waitfor, port->sock, 0, WAIT_EVENT_SSL_OPEN_SERVER)`,
/// driven through the FeBe wait machinery. Returns 0 on success, STATUS_ERROR
/// on failure (the COMMERROR already emitted inside the backend).
fn be_tls_open_server(port: &mut Port) -> PgResult<i32> {
    let sock = port.sock;
    let res = tls::be_tls_open_server(port, |_waitfor| {
        // WaitLatchOrSocket(NULL, waitfor, port->sock, 0,
        //                   WAIT_EVENT_SSL_OPEN_SERVER) — wait on this socket.
        // Modeled on the FeBe wait set's socket position.
        let _ = sock;
        backend_libpq_pqcomm_seams::modify_fe_be_wait_set_socket::call(_waitfor);
        let _ = backend_libpq_pqcomm_seams::wait_event_set_wait_fe_be::call(
            -1,
            WAIT_EVENT_SSL_OPEN_SERVER,
        );
    })?;
    match res {
        Ok(r) => {
            port.ssl_in_use = r.ssl_in_use;
            port.alpn_used = r.alpn_used;
            port.peer_cn = r.peer_cn;
            port.peer_dn = r.peer_dn;
            port.peer_cert_valid = r.peer_cert_valid;
            Ok(0)
        }
        Err(()) => Ok(STATUS_ERROR),
    }
}

/// `WAIT_EVENT_SSL_OPEN_SERVER` (utils/wait_event.h) — `PG_WAIT_CLIENT | 5`.
const WAIT_EVENT_SSL_OPEN_SERVER: u32 = 0x06000000 | 5;

/// `be_gssapi_read(port, ptr, len)` — the GSS transport read arm. DEAD under
/// the non-`ENABLE_GSS` build; compiler-folded away and never invoked. The GSS
/// transport backend (be-secure-gssapi.c) is unported, so this is a loud panic
/// rather than a fabricated delegation (mirror-PG-and-panic).
fn be_gssapi_read(_port: &mut Port, _buf: &mut [u8]) -> isize {
    panic!(
        "be_gssapi_read: GSS transport arm is dead under non-ENABLE_GSS build; \
         be-secure-gssapi.c is unported"
    )
}

/// `be_gssapi_write(port, ptr, len)` — see [`be_gssapi_read`].
fn be_gssapi_write(_port: &mut Port, _buf: &[u8]) -> isize {
    panic!(
        "be_gssapi_write: GSS transport arm is dead under non-ENABLE_GSS build; \
         be-secure-gssapi.c is unported"
    )
}

/* ============================================================== *
 *  SockAddr family helper (the `port->laddr.addr.ss_family` reads).
 * ============================================================== */

fn sockaddr_family(sa: &types_net::SockAddr) -> i32 {
    let mut ss: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let n = (sa.salen as usize)
        .min(std::mem::size_of::<libc::sockaddr_storage>())
        .min(sa.addr.len());
    unsafe {
        std::ptr::copy_nonoverlapping(
            sa.addr.as_ptr(),
            (&mut ss as *mut libc::sockaddr_storage).cast::<u8>(),
            n,
        );
    }
    ss.ss_family as i32
}

/* ============================================================== *
 *  Negotiation guards (backend_startup.c crossings) — owned here.
 * ============================================================== */

/// `#ifdef USE_SSL` — whether this build supports SSL.
fn ssl_supported() -> bool {
    USE_SSL
}

/// `#ifdef ENABLE_GSS` — whether this build supports GSSAPI encryption.
fn gss_supported() -> bool {
    ENABLE_GSS
}

/// `!LoadedSSL || port->laddr.addr.ss_family == AF_UNIX` — SSL is not offered
/// for this connection.
fn ssl_negotiation_disabled(port: &mut Port) -> bool {
    !loaded_ssl() || sockaddr_family(&port.laddr) == libc::AF_UNIX
}

/// `port->laddr.addr.ss_family == AF_UNIX` — GSSAPI encryption is not offered
/// over a Unix-domain socket.
fn gss_negotiation_disabled(port: &mut Port) -> bool {
    sockaddr_family(&port.laddr) == libc::AF_UNIX
}

/* ============================================================== *
 *  Seam adapters + installation.
 * ============================================================== */

/// Install this unit's inward seams (the be-secure contract). The `be_tls_get_*`
/// accessors are installed by the openssl backend (its `init_seams`). The GSS
/// guards/openers (`gss_supported`, `gss_negotiation_disabled`,
/// `secure_open_gssapi`) belong to the unported be-secure-gssapi unit and are
/// NOT installed here — they panic-until-bound when that unit lands.
///
/// `secure_open_server` is declared infallible (`-> i32`); our owner returns
/// `PgResult<i32>` only because the dead `USE_SSL` arm can `ereport(FATAL)` on
/// postmaster exit. With `USE_SSL == false` it always returns `Ok(0)`, so the
/// `.expect` is unreachable in this build (a FATAL would not return in C
/// either).
///
/// The build guards (`ssl_supported`/`gss_supported`, the `#ifdef` constants)
/// and the negotiation predicates (`ssl_negotiation_disabled`/
/// `gss_negotiation_disabled`, the `port->laddr`/`LoadedSSL` checks) are owned
/// here, since be-secure owns `LoadedSSL` and the socket-family helper. Only
/// the actual GSS handshake `secure_open_gssapi` (be-secure-gssapi.c) is left
/// to the unported GSS unit (panic-until-bound).
pub fn init_seams() {
    use backend_libpq_be_secure_seams as s;
    s::secure_read::set(secure_read);
    s::secure_write::set(secure_write);
    s::secure_close::set(secure_close);
    s::ssl_supported::set(ssl_supported);
    s::gss_supported::set(gss_supported);
    s::ssl_negotiation_disabled::set(ssl_negotiation_disabled);
    s::gss_negotiation_disabled::set(gss_negotiation_disabled);
    s::secure_open_server::set(|port| secure_open_server(port).expect("secure_open_server"));
}
