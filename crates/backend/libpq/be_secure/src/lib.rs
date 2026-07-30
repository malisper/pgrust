// be-secure.c, USE_SSL build (openssl), non-ENABLE_GSS: the GSS arms are
// compiled out as in C.

#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use elog::ereport;
use types_core::STATUS_ERROR;
use types_error::{ErrorLocation, PgResult, DEBUG2, ERROR, LOG};
use types_storage::waiteventset::{WL_LATCH_SET, WL_SOCKET_READABLE, WL_SOCKET_WRITEABLE};

// USE_SSL selector: the OpenSSL backend when the `ssl` feature is on and the
// target can build vendored OpenSSL (everything but wasm32); otherwise C's
// !USE_SSL shape — the be_tls_* entry points don't exist in C, and every
// caller is gated on LoadedSSL / port->ssl_in_use, which never become true.
#[cfg(all(feature = "ssl", not(target_family = "wasm")))]
use be_secure_openssl as tls_impl;
#[cfg(not(all(feature = "ssl", not(target_family = "wasm"))))]
mod no_ssl_stub;
#[cfg(not(all(feature = "ssl", not(target_family = "wasm"))))]
use no_ssl_stub as tls_impl;

// WAIT_EVENT_CLIENT_READ / WAIT_EVENT_CLIENT_WRITE: PG_WAIT_CLIENT | 0 / 1.
const WAIT_EVENT_CLIENT_READ: u32 = 0x0600_0000;
const WAIT_EVENT_CLIENT_WRITE: u32 = 0x0600_0000 | 1;

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn errno() -> i32 {
    unsafe { *libc::__error() }
}

#[cfg(not(any(target_os = "macos", target_os = "ios")))]
fn errno() -> i32 {
    unsafe { *libc::__errno_location() }
}

fn would_block(e: i32) -> bool {
    e == libc::EWOULDBLOCK || e == libc::EAGAIN
}

// pqcomm's cells, never MyProcPort: auth-time FATALs send under its borrow.
fn my_port_state() -> (i32, bool, bool) {
    pqcomm::client_socket_state().expect("secure_read/write before pq_init")
}

/// The seam's `ssize_t` contract: `Ok(0)` is EOF, `Err(errno)` is the C `-1`.
fn ssize_result(n: isize, e: i32) -> Result<usize, i32> {
    if n >= 0 {
        Ok(n as usize)
    } else {
        Err(e)
    }
}

pub fn secure_initialize(is_server_start: bool) -> PgResult<i32> {
    tls_impl::be_tls_init(is_server_start)
}

pub fn secure_destroy() {
    tls_impl::be_tls_destroy()
}

pub fn secure_loaded_verify_locations() -> bool {
    tls_impl::ssl_loaded_verify_locations()
}

pub fn secure_open_server() -> PgResult<i32> {
    // Push unencrypted buffered data back through SSL setup.
    let len = pqcomm::pq_buffer_remaining_data();
    let mut raw_buf = vec![0u8; len.max(0) as usize];
    if len > 0 {
        pqcomm::pq_startmsgread()?;
        if pqcomm::pq_getbytes(&mut raw_buf)? == pqcomm::EOF {
            return Ok(STATUS_ERROR);
        }
        pqcomm::pq_endmsgread();
    }
    debug_assert_eq!(pqcomm::pq_buffer_remaining_data(), 0);

    let (sock, _, _) = my_port_state();
    let open = tls_impl::be_tls_open_server(sock, raw_buf)?;

    if open.raw_remaining > 0 {
        ereport(LOG)
            .errmsg_internal("buffered unencrypted data remains after negotiating SSL connection")
            .finish(loc("secure_open_server"))?;
        return Ok(STATUS_ERROR);
    }

    if open.r == 0 {
        let alpn_used = open.alpn_used;
        let peer_cn = open.peer_cn.clone();
        let peer_dn = open.peer_dn.clone();
        let peer_cert_valid = open.peer_cert_valid;
        init_small::globals::WithMyProcPort(|p| {
            p.ssl_in_use = true;
            p.alpn_used = alpn_used;
            p.peer_cn = peer_cn.clone();
            p.peer_dn = peer_dn.clone();
            p.peer_cert_valid = peer_cert_valid;
        });
    }

    ereport(DEBUG2)
        .errmsg_internal(format!(
            "SSL connection from DN:\"{}\" CN:\"{}\"",
            open.peer_dn.as_deref().unwrap_or("(anonymous)"),
            open.peer_cn.as_deref().unwrap_or("(anonymous)"),
        ))
        .finish(loc("secure_open_server"))?;
    Ok(open.r)
}

pub fn secure_close() {
    if let Some((_, _, ssl_in_use)) = pqcomm::client_socket_state() {
        if ssl_in_use {
            tls_impl::be_tls_close();
            pqcomm::set_ssl_in_use(false);
        }
    }
}

pub fn be_tls_get_certificate_hash() -> PgResult<Option<Vec<u8>>> {
    tls_impl::be_tls_get_certificate_hash()
}

pub fn be_tls_get_version() -> Option<String> {
    tls_impl::be_tls_get_version()
}

pub fn be_tls_get_cipher() -> Option<String> {
    tls_impl::be_tls_get_cipher()
}

pub fn be_tls_get_cipher_bits() -> i32 {
    tls_impl::be_tls_get_cipher_bits()
}

pub fn secure_raw_read(sock: i32, buf: &mut [u8]) -> isize {
    // SAFETY: buf is valid writable memory of buf.len() bytes.
    unsafe { libc::recv(sock, buf.as_mut_ptr().cast(), buf.len(), 0) }
}

pub fn secure_raw_write(sock: i32, buf: &[u8]) -> isize {
    // SAFETY: buf is valid readable memory of buf.len() bytes.
    unsafe { libc::send(sock, buf.as_ptr().cast(), buf.len(), 0) }
}

#[cold]
#[inline(never)]
fn tls_read_arm(buf: &mut [u8]) -> PgResult<(isize, i32, u32)> {
    let io = tls_impl::be_tls_read(buf)?;
    Ok((io.n, io.errno, io.waitfor))
}

#[cold]
#[inline(never)]
fn tls_write_arm(buf: &[u8]) -> PgResult<(isize, i32, u32)> {
    let io = tls_impl::be_tls_write(buf)?;
    Ok((io.n, io.errno, io.waitfor))
}

pub fn secure_read(buf: &mut [u8]) -> PgResult<Result<usize, i32>> {
    postgres_seams::process_client_read_interrupt::call(false)?;

    let (n, e) = loop {
        let (sock, noblock, ssl_in_use) = my_port_state();
        let (n, e, waitfor) = if ssl_in_use {
            tls_read_arm(buf)?
        } else {
            let n = secure_raw_read(sock, buf);
            (n, errno(), WL_SOCKET_READABLE)
        };

        if n < 0 && !noblock && would_block(e) {
            pqcomm::pq_modify_fe_be_wait_set_socket(waitfor)?;
            let events = pqcomm::pq_wait_event_set_wait_fe_be(-1, WAIT_EVENT_CLIENT_READ)?;
            if events & WL_LATCH_SET != 0 {
                latch_seams::reset_latch_my_latch::call();
                postgres_seams::process_client_read_interrupt::call(true)?;
            }
            continue;
        }
        break (n, e);
    };

    postgres_seams::process_client_read_interrupt::call(false)?;

    Ok(ssize_result(n, e))
}

pub fn secure_write(buf: &[u8]) -> PgResult<Result<usize, i32>> {
    postgres_seams::process_client_write_interrupt::call(false)?;

    let (n, e) = loop {
        let (sock, noblock, ssl_in_use) = my_port_state();
        let (n, e, waitfor) = if ssl_in_use {
            tls_write_arm(buf)?
        } else {
            let n = secure_raw_write(sock, buf);
            (n, errno(), WL_SOCKET_WRITEABLE)
        };

        if n < 0 && !noblock && would_block(e) {
            pqcomm::pq_modify_fe_be_wait_set_socket(waitfor)?;
            let events = pqcomm::pq_wait_event_set_wait_fe_be(-1, WAIT_EVENT_CLIENT_WRITE)?;
            if events & WL_LATCH_SET != 0 {
                latch_seams::reset_latch_my_latch::call();
                postgres_seams::process_client_write_interrupt::call(true)?;
            }
            continue;
        }
        break (n, e);
    };

    postgres_seams::process_client_write_interrupt::call(false)?;

    Ok(ssize_result(n, e))
}

pub fn init_seams() {
    be_secure_seams::secure_read::set(secure_read);
    be_secure_seams::secure_write::set(secure_write);
    be_secure_seams::secure_close::set(secure_close);
    be_secure_seams::be_tls_get_certificate_hash::set(|| {
        match be_tls_get_certificate_hash()? {
            Some(hash) => Ok(hash),
            // None (no live TLS conn/cert) is unreachable from the SCRAM
            // caller, which is gated on ssl_in_use; C would deref NULL here.
            None => ereport(ERROR)
                .errmsg_internal("no server certificate available for channel binding")
                .finish(loc("be_tls_get_certificate_hash"))
                .map(|()| Vec::new()),
        }
    });
}

#[cfg(test)]
mod tests;
