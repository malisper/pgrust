// be-secure.c, plain-socket build (non-USE_SSL, non-ENABLE_GSS): the
// encrypted arms and port->raw_buf defer loudly with the TLS/GSS units.

#![allow(non_upper_case_globals)]

use types_error::PgResult;
use types_storage::waiteventset::{WL_LATCH_SET, WL_SOCKET_READABLE, WL_SOCKET_WRITEABLE};


// WAIT_EVENT_CLIENT_READ / WAIT_EVENT_CLIENT_WRITE: PG_WAIT_CLIENT | 0 / 1.
const WAIT_EVENT_CLIENT_READ: u32 = 0x0600_0000;
const WAIT_EVENT_CLIENT_WRITE: u32 = 0x0600_0000 | 1;

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

#[cold]
#[inline(never)]
fn encrypted_arm_unported() -> ! {
    panic!("be-secure: TLS/GSS transport arms are unported (ssl_in_use on a plain-socket build)")
}

// pqcomm's cells, never MyProcPort: auth-time FATALs send under its borrow.
fn my_port_state() -> (i32, bool) {
    let (sock, noblock, ssl_in_use) =
        pqcomm::client_socket_state().expect("secure_read/write before pq_init");
    if ssl_in_use {
        encrypted_arm_unported();
    }
    (sock, noblock)
}

/// The seam's `ssize_t` contract: `Ok(0)` is EOF, `Err(errno)` is the C `-1`.
fn ssize_result(n: isize, e: i32) -> Result<usize, i32> {
    if n >= 0 {
        Ok(n as usize)
    } else {
        Err(e)
    }
}

pub fn secure_raw_read(sock: i32, buf: &mut [u8]) -> isize {
    // SAFETY: buf is valid writable memory of buf.len() bytes.
    unsafe { libc::recv(sock, buf.as_mut_ptr().cast(), buf.len(), 0) }
}

pub fn secure_raw_write(sock: i32, buf: &[u8]) -> isize {
    // SAFETY: buf is valid readable memory of buf.len() bytes.
    unsafe { libc::send(sock, buf.as_ptr().cast(), buf.len(), 0) }
}

pub fn secure_read(buf: &mut [u8]) -> PgResult<Result<usize, i32>> {
    postgres_seams::process_client_read_interrupt::call(false)?;

    let (n, e) = loop {
        let (sock, noblock) = my_port_state();
        let n = secure_raw_read(sock, buf);
        let e = errno();

        if n < 0 && !noblock && would_block(e) {
            pqcomm::pq_modify_fe_be_wait_set_socket(WL_SOCKET_READABLE)?;
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
        let (sock, noblock) = my_port_state();
        let n = secure_raw_write(sock, buf);
        let e = errno();

        if n < 0 && !noblock && would_block(e) {
            pqcomm::pq_modify_fe_be_wait_set_socket(WL_SOCKET_WRITEABLE)?;
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
}

#[cfg(test)]
mod tests;
