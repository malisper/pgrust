//! `src/port/noblock.c` — set a file descriptor as blocking or non-blocking.
//!
//! This is the non-Windows (unix/macOS) implementation. The Windows path uses
//! `ioctlsocket(FIONBIO)`; on unix we use `fcntl(F_GETFL)` / `fcntl(F_SETFL)`.

use types_core::pgsocket;

/// Put socket into nonblock mode.
///
/// Returns true on success, false on failure. Never ereports.
///
/// Faithful port of the `#if !defined(WIN32)` branch of `pg_set_noblock`.
pub fn pg_set_noblock(sock: pgsocket) -> bool {
    // SAFETY: fcntl on a valid socket fd; failures are reported via the return
    // value exactly as the C code checks.
    let flags = unsafe { libc::fcntl(sock, libc::F_GETFL) };
    if flags < 0 {
        return false;
    }
    if unsafe { libc::fcntl(sock, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1 {
        return false;
    }
    true
}

/// Put socket into blocking mode.
///
/// Returns true on success, false on failure. Faithful port of the
/// `#if !defined(WIN32)` branch of `pg_set_block`. Retained for completeness
/// (the seam crate currently only declares `pg_set_noblock`).
pub fn pg_set_block(sock: pgsocket) -> bool {
    // SAFETY: see `pg_set_noblock`.
    let flags = unsafe { libc::fcntl(sock, libc::F_GETFL) };
    if flags < 0 {
        return false;
    }
    if unsafe { libc::fcntl(sock, libc::F_SETFL, flags & !libc::O_NONBLOCK) } == -1 {
        return false;
    }
    true
}

/// Install this crate's seams.
pub fn init_seams() {
    noblock_seams::pg_set_noblock::set(pg_set_noblock);
}
