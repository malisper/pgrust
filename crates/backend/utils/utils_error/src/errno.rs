//! Platform `errno` values and the errno -> SQLSTATE mappings
//! (`errcode_for_file_access` / `errcode_for_socket_access`), plus the `%m`
//! (strerror) substitution used by the message-building functions.

#![allow(dead_code)]

#[cfg(not(target_family = "wasm"))]
use std::ffi::CStr;

use ::types_error::{
    SqlState, ERRCODE_CONNECTION_FAILURE, ERRCODE_DISK_FULL, ERRCODE_DUPLICATE_FILE,
    ERRCODE_FILE_NAME_TOO_LONG, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INSUFFICIENT_RESOURCES,
    ERRCODE_INTERNAL_ERROR, ERRCODE_IO_ERROR, ERRCODE_OUT_OF_MEMORY, ERRCODE_UNDEFINED_FILE,
    ERRCODE_WRONG_OBJECT_TYPE,
};

// On wasm (`target_family = "wasm"`) `libc` does not export the errno
// constants, `strerror`, or any of the file/socket syscalls. These errno
// values feed only the errno -> SQLSTATE classification (`%m` substitution and
// file/socket access SQLSTATE mapping); single-user wasm rarely produces real
// OS errnos, but the constants must exist and be coherent. We pin them to the
// standard 64-bit Linux numeric values, identical to what `libc::E*` yields
// natively, so the match arms below classify the same way on every target.
#[cfg(not(target_family = "wasm"))]
mod errno_consts {
    pub const EPERM: i32 = libc::EPERM;
    pub const ENOENT: i32 = libc::ENOENT;
    pub const EIO: i32 = libc::EIO;
    pub const ENOMEM: i32 = libc::ENOMEM;
    pub const EACCES: i32 = libc::EACCES;
    pub const EEXIST: i32 = libc::EEXIST;
    pub const ENOTDIR: i32 = libc::ENOTDIR;
    pub const EISDIR: i32 = libc::EISDIR;
    pub const EINVAL: i32 = libc::EINVAL;
    pub const ENFILE: i32 = libc::ENFILE;
    pub const EMFILE: i32 = libc::EMFILE;
    pub const ENOSPC: i32 = libc::ENOSPC;
    pub const EROFS: i32 = libc::EROFS;
    pub const EPIPE: i32 = libc::EPIPE;
    pub const ENAMETOOLONG: i32 = libc::ENAMETOOLONG;
    pub const ENOTEMPTY: i32 = libc::ENOTEMPTY;
    pub const ENETDOWN: i32 = libc::ENETDOWN;
    pub const ENETUNREACH: i32 = libc::ENETUNREACH;
    pub const ENETRESET: i32 = libc::ENETRESET;
    pub const ECONNABORTED: i32 = libc::ECONNABORTED;
    pub const ECONNRESET: i32 = libc::ECONNRESET;
    pub const ETIMEDOUT: i32 = libc::ETIMEDOUT;
    pub const EHOSTDOWN: i32 = libc::EHOSTDOWN;
    pub const EHOSTUNREACH: i32 = libc::EHOSTUNREACH;
}

#[cfg(target_family = "wasm")]
mod errno_consts {
    // Standard Linux errno numbers (asm-generic/errno-base.h + errno.h).
    pub const EPERM: i32 = 1;
    pub const ENOENT: i32 = 2;
    pub const EIO: i32 = 5;
    pub const ENOMEM: i32 = 12;
    pub const EACCES: i32 = 13;
    pub const EEXIST: i32 = 17;
    pub const ENOTDIR: i32 = 20;
    pub const EISDIR: i32 = 21;
    pub const EINVAL: i32 = 22;
    pub const ENFILE: i32 = 23;
    pub const EMFILE: i32 = 24;
    pub const ENOSPC: i32 = 28;
    pub const EROFS: i32 = 30;
    pub const EPIPE: i32 = 32;
    pub const ENAMETOOLONG: i32 = 36;
    pub const ENOTEMPTY: i32 = 39;
    pub const ENETDOWN: i32 = 100;
    pub const ENETUNREACH: i32 = 101;
    pub const ENETRESET: i32 = 102;
    pub const ECONNABORTED: i32 = 103;
    pub const ECONNRESET: i32 = 104;
    pub const ETIMEDOUT: i32 = 110;
    pub const EHOSTDOWN: i32 = 112;
    pub const EHOSTUNREACH: i32 = 113;
}

pub use errno_consts::*;

/// The errno -> SQLSTATE switch body of `errcode_for_file_access` (the C
/// function reads the saved errno from the current error frame; the
/// frame-mutating entry point lives in `stack.rs`).
pub fn sqlstate_for_file_access(errno: i32) -> SqlState {
    match errno {
        // Permission-denied failures
        EPERM | EACCES | EROFS => ERRCODE_INSUFFICIENT_PRIVILEGE,
        // File not found
        ENOENT => ERRCODE_UNDEFINED_FILE,
        // Duplicate file
        EEXIST => ERRCODE_DUPLICATE_FILE,
        // Wrong object type or state
        ENOTDIR | EISDIR | ENOTEMPTY => ERRCODE_WRONG_OBJECT_TYPE,
        // Insufficient resources
        ENOSPC => ERRCODE_DISK_FULL,
        ENOMEM => ERRCODE_OUT_OF_MEMORY,
        ENFILE | EMFILE => ERRCODE_INSUFFICIENT_RESOURCES,
        // Hardware failure
        EIO => ERRCODE_IO_ERROR,
        ENAMETOOLONG => ERRCODE_FILE_NAME_TOO_LONG,
        // All else is classified as internal errors
        _ => ERRCODE_INTERNAL_ERROR,
    }
}

/// The errno -> SQLSTATE switch body of `errcode_for_socket_access`
/// (`ALL_CONNECTION_FAILURE_ERRNOS` from port.h).
pub fn sqlstate_for_socket_access(errno: i32) -> SqlState {
    match errno {
        EPIPE | ECONNRESET | ECONNABORTED | EHOSTDOWN | EHOSTUNREACH | ENETDOWN | ENETRESET
        | ENETUNREACH | ETIMEDOUT => ERRCODE_CONNECTION_FAILURE,
        _ => ERRCODE_INTERNAL_ERROR,
    }
}

/// `errno` as seen by the calling thread right now (the value elog.c saves
/// into `edata->saved_errno` at `get_error_stack_entry`).
///
/// On wasm64-unknown-unknown `std::io::Error::last_os_error()` is INERT (std's
/// errno is not wired to anything — it stays 0), but the file/stat/open shims in
/// `wasm-libc-shim` DO set their own thread-local errno (ENOENT/EEXIST/EMFILE/…).
/// Ported code that branches on errno (e.g. tablespace `stat`==ENOENT -> create
/// the dir, EMFILE retry loops) must read THAT errno, so on wasm read the shim's.
#[cfg(not(target_family = "wasm"))]
pub fn current_errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}
#[cfg(target_family = "wasm")]
pub fn current_errno() -> i32 {
    wasm_libc_shim::errno()
}

/// `strerror(errnum)` — the text `%m` expands to.
#[cfg(not(target_family = "wasm"))]
pub fn strerror(errnum: i32) -> String {
    // SAFETY: strerror returns a pointer to a NUL-terminated string; we copy
    // it out immediately.
    unsafe {
        let ptr = libc::strerror(errnum);
        if ptr.is_null() {
            format!("unrecognized error {}", errnum)
        } else {
            CStr::from_ptr(ptr).to_string_lossy().into_owned()
        }
    }
}

/// `strerror(errnum)` — wasm has no `libc::strerror`; std's
/// `from_raw_os_error` gives an equivalent human-readable description.
#[cfg(target_family = "wasm")]
pub fn strerror(errnum: i32) -> String {
    std::io::Error::from_raw_os_error(errnum).to_string()
}

/// Expand `%m` in a (caller-preformatted) message string using the saved
/// errno, mirroring the `errno = edata->saved_errno; vsnprintf(...%m...)`
/// dance in `EVALUATE_MESSAGE`. `%%m` is left alone (printf would render the
/// literal `%m` from it only after `%%` -> `%`, which the caller's `format!`
/// already performed, so a plain replace matches the post-format text).
pub fn replace_percent_m(message: &str, errno: i32) -> String {
    if !message.contains("%m") {
        return message.to_owned();
    }
    message.replace("%m", &strerror(errno))
}
