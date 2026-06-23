use core::ffi::{c_char, c_int};

use crate::Size;

// Protocol message type bytes (src/include/libpq/protocol.h).  Additive: the
// COPY-protocol tags needed by the copyto.c / copyfrom.c ports.
/// `PqMsg_CopyOutResponse` (`'H'`) — backend → frontend, COPY OUT starting.
pub const PqMsg_CopyOutResponse: c_char = b'H' as c_char;
/// `PqMsg_CopyInResponse` (`'G'`) — backend → frontend, COPY IN starting.
pub const PqMsg_CopyInResponse: c_char = b'G' as c_char;
/// `PqMsg_CopyData` (`'d'`) — a chunk of COPY data.
pub const PqMsg_CopyData: c_char = b'd' as c_char;
/// `PqMsg_CopyDone` (`'c'`) — end of COPY data.
pub const PqMsg_CopyDone: c_char = b'c' as c_char;

// Read/write mode flags for inversion (large object) calls
// (`libpq/libpq-fs.h`).
/// `INV_WRITE` (`libpq-fs.h:21`) — open a large object for writing.
pub const INV_WRITE: c_int = 0x0002_0000;
/// `INV_READ` (`libpq-fs.h:22`) — open a large object for reading.
pub const INV_READ: c_int = 0x0004_0000;

#[derive(Clone, Copy)]
#[repr(C)]
pub struct PQcommMethods {
    pub comm_reset: Option<unsafe extern "C" fn()>,
    pub flush: Option<unsafe extern "C" fn() -> c_int>,
    pub flush_if_writable: Option<unsafe extern "C" fn() -> c_int>,
    pub is_send_pending: Option<unsafe extern "C" fn() -> bool>,
    pub putmessage: Option<unsafe extern "C" fn(c_char, *const c_char, Size) -> c_int>,
    pub putmessage_noblock: Option<unsafe extern "C" fn(c_char, *const c_char, Size)>,
}
