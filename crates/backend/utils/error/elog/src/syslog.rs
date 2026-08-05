//! `write_syslog` and the syslog connection state (HAVE_SYSLOG branch).
//! State is process-global by libc's constraint (one openlog connection, one
//! retained ident pointer per process) — see notes/elog-divergences.md.

use std::ffi::CString;
use std::sync::Mutex;

pub const PG_SYSLOG_LIMIT: usize = 900;

struct SyslogState {
    openlog_done: bool,
    ident: Option<CString>,
    facility: i32,
    seq: u64,
}

#[cfg(not(target_family = "wasm"))]
const DEFAULT_SYSLOG_FACILITY: i32 = libc::LOG_LOCAL0;
// wasm32: no syslogd on WASI; the numeric value (16<<3) only feeds the
// bookkeeping state, never a syscall.
#[cfg(target_family = "wasm")]
const DEFAULT_SYSLOG_FACILITY: i32 = 16 << 3;

static SYSLOG_STATE: Mutex<SyslogState> = Mutex::new(SyslogState {
    openlog_done: false,
    ident: None,
    facility: DEFAULT_SYSLOG_FACILITY,
    seq: 0,
});

pub(crate) fn assign_syslog_ident(newval: &str) {
    let mut state = SYSLOG_STATE.lock().expect("syslog state poisoned");
    let changed = state
        .ident
        .as_ref()
        .is_none_or(|old| old.as_bytes() != newval.as_bytes());
    if changed {
        if state.openlog_done {
            // SAFETY: closelog has no preconditions.
            #[cfg(not(target_family = "wasm"))]
            unsafe {
                libc::closelog()
            };
            state.openlog_done = false;
        }
        state.ident = CString::new(newval).ok();
    }
}

pub(crate) fn syslog_facility() -> i32 {
    SYSLOG_STATE.lock().expect("syslog state poisoned").facility
}

pub(crate) fn assign_syslog_facility(newval: i32) {
    let mut state = SYSLOG_STATE.lock().expect("syslog state poisoned");
    if state.facility != newval {
        if state.openlog_done {
            // SAFETY: closelog has no preconditions.
            #[cfg(not(target_family = "wasm"))]
            unsafe {
                libc::closelog()
            };
            state.openlog_done = false;
        }
        state.facility = newval;
    }
}

#[cfg(not(target_family = "wasm"))]
fn open_syslog(ident: &Option<CString>, facility: i32) {
    let ident_ptr = ident.as_ref().map_or(c"postgres".as_ptr(), |i| i.as_ptr());
    // SAFETY: ident_ptr is a live NUL-terminated string; openlog retains it,
    // and the owning CString lives in the static SYSLOG_STATE for as long as
    // the connection stays open.
    unsafe {
        libc::openlog(
            ident_ptr,
            libc::LOG_PID | libc::LOG_NDELAY | libc::LOG_NOWAIT,
            facility,
        );
    }
}

// wasm32: no syslogd on WASI — connection is a no-op and messages are
// dropped (stderr stays the wasm log path; report.rs never selects the
// syslog destination on wasm).
#[cfg(target_family = "wasm")]
fn open_syslog(_ident: &Option<CString>, _facility: i32) {}

#[cfg(not(target_family = "wasm"))]
fn raw_syslog(level: i32, message: &[u8]) {
    // Interior NULs cannot occur in text built from Rust strings, but guard.
    let Ok(cmsg) = CString::new(message) else {
        return;
    };
    // SAFETY: both pointers are live NUL-terminated strings; "%s" consumes
    // exactly the one vararg.
    unsafe {
        libc::syslog(level, c"%s".as_ptr(), cmsg.as_ptr());
    }
}

#[cfg(target_family = "wasm")]
fn raw_syslog(_level: i32, _message: &[u8]) {}

#[cold]
#[inline(never)]
pub fn write_syslog(level: i32, line: &str) {
    let (seq, do_split) = {
        let mut state = SYSLOG_STATE.lock().expect("syslog state poisoned");

        if !state.openlog_done {
            open_syslog(&state.ident, state.facility);
            state.openlog_done = true;
        }

        state.seq += 1;
        (state.seq, crate::config::syslog_split_messages())
    };

    let bytes = line.as_bytes();
    let mut len = bytes.len();
    let mut pos = 0usize;
    let mut nlpos = memchr_newline(bytes, pos);

    if do_split && (len > PG_SYSLOG_LIMIT || nlpos.is_some()) {
        let mut chunk_nr = 0;

        while len > 0 {
            if bytes[pos] == b'\n' {
                pos += 1;
                len -= 1;
                nlpos = memchr_newline(bytes, pos);
                continue;
            }

            let mut buflen = match nlpos {
                Some(nl) => nl - pos,
                None => len,
            };
            buflen = buflen.min(PG_SYSLOG_LIMIT);

            // pg_mbcliplen: clip at a UTF-8 char boundary (owned strings).
            while buflen > 0 && !line.is_char_boundary(pos + buflen) {
                buflen -= 1;
            }
            if buflen == 0 {
                return;
            }

            if pos + buflen < bytes.len() && !c_isspace(bytes[pos + buflen]) {
                let mut i = buflen - 1;
                while i > 0 && !c_isspace(bytes[pos + i]) {
                    i -= 1;
                }
                if i > 0 {
                    buflen = i;
                }
            }

            chunk_nr += 1;

            let chunk = &bytes[pos..pos + buflen];
            let mut msg = if crate::config::syslog_sequence_numbers() {
                format!("[{}-{}] ", seq, chunk_nr).into_bytes()
            } else {
                format!("[{}] ", chunk_nr).into_bytes()
            };
            msg.extend_from_slice(chunk);
            raw_syslog(level, &msg);

            pos += buflen;
            len -= buflen;
        }
    } else {
        if crate::config::syslog_sequence_numbers() {
            let mut msg = format!("[{}] ", seq).into_bytes();
            msg.extend_from_slice(bytes);
            raw_syslog(level, &msg);
        } else {
            raw_syslog(level, bytes);
        }
    }
}

fn memchr_newline(bytes: &[u8], from: usize) -> Option<usize> {
    bytes[from..].iter().position(|&b| b == b'\n').map(|i| from + i)
}

fn c_isspace(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}
