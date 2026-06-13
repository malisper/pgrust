//! `write_syslog` and the syslog connection state (HAVE_SYSLOG branch of
//! elog.c).
//!
//! Sanctioned divergence from the per-backend thread_local rule: libc's
//! openlog/syslog connection is genuinely process-global (one connection, one
//! retained ident pointer per process), so [`SYSLOG_STATE`] — including the
//! `syslog_ident`/`syslog_facility` values bound into that connection and the
//! message sequence number — is one shared, Mutex-guarded static. Under a
//! threaded server a backend's SET of those GUCs reopens the shared
//! connection for every thread; that is libc's constraint, not a porting
//! shortcut.

use std::ffi::CString;
use std::sync::Mutex;

/// Max string length to send to syslog() — leaves room for the
/// sequence-number prefix and syslog's own prefix under the common 1024-byte
/// implementation limits.
pub const PG_SYSLOG_LIMIT: usize = 900;

struct SyslogState {
    openlog_done: bool,
    /// Owned ident storage; openlog(3) retains the pointer, so the CString
    /// must stay alive while the connection is open (the C strdup).
    ident: Option<CString>,
    facility: i32,
    /// `static unsigned long seq` in write_syslog.
    seq: u64,
}

static SYSLOG_STATE: Mutex<SyslogState> = Mutex::new(SyslogState {
    openlog_done: false,
    ident: None,
    facility: libc::LOG_LOCAL0,
    seq: 0,
});

/// GUC assign_hook body for `syslog_ident`: don't thrash the connection if
/// unchanged; otherwise close it so the next write reopens with the new ident.
pub(crate) fn assign_syslog_ident(newval: &str) {
    let mut state = SYSLOG_STATE.lock().expect("syslog state poisoned");
    let changed = state
        .ident
        .as_ref()
        .map_or(true, |old| old.as_bytes() != newval.as_bytes());
    if changed {
        if state.openlog_done {
            unsafe { libc::closelog() };
            state.openlog_done = false;
        }
        state.ident = CString::new(newval).ok();
    }
}

/// GUC assign_hook body for `syslog_facility`.
pub(crate) fn assign_syslog_facility(newval: i32) {
    let mut state = SYSLOG_STATE.lock().expect("syslog state poisoned");
    if state.facility != newval {
        if state.openlog_done {
            unsafe { libc::closelog() };
            state.openlog_done = false;
        }
        state.facility = newval;
    }
}

fn raw_syslog(level: i32, message: &[u8]) {
    // Interior NULs cannot occur in text built from Rust strings, but guard.
    let Ok(cmsg) = CString::new(message) else {
        return;
    };
    unsafe {
        libc::syslog(level, c"%s".as_ptr(), cmsg.as_ptr());
    }
}

/// `write_syslog` — write one message line to syslog, splitting long messages
/// and messages with embedded newlines into multiple syslog() calls (many
/// syslog implementations mishandle long messages).
pub fn write_syslog(level: i32, line: &str) {
    let (seq, do_split) = {
        let mut state = SYSLOG_STATE.lock().expect("syslog state poisoned");

        // Open syslog connection if not done yet
        if !state.openlog_done {
            let ident_ptr = state
                .ident
                .as_ref()
                .map_or(c"postgres".as_ptr(), |i| i.as_ptr());
            unsafe {
                libc::openlog(
                    ident_ptr,
                    libc::LOG_PID | libc::LOG_NDELAY | libc::LOG_NOWAIT,
                    state.facility,
                );
            }
            state.openlog_done = true;
        }

        // A sequence number on each log message suppresses "same" messages.
        state.seq += 1;
        (state.seq, crate::config::syslog_split_messages())
    };

    let bytes = line.as_bytes();
    let mut len = bytes.len();
    let mut pos = 0usize;
    let mut nlpos = memchr_newline(bytes, pos);

    // Divide into multiple syslog() calls if the message is too long or
    // contains embedded newline(s).
    if do_split && (len > PG_SYSLOG_LIMIT || nlpos.is_some()) {
        let mut chunk_nr = 0;

        while len > 0 {
            // if we start at a newline, move ahead one char
            if bytes[pos] == b'\n' {
                pos += 1;
                len -= 1;
                // we need to recompute the next newline's position, too
                nlpos = memchr_newline(bytes, pos);
                continue;
            }

            // copy one line, or as much as will fit
            let mut buflen = match nlpos {
                Some(nl) => nl - pos,
                None => len,
            };
            buflen = buflen.min(PG_SYSLOG_LIMIT);

            // trim to multibyte letter boundary (the pg_mbcliplen call; the
            // owned strings are UTF-8, so clip at a UTF-8 char boundary)
            while buflen > 0 && !line.is_char_boundary(pos + buflen) {
                buflen -= 1;
            }
            if buflen == 0 {
                return;
            }

            // already word boundary?
            if pos + buflen < bytes.len() && !c_isspace(bytes[pos + buflen]) {
                // try to divide at word boundary
                let mut i = buflen - 1;
                while i > 0 && !c_isspace(bytes[pos + i]) {
                    i -= 1;
                }
                if i > 0 {
                    // else couldn't divide word boundary
                    buflen = i;
                }
            }

            chunk_nr += 1;

            let chunk = &bytes[pos..pos + buflen];
            if crate::config::syslog_sequence_numbers() {
                let mut msg = format!("[{}-{}] ", seq, chunk_nr).into_bytes();
                msg.extend_from_slice(chunk);
                raw_syslog(level, &msg);
            } else {
                let mut msg = format!("[{}] ", chunk_nr).into_bytes();
                msg.extend_from_slice(chunk);
                raw_syslog(level, &msg);
            }

            pos += buflen;
            len -= buflen;
            // nlpos stays valid as an absolute offset (the C keeps an
            // absolute pointer); buflen never passes the newline, and landing
            // exactly on it routes through the newline-skip branch above.
        }
    } else {
        // message short enough
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

/// C-locale `isspace`: space, \t, \n, \v, \f, \r (Rust's
/// `is_ascii_whitespace` omits vertical tab).
fn c_isspace(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}
