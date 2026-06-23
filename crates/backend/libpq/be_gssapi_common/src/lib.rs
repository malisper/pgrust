//! Port of `src/backend/libpq/be-gssapi-common.c`.
//!
//! "Common code for GSSAPI authentication and encryption."
//!
//! The MIT/Heimdal GSSAPI library (`gssapi.h` / `libgssapi_krb5`) is a
//! genuinely-EXTERNAL C library and is NOT linked into this build. Every
//! `gss_*` call and the OS `setenv` are routed through
//! [`be_gssapi_common_seams`]; their slots are deliberately
//! left UNINSTALLED (no krb5 stack is present), so a call panics loudly —
//! mirror-PG-and-panic for an absent external dependency.
//!
//! This crate's OWN logic is ported faithfully:
//!   * [`pg_gss_error_int`] — the fixed-128-byte error-buffer assembly (space
//!     separator between fragments, `Min(len - i, length)` truncating copy,
//!     NUL termination, and the `COMMERROR` "incomplete GSS error report"
//!     overflow path). The running index `i` may exceed `len`, exactly as in
//!     the C.
//!   * [`pg_gss_error`] — two 128-byte buffers, then one `COMMERROR`
//!     `errmsg_internal` + `errdetail_internal` report.
//!   * [`pg_store_delegated_credential`] — `gss_store_cred_into` (implicit
//!     `{"ccache" -> "MEMORY:"}` store, `GSS_C_INITIATE`, overwrite +
//!     default), error-check, `gss_release_cred`, error-check, then
//!     `setenv("KRB5CCNAME", "MEMORY:", 1)`.
//!
//! Idiomatic vs the C: the opaque `gss_cred_id_t` handle is a plain `u64`
//! token; `OM_uint32` is `u32`. These functions return `void` in C and report
//! at `COMMERROR` (below `ERROR`, never longjmps), so the ports stay
//! `()`-returning and emit via the error subsystem (which yields `Ok(())` at
//! this elevel).
//!
//! Consumers (`auth.c` GSSAPI / `be-secure-gssapi.c`) are unported; once they
//! land they call [`pg_gss_error`] / [`pg_store_delegated_credential`]
//! directly. This crate's `init_seams()` installs nothing — it owns no inward
//! seams and the outward `gss_*` seams have no Rust producer.

use core::cmp::min;

use utils_error::ereport;
use be_gssapi_common_seams as gss_seam;
use types_error::{ErrorLocation, COMMERROR};

const FILENAME: &str = "be-gssapi-common.c";

/// `#define GSS_MEMORY_CACHE "MEMORY:"`
const GSS_MEMORY_CACHE: &str = "MEMORY:";

/// `GSS_S_COMPLETE` — operation completed successfully.
const GSS_S_COMPLETE: u32 = 0;

/// `GSS_C_GSS_CODE` — fetch GSS major status messages.
const GSS_C_GSS_CODE: i32 = 1;
/// `GSS_C_MECH_CODE` — fetch mechanism minor status messages.
const GSS_C_MECH_CODE: i32 = 2;

/// Capacity of each per-status error scratch buffer (the C `char msg[128]`).
const ERR_BUF_LEN: usize = 128;

/// Fetch all errors of a specific type and append into `s` (a fixed-size byte
/// buffer of length [`ERR_BUF_LEN`]). If more than one fragment is obtained,
/// separate them with spaces. Called once for `GSS_C_GSS_CODE` and once for
/// `GSS_C_MECH_CODE`.
///
/// Faithful to the C `pg_GSS_error_int`: the running write index `i` may
/// exceed `len`, each fragment is copied with `Min(len - i, length)`,
/// fragments are space-separated, and on overflow a `COMMERROR` "incomplete
/// GSS error report" is emitted with the buffer forcibly NUL-terminated at
/// `len - 1`.
fn pg_gss_error_int(s: &mut [u8; ERR_BUF_LEN], stat: u32, type_: i32) {
    let len = ERR_BUF_LEN;
    let mut i: usize = 0;
    let mut msg_ctx: u32 = 0;

    // do { ... } while (msg_ctx)
    loop {
        // if (gss_display_status(...) != GSS_S_COMPLETE) break;
        // The seam returns the next message context plus, on success, the
        // already-released message bytes (gmsg.value[..gmsg.length]); `None`
        // models a non-GSS_S_COMPLETE return.
        let (next_ctx, gmsg) = gss_seam::gss_display_status::call(stat, type_, msg_ctx);
        let gmsg = match gmsg {
            Some(bytes) => bytes,
            None => break,
        };
        msg_ctx = next_ctx;

        if i > 0 {
            if i < len {
                s[i] = b' ';
            }
            i += 1;
        }
        if i < len {
            // memcpy(s + i, gmsg.value, Min(len - i, gmsg.length));
            let n = min(len - i, gmsg.len());
            s[i..i + n].copy_from_slice(&gmsg[..n]);
        }
        i += gmsg.len();

        if msg_ctx == 0 {
            break;
        }
    }

    // add nul termination
    if i < len {
        s[i] = b'\0';
    } else {
        // elog(COMMERROR, "incomplete GSS error report");
        let _ = ereport(COMMERROR)
            .errmsg_internal("incomplete GSS error report")
            .finish(ErrorLocation::new(FILENAME, 0, "pg_GSS_error_int"));
        s[len - 1] = b'\0';
    }
}

/// Interpret a fixed-size NUL-terminated byte buffer as a `&str` for reporting,
/// matching the C `%s` printf semantics (stop at the first NUL).
fn cbuf_to_str(buf: &[u8]) -> &str {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    core::str::from_utf8(&buf[..end]).unwrap_or("")
}

/// Report the GSSAPI error described by `maj_stat`/`min_stat`.
///
/// `errmsg` should be an already-translated primary error message; the GSSAPI
/// info is appended as errdetail.
///
/// The error is always reported with elevel `COMMERROR`; we daren't try to
/// send it to the client, as that'd likely lead to infinite recursion when
/// elog.c tries to write to the client.
///
/// To avoid memory allocation, total error size is capped (at 128 bytes for
/// each of major and minor). No known mechanisms will produce error messages
/// beyond this cap.
pub fn pg_gss_error(errmsg: &str, maj_stat: u32, min_stat: u32) {
    let mut msg_major = [0u8; ERR_BUF_LEN];
    let mut msg_minor = [0u8; ERR_BUF_LEN];

    // Fetch major status message.
    pg_gss_error_int(&mut msg_major, maj_stat, GSS_C_GSS_CODE);

    // Fetch mechanism minor status message.
    pg_gss_error_int(&mut msg_minor, min_stat, GSS_C_MECH_CODE);

    // errmsg_internal, since translation of the first part must be done before
    // calling this function anyway.
    let _ = ereport(COMMERROR)
        .errmsg_internal(errmsg.to_string())
        .errdetail_internal(format!(
            "{}: {}",
            cbuf_to_str(&msg_major),
            cbuf_to_str(&msg_minor)
        ))
        .finish(ErrorLocation::new(FILENAME, 0, "pg_GSS_error"));
}

/// Store the credentials passed in into the memory cache for later usage.
///
/// This allows credentials to be delegated to us for us to use to connect to
/// other systems with, using, e.g. postgres_fdw or dblink.
///
/// `cred` is the opaque GSSAPI `gss_cred_id_t` handle (a `u64` token here).
pub fn pg_store_delegated_credential(cred: u64) {
    // Make the delegated credential only available to current process.
    //
    // The C builds a single-element `{"ccache" -> "MEMORY:"}` key/value store
    // and calls gss_store_cred_into(GSS_C_INITIATE, GSS_C_NULL_OID,
    // overwrite = true, default = true). That fixed shape is folded into the
    // seam.
    let (major, minor) = gss_seam::gss_store_cred_into::call(cred);
    if major != GSS_S_COMPLETE {
        pg_gss_error("gss_store_cred", major, minor);
    }

    // Credential stored, so we can release our credential handle.
    let (major, minor) = gss_seam::gss_release_cred::call(cred);
    if major != GSS_S_COMPLETE {
        pg_gss_error("gss_release_cred", major, minor);
    }

    // Set KRB5CCNAME for this backend, so that later calls to gss_acquire_cred
    // will find the delegated credentials we stored.
    let _ = gss_seam::setenv::call("KRB5CCNAME", GSS_MEMORY_CACHE, 1);
}

/// Install this crate's inward seams. It owns none (its public functions are
/// called directly by GSSAPI consumers once they land), and the outward
/// `gss_*` seams have no Rust producer (absent external krb5 library), so this
/// is intentionally empty — present for the uniform `seams-init` contract.
pub fn init_seams() {}

#[cfg(test)]
mod tests {
    use super::*;
    use be_gssapi_common_seams as s;
    use std::cell::RefCell;
    use std::sync::{Mutex, MutexGuard, Once};

    // The seams are process-global `OnceLock` slots: each may be installed at
    // most once for the whole test binary. So we install ONE dispatcher per
    // seam (in `setup`) that forwards to a per-thread closure the active test
    // swaps in. A process-wide mutex serializes the tests since the closures
    // live in thread-locals but the seams are shared. Recover from poisoning
    // so one panicking test does not cascade-fail the others.
    static SEAM_LOCK: Mutex<()> = Mutex::new(());

    type DisplayFn = Box<dyn Fn(u32, i32, u32) -> (u32, Option<Vec<u8>>) + Send>;
    type StatusFn = Box<dyn Fn(u64) -> (u32, u32) + Send>;
    type SetenvFn = Box<dyn Fn(&str, &str, i32) -> i32 + Send>;

    thread_local! {
        static DISPLAY: RefCell<Option<DisplayFn>> = const { RefCell::new(None) };
        static STORE: RefCell<Option<StatusFn>> = const { RefCell::new(None) };
        static RELEASE: RefCell<Option<StatusFn>> = const { RefCell::new(None) };
        static SETENV: RefCell<Option<SetenvFn>> = const { RefCell::new(None) };
    }

    static SETUP: Once = Once::new();
    fn setup() -> MutexGuard<'static, ()> {
        let guard = SEAM_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        SETUP.call_once(|| {
            // COMMERROR reports run through the error subsystem, which consults
            // the `log_min_messages` GUC. A benign value lets the report finish.
            utils_error::config::set_log_min_messages(types_error::WARNING);

            s::gss_display_status::set(|stat, type_, ctx| {
                DISPLAY.with(|f| (f.borrow().as_ref().expect("DISPLAY set"))(stat, type_, ctx))
            });
            s::gss_store_cred_into::set(|cred| {
                STORE.with(|f| (f.borrow().as_ref().expect("STORE set"))(cred))
            });
            s::gss_release_cred::set(|cred| {
                RELEASE.with(|f| (f.borrow().as_ref().expect("RELEASE set"))(cred))
            });
            s::setenv::set(|name, value, overwrite| {
                SETENV.with(|f| (f.borrow().as_ref().expect("SETENV set"))(name, value, overwrite))
            });
        });
        guard
    }

    fn set_display(f: DisplayFn) {
        DISPLAY.with(|c| *c.borrow_mut() = Some(f));
    }

    /// A single GSS message fragment, no continuation: NUL-terminated copy.
    #[test]
    fn error_int_single_fragment() {
        let _g = setup();
        set_display(Box::new(|_stat, _type, ctx| {
            if ctx == 0 {
                (0, Some(b"hello".to_vec()))
            } else {
                (0, None)
            }
        }));
        let mut buf = [0xFFu8; ERR_BUF_LEN];
        pg_gss_error_int(&mut buf, 7, GSS_C_GSS_CODE);
        assert_eq!(cbuf_to_str(&buf), "hello");
    }

    /// Two fragments (msg_ctx continuation) get a space separator.
    #[test]
    fn error_int_two_fragments_space_separated() {
        let _g = setup();
        set_display(Box::new(|_stat, _type, ctx| match ctx {
            0 => (1, Some(b"foo".to_vec())),
            _ => (0, Some(b"bar".to_vec())),
        }));
        let mut buf = [0u8; ERR_BUF_LEN];
        pg_gss_error_int(&mut buf, 1, GSS_C_GSS_CODE);
        assert_eq!(cbuf_to_str(&buf), "foo bar");
    }

    /// An oversized fragment is truncated to the buffer and NUL-terminated at
    /// `len - 1` (the C overflow path); the report is emitted via COMMERROR.
    #[test]
    fn error_int_overflow_truncates() {
        let _g = setup();
        set_display(Box::new(|_stat, _type, ctx| {
            if ctx == 0 {
                (0, Some(vec![b'x'; 200]))
            } else {
                (0, None)
            }
        }));
        let mut buf = [0u8; ERR_BUF_LEN];
        pg_gss_error_int(&mut buf, 1, GSS_C_MECH_CODE);
        // i reaches 200 (> 128) so the else branch NUL-terminates at len-1.
        assert_eq!(buf[ERR_BUF_LEN - 1], 0);
        assert_eq!(cbuf_to_str(&buf).len(), ERR_BUF_LEN - 1);
    }

    /// Empty status (display_status returns None immediately) yields "".
    #[test]
    fn error_int_empty() {
        let _g = setup();
        set_display(Box::new(|_stat, _type, _ctx| (0, None)));
        let mut buf = [0xAAu8; ERR_BUF_LEN];
        pg_gss_error_int(&mut buf, 0, GSS_C_GSS_CODE);
        assert_eq!(cbuf_to_str(&buf), "");
    }

    /// pg_store_delegated_credential: happy path drives store -> release ->
    /// setenv with the expected argument shape.
    #[test]
    fn store_delegated_credential_happy_path() {
        use std::sync::atomic::{AtomicU32, Ordering};
        static STORE_CALLS: AtomicU32 = AtomicU32::new(0);
        static RELEASE_CALLS: AtomicU32 = AtomicU32::new(0);
        static SETENV_CALLS: AtomicU32 = AtomicU32::new(0);

        let _g = setup();
        STORE.with(|c| {
            *c.borrow_mut() = Some(Box::new(|cred| {
                assert_eq!(cred, 0x1234);
                STORE_CALLS.fetch_add(1, Ordering::SeqCst);
                (GSS_S_COMPLETE, 0)
            }))
        });
        RELEASE.with(|c| {
            *c.borrow_mut() = Some(Box::new(|cred| {
                assert_eq!(cred, 0x1234);
                RELEASE_CALLS.fetch_add(1, Ordering::SeqCst);
                (GSS_S_COMPLETE, 0)
            }))
        });
        SETENV.with(|c| {
            *c.borrow_mut() = Some(Box::new(|name, value, overwrite| {
                assert_eq!(name, "KRB5CCNAME");
                assert_eq!(value, "MEMORY:");
                assert_eq!(overwrite, 1);
                SETENV_CALLS.fetch_add(1, Ordering::SeqCst);
                0
            }))
        });

        pg_store_delegated_credential(0x1234);
        assert_eq!(STORE_CALLS.load(Ordering::SeqCst), 1);
        assert_eq!(RELEASE_CALLS.load(Ordering::SeqCst), 1);
        assert_eq!(SETENV_CALLS.load(Ordering::SeqCst), 1);
    }
}
