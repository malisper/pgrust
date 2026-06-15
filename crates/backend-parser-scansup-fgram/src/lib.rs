//! Port of `src/backend/parser/scansup.c` (PostgreSQL 18.3) -- scanner
//! support routines used by the core lexer.
//!
//! Every scansup.c function is ported with its original C name and
//! logic/branch order/message text/SQLSTATE preserved 1:1:
//!
//!   * [`downcase_truncate_identifier`] / [`downcase_identifier`] -- ASCII +
//!     locale-aware (high-bit, single-byte encoding) downcasing, with optional
//!     `NAMEDATALEN`-based truncation.  Allocates the result through
//!     `MemoryContextScope`/`palloc` exactly as the C `palloc(len + 1)`.
//!   * [`truncate_identifier`] -- in-place truncation to `NAMEDATALEN-1` bytes
//!     with the `NOTICE`/`ERRCODE_NAME_TOO_LONG` warning text.
//!   * [`scanner_isspace`] -- the flex `{space}` predicate.
//!
//! Cross-subsystem calls -- `pg_database_encoding_max_length` and
//! `pg_mbcliplen` (the multibyte subsystem) -- are reused directly from
//! `backend-utils-mb`; `tolower`/`isupper` for high-bit bytes go through
//! `libc` exactly as C's `<ctype.h>`.
//!
//! No `extern "C"`; soft errors flow through `backend-utils-error`.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

use backend_utils_error::{ereport, ErrorLocation, PgResult, ERRCODE_NAME_TOO_LONG, NOTICE};
use backend_utils_mb::{pg_database_encoding_max_length, pg_mbcliplen};
use backend_utils_mmgr::{palloc, MemoryContextScope};
use core::ffi::c_int;
use pgrust_pg_ffi::NAMEDATALEN;

/// `IS_HIGHBIT_SET(ch)` (c.h): true when the high bit of the byte is set.
#[inline]
fn is_highbit_set(ch: u8) -> bool {
    ch & 0x80 != 0
}

/// `downcase_truncate_identifier()` (scansup.c:36) --- do appropriate
/// downcasing and truncation of an unquoted identifier.  Optionally warn of
/// truncation.  Returns a palloc'd string containing the adjusted identifier.
pub fn downcase_truncate_identifier<'ctx>(
    scope: &MemoryContextScope<'ctx>,
    ident: &[u8],
    len: c_int,
    warn: bool,
) -> PgResult<PgIdentifier<'ctx>> {
    downcase_identifier(scope, ident, len, warn, true)
}

/// `downcase_identifier()` (scansup.c:45) --- a workhorse for
/// `downcase_truncate_identifier`.
///
/// SQL99 specifies Unicode-aware case normalization, which we don't yet have
/// the infrastructure for.  Instead we use `tolower()` for characters with the
/// high bit set, as long as they aren't part of a multi-byte character, and an
/// ASCII-only downcasing for 7-bit characters.
pub fn downcase_identifier<'ctx>(
    scope: &MemoryContextScope<'ctx>,
    ident: &[u8],
    len: c_int,
    warn: bool,
    truncate: bool,
) -> PgResult<PgIdentifier<'ctx>> {
    let len = len as usize;
    let mut result = palloc(scope, (len + 1) as pgrust_pg_ffi::Size)?;
    let enc_is_single_byte = pg_database_encoding_max_length() == 1;

    let buf = result.as_mut_slice();
    for i in 0..len {
        let mut ch: u8 = ident[i];

        if ch.is_ascii_uppercase() {
            ch += b'a' - b'A';
        } else if enc_is_single_byte
            && is_highbit_set(ch)
            && (unsafe { libc::isupper(ch as c_int) } != 0)
        {
            ch = unsafe { libc::tolower(ch as c_int) } as u8;
        }
        buf[i] = ch;
    }
    buf[len] = b'\0';

    let mut ident_out = PgIdentifier {
        memory: result,
        len,
    };
    if len >= NAMEDATALEN as usize && truncate {
        truncate_identifier(ident_out.as_mut_slice_with_nul(), len as c_int, warn)?;
        // truncate_identifier may have shortened the logical length; recompute
        // from the NUL terminator it wrote, mirroring the in-place C contract.
        ident_out.len = ident_out
            .as_slice_with_nul()
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(len);
    }
    Ok(ident_out)
}

/// `truncate_identifier()` (scansup.c:92) --- truncate an identifier to
/// `NAMEDATALEN-1` bytes.  The given string is modified in-place, if
/// necessary.  A `NOTICE` is issued if requested.
///
/// `ident` is the NUL-terminated buffer; `len` is the byte length (excluding
/// the terminator), passed in to save a `strlen()` call as in C.
pub fn truncate_identifier(ident: &mut [u8], len: c_int, warn: bool) -> PgResult<()> {
    if len >= NAMEDATALEN {
        let new_len = pg_mbcliplen(ident, len, NAMEDATALEN - 1)?;
        if warn {
            let full = cstr_lossy(ident, len as usize);
            let clipped = cstr_lossy(ident, new_len as usize);
            ereport(NOTICE)
                .errcode(ERRCODE_NAME_TOO_LONG)
                .errmsg(format!(
                    "identifier \"{full}\" will be truncated to \"{clipped}\""
                ))
                .finish(ErrorLocation::new(
                    "../src/backend/parser/scansup.c",
                    102,
                    "truncate_identifier",
                ))?;
        }
        ident[new_len as usize] = b'\0';
    }
    Ok(())
}

/// `scanner_isspace()` (scansup.c:116) --- return true if the flex scanner
/// considers `ch` whitespace.  This must match scan.l's list of `{space}`
/// characters.
pub fn scanner_isspace(ch: u8) -> bool {
    matches!(
        ch,
        b' ' | b'\t' | b'\n' | b'\r' | 0x0b /* \v */ | 0x0c /* \f */
    )
}

/// Render the first `len` bytes of `ident` as a lossy UTF-8 string for the
/// truncation NOTICE message text.
fn cstr_lossy(ident: &[u8], len: usize) -> String {
    String::from_utf8_lossy(&ident[..len.min(ident.len())]).into_owned()
}

/// A downcased/truncated identifier, owning the `palloc(len + 1)` NUL-terminated
/// buffer the C routines return.  `len` is the logical byte length (excluding
/// the NUL terminator).
pub struct PgIdentifier<'ctx> {
    memory: backend_utils_mmgr::PgMemory<'ctx>,
    len: usize,
}

impl<'ctx> PgIdentifier<'ctx> {
    /// The identifier bytes without the trailing NUL.
    pub fn as_bytes(&self) -> &[u8] {
        &self.memory.as_slice()[..self.len]
    }

    /// The identifier as a lossy UTF-8 string view.
    pub fn to_string_lossy(&self) -> String {
        String::from_utf8_lossy(self.as_bytes()).into_owned()
    }

    /// The full buffer including the trailing NUL.
    fn as_slice_with_nul(&self) -> &[u8] {
        &self.memory.as_slice()[..=self.len]
    }

    /// The full buffer including the trailing NUL, mutable (for in-place
    /// truncation as the C code performs).
    fn as_mut_slice_with_nul(&mut self) -> &mut [u8] {
        let total = self.len + 1;
        &mut self.memory.as_mut_slice()[..total]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use backend_utils_error::{set_report_sink, unpack_sqlstate, PgError};
    use backend_utils_mb::SetDatabaseEncoding;
    use backend_utils_mmgr::OwnedMemoryContext;
    use std::sync::Mutex;

    /// Serializes tests that touch process-global state (the report sink and the
    /// database encoding), mirroring `backend-utils-error`'s `REPORT_TEST_LOCK`.
    static GLOBAL_STATE_LOCK: Mutex<()> = Mutex::new(());

    /// Captured reports for the active golden test.
    static CAPTURED: Mutex<Vec<PgError>> = Mutex::new(Vec::new());

    fn capture_report(error: &PgError) {
        CAPTURED.lock().unwrap().push(error.clone());
    }

    fn test_scope() -> OwnedMemoryContext {
        OwnedMemoryContext::alloc_set(None, "scansup-test", 1024, 8192, 8192).unwrap()
    }

    #[test]
    fn scanner_isspace_matches_scan_l() {
        // scan.l's {space} set: space, tab, NL, CR, VT (\v = 0x0b), FF (\f = 0x0c).
        for c in [b' ', b'\t', b'\n', b'\r', 0x0b, 0x0c] {
            assert!(scanner_isspace(c));
        }
        for c in [b'a', b'0', b'_', 0u8, 0x1f, 0x7f, 0x80, 0xff] {
            assert!(!scanner_isspace(c));
        }
    }

    #[test]
    fn downcase_ascii_only() {
        let _guard = GLOBAL_STATE_LOCK.lock().unwrap();
        let ctx = test_scope();
        let scope = ctx.scope();
        // 7-bit ASCII uppercase is lowercased; digits/underscore untouched.
        let input = b"FooBar_123XYZ";
        let id = downcase_truncate_identifier(&scope, input, input.len() as c_int, false).unwrap();
        assert_eq!(id.as_bytes(), b"foobar_123xyz");
    }

    #[test]
    fn downcase_no_truncation_under_namedatalen() {
        let _guard = GLOBAL_STATE_LOCK.lock().unwrap();
        let ctx = test_scope();
        let scope = ctx.scope();
        // NAMEDATALEN-1 = 63 bytes: at the limit, no truncation, no NOTICE.
        let input = vec![b'A'; (NAMEDATALEN - 1) as usize];
        let id = downcase_truncate_identifier(&scope, &input, input.len() as c_int, true).unwrap();
        assert_eq!(id.as_bytes(), &vec![b'a'; (NAMEDATALEN - 1) as usize][..]);
    }

    /// Golden test against PG 18.3 `src/test/regress/expected/create_view.out`
    /// lines 1894-1895: a 64-byte identifier is truncated to NAMEDATALEN-1 = 63
    /// bytes with the exact `42622` NOTICE text PostgreSQL emits.
    #[test]
    fn truncate_identifier_golden_notice_matches_pg_regress() {
        let _guard = GLOBAL_STATE_LOCK.lock().unwrap();
        SetDatabaseEncoding(pgrust_pg_ffi::PG_SQL_ASCII).unwrap();
        CAPTURED.lock().unwrap().clear();
        let previous = set_report_sink(Some(capture_report));

        // create_view.out:1894 is a 64-byte identifier (63 'x' + a trailing 'y')
        // truncated to 63 'x'. We reproduce that exact 64-byte / 63-byte split.
        let mut input = vec![b'x'; 63];
        input.push(b'y'); // total 64 bytes (>= NAMEDATALEN)
        let mut buf = input.clone();
        buf.push(b'\0');

        let res = truncate_identifier(&mut buf, input.len() as c_int, true);
        assert!(res.is_ok());

        // In-place truncation wrote the NUL at byte NAMEDATALEN-1 = 63.
        assert_eq!(buf[(NAMEDATALEN - 1) as usize], b'\0');
        assert_eq!(&buf[..(NAMEDATALEN - 1) as usize], &vec![b'x'; 63][..]);

        let captured = CAPTURED.lock().unwrap();
        assert_eq!(captured.len(), 1, "exactly one NOTICE emitted");
        let report = &captured[0];
        assert_eq!(report.level(), NOTICE);
        assert_eq!(unpack_sqlstate(report.sqlstate()), *b"42622");
        let clipped = "x".repeat(63);
        let full = format!("{clipped}y");
        assert_eq!(
            report.message(),
            format!("identifier \"{full}\" will be truncated to \"{clipped}\"")
        );
        drop(captured);

        set_report_sink(previous);
    }

    #[test]
    fn truncate_identifier_no_warn_no_notice() {
        let _guard = GLOBAL_STATE_LOCK.lock().unwrap();
        SetDatabaseEncoding(pgrust_pg_ffi::PG_SQL_ASCII).unwrap();
        CAPTURED.lock().unwrap().clear();
        let previous = set_report_sink(Some(capture_report));

        let mut buf = vec![b'z'; 70];
        buf.push(b'\0');
        truncate_identifier(&mut buf, 70, false).unwrap();
        // Truncated in-place, but no NOTICE because warn = false.
        assert_eq!(buf[(NAMEDATALEN - 1) as usize], b'\0');
        assert!(CAPTURED.lock().unwrap().is_empty());

        set_report_sink(previous);
    }

    #[test]
    fn downcase_truncate_full_path_emits_notice() {
        let _guard = GLOBAL_STATE_LOCK.lock().unwrap();
        SetDatabaseEncoding(pgrust_pg_ffi::PG_SQL_ASCII).unwrap();
        CAPTURED.lock().unwrap().clear();
        let previous = set_report_sink(Some(capture_report));
        let ctx = test_scope();
        let scope = ctx.scope();

        // 80 'A' bytes: downcased to 'a' and truncated to 63 bytes with a NOTICE.
        let input = vec![b'A'; 80];
        let id = downcase_truncate_identifier(&scope, &input, input.len() as c_int, true).unwrap();
        assert_eq!(id.as_bytes().len(), (NAMEDATALEN - 1) as usize);
        assert_eq!(id.as_bytes(), &vec![b'a'; (NAMEDATALEN - 1) as usize][..]);

        let captured = CAPTURED.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].level(), NOTICE);
        assert_eq!(unpack_sqlstate(captured[0].sqlstate()), *b"42622");
        drop(captured);

        set_report_sink(previous);
    }
}
