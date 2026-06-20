//! Idiomatic port of PostgreSQL's `src/backend/utils/mb/mbutils.c`: the
//! per-backend client/database/message encoding state and the string
//! encoding-conversion entry points.
//!
//! The three encoding selectors (`ClientEncoding`/`DatabaseEncoding`/
//! `MessageEncoding`) and the conversion-procedure cache (`ConvProcList`,
//! `ToServerConvProc`, `ToClientConvProc`, `Utf8ToServerConvProc`) are
//! per-backend C globals; they become `thread_local` here. C caches the
//! resolved `FmgrInfo` of each conversion proc; because an `FmgrInfo` is bound
//! to a `CurrentMemoryContext` and cannot live in a lifetime-free backend cache,
//! we cache the conversion-proc *OIDs* instead and re-dispatch by OID through
//! the fmgr-owned `convert_via_proc` seam. This is behavior-preserving: the C
//! cache exists to avoid catalog access during transaction rollback, and a
//! by-OID dispatch of a registered conversion function does not touch the
//! catalog either.
//!
//! The conversion functions share the C API quirk that, when no conversion is
//! required, the *source* string is returned unchanged. Pointer identity does
//! not cross a function boundary in safe Rust, so that outcome is modelled as
//! `Ok(None)` ("the caller's bytes stand"); a performed conversion returns
//! `Ok(Some(bytes))`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

use backend_utils_error::{ereport, PgResult};
use mcx::{slice_in, vec_with_capacity_in, Mcx, MemoryContext, PgVec};
use types_core::{Oid, OidIsValid, InvalidOid};
use types_error::{
    ERRCODE_CHARACTER_NOT_IN_REPERTOIRE, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_FUNCTION,
    ERRCODE_UNTRANSLATABLE_CHARACTER, ERROR,
};
use types_wchar::encoding::{
    pg_enc, pg_valid_be_encoding, pg_valid_encoding, pg_valid_fe_encoding,
    PG_ENCODING_IS_CLIENT_ONLY, PG_SQL_ASCII, PG_UTF8,
};
use types_wchar::wchar::PgWChar;

use common_wchar::{
    pg_encoding_max_length, pg_encoding_mblen_or_incomplete, pg_wchar_table,
};

use types_tuple::heaptuple::NameData;

// Outward seams.
use backend_access_transam_xact_seams::is_transaction_state;
use backend_catalog_namespace::FindDefaultConversionProc;
use backend_utils_adt_name::namein;
use backend_utils_fmgr_fmgr_seams::{convert_via_proc, convert_via_proc_counted};
use common_encnames_seams::{pg_char_to_encoding, pg_encoding_to_char};

mod fmgr_builtins;
mod tests;

// ---------------------------------------------------------------------------
// Constants (mb/pg_wchar.h, memutils.h).
// ---------------------------------------------------------------------------

/// `MAX_CONVERSION_GROWTH` (mb/pg_wchar.h): worst-case output-byte expansion of
/// any encoding conversion, per input byte.
const MAX_CONVERSION_GROWTH: usize = 4;

/// `MAX_MULTIBYTE_CHAR_LEN` (mb/pg_wchar.h).
const MAX_MULTIBYTE_CHAR_LEN: usize = 4;

/// `MaxAllocSize` (memutils.h).
const MAX_ALLOC_SIZE: usize = 0x3FFF_FFFF;

/// `MaxAllocHugeSize` (memutils.h): `SIZE_MAX / 2`.
const MAX_ALLOC_HUGE_SIZE: usize = usize::MAX / 2;

// ---------------------------------------------------------------------------
// Per-backend encoding state (the C file-scope statics).
// ---------------------------------------------------------------------------

static CLIENT_ENCODING: AtomicI32 = AtomicI32::new(PG_SQL_ASCII);
static DATABASE_ENCODING: AtomicI32 = AtomicI32::new(PG_SQL_ASCII);
static MESSAGE_ENCODING: AtomicI32 = AtomicI32::new(PG_SQL_ASCII);

/// `backend_startup_complete` / `pending_client_encoding`.
static BACKEND_STARTUP_COMPLETE: AtomicBool = AtomicBool::new(false);
static PENDING_CLIENT_ENCODING: AtomicI32 = AtomicI32::new(PG_SQL_ASCII);

/// A cached conversion-procedure pair, keyed by the server/client encoding ids
/// (C `ConvProcInfo`). C caches the resolved `FmgrInfo`; we cache the proc OIDs
/// and re-dispatch by OID (see the module docstring).
#[derive(Clone, Copy)]
struct ConvProcInfo {
    s_encoding: pg_enc,
    c_encoding: pg_enc,
    to_server_proc: Oid,
    to_client_proc: Oid,
}

thread_local! {
    /// C `ConvProcList` — newest entry at the head.
    static CONV_PROC_LIST: RefCell<Vec<ConvProcInfo>> = const { RefCell::new(Vec::new()) };

    /// The active to-server / to-client conversion proc OIDs (C
    /// `ToServerConvProc` / `ToClientConvProc`; `InvalidOid` is C's NULL = "no
    /// conversion needed").
    static TO_SERVER_CONV_PROC: RefCell<Oid> = const { RefCell::new(InvalidOid) };
    static TO_CLIENT_CONV_PROC: RefCell<Oid> = const { RefCell::new(InvalidOid) };

    /// C `Utf8ToServerConvProc` — UTF-8 to server-encoding conversion proc OID.
    static UTF8_TO_SERVER_CONV_PROC: RefCell<Oid> = const { RefCell::new(InvalidOid) };
}

#[inline]
fn database_encoding() -> pg_enc {
    DATABASE_ENCODING.load(Ordering::Relaxed)
}

#[inline]
fn client_encoding() -> pg_enc {
    CLIENT_ENCODING.load(Ordering::Relaxed)
}

/// Name of `encoding` for an error message (C reads `pg_enc2name_tbl[enc].name`).
fn enc_name(encoding: pg_enc) -> &'static str {
    pg_encoding_to_char::call(encoding)
}

// ---------------------------------------------------------------------------
// Client-encoding setup (PrepareClientEncoding / SetClientEncoding /
// InitializeClientEncoding).
// ---------------------------------------------------------------------------

/// `PrepareClientEncoding(encoding)`. Returns `Ok(0)` on success, `Ok(-1)` on a
/// bad encoding or missing conversion; `Err` carries the catalog-lookup
/// `ereport(ERROR)`s of `FindDefaultConversionProc`.
pub fn PrepareClientEncoding(mcx: Mcx<'_>, encoding: pg_enc) -> PgResult<i32> {
    if !pg_valid_fe_encoding(encoding) {
        return Ok(-1);
    }

    if !BACKEND_STARTUP_COMPLETE.load(Ordering::Relaxed) {
        return Ok(0);
    }

    let current_server_encoding = database_encoding();

    if current_server_encoding == encoding
        || current_server_encoding == PG_SQL_ASCII
        || encoding == PG_SQL_ASCII
    {
        return Ok(0);
    }

    if is_transaction_state::call() {
        let to_server_proc = FindDefaultConversionProc(mcx, encoding, current_server_encoding)?;
        if !OidIsValid(to_server_proc) {
            return Ok(-1);
        }
        let to_client_proc = FindDefaultConversionProc(mcx, current_server_encoding, encoding)?;
        if !OidIsValid(to_client_proc) {
            return Ok(-1);
        }

        let convinfo = ConvProcInfo {
            s_encoding: current_server_encoding,
            c_encoding: encoding,
            to_server_proc,
            to_client_proc,
        };

        CONV_PROC_LIST.with(|l| l.borrow_mut().insert(0, convinfo));

        // Older entries for the same pair are cleaned up by SetClientEncoding.
        Ok(0)
    } else {
        // Outside a live transaction we can only restore a cached setting.
        let found = CONV_PROC_LIST.with(|l| {
            l.borrow().iter().any(|info| {
                info.s_encoding == current_server_encoding && info.c_encoding == encoding
            })
        });
        if found {
            Ok(0)
        } else {
            Ok(-1)
        }
    }
}

/// `SetClientEncoding(encoding)`. Returns `Ok(0)`/`Ok(-1)` like C.
pub fn SetClientEncoding(encoding: pg_enc) -> PgResult<i32> {
    if !pg_valid_fe_encoding(encoding) {
        return Ok(-1);
    }

    if !BACKEND_STARTUP_COMPLETE.load(Ordering::Relaxed) {
        PENDING_CLIENT_ENCODING.store(encoding, Ordering::Relaxed);
        return Ok(0);
    }

    let current_server_encoding = database_encoding();

    if current_server_encoding == encoding
        || current_server_encoding == PG_SQL_ASCII
        || encoding == PG_SQL_ASCII
    {
        CLIENT_ENCODING.store(encoding, Ordering::Relaxed);
        TO_SERVER_CONV_PROC.with(|p| *p.borrow_mut() = InvalidOid);
        TO_CLIENT_CONV_PROC.with(|p| *p.borrow_mut() = InvalidOid);
        return Ok(0);
    }

    // Search the cache for the entry PrepareClientEncoding prepared; release any
    // duplicate entries so repeated Prepare/Set cycles don't leak.
    let mut found = false;
    CONV_PROC_LIST.with(|l| {
        let mut list = l.borrow_mut();
        let mut i = 0;
        while i < list.len() {
            let info = list[i];
            if info.s_encoding == current_server_encoding && info.c_encoding == encoding {
                if !found {
                    CLIENT_ENCODING.store(encoding, Ordering::Relaxed);
                    TO_SERVER_CONV_PROC.with(|p| *p.borrow_mut() = info.to_server_proc);
                    TO_CLIENT_CONV_PROC.with(|p| *p.borrow_mut() = info.to_client_proc);
                    found = true;
                    i += 1;
                } else {
                    list.remove(i);
                }
            } else {
                i += 1;
            }
        }
    });

    if found {
        Ok(0)
    } else {
        Ok(-1)
    }
}

/// `InitializeClientEncoding()` — called once during backend startup.
pub fn InitializeClientEncoding(mcx: Mcx<'_>) -> PgResult<()> {
    debug_assert!(!BACKEND_STARTUP_COMPLETE.load(Ordering::Relaxed));
    BACKEND_STARTUP_COMPLETE.store(true, Ordering::Relaxed);

    let pending = PENDING_CLIENT_ENCODING.load(Ordering::Relaxed);
    if PrepareClientEncoding(mcx, pending)? < 0 || SetClientEncoding(pending)? < 0 {
        // The requested conversion is unavailable; we couldn't fail earlier.
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "conversion between {} and {} is not supported",
                enc_name(pending),
                GetDatabaseEncodingName()
            ))
            .into_error());
    }

    // Look up the UTF8-to-server conversion function if needed.
    let current_server_encoding = database_encoding();
    if current_server_encoding != PG_UTF8 && current_server_encoding != PG_SQL_ASCII {
        let utf8_to_server_proc =
            FindDefaultConversionProc(mcx, PG_UTF8, current_server_encoding)?;
        if OidIsValid(utf8_to_server_proc) {
            UTF8_TO_SERVER_CONV_PROC.with(|p| *p.borrow_mut() = utf8_to_server_proc);
        }
    }
    Ok(())
}

/// `pg_get_client_encoding()`.
pub fn pg_get_client_encoding() -> pg_enc {
    client_encoding()
}

/// `pg_get_client_encoding_name()`.
pub fn pg_get_client_encoding_name() -> &'static str {
    enc_name(client_encoding())
}

// ---------------------------------------------------------------------------
// Encoding conversion (general case).
// ---------------------------------------------------------------------------

/// `pg_do_encoding_conversion(src, len, src_encoding, dest_encoding)`.
/// `Ok(None)` means no conversion was performed (C returns the source pointer);
/// `Ok(Some(v))` carries the converted bytes allocated in `mcx`.
pub fn pg_do_encoding_conversion<'mcx>(
    mcx: Mcx<'mcx>,
    src: &[u8],
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    if src.is_empty() {
        return Ok(None); // empty string is always valid
    }

    if src_encoding == dest_encoding {
        return Ok(None); // no conversion required, assume valid
    }

    if dest_encoding == PG_SQL_ASCII {
        return Ok(None); // any string is valid in SQL_ASCII
    }

    if src_encoding == PG_SQL_ASCII {
        // No conversion is possible, but we must validate the result.
        pg_verify_mbstr(dest_encoding, src, false)?;
        return Ok(None);
    }

    if !is_transaction_state::call() {
        // shouldn't happen
        return elog_error("cannot perform encoding conversion outside a transaction");
    }

    let proc = FindDefaultConversionProc(mcx, src_encoding, dest_encoding)?;
    if !OidIsValid(proc) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "default conversion function for encoding \"{}\" to \"{}\" does not exist",
                enc_name(src_encoding),
                enc_name(dest_encoding)
            ))
            .into_error());
    }

    // Guard against integer overflow in the output-buffer sizing.
    if src.len() >= MAX_ALLOC_HUGE_SIZE / MAX_CONVERSION_GROWTH {
        return Err(too_long_error(src.len()));
    }

    let result = convert_via_proc::call(mcx, proc, src_encoding, dest_encoding, src, false)?;

    if src.len() > 1_000_000 && result.len() >= MAX_ALLOC_SIZE {
        return Err(too_long_error(src.len()));
    }

    Ok(Some(result))
}

/// `pg_do_encoding_conversion_buf(proc, src_encoding, dest_encoding, src,
/// srclen, dest, destlen, noError)` — convert into a caller buffer of capacity
/// `destlen`, limiting the input so the worst-case output fits. The caller has
/// already looked up `proc` via `FindDefaultConversionProc`. Returns the byte
/// count written (C's `DatumGetInt32` of the conversion result), with the
/// converted bytes filled into `dest`.
pub fn pg_do_encoding_conversion_buf(
    mcx: Mcx<'_>,
    proc: Oid,
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    dest: &mut [u8],
    no_error: bool,
) -> PgResult<i32> {
    let destlen = dest.len();
    // If the destination buffer is not large enough for the worst case, limit
    // the input size passed to the conversion function. C: `(destlen - 1) /
    // MAX_CONVERSION_GROWTH` (destlen is a buffer size, always >= 1).
    let cap = destlen.saturating_sub(1) / MAX_CONVERSION_GROWTH;
    let srclen = src.len().min(cap);

    let (result, converted) = convert_via_proc_counted::call(
        mcx,
        proc,
        src_encoding,
        dest_encoding,
        &src[..srclen],
        no_error,
    )?;
    // The conversion function writes its NUL-terminated output into `dest`.
    let n = converted.len().min(destlen.saturating_sub(1));
    dest[..n].copy_from_slice(&converted[..n]);
    if n < destlen {
        dest[n] = 0;
    }
    Ok(result)
}

/// `pg_client_to_server(s, len)`.
pub fn pg_client_to_server<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    pg_any_to_server(mcx, s, client_encoding())
}

/// `pg_any_to_server(s, len, encoding)`. Always validates, even when no
/// conversion is needed (input comes from outside the database).
pub fn pg_any_to_server<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
    encoding: pg_enc,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let db_encoding = database_encoding();

    if s.is_empty() {
        return Ok(None);
    }

    if encoding == db_encoding || encoding == PG_SQL_ASCII {
        // No conversion needed, but validate.
        pg_verify_mbstr(db_encoding, s, false)?;
        return Ok(None);
    }

    if db_encoding == PG_SQL_ASCII {
        // No conversion possible; validate per the client encoding, or reject
        // non-ASCII for an ASCII-unsafe client encoding.
        if pg_valid_be_encoding(encoding) {
            pg_verify_mbstr(encoding, s, false)?;
        } else {
            for &b in s {
                if b == 0 || (b & 0x80) != 0 {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE)
                        .errmsg(format!(
                            "invalid byte value for encoding \"{}\": 0x{:02x}",
                            enc_name(PG_SQL_ASCII),
                            b
                        ))
                        .into_error());
                }
            }
        }
        return Ok(None);
    }

    // Fast path using the cached conversion function.
    if encoding == client_encoding() {
        return perform_default_encoding_conversion(mcx, s, true);
    }

    // General case (will not work outside a transaction).
    pg_do_encoding_conversion(mcx, s, encoding, db_encoding)
}

/// `pg_server_to_client(s, len)`.
pub fn pg_server_to_client<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    pg_server_to_any(mcx, s, client_encoding())
}

/// `pg_server_to_any(s, len, encoding)`.
pub fn pg_server_to_any<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
    encoding: pg_enc,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let db_encoding = database_encoding();

    if s.is_empty() {
        return Ok(None);
    }

    if encoding == db_encoding || encoding == PG_SQL_ASCII {
        return Ok(None); // assume data is valid
    }

    if db_encoding == PG_SQL_ASCII {
        // No conversion possible, but validate the result.
        pg_verify_mbstr(encoding, s, false)?;
        return Ok(None);
    }

    // Fast path using the cached conversion function.
    if encoding == client_encoding() {
        return perform_default_encoding_conversion(mcx, s, false);
    }

    // General case (will not work outside a transaction).
    pg_do_encoding_conversion(mcx, s, db_encoding, encoding)
}

/// `perform_default_encoding_conversion(src, len, is_client_to_server)` — uses
/// the cached conversion proc. Safe outside a transaction.
fn perform_default_encoding_conversion<'mcx>(
    mcx: Mcx<'mcx>,
    src: &[u8],
    is_client_to_server: bool,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let (src_encoding, dest_encoding, proc) = if is_client_to_server {
        (
            client_encoding(),
            database_encoding(),
            TO_SERVER_CONV_PROC.with(|p| *p.borrow()),
        )
    } else {
        (
            database_encoding(),
            client_encoding(),
            TO_CLIENT_CONV_PROC.with(|p| *p.borrow()),
        )
    };

    if !OidIsValid(proc) {
        return Ok(None);
    }

    if src.len() >= MAX_ALLOC_HUGE_SIZE / MAX_CONVERSION_GROWTH {
        return Err(too_long_error(src.len()));
    }

    let result = convert_via_proc::call(mcx, proc, src_encoding, dest_encoding, src, false)?;

    if src.len() > 1_000_000 && result.len() >= MAX_ALLOC_SIZE {
        return Err(too_long_error(src.len()));
    }

    Ok(Some(result))
}

/// `pg_unicode_to_server(c, s)` — convert one Unicode code point to the server
/// encoding, returning the encoded bytes (no trailing NUL). Throws on failure.
pub fn pg_unicode_to_server<'mcx>(mcx: Mcx<'mcx>, c: PgWChar) -> PgResult<PgVec<'mcx, u8>> {
    if !is_valid_unicode_codepoint(c) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("invalid Unicode code point")
            .into_error());
    }

    // ASCII range: trivial.
    if c <= 0x7F {
        return slice_in(mcx, &[c as u8]);
    }

    let server_encoding = database_encoding();
    if server_encoding == PG_UTF8 {
        let mut buf = [0u8; MAX_MULTIBYTE_CHAR_LEN + 1];
        unicode_to_utf8(c, &mut buf);
        let n = pg_utf_mblen(&buf) as usize;
        return slice_in(mcx, &buf[..n]);
    }

    // All other cases need a conversion function.
    let proc = UTF8_TO_SERVER_CONV_PROC.with(|p| *p.borrow());
    if !OidIsValid(proc) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "conversion between {} and {} is not supported",
                enc_name(PG_UTF8),
                GetDatabaseEncodingName()
            ))
            .into_error());
    }

    let mut c_as_utf8 = [0u8; MAX_MULTIBYTE_CHAR_LEN + 1];
    unicode_to_utf8(c, &mut c_as_utf8);
    let c_as_utf8_len = pg_utf_mblen(&c_as_utf8) as usize;

    convert_via_proc::call(
        mcx,
        proc,
        PG_UTF8,
        server_encoding,
        &c_as_utf8[..c_as_utf8_len],
        false,
    )
}

/// `pg_unicode_to_server_noerror(c, s)` — like [`pg_unicode_to_server`] but
/// returns `Ok(None)` on conversion failure instead of throwing. `Ok(Some(v))`
/// carries the encoded bytes (no trailing NUL).
pub fn pg_unicode_to_server_noerror<'mcx>(
    mcx: Mcx<'mcx>,
    c: PgWChar,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    if !is_valid_unicode_codepoint(c) {
        return Ok(None);
    }

    if c <= 0x7F {
        return Ok(Some(slice_in(mcx, &[c as u8])?));
    }

    let server_encoding = database_encoding();
    if server_encoding == PG_UTF8 {
        let mut buf = [0u8; MAX_MULTIBYTE_CHAR_LEN + 1];
        unicode_to_utf8(c, &mut buf);
        let n = pg_utf_mblen(&buf) as usize;
        return Ok(Some(slice_in(mcx, &buf[..n])?));
    }

    let proc = UTF8_TO_SERVER_CONV_PROC.with(|p| *p.borrow());
    if !OidIsValid(proc) {
        return Ok(None);
    }

    let mut c_as_utf8 = [0u8; MAX_MULTIBYTE_CHAR_LEN + 1];
    unicode_to_utf8(c, &mut c_as_utf8);
    let c_as_utf8_len = pg_utf_mblen(&c_as_utf8) as usize;

    // noError = true: the proc returns the number of source bytes it converted.
    let (converted_len, bytes) = convert_via_proc_counted::call(
        mcx,
        proc,
        PG_UTF8,
        server_encoding,
        &c_as_utf8[..c_as_utf8_len],
        true,
    )?;

    // Conversion succeeded iff it consumed the whole input.
    if converted_len == c_as_utf8_len as i32 {
        Ok(Some(bytes))
    } else {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// SQL-callable conversion entry points (operate on byte payloads).
// ---------------------------------------------------------------------------

/// `pg_convert(string, src_encoding_name, dest_encoding_name)` — convert
/// `string` between two arbitrary encodings (named), returning the converted
/// bytea payload (`Ok(None)` when the source bytes stand unchanged).
pub fn pg_convert<'mcx>(
    mcx: Mcx<'mcx>,
    string: &[u8],
    src_encoding_name: &str,
    dest_encoding_name: &str,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let src_encoding = pg_char_to_encoding::call(src_encoding_name);
    let dest_encoding = pg_char_to_encoding::call(dest_encoding_name);

    if src_encoding < 0 {
        return Err(invalid_encoding_name_error("source", src_encoding_name));
    }
    if dest_encoding < 0 {
        return Err(invalid_encoding_name_error("destination", dest_encoding_name));
    }

    // Make sure the source string is valid.
    pg_verify_mbstr(src_encoding, string, false)?;
    pg_do_encoding_conversion(mcx, string, src_encoding, dest_encoding)
}

/// `pg_convert_to(string, dest_encoding_name)` — convert from the database
/// encoding to `dest_encoding_name`.
pub fn pg_convert_to<'mcx>(
    mcx: Mcx<'mcx>,
    string: &[u8],
    dest_encoding_name: &str,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    pg_convert(mcx, string, GetDatabaseEncodingName(), dest_encoding_name)
}

/// `pg_convert_from(string, src_encoding_name)` — convert from
/// `src_encoding_name` to the database encoding.
pub fn pg_convert_from<'mcx>(
    mcx: Mcx<'mcx>,
    string: &[u8],
    src_encoding_name: &str,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    pg_convert(mcx, string, src_encoding_name, GetDatabaseEncodingName())
}

/// `length_in_encoding(string, src_encoding_name)` — the character length of
/// `string` interpreted in the named source encoding; errors on invalid data.
pub fn length_in_encoding(string: &[u8], src_encoding_name: &str) -> PgResult<i32> {
    let src_encoding = pg_char_to_encoding::call(src_encoding_name);
    if src_encoding < 0 {
        return Err(invalid_encoding_name_error("", src_encoding_name));
    }
    pg_verify_mbstr_len(src_encoding, string, false)
}

/// `pg_encoding_max_length_sql(encoding)` — `Some(maxmblen)` for a valid
/// encoding, `None` (C SQL NULL) otherwise.
pub fn pg_encoding_max_length_sql(encoding: pg_enc) -> Option<i32> {
    if pg_valid_encoding(encoding) {
        Some(pg_wchar_table[encoding as usize].maxmblen)
    } else {
        None
    }
}

/// `getdatabaseencoding()` (SQL) — the database encoding name as a `name`.
pub fn getdatabaseencoding() -> PgResult<NameData> {
    namein(GetDatabaseEncodingName())
}

/// `pg_client_encoding()` (SQL) — the client encoding name as a `name`.
pub fn pg_client_encoding() -> PgResult<NameData> {
    namein(pg_get_client_encoding_name())
}

/// `PG_char_to_encoding(name)` (SQL) — encoding id for an encoding name.
pub fn PG_char_to_encoding(name: &NameData) -> i32 {
    pg_char_to_encoding::call(name_str(name))
}

/// `PG_encoding_to_char(encoding)` (SQL) — encoding name (as `name`) for an id.
pub fn PG_encoding_to_char(encoding: i32) -> PgResult<NameData> {
    namein(pg_encoding_to_char::call(encoding))
}

// ---------------------------------------------------------------------------
// wchar / mb length family (over pg_wchar_table for the database encoding).
// ---------------------------------------------------------------------------

/// `pg_mb2wchar(from)`.
pub fn pg_mb2wchar<'mcx>(mcx: Mcx<'mcx>, from: &[u8]) -> PgResult<PgVec<'mcx, PgWChar>> {
    let len = c_string_len(from);
    pg_encoding_mb2wchar_with_len(mcx, database_encoding(), &from[..len], len as i32)
}

/// `pg_mb2wchar_with_len(from, to, len)`.
pub fn pg_mb2wchar_with_len<'mcx>(
    mcx: Mcx<'mcx>,
    from: &[u8],
) -> PgResult<PgVec<'mcx, PgWChar>> {
    pg_encoding_mb2wchar_with_len(mcx, database_encoding(), from, from.len() as i32)
}

/// `pg_encoding_mb2wchar_with_len(encoding, from, to, len)`.
pub fn pg_encoding_mb2wchar_with_len<'mcx>(
    mcx: Mcx<'mcx>,
    encoding: pg_enc,
    from: &[u8],
    len: i32,
) -> PgResult<PgVec<'mcx, PgWChar>> {
    // C writes into a caller buffer sized (len + 1) wchars; the converter fills
    // up to len wchars plus a NUL terminator, then returns the wchar count.
    let mut to: PgVec<PgWChar> = vec_with_capacity_in(mcx, len as usize + 1)?;
    to.resize(len as usize + 1, 0);
    let n = (pg_wchar_table[encoding as usize].mb2wchar_with_len)(from, to.as_mut_slice(), len);
    to.truncate(n as usize);
    Ok(to)
}

/// `pg_wchar2mb(from)`.
pub fn pg_wchar2mb<'mcx>(mcx: Mcx<'mcx>, from: &[PgWChar]) -> PgResult<PgVec<'mcx, u8>> {
    let len = pg_wchar_strlen(from);
    pg_encoding_wchar2mb_with_len(mcx, database_encoding(), &from[..len], len as i32)
}

/// `pg_wchar2mb_with_len(from, to, len)`.
pub fn pg_wchar2mb_with_len<'mcx>(
    mcx: Mcx<'mcx>,
    from: &[PgWChar],
) -> PgResult<PgVec<'mcx, u8>> {
    pg_encoding_wchar2mb_with_len(mcx, database_encoding(), from, from.len() as i32)
}

/// `pg_encoding_wchar2mb_with_len(encoding, from, to, len)`.
pub fn pg_encoding_wchar2mb_with_len<'mcx>(
    mcx: Mcx<'mcx>,
    encoding: pg_enc,
    from: &[PgWChar],
    len: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    // Worst case: maxmblen bytes per wchar plus a NUL.
    let cap = (len as usize) * (pg_wchar_table[encoding as usize].maxmblen as usize) + 1;
    let mut to: PgVec<u8> = vec_with_capacity_in(mcx, cap)?;
    to.resize(cap, 0);
    let n = (pg_wchar_table[encoding as usize].wchar2mb_with_len)(from, to.as_mut_slice(), len);
    to.truncate(n as usize);
    Ok(to)
}

/// `pg_mblen_cstr(mbstr)` — byte length of the leading character of a
/// NUL-terminated string; errors if the sequence would hit the terminator.
pub fn pg_mblen_cstr(mbstr: &[u8]) -> PgResult<i32> {
    let length = (pg_wchar_table[database_encoding() as usize].mblen)(mbstr);
    // The .mblen functions return 1 for a terminator; some callers depend on it.
    for i in 1..(length as usize) {
        if mbstr.get(i).copied() == Some(0) {
            return report_invalid_encoding_db(mbstr, length, i as i32);
        }
    }
    Ok(length)
}

/// `pg_mblen_range(mbstr, end)` — leading-character byte length bounded by the
/// slice end; errors if the sequence would exceed the range.
pub fn pg_mblen_range(mbstr: &[u8]) -> PgResult<i32> {
    let length = (pg_wchar_table[database_encoding() as usize].mblen)(mbstr);
    debug_assert!(!mbstr.is_empty());
    if (length as usize) > mbstr.len() {
        return report_invalid_encoding_db(mbstr, length, mbstr.len() as i32);
    }
    Ok(length)
}

/// `pg_mblen_with_len(mbstr, limit)`.
pub fn pg_mblen_with_len(mbstr: &[u8], limit: i32) -> PgResult<i32> {
    let length = (pg_wchar_table[database_encoding() as usize].mblen)(mbstr);
    debug_assert!(limit >= 1);
    if length > limit {
        return report_invalid_encoding_db(mbstr, length, limit);
    }
    Ok(length)
}

/// `pg_mblen_unbounded(mbstr)` — leading-character byte length, no bounds check.
pub fn pg_mblen_unbounded(mbstr: &[u8]) -> i32 {
    (pg_wchar_table[database_encoding() as usize].mblen)(mbstr)
}

/// `pg_mblen(mbstr)` — historical name for `pg_mblen_unbounded`.
pub fn pg_mblen(mbstr: &[u8]) -> i32 {
    pg_mblen_unbounded(mbstr)
}

/// `pg_dsplen(mbstr)` — display width of the leading character.
pub fn pg_dsplen(mbstr: &[u8]) -> i32 {
    (pg_wchar_table[database_encoding() as usize].dsplen)(mbstr)
}

/// `pg_mbstrlen(mbstr)` — character length of a NUL-terminated string.
pub fn pg_mbstrlen(mbstr: &[u8]) -> PgResult<i32> {
    if pg_database_encoding_max_length() == 1 {
        return Ok(c_string_len(mbstr) as i32);
    }
    let mut len = 0;
    let mut pos = 0;
    while mbstr.get(pos).copied().unwrap_or(0) != 0 {
        pos += pg_mblen_cstr(&mbstr[pos..])? as usize;
        len += 1;
    }
    Ok(len)
}

/// `pg_mbstrlen_with_len(mbstr, limit)` — character length of the first `limit`
/// bytes (stops at the first NUL or `limit`).
pub fn pg_mbstrlen_with_len(mbstr: &[u8], limit: i32) -> PgResult<i32> {
    if pg_database_encoding_max_length() == 1 {
        return Ok(limit);
    }
    let mut len = 0;
    let mut limit = limit;
    let mut pos = 0;
    while limit > 0 && mbstr.get(pos).copied().unwrap_or(0) != 0 {
        let l = pg_mblen_with_len(&mbstr[pos..], limit)?;
        limit -= l;
        pos += l as usize;
        len += 1;
    }
    Ok(len)
}

/// `pg_mbcliplen(mbstr, len, limit)`.
pub fn pg_mbcliplen(mbstr: &[u8], len: i32, limit: i32) -> i32 {
    pg_encoding_mbcliplen(database_encoding(), mbstr, len, limit)
}

/// `pg_encoding_mbcliplen(encoding, mbstr, len, limit)` — byte length of the
/// longest prefix at most `limit` bytes without splitting a character.
pub fn pg_encoding_mbcliplen(encoding: pg_enc, mbstr: &[u8], len: i32, limit: i32) -> i32 {
    if pg_encoding_max_length(encoding) == 1 {
        return cliplen(mbstr, len, limit);
    }

    let mblen_fn = pg_wchar_table[encoding as usize].mblen;
    let mut clen = 0;
    let mut len = len;
    let mut pos = 0;
    while len > 0 && mbstr.get(pos).copied().unwrap_or(0) != 0 {
        let l = mblen_fn(&mbstr[pos..]);
        if (clen + l) > limit {
            break;
        }
        clen += l;
        if clen == limit {
            break;
        }
        len -= l;
        pos += l as usize;
    }
    clen
}

/// `pg_mbcharcliplen(mbstr, len, limit)` — like `pg_mbcliplen`, with `limit`
/// measured in characters.
pub fn pg_mbcharcliplen(mbstr: &[u8], len: i32, limit: i32) -> PgResult<i32> {
    if pg_database_encoding_max_length() == 1 {
        return Ok(cliplen(mbstr, len, limit));
    }
    let mut clen = 0;
    let mut nch = 0;
    let mut len = len;
    let mut pos = 0;
    while len > 0 && mbstr.get(pos).copied().unwrap_or(0) != 0 {
        let l = pg_mblen_with_len(&mbstr[pos..], len)?;
        nch += 1;
        if nch > limit {
            break;
        }
        clen += l;
        len -= l;
        pos += l as usize;
    }
    Ok(clen)
}

/// `cliplen(str, len, limit)` — mbcliplen for a single-byte encoding.
fn cliplen(s: &[u8], len: i32, limit: i32) -> i32 {
    let len = len.min(limit);
    let mut l = 0;
    while l < len && s.get(l as usize).copied().unwrap_or(0) != 0 {
        l += 1;
    }
    l
}

// ---------------------------------------------------------------------------
// Encoding-state setters / getters.
// ---------------------------------------------------------------------------

/// `SetDatabaseEncoding(encoding)`.
pub fn SetDatabaseEncoding(encoding: pg_enc) -> PgResult<()> {
    if !pg_valid_be_encoding(encoding) {
        return elog_error(&format!("invalid database encoding: {encoding}"));
    }
    DATABASE_ENCODING.store(encoding, Ordering::Relaxed);
    Ok(())
}

/// `SetMessageEncoding(encoding)`.
pub fn SetMessageEncoding(encoding: pg_enc) -> PgResult<()> {
    debug_assert!(pg_valid_encoding(encoding));
    MESSAGE_ENCODING.store(encoding, Ordering::Relaxed);
    Ok(())
}

/// `GetDatabaseEncoding()`.
pub fn GetDatabaseEncoding() -> pg_enc {
    database_encoding()
}

/// `GetDatabaseEncodingName()`.
pub fn GetDatabaseEncodingName() -> &'static str {
    enc_name(database_encoding())
}

/// `GetMessageEncoding()`.
pub fn GetMessageEncoding() -> pg_enc {
    MESSAGE_ENCODING.load(Ordering::Relaxed)
}

/// `pg_database_encoding_max_length()`.
pub fn pg_database_encoding_max_length() -> i32 {
    pg_wchar_table[database_encoding() as usize].maxmblen
}

// ---------------------------------------------------------------------------
// Character incrementers (make_greater_string support).
// ---------------------------------------------------------------------------

/// A character-incrementer function (C `mbcharacter_incrementer`).
pub type MbcharacterIncrementer = fn(&mut [u8]) -> bool;

/// `pg_database_encoding_character_incrementer()`.
pub fn pg_database_encoding_character_incrementer() -> MbcharacterIncrementer {
    match database_encoding() {
        PG_UTF8 => pg_utf8_increment,
        x if x == types_wchar::encoding::PG_EUC_JP => pg_eucjp_increment,
        _ => pg_generic_charinc,
    }
}

/// `pg_generic_charinc(charptr, len)`.
pub fn pg_generic_charinc(charptr: &mut [u8]) -> bool {
    let mbverify = pg_wchar_table[database_encoding() as usize].mbverifychar;
    let len = charptr.len() as i32;
    let last = charptr.len() - 1;
    while charptr[last] < 255 {
        charptr[last] += 1;
        if mbverify(charptr, len) == len {
            return true;
        }
    }
    false
}

/// `pg_utf8_increment(charptr, length)`.
///
/// Mirrors the C `switch (length)` whose cases 4->3->2->1 fall through (and
/// whose `default` rejects lengths 5 and 6 outright). Each `break` in C exits
/// the switch with success; the implementation models the fall-through with an
/// early `return true` once a byte is incremented.
pub fn pg_utf8_increment(charptr: &mut [u8]) -> bool {
    let length = charptr.len();

    // default: reject lengths 5 and 6 (C never reads those bytes).
    if !(1..=4).contains(&length) {
        return false;
    }

    // case 4 (fall through to 3, 2, 1):
    if length == 4 && charptr[3] < 0xBF {
        charptr[3] += 1;
        return true;
    }
    // case 3:
    if length >= 3 && charptr[2] < 0xBF {
        charptr[2] += 1;
        return true;
    }
    // case 2:
    if length >= 2 {
        let limit = match charptr[0] {
            0xED => 0x9F,
            0xF4 => 0x8F,
            _ => 0xBF,
        };
        if charptr[1] < limit {
            charptr[1] += 1;
            return true;
        }
    }
    // case 1:
    let a = charptr[0];
    if a == 0x7F || a == 0xDF || a == 0xEF || a == 0xF4 {
        return false;
    }
    charptr[0] += 1;
    true
}

/// `pg_eucjp_increment(charptr, length)`.
pub fn pg_eucjp_increment(charptr: &mut [u8]) -> bool {
    const SS2: u8 = 0x8e;
    const SS3: u8 = 0x8f;
    let length = charptr.len();
    let c1 = charptr[0];

    match c1 {
        SS2 => {
            // JIS X 0201
            if length != 2 {
                return false;
            }
            let c2 = charptr[1];
            if c2 >= 0xdf {
                charptr[0] = 0xa1;
                charptr[1] = 0xa1;
            } else if c2 < 0xa1 {
                charptr[1] = 0xa1;
            } else {
                charptr[1] += 1;
            }
            true
        }
        SS3 => {
            // JIS X 0212
            if length != 3 {
                return false;
            }
            for i in (1..=2).rev() {
                let c2 = charptr[i];
                if c2 < 0xa1 {
                    charptr[i] = 0xa1;
                    return true;
                } else if c2 < 0xfe {
                    charptr[i] += 1;
                    return true;
                }
            }
            false
        }
        _ => {
            if (c1 & 0x80) != 0 {
                // JIS X 0208?
                if length != 2 {
                    return false;
                }
                for i in (0..=1).rev() {
                    let c2 = charptr[i];
                    if c2 < 0xa1 {
                        charptr[i] = 0xa1;
                        return true;
                    } else if c2 < 0xfe {
                        charptr[i] += 1;
                        return true;
                    }
                }
                false
            } else {
                // ASCII single byte
                if c1 > 0x7e {
                    return false;
                }
                charptr[0] += 1;
                true
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Multibyte-string verification.
// ---------------------------------------------------------------------------

/// `pg_verifymbstr(mbstr, len, noError)` — verify in the database encoding.
pub fn pg_verifymbstr(mbstr: &[u8], no_error: bool) -> PgResult<bool> {
    pg_verify_mbstr(database_encoding(), mbstr, no_error)
}

/// `pg_verify_mbstr(encoding, mbstr, len, noError)`.
pub fn pg_verify_mbstr(encoding: pg_enc, mbstr: &[u8], no_error: bool) -> PgResult<bool> {
    debug_assert!(pg_valid_encoding(encoding));
    let oklen = (pg_wchar_table[encoding as usize].mbverifystr)(mbstr, mbstr.len() as i32);
    if oklen != mbstr.len() as i32 {
        if no_error {
            return Ok(false);
        }
        report_invalid_encoding(encoding, &mbstr[oklen as usize..])?;
    }
    Ok(true)
}

/// `pg_verify_mbstr_len(encoding, mbstr, len, noError)` — verify and return the
/// character count (or `-1` with `noError`).
pub fn pg_verify_mbstr_len(encoding: pg_enc, mbstr: &[u8], no_error: bool) -> PgResult<i32> {
    debug_assert!(pg_valid_encoding(encoding));

    // Single-byte encodings only need to reject NUL.
    if pg_encoding_max_length(encoding) <= 1 {
        match mbstr.iter().position(|&b| b == 0) {
            None => return Ok(mbstr.len() as i32),
            Some(nullpos) => {
                if no_error {
                    return Ok(-1);
                }
                report_invalid_encoding(encoding, &mbstr[nullpos..])?;
            }
        }
    }

    let mbverifychar = pg_wchar_table[encoding as usize].mbverifychar;
    let mut mb_len = 0;
    let mut pos = 0;
    while pos < mbstr.len() {
        let remaining = &mbstr[pos..];
        // Fast path for ASCII-subset characters.
        if (remaining[0] & 0x80) == 0 {
            if remaining[0] != 0 {
                mb_len += 1;
                pos += 1;
                continue;
            }
            if no_error {
                return Ok(-1);
            }
            report_invalid_encoding(encoding, remaining)?;
        }

        let l = mbverifychar(remaining, remaining.len() as i32);
        if l < 0 {
            if no_error {
                return Ok(-1);
            }
            report_invalid_encoding(encoding, remaining)?;
        }
        pos += l as usize;
        mb_len += 1;
    }
    Ok(mb_len)
}

// ---------------------------------------------------------------------------
// Conversion-argument checking and error reporting.
// ---------------------------------------------------------------------------

/// `check_encoding_conversion_args(...)` — validate a conversion function's
/// arguments. The `CHECK_ENCODING_CONVERSION_ARGS` macro expands to this.
pub fn check_encoding_conversion_args(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    len: i32,
    expected_src_encoding: pg_enc,
    expected_dest_encoding: pg_enc,
) -> PgResult<()> {
    if !pg_valid_encoding(src_encoding) {
        return elog_error(&format!("invalid source encoding ID: {src_encoding}"));
    }
    if src_encoding != expected_src_encoding && expected_src_encoding >= 0 {
        return elog_error(&format!(
            "expected source encoding \"{}\", but got \"{}\"",
            enc_name(expected_src_encoding),
            enc_name(src_encoding)
        ));
    }
    if !pg_valid_encoding(dest_encoding) {
        return elog_error(&format!("invalid destination encoding ID: {dest_encoding}"));
    }
    if dest_encoding != expected_dest_encoding && expected_dest_encoding >= 0 {
        return elog_error(&format!(
            "expected destination encoding \"{}\", but got \"{}\"",
            enc_name(expected_dest_encoding),
            enc_name(dest_encoding)
        ));
    }
    if len < 0 {
        return elog_error("encoding conversion length must not be negative");
    }
    Ok(())
}

/// `report_invalid_encoding(encoding, mbstr, len)` — always `Err`.
pub fn report_invalid_encoding(encoding: pg_enc, mbstr: &[u8]) -> PgResult<()> {
    let l = pg_encoding_mblen_or_incomplete(encoding, mbstr);
    report_invalid_encoding_int(encoding, mbstr, l, mbstr.len() as i32)
}

fn report_invalid_encoding_int<T>(
    encoding: pg_enc,
    mbstr: &[u8],
    mblen: i32,
    len: i32,
) -> PgResult<T> {
    let buf = byte_sequence(mbstr, mblen, len);
    Err(ereport(ERROR)
        .errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE)
        .errmsg(format!(
            "invalid byte sequence for encoding \"{}\": {}",
            enc_name(encoding),
            buf
        ))
        .into_error())
}

fn report_invalid_encoding_db<T>(mbstr: &[u8], mblen: i32, len: i32) -> PgResult<T> {
    report_invalid_encoding_int(database_encoding(), mbstr, mblen, len)
}

/// `report_untranslatable_char(src_encoding, dest_encoding, mbstr, len)` —
/// always `Err`.
pub fn report_untranslatable_char(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    mbstr: &[u8],
) -> PgResult<()> {
    let l = pg_encoding_mblen_or_incomplete(src_encoding, mbstr);
    let buf = byte_sequence(mbstr, l, mbstr.len() as i32);
    Err(ereport(ERROR)
        .errcode(ERRCODE_UNTRANSLATABLE_CHARACTER)
        .errmsg(format!(
            "character with byte sequence {} in encoding \"{}\" has no equivalent in encoding \"{}\"",
            buf,
            enc_name(src_encoding),
            enc_name(dest_encoding)
        ))
        .into_error())
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

/// Render the leading `min(mblen, len, 8)` bytes as space-separated `0xNN`.
fn byte_sequence(mbstr: &[u8], mblen: i32, len: i32) -> String {
    let jlimit = mblen.min(len).min(8).max(0) as usize;
    let mut p = String::new();
    for j in 0..jlimit {
        if j > 0 {
            p.push(' ');
        }
        p.push_str(&format!("0x{:02x}", mbstr[j]));
    }
    p
}

/// Length up to (not including) the first NUL, or the whole slice (C `strlen`).
fn c_string_len(bytes: &[u8]) -> usize {
    bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len())
}

/// Number of wchars up to (not including) the first 0 (C `pg_wchar_strlen`).
fn pg_wchar_strlen(chars: &[PgWChar]) -> usize {
    chars.iter().position(|&c| c == 0).unwrap_or(chars.len())
}

fn elog_error<T>(message: &str) -> PgResult<T> {
    Err(ereport(ERROR)
        .errmsg_internal(message.to_string())
        .into_error())
}

/// `bytea`/`name` validation error for `pg_convert*` / `length_in_encoding`:
/// "invalid {kind} encoding name \"{name}\"" with `ERRCODE_INVALID_PARAMETER_VALUE`.
/// `kind` is "source"/"destination"/"" (the bare "invalid encoding name" form).
fn invalid_encoding_name_error(kind: &str, name: &str) -> types_error::PgError {
    let prefix = if kind.is_empty() {
        "invalid encoding name".to_string()
    } else {
        format!("invalid {kind} encoding name")
    };
    ereport(ERROR)
        .errcode(types_error::ERRCODE_INVALID_PARAMETER_VALUE)
        .errmsg(format!("{prefix} \"{name}\""))
        .into_error()
}

/// The `name`'s bytes up to the first NUL, as `&str` (C `NameStr`).
fn name_str(name: &NameData) -> &str {
    std::str::from_utf8(name.name_str()).unwrap_or("")
}

fn too_long_error(len: usize) -> types_error::PgError {
    ereport(ERROR)
        .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .errmsg("out of memory")
        .errdetail(format!(
            "String of {len} bytes is too long for encoding conversion."
        ))
        .into_error()
}

// ---------------------------------------------------------------------------
// UTF-8 / Unicode code-point helpers (common/wchar.c, mb/pg_wchar.h).
// ---------------------------------------------------------------------------

/// `is_valid_unicode_codepoint(c)` (mb/pg_wchar.h).
fn is_valid_unicode_codepoint(c: PgWChar) -> bool {
    c > 0 && c <= 0x0010_FFFF
}

/// `unicode_to_utf8(c, utf8string)` (common/wchar.c).
fn unicode_to_utf8(c: PgWChar, utf8string: &mut [u8]) {
    if c <= 0x7F {
        utf8string[0] = c as u8;
    } else if c <= 0x7FF {
        utf8string[0] = (0xC0 | ((c >> 6) & 0x1F)) as u8;
        utf8string[1] = (0x80 | (c & 0x3F)) as u8;
    } else if c <= 0xFFFF {
        utf8string[0] = (0xE0 | ((c >> 12) & 0x0F)) as u8;
        utf8string[1] = (0x80 | ((c >> 6) & 0x3F)) as u8;
        utf8string[2] = (0x80 | (c & 0x3F)) as u8;
    } else {
        utf8string[0] = (0xF0 | ((c >> 18) & 0x07)) as u8;
        utf8string[1] = (0x80 | ((c >> 12) & 0x3F)) as u8;
        utf8string[2] = (0x80 | ((c >> 6) & 0x3F)) as u8;
        utf8string[3] = (0x80 | (c & 0x3F)) as u8;
    }
}

/// `pg_utf_mblen(s)` (common/wchar.c): byte length of the leading UTF-8 char.
fn pg_utf_mblen(s: &[u8]) -> i32 {
    let b = s[0];
    if b < 0x80 {
        1
    } else if (b & 0xE0) == 0xC0 {
        2
    } else if (b & 0xF0) == 0xE0 {
        3
    } else if (b & 0xF8) == 0xF0 {
        4
    } else if (b & 0xFC) == 0xF8 {
        5
    } else if (b & 0xFE) == 0xFC {
        6
    } else {
        1
    }
}

// ---------------------------------------------------------------------------
// Seam installation.
// ---------------------------------------------------------------------------

/// Install every seam in `backend-utils-mb-mbutils-seams`.
pub fn init_seams() {
    use backend_utils_mb_mbutils_seams as seams;

    // Register this crate's SQL-callable encoding functions into the fmgr-core
    // builtin table (C: fmgr_builtins[]) so by-OID dispatch resolves them.
    fmgr_builtins::register_mbutils_builtins();

    seams::pg_verifymbstr::set(pg_verifymbstr);
    seams::pg_server_to_client::set(pg_server_to_client);
    seams::pg_client_to_server::set(pg_client_to_server);
    // The `pg_mbstrlen_with_len`/`pg_mbcharcliplen`/`pg_mblen_range` seams are
    // fallible (`-> PgResult<i32>`). C's bodies `report_invalid_encoding`
    // (ereport ERROR, via longjmp) on a byte sequence invalid in the database
    // encoding; we carry that `PgError` on `Err` so it flows through the normal
    // ereport→ERROR rendering at the caller (the regression suite exercises this
    // on truncated/invalid multibyte strings).
    seams::pg_mbstrlen_with_len::set(pg_mbstrlen_with_len);
    seams::pg_mbcliplen::set(pg_mbcliplen);
    seams::pg_mbcharcliplen::set(pg_mbcharcliplen);
    seams::pg_mb2wchar_with_len::set(pg_mb2wchar_with_len);
    seams::pg_wchar2mb_with_len::set(pg_wchar2mb_with_len);
    seams::pg_mblen_range::set(pg_mblen_range);
    seams::pg_database_encoding_max_length::set(pg_database_encoding_max_length);
    seams::get_database_encoding::set(GetDatabaseEncoding);
    seams::get_database_encoding_name::set(GetDatabaseEncodingName);
    // NOTE: `is_encoding_supported_by_icu` is declared in this seam crate but is
    // `common/encnames.c` logic (it reads `pg_enc2icu_tbl`), not mbutils.c. Its
    // real owner is the encnames unit (`common-extra-encnames-fgram`), which now
    // installs it from ITS `init_seams()` — we deliberately do NOT install it
    // here. (Cross-crate install: the seam's true C owner is encnames, so a
    // `::set` there satisfies the install-completeness guard.)
    seams::set_database_encoding::set(SetDatabaseEncoding);
    seams::initialize_client_encoding::set(|| {
        // The seam carries no Mcx; FindDefaultConversionProc needs one. Run it in
        // a scratch context (the catalog lookups it does allocate transiently).
        let ctx = MemoryContext::new("InitializeClientEncoding");
        InitializeClientEncoding(ctx.mcx())
    });
    seams::pg_server_to_any::set(pg_server_to_any);
    seams::pg_any_to_server::set(pg_any_to_server);
    seams::pg_get_client_encoding::set(pg_get_client_encoding);
    seams::pg_encoding_mblen::set(|encoding, mbstr| {
        common_wchar::pg_encoding_mblen(encoding, mbstr).unwrap_or(1)
    });
    seams::pg_encoding_is_client_only::set(PG_ENCODING_IS_CLIENT_ONLY);
    seams::pg_unicode_to_server::set(pg_unicode_to_server);
    seams::report_invalid_encoding::set(report_invalid_encoding);
    seams::report_untranslatable_char::set(report_untranslatable_char);
    seams::check_encoding_conversion_args::set(check_encoding_conversion_args);
    seams::find_default_conversion_proc::set(|for_encoding, to_encoding| {
        // The seam carries no Mcx; FindDefaultConversionProc allocates its
        // catalog lookups transiently. Run it in a scratch context.
        let ctx = MemoryContext::new("FindDefaultConversionProc");
        FindDefaultConversionProc(ctx.mcx(), for_encoding, to_encoding)
    });
    seams::pg_do_encoding_conversion_buf::set(
        |mcx, proc, src_encoding, dest_encoding, src, dst_capacity, no_error| {
            // Mirror pg_do_encoding_conversion_buf (mbutils.c): limit the input
            // so the worst-case output (MAX_CONVERSION_GROWTH per byte) fits the
            // caller's destination buffer of capacity `dst_capacity`
            // (C: `(destlen - 1) / MAX_CONVERSION_GROWTH`; destlen >= 1).
            let cap = (dst_capacity.max(1) as usize - 1) / MAX_CONVERSION_GROWTH;
            let srclen = src.len().min(cap);
            convert_via_proc_counted::call(
                mcx,
                proc,
                src_encoding,
                dest_encoding,
                &src[..srclen],
                no_error,
            )
        },
    );

    // `SetMessageEncoding` is a mbutils.c routine, but its declaration is homed in
    // pg_locale.c's seam crate (the consumer is pg_perm_setlocale). Install it
    // here, in the real owner. `pg_enc` is `i32`, so the cast is identity; the
    // `PgResult` is infallible for a valid encoding (debug_assert only).
    backend_utils_adt_pg_locale_env_seams::set_message_encoding::set(|encoding| {
        SetMessageEncoding(encoding).expect("SetMessageEncoding on an invalid encoding")
    });

    // Parallel-worker bring-up reads the database encoding and sets the worker's
    // client encoding to match (parallel.c:1424 `GetDatabaseEncoding()` /
    // `SetClientEncoding(...)`). These are mbutils.c routines; `pg_enc` is `i32`,
    // so both contracts match identity. The parallel-rt seam crate is a leaf
    // (no cycle); install from the real owner here.
    backend_access_transam_parallel_rt_seams::get_database_encoding::set(|| {
        Ok(GetDatabaseEncoding())
    });
    backend_access_transam_parallel_rt_seams::set_client_encoding::set(SetClientEncoding);
}

