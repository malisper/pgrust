#![allow(non_snake_case)]

pub mod builtins;
pub use builtins::MBUTILS_BUILTINS;

// C mbutils.c returns the *source pointer* when no conversion is performed;
// pointer identity does not cross a safe-Rust boundary, so that outcome is
// `Ok(None)` ("the caller's bytes stand") and a performed conversion is
// `Ok(Some(bytes))` (no trailing NUL) allocated in `mcx`.

use core::cell::{Cell, RefCell};

use datum::Datum;
use mcx::{slice_in, vec_with_capacity_in, Mcx, PgVec};
use types_core::{InvalidOid, Oid};
use types_error::{
    PgError, PgResult, ERRCODE_CHARACTER_NOT_IN_REPERTOIRE, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_FUNCTION,
    ERRCODE_UNTRANSLATABLE_CHARACTER, FATAL,
};
use types_fmgr::{direct_function_call6_coll, PGFunction};
use wchar::{
    is_valid_unicode_codepoint, pg_enc, pg_encoding_dsplen, pg_encoding_max_length,
    pg_encoding_mblen, pg_encoding_mblen_or_incomplete, pg_encoding_verifymbchar,
    pg_encoding_verifymbstr, pg_utf_mblen, pg_valid_be_encoding, pg_valid_encoding,
    pg_valid_fe_encoding, pg_wchar, pg_wchar_table, unicode_to_utf8, PG_EUC_JP, PG_SQL_ASCII,
    PG_UTF8,
};

pub const MAX_CONVERSION_GROWTH: usize = 4;
const MAX_MULTIBYTE_CHAR_LEN: usize = 4;
const MAX_ALLOC_SIZE: usize = 0x3FFF_FFFF;
const MAX_ALLOC_HUGE_SIZE: usize = usize::MAX / 2;

// pg_enc2name_tbl names (common/encnames.c, indexed by pg_enc); homed here
// until the encnames unit lands — mbutils.c is its only backend reader.
static PG_ENC2NAME: [&str; wchar::_PG_LAST_ENCODING_ as usize] = [
    "SQL_ASCII",
    "EUC_JP",
    "EUC_CN",
    "EUC_KR",
    "EUC_TW",
    "EUC_JIS_2004",
    "UTF8",
    "MULE_INTERNAL",
    "LATIN1",
    "LATIN2",
    "LATIN3",
    "LATIN4",
    "LATIN5",
    "LATIN6",
    "LATIN7",
    "LATIN8",
    "LATIN9",
    "LATIN10",
    "WIN1256",
    "WIN1258",
    "WIN866",
    "WIN874",
    "KOI8R",
    "WIN1251",
    "WIN1252",
    "ISO_8859_5",
    "ISO_8859_6",
    "ISO_8859_7",
    "ISO_8859_8",
    "WIN1250",
    "WIN1253",
    "WIN1254",
    "WIN1255",
    "WIN1257",
    "KOI8U",
    "SJIS",
    "BIG5",
    "GBK",
    "UHC",
    "GB18030",
    "JOHAB",
    "SHIFT_JIS_2004",
];

fn enc_name(encoding: pg_enc) -> &'static str {
    PG_ENC2NAME[encoding as usize]
}

// pg_encname_tbl (common/encnames.c), sorted by clean name for binary search.
static PG_ENCNAME: [(&str, pg_enc); 81] = [
    ("abc", wchar::PG_WIN1258),
    ("alt", wchar::PG_WIN866),
    ("big5", wchar::PG_BIG5),
    ("euccn", wchar::PG_EUC_CN),
    ("eucjis2004", wchar::PG_EUC_JIS_2004),
    ("eucjp", PG_EUC_JP),
    ("euckr", wchar::PG_EUC_KR),
    ("euctw", wchar::PG_EUC_TW),
    ("gb18030", wchar::PG_GB18030),
    ("gbk", wchar::PG_GBK),
    ("iso88591", wchar::PG_LATIN1),
    ("iso885910", wchar::PG_LATIN6),
    ("iso885913", wchar::PG_LATIN7),
    ("iso885914", wchar::PG_LATIN8),
    ("iso885915", wchar::PG_LATIN9),
    ("iso885916", wchar::PG_LATIN10),
    ("iso88592", wchar::PG_LATIN2),
    ("iso88593", wchar::PG_LATIN3),
    ("iso88594", wchar::PG_LATIN4),
    ("iso88595", wchar::PG_ISO_8859_5),
    ("iso88596", wchar::PG_ISO_8859_6),
    ("iso88597", wchar::PG_ISO_8859_7),
    ("iso88598", wchar::PG_ISO_8859_8),
    ("iso88599", wchar::PG_LATIN5),
    ("johab", wchar::PG_JOHAB),
    ("koi8", wchar::PG_KOI8R),
    ("koi8r", wchar::PG_KOI8R),
    ("koi8u", wchar::PG_KOI8U),
    ("latin1", wchar::PG_LATIN1),
    ("latin10", wchar::PG_LATIN10),
    ("latin2", wchar::PG_LATIN2),
    ("latin3", wchar::PG_LATIN3),
    ("latin4", wchar::PG_LATIN4),
    ("latin5", wchar::PG_LATIN5),
    ("latin6", wchar::PG_LATIN6),
    ("latin7", wchar::PG_LATIN7),
    ("latin8", wchar::PG_LATIN8),
    ("latin9", wchar::PG_LATIN9),
    ("mskanji", wchar::PG_SJIS),
    ("muleinternal", wchar::PG_MULE_INTERNAL),
    ("shiftjis", wchar::PG_SJIS),
    ("shiftjis2004", wchar::PG_SHIFT_JIS_2004),
    ("sjis", wchar::PG_SJIS),
    ("sqlascii", PG_SQL_ASCII),
    ("tcvn", wchar::PG_WIN1258),
    ("tcvn5712", wchar::PG_WIN1258),
    ("uhc", wchar::PG_UHC),
    ("unicode", PG_UTF8),
    ("utf8", PG_UTF8),
    ("vscii", wchar::PG_WIN1258),
    ("win", wchar::PG_WIN1251),
    ("win1250", wchar::PG_WIN1250),
    ("win1251", wchar::PG_WIN1251),
    ("win1252", wchar::PG_WIN1252),
    ("win1253", wchar::PG_WIN1253),
    ("win1254", wchar::PG_WIN1254),
    ("win1255", wchar::PG_WIN1255),
    ("win1256", wchar::PG_WIN1256),
    ("win1257", wchar::PG_WIN1257),
    ("win1258", wchar::PG_WIN1258),
    ("win866", wchar::PG_WIN866),
    ("win874", wchar::PG_WIN874),
    ("win932", wchar::PG_SJIS),
    ("win936", wchar::PG_GBK),
    ("win949", wchar::PG_UHC),
    ("win950", wchar::PG_BIG5),
    ("windows1250", wchar::PG_WIN1250),
    ("windows1251", wchar::PG_WIN1251),
    ("windows1252", wchar::PG_WIN1252),
    ("windows1253", wchar::PG_WIN1253),
    ("windows1254", wchar::PG_WIN1254),
    ("windows1255", wchar::PG_WIN1255),
    ("windows1256", wchar::PG_WIN1256),
    ("windows1257", wchar::PG_WIN1257),
    ("windows1258", wchar::PG_WIN1258),
    ("windows866", wchar::PG_WIN866),
    ("windows874", wchar::PG_WIN874),
    ("windows932", wchar::PG_SJIS),
    ("windows936", wchar::PG_GBK),
    ("windows949", wchar::PG_UHC),
    ("windows950", wchar::PG_BIG5),
];

const NAMEDATALEN: usize = 64;

// clean_encoding_name (encnames.c): keep alnum, ASCII-lowercase.
fn clean_encoding_name(key: &[u8], buf: &mut [u8; NAMEDATALEN]) -> usize {
    let mut n = 0;
    for &b in key {
        if b.is_ascii_alphanumeric() {
            buf[n] = b.to_ascii_lowercase();
            n += 1;
        }
    }
    n
}

/// Byte-level entry: C (mbutils.c pg_char_to_encoding) cleans the name
/// byte-wise and never encoding-validates it, so non-UTF-8 bytes must not
/// reject the whole name — reachable via SQL_ASCII server encodings.
/// Divergence #10 (proofs/encnames): the SQL wrapper's former
/// from_utf8(..).unwrap_or("") returned -1 where C returns the cleaned
/// match (ground-truthed glibc PG 18.4).
pub fn pg_char_to_encoding_bytes(name: &[u8]) -> pg_enc {
    if name.is_empty() || name.len() >= NAMEDATALEN {
        return -1;
    }
    let mut buf = [0u8; NAMEDATALEN];
    let n = clean_encoding_name(name, &mut buf);
    let key = &buf[..n];
    match PG_ENCNAME.binary_search_by(|(nm, _)| nm.as_bytes().cmp(key)) {
        Ok(idx) => PG_ENCNAME[idx].1,
        Err(_) => -1,
    }
}

pub fn pg_char_to_encoding(name: &str) -> pg_enc {
    pg_char_to_encoding_bytes(name.as_bytes())
}

pub fn pg_valid_client_encoding(name: &str) -> pg_enc {
    let enc = pg_char_to_encoding(name);
    if enc < 0 || !pg_valid_fe_encoding(enc) {
        return -1;
    }
    enc
}

pub fn pg_valid_server_encoding(name: &str) -> pg_enc {
    let enc = pg_char_to_encoding(name);
    if enc < 0 || !pg_valid_be_encoding(enc) {
        return -1;
    }
    enc
}

pub fn pg_encoding_to_char(encoding: pg_enc) -> &'static str {
    if pg_valid_encoding(encoding) {
        enc_name(encoding)
    } else {
        ""
    }
}

/// The resolved conversion procedure (C caches a `FmgrInfo`; the resolved
/// `fn_addr` is the resolve-once payload — conversion procs use no `fn_extra`).
#[derive(Clone, Copy)]
struct ResolvedConvProc {
    fn_addr: PGFunction,
}

#[derive(Clone, Copy)]
struct ConvProcInfo {
    s_encoding: pg_enc,
    c_encoding: pg_enc,
    to_server: ResolvedConvProc,
    to_client: ResolvedConvProc,
}

thread_local! {
    static CLIENT_ENCODING: Cell<pg_enc> = const { Cell::new(PG_SQL_ASCII) };
    static DATABASE_ENCODING: Cell<pg_enc> = const { Cell::new(PG_SQL_ASCII) };
    static MESSAGE_ENCODING: Cell<pg_enc> = const { Cell::new(PG_SQL_ASCII) };
    static BACKEND_STARTUP_COMPLETE: Cell<bool> = const { Cell::new(false) };
    static PENDING_CLIENT_ENCODING: Cell<pg_enc> = const { Cell::new(PG_SQL_ASCII) };
    static TO_SERVER_CONV_PROC: Cell<Option<ResolvedConvProc>> = const { Cell::new(None) };
    static TO_CLIENT_CONV_PROC: Cell<Option<ResolvedConvProc>> = const { Cell::new(None) };
    static UTF8_TO_SERVER_CONV_PROC: Cell<Option<ResolvedConvProc>> = const { Cell::new(None) };
    // C ConvProcList lives in TopMemoryContext for the backend lifetime; no
    // arena outlives the backend thread, so a std Vec of Copy entries.
    static CONV_PROC_LIST: RefCell<Vec<ConvProcInfo>> = const { RefCell::new(Vec::new()) };
}

#[inline]
fn database_encoding() -> pg_enc {
    DATABASE_ENCODING.with(|e| e.get())
}

#[inline]
fn client_encoding() -> pg_enc {
    CLIENT_ENCODING.with(|e| e.get())
}

fn resolve_conv_proc(proc: Oid) -> PgResult<ResolvedConvProc> {
    let finfo = fmgr_seams::fmgr_info::call(proc)?;
    Ok(ResolvedConvProc {
        fn_addr: finfo.fn_addr,
    })
}

pub fn PrepareClientEncoding(encoding: pg_enc) -> PgResult<i32> {
    if !pg_valid_fe_encoding(encoding) {
        return Ok(-1);
    }
    if !BACKEND_STARTUP_COMPLETE.with(|c| c.get()) {
        return Ok(0);
    }
    let current_server_encoding = database_encoding();
    if current_server_encoding == encoding
        || current_server_encoding == PG_SQL_ASCII
        || encoding == PG_SQL_ASCII
    {
        return Ok(0);
    }

    if xact_seams::is_transaction_state::call() {
        let to_server_proc =
            namespace_seams::find_default_conversion_proc::call(encoding, current_server_encoding)?;
        if to_server_proc == InvalidOid {
            return Ok(-1);
        }
        let to_client_proc =
            namespace_seams::find_default_conversion_proc::call(current_server_encoding, encoding)?;
        if to_client_proc == InvalidOid {
            return Ok(-1);
        }
        let convinfo = ConvProcInfo {
            s_encoding: current_server_encoding,
            c_encoding: encoding,
            to_server: resolve_conv_proc(to_server_proc)?,
            to_client: resolve_conv_proc(to_client_proc)?,
        };
        // Newest entry at the head; SetClientEncoding prunes older duplicates.
        CONV_PROC_LIST.with(|l| l.borrow_mut().insert(0, convinfo));
        Ok(0)
    } else {
        // Not in a live transaction: only a previously cached pair can be restored.
        let found = CONV_PROC_LIST.with(|l| {
            l.borrow().iter().any(|info| {
                info.s_encoding == current_server_encoding && info.c_encoding == encoding
            })
        });
        Ok(if found { 0 } else { -1 })
    }
}

pub fn SetClientEncoding(encoding: pg_enc) -> PgResult<i32> {
    if !pg_valid_fe_encoding(encoding) {
        return Ok(-1);
    }
    if !BACKEND_STARTUP_COMPLETE.with(|c| c.get()) {
        PENDING_CLIENT_ENCODING.with(|c| c.set(encoding));
        return Ok(0);
    }
    let current_server_encoding = database_encoding();
    if current_server_encoding == encoding
        || current_server_encoding == PG_SQL_ASCII
        || encoding == PG_SQL_ASCII
    {
        CLIENT_ENCODING.with(|c| c.set(encoding));
        TO_SERVER_CONV_PROC.with(|p| p.set(None));
        TO_CLIENT_CONV_PROC.with(|p| p.set(None));
        return Ok(0);
    }

    let mut found = false;
    CONV_PROC_LIST.with(|l| {
        let mut list = l.borrow_mut();
        let mut i = 0;
        while i < list.len() {
            let info = list[i];
            if info.s_encoding == current_server_encoding && info.c_encoding == encoding {
                if !found {
                    CLIENT_ENCODING.with(|c| c.set(encoding));
                    TO_SERVER_CONV_PROC.with(|p| p.set(Some(info.to_server)));
                    TO_CLIENT_CONV_PROC.with(|p| p.set(Some(info.to_client)));
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
    Ok(if found { 0 } else { -1 })
}

pub fn InitializeClientEncoding() -> PgResult<()> {
    debug_assert!(!BACKEND_STARTUP_COMPLETE.with(|c| c.get()));
    BACKEND_STARTUP_COMPLETE.with(|c| c.set(true));

    let pending = PENDING_CLIENT_ENCODING.with(|c| c.get());
    if PrepareClientEncoding(pending)? < 0 || SetClientEncoding(pending)? < 0 {
        return Err(conversion_not_supported_fatal(pending));
    }

    let current_server_encoding = database_encoding();
    if current_server_encoding != PG_UTF8 && current_server_encoding != PG_SQL_ASCII {
        let utf8_to_server_proc =
            namespace_seams::find_default_conversion_proc::call(PG_UTF8, current_server_encoding)?;
        if utf8_to_server_proc != InvalidOid {
            let resolved = resolve_conv_proc(utf8_to_server_proc)?;
            UTF8_TO_SERVER_CONV_PROC.with(|p| p.set(Some(resolved)));
        }
    }
    Ok(())
}

pub fn pg_get_client_encoding() -> pg_enc {
    client_encoding()
}

pub fn pg_get_client_encoding_name() -> &'static str {
    enc_name(client_encoding())
}

/// Invoke a conversion proc (C `FunctionCall6`): `dest` gets the worst-case
/// `len * MAX_CONVERSION_GROWTH + 1` buffer, the proc writes a NUL-terminated
/// string into it and returns the source-byte count converted.
fn convert_with_proc<'mcx>(
    mcx: Mcx<'mcx>,
    proc: ResolvedConvProc,
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<(i32, PgVec<'mcx, u8>)> {
    let cap = src.len() * MAX_CONVERSION_GROWTH + 1;
    let mut dest: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, cap)?;
    let consumed = direct_function_call6_coll(
        proc.fn_addr,
        InvalidOid,
        Datum::from_i32(src_encoding),
        Datum::from_i32(dest_encoding),
        Datum::from_usize(src.as_ptr() as usize),
        Datum::from_usize(dest.as_mut_ptr() as usize),
        Datum::from_i32(src.len() as i32),
        Datum::from_bool(no_error),
    )?;
    // SAFETY: the conversion-proc contract (mbutils.c): at most
    // src.len() * MAX_CONVERSION_GROWTH output bytes plus a NUL were written
    // into `dest`; every byte up to and including that NUL is initialized.
    let n = unsafe {
        let ptr = dest.as_ptr();
        let mut n = 0usize;
        while *ptr.add(n) != 0 {
            n += 1;
        }
        dest.set_len(n);
        n
    };
    debug_assert!(n < cap);
    Ok((consumed.as_i32(), dest))
}

pub fn pg_do_encoding_conversion<'mcx>(
    mcx: Mcx<'mcx>,
    src: &[u8],
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    if src.is_empty() {
        return Ok(None);
    }
    if src_encoding == dest_encoding {
        return Ok(None);
    }
    if dest_encoding == PG_SQL_ASCII {
        return Ok(None);
    }
    if src_encoding == PG_SQL_ASCII {
        pg_verify_mbstr(dest_encoding, src, false)?;
        return Ok(None);
    }

    if !xact_seams::is_transaction_state::call() {
        return Err(internal_error(
            "cannot perform encoding conversion outside a transaction",
        ));
    }

    let proc = namespace_seams::find_default_conversion_proc::call(src_encoding, dest_encoding)?;
    if proc == InvalidOid {
        return Err(no_default_conversion_error(src_encoding, dest_encoding));
    }

    if src.len() >= MAX_ALLOC_HUGE_SIZE / MAX_CONVERSION_GROWTH {
        return Err(too_long_error(src.len()));
    }

    // General case: C's OidFunctionCall6 re-resolves per call (only the
    // client<->server default pair is cached); mirrored here.
    let resolved = resolve_conv_proc(proc)?;
    let (_, result) = convert_with_proc(mcx, resolved, src_encoding, dest_encoding, src, false)?;

    if src.len() > 1_000_000 && result.len() >= MAX_ALLOC_SIZE {
        return Err(too_long_error(src.len()));
    }
    Ok(Some(result))
}

/// C `pg_do_encoding_conversion_buf`: `proc` was already looked up; the input
/// is clipped so the worst-case output fits a `dst_capacity`-byte buffer.
/// Returns (source bytes converted, output bytes without the trailing NUL).
pub fn pg_do_encoding_conversion_buf<'mcx>(
    mcx: Mcx<'mcx>,
    proc: Oid,
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    dst_capacity: i32,
    no_error: bool,
) -> PgResult<(i32, PgVec<'mcx, u8>)> {
    let cap = (dst_capacity.max(1) as usize - 1) / MAX_CONVERSION_GROWTH;
    let srclen = src.len().min(cap);
    let resolved = resolve_conv_proc(proc)?;
    convert_with_proc(
        mcx,
        resolved,
        src_encoding,
        dest_encoding,
        &src[..srclen],
        no_error,
    )
}

// C returns the source pointer in a register; the Option<PgVec> analog is
// >16B (sret through memory) unless the identity arm inlines into the caller.
#[inline]
pub fn pg_client_to_server<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<Option<PgVec<'mcx, u8>>> {
    pg_any_to_server(mcx, s, client_encoding())
}

/// Always validates, even when no conversion is needed: the input comes from
/// outside the database.
// inline(always): the identity arm must fold into the caller or the
// >16B return is materialized through memory (sret), the per-call constant
// behind the mb_client_to_server_8 FAIL (docs/benchmarks/mbutils.md).
#[inline(always)]
pub fn pg_any_to_server<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
    encoding: pg_enc,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    if s.is_empty() {
        return Ok(None);
    }
    let db_encoding = database_encoding();

    if encoding == db_encoding || encoding == PG_SQL_ASCII {
        pg_verify_mbstr(db_encoding, s, false)?;
        return Ok(None);
    }

    if db_encoding == PG_SQL_ASCII {
        // No conversion possible; validate under the client encoding if it is
        // server-legal, else reject NULs and high-bit bytes outright.
        if pg_valid_be_encoding(encoding) {
            pg_verify_mbstr(encoding, s, false)?;
        } else {
            for &b in s {
                if b == 0 || (b & 0x80) != 0 {
                    return Err(invalid_byte_value_error(b));
                }
            }
        }
        return Ok(None);
    }

    if encoding == client_encoding() {
        return perform_default_encoding_conversion(mcx, s, true);
    }

    pg_do_encoding_conversion(mcx, s, encoding, db_encoding)
}

pub fn pg_server_to_client<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<Option<PgVec<'mcx, u8>>> {
    pg_server_to_any(mcx, s, client_encoding())
}

pub fn pg_server_to_any<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
    encoding: pg_enc,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    if s.is_empty() {
        return Ok(None);
    }
    let db_encoding = database_encoding();

    if encoding == db_encoding || encoding == PG_SQL_ASCII {
        return Ok(None);
    }
    if db_encoding == PG_SQL_ASCII {
        pg_verify_mbstr(encoding, s, false)?;
        return Ok(None);
    }
    if encoding == client_encoding() {
        return perform_default_encoding_conversion(mcx, s, false);
    }
    pg_do_encoding_conversion(mcx, s, db_encoding, encoding)
}

/// The hoistable per-message test: `pg_server_to_client` is a guaranteed
/// no-op iff the client encoding is the server encoding or SQL_ASCII
/// (printtup resolves this once per result set — strategy lever 2).
pub fn server_to_client_conversion_needed() -> bool {
    let client = client_encoding();
    client != database_encoding() && client != PG_SQL_ASCII
}

/// Uses the SetClientEncoding-cached proc, so it is safe outside transactions;
/// with no conversion set up it performs none.
fn perform_default_encoding_conversion<'mcx>(
    mcx: Mcx<'mcx>,
    src: &[u8],
    is_client_to_server: bool,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let (src_encoding, dest_encoding, proc) = if is_client_to_server {
        (
            client_encoding(),
            database_encoding(),
            TO_SERVER_CONV_PROC.with(|p| p.get()),
        )
    } else {
        (
            database_encoding(),
            client_encoding(),
            TO_CLIENT_CONV_PROC.with(|p| p.get()),
        )
    };
    let Some(proc) = proc else {
        return Ok(None);
    };

    if src.len() >= MAX_ALLOC_HUGE_SIZE / MAX_CONVERSION_GROWTH {
        return Err(too_long_error(src.len()));
    }
    let (_, result) = convert_with_proc(mcx, proc, src_encoding, dest_encoding, src, false)?;
    if src.len() > 1_000_000 && result.len() >= MAX_ALLOC_SIZE {
        return Err(too_long_error(src.len()));
    }
    Ok(Some(result))
}

/// Convert one Unicode code point to the server encoding (no trailing NUL).
/// Relies on the InitializeClientEncoding-cached UTF8-to-server proc, so it is
/// safe outside transactions (the parser calls it in aborted transactions).
pub fn pg_unicode_to_server<'mcx>(mcx: Mcx<'mcx>, c: pg_wchar) -> PgResult<PgVec<'mcx, u8>> {
    if !is_valid_unicode_codepoint(c) {
        return Err(invalid_codepoint_error());
    }
    if c <= 0x7F {
        return slice_in(mcx, &[c as u8]);
    }
    let server_encoding = database_encoding();
    if server_encoding == PG_UTF8 {
        let mut buf = [0u8; MAX_MULTIBYTE_CHAR_LEN];
        unicode_to_utf8(c, &mut buf);
        let n = pg_utf_mblen(&buf) as usize;
        return slice_in(mcx, &buf[..n]);
    }

    let Some(proc) = UTF8_TO_SERVER_CONV_PROC.with(|p| p.get()) else {
        return Err(conversion_not_supported_error(PG_UTF8, server_encoding));
    };
    let mut c_as_utf8 = [0u8; MAX_MULTIBYTE_CHAR_LEN];
    unicode_to_utf8(c, &mut c_as_utf8);
    let n = pg_utf_mblen(&c_as_utf8) as usize;
    let (_, out) = convert_with_proc(mcx, proc, PG_UTF8, server_encoding, &c_as_utf8[..n], false)?;
    Ok(out)
}

/// Like [`pg_unicode_to_server`], but conversion failure is `Ok(None)`.
pub fn pg_unicode_to_server_noerror<'mcx>(
    mcx: Mcx<'mcx>,
    c: pg_wchar,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    if !is_valid_unicode_codepoint(c) {
        return Ok(None);
    }
    if c <= 0x7F {
        return Ok(Some(slice_in(mcx, &[c as u8])?));
    }
    let server_encoding = database_encoding();
    if server_encoding == PG_UTF8 {
        let mut buf = [0u8; MAX_MULTIBYTE_CHAR_LEN];
        unicode_to_utf8(c, &mut buf);
        let n = pg_utf_mblen(&buf) as usize;
        return Ok(Some(slice_in(mcx, &buf[..n])?));
    }

    let Some(proc) = UTF8_TO_SERVER_CONV_PROC.with(|p| p.get()) else {
        return Ok(None);
    };
    let mut c_as_utf8 = [0u8; MAX_MULTIBYTE_CHAR_LEN];
    unicode_to_utf8(c, &mut c_as_utf8);
    let n = pg_utf_mblen(&c_as_utf8) as usize;
    let (consumed, out) =
        convert_with_proc(mcx, proc, PG_UTF8, server_encoding, &c_as_utf8[..n], true)?;
    Ok((consumed == n as i32).then_some(out))
}

pub fn pg_mb2wchar_with_len<'mcx>(mcx: Mcx<'mcx>, from: &[u8]) -> PgResult<PgVec<'mcx, pg_wchar>> {
    pg_encoding_mb2wchar_with_len(mcx, database_encoding(), from)
}

pub fn pg_encoding_mb2wchar_with_len<'mcx>(
    mcx: Mcx<'mcx>,
    encoding: pg_enc,
    from: &[u8],
) -> PgResult<PgVec<'mcx, pg_wchar>> {
    let conv = pg_wchar_table[encoding as usize]
        .mb2wchar_with_len
        .unwrap_or_else(|| panic!("mb2wchar: client-only encoding {encoding} has no converter"));
    let mut to: PgVec<'mcx, pg_wchar> = vec_with_capacity_in(mcx, from.len() + 1)?;
    to.resize(from.len() + 1, 0);
    let n = conv(from, &mut to);
    to.truncate(n as usize);
    Ok(to)
}

pub fn pg_wchar2mb_with_len<'mcx>(
    mcx: Mcx<'mcx>,
    from: &[pg_wchar],
) -> PgResult<PgVec<'mcx, u8>> {
    pg_encoding_wchar2mb_with_len(mcx, database_encoding(), from)
}

pub fn pg_encoding_wchar2mb_with_len<'mcx>(
    mcx: Mcx<'mcx>,
    encoding: pg_enc,
    from: &[pg_wchar],
) -> PgResult<PgVec<'mcx, u8>> {
    let conv = pg_wchar_table[encoding as usize]
        .wchar2mb_with_len
        .unwrap_or_else(|| panic!("wchar2mb: client-only encoding {encoding} has no converter"));
    let cap = from.len() * pg_wchar_table[encoding as usize].maxmblen as usize + 1;
    let mut to: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, cap)?;
    to.resize(cap, 0);
    let n = conv(from, &mut to);
    to.truncate(n as usize);
    Ok(to)
}

/// Byte length of the leading character, bounded by the slice end (C's
/// `pg_mblen_range(mbstr, end)`); errors if the character overruns it.
pub fn pg_mblen_range(mbstr: &[u8]) -> PgResult<i32> {
    debug_assert!(!mbstr.is_empty());
    let length = pg_encoding_mblen(database_encoding(), mbstr);
    if length as usize > mbstr.len() {
        return Err(report_invalid_encoding_db(mbstr, length, mbstr.len() as i32));
    }
    Ok(length)
}

pub fn pg_mblen_with_len(mbstr: &[u8], limit: i32) -> PgResult<i32> {
    pg_encoding_mblen_with_len(database_encoding(), mbstr, limit)
}

/// [`pg_mblen_with_len`] parameterized by encoding (mbutils.c
/// `pg_mblen_with_len`, generalized the way `pg_encoding_mbstrlen_with_len`
/// generalizes `pg_mbstrlen_with_len`).
pub fn pg_encoding_mblen_with_len(encoding: pg_enc, mbstr: &[u8], limit: i32) -> PgResult<i32> {
    debug_assert!(limit >= 1);
    let length = pg_encoding_mblen(encoding, mbstr);
    if length > limit {
        return Err(report_invalid_encoding_int(encoding, mbstr, length, limit));
    }
    Ok(length)
}

/// No bounds check; only safe on already-verified strings (C `pg_mblen`).
pub fn pg_mblen(mbstr: &[u8]) -> i32 {
    pg_encoding_mblen(database_encoding(), mbstr)
}

pub fn pg_dsplen(mbstr: &[u8]) -> i32 {
    pg_encoding_dsplen(database_encoding(), mbstr)
}

/// Character count of a NUL- or slice-terminated string (C `pg_mbstrlen`).
pub fn pg_mbstrlen(mbstr: &[u8]) -> PgResult<i32> {
    if pg_database_encoding_max_length() == 1 {
        return Ok(c_string_len(mbstr) as i32);
    }
    let mut len = 0;
    let mut pos = 0usize;
    while pos < mbstr.len() && mbstr[pos] != 0 {
        pos += pg_mblen_range(&mbstr[pos..])? as usize;
        len += 1;
    }
    Ok(len)
}

/// Character count of the slice, stopping at a NUL (C `pg_mbstrlen_with_len`
/// with `limit` = the slice length). Matches C: ereports if the last
/// character's claimed length overruns the slice.
pub fn pg_mbstrlen_with_len(mbstr: &[u8]) -> PgResult<i32> {
    pg_encoding_mbstrlen_with_len(database_encoding(), mbstr)
}

/// [`pg_mbstrlen_with_len`] parameterized by encoding (parser callers thread
/// the server encoding explicitly).
pub fn pg_encoding_mbstrlen_with_len(encoding: pg_enc, mbstr: &[u8]) -> PgResult<i32> {
    if pg_encoding_max_length(encoding) == 1 {
        return Ok(mbstr.len() as i32);
    }
    let mut len = 0;
    let mut pos = 0usize;
    let mut limit = mbstr.len() as i32;
    while limit > 0 && mbstr[pos] != 0 {
        // ASCII bytes are mblen 1 in UTF-8; count a run in one SWAR sweep.
        // The `< 0x80` peek keeps the fast path OFF multibyte lead bytes
        // (ascii_run would return 0 there) — one already-loaded byte compare,
        // so a fully-multibyte string pays no fast-path tax. NUL is excluded
        // by the while guard. Every input scores identically to the plain
        // per-char loop; the run is pure acceleration.
        if encoding == PG_UTF8 && mbstr[pos] < 0x80 {
            let run = ascii_run(&mbstr[pos..]);
            pos += run;
            limit -= run as i32;
            len += run as i32;
            if limit <= 0 || mbstr[pos] == 0 {
                break;
            }
        }
        let l = pg_encoding_mblen_with_len(encoding, &mbstr[pos..], limit)?;
        limit -= l;
        pos += l as usize;
        len += 1;
    }
    Ok(len)
}

/// Length of the leading run of bytes in 0x01..=0x7F.
#[inline]
fn ascii_run(s: &[u8]) -> usize {
    const LO: u64 = 0x0101_0101_0101_0101;
    const HI: u64 = 0x8080_8080_8080_8080;
    let stop = |w: u64| ((w.wrapping_sub(LO) & !w) | w) & HI;
    let mut i = 0usize;
    while i + 16 <= s.len() {
        let sa = stop(u64::from_le_bytes(s[i..i + 8].try_into().unwrap()));
        let sb = stop(u64::from_le_bytes(s[i + 8..i + 16].try_into().unwrap()));
        if sa | sb != 0 {
            return if sa != 0 {
                i + (sa.trailing_zeros() / 8) as usize
            } else {
                i + 8 + (sb.trailing_zeros() / 8) as usize
            };
        }
        i += 16;
    }
    while i + 8 <= s.len() {
        let sa = stop(u64::from_le_bytes(s[i..i + 8].try_into().unwrap()));
        if sa != 0 {
            return i + (sa.trailing_zeros() / 8) as usize;
        }
        i += 8;
    }
    while i < s.len() && s[i].wrapping_sub(1) < 0x7f {
        i += 1;
    }
    i
}

pub fn pg_mbcliplen(mbstr: &[u8], len: i32, limit: i32) -> i32 {
    pg_encoding_mbcliplen(database_encoding(), mbstr, len, limit)
}

/// Byte length of the longest prefix of the first `len` bytes not exceeding
/// `limit` bytes without splitting a character; string must be valid.
pub fn pg_encoding_mbcliplen(encoding: pg_enc, mbstr: &[u8], len: i32, limit: i32) -> i32 {
    if pg_encoding_max_length(encoding) == 1 {
        return cliplen(mbstr, len, limit);
    }
    let mut clen = 0;
    let mut len = len;
    let mut pos = 0usize;
    while len > 0 && mbstr.get(pos).copied().unwrap_or(0) != 0 {
        let l = pg_encoding_mblen(encoding, &mbstr[pos..]);
        if clen + l > limit {
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

/// Like [`pg_mbcliplen`] with `limit` counted in characters.
pub fn pg_mbcharcliplen(mbstr: &[u8], len: i32, limit: i32) -> PgResult<i32> {
    if pg_database_encoding_max_length() == 1 {
        return Ok(cliplen(mbstr, len, limit));
    }
    let mut clen = 0;
    let mut nch = 0;
    let mut len = len;
    let mut pos = 0usize;
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

fn cliplen(s: &[u8], len: i32, limit: i32) -> i32 {
    let len = len.min(limit);
    let mut l = 0;
    while l < len && s.get(l as usize).copied().unwrap_or(0) != 0 {
        l += 1;
    }
    l
}

pub fn SetDatabaseEncoding(encoding: pg_enc) -> PgResult<()> {
    if !pg_valid_be_encoding(encoding) {
        return Err(internal_error(&format!(
            "invalid database encoding: {encoding}"
        )));
    }
    DATABASE_ENCODING.with(|e| e.set(encoding));
    Ok(())
}

pub fn SetMessageEncoding(encoding: pg_enc) {
    debug_assert!(pg_valid_encoding(encoding));
    MESSAGE_ENCODING.with(|e| e.set(encoding));
}

pub fn GetDatabaseEncoding() -> pg_enc {
    database_encoding()
}

pub fn GetDatabaseEncodingName() -> &'static str {
    enc_name(database_encoding())
}

pub fn GetMessageEncoding() -> pg_enc {
    MESSAGE_ENCODING.with(|e| e.get())
}

pub fn pg_database_encoding_max_length() -> i32 {
    pg_wchar_table[database_encoding() as usize].maxmblen
}

pub type MbcharacterIncrementer = fn(&mut [u8]) -> bool;

pub fn pg_database_encoding_character_incrementer() -> MbcharacterIncrementer {
    match database_encoding() {
        PG_UTF8 => pg_utf8_increment,
        PG_EUC_JP => pg_eucjp_increment,
        _ => pg_generic_charinc,
    }
}

pub fn pg_generic_charinc(charptr: &mut [u8]) -> bool {
    let encoding = database_encoding();
    let len = charptr.len() as i32;
    let last = charptr.len() - 1;
    while charptr[last] < 255 {
        charptr[last] += 1;
        if pg_encoding_verifymbchar(encoding, charptr) == len {
            return true;
        }
    }
    false
}

/// C's `switch (length)` cases 4..1 fall through; each successful increment
/// exits with success, and lengths outside 1..=4 are rejected.
pub fn pg_utf8_increment(charptr: &mut [u8]) -> bool {
    let length = charptr.len();
    if !(1..=4).contains(&length) {
        return false;
    }
    if length == 4 && charptr[3] < 0xBF {
        charptr[3] += 1;
        return true;
    }
    if length >= 3 && charptr[2] < 0xBF {
        charptr[2] += 1;
        return true;
    }
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
    let a = charptr[0];
    if a == 0x7F || a == 0xDF || a == 0xEF || a == 0xF4 {
        return false;
    }
    charptr[0] += 1;
    true
}

pub fn pg_eucjp_increment(charptr: &mut [u8]) -> bool {
    const SS2: u8 = 0x8e;
    const SS3: u8 = 0x8f;
    let length = charptr.len();
    let c1 = charptr[0];
    match c1 {
        SS2 => {
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
            if c1 & 0x80 != 0 {
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
                if c1 > 0x7e {
                    return false;
                }
                charptr[0] += 1;
                true
            }
        }
    }
}

pub fn pg_verifymbstr(mbstr: &[u8], no_error: bool) -> PgResult<bool> {
    pg_verify_mbstr(database_encoding(), mbstr, no_error)
}

pub fn pg_verify_mbstr(encoding: pg_enc, mbstr: &[u8], no_error: bool) -> PgResult<bool> {
    debug_assert!(pg_valid_encoding(encoding));
    let oklen = pg_encoding_verifymbstr(encoding, mbstr) as usize;
    if oklen != mbstr.len() {
        if no_error {
            return Ok(false);
        }
        return Err(report_invalid_encoding(encoding, &mbstr[oklen..]));
    }
    Ok(true)
}

/// Verify and count characters: `Ok(count)`, or `Ok(-1)` with `no_error`.
pub fn pg_verify_mbstr_len(encoding: pg_enc, mbstr: &[u8], no_error: bool) -> PgResult<i32> {
    debug_assert!(pg_valid_encoding(encoding));

    if pg_encoding_max_length(encoding) <= 1 {
        return match mbstr.iter().position(|&b| b == 0) {
            None => Ok(mbstr.len() as i32),
            Some(_) if no_error => Ok(-1),
            Some(nullpos) => Err(report_invalid_encoding(encoding, &mbstr[nullpos..])),
        };
    }

    let mut mb_len = 0;
    let mut pos = 0usize;
    while pos < mbstr.len() {
        let remaining = &mbstr[pos..];
        if remaining[0] & 0x80 == 0 {
            if remaining[0] == 0 {
                if no_error {
                    return Ok(-1);
                }
                return Err(report_invalid_encoding(encoding, remaining));
            }
            mb_len += 1;
            pos += 1;
            continue;
        }
        let l = pg_encoding_verifymbchar(encoding, remaining);
        if l < 0 {
            if no_error {
                return Ok(-1);
            }
            return Err(report_invalid_encoding(encoding, remaining));
        }
        pos += l as usize;
        mb_len += 1;
    }
    Ok(mb_len)
}

/// C `CHECK_ENCODING_CONVERSION_ARGS`; `-1` expected-encoding means "any".
pub fn check_encoding_conversion_args(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    len: i32,
    expected_src_encoding: pg_enc,
    expected_dest_encoding: pg_enc,
) -> PgResult<()> {
    if !pg_valid_encoding(src_encoding) {
        return Err(internal_error(&format!(
            "invalid source encoding ID: {src_encoding}"
        )));
    }
    if src_encoding != expected_src_encoding && expected_src_encoding >= 0 {
        return Err(internal_error(&format!(
            "expected source encoding \"{}\", but got \"{}\"",
            enc_name(expected_src_encoding),
            enc_name(src_encoding)
        )));
    }
    if !pg_valid_encoding(dest_encoding) {
        return Err(internal_error(&format!(
            "invalid destination encoding ID: {dest_encoding}"
        )));
    }
    if dest_encoding != expected_dest_encoding && expected_dest_encoding >= 0 {
        return Err(internal_error(&format!(
            "expected destination encoding \"{}\", but got \"{}\"",
            enc_name(expected_dest_encoding),
            enc_name(dest_encoding)
        )));
    }
    if len < 0 {
        return Err(internal_error(
            "encoding conversion length must not be negative",
        ));
    }
    Ok(())
}

#[cold]
pub fn report_invalid_encoding(encoding: pg_enc, mbstr: &[u8]) -> Box<PgError> {
    let l = pg_encoding_mblen_or_incomplete(encoding, mbstr);
    report_invalid_encoding_int(encoding, mbstr, l, mbstr.len() as i32)
}

#[track_caller]
#[cold]
fn report_invalid_encoding_int(
    encoding: pg_enc,
    mbstr: &[u8],
    mblen: i32,
    len: i32,
) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "invalid byte sequence for encoding \"{}\": {}",
            enc_name(encoding),
            byte_sequence(mbstr, mblen, len)
        ))
        .with_sqlstate(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
    )
}

#[track_caller]
#[cold]
fn report_invalid_encoding_db(mbstr: &[u8], mblen: i32, len: i32) -> Box<PgError> {
    report_invalid_encoding_int(database_encoding(), mbstr, mblen, len)
}

#[cold]
pub fn report_untranslatable_char(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    mbstr: &[u8],
) -> Box<PgError> {
    let l = pg_encoding_mblen_or_incomplete(src_encoding, mbstr);
    Box::new(
        PgError::error(format!(
            "character with byte sequence {} in encoding \"{}\" has no equivalent in encoding \"{}\"",
            byte_sequence(mbstr, l, mbstr.len() as i32),
            enc_name(src_encoding),
            enc_name(dest_encoding)
        ))
        .with_sqlstate(ERRCODE_UNTRANSLATABLE_CHARACTER),
    )
}

/// The leading `min(mblen, len, 8)` bytes as space-separated `0xNN`.
// pub for proofs/text-slice (Kani stubs the message-text construction;
// visibility-only change per the 2026-07-28 shipped-edits ruling).
pub fn byte_sequence(mbstr: &[u8], mblen: i32, len: i32) -> String {
    let jlimit = (mblen.min(len).max(0) as usize).min(8).min(mbstr.len());
    let mut p = String::with_capacity(jlimit * 5);
    for (j, b) in mbstr[..jlimit].iter().enumerate() {
        if j > 0 {
            p.push(' ');
        }
        p.push_str(&format!("0x{b:02x}"));
    }
    p
}

fn c_string_len(bytes: &[u8]) -> usize {
    bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len())
}

#[track_caller]
#[cold]
fn internal_error(message: &str) -> Box<PgError> {
    Box::new(PgError::error(message.to_string()))
}

#[track_caller]
#[cold]
fn too_long_error(len: usize) -> Box<PgError> {
    Box::new(
        PgError::error("out of memory")
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .with_detail(format!(
                "String of {len} bytes is too long for encoding conversion."
            )),
    )
}

#[track_caller]
#[cold]
fn no_default_conversion_error(src_encoding: pg_enc, dest_encoding: pg_enc) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "default conversion function for encoding \"{}\" to \"{}\" does not exist",
            enc_name(src_encoding),
            enc_name(dest_encoding)
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION),
    )
}

#[track_caller]
#[cold]
fn conversion_not_supported_error(src_encoding: pg_enc, dest_encoding: pg_enc) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "conversion between {} and {} is not supported",
            enc_name(src_encoding),
            enc_name(dest_encoding)
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

#[track_caller]
#[cold]
fn conversion_not_supported_fatal(pending: pg_enc) -> Box<PgError> {
    Box::new(
        PgError::new(
            FATAL,
            format!(
                "conversion between {} and {} is not supported",
                enc_name(pending),
                GetDatabaseEncodingName()
            ),
        )
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

#[track_caller]
#[cold]
fn invalid_byte_value_error(b: u8) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "invalid byte value for encoding \"{}\": 0x{b:02x}",
            enc_name(PG_SQL_ASCII)
        ))
        .with_sqlstate(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
    )
}

#[track_caller]
#[cold]
fn invalid_codepoint_error() -> Box<PgError> {
    Box::new(PgError::error("invalid Unicode code point").with_sqlstate(ERRCODE_SYNTAX_ERROR))
}

pub fn init_seams() {
    use mbutils_seams as seams;
    seams::pg_server_to_client::set(pg_server_to_client);
    seams::server_to_client_conversion_needed::set(server_to_client_conversion_needed);
    seams::pg_database_encoding_max_length::set(pg_database_encoding_max_length);
    seams::pg_mbstrlen_with_len::set(pg_mbstrlen_with_len);
    seams::pg_mblen_range::set(pg_mblen_range);
    seams::get_database_encoding::set(GetDatabaseEncoding);
    seams::get_database_encoding_name::set(GetDatabaseEncodingName);
    seams::set_database_encoding::set(SetDatabaseEncoding);
    seams::initialize_client_encoding::set(InitializeClientEncoding);
    seams::pg_mbcliplen::set(pg_mbcliplen);
    seams::pg_mb2wchar_with_len::set(pg_mb2wchar_with_len);
    seams::pg_wchar2mb_with_len::set(pg_wchar2mb_with_len);
    seams::pg_encoding_mblen::set(wchar::pg_encoding_mblen);
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod divergence10_regression {
    // Divergence #10: byte-wise cleaning must match C for non-UTF-8 names
    // (reachable via SQL_ASCII; ground truth glibc PG 18.4: 'utf\xF18' -> 6).
    #[test]
    fn non_utf8_name_cleans_byte_wise() {
        assert_eq!(super::pg_char_to_encoding_bytes(b"utf\xF18"), 6);
        assert_eq!(super::pg_char_to_encoding_bytes(b"utf\xFF8"), 6);
        assert_eq!(super::pg_char_to_encoding_bytes(b"utfx8"), -1); // alnum kept
        assert_eq!(super::pg_char_to_encoding_bytes(b"utf8"), 6);
        assert_eq!(super::pg_char_to_encoding_bytes(b""), -1);
    }
}
