#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

//! `backend-utils-adt-ascii` — port of PostgreSQL 18.3
//! `src/backend/utils/adt/ascii.c`: the `to_ascii` family of functions, which
//! transliterate the high (8-bit) characters of a handful of Latin/Windows
//! single-byte encodings down to plain 7-bit ASCII, plus the
//! `ascii_safe_strlcpy` helper.
//!
//! | C symbol            | this crate                  | role                                   |
//! |---------------------|-----------------------------|----------------------------------------|
//! | `pg_to_ascii`       | [`pg_to_ascii`]             | the transliteration core (table-driven)|
//! | `encode_to_ascii`   | [`encode_to_ascii`]         | `text` → `text` wrapper over the core  |
//! | `to_ascii_encname`  | [`to_ascii_encname`]        | SQL `to_ascii(text, name)`             |
//! | `to_ascii_enc`      | [`to_ascii_enc`]            | SQL `to_ascii(text, int)`              |
//! | `to_ascii_default`  | [`to_ascii_default`]        | SQL `to_ascii(text)`                   |
//! | `ascii_safe_strlcpy`| [`ascii_safe_strlcpy`]      | safe ASCII-only `strlcpy`              |
//!
//! `text` is pass-by-reference; at the fmgr boundary
//! ([`types_fmgr::boundary`]) its referent is carried as an owned
//! [`RefPayload::Varlena`] holding the content bytes. The ASCII
//! transliteration is a byte-for-byte 1:1 mapping, so the output `text` is
//! exactly as long as the input (C `encode_to_ascii` rewrites the detoasted
//! buffer in place and returns it unchanged in length).
//!
//! The only allocation is the returned value itself (the caller's owned
//! result, the `palloc`-into-the-caller's-context analog). It is grown
//! OOM-safely with `try_reserve_exact` against the validated input length.

pub mod fmgr_builtins;

use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_OUT_OF_MEMORY,
    ERRCODE_UNDEFINED_OBJECT,
};
use types_fmgr::boundary::{FmgrArg, FmgrOut, RefPayload};
use types_wchar::encoding::{
    pg_enc, pg_valid_encoding, PG_LATIN1, PG_LATIN2, PG_LATIN9, PG_WIN1250,
};

/// `RANGE_128` (ascii.c:38): high-byte range start for WIN1250.
const RANGE_128: u8 = 128;
/// `RANGE_160` (ascii.c:39): high-byte range start for the Latin encodings.
const RANGE_160: u8 = 160;

// The per-encoding transliteration tables (ascii.c:46-70). Each maps the byte
// values `[range, 256)` (index `byte - range`) to a 7-bit ASCII stand-in;
// bytes in `[128, range)` collapse to a space and bytes `< 128` pass through.
// Byte-for-byte the C string literals (the terminating NUL is not part of the
// table — C indexes only `byte - range < 256 - range` entries).

/// LATIN1 (ISO 8859-1) → ASCII (ascii.c:46).
const LATIN1_ASCII: &[u8] =
    b"  cL Y  \"Ca  -R     'u .,      ?AAAAAAACEEEEIIII NOOOOOxOUUUUYTBaaaaaaaceeeeiiii nooooo/ouuuuyty";
/// LATIN2 (ISO 8859-2) → ASCII (ascii.c:54).
const LATIN2_ASCII: &[u8] =
    b" A L LS \"SSTZ-ZZ a,l'ls ,sstz\"zzRAAAALCCCEEEEIIDDNNOOOOxRUUUUYTBraaaalccceeeeiiddnnoooo/ruuuuyt.";
/// LATIN9 (ISO 8859-15) → ASCII (ascii.c:62).
const LATIN9_ASCII: &[u8] =
    b"  cL YS sCa  -R     Zu .z   EeY?AAAAAAACEEEEIIII NOOOOOxOUUUUYTBaaaaaaaceeeeiiii nooooo/ouuuuyty";
/// WIN1250 (Windows-1250) → ASCII (ascii.c:70).
const WIN1250_ASCII: &[u8] =
    b"  ' \"    %S<STZZ `'\"\".--  s>stzz   L A  \"CS  -RZ  ,l'u .,as L\"lzRAAAALCCCEEEEIIDDNNOOOOxRUUUUYTBraaaalccceeeeiiddnnoooo/ruuuuyt ";

/// `pg_to_ascii` (ascii.c:28): transliterate the high characters of `src` (the
/// `text` content bytes) down to plain ASCII for the supported encodings,
/// returning the owned result (`dest` in C, the same length as `src`).
///
/// * bytes `< 128` pass through unchanged;
/// * bytes in `[128, range)` become a space (`' '`);
/// * bytes in `[range, 256)` map through the encoding's table.
///
/// `range` is `RANGE_160` for the Latin encodings and `RANGE_128` for WIN1250.
/// An unsupported encoding is the C `ereport(ERROR, FEATURE_NOT_SUPPORTED)`.
pub fn pg_to_ascii(src: &[u8], enc: pg_enc) -> PgResult<Vec<u8>> {
    let (ascii, range) = ascii_table(enc)?;

    // The result is exactly as long as the input (1:1 byte mapping). Grow it
    // OOM-safely against that validated bound (project HARD RULE).
    let mut dest = Vec::new();
    dest.try_reserve_exact(src.len())
        .map_err(|_| out_of_memory())?;

    for &byte in src {
        if byte < 128 {
            // C: *dest++ = *x;
            dest.push(byte);
        } else if byte < range {
            // C: *dest++ = ' ';
            dest.push(b' ');
        } else {
            // C: *dest++ = ascii[*x - range];
            dest.push(ascii[(byte - range) as usize]);
        }
    }

    Ok(dest)
}

/// `encode_to_ascii` (ascii.c:103): the `text` → `text` wrapper. `data` is the
/// input `text` content bytes; the result is the transliterated content (same
/// length), to be re-wrapped as a `text` payload by the SQL entry points.
pub fn encode_to_ascii(data: &[u8], enc: pg_enc) -> PgResult<Vec<u8>> {
    pg_to_ascii(data, enc)
}

/// `to_ascii_encname` (ascii.c:119): SQL `to_ascii(string text, encoding name)`.
///
/// Resolves `encname` to an encoding id (C `pg_char_to_encoding`); a negative
/// result is the C `ereport(ERROR, UNDEFINED_OBJECT, "%s is not a valid
/// encoding name")`. Then transliterates `data` to an ASCII `text` payload.
pub fn to_ascii_encname<'mcx>(data: FmgrArg<'_, 'mcx>, encname: &str) -> PgResult<FmgrOut<'mcx>> {
    let enc = common_encnames_seams::pg_char_to_encoding::call(encname);
    if enc < 0 {
        return Err(invalid_encoding_name(encname));
    }

    encode_to_ascii_arg(data, enc)
}

/// `to_ascii_enc` (ascii.c:138): SQL `to_ascii(string text, encoding int)`.
///
/// Validates `enc` against `PG_VALID_ENCODING`; a bad code is the C
/// `ereport(ERROR, UNDEFINED_OBJECT, "%d is not a valid encoding code")`. Then
/// transliterates `data` to an ASCII `text` payload.
pub fn to_ascii_enc<'mcx>(data: FmgrArg<'_, 'mcx>, enc: pg_enc) -> PgResult<FmgrOut<'mcx>> {
    if !pg_valid_encoding(enc) {
        return Err(invalid_encoding_code(enc));
    }

    encode_to_ascii_arg(data, enc)
}

/// `to_ascii_default` (ascii.c:156): SQL `to_ascii(string text)` —
/// transliterate using the current database encoding (`GetDatabaseEncoding()`,
/// via the provider-owned mbutils seam).
pub fn to_ascii_default<'mcx>(data: FmgrArg<'_, 'mcx>) -> PgResult<FmgrOut<'mcx>> {
    let enc = backend_utils_mb_mbutils_seams::get_database_encoding::call();
    encode_to_ascii_arg(data, enc)
}

/// `ascii_safe_strlcpy` (ascii.c:174): a `strlcpy` that copies at most
/// `dest.len() - 1` bytes of `src`, NUL-terminating `dest`, and replaces any
/// byte that is neither printable ASCII (`32..=127`) nor one of `\n`/`\r`/`\t`
/// with `'?'`. Copying stops at the first NUL in `src`. A zero-length `dest` is
/// a no-op (matching C's `if (destsiz == 0) return`). Must never raise — it is
/// called in the postmaster.
pub fn ascii_safe_strlcpy(dest: &mut [u8], src: &[u8]) {
    // C: if (destsiz == 0) return; /* corner case: no room for trailing nul */
    if dest.is_empty() {
        return;
    }

    // C: while (--destsiz > 0) { ch = *src++; if (ch == '\0') break; ... }
    // i.e. at most destsiz-1 bytes are written, leaving room for the NUL.
    let mut written = 0;
    for &ch in src {
        if written + 1 >= dest.len() || ch == 0 {
            break;
        }

        // C: if (32 <= ch && ch <= 127) *dest = ch;
        //    else if (ch == '\n' || ch == '\r' || ch == '\t') *dest = ch;
        //    else *dest = '?';
        dest[written] = if (32..=127).contains(&ch) || matches!(ch, b'\n' | b'\r' | b'\t') {
            ch
        } else {
            b'?'
        };
        written += 1;
    }

    // C: *dest = '\0';
    dest[written] = 0;
}

/// Shared body of the three `to_ascii_*` entry points: borrow the input `text`
/// content from the [`FmgrArg`], transliterate, and re-wrap as an owned `text`
/// payload [`FmgrOut`].
fn encode_to_ascii_arg<'mcx>(data: FmgrArg<'_, 'mcx>, enc: pg_enc) -> PgResult<FmgrOut<'mcx>> {
    let bytes = arg_text(data);
    let out = encode_to_ascii(bytes, enc)?;
    Ok(FmgrOut::Ref(RefPayload::Varlena(out)))
}

/// `PG_GETARG_TEXT_P_COPY(0)` → `VARDATA`: borrow the by-reference `text`
/// referent's content bytes. (The C copy is implicit: the port produces a
/// fresh result `Vec` rather than rewriting in place.)
fn arg_text<'a>(arg: FmgrArg<'a, '_>) -> &'a [u8] {
    match arg {
        FmgrArg::Ref(RefPayload::Varlena(b)) => b.as_slice(),
        FmgrArg::Ref(RefPayload::Cstring(s)) => s.as_bytes(),
        FmgrArg::Ref(RefPayload::Expanded(_)) | FmgrArg::ByVal(_) => &[],
    }
}

/// Select the transliteration table and high-byte range for `enc`
/// (ascii.c:41-78). An unsupported encoding is the C
/// `ereport(ERROR, FEATURE_NOT_SUPPORTED)`.
fn ascii_table(enc: pg_enc) -> PgResult<(&'static [u8], u8)> {
    match enc {
        PG_LATIN1 => Ok((LATIN1_ASCII, RANGE_160)),
        PG_LATIN2 => Ok((LATIN2_ASCII, RANGE_160)),
        PG_LATIN9 => Ok((LATIN9_ASCII, RANGE_160)),
        PG_WIN1250 => Ok((WIN1250_ASCII, RANGE_128)),
        _ => Err(unsupported_encoding(enc)),
    }
}

/// C: `ereport(ERROR, errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("encoding
/// conversion from %s to ASCII not supported", pg_encoding_to_char(enc)))`
/// (ascii.c:75).
fn unsupported_encoding(enc: pg_enc) -> PgError {
    let name = common_encnames_seams::pg_encoding_to_char::call(enc);
    PgError::error(format!(
        "encoding conversion from {name} to ASCII not supported"
    ))
    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

/// C: `ereport(ERROR, errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("%s is not a
/// valid encoding name", encname))` (ascii.c:126).
fn invalid_encoding_name(encname: &str) -> PgError {
    PgError::error(format!("{encname} is not a valid encoding name"))
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT)
}

/// C: `ereport(ERROR, errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("%d is not a
/// valid encoding code", enc))` (ascii.c:144).
fn invalid_encoding_code(enc: pg_enc) -> PgError {
    PgError::error(format!("{enc} is not a valid encoding code"))
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT)
}

/// The `palloc`-out-of-memory analog: `ereport(ERROR, OUT_OF_MEMORY)`. The
/// fmgr-boundary result is a global-allocator `Vec`, so OOM is surfaced
/// directly rather than via an `Mcx` handle.
fn out_of_memory() -> PgError {
    PgError::error("out of memory").with_sqlstate(ERRCODE_OUT_OF_MEMORY)
}

/// Install this crate's seams. Registers the `to_ascii` SQL builtins into the
/// fmgr-core builtin table (C: `fmgr_builtins[]`). Called once at startup by
/// `seams-init::init_all`.
pub fn init_seams() {
    fmgr_builtins::register_ascii_builtins();
}

#[cfg(test)]
mod tests;
