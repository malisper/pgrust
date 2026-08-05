use alloc::boxed::Box;
use mcx::{vec_with_capacity_in, Mcx, PgVec};
use crate::scanner_isspace;
use types_error::PgError;
use wchar::{
    is_utf16_surrogate_first, is_utf16_surrogate_second, is_valid_unicode_codepoint, pg_enc,
    pg_wchar, surrogate_pair_to_codepoint, unicode_to_utf8, unicode_utf8len, PG_UTF8,
};

/// A U&-literal de-escape failure. `location` is the raw byte offset of the
/// offending escape (C's `in - str + position + 3`); the caller (base_yylex)
/// renders it as ereport(ERROR, ERRCODE_SYNTAX_ERROR, ...) with the cursor
/// run through scanner_errposition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UdeescapeError {
    pub message: &'static str,
    pub location: i32,
    pub hint: Option<&'static str>,
}

/// Either one of str_udeescape's own escape errors or a hard error out of
/// pg_unicode_to_server (0A000). C wraps each escape's processing —
/// including the pg_unicode_to_server calls — in
/// setup_scanner_errposition_callback(.., in - str + position + 3)
/// (parser.c: "Any errors reported while processing this escape sequence
/// will have an error cursor pointing at the escape"), so both kinds carry
/// the escape's offset and the caller renders the cursor.
#[derive(Debug)]
pub enum UdeescapeFailure {
    Escape(UdeescapeError),
    Hard {
        error: Box<PgError>,
        /// Byte offset of the escape whose conversion failed
        /// (C's `in - str + position + 3`).
        location: i32,
    },
}

impl From<UdeescapeError> for UdeescapeFailure {
    fn from(e: UdeescapeError) -> Self {
        UdeescapeFailure::Escape(e)
    }
}

// Call sites guard with is_ascii_hexdigit (C's unreached elog).
fn hexval(c: u8) -> u32 {
    match c {
        b'0'..=b'9' => u32::from(c - b'0'),
        b'a'..=b'f' => u32::from(c - b'a') + 0xA,
        b'A'..=b'F' => u32::from(c - b'A') + 0xA,
        _ => unreachable!("invalid hexadecimal digit"),
    }
}

fn check_unicode_value(c: pg_wchar, escpos: i32) -> Result<(), UdeescapeError> {
    if is_valid_unicode_codepoint(c) {
        Ok(())
    } else {
        Err(UdeescapeError { message: "invalid Unicode escape value", location: escpos, hint: None })
    }
}

pub fn check_uescapechar(escape: u8) -> bool {
    !(escape.is_ascii_hexdigit()
        || escape == b'+'
        || escape == b'\''
        || escape == b'"'
        || scanner_isspace(escape))
}

// pg_unicode_to_server (mbutils.c) with the ASCII / UTF-8-server fast paths
// inlined; other server encodings run the mbutils conversion-proc lane,
// whose errors (untranslatable character / missing default conversion) C
// reports with an error cursor at the escape via str_udeescape's
// setup_scanner_errposition_callback; `escpos` threads that offset.
fn pg_unicode_to_server<'mcx>(
    mcx: Mcx<'mcx>,
    c: pg_wchar,
    out: &mut PgVec<'mcx, u8>,
    server_encoding: pg_enc,
    escpos: i32,
) -> Result<(), UdeescapeFailure> {
    if c > 0x7F && server_encoding != PG_UTF8 {
        // The encoding parameter is a port-ism (C reads GetDatabaseEncoding
        // itself); the conversion below targets the database encoding, so a
        // drifted caller must fail loudly rather than emit wrong bytes.
        assert_eq!(
            server_encoding,
            mbutils::GetDatabaseEncoding(),
            "str_udeescape encoding drifted from the database encoding"
        );
        let bytes = mbutils::pg_unicode_to_server(mcx, c)
            .map_err(|error| UdeescapeFailure::Hard { error, location: escpos })?;
        mcx::vec_append_bytes(out, &bytes).unwrap_or_else(|e| panic!("{}", e.message()));
        return Ok(());
    }
    let mut buf = [0u8; 4];
    unicode_to_utf8(c, &mut buf);
    let n = unicode_utf8len(c) as usize;
    mcx::vec_append_bytes(out, &buf[..n]).unwrap_or_else(|e| panic!("{}", e.message()));
    Ok(())
}

/// `str_udeescape` (parser.c): decode the Unicode escapes of a `U&'...'` /
/// `U&"..."` body into a server-encoding string in `mcx`. `position` is the
/// byte offset of the token start (error cursors).
pub fn str_udeescape<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
    escape: u8,
    position: i32,
    server_encoding: pg_enc,
) -> Result<PgVec<'mcx, u8>, UdeescapeFailure> {
    let mut out: PgVec<'mcx, u8> =
        vec_with_capacity_in(mcx, s.len()).unwrap_or_else(|e| panic!("{}", e.message()));
    let mut pair_first: pg_wchar = 0;
    // NUL-terminated-buffer reads past the end come back 0, as in C.
    let byte = |off: usize| -> u8 { s.get(off).copied().unwrap_or(0) };
    let escpos_at = |i: usize| -> i32 { i as i32 + position + 3 }; // 3 for U&"
    let invalid_pair = |at: i32| UdeescapeError {
        message: "invalid Unicode surrogate pair",
        location: at,
        hint: None,
    };

    let mut i = 0usize;
    while i < s.len() {
        if s[i] == escape {
            let escpos = escpos_at(i);
            if byte(i + 1) == escape {
                if pair_first != 0 {
                    return Err(invalid_pair(escpos).into());
                }
                out.push(escape);
                i += 2;
            } else if (1..=4).all(|k| byte(i + k).is_ascii_hexdigit()) {
                let unicode = (hexval(byte(i + 1)) << 12)
                    + (hexval(byte(i + 2)) << 8)
                    + (hexval(byte(i + 3)) << 4)
                    + hexval(byte(i + 4));
                emit(mcx, &mut out, &mut pair_first, unicode, escpos, server_encoding)?;
                i += 5;
            } else if byte(i + 1) == b'+' && (2..=7).all(|k| byte(i + k).is_ascii_hexdigit()) {
                let unicode = (hexval(byte(i + 2)) << 20)
                    + (hexval(byte(i + 3)) << 16)
                    + (hexval(byte(i + 4)) << 12)
                    + (hexval(byte(i + 5)) << 8)
                    + (hexval(byte(i + 6)) << 4)
                    + hexval(byte(i + 7));
                emit(mcx, &mut out, &mut pair_first, unicode, escpos, server_encoding)?;
                i += 8;
            } else {
                return Err(UdeescapeError {
                    message: "invalid Unicode escape",
                    location: escpos,
                    hint: Some("Unicode escapes must be \\XXXX or \\+XXXXXX."),
                }
                .into());
            }
        } else {
            if pair_first != 0 {
                return Err(invalid_pair(escpos_at(i)).into());
            }
            out.push(s[i]);
            i += 1;
        }
    }
    if pair_first != 0 {
        return Err(invalid_pair(escpos_at(i)).into());
    }
    Ok(out)
}

// The shared value-check + surrogate-pair + emit tail of both escape arms.
fn emit<'mcx>(
    mcx: Mcx<'mcx>,
    out: &mut PgVec<'mcx, u8>,
    pair_first: &mut pg_wchar,
    unicode: pg_wchar,
    escpos: i32,
    server_encoding: pg_enc,
) -> Result<(), UdeescapeFailure> {
    check_unicode_value(unicode, escpos)?;
    let invalid_pair =
        UdeescapeError { message: "invalid Unicode surrogate pair", location: escpos, hint: None };
    let cp = if *pair_first != 0 {
        if !is_utf16_surrogate_second(unicode) {
            return Err(invalid_pair.into());
        }
        let cp = surrogate_pair_to_codepoint(*pair_first, unicode);
        *pair_first = 0;
        cp
    } else if is_utf16_surrogate_second(unicode) {
        return Err(invalid_pair.into());
    } else if is_utf16_surrogate_first(unicode) {
        *pair_first = unicode;
        return Ok(());
    } else {
        unicode
    };
    pg_unicode_to_server(mcx, cp, out, server_encoding, escpos)
}
