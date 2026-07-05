use mcx::{vec_with_capacity_in, Mcx, PgVec};
use crate::scanner_isspace;
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

fn pg_unicode_to_server(c: pg_wchar, out: &mut PgVec<'_, u8>, server_encoding: pg_enc) {
    if server_encoding != PG_UTF8 {
        panic!(
            "pg_unicode_to_server: non-UTF8 server encoding needs the mbutils.c \
             conversion layer (backend-utils-mb unported)"
        );
    }
    let mut buf = [0u8; 4];
    unicode_to_utf8(c, &mut buf);
    let n = unicode_utf8len(c) as usize;
    mcx::vec_append_bytes(out, &buf[..n]).unwrap_or_else(|e| panic!("{}", e.message()));
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
) -> Result<PgVec<'mcx, u8>, UdeescapeError> {
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
                    return Err(invalid_pair(escpos));
                }
                out.push(escape);
                i += 2;
            } else if (1..=4).all(|k| byte(i + k).is_ascii_hexdigit()) {
                let unicode = (hexval(byte(i + 1)) << 12)
                    + (hexval(byte(i + 2)) << 8)
                    + (hexval(byte(i + 3)) << 4)
                    + hexval(byte(i + 4));
                emit(&mut out, &mut pair_first, unicode, escpos, server_encoding)?;
                i += 5;
            } else if byte(i + 1) == b'+' && (2..=7).all(|k| byte(i + k).is_ascii_hexdigit()) {
                let unicode = (hexval(byte(i + 2)) << 20)
                    + (hexval(byte(i + 3)) << 16)
                    + (hexval(byte(i + 4)) << 12)
                    + (hexval(byte(i + 5)) << 8)
                    + (hexval(byte(i + 6)) << 4)
                    + hexval(byte(i + 7));
                emit(&mut out, &mut pair_first, unicode, escpos, server_encoding)?;
                i += 8;
            } else {
                return Err(UdeescapeError {
                    message: "invalid Unicode escape",
                    location: escpos,
                    hint: Some("Unicode escapes must be \\XXXX or \\+XXXXXX."),
                });
            }
        } else {
            if pair_first != 0 {
                return Err(invalid_pair(escpos_at(i)));
            }
            out.push(s[i]);
            i += 1;
        }
    }
    if pair_first != 0 {
        return Err(invalid_pair(escpos_at(i)));
    }
    Ok(out)
}

// The shared value-check + surrogate-pair + emit tail of both escape arms.
fn emit(
    out: &mut PgVec<'_, u8>,
    pair_first: &mut pg_wchar,
    unicode: pg_wchar,
    escpos: i32,
    server_encoding: pg_enc,
) -> Result<(), UdeescapeError> {
    check_unicode_value(unicode, escpos)?;
    let invalid_pair =
        UdeescapeError { message: "invalid Unicode surrogate pair", location: escpos, hint: None };
    let cp = if *pair_first != 0 {
        if !is_utf16_surrogate_second(unicode) {
            return Err(invalid_pair);
        }
        let cp = surrogate_pair_to_codepoint(*pair_first, unicode);
        *pair_first = 0;
        cp
    } else if is_utf16_surrogate_second(unicode) {
        return Err(invalid_pair);
    } else if is_utf16_surrogate_first(unicode) {
        *pair_first = unicode;
        return Ok(());
    } else {
        unicode
    };
    pg_unicode_to_server(cp, out, server_encoding);
    Ok(())
}
