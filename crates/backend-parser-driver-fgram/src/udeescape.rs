//! Unicode de-escaping support (`parser.c:326-527`): `hexval`,
//! `check_unicode_value`, `check_uescapechar`, and `str_udeescape`.

use crate::{
    is_utf16_surrogate_first, is_utf16_surrogate_second, is_valid_unicode_codepoint,
    surrogate_pair_to_codepoint, PgWchar,
};
use backend_parser_scan::UnicodeToServerSeam;
use backend_parser_scansup::scanner_isspace;

/// An error raised while de-escaping a `U&'...'`/`U&"..."` literal.
#[derive(Clone, Debug)]
pub struct DeError {
    pub message: String,
    pub location: i32,
    /// `errhint` text (C: the `"invalid Unicode escape"` error carries
    /// `errhint("Unicode escapes must be \\XXXX or \\+XXXXXX.")`; the value- and
    /// surrogate-pair errors carry none).
    pub hint: Option<&'static str>,
}

/// `hexval()` (parser.c:327) -- value of a hex digit (caller verified).
fn hexval(c: u8) -> u32 {
    if c.is_ascii_digit() {
        (c - b'0') as u32
    } else if (b'a'..=b'f').contains(&c) {
        (c - b'a') as u32 + 0xA
    } else if (b'A'..=b'F').contains(&c) {
        (c - b'A') as u32 + 0xA
    } else {
        // elog(ERROR, "invalid hexadecimal digit") -- not reached given the
        // isxdigit() guards at every call site.
        0
    }
}

/// `check_unicode_value()` (parser.c:341).
fn check_unicode_value(c: PgWchar, position: i32) -> Result<(), DeError> {
    if !is_valid_unicode_codepoint(c) {
        Err(DeError {
            message: "invalid Unicode escape value".to_string(),
            location: position,
            hint: None,
        })
    } else {
        Ok(())
    }
}

/// `check_uescapechar()` (parser.c:351) -- is `escape` acceptable as the
/// Unicode escape character in the `UESCAPE` syntax?
pub fn check_uescapechar(escape: u8) -> bool {
    !(escape.is_ascii_hexdigit()
        || escape == b'+'
        || escape == b'\''
        || escape == b'"'
        || scanner_isspace(escape))
}

/// `str_udeescape()` (parser.c:371) -- process Unicode escapes in `str`,
/// producing a plain string in the server encoding.
///
/// `escape` is the escape character; `position` is the byte offset of the
/// `U&'`/`U&"` token start (used for error cursors); `seam` converts code
/// points to the server encoding.
pub fn str_udeescape(
    str_: &[u8],
    escape: u8,
    position: i32,
    seam: &dyn UnicodeToServerSeam,
) -> Result<Vec<u8>, DeError> {
    let mut out: Vec<u8> = Vec::with_capacity(str_.len());
    let mut pair_first: PgWchar = 0;
    let in_bytes = str_;
    let mut i = 0usize;

    // byte() returns the byte at offset off into the *original* string, or 0
    // (NUL) at/after the end -- matching C's reads of in[1..7] past a possible
    // terminator (the buffer there is NUL-terminated).
    let byte = |off: usize| -> u8 { in_bytes.get(off).copied().unwrap_or(0) };

    while i < in_bytes.len() {
        if in_bytes[i] == escape {
            // Error cursor for this escape sequence: in - str + position + 3.
            let escpos = i as i32 + position + 3;
            if byte(i + 1) == escape {
                if pair_first != 0 {
                    return Err(invalid_pair(escpos));
                }
                out.push(escape);
                i += 2;
            } else if byte(i + 1).is_ascii_hexdigit()
                && byte(i + 2).is_ascii_hexdigit()
                && byte(i + 3).is_ascii_hexdigit()
                && byte(i + 4).is_ascii_hexdigit()
            {
                let unicode = (hexval(byte(i + 1)) << 12)
                    + (hexval(byte(i + 2)) << 8)
                    + (hexval(byte(i + 3)) << 4)
                    + hexval(byte(i + 4));
                check_unicode_value(unicode, escpos)?;
                let cp = combine_pair(&mut pair_first, unicode, escpos)?;
                if let Some(cp) = cp {
                    append_codepoint(&mut out, cp, seam, escpos)?;
                }
                i += 5;
            } else if byte(i + 1) == b'+'
                && byte(i + 2).is_ascii_hexdigit()
                && byte(i + 3).is_ascii_hexdigit()
                && byte(i + 4).is_ascii_hexdigit()
                && byte(i + 5).is_ascii_hexdigit()
                && byte(i + 6).is_ascii_hexdigit()
                && byte(i + 7).is_ascii_hexdigit()
            {
                let unicode = (hexval(byte(i + 2)) << 20)
                    + (hexval(byte(i + 3)) << 16)
                    + (hexval(byte(i + 4)) << 12)
                    + (hexval(byte(i + 5)) << 8)
                    + (hexval(byte(i + 6)) << 4)
                    + hexval(byte(i + 7));
                check_unicode_value(unicode, escpos)?;
                let cp = combine_pair(&mut pair_first, unicode, escpos)?;
                if let Some(cp) = cp {
                    append_codepoint(&mut out, cp, seam, escpos)?;
                }
                i += 8;
            } else {
                return Err(DeError {
                    message: "invalid Unicode escape".to_string(),
                    location: escpos,
                    hint: Some("Unicode escapes must be \\XXXX or \\+XXXXXX."),
                });
            }
        } else {
            if pair_first != 0 {
                return Err(invalid_pair(i as i32 + position + 3));
            }
            out.push(in_bytes[i]);
            i += 1;
        }
    }

    // Unfinished surrogate pair?
    if pair_first != 0 {
        return Err(invalid_pair(i as i32 + position + 3));
    }

    Ok(out)
}

/// The surrogate-pair combining logic shared by the two hex-escape arms of
/// `str_udeescape`.  Updates `pair_first`, returning `Some(codepoint)` to emit
/// or `None` when the first half of a pair was just stored.
fn combine_pair(
    pair_first: &mut PgWchar,
    unicode: PgWchar,
    escpos: i32,
) -> Result<Option<PgWchar>, DeError> {
    if *pair_first != 0 {
        if is_utf16_surrogate_second(unicode) {
            let cp = surrogate_pair_to_codepoint(*pair_first, unicode);
            *pair_first = 0;
            return Ok(Some(cp));
        } else {
            return Err(invalid_pair(escpos));
        }
    } else if is_utf16_surrogate_second(unicode) {
        return Err(invalid_pair(escpos));
    }

    if is_utf16_surrogate_first(unicode) {
        *pair_first = unicode;
        Ok(None)
    } else {
        Ok(Some(unicode))
    }
}

/// `pg_unicode_to_server(c, out)` then advance.
fn append_codepoint(
    out: &mut Vec<u8>,
    c: PgWchar,
    seam: &dyn UnicodeToServerSeam,
    escpos: i32,
) -> Result<(), DeError> {
    let bytes = seam.pg_unicode_to_server(c).map_err(|_| DeError {
        message: "invalid Unicode escape value".to_string(),
        location: escpos,
        hint: None,
    })?;
    out.extend_from_slice(&bytes);
    Ok(())
}

fn invalid_pair(location: i32) -> DeError {
    DeError {
        message: "invalid Unicode surrogate pair".to_string(),
        location,
        hint: None,
    }
}
