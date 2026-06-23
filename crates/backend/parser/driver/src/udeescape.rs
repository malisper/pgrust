//! Unicode de-escaping support (`parser.c:326-527`): `hexval`,
//! `check_unicode_value`, `check_uescapechar`, and `str_udeescape`.
//!
//! These are parser.c's OWN, node-independent routines, ported 1:1. The single
//! genuine external — `pg_unicode_to_server` (the code-point → server-encoding
//! conversion owned by `utils/mb/mbutils.c`) — crosses the mb seam.

use mcx::{Mcx, PgVec};
use types_core::PgWChar;
use types_error::{PgError, PgResult, ERRCODE_SYNTAX_ERROR};

use mbutils_seams as mb;

use crate::{
    is_utf16_surrogate_first, is_utf16_surrogate_second, is_valid_unicode_codepoint,
    surrogate_pair_to_codepoint, MAX_UNICODE_EQUIVALENT_STRING,
};

/// `hexval()` (parser.c:327) — value of a hex digit (caller verified that it
/// is one). The C `elog(ERROR, "invalid hexadecimal digit")` fallthrough is
/// unreachable given the `isxdigit()` guards at every call site.
fn hexval(c: u8) -> PgWChar {
    if c.is_ascii_digit() {
        (c - b'0') as PgWChar
    } else if (b'a'..=b'f').contains(&c) {
        (c - b'a') as PgWChar + 0xA
    } else if (b'A'..=b'F').contains(&c) {
        (c - b'A') as PgWChar + 0xA
    } else {
        0
    }
}

/// `check_unicode_value()` (parser.c:341) — is the code point acceptable?
fn check_unicode_value(c: PgWChar, position: i32) -> PgResult<()> {
    if !is_valid_unicode_codepoint(c) {
        Err(syntax_error("invalid Unicode escape value", position))
    } else {
        Ok(())
    }
}

/// `check_uescapechar()` (parser.c:351) — is `escape` acceptable as the Unicode
/// escape character in the `UESCAPE` syntax?
pub fn check_uescapechar(escape: u8) -> bool {
    !(escape.is_ascii_hexdigit()
        || escape == b'+'
        || escape == b'\''
        || escape == b'"'
        || scanner_isspace(escape))
}

/// `scanner_isspace(ch)` (scansup.c:117) — true iff the flex scanner treats
/// `ch` as whitespace. Reimplemented in place per the repo precedent
/// (`arrayfuncs`/`varlena`/`misc2`); the flex `{space}` set is exactly these
/// five bytes — NOT locale `isspace`.
fn scanner_isspace(ch: u8) -> bool {
    ch == b' ' || ch == b'\t' || ch == b'\n' || ch == b'\r' || ch == 0x0c
}

/// `str_udeescape()` (parser.c:371) — process Unicode escapes in `str_`,
/// producing a palloc'd plain string in the server encoding.
///
/// `escape` is the escape character; `position` is the byte offset of the
/// `U&'`/`U&"` token start (used for error cursors); code points are converted
/// through the `pg_unicode_to_server` seam. The result is allocated in `mcx`.
/// Error locations are the *byte* offset of the offending escape
/// (`in - str + position + 3`); the caller (`base_yylex`) runs them through
/// `scanner_errposition`, exactly as C does (C activates an
/// `setup_scanner_errposition_callback` for the duration of each escape).
pub fn str_udeescape<'mcx>(
    mcx: Mcx<'mcx>,
    str_: &[u8],
    escape: u8,
    position: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    // C:386-387 guesstimate the result is no longer than the input, with enough
    // padding for one Unicode conversion. `allocator_api2::Vec` grows on demand
    // (the C repalloc loop at :393-401 is the same amortized growth), so we just
    // reserve the initial estimate.
    let new_len = str_.len() + MAX_UNICODE_EQUIVALENT_STRING + 1;
    let mut out: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, new_len)?;

    let mut pair_first: PgWChar = 0;
    let in_bytes = str_;
    let mut i = 0usize;

    // C reads in[1..7] which can run one past a possible NUL terminator; the C
    // buffer is NUL-terminated, so a read past the end yields 0. byte() mirrors
    // that: any offset at/after the end reads as 0 (NUL).
    let byte = |off: usize| -> u8 { in_bytes.get(off).copied().unwrap_or(0) };

    while i < in_bytes.len() {
        if in_bytes[i] == escape {
            // C:409-410 error cursor for this escape: in - str + position + 3.
            let escpos = i as i32 + position + 3;
            if byte(i + 1) == escape {
                // C:411-417 doubled escape -> a literal escape char.
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
                // C:418-451 4-hex-digit \XXXX escape.
                let unicode = (hexval(byte(i + 1)) << 12)
                    + (hexval(byte(i + 2)) << 8)
                    + (hexval(byte(i + 3)) << 4)
                    + hexval(byte(i + 4));
                check_unicode_value(unicode, escpos)?;
                if let Some(cp) = combine_pair(&mut pair_first, unicode, escpos)? {
                    append_codepoint(&mut out, cp, escpos)?;
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
                // C:452-490 6-hex-digit \+XXXXXX escape.
                let unicode = (hexval(byte(i + 2)) << 20)
                    + (hexval(byte(i + 3)) << 16)
                    + (hexval(byte(i + 4)) << 12)
                    + (hexval(byte(i + 5)) << 8)
                    + (hexval(byte(i + 6)) << 4)
                    + hexval(byte(i + 7));
                check_unicode_value(unicode, escpos)?;
                if let Some(cp) = combine_pair(&mut pair_first, unicode, escpos)? {
                    append_codepoint(&mut out, cp, escpos)?;
                }
                i += 8;
            } else {
                // C:491-495 anything else is an invalid escape.
                return Err(syntax_error("invalid Unicode escape", escpos)
                    .with_hint("Unicode escapes must be \\XXXX or \\+XXXXXX."));
            }
        } else {
            // C:499-505 ordinary character.
            if pair_first != 0 {
                return Err(invalid_pair(i as i32 + position + 3));
            }
            out.push(in_bytes[i]);
            i += 1;
        }
    }

    // C:508-510 unfinished surrogate pair?
    if pair_first != 0 {
        return Err(invalid_pair(i as i32 + position + 3));
    }

    Ok(out)
}

/// The surrogate-pair combining logic shared by the two hex-escape arms of
/// `str_udeescape` (parser.c:430-449 / :469-488). Updates `pair_first`,
/// returning `Some(codepoint)` to emit, or `None` when the first half of a pair
/// was just stored.
fn combine_pair(
    pair_first: &mut PgWChar,
    unicode: PgWChar,
    escpos: i32,
) -> PgResult<Option<PgWChar>> {
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

/// `pg_unicode_to_server(c, out); out += strlen(out)` (parser.c:447-448 /
/// :486-487). The conversion is the genuine external (`utils/mb/mbutils.c`),
/// reached through the mb seam.
fn append_codepoint<'mcx>(out: &mut PgVec<'mcx, u8>, c: PgWChar, _escpos: i32) -> PgResult<()> {
    let encoded = mb::pg_unicode_to_server::call(*out.allocator(), c)?;
    out.extend_from_slice(&encoded);
    Ok(())
}

/// `invalid_pair:` (parser.c:520) — the "invalid Unicode surrogate pair" error,
/// with the byte error cursor.
fn invalid_pair(location: i32) -> PgError {
    syntax_error("invalid Unicode surrogate pair", location)
}

/// An `ERRCODE_SYNTAX_ERROR` carrying the byte error cursor (the caller converts
/// it to a 1-based character cursor via `scanner_errposition`).
fn syntax_error(message: &str, location: i32) -> PgError {
    PgError::error(message)
        .with_sqlstate(ERRCODE_SYNTAX_ERROR)
        .with_cursor_position(location)
}
