// NB: not `#![no_std]` — the fmgr builtin registration layer (`fmgr_builtins`)
// registers the `char.c` builtins into the fmgr-core table (C: `fmgr_builtins[]`),
// which uses `String`/`std`.
#![allow(clippy::result_large_err)]

//! Port of PostgreSQL 18.3 `src/backend/utils/adt/char.c`: the built-in type
//! `"char"` (a single-byte type, *not* the SQL `CHAR(n)` / `bpchar` type) — its
//! text/binary I/O routines, comparison operators, and the `int4` / `text`
//! casts.
//!
//! The fmgr/`Datum` marshalling layer (argument decode, registry rows, the
//! `PG_FUNCTION_ARGS` boundary) is not part of this unit; like the sibling adt
//! ports (`probe-adt-scalar-bool`, `backend-utils-adt-numutils`) these are plain
//! typed Rust functions. A pass-by-value `"char"` value is an [`i8`] (C `char`,
//! carried in the low byte of a `Datum`); `cstring` arrives as `&str`; a `text`
//! argument arrives as its detoasted `VARDATA_ANY` payload (`&[u8]`); binary I/O
//! uses the [`StringInfo`] message buffer.
//!
//! C does comparisons as though `char` is unsigned (`uint8`) and integer
//! conversions as though it is signed (`int8`); both are preserved exactly.
//!
//! Calls into other units that would form a dependency cycle go through that
//! unit's seam crate: `cstring_to_text` (varlena). The `pqformat` send/recv
//! helpers are non-cyclic and called directly.

extern crate alloc;

mod fmgr_builtins;

use backend_libpq_pqformat::{pq_begintypsend, pq_endtypsend, pq_getmsgbyte, pq_sendbyte};
use backend_utils_adt_varlena_seams::cstring_to_text;
use mcx::{Mcx, PgString};
use types_datum::{Bytea, Datum};
use types_error::{PgError, PgResult, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE};
use types_stringinfo::StringInfo;

// `char.c` octal helpers (char.c:24-26).

/// `ISOCTAL(c)` (char.c:24): `'0' <= c <= '7'`.
fn is_octal(c: u8) -> bool {
    (b'0'..=b'7').contains(&c)
}

/// `TOOCTAL(c)` (char.c:25): `c + '0'` — octal digit value → ASCII digit.
fn to_octal(c: u8) -> u8 {
    c + b'0'
}

/// `FROMOCTAL(c)` (char.c:26): `(unsigned char) c - '0'` — ASCII octal digit →
/// value. C promotes the result to `int` for the shift/add expression, so this
/// returns the value already widened to match C's integer promotion (the
/// `(o1 << 6) + (o2 << 3) + o3` sum is then truncated back to a byte on the
/// `PG_RETURN_CHAR` assignment).
fn from_octal(c: u8) -> u32 {
    (c - b'0') as u32
}

/// Decode a `\ooo` triple to its byte. C computes
/// `(FROMOCTAL(o1) << 6) + (FROMOCTAL(o2) << 3) + FROMOCTAL(o3)` in `int`
/// arithmetic, then truncates to `char` on the `PG_RETURN_CHAR` assignment —
/// so a leading digit > 3 (e.g. `\700`) wraps modulo 256, exactly as the `as
/// u8` truncation here does.
fn decode_octal(o1: u8, o2: u8, o3: u8) -> u8 {
    ((from_octal(o1) << 6) + (from_octal(o2) << 3) + from_octal(o3)) as u8
}

/// `IS_HIGHBIT_SET(ch)` (`c.h`): the byte's top bit is set.
fn is_highbit_set(c: u8) -> bool {
    (c & 0x80) != 0
}

// ===========================================================================
// USER I/O ROUTINES (char.c:29-113)
// ===========================================================================

/// `charin` (char.c:40): converts `"x"` → `'x'`.
///
/// Accepts the formats [`charout`] produces. A 4-byte `\ooo` octal escape (the
/// leading backslash and three octal digits) decodes to the single byte
/// `(o1<<6) + (o2<<3) + o3`. Otherwise the first input byte is taken as the
/// value (a zero-length input yields `'\0'`), silently discarding any remaining
/// bytes — the documented backwards-compatibility provision for multibyte input.
pub fn charin(ch: &str) -> i8 {
    let bytes = ch.as_bytes();

    if bytes.len() == 4
        && bytes[0] == b'\\'
        && is_octal(bytes[1])
        && is_octal(bytes[2])
        && is_octal(bytes[3])
    {
        return decode_octal(bytes[1], bytes[2], bytes[3]) as i8;
    }

    // This does the right thing for a zero-length input string (C reads the
    // terminating NUL as ch[0]).
    bytes.first().copied().unwrap_or(0) as i8
}

/// `charout` (char.c:63): converts `'x'` → `"x"`. The output formats are:
///
/// 1. `0x00` → empty string.
/// 2. `0x01..0x7F` → a single ASCII byte.
/// 3. `0x80..0xFF` → `\ooo` (backslash and 3 octal digits), matching the
///    traditional `bytea` "escape" format.
pub fn charout(mcx: Mcx<'_>, ch: i8) -> PgResult<PgString<'_>> {
    let ch = ch as u8;
    let mut result = PgString::new_in(mcx);

    if is_highbit_set(ch) {
        result.try_push('\\')?;
        result.try_push(to_octal(ch >> 6) as char)?;
        result.try_push(to_octal((ch >> 3) & 0o7) as char)?;
        result.try_push(to_octal(ch & 0o7) as char)?;
    } else if ch != 0 {
        // C writes result[0] = ch; result[1] = '\0'. This produces acceptable
        // results for 0x00 as well (an empty cstring).
        result.try_push(ch as char)?;
    }

    Ok(result)
}

/// `charrecv` (char.c:93): external binary format → `"char"`. The external
/// representation is one byte, with no character set conversion.
pub fn charrecv(buf: &mut StringInfo<'_>) -> PgResult<i8> {
    Ok(pq_getmsgbyte(buf)? as i8)
}

/// `charsend` (char.c:104): `"char"` → binary format (a one-byte `bytea`).
pub fn charsend(mcx: Mcx<'_>, arg1: i8) -> PgResult<Bytea<'_>> {
    let mut buf = pq_begintypsend(mcx)?;
    pq_sendbyte(&mut buf, arg1 as u8)?;
    Ok(pq_endtypsend(buf))
}

// ===========================================================================
// PUBLIC ROUTINES — comparisons, int4 casts, text casts (char.c:115-254)
//
// NOTE (char.c:119): comparisons are done as though char is unsigned (uint8);
// conversions to and from integer are done as though char is signed (int8).
// ===========================================================================

/// `chareq` (char.c:126).
pub fn chareq(arg1: i8, arg2: i8) -> bool {
    arg1 == arg2
}

/// `charne` (char.c:135).
pub fn charne(arg1: i8, arg2: i8) -> bool {
    arg1 != arg2
}

/// `charlt` (char.c:144) — compared as `(uint8)`.
pub fn charlt(arg1: i8, arg2: i8) -> bool {
    (arg1 as u8) < (arg2 as u8)
}

/// `charle` (char.c:153) — compared as `(uint8)`.
pub fn charle(arg1: i8, arg2: i8) -> bool {
    (arg1 as u8) <= (arg2 as u8)
}

/// `chargt` (char.c:162) — compared as `(uint8)`.
pub fn chargt(arg1: i8, arg2: i8) -> bool {
    (arg1 as u8) > (arg2 as u8)
}

/// `charge` (char.c:171) — compared as `(uint8)`.
pub fn charge(arg1: i8, arg2: i8) -> bool {
    (arg1 as u8) >= (arg2 as u8)
}

/// `chartoi4` (char.c:181): `(int32) ((int8) arg1)` — sign-extended.
pub fn chartoi4(arg1: i8) -> i32 {
    arg1 as i32
}

/// `i4tochar` (char.c:189): range-checked narrowing to a signed byte. C:
/// out-of-`[SCHAR_MIN, SCHAR_MAX]` raises `ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE`,
/// `"\"char\" out of range"`.
pub fn i4tochar(arg1: i32) -> PgResult<i8> {
    if arg1 < i8::MIN as i32 || arg1 > i8::MAX as i32 {
        return Err(
            PgError::error("\"char\" out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
        );
    }
    Ok(arg1 as i8)
}

/// `text_char` (char.c:203): `text` → `"char"`. `payload` is the detoasted
/// `VARDATA_ANY` of the argument (`VARSIZE_ANY_EXHDR` bytes).
///
/// Conversion rules are the same as in [`charin`], but here the empty-string
/// case is handled honestly (a zero-length `text` yields `'\0'`).
pub fn text_char(payload: &[u8]) -> i8 {
    if payload.len() == 4
        && payload[0] == b'\\'
        && is_octal(payload[1])
        && is_octal(payload[2])
        && is_octal(payload[3])
    {
        decode_octal(payload[1], payload[2], payload[3]) as i8
    } else if !payload.is_empty() {
        payload[0] as i8
    } else {
        0
    }
}

/// `char_text` (char.c:227): `"char"` → `text`, returned as the `text` `Datum`
/// the caller passes on (`PG_RETURN_TEXT_P`).
///
/// Conversion rules are the same as in [`charout`] (which is honest about
/// converting `0x00` to an empty string and renders a high-bit byte as the
/// 4-char `\ooo` octal escape); the resulting cstring is wrapped into a `text`
/// varlena. The produced payload bytes — and therefore `VARSIZE` — are
/// byte-identical to C's hand-built image (`palloc(VARHDRSZ + 4)` then the same
/// branch on `IS_HIGHBIT_SET` / `arg1 != '\0'`).
pub fn char_text(mcx: Mcx<'_>, arg1: i8) -> PgResult<Datum> {
    let s = charout(mcx, arg1)?;
    cstring_to_text::call(mcx, s.as_str())
}

/// This unit has no inbound cyclic callers, so it owns no seam crate and
/// installs no seams. `init_seams()` registers the `char.c` builtins into the
/// fmgr fast-path table (C: `fmgr_builtins[]`) so `fmgr_isbuiltin` resolves
/// `chareq` (and the rest of the family) during early catalog scans without
/// recursing into the not-yet-built syscache.
pub fn init_seams() {
    fmgr_builtins::register_char_builtins();
}

#[cfg(test)]
mod tests;
