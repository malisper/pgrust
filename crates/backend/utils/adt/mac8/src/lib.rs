#![allow(non_snake_case)]
// `PgResult<T>` carries the workspace-wide `PgError`, which is large; every
// sibling adt crate allows this lint rather than boxing the soft-error path away
// from the C-identical control flow.
#![allow(clippy::result_large_err)]

//! Port of `src/backend/utils/adt/mac8.c`: the `macaddr8` (8-byte, EUI-64 MAC
//! address) datatype, plus conversions to/from the 6-byte `macaddr` (EUI-48).
//!
//! EUI-48 (6 byte) MAC addresses are accepted as input and are stored in EUI-64
//! format, with the 4th and 5th bytes set to `FF` and `FE`. Output is always in
//! 8 byte (EUI-64) format.
//!
//! Every function in `mac8.c` is ported here against postgres-18.3 (branch order,
//! message text, and SQLSTATE preserved). `macaddr8` values are modeled as the
//! owned [`macaddr8`] payload (`types_network`). The fmgr/Datum/varlena/StringInfo
//! envelope is the project-wide systemic deferral: [`macaddr8_in`] takes the input
//! text `&[u8]`, [`macaddr8_out`] returns the cstring text `Vec<u8>`,
//! [`macaddr8_recv`] takes the raw external binary body `&[u8]`, and
//! [`macaddr8_send`] returns the `bytea` payload bytes.
//!
//! Hashing uses the in-repo [`hashfn`] (`hash_bytes` / `hash_bytes_extended`)
//! directly. `mac8.c` has no SortSupport routine; it owns no inward seam and
//! reaches no unported neighbour, so this crate installs no seams.

extern crate alloc;

pub mod fmgr_builtins;

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use hashfn::{hash_bytes, hash_bytes_extended};
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};
use types_network::{macaddr, macaddr8};

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// Utility macros used for sorting and comparing (mac8.c:33-37).
// ---------------------------------------------------------------------------

/// `hibits(addr)` (mac8.c:33): the high 32 bits as an unsigned value.
///
/// In C this is `(unsigned long)((a<<24) | (b<<16) | (c<<8) | d)`; the operands
/// promote to (signed) `int` for the shifts, so a high byte >= 0x80 makes
/// `a<<24` a negative `int` that sign-extends when cast to `unsigned long`.
/// Computing it in `u32` instead produces the bare 32-bit value, but the two
/// schemes induce the *same* ordering in `macaddr8_cmp_internal`: sign-extension
/// is monotonic over the unsigned-32 total order, so `hibits`/`lobits`
/// comparisons match C exactly.
#[inline]
fn hibits(addr: &macaddr8) -> u32 {
    ((addr.a as u32) << 24) | ((addr.b as u32) << 16) | ((addr.c as u32) << 8) | (addr.d as u32)
}

/// `lobits(addr)` (mac8.c:36): the low 32 bits as an unsigned value.
#[inline]
fn lobits(addr: &macaddr8) -> u32 {
    ((addr.e as u32) << 24) | ((addr.f as u32) << 16) | ((addr.g as u32) << 8) | (addr.h as u32)
}

/// View a `macaddr8` as the `sizeof(macaddr8)` raw bytes that C hashes / stores.
/// `macaddr8` is eight `u8` fields, so this is the exact in-memory image
/// (`[a, b, c, d, e, f, g, h]`).
#[inline]
fn macaddr8_bytes(key: &macaddr8) -> [u8; 8] {
    [key.a, key.b, key.c, key.d, key.e, key.f, key.g, key.h]
}

/// Build a `macaddr8` from its eight-byte image.
#[inline]
fn macaddr8_from_bytes(b: [u8; 8]) -> macaddr8 {
    macaddr8 {
        a: b[0],
        b: b[1],
        c: b[2],
        d: b[3],
        e: b[4],
        f: b[5],
        g: b[6],
        h: b[7],
    }
}

/// C `isspace` for the `"C"` locale. `mac8.c` only ever calls `isspace()` on
/// `unsigned char` values, so values > 127 never match.
#[inline]
fn is_c_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}

/// Per-byte hex lookup table (mac8.c:41). `-1` marks a non-hex character; only
/// the first 128 entries are populated (callers must reject bytes > 127 first).
const HEXLOOKUP: [i8; 128] = [
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, //
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, //
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, //
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1, //
    -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1, //
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, //
    -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1, //
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, //
];

/// `hex2_to_uchar` (mac8.c:58): convert 2 hex digits to a byte.
///
/// Sets `*badhex` to true if the end of the string is reached (here the slice
/// runs out, mirroring C's `'\0'` sentinel), or if either character is not a
/// valid hex digit. `ptr` is the remaining input slice starting at the first of
/// the two digits.
fn hex2_to_uchar(ptr: &[u8], badhex: &mut bool) -> u8 {
    // Handle the first character.
    let Some(&c0) = ptr.first() else {
        *badhex = true;
        return 0;
    };
    if c0 > 127 {
        *badhex = true;
        return 0;
    }
    let lookup = HEXLOOKUP[c0 as usize];
    if lookup < 0 {
        *badhex = true;
        return 0;
    }
    let mut ret: u8 = (lookup as u8) << 4;

    // Move to the second character.
    let Some(&c1) = ptr.get(1) else {
        *badhex = true;
        return 0;
    };
    if c1 > 127 {
        *badhex = true;
        return 0;
    }
    let lookup = HEXLOOKUP[c1 as usize];
    if lookup < 0 {
        *badhex = true;
        return 0;
    }
    ret = ret.wrapping_add(lookup as u8);

    ret
}

// ---------------------------------------------------------------------------
// MAC address reader (mac8.c:97).
// ---------------------------------------------------------------------------

/// `macaddr8_in` (mac8.c:97): MAC address (EUI-48 and EUI-64) reader; accepts
/// several common notations. EUI-48 (6-byte) input is stored in EUI-64 format
/// with the 4th and 5th bytes set to `FF` and `FE`.
///
/// On a parse failure the soft-error sink `escontext` is consulted: with one
/// installed the error is *saved* and `Ok(None)` is returned (C yields
/// `(Datum) 0`); without one it is rethrown as a hard `Err`.
///
/// `str` is the input cstring content (C's `PG_GETARG_CSTRING(0)`, sans the NUL).
pub fn macaddr8_in(
    str: &[u8],
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<macaddr8>> {
    let mut pos = 0usize;
    let mut badhex = false;
    let mut a: u8 = 0;
    let mut b: u8 = 0;
    let mut c: u8 = 0;
    let mut d: u8 = 0;
    let mut e: u8 = 0;
    let mut f: u8 = 0;
    let mut g: u8 = 0;
    let mut h: u8 = 0;
    let mut count = 0i32;
    let mut spacer: u8 = b'\0';

    // Byte peek (returns 0 at/after end, matching the C NUL sentinel for `*ptr`).
    let at = |p: usize| -> u8 { str.get(p).copied().unwrap_or(0) };

    // skip leading spaces
    while at(pos) != 0 && is_c_space(at(pos)) {
        pos += 1;
    }

    // digits must always come in pairs
    while at(pos) != 0 && at(pos + 1) != 0 {
        // Attempt to decode each byte, which must be 2 hex digits in a row.
        // Either 6 or 8 byte MAC addresses are supported.

        // Attempt to collect a byte
        count += 1;

        match count {
            1 => a = hex2_to_uchar(&str[pos..], &mut badhex),
            2 => b = hex2_to_uchar(&str[pos..], &mut badhex),
            3 => c = hex2_to_uchar(&str[pos..], &mut badhex),
            4 => d = hex2_to_uchar(&str[pos..], &mut badhex),
            5 => e = hex2_to_uchar(&str[pos..], &mut badhex),
            6 => f = hex2_to_uchar(&str[pos..], &mut badhex),
            7 => g = hex2_to_uchar(&str[pos..], &mut badhex),
            8 => h = hex2_to_uchar(&str[pos..], &mut badhex),
            // must be trailing garbage...
            _ => return fail(escontext, str),
        }

        if badhex {
            return fail(escontext, str);
        }

        // Move forward to where the next byte should be
        pos += 2;

        // Check for a spacer, these are valid, anything else is not
        let ch = at(pos);
        if ch == b':' || ch == b'-' || ch == b'.' {
            // remember the spacer used, if it changes then it isn't valid
            if spacer == b'\0' {
                spacer = ch;
            }
            // Have to use the same spacer throughout
            else if spacer != ch {
                return fail(escontext, str);
            }

            // move past the spacer
            pos += 1;
        }

        // allow trailing whitespace after if we have 6 or 8 bytes
        if (count == 6 || count == 8) && is_c_space(at(pos)) {
            pos += 1;
            while at(pos) != 0 && is_c_space(at(pos)) {
                pos += 1;
            }

            // If we found a space and then non-space, it's invalid
            if at(pos) != 0 {
                return fail(escontext, str);
            }
        }
    }

    // Convert a 6 byte MAC address to macaddr8
    if count == 6 {
        h = f;
        g = e;
        f = d;

        d = 0xFF;
        e = 0xFE;
    } else if count != 8 {
        return fail(escontext, str);
    }

    Ok(Some(macaddr8_from_bytes([a, b, c, d, e, f, g, h])))
}

/// `macaddr8_in` failure path (mac8.c:223 `fail:`).
fn fail(
    escontext: Option<&mut SoftErrorContext>,
    str: &[u8],
) -> PgResult<Option<macaddr8>> {
    ereturn(
        escontext,
        None,
        PgError::error(format!(
            "invalid input syntax for type {}: \"{}\"",
            "macaddr8",
            String::from_utf8_lossy(str)
        ))
        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
    )
}

// ---------------------------------------------------------------------------
// MAC8 address output function (mac8.c:238).
// ---------------------------------------------------------------------------

/// `macaddr8_out` (mac8.c:238): render a `macaddr8` as fixed EUI-64
/// `aa:bb:cc:dd:ee:ff:gg:hh`.
///
/// C `palloc(32)` + `snprintf(..., "%02x:%02x:...:%02x", ...)`. The returned
/// bytes are the cstring text without the trailing NUL.
pub fn macaddr8_out(addr: &macaddr8) -> Vec<u8> {
    format!(
        "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
        addr.a, addr.b, addr.c, addr.d, addr.e, addr.f, addr.g, addr.h
    )
    .into_bytes()
}

// ---------------------------------------------------------------------------
// Binary I/O (mac8.c:254, 287).
// ---------------------------------------------------------------------------

/// A minimal forward cursor over a binary message body, modeling the part of
/// `pq_getmsgbyte` that `macaddr8_recv` exercises (advancing one byte at a time
/// through the `StringInfo` buffer). The `StringInfo`/`pqformat` envelope is the
/// deferred fmgr boundary; recv reads the raw message bytes directly.
struct MsgCursor<'a> {
    data: &'a [u8],
    cursor: usize,
}

impl<'a> MsgCursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, cursor: 0 }
    }

    /// `pq_getmsgbyte(buf)` — read one byte, advancing the cursor. Past the end
    /// of the buffer the C routine raises "insufficient data left in message".
    fn get_byte(&mut self) -> PgResult<u8> {
        if self.cursor >= self.data.len() {
            return Err(PgError::error("insufficient data left in message")
                .with_sqlstate(::types_error::ERRCODE_INVALID_BINARY_REPRESENTATION));
        }
        let b = self.data[self.cursor];
        self.cursor += 1;
        Ok(b)
    }
}

/// `macaddr8_recv` (mac8.c:254): external binary format (EUI-48 and EUI-64) to
/// `macaddr8`. The external representation is just the bytes, MSB first; a 6-byte
/// message has the `FF FE` 4th/5th bytes injected. `buf` is the raw external
/// binary message body.
pub fn macaddr8_recv(buf: &[u8]) -> PgResult<macaddr8> {
    let mut cur = MsgCursor::new(buf);

    let a = cur.get_byte()?;
    let b = cur.get_byte()?;
    let c = cur.get_byte()?;

    let (d, e);
    if buf.len() == 6 {
        d = 0xFF;
        e = 0xFE;
    } else {
        d = cur.get_byte()?;
        e = cur.get_byte()?;
    }

    let f = cur.get_byte()?;
    let g = cur.get_byte()?;
    let h = cur.get_byte()?;

    Ok(macaddr8_from_bytes([a, b, c, d, e, f, g, h]))
}

/// `macaddr8_send` (mac8.c:287): `macaddr8` (EUI-64) to external binary format,
/// returning the `bytea` payload bytes (the fmgr layer wraps them with the
/// varlena header). C builds them with `pq_begintypsend` + eight `pq_sendbyte` +
/// `pq_endtypsend`; the body is exactly the eight address bytes in order.
pub fn macaddr8_send(addr: &macaddr8) -> Vec<u8> {
    macaddr8_bytes(addr).to_vec()
}

// ---------------------------------------------------------------------------
// Comparison (mac8.c:309-389).
// ---------------------------------------------------------------------------

/// `macaddr8_cmp_internal` (mac8.c:309): comparison function for sorting.
pub fn macaddr8_cmp_internal(a1: &macaddr8, a2: &macaddr8) -> i32 {
    if hibits(a1) < hibits(a2) {
        -1
    } else if hibits(a1) > hibits(a2) {
        1
    } else if lobits(a1) < lobits(a2) {
        -1
    } else if lobits(a1) > lobits(a2) {
        1
    } else {
        0
    }
}

/// `macaddr8_cmp` (mac8.c:324).
pub fn macaddr8_cmp(a1: &macaddr8, a2: &macaddr8) -> i32 {
    macaddr8_cmp_internal(a1, a2)
}

/// `macaddr8_lt` (mac8.c:337).
pub fn macaddr8_lt(a1: &macaddr8, a2: &macaddr8) -> bool {
    macaddr8_cmp_internal(a1, a2) < 0
}

/// `macaddr8_le` (mac8.c:346).
pub fn macaddr8_le(a1: &macaddr8, a2: &macaddr8) -> bool {
    macaddr8_cmp_internal(a1, a2) <= 0
}

/// `macaddr8_eq` (mac8.c:355).
pub fn macaddr8_eq(a1: &macaddr8, a2: &macaddr8) -> bool {
    macaddr8_cmp_internal(a1, a2) == 0
}

/// `macaddr8_ge` (mac8.c:364).
pub fn macaddr8_ge(a1: &macaddr8, a2: &macaddr8) -> bool {
    macaddr8_cmp_internal(a1, a2) >= 0
}

/// `macaddr8_gt` (mac8.c:373).
pub fn macaddr8_gt(a1: &macaddr8, a2: &macaddr8) -> bool {
    macaddr8_cmp_internal(a1, a2) > 0
}

/// `macaddr8_ne` (mac8.c:382).
pub fn macaddr8_ne(a1: &macaddr8, a2: &macaddr8) -> bool {
    macaddr8_cmp_internal(a1, a2) != 0
}

// ---------------------------------------------------------------------------
// Hashing (mac8.c:394-409).
// ---------------------------------------------------------------------------

/// `hashmacaddr8` (mac8.c:394): `hash_any((unsigned char *) key, sizeof(macaddr8))`.
pub fn hashmacaddr8(key: &macaddr8) -> u32 {
    hash_bytes(&macaddr8_bytes(key))
}

/// `hashmacaddr8extended` (mac8.c:402):
/// `hash_any_extended(key, sizeof(macaddr8), seed)`.
pub fn hashmacaddr8extended(key: &macaddr8, seed: u64) -> u64 {
    hash_bytes_extended(&macaddr8_bytes(key), seed)
}

// ---------------------------------------------------------------------------
// Arithmetic: bitwise NOT, AND, OR (mac8.c:414-471).
// ---------------------------------------------------------------------------

/// `macaddr8_not` (mac8.c:414): bitwise complement.
pub fn macaddr8_not(addr: &macaddr8) -> macaddr8 {
    macaddr8 {
        a: !addr.a,
        b: !addr.b,
        c: !addr.c,
        d: !addr.d,
        e: !addr.e,
        f: !addr.f,
        g: !addr.g,
        h: !addr.h,
    }
}

/// `macaddr8_and` (mac8.c:433): bitwise AND.
pub fn macaddr8_and(addr1: &macaddr8, addr2: &macaddr8) -> macaddr8 {
    macaddr8 {
        a: addr1.a & addr2.a,
        b: addr1.b & addr2.b,
        c: addr1.c & addr2.c,
        d: addr1.d & addr2.d,
        e: addr1.e & addr2.e,
        f: addr1.f & addr2.f,
        g: addr1.g & addr2.g,
        h: addr1.h & addr2.h,
    }
}

/// `macaddr8_or` (mac8.c:453): bitwise OR.
pub fn macaddr8_or(addr1: &macaddr8, addr2: &macaddr8) -> macaddr8 {
    macaddr8 {
        a: addr1.a | addr2.a,
        b: addr1.b | addr2.b,
        c: addr1.c | addr2.c,
        d: addr1.d | addr2.d,
        e: addr1.e | addr2.e,
        f: addr1.f | addr2.f,
        g: addr1.g | addr2.g,
        h: addr1.h | addr2.h,
    }
}

// ---------------------------------------------------------------------------
// Truncation (mac8.c:477).
// ---------------------------------------------------------------------------

/// `macaddr8_trunc` (mac8.c:477): zero the trailing five bytes so MAC8
/// manufacturers can be compared.
pub fn macaddr8_trunc(addr: &macaddr8) -> macaddr8 {
    macaddr8 {
        a: addr.a,
        b: addr.b,
        c: addr.c,
        d: 0,
        e: 0,
        f: 0,
        g: 0,
        h: 0,
    }
}

// ---------------------------------------------------------------------------
// Set 7th bit for modified EUI-64 as used in IPv6 (mac8.c:500).
// ---------------------------------------------------------------------------

/// `macaddr8_set7bit` (mac8.c:500): set the 7th bit for a modified EUI-64 as
/// used in IPv6.
pub fn macaddr8_set7bit(addr: &macaddr8) -> macaddr8 {
    macaddr8 {
        a: addr.a | 0x02,
        b: addr.b,
        c: addr.c,
        d: addr.d,
        e: addr.e,
        f: addr.f,
        g: addr.g,
        h: addr.h,
    }
}

// ---------------------------------------------------------------------------
// Conversion operators (mac8.c:524, 545).
// ---------------------------------------------------------------------------

/// `macaddrtomacaddr8` (mac8.c:524): widen `macaddr` (EUI-48) to `macaddr8`
/// (EUI-64) by inserting `FF FE` as the 4th and 5th bytes.
pub fn macaddrtomacaddr8(addr6: &macaddr) -> macaddr8 {
    macaddr8 {
        a: addr6.a,
        b: addr6.b,
        c: addr6.c,
        d: 0xFF,
        e: 0xFE,
        f: addr6.d,
        g: addr6.e,
        h: addr6.f,
    }
}

/// `macaddr8tomacaddr` (mac8.c:545): narrow `macaddr8` (EUI-64) to `macaddr`
/// (EUI-48); only valid when the 4th and 5th bytes are `FF` and `FE`.
pub fn macaddr8tomacaddr(addr: &macaddr8) -> PgResult<macaddr> {
    if (addr.d != 0xFF) || (addr.e != 0xFE) {
        return Err(PgError::error("macaddr8 data out of range to convert to macaddr")
            .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .with_hint(
                "Only addresses that have FF and FE as values in the \
                 4th and 5th bytes from the left, for example \
                 xx:xx:xx:ff:fe:xx:xx:xx, are eligible to be converted \
                 from macaddr8 to macaddr.",
            ));
    }

    Ok(macaddr {
        a: addr.a,
        b: addr.b,
        c: addr.c,
        d: addr.f,
        e: addr.g,
        f: addr.h,
    })
}

/// `mac8.c` owns no inward seam (its functions are reached directly or via the
/// not-yet-modeled fmgr/PGFunction registry) and reaches no unported neighbour,
/// so this installs nothing.
pub fn init_seams() {
    fmgr_builtins::register_mac8_builtins();
}
