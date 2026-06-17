#![allow(non_snake_case)]
// `PgResult<T>` carries the workspace-wide `PgError`, which is large; every
// sibling adt crate allows this lint rather than boxing the soft-error path away
// from the C-identical control flow.
#![allow(clippy::result_large_err)]

//! Port of `src/backend/utils/adt/mac.c`: the `macaddr` (6-byte, EUI-48 MAC
//! address) datatype — I/O conversion, comparison, hashing, bitwise ops,
//! truncation, and SortSupport.
//!
//! Every function in `mac.c` is ported here against postgres-18.3 (branch order,
//! message text, and SQLSTATE preserved). `macaddr` values are modeled as the
//! owned [`macaddr`] payload (`types_network`). The fmgr/Datum/varlena/StringInfo
//! envelope is the project-wide systemic deferral: [`macaddr_in`] takes the input
//! text `&[u8]`, [`macaddr_out`] returns the cstring text `Vec<u8>`,
//! [`macaddr_recv`] takes the raw external binary body `&[u8]`, and
//! [`macaddr_send`] returns the `bytea` payload bytes.
//!
//! Hashing uses the in-repo [`common_hashfn`] (`hash_bytes` / `hash_bytes_extended`)
//! directly. The genuinely-external substrate of `macaddr_sortsupport` —
//! installing the comparator / abbrev callbacks into the live `SortSupportData`
//! node, the HyperLogLog cardinality estimator, and the `trace_sort` GUC —
//! crosses [`backend_utils_adt_mac_seams::sortsupport`] (called here, installed
//! by the unported tuplesort / `lib/hyperloglog` owner — a loud panic until it
//! lands). The pure comparator ([`macaddr_fast_cmp`]) and abbreviated-key packing
//! ([`macaddr_abbrev_convert_bits`]) are in-crate.

extern crate alloc;

pub mod fmgr_builtins;

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use backend_utils_adt_mac_seams::sortsupport;
use common_hashfn::{hash_bytes, hash_bytes_extended};
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};
use types_network::macaddr;

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// Utility macros used for sorting and comparing (mac.c:30-34).
// ---------------------------------------------------------------------------

/// `hibits(addr)` (mac.c:30): `((addr->a << 16) | (addr->b << 8) | addr->c)`.
#[inline]
fn hibits(addr: &macaddr) -> u64 {
    ((addr.a as u64) << 16) | ((addr.b as u64) << 8) | (addr.c as u64)
}

/// `lobits(addr)` (mac.c:33): `((addr->d << 16) | (addr->e << 8) | addr->f)`.
#[inline]
fn lobits(addr: &macaddr) -> u64 {
    ((addr.d as u64) << 16) | ((addr.e as u64) << 8) | (addr.f as u64)
}

/// View a `macaddr` as the `sizeof(macaddr)` raw bytes that C hashes / stores.
/// `macaddr` is six `u8` fields, so this is the exact in-memory image
/// (`[a, b, c, d, e, f]`).
#[inline]
fn macaddr_bytes(key: &macaddr) -> [u8; 6] {
    [key.a, key.b, key.c, key.d, key.e, key.f]
}

// ---------------------------------------------------------------------------
// MAC address reader (mac.c:55).
// ---------------------------------------------------------------------------

/// `macaddr_in` (mac.c:55): parse text into a `macaddr`, accepting several common
/// notations.  On a parse failure the soft-error sink `escontext` is consulted:
/// with one installed the error is *saved* and `Ok(None)` is returned (C yields
/// `(Datum) 0`); without one it is rethrown as a hard `Err`.
///
/// `str` is the input cstring content (C's `PG_GETARG_CSTRING(0)`, sans the NUL).
pub fn macaddr_in(
    str: &[u8],
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<macaddr>> {
    // The seven C `sscanf` variants are tried in order; each requires exactly
    // six octets and no trailing non-whitespace junk (C `count == 6`).
    let parsed = scan_macaddr(str, b':', false)
        .or_else(|| scan_macaddr(str, b'-', false))
        .or_else(|| scan_macaddr(str, b':', true))
        .or_else(|| scan_macaddr_dash_pairs(str))
        .or_else(|| scan_macaddr_dot(str))
        .or_else(|| scan_macaddr_dash_doubles(str))
        .or_else(|| scan_macaddr_plain(str));

    let Some([a, b, c, d, e, f]) = parsed else {
        return ereturn(
            escontext,
            None,
            PgError::error(format!(
                "invalid input syntax for type {}: \"{}\"",
                "macaddr",
                String::from_utf8_lossy(str)
            ))
            .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
        );
    };

    if !(0..=255).contains(&a)
        || !(0..=255).contains(&b)
        || !(0..=255).contains(&c)
        || !(0..=255).contains(&d)
        || !(0..=255).contains(&e)
        || !(0..=255).contains(&f)
    {
        return ereturn(
            escontext,
            None,
            PgError::error(format!(
                "invalid octet value in \"macaddr\" value: \"{}\"",
                String::from_utf8_lossy(str)
            ))
            .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
        );
    }

    Ok(Some(macaddr {
        a: a as u8,
        b: b as u8,
        c: c as u8,
        d: d as u8,
        e: e as u8,
        f: f as u8,
    }))
}

// ---------------------------------------------------------------------------
// MAC address output function (mac.c:121).
// ---------------------------------------------------------------------------

/// `macaddr_out` (mac.c:121): render a `macaddr` as fixed `aa:bb:cc:dd:ee:ff`.
///
/// C `palloc(32)` + `snprintf(..., "%02x:%02x:%02x:%02x:%02x:%02x", ...)`.  The
/// returned bytes are the cstring text without the trailing NUL.
pub fn macaddr_out(addr: &macaddr) -> Vec<u8> {
    format!(
        "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
        addr.a, addr.b, addr.c, addr.d, addr.e, addr.f
    )
    .into_bytes()
}

// ---------------------------------------------------------------------------
// Binary I/O (mac.c:140, 161).
// ---------------------------------------------------------------------------

/// A minimal forward cursor over a binary message body, modeling the part of
/// `pq_getmsgbyte` that `macaddr_recv` exercises (advancing one byte at a time
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
                .with_sqlstate(types_error::ERRCODE_INVALID_BINARY_REPRESENTATION));
        }
        let b = self.data[self.cursor];
        self.cursor += 1;
        Ok(b)
    }
}

/// `macaddr_recv` (mac.c:140): external binary format (six bytes, MSB first) to
/// `macaddr`. `buf` is the raw external binary message body.
pub fn macaddr_recv(buf: &[u8]) -> PgResult<macaddr> {
    let mut cur = MsgCursor::new(buf);
    Ok(macaddr {
        a: cur.get_byte()?,
        b: cur.get_byte()?,
        c: cur.get_byte()?,
        d: cur.get_byte()?,
        e: cur.get_byte()?,
        f: cur.get_byte()?,
    })
}

/// `macaddr_send` (mac.c:161): `macaddr` to external binary format, returning the
/// `bytea` payload bytes (the fmgr layer wraps them with the varlena header).
/// C builds them with `pq_begintypsend` + six `pq_sendbyte` + `pq_endtypsend`;
/// the body is exactly the six address bytes in order.
pub fn macaddr_send(addr: &macaddr) -> Vec<u8> {
    macaddr_bytes(addr).to_vec()
}

// ---------------------------------------------------------------------------
// Comparison (mac.c:181-261).
// ---------------------------------------------------------------------------

/// `macaddr_cmp_internal` (mac.c:181): the raw three-way comparator.
pub fn macaddr_cmp_internal(a1: &macaddr, a2: &macaddr) -> i32 {
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

/// `macaddr_cmp` (mac.c:197).
pub fn macaddr_cmp(a1: &macaddr, a2: &macaddr) -> i32 {
    macaddr_cmp_internal(a1, a2)
}

/// `macaddr_lt` (mac.c:210).
pub fn macaddr_lt(a1: &macaddr, a2: &macaddr) -> bool {
    macaddr_cmp_internal(a1, a2) < 0
}

/// `macaddr_le` (mac.c:219).
pub fn macaddr_le(a1: &macaddr, a2: &macaddr) -> bool {
    macaddr_cmp_internal(a1, a2) <= 0
}

/// `macaddr_eq` (mac.c:228).
pub fn macaddr_eq(a1: &macaddr, a2: &macaddr) -> bool {
    macaddr_cmp_internal(a1, a2) == 0
}

/// `macaddr_ge` (mac.c:237).
pub fn macaddr_ge(a1: &macaddr, a2: &macaddr) -> bool {
    macaddr_cmp_internal(a1, a2) >= 0
}

/// `macaddr_gt` (mac.c:246).
pub fn macaddr_gt(a1: &macaddr, a2: &macaddr) -> bool {
    macaddr_cmp_internal(a1, a2) > 0
}

/// `macaddr_ne` (mac.c:255).
pub fn macaddr_ne(a1: &macaddr, a2: &macaddr) -> bool {
    macaddr_cmp_internal(a1, a2) != 0
}

// ---------------------------------------------------------------------------
// Hashing (mac.c:267-281).
// ---------------------------------------------------------------------------

/// `hashmacaddr` (mac.c:267): `hash_any((unsigned char *) key, sizeof(macaddr))`.
pub fn hashmacaddr(key: &macaddr) -> u32 {
    hash_bytes(&macaddr_bytes(key))
}

/// `hashmacaddrextended` (mac.c:275):
/// `hash_any_extended(key, sizeof(macaddr), seed)`.
pub fn hashmacaddrextended(key: &macaddr, seed: u64) -> u64 {
    hash_bytes_extended(&macaddr_bytes(key), seed)
}

// ---------------------------------------------------------------------------
// Arithmetic: bitwise NOT, AND, OR (mac.c:287-333).
// ---------------------------------------------------------------------------

/// `macaddr_not` (mac.c:287): bitwise complement.
pub fn macaddr_not(addr: &macaddr) -> macaddr {
    macaddr {
        a: !addr.a,
        b: !addr.b,
        c: !addr.c,
        d: !addr.d,
        e: !addr.e,
        f: !addr.f,
    }
}

/// `macaddr_and` (mac.c:303): bitwise AND.
pub fn macaddr_and(addr1: &macaddr, addr2: &macaddr) -> macaddr {
    macaddr {
        a: addr1.a & addr2.a,
        b: addr1.b & addr2.b,
        c: addr1.c & addr2.c,
        d: addr1.d & addr2.d,
        e: addr1.e & addr2.e,
        f: addr1.f & addr2.f,
    }
}

/// `macaddr_or` (mac.c:320): bitwise OR.
pub fn macaddr_or(addr1: &macaddr, addr2: &macaddr) -> macaddr {
    macaddr {
        a: addr1.a | addr2.a,
        b: addr1.b | addr2.b,
        c: addr1.c | addr2.c,
        d: addr1.d | addr2.d,
        e: addr1.e | addr2.e,
        f: addr1.f | addr2.f,
    }
}

// ---------------------------------------------------------------------------
// Truncation (mac.c:341).
// ---------------------------------------------------------------------------

/// `macaddr_trunc` (mac.c:341): zero the trailing three bytes so MAC
/// manufacturers can be compared.
pub fn macaddr_trunc(addr: &macaddr) -> macaddr {
    macaddr {
        a: addr.a,
        b: addr.b,
        c: addr.c,
        d: 0,
        e: 0,
        f: 0,
    }
}

// ---------------------------------------------------------------------------
// SortSupport (mac.c:37-43, 363-526).
// ---------------------------------------------------------------------------

/// `SIZEOF_DATUM` for the target ABI (`Datum == usize`).
const SIZEOF_DATUM: usize = core::mem::size_of::<usize>();

/// `macaddr_fast_cmp` (mac.c:399): the SortSupport comparison kernel.  Inputs are
/// the already-unpacked `DatumGetMacaddrP(x)` / `DatumGetMacaddrP(y)` values.
#[inline]
pub fn macaddr_fast_cmp(arg1: &macaddr, arg2: &macaddr) -> i32 {
    macaddr_cmp_internal(arg1, arg2)
}

/// `macaddr_abbrev_convert` (mac.c:477), the pure key-packing core.
///
/// Packs the six bytes of a MAC address into a `Datum`-sized integer and
/// byteswaps to big-endian-native so the unsigned 3-way comparator
/// (`ssup_datum_unsigned_cmp`) orders correctly. On a 64-bit machine
/// (`SIZEOF_DATUM == 8`) the datum is zeroed and the six bytes copied into the
/// low end, leaving two zero padding bytes; on a 32-bit machine only the first
/// `SIZEOF_DATUM` bytes are copied. Minus the HyperLogLog `addHyperLogLog` side
/// effect and `uss->input_count` / `estimating` bookkeeping, which live behind
/// the [`backend_utils_adt_mac_seams::sortsupport`] registrar.
pub fn macaddr_abbrev_convert_bits(authoritative: &macaddr) -> usize {
    let src = macaddr_bytes(authoritative);
    let mut res_bytes = [0u8; SIZEOF_DATUM];
    if SIZEOF_DATUM == 8 {
        // memset(&res, 0, 8); memcpy(&res, authoritative, sizeof(macaddr));
        res_bytes[..src.len()].copy_from_slice(&src);
    } else {
        // 32-bit C branch: memcpy(&res, authoritative, SIZEOF_DATUM). On every
        // real target SIZEOF_DATUM is 4 or 8; macaddr is 6 bytes, so
        // SIZEOF_DATUM <= src.len() and src[..SIZEOF_DATUM] is in-bounds.
        debug_assert!(SIZEOF_DATUM <= src.len());
        res_bytes.copy_from_slice(&src[..SIZEOF_DATUM]);
    }
    let res = usize::from_ne_bytes(res_bytes);

    // res = DatumBigEndianToNative(res); -- byteswap on little-endian.
    if cfg!(target_endian = "little") {
        res.swap_bytes()
    } else {
        res
    }
}

/// `macaddr_sortsupport` (mac.c:363): SortSupport strategy routine.
///
/// Installing `macaddr_fast_cmp` / `macaddr_abbrev_convert` /
/// `macaddr_abbrev_abort` into the live `SortSupportData` node — together with
/// allocating the `macaddr_sortsupport_state` in `ssup_cxt`, the HyperLogLog
/// estimator, and the `trace_sort` LOG lines — belongs to the tuplesort /
/// `lib/hyperloglog` subsystems and is delegated to the
/// [`backend_utils_adt_mac_seams::sortsupport::register`] seam. The pure
/// comparator is [`macaddr_fast_cmp`] and the pure key packing is
/// [`macaddr_abbrev_convert_bits`]. Returns whether a registrar was wired (the
/// default is a faithful no-op, as if sortsupport were never registered).
pub fn macaddr_sortsupport() -> bool {
    sortsupport::register::call()
}

// ---------------------------------------------------------------------------
// sscanf emulation for the seven accepted notations (mac.c:71-90).
//
// Each C variant is `sscanf(str, "<fmt>%1s", &a..&f, junk)` and is accepted only
// if it converts exactly six octets *and* the trailing `%1s` matched nothing
// (i.e. the C return value is 6, not 7).  We mirror C's scanf rules for the
// conversions actually used: `%x` and `%2x` skip leading whitespace, accept an
// optional sign and `0x`/`0X` prefix, then read hex digits (bounded by the field
// width).  The trailing `%1s` matches one run of non-whitespace characters after
// skipping whitespace; if any such character exists the input is rejected.
// ---------------------------------------------------------------------------

/// Cursor over the input bytes, mirroring `sscanf`'s consumption.
struct Scanner<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Scanner<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Scanner { bytes, pos: 0 }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn skip_whitespace(&mut self) {
        while let Some(byte) = self.peek() {
            if is_c_space(byte) {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    /// `%x` (width `None`) or `%Nx` (width `Some(N)`): a hexadecimal conversion.
    ///
    /// Returns the converted value, or `None` on a matching failure (which makes
    /// the enclosing `sscanf` stop and yield a count short of six).
    fn scan_hex(&mut self, width: Option<usize>) -> Option<i64> {
        // %conversions (other than %c/%[/%n) skip leading whitespace.
        self.skip_whitespace();

        let max = width.unwrap_or(usize::MAX);
        let mut consumed = 0usize; // chars counted against the field width
        let mut negative = false;

        // Optional sign.
        match self.peek() {
            Some(b'+') if consumed < max => {
                self.pos += 1;
                consumed += 1;
            }
            Some(b'-') if consumed < max => {
                self.pos += 1;
                consumed += 1;
                negative = true;
            }
            _ => {}
        }

        // Optional "0x" / "0X" prefix, only if a hex digit could still follow
        // within the field width.
        if consumed < max && self.peek() == Some(b'0') {
            // Tentatively consume the leading '0'.
            let save_pos = self.pos;
            let save_consumed = consumed;
            self.pos += 1;
            consumed += 1;
            if consumed < max && matches!(self.peek(), Some(b'x') | Some(b'X')) {
                self.pos += 1;
                consumed += 1;
                // If no hex digit follows the prefix (within width), the '0x'
                // is rolled back to a single matched '0' (glibc treats the prior
                // '0' as the conversion result).
                if consumed >= max || !self.peek().is_some_and(is_hex_digit) {
                    // The value is the '0' already read; the 'x' is left
                    // unconsumed so the literal scan after the conversion can
                    // (fail to) match it.  We've over-consumed 'x', so back up.
                    self.pos -= 1;
                    return Some(0);
                }
                // Fall through: digits accumulate below, starting from value 0.
                let mut value: i64 = 0;
                while consumed < max {
                    match self.peek() {
                        Some(byte) if is_hex_digit(byte) => {
                            value = value.wrapping_mul(16).wrapping_add(hex_value(byte) as i64);
                            self.pos += 1;
                            consumed += 1;
                        }
                        _ => break,
                    }
                }
                return Some(if negative { -value } else { value });
            }
            // Not a prefix; restore and let the digit loop pick up the '0'.
            self.pos = save_pos;
            consumed = save_consumed;
        }

        // Plain hex digit run.
        let mut value: i64 = 0;
        let mut any = false;
        while consumed < max {
            match self.peek() {
                Some(byte) if is_hex_digit(byte) => {
                    value = value.wrapping_mul(16).wrapping_add(hex_value(byte) as i64);
                    self.pos += 1;
                    consumed += 1;
                    any = true;
                }
                _ => break,
            }
        }

        if !any {
            return None;
        }
        Some(if negative { -value } else { value })
    }

    /// Match a single literal byte (a separator in the format string).  Literal
    /// characters in a scanf format are matched verbatim and do not skip
    /// whitespace.  Returns `false` on mismatch.
    fn scan_literal(&mut self, expected: u8) -> bool {
        if self.peek() == Some(expected) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    /// `%1s`: skip whitespace, then read up to one non-whitespace byte.  Returns
    /// `true` iff at least one such byte was read (the C `%1s` "matched").
    fn scan_trailing_junk(&mut self) -> bool {
        self.skip_whitespace();
        match self.peek() {
            Some(byte) if !is_c_space(byte) => {
                self.pos += 1;
                true
            }
            _ => false,
        }
    }
}

/// Run six `%x`/`%2x` conversions separated by `sep`, then a trailing `%1s`.
///
/// `colon_pairs` selects between the `%x` (one conversion per octet, used by the
/// `:` and `-` separated forms, mac.c:71-75) and the `%2x%2x%2x:%2x%2x%2x` form
/// (mac.c:77).  Returns the six octets iff the whole address converted with no
/// trailing junk (C `count == 6`).
fn scan_macaddr(bytes: &[u8], sep: u8, colon_pairs: bool) -> Option<[i64; 6]> {
    let mut scanner = Scanner::new(bytes);
    let mut out = [0i64; 6];

    if colon_pairs {
        // "%2x%2x%2x:%2x%2x%2x%1s" (mac.c:77).
        out[0] = scanner.scan_hex(Some(2))?;
        out[1] = scanner.scan_hex(Some(2))?;
        out[2] = scanner.scan_hex(Some(2))?;
        if !scanner.scan_literal(sep) {
            return None;
        }
        out[3] = scanner.scan_hex(Some(2))?;
        out[4] = scanner.scan_hex(Some(2))?;
        out[5] = scanner.scan_hex(Some(2))?;
    } else {
        // "%x:%x:%x:%x:%x:%x%1s" / "%x-%x-...-%x%1s" (mac.c:71, 74).
        out[0] = scanner.scan_hex(None)?;
        for slot in out.iter_mut().skip(1) {
            if !scanner.scan_literal(sep) {
                return None;
            }
            *slot = scanner.scan_hex(None)?;
        }
    }

    if scanner.scan_trailing_junk() {
        return None;
    }
    Some(out)
}

/// "%2x%2x%2x-%2x%2x%2x%1s" (mac.c:80).
fn scan_macaddr_dash_pairs(bytes: &[u8]) -> Option<[i64; 6]> {
    scan_macaddr(bytes, b'-', true)
}

/// "%2x%2x.%2x%2x.%2x%2x%1s" (mac.c:83).
fn scan_macaddr_dot(bytes: &[u8]) -> Option<[i64; 6]> {
    let mut scanner = Scanner::new(bytes);
    let mut out = [0i64; 6];
    out[0] = scanner.scan_hex(Some(2))?;
    out[1] = scanner.scan_hex(Some(2))?;
    if !scanner.scan_literal(b'.') {
        return None;
    }
    out[2] = scanner.scan_hex(Some(2))?;
    out[3] = scanner.scan_hex(Some(2))?;
    if !scanner.scan_literal(b'.') {
        return None;
    }
    out[4] = scanner.scan_hex(Some(2))?;
    out[5] = scanner.scan_hex(Some(2))?;
    if scanner.scan_trailing_junk() {
        return None;
    }
    Some(out)
}

/// "%2x%2x-%2x%2x-%2x%2x%1s" (mac.c:86).
fn scan_macaddr_dash_doubles(bytes: &[u8]) -> Option<[i64; 6]> {
    let mut scanner = Scanner::new(bytes);
    let mut out = [0i64; 6];
    out[0] = scanner.scan_hex(Some(2))?;
    out[1] = scanner.scan_hex(Some(2))?;
    if !scanner.scan_literal(b'-') {
        return None;
    }
    out[2] = scanner.scan_hex(Some(2))?;
    out[3] = scanner.scan_hex(Some(2))?;
    if !scanner.scan_literal(b'-') {
        return None;
    }
    out[4] = scanner.scan_hex(Some(2))?;
    out[5] = scanner.scan_hex(Some(2))?;
    if scanner.scan_trailing_junk() {
        return None;
    }
    Some(out)
}

/// "%2x%2x%2x%2x%2x%2x%1s" (mac.c:89).
fn scan_macaddr_plain(bytes: &[u8]) -> Option<[i64; 6]> {
    let mut scanner = Scanner::new(bytes);
    let mut out = [0i64; 6];
    for slot in out.iter_mut() {
        *slot = scanner.scan_hex(Some(2))?;
    }
    if scanner.scan_trailing_junk() {
        return None;
    }
    Some(out)
}

/// C `isspace` for the `"C"` locale (what scanf uses to skip whitespace).
#[inline]
fn is_c_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}

#[inline]
fn is_hex_digit(byte: u8) -> bool {
    byte.is_ascii_hexdigit()
}

#[inline]
fn hex_value(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => 0,
    }
}

/// Install this unit's outward seams. `mac.c` owns no inward seam consumed
/// elsewhere (its functions are reached directly or via the not-yet-modeled
/// fmgr/PGFunction registry), so this installs nothing; the
/// `backend-utils-adt-mac-seams::sortsupport::register` slot is OUTWARD, owned
/// by the unported tuplesort/hyperloglog subsystem.
pub fn init_seams() {
    fmgr_builtins::register_mac_builtins();
}
