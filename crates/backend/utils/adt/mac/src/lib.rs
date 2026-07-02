//! mac.c: EUI-48 macaddr cores. macaddr_sortsupport (abbrev keys +
//! HyperLogLog) waits on the SortSupport unit (btnamesortsupport precedent).

pub mod builtins;
#[cfg(test)]
mod tests;

extern crate alloc;

use alloc::format;

use ::datum::Bytea;
use ::mcx::Mcx;
use ::stringinfo::StringInfo;
use ::types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MacAddr {
    pub a: u8,
    pub b: u8,
    pub c: u8,
    pub d: u8,
    pub e: u8,
    pub f: u8,
}

const _: () = assert!(core::mem::size_of::<MacAddr>() == 6);

impl MacAddr {
    #[inline]
    pub fn from_bytes(b: [u8; 6]) -> MacAddr {
        MacAddr {
            a: b[0],
            b: b[1],
            c: b[2],
            d: b[3],
            e: b[4],
            f: b[5],
        }
    }

    #[inline]
    pub fn to_bytes(self) -> [u8; 6] {
        [self.a, self.b, self.c, self.d, self.e, self.f]
    }
}

#[inline]
fn hibits(addr: &MacAddr) -> u32 {
    ((addr.a as u32) << 16) | ((addr.b as u32) << 8) | (addr.c as u32)
}

#[inline]
fn lobits(addr: &MacAddr) -> u32 {
    ((addr.d as u32) << 16) | ((addr.e as u32) << 8) | (addr.f as u32)
}

#[inline]
pub fn is_c_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

#[inline]
fn hex_value(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => byte - b'A' + 10,
    }
}

// sscanf consumption model for the directives mac.c uses: %x / %Nx skip
// whitespace then take an optional sign and 0x prefix; literals match
// verbatim; the trailing %1s skips whitespace then matches one junk byte.
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
        while self.peek().is_some_and(is_c_space) {
            self.pos += 1;
        }
    }

    fn scan_hex(&mut self, width: Option<usize>) -> Option<i64> {
        self.skip_whitespace();
        let max = width.unwrap_or(usize::MAX);
        let mut consumed = 0usize;
        let mut negative = false;

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

        if consumed < max && self.peek() == Some(b'0') {
            let save_pos = self.pos;
            let save_consumed = consumed;
            self.pos += 1;
            consumed += 1;
            if consumed < max && matches!(self.peek(), Some(b'x') | Some(b'X')) {
                self.pos += 1;
                consumed += 1;
                if consumed >= max || !self.peek().is_some_and(|b| b.is_ascii_hexdigit()) {
                    // glibc keeps the leading '0' as the result, 'x' unread.
                    self.pos -= 1;
                    return Some(0);
                }
                let mut value: i64 = 0;
                while consumed < max {
                    match self.peek() {
                        Some(byte) if byte.is_ascii_hexdigit() => {
                            value = value.wrapping_mul(16).wrapping_add(hex_value(byte) as i64);
                            self.pos += 1;
                            consumed += 1;
                        }
                        _ => break,
                    }
                }
                return Some(if negative { -value } else { value });
            }
            self.pos = save_pos;
            consumed = save_consumed;
        }

        let mut value: i64 = 0;
        let mut any = false;
        while consumed < max {
            match self.peek() {
                Some(byte) if byte.is_ascii_hexdigit() => {
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

    fn scan_literal(&mut self, expected: u8) -> bool {
        if self.peek() == Some(expected) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn scan_trailing_junk(&mut self) -> bool {
        self.skip_whitespace();
        self.peek().is_some_and(|b| !is_c_space(b))
    }
}

// "%x<sep>%x..." (mac.c:71,74) or "%2x%2x%2x<sep>%2x%2x%2x" (mac.c:77,80).
fn scan_six(bytes: &[u8], sep: u8, pairs: bool) -> Option<[i64; 6]> {
    let mut s = Scanner::new(bytes);
    let mut out = [0i64; 6];
    if pairs {
        for slot in out.iter_mut().take(3) {
            *slot = s.scan_hex(Some(2))?;
        }
        if !s.scan_literal(sep) {
            return None;
        }
        for slot in out.iter_mut().skip(3) {
            *slot = s.scan_hex(Some(2))?;
        }
    } else {
        out[0] = s.scan_hex(None)?;
        for slot in out.iter_mut().skip(1) {
            if !s.scan_literal(sep) {
                return None;
            }
            *slot = s.scan_hex(None)?;
        }
    }
    if s.scan_trailing_junk() {
        return None;
    }
    Some(out)
}

// "%2x%2x<sep>%2x%2x<sep>%2x%2x" (mac.c:83,86) or unseparated (mac.c:89).
fn scan_double_groups(bytes: &[u8], sep: Option<u8>) -> Option<[i64; 6]> {
    let mut s = Scanner::new(bytes);
    let mut out = [0i64; 6];
    for (i, slot) in out.iter_mut().enumerate() {
        if i == 2 || i == 4 {
            if let Some(sep) = sep {
                if !s.scan_literal(sep) {
                    return None;
                }
            }
        }
        *slot = s.scan_hex(Some(2))?;
    }
    if s.scan_trailing_junk() {
        return None;
    }
    Some(out)
}

#[cold]
#[inline(never)]
fn invalid_syntax_err(input: &str) -> PgError {
    PgError::error(format!(
        "invalid input syntax for type {}: \"{input}\"",
        "macaddr"
    ))
    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}

#[cold]
#[inline(never)]
fn octet_range_err(input: &str) -> PgError {
    PgError::error(format!(
        "invalid octet value in \"macaddr\" value: \"{input}\""
    ))
    .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

pub fn macaddr_in(str: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<MacAddr> {
    let bytes = str.as_bytes();
    let parsed = scan_six(bytes, b':', false)
        .or_else(|| scan_six(bytes, b'-', false))
        .or_else(|| scan_six(bytes, b':', true))
        .or_else(|| scan_six(bytes, b'-', true))
        .or_else(|| scan_double_groups(bytes, Some(b'.')))
        .or_else(|| scan_double_groups(bytes, Some(b'-')))
        .or_else(|| scan_double_groups(bytes, None));

    let Some(octets) = parsed else {
        return ereturn(escontext, MacAddr::default(), invalid_syntax_err(str));
    };

    if octets.iter().any(|&v| !(0..=255).contains(&v)) {
        return ereturn(escontext, MacAddr::default(), octet_range_err(str));
    }

    Ok(MacAddr::from_bytes(octets.map(|v| v as u8)))
}

pub const MACADDR_OUT_LEN: usize = 18;

pub fn macaddr_out_into(addr: &MacAddr, buf: &mut [u8; MACADDR_OUT_LEN]) -> usize {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = addr.to_bytes();
    for (i, byte) in bytes.iter().enumerate() {
        buf[i * 3] = HEX[(byte >> 4) as usize];
        buf[i * 3 + 1] = HEX[(byte & 0x0f) as usize];
        if i < 5 {
            buf[i * 3 + 2] = b':';
        }
    }
    17
}

pub fn macaddr_recv(buf: &mut StringInfo<'_>) -> PgResult<MacAddr> {
    Ok(MacAddr {
        a: pqformat::pq_getmsgbyte(buf)? as u8,
        b: pqformat::pq_getmsgbyte(buf)? as u8,
        c: pqformat::pq_getmsgbyte(buf)? as u8,
        d: pqformat::pq_getmsgbyte(buf)? as u8,
        e: pqformat::pq_getmsgbyte(buf)? as u8,
        f: pqformat::pq_getmsgbyte(buf)? as u8,
    })
}

pub fn macaddr_send<'mcx>(mcx: Mcx<'mcx>, addr: &MacAddr) -> PgResult<Bytea<'mcx>> {
    let mut buf = pqformat::pq_begintypsend(mcx)?;
    for byte in addr.to_bytes() {
        pqformat::pq_sendbyte(&mut buf, byte)?;
    }
    Ok(pqformat::pq_endtypsend(buf))
}

pub fn macaddr_cmp_internal(a1: &MacAddr, a2: &MacAddr) -> i32 {
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

pub fn macaddr_cmp(a1: &MacAddr, a2: &MacAddr) -> i32 {
    macaddr_cmp_internal(a1, a2)
}

pub fn macaddr_lt(a1: &MacAddr, a2: &MacAddr) -> bool {
    macaddr_cmp_internal(a1, a2) < 0
}

pub fn macaddr_le(a1: &MacAddr, a2: &MacAddr) -> bool {
    macaddr_cmp_internal(a1, a2) <= 0
}

pub fn macaddr_eq(a1: &MacAddr, a2: &MacAddr) -> bool {
    macaddr_cmp_internal(a1, a2) == 0
}

pub fn macaddr_ge(a1: &MacAddr, a2: &MacAddr) -> bool {
    macaddr_cmp_internal(a1, a2) >= 0
}

pub fn macaddr_gt(a1: &MacAddr, a2: &MacAddr) -> bool {
    macaddr_cmp_internal(a1, a2) > 0
}

pub fn macaddr_ne(a1: &MacAddr, a2: &MacAddr) -> bool {
    macaddr_cmp_internal(a1, a2) != 0
}

pub fn hashmacaddr(key: &MacAddr) -> u32 {
    hashfn::hash_bytes(&key.to_bytes())
}

pub fn hashmacaddrextended(key: &MacAddr, seed: u64) -> u64 {
    hashfn::hash_bytes_extended(&key.to_bytes(), seed)
}

pub fn macaddr_not(addr: &MacAddr) -> MacAddr {
    MacAddr::from_bytes(addr.to_bytes().map(|b| !b))
}

pub fn macaddr_and(addr1: &MacAddr, addr2: &MacAddr) -> MacAddr {
    let (b1, b2) = (addr1.to_bytes(), addr2.to_bytes());
    MacAddr::from_bytes(core::array::from_fn(|i| b1[i] & b2[i]))
}

pub fn macaddr_or(addr1: &MacAddr, addr2: &MacAddr) -> MacAddr {
    let (b1, b2) = (addr1.to_bytes(), addr2.to_bytes());
    MacAddr::from_bytes(core::array::from_fn(|i| b1[i] | b2[i]))
}

pub fn macaddr_trunc(addr: &MacAddr) -> MacAddr {
    MacAddr {
        a: addr.a,
        b: addr.b,
        c: addr.c,
        d: 0,
        e: 0,
        f: 0,
    }
}
