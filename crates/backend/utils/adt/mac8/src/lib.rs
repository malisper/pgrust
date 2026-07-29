//! mac8.c: EUI-64 macaddr8 cores. EUI-48 input is stored as EUI-64 with FF/FE
//! in bytes 4/5. No sortsupport in the C unit.

pub mod builtins;
#[cfg(test)]
mod tests;

extern crate alloc;

use alloc::format;

use ::adt_mac::{is_c_space, MacAddr};
use ::datum::Bytea;
use ::mcx::Mcx;
use ::stringinfo::StringInfo;
use ::types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MacAddr8 {
    pub a: u8,
    pub b: u8,
    pub c: u8,
    pub d: u8,
    pub e: u8,
    pub f: u8,
    pub g: u8,
    pub h: u8,
}

const _: () = assert!(core::mem::size_of::<MacAddr8>() == 8);

impl MacAddr8 {
    #[inline]
    pub fn from_bytes(b: [u8; 8]) -> MacAddr8 {
        MacAddr8 {
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

    #[inline]
    pub fn to_bytes(self) -> [u8; 8] {
        [
            self.a, self.b, self.c, self.d, self.e, self.f, self.g, self.h,
        ]
    }
}

// C packs into (unsigned long)(a<<24|...): the int sign-extension for
// a >= 0x80 is order-preserving vs plain u32 packing, so u32 is exact.
#[inline]
fn hibits(addr: &MacAddr8) -> u32 {
    ((addr.a as u32) << 24) | ((addr.b as u32) << 16) | ((addr.c as u32) << 8) | (addr.d as u32)
}

#[inline]
fn lobits(addr: &MacAddr8) -> u32 {
    ((addr.e as u32) << 24) | ((addr.f as u32) << 16) | ((addr.g as u32) << 8) | (addr.h as u32)
}

const HEXLOOKUP: [i8; 128] = {
    let mut t = [-1i8; 128];
    let mut i = 0u8;
    while i < 10 {
        t[(b'0' + i) as usize] = i as i8;
        i += 1;
    }
    let mut j = 0u8;
    while j < 6 {
        t[(b'a' + j) as usize] = (10 + j) as i8;
        t[(b'A' + j) as usize] = (10 + j) as i8;
        j += 1;
    }
    t
};

#[inline]
fn hex2_to_uchar(bytes: &[u8], pos: usize, badhex: &mut bool) -> u8 {
    let hi = bytes.get(pos).copied().unwrap_or(0);
    if hi > 127 || HEXLOOKUP[hi as usize] < 0 {
        *badhex = true;
        return 0;
    }
    let lo = bytes.get(pos + 1).copied().unwrap_or(0);
    if lo > 127 || HEXLOOKUP[lo as usize] < 0 {
        *badhex = true;
        return 0;
    }
    ((HEXLOOKUP[hi as usize] as u8) << 4) + HEXLOOKUP[lo as usize] as u8
}

#[cold]
#[inline(never)]
fn invalid_syntax_err(input: &str) -> PgError {
    PgError::error(format!(
        "invalid input syntax for type {}: \"{input}\"",
        "macaddr8"
    ))
    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}

pub fn macaddr8_in(str: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<MacAddr8> {
    match macaddr8_in_internal(str.as_bytes()) {
        Some(addr) => Ok(addr),
        None => ereturn(escontext, MacAddr8::default(), invalid_syntax_err(str)),
    }
}

// The C state machine verbatim: NUL-terminated cursor semantics, so reads at
// or past bytes.len() yield 0.
// pub (visibility only) for the proofs/mac8 Kani equivalence harnesses.
pub fn macaddr8_in_internal(bytes: &[u8]) -> Option<MacAddr8> {
    let at = |i: usize| bytes.get(i).copied().unwrap_or(0);
    let mut ptr = 0usize;
    let mut badhex = false;
    let mut out = [0u8; 8];
    let mut count = 0usize;
    let mut spacer = 0u8;

    while at(ptr) != 0 && is_c_space(at(ptr)) {
        ptr += 1;
    }

    // Digits must always come in pairs.
    while at(ptr) != 0 && at(ptr + 1) != 0 {
        count += 1;
        if count > 8 {
            return None;
        }
        out[count - 1] = hex2_to_uchar(bytes, ptr, &mut badhex);
        if badhex {
            return None;
        }
        ptr += 2;

        if matches!(at(ptr), b':' | b'-' | b'.') {
            if spacer == 0 {
                spacer = at(ptr);
            } else if spacer != at(ptr) {
                return None;
            }
            ptr += 1;
        }

        if (count == 6 || count == 8) && at(ptr) != 0 && is_c_space(at(ptr)) {
            loop {
                ptr += 1;
                if at(ptr) == 0 || !is_c_space(at(ptr)) {
                    break;
                }
            }
            if at(ptr) != 0 {
                return None;
            }
        }
    }

    if count == 6 {
        out[7] = out[5];
        out[6] = out[4];
        out[5] = out[3];
        out[3] = 0xFF;
        out[4] = 0xFE;
    } else if count != 8 {
        return None;
    }

    Some(MacAddr8::from_bytes(out))
}

pub const MACADDR8_OUT_LEN: usize = 24;

pub fn macaddr8_out_into(addr: &MacAddr8, buf: &mut [u8; MACADDR8_OUT_LEN]) -> usize {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = addr.to_bytes();
    for (i, byte) in bytes.iter().enumerate() {
        buf[i * 3] = HEX[(byte >> 4) as usize];
        buf[i * 3 + 1] = HEX[(byte & 0x0f) as usize];
        if i < 7 {
            buf[i * 3 + 2] = b':';
        }
    }
    23
}

pub fn macaddr8_recv(buf: &mut StringInfo<'_>) -> PgResult<MacAddr8> {
    let mut addr = MacAddr8 {
        a: pqformat::pq_getmsgbyte(buf)? as u8,
        b: pqformat::pq_getmsgbyte(buf)? as u8,
        c: pqformat::pq_getmsgbyte(buf)? as u8,
        ..Default::default()
    };
    if buf.len() == 6 {
        addr.d = 0xFF;
        addr.e = 0xFE;
    } else {
        addr.d = pqformat::pq_getmsgbyte(buf)? as u8;
        addr.e = pqformat::pq_getmsgbyte(buf)? as u8;
    }
    addr.f = pqformat::pq_getmsgbyte(buf)? as u8;
    addr.g = pqformat::pq_getmsgbyte(buf)? as u8;
    addr.h = pqformat::pq_getmsgbyte(buf)? as u8;
    Ok(addr)
}

pub fn macaddr8_send<'mcx>(mcx: Mcx<'mcx>, addr: &MacAddr8) -> PgResult<Bytea<'mcx>> {
    let mut buf = pqformat::pq_begintypsend(mcx)?;
    for byte in addr.to_bytes() {
        pqformat::pq_sendbyte(&mut buf, byte)?;
    }
    Ok(pqformat::pq_endtypsend(buf))
}

pub fn macaddr8_cmp_internal(a1: &MacAddr8, a2: &MacAddr8) -> i32 {
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

pub fn macaddr8_cmp(a1: &MacAddr8, a2: &MacAddr8) -> i32 {
    macaddr8_cmp_internal(a1, a2)
}

pub fn macaddr8_lt(a1: &MacAddr8, a2: &MacAddr8) -> bool {
    macaddr8_cmp_internal(a1, a2) < 0
}

pub fn macaddr8_le(a1: &MacAddr8, a2: &MacAddr8) -> bool {
    macaddr8_cmp_internal(a1, a2) <= 0
}

pub fn macaddr8_eq(a1: &MacAddr8, a2: &MacAddr8) -> bool {
    macaddr8_cmp_internal(a1, a2) == 0
}

pub fn macaddr8_ge(a1: &MacAddr8, a2: &MacAddr8) -> bool {
    macaddr8_cmp_internal(a1, a2) >= 0
}

pub fn macaddr8_gt(a1: &MacAddr8, a2: &MacAddr8) -> bool {
    macaddr8_cmp_internal(a1, a2) > 0
}

pub fn macaddr8_ne(a1: &MacAddr8, a2: &MacAddr8) -> bool {
    macaddr8_cmp_internal(a1, a2) != 0
}

pub fn hashmacaddr8(key: &MacAddr8) -> u32 {
    hashfn::hash_bytes(&key.to_bytes())
}

pub fn hashmacaddr8extended(key: &MacAddr8, seed: u64) -> u64 {
    hashfn::hash_bytes_extended(&key.to_bytes(), seed)
}

pub fn macaddr8_not(addr: &MacAddr8) -> MacAddr8 {
    MacAddr8::from_bytes(addr.to_bytes().map(|b| !b))
}

pub fn macaddr8_and(addr1: &MacAddr8, addr2: &MacAddr8) -> MacAddr8 {
    let (b1, b2) = (addr1.to_bytes(), addr2.to_bytes());
    MacAddr8::from_bytes(core::array::from_fn(|i| b1[i] & b2[i]))
}

pub fn macaddr8_or(addr1: &MacAddr8, addr2: &MacAddr8) -> MacAddr8 {
    let (b1, b2) = (addr1.to_bytes(), addr2.to_bytes());
    MacAddr8::from_bytes(core::array::from_fn(|i| b1[i] | b2[i]))
}

pub fn macaddr8_trunc(addr: &MacAddr8) -> MacAddr8 {
    MacAddr8 {
        a: addr.a,
        b: addr.b,
        c: addr.c,
        ..Default::default()
    }
}

pub fn macaddr8_set7bit(addr: &MacAddr8) -> MacAddr8 {
    MacAddr8 {
        a: addr.a | 0x02,
        ..*addr
    }
}

pub fn macaddrtomacaddr8(addr6: &MacAddr) -> MacAddr8 {
    MacAddr8 {
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

#[cold]
#[inline(never)]
fn out_of_range_err() -> PgError {
    PgError::error("macaddr8 data out of range to convert to macaddr")
        .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
        .with_hint(
            "Only addresses that have FF and FE as values in the 4th and 5th bytes from the left, for example xx:xx:xx:ff:fe:xx:xx:xx, are eligible to be converted from macaddr8 to macaddr.",
        )
}

pub fn macaddr8tomacaddr(addr: &MacAddr8) -> PgResult<MacAddr> {
    if addr.d != 0xFF || addr.e != 0xFE {
        return Err(out_of_range_err().into());
    }
    Ok(MacAddr {
        a: addr.a,
        b: addr.b,
        c: addr.c,
        d: addr.f,
        e: addr.g,
        f: addr.h,
    })
}
