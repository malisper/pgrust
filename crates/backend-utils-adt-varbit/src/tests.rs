//! Unit tests for varbit, checked against PostgreSQL behavior.

use super::*;
use std::sync::Once;

static INIT: Once = Once::new();

/// The only seam this crate calls is `pg_mblen_range` (used solely to size the
/// fragment printed in a bad-digit error message). Install a single-byte mock.
fn init() {
    INIT.call_once(|| {
        mb::pg_mblen_range::set(|_s: &[u8]| 1i32);
    });
}

fn vb<'mcx>(mcx: Mcx<'mcx>, bit_len: i32, bytes: &[u8]) -> VarBit<'mcx> {
    VarBit {
        bit_len,
        data: mcx::slice_in(mcx, bytes).unwrap(),
    }
}

#[test]
fn in_bit_basic() {
    let root = mcx::MemoryContext::new("test");
    // '101' -> bit(3): byte 0b10100000 = 0xA0
    let r = bit_in(root.mcx(), b"101", 0, None).unwrap().unwrap();
    assert_eq!(r.bit_len, 3);
    assert_eq!(&r.data[..], &[0xA0]);
}

#[test]
fn in_bit_prefixed_and_hex() {
    let root = mcx::MemoryContext::new("test");
    let r = bit_in(root.mcx(), b"B101", 0, None).unwrap().unwrap();
    assert_eq!(&r.data[..], &[0xA0]);
    assert_eq!(r.bit_len, 3);

    // x'A' = 1010 -> bit_len 4, byte 0xA0
    let r = bit_in(root.mcx(), b"xA", 0, None).unwrap().unwrap();
    assert_eq!(r.bit_len, 4);
    assert_eq!(&r.data[..], &[0xA0]);

    // x'FF' = 8 bits 0xFF
    let r = bit_in(root.mcx(), b"xFF", 0, None).unwrap().unwrap();
    assert_eq!(r.bit_len, 8);
    assert_eq!(&r.data[..], &[0xFF]);
}

#[test]
fn in_bit_typmod_mismatch() {
    let root = mcx::MemoryContext::new("test");
    // bit(4) with 3 bits errors
    assert!(bit_in(root.mcx(), b"101", 4, None).is_err());
    // exact match ok
    let r = bit_in(root.mcx(), b"1010", 4, None).unwrap().unwrap();
    assert_eq!(r.bit_len, 4);
}

#[test]
fn in_bad_digit() {
    init();
    let root = mcx::MemoryContext::new("test");
    assert!(bit_in(root.mcx(), b"102", 0, None).is_err());
    assert!(bit_in(root.mcx(), b"xG", 0, None).is_err());
}

#[test]
fn varbit_in_truncates_typmod() {
    let root = mcx::MemoryContext::new("test");
    // varbit(8) with 3 bits: fits, bit_len = 3
    let r = varbit_in(root.mcx(), b"101", 8, None).unwrap().unwrap();
    assert_eq!(r.bit_len, 3);
    // too long errors
    assert!(varbit_in(root.mcx(), b"101010", 4, None).is_err());
}

#[test]
fn out_roundtrip() {
    let root = mcx::MemoryContext::new("test");
    let v = vb(root.mcx(), 3, &[0xA0]);
    let s = varbit_out(root.mcx(), v.as_ref()).unwrap();
    assert_eq!(&s[..s.len() - 1], b"101");
    assert_eq!(s[s.len() - 1], 0);

    let v = vb(root.mcx(), 8, &[0xFF]);
    let s = bit_out(root.mcx(), v.as_ref()).unwrap();
    assert_eq!(&s[..s.len() - 1], b"11111111");
}

#[test]
fn cmp_ops() {
    let root = mcx::MemoryContext::new("test");
    let a = vb(root.mcx(), 4, &[0xA0]); // 1010
    let b = vb(root.mcx(), 4, &[0xB0]); // 1011
    assert!(bitlt(a.as_ref(), b.as_ref()));
    assert!(!bitgt(a.as_ref(), b.as_ref()));
    assert!(biteq(a.as_ref(), a.as_ref()));
    assert!(bitne(a.as_ref(), b.as_ref()));
    assert_eq!(bitcmp(a.as_ref(), b.as_ref()), -1);

    // different lengths: longer is bigger when prefix equal
    let c = vb(root.mcx(), 3, &[0xA0]); // 101
    let d = vb(root.mcx(), 4, &[0xA0]); // 1010
    assert!(bitlt(c.as_ref(), d.as_ref()));
    assert!(!biteq(c.as_ref(), d.as_ref()));
}

#[test]
fn logical_ops() {
    let root = mcx::MemoryContext::new("test");
    let a = vb(root.mcx(), 8, &[0b11001100]);
    let b = vb(root.mcx(), 8, &[0b10101010]);
    assert_eq!(&bit_and(root.mcx(), a.as_ref(), b.as_ref()).unwrap().data[..], &[0b10001000]);
    assert_eq!(&bit_or(root.mcx(), a.as_ref(), b.as_ref()).unwrap().data[..], &[0b11101110]);
    assert_eq!(&bitxor(root.mcx(), a.as_ref(), b.as_ref()).unwrap().data[..], &[0b01100110]);
    // not pads correctly: 3-bit string 101 -> not is 010 padded to 0b01000000
    let c = vb(root.mcx(), 3, &[0b10100000]);
    let n = bitnot(root.mcx(), c.as_ref()).unwrap();
    assert_eq!(&n.data[..], &[0b01000000]);

    // mismatched sizes error
    let e = vb(root.mcx(), 4, &[0xA0]);
    assert!(bit_and(root.mcx(), a.as_ref(), e.as_ref()).is_err());
}

#[test]
fn cat() {
    let root = mcx::MemoryContext::new("test");
    // 101 || 11 = 10111 (5 bits) -> 0b10111000
    let a = vb(root.mcx(), 3, &[0b10100000]);
    let b = vb(root.mcx(), 2, &[0b11000000]);
    let r = bitcat(root.mcx(), a.as_ref(), b.as_ref()).unwrap();
    assert_eq!(r.bit_len, 5);
    assert_eq!(&r.data[..], &[0b10111000]);
}

#[test]
fn substr() {
    let root = mcx::MemoryContext::new("test");
    // 11010 (5 bits) substring(2, 3) = 101
    let a = vb(root.mcx(), 5, &[0b11010000]);
    let r = bitsubstr(root.mcx(), a.as_ref(), 2, 3).unwrap();
    assert_eq!(r.bit_len, 3);
    assert_eq!(&r.data[..], &[0b10100000]);

    // no-len from position 3 = 010
    let r = bitsubstr_no_len(root.mcx(), a.as_ref(), 3).unwrap();
    assert_eq!(r.bit_len, 3);
    assert_eq!(&r.data[..], &[0b01000000]);

    // negative length errors
    assert!(bitsubstr(root.mcx(), a.as_ref(), 1, -1).is_err());
}

#[test]
fn shifts() {
    let root = mcx::MemoryContext::new("test");
    let a = vb(root.mcx(), 8, &[0b00010010]);
    // left shift 2 -> 01001000
    let l = bitshiftleft(root.mcx(), a.as_ref(), 2).unwrap();
    assert_eq!(&l.data[..], &[0b01001000]);
    // right shift 2 -> 00000100
    let r = bitshiftright(root.mcx(), a.as_ref(), 2).unwrap();
    assert_eq!(&r.data[..], &[0b00000100]);
    // negative left == right
    let nl = bitshiftleft(root.mcx(), a.as_ref(), -2).unwrap();
    assert_eq!(&nl.data[..], &r.data[..]);
    // shift all out
    let z = bitshiftleft(root.mcx(), a.as_ref(), 8).unwrap();
    assert_eq!(&z.data[..], &[0u8]);
}

#[test]
fn count_and_length() {
    let root = mcx::MemoryContext::new("test");
    let a = vb(root.mcx(), 8, &[0b10110010]);
    assert_eq!(bit_bit_count(a.as_ref()), 4);
    assert_eq!(bitlength(a.as_ref()), 8);
    assert_eq!(bitoctetlength(a.as_ref()), 1);
}

#[test]
fn int_casts() {
    let root = mcx::MemoryContext::new("test");
    // 5 as bit(8) = 00000101
    let r = bitfromint4(root.mcx(), 5, 8).unwrap();
    assert_eq!(r.bit_len, 8);
    assert_eq!(&r.data[..], &[0b00000101]);
    assert_eq!(bittoint4(r.as_ref()).unwrap(), 5);

    // round trip a bigger value through int8
    let r = bitfromint8(root.mcx(), 0x1234, 16).unwrap();
    assert_eq!(bittoint8(r.as_ref()).unwrap(), 0x1234);

    // negative sign-fill: -1 as bit(8) = 11111111
    let r = bitfromint4(root.mcx(), -1, 8).unwrap();
    assert_eq!(&r.data[..], &[0xFF]);

    // too long errors
    let big = vb(root.mcx(), 40, &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
    assert!(bittoint4(big.as_ref()).is_err());
}

#[test]
fn getset_bit() {
    let root = mcx::MemoryContext::new("test");
    let a = vb(root.mcx(), 8, &[0b10000001]);
    assert_eq!(bitgetbit(a.as_ref(), 0).unwrap(), 1);
    assert_eq!(bitgetbit(a.as_ref(), 1).unwrap(), 0);
    assert_eq!(bitgetbit(a.as_ref(), 7).unwrap(), 1);
    assert!(bitgetbit(a.as_ref(), 8).is_err());

    let s = bitsetbit(root.mcx(), a.as_ref(), 1, 1).unwrap();
    assert_eq!(&s.data[..], &[0b11000001]);
    let s = bitsetbit(root.mcx(), a.as_ref(), 0, 0).unwrap();
    assert_eq!(&s.data[..], &[0b00000001]);
    assert!(bitsetbit(root.mcx(), a.as_ref(), 0, 2).is_err());
}

#[test]
fn position() {
    let root = mcx::MemoryContext::new("test");
    // str = 11010 (5), substr = 101 -> at position 2
    let str = vb(root.mcx(), 5, &[0b11010000]);
    let sub = vb(root.mcx(), 3, &[0b10100000]);
    assert_eq!(bitposition(str.as_ref(), sub.as_ref()), 2);

    // empty substr returns 1
    let empty = vb(root.mcx(), 0, &[]);
    assert_eq!(bitposition(str.as_ref(), empty.as_ref()), 1);

    // not found
    let nope = vb(root.mcx(), 3, &[0b11100000]);
    assert_eq!(bitposition(str.as_ref(), nope.as_ref()), 0);
}

#[test]
fn casts_bit_varbit() {
    let root = mcx::MemoryContext::new("test");
    let a = vb(root.mcx(), 8, &[0xFF]);
    // explicit bit(4) truncates
    let r = bit(root.mcx(), a.as_ref(), 4, true).unwrap();
    assert_eq!(r.bit_len, 4);
    assert_eq!(&r.data[..], &[0xF0]);
    // implicit mismatch errors
    assert!(bit(root.mcx(), a.as_ref(), 4, false).is_err());

    // varbit explicit truncate
    let r = varbit(root.mcx(), a.as_ref(), 4, true).unwrap();
    assert_eq!(r.bit_len, 4);
    assert_eq!(&r.data[..], &[0xF0]);
    assert!(varbit(root.mcx(), a.as_ref(), 4, false).is_err());
}

#[test]
fn typmod_inout() {
    let root = mcx::MemoryContext::new("test");
    let s = bittypmodout(root.mcx(), 5).unwrap();
    assert_eq!(&s[..s.len() - 1], b"(5)");
    let s = bittypmodout(root.mcx(), -1).unwrap();
    assert_eq!(&s[..s.len() - 1], b"");

    assert!(anybit_typmodin(&[0], "bit").is_err());
    assert!(anybit_typmodin(&[1, 2], "bit").is_err());
    assert_eq!(anybit_typmodin(&[5], "bit").unwrap(), 5);
}

#[test]
fn overlay() {
    let root = mcx::MemoryContext::new("test");
    // t1 = 11111 (5 bits), overlay 00 at position 2 length 2 -> 1 00 11 = 10011
    let t1 = vb(root.mcx(), 5, &[0b11111000]);
    let t2 = vb(root.mcx(), 2, &[0b00000000]);
    let r = bitoverlay(root.mcx(), t1.as_ref(), t2.as_ref(), 2, 2).unwrap();
    assert_eq!(r.bit_len, 5);
    assert_eq!(&r.data[..], &[0b10011000]);
}
