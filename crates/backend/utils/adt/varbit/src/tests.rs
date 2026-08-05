use ::mcx::MemoryContext;
use ::stringinfo::StringInfo;

use crate::*;

fn img<'m>(mcx: Mcx<'m>, s: &str) -> PgVec<'m, u8> {
    bit_in_cstr(mcx, s.as_bytes()).unwrap()
}

fn s(mcx: Mcx<'_>, payload: &[u8]) -> alloc::string::String {
    let out = bits_out(mcx, payload).unwrap();
    core::str::from_utf8(&out[..out.len() - 1]).unwrap().into()
}

#[test]
fn logic_ops() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let a = img(mcx, "b1101");
    let b = img(mcx, "b1011");
    let and = bit_logic(mcx, &a[4..], &b[4..], |x, y| x & y, "AND").unwrap();
    assert_eq!(s(mcx, &and[4..]), "1001");
    let or = bit_logic(mcx, &a[4..], &b[4..], |x, y| x | y, "OR").unwrap();
    assert_eq!(s(mcx, &or[4..]), "1111");
    let xor = bit_logic(mcx, &a[4..], &b[4..], |x, y| x ^ y, "XOR").unwrap();
    assert_eq!(s(mcx, &xor[4..]), "0110");
    let c = img(mcx, "b11");
    let e = bit_logic(mcx, &a[4..], &c[4..], |x, y| x & y, "AND").unwrap_err();
    assert!(format!("{e:?}").contains("cannot AND bit strings of different sizes"));
}

#[test]
fn not_zero_pads() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let a = img(mcx, "b1101");
    let n = bitnot_core(mcx, &a[4..]).unwrap();
    assert_eq!(s(mcx, &n[4..]), "0010");
    assert_eq!(payload_bits(&n[4..]), &[0b0010_0000]);
}

#[test]
fn shifts() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let a = img(mcx, "b11011");
    let l2 = bitshiftleft_core(mcx, &a[4..], 2).unwrap();
    assert_eq!(s(mcx, &l2[4..]), "01100");
    let r2 = bitshiftright_core(mcx, &a[4..], 2).unwrap();
    assert_eq!(s(mcx, &r2[4..]), "00110");
    let ln2 = bitshiftleft_core(mcx, &a[4..], -2).unwrap();
    assert_eq!(s(mcx, &ln2[4..]), "00110");
    let all = bitshiftleft_core(mcx, &a[4..], 5).unwrap();
    assert_eq!(s(mcx, &all[4..]), "00000");
    let big = img(mcx, "b111000111000111");
    let r9 = bitshiftright_core(mcx, &big[4..], 9).unwrap();
    assert_eq!(s(mcx, &r9[4..]), "000000000111000");
    assert_eq!(payload_bits(&r9[4..])[1] & 1, 0);
    let l9 = bitshiftleft_core(mcx, &big[4..], 9).unwrap();
    assert_eq!(s(mcx, &l9[4..]), "000111000000000");
}

#[test]
fn catenate() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let a = img(mcx, "b101");
    let b = img(mcx, "b11011");
    let c = bit_catenate(mcx, &a[4..], &b[4..]).unwrap();
    assert_eq!(s(mcx, &c[4..]), "10111011");
    let d = bit_catenate(mcx, &c[4..], &a[4..]).unwrap();
    assert_eq!(s(mcx, &d[4..]), "10111011101");
    let e = img(mcx, "b");
    let f = bit_catenate(mcx, &a[4..], &e[4..]).unwrap();
    assert_eq!(s(mcx, &f[4..]), "101");
}

#[test]
fn substring_cases() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let a = img(mcx, "b110010111");
    let r = bitsubstring(mcx, &a[4..], 3, 4, false).unwrap();
    assert_eq!(s(mcx, &r[4..]), "0010");
    let r = bitsubstring(mcx, &a[4..], 5, -1, true).unwrap();
    assert_eq!(s(mcx, &r[4..]), "10111");
    let r = bitsubstring(mcx, &a[4..], -2, 5, false).unwrap();
    assert_eq!(s(mcx, &r[4..]), "11");
    let r = bitsubstring(mcx, &a[4..], 100, 4, false).unwrap();
    assert_eq!(s(mcx, &r[4..]), "");
    let r = bitsubstring(mcx, &a[4..], 3, i32::MAX, false).unwrap();
    assert_eq!(s(mcx, &r[4..]), "0010111");
    let e = bitsubstring(mcx, &a[4..], 3, -1, false).unwrap_err();
    assert!(format!("{e:?}").contains("negative substring length not allowed"));
}

#[test]
fn overlay_cases() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let t1 = img(mcx, "b0000");
    let t2 = img(mcx, "b11");
    let r = bit_overlay(mcx, &t1[4..], &t2[4..], 2, 2).unwrap();
    assert_eq!(s(mcx, &r[4..]), "0110");
    let r = bit_overlay(mcx, &t1[4..], &t2[4..], 2, t2_len(&t2)).unwrap();
    assert_eq!(s(mcx, &r[4..]), "0110");
    let e = bit_overlay(mcx, &t1[4..], &t2[4..], 0, 2).unwrap_err();
    assert!(format!("{e:?}").contains("negative substring length"));
    let e = bit_overlay(mcx, &t1[4..], &t2[4..], 2, i32::MAX).unwrap_err();
    assert!(format!("{e:?}").contains("integer out of range"));
}

fn t2_len(t2: &[u8]) -> i32 {
    payload_bitlen(&t2[4..]) as i32
}

#[test]
fn int_casts() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let a = img(mcx, "b1101");
    assert_eq!(bittoint4_core(&a[4..]).unwrap(), 13);
    assert_eq!(bittoint8_core(&a[4..]).unwrap(), 13);
    let full = img(mcx, "x80000000");
    assert_eq!(bittoint4_core(&full[4..]).unwrap(), i32::MIN);
    let long = img(mcx, "x800000000");
    assert!(bittoint4_core(&long[4..]).is_err());
    assert_eq!(bittoint8_core(&long[4..]).unwrap(), 0x8_0000_0000);
    let rt = bitfromint4_core(mcx, -7, 11).unwrap();
    assert_eq!(s(mcx, &rt[4..]), "11111111001");
    let rt8 = bitfromint8_core(mcx, -7, 11).unwrap();
    assert_eq!(s(mcx, &rt8[4..]), "11111111001");
}

#[test]
fn coercions() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let a = img(mcx, "b101");
    let r = bit_coerce(mcx, &a[4..], 5, true).unwrap().unwrap();
    assert_eq!(s(mcx, &r[4..]), "10100");
    assert!(bit_coerce(mcx, &a[4..], 3, true).unwrap().is_none());
    assert!(bit_coerce(mcx, &a[4..], -1, true).unwrap().is_none());
    assert!(bit_coerce(mcx, &a[4..], 5, false).is_err());
    let v = img(mcx, "b10111");
    let r = varbit_coerce(mcx, &v[4..], 3, true).unwrap().unwrap();
    assert_eq!(s(mcx, &r[4..]), "101");
    assert!(varbit_coerce(mcx, &v[4..], 5, true).unwrap().is_none());
    assert!(varbit_coerce(mcx, &v[4..], 3, false).is_err());
    let r = bit_coerce(mcx, &v[4..], 2, true).unwrap().unwrap();
    assert_eq!(s(mcx, &r[4..]), "10");
}

#[test]
fn setbit_getbit() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let a = img(mcx, "b0000");
    let r = bitsetbit_core(mcx, &a[4..], 2, 1).unwrap();
    assert_eq!(s(mcx, &r[4..]), "0010");
    let r = bitsetbit_core(mcx, &r[4..], 2, 0).unwrap();
    assert_eq!(s(mcx, &r[4..]), "0000");
    assert!(bitsetbit_core(mcx, &a[4..], 4, 1).is_err());
    assert!(bitsetbit_core(mcx, &a[4..], -1, 1).is_err());
    assert!(bitsetbit_core(mcx, &a[4..], 2, 3).is_err());
    let b = img(mcx, "b0100");
    assert_eq!(bitgetbit_core(&b[4..], 1).unwrap(), 1);
    assert_eq!(bitgetbit_core(&b[4..], 0).unwrap(), 0);
    assert!(bitgetbit_core(&b[4..], 9).is_err());
}

#[test]
fn position() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let hay = img(mcx, "b110010");
    let needle = img(mcx, "b0010");
    assert_eq!(bitposition_core(&hay[4..], &needle[4..]), 3);
    let missing = img(mcx, "b1111");
    assert_eq!(bitposition_core(&hay[4..], &missing[4..]), 0);
    let empty = img(mcx, "b");
    assert_eq!(bitposition_core(&hay[4..], &empty[4..]), 1);
    assert_eq!(bitposition_core(&empty[4..], &needle[4..]), 0);
    let big = img(mcx, "b111000111000110001");
    let sub = img(mcx, "b0001");
    assert_eq!(bitposition_core(&big[4..], &sub[4..]), 4);
    let sub2 = img(mcx, "b110001");
    assert_eq!(bitposition_core(&big[4..], &sub2[4..]), 2);
    let sub3 = img(mcx, "b011000");
    assert_eq!(bitposition_core(&big[4..], &sub3[4..]), 12);
}

#[test]
fn counts() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let a = img(mcx, "b110110001");
    let n: i64 = payload_bits(&a[4..]).iter().map(|&b| b.count_ones() as i64).sum();
    assert_eq!(n, 5);
    assert_eq!(payload_bitlen(&a[4..]), 9);
    assert_eq!(payload_bits(&a[4..]).len(), 2);
}

#[test]
fn recv_roundtrip() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut buf = StringInfo::new_in(mcx).unwrap();
    buf.append_bytes(&5i32.to_be_bytes()).unwrap();
    buf.append_bytes(&[0b1011_1000]).unwrap();
    let r = bits_recv(mcx, &mut buf, -1, false).unwrap();
    assert_eq!(s(mcx, &r[4..]), "10111");

    let mut buf = StringInfo::new_in(mcx).unwrap();
    buf.append_bytes(&5i32.to_be_bytes()).unwrap();
    buf.append_bytes(&[0b1011_1111]).unwrap();
    let r = bits_recv(mcx, &mut buf, -1, true).unwrap();
    assert_eq!(payload_bits(&r[4..]), &[0b1011_1000]);

    let mut buf = StringInfo::new_in(mcx).unwrap();
    buf.append_bytes(&(-1i32).to_be_bytes()).unwrap();
    let e = bits_recv(mcx, &mut buf, -1, true).unwrap_err();
    assert!(format!("{e:?}").contains("invalid length in external bit string"));

    let mut buf = StringInfo::new_in(mcx).unwrap();
    buf.append_bytes(&5i32.to_be_bytes()).unwrap();
    buf.append_bytes(&[0b1011_1000]).unwrap();
    let e = bits_recv(mcx, &mut buf, 4, true).unwrap_err();
    assert!(format!("{e:?}").contains("does not match type bit(4)"));

    let mut buf = StringInfo::new_in(mcx).unwrap();
    buf.append_bytes(&5i32.to_be_bytes()).unwrap();
    buf.append_bytes(&[0b1011_1000]).unwrap();
    let e = bits_recv(mcx, &mut buf, 4, false).unwrap_err();
    assert!(format!("{e:?}").contains("too long for type bit varying(4)"));
}

// fnconf campaign-2, OID 1685 bit(varbit,int4,bool): the base-binary replay
// abort was attributed to "a 752 MB bit-string length allocation" (harness
// int4 draw 752915532 at call 73). Static audit + live byte-compare vs C 18.3
// found no bounds gap: C varbit.c bit() returns the arg unchanged when
// (len <= 0 || len > VARBITMAXLEN || len == VARBITLEN(arg)), bit_coerce
// carries the same guard, and the huge text-out class fails C-identically at
// the MaxAllocSize gate (both engines: "invalid memory alloc request size
// 2109372046" for draw 2109372045; draw 752915532 succeeds on both). These
// pins freeze the adversarial-length behavior deterministically.
#[test]
fn bit_coerce_c_bounds_guards() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let a = img(mcx, "b101");
    // len <= 0: source returned unchanged (no error, no allocation).
    assert!(bit_coerce(mcx, &a[4..], 0, true).unwrap().is_none());
    assert!(bit_coerce(mcx, &a[4..], -1, true).unwrap().is_none());
    assert!(bit_coerce(mcx, &a[4..], i32::MIN, true).unwrap().is_none());
    // len > VARBITMAXLEN (INT_MAX-7): unchanged — i32::MAX must NOT allocate.
    assert!(bit_coerce(mcx, &a[4..], i32::MAX, true).unwrap().is_none());
    assert!(bit_coerce(mcx, &a[4..], (VARBITMAXLEN + 1) as i32, true).unwrap().is_none());
    // len == VARBITLEN(arg): unchanged.
    assert!(bit_coerce(mcx, &a[4..], 3, true).unwrap().is_none());
    // in-range explicit cast still widens with zero padding.
    let some = bit_coerce(mcx, &a[4..], 11, true).unwrap().unwrap();
    assert_eq!(s(mcx, &some[4..]), "10100000000");
    // implicit cast at a mismatched length errors like C.
    let e = bit_coerce(mcx, &a[4..], 11, false).unwrap_err();
    assert!(format!("{e:?}").contains("bit string length 3 does not match type bit(11)"));
}

#[test]
fn bits_out_huge_length_clean_error_like_c() {
    // Payload header claiming the >MaxAllocSize harness class (draw
    // 2109372045): text-out must fail at the alloc gate BEFORE touching the
    // (absent) body — C 18.3 bit_out pallocs len+1 and errors
    // "invalid memory alloc request size 2109372046" (verified live on both
    // engines, byte-identical). Clean PgError; no panic, no abort.
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut payload = alloc::vec::Vec::new();
    payload.extend_from_slice(&2109372045i32.to_ne_bytes());
    payload.extend_from_slice(&[0u8; 4]);
    let e = bits_out(mcx, &payload).unwrap_err();
    assert!(format!("{e:?}").contains("invalid memory alloc request size 2109372046"));
}
