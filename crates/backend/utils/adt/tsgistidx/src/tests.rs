use ::adt_tsvector_core::query::TsQueryRef;
use ::mcx::{Mcx, MemoryContext};

use crate::*;

fn arr_image<'m>(mcx: Mcx<'m>, crcs: &[i32]) -> GtsRef<'m> {
    let size = 8 + crcs.len() * 4;
    let mut img: ::mcx::PgVec<'m, u8> = mcx::vec_with_capacity_in(mcx, size).unwrap();
    mcx::vec_append_bytes(&mut img, &::types_tuple::varatt::set_varsize_4b_word(size as u32).to_ne_bytes()).unwrap();
    mcx::vec_append_bytes(&mut img, &ARRKEY.to_ne_bytes()).unwrap();
    for c in crcs {
        mcx::vec_append_bytes(&mut img, &c.to_ne_bytes()).unwrap();
    }
    GtsRef { image: img.leak() }
}

fn sign_image<'m>(mcx: Mcx<'m>, crcs: &[i32], siglen: usize) -> GtsRef<'m> {
    let size = 8 + siglen;
    let mut img: ::mcx::PgVec<'m, u8> = mcx::vec_with_capacity_in(mcx, size).unwrap();
    mcx::vec_append_bytes(&mut img, &::types_tuple::varatt::set_varsize_4b_word(size as u32).to_ne_bytes()).unwrap();
    mcx::vec_append_bytes(&mut img, &SIGNKEY.to_ne_bytes()).unwrap();
    img.resize(size, 0);
    for &c in crcs {
        let i = (c as u32 as usize) % (siglen * 8);
        img[8 + i / 8] |= 1 << (i % 8);
    }
    GtsRef { image: img.leak() }
}

fn tsq<'m>(mcx: Mcx<'m>, s: &str) -> ::mcx::PgVec<'m, u8> {
    ::adt_tsquery_core::io::tsquery_in_core(mcx, s.as_bytes(), None)
        .expect("tsquery parse")
        .expect("no soft error")
}

fn crc(s: &[u8]) -> i32 {
    ::crc32c::legacy_crc32_lexeme(s) as i32
}

#[test]
fn consistent_arr_key() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut crcs = [crc(b"foo"), crc(b"bar")];
    crcs.sort_unstable();
    let key = arr_image(mcx, &crcs);

    let q = tsq(mcx, "foo & bar");
    let qr = TsQueryRef { payload: &q[4..] };
    assert!(gtsvector_consistent_core(mcx, key, qr).unwrap());

    let q = tsq(mcx, "foo & baz");
    let qr = TsQueryRef { payload: &q[4..] };
    assert!(!gtsvector_consistent_core(mcx, key, qr).unwrap());

    // prefix is always a maybe on hashes
    let q = tsq(mcx, "zzz:*");
    let qr = TsQueryRef { payload: &q[4..] };
    assert!(gtsvector_consistent_core(mcx, key, qr).unwrap());
}

#[test]
fn consistent_sign_key() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let key = sign_image(mcx, &[crc(b"foo"), crc(b"bar")], SIGLEN_DEFAULT);

    let q = tsq(mcx, "foo");
    let qr = TsQueryRef { payload: &q[4..] };
    assert!(gtsvector_consistent_core(mcx, key, qr).unwrap());

    let q = tsq(mcx, "foo & !bar");
    let qr = TsQueryRef { payload: &q[4..] };
    // NOT over a maybe stays maybe: signature lanes are inexact
    assert!(gtsvector_consistent_core(mcx, key, qr).unwrap());
}

#[test]
fn key_flags_and_sizes() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let a = arr_image(mcx, &[1, 2, 3]);
    assert!(a.is_arrkey() && !a.is_signkey() && !a.is_alltrue());
    assert_eq!(a.arrnelem(), 3);
    assert_eq!(a.arr_at(2), 3);

    let s = sign_image(mcx, &[7], SIGLEN_DEFAULT);
    assert!(s.is_signkey() && !s.is_alltrue());
    assert_eq!(s.siglen(), SIGLEN_DEFAULT);
    assert_eq!(sizebitvec(s.sign(), SIGLEN_DEFAULT), 1);
}

#[test]
fn hemdist_alltrue() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let size = 8usize;
    let mut img: ::mcx::PgVec<'_, u8> = mcx::vec_with_capacity_in(mcx, size).unwrap();
    mcx::vec_append_bytes(&mut img, &::types_tuple::varatt::set_varsize_4b_word(size as u32).to_ne_bytes()).unwrap();
    mcx::vec_append_bytes(&mut img, &(SIGNKEY | ALLISTRUE).to_ne_bytes()).unwrap();
    let alltrue = GtsRef { image: img.leak() };
    assert!(alltrue.is_alltrue());

    let empty = sign_image(mcx, &[], SIGLEN_DEFAULT);
    assert_eq!(hemdist(alltrue, alltrue), 0);
    assert_eq!(hemdist(alltrue, empty), (SIGLEN_DEFAULT * 8) as i32);
    assert_eq!(hemdist(empty, alltrue), (SIGLEN_DEFAULT * 8) as i32);
}
