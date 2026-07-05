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

#[test]
fn gtsquery_consistent_contains_and_containedby() {
    use ::types_scan::scankey::{RTContainedByStrategyNumber, RTContainsStrategyNumber};

    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    // tsquery_in_core leaves the varsize word for the fmgr wrapper to stamp.
    let tsq_img = |s: &str| {
        let mut q = tsq(mcx, s);
        let w = ::types_tuple::varatt::set_varsize_4b_word(q.len() as u32).to_ne_bytes();
        q[0..4].copy_from_slice(&w);
        q
    };
    let key_q = tsq_img("foo & bar");
    let key = make_tsquery_sign(TsQueryRef { payload: &key_q[4..] });
    let sub_q = tsq_img("foo");
    let sub = make_tsquery_sign(TsQueryRef { payload: &sub_q[4..] });
    assert_eq!(key & sub, sub);
    assert_eq!(sub.count_ones(), 1);

    let run = |page_is_leaf: bool, strategy: u16, query: &[u8]| -> (bool, bool) {
        let entry = ::types_gist::GISTENTRY::init(
            ::datum::Datum::from_u64(key),
            0,
            false,
            page_is_leaf,
        );
        let mut recheck = false;
        let mut fci = ::types_fmgr::LocalFcinfo::<5>::new(0);
        // SAFETY: ctx outlives the call.
        unsafe { fci.set_result_mcx(mcx) };
        fci.set_arg(0, ::datum::Datum::from_usize(core::ptr::from_ref(&entry) as usize));
        fci.set_arg(1, ::datum::Datum::from_usize(query.as_ptr() as usize));
        fci.set_arg(2, ::datum::Datum::from_u32(strategy as u32));
        fci.set_arg(3, ::datum::Datum::from_u32(0));
        fci.set_arg(4, ::datum::Datum::from_usize(core::ptr::from_mut(&mut recheck) as usize));
        let d = fc_gtsquery_consistent(None, &mut fci).unwrap();
        (d.as_bool(), recheck)
    };

    // leaf: contains needs all of the query's bits; containedby needs all of the key's.
    assert_eq!(run(true, RTContainsStrategyNumber, &sub_q), (true, true));
    assert_eq!(run(true, RTContainedByStrategyNumber, &sub_q), (false, true));
    assert_eq!(run(true, RTContainedByStrategyNumber, &key_q), (true, true));
    // internal page: any overlap passes either strategy.
    assert_eq!(run(false, RTContainsStrategyNumber, &sub_q), (true, true));
    assert_eq!(run(false, RTContainedByStrategyNumber, &sub_q), (true, true));
    // unknown strategy is false.
    assert_eq!(run(true, 1, &sub_q), (false, true));

    let miss_q = tsq_img("zzzzqq");
    let miss = make_tsquery_sign(TsQueryRef { payload: &miss_q[4..] });
    if key & miss == 0 {
        assert_eq!(run(true, RTContainsStrategyNumber, &miss_q), (false, true));
        assert_eq!(run(false, RTContainsStrategyNumber, &miss_q), (false, true));
    }
}
