use super::*;
use ::adt_rangetypes::{range_deserialize, range_serialize, ElemInfo};
use ::mcx::MemoryContext;
use ::types_fmgr::LocalFcinfo;

fn fc_i32_cmp(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (fcinfo.arg(0).as_i32(), fcinfo.arg(1).as_i32());
    Ok(Datum::from_i32(a.cmp(&b) as i32))
}

fn fc_i32_subdiff(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (fcinfo.arg(0).as_i32(), fcinfo.arg(1).as_i32());
    Ok(Datum::from_f64(a as f64 - b as f64))
}

const INT4RANGE: Oid = 3904;
const F_INT4RANGE_CANONICAL: Oid = 3914;

fn int4_ri() -> RangeInfo {
    RangeInfo {
        pin: None,
        rngtypid: INT4RANGE,
        collation: InvalidOid,
        elem_typid: 23,
        elem: ElemInfo { typlen: 4, typbyval: true, typalign: b'i', typstorage: b'p' },
        cmp: FmgrInfo::new(fc_i32_cmp, 351, 2, true, false),
        canonical_oid: F_INT4RANGE_CANONICAL,
        elem_hash: None,
        elem_hash_extended: None,
        own_typlen: -1,
        own_typbyval: false,
        own_typalign: b'i',
    }
}

fn cache(subdiff: bool) -> RangeGistCache {
    RangeGistCache {
        ri: int4_ri(),
        subdiff: subdiff.then(|| FmgrInfo::new(fc_i32_subdiff, 3922, 2, true, false)),
    }
}

fn bound(val: i32, inclusive: bool, lower: bool) -> RangeBound {
    RangeBound { val: Datum::from_i32(val), infinite: false, inclusive, lower }
}

fn inf_bound(lower: bool) -> RangeBound {
    RangeBound { val: Datum::from_usize(0), infinite: true, inclusive: false, lower }
}

fn mk<'m>(mcx: Mcx<'m>, cache: &mut RangeGistCache, lo: RangeBound, up: RangeBound) -> &'m [u8] {
    let mut lo = lo;
    let mut up = up;
    leak_image(
        range_serialize(mcx, &mut cache.ri, &mut lo, &mut up, false, None).unwrap().unwrap(),
    )
}

fn mk_empty<'m>(mcx: Mcx<'m>, cache: &mut RangeGistCache) -> &'m [u8] {
    let mut lo = bound(0, true, true);
    let mut up = bound(0, false, false);
    leak_image(
        range_serialize(mcx, &mut cache.ri, &mut lo, &mut up, true, None).unwrap().unwrap(),
    )
}

#[test]
fn gist_range_class_bits() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let mut c = cache(false);
    assert_eq!(get_gist_range_class(mk(mcx, &mut c, bound(1, true, true), bound(5, false, false))), CLS_NORMAL);
    assert_eq!(get_gist_range_class(mk(mcx, &mut c, inf_bound(true), bound(5, false, false))), CLS_LOWER_INF);
    assert_eq!(get_gist_range_class(mk(mcx, &mut c, bound(1, true, true), inf_bound(false))), CLS_UPPER_INF);
    assert_eq!(get_gist_range_class(mk(mcx, &mut c, inf_bound(true), inf_bound(false))), CLS_LOWER_INF | CLS_UPPER_INF);
    assert_eq!(get_gist_range_class(mk_empty(mcx, &mut c)), CLS_EMPTY);
    let ce = set_contain_empty_copy(mcx, mk(mcx, &mut c, bound(1, true, true), bound(5, false, false))).unwrap();
    assert_eq!(get_gist_range_class(ce), CLS_CONTAIN_EMPTY);
}

#[test]
fn super_union_identity_and_absorb() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let mut c = cache(false);
    let a = mk(mcx, &mut c, bound(1, true, true), bound(10, false, false));
    let b = mk(mcx, &mut c, bound(3, true, true), bound(5, false, false));
    // a contains b: identity return of a.
    let u = range_super_union(mcx, &mut c, a, b).unwrap();
    assert_eq!(u.as_ptr(), a.as_ptr());
    // disjoint: gap absorbed.
    let d = mk(mcx, &mut c, bound(20, true, true), bound(30, false, false));
    let u2 = range_super_union(mcx, &mut c, a, d).unwrap();
    let (lo, up, empty) = range_deserialize(&c.ri.elem, u2);
    assert!(!empty);
    assert_eq!(lo.val.as_i32(), 1);
    assert_eq!(up.val.as_i32(), 30);
    // empty operand: marks CONTAIN_EMPTY on a copy.
    let e = mk_empty(mcx, &mut c);
    let u3 = range_super_union(mcx, &mut c, e, a).unwrap();
    assert_ne!(u3.as_ptr(), a.as_ptr());
    assert_eq!(range_get_flags(u3) & RANGE_CONTAIN_EMPTY, RANGE_CONTAIN_EMPTY);
    // already-contain-empty operand returns as-is.
    let u4 = range_super_union(mcx, &mut c, e, u3).unwrap();
    assert_eq!(u4.as_ptr(), u3.as_ptr());
}

fn call_penalty(mcx: Mcx<'_>, c: RangeGistCache, orig: &[u8], new: &[u8]) -> f32 {
    let mut fi = FmgrInfo::new(fc_range_gist_penalty, 3879, 3, true, false);
    fi.set_fn_extra(c);
    let e1 = GISTENTRY::init(Datum::from_usize(orig.as_ptr() as usize), 0, false, false);
    let e2 = GISTENTRY::init(Datum::from_usize(new.as_ptr() as usize), 0, false, false);
    let mut penalty: f32 = -1.0;
    let mut frame = LocalFcinfo::<3>::fresh(InvalidOid);
    // SAFETY: mcx outlives the call.
    unsafe { frame.set_result_mcx(mcx) };
    frame.set_arg(0, Datum::from_usize(&e1 as *const GISTENTRY as usize));
    frame.set_arg(1, Datum::from_usize(&e2 as *const GISTENTRY as usize));
    frame.set_arg(2, Datum::from_usize(&mut penalty as *mut f32 as usize));
    fi.invoke(&mut frame).unwrap();
    penalty
}

#[test]
fn penalty_classes_match_c() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let mut c = cache(true);
    let normal = mk(mcx, &mut c, bound(10, true, true), bound(20, false, false));
    let wider = mk(mcx, &mut c, bound(5, true, true), bound(25, false, false));
    let empty = mk_empty(mcx, &mut c);
    let both_inf = mk(mcx, &mut c, inf_bound(true), inf_bound(false));
    let lower_inf = mk(mcx, &mut c, inf_bound(true), bound(20, false, false));

    // empty into empty / contains-empty / (-inf,+inf) / half-inf / normal.
    assert_eq!(call_penalty(mcx, cache(true), empty, empty), 0.0);
    assert_eq!(call_penalty(mcx, cache(true), both_inf, empty), 2.0);
    assert_eq!(call_penalty(mcx, cache(true), lower_inf, empty), 3.0);
    assert_eq!(call_penalty(mcx, cache(true), normal, empty), 4.0);
    // (-inf,+inf) into (-inf,+inf) / half-inf / normal.
    assert_eq!(call_penalty(mcx, cache(true), both_inf, both_inf), 0.0);
    assert_eq!(call_penalty(mcx, cache(true), lower_inf, both_inf), 2.0);
    assert_eq!(call_penalty(mcx, cache(true), normal, both_inf), 4.0);
    // normal into normal: subtype_diff extension (5..25 into 10..20 = 5+5).
    assert_eq!(call_penalty(mcx, cache(true), normal, wider), 10.0);
    // normal into contained: no extension.
    assert_eq!(call_penalty(mcx, cache(true), wider, normal), 0.0);
    // normal into empty original: infinity.
    assert_eq!(call_penalty(mcx, cache(true), empty, normal), f32::INFINITY);
    // (-inf,x) into normal: infinity.
    assert_eq!(call_penalty(mcx, cache(true), normal, lower_inf), f32::INFINITY);
    // (-inf,25) into (-inf,20): upper extension via subdiff = 5.
    let lower_inf25 = mk(mcx, &mut c, inf_bound(true), bound(25, false, false));
    assert_eq!(call_penalty(mcx, cache(true), lower_inf, lower_inf25), 5.0);
}

#[test]
fn picksplit_assigns_every_offset_once() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let mut c = cache(true);
    // 1-based picksplit vector: overlapping normal ranges.
    let mut ranges: Vec<&[u8]> = vec![&[]];
    for i in 0..8 {
        ranges.push(mk(mcx, &mut c, bound(i * 2, true, true), bound(i * 2 + 3, false, false)));
    }
    let mut v = GistSplitVec {
        spl_left: Vec::new(),
        spl_ldatum: Datum::from_usize(0),
        spl_ldatum_exists: false,
        spl_right: Vec::new(),
        spl_rdatum: Datum::from_usize(0),
        spl_rdatum_exists: false,
    };
    double_sorting_split(mcx, &mut c, &ranges, &mut v).unwrap();
    let mut seen: Vec<u16> = v.spl_left.iter().chain(v.spl_right.iter()).copied().collect();
    seen.sort_unstable();
    assert_eq!(seen, (1..=8).collect::<Vec<u16>>());
    assert!(!v.spl_left.is_empty() && !v.spl_right.is_empty());
    // union predicates cover their side.
    let (llo, lup, _) = range_deserialize(&c.ri.elem, unsafe {
        core::slice::from_raw_parts(
            v.spl_ldatum.as_usize() as *const u8,
            ::adt_rangetypes::varsize_4b(v.spl_ldatum.as_usize() as *const u8),
        )
    });
    assert!(llo.val.as_i32() <= lup.val.as_i32());
}

#[test]
fn class_split_mixed_classes() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let mut c = cache(false);
    let normal = mk(mcx, &mut c, bound(1, true, true), bound(5, false, false));
    let einf = mk(mcx, &mut c, inf_bound(true), bound(5, false, false));
    let ranges: Vec<&[u8]> = vec![&[], normal, einf, normal, einf];
    let mut v = GistSplitVec {
        spl_left: Vec::new(),
        spl_ldatum: Datum::from_usize(0),
        spl_ldatum_exists: false,
        spl_right: Vec::new(),
        spl_rdatum: Datum::from_usize(0),
        spl_rdatum_exists: false,
    };
    // CLS_NORMAL goes right, CLS_LOWER_INF stays left.
    let mut groups = [SplitLR::Left; CLS_COUNT];
    groups[CLS_NORMAL] = SplitLR::Right;
    class_split(mcx, &mut c, &ranges, &mut v, &groups).unwrap();
    assert_eq!(v.spl_right, vec![1, 3]);
    assert_eq!(v.spl_left, vec![2, 4]);
}

#[test]
fn leaf_consistent_strategies() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let mut c = cache(false);
    let key = mk(mcx, &mut c, bound(1, true, true), bound(10, false, false));
    let q = mk(mcx, &mut c, bound(10, true, true), bound(20, false, false));
    assert!(consistent_leaf_range(mcx, &mut c.ri, RANGESTRAT_BEFORE, key, q).unwrap());
    assert!(consistent_leaf_range(mcx, &mut c.ri, RANGESTRAT_ADJACENT, key, q).unwrap());
    assert!(!consistent_leaf_range(mcx, &mut c.ri, RANGESTRAT_OVERLAPS, key, q).unwrap());
    assert!(consistent_leaf_element(mcx, &mut c.ri, RANGESTRAT_CONTAINS_ELEM, key, Datum::from_i32(5)).unwrap());
    assert!(!consistent_leaf_element(mcx, &mut c.ri, RANGESTRAT_CONTAINS_ELEM, key, Datum::from_i32(10)).unwrap());
    // int page: contained_by descends when key contains empties.
    let ce = set_contain_empty_copy(mcx, key).unwrap();
    assert!(consistent_int_range(mcx, &mut c.ri, RANGESTRAT_CONTAINED_BY, ce, q).unwrap());
    let err = consistent_leaf_range(mcx, &mut c.ri, 99, key, q).unwrap_err();
    assert!(err.message().contains("unrecognized range strategy"));
}
