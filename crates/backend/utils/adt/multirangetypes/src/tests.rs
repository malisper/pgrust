use super::*;
use ::adt_rangetypes::{make_range, ElemInfo, RangeInfo};
use ::mcx::MemoryContext;
use ::types_error::PgResult;
use ::types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

fn fc_i32_cmp(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (fcinfo.arg(0).as_i32(), fcinfo.arg(1).as_i32());
    Ok(Datum::from_i32(a.cmp(&b) as i32))
}

const INT4RANGE: Oid = 3904;
const INT4MULTIRANGE: Oid = 4451;
const F_INT4RANGE_CANONICAL: Oid = 3914;

fn int4_rng() -> RangeInfo {
    RangeInfo {
        pin: None,
        rngtypid: INT4RANGE,
        collation: 0,
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

fn mk<'m>(mcx: ::mcx::Mcx<'m>, rng: &mut RangeInfo, lo: i32, hi: i32) -> PgVec<'m, u8> {
    let mut lower = ::adt_rangetypes::RangeBound {
        val: Datum::from_i32(lo),
        infinite: false,
        inclusive: true,
        lower: true,
    };
    let mut upper = ::adt_rangetypes::RangeBound {
        val: Datum::from_i32(hi),
        infinite: false,
        inclusive: false,
        lower: false,
    };
    make_range(mcx, rng, &mut lower, &mut upper, false, None).unwrap().unwrap()
}

#[test]
fn make_multirange_sorts_and_merges() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let mut rng = int4_rng();
    let r1 = mk(mcx, &mut rng, 2, 5);
    let r2 = mk(mcx, &mut rng, 1, 3);
    let r3 = mk(mcx, &mut rng, 7, 8);
    let r4 = mk(mcx, &mut rng, 5, 6); // adjacent to [1,5)
    let empty = ::adt_rangetypes::make_empty_range(mcx, &mut rng).unwrap();
    let mut ranges: PgVec<'_, &[u8]> = ::mcx::vec_with_capacity_in(mcx, 5).unwrap();
    for r in [&r1, &r2, &r3, &r4, &empty] {
        ranges.push(&r[..]);
    }
    let mr = make_multirange(mcx, INT4MULTIRANGE, &mut rng, &mut ranges).unwrap();
    assert_eq!(multirange_type_oid(&mr), INT4MULTIRANGE);
    assert_eq!(multirange_count(&mr), 2);
    let (lo, up) = multirange_get_bounds(&rng, &mr, 0);
    assert_eq!((lo.val.as_i32(), up.val.as_i32()), (1, 6));
    let (lo, up) = multirange_get_bounds(&rng, &mr, 1);
    assert_eq!((lo.val.as_i32(), up.val.as_i32()), (7, 8));

    // layout: hdr 12 + items 4 + flags 2 -> aligned 20; 4 bound values
    assert_eq!(mr.len(), 20 + 16);
    assert_eq!(multirange_flags(&mr, 0), ::adt_rangetypes::RANGE_LB_INC);

    // get_range reconstructs a self-contained image
    let rimg = multirange_get_range(mcx, &rng, &mr, 1).unwrap();
    let (lo, up, empty2) = ::adt_rangetypes::range_deserialize(&rng.elem, &rimg);
    assert!(!empty2);
    assert_eq!((lo.val.as_i32(), up.val.as_i32()), (7, 8));
    assert_eq!(::adt_rangetypes::range_type_oid(&rimg), INT4RANGE);
}

#[test]
fn empty_multirange_layout() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let mut rng = int4_rng();
    let mr = make_empty_multirange(mcx, INT4MULTIRANGE, &mut rng).unwrap();
    assert_eq!(mr.len(), 12);
    assert!(multirange_is_empty(&mr));
}

#[test]
fn contains_and_overlaps_bsearch() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let mut rng = int4_rng();
    let parts = [(1, 3), (5, 7), (10, 20)];
    let mut ranges: PgVec<'_, &[u8]> = ::mcx::vec_with_capacity_in(mcx, 3).unwrap();
    for &(a, b) in &parts {
        ranges.push(leak_image(mk(mcx, &mut rng, a, b)));
    }
    let mr = make_multirange(mcx, INT4MULTIRANGE, &mut rng, &mut ranges).unwrap();
    assert_eq!(multirange_count(&mr), 3);

    for (v, want) in [(0, false), (1, true), (3, false), (6, true), (15, true), (20, false)] {
        assert_eq!(
            multirange_contains_elem_internal(mcx, &mut rng, &mr, Datum::from_i32(v)).unwrap(),
            want,
            "elem {v}"
        );
    }

    let probe = mk(mcx, &mut rng, 11, 14);
    assert!(multirange_contains_range_internal(mcx, &mut rng, &mr, &probe).unwrap());
    let probe = mk(mcx, &mut rng, 6, 12);
    assert!(!multirange_contains_range_internal(mcx, &mut rng, &mr, &probe).unwrap());
    assert!(range_overlaps_multirange_internal(mcx, &mut rng, &probe, &mr).unwrap());
    let probe = mk(mcx, &mut rng, 8, 9);
    assert!(!range_overlaps_multirange_internal(mcx, &mut rng, &probe, &mr).unwrap());
}

#[test]
fn cmp_and_eq_and_setops() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let mut rng = int4_rng();
    let build = |rng: &mut RangeInfo, parts: &[(i32, i32)]| {
        let mut ranges: PgVec<'_, &[u8]> = ::mcx::vec_with_capacity_in(mcx, parts.len()).unwrap();
        for &(a, b) in parts {
            ranges.push(leak_image(mk(mcx, rng, a, b)));
        }
        make_multirange(mcx, INT4MULTIRANGE, rng, &mut ranges).unwrap()
    };
    let a = build(&mut rng, &[(1, 3), (5, 8)]);
    let b = build(&mut rng, &[(1, 3)]);
    let c = build(&mut rng, &[(2, 6), (7, 10)]);

    assert!(multirange_eq_internal(mcx, &mut rng, &a, &a).unwrap());
    assert!(!multirange_eq_internal(mcx, &mut rng, &a, &b).unwrap());
    // shorter with equal prefix sorts first
    assert_eq!(multirange_cmp_internal(mcx, &mut rng, &b, &a).unwrap(), -1);
    assert_eq!(multirange_cmp_internal(mcx, &mut rng, &a, &c).unwrap(), -1);

    // minus: {[1,3),[5,8)} - {[2,6),[7,10)} = {[1,2),[6,7)}
    let r1 = multirange_deserialize(mcx, &rng, &a).unwrap();
    let r2 = multirange_deserialize(mcx, &rng, &c).unwrap();
    let m = multirange_minus_internal(mcx, INT4MULTIRANGE, &mut rng, &r1, &r2).unwrap();
    assert_eq!(multirange_count(&m), 2);
    let (lo, up) = multirange_get_bounds(&rng, &m, 0);
    assert_eq!((lo.val.as_i32(), up.val.as_i32()), (1, 2));
    let (lo, up) = multirange_get_bounds(&rng, &m, 1);
    assert_eq!((lo.val.as_i32(), up.val.as_i32()), (6, 7));

    // intersect: {[2,3),[5,6),[7,8)}
    let m = multirange_intersect_internal(mcx, INT4MULTIRANGE, &mut rng, &r1, &r2).unwrap();
    assert_eq!(multirange_count(&m), 3);
    let (lo, up) = multirange_get_bounds(&rng, &m, 1);
    assert_eq!((lo.val.as_i32(), up.val.as_i32()), (5, 6));

    // union range across the whole multirange
    let u = multirange_get_union_range(mcx, &mut rng, &a).unwrap();
    let (lo, up, _e) = ::adt_rangetypes::range_deserialize(&rng.elem, &u);
    assert_eq!((lo.val.as_i32(), up.val.as_i32()), (1, 8));
}

#[test]
fn offsets_use_stride_items_past_four_ranges() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let mut rng = int4_rng();
    let parts: Vec<(i32, i32)> = (0..9).map(|i| (i * 10, i * 10 + 5)).collect();
    let mut ranges: PgVec<'_, &[u8]> = ::mcx::vec_with_capacity_in(mcx, parts.len()).unwrap();
    for &(a, b) in &parts {
        ranges.push(leak_image(mk(mcx, &mut rng, a, b)));
    }
    let mr = make_multirange(mcx, INT4MULTIRANGE, &mut rng, &mut ranges).unwrap();
    assert_eq!(multirange_count(&mr), 9);
    for (i, &(a, b)) in parts.iter().enumerate() {
        let (lo, up) = multirange_get_bounds(&rng, &mr, i);
        assert_eq!((lo.val.as_i32(), up.val.as_i32()), (a, b), "range {i}");
    }
}
