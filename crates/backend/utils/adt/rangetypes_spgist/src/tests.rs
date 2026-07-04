use super::*;
use ::adt_rangetypes::ElemInfo;
use ::mcx::MemoryContext;
use ::types_core::InvalidOid;

fn fc_i32_cmp(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (fcinfo.arg(0).as_i32(), fcinfo.arg(1).as_i32());
    Ok(Datum::from_i32(a.cmp(&b) as i32))
}

fn int4_ri() -> RangeInfo {
    RangeInfo {
        pin: None,
        rngtypid: 3904,
        collation: InvalidOid,
        elem_typid: 23,
        elem: ElemInfo { typlen: 4, typbyval: true, typalign: b'i', typstorage: b'p' },
        cmp: FmgrInfo::new(fc_i32_cmp, 351, 2, true, false),
        canonical_oid: 3914,
        elem_hash: None,
        elem_hash_extended: None,
        own_typlen: -1,
        own_typbyval: false,
        own_typalign: b'i',
    }
}

fn bound(val: i32, inclusive: bool, lower: bool) -> RangeBound {
    RangeBound { val: Datum::from_i32(val), infinite: false, inclusive, lower }
}

fn mk<'m>(mcx: Mcx<'m>, ri: &mut RangeInfo, lo: i32, hi: i32) -> &'m [u8] {
    let mut l = bound(lo, true, true);
    let mut u = bound(hi, false, false);
    ::adt_multirangetypes::leak_image(
        range_serialize(mcx, ri, &mut l, &mut u, false, None).unwrap().unwrap(),
    )
}

fn mk_empty<'m>(mcx: Mcx<'m>, ri: &mut RangeInfo) -> &'m [u8] {
    let mut l = bound(0, true, true);
    let mut u = bound(0, false, false);
    ::adt_multirangetypes::leak_image(
        range_serialize(mcx, ri, &mut l, &mut u, true, None).unwrap().unwrap(),
    )
}

#[test]
fn quadrants_match_c_convention() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let mut ri = int4_ri();
    let centroid = mk(mcx, &mut ri, 10, 20);
    // (lower vs c.lower, upper vs c.upper): (>=,>=) 1, (>=,<) 2, (<,<) 3,
    // (<,>=) 4, empty 5.
    let cases = [
        (mk(mcx, &mut ri, 10, 20), 1),
        (mk(mcx, &mut ri, 15, 25), 1),
        (mk(mcx, &mut ri, 12, 15), 2),
        (mk(mcx, &mut ri, 5, 15), 3),
        (mk(mcx, &mut ri, 5, 25), 4),
        (mk_empty(mcx, &mut ri), 5),
    ];
    for (tst, want) in cases {
        assert_eq!(get_quadrant(mcx, &mut ri, centroid, tst).unwrap(), want);
    }
}

#[test]
fn adjacent_cmp_bounds_table() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let mut ri = int4_ri();
    // C comment table: argument [..., 500) vs centroid lower bounds.
    let arg = bound(500, false, false);
    assert_eq!(adjacent_cmp_bounds(mcx, &mut ri, &arg, &bound(498, true, true)).unwrap(), 1);
    assert_eq!(adjacent_cmp_bounds(mcx, &mut ri, &arg, &bound(499, true, true)).unwrap(), 1);
    assert_eq!(adjacent_cmp_bounds(mcx, &mut ri, &arg, &bound(500, true, true)).unwrap(), 1);
    assert_eq!(adjacent_cmp_bounds(mcx, &mut ri, &arg, &bound(501, true, true)).unwrap(), -1);
    // argument [500, ...) vs centroid upper bounds.
    let arg = bound(500, true, true);
    assert_eq!(adjacent_cmp_bounds(mcx, &mut ri, &arg, &bound(499, false, false)).unwrap(), 1);
    assert_eq!(adjacent_cmp_bounds(mcx, &mut ri, &arg, &bound(500, false, false)).unwrap(), 1);
    assert_eq!(adjacent_cmp_bounds(mcx, &mut ri, &arg, &bound(501, false, false)).unwrap(), -1);
    assert_eq!(adjacent_cmp_bounds(mcx, &mut ri, &arg, &bound(502, false, false)).unwrap(), -1);
}

#[test]
fn config_shape() {
    let cfgin = spgConfigIn::default();
    let mut cfg = spgConfigOut::default();
    let mut frame = ::types_fmgr::LocalFcinfo::<2>::fresh(InvalidOid);
    frame.set_arg(0, Datum::from_usize(&cfgin as *const spgConfigIn as usize));
    frame.set_arg(1, Datum::from_usize(&mut cfg as *mut spgConfigOut as usize));
    let mut fi = FmgrInfo::new(fc_spg_range_quad_config, 3469, 2, true, false);
    fi.invoke(&mut frame).unwrap();
    assert_eq!(cfg.prefixType, ANYRANGEOID);
    assert_eq!(cfg.labelType, VOIDOID);
    assert!(cfg.canReturnData);
    assert!(!cfg.longValuesOK);
}
