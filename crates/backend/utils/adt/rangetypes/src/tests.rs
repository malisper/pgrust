use super::*;
use ::mcx::MemoryContext;
use ::types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

fn fc_i32_cmp(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (fcinfo.arg(0).as_i32(), fcinfo.arg(1).as_i32());
    Ok(Datum::from_i32(a.cmp(&b) as i32))
}

const INT4RANGE: Oid = 3904;

fn int4_ri(canonical: bool) -> RangeInfo {
    RangeInfo {
        pin: None,
        rngtypid: INT4RANGE,
        collation: InvalidOid,
        elem_typid: 23,
        elem: ElemInfo { typlen: 4, typbyval: true, typalign: b'i', typstorage: b'p' },
        cmp: FmgrInfo::new(fc_i32_cmp, 351, 2, true, false),
        canonical_oid: if canonical { F_INT4RANGE_CANONICAL } else { InvalidOid },
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

fn inf_bound(lower: bool) -> RangeBound {
    RangeBound { val: Datum::from_usize(0), infinite: true, inclusive: false, lower }
}

fn fc_i64_cmp(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (fcinfo.arg(0).as_i64(), fcinfo.arg(1).as_i64());
    Ok(Datum::from_i32(a.cmp(&b) as i32))
}

const INT8RANGE: Oid = 3926;

fn int8_ri() -> RangeInfo {
    RangeInfo {
        pin: None,
        rngtypid: INT8RANGE,
        collation: InvalidOid,
        elem_typid: 20,
        elem: ElemInfo { typlen: 8, typbyval: true, typalign: b'd', typstorage: b'p' },
        cmp: FmgrInfo::new(fc_i64_cmp, 351, 2, true, false),
        canonical_oid: InvalidOid,
        elem_hash: None,
        elem_hash_extended: None,
        own_typlen: -1,
        own_typbyval: false,
        own_typalign: b'd',
    }
}

// WASM-SUBPLANFIX regression: datum_write's byval arm copies `typlen` bytes
// from the FULL 8-byte Datum word (C store_att_byval; SIZEOF_DATUM pinned to
// 8 on every target). A usize image on wasm32 holds only 4 bytes, so 8-byte
// byval range subtypes panicked at `bytes[..8]` and high-word bound values
// could never serialize.
#[test]
fn int8_bounds_serialize_full_datum_word() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let mut ri = int8_ri();
    let lo_v: i64 = 0x1_0000_0001; // > 2^32: the high word is load-bearing
    let up_v: i64 = 0x2_0000_0007;
    let mut lo = RangeBound { val: Datum::from_i64(lo_v), infinite: false, inclusive: true, lower: true };
    let mut up = RangeBound { val: Datum::from_i64(up_v), infinite: false, inclusive: false, lower: false };
    let img = range_serialize(mcx, &mut ri, &mut lo, &mut up, false, None).unwrap().unwrap();
    // vl(4) + oid(4) + 8 + 8 + flags(1) = 25
    assert_eq!(img.len(), 25);
    assert_eq!(i64::from_ne_bytes(img[8..16].try_into().unwrap()), lo_v);
    assert_eq!(i64::from_ne_bytes(img[16..24].try_into().unwrap()), up_v);
    let (lo2, up2, empty) = range_deserialize(&ri.elem, &img);
    assert!(!empty);
    assert_eq!(lo2.val.as_i64(), lo_v);
    assert_eq!(up2.val.as_i64(), up_v);
}

#[test]
fn serialize_layout_is_byte_exact() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let mut ri = int4_ri(false);
    let mut lo = bound(1, true, true);
    let mut up = bound(10, false, false);
    let img = range_serialize(mcx, &mut ri, &mut lo, &mut up, false, None).unwrap().unwrap();
    // vl(4) + oid(4) + 4 + 4 + flags(1) = 17
    assert_eq!(img.len(), 17);
    assert_eq!(range_type_oid(&img), INT4RANGE);
    assert_eq!(i32::from_ne_bytes(img[8..12].try_into().unwrap()), 1);
    assert_eq!(i32::from_ne_bytes(img[12..16].try_into().unwrap()), 10);
    assert_eq!(range_get_flags(&img), RANGE_LB_INC);
    // varlena header encodes total size << 2
    assert_eq!(u32::from_ne_bytes(img[0..4].try_into().unwrap()) >> 2, 17);

    let (lo2, up2, empty) = range_deserialize(&ri.elem, &img);
    assert!(!empty);
    assert_eq!(lo2.val.as_i32(), 1);
    assert!(lo2.inclusive && !lo2.infinite);
    assert_eq!(up2.val.as_i32(), 10);
    assert!(!up2.inclusive);
}

#[test]
fn serialize_empty_and_bound_order() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let mut ri = int4_ri(false);
    // equal bounds, not both inclusive -> empty (9 bytes: hdr + flags)
    let img = range_serialize(mcx, &mut ri, &mut bound(5, false, true), &mut bound(5, true, false), false, None)
        .unwrap()
        .unwrap();
    assert_eq!(img.len(), 9);
    assert_eq!(range_get_flags(&img), RANGE_EMPTY);
    // lower > upper errors
    let err = range_serialize(mcx, &mut ri, &mut bound(6, true, true), &mut bound(5, true, false), false, None)
        .unwrap_err();
    assert!(err.message().contains("less than or equal"));
}

#[test]
fn canonical_normalizes_discrete_bounds() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let mut ri = int4_ri(true);
    // (1,5] -> [2,6)
    let img = make_range(mcx, &mut ri, &mut bound(1, false, true), &mut bound(5, true, false), false, None)
        .unwrap()
        .unwrap();
    let (lo, up, empty) = range_deserialize(&ri.elem, &img);
    assert!(!empty);
    assert_eq!(lo.val.as_i32(), 2);
    assert!(lo.inclusive);
    assert_eq!(up.val.as_i32(), 6);
    assert!(!up.inclusive);
    // (5,5] is empty BEFORE canonical runs: INT32_MAX bound never overflows
    let img = make_range(
        mcx,
        &mut ri,
        &mut bound(i32::MAX, false, true),
        &mut bound(i32::MAX, true, false),
        false,
        None,
    )
    .unwrap()
    .unwrap();
    assert!(range_is_empty(&img));
    // [MAX,MAX] canonical overflows on the upper bound
    let err = make_range(
        mcx,
        &mut ri,
        &mut bound(i32::MAX, true, true),
        &mut bound(i32::MAX, true, false),
        false,
        None,
    )
    .unwrap_err();
    assert!(err.message().contains("integer out of range"));
}

#[test]
fn infinite_bounds_serialize_without_payload() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let mut ri = int4_ri(false);
    let img =
        range_serialize(mcx, &mut ri, &mut inf_bound(true), &mut bound(3, false, false), false, None)
            .unwrap()
            .unwrap();
    assert_eq!(img.len(), 13); // hdr + one 4-byte bound + flags
    assert_eq!(range_get_flags(&img), RANGE_LB_INF);
    let (lo, up, _e) = range_deserialize(&ri.elem, &img);
    assert!(lo.infinite);
    assert_eq!(up.val.as_i32(), 3);
}

#[test]
fn cmp_bounds_matrix() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let mut ri = int4_ri(false);
    // -inf lower < finite
    assert_eq!(range_cmp_bounds(mcx, &mut ri, &inf_bound(true), &bound(0, true, true)).unwrap(), -1);
    // +inf upper > finite
    assert_eq!(range_cmp_bounds(mcx, &mut ri, &inf_bound(false), &bound(0, true, true)).unwrap(), 1);
    // equal value: exclusive lower > inclusive lower
    assert_eq!(
        range_cmp_bounds(mcx, &mut ri, &bound(5, false, true), &bound(5, true, true)).unwrap(),
        1
    );
    // equal value: exclusive upper < inclusive upper
    assert_eq!(
        range_cmp_bounds(mcx, &mut ri, &bound(5, false, false), &bound(5, true, false)).unwrap(),
        -1
    );
    // both inclusive equal, mixed lower/upper: equal
    assert_eq!(
        range_cmp_bounds(mcx, &mut ri, &bound(5, true, false), &bound(5, true, true)).unwrap(),
        0
    );
    // both exclusive equal: lower > upper
    assert_eq!(
        range_cmp_bounds(mcx, &mut ri, &bound(5, false, true), &bound(5, false, false)).unwrap(),
        1
    );
}

#[test]
fn parse_and_deparse_round_trip_grammar() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let p = crate::io::range_parse(mcx, b"  [1,10) ", None).unwrap().unwrap();
    assert_eq!(p.flags, RANGE_LB_INC);
    assert_eq!(p.lbound.as_deref(), Some(&b"1"[..]));
    assert_eq!(p.ubound.as_deref(), Some(&b"10"[..]));

    let p = crate::io::range_parse(mcx, b"EMPTY", None).unwrap().unwrap();
    assert_eq!(p.flags, RANGE_EMPTY);

    let p = crate::io::range_parse(mcx, b"(,]", None).unwrap().unwrap();
    assert_eq!(p.flags, RANGE_LB_INF | RANGE_UB_INF | RANGE_UB_INC);
    assert!(p.lbound.is_none() && p.ubound.is_none());

    // quoting and escapes
    let p = crate::io::range_parse(mcx, br#"["a ""b",\ c)"#, None).unwrap().unwrap();
    assert_eq!(p.lbound.as_deref(), Some(&br#"a "b"#[..]));
    assert_eq!(p.ubound.as_deref(), Some(&b" c"[..]));

    // quoted empty string is a bound, not infinity
    let p = crate::io::range_parse(mcx, br#"["",)"#, None).unwrap().unwrap();
    assert_eq!(p.lbound.as_deref(), Some(&b""[..]));

    for (bad, detail) in [
        (&b"1,2)"[..], "Missing left parenthesis or bracket."),
        (b"[1 2)", "Missing comma after lower bound."),
        (b"[1,2,3)", "Too many commas."),
        (b"[1,2) x", "Junk after right parenthesis or bracket."),
        (b"empty x", "Junk after \"empty\" key word."),
        (b"[1,2", "Unexpected end of input."),
    ] {
        let err = crate::io::range_parse(mcx, bad, None).unwrap_err();
        assert_eq!(err.detail(), Some(detail), "case {:?}", String::from_utf8_lossy(bad));
        assert_eq!(err.sqlstate(), ::types_error::ERRCODE_INVALID_TEXT_REPRESENTATION);
    }

    let out = crate::io::range_deparse(mcx, RANGE_LB_INC, Some(b"a b"), Some(b"c\"d")).unwrap();
    assert_eq!(&out[..], b"[\"a b\",\"c\"\"d\")\0");
}

#[test]
fn deparse_quoting_rules() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let out = crate::io::range_deparse(mcx, RANGE_LB_INC | RANGE_UB_INC, Some(b"1"), Some(b"2"))
        .unwrap();
    assert_eq!(&out[..], b"[1,2]\0");
    let out = crate::io::range_deparse(mcx, RANGE_EMPTY, None, None).unwrap();
    assert_eq!(&out[..], b"empty\0");
    let out = crate::io::range_deparse(mcx, 0, Some(b""), Some(b"a\\b")).unwrap();
    assert_eq!(&out[..], b"(\"\",\"a\\\\b\")\0");
}

#[test]
fn ops_over_int4_ranges() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let mut ri = int4_ri(true);
    let mk = |ri: &mut RangeInfo, lo: i32, hi: i32| {
        make_range(mcx, ri, &mut bound(lo, true, true), &mut bound(hi, false, false), false, None)
            .unwrap()
            .unwrap()
    };
    let a = mk(&mut ri, 1, 5);
    let b = mk(&mut ri, 3, 8);
    let c = mk(&mut ri, 5, 8);
    let empty = make_empty_range(mcx, &mut ri).unwrap();

    assert!(crate::ops::range_overlaps_internal(mcx, &mut ri, &a, &b).unwrap());
    assert!(!crate::ops::range_overlaps_internal(mcx, &mut ri, &a, &c).unwrap());
    assert!(crate::ops::range_adjacent_internal(mcx, &mut ri, &a, &c).unwrap());
    assert!(crate::ops::range_before_internal(mcx, &mut ri, &a, &c).unwrap());
    assert!(crate::ops::range_after_internal(mcx, &mut ri, &c, &a).unwrap());
    assert!(crate::ops::range_contains_elem_internal(mcx, &mut ri, &a, Datum::from_i32(4)).unwrap());
    assert!(!crate::ops::range_contains_elem_internal(mcx, &mut ri, &a, Datum::from_i32(5)).unwrap());
    assert!(!crate::ops::range_contains_elem_internal(mcx, &mut ri, &empty, Datum::from_i32(1)).unwrap());
    assert!(crate::ops::range_eq_internal(mcx, &mut ri, &a, &a).unwrap());
    assert!(crate::ops::range_ne_internal(mcx, &mut ri, &a, &b).unwrap());

    // union/intersect/minus
    match crate::ops::range_union_internal(mcx, &mut ri, &a, &b, true).unwrap() {
        crate::ops::UnionResult::New(u) => {
            let (lo, up, _e) = range_deserialize(&ri.elem, &u);
            assert_eq!((lo.val.as_i32(), up.val.as_i32()), (1, 8));
        }
        _ => panic!("expected new image"),
    }
    let i = crate::ops::range_intersect_internal(mcx, &mut ri, &a, &b).unwrap();
    let (lo, up, _e) = range_deserialize(&ri.elem, &i);
    assert_eq!((lo.val.as_i32(), up.val.as_i32()), (3, 5));
    match crate::ops::range_minus_internal(mcx, &mut ri, &a, &b).unwrap() {
        crate::ops::MinusResult::New(m) => {
            let (lo, up, _e) = range_deserialize(&ri.elem, &m);
            assert_eq!((lo.val.as_i32(), up.val.as_i32()), (1, 3));
        }
        _ => panic!("expected new image"),
    }
    // disjoint union errors, merge doesn't
    let d = mk(&mut ri, 7, 9);
    assert!(crate::ops::range_union_internal(mcx, &mut ri, &a, &d, true).is_err());
    match crate::ops::range_union_internal(mcx, &mut ri, &a, &d, false).unwrap() {
        crate::ops::UnionResult::New(u) => {
            let (lo, up, _e) = range_deserialize(&ri.elem, &u);
            assert_eq!((lo.val.as_i32(), up.val.as_i32()), (1, 9));
        }
        _ => panic!("expected new image"),
    }

    // cmp: empty sorts first
    assert_eq!(crate::ops::range_cmp_internal(mcx, &mut ri, &empty, &a).unwrap(), -1);
    assert_eq!(crate::ops::range_cmp_internal(mcx, &mut ri, &a, &b).unwrap(), -1);
    assert_eq!(crate::ops::range_cmp_internal(mcx, &mut ri, &a, &a).unwrap(), 0);

    // split
    let wide = mk(&mut ri, 0, 10);
    let mid = mk(&mut ri, 4, 6);
    let (s1, s2) = crate::ops::range_split_internal(mcx, &mut ri, &wide, &mid).unwrap().unwrap();
    let (lo, up, _e) = range_deserialize(&ri.elem, &s1);
    assert_eq!((lo.val.as_i32(), up.val.as_i32()), (0, 4));
    let (lo, up, _e) = range_deserialize(&ri.elem, &s2);
    assert_eq!((lo.val.as_i32(), up.val.as_i32()), (6, 10));
}

#[test]
fn short_varlena_bounds_pack_without_padding() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    // A byref packable elem type (numeric-shaped): two 4-byte-header varlenas
    // must pack to short form back to back after the 8-byte range header.
    let mut ri = int4_ri(false);
    ri.elem = ElemInfo { typlen: -1, typbyval: false, typalign: b'i', typstorage: b'm' };
    fn fc_varlena_cmp(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
        // SAFETY: test datums are live 4-byte-header varlenas.
        let read = |d: Datum| unsafe {
            let p = d.as_usize() as *const u8;
            if *p & 0x01 == 0x01 {
                *(p.add(1)) as i32
            } else {
                *(p.add(4)) as i32
            }
        };
        let (a, b) = (read(fcinfo.arg(0)), read(fcinfo.arg(1)));
        Ok(Datum::from_i32(a.cmp(&b) as i32))
    }
    ri.cmp = FmgrInfo::new(fc_varlena_cmp, 0, 2, true, false);

    let v1: [u8; 5] = [5 << 2, 0, 0, 0, 7];
    let v2: [u8; 5] = [5 << 2, 0, 0, 0, 9];
    let mut lo = RangeBound {
        val: Datum::from_usize(v1.as_ptr() as usize),
        infinite: false,
        inclusive: true,
        lower: true,
    };
    let mut up = RangeBound {
        val: Datum::from_usize(v2.as_ptr() as usize),
        infinite: false,
        inclusive: false,
        lower: false,
    };
    let img = range_serialize(mcx, &mut ri, &mut lo, &mut up, false, None).unwrap().unwrap();
    // 8 hdr + 2 short varlenas of 2 bytes each + flags = 13, no padding.
    assert_eq!(img.len(), 13);
    assert_eq!(img[8], (2 << 1) | 1);
    assert_eq!(img[9], 7);
    assert_eq!(img[10], (2 << 1) | 1);
    assert_eq!(img[11], 9);
    let (lo2, up2, _e) = range_deserialize(&ri.elem, &img);
    // deserialized datums point at the short headers inside the image
    assert_eq!(lo2.val.as_usize(), img[8..].as_ptr() as usize);
    assert_eq!(up2.val.as_usize(), img[10..].as_ptr() as usize);
}

// Bound-detoast law (C rangetypes.c:1855-1874 PG_DETOAST_DATUM_PACKED): an
// external or compressed bound must be inlined/decompressed before packing —
// never a toast pointer inside a range — while a short-header bound stays
// as-is. Hand-built images; the detoast seam gets the real detoast crate and
// on-disk pointers resolve against a canned in-test toast store.
mod bound_detoast {
    use super::*;
    use ::mcx::{vec_with_capacity_in, PgVec};
    use std::collections::HashMap;
    use std::sync::Mutex;

    static TOAST_STORE: Mutex<Option<HashMap<u32, std::vec::Vec<u8>>>> = Mutex::new(None);

    fn install_test_detoast() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            ::detoast_seams::detoast_attr::set(::detoast::detoast_attr);
            ::toast_internals_seams::toast_fetch_datum::set(test_toast_fetch);
        });
    }

    fn test_toast_fetch<'mcx>(mcx: Mcx<'mcx>, attr: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
        assert_eq!((attr[0], attr[1], attr.len()), (0x01, 0x12, 18));
        let valueid = u32::from_ne_bytes(attr[10..14].try_into().unwrap());
        let store = TOAST_STORE.lock().unwrap();
        let payload = store
            .as_ref()
            .and_then(|m| m.get(&valueid))
            .expect("test toast store: unknown va_valueid");
        let mut out = vec_with_capacity_in(mcx, payload.len())?;
        out.extend_from_slice(payload);
        Ok(out)
    }

    fn flat(mcx: Mcx<'_>, payload: &[u8]) -> Datum {
        let total = 4 + payload.len();
        let mut v: PgVec<u8> = vec_with_capacity_in(mcx, total).unwrap();
        v.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
        v.extend_from_slice(payload);
        let p = v.as_ptr();
        core::mem::forget(v);
        Datum::from_usize(p as usize)
    }

    fn pglz_img(mcx: Mcx<'_>, payload: &[u8]) -> Datum {
        use core::mem::MaybeUninit;
        let mut dst: std::vec::Vec<MaybeUninit<u8>> =
            std::vec![MaybeUninit::uninit(); pglz::pglz_max_output(payload.len())];
        let clen = pglz::pglz_compress_into(payload, &mut dst, &pglz::PGLZ_STRATEGY_DEFAULT)
            .expect("test payload must compress");
        let total = 8 + clen;
        let mut v: PgVec<u8> = vec_with_capacity_in(mcx, total).unwrap();
        v.extend_from_slice(&(((total as u32) << 2) | 0x02).to_ne_bytes());
        v.extend_from_slice(&(payload.len() as u32).to_ne_bytes());
        // SAFETY: pglz_compress_into initialized the first clen bytes.
        v.extend_from_slice(unsafe {
            core::slice::from_raw_parts(dst.as_ptr().cast::<u8>(), clen)
        });
        let p = v.as_ptr();
        core::mem::forget(v);
        Datum::from_usize(p as usize)
    }

    fn ondisk(mcx: Mcx<'_>, valueid: u32, payload: &[u8]) -> Datum {
        {
            let mut full = std::vec::Vec::with_capacity(4 + payload.len());
            full.extend_from_slice(&(((4 + payload.len()) as u32) << 2).to_ne_bytes());
            full.extend_from_slice(payload);
            let mut store = TOAST_STORE.lock().unwrap();
            store.get_or_insert_with(HashMap::new).insert(valueid, full);
        }
        let rawsize = (4 + payload.len()) as u32;
        let mut v: PgVec<u8> = vec_with_capacity_in(mcx, 18).unwrap();
        v.push(0x01);
        v.push(0x12); // VARTAG_ONDISK
        v.extend_from_slice(&rawsize.to_ne_bytes());
        v.extend_from_slice(&(rawsize - 4).to_ne_bytes());
        v.extend_from_slice(&valueid.to_ne_bytes());
        v.extend_from_slice(&0u32.to_ne_bytes());
        let p = v.as_ptr();
        core::mem::forget(v);
        Datum::from_usize(p as usize)
    }

    fn short(mcx: Mcx<'_>, payload: &[u8]) -> Datum {
        assert!(payload.len() <= 126);
        let total = 1 + payload.len();
        let mut v: PgVec<u8> = vec_with_capacity_in(mcx, total).unwrap();
        v.push(((total as u8) << 1) | 1);
        v.extend_from_slice(payload);
        let p = v.as_ptr();
        core::mem::forget(v);
        Datum::from_usize(p as usize)
    }

    // A text-flavored range info; cmp never runs in these tests (the upper
    // bound is infinite, so range_cmp_bound_values shortcuts).
    fn text_ri() -> RangeInfo {
        fn fc_never(_f: Option<&mut FmgrInfo>, _fc: &mut Fcinfo) -> PgResult<Datum> {
            panic!("cmp must not run: upper bound is infinite");
        }
        RangeInfo {
            pin: None,
            rngtypid: 99001,
            collation: InvalidOid,
            elem_typid: 25,
            elem: ElemInfo { typlen: -1, typbyval: false, typalign: b'i', typstorage: b'x' },
            cmp: FmgrInfo::new(fc_never, 360, 2, true, false),
            canonical_oid: InvalidOid,
            elem_hash: None,
            elem_hash_extended: None,
            own_typlen: -1,
            own_typbyval: false,
            own_typalign: b'i',
        }
    }

    fn text_bound(val: Datum) -> RangeBound {
        RangeBound { val, infinite: false, inclusive: true, lower: true }
    }

    fn serialize_lower<'m>(mcx: Mcx<'m>, val: Datum) -> PgVec<'m, u8> {
        let mut ri = text_ri();
        let mut lo = text_bound(val);
        let mut up = inf_bound(false);
        range_serialize(mcx, &mut ri, &mut lo, &mut up, false, None).unwrap().unwrap()
    }

    #[test]
    fn external_bound_is_inlined() {
        install_test_detoast();
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let payload: std::vec::Vec<u8> =
            b"range external ".iter().copied().cycle().take(2400).collect();
        let got = serialize_lower(mcx, ondisk(mcx, 8001, &payload));
        let want = serialize_lower(mcx, flat(mcx, &payload));
        assert_eq!(&got[..], &want[..], "external bound must serialize as the inline value");
    }

    #[test]
    fn compressed_bound_is_decompressed() {
        install_test_detoast();
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let payload: std::vec::Vec<u8> =
            b"range compressible ".iter().copied().cycle().take(500).collect();
        let got = serialize_lower(mcx, pglz_img(mcx, &payload));
        let want = serialize_lower(mcx, flat(mcx, &payload));
        assert_eq!(&got[..], &want[..], "compressed bound must serialize decompressed");
    }

    #[test]
    fn short_bound_stays_packed() {
        install_test_detoast();
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        // PACKED law: a short-header bound stays short; a small flat bound is
        // re-packed short by datum_write, so both images agree.
        let payload = b"short bound";
        let got = serialize_lower(mcx, short(mcx, payload));
        let want = serialize_lower(mcx, flat(mcx, payload));
        assert_eq!(&got[..], &want[..]);
    }
}
