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

    pub(super) fn install_test_detoast() {
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

// P1 regression (fuzz-found 2026-07-31, lane p1-laneac): range_recv sized its
// bound buffer from the UNVALIDATED wire length before pq_getmsgbytes ran.
// bound_len == 0 produced a zero-capacity StringInfo whose unconditional NUL
// write went through PgVec's dangling sentinel — release SEGV (a debug-only
// debug_assert masked it). Ground-truthed on postgres:18.3: the same wire
// raises 08P01 "insufficient data left in message" and the backend survives.
// C validates first (pq_getmsgbytes), then takes a fixed-size initStringInfo.
mod recv_wire {
    use super::*;
    use crate::io::{range_recv, RangeIOData};
    use ::types_error::ERRCODE_PROTOCOL_VIOLATION;

    /// int4recv stand-in: consumes exactly 4 network-order bytes.
    fn fc_i32_recv(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
        // SAFETY: receive_function_call passes a live StringInfo in arg 0.
        let buf = unsafe { fcinfo.arg_stringinfo(0) };
        Ok(Datum::from_i32(::pqformat::pq_getmsgint(buf, 4)? as i32))
    }

    fn io_data() -> RangeIOData {
        RangeIOData {
            ri: int4_ri(false),
            typioproc: FmgrInfo::new(fc_i32_recv, 2406, 3, true, false),
            typioparam: 23,
        }
    }

    fn recv(mcx: ::mcx::Mcx<'_>, wire: &[u8]) -> PgResult<()> {
        let mut buf = ::stringinfo::StringInfo::new_in(mcx)?;
        buf.append_bytes(wire)?;
        let mut cache = io_data();
        range_recv(mcx, &mut cache, &mut buf, -1).map(|_| ())
    }

    #[test]
    fn zero_length_bound_errors_and_does_not_crash() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        // flags = LB_INC|UB_INC (both bounds present), lower bound length 0.
        let wire = [RANGE_LB_INC | RANGE_UB_INC, 0, 0, 0, 0];
        let e = recv(mcx, &wire).expect_err("zero-length bound must be a protocol error");
        assert_eq!(e.sqlstate, ERRCODE_PROTOCOL_VIOLATION);
    }

    #[test]
    fn oversized_bound_length_errors_before_allocating() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        // A ~4 GiB bound length on a 5-byte message: C's pq_getmsgbytes
        // rejects it before any allocation, so pgrust must too (never a
        // multi-GiB reserve).
        let wire = [RANGE_LB_INC | RANGE_UB_INC, 0xEB, 0xFF, 0xFF, 0xFF];
        let e = recv(mcx, &wire).expect_err("oversized bound length must be a protocol error");
        assert_eq!(e.sqlstate, ERRCODE_PROTOCOL_VIOLATION);
    }

    #[test]
    fn well_formed_bounds_still_round_trip() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let mut wire = std::vec::Vec::new();
        wire.push(RANGE_LB_INC);
        wire.extend_from_slice(&4u32.to_be_bytes());
        wire.extend_from_slice(&1i32.to_be_bytes());
        wire.extend_from_slice(&4u32.to_be_bytes());
        wire.extend_from_slice(&9i32.to_be_bytes());
        recv(mcx, &wire).expect("well-formed [1,9) wire must receive");
    }
}

/// p1-rangeguard REGRESSION (release blocker, task #78): rangetypes.c guards
/// SEVEN functions with check_stack_depth() — range_in(104), range_out(151),
/// range_recv(190), range_send(273), range_cmp(1264), hash_range(1407),
/// hash_range_extended(1474) — every one commented "recurses when subtype is
/// a range type". The port had ZERO of them, and this hole ROUTES AROUND the
/// parser's guard: the nesting comes from CREATE TYPE (a range whose subtype
/// is another range type), so the recursion runs through the element type's
/// I/O / cmp / hash function via fmgr, not through expression depth.
///
/// Without the guard, deep nesting overflows the thread stack and the Rust
/// runtime aborts the PROCESS. pgrust is thread-per-backend, so that kills
/// every session, not just the offending one. C 18.3 on the same shape raises
/// ERRCODE_STATEMENT_TOO_COMPLEX (54001) and survives.
///
/// The rig emulates exactly the production dispatch: each nested fc function
/// resolves the inner range type from the argument image's embedded rngtypid
/// (what flinfo_ri / cached_range_io_data do via typcache) and re-enters the
/// same rangetypes entry point, so the frames on the stack are the real
/// recursion frames. The serialized value and the binary wire form are LINEAR
/// in nesting depth, so these six shapes are all reachable with small input.
/// (range_in is the seventh C site; its guard is ported for parity, but its
/// TEXT form needs quote-doubling per level — exponential input — so it
/// cannot be driven deep by any feasible input in C or pgrust.)
mod stack_guard {
    use super::*;
    use crate::builtins::arg_range;
    use crate::io::{self, RangeIOData};
    use crate::ops;
    use ::lsyscache::IOFuncSelector;
    use ::types_error::ERRCODE_STATEMENT_TOO_COMPLEX;
    use ::types_fmgr::{byref_result, cstring_result, varlena_result};

    // Synthetic, non-catalog oids: a range over int4, and a range whose
    // subtype is a range (the leaf one, or itself at every deeper level).
    const LEAF_RANGE: Oid = 999_000;
    const NESTED_RANGE: Oid = 999_001;

    // Inner range bounds < 127 bytes are stored SHORT (1-byte header), so
    // arg_range takes the detoast path; share bound_detoast's process-wide
    // installer (a seam panics if installed twice).
    fn install_detoast() {
        super::bound_detoast::install_test_detoast();
    }

    fn fc_leaf_i32_hash(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
        Ok(Datum::from_i32(fcinfo.arg(0).as_i32()))
    }

    fn fc_leaf_i32_hash_ext(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
        Ok(Datum::from_u64(fcinfo.arg(0).as_i32() as u64 ^ fcinfo.arg(1).as_u64()))
    }

    fn fc_nested_hash(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
        let mcx = fcinfo.result_mcx();
        let r = arg_range(fcinfo, 0, mcx)?;
        let mut ri = ri_for(range_type_oid(&r));
        Ok(Datum::from_i32(ops::hash_range_internal(mcx, &mut ri, &r)? as i32))
    }

    fn fc_nested_hash_ext(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
        let mcx = fcinfo.result_mcx();
        let r = arg_range(fcinfo, 0, mcx)?;
        let seed = fcinfo.arg(1);
        let mut ri = ri_for(range_type_oid(&r));
        Ok(Datum::from_u64(ops::hash_range_extended_internal(mcx, &mut ri, &r, seed)?))
    }

    fn fc_nested_cmp(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
        let mcx = fcinfo.result_mcx();
        let r1 = arg_range(fcinfo, 0, mcx)?;
        let r2 = arg_range(fcinfo, 1, mcx)?;
        let mut ri = ri_for(range_type_oid(&r1));
        Ok(Datum::from_i32(ops::range_cmp_internal(mcx, &mut ri, &r1, &r2)?))
    }

    fn fc_leaf_i32_out(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
        let mcx = fcinfo.result_mcx();
        let s = format!("{}\0", fcinfo.arg(0).as_i32());
        let mut v: ::mcx::PgVec<'_, u8> = ::mcx::vec_with_capacity_in(mcx, s.len())?;
        ::mcx::vec_append_bytes(&mut v, s.as_bytes())?;
        Ok(cstring_result(v))
    }

    fn fc_nested_out(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
        let mcx = fcinfo.result_mcx();
        let r = arg_range(fcinfo, 0, mcx)?;
        let mut cache = io_cache_for(range_type_oid(&r), IOFuncSelector::IOFunc_output);
        Ok(cstring_result(io::range_out(mcx, &mut cache, &r)?))
    }

    fn fc_leaf_i32_send(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
        let mcx = fcinfo.result_mcx();
        let mut buf = ::pqformat::pq_begintypsend(mcx)?;
        ::pqformat::pq_sendint32(&mut buf, fcinfo.arg(0).as_i32() as u32)?;
        Ok(varlena_result(::pqformat::pq_endtypsend(buf)))
    }

    fn fc_nested_send(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
        let mcx = fcinfo.result_mcx();
        let r = arg_range(fcinfo, 0, mcx)?;
        let mut cache = io_cache_for(range_type_oid(&r), IOFuncSelector::IOFunc_send);
        Ok(varlena_result(io::range_send(mcx, &mut cache, &r)?))
    }

    fn fc_leaf_i32_recv(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
        // SAFETY: receive_function_call passes a live StringInfo in arg 0.
        let buf = unsafe { fcinfo.arg_stringinfo(0) };
        Ok(Datum::from_i32(::pqformat::pq_getmsgint(buf, 4)? as i32))
    }

    fn fc_nested_recv(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
        let mcx = fcinfo.result_mcx();
        // SAFETY: receive_function_call passes a live StringInfo in arg 0.
        let buf = unsafe { fcinfo.arg_stringinfo(0) };
        // The wire form carries no oid; the leaf-range wire is exactly
        // flags(1) + len(4) + int4(4) = 9 bytes, a deeper one is >= 14.
        let inner = if buf.len() - buf.cursor == 9 { LEAF_RANGE } else { NESTED_RANGE };
        let mut cache = io_cache_for(inner, IOFuncSelector::IOFunc_receive);
        let img = io::range_recv(mcx, &mut cache, buf, -1)?;
        byref_result(mcx, &img)
    }

    /// What cached_range_info / flinfo_ri resolve from typcache in production.
    fn ri_for(rngtypid: Oid) -> RangeInfo {
        if rngtypid == LEAF_RANGE {
            RangeInfo {
                pin: None,
                rngtypid: LEAF_RANGE,
                collation: InvalidOid,
                elem_typid: 23,
                elem: ElemInfo { typlen: 4, typbyval: true, typalign: b'i', typstorage: b'p' },
                cmp: FmgrInfo::new(fc_i32_cmp, 351, 2, true, false),
                canonical_oid: InvalidOid,
                elem_hash: Some(FmgrInfo::new(fc_leaf_i32_hash, 425, 1, true, false)),
                elem_hash_extended: Some(FmgrInfo::new(fc_leaf_i32_hash_ext, 442, 2, true, false)),
                own_typlen: -1,
                own_typbyval: false,
                own_typalign: b'i',
            }
        } else {
            RangeInfo {
                pin: None,
                rngtypid: NESTED_RANGE,
                collation: InvalidOid,
                elem_typid: NESTED_RANGE,
                elem: ElemInfo { typlen: -1, typbyval: false, typalign: b'i', typstorage: b'x' },
                cmp: FmgrInfo::new(fc_nested_cmp, 3870, 2, true, false),
                canonical_oid: InvalidOid,
                elem_hash: Some(FmgrInfo::new(fc_nested_hash, 3902, 1, true, false)),
                elem_hash_extended: Some(FmgrInfo::new(fc_nested_hash_ext, 3417, 2, true, false)),
                own_typlen: -1,
                own_typbyval: false,
                own_typalign: b'i',
            }
        }
    }

    /// What cached_range_io_data resolves in production: the range's typcache
    /// info plus the ELEMENT type's I/O function.
    fn io_cache_for(rngtypid: Oid, func: IOFuncSelector) -> RangeIOData {
        let leaf = rngtypid == LEAF_RANGE;
        let typioproc = match func {
            IOFuncSelector::IOFunc_output => {
                if leaf {
                    FmgrInfo::new(fc_leaf_i32_out, 43, 1, true, false)
                } else {
                    FmgrInfo::new(fc_nested_out, 3835, 1, true, false)
                }
            }
            IOFuncSelector::IOFunc_send => {
                if leaf {
                    FmgrInfo::new(fc_leaf_i32_send, 2406, 1, true, false)
                } else {
                    FmgrInfo::new(fc_nested_send, 3836, 1, true, false)
                }
            }
            IOFuncSelector::IOFunc_receive => {
                if leaf {
                    FmgrInfo::new(fc_leaf_i32_recv, 2404, 3, true, false)
                } else {
                    FmgrInfo::new(fc_nested_recv, 3834, 3, true, false)
                }
            }
            _ => unreachable!("text input is not deep-drivable (exponential literal)"),
        };
        RangeIOData { ri: ri_for(rngtypid), typioproc, typioparam: if leaf { 23 } else { NESTED_RANGE } }
    }

    /// 8-byte-aligned copy of a serialized range image (bound datums are read
    /// through varlena headers; keep them aligned like palloc does).
    fn img_copy(img: &[u8]) -> (Vec<u64>, usize) {
        let n = img.len();
        let mut v = vec![0u64; n.div_ceil(8)];
        // SAFETY: destination has >= n writable bytes.
        unsafe { std::ptr::copy_nonoverlapping(img.as_ptr(), v.as_mut_ptr() as *mut u8, n) };
        (v, n)
    }

    /// depth-1 nested serialized value, built bottom-up with the real
    /// serializer (level 1 = LEAF_RANGE over int4 [1,2); each further level =
    /// NESTED_RANGE with the previous image as its lower bound, upper
    /// infinite). Linear in depth; each level's scratch context is dropped.
    fn build_nested_img(depth: usize) -> (Vec<u64>, usize) {
        assert!(depth >= 1);
        let (mut buf, mut len);
        {
            let cx = MemoryContext::new("leaf");
            let mcx = cx.mcx();
            let mut ri = ri_for(LEAF_RANGE);
            let mut lo = bound(1, true, true);
            let mut up = bound(2, false, false);
            let img = range_serialize(mcx, &mut ri, &mut lo, &mut up, false, None)
                .unwrap()
                .unwrap();
            (buf, len) = img_copy(&img);
        }
        for _ in 1..depth {
            let cx = MemoryContext::new("lvl");
            let mcx = cx.mcx();
            let mut ri = ri_for(NESTED_RANGE);
            let mut lo = RangeBound {
                val: Datum::from_usize(buf.as_ptr() as usize),
                infinite: false,
                inclusive: true,
                lower: true,
            };
            let mut up = inf_bound(false);
            let img = range_serialize(mcx, &mut ri, &mut lo, &mut up, false, None)
                .unwrap()
                .unwrap();
            (buf, len) = img_copy(&img);
        }
        (buf, len)
    }

    fn img_bytes(buf: &[u64], len: usize) -> &[u8] {
        // SAFETY: img_copy wrote `len` initialized bytes at the buffer start.
        unsafe { core::slice::from_raw_parts(buf.as_ptr() as *const u8, len) }
    }

    /// range_recv wire image of the same nested value: linear in depth.
    fn build_nested_wire(depth: usize) -> Vec<u8> {
        assert!(depth >= 1);
        // Leaf level is [1,) — exactly flags(1) + len(4) + int4(4) = 9 bytes,
        // which is fc_nested_recv's leaf discriminator.
        let mut wire = vec![RANGE_LB_INC | RANGE_UB_INF];
        wire.extend_from_slice(&4u32.to_be_bytes());
        wire.extend_from_slice(&1i32.to_be_bytes());
        for _ in 1..depth {
            let mut outer = vec![RANGE_LB_INC | RANGE_UB_INF];
            outer.extend_from_slice(&(wire.len() as u32).to_be_bytes());
            outer.extend_from_slice(&wire);
            wire = outer;
        }
        wire
    }

    /// Shallow sanity: the emulated dispatch really recurses through the real
    /// entry points and produces the right answers.
    #[test]
    fn nested_range_dispatch_is_real() {
        install_detoast();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let (buf, len) = build_nested_img(3);
        let img = img_bytes(&buf, len);
        assert_eq!(range_type_oid(img), NESTED_RANGE);

        let mut ri = ri_for(NESTED_RANGE);
        assert_eq!(ops::range_cmp_internal(mcx, &mut ri, img, img).unwrap(), 0);

        let mut ri = ri_for(NESTED_RANGE);
        let h1 = ops::hash_range_internal(mcx, &mut ri, img).unwrap();
        let mut ri = ri_for(NESTED_RANGE);
        let h2 = ops::hash_range_internal(mcx, &mut ri, img).unwrap();
        assert_eq!(h1, h2);

        let mut cache = io_cache_for(NESTED_RANGE, IOFuncSelector::IOFunc_output);
        let out = io::range_out(mcx, &mut cache, img).unwrap();
        // ["["[1,2)",)",) with per-level quote doubling — the exponential form.
        assert_eq!(&out[..out.len() - 1], br#"["[""[1,2)"",)",)"#);

        let mut cache = io_cache_for(NESTED_RANGE, IOFuncSelector::IOFunc_send);
        io::range_send(mcx, &mut cache, img).unwrap();

        let wire = build_nested_wire(3);
        let mut buf = ::stringinfo::StringInfo::new_in(mcx).unwrap();
        buf.append_bytes(&wire).unwrap();
        let mut cache = io_cache_for(NESTED_RANGE, IOFuncSelector::IOFunc_receive);
        let img2 = io::range_recv(mcx, &mut cache, &mut buf, -1).unwrap();
        assert_eq!(range_type_oid(&img2), NESTED_RANGE);
    }

    /// Runs each deep probe in a subprocess because pre-fix the recursion
    /// aborts the whole process (stack overflow), and a guard asserted
    /// without a survivable harness is assumed, not proven.
    #[test]
    fn range_deep_nesting_raises_54001_and_does_not_abort() {
        // Every one of these ABORTED the process before the fix
        // (--release, 8 MiB probe stack).
        const CASES: [(&str, usize); 8] = [
            ("cmp", 30_000),
            ("hash", 30_000),
            ("hashext", 30_000),
            ("out", 30_000),
            ("send", 30_000),
            ("recv", 30_000),
            ("recv", 200_000),
            ("cmp", 100_000),
        ];
        if let (Ok(d), Ok(kind)) =
            (std::env::var("RANGE_STACK_PROBE_DEPTH"), std::env::var("RANGE_STACK_PROBE_KIND"))
        {
            let depth: usize = d.parse().unwrap();
            let h = std::thread::Builder::new()
                // Production HEADROOM: an 8 MiB worker stack paired with
                // max_stack_depth = 2048 kB. A 2 MiB / 2048 kB pairing has
                // zero headroom and reddens the fleet's dev profile.
                .stack_size(8 << 20)
                .spawn(move || {
                    // A backend thread records its stack base at spawn
                    // (C: main()). Without this, stack_is_too_deep()
                    // short-circuits on base == 0 and every guard is INERT —
                    // the test would be vacuous.
                    ::stack_depth::set_stack_base();
                    ::stack_depth::assign_max_stack_depth(2048);
                    install_detoast();
                    let ctx = MemoryContext::new("t");
                    let mcx = ctx.mcx();
                    let r: PgResult<usize> = match kind.as_str() {
                        "cmp" => {
                            let (buf, len) = build_nested_img(depth);
                            let img = img_bytes(&buf, len);
                            let mut ri = ri_for(NESTED_RANGE);
                            ops::range_cmp_internal(mcx, &mut ri, img, img).map(|c| c as usize)
                        }
                        "hash" => {
                            let (buf, len) = build_nested_img(depth);
                            let img = img_bytes(&buf, len);
                            let mut ri = ri_for(NESTED_RANGE);
                            ops::hash_range_internal(mcx, &mut ri, img).map(|h| h as usize)
                        }
                        "hashext" => {
                            let (buf, len) = build_nested_img(depth);
                            let img = img_bytes(&buf, len);
                            let mut ri = ri_for(NESTED_RANGE);
                            ops::hash_range_extended_internal(mcx, &mut ri, img, Datum::from_u64(11))
                                .map(|h| h as usize)
                        }
                        "out" => {
                            let (buf, len) = build_nested_img(depth);
                            let img = img_bytes(&buf, len);
                            let mut cache = io_cache_for(NESTED_RANGE, IOFuncSelector::IOFunc_output);
                            io::range_out(mcx, &mut cache, img).map(|v| v.len())
                        }
                        "send" => {
                            let (buf, len) = build_nested_img(depth);
                            let img = img_bytes(&buf, len);
                            let mut cache = io_cache_for(NESTED_RANGE, IOFuncSelector::IOFunc_send);
                            io::range_send(mcx, &mut cache, img).map(|_| 0)
                        }
                        "recv" => {
                            let wire = build_nested_wire(depth);
                            let mut buf = ::stringinfo::StringInfo::new_in(mcx).unwrap();
                            buf.append_bytes(&wire).unwrap();
                            let mut cache =
                                io_cache_for(NESTED_RANGE, IOFuncSelector::IOFunc_receive);
                            io::range_recv(mcx, &mut cache, &mut buf, -1).map(|v| v.len())
                        }
                        other => panic!("bad probe kind {other}"),
                    };
                    r
                })
                .unwrap();
            match h.join().expect("probe thread must not panic") {
                Ok(n) => eprintln!("PROBE OK {n}"),
                Err(e) => eprintln!(
                    "PROBE ERR {}",
                    if e.sqlstate == ERRCODE_STATEMENT_TOO_COMPLEX { "54001" } else { "other" }
                ),
            }
            return;
        }
        let exe = std::env::current_exe().unwrap();
        for (kind, depth) in CASES {
            let out = std::process::Command::new(&exe)
                .args([
                    "--exact",
                    "--nocapture",
                    "tests::stack_guard::range_deep_nesting_raises_54001_and_does_not_abort",
                ])
                .env("RANGE_STACK_PROBE_KIND", kind)
                .env("RANGE_STACK_PROBE_DEPTH", depth.to_string())
                .output()
                .unwrap();
            let se = String::from_utf8_lossy(&out.stderr);
            let line = se.lines().find(|l| l.starts_with("PROBE")).unwrap_or_else(|| {
                panic!("{kind}/{depth}: process died without a verdict (stack overflow): {se}")
            });
            // The depth at which the guard trips is a function of frame size
            // and is NOT a comparison surface against C. That the process
            // SURVIVES and reports a clean 54001 rather than aborting is.
            assert_eq!(
                line, "PROBE ERR 54001",
                "{kind}/{depth}: expected a clean 54001, got {line:?}"
            );
        }
    }
}

/// Boundary-guard audit findings 5/7 (range arm): range_deparse escaped
/// bounds into an unceilinged PgVec. C builds range output in a StringInfo,
/// so an over-1GB bound raises "string buffer exceeds maximum allowed
/// length" immediately. Pre-fix this test FAILS because the over-ceiling
/// output succeeds. (~537MB of '"' doubles under bound quoting.)
#[test]
fn range_deparse_over_ceiling_bound_raises_stringinfo_error() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let n = ::mcx::MAX_ALLOC_SIZE / 2 + 16;
    let huge = std::vec![b'"'; n];
    let err = crate::io::range_deparse(mcx, RANGE_LB_INC, Some(&huge), Some(b"x"))
        .expect_err("range output above MaxAllocSize must raise the StringInfo ceiling error");
    assert_eq!(
        err.message(),
        std::format!(
            "string buffer exceeds maximum allowed length ({} bytes)",
            ::mcx::MAX_ALLOC_SIZE
        )
    );
}

/// Task #85 (RELEASE BLOCKER): `SELECT v::text` on a deeply-nested range
/// value (a range whose subtype is another range type) OS-OOM-killed the
/// server. Each nesting level's range_out feeds the previous level's text
/// back through range_deparse, and bound quoting doubles every '"' — so the
/// deparse buffer grows exponentially with depth while nothing enforced C's
/// MaxAllocSize ceiling. C builds this in a StringInfo; enlargeStringInfo
/// raises errcode 54000 (program_limit_exceeded) "string buffer exceeds
/// maximum allowed length (1073741823 bytes)" at the first over-ceiling
/// append. This drives the same iterated amplification: the crossing level
/// must return that exact catchable error. Pre-fix this test FAILS — the
/// over-ceiling level returns Ok after consuming gigabytes.
#[test]
fn range_deparse_nested_amplification_hits_ceiling_catchably() {
    use ::types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED;
    // Level-0 bound: 1 MiB of '"' — forces quoting and per-level doubling,
    // the amplification a nested range ::text cast produces.
    let mut bound = std::vec![b'"'; 1 << 20];
    for _depth in 0..16 {
        let ctx = MemoryContext::new("t85");
        let r = crate::io::range_deparse(ctx.mcx(), RANGE_LB_INC, Some(&bound), Some(b"x"));
        match r {
            Ok(v) => {
                assert!(
                    v.len() <= ::mcx::MAX_ALLOC_SIZE,
                    "over-ceiling nested deparse succeeded ({} bytes)",
                    v.len()
                );
                // Drop the NUL; the output is the next nesting level's bound.
                bound = v[..v.len() - 1].to_vec();
            }
            Err(e) => {
                assert_eq!(e.sqlstate(), ERRCODE_PROGRAM_LIMIT_EXCEEDED);
                assert_eq!(
                    e.message(),
                    std::format!(
                        "string buffer exceeds maximum allowed length ({} bytes)",
                        ::mcx::MAX_ALLOC_SIZE
                    )
                );
                return;
            }
        }
    }
    panic!("nested range deparse never hit the MaxAllocSize ceiling");
}

// pseudotypes.c: anyrange_out/anycompatiblerange_out are `return
// range_out(fcinfo)`; the aliases must resolve to the same fc body.
#[test]
fn pseudotype_aliases_delegate_to_range_out() {
    let by_oid = |oid: types_core::Oid| {
        crate::builtins::RANGETYPES_BUILTINS
            .iter()
            .find(|b| b.foid == oid)
            .unwrap_or_else(|| panic!("oid {oid} not registered"))
    };
    assert_eq!(by_oid(3833).func as usize, crate::builtins::fc_range_out as usize);
    assert_eq!(by_oid(5095).func as usize, crate::builtins::fc_range_out as usize);
    assert_eq!(by_oid(5095).name, "anycompatiblerange_out");
}
