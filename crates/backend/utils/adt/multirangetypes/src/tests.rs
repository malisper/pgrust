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

// P1 sibling regression (fuzz-found 2026-07-31, lane p1-laneac): the same
// unvalidated-wire-length-sized buffer defect as adt_rangetypes range_recv.
// multirange_recv sized its per-element StringInfo from the wire range_len
// before pq_getmsgbytes validated it, so range_len == 0 wrote through a
// zero-capacity PgVec sentinel (release SEGV; debug_assert-masked) and a bogus
// huge length requested a reserve C never attempts. C validates first
// (pq_getmsgbytes), then resets a fixed-size initStringInfo buffer.
mod recv_wire {
    use super::*;
    use crate::io::{multirange_recv, MultirangeIOData};
    use ::types_error::ERRCODE_PROTOCOL_VIOLATION;

    /// range_recv stand-in: consumes the whole element buffer and returns a
    /// serialized int4range image, so the outer loop's wire handling is what
    /// the test exercises.
    fn fc_range_recv(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
        // SAFETY: receive_function_call passes a live StringInfo in arg 0.
        let buf = unsafe { fcinfo.arg_stringinfo(0) };
        let _flags = ::pqformat::pq_getmsgbyte(buf)?;
        let mcx = fcinfo.result_mcx();
        let mut rng = int4_rng();
        let v = mk(mcx, &mut rng, 1, 9);
        Ok(Datum::from_usize(v.leak().as_ptr() as usize))
    }

    fn io_data() -> MultirangeIOData {
        MultirangeIOData {
            mi: MultirangeInfo { pin: None, mltrngtypid: INT4MULTIRANGE, rng: int4_rng() },
            typioproc: FmgrInfo::new(fc_range_recv, 3836, 3, true, false),
            typioparam: INT4RANGE,
        }
    }

    fn recv(mcx: ::mcx::Mcx<'_>, wire: &[u8]) -> PgResult<()> {
        let mut buf = ::stringinfo::StringInfo::new_in(mcx)?;
        buf.append_bytes(wire)?;
        let mut cache = io_data();
        multirange_recv(mcx, &mut cache, &mut buf, -1).map(|_| ())
    }

    #[test]
    fn zero_length_range_errors_and_does_not_crash() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        // range_count = 1, then a zero-length range element.
        let mut wire = std::vec::Vec::new();
        wire.extend_from_slice(&1u32.to_be_bytes());
        wire.extend_from_slice(&0u32.to_be_bytes());
        let e = recv(mcx, &wire).expect_err("zero-length range must be a protocol error");
        assert_eq!(e.sqlstate, ERRCODE_PROTOCOL_VIOLATION);
    }

    #[test]
    fn oversized_range_length_errors_before_allocating() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let mut wire = std::vec::Vec::new();
        wire.extend_from_slice(&1u32.to_be_bytes());
        wire.extend_from_slice(&0xEBFFFFFFu32.to_be_bytes());
        let e = recv(mcx, &wire).expect_err("oversized range length must be a protocol error");
        assert_eq!(e.sqlstate, ERRCODE_PROTOCOL_VIOLATION);
    }

    #[test]
    fn well_formed_element_still_round_trips() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let mut wire = std::vec::Vec::new();
        wire.extend_from_slice(&1u32.to_be_bytes());
        wire.extend_from_slice(&1u32.to_be_bytes());
        wire.push(::adt_rangetypes::RANGE_LB_INC);
        recv(mcx, &wire).expect("well-formed one-element wire must receive");
    }
}

/// Boundary-guard audit findings 5/7 (multirange arm): multirange_out
/// concatenated member-range strings into an unceilinged PgVec. C builds
/// multirange output in a StringInfo, so an over-1GB output raises "string
/// buffer exceeds maximum allowed length" immediately. Pre-fix this test
/// FAILS because the over-ceiling output succeeds. (The stub range out proc
/// returns a ~537MB cstring per member; two members cross 1GB.)
mod out_ceiling {
    use super::*;
    use crate::io::{multirange_out, MultirangeIOData};

    /// range_out stand-in returning a huge NUL-terminated cstring, built in
    /// the caller's result mcx (the fc_mytextin pattern from rowtypes'
    /// tests_ws.rs) so the datum outlives this call without any TLS scratch
    /// (the tree-wide TLS census is textual and pinned; a test thread_local
    /// here would move it).
    fn fc_huge_range_out(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
        let mcx = fcinfo.result_mcx();
        let n = ::mcx::MAX_ALLOC_SIZE / 2 + 16;
        let mut b: PgVec<'_, u8> = ::mcx::vec_with_capacity_in(mcx, n + 1)?;
        b.resize(n, b'x');
        b.push(0);
        let d = Datum::from_usize(b.as_ptr() as usize);
        // Lives until the test's MemoryContext drops.
        core::mem::forget(b);
        Ok(d)
    }

    #[test]
    fn multirange_out_over_ceiling_output_raises_stringinfo_error() {
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let mut cache = MultirangeIOData {
            mi: MultirangeInfo { pin: None, mltrngtypid: INT4MULTIRANGE, rng: int4_rng() },
            typioproc: FmgrInfo::new(fc_huge_range_out, 3840, 1, true, false),
            typioparam: INT4RANGE,
        };
        let mut rng = int4_rng();
        let r1 = mk(mcx, &mut rng, 1, 9);
        let r2 = mk(mcx, &mut rng, 20, 30);
        let mut ranges: PgVec<'_, &[u8]> = ::mcx::vec_with_capacity_in(mcx, 2).unwrap();
        ranges.push(&r1[..]);
        ranges.push(&r2[..]);
        let mr = crate::make_multirange(mcx, INT4MULTIRANGE, &mut cache.mi.rng, &mut ranges)
            .unwrap();
        let err = multirange_out(mcx, &mut cache, &mr).expect_err(
            "multirange output above MaxAllocSize must raise the StringInfo ceiling error",
        );
        assert_eq!(
            err.message(),
            std::format!(
                "string buffer exceeds maximum allowed length ({} bytes)",
                ::mcx::MAX_ALLOC_SIZE
            )
        );
    }
}

// pseudotypes.c: anymultirange_out/anycompatiblemultirange_out are `return
// multirange_out(fcinfo)`; the aliases must resolve to the same fc body.
#[test]
fn pseudotype_aliases_delegate_to_multirange_out() {
    let by_oid = |oid: types_core::Oid| {
        crate::builtins::MULTIRANGETYPES_BUILTINS
            .iter()
            .find(|b| b.foid == oid)
            .unwrap_or_else(|| panic!("oid {oid} not registered"))
    };
    assert_eq!(by_oid(4230).func as usize, crate::builtins::fc_multirange_out as usize);
    assert_eq!(by_oid(4227).func as usize, crate::builtins::fc_multirange_out as usize);
    assert_eq!(by_oid(4227).name, "anycompatiblemultirange_out");
}
