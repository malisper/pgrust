use ::datum::Datum;
use ::fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData, LocalFcinfo, TRACK_FUNC_ALL};
use ::types_core::{primitive::InvalidOid, Oid};
use ::types_error::PgResult;

use crate::*;

fn int4pl_body(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    Ok(Datum::from_i32(fcinfo.arg_i32(0) + fcinfo.arg_i32(1)))
}

#[test]
fn table_matches_canonical_and_is_sorted() {
    assert_eq!(FMGR_BUILTINS.len(), CANONICAL.len());
    assert_eq!(FMGR_BUILTINS.len(), 3102);
    assert_eq!(FMGR_BUILTINS[FMGR_BUILTINS.len() - 1].foid, FMGR_LAST_BUILTIN_OID);
    for (i, (b, c)) in FMGR_BUILTINS.iter().zip(CANONICAL.iter()).enumerate() {
        assert_eq!((b.foid, b.name, b.nargs, b.strict, b.retset), *c);
        if i > 0 {
            assert!(FMGR_BUILTINS[i - 1].foid < b.foid);
        }
    }
}

// SDK-matrix wirefmt lane: binary send/recv functions clients reach through
// pg_type's typsend/typreceive must resolve to real ports, not the
// not-yet-implemented stub (matrix gaps M2/M4/M5/M8/M10, 2026-07-18).
#[test]
fn binary_wire_send_recv_holes_stay_ported() {
    for (oid, name) in [
        (198 as Oid, "pg_node_tree_send"),
        (2410, "int2vectorrecv"),
        (2411, "int2vectorsend"),
        (2416, "unknownrecv"),
        (2417, "unknownsend"),
        (2492, "cash_recv"),
        (2493, "cash_send"),
        (3121, "void_send"),
    ] {
        let b = fmgr_isbuiltin(oid).unwrap_or_else(|| panic!("{name} missing from canonical"));
        assert_eq!(b.name, name);
        assert!(
            b.func as usize != builtin_not_ported as usize,
            "{name} (OID {oid}) resolves to the not-ported stub"
        );
    }
}

#[test]
fn oid_index_round_trips_every_entry() {
    for b in FMGR_BUILTINS.iter() {
        let hit = fmgr_isbuiltin(b.foid).unwrap();
        assert!(core::ptr::eq(hit, b));
    }
}

#[test]
fn isbuiltin_misses_match_c() {
    assert!(fmgr_isbuiltin(InvalidOid).is_none());
    assert!(fmgr_isbuiltin(58).is_none());
    assert!(fmgr_isbuiltin(FMGR_LAST_BUILTIN_OID + 1).is_none());
    assert!(fmgr_isbuiltin(u32::MAX).is_none());
    assert!(fmgr_isbuiltin(6411).is_none());
}

#[test]
fn known_builtin_metadata() {
    let b = fmgr_isbuiltin(177).unwrap();
    assert_eq!((b.name, b.nargs, b.strict, b.retset), ("int4pl", 2, true, false));
    let b = fmgr_isbuiltin(6430).unwrap();
    assert_eq!(b.name, "uuidv7_interval");
    let b = fmgr_isbuiltin(3).unwrap();
    assert_eq!(b.name, "heap_tableam_handler");
    let b = fmgr_isbuiltin(6401).unwrap();
    assert!(b.retset && b.strict && b.nargs == 0);
}

#[test]
fn fmgr_info_builtin_fast_path() {
    let f = fmgr_info(177).unwrap();
    assert_eq!(f.fn_oid, 177);
    assert_eq!(f.fn_nargs, 2);
    assert!(f.fn_strict);
    assert!(!f.fn_retset);
    assert_eq!(f.fn_stats, TRACK_FUNC_ALL);
    assert!(f.fn_extra.is_none());
    assert!(f.fn_expr.is_none());
}

#[test]
fn fmgr_info_into_refills_carrier() {
    let mut f = FmgrInfo::unresolved();
    fmgr_info_into(177, &mut f).unwrap();
    assert_eq!((f.fn_oid, f.fn_nargs, f.fn_strict, f.fn_retset), (177, 2, true, false));
    f.set_fn_extra(41i32);
    fmgr_info_into(65, &mut f).unwrap();
    assert_eq!((f.fn_oid, f.fn_nargs), (65, 2));
    assert!(f.fn_extra.is_none());
    assert!(f.fn_expr.is_none());
    assert_eq!(f.fn_stats, TRACK_FUNC_ALL);
}

#[test]
#[should_panic(expected = "seam not installed")]
fn fmgr_info_non_builtin_reads_pg_proc() {
    let _ = fmgr_info(16384);
}

#[test]
fn unported_builtin_invocation_is_clean_feature_error() {
    // Any still-unported canonical entry; a pinned oid goes stale the moment
    // some lane ports it (1294 did).
    let b = FMGR_BUILTINS
        .iter()
        .find(|b| {
            b.func as usize == builtin_not_ported as usize
                && late_builtin(b.foid).is_none()
                && extra_builtin(b.foid).is_none()
        })
        .expect("no unported canonical builtin left");
    let (oid, name) = (b.foid, b.name);
    let mut f = fmgr_info(oid).unwrap();
    let mut fci = LocalFcinfo::<0>::new(InvalidOid);
    let err = f.invoke(&mut fci).unwrap_err();
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
    assert!(err.message().contains(name), "{}", err.message());
    assert!(err.message().contains("not yet implemented"), "{}", err.message());
}

#[test]
fn ported_builtin_invokes() {
    let mut f = fmgr_info(177).unwrap();
    let mut fci = LocalFcinfo::<2>::new(InvalidOid);
    fci.set_arg(0, Datum::from_i32(40));
    fci.set_arg(1, Datum::from_i32(2));
    assert_eq!(f.invoke(&mut fci).unwrap().as_i32(), 42);
    let mut eq = fmgr_info(65).unwrap();
    let mut fci = LocalFcinfo::<2>::new(InvalidOid);
    fci.set_arg(0, Datum::from_i32(7));
    fci.set_arg(1, Datum::from_i32(7));
    assert!(eq.invoke(&mut fci).unwrap().as_bool());
}

#[test]
fn internal_function_lookup() {
    assert_eq!(fmgr_internal_function("int4pl"), 177);
    assert_eq!(fmgr_internal_function("uuidv7"), 6429);
    assert_eq!(fmgr_internal_function("no_such_function"), InvalidOid);
    assert_eq!(fmgr_internal_function(""), InvalidOid);
}

#[test]
#[should_panic(expected = "seam not installed")]
fn oid_function_call_non_builtin_reads_pg_proc() {
    let _ = oid_function_call2_coll(16385, InvalidOid, Datum::from_i32(1), Datum::from_i32(2));
}

const TEST_ENTRIES: &[FmgrBuiltin] = &[
    FmgrBuiltin { foid: 65, name: "int4eq", nargs: 2, strict: true, retset: false, func: int4pl_body },
    FmgrBuiltin { foid: 177, name: "int4pl", nargs: 2, strict: true, retset: false, func: int4pl_body },
];
const TEST_INDEX: BuiltinOidIndex<FMGR_OID_INDEX_SIZE> = BuiltinOidIndex::build(TEST_ENTRIES);

#[test]
fn generic_table_resolve_and_call() {
    let fbp = TEST_INDEX.lookup(TEST_ENTRIES, 177).unwrap();
    let mut flinfo = fmgr_info_from_builtin(fbp, 177);
    let r = function_call2_coll(&mut flinfo, InvalidOid, Datum::from_i32(40), Datum::from_i32(2));
    assert_eq!(r.unwrap().as_i32(), 42);
    assert!(TEST_INDEX.lookup(TEST_ENTRIES, 66).is_none());
    assert!(TEST_INDEX.lookup(TEST_ENTRIES, 0).is_none());
}

#[test]
fn resolve_once_carrier_reuse() {
    let fbp = TEST_INDEX.lookup(TEST_ENTRIES, 65).unwrap();
    let mut flinfo = fmgr_info_from_builtin(fbp, 65);
    for i in 0..100i32 {
        let r = function_call2_coll(&mut flinfo, InvalidOid, Datum::from_i32(i), Datum::from_i32(1));
        assert_eq!(r.unwrap().as_i32(), i + 1);
    }
    assert_eq!(flinfo.fn_oid, 65);
}

#[test]
fn merged_family_builtins_invoke() {
    let mut f = fmgr_info(60).unwrap();
    assert_eq!(f.fn_oid, 60);
    let mut fci = LocalFcinfo::<2>::new(InvalidOid);
    fci.set_arg(0, Datum::from_bool(true));
    fci.set_arg(1, Datum::from_bool(true));
    assert!(f.invoke(&mut fci).unwrap().as_bool());

    let mut f = fmgr_info(463).unwrap();
    let mut fci = LocalFcinfo::<2>::new(InvalidOid);
    fci.set_arg(0, Datum::from_i64(40));
    fci.set_arg(1, Datum::from_i64(2));
    assert_eq!(f.invoke(&mut fci).unwrap().as_i64(), 42);

    let mut f = fmgr_info(218).unwrap();
    let mut fci = LocalFcinfo::<2>::new(InvalidOid);
    fci.set_arg(0, Datum::from_f64(1.5));
    fci.set_arg(1, Datum::from_f64(2.25));
    assert_eq!(f.invoke(&mut fci).unwrap().as_f64(), 3.75);
}

#[test]
fn merged_family_overlay_covers_all_tables() {
    for name in ["booleq", "float8pl", "int4pl", "int8pl", "nameeq", "texteq", "textout"] {
        let oid = fmgr_internal_function(name);
        assert!(
            ported::PORTED.iter().any(|(o, _)| *o == oid),
            "{name} ({oid}) missing from the merged PORTED overlay"
        );
    }
}

#[test]
fn init_seams_installs_fmgr_info() {
    init_seams();
    let f = fmgr_seams::fmgr_info::call(177).unwrap();
    assert_eq!((f.fn_oid, f.fn_nargs), (177, 2));
}

#[test]
fn ported_overlay_is_subset_of_canonical() {
    for (oid, _) in ported::PORTED.iter() {
        assert!(fmgr_isbuiltin(*oid).is_some());
    }
}

// Result-mcx convention, end-to-end through fmgr_info-resolved carriers
// (notes/fc-result-convention.md).
mod result_convention {
    use super::*;
    use ::datum::VarlenaRef;
    use ::mcx::MemoryContext;
    use alloc::vec::Vec;

    fn text_image(s: &[u8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(4 + s.len());
        v.extend_from_slice(&::datum::varlena::set_varsize_4b(4 + s.len()));
        v.extend_from_slice(s);
        v
    }

    fn call2_in(oid: Oid, coll: Oid, ctx: &MemoryContext, a: Datum, b: Datum) -> Datum {
        let mut flinfo = fmgr_info(oid).unwrap();
        let mut fci = LocalFcinfo::<2>::fresh(coll);
        // SAFETY: ctx outlives the single call below.
        unsafe { fci.set_result_mcx(ctx.mcx()) };
        fci.set_arg(0, a);
        fci.set_arg(1, b);
        let d = flinfo.invoke(&mut fci).unwrap();
        assert!(!fci.isnull);
        d
    }

    #[test]
    fn textcat_allocates_in_armed_context_and_reset_frees() {
        let mut ctx = MemoryContext::new_bump("textcat");
        let a = text_image(b"hello");
        let b = text_image(b"world");
        let before = ctx.used();
        let d = call2_in(
            1258,
            InvalidOid,
            &ctx,
            Datum::from_usize(a.as_ptr() as usize),
            Datum::from_usize(b.as_ptr() as usize),
        );
        // SAFETY: textcat result is a live 4B-header varlena in ctx.
        let r = unsafe { VarlenaRef::from_ptr(d.as_usize() as *const u8) };
        assert_eq!(r.data(), b"helloworld");
        assert!(ctx.used() > before);
        // Reset reclaims the results: repeated call+reset cycles never grow
        // the arena past the keeper block it retains.
        ctx.reset();
        let keeper = ctx.used();
        for _ in 0..64 {
            let d = call2_in(
                1258,
                InvalidOid,
                &ctx,
                Datum::from_usize(a.as_ptr() as usize),
                Datum::from_usize(b.as_ptr() as usize),
            );
            // SAFETY: as above.
            let r = unsafe { VarlenaRef::from_ptr(d.as_usize() as *const u8) };
            assert_eq!(r.data(), b"helloworld");
            ctx.reset();
        }
        assert_eq!(ctx.used(), keeper);
    }

    #[test]
    fn lower_through_fmgr_info() {
        let ctx = MemoryContext::new_bump("lower");
        let mut flinfo = fmgr_info(870).unwrap();
        assert!(flinfo.fn_strict);
        let arg = text_image(b"MiXeD");
        let mut fci = LocalFcinfo::<1>::fresh(::types_core::C_COLLATION_OID);
        // SAFETY: ctx outlives the single call below.
        unsafe { fci.set_result_mcx(ctx.mcx()) };
        fci.set_arg(0, Datum::from_usize(arg.as_ptr() as usize));
        let d = flinfo.invoke(&mut fci).unwrap();
        // SAFETY: lower result is a live 4B-header varlena in ctx.
        let r = unsafe { VarlenaRef::from_ptr(d.as_usize() as *const u8) };
        assert_eq!(r.data(), b"mixed");
    }

    #[test]
    fn numeric_add_result_owned_by_context() {
        let mut ctx = MemoryContext::new_bump("numeric");
        let a = ::adt_numeric::int64_to_numeric(20);
        let b = ::adt_numeric::int64_to_numeric(22);
        let d = call2_in(
            1724,
            InvalidOid,
            &ctx,
            Datum::from_usize(a.as_bytes().as_ptr() as usize),
            Datum::from_usize(b.as_bytes().as_ptr() as usize),
        );
        let p = d.as_usize() as *const u8;
        assert_eq!(p as usize % 8, 0);
        // SAFETY: numeric_add result is a live 4B-header varlena in ctx.
        let r = unsafe { VarlenaRef::from_ptr(p) };
        let want = ::adt_numeric::int64_to_numeric(42);
        assert_eq!(r.as_bytes(), want.as_bytes());
        ctx.reset();
        let keeper = ctx.used();
        for _ in 0..64 {
            let _ = call2_in(
                1724,
                InvalidOid,
                &ctx,
                Datum::from_usize(a.as_bytes().as_ptr() as usize),
                Datum::from_usize(b.as_bytes().as_ptr() as usize),
            );
            ctx.reset();
        }
        assert_eq!(ctx.used(), keeper);
    }

    #[test]
    fn numeric_cmp_needs_no_arming() {
        let a = ::adt_numeric::int64_to_numeric(7);
        let b = ::adt_numeric::int64_to_numeric(9);
        let mut flinfo = fmgr_info(1769).unwrap();
        let d = function_call2_coll(
            &mut flinfo,
            InvalidOid,
            Datum::from_usize(a.as_bytes().as_ptr() as usize),
            Datum::from_usize(b.as_bytes().as_ptr() as usize),
        )
        .unwrap();
        assert_eq!(d.as_i32(), -1);
    }

    #[test]
    #[should_panic(expected = "never armed")]
    fn varlena_result_without_arming_panics() {
        let a = text_image(b"x");
        let b = text_image(b"y");
        let mut flinfo = fmgr_info(1258).unwrap();
        let _ = function_call2_coll(
            &mut flinfo,
            InvalidOid,
            Datum::from_usize(a.as_ptr() as usize),
            Datum::from_usize(b.as_ptr() as usize),
        );
    }
}

#[test]
fn error_save_context_tag_matches_nodetags() {
    assert_eq!(
        ::fmgr::T_ERROR_SAVE_CONTEXT,
        ::nodes::NodeTag::T_ErrorSaveContext as u32
    );
}

#[test]
fn agg_state_tag_matches_nodetags() {
    assert_eq!(::fmgr::T_AGG_STATE, ::nodes::NodeTag::T_AggState as u32);
}

#[test]
fn call_context_tag_matches_nodetags() {
    assert_eq!(::fmgr::T_CALL_CONTEXT, ::nodes::NodeTag::T_CallContext as u32);
}

#[test]
fn input_function_call_safe_over_resolved_int4in() {
    let ctx = ::mcx::MemoryContext::new_bump("ifcs-test");
    let mut flinfo = fmgr_info(42).unwrap();
    let mut result = Datum::null();

    assert!(input_function_call_safe(
        &mut flinfo, Some(c"1234"), 0, -1, ctx.mcx(), None, &mut result
    )
    .unwrap());
    assert_eq!(result.as_i32(), 1234);

    let mut esc = ErrorSaveNode::new(true);
    assert!(!input_function_call_safe(
        &mut flinfo, Some(c"not-an-int"), 0, -1, ctx.mcx(), Some(&mut esc), &mut result
    )
    .unwrap());
    assert!(esc.ctx.error_occurred());
    let saved = esc.ctx.error().unwrap();
    assert_eq!(saved.sqlstate(), ::types_error::ERRCODE_INVALID_TEXT_REPRESENTATION);

    let err = input_function_call_safe(
        &mut flinfo, Some(c"not-an-int"), 0, -1, ctx.mcx(), None, &mut result
    )
    .unwrap_err();
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_INVALID_TEXT_REPRESENTATION);
}

// Binary wire round-trip through the registered recv/send builtins:
// Datum -> send fc -> bytea -> recv fc -> Datum, exercising the actual OID
// registration (a not-ported OID would panic in builtin_not_ported).
mod wire_round_trip {
    use ::datum::varlena::VarlenaRef;
    use ::datum::{Datum, Varlena};
    use ::fmgr::{receive_function_call, send_function_call};
    use ::mcx::{Mcx, MemoryContext, PgVec};
    use ::stringinfo::StringInfo;

    use crate::fmgr_info;

    // send oid, then recv oid; runs the datum both ways.
    fn round_trip_byval(mcx: Mcx<'_>, send_oid: u32, recv_oid: u32, d: Datum) -> Datum {
        let mut send = fmgr_info(send_oid).unwrap();
        let out = send_function_call(&mut send, d, mcx).unwrap();
        // SAFETY: send builtins return a 4B-header bytea image.
        let payload = unsafe { VarlenaRef::from_ptr(out.as_usize() as *const u8) }.data();
        let mut buf = StringInfo::with_capacity_in(mcx, payload.len() + 1).unwrap();
        buf.append_bytes(payload).unwrap();
        let mut recv = fmgr_info(recv_oid).unwrap();
        let back = receive_function_call(&mut recv, Some(&mut buf), 0, -1, mcx).unwrap();
        assert_eq!(buf.cursor, buf.len(), "recv must consume the whole buffer");
        back
    }

    fn text_datum(mcx: Mcx<'_>, s: &[u8]) -> Datum {
        let mut img: PgVec<u8> = PgVec::new_in(mcx);
        img.try_reserve_exact(4 + s.len()).unwrap();
        img.extend_from_slice(&[0u8; 4]);
        img.extend_from_slice(s);
        let v = Varlena::from_image(img);
        let d = Datum::from_usize(v.as_bytes().as_ptr() as usize);
        core::mem::forget(v);
        d
    }

    #[test]
    fn byval_types_round_trip_bit_identical() {
        let ctx = MemoryContext::new("wire-rt");
        let mcx = ctx.mcx();

        assert_eq!(round_trip_byval(mcx, 2437, 2436, Datum::from_bool(true)).as_bool(), true);
        assert_eq!(round_trip_byval(mcx, 2437, 2436, Datum::from_bool(false)).as_bool(), false);

        for v in [0i16, -1, 12345, i16::MIN, i16::MAX] {
            assert_eq!(round_trip_byval(mcx, 2405, 2404, Datum::from_i16(v)).as_i16(), v);
        }
        for v in [0i32, -123456789, i32::MIN, i32::MAX] {
            assert_eq!(round_trip_byval(mcx, 2407, 2406, Datum::from_i32(v)).as_i32(), v);
        }
        for v in [0i64, -1234567890123, i64::MIN, i64::MAX] {
            assert_eq!(round_trip_byval(mcx, 2409, 2408, Datum::from_i64(v)).as_i64(), v);
        }
        for v in [0.0f32, -3.5, f32::MIN, f32::MAX] {
            assert_eq!(round_trip_byval(mcx, 2425, 2424, Datum::from_f32(v)).as_f32(), v);
        }
        let nan = round_trip_byval(mcx, 2425, 2424, Datum::from_f32(f32::NAN));
        assert!(nan.as_f32().is_nan());
        for v in [0.0f64, 2.718281828459045, f64::MIN, f64::MAX] {
            assert_eq!(round_trip_byval(mcx, 2427, 2426, Datum::from_f64(v)).as_f64(), v);
        }
    }

    // bytea recv/send take no encoding conversion (text recv/send route through
    // the mbutils server<->client seam, which the extended-query byte-trace
    // integration exercises with the seam installed).
    #[test]
    fn bytea_round_trip() {
        let ctx = MemoryContext::new("wire-rt-varlena");
        let mcx = ctx.mcx();
        for s in [&b""[..], b"hello", b"binary\x00wire\xff"] {
            let d = text_datum(mcx, s);
            let back = round_trip_byval(mcx, 2413, 2412, d);
            // SAFETY: recv returns a 4B-header bytea varlena image.
            let got = unsafe { VarlenaRef::from_ptr(back.as_usize() as *const u8) }.data();
            assert_eq!(got, s, "bytea recv/send payload mismatch");
        }
    }
}

#[test]
fn thin_tables_sorted_and_refereed() {
    for t in THIN_TABLES {
        for w in t.windows(2) {
            assert!(w[0].foid < w[1].foid, "thin table not oid-ascending at {}", w[1].foid);
        }
        for e in *t {
            let b = fmgr_isbuiltin(e.foid).expect("thin row without builtin row");
            assert_eq!(b.nargs, e.nargs, "thin arity mismatch ({})", e.foid);
            assert_eq!(b.func as usize, e.func as usize, "thin referee mismatch ({})", e.foid);
        }
    }
    let f = fmgr_info(177).unwrap();
    assert!(fmgr_thin_builtin(&f, 2).is_some());
    assert!(fmgr_thin_builtin(&f, 1).is_none(), "arity mismatch must not get a thin twin");
    let mut g = fmgr_info(177).unwrap();
    g.fn_addr = int4pl_body;
    assert!(fmgr_thin_builtin(&g, 2).is_none(), "diverging fn_addr must not get a thin twin");
    assert!(fmgr_thin_builtin(&fmgr_info(65).unwrap(), 2).is_some());
    assert!(fmgr_thin_builtin(&fmgr_info(1219).unwrap(), 1).is_some(), "int8inc thin row");
}

#[test]
fn thin_twin_matches_wrapper() {
    for (oid, a, b) in [(177u32, 40i32, 2i32), (65, 7, 7), (66, -3, 4), (154, 40, 8)] {
        let mut f = fmgr_info(oid).unwrap();
        let thin = fmgr_thin_builtin(&f, 2).unwrap();
        let mut fcinfo = LocalFcinfo::<2>::fresh(0);
        fcinfo.set_arg(0, Datum::from_i32(a));
        fcinfo.set_arg(1, Datum::from_i32(b));
        let want = f.invoke(&mut fcinfo).unwrap();
        assert!(!fcinfo.isnull);
        // SAFETY: live 2-arg image; thin contract per the registry.
        let got = unsafe { thin(core::ptr::NonNull::from(&mut fcinfo).cast()) }.unwrap();
        assert_eq!(want.as_usize(), got.as_usize(), "oid {oid}");
    }
    // Error surface: int4pl overflow through both ABIs.
    let mut f = fmgr_info(177).unwrap();
    let thin = fmgr_thin_builtin(&f, 2).unwrap();
    let mut fcinfo = LocalFcinfo::<2>::fresh(0);
    fcinfo.set_arg(0, Datum::from_i32(i32::MAX));
    fcinfo.set_arg(1, Datum::from_i32(1));
    let e1 = f.invoke(&mut fcinfo).unwrap_err();
    // SAFETY: as above.
    let e2 = unsafe { thin(core::ptr::NonNull::from(&mut fcinfo).cast()) }.unwrap_err();
    assert_eq!(e1.sqlstate(), e2.sqlstate());
}
