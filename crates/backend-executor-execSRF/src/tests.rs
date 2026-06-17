//! Drive `ExecMakeTableFunctionResult` over a constructed value-per-call SRF
//! (`generate_series(1,3)`-shaped) and prove the executor-frame dispatch yields
//! THREE rows {1, 2, 3} into the result tuplestore — the #349 K2 milestone.
//!
//! The SRF body is the canonical `generate_series_step_int4` value-per-call
//! protocol (mirroring `funcapi::srf_support`'s proven proof-of-life), wrapped
//! as a [`types_nodes::execexpr::PGFunction`] (the executor-frame ABI whose
//! frame carries the LIVE `ReturnSetInfo`) and registered in this unit's
//! executor-frame SRF table. The tuplestore is a recording mock (the nodeMaterial
//! test pattern) so the produced scalar series is observable without the slot /
//! MinimalTuple decode stack.

use core::any::Any;
use std::sync::Once;

use mcx::{Mcx, MemoryContext, PgBox};
use types_datum::NullableDatum;
use types_nodes::execexpr::{ExprDoneCond, SetExprState};
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_nodes::funcapi::FuncCallContext;
use types_nodes::primnodes::{Expr, FuncExpr};
use types_nodes::{EStateData, ExprContext};
use types_tuple::backend_access_common_heaptuple::Datum;

use super::*;

const INT4OID: types_core::Oid = 23;
/// A test SRF OID (outside the registered-builtin range; the executor-frame SRF
/// table is keyed independently of the by-OID builtin registry).
const TEST_SRF_OID: types_core::Oid = 1_000_001;

static INSTALL: Once = Once::new();

/// A recording tuplestore: every `tuplestore_putvalues` appends the scalar int4
/// of column 1 (and its null flag). The nodeMaterial-test mock-store pattern.
#[derive(Default)]
struct MockStore {
    rows: alloc::vec::Vec<(i32, bool)>,
}

fn install() {
    // tuplestore_begin_heap → a carrier wrapping a fresh MockStore.
    backend_utils_sort_storage_seams::tuplestore_begin_heap::set(
        |mcx, _random, _interxact, _kb| {
            Ok(mcx::alloc_in(
                mcx,
                Tuplestorestate::begin(mcx, MockStore::default())?,
            )?)
        },
    );
    // tuplestore_putvalues → record (value, isnull) of column 1.
    backend_utils_sort_storage_seams::tuplestore_putvalues::set(|state, _tdesc, values, nulls| {
        let store = state
            .payload_mut()
            .expect("MockStore payload present")
            .downcast_mut::<MockStore>()
            .expect("payload is MockStore");
        let v = values.first().map(|d| d.as_i32()).unwrap_or(0);
        let n = nulls.first().copied().unwrap_or(false);
        store.rows.push((v, n));
        Ok(())
    });

    // type_is_rowtype(INT4) → false (scalar return → the loop builds a 1-col
    // descriptor and stores the scalar result directly).
    backend_utils_cache_lsyscache_seams::type_is_rowtype::set(|_typid| Ok(false));

    // TupleDescInitEntry looks up the column type's metadata; return int4's
    // (typlen=4, by-value, 'i' align, 'p' storage).
    backend_utils_cache_syscache_seams::search_type_attr_info::set(|_oid| {
        Ok(Some(
            types_tuple::backend_access_common_tupdesc::PgTypeInfo {
                typlen: 4,
                typbyval: true,
                typalign: b'i' as i8,
                typstorage: b'p' as i8,
                typcollation: 0,
            },
        ))
    });

    // work_mem default (KB).
    backend_utils_init_small_seams::work_mem::set(|| 4096);

    // Register the executor-frame SRF under the test OID.
    register_srf(TEST_SRF_OID, generate_series_step_int4);
    // Register the real int4/int8 generate_series builtins (the SRF
    // registrations init_seams does) so the real-builtin test dispatches OID 1067.
    crate::generate_series::register_generate_series();
}

fn setup() {
    INSTALL.call_once(install);
}

/// `generate_series_step_int4(PG_FUNCTION_ARGS)` (int.c:1537) as an
/// executor-frame `PGFunction`. Reads its two int4 args from the call frame,
/// drives the value-per-call protocol over the `fn_extra` cross-call channel,
/// and writes `isDone` onto the LIVE `ReturnSetInfo` each call.
fn generate_series_step_int4<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> Datum<'mcx> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("generate_series: fn_mcxt set by ExecMakeTableFunctionResult");

    // C: if (SRF_IS_FIRSTCALL()) { funcctx = SRF_FIRSTCALL_INIT(); fctx = ...; }
    if fcinfo.fn_extra.is_none() {
        let start = fcinfo.args[0].value.as_i32();
        let finish = fcinfo.args[1].value.as_i32();
        let step = if fcinfo.nargs == 3 {
            fcinfo.args[2].value.as_i32()
        } else {
            1
        };
        assert!(step != 0, "test uses non-zero step");
        backend_utils_fmgr_funcapi::srf_support::init_MultiFuncCall(fcinfo)
            .expect("init_MultiFuncCall");
        let fctx = erase_user_fctx(
            mcx,
            GenerateSeriesFctx {
                current: start,
                finish,
                step,
            },
        );
        let funcctx = backend_utils_fmgr_funcapi::srf_support::per_MultiFuncCall(fcinfo)
            .expect("per_MultiFuncCall");
        funcctx.user_fctx = Some(fctx);
    }

    // C: funcctx = SRF_PERCALL_SETUP(); fctx = funcctx->user_fctx;
    let funcctx: &mut FuncCallContext<'mcx> =
        backend_utils_fmgr_funcapi::srf_support::per_MultiFuncCall(fcinfo)
            .expect("per_MultiFuncCall");
    let fctx: &mut GenerateSeriesFctx = funcctx
        .user_fctx
        .as_mut()
        .expect("user_fctx present")
        .downcast_mut::<GenerateSeriesFctx>()
        .expect("user_fctx is GenerateSeriesFctx");
    let result = fctx.current;

    let in_range = (fctx.step > 0 && fctx.current <= fctx.finish)
        || (fctx.step < 0 && fctx.current >= fctx.finish);

    if in_range {
        match fctx.current.checked_add(fctx.step) {
            Some(next) => fctx.current = next,
            None => fctx.step = 0,
        }
        funcctx.call_cntr += 1;
        // SRF_RETURN_NEXT: rsi->isDone = ExprMultipleResult; return Datum.
        fcinfo
            .resultinfo
            .as_mut()
            .expect("resultinfo present for SRF call")
            .isDone = ExprDoneCond::ExprMultipleResult;
        fcinfo.isnull = false;
        Datum::from_i32(result)
    } else {
        // SRF_RETURN_DONE: end_MultiFuncCall + rsi->isDone = ExprEndResult.
        backend_utils_fmgr_funcapi::srf_support::end_MultiFuncCall(fcinfo)
            .expect("end_MultiFuncCall");
        fcinfo
            .resultinfo
            .as_mut()
            .expect("resultinfo present for SRF call")
            .isDone = ExprDoneCond::ExprEndResult;
        fcinfo.isnull = true;
        Datum::from_i32(0)
    }
}

#[derive(Debug)]
struct GenerateSeriesFctx {
    current: i32,
    finish: i32,
    step: i32,
}

fn erase_user_fctx<'mcx, T: Any>(mcx: Mcx<'mcx>, v: T) -> PgBox<'mcx, dyn Any> {
    let boxed = mcx::alloc_in(mcx, v).expect("alloc user_fctx");
    let (ptr, alloc) = PgBox::into_raw_with_allocator(boxed);
    // SAFETY: `ptr`/`alloc` came from `into_raw_with_allocator`; the cast only
    // attaches the `dyn Any` vtable.
    unsafe { PgBox::from_raw_in(ptr as *mut dyn Any, alloc) }
}

/// Build the per-node ExprContext in the EState pool and return its id.
fn push_econtext<'mcx>(estate: &mut EStateData<'mcx>) -> types_nodes::EcxtId {
    let per_query = estate.es_query_cxt;
    let per_tuple = per_query.context().new_child("per-tuple");
    let ecxt = ExprContext {
        ecxt_scantuple: None,
        ecxt_innertuple: None,
        ecxt_outertuple: None,
        ecxt_oldtuple: None,
        ecxt_newtuple: None,
        ecxt_per_query_memory: per_query,
        ecxt_per_tuple_memory: per_tuple,
        ecxt_aggvalues: mcx::PgVec::new_in(per_query),
        ecxt_aggnulls: mcx::PgVec::new_in(per_query),
        caseValue_datum: Datum::default(),
        caseValue_isNull: false,
        domainValue_datum: Datum::default(),
        domainValue_isNull: false,
        ecxt_callbacks: None,
    };
    estate.add_expr_context(ecxt).expect("add ExprContext")
}

/// A `FuncExpr` returning `setof int4` (the SRF call expression the SetExprState
/// wraps). The args are empty here so `ExecEvalFuncArgs` is a no-op; the test
/// pre-populates the call frame's args directly (the value-per-call loop and the
/// SRF body are what is under test, not arg compilation).
fn srf_funcexpr() -> Expr {
    Expr::FuncExpr(FuncExpr {
        funcid: TEST_SRF_OID,
        funcresulttype: INT4OID,
        funcretset: true,
        funcvariadic: false,
        funcformat: Default::default(),
        funccollid: 0,
        inputcollid: 0,
        args: alloc::vec::Vec::new(),
        location: -1,
    })
}

#[test]
fn make_table_function_result_yields_three_rows() {
    setup();
    let ctx = MemoryContext::new("execSRF per-query");
    let mcx = ctx.mcx();
    let mut estate = EStateData::new_in(mcx);
    let econtext = push_econtext(&mut estate);

    // Build the SetExprState by hand: the SRF call, funcReturnsSet=true, a call
    // frame pre-loaded with args [1, 3] (so the no-op ExecEvalFuncArgs leaves
    // them in place), fn_oid = the registered test SRF.
    let mut setexpr = SetExprState::default();
    setexpr.funcReturnsSet = true;
    setexpr.expr = Some(mcx::alloc_in(mcx, srf_funcexpr()).unwrap());
    setexpr.func.fn_oid = TEST_SRF_OID;
    setexpr.func.fn_retset = true;
    setexpr.func.fn_strict = false;
    // args list empty → ExecEvalFuncArgs no-op.
    setexpr.args = Some(mcx::PgVec::new_in(mcx));
    // Call frame with two int4 args = generate_series(1, 3).
    let fcinfo = FunctionCallInfoBaseData {
        flinfo: Some(setexpr.func.clone()),
        nargs: 2,
        args: alloc::vec![
            NullableDatum::value(types_datum::Datum::from_i32(1)),
            NullableDatum::value(types_datum::Datum::from_i32(3)),
        ],
        ..Default::default()
    };
    setexpr.fcinfo = Some(mcx::alloc_in(mcx, fcinfo).unwrap());

    // A 1-column expected descriptor (int4).
    let expected = backend_access_common_tupdesc::CreateTemplateTupleDesc(mcx, 1).unwrap();
    let mut expected = mcx::alloc_in(mcx, expected).unwrap();
    backend_access_common_tupdesc::TupleDescInitEntry(&mut expected, 1, Some("g"), INT4OID, -1, 0)
        .unwrap();

    let mut arg_ctx = ctx.mcx().context().new_child("argcontext");

    let tstore = ExecMakeTableFunctionResult(
        &mut setexpr,
        econtext,
        &mut arg_ctx,
        &expected,
        false,
        &mut estate,
    )
    .expect("ExecMakeTableFunctionResult");

    // Inspect the recording mock store: exactly the rows {1, 2, 3}, none null.
    let store = tstore
        .payload()
        .expect("setResult is a live tuplestore")
        .downcast_ref::<MockStore>()
        .expect("payload is MockStore");
    let vals: alloc::vec::Vec<i32> = store.rows.iter().map(|&(v, _n)| v).collect();
    assert_eq!(vals, alloc::vec![1, 2, 3], "value-per-call SRF produced 3 rows");
    assert!(
        store.rows.iter().all(|&(_v, n)| !n),
        "no NULL rows for a producing scalar SRF"
    );
}

#[test]
fn real_generate_series_int4_yields_three_rows() {
    // Drive the ACTUAL registered generate_series_int4 (pg_proc OID 1067), the
    // builtin `SELECT * FROM generate_series(1,3)` resolves to, through
    // ExecMakeTableFunctionResult — proving the real SRF (not a test stub)
    // produces {1, 2, 3} via the executor-frame dispatch.
    setup();
    let ctx = MemoryContext::new("execSRF gs per-query");
    let mcx = ctx.mcx();
    let mut estate = EStateData::new_in(mcx);
    let econtext = push_econtext(&mut estate);

    const GENERATE_SERIES_INT4: types_core::Oid = 1067;

    let mut setexpr = SetExprState::default();
    setexpr.funcReturnsSet = true;
    setexpr.expr = Some(mcx::alloc_in(
        mcx,
        Expr::FuncExpr(FuncExpr {
            funcid: GENERATE_SERIES_INT4,
            funcresulttype: INT4OID,
            funcretset: true,
            funcvariadic: false,
            funcformat: Default::default(),
            funccollid: 0,
            inputcollid: 0,
            args: alloc::vec::Vec::new(),
            location: -1,
        }),
    )
    .unwrap());
    setexpr.func.fn_oid = GENERATE_SERIES_INT4;
    setexpr.func.fn_retset = true;
    setexpr.func.fn_strict = false;
    setexpr.args = Some(mcx::PgVec::new_in(mcx));
    let fcinfo = FunctionCallInfoBaseData {
        flinfo: Some(setexpr.func.clone()),
        nargs: 2,
        args: alloc::vec![
            NullableDatum::value(types_datum::Datum::from_i32(1)),
            NullableDatum::value(types_datum::Datum::from_i32(3)),
        ],
        ..Default::default()
    };
    setexpr.fcinfo = Some(mcx::alloc_in(mcx, fcinfo).unwrap());

    let expected = backend_access_common_tupdesc::CreateTemplateTupleDesc(mcx, 1).unwrap();
    let mut expected = mcx::alloc_in(mcx, expected).unwrap();
    backend_access_common_tupdesc::TupleDescInitEntry(
        &mut expected,
        1,
        Some("generate_series"),
        INT4OID,
        -1,
        0,
    )
    .unwrap();

    let mut arg_ctx = ctx.mcx().context().new_child("argcontext");
    let tstore = ExecMakeTableFunctionResult(
        &mut setexpr,
        econtext,
        &mut arg_ctx,
        &expected,
        false,
        &mut estate,
    )
    .expect("ExecMakeTableFunctionResult over real generate_series_int4");

    let store = tstore
        .payload()
        .expect("setResult tuplestore")
        .downcast_ref::<MockStore>()
        .expect("MockStore");
    let vals: alloc::vec::Vec<i32> = store.rows.iter().map(|&(v, _)| v).collect();
    assert_eq!(vals, alloc::vec![1, 2, 3], "generate_series(1,3) → {{1,2,3}}");
}

#[test]
fn unregistered_srf_oid_errors() {
    setup();
    let ctx = MemoryContext::new("execSRF per-query");
    let mcx = ctx.mcx();
    let mut frame = FunctionCallInfoBaseData {
        fn_mcxt: Some(mcx),
        ..Default::default()
    };
    let err = srf_invoke_by_oid(424242, &mut frame).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::error::ERRCODE_UNDEFINED_FUNCTION);
}
