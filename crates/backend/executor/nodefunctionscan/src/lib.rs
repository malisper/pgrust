// nodeFunctionscan.c simple case + execSRF.c's table-function half; ROWS
// FROM, ORDINALITY and coldeflists are loud.
#![allow(non_snake_case)]

extern crate alloc;

use alloc::rc::Rc;

use ::datum::NullableDatum;
use ::execexpr::{exec_eval_expr, exec_init_expr, exec_init_qual, EvalSlots, ExprState};
use ::execscan::{exec_scan_extended, ScanNode, ScanState};
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{Mcx, PgBox, PgVec};
use ::types_error::{PgError, PgResult, ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED};
use ::types_fmgr::{
    ExprDoneCond, FmgrInfo, LocalFcinfo, ReturnSetInfo, SetFunctionReturnMode, SFRM_Materialize,
    SFRM_Materialize_Preferred, SFRM_Materialize_Random, SFRM_ValuePerCall,
};
use ::types_nodes::plannodes::FunctionScan;
use ::types_nodes::RangeTblFunction;
use ::types_slot::{TupleSlotKind, EXEC_FLAG_BACKWARD};
use ::types_tuple::TupleDescData;
use ::tuplestore::Tuplestore;

#[cfg(test)]
mod tests;

pub fn init_seams() {}

// SetExprState resolved once at init; fn_extra carries the SRF frame.
struct SetExprState<'mcx> {
    flinfo: FmgrInfo,
    args: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>,
    collation: u32,
    returns_set: bool,
    // C's returnsTuple: composite results are exploded into columns.
    returns_tuple: bool,
}

pub struct FunctionScanState<'mcx> {
    pub ss: ScanState<'mcx>,
    setexpr: SetExprState<'mcx>,
    tupdesc: Option<Rc<TupleDescData<'mcx>>>,
    tstore: Option<Tuplestore>,
    eflags: i32,
}

impl<'mcx> ScanNode<'mcx> for FunctionScanState<'mcx> {
    #[inline(always)]
    fn ss_mut(&mut self) -> &mut ScanState<'mcx> {
        &mut self.ss
    }

    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        if self.tstore.is_none() {
            let mut store = exec_make_table_function_result(
                &mut self.setexpr,
                self.tupdesc.as_ref().expect("function scan already ended"),
                self.eflags & EXEC_FLAG_BACKWARD != 0,
                estate,
                self.ss.ps_ExprContext,
            )?;
            store.rescan();
            self.tstore = Some(store);
        }
        let forward =
            matches!(estate.es_direction, ::types_scan::ScanDirection::ForwardScanDirection);
        let mcx = estate.es_query_cxt;
        let slot = estate.slot_mut(self.ss.ss_ScanTupleSlot);
        self.tstore.as_mut().unwrap().gettupleslot(forward, false, slot, mcx)
    }
}

pub fn exec_function_scan<'mcx>(
    node: &mut FunctionScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    match (node.ss.qual.is_some(), node.ss.ps_ProjInfo.is_some()) {
        (false, false) => exec_scan_extended::<_, false, false>(node, estate),
        (true, false) => exec_scan_extended::<_, true, false>(node, estate),
        (false, true) => exec_scan_extended::<_, false, true>(node, estate),
        (true, true) => exec_scan_extended::<_, true, true>(node, estate),
    }
}

/// `ExecInitFunctionScan`, simple case (nfuncs == 1, no ordinality).
pub fn exec_init_function_scan<'mcx>(
    mcx: Mcx<'mcx>,
    node: &FunctionScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<FunctionScanState<'mcx>> {
    debug_assert!(node.scan.plan.lefttree.is_none() && node.scan.plan.righttree.is_none());
    if node.funcordinality || node.functions.len() != 1 {
        panic!(
            "ExecInitFunctionScan (nodeFunctionscan.c): ORDINALITY / multiple \
             functions unported — unit backend-executor-nodeFunctionscan"
        );
    }

    let rtfunc = node
        .functions
        .nth(0)
        .as_range_tbl_function()
        .expect("FunctionScan functions cell is RangeTblFunction");
    let mut setexpr = exec_init_table_function_result(mcx, rtfunc, estate)?;
    let (tupdesc, returns_tuple) = build_function_tupdesc(mcx, rtfunc)?;
    setexpr.returns_tuple = returns_tuple;

    let mut scan_tupdesc = tupdesc::CreateTupleDescCopy(mcx, &tupdesc)?;
    scan_tupdesc.tdtypeid = types_core::catalog::RECORDOID;
    scan_tupdesc.tdtypmod = -1;

    let ps_ExprContext = estate.exec_assign_expr_context();
    let ss_ScanTupleSlot =
        estate.exec_init_extra_tuple_slot(Some(Rc::new(scan_tupdesc)), TupleSlotKind::MinimalTuple);

    let mut ss = ScanState {
        qual: None,
        ps_ProjInfo: None,
        ps_ExprContext,
        scanrelid: node.scan.scanrelid,
        ss_currentRelation: None,
        ss_currentScanDesc: None,
        ss_ScanTupleSlot,
        instr_idx: None,
    };
    execscan::exec_assign_scan_projection_info(mcx, estate, &mut ss, &node.scan.plan.targetlist)?;
    ss.qual = exec_init_qual(mcx, &node.scan.plan.qual, estate.param_bind())?;

    Ok(FunctionScanState { ss, setexpr, tupdesc: Some(Rc::new(tupdesc)), tstore: None, eflags })
}

fn exec_init_table_function_result<'mcx>(
    mcx: Mcx<'mcx>,
    rtfunc: &RangeTblFunction<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<SetExprState<'mcx>> {
    let fexpr = rtfunc.funcexpr.expect("RangeTblFunction has funcexpr");
    let Some(func) = fexpr.as_func_expr() else {
        panic!(
            "ExecInitTableFunctionResult (execSRF.c): elidedFuncState (planner-folded \
             non-SRF item) unported — unit backend-executor-execSRF"
        );
    };
    let mut args: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>> = PgVec::new_in(mcx);
    for arg in &func.args {
        args.push(
            exec_init_expr(mcx, Some(arg), estate.param_bind())?
                .expect("non-NULL arg expression"),
        );
    }
    // init_sexpr's ACL_EXECUTE check omitted: only built-ins (PUBLIC
    // execute) resolve on this lane.
    let flinfo = fmgr_core::fmgr_info(func.funcid)?;
    Ok(SetExprState {
        flinfo,
        args,
        collation: func.inputcollid,
        returns_set: func.funcretset,
        returns_tuple: false,
    })
}

fn build_function_tupdesc<'mcx>(
    mcx: Mcx<'mcx>,
    rtfunc: &RangeTblFunction<'mcx>,
) -> PgResult<(TupleDescData<'mcx>, bool)> {
    debug_assert!(rtfunc.funccolnames.is_nil());
    let fexpr = rtfunc.funcexpr.expect("RangeTblFunction has funcexpr");
    let resolved = funcapi::get_expr_result_type(mcx, Some(fexpr))?;
    match resolved.class {
        funcapi::TypeFuncClass::Scalar => {
            let mut d = tupdesc::CreateTemplateTupleDesc(mcx, 1)?;
            tupdesc::TupleDescInitEntry(&mut d, 1, None, resolved.result_type_id, -1, 0)?;
            tupdesc::TupleDescInitEntryCollation(&mut d, 1, execscan::expr_collation(fexpr));
            debug_assert_eq!(rtfunc.funccolcount, 1);
            Ok((d, false))
        }
        funcapi::TypeFuncClass::Composite | funcapi::TypeFuncClass::Record => {
            let d = resolved.result_tuple_desc.unwrap_or_else(|| {
                panic!(
                    "ExecInitFunctionScan (nodeFunctionscan.c): {:?} result without a \
                     tupdesc (coldeflist lane unported)",
                    resolved.class
                )
            });
            debug_assert_eq!(rtfunc.funccolcount as i32, d.natts);
            Ok((d, true))
        }
        other => panic!(
            "ExecInitFunctionScan (nodeFunctionscan.c): function result class {other:?} \
             unported"
        ),
    }
}

#[cold]
#[inline(never)]
fn value_per_call_violated() -> Box<PgError> {
    Box::new(
        PgError::error("table-function protocol for value-per-call mode was not followed")
            .with_sqlstate(ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED),
    )
}

#[cold]
#[inline(never)]
fn materialize_violated() -> Box<PgError> {
    Box::new(
        PgError::error("table-function protocol for materialize mode was not followed")
            .with_sqlstate(ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED),
    )
}

/// `ExecMakeTableFunctionResult`, ValuePerCall arm.
fn exec_make_table_function_result<'mcx>(
    setexpr: &mut SetExprState<'mcx>,
    expected_desc: &TupleDescData<'mcx>,
    random_access: bool,
    estate: &mut EStateData<'mcx>,
    ecxt: ::executils::EcxtId,
) -> PgResult<Tuplestore> {
    match setexpr.args.len() {
        0 => run_value_per_call::<0>(setexpr, expected_desc, random_access, estate, ecxt),
        1 => run_value_per_call::<1>(setexpr, expected_desc, random_access, estate, ecxt),
        2 => run_value_per_call::<2>(setexpr, expected_desc, random_access, estate, ecxt),
        3 => run_value_per_call::<3>(setexpr, expected_desc, random_access, estate, ecxt),
        4 => run_value_per_call::<4>(setexpr, expected_desc, random_access, estate, ecxt),
        n => panic!("ExecMakeTableFunctionResult: {n}-argument SRF — widen the fcinfo dispatch"),
    }
}

fn run_value_per_call<'mcx, const N: usize>(
    setexpr: &mut SetExprState<'mcx>,
    expected_desc: &TupleDescData<'mcx>,
    random_access: bool,
    estate: &mut EStateData<'mcx>,
    ecxt: ::executils::EcxtId,
) -> PgResult<Tuplestore> {
    let work_mem = init_small::globals::work_mem();
    let mut allowed = SFRM_ValuePerCall | SFRM_Materialize | SFRM_Materialize_Preferred;
    if random_access {
        allowed |= SFRM_Materialize_Random;
    }
    let mut rsinfo = ReturnSetInfo::new(allowed);
    // SAFETY: expectedDesc contract — points at the scan tupdesc, which
    // outlives this call frame; rsinfo dies with the frame.
    rsinfo.expectedDesc =
        Some(core::ptr::NonNull::from(expected_desc).cast::<core::ffi::c_void>());
    let mut fcinfo = LocalFcinfo::<N>::new(setexpr.collation);
    fcinfo.resultinfo = rsinfo.as_fmnode_ptr();
    // C evaluates the SRF in econtext per-tuple memory (reset per call); the
    // row is copied into the tuplestore before the next call's reset.
    // SAFETY: the ExprContext outlives this loop's stack frame.
    unsafe { fcinfo.set_result_mcx(estate.ecxt(ecxt).per_tuple_mcx()) };

    // ExecEvalFuncArgs; by-ref arg datums (e.g. a built array) land in the
    // scan econtext's per-tuple memory, which lives across the SRF loop.
    let mut all_null_skip = false;
    for i in 0..N {
        // SAFETY: the per-tuple context outlives this scan-start call.
        unsafe { setexpr.args[i].arm_result_mcx_raw(estate.ecxt(ecxt).per_tuple_mcx()) };
        let mut slots = EvalSlots { scan: None, inner: None, outer: None };
        let NullableDatum { value, isnull } = exec_eval_expr(&mut setexpr.args[i], &mut slots)?;
        if isnull {
            fcinfo.set_arg_null(i);
            all_null_skip |= setexpr.flinfo.fn_strict;
        } else {
            fcinfo.set_arg(i, value);
        }
    }

    let mut store = Tuplestore::begin_heap(random_access, false, work_mem);
    if all_null_skip {
        return Ok(store);
    }

    let mut first_time = true;
    loop {
        estate.ecxt_mut(ecxt).reset();
        fcinfo.isnull = false;
        rsinfo.isDone = ExprDoneCond::ExprSingleResult;
        let result = setexpr.flinfo.invoke(&mut fcinfo)?;

        match rsinfo.returnMode {
            SetFunctionReturnMode::ValuePerCall => {
                if rsinfo.isDone == ExprDoneCond::ExprEndResult {
                    break;
                }
                if setexpr.returns_tuple {
                    put_composite_row(&mut store, expected_desc, result, fcinfo.isnull, estate)?;
                } else {
                    store.putvalues(expected_desc, &[result], &[fcinfo.isnull])?;
                }
                if rsinfo.isDone != ExprDoneCond::ExprMultipleResult {
                    break;
                }
                if !setexpr.returns_set {
                    return Err(value_per_call_violated());
                }
            }
            SetFunctionReturnMode::Materialize => {
                if !first_time
                    || rsinfo.isDone != ExprDoneCond::ExprSingleResult
                    || !setexpr.returns_set
                {
                    return Err(materialize_violated());
                }
                // C's setResult-NULL leg hands back an empty tuplestore; the
                // pre-built `store` already is one.
                if let Some(set_result) = rsinfo.setResult.take() {
                    store = *set_result
                        .downcast::<Tuplestore>()
                        .expect("rsinfo.setResult downcasts to Tuplestore");
                }
                break;
            }
        }
        first_time = false;
    }
    Ok(store)
}

// C execSRF.c returnsTuple arm: explode the composite datum into columns
// (tuplestore_puttuple of the embedded tuple data; the rowtype-consistency
// check C performs for RECORD results is subsumed by deforming with the
// scan's expected descriptor).
fn put_composite_row<'mcx>(
    store: &mut Tuplestore,
    expected_desc: &TupleDescData<'mcx>,
    result: ::datum::Datum,
    isnull: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let natts = expected_desc.natts as usize;
    let mcx = estate.es_query_cxt;
    let mut values: PgVec<'_, ::datum::Datum> = ::mcx::vec_with_capacity_in(mcx, natts)?;
    let mut nulls: PgVec<'_, bool> = ::mcx::vec_with_capacity_in(mcx, natts)?;
    values.resize(natts, ::datum::Datum::null());
    nulls.resize(natts, true);
    if !isnull {
        let p = result.as_usize() as *const u8;
        // SAFETY: a non-null composite result datum is a live HeapTupleHeader
        // image readable for its datum length.
        let header = unsafe { &*(p as *const ::types_tuple::htup::HeapTupleHeaderData) };
        let t_len = header.datum_length();
        // SAFETY: same image, exclusive for this call.
        let tuple = unsafe {
            ::types_tuple::htup::HeapTupleData::from_raw_parts(
                p,
                t_len,
                Default::default(),
                ::types_core::InvalidOid,
            )
        };
        ::types_tuple::getattr::heap_deform_tuple(&tuple, expected_desc, &mut values, &mut nulls);
    }
    store.putvalues(expected_desc, &values, &nulls)
}

pub fn exec_end_function_scan(node: &mut FunctionScanState<'_>) {
    if let Some(store) = node.tstore.take() {
        store.end();
    }
    node.setexpr.flinfo.fn_extra = None;
    node.setexpr.args.clear();
    node.tupdesc = None;
}

/// `ExecReScanFunctionScan`; the chgParam recompute leg is dead, rewind only.
pub fn exec_rescan_function_scan<'mcx>(
    node: &mut FunctionScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    execscan::exec_scan_rescan(&mut node.ss, estate);
    if let Some(store) = node.tstore.as_mut() {
        store.rescan();
    }
    Ok(())
}

// Exempt: all released in exec_end_function_scan.
mcx::forget_safe_struct!(
    SetExprState<'_> { collation, returns_set, returns_tuple; flinfo, args },
    FunctionScanState<'_> { ss, setexpr, eflags; tupdesc, tstore },
);
