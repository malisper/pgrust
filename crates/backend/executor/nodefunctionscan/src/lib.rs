// nodeFunctionscan.c + execSRF.c's table-function half.
#![allow(non_snake_case)]

extern crate alloc;

use alloc::rc::Rc;
use core::alloc::Layout;
use core::ptr::NonNull;

use ::datum::NullableDatum;
use ::execexpr::{exec_eval_expr, exec_init_expr, exec_init_qual, EvalSlots, ExprState};
use ::execscan::{exec_scan_epq, exec_scan_extended, ScanNode, ScanState};
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{Allocator, Mcx, MemoryContext, PgBox, PgVec};
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
    // None only when elided_func_state is Some (C's fn_oid = InvalidOid).
    flinfo: Option<FmgrInfo>,
    args: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>,
    collation: u32,
    returns_set: bool,
    // C's returnsTuple: composite results are exploded into columns.
    returns_tuple: bool,
    // C's elidedFuncState: the planner constant-folded/inlined the non-SRF
    // call, so the tree is no longer a FuncExpr; evaluate it generically.
    elided_func_state: Option<PgBox<'mcx, ExprState<'mcx>>>,
}

struct FunctionScanPerFuncState<'mcx> {
    setexpr: SetExprState<'mcx>,
    tupdesc: Rc<TupleDescData<'mcx>>,
    colcount: i32,
    tstore: Option<Tuplestore>,
    // 1 + the actual row count once known; -1 until then (backward scans).
    rowcount: i64,
    func_slot: Option<ExecSlotId>,
    funcparams: &'mcx ::types_nodes::bitmapset::Bitmapset<'mcx>,
}

pub struct FunctionScanState<'mcx> {
    pub ss: ScanState<'mcx>,
    simple: bool,
    ordinality: bool,
    ordinal: i64,
    funcstates: PgVec<'mcx, FunctionScanPerFuncState<'mcx>>,
    // Retained per-row scratch for the general path's column copy.
    scratch: PgVec<'mcx, NullableDatum>,
    // C argcontext ("Table function arguments"): SRF argument values must
    // survive the ValuePerCall loop's per-tuple resets; reset per rescan.
    // Arena-slot + reset-callback ownership (the nodememoize make_table_ctx
    // idiom): pgrust child links are accounting-only weak refs, so unlike C
    // — where FreeExecutorState's MemoryContextDelete(es_query_cxt) recurses
    // into argcontext — nothing reclaims an owned context embedded in a
    // forgotten node state. Holding it by value here leaked ~8KB per
    // FunctionScan per execution (TPROC-C leak #3, notes/memleak4-lane.md).
    // The registered callback drops it on BOTH teardown paths: the estate
    // context reset (exec_ctx_pool park) and the abort-path context drop.
    arg_mcx: NonNull<MemoryContext>,
    eflags: i32,
}

// C argcontext lifetime (execMain.c FreeExecutorState recursion), built the
// way nodememoize builds MemoizeHashTable: the context VALUE lives in the
// estate arena at a stable address, and the estate context's reset callback
// is the reclaim point (fires exactly once, before the arena bytes go).
fn make_arg_ctx(mcx: Mcx<'_>) -> PgResult<NonNull<MemoryContext>> {
    let layout = Layout::new::<MemoryContext>();
    let raw = mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?;
    let p: NonNull<MemoryContext> = raw.cast();
    // SAFETY: fresh allocation of the exact layout.
    unsafe { p.write(mcx.context().new_child_bump("Table function arguments")) };
    // SAFETY: fires exactly once, before the arena bytes are reclaimed.
    mcx.context()
        .register_reset_callback(move || unsafe { core::ptr::drop_in_place(p.as_ptr()) });
    Ok(p)
}

impl<'mcx> ScanNode<'mcx> for FunctionScanState<'mcx> {
    #[inline(always)]
    fn ss_mut(&mut self) -> &mut ScanState<'mcx> {
        &mut self.ss
    }

    /// `FunctionRecheck`: nothing to check.
    fn epq_recheck(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        _slot: ExecSlotId,
    ) -> PgResult<bool> {
        Ok(true)
    }

    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        let forward =
            matches!(estate.es_direction, ::types_scan::ScanDirection::ForwardScanDirection);
        let mcx = estate.es_query_cxt;

        if self.simple {
            let fs = &mut self.funcstates[0];
            if fs.tstore.is_none() {
                let mut store = exec_make_table_function_result(
                    &mut fs.setexpr,
                    &fs.tupdesc,
                    self.eflags & EXEC_FLAG_BACKWARD != 0,
                    estate,
                    self.ss.ps_ExprContext,
                    // SAFETY: arena slot armed by make_arg_ctx; exclusive
                    // during the scan (dropped only by the estate reset cb).
                    unsafe { self.arg_mcx.as_mut() },
                )?;
                store.rescan();
                fs.tstore = Some(store);
            }
            let fs = &mut self.funcstates[0];
            let slot = estate.slot_mut(self.ss.ss_ScanTupleSlot);
            return fs.tstore.as_mut().unwrap().gettupleslot(forward, false, slot, mcx);
        }

        // Move the ordinal off either end by exactly one before the
        // end-of-data check (C FunctionNext).
        let oldpos = self.ordinal;
        if forward {
            self.ordinal += 1;
        } else {
            self.ordinal -= 1;
        }

        exectuples::exec_clear_tuple(estate.slot_mut(self.ss.ss_ScanTupleSlot), mcx);
        let mut att = 0usize;
        let mut alldone = true;

        for funcno in 0..self.funcstates.len() {
            let fs = &mut self.funcstates[funcno];
            if fs.tstore.is_none() {
                let mut store = exec_make_table_function_result(
                    &mut fs.setexpr,
                    &fs.tupdesc,
                    self.eflags & EXEC_FLAG_BACKWARD != 0,
                    estate,
                    self.ss.ps_ExprContext,
                    // SAFETY: arena slot armed by make_arg_ctx; exclusive
                    // during the scan (dropped only by the estate reset cb).
                    unsafe { self.arg_mcx.as_mut() },
                )?;
                store.rescan();
                fs.tstore = Some(store);
            }
            let fs = &mut self.funcstates[funcno];
            let func_slot = fs.func_slot.expect("general path has per-function slots");
            let got = if fs.rowcount != -1 && fs.rowcount < oldpos {
                exectuples::exec_clear_tuple(estate.slot_mut(func_slot), mcx);
                false
            } else {
                fs.tstore.as_mut().unwrap().gettupleslot(
                    forward,
                    false,
                    estate.slot_mut(func_slot),
                    mcx,
                )?
            };

            let colcount = fs.colcount as usize;
            if !got {
                if forward && fs.rowcount == -1 {
                    fs.rowcount = self.ordinal;
                }
                self.scratch.clear();
                self.scratch.resize(colcount, NullableDatum::null());
            } else {
                let fslot = estate.slot_mut(func_slot);
                exectuples::slot_getallattrs(fslot);
                let base = fslot.base_mut();
                self.scratch.clear();
                for i in 0..colcount {
                    self.scratch.push(NullableDatum {
                        value: base.tts_values[i],
                        isnull: base.tts_isnull[i],
                    });
                }
                alldone = false;
            }
            let base = estate.slot_mut(self.ss.ss_ScanTupleSlot).base_mut();
            for d in self.scratch.iter() {
                base.tts_values[att] = d.value;
                base.tts_isnull[att] = d.isnull;
                att += 1;
            }
        }

        if self.ordinality {
            let base = estate.slot_mut(self.ss.ss_ScanTupleSlot).base_mut();
            base.tts_values[att] = ::datum::Datum::from_i64(self.ordinal);
            base.tts_isnull[att] = false;
        }

        if !alldone {
            exectuples::exec_store_virtual_tuple(estate.slot_mut(self.ss.ss_ScanTupleSlot));
        }
        Ok(!alldone)
    }
}

pub fn exec_function_scan<'mcx>(
    node: &mut FunctionScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    // C ExecScan reads es_epq_active per call: under an EvalPlanQual recheck
    // the fetch substitutes this rel's marked original row (relsubs_rowmark
    // wholerow junk) instead of re-running the function — re-emitting all
    // rows lets a parameterized-inner join consume the target's test tuple
    // at the wrong outer row and silently skip the row (epqjoin lane).
    if estate.es_epq_active {
        return exec_scan_epq(node, estate);
    }
    match (node.ss.qual.is_some(), node.ss.ps_ProjInfo.is_some()) {
        (false, false) => exec_scan_extended::<_, false, false>(node, estate),
        (true, false) => exec_scan_extended::<_, true, false>(node, estate),
        (false, true) => exec_scan_extended::<_, false, true>(node, estate),
        (true, true) => exec_scan_extended::<_, true, true>(node, estate),
    }
}

/// `ExecInitFunctionScan`.
pub fn exec_init_function_scan<'mcx>(
    mcx: Mcx<'mcx>,
    node: &FunctionScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<FunctionScanState<'mcx>> {
    debug_assert!(node.scan.plan.lefttree.is_none() && node.scan.plan.righttree.is_none());
    let nfuncs = node.functions.len();
    let ordinality = node.funcordinality;
    let simple = nfuncs == 1 && !ordinality;

    let mut funcstates: PgVec<'mcx, FunctionScanPerFuncState<'mcx>> = PgVec::new_in(mcx);
    let mut natts: i32 = 0;
    for f in &node.functions {
        let rtfunc = f
            .as_range_tbl_function()
            .expect("FunctionScan functions cell is RangeTblFunction");
        let mut setexpr = exec_init_table_function_result(mcx, rtfunc, estate)?;
        let (tupdesc, returns_tuple) = build_function_tupdesc(mcx, rtfunc)?;
        setexpr.returns_tuple = returns_tuple;
        // C: colcount is the plan-time funccolcount; a named composite may
        // have gained or lost columns since, so fs->tupdesc can differ.
        natts += rtfunc.funccolcount;
        funcstates.push(FunctionScanPerFuncState {
            setexpr,
            colcount: rtfunc.funccolcount,
            tupdesc: Rc::new(tupdesc),
            tstore: None,
            rowcount: -1,
            func_slot: None,
            funcparams: &rtfunc.funcparams,
        });
    }

    let mut scan_tupdesc = if simple {
        tupdesc::CreateTupleDescCopy(mcx, &funcstates[0].tupdesc)?
    } else {
        let mut d =
            tupdesc::CreateTemplateTupleDesc(mcx, natts + if ordinality { 1 } else { 0 })?;
        let mut attno: i16 = 0;
        for fs in funcstates.iter() {
            for j in 1..=fs.colcount {
                attno += 1;
                tupdesc::TupleDescCopyEntry(&mut d, attno, &fs.tupdesc, j as i16);
            }
        }
        if ordinality {
            attno += 1;
            tupdesc::TupleDescInitEntry(
                &mut d,
                attno,
                Some("ordinality"),
                types_core::catalog::INT8OID,
                -1,
                0,
            )?;
        }
        d
    };
    scan_tupdesc.tdtypeid = types_core::catalog::RECORDOID;
    scan_tupdesc.tdtypmod = -1;

    let ps_ExprContext = estate.exec_assign_expr_context();
    let ss_ScanTupleSlot = estate.exec_init_extra_tuple_slot(
        Some(Rc::new(scan_tupdesc)),
        if simple { TupleSlotKind::MinimalTuple } else { TupleSlotKind::Virtual },
    );
    if !simple {
        for fs in funcstates.iter_mut() {
            fs.func_slot = Some(estate.exec_init_extra_tuple_slot(
                Some(Rc::clone(&fs.tupdesc)),
                TupleSlotKind::MinimalTuple,
            ));
        }
    }

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
    ss.qual = {
        let pb = estate.param_bind();
        ::executils::with_subplan_compile_env(estate, |env| {
            ::execexpr::exec_init_qual_subplans(mcx, &node.scan.plan.qual, pb, env)
        })?
    };

    Ok(FunctionScanState {
        ss,
        simple,
        ordinality,
        ordinal: 0,
        funcstates,
        scratch: PgVec::new_in(mcx),
        // Bump (leak-then-reset) like ExprContext's per-tuple context: arg
        // evaluation leaks by-ref datums here by design (C argContext).
        arg_mcx: make_arg_ctx(mcx)?,
        eflags,
    })
}

fn exec_init_table_function_result<'mcx>(
    mcx: Mcx<'mcx>,
    rtfunc: &RangeTblFunction<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<SetExprState<'mcx>> {
    let fexpr = rtfunc.funcexpr.expect("RangeTblFunction has funcexpr");
    let Some(func) = fexpr.as_func_expr() else {
        let elided = exec_init_expr(mcx, Some(fexpr), estate.param_bind())?
            .expect("non-NULL elided table function expression");
        return Ok(SetExprState {
            flinfo: None,
            args: PgVec::new_in(mcx),
            collation: types_core::InvalidOid,
            returns_set: false,
            returns_tuple: false,
            elided_func_state: Some(elided),
        });
    };
    let mut args: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>> = PgVec::new_in(mcx);
    for arg in &func.args {
        args.push(
            exec_init_expr(mcx, Some(arg), estate.param_bind())?
                .expect("non-NULL arg expression"),
        );
    }
    // init_sexpr's ACL_EXECUTE check (execQual.c): contrib functions REVOKE
    // PUBLIC (pg_buffercache 1.3+, pg_stat_statements), so the old
    // "built-ins are PUBLIC-execute" shortcut no longer holds on this path.
    {
        const PROCEDURE_RELATION_ID: types_core::Oid = 1255;
        const ACL_EXECUTE: u64 = 1 << 7;
        const ACLCHECK_OK: i32 = 0;
        let userid = miscinit_seams::get_user_id::call();
        let aclresult = aclchk_seams::object_aclcheck::call(
            PROCEDURE_RELATION_ID,
            func.funcid,
            userid,
            ACL_EXECUTE,
        )?;
        if aclresult != ACLCHECK_OK {
            let name = lsyscache::get_func_name(mcx, func.funcid)?;
            let name = name.as_ref().map(|n| n.as_str()).unwrap_or("(unknown)");
            return Err(Box::new(
                PgError::error(format!("permission denied for function {name}"))
                    .with_sqlstate(::types_error::ERRCODE_INSUFFICIENT_PRIVILEGE),
            ));
        }
    }
    let mut flinfo = fmgr_core::fmgr_info(func.funcid)?;
    // C init_sexpr: fmgr_info_set_expr((Node *) sexpr->expr, &sexpr->func) —
    // variadic-"any" callees read arg types off fn_expr.
    flinfo.fn_expr = Some(::execexpr::erase_fn_expr(mcx, fexpr)?);
    Ok(SetExprState {
        flinfo: Some(flinfo),
        args,
        collation: func.inputcollid,
        returns_set: func.funcretset,
        returns_tuple: false,
        elided_func_state: None,
    })
}

fn build_function_tupdesc<'mcx>(
    mcx: Mcx<'mcx>,
    rtfunc: &RangeTblFunction<'mcx>,
) -> PgResult<(TupleDescData<'mcx>, bool)> {
    let fexpr = rtfunc.funcexpr.expect("RangeTblFunction has funcexpr");
    // C: a coldeflist takes priority regardless of the resolved result class
    // (the RECORD-Const leg can resolve Composite with a different natts).
    if !rtfunc.funccolnames.is_nil() {
        // BuildDescFromLists over the parse-time coldeflist + BlessTupleDesc.
        let n = rtfunc.funccolnames.len();
        let mut d = tupdesc::CreateTemplateTupleDesc(mcx, n as i32)?;
        for i in 0..n {
            let attno = (i + 1) as i16;
            let name = rtfunc
                .funccolnames
                .nth(i)
                .as_string()
                .expect("funccolnames cell is String")
                .sval;
            tupdesc::TupleDescInitEntry(
                &mut d,
                attno,
                Some(name),
                rtfunc.funccoltypes.nth(i),
                rtfunc.funccoltypmods.nth(i),
                0,
            )?;
            tupdesc::TupleDescInitEntryCollation(&mut d, attno, rtfunc.funccolcollations.nth(i));
        }
        if d.tdtypeid == types_core::catalog::RECORDOID && d.tdtypmod < 0 {
            typcache_seams::assign_record_type_typmod::call(&mut d)?;
        }
        return Ok((d, true));
    }
    let resolved = funcapi::get_expr_result_type(mcx, Some(fexpr))?;
    match resolved.class {
        funcapi::TypeFuncClass::Scalar => {
            let mut d = tupdesc::CreateTemplateTupleDesc(mcx, 1)?;
            tupdesc::TupleDescInitEntry(&mut d, 1, None, resolved.result_type_id, -1, 0)?;
            tupdesc::TupleDescInitEntryCollation(&mut d, 1, execscan::expr_collation(fexpr));
            Ok((d, false))
        }
        funcapi::TypeFuncClass::Composite | funcapi::TypeFuncClass::CompositeDomain => {
            let d = resolved.result_tuple_desc.unwrap_or_else(|| {
                panic!(
                    "ExecInitFunctionScan (nodeFunctionscan.c): {:?} result without a \
                     tupdesc",
                    resolved.class
                )
            });
            Ok((d, true))
        }
        other => panic!(
            "ExecInitFunctionScan (nodeFunctionscan.c): function result class {other:?} \
             without a coldeflist"
        ),
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn value_per_call_violated() -> Box<PgError> {
    Box::new(
        PgError::error("table-function protocol for value-per-call mode was not followed")
            .with_sqlstate(ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED),
    )
}

#[track_caller]
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
    arg_mcx: &mut ::mcx::MemoryContext,
) -> PgResult<Tuplestore> {
    // ExecEvalParamExec pending-initplan arm, hoisted out of the interpreter
    // (execscan pattern): the funcexpr args may carry InitPlan Params.
    for st in setexpr.elided_func_state.iter().map(|b| &**b).chain(setexpr.args.iter().map(|b| &**b))
    {
        let deps = st.param_exec_deps();
        if !deps.is_empty() {
            ::executils::exec_eval_param_exec_params(estate, deps)?;
        }
    }
    if setexpr.elided_func_state.is_some() {
        return run_elided(setexpr, expected_desc, random_access, estate, ecxt);
    }
    match setexpr.args.len() {
        0 => run_value_per_call::<0>(setexpr, expected_desc, random_access, estate, ecxt, arg_mcx),
        1 => run_value_per_call::<1>(setexpr, expected_desc, random_access, estate, ecxt, arg_mcx),
        2 => run_value_per_call::<2>(setexpr, expected_desc, random_access, estate, ecxt, arg_mcx),
        3 => run_value_per_call::<3>(setexpr, expected_desc, random_access, estate, ecxt, arg_mcx),
        4 => run_value_per_call::<4>(setexpr, expected_desc, random_access, estate, ecxt, arg_mcx),
        // pg_create_logical_replication_slot(name, plugin, temporary,
        // twophase, failover) — pg_createsubscriber's slot creation.
        5 => run_value_per_call::<5>(setexpr, expected_desc, random_access, estate, ecxt, arg_mcx),
        // contrib tablefunc connectby: 6- and 7-argument forms.
        6 => run_value_per_call::<6>(setexpr, expected_desc, random_access, estate, ecxt, arg_mcx),
        7 => run_value_per_call::<7>(setexpr, expected_desc, random_access, estate, ecxt, arg_mcx),
        // pg_restore_attribute_stats over pg_stats rows is a 36-arg
        // variadic-"any" SRF call (stats_import).
        36 => {
            run_value_per_call::<36>(setexpr, expected_desc, random_access, estate, ecxt, arg_mcx)
        }
        n => panic!("ExecMakeTableFunctionResult: {n}-argument SRF — widen the fcinfo dispatch"),
    }
}

// C's elidedFuncState leg of ExecMakeTableFunctionResult: generic ExecEvalExpr
// with isDone pinned to ExprSingleResult, so the ValuePerCall loop stores
// exactly one row (all-nulls expansion for a NULL composite, as C).
fn run_elided<'mcx>(
    setexpr: &mut SetExprState<'mcx>,
    expected_desc: &TupleDescData<'mcx>,
    random_access: bool,
    estate: &mut EStateData<'mcx>,
    ecxt: ::executils::EcxtId,
) -> PgResult<Tuplestore> {
    let work_mem = init_small::globals::work_mem();
    let mut store = Tuplestore::begin_heap(random_access, false, work_mem);
    estate.ecxt_mut(ecxt).reset();
    let elided = setexpr.elided_func_state.as_mut().expect("run_elided has elidedFuncState");
    // The row is copied into the tuplestore before the next per-tuple reset.
    // SAFETY: the ExprContext outlives this call frame.
    unsafe { elided.arm_result_mcx_raw(estate.ecxt(ecxt).per_tuple_mcx()) };
    let mut slots = EvalSlots { scan: None, inner: None, outer: None };
    let NullableDatum { value, isnull } = exec_eval_expr(elided, &mut slots)?;
    if setexpr.returns_tuple {
        let mut set_desc: Option<TupleDescData<'mcx>> = None;
        put_composite_row(&mut store, expected_desc, &mut set_desc, value, isnull, estate)?;
        // C: cross-check the function-provided tupdesc against expectedDesc.
        if let Some(d) = &set_desc {
            tupledesc_match(expected_desc, d)?;
        }
    } else {
        store.putvalues(expected_desc, &[value], &[isnull])?;
    }
    Ok(store)
}

fn run_value_per_call<'mcx, const N: usize>(
    setexpr: &mut SetExprState<'mcx>,
    expected_desc: &TupleDescData<'mcx>,
    random_access: bool,
    estate: &mut EStateData<'mcx>,
    ecxt: ::executils::EcxtId,
    arg_mcx: &mut ::mcx::MemoryContext,
) -> PgResult<Tuplestore> {
    let work_mem = init_small::globals::work_mem();
    let mut allowed = SFRM_ValuePerCall | SFRM_Materialize | SFRM_Materialize_Preferred;
    if random_access {
        allowed |= SFRM_Materialize_Random;
    }
    let flinfo = setexpr.flinfo.as_mut().expect("ValuePerCall path has a resolved function");
    let mut rsinfo = ReturnSetInfo::new(allowed);
    // SAFETY: expectedDesc contract — points at the scan tupdesc, which
    // outlives this call frame; rsinfo dies with the frame.
    rsinfo.expectedDesc =
        Some(core::ptr::NonNull::from(expected_desc).cast::<core::ffi::c_void>());
    let mut fcinfo = LocalFcinfo::<N>::new(setexpr.collation);
    // fcinfo.resultinfo and the result mcx are armed inside the loop, before
    // each invoke (miri F6/F9: per-invoke provenance re-arm).

    // ExecEvalFuncArgs; C evaluates the arguments in argContext — by-ref arg
    // datums must survive the loop's per-tuple resets below (execSRF.c:119).
    arg_mcx.reset();
    let mut all_null_skip = false;
    for i in 0..N {
        // SAFETY: arg_mcx is owned by the scan state and outlives this loop;
        // it is only reset at the next scan start.
        unsafe { setexpr.args[i].arm_result_mcx_raw(arg_mcx.mcx()) };
        let mut slots = EvalSlots { scan: None, inner: None, outer: None };
        let NullableDatum { value, isnull } = exec_eval_expr(&mut setexpr.args[i], &mut slots)?;
        if isnull {
            fcinfo.set_arg_null(i);
            all_null_skip |= flinfo.fn_strict;
        } else {
            fcinfo.set_arg(i, value);
        }
    }

    let mut store = Tuplestore::begin_heap(random_access, false, work_mem);
    if all_null_skip {
        // execSRF.c no_function_result: a strict function skipped for a NULL
        // argument acts like it returned NULL — for a set-returning function
        // that's an empty result, but a non-set function still contributes
        // one all-nulls row (expectedDesc-shaped).
        if !setexpr.returns_set {
            let mut none = None;
            put_composite_row(
                &mut store,
                expected_desc,
                &mut none,
                ::datum::Datum::null(),
                true,
                estate,
            )?;
        }
        return Ok(store);
    }

    let mut set_desc: Option<TupleDescData<'mcx>> = None;
    let mut first_time = true;
    loop {
        // C parity (execSRF.c ExecMakeTableFunctionResult loop top): an SRF
        // materialize fill produces its WHOLE result set here — without this
        // the fill is cancel/die-deaf for its entire duration (the incumbent
        // path wedges on user cancel, and a statement-task chase can never
        // land — GL-STMTTASK-1 found it via a wedged gang board). The
        // pending-flag pre-test is the tuplestore cfi() shape: one TLS read
        // per row; the seam dispatch only on a pending interrupt (also keeps
        // seam-less unit-test processes runnable).
        if init_small::globals::InterruptPending() {
            ::postgres_seams::check_for_interrupts::call()?;
        }
        estate.ecxt_mut(ecxt).reset();
        // C: pgstat_init_function_usage's `pgstat_track_functions <= fn_stats`
        // early-out, hoisted to the caller as the crate's API requires.
        let fcu = if flinfo.fn_stats < ::types_fmgr::TRACK_FUNC_ALL
            && ::pgstat::function::pgstat_track_functions() > flinfo.fn_stats as i32
        {
            Some(::pgstat::function::pgstat_init_function_usage(flinfo.fn_oid)?)
        } else {
            None
        };
        fcinfo.isnull = false;
        rsinfo.isDone = ExprDoneCond::ExprSingleResult;
        // Re-arm resultinfo before EVERY invoke: the isDone reset above (and
        // the driver's rsinfo reads/takes after the previous invoke) go
        // through fresh `&mut rsinfo` borrows, which invalidate a previously
        // armed pointer's provenance; the callee re-derives through it
        // (rsinfo_mut). C arms once (execSRF.c ExecMakeTableFunctionResult),
        // but C has no aliasing model to keep the armed pointer live across
        // the safe field writes. (Miri F6, notes/miri-pilot-lane.md.)
        fcinfo.resultinfo = rsinfo.as_fmnode_ptr();
        // Re-arm the result mcx too: the per-tuple reset() at the loop top
        // takes a fresh `&mut` on the ExprContext, invalidating a result-mcx
        // pointer armed on an earlier iteration (miri F9, same class as F6).
        // The row is copied into the tuplestore before the next call's reset.
        // SAFETY: the ExprContext outlives this loop's stack frame.
        unsafe { fcinfo.set_result_mcx(estate.ecxt(ecxt).per_tuple_mcx()) };
        let result = flinfo.invoke(&mut fcinfo)?;
        if let Some(fcu) = &fcu {
            ::pgstat::function::pgstat_end_function_usage(
                fcu,
                rsinfo.isDone != ExprDoneCond::ExprMultipleResult,
            );
        }

        match rsinfo.returnMode {
            SetFunctionReturnMode::ValuePerCall => {
                if rsinfo.isDone == ExprDoneCond::ExprEndResult {
                    break;
                }
                if setexpr.returns_tuple {
                    put_composite_row(
                        &mut store,
                        expected_desc,
                        &mut set_desc,
                        result,
                        fcinfo.isnull,
                        estate,
                    )?;
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
                // C: tupledesc_match(expectedDesc, rsinfo.setDesc).
                if let Some(set_desc) = rsinfo.setDesc {
                    // SAFETY: setDesc contract — a live TupleDescData for the
                    // duration of the call.
                    let src = unsafe { set_desc.cast::<TupleDescData<'_>>().as_ref() };
                    tupledesc_match(expected_desc, src)?;
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
    // C: if the set carried its own tupdesc (RECORD-returning ValuePerCall
    // rows), cross-check it against expectedDesc.
    if let Some(d) = &set_desc {
        tupledesc_match(expected_desc, d)?;
    }
    Ok(store)
}

#[track_caller]
#[cold]
fn tupledesc_mismatch(detail: String) -> Box<PgError> {
    Box::new(
        PgError::error("function return row and query-specified return row do not match")
            .with_sqlstate(::types_error::ERRCODE_DATATYPE_MISMATCH)
            .with_detail(detail),
    )
}

// C tupledesc_match (execSRF.c).
fn tupledesc_match(
    dst_tupdesc: &TupleDescData<'_>,
    src_tupdesc: &TupleDescData<'_>,
) -> PgResult<()> {
    if dst_tupdesc.natts != src_tupdesc.natts {
        let (s, d) = (src_tupdesc.natts, dst_tupdesc.natts);
        let noun = if s == 1 { "attribute" } else { "attributes" };
        return Err(tupledesc_mismatch(format!(
            "Returned row contains {s} {noun}, but query expects {d}."
        )));
    }
    for i in 0..dst_tupdesc.natts as usize {
        let dattr = &dst_tupdesc.attrs[i];
        let sattr = &src_tupdesc.attrs[i];
        if ::coerce::IsBinaryCoercible(sattr.atttypid, dattr.atttypid)? {
            continue;
        }
        if !dattr.attisdropped {
            return Err(tupledesc_mismatch(format!(
                "Returned type {} at ordinal position {}, but query expects {}.",
                ::format_type::format_type_be(sattr.atttypid)?,
                i + 1,
                ::format_type::format_type_be(dattr.atttypid)?,
            )));
        }
        if dattr.attlen != sattr.attlen || dattr.attalign != sattr.attalign {
            return Err(tupledesc_mismatch(format!(
                "Physical storage mismatch on dropped attribute at ordinal position {}.",
                i + 1
            )));
        }
    }
    Ok(())
}

// execSRF.c returnsTuple arm: the row is stored as its own rowtype; setDesc
// captures the first row's embedded type so the caller can cross-check it
// against expectedDesc (tupledesc_match) once the set is complete.
fn put_composite_row<'mcx>(
    store: &mut Tuplestore,
    expected_desc: &TupleDescData<'mcx>,
    set_desc: &mut Option<TupleDescData<'mcx>>,
    result: ::datum::Datum,
    isnull: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    if isnull {
        // C: a NULL from a tuple-returning function expands to a row of all
        // nulls, shaped by expectedDesc.
        let natts = expected_desc.natts as usize;
        let mut values: PgVec<'_, ::datum::Datum> = ::mcx::vec_with_capacity_in(mcx, natts)?;
        let mut nulls: PgVec<'_, bool> = ::mcx::vec_with_capacity_in(mcx, natts)?;
        values.resize(natts, ::datum::Datum::null());
        nulls.resize(natts, true);
        return store.putvalues(expected_desc, &values, &nulls);
    }
    // C DatumGetHeapTupleHeader: detoast (expanded records arrive as
    // extended datums) before reading the composite header.
    let src = result.as_usize() as *const u8;
    // SAFETY: a non-null composite result datum is a live varlena image.
    let _flat;
    let p = unsafe {
        if !::types_tuple::varatt::varatt_is_4b_u(src) {
            let image =
                core::slice::from_raw_parts(src, ::types_tuple::varatt::varsize_any(src));
            _flat = ::detoast_seams::detoast_attr::call(mcx, image)?;
            _flat.as_ptr()
        } else {
            src
        }
    };
    // SAFETY: p is a plain composite HeapTupleHeader image readable for its
    // datum length.
    let header = unsafe { &*(p as *const ::types_tuple::htup::HeapTupleHeaderData) };
    match set_desc {
        None => {
            // C: first non-NULL result — resolve the tupdesc from the type
            // info embedded in the rowtype datum.
            *set_desc = Some(typcache_seams::lookup_rowtype_tupdesc_copy::call(
                mcx,
                header.type_id(),
                header.typmod(),
            )?);
        }
        Some(d) => {
            if header.type_id() != d.tdtypeid || header.typmod() != d.tdtypmod {
                return Err(Box::new(
                    PgError::error("rows returned by function are not all of the same row type")
                        .with_sqlstate(::types_error::ERRCODE_DATATYPE_MISMATCH),
                ));
            }
        }
    }
    // SAFETY: same image, exclusive for this call.
    let tuple = unsafe {
        ::types_tuple::htup::HeapTupleData::from_raw_parts(
            p,
            header.datum_length(),
            Default::default(),
            ::types_core::InvalidOid,
        )
    };
    store.put_heap_tuple(&tuple)
}

pub fn exec_end_function_scan(node: &mut FunctionScanState<'_>) {
    for fs in node.funcstates.iter_mut() {
        if let Some(store) = fs.tstore.take() {
            store.end();
        }
        if let Some(flinfo) = fs.setexpr.flinfo.as_mut() {
            flinfo.fn_extra = None;
        }
        fs.setexpr.args.clear();
    }
    node.funcstates.clear();
    // arg_mcx is NOT freed here — C parity: ExecEndFunctionScan leaves
    // argcontext to FreeExecutorState; our equivalent is the estate-reset
    // callback registered by make_arg_ctx (fires at exec_ctx_pool park on
    // success and at the estate-context drop on abort).
}

/// `ExecReScanFunctionScan`, chgParam-NULL arm: rewind the tuplestores.
pub fn exec_rescan_function_scan<'mcx>(
    node: &mut FunctionScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    for fs in node.funcstates.iter_mut() {
        if let Some(slot) = fs.func_slot {
            exectuples::exec_clear_tuple(estate.slot_mut(slot), mcx);
        }
    }
    execscan::exec_scan_rescan(&mut node.ss, estate);
    node.ordinal = 0;
    for fs in node.funcstates.iter_mut() {
        if let Some(store) = fs.tstore.as_mut() {
            store.rescan();
        }
    }
    Ok(())
}

/// Changed-params rescan: drop affected tuplestores; the next fetch re-evaluates.
pub fn exec_rescan_function_scan_chg<'mcx>(
    node: &mut FunctionScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    chg: &::types_nodes::bitmapset::Bitmapset<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    for fs in node.funcstates.iter_mut() {
        if let Some(slot) = fs.func_slot {
            exectuples::exec_clear_tuple(estate.slot_mut(slot), mcx);
        }
    }
    execscan::exec_scan_rescan(&mut node.ss, estate);
    node.ordinal = 0;
    for fs in node.funcstates.iter_mut() {
        if chg.overlap(fs.funcparams) {
            if let Some(store) = fs.tstore.take() {
                store.end();
            }
            fs.rowcount = -1;
        } else if let Some(store) = fs.tstore.as_mut() {
            store.rescan();
        }
    }
    Ok(())
}

// Exempt: all released in exec_end_function_scan.
mcx::forget_safe_struct!(
    SetExprState<'_> { collation, returns_set, returns_tuple; flinfo, args, elided_func_state },
    FunctionScanPerFuncState<'_> { colcount, rowcount; setexpr, tupdesc, tstore, func_slot, funcparams },
    // arg_mcx: NonNull to an arena slot; the context value it points at is
    // dropped by the estate-reset callback (make_arg_ctx), never by this
    // struct — the old by-value field leaked its arena on every execution
    // (leak #3: "query-context teardown reclaims it" was wrong; reset/park
    // never touches child contexts).
    FunctionScanState<'_> { ss, simple, ordinality, ordinal, eflags, arg_mcx; funcstates, scratch },
);
