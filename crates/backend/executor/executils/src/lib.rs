// execUtils.c executor-state half. EState is the per-query resource owner
// (docs/no-drop.md): droppy resources live here by value, arena-resident
// nodes hold u32 handles. Query + per-tuple contexts are bump arenas.
#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::rc::Rc;
use alloc::vec::Vec;

use ::datum::Datum;
use ::mcx::{Mcx, McxOwned, MemoryContext, PgBox, PgVec};
use ::queryenvironment::QueryEnvironment;
use ::snapmgr::Snapshot;
use ::types_core::instrument::{
    AggregateInstrumentation, BitmapHeapScanInstrumentation, HashInstrumentation,
    IncrementalSortInfo, Instrumentation, RuntimeEaPipeline, TuplesortInstrumentation,
};
use ::types_core::CommandId;
use ::types_error::{PgError, PgResult};
use ::types_nodes::bitmapset::Bitmapset;
use ::types_nodes::list::NodeList;
use ::types_nodes::parsenodes::{RTEKind, RangeTblEntry};
use ::types_portal::params::{ParamBind, ParamExecData, ParamExternData};
use ::types_nodes::plannodes::PlannedStmt;
use ::types_rel::{AccessShareLock, NoLock, Relation};
use ::types_scan::ScanDirection;
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_tuple::TupleDescData;

pub fn init_seams() {}

macro_rules! p3 {
    ($($name:ident),+ $(,)?) => {$(
        #[derive(Debug, Clone, Copy)]
        pub struct $name(core::convert::Infallible);
    )+};
}
// Unconstructible placeholders: provably None until the owning unit lands.
p3!(
    ModifyTableP3,
);

/// C ExecRowMark (execnodes.h). The open relation is es_relations[rti-1]
/// (C stores the same pointer ExecGetRangeTableRelation returns); ermExtra
/// belongs to FDW rowmarks, loud upstream.
#[derive(Debug, Clone, Copy)]
#[allow(non_snake_case)]
pub struct ExecRowMark {
    pub relid: ::types_core::Oid,
    pub rti: u32,
    pub prti: u32,
    pub rowmarkId: u32,
    pub markType: ::types_nodes::plannodes::RowMarkType,
    pub strength: ::types_nodes::LockClauseStrength,
    pub waitPolicy: ::types_nodes::LockWaitPolicy,
    pub ermActive: bool,
    pub curCtid: ::types_tuple::ItemPointerData,
}

/// C `PlanState *` cell of es_subplanstates, type-erased against the
/// executils<->execmain crate cycle (execmain owns both sides of the cast).
#[derive(Debug, Clone, Copy)]
pub struct SubplanStateCell(pub core::ptr::NonNull<()>);

/// ExecSetParamPlan dispatch slot (nodeSubplan.c lives in execmain).
pub type SubplanHook =
    for<'a, 'mcx> unsafe fn(core::ptr::NonNull<()>, &'a mut EStateData<'mcx>) -> PgResult<()>;

/// One-tuple pull from an es_subplanstates cell (CteScanNext's
/// ExecProcNode(cteplanstate); dispatch lives in execmain).
pub type CteProcHook = for<'a, 'mcx> unsafe fn(
    SubplanStateCell,
    &'a mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>>;

pub type SubplanInitHook = for<'x> unsafe fn(
    core::ptr::NonNull<()>,
    types_nodes::Node<'x>,
    Option<execexpr::AggBind>,
) -> PgResult<core::ptr::NonNull<()>>;

/// `outer` carries the owning node's explicit outer row when it lives
/// outside es_tupleTable (WindowAgg/Agg node-local slots): the hook's
/// testexpr/LHS-projection evals bind it as the Outer slot, C's
/// econtext->ecxt_outertuple the owner set before ExecProject.
pub type SubplanEvalHook = for<'a, 'b, 'mcx> unsafe fn(
    core::ptr::NonNull<()>,
    &'a mut EStateData<'mcx>,
    EcxtId,
    Option<&'b mut SlotData<'mcx>>,
) -> PgResult<::datum::NullableDatum>;

/// C's CteScanState leader fields (cte_table/eof_cte), hoisted to the estate
/// keyed by cteParam: the leader/follower alias becomes an owned entry.
pub struct CteShared {
    pub tuplestore: ::tuplestore::Tuplestore,
    pub eof_cte: bool,
    /// Rows pulled from the CTE subplan; the materialize-once probe.
    pub fills: u32,
}

/// C's RecursiveUnionState tables + result rowtype, hoisted to the estate
/// keyed by wtParam (the C es_param_exec_vals rustate alias, owned).
pub struct WorkTableShared {
    pub working_table: ::tuplestore::Tuplestore,
    pub intermediate_table: ::tuplestore::Tuplestore,
    pub desc: Rc<TupleDescData<'static>>,
}

/// C ExecEvalParamExec's pending-initplan arm, hoisted to the owning node.
pub fn exec_eval_param_exec_params(
    estate: &mut EStateData<'_>,
    deps: &[u32],
) -> PgResult<()> {
    for &pid in deps {
        if estate.es_param_exec_vals[pid as usize].exec_plan {
            exec_set_param_plan(estate, pid)?;
        }
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn exec_set_param_plan(estate: &mut EStateData<'_>, pid: u32) -> PgResult<()> {
    let sstate = estate.es_param_subplans[pid as usize]
        .expect("pending PARAM_EXEC without an initplan SubPlanState");
    let hook = estate
        .es_subplan_hook
        .expect("pending PARAM_EXEC before execmain installed the subplan hook");
    // SAFETY: cell installed by execmain's ExecInitSubPlan on this estate.
    unsafe { hook(sstate.0, estate) }
}

pub fn with_subplan_compile_env<'mcx, R>(
    estate: &mut EStateData<'mcx>,
    f: impl FnOnce(Option<execexpr::SubplanCompileEnv>) -> R,
) -> R {
    with_subplan_compile_env_parent(estate, None, f)
}

/// The estate range table, 'static-restamped for SubplanCompileEnv.rtable
/// (plan-lived; the restamp stays behind the compile-scoped env).
pub fn subplan_env_rtable<'mcx>(
    estate: &EStateData<'mcx>,
) -> core::ptr::NonNull<[&'static RangeTblEntry<'static>]> {
    // SAFETY: lifetime restamp only; es_range_table lives in es_query_cxt.
    unsafe {
        core::mem::transmute::<
            core::ptr::NonNull<[&RangeTblEntry<'mcx>]>,
            core::ptr::NonNull<[&'static RangeTblEntry<'static>]>,
        >(core::ptr::NonNull::from(&estate.es_range_table[..]))
    }
}

/// [`with_subplan_compile_env`] under a SubqueryScan/CteScan parent: the
/// subplan's targetlist reaches EEOP_WHOLEROW's junk-filter build (C
/// ExecInitWholeRowVar's state->parent walk).
pub fn with_subplan_compile_env_parent<'mcx, R>(
    estate: &mut EStateData<'mcx>,
    parent_subplan_tlist: Option<&types_nodes::list::NodeList<'mcx>>,
    f: impl FnOnce(Option<execexpr::SubplanCompileEnv>) -> R,
) -> R {
    let rtable = subplan_env_rtable(estate);
    // SAFETY: 'static restamp of a plan-lived (es_query_cxt) node tree; it
    // stays behind the compile-scoped env.
    let parent_subplan_tlist = parent_subplan_tlist.map(|t| unsafe {
        core::mem::transmute::<
            core::ptr::NonNull<types_nodes::list::NodeList<'mcx>>,
            core::ptr::NonNull<types_nodes::list::NodeList<'static>>,
        >(core::ptr::NonNull::from(t))
    });
    // The env exists when SubPlans can appear (init hook set), when the
    // whole-row junk-filter leg needs the parent subplan tlist, or when the
    // plan reads any relation (EEOP_WHOLEROW eref aliasing rides env.rtable
    // and needs an RTE to exist); rtable-free plain compiles (SELECT 1) keep
    // the pre-env cost (C's ExprState.parent is free).
    let init = estate.es_subplan_init_hook;
    let env = (init.is_some()
        || parent_subplan_tlist.is_some()
        || !estate.es_range_table.is_empty())
    .then(|| {
        execexpr::SubplanCompileEnv {
            estate: core::ptr::NonNull::from(&mut *estate).cast(),
            init,
            agg: None,
            rtable: Some(rtable),
            parent_subplan_tlist,
        }
    });
    f(env)
}

pub fn with_ecxt_eval_slots<'mcx, R>(
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
    result: Option<ExecSlotId>,
    f: impl FnOnce(&mut execexpr::EvalSlots<'_, 'mcx>, Option<&mut SlotData<'mcx>>, Mcx<'mcx>) -> PgResult<R>,
) -> PgResult<R> {
    let mcx = estate.es_query_cxt;
    let (scan, inner, outer) = {
        let e = estate.ecxt(ecxt);
        (e.ecxt_scantuple, e.ecxt_innertuple, e.ecxt_outertuple)
    };
    let table: &mut [SlotData<'mcx>] = &mut estate.es_tupleTable;
    let ids = [scan, inner, outer, result];
    for (i, id) in ids.iter().enumerate() {
        if let Some(a) = id {
            assert!((a.0 as usize) < table.len(), "slot id out of range");
            for later in &ids[i + 1..] {
                assert!(Some(*a) != *later, "aliased slot ids in expression eval");
            }
        }
    }
    let base = table.as_mut_ptr();
    // SAFETY: indices bounds-checked and pairwise-distinct above, so the four
    // derived &mut are disjoint elements of one live slice.
    let get = |id: Option<ExecSlotId>| id.map(|i| unsafe { &mut *base.add(i.0 as usize) });
    let mut slots =
        execexpr::EvalSlots { scan: get(scan), inner: get(inner), outer: get(outer) };
    f(&mut slots, get(result), mcx)
}

#[cold]
#[inline(never)]
/// Public driver entry for owning nodes whose eval slots aren't ecxt-shaped
/// (nodeModifyTable's RETURNING projection): run one suspended SubPlan.
pub fn run_subplan_eval<'mcx>(
    sstate: core::ptr::NonNull<()>,
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
) -> PgResult<::datum::NullableDatum> {
    run_subplan_eval_hook(sstate, estate, ecxt, None)
}

/// `outer` must live outside es_tupleTable (the hook derives &mut table
/// entries while it is held).
fn run_subplan_eval_hook<'mcx>(
    sstate: core::ptr::NonNull<()>,
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
    outer: Option<&mut SlotData<'mcx>>,
) -> PgResult<::datum::NullableDatum> {
    let hook = estate
        .es_subplan_eval_hook
        .expect("SubPlan step before execmain installed the eval hook");
    // SAFETY: sstate was installed by execmain's ExecInitSubPlan on this estate.
    unsafe { hook(sstate, estate, ecxt, outer) }
}

pub fn exec_qual_with_subplans<'mcx>(
    state: Option<&mut execexpr::ExprState<'mcx>>,
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
) -> PgResult<bool> {
    let Some(state) = state else {
        return Ok(true);
    };
    // Quals evaluate in the per-tuple context (C ecxt_per_tuple_memory):
    // strict-bool programs never allocate, but arg-detoasting operators
    // (jsonb @> ...) scribble scratch through the frame's result mcx.
    // SAFETY: the per-tuple context object outlives the plan (reset-only).
    unsafe { state.arm_result_mcx_raw(estate.ecxt(ecxt).per_tuple_mcx()) };
    // C ExecEvalParamExec's pending-initplan arm: an expression can reference
    // an initplan's output param without carrying any SubPlan step of its own
    // (hash/merge keys, runtime index keys — the t30 sqlsmith interp.rs:84
    // site), so the suspension pump alone cannot cover it. Run the owed
    // initplans before evaluation, exactly like the per-node hoists.
    let deps = state.param_exec_deps();
    if !deps.is_empty() {
        exec_eval_param_exec_params(estate, deps)?;
    }
    let mut resume: Option<execexpr::Resume> = None;
    loop {
        let outcome = {
            let r = resume.take();
            let state = &mut *state;
            with_ecxt_eval_slots(estate, ecxt, None, move |slots, _, _| {
                execexpr::exec_qual_outcome(state, slots, r)
            })?
        };
        match outcome {
            execexpr::QualOutcome::Done(b) => return Ok(b),
            execexpr::QualOutcome::Suspended(s) => {
                let r = run_subplan_eval_hook(s.sstate, estate, ecxt, None)?;
                resume = Some(s.resume_with(r));
            }
        }
    }
}

/// C `ExecQualAndReset` with the tuple bound as the scan slot — the
/// IndexRecheck / lossy-fetch recheck / BitmapHeapRecheck shape shared by
/// nodeindexscan, nodeindexonlyscan, and nodebitmapheapscan. A qual carrying
/// SubPlans (indexqualorig with a subquery comparison value pushed into the
/// Index Cond) routes through the subplan driver — C's ExecQual recurses
/// into ExecEvalSubPlan; the decomposed interpreter pumps EEOP_SUBPLAN
/// suspensions instead.
pub fn exec_recheck_qual_and_reset<'mcx>(
    qual: Option<&mut execexpr::ExprState<'mcx>>,
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
    slot: ExecSlotId,
) -> PgResult<bool> {
    estate.ecxt_mut(ecxt).ecxt_scantuple = Some(slot);
    let Some(qual) = qual else {
        estate.ecxt_mut(ecxt).reset();
        return Ok(true);
    };
    // Quals evaluate in the per-tuple context (C ecxt_per_tuple_memory);
    // arg-detoasting operators scribble scratch through the result mcx.
    // SAFETY: the per-tuple context object outlives the plan (reset-only).
    unsafe { qual.arm_result_mcx_raw(estate.ecxt(ecxt).per_tuple_mcx()) };
    let passes = if qual.has_subplan() {
        exec_qual_with_subplans(Some(qual), estate, ecxt)?
    } else {
        // Pending-initplan arm for the plain path too: initplan params need
        // no SubPlan step in the qual (see the with_subplans entry points).
        let deps = qual.param_exec_deps();
        if !deps.is_empty() {
            exec_eval_param_exec_params(estate, deps)?;
        }
        let mut slots = execexpr::EvalSlots {
            scan: Some(estate.slot_mut(slot)),
            inner: None,
            outer: None,
        };
        execexpr::exec_qual(Some(qual), &mut slots)?
    };
    estate.ecxt_mut(ecxt).reset();
    Ok(passes)
}

/// [`exec_qual_with_subplans`] over an explicit outer slot living outside
/// es_tupleTable (grouped Agg's node-local group slot).
pub fn exec_qual_with_subplans_outer<'mcx>(
    state: Option<&mut execexpr::ExprState<'mcx>>,
    outer: &mut SlotData<'mcx>,
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
) -> PgResult<bool> {
    let Some(state) = state else {
        return Ok(true);
    };
    // SAFETY: the per-tuple context object outlives the plan (reset-only).
    unsafe { state.arm_result_mcx_raw(estate.ecxt(ecxt).per_tuple_mcx()) };
    // C ExecEvalParamExec's pending-initplan arm: an expression can reference
    // an initplan's output param without carrying any SubPlan step of its own
    // (hash/merge keys, runtime index keys — the t30 sqlsmith interp.rs:84
    // site), so the suspension pump alone cannot cover it. Run the owed
    // initplans before evaluation, exactly like the per-node hoists.
    let deps = state.param_exec_deps();
    if !deps.is_empty() {
        exec_eval_param_exec_params(estate, deps)?;
    }
    let mut resume: Option<execexpr::Resume> = None;
    loop {
        let outcome = {
            let r = resume.take();
            let mut slots =
                execexpr::EvalSlots { scan: None, inner: None, outer: Some(&mut *outer) };
            execexpr::exec_qual_outcome(state, &mut slots, r)?
        };
        match outcome {
            execexpr::QualOutcome::Done(b) => return Ok(b),
            execexpr::QualOutcome::Suspended(s) => {
                let r = run_subplan_eval_hook(s.sstate, estate, ecxt, Some(&mut *outer))?;
                resume = Some(s.resume_with(r));
            }
        }
    }
}

/// [`exec_eval_expr_with_subplans`] over an explicit outer slot living
/// outside es_tupleTable (Agg's node-local group/spill slots). The state's
/// result mcx must already be armed by the caller.
pub fn exec_eval_expr_with_subplans_outer<'mcx>(
    state: &mut execexpr::ExprState<'mcx>,
    outer: &mut SlotData<'mcx>,
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
) -> PgResult<::datum::NullableDatum> {
    // C ExecEvalParamExec's pending-initplan arm: an expression can reference
    // an initplan's output param without carrying any SubPlan step of its own
    // (hash/merge keys, runtime index keys — the t30 sqlsmith interp.rs:84
    // site), so the suspension pump alone cannot cover it. Run the owed
    // initplans before evaluation, exactly like the per-node hoists.
    let deps = state.param_exec_deps();
    if !deps.is_empty() {
        exec_eval_param_exec_params(estate, deps)?;
    }
    let mut resume: Option<execexpr::Resume> = None;
    loop {
        let outcome = {
            let r = resume.take();
            let mut slots =
                execexpr::EvalSlots { scan: None, inner: None, outer: Some(&mut *outer) };
            execexpr::exec_eval_expr_outcome(state, &mut slots, r)?
        };
        match outcome {
            execexpr::EvalOutcome::Done(nd) => return Ok(nd),
            execexpr::EvalOutcome::Suspended(s) => {
                let r = run_subplan_eval_hook(s.sstate, estate, ecxt, Some(&mut *outer))?;
                resume = Some(s.resume_with(r));
            }
        }
    }
}

/// [`exec_project_with_subplans`] over an explicit outer slot living outside
/// es_tupleTable (grouped Agg's node-local group slot).
pub fn exec_project_with_subplans_outer<'mcx>(
    state: &mut execexpr::ExprState<'mcx>,
    outer: &mut SlotData<'mcx>,
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
    result: ExecSlotId,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    state.arm_result_mcx(mcx);
    exectuples::exec_clear_tuple(estate.slot_mut(result), mcx);
    // C ExecEvalParamExec's pending-initplan arm: an expression can reference
    // an initplan's output param without carrying any SubPlan step of its own
    // (hash/merge keys, runtime index keys — the t30 sqlsmith interp.rs:84
    // site), so the suspension pump alone cannot cover it. Run the owed
    // initplans before evaluation, exactly like the per-node hoists.
    let deps = state.param_exec_deps();
    if !deps.is_empty() {
        exec_eval_param_exec_params(estate, deps)?;
    }
    let mut resume: Option<execexpr::Resume> = None;
    loop {
        let suspended = {
            let r = resume.take();
            let result_slot = estate.slot_mut(result);
            let mut slots =
                execexpr::EvalSlots { scan: None, inner: None, outer: Some(&mut *outer) };
            execexpr::exec_project_outcome(state, &mut slots, result_slot, r)?
        };
        match suspended {
            None => {
                exectuples::exec_store_virtual_tuple(estate.slot_mut(result));
                return Ok(());
            }
            Some(s) => {
                let r = run_subplan_eval_hook(s.sstate, estate, ecxt, Some(&mut *outer))?;
                resume = Some(s.resume_with(r));
            }
        }
    }
}

/// [`exec_eval_expr_with_subplans`] over an es_tupleTable slot bound as the
/// INNER input (hash-key evaluation with SubPlan keys; nodeHash.c /
/// nodeHashjoin.c ExecHashGetHashValue). The state's result mcx must already
/// be armed by the caller.
pub fn exec_eval_expr_with_subplans_hashkey<'mcx>(
    state: &mut execexpr::ExprState<'mcx>,
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
    slot_id: ExecSlotId,
) -> PgResult<::datum::NullableDatum> {
    let deps = state.param_exec_deps();
    if !deps.is_empty() {
        exec_eval_param_exec_params(estate, deps)?;
    }
    let mut resume: Option<execexpr::Resume> = None;
    loop {
        let outcome = {
            let r = resume.take();
            let slot = &mut estate.es_tupleTable[slot_id.0 as usize];
            let mut slots =
                execexpr::EvalSlots { scan: None, inner: Some(slot), outer: None };
            execexpr::exec_eval_expr_outcome(state, &mut slots, r)?
        };
        match outcome {
            execexpr::EvalOutcome::Done(nd) => return Ok(nd),
            execexpr::EvalOutcome::Suspended(s) => {
                let r = run_subplan_eval_hook(s.sstate, estate, ecxt, None)?;
                resume = Some(s.resume_with(r));
            }
        }
    }
}

pub fn exec_eval_expr_with_subplans<'mcx>(
    state: &mut execexpr::ExprState<'mcx>,
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
) -> PgResult<::datum::NullableDatum> {
    // C ExecEvalParamExec's pending-initplan arm: an expression can reference
    // an initplan's output param without carrying any SubPlan step of its own
    // (hash/merge keys, runtime index keys — the t30 sqlsmith interp.rs:84
    // site), so the suspension pump alone cannot cover it. Run the owed
    // initplans before evaluation, exactly like the per-node hoists.
    let deps = state.param_exec_deps();
    if !deps.is_empty() {
        exec_eval_param_exec_params(estate, deps)?;
    }
    let mut resume: Option<execexpr::Resume> = None;
    loop {
        let outcome = {
            let r = resume.take();
            let state = &mut *state;
            with_ecxt_eval_slots(estate, ecxt, None, move |slots, _, _| {
                execexpr::exec_eval_expr_outcome(state, slots, r)
            })?
        };
        match outcome {
            execexpr::EvalOutcome::Done(nd) => return Ok(nd),
            execexpr::EvalOutcome::Suspended(s) => {
                let r = run_subplan_eval_hook(s.sstate, estate, ecxt, None)?;
                resume = Some(s.resume_with(r));
            }
        }
    }
}

/// [`exec_eval_expr_with_subplans_inner_slot`] with the tuple bound as the
/// outer slot (mergeclause outer-side key evaluation).
pub fn exec_eval_expr_with_subplans_outer_slot<'mcx>(
    state: &mut execexpr::ExprState<'mcx>,
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
    outer: ExecSlotId,
) -> PgResult<::datum::NullableDatum> {
    let mut resume: Option<execexpr::Resume> = None;
    loop {
        let outcome = {
            let r = resume.take();
            let slot = &mut estate.es_tupleTable[outer.0 as usize];
            let mut slots =
                execexpr::EvalSlots { scan: None, inner: None, outer: Some(slot) };
            execexpr::exec_eval_expr_outcome(state, &mut slots, r)?
        };
        match outcome {
            execexpr::EvalOutcome::Done(nd) => return Ok(nd),
            execexpr::EvalOutcome::Suspended(s) => {
                let r = run_subplan_eval_hook(s.sstate, estate, ecxt, None)?;
                resume = Some(s.resume_with(r));
            }
        }
    }
}

/// [`exec_eval_expr_with_subplans`] over an explicit inner slot id — hash
/// key evaluation binds the input tuple as the inner slot (C ExecInitHash's
/// hash_expr with the parent planstate).
pub fn exec_eval_expr_with_subplans_inner_slot<'mcx>(
    state: &mut execexpr::ExprState<'mcx>,
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
    inner: ExecSlotId,
) -> PgResult<::datum::NullableDatum> {
    // C ExecEvalParamExec's pending-initplan arm: an expression can reference
    // an initplan's output param without carrying any SubPlan step of its own
    // (hash/merge keys, runtime index keys — the t30 sqlsmith interp.rs:84
    // site), so the suspension pump alone cannot cover it. Run the owed
    // initplans before evaluation, exactly like the per-node hoists.
    let deps = state.param_exec_deps();
    if !deps.is_empty() {
        exec_eval_param_exec_params(estate, deps)?;
    }
    let mut resume: Option<execexpr::Resume> = None;
    loop {
        let outcome = {
            let r = resume.take();
            let slot = &mut estate.es_tupleTable[inner.0 as usize];
            let mut slots =
                execexpr::EvalSlots { scan: None, inner: Some(slot), outer: None };
            execexpr::exec_eval_expr_outcome(state, &mut slots, r)?
        };
        match outcome {
            execexpr::EvalOutcome::Done(nd) => return Ok(nd),
            execexpr::EvalOutcome::Suspended(s) => {
                let r = run_subplan_eval_hook(s.sstate, estate, ecxt, None)?;
                resume = Some(s.resume_with(r));
            }
        }
    }
}

pub fn exec_project_with_subplans<'mcx>(
    state: &mut execexpr::ExprState<'mcx>,
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
    result: ExecSlotId,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    state.arm_result_mcx(mcx);
    exectuples::exec_clear_tuple(estate.slot_mut(result), mcx);
    // C ExecEvalParamExec's pending-initplan arm: an expression can reference
    // an initplan's output param without carrying any SubPlan step of its own
    // (hash/merge keys, runtime index keys — the t30 sqlsmith interp.rs:84
    // site), so the suspension pump alone cannot cover it. Run the owed
    // initplans before evaluation, exactly like the per-node hoists.
    let deps = state.param_exec_deps();
    if !deps.is_empty() {
        exec_eval_param_exec_params(estate, deps)?;
    }
    let mut resume: Option<execexpr::Resume> = None;
    loop {
        let suspended = {
            let r = resume.take();
            let state = &mut *state;
            with_ecxt_eval_slots(estate, ecxt, Some(result), move |slots, rslot, _| {
                execexpr::exec_project_outcome(
                    state,
                    slots,
                    rslot.expect("projection result slot"),
                    r,
                )
            })?
        };
        match suspended {
            None => {
                exectuples::exec_store_virtual_tuple(estate.slot_mut(result));
                return Ok(());
            }
            Some(s) => {
                let r = run_subplan_eval_hook(s.sstate, estate, ecxt, None)?;
                resume = Some(s.resume_with(r));
            }
        }
    }
}

/// C JunkFilter (execnodes.h); construction/filtering live in execjunk.
#[allow(non_snake_case)]
pub struct JunkFilter<'mcx> {
    pub jf_cleanTupType: Rc<TupleDescData<'mcx>>,
    /// One entry per clean attribute: 1-based resno in the dirty tuple, 0 = NULL.
    pub jf_cleanMap: &'mcx [i16],
    pub jf_resultSlot: ExecSlotId,
}

/// C ResultRelInfo slice; the open relation is es_relations[rti-1].
#[derive(Debug, Clone, Copy)]
#[allow(non_snake_case)]
pub struct ResultRelInfo {
    pub ri_RangeTableIndex: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EcxtId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecSlotId(pub u32);

/// Operator→operator page-batch seam for the lane executor (lane-executor-v2
/// design §Architecture 1). A source stages a batch, then serves individual
/// rows into an outer slot with its scan/build qual applied; the batched lane
/// operators (fused agg, and — Phase 1 — the SeqScan-owning lane driver)
/// consume batches through this trait instead of the per-tuple node recursion.
/// Formerly `nodeagg::AggBatchSource` (agg-scoped); promoted here so both the
/// agg consumer and the execmain lane driver share one seam. `nodeagg`
/// re-exports it as `AggBatchSource` so the fused-agg path is unchanged.
pub trait BatchSource<'mcx> {
    /// Stage the next page batch; 0 = input exhausted.
    fn next_batch(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<u32>;
    /// Store staged tuple `i` into the outer slot and apply the scan qual;
    /// false = filtered out.
    fn fetch_tuple(&mut self, i: u32, estate: &mut EStateData<'mcx>) -> PgResult<bool>;
    fn outer_slot(&self) -> ExecSlotId;
    fn has_qual(&self) -> bool;
    /// True only when `next_batch` counts VISIBLE, qual-passing rows (the
    /// storeless drain never calls `fetch_tuple`). Sources resolving
    /// visibility or quals at fetch time must return false.
    fn storeless_ok(&self) -> bool {
        !self.has_qual()
    }
    /// Batched qual census over the staged batch: VISIBLE rows passing the
    /// qual, any per-row-only rows resolved inside. None = the per-row drain
    /// owns the batch. Only sources whose census preserves per-row qual
    /// semantics (non-erroring kernel quals) may return Some.
    fn qualifying_count(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        _n: u32,
    ) -> PgResult<Option<u32>> {
        Ok(None)
    }
    /// Fetch-dead skip snapshot of the CURRENT staged batch: a CLEARED bit
    /// is a position `fetch_tuple` rejects with no observable effect (the
    /// staged qual bitmap's verdict — definitive even for hybrid requal
    /// quals, whose survivors carry SET bits and re-check per row), so a
    /// batch drain may word-skip cleared positions without the call
    /// (`exectuples::for_each_live` — the wordskip lane's shared idiom).
    /// Default `None` = no live bitmap; every position must be fetched.
    fn skip_words(&self) -> Option<[u64; ::exectuples::SOA_BM_WORDS]> {
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuxCxtId(pub u32);

pub type ExprContextCallbackFunction = for<'a> fn(Mcx<'a>, Datum);

#[derive(Debug, Clone, Copy)]
pub struct ExprContextCB {
    pub function: ExprContextCallbackFunction,
    pub arg: Datum,
}

#[derive(Debug)]
pub struct ExprContextData<'mcx> {
    // P1 address-stability constraint: es_exprcontexts relocates on growth,
    // and compiled programs arm raw NonNull<MemoryContext> pointers at this
    // context (arm_result_mcx_raw). The arena box pins the struct so those
    // pointers survive every later create_expr_context.
    per_tuple: PgBox<'mcx, MemoryContext>,
    pub ecxt_scantuple: Option<ExecSlotId>,
    pub ecxt_innertuple: Option<ExecSlotId>,
    pub ecxt_outertuple: Option<ExecSlotId>,
    pub ecxt_param_exec_vals: Option<core::ptr::NonNull<ParamExecData>>,
    pub ecxt_param_list_info: Option<&'mcx [ParamExternData]>,
    pub ecxt_aggvalues: PgVec<'mcx, Datum>,
    pub ecxt_aggnulls: PgVec<'mcx, bool>,
    pub caseValue_datum: Datum,
    pub caseValue_isNull: bool,
    pub domainValue_datum: Datum,
    pub domainValue_isNull: bool,
    callbacks: PgVec<'mcx, ExprContextCB>,
}

impl<'mcx> ExprContextData<'mcx> {
    fn new(per_query: Mcx<'mcx>, per_tuple: PgBox<'mcx, MemoryContext>) -> Self {
        ExprContextData {
            per_tuple,
            ecxt_scantuple: None,
            ecxt_innertuple: None,
            ecxt_outertuple: None,
            ecxt_param_exec_vals: None,
            ecxt_param_list_info: None,
            ecxt_aggvalues: PgVec::new_in(per_query),
            ecxt_aggnulls: PgVec::new_in(per_query),
            caseValue_datum: Datum::null(),
            caseValue_isNull: true,
            domainValue_datum: Datum::null(),
            domainValue_isNull: true,
            callbacks: PgVec::new_in(per_query),
        }
    }

    #[inline]
    pub fn per_tuple_mcx(&self) -> Mcx<'_> {
        self.per_tuple.mcx()
    }

    /// `ResetExprContext`: THE per-row arena reset — bump rewind only.
    #[inline]
    pub fn reset(&mut self) {
        self.per_tuple.reset();
    }

    pub fn register_shutdown_callback(
        &mut self,
        function: ExprContextCallbackFunction,
        arg: Datum,
    ) {
        self.callbacks.push(ExprContextCB { function, arg });
    }

    pub fn unregister_shutdown_callback(
        &mut self,
        function: ExprContextCallbackFunction,
        arg: Datum,
    ) {
        #[allow(unpredictable_function_pointer_comparisons)]
        self.callbacks
            .retain(|cb| !(cb.function == function && cb.arg == arg));
    }

    /// `ShutdownExprContext`: newest-first; `is_commit=false` only empties.
    pub fn shutdown(&mut self, is_commit: bool) {
        if self.callbacks.is_empty() {
            return;
        }
        while let Some(cb) = self.callbacks.pop() {
            if is_commit {
                (cb.function)(self.per_tuple.mcx(), cb.arg);
            }
        }
    }

    /// `ReScanExprContext(econtext)`.
    pub fn rescan(&mut self) {
        self.shutdown(true);
        self.per_tuple.reset();
    }
}

/// Estate-owned extern-param buffer (ptr, len); arena-backed, no drop.
#[derive(Clone, Copy)]
pub struct ParamStable(pub core::ptr::NonNull<ParamExternData>, pub u32);

pub struct EStateData<'mcx> {
    pub es_query_cxt: Mcx<'mcx>,
    pub es_direction: ScanDirection,
    pub es_snapshot: Option<Snapshot>,
    pub es_crosscheck_snapshot: Option<Snapshot>,
    pub es_range_table: PgVec<'mcx, &'mcx RangeTblEntry<'mcx>>,
    pub es_range_table_size: u32,
    pub es_relations: PgVec<'mcx, Option<Relation<'mcx>>>,
    pub es_rowmarks: PgVec<'mcx, Option<ExecRowMark>>,
    pub es_rteperminfos: Option<&'mcx NodeList<'mcx>>,
    pub es_plannedstmt: Option<&'mcx PlannedStmt<'mcx>>,
    /// Initial-pruning results, one entry per PlannedStmt.partPruneInfos
    /// element (None = no initial pruning ran for that pruneinfo).
    pub es_part_prune_results: PgVec<'mcx, Option<Bitmapset<'mcx>>>,
    pub es_unpruned_relids: Bitmapset<'mcx>,
    pub es_junkFilter: Option<JunkFilter<'mcx>>,
    pub es_output_cid: CommandId,
    pub es_result_relations: PgVec<'mcx, Option<ResultRelInfo>>,
    pub es_opened_result_relations: PgVec<'mcx, ResultRelInfo>,
    pub es_tuple_routing_result_relations: PgVec<'mcx, ResultRelInfo>,
    pub es_trig_target_relations: PgVec<'mcx, ResultRelInfo>,
    pub es_insert_pending_result_relations: PgVec<'mcx, ResultRelInfo>,
    pub es_insert_pending_modifytables: PgVec<'mcx, ModifyTableP3>,
    pub es_param_list_info: Option<&'mcx [ParamExternData]>,
    // Executor-skeleton stable extern-param images (no C counterpart):
    // compiled ParamExtern steps resolve into this estate-owned buffer
    // instead of the portal's per-EXECUTE array; restamped on skeleton reuse.
    pub es_param_stable: Option<ParamStable>,
    pub es_param_exec_vals: PgVec<'mcx, ParamExecData>,
    pub es_queryEnv: Option<&'mcx QueryEnvironment<'mcx>>,
    pub es_tupleTable: PgVec<'mcx, SlotData<'mcx>>,
    pub es_processed: u64,
    pub es_total_processed: u64,
    pub es_top_eflags: i32,
    pub es_instrument: i32,
    // Keyed by plan_node_id (C: per-PlanState); empty when es_instrument == 0.
    pub es_instrumentation: PgVec<'mcx, Instrumentation>,
    // EA-on-morsels refusal transparency (docs/design/ea-morsels.md §6): one
    // record per (node, arm) the runtime admission walk refused while
    // instrumented AND armed. Empty on every unarmed/uninstrumented path —
    // the EXPLAIN emission gate is "records exist", which keeps unarmed EA
    // output byte-identical to C.
    pub es_runtime_ea_refusals: PgVec<'mcx, RuntimeEaRefusal>,
    // EA-on-morsels pipeline reports (ea-morsels.md §4): one per engaged
    // runtime pipeline phase, pushed by the arm's leader merge on a clean
    // Completed outcome. Same emission-gate law as the refusals: empty on
    // every unarmed/uninstrumented path.
    pub es_runtime_ea_pipelines: PgVec<'mcx, RuntimeEaPipeline>,
    // EXPLAIN (ENGINE) per-node engine attribution (single-executor Phase
    // 0.2): records exist iff ExecutorStart saw EXEC_FLAG_ENGINE_REPORT —
    // `engine_capture()` derives that directly from `es_top_eflags` (stored
    // unconditionally anyway), so the executor entry path never tests the
    // flag (se-entrycost). The emission gate is identical to the EA records
    // above — es_engine_events stays empty on every non-ENGINE path, so
    // default EXPLAIN output is untouched.
    pub es_engine_events: PgVec<'mcx, EngineEvent>,
    // (plan_node_id, metrics); C's AggState fields, hoisted for the Plan walk.
    pub es_agg_instrumentation: PgVec<'mcx, (i32, AggregateInstrumentation)>,
    pub es_sort_instrumentation: PgVec<'mcx, (i32, TuplesortInstrumentation)>,
    pub es_incsort_instrumentation: PgVec<'mcx, (i32, IncrementalSortInfo)>,
    pub es_hash_instrumentation: PgVec<'mcx, (i32, HashInstrumentation)>,
    /// (plan_node_id, nsearches); C's IndexScanInstrumentation, hoisted.
    pub es_index_instrumentation: PgVec<'mcx, (i32, u64)>,
    /// (plan_node_id, stats); C's BitmapHeapScanState.stats, hoisted for the
    /// parallel-worker report (leader-side EXPLAIN reads the planstate).
    pub es_bitmap_instrumentation: PgVec<'mcx, (i32, BitmapHeapScanInstrumentation)>,
    /// One entry per parallel worker (C PlanState.worker_instrument + the
    /// per-node shared_info arrays, hoisted; execParallel retrieve fills it).
    pub es_worker_instrument: PgVec<'mcx, WorkerInstr<'mcx>>,
    // Node-owned resettable contexts (C's node-local AllocSets): droppy, so
    // they live in the estate owner; nodes hold AuxCxtId (docs/no-drop.md).
    es_aux_contexts: PgVec<'mcx, PgBox<'mcx, MemoryContext>>,
    pub es_finished: bool,
    es_exprcontexts: PgVec<'mcx, Option<ExprContextData<'mcx>>>,
    pub es_subplanstates: PgVec<'mcx, SubplanStateCell>,
    /// paramid -> initplan SubPlanState (C's ParamExecData.execPlan pointer).
    pub es_param_subplans: PgVec<'mcx, Option<SubplanStateCell>>,
    pub es_subplan_hook: Option<SubplanHook>,
    pub es_subplan_init_hook: Option<SubplanInitHook>,
    pub es_subplan_eval_hook: Option<SubplanEvalHook>,
    /// Droppy SubPlan expr states: explicit take+drop at executor end.
    pub es_subplan_expr_states:
        PgVec<'mcx, (core::ptr::NonNull<()>, unsafe fn(core::ptr::NonNull<()>))>,
    /// cteParam -> shared CTE state; the leader installs, followers replay.
    pub es_cte_shared: PgVec<'mcx, Option<CteShared>>,
    pub es_worktable_shared: PgVec<'mcx, Option<WorkTableShared>>,
    pub es_cte_proc_hook: Option<CteProcHook>,
    /// wCTE ModifyTable subplan roots (es_subplanstates cells), init order;
    /// ExecPostprocessPlan drains them in reverse (C builds with lcons).
    pub es_auxmodifytables: PgVec<'mcx, SubplanStateCell>,
    es_per_tuple_exprcontext: Option<EcxtId>,
    pub es_sourceText: Option<&'mcx str>,
    pub es_use_parallel_mode: bool,
    /// GL-FIXCOUNT-2: this execution's plan tree HAS been parallel-wired —
    /// some scan node received its shared `ParallelTableScanDescShared`
    /// through `ExecParallelInitializeDSM` (leader) or
    /// `ExecParallelInitializeWorker` (worker). Published by the scan-node
    /// initializers, read by the scan-descriptor-open chokepoint
    /// (`nodeseqscan::open_scandesc`): once wiring has happened, a scan
    /// state over a `parallel_aware` plan node that holds NO wiring is a
    /// SECOND, private, whole-relation scan of a relation this participant
    /// is already dividing through the shared cursor — every participant
    /// would then produce the global aggregate and the finalize would sum
    /// them. Distinguishes that from the legitimate un-wired case (a Gather
    /// that never initialized a DSM because `es_use_parallel_mode` was
    /// false, where the leader is the only participant and a private serial
    /// descriptor is correct).
    pub es_parallel_scan_wired: bool,
    pub es_parallel_workers_to_launch: i32,
    pub es_parallel_workers_launched: i32,
    pub es_jit_flags: i32,
    // C es_jit (JitContext): this execution's copy-and-patch kernels +
    // instrumentation; blocks released in teardown (droppy owner).
    pub es_jit_blocks: Vec<::jit_deform::CodeBlock>,
    pub es_jit_instr: ::jit_deform::JitInstrumentation,
    // C EPQState.relsubs_*, hosted on the (shared) estate — no child EState.
    pub es_epq: Option<EpqSubs<'mcx>>,
    // C `es_epq_active != NULL`; scan nodes select their EPQ variant on it.
    pub es_epq_active: bool,
    /// se-delegtax SH-F: the row-mode LEAF fast-admit byte — true iff the
    /// lane master GUC is on AND no per-execution diagnostics are armed
    /// (es_epq_active false, es_instrument == 0, no ENGINE capture, lane
    /// stats/trace disarmed). Maintained by lanev2::refresh_lane_leaf_fast
    /// at ExecutorStart-end and the EPQ toggle sites; every input except
    /// EPQ is per-execution static (instr growth sites are all gated on
    /// es_instrument != 0, set before InitPlan). When true, a leaf pull
    /// verdict admits with ONE byte load + the inline direction check —
    /// every tick/capture-asserting channel has diagnostics armed and
    /// therefore runs the full slow path, so accounting fidelity is
    /// structurally unaffected. Default false (EPQ/worker estates stay on
    /// the slow path).
    pub es_lane_leaf_fast: bool,
    /// GL-ROWMODE-1: per-execution dedup bitmask for the row-mode OWNED
    /// engagement trace (bit = lanev2 ShapeClass discriminant; bit 63 is the
    /// instrumented-execution note). The OWNED verdict/tick cadence is per
    /// pull (row-mode law §3.3) and stays per pull — but the TRACE line is
    /// emitted at most once per class per execution (per worker: each worker
    /// dedups on its own estate). A per-pull trace on a delegation leaf that
    /// sits on a per-inner-row pull path (a merge join's Materialize inner
    /// re-pulled across mark/restore) is one format!+stderr write per inner
    /// row per worker: a trace-armed boot turned a ~50ms statement into
    /// ~10-17s and masqueraded as an engine collapse in a measurement
    /// vehicle. Written only by lanev2::lane_trace_owned_once /
    /// refresh_lane_leaf_fast under an armed trace; stays 0 at default
    /// config. Estate-resident by the TLS-census-zero law.
    pub es_lane_trace_owned: u64,
    /// wave-9 WS-AI (forward-pull cursors inc-1, contract §3 / lane-cursors.md
    /// §1): the per-run emission budget of a count-limited forward SELECT
    /// run — `Some(count)` iff `PGRUST_LANE_V2_CURSORS` is ON and this
    /// ExecutorRun is the §3.1 count-exact suspension shape (knob-ON,
    /// count != 0, forward, SELECT, serial). Written UNCONDITIONALLY at
    /// every `execute_plan` entry (compute = `lanev2::cursor_run_budget_
    /// install`), so it is per-run by construction, nested-run-safe (each
    /// run owns its estate) and unwind-safe with no guard. Estate-resident
    /// rather than TLS by the TLS-census-zero law (wave-9 contract §8 law
    /// 8); the `es_processed`/`es_direction` per-run-state precedent.
    /// Knob-OFF and count-0 runs always read None. First consumer = the
    /// inc-1b park/settle walker (lanev2/batch_source.rs glue).
    pub es_cursor_run_budget: Option<u64>,
    /// wave-9.5 WS-AI (cursors inc-1b, lane-cursors.md §2 — EX-AI-2, the
    /// EX-AI-1 estate-surface shape, recorded for board ratification in
    /// notes/se-wave9-ai.md): the previous budgeted run SETTLED a
    /// lane-staged claim (park record node-resident); the next
    /// `execute_plan` entry must run the resume walk before the first pull
    /// touches staged state. Set only by the knob-ON settle walker; cleared
    /// at every resume; knob-OFF world always reads false (one predictable
    /// per-run branch). Estate-resident by the same TLS-census-zero
    /// argument as `es_cursor_run_budget` above.
    pub es_lane_cursor_parked: bool,
    /// wave-9.5 WS-AJ (SPI Stage-A seam, docs/design/lane-spi.md §1/§3;
    /// worklog notes/se-spi-stage-a.md): the per-run emission budget of a
    /// tcount-limited SPI-statement run — `Some(tcount)` iff
    /// `PGRUST_LANE_V2_SPI` is ON and this ExecutorRun is a count-limited
    /// `CommandDest::Spi` shape (knob-ON, tcount != 0, forward, SELECT,
    /// serial): `_SPI_pquery`'s count-exact STOP or an SPI portal fetch
    /// (the RESUMABLE producer — notes/se-spi-stage-a.md §8). Written
    /// UNCONDITIONALLY at every `execute_plan` entry (compute =
    /// `lanev2::spi_run_budget_install`) — the `es_cursor_run_budget`
    /// idiom verbatim: per-run by construction, nested-run-safe (a nested
    /// SPI statement owns its own estate) and unwind-safe with no guard.
    /// Consumer: the settle walk below the drive loop retires lane-staged
    /// claims at the count-limited stop, BEFORE ExecutorFinish/End reach
    /// the plancache release points (lane-spi.md INVARIANT 5), and its
    /// parked result arms `es_lane_cursor_parked` (the shared WS-AI
    /// resume signal) so the portal-fetch producer's next run
    /// repossesses. Estate-resident by the same TLS-census-zero argument
    /// as the two fields above. Knob-OFF and tcount-0 runs always read
    /// None.
    pub es_spi_run_budget: Option<u64>,
}

/// One worker's instrumentation snapshot: `instrument` is indexed by
/// plan_node_id; the side tables are (plan_node_id, data) pairs.
/// `node_ids` lists the Gather subtree's plan_node_ids: C attaches
/// worker_instrument only to planstates under the Gather
/// (ExecParallelRetrieveInstrumentation walks that subtree), so nodes
/// outside it must not grow a Workers group in EXPLAIN.
pub struct WorkerInstr<'mcx> {
    pub node_ids: PgVec<'mcx, i32>,
    pub instrument: PgVec<'mcx, Instrumentation>,
    pub sort: PgVec<'mcx, (i32, TuplesortInstrumentation)>,
    pub incsort: PgVec<'mcx, (i32, IncrementalSortInfo)>,
    pub agg: PgVec<'mcx, (i32, AggregateInstrumentation)>,
    pub hash: PgVec<'mcx, (i32, HashInstrumentation)>,
    pub index: PgVec<'mcx, (i32, u64)>,
    pub bitmap: PgVec<'mcx, (i32, BitmapHeapScanInstrumentation)>,
}

// C ExecAuxRowMark's junk attnos, keyed by markType: an EPQ recheck
// re-fetches a non-locked source rel's row by ctid (ROW_MARK_REFERENCE) or
// re-returns the wholerow junk datum (ROW_MARK_COPY) from origslot.
#[derive(Clone, Copy, Debug)]
pub enum EpqRowMarkFetch {
    Reference { ctid_attno: i16 },
    Copy { whole_attno: i16 },
}

/// The ONE EPQ state store: C `EPQState`'s relsubs_* arrays, held by the
/// EPQ owner (ModifyTable/LockRows) and swapped into `EStateData::es_epq`
/// only for a recheck's duration — nested EPQ is safe because each owner
/// holds its own copy (no child EState; the shared-estate capture-model
/// decision and the field-by-field C mapping live in
/// docs/design/lane-epq.md §3/§4). WS-U wave-5: shape FROZEN — inc-5's
/// lane-hosted rechecks consume these same fields as captured-singleton
/// source state; no WS extends this struct during the migration.
pub struct EpqSubs<'mcx> {
    pub relsubs_slot: PgVec<'mcx, Option<ExecSlotId>>,
    pub relsubs_done: PgVec<'mcx, bool>,
    pub relsubs_blocked: PgVec<'mcx, bool>,
    /// C EPQState.relsubs_rowmark: non-locking aux rowmarks by rti-1.
    pub relsubs_rowmark: PgVec<'mcx, Option<EpqRowMarkFetch>>,
    /// C EPQState.origslot (EvalPlanQualSetSlot): the plan row under recheck.
    pub origslot: Option<ExecSlotId>,
}

/// `EvalPlanQualInit` relsubs alloc for one EPQ owner (ModifyTable/LockRows),
/// deferred to first use; `result_rti` starts blocked (C EvalPlanQualStart).
pub fn ensure_epq_subs<'a, 'mcx>(
    subs: &'a mut Option<EpqSubs<'mcx>>,
    mcx: ::mcx::Mcx<'mcx>,
    rtsize: usize,
    result_rti: u32,
) -> &'a mut EpqSubs<'mcx> {
    if subs.is_none() {
        debug_assert!(result_rti >= 1 && result_rti as usize <= rtsize);
        let mut relsubs_slot = PgVec::new_in(mcx);
        relsubs_slot.resize(rtsize, None);
        let mut relsubs_done = PgVec::new_in(mcx);
        relsubs_done.resize(rtsize, false);
        let mut relsubs_blocked = PgVec::new_in(mcx);
        relsubs_blocked.resize(rtsize, false);
        relsubs_blocked[(result_rti - 1) as usize] = true;
        relsubs_done[(result_rti - 1) as usize] = true;
        let mut relsubs_rowmark = PgVec::new_in(mcx);
        relsubs_rowmark.resize(rtsize, None);
        *subs = Some(EpqSubs {
            relsubs_slot,
            relsubs_done,
            relsubs_blocked,
            relsubs_rowmark,
            origslot: None,
        });
    }
    subs.as_mut().expect("just ensured")
}

/// EA-on-morsels refusal record (docs/design/ea-morsels.md §6): the runtime
/// admission walk's verdict for an instrumented, armed node that did not
/// engage. Static vocabulary only — reasons are the walk's own refuse
/// strings / RefuseReason names; never formatted at record time.
#[derive(Clone, Copy, Debug)]
pub struct RuntimeEaRefusal {
    pub plan_node_id: i32,
    pub arm: &'static str,
    pub reason: &'static str,
}

/// Which engine owned a plan node's execution (the EXPLAIN (ENGINE)
/// vocabulary; single-executor migration Phase 0.2). No row-mode variant by
/// integration-contract ruling 1e: row-mode-hosted nodes report `Lane` with
/// their ShapeClass name as the class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EngineKind {
    /// Serial lane-v2 push pipeline owns the node.
    Lane,
    /// Volcano row spine ran it (detail carries the refusal reason, "" if
    /// the node was never offered to a lane).
    Spine,
    /// Spine refusal whose reason is admission-economics-fused-drive — the
    /// legacy fused batch arm owns the shape (displayed "spine/fused-arm").
    FusedArm,
    /// Morsel-runtime arm engaged (pipeline identity via RuntimeEaPipeline).
    Runtime,
}

/// One per-node engine attribution record (EXPLAIN (ENGINE); emission-gate
/// law identical to RuntimeEaRefusal: empty unless EXEC_FLAG_ENGINE_REPORT).
#[derive(Clone, Copy, Debug)]
pub struct EngineEvent {
    pub plan_node_id: i32,
    pub engine: EngineKind,
    /// ShapeClass::name() / router ArmClass name — static vocabulary only.
    pub class: &'static str,
    /// RefuseReason::name() or an arm refuse-string; "" for owned.
    pub detail: &'static str,
}

impl<'mcx> EStateData<'mcx> {
    /// Record a runtime-EA refusal (cold: instrumented+armed refusals only).
    /// Dedup on (node, arm): refused shapes re-walk admission per call and
    /// must not accrete duplicate lines; first reason wins (it is the
    /// admission walk's first failing gate, the one C-order determinism
    /// gives us).
    #[cold]
    pub fn runtime_ea_record_refusal(
        &mut self,
        plan_node_id: i32,
        arm: &'static str,
        reason: &'static str,
    ) {
        if self
            .es_runtime_ea_refusals
            .iter()
            .any(|r| r.plan_node_id == plan_node_id && r.arm == arm)
        {
            return;
        }
        self.es_runtime_ea_refusals.push(RuntimeEaRefusal { plan_node_id, arm, reason });
    }

    /// EXPLAIN (ENGINE) capture armed for this execution? Derived from
    /// `es_top_eflags` (the one word ExecutorStart stores regardless), so
    /// arming costs the default executor entry path zero instructions
    /// (se-entrycost); the flag test runs only at the lanev2 verdict
    /// chokepoints that gate their capture arm on this.
    #[inline]
    pub fn engine_capture(&self) -> bool {
        self.es_top_eflags & ::types_slot::EXEC_FLAG_ENGINE_REPORT != 0
    }

    /// Record an engine attribution (cold: ENGINE-capture paths only).
    /// Dedup on (plan_node_id, class); first record wins — it is the
    /// memoized verdict / the admission walk's first failing gate, matching
    /// `runtime_ea_record_refusal`'s determinism law. Linear dedup scan:
    /// bounded by plan size × classes, ENGINE-diagnostics-only by the
    /// emission gate.
    #[cold]
    pub fn engine_record(
        &mut self,
        plan_node_id: i32,
        engine: EngineKind,
        class: &'static str,
        detail: &'static str,
    ) {
        if self
            .es_engine_events
            .iter()
            .any(|e| e.plan_node_id == plan_node_id && e.class == class)
        {
            return;
        }
        self.es_engine_events.push(EngineEvent { plan_node_id, engine, class, detail });
    }

    /// `InstrCountFiltered1` (execnodes.h); idx is the node's
    /// es_instrumentation slot, None when not instrumented.
    #[inline]
    pub fn instr_count_filtered1(&mut self, idx: Option<u32>) {
        if let Some(ix) = idx {
            self.es_instrumentation[ix as usize].nfiltered1 += 1.0;
        }
    }

    /// `InstrCountFiltered2` (execnodes.h).
    #[inline]
    pub fn instr_count_filtered2(&mut self, idx: Option<u32>) {
        if let Some(ix) = idx {
            self.es_instrumentation[ix as usize].nfiltered2 += 1.0;
        }
    }

    /// C `scan->instrument->nsearches`, republished per node; ANALYZE-only.
    #[cold]
    pub fn instr_set_index_nsearches(&mut self, plan_node_id: i32, nsearches: u64) {
        for e in self.es_index_instrumentation.iter_mut() {
            if e.0 == plan_node_id {
                e.1 = nsearches;
                return;
            }
        }
        self.es_index_instrumentation.push((plan_node_id, nsearches));
    }

    /// Current bitmap heap scan page stats, republished per node; ANALYZE +
    /// parallel only.
    #[cold]
    pub fn instr_set_bitmap_stats(
        &mut self,
        plan_node_id: i32,
        stats: BitmapHeapScanInstrumentation,
    ) {
        for e in self.es_bitmap_instrumentation.iter_mut() {
            if e.0 == plan_node_id {
                e.1 = stats;
                return;
            }
        }
        self.es_bitmap_instrumentation.push((plan_node_id, stats));
    }

    pub fn new_in(mcx: Mcx<'mcx>) -> Self {
        EStateData {
            es_query_cxt: mcx,
            es_direction: ScanDirection::ForwardScanDirection,
            es_snapshot: None,
            es_crosscheck_snapshot: None,
            es_range_table: PgVec::new_in(mcx),
            es_range_table_size: 0,
            es_relations: PgVec::new_in(mcx),
            es_rowmarks: PgVec::new_in(mcx),
            es_rteperminfos: None,
            es_plannedstmt: None,
            es_part_prune_results: PgVec::new_in(mcx),
            es_unpruned_relids: Bitmapset::empty(),
            es_junkFilter: None,
            es_output_cid: 0,
            es_result_relations: PgVec::new_in(mcx),
            es_opened_result_relations: PgVec::new_in(mcx),
            es_tuple_routing_result_relations: PgVec::new_in(mcx),
            es_trig_target_relations: PgVec::new_in(mcx),
            es_insert_pending_result_relations: PgVec::new_in(mcx),
            es_insert_pending_modifytables: PgVec::new_in(mcx),
            es_param_list_info: None,
            es_param_stable: None,
            es_param_exec_vals: PgVec::new_in(mcx),
            es_queryEnv: None,
            es_tupleTable: PgVec::new_in(mcx),
            es_processed: 0,
            es_total_processed: 0,
            es_top_eflags: 0,
            es_instrument: 0,
            es_lane_leaf_fast: false,
            es_lane_trace_owned: 0,
            es_cursor_run_budget: None,
            es_lane_cursor_parked: false,
            es_spi_run_budget: None,
            es_instrumentation: PgVec::new_in(mcx),
            es_runtime_ea_refusals: PgVec::new_in(mcx),
            es_runtime_ea_pipelines: PgVec::new_in(mcx),
            es_engine_events: PgVec::new_in(mcx),
            es_agg_instrumentation: PgVec::new_in(mcx),
            es_sort_instrumentation: PgVec::new_in(mcx),
            es_incsort_instrumentation: PgVec::new_in(mcx),
            es_hash_instrumentation: PgVec::new_in(mcx),
            es_index_instrumentation: PgVec::new_in(mcx),
            es_bitmap_instrumentation: PgVec::new_in(mcx),
            es_worker_instrument: PgVec::new_in(mcx),
            es_aux_contexts: PgVec::new_in(mcx),
            es_finished: false,
            es_exprcontexts: PgVec::new_in(mcx),
            es_subplanstates: PgVec::new_in(mcx),
            es_param_subplans: PgVec::new_in(mcx),
            es_subplan_hook: None,
            es_subplan_init_hook: None,
            es_subplan_eval_hook: None,
            es_subplan_expr_states: PgVec::new_in(mcx),
            es_cte_shared: PgVec::new_in(mcx),
            es_worktable_shared: PgVec::new_in(mcx),
            es_cte_proc_hook: None,
            es_auxmodifytables: PgVec::new_in(mcx),
            es_per_tuple_exprcontext: None,
            es_sourceText: None,
            es_use_parallel_mode: false,
            es_parallel_scan_wired: false,
            es_parallel_workers_to_launch: 0,
            es_parallel_workers_launched: 0,
            es_jit_flags: 0,
            es_jit_blocks: Vec::new(),
            es_jit_instr: ::jit_deform::JitInstrumentation::default(),
            es_epq: None,
            es_epq_active: false,
        }
    }

    /// Range-table size for owner-held EPQ relsubs sizing.
    pub fn epq_rtsize(&self) -> usize {
        self.es_range_table_size as usize
    }

    /// `CreateExprContext(estate)`.
    pub fn create_expr_context(&mut self) -> EcxtId {
        let per_tuple = PgBox::new_in(
            self.es_query_cxt.context().new_child_bump("ExprContext"),
            self.es_query_cxt,
        );
        let mut ecxt = ExprContextData::new(self.es_query_cxt, per_tuple);
        ecxt.ecxt_param_list_info = self.es_param_list_info;
        ecxt.ecxt_param_exec_vals = core::ptr::NonNull::new(self.es_param_exec_vals.as_mut_ptr());
        let id = EcxtId(self.es_exprcontexts.len() as u32);
        self.es_exprcontexts.push(Some(ecxt));
        id
    }

    /// Resolve-once binding for expression compile; es_param_exec_vals is
    /// sized at ExecutorStart and never grown, so its element pointers are
    /// stable for the query.
    pub fn param_bind(&mut self) -> ParamBind<'mcx> {
        ParamBind {
            extern_params: self.es_param_list_info,
            exec_vals: core::ptr::NonNull::new(self.es_param_exec_vals.as_mut_ptr()),
            n_exec: self.es_param_exec_vals.len() as u32,
        }
    }

    /// Copy the portal's extern params into an estate-owned buffer so
    /// compiled ParamExtern steps survive the portal (executor skeleton);
    /// the returned slice is what es_param_list_info must hold.
    pub fn param_stable_install(
        &mut self,
        src: &[ParamExternData],
    ) -> PgResult<&'mcx [ParamExternData]> {
        use ::mcx::Allocator;
        let n = src.len();
        if n == 0 {
            self.es_param_stable = Some(ParamStable(core::ptr::NonNull::dangling(), 0));
            return Ok(&[]);
        }
        let layout = core::alloc::Layout::array::<ParamExternData>(n)
            .expect("param array layout");
        let p: core::ptr::NonNull<ParamExternData> = self
            .es_query_cxt
            .allocate(layout)
            .map_err(|_| self.es_query_cxt.oom(layout.size()))?
            .cast();
        // SAFETY: fresh exclusive allocation of n elements.
        unsafe { core::ptr::copy_nonoverlapping(src.as_ptr(), p.as_ptr(), n) };
        self.es_param_stable = Some(ParamStable(p, n as u32));
        // SAFETY: arena-backed; lives until the ExecutorState context dies.
        Ok(unsafe { core::slice::from_raw_parts(p.as_ptr(), n) })
    }

    /// Skeleton reuse: restamp the stable buffer from this EXECUTE's params.
    /// False = shape/type mismatch — the caller must rebuild instead (the
    /// fresh compile re-runs C's per-execution param checks and errors).
    pub fn param_stable_restamp(&mut self, new: Option<&[ParamExternData]>) -> bool {
        match (self.es_param_stable, new) {
            (None, None) => true,
            (Some(ParamStable(_, n)), None) => n == 0,
            (Some(ParamStable(p, n)), Some(new)) if new.len() == n as usize => {
                for (i, prm) in new.iter().enumerate() {
                    // SAFETY: i < n, the buffer's installed length.
                    if prm.ptype == 0 || unsafe { (*p.as_ptr().add(i)).ptype } != prm.ptype {
                        return false;
                    }
                }
                for (i, prm) in new.iter().enumerate() {
                    // SAFETY: same bounds; compiled steps read these cells
                    // only during evaluation, never concurrently with this.
                    unsafe { p.as_ptr().add(i).write(*prm) };
                }
                true
            }
            _ => false,
        }
    }

    /// `CreateWorkExprContext`; the bump backend has no work_mem block dial.
    pub fn create_work_expr_context(&mut self) -> EcxtId {
        self.create_expr_context()
    }

    pub fn create_aux_context(&mut self, name: &'static str) -> AuxCxtId {
        // Boxed for the same address-stability constraint as ExprContextData
        // per_tuple: PgVec allocator handles taken from aux_mcx must survive
        // es_aux_contexts growth.
        let cxt = PgBox::new_in(
            self.es_query_cxt.context().new_child_bump(name),
            self.es_query_cxt,
        );
        let id = AuxCxtId(self.es_aux_contexts.len() as u32);
        self.es_aux_contexts.push(cxt);
        id
    }

    #[inline]
    pub fn aux_mcx(&self, id: AuxCxtId) -> Mcx<'_> {
        self.es_aux_contexts[id.0 as usize].mcx()
    }

    pub fn reset_aux_context(&mut self, id: AuxCxtId) {
        self.es_aux_contexts[id.0 as usize].reset();
    }

    #[inline]
    pub fn slot_and_aux_mcx(
        &mut self,
        slot: ExecSlotId,
        aux: AuxCxtId,
    ) -> (&mut SlotData<'mcx>, Mcx<'_>) {
        (
            &mut self.es_tupleTable[slot.0 as usize],
            self.es_aux_contexts[aux.0 as usize].mcx(),
        )
    }

    #[inline]
    pub fn slot_and_per_tuple_mcx(
        &mut self,
        slot: ExecSlotId,
        ecxt: EcxtId,
    ) -> (&mut SlotData<'mcx>, Mcx<'_>) {
        (
            &mut self.es_tupleTable[slot.0 as usize],
            self.es_exprcontexts[ecxt.0 as usize]
                .as_ref()
                .expect("expr context is live")
                .per_tuple_mcx(),
        )
    }

    /// `ExecAssignExprContext`: PlanState.ps_ExprContext stores the id.
    pub fn exec_assign_expr_context(&mut self) -> EcxtId {
        self.create_expr_context()
    }

    // Ids are estate-minted, tables append-only: index always in bounds
    // (bounds checks were a per-row tax in every node's fetch loop).
    #[inline]
    pub fn ecxt(&self, id: EcxtId) -> &ExprContextData<'mcx> {
        debug_assert!((id.0 as usize) < self.es_exprcontexts.len());
        // SAFETY: id provenance above.
        unsafe { self.es_exprcontexts.get_unchecked(id.0 as usize) }
            .as_ref()
            .expect("ExprContext used after FreeExprContext")
    }

    #[inline]
    pub fn ecxt_mut(&mut self, id: EcxtId) -> &mut ExprContextData<'mcx> {
        debug_assert!((id.0 as usize) < self.es_exprcontexts.len());
        // SAFETY: id provenance above.
        unsafe { self.es_exprcontexts.get_unchecked_mut(id.0 as usize) }
            .as_mut()
            .expect("ExprContext used after FreeExprContext")
    }

    #[inline]
    pub fn reset_expr_context(&mut self, id: EcxtId) {
        self.ecxt_mut(id).reset();
    }

    /// `FreeExprContext(econtext, isCommit)`.
    pub fn free_expr_context(&mut self, id: EcxtId, is_commit: bool) {
        if let Some(mut ecxt) = self.es_exprcontexts[id.0 as usize].take() {
            ecxt.shutdown(is_commit);
        }
        if self.es_per_tuple_exprcontext == Some(id) {
            self.es_per_tuple_exprcontext = None;
        }
    }

    /// `GetPerTupleExprContext(estate)` / `MakePerTupleExprContext(estate)`.
    pub fn get_per_tuple_expr_context(&mut self) -> EcxtId {
        match self.es_per_tuple_exprcontext {
            Some(id) => id,
            None => {
                let id = self.create_expr_context();
                self.es_per_tuple_exprcontext = Some(id);
                id
            }
        }
    }

    /// `GetPerTupleMemoryContext(estate)`.
    pub fn get_per_tuple_memory(&mut self) -> Mcx<'_> {
        let id = self.get_per_tuple_expr_context();
        self.ecxt(id).per_tuple.mcx()
    }

    /// `ResetPerTupleExprContext(estate)`: no-op when never made.
    #[inline]
    pub fn reset_per_tuple_expr_context(&mut self) {
        if let Some(id) = self.es_per_tuple_exprcontext {
            self.ecxt_mut(id).reset();
        }
    }

    /// `ExecInitExtraTupleSlot` (execTuples.c).
    pub fn exec_init_extra_tuple_slot(
        &mut self,
        desc: Option<Rc<TupleDescData<'mcx>>>,
        kind: TupleSlotKind,
    ) -> ExecSlotId {
        let slot = exectuples::make_tuple_table_slot(self.es_query_cxt, kind, desc);
        let id = ExecSlotId(self.es_tupleTable.len() as u32);
        self.es_tupleTable.push(slot);
        id
    }

    pub fn cte_shared_slot(&mut self, param: usize) -> &mut Option<CteShared> {
        while self.es_cte_shared.len() <= param {
            self.es_cte_shared.push(None);
        }
        &mut self.es_cte_shared[param]
    }

    pub fn worktable_shared_slot(&mut self, param: usize) -> &mut Option<WorkTableShared> {
        while self.es_worktable_shared.len() <= param {
            self.es_worktable_shared.push(None);
        }
        &mut self.es_worktable_shared[param]
    }

    /// (subplan rows pulled, tuplestore rows) for the cteParam — the
    /// materialize-once proof reads fills == tuples == |CTE result|.
    pub fn cte_fill_probe(&self, param: usize) -> Option<(u32, i64)> {
        self.es_cte_shared
            .get(param)
            .and_then(|s| s.as_ref())
            .map(|s| (s.fills, s.tuplestore.tuple_count()))
    }

    #[inline]
    pub fn slot(&self, id: ExecSlotId) -> &SlotData<'mcx> {
        debug_assert!((id.0 as usize) < self.es_tupleTable.len());
        // SAFETY: id provenance (ecxt note).
        unsafe { self.es_tupleTable.get_unchecked(id.0 as usize) }
    }

    #[inline]
    pub fn slot_mut(&mut self, id: ExecSlotId) -> &mut SlotData<'mcx> {
        debug_assert!((id.0 as usize) < self.es_tupleTable.len());
        // SAFETY: id provenance (ecxt note).
        unsafe { self.es_tupleTable.get_unchecked_mut(id.0 as usize) }
    }

    /// `ExecResetTupleTable(estate->es_tupleTable, shouldFree)` (execTuples.c).
    pub fn exec_reset_tuple_table(&mut self, should_free: bool) {
        let mcx = self.es_query_cxt;
        for slot in self.es_tupleTable.iter_mut() {
            exectuples::exec_clear_tuple(slot, mcx);
            slot.base_mut().tts_tupleDescriptor = None;
        }
        if should_free {
            self.es_tupleTable.clear();
        }
    }

    /// `ExecInitRangeTable(estate, rangeTable, permInfos, unpruned_relids)`.
    pub fn exec_init_range_table(
        &mut self,
        range_table: &'mcx NodeList<'mcx>,
        perm_infos: &'mcx NodeList<'mcx>,
        unpruned_relids: Bitmapset<'mcx>,
    ) -> PgResult<()> {
        self.es_range_table.reserve(range_table.len());
        for rte_node in range_table.iter() {
            let rte = rte_node
                .as_range_tbl_entry()
                .expect("rtable cell is a RangeTblEntry");
            match rte.rtekind {
                RTEKind::RTE_RELATION
                | RTEKind::RTE_RESULT
                | RTEKind::RTE_FUNCTION
                | RTEKind::RTE_VALUES
                | RTEKind::RTE_JOIN
                | RTEKind::RTE_TABLEFUNC
                | RTEKind::RTE_CTE
                | RTEKind::RTE_NAMEDTUPLESTORE
                // The grouping-step RTE rides the flat rtable expr-free
                // (setrefs zaps groupexprs); C's ExecInitRangeTable is
                // kind-agnostic.
                | RTEKind::RTE_GROUP => {}
                // A pulled-up (dead) subquery RTE stays in the range table
                // for its lock/ACL surface, as in C; a live subquery is the
                // unported SubqueryScan lane.
                RTEKind::RTE_SUBQUERY if rte.subquery.is_none() => {}
                other => panic!(
                    "ExecInitRangeTable (execUtils.c): {other:?} lane not ported"
                ),
            }
            if !rte.securityQuals.is_nil() {
                panic!("ExecInitRangeTable: row-level security (securityQuals) not ported");
            }
            self.es_range_table.push(rte);
            self.es_relations.push(None);
        }
        self.es_rteperminfos = Some(perm_infos);
        self.es_range_table_size = range_table.len() as u32;
        self.es_unpruned_relids = unpruned_relids;
        Ok(())
    }

    /// `exec_rt_fetch(rti, estate)` (executor.h); rti is 1-based.
    #[inline]
    pub fn exec_rt_fetch(&self, rti: u32) -> &'mcx RangeTblEntry<'mcx> {
        self.es_range_table[(rti - 1) as usize]
    }

    /// `ExecGetRangeTableRelation(estate, rti, isResultRel)`.
    pub fn exec_get_range_table_relation(
        &mut self,
        rti: u32,
        is_result_rel: bool,
    ) -> PgResult<&Relation<'mcx>> {
        assert!(rti > 0 && rti <= self.es_range_table_size);
        if !is_result_rel && !self.es_unpruned_relids.is_member(rti as i32) {
            return Err(pruned_relation_error());
        }
        let idx = (rti - 1) as usize;
        if self.es_relations[idx].is_none() {
            let rte = self.exec_rt_fetch(rti);
            assert!(
                rte.rtekind == RTEKind::RTE_RELATION,
                "ExecGetRangeTableRelation of a non-relation RTE"
            );
            // A parallel worker takes its own lock (sane behavior if the
            // leader exits first); the leader relies on parser/plancache
            // already holding rellockmode (C asserts past AccessShareLock).
            // is_installed: crate tests run without parallel::init_seams;
            // uninstalled means no worker can exist.
            let in_worker = parallel_seams::is_parallel_worker::is_installed()
                && parallel_seams::is_parallel_worker::call();
            let rel = if in_worker {
                table::table_open(self.es_query_cxt, rte.relid, rte.rellockmode)?
            } else {
                let rel = table::table_open(self.es_query_cxt, rte.relid, NoLock)?;
                debug_assert!(
                    rte.rellockmode == AccessShareLock
                        || lmgr_seams::check_relation_locked_by_me::call(
                            rel.rd_id,
                            rte.rellockmode,
                            false
                        )
                );
                rel
            };
            self.es_relations[idx] = Some(rel);
        }
        Ok(self.es_relations[idx].as_ref().unwrap())
    }

    pub fn exec_init_result_relation(&mut self, rti: u32) -> PgResult<()> {
        self.exec_get_range_table_relation(rti, true)?;
        if self.es_result_relations.len() < self.es_range_table_size as usize {
            let n = self.es_range_table_size as usize - self.es_result_relations.len();
            self.es_result_relations.extend((0..n).map(|_| None));
        }
        let info = ResultRelInfo { ri_RangeTableIndex: rti };
        self.es_result_relations[(rti - 1) as usize] = Some(info);
        self.es_opened_result_relations.push(info);
        Ok(())
    }

    // ExecCloseResultRelations index/trigger lanes are loud upstream.
    pub fn exec_close_result_relations(&mut self) {
        self.es_opened_result_relations.clear();
        debug_assert!(self.es_trig_target_relations.is_empty());
    }

    /// `ExecCloseRangeTableRelations(estate)`: locks are kept, as in C.
    pub fn exec_close_range_table_relations(&mut self) -> PgResult<()> {
        for slot in self.es_relations.iter_mut() {
            if let Some(rel) = slot.take() {
                table::table_close(rel, NoLock)?;
            }
        }
        Ok(())
    }

    /// `FreeExecutorState` non-memory half; newest-first (C lcons order).
    pub fn teardown(&mut self) {
        for i in (0..self.es_exprcontexts.len()).rev() {
            if self.es_exprcontexts[i].is_some() {
                self.free_expr_context(EcxtId(i as u32), true);
            }
        }
        self.es_junkFilter = None;
        for slot in self.es_cte_shared.iter_mut() {
            *slot = None;
        }
        for slot in self.es_worktable_shared.iter_mut() {
            *slot = None;
        }
        self.es_aux_contexts.clear();
        self.es_jit_blocks.clear();
    }

    /// True iff every census-exempt owner has been released — the
    /// `free_forget` precondition (forgetting a live one leaks).
    pub fn owners_released(&self) -> bool {
        self.es_snapshot.is_none()
            && self.es_crosscheck_snapshot.is_none()
            && self.es_junkFilter.is_none()
            && self.es_relations.iter().all(Option::is_none)
            && self.es_exprcontexts.iter().all(Option::is_none)
            && self.es_cte_shared.iter().all(Option::is_none)
            && self.es_worktable_shared.iter().all(Option::is_none)
            && self.es_aux_contexts.is_empty()
            && self.es_jit_blocks.is_empty()
            && self.es_subplan_expr_states.is_empty()
            && self
                .es_tupleTable
                .iter()
                .all(|s| s.base().tts_tupleDescriptor.is_none())
    }
}

mcx::forget_safe_nodrop!(
    SubplanStateCell, ResultRelInfo, EcxtId, ExecSlotId, ExecRowMark, ParamStable,
    EpqRowMarkFetch,
);

// Exempt groups: [1] droppy owners, all released before the exec bundle is
// forgotten (teardown/exec_reset_tuple_table/exec_close_range_table_relations/
// UnregisterSnapshot in standard_executor_end; owners_released() asserts it);
// [2] no-drop foreign types without ForgetSafe impls, const-proven here.
const _: () = assert!(!core::mem::needs_drop::<ScanDirection>());
const _: () = assert!(!core::mem::needs_drop::<Option<SubplanInitHook>>());
const _: () = assert!(!core::mem::needs_drop::<Option<SubplanEvalHook>>());
const _: () = assert!(!core::mem::needs_drop::<(core::ptr::NonNull<()>, unsafe fn(core::ptr::NonNull<()>))>());
const _: () = assert!(!core::mem::needs_drop::<ModifyTableP3>());
const _: () = assert!(!core::mem::needs_drop::<Instrumentation>());
const _: () = assert!(!core::mem::needs_drop::<(i32, AggregateInstrumentation)>());
const _: () = assert!(!core::mem::needs_drop::<(i32, TuplesortInstrumentation)>());
const _: () = assert!(!core::mem::needs_drop::<Option<SubplanHook>>());
const _: () = assert!(!core::mem::needs_drop::<Option<CteProcHook>>());
const _: () = assert!(!core::mem::needs_drop::<Option<ParamExecData>>());
const _: () = assert!(!core::mem::needs_drop::<(i32, IncrementalSortInfo)>());
const _: () = assert!(!core::mem::needs_drop::<(i32, HashInstrumentation)>());
const _: () = assert!(!core::mem::needs_drop::<(i32, u64)>());
const _: () = assert!(!core::mem::needs_drop::<RuntimeEaRefusal>());
const _: () = assert!(!core::mem::needs_drop::<RuntimeEaPipeline>());
const _: () = assert!(!core::mem::needs_drop::<EngineEvent>());
mcx::forget_safe_struct!(
    EpqSubs<'_> { relsubs_slot, relsubs_done, relsubs_blocked, relsubs_rowmark, origslot },
    EStateData<'_> {
        es_query_cxt, es_range_table, es_range_table_size,
        es_rteperminfos, es_plannedstmt,
        es_unpruned_relids, es_output_cid, es_result_relations,
        es_opened_result_relations, es_tuple_routing_result_relations,
        es_trig_target_relations, es_insert_pending_result_relations,
        es_param_list_info, es_param_stable, es_queryEnv, es_processed,
        es_total_processed,
        es_top_eflags, es_instrument, es_finished, es_subplanstates,
        es_param_subplans, es_per_tuple_exprcontext,
        es_sourceText, es_use_parallel_mode, es_parallel_scan_wired,
        es_parallel_workers_to_launch,
        es_parallel_workers_launched, es_jit_flags, es_jit_instr, es_epq,
        es_epq_active, es_lane_leaf_fast, es_lane_trace_owned, es_cursor_run_budget,
        es_lane_cursor_parked,
        es_spi_run_budget, es_rowmarks;
        es_jit_blocks,
        es_snapshot, es_crosscheck_snapshot, es_relations, es_junkFilter,
        es_tupleTable, es_exprcontexts, es_cte_shared, es_worktable_shared,
        es_aux_contexts,
        es_direction, es_part_prune_results,
        es_insert_pending_modifytables, es_auxmodifytables,
        es_param_exec_vals, es_instrumentation, es_runtime_ea_refusals,
        es_runtime_ea_pipelines, es_engine_events,
        es_agg_instrumentation,
        es_sort_instrumentation, es_incsort_instrumentation,
        es_hash_instrumentation, es_index_instrumentation,
        es_bitmap_instrumentation, es_worker_instrument,
        es_subplan_hook, es_subplan_init_hook,
        es_subplan_eval_hook, es_subplan_expr_states, es_cte_proc_hook,
    },
);
// SAFETY: arena-backed PgVecs of no-drop Copy payloads (asserted above);
// forgetting reclaims nothing beyond arena bytes.
unsafe impl mcx::ForgetSafe for WorkerInstr<'_> {}

#[cold]
#[inline(never)]
fn pruned_relation_error() -> alloc::boxed::Box<PgError> {
    alloc::boxed::Box::new(PgError::error("trying to open a pruned relation"))
}

::mcx::bind!(pub EStateTy => EStateData<'mcx>);

/// The C `EState*`: the "ExecutorState" context + state, one movable value.
pub type ExecutorState = McxOwned<EStateTy>;

/// `CreateExecutorState()`; `parent` is C's CurrentMemoryContext. Bump: C
/// never pfrees out of this context; droppy owner fields still drop.
pub fn create_executor_state(parent: &MemoryContext) -> PgResult<ExecutorState> {
    McxOwned::try_new(parent.new_child_bump("ExecutorState"), |mcx| {
        Ok(EStateData::new_in(mcx))
    })
}

/// `FreeExecutorState`: bundle drop = `MemoryContextDelete(es_query_cxt)`.
pub fn free_executor_state(mut estate: ExecutorState) {
    estate.with_mut(|es| es.teardown());
}

/// `CreateStandaloneExprContext`: per-query memory is the caller's context.
#[derive(Debug)]
pub struct StandaloneExprContext<'mcx>(ExprContextData<'mcx>);

pub fn create_standalone_expr_context(mcx: Mcx<'_>) -> StandaloneExprContext<'_> {
    StandaloneExprContext(ExprContextData::new(
        mcx,
        PgBox::new_in(mcx.context().new_child_bump("ExprContext"), mcx),
    ))
}

impl<'mcx> core::ops::Deref for StandaloneExprContext<'mcx> {
    type Target = ExprContextData<'mcx>;
    fn deref(&self) -> &ExprContextData<'mcx> {
        &self.0
    }
}

impl<'mcx> core::ops::DerefMut for StandaloneExprContext<'mcx> {
    fn deref_mut(&mut self) -> &mut ExprContextData<'mcx> {
        &mut self.0
    }
}

/// `executor_errposition`: 1-based char position for errposition(), else 0.
pub fn executor_errposition(estate: Option<&EStateData<'_>>, location: i32) -> i32 {
    if location < 0 {
        return 0;
    }
    let Some(src) = estate.and_then(|es| es.es_sourceText) else {
        return 0;
    };
    let prefix = &src.as_bytes()[..(location as usize).min(src.len())];
    // Defensive fallback: not yet wired into a real ereport() call, so an
    // encoding error here (prefix is source text, always valid in practice)
    // has no error-reporting path of its own to feed into.
    mbutils_seams::pg_mbstrlen_with_len::call(prefix).unwrap_or(location) + 1
}

#[cfg(test)]
mod tests;
