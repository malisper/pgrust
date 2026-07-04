// execUtils.c executor-state half. EState is the per-query resource owner
// (docs/no-drop.md): droppy resources live here by value, arena-resident
// nodes hold u32 handles. Query + per-tuple contexts are bump arenas.
#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::rc::Rc;

use ::datum::Datum;
use ::mcx::{Mcx, McxOwned, MemoryContext, PgVec};
use ::queryenvironment::QueryEnvironment;
use ::snapmgr::Snapshot;
use ::types_core::instrument::{
    AggregateInstrumentation, HashInstrumentation, IncrementalSortInfo, Instrumentation,
    TuplesortInstrumentation,
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

pub type SubplanInitHook =
    for<'x> unsafe fn(core::ptr::NonNull<()>, types_nodes::Node<'x>) -> PgResult<core::ptr::NonNull<()>>;

pub type SubplanEvalHook = for<'a, 'mcx> unsafe fn(
    core::ptr::NonNull<()>,
    &'a mut EStateData<'mcx>,
    EcxtId,
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
    let env = estate.es_subplan_init_hook.map(|init| execexpr::SubplanCompileEnv {
        estate: core::ptr::NonNull::from(&mut *estate).cast(),
        init,
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
fn run_subplan_eval_hook<'mcx>(
    sstate: core::ptr::NonNull<()>,
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
) -> PgResult<::datum::NullableDatum> {
    let hook = estate
        .es_subplan_eval_hook
        .expect("SubPlan step before execmain installed the eval hook");
    // SAFETY: sstate was installed by execmain's ExecInitSubPlan on this estate.
    unsafe { hook(sstate, estate, ecxt) }
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
                let r = run_subplan_eval_hook(s.sstate, estate, ecxt)?;
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
                let r = run_subplan_eval_hook(s.sstate, estate, ecxt)?;
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
    per_tuple: MemoryContext,
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
    fn new(per_query: Mcx<'mcx>, per_tuple: MemoryContext) -> Self {
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
    pub es_param_exec_vals: PgVec<'mcx, ParamExecData>,
    pub es_queryEnv: Option<&'mcx QueryEnvironment<'mcx>>,
    pub es_tupleTable: PgVec<'mcx, SlotData<'mcx>>,
    pub es_processed: u64,
    pub es_total_processed: u64,
    pub es_top_eflags: i32,
    pub es_instrument: i32,
    // Keyed by plan_node_id (C: per-PlanState); empty when es_instrument == 0.
    pub es_instrumentation: PgVec<'mcx, Instrumentation>,
    // (plan_node_id, metrics); C's AggState fields, hoisted for the Plan walk.
    pub es_agg_instrumentation: PgVec<'mcx, (i32, AggregateInstrumentation)>,
    pub es_sort_instrumentation: PgVec<'mcx, (i32, TuplesortInstrumentation)>,
    pub es_incsort_instrumentation: PgVec<'mcx, (i32, IncrementalSortInfo)>,
    pub es_hash_instrumentation: PgVec<'mcx, (i32, HashInstrumentation)>,
    /// (plan_node_id, nsearches); C's IndexScanInstrumentation, hoisted.
    pub es_index_instrumentation: PgVec<'mcx, (i32, u64)>,
    // Node-owned resettable contexts (C's node-local AllocSets): droppy, so
    // they live in the estate owner; nodes hold AuxCxtId (docs/no-drop.md).
    es_aux_contexts: PgVec<'mcx, MemoryContext>,
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
    pub es_auxmodifytables: PgVec<'mcx, ModifyTableP3>,
    es_per_tuple_exprcontext: Option<EcxtId>,
    pub es_sourceText: Option<&'mcx str>,
    pub es_use_parallel_mode: bool,
    pub es_parallel_workers_to_launch: i32,
    pub es_parallel_workers_launched: i32,
    pub es_jit_flags: i32,
    // C EPQState.relsubs_*, hosted on the (shared) estate — no child EState.
    pub es_epq: Option<EpqSubs<'mcx>>,
    // C `es_epq_active != NULL`; scan nodes select their EPQ variant on it.
    pub es_epq_active: bool,
}

pub struct EpqSubs<'mcx> {
    pub relsubs_slot: PgVec<'mcx, Option<ExecSlotId>>,
    pub relsubs_done: PgVec<'mcx, bool>,
    pub relsubs_blocked: PgVec<'mcx, bool>,
}

impl<'mcx> EStateData<'mcx> {
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
            es_param_exec_vals: PgVec::new_in(mcx),
            es_queryEnv: None,
            es_tupleTable: PgVec::new_in(mcx),
            es_processed: 0,
            es_total_processed: 0,
            es_top_eflags: 0,
            es_instrument: 0,
            es_instrumentation: PgVec::new_in(mcx),
            es_agg_instrumentation: PgVec::new_in(mcx),
            es_sort_instrumentation: PgVec::new_in(mcx),
            es_incsort_instrumentation: PgVec::new_in(mcx),
            es_hash_instrumentation: PgVec::new_in(mcx),
            es_index_instrumentation: PgVec::new_in(mcx),
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
            es_parallel_workers_to_launch: 0,
            es_parallel_workers_launched: 0,
            es_jit_flags: 0,
            es_epq: None,
            es_epq_active: false,
        }
    }

    /// `EvalPlanQualInit` relsubs alloc, deferred to first EPQ use;
    /// `result_rti` starts blocked (C EvalPlanQualStart).
    pub fn epq_ensure(&mut self, result_rti: u32) -> &mut EpqSubs<'mcx> {
        if self.es_epq.is_none() {
            let mcx = self.es_query_cxt;
            let n = self.es_range_table_size as usize;
            debug_assert!(result_rti >= 1 && result_rti as usize <= n);
            let mut relsubs_slot = PgVec::new_in(mcx);
            relsubs_slot.resize(n, None);
            let mut relsubs_done = PgVec::new_in(mcx);
            relsubs_done.resize(n, false);
            let mut relsubs_blocked = PgVec::new_in(mcx);
            relsubs_blocked.resize(n, false);
            relsubs_blocked[(result_rti - 1) as usize] = true;
            relsubs_done[(result_rti - 1) as usize] = true;
            self.es_epq = Some(EpqSubs { relsubs_slot, relsubs_done, relsubs_blocked });
        }
        self.es_epq.as_mut().expect("just ensured")
    }

    /// `CreateExprContext(estate)`.
    pub fn create_expr_context(&mut self) -> EcxtId {
        let per_tuple = self.es_query_cxt.context().new_child_bump("ExprContext");
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

    /// `CreateWorkExprContext`; the bump backend has no work_mem block dial.
    pub fn create_work_expr_context(&mut self) -> EcxtId {
        self.create_expr_context()
    }

    pub fn create_aux_context(&mut self, name: &'static str) -> AuxCxtId {
        let cxt = self.es_query_cxt.context().new_child_bump(name);
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
                | RTEKind::RTE_CTE => {}
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
            // C's IsParallelWorker arm takes its own lock; workers unported.
            let rel = table::table_open(self.es_query_cxt, rte.relid, NoLock)?;
            // AcquireExecutorLocks contract: parser/plancache already hold
            // rellockmode (C asserts the same past AccessShareLock).
            debug_assert!(
                rte.rellockmode == AccessShareLock
                    || lmgr_seams::check_relation_locked_by_me::call(
                        rel.rd_id,
                        rte.rellockmode,
                        false
                    )
            );
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
            && self.es_subplan_expr_states.is_empty()
            && self
                .es_tupleTable
                .iter()
                .all(|s| s.base().tts_tupleDescriptor.is_none())
    }
}

mcx::forget_safe_nodrop!(
    SubplanStateCell, ResultRelInfo, EcxtId, ExecSlotId, ExecRowMark,
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
mcx::forget_safe_struct!(
    EpqSubs<'_> { relsubs_slot, relsubs_done, relsubs_blocked },
    EStateData<'_> {
        es_query_cxt, es_range_table, es_range_table_size,
        es_rteperminfos, es_plannedstmt,
        es_unpruned_relids, es_output_cid, es_result_relations,
        es_opened_result_relations, es_tuple_routing_result_relations,
        es_trig_target_relations, es_insert_pending_result_relations,
        es_param_list_info, es_queryEnv, es_processed, es_total_processed,
        es_top_eflags, es_instrument, es_finished, es_subplanstates,
        es_param_subplans, es_per_tuple_exprcontext,
        es_sourceText, es_use_parallel_mode, es_parallel_workers_to_launch,
        es_parallel_workers_launched, es_jit_flags, es_epq, es_epq_active,
        es_rowmarks;
        es_snapshot, es_crosscheck_snapshot, es_relations, es_junkFilter,
        es_tupleTable, es_exprcontexts, es_cte_shared, es_worktable_shared,
        es_aux_contexts,
        es_direction, es_part_prune_results,
        es_insert_pending_modifytables, es_auxmodifytables,
        es_param_exec_vals, es_instrumentation, es_agg_instrumentation,
        es_sort_instrumentation, es_incsort_instrumentation,
        es_hash_instrumentation, es_index_instrumentation, es_subplan_hook, es_subplan_init_hook,
        es_subplan_eval_hook, es_subplan_expr_states, es_cte_proc_hook,
    },
);

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
        mcx.context().new_child_bump("ExprContext"),
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
    mbutils_seams::pg_mbstrlen_with_len::call(prefix) + 1
}

#[cfg(test)]
mod tests;
