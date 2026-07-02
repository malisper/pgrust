// execUtils.c executor-state half: EState + ExprContext lifecycle.
// EState is the per-query resource owner (docs/no-drop.md): every droppy
// resource (per-tuple contexts, snapshot Rcs, relation opens, slots) lives
// here by value; arena-resident executor nodes hold u32 handles (EcxtId,
// ExecSlotId). Both the "ExecutorState" context and each ExprContext's
// per-tuple context are bump arenas; ResetExprContext is a wholesale rewind.
// PlanState-coupled routines (slot-ops probes, projection assignment, scan
// init, ResultRelInfo/range-table internals, GetAttributeBy*) land with
// nodes phase 3 / execExpr; their EState fields are `*P3` handles below.
#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::rc::Rc;

use ::datum::Datum;
use ::mcx::{Mcx, McxOwned, MemoryContext, PgVec};
use ::queryenvironment::QueryEnvironment;
use ::snapmgr::Snapshot;
use ::types_core::CommandId;
use ::types_error::PgResult;
use ::types_rel::Relation;
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
// Unconstructible placeholders: the field exists (EState shape is complete),
// its value is provably None until the owning unit lands and swaps the type.
p3!(
    PlannedStmtP3,
    RangeTableP3,
    PermInfosP3,
    PartPruneP3,
    JunkFilterP3,
    ResultRelInfoP3,
    RowMarkP3,
    ModifyTableP3,
    ParamListInfoP3,
    ParamExecP3,
    PlanStateP3,
    UnprunedRelidsP3,
);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EcxtId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecSlotId(pub u32);

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
    pub ecxt_param_exec_vals: Option<ParamExecP3>,
    pub ecxt_param_list_info: Option<ParamListInfoP3>,
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

    /// `ResetExprContext(econtext)` (executor.h): THE per-row arena reset —
    /// bump rewind, no callbacks, no per-object work.
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

    /// `ShutdownExprContext`: fire callbacks newest-first inside the
    /// per-tuple context; `is_commit=false` empties the list without calling.
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
    pub es_range_table: Option<RangeTableP3>,
    pub es_range_table_size: u32,
    pub es_relations: PgVec<'mcx, Option<Relation<'mcx>>>,
    pub es_rowmarks: PgVec<'mcx, Option<RowMarkP3>>,
    pub es_rteperminfos: Option<PermInfosP3>,
    pub es_plannedstmt: Option<PlannedStmtP3>,
    pub es_part_prune_infos: Option<PartPruneP3>,
    pub es_unpruned_relids: Option<UnprunedRelidsP3>,
    pub es_junkFilter: Option<JunkFilterP3>,
    pub es_output_cid: CommandId,
    pub es_result_relations: PgVec<'mcx, Option<ResultRelInfoP3>>,
    pub es_opened_result_relations: PgVec<'mcx, ResultRelInfoP3>,
    pub es_tuple_routing_result_relations: PgVec<'mcx, ResultRelInfoP3>,
    pub es_trig_target_relations: PgVec<'mcx, ResultRelInfoP3>,
    pub es_insert_pending_result_relations: PgVec<'mcx, ResultRelInfoP3>,
    pub es_insert_pending_modifytables: PgVec<'mcx, ModifyTableP3>,
    pub es_param_list_info: Option<ParamListInfoP3>,
    pub es_param_exec_vals: PgVec<'mcx, ParamExecP3>,
    pub es_queryEnv: Option<&'mcx QueryEnvironment<'mcx>>,
    pub es_tupleTable: PgVec<'mcx, SlotData<'mcx>>,
    pub es_processed: u64,
    pub es_total_processed: u64,
    pub es_top_eflags: i32,
    pub es_instrument: i32,
    pub es_finished: bool,
    es_exprcontexts: PgVec<'mcx, Option<ExprContextData<'mcx>>>,
    pub es_subplanstates: PgVec<'mcx, PlanStateP3>,
    pub es_auxmodifytables: PgVec<'mcx, ModifyTableP3>,
    es_per_tuple_exprcontext: Option<EcxtId>,
    pub es_sourceText: Option<&'mcx str>,
    pub es_use_parallel_mode: bool,
    pub es_parallel_workers_to_launch: i32,
    pub es_parallel_workers_launched: i32,
    pub es_jit_flags: i32,
}

impl<'mcx> EStateData<'mcx> {
    pub fn new_in(mcx: Mcx<'mcx>) -> Self {
        EStateData {
            es_query_cxt: mcx,
            es_direction: ScanDirection::ForwardScanDirection,
            es_snapshot: None,
            es_crosscheck_snapshot: None,
            es_range_table: None,
            es_range_table_size: 0,
            es_relations: PgVec::new_in(mcx),
            es_rowmarks: PgVec::new_in(mcx),
            es_rteperminfos: None,
            es_plannedstmt: None,
            es_part_prune_infos: None,
            es_unpruned_relids: None,
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
            es_finished: false,
            es_exprcontexts: PgVec::new_in(mcx),
            es_subplanstates: PgVec::new_in(mcx),
            es_auxmodifytables: PgVec::new_in(mcx),
            es_per_tuple_exprcontext: None,
            es_sourceText: None,
            es_use_parallel_mode: false,
            es_parallel_workers_to_launch: 0,
            es_parallel_workers_launched: 0,
            es_jit_flags: 0,
        }
    }

    /// `CreateExprContext(estate)`.
    pub fn create_expr_context(&mut self) -> EcxtId {
        let per_tuple = self.es_query_cxt.context().new_child_bump("ExprContext");
        let ecxt = ExprContextData::new(self.es_query_cxt, per_tuple);
        let id = EcxtId(self.es_exprcontexts.len() as u32);
        self.es_exprcontexts.push(Some(ecxt));
        id
    }

    /// `CreateWorkExprContext(estate)`. C caps the AllocSet max block size at
    /// prevpower2(work_mem KB / 16); the bump backend has no block-size dial,
    /// so this is the default context (growth policy is the arena's).
    pub fn create_work_expr_context(&mut self) -> EcxtId {
        self.create_expr_context()
    }

    /// `ExecAssignExprContext(estate, planstate)`: the returned id is what
    /// PlanState.ps_ExprContext stores when execProcnode lands.
    pub fn exec_assign_expr_context(&mut self) -> EcxtId {
        self.create_expr_context()
    }

    #[inline]
    pub fn ecxt(&self, id: EcxtId) -> &ExprContextData<'mcx> {
        self.es_exprcontexts[id.0 as usize]
            .as_ref()
            .expect("ExprContext used after FreeExprContext")
    }

    #[inline]
    pub fn ecxt_mut(&mut self, id: EcxtId) -> &mut ExprContextData<'mcx> {
        self.es_exprcontexts[id.0 as usize]
            .as_mut()
            .expect("ExprContext used after FreeExprContext")
    }

    #[inline]
    pub fn reset_expr_context(&mut self, id: EcxtId) {
        self.ecxt_mut(id).reset();
    }

    /// `FreeExprContext(econtext, isCommit)`: shutdown callbacks, delete the
    /// per-tuple context, unlink from the EState.
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

    /// `ResetPerTupleExprContext(estate)`: per-row wholesale rewind of the
    /// output ExprContext, no-op when it was never made.
    #[inline]
    pub fn reset_per_tuple_expr_context(&mut self) {
        if let Some(id) = self.es_per_tuple_exprcontext {
            self.ecxt_mut(id).reset();
        }
    }

    /// `ExecInitExtraTupleSlot(estate, tupledesc, tts_ops)` (execTuples.c):
    /// EState-lifetime slot registered in es_tupleTable.
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

    #[inline]
    pub fn slot(&self, id: ExecSlotId) -> &SlotData<'mcx> {
        &self.es_tupleTable[id.0 as usize]
    }

    #[inline]
    pub fn slot_mut(&mut self, id: ExecSlotId) -> &mut SlotData<'mcx> {
        &mut self.es_tupleTable[id.0 as usize]
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

    /// `ExecGetRangeTableRelation(estate, rti, isResultRel)`.
    pub fn exec_get_range_table_relation(
        &mut self,
        _rti: u32,
        _is_result_rel: bool,
    ) -> PgResult<&Relation<'mcx>> {
        panic!("executils::exec_get_range_table_relation: pending nodes phase 3 (RangeTblEntry)");
    }

    /// `FreeExecutorState`'s non-memory teardown, exposed for ExecutorEnd:
    /// shut down remaining ExprContexts newest-first (C walks the lcons list).
    pub fn teardown(&mut self) {
        for i in (0..self.es_exprcontexts.len()).rev() {
            if self.es_exprcontexts[i].is_some() {
                self.free_expr_context(EcxtId(i as u32), true);
            }
        }
    }
}

::mcx::bind!(pub EStateTy => EStateData<'mcx>);

/// The C `EState*`: the per-query "ExecutorState" bump context and the state
/// it owns, movable as one value.
pub type ExecutorState = McxOwned<EStateTy>;

/// `CreateExecutorState()`; `parent` is C's CurrentMemoryContext. Bump
/// backend: C never pfrees out of this context (wholesale delete at end),
/// while droppy owner fields (Rc, MemoryContext, Relation) still drop.
pub fn create_executor_state(parent: &MemoryContext) -> PgResult<ExecutorState> {
    McxOwned::try_new(parent.new_child_bump("ExecutorState"), |mcx| {
        Ok(EStateData::new_in(mcx))
    })
}

/// `FreeExecutorState(estate)`: fire remaining ExprContext shutdowns, then
/// the consumed bundle's drop is C's `MemoryContextDelete(es_query_cxt)`.
pub fn free_executor_state(mut estate: ExecutorState) {
    estate.with_mut(|es| es.teardown());
}

/// Standalone `ExprContext` (`CreateStandaloneExprContext`): per-query memory
/// is the caller's context; caller frees / rescans it.
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

/// `executor_errposition(estate, location)`: returns the 1-based character
/// position for the caller's errposition(), 0 when unavailable.
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
