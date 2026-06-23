//! The canonical owned `QueryDesc` (`executor/execdesc.h`).
//!
//! `QueryDesc` is the executor's per-invocation handle: it owns the running
//! query's working storage (the `EState`), the initialized plan-state tree, the
//! destination receiver, and the read-only inputs (`PlannedStmt`, the source
//! text, the snapshots, the parameter list). `CreateQueryDesc` (execMain.c)
//! builds it before `ExecutorStart`; `ExecutorStart` fills the `EState`/plan
//! state / result tupdesc; `ExecutorRun` drives it; `ExecutorEnd` /
//! `FreeQueryDesc` tear it down.
//!
//! ## The owned model (why a bundle)
//!
//! In this repo an `EState` is an [`::mcx::McxOwned`] bundle: the `EStateData`
//! node lives *inside* its own per-query "ExecutorState" context (the C
//! `es_query_cxt`), and everything `ExecutorStart` builds — the plan-state
//! tree, the slot/exprcontext/result-rel pools, the result `TupleDesc` — is
//! allocated *in that same inner context*. None of it can be a sibling
//! `PgBox<'mcx, _>` field on a `QueryDesc<'mcx>` at an *outer* lifetime, because
//! the inner context's lifetime is private to the bundle (it is heap-pinned and
//! moves with the bundle). So the executor working state — the `EState`, the
//! plan-state tree, and the read-only `PlannedStmt`/source-text copies the
//! query reads — is held as **one** `McxOwned` bundle ([`QueryWorkState`]); the
//! `QueryDesc` is the lifetime-free handle that owns it together with the small
//! `Copy`/`Rc`/handle inputs.
//!
//! ## The four historical views this collapses (and the bridges that remain)
//!
//! This is the single owned model the executor-ownership keystone (#169)
//! collapses the trimmed views onto. The portal view has been retired onto this
//! owned value (`PortalData.queryDesc` now holds a `QueryDesc` directly — F1b),
//! and copyto's old `{tupDesc, exec_token}` view in `copy_query` has likewise
//! been retired onto this owned value (F1b). The remaining older views still
//! exist as bridges:
//!
//! - the opaque `QueryDescHandle` newtypes in `types-matview` /
//!   `types-execparallel`.

use ::mcx::{Mcx, McxOwned, MemoryContext, PgBox, PgString, PgVec};

use crate::execnodes::EStateData;
use crate::nodeindexscan::PlannedStmt;
use crate::nodes::{CmdType, Node};
use crate::params::ParamListInfo;
use crate::parsestmt::DestReceiverHandle;
use crate::planstate::PlanStateNode;
use ::types_error::PgResult;
use ::types_tuple::heaptuple::TupleDescData;

/// The executor working state a started query owns, all living in the one
/// per-query "ExecutorState" context.
///
/// `ExecutorStart` fills `planstate` (`ExecInitNode`) and `result_tupdesc`
/// (`ExecGetResultType`); they stay `None` between `CreateQueryDesc` and
/// `ExecutorStart` (the C `NULL`). The `EState` owns the whole plan-state tree,
/// so the tree's `EStateLink` back-pointers (`PlanState.state`) stay valid as
/// long as the bundle keeps both alive together — which it does, because both
/// live in the same context that the bundle owns.
pub struct QueryWorkState<'mcx> {
    /// `EState *estate` — the per-Executor-invocation working storage, made by
    /// `CreateExecutorState` inside this bundle's context.
    pub estate: EStateData<'mcx>,
    /// `PlannedStmt *plannedstmt` — the plan to execute. C aliases the
    /// planner-owned tree; the owned model holds a `copyObject`-shape copy in
    /// the per-query context so the bundle is self-contained.
    pub plannedstmt: PgBox<'mcx, PlannedStmt<'mcx>>,
    /// `const char *sourceText` — source text of the query, copied into the
    /// per-query context.
    pub source_text: PgString<'mcx>,
    /// `PlanState *planstate` — the initialized top-level plan-state tree, built
    /// by `ExecutorStart` (`ExecInitNode`). `None` before `ExecutorStart`.
    pub planstate: Option<PgBox<'mcx, PlanStateNode<'mcx>>>,
}

::mcx::bind!(pub QueryWorkStateTy => QueryWorkState<'mcx>);

/// `QueryDesc` (`executor/execdesc.h`) — the owned executor invocation handle.
///
/// Lifetime-free: the executor working state lives in [`QueryDesc::work`] (an
/// [`McxOwned`] bundle whose inner context the bundle owns); the rest are
/// `Copy`/`Rc`/handle inputs copied in by `CreateQueryDesc`.
pub struct QueryDesc {
    /// `CmdType operation` — `CMD_SELECT`/`INSERT`/`UPDATE`/`DELETE`/`MERGE`/
    /// `UTILITY`, copied from `plannedstmt.commandType` by `CreateQueryDesc`.
    pub operation: CmdType,
    /// `Snapshot snapshot` — snapshot to use for the query, or `None`
    /// (C `InvalidSnapshot`).
    pub snapshot: Option<alloc::rc::Rc<snapshot::SnapshotData>>,
    /// `Snapshot crosscheck_snapshot` — crosscheck snapshot for RI updates/
    /// deletes, or `None` (C `InvalidSnapshot`).
    pub crosscheck_snapshot: Option<alloc::rc::Rc<snapshot::SnapshotData>>,
    /// `DestReceiver *dest` — destination for tuple output. Carried as the
    /// receiver-handle bridge until the DestReceiver receiver-value router
    /// (F0b / tcop-dest keystone) lands; `NULL` is no output.
    pub dest: DestReceiverHandle,
    /// `ParamListInfo params` — external parameter values, or `None`.
    pub params: ParamListInfo,
    /// `int instrument_options` — OR of `InstrumentOption` flags.
    pub instrument_options: i32,
    /// `bool already_executed` — `ExecutorRun` has already been called once.
    pub already_executed: bool,
    /// The executor working state (the `EState` + plan-state tree + the
    /// read-only `PlannedStmt`/source-text copies). `ExecutorStart` populates
    /// `planstate`; `ExecutorEnd`/`FreeQueryDesc` drop the bundle.
    pub work: McxOwned<QueryWorkStateTy>,
}

impl QueryDesc {
    /// `CreateQueryDesc(plannedstmt, sourceText, snapshot, crosscheck_snapshot,
    /// dest, params, queryEnv, instrument_options)` (execMain.c): create the
    /// per-query "ExecutorState" context, the `EState` in it
    /// (`CreateExecutorState`), and copy the read-only inputs in. `planstate` /
    /// the result tupdesc stay `None` until `ExecutorStart`.
    ///
    /// `parent` is the `CurrentMemoryContext` the per-query context is made an
    /// (accounting) child of.
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        parent: &MemoryContext,
        plannedstmt: &PlannedStmt<'_>,
        source_text: &str,
        snapshot: Option<alloc::rc::Rc<snapshot::SnapshotData>>,
        crosscheck_snapshot: Option<alloc::rc::Rc<snapshot::SnapshotData>>,
        dest: DestReceiverHandle,
        params: ParamListInfo,
        instrument_options: i32,
    ) -> PgResult<Self> {
        // qd->operation = plannedstmt->commandType;
        let operation = plannedstmt.commandType;
        // qcontext = AllocSetContextCreate(...,"ExecutorState",...);
        // estate = CreateExecutorState() within it; copy plannedstmt + sourceText.
        let work = McxOwned::<QueryWorkStateTy>::try_new(
            parent.new_child("ExecutorState"),
            |mcx: Mcx<'_>| {
                let plannedstmt = ::mcx::alloc_in(mcx, plannedstmt.clone_in(mcx)?)?;
                let source_text = PgString::from_str_in(source_text, mcx)?;
                Ok(QueryWorkState {
                    estate: EStateData::new_in(mcx),
                    plannedstmt,
                    source_text,
                    planstate: None,
                })
            },
        )?;
        Ok(QueryDesc {
            operation,
            snapshot,
            crosscheck_snapshot,
            dest,
            params,
            instrument_options,
            already_executed: false,
            work,
        })
    }

    // =======================================================================
    // Consumer-facing accessors (QueryDesc de-handle F1b).
    //
    // These let the executor's consumers (portalcmds / copyto / matview /
    // execParallel) read or mutate the owned `QueryDesc` *without* reaching
    // into the executor's internals (execMain's `EState`/plan-state layout):
    // the historical `snapshot`/`dest` views become plain field reads, and the
    // bundle-interior views (`result_tupdesc` / `es_processed` / the
    // `EState`/plan-state mutators) go through these helpers, which open the
    // `McxOwned` bundle internally so no `'mcx` borrow escapes.
    // =======================================================================

    /// `ExecGetResultType(queryDesc->planstate)` (execMain.c, via execUtils):
    /// the top plan node's result `TupleDesc` (`planstate->ps_ResultTupleDesc`),
    /// which is copyto's `tupDesc`. Runs `f` against the borrowed descriptor
    /// (`None` before `ExecutorStart` builds the plan-state tree, or when the
    /// node carries no result tupdesc); the closure returns an owned `R` so no
    /// `'mcx` borrow leaves the bundle.
    pub fn with_result_tupdesc<R>(&self, f: impl FnOnce(Option<&TupleDescData<'_>>) -> R) -> R {
        self.work.with(|w| {
            // C `ExecutorStart`/InitPlan sets `queryDesc->tupDesc` to the junk
            // filter's *cleaned* tuple type when a junk filter is present (e.g.
            // a top SELECT plan with resjunk columns from FOR UPDATE / ORDER BY),
            // falling back to the top plan node's result tupdesc otherwise. The
            // cleaned descriptor is what COPY-(query)-TO builds its attnumlist
            // from, and it must match the junk-filtered slot the executor
            // delivers to the dest receiver — otherwise COPY reads the wrong /
            // out-of-range columns.
            let td = if let Some(jf) = w.estate.es_junkFilter.as_deref() {
                jf.jf_cleanTupType.as_deref()
            } else {
                w.planstate
                    .as_ref()
                    .and_then(|ps| ps.ps_head().ps_ResultTupleDesc.as_deref())
            };
            f(td)
        })
    }

    /// `queryDesc->estate->es_processed` (execMain.c) — the number of tuples
    /// processed by the current command, the value matview's
    /// `refresh_matview_datafill` reads off the finished query.
    pub fn es_processed(&self) -> u64 {
        self.work.with(|w| w.estate.es_processed)
    }

    /// `queryDesc->plannedstmt->queryId` (execdesc.h) — the 64-bit query
    /// identifier the planner copied from the analyzed `Query` (under
    /// `compute_query_id`). Read by the `pg_stat_statements` ExecutorStart/End
    /// hooks. `0` when query-id computation is disabled.
    pub fn query_id(&self) -> i64 {
        self.work.with(|w| w.plannedstmt.queryId)
    }

    /// `queryDesc->plannedstmt->stmt_location` (execdesc.h) — the start offset of
    /// this statement in a (possibly multi-statement) source string.
    pub fn stmt_location(&self) -> i32 {
        self.work.with(|w| w.plannedstmt.stmt_location)
    }

    /// `queryDesc->plannedstmt->stmt_len` (execdesc.h) — the length in bytes of
    /// this statement in the source string (0 = to end of string).
    pub fn stmt_len(&self) -> i32 {
        self.work.with(|w| w.plannedstmt.stmt_len)
    }

    /// `queryDesc->estate->es_total_processed` — total tuples across all
    /// `ExecutorRun` firings (the value `pg_stat_statements` records as `rows`).
    pub fn es_total_processed(&self) -> u64 {
        self.work.with(|w| w.estate.es_total_processed)
    }

    /// `queryDesc->estate->es_parallel_workers_to_launch`.
    pub fn es_parallel_workers_to_launch(&self) -> i32 {
        self.work.with(|w| w.estate.es_parallel_workers_to_launch)
    }

    /// `queryDesc->estate->es_parallel_workers_launched`.
    pub fn es_parallel_workers_launched(&self) -> i32 {
        self.work.with(|w| w.estate.es_parallel_workers_launched)
    }

    /// Mutable access to the owned `EState` (`queryDesc->estate`) through the
    /// bundle. `execParallel` reaches the `EState` interior this way
    /// (`ExecParallelCreateReaders` / `ExecInitParallelPlan` thread the live
    /// `EState`); the closure must typecheck for an arbitrary `'mcx`
    /// (`McxOwned::with_mut`), so no borrow escapes.
    pub fn with_estate_mut<R>(&mut self, f: impl for<'mcx> FnOnce(&mut EStateData<'mcx>) -> R) -> R {
        self.work.with_mut(|w| f(&mut w.estate))
    }

    /// `queryDesc->sourceText` (execdesc.h) — the query source text the parallel
    /// worker copies into `debug_query_string`/`pgstat_report_activity`.
    pub fn source_text_owned(&self) -> alloc::string::String {
        self.work.with(|w| w.source_text.as_str().into())
    }

    /// `queryDesc->plannedstmt->jitFlags = jit_flags` (execParallel.c worker
    /// setup): inherit the leader's JIT decision into the worker plan.
    pub fn set_jit_flags(&mut self, jit_flags: i32) {
        self.work.with_mut(|w| w.plannedstmt.jitFlags = jit_flags);
    }

    /// `queryDesc->estate->es_jit != NULL` — whether a JIT context was created
    /// for this query (false while JIT is unported, as `es_jit` is never set).
    pub fn estate_has_jit(&self) -> bool {
        self.work.with(|w| w.estate.es_jit.0.is_some())
    }

    /// Mutable access to the owned top plan-state tree
    /// (`queryDesc->planstate`) through the bundle. `None` before
    /// `ExecutorStart` builds it. `execParallel` reaches the plan-state interior
    /// this way; the closure must typecheck for an arbitrary `'mcx`, so no
    /// borrow escapes.
    pub fn with_planstate_mut<R>(
        &mut self,
        f: impl for<'mcx> FnOnce(Option<&mut PlanStateNode<'mcx>>) -> R,
    ) -> R {
        self.work
            .with_mut(|w| f(w.planstate.as_deref_mut()))
    }

    /// The leak-projection InitPlan needs (`execMain.c` `InitPlan`): hand the
    /// closure (1) an HONEST `&'mcx Node` for the plan tree
    /// (`plannedstmt->planTree`), (2) a split `&mut EStateData`, and (3) a
    /// `&mut Option<PgBox<PlanStateNode>>` slot to store the built tree into
    /// (`queryDesc->planstate`).
    ///
    /// ## Why this primitive exists
    ///
    /// `ExecInitNode` is signed `node: Option<&'mcx Node<'mcx>>` (C's
    /// `ExecInitNode(plannedstmt->planTree, estate, eflags)`), so `InitPlan`
    /// must pass the plan tree as a real `&'mcx Node`. But the tree lives inside
    /// the [`McxOwned`] bundle (`w.plannedstmt.planTree`), whose interior is only
    /// reachable through `for<'mcx>`-universal closures so that no borrow at the
    /// bundle's private lifetime escapes. This accessor bridges the two: inside
    /// the bundle (at its genuine `'mcx`), it leaks the `planTree`
    /// [`PgBox`](::mcx::PgBox) into an honest `&'mcx Node` via [`::mcx::leak_in`]
    /// (the value still lives until the per-query context drop — faithful to C's
    /// "plan freed with its context"), takes the split `&mut estate` /
    /// `&mut planstate` borrows, and runs the `for<'mcx>` closure. The closure
    /// body must typecheck for an arbitrary `'mcx`, so the `&'mcx Node` cannot
    /// leave the bundle — the same soundness guarantee the other accessors give.
    ///
    /// `planTree` is `None` (the C `plannedstmt->planTree == NULL`) only for a
    /// degenerate plan; the closure sees `None` and would mirror the C handling
    /// (`ExecInitNode(NULL, ...)` returns `NULL`). The leak consumes the bundle's
    /// `planTree` box; the leaked value lives on in the same context until the
    /// bundle (and its per-query context) is dropped, so nothing is freed early.
    pub fn with_plan_and_estate_mut<R>(
        &mut self,
        f: impl for<'mcx> FnOnce(
            Option<&'mcx Node<'mcx>>,
            &mut PlannedStmt<'mcx>,
            &mut EStateData<'mcx>,
            &mut Option<PgBox<'mcx, PlanStateNode<'mcx>>>,
        ) -> R,
    ) -> R {
        self.work.with_mut(|w| {
            // plan = plannedstmt->planTree; leak the owning PgBox into an honest
            // &'mcx Node (lives until the per-query context drops). `take()`
            // moves the box out of the bundle so the leak owns the allocation;
            // the leaked &mut is re-borrowed as a shared &'mcx Node for
            // ExecInitNode's `Option<&'mcx Node>` parameter.
            let plan: Option<&Node<'_>> = w
                .plannedstmt
                .planTree
                .take()
                .map(|tree| &*::mcx::leak_in(tree));
            // The remaining `plannedstmt` fields stay reachable so `InitPlan`
            // can move the range table / permInfos / unprunableRelids out of the
            // bundle into the `EState` (`ExecInitRangeTable`) and read the
            // `commandType` / `rowMarks` / `subplans` / `partPruneInfos` guards.
            f(plan, &mut w.plannedstmt, &mut w.estate, &mut w.planstate)
        })
    }

    /// Mutable access to the owned `EState` *and* the built top plan-state tree
    /// together (`queryDesc->estate` + `queryDesc->planstate`), through the
    /// bundle. `ExecutePlan` / `ExecEndPlan` drive both at once (the per-tuple
    /// `ExecProcNode(planstate)` and the teardown `ExecEndNode(planstate)` both
    /// take `estate` too). `None` planstate is the C "plan never started"
    /// (degenerate `planTree == NULL`); the closure typechecks for an arbitrary
    /// `'mcx`, so no borrow escapes the bundle.
    pub fn with_estate_and_planstate_mut<R>(
        &mut self,
        f: impl for<'mcx> FnOnce(&mut EStateData<'mcx>, Option<&mut PlanStateNode<'mcx>>) -> R,
    ) -> R {
        self.work
            .with_mut(|w| f(&mut w.estate, w.planstate.as_deref_mut()))
    }
}

// Touch `PgVec` so a later builder allocating bundle-internal vectors keeps the
// import live without churn.
#[allow(dead_code)]
fn _allocator_witness<'a>(mcx: Mcx<'a>) -> PgVec<'a, u8> {
    PgVec::new_in(mcx)
}
