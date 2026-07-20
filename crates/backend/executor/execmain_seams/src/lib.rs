use std::rc::Rc;

use tcop_dest::DestReceiver;
use types_dest::CommandDest;
use types_error::PgResult;
use types_nodes::nodes_enums::CmdType;
use types_nodes::plannodes::PlannedStmt;
use types_portal::{CachedPlanHandle, ParamListHandle, QueryDescHandle, QueryEnvHandle};
use types_scan::sdir::ScanDirection;
use types_core::instrument::{
    AggregateInstrumentation, BitmapHeapScanInstrumentation, HashInstrumentation,
    IncrementalSortInfo, Instrumentation, MemoizeInstrumentation, TuplesortInstrumentation,
    TuplestoreInstrumentation,
};
use types_snapshot::SnapshotData;
use types_tuple::TupleDescData;

pub type Snapshot = Rc<SnapshotData<'static>>;

seam_core::seam!(
    // Retention contract: caller keeps plannedstmt/source_text alive until
    // free_query_desc (C's raw-pointer rule); the live receiver threads
    // per-run, dest is only the marker.
    pub fn create_query_desc<'p, 'a, 's>(
        plannedstmt: &'p PlannedStmt<'a>,
        source_text: &'s str,
        snapshot: Option<Snapshot>,
        crosscheck_snapshot: Option<Snapshot>,
        dest: CommandDest,
        params: ParamListHandle,
        query_env: QueryEnvHandle,
        instrument_options: i32,
    ) -> PgResult<QueryDescHandle>
);

seam_core::seam!(
    pub fn free_query_desc(query_desc: QueryDescHandle)
);

seam_core::seam!(
    // Cached-plan handle for the NEXT create_query_desc on this backend (no C
    // counterpart): the executor-skeleton cache keys and refcounts on it.
    pub fn note_cplan_for_query_desc(cplan: CachedPlanHandle)
);

seam_core::seam!(
    // Abort-path reclamation: C frees the QueryDesc with the portal context
    // and never runs ExecutorEnd; snapshot registrations are the resource
    // owner's to release.
    pub fn release_query_desc(query_desc: QueryDescHandle)
);

seam_core::seam!(
    pub fn executor_start(query_desc: QueryDescHandle, eflags: i32) -> PgResult<()>
);

seam_core::seam!(
    pub fn executor_run<'d, 'mcx>(
        query_desc: QueryDescHandle,
        direction: ScanDirection,
        count: u64,
        dest: &'d mut DestReceiver<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn executor_finish(query_desc: QueryDescHandle) -> PgResult<()>
);

seam_core::seam!(
    // Portal-retention park (no C counterpart): ExecutorFinish + in-place
    // skeleton disarm, executor left attached to the QueryDesc. false = the
    // normal ExecutorEnd + FreeQueryDesc ran instead; the caller's cleanup
    // hook is consumed either way.
    pub fn executor_finish_and_park(query_desc: QueryDescHandle) -> PgResult<bool>
);

seam_core::seam!(
    // Portal-retention reuse (no C counterpart): rearm the QueryDesc's
    // retained executor under this execution's snapshot/params. false =
    // param shape mismatch; caller sheds the retained executor and builds
    // fresh via ExecutorStart.
    pub fn executor_rearm(
        query_desc: QueryDescHandle,
        snapshot: Option<Snapshot>,
        params: ParamListHandle,
    ) -> PgResult<bool>
);

seam_core::seam!(
    pub fn executor_rewind(query_desc: QueryDescHandle) -> PgResult<()>
);

seam_core::seam!(
    pub fn executor_end(query_desc: QueryDescHandle) -> PgResult<()>
);

seam_core::seam!(
    pub fn query_desc_es_processed(query_desc: QueryDescHandle) -> u64
);

seam_core::seam!(
    pub fn query_desc_snapshot(query_desc: QueryDescHandle) -> Option<Snapshot>
);

seam_core::seam!(
    pub fn query_desc_result_tupdesc(
        query_desc: QueryDescHandle,
    ) -> Option<Rc<TupleDescData<'static>>>
);

seam_core::seam!(
    pub fn query_desc_operation(query_desc: QueryDescHandle) -> CmdType
);

seam_core::seam!(
    // ExplainNode's planstate->instrument read (runs C's forced InstrEndLoop).
    pub fn query_desc_instrument(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<Instrumentation>
);

seam_core::seam!(
    // ExplainMissingMembers/ExplainMemberNodes: initially valid subplan
    // indexes for the Append at part_prune_index (None = no initial pruning).
    pub fn query_desc_prune_result(
        query_desc: QueryDescHandle,
        part_prune_index: i32,
    ) -> Option<Vec<i32>>
);

seam_core::seam!(
    // show_modifytable_info reads mtstate->resultRelInfo, which excludes
    // initially-pruned result relations: es_unpruned_relids membership.
    pub fn query_desc_rti_unpruned(query_desc: QueryDescHandle, rti: i32) -> Option<bool>
);

seam_core::seam!(
    pub fn query_desc_agg_instrument(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<AggregateInstrumentation>
);

seam_core::seam!(
    pub fn query_desc_sort_instrument(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<TuplesortInstrumentation>
);

seam_core::seam!(
    pub fn query_desc_incsort_instrument(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<IncrementalSortInfo>
);

seam_core::seam!(
    pub fn query_desc_index_instrument(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<u64>
);

seam_core::seam!(
    pub fn query_desc_hash_instrument(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<HashInstrumentation>
);

seam_core::seam!(
    pub fn query_desc_tuplestore_instrument(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<TuplestoreInstrumentation>
);

seam_core::seam!(
    pub fn query_desc_memoize_instrument(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<MemoizeInstrumentation>
);

seam_core::seam!(
    pub fn query_desc_bitmap_instrument(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<BitmapHeapScanInstrumentation>
);

seam_core::seam!(
    pub fn query_desc_index_searches(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<u64>
);

seam_core::seam!(
    // C queryDesc->estate->{es_jit_flags, es_jit->instr} for EXPLAIN's JIT
    // block: (jit_flags, created_functions, generation_nanos).
    pub fn query_desc_jit_instr(query_desc: QueryDescHandle) -> (i32, i32, u64)
);

seam_core::seam!(
    // EA-on-morsels (docs/design/ea-morsels.md §6): the runtime admission
    // walk's refusal records for a node — (arm, reason) pairs. None = no
    // records, print nothing (records exist ONLY on armed + instrumented
    // walks, which is the emission gate keeping unarmed EA output C-exact).
    pub fn query_desc_runtime_ea_refusals(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<Vec<(&'static str, &'static str)>>
);

seam_core::seam!(
    // EA-on-morsels (docs/design/ea-morsels.md §4): the engaged runtime
    // pipeline reports rooted at a node. None = none (reports exist ONLY on
    // armed + instrumented engagements — the same emission-gate law as the
    // refusal records).
    pub fn query_desc_runtime_ea_pipeline(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<Vec<types_core::instrument::RuntimeEaPipeline>>
);

seam_core::seam!(
    // EXPLAIN (ENGINE) (single-executor Phase 0.2): per-node engine
    // attribution records — (engine, class, detail) triples. None = none
    // (records exist ONLY under EXEC_FLAG_ENGINE_REPORT — the same
    // emission-gate law as the EA records above, keeping default EXPLAIN
    // output byte-identical).
    pub fn query_desc_engine_events(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<Vec<(types_core::instrument::EngineKindWire, &'static str, &'static str)>>
);

seam_core::seam!(
    // Gather/GatherMerge nworkers_launched (EXPLAIN's Workers Launched).
    pub fn query_desc_workers_launched(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<i32>
);

seam_core::seam!(
    // MERGE (mt_merge_inserted, mt_merge_updated, mt_merge_deleted) for
    // EXPLAIN ANALYZE's Tuples: line (skipped = source total - these).
    pub fn query_desc_merge_instrument(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<(f64, f64, f64)>
);

seam_core::seam!(
    // Per-worker Instrumentation for a node, indexed by worker number
    // (C planstate->worker_instrument).
    pub fn query_desc_worker_instrument(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<Vec<Instrumentation>>
);

seam_core::seam!(
    // (worker number, sort instrumentation) pairs (C SortState.shared_info).
    pub fn query_desc_worker_sort_instrument(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<Vec<(i32, TuplesortInstrumentation)>>
);

seam_core::seam!(
    // (worker number, heap-block stats) pairs
    // (C BitmapHeapScanState.sinstrument).
    pub fn query_desc_worker_bitmap_instrument(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<Vec<(i32, BitmapHeapScanInstrumentation)>>
);

seam_core::seam!(
    // (worker number, incsort info) pairs (C IncrementalSortState.shared_info).
    pub fn query_desc_worker_incsort_instrument(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
    ) -> Option<Vec<(i32, IncrementalSortInfo)>>
);

seam_core::seam!(
    // show_foreignscan_info (explain.c): drive the ForeignScan node's
    // provider ExplainForeignScan; properties cross as (label, value) pairs
    // (types_nodes::FdwExplainProp — C passes ExplainState, which would
    // cycle the crate graph; FdwExplainFlags marshals the ExplainState bits
    // the hooks read: es->costs, es->verbose).
    pub fn query_desc_foreign_explain<'e>(
        query_desc: QueryDescHandle,
        plan_node_id: i32,
        flags: types_nodes::FdwExplainFlags,
        emit: &'e mut dyn FnMut(&str, types_nodes::FdwExplainProp<'_>) -> PgResult<()>,
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn exec_clean_type_from_tl<'p, 'a>(
        pstmt: &'p PlannedStmt<'a>,
    ) -> PgResult<Rc<TupleDescData<'static>>>
);

seam_core::seam!(
    // execCurrentOf (execCurrent.c): C's (cexpr, econtext, table_oid, *tid)
    // -> bool; None = valid cursor not on a row of this table. Direct dep
    // would cycle (execmain -> nodetidscan -> execmain). table_name rides in
    // from the caller's open scan relation (C re-derives it via syscache).
    pub fn exec_current_of<'a>(
        cursor_name: Option<&'a str>,
        cursor_param: i32,
        table_oid: types_core::Oid,
        table_name: &'a str,
    ) -> PgResult<Option<types_tuple::ItemPointerData>>
);

seam_core::seam!(
    // ExecCheckPermissions (execMain.c) over a bare permInfos list; COPY
    // checks per-column privileges without a PlannedStmt (copy.c DoCopy).
    pub fn exec_check_permissions<'p, 'a>(
        perm_infos: &'p types_nodes::NodeList<'a>,
    ) -> PgResult<()>
);

// --- WS-CA wave-10 (cursors inc-2, contract §4; escalation EX-CA-1 in
// notes/se-wave10-ca.md §3 — appended here because production pquery reaches
// the executor only through this crate; the exec_current_of seam above is the
// precedent shape). -----------------------------------------------------------

seam_core::seam!(
    // §4.1 eligibility shape test, run once per SCROLL portal at PortalStart
    // (BEFORE ExecutorStart fixes the eflags): true iff search_plan_tree's
    // spine can ever resolve a simply-updatable scan of this plan. Eligible
    // armed portals keep C's REWIND|BACKWARD child flags so the fill drive
    // stays on the row chain (the contract §3.3 carve-out; the batch engine's
    // eflags refusal is the interim mechanism until WS-CB's named
    // cursor-currentof-tidcapture reason supersedes it). false ⇒ the
    // per-table walk can never succeed: no capture, error arms only.
    pub fn cursor_plan_current_of_eligible<'p, 'a>(
        pstmt: &'p PlannedStmt<'a>,
    ) -> bool
);

seam_core::seam!(
    // §4.2 per-row capture after each single-row forward fill drive of a
    // CURRENT-OF-eligible plan: the (tableoid, block<<16|offset ctid)
    // identity of the scan-state row that produced the plan's current output
    // row — the SAME data C's execCurrentOf reads (execCurrent.c:155-232).
    // None = no positioned scan (the caller stores an invalid-identity row to
    // keep sidecar/store row alignment).
    pub fn cursor_capture_current(
        query_desc: QueryDescHandle,
    ) -> PgResult<Option<(types_core::Oid, u64)>>
);
// --- end WS-CA wave-10 ---------------------------------------------------------

// --- SEAM-WIRING (SE10-GATES item 1, se/seam-wiring; notes/se-seam-wiring.md) ---
// The CA-side consumption of WS-CB's EX-CB-1 faces. Production pquery reaches
// the executor only through this crate (the EX-CA-1 precedent above), so the
// three lanev2-hosted portal faces ride seams; execmain installs them in
// init_seams onto the push.rs implementations. All three are cheap statics —
// no PgResult ceremony needed.

seam_core::seam!(
    // §7.3 knob face — THE single knob cell (lanev2/push.rs `CURSORS`,
    // env PGRUST_LANE_V2_CURSORS): the portal layer gates store arming on
    // it. Replaces the retired portalmem duplicate cell (CB review F1(a)).
    pub fn cursor_store_fill_enabled() -> bool
);

seam_core::seam!(
    // §6 deletion-clock staging: called once per store ARMING decision at
    // PortalStart (store_armed = true). Arms the run seam's forward-only
    // debug assert (a store-armed knob-ON world never legally drives the
    // executor backward) — CB review F1(b).
    pub fn cursor_store_armed_note()
);

// R1a (night/r1a-impl, §2a reason-41 completion): the §3.3
// `cursor_fill_tid_capture_refused` accounting seam is RETIRED. Its sole
// caller — fill_portal_store_to's row-chain arm B, with its post-run
// `ss_ScanTupleSlot` read — was deleted; every CURRENT-OF-eligible fill now
// captures identity IN-RUN (batch sink / capture row loop), so reason 41
// never fires (stats.rs keeps the discriminant as an append-only TOMBSTONE).
// --- end SEAM-WIRING -------------------------------------------------------------

// --- SE-R41 (reason-41 retirement, se/r41-retire; notes/se-r41-retire.md) --------

seam_core::seam!(
    // §3.1 capture-batchable probe, run at PortalStart only for
    // CURRENT-OF-ELIGIBLE armed portals: true iff the plan is the batch
    // store-fill shape (bare T_SeqScan top over a tid-capable heap AM —
    // the plan-side twin of `cursor_store_batch_fill`'s planstate gate).
    // TRUE ⇒ the portal takes the PLAIN store-armed eflags and its fill
    // captures §4.2 identity INSIDE the run (batch sink / capture row
    // loop); FALSE (and uninstalled worlds) keep the D-CA-2 fence + the
    // row-chain capture loop verbatim.
    pub fn cursor_plan_capture_batch_fill<'p, 'a>(
        pstmt: &'p PlannedStmt<'a>,
    ) -> bool
);
// --- end SE-R41 ------------------------------------------------------------------
