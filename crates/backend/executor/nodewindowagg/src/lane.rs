//! Lane-executor-v2 window seams (single-executor Phase 1, WS-H inc-1).
//!
//! The node crate owns ALL semantics; the lane (execmain/lanev2/windows.rs)
//! owns only control flow — the ratified TupleOp contract (lanev2 sorted-agg
//! precedent). This module hosts the W1 class: `WindowAgg(frameOptions ==
//! FRAMEOPTION_DEFAULTS)` whose window functions are all in {row_number(),
//! rank(), dense_rank(), plain default-frame aggregates via the node's
//! compiled evaltrans}, no runCondition, no qual.
//!
//! The machine is group-at-a-time over the sorted input stream, O(1) side
//! state per row:
//!
//!   accept(row) →  part_eq vs first_part_slot (spool_tuples' own compare)
//!               →  ord_eq vs the open peer group's HEAD row (ONE
//!                  grouping-equality eval per row, shared by ALL rank-family
//!                  functions and the agg peer-frame — the row engine's agg
//!                  lookahead compares each row against the group head too,
//!                  and btree-opfamily equality is transitive)
//!               →  puttupleslot into a lane-private work_mem Tuplestore
//!                  (tuplestore-native spill; byte-equal budget to the row
//!                  engine's buffer)
//!               →  evaltrans transition (same compiled program, same row
//!                  order, per-row tmpcontext reset cadence).
//!
//! On a peer boundary: ONE finalize per peer group (the row engine's own
//! cadence — `eval_windowaggregates_default` finalizes on the group's first
//! row and reuses `agg_saved` for the rest) via the shared
//! `default_agg_finalize_save`; then emission serves the finalized group's
//! rows sequentially from the store's trailing forward read pointer (zero
//! seek churn), computing row_number/rank/dense_rank as pure arithmetic from
//! the ONE boundary stream, writing through ecxt_aggvalues/ecxt_aggnulls,
//! and projecting through the node's own projection (incl. the subplan arm)
//! with the per-row ps_ExprContext reset cadence replayed.
//!
//! CORRECTNESS TRAPS the corpus pins (scripts/dualexec/corpus-windows.sql):
//! peer rows (ties) under the default frame make running aggregates step by
//! PEER GROUP, not by row — every member of a peer group sees the aggregate
//! over the whole group; rank jumps by peer-group size while dense_rank
//! steps by 1; NULL ORDER BY keys are peers of each other (grouping equality
//! treats NULLs as equal, exactly the ported `are_peers` program).
//!
//! Returned-slot lifetime: a projected row's by-ref datums may alias the
//! lane store's tuple arena (in-mem borrowed fetches are valid until
//! clear/end; the File arm always copies). The store is therefore cleared
//! only on the FIRST begin of the NEXT partition — which the driver
//! structure puts in a LATER pull than the one that returned the previous
//! partition's last row (capacity-one root pauses per emitted row) — the
//! Volcano contract's "valid until the next call", exactly the row engine's
//! own `release_partition` timing.

use ::datum::{Datum, NullableDatum};
use ::execexpr::{exec_eval_expr, exec_project, exec_qual, EvalSlots};
use ::executils::{EStateData, ExecSlotId};
use ::tuplestore::Tuplestore;
use ::types_error::{PgError, PgResult};
use ::types_nodes::rawnodes::FRAMEOPTION_DEFAULTS;

use crate::{WaStatus, WfKind, WindowAggStateData};

/// Lane-private cross-call drive state (stored on execmain's WindowAggNode).
/// The store's read pointer 0 is the trailing emit pointer (forward-only,
/// `set_eflags(0)`); slots live on the node: `first_part_slot` keeps the
/// current partition's first row / the parked next-partition row (the row
/// engine's own convention), `agg_row_slot` holds the OPEN peer group's head
/// row (the row engine's own agg peer-lookahead slot), `scan_slot` carries
/// the emitted row into the projection.
pub struct LaneWindowDrive {
    store: Option<Tuplestore>,
    work_mem_kb: i32,
    /// Rows spooled into the store for the current partition.
    spooled: i64,
    /// Next partition-relative position to emit (== rows already emitted).
    emit_pos: i64,
    /// Exclusive end of the finalized (emittable) prefix.
    emit_end: i64,
    /// rank of the rows in `[group_start_of_emitting_group, emit_end)`.
    emit_rank: i64,
    /// dense_rank of those rows.
    emit_dense: i64,
    /// First position of the OPEN (unfinalized) peer group.
    group_start: i64,
    /// 1-based ordinal of the open peer group (= its dense_rank).
    group_ord: i64,
    /// A partition is being accumulated / emitted.
    partition_open: bool,
    /// The open partition will receive no more rows (all groups final).
    partition_done: bool,
    /// The NEXT partition's first row is parked in the node's
    /// first_part_slot (the row engine's own convention).
    boundary_saved: bool,
}

impl LaneWindowDrive {
    pub fn new(work_mem_kb: i32) -> Self {
        LaneWindowDrive {
            store: None,
            work_mem_kb,
            spooled: 0,
            emit_pos: 0,
            emit_end: 0,
            emit_rank: 0,
            emit_dense: 0,
            group_start: 0,
            group_ord: 0,
            partition_open: false,
            partition_done: false,
            boundary_saved: false,
        }
    }
}

/// Structural (init-stable, memoizable) admission census for the W1 class.
/// Admission is program-based, not per-aggregate: any aggregate the node
/// compiled into its default-frame evaltrans qualifies (`peragg` empty =
/// no framed carriers exist). Freshness is separate (`lane_window_fresh`).
pub fn lane_window_shape_admissible(state: &WindowAggStateData<'_>) -> bool {
    state.frameOptions == FRAMEOPTION_DEFAULTS
        && state.peragg.is_empty()
        && state.runcondition.is_none()
        && state.qual.is_none()
        && state.perfunc.iter().all(|pf| {
            matches!(
                pf.kind,
                WfKind::RowNumber | WfKind::Rank | WfKind::DenseRank | WfKind::PlainAgg { .. }
            )
        })
}

/// Node freshness: the lane owns from the FIRST pull or never (sticky
/// ownership is all-or-nothing per (re)scan; a row-engine-driven node holds
/// Volcano state the lane machine cannot resume).
pub fn lane_window_fresh(state: &WindowAggStateData<'_>) -> bool {
    state.status == WaStatus::Run
        && state.next_partition
        && !state.first_part_valid
        && !state.partition_spooled
        && state.buffer.is_none()
        && state.spooled_rows == 0
}

/// The drained guard (exec_window_agg's `status == Done` arm).
pub fn lane_window_done(state: &WindowAggStateData<'_>) -> bool {
    state.status == WaStatus::Done
}

/// Plan node id for the EXPLAIN (ENGINE) chokepoint capture.
pub fn lane_plan_node_id(state: &WindowAggStateData<'_>) -> i32 {
    state.plan.plan.plan_node_id
}

/// First-pull prologue: exec_window_agg's `all_first` arm. For the admitted
/// FRAMEOPTION_DEFAULTS shape there are no frame offsets, so this evaluates
/// nothing — it replays the same ps_ExprContext reset and flag flip, keeping
/// the node state identical to the row engine's after its first call (a
/// pre-ownership feed refuse falls back byte-safely).
pub fn lane_window_begin<'mcx>(
    state: &mut WindowAggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if state.all_first {
        state.calculate_frame_offsets(estate)?;
    }
    Ok(())
}

pub enum LaneAccept {
    /// Row absorbed; feed the next one.
    NeedMore,
    /// A peer group was finalized (a new group opened with this row);
    /// its rows await emission.
    GroupReady,
    /// A partition boundary: the open partition's last group was finalized
    /// (rows await emission) and the incoming row is parked as the next
    /// partition's first row.
    PartitionBoundary,
}

/// One sorted input row.
pub fn lane_window_accept<'mcx>(
    state: &mut WindowAggStateData<'mcx>,
    drive: &mut LaneWindowDrive,
    estate: &mut EStateData<'mcx>,
    tuple: ExecSlotId,
) -> PgResult<LaneAccept> {
    debug_assert!(!lane_window_emit_pending(drive), "accept while rows pend emission");
    let mcx = estate.es_query_cxt;
    // `partition_done` = the open partition's groups are all final (its
    // boundary row is parked); the next accept must begin the NEW partition.
    if !drive.partition_open || drive.partition_done {
        if drive.boundary_saved {
            // First accept of the next partition: begin it from the parked
            // row (this is where the previous partition's store contents are
            // cleared — see the module doc on returned-slot lifetime).
            begin_partition(state, drive, estate, None)?;
            // Fall through: the incoming row joins (or bounds) the new one.
        } else {
            // First row of the stream.
            begin_partition(state, drive, estate, Some(tuple))?;
            return Ok(LaneAccept::NeedMore);
        }
    }

    // Partition boundary check — spool_tuples' own compare + reset cadence.
    if state.plan.partNumCols > 0 {
        let same = {
            let WindowAggStateData { ref mut part_eq, ref mut first_part_slot, .. } = *state;
            let outer_slot = estate.slot_mut(tuple);
            let mut slots =
                EvalSlots { scan: None, inner: Some(first_part_slot), outer: Some(outer_slot) };
            exec_qual(part_eq.as_deref_mut(), &mut slots)?
        };
        estate.reset_expr_context(state.tmpcontext);
        if !same {
            // Park the next partition's first row (the row engine's own
            // first_part_slot convention, spool_tuples' boundary arm).
            {
                let outer_slot = estate.slot_mut(tuple);
                exectuples::exec_copy_slot(&mut state.first_part_slot, outer_slot, mcx, mcx)?;
            }
            drive.boundary_saved = true;
            drive.partition_done = true;
            close_open_group(state, drive, estate)?;
            return Ok(LaneAccept::PartitionBoundary);
        }
    }

    // ONE peer-boundary eval per row against the open group's head — the
    // row engine's are_peers compare (+ its tmpcontext reset cadence),
    // shared by the rank family and the agg peer-frame.
    let peers = if state.ord_eq.is_none() {
        // No ORDER BY: all partition rows are peers (are_peers' None arm).
        true
    } else {
        let r = {
            let WindowAggStateData { ref mut ord_eq, ref mut agg_row_slot, .. } = *state;
            let outer_slot = estate.slot_mut(tuple);
            let mut slots =
                EvalSlots { scan: None, inner: Some(agg_row_slot), outer: Some(outer_slot) };
            exec_qual(ord_eq.as_deref_mut(), &mut slots)?
        };
        estate.reset_expr_context(state.tmpcontext);
        r
    };
    if peers {
        spool_and_transition(state, drive, estate, tuple)?;
        return Ok(LaneAccept::NeedMore);
    }

    // Peer-group boundary: finalize [group_start, spooled) FIRST, then open
    // the next group with this row — the boundary row's transition lands
    // after the finalize, exactly the row engine's order (its agg lookahead
    // holds the boundary row un-transitioned across the finalize).
    close_open_group(state, drive, estate)?;
    spool_and_transition(state, drive, estate, tuple)?;
    {
        let WindowAggStateData { ref mut agg_row_slot, .. } = *state;
        let outer_slot = estate.slot_mut(tuple);
        exectuples::exec_copy_slot(agg_row_slot, outer_slot, mcx, mcx)?;
    }
    drive.group_start = drive.spooled - 1;
    drive.group_ord += 1;
    Ok(LaneAccept::GroupReady)
}

/// Source exhausted. Closes the open group / partition, then (once that is
/// fully emitted and this is called again) begins the parked partition, if
/// any. Returns whether rows await emission; `false` = fully drained (the
/// node is marked Done — drained stays drained, idempotent).
pub fn lane_window_input_done<'mcx>(
    state: &mut WindowAggStateData<'mcx>,
    drive: &mut LaneWindowDrive,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    loop {
        if lane_window_emit_pending(drive) {
            return Ok(true);
        }
        if drive.partition_open && !drive.partition_done {
            drive.partition_done = true;
            close_open_group(state, drive, estate)?;
            continue;
        }
        if drive.boundary_saved {
            // The parked row is a (possibly one-row) final partition.
            begin_partition(state, drive, estate, None)?;
            continue;
        }
        state.status = WaStatus::Done;
        return Ok(false);
    }
}

pub fn lane_window_emit_pending(drive: &LaneWindowDrive) -> bool {
    drive.emit_pos < drive.emit_end
}

/// Next projected row of the finalized region: trailing-pointer fetch into
/// scan_slot (copy=false — sound per the store's in-mem borrow contract; the
/// File arm always copies), write_agg_result per wfunc (arithmetic ranks
/// from the boundary stream + the group's saved aggregate results), per-row
/// ps_ExprContext reset + the node's own projection incl. the subplan arm.
/// `None` = region drained (the driver returns to accept).
pub fn lane_window_emit_next<'mcx>(
    state: &mut WindowAggStateData<'mcx>,
    drive: &mut LaneWindowDrive,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    if !lane_window_emit_pending(drive) {
        return Ok(None);
    }
    let mcx = estate.es_query_cxt;
    // The row engine's per-output-row reset (exec_window_agg's loop top):
    // frees the PREVIOUS row's per-tuple projection memory — the parent
    // consumed that row before pulling again.
    estate.reset_expr_context(state.ps_ExprContext);
    {
        let store = drive.store.as_mut().expect("emit region implies a store");
        if !store.gettupleslot(true, false, &mut state.scan_slot, mcx)? {
            return Err(Box::new(PgError::error(
                "lane-v2 windows: unexpected end of tuplestore".to_string(),
            )));
        }
    }
    let pos = drive.emit_pos;
    for i in 0..state.perfunc.len() {
        let kind = state.perfunc[i].kind;
        let wfuncno = state.perfunc[i].wfuncno as usize;
        let nd = match kind {
            WfKind::RowNumber => NullableDatum::value(Datum::from_i64(pos + 1)),
            WfKind::Rank => NullableDatum::value(Datum::from_i64(drive.emit_rank)),
            WfKind::DenseRank => NullableDatum::value(Datum::from_i64(drive.emit_dense)),
            // The aggcontext frame-reuse copy — the same datum bytes the row
            // engine serves every non-first row of the group from agg_saved.
            WfKind::PlainAgg { aggno } => state.agg_saved[aggno as usize],
            _ => unreachable!("lane_window_shape_admissible admitted a non-W1 function"),
        };
        state.write_agg_result(wfuncno, nd);
    }
    drive.emit_pos += 1;
    // The node's own projection, both arms (exec_window_agg's tail).
    if state.proj.has_subplan() {
        let ecxt = state.ps_ExprContext;
        let result = state.ps_ResultTupleSlot;
        let WindowAggStateData { ref mut proj, ref mut scan_slot, .. } = *state;
        ::executils::exec_project_with_subplans_outer(proj, scan_slot, estate, ecxt, result)?;
    } else {
        let result_slot = estate.slot_mut(state.ps_ResultTupleSlot);
        let mut slots = EvalSlots { scan: None, inner: None, outer: Some(&mut state.scan_slot) };
        exec_project(&mut state.proj, &mut slots, result_slot, mcx)?;
    }
    Ok(Some(state.ps_ResultTupleSlot))
}

/// Rescan/end hook: forget everything, keep the store allocation (its
/// contents are cleared; read pointer 0 rewinds to the head).
pub fn lane_window_reset(drive: &mut LaneWindowDrive) {
    if let Some(store) = drive.store.as_mut() {
        store.clear();
    }
    drive.spooled = 0;
    drive.emit_pos = 0;
    drive.emit_end = 0;
    drive.emit_rank = 0;
    drive.emit_dense = 0;
    drive.group_start = 0;
    drive.group_ord = 0;
    drive.partition_open = false;
    drive.partition_done = false;
    drive.boundary_saved = false;
}

/// Begin a partition: deferred previous-partition clear, per-partition agg
/// restart (the shared `default_agg_partition_init`), spool + transition of
/// row 0 (always served from first_part_slot — both the incoming-row arm,
/// which copies it there first per the row engine's begin_partition, and the
/// parked-boundary arm, where it already sits).
fn begin_partition<'mcx>(
    state: &mut WindowAggStateData<'mcx>,
    drive: &mut LaneWindowDrive,
    estate: &mut EStateData<'mcx>,
    tuple: Option<ExecSlotId>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    match drive.store.as_mut() {
        Some(store) => {
            if drive.spooled > 0 {
                // The previous partition's rows die here — a LATER pull than
                // the one that returned its last row (module doc).
                store.clear();
            }
        }
        None => {
            // The row engine's prepare_tuplestore budget (work_mem), one
            // forward-only read pointer (the trailing emit pointer).
            let mut store = Tuplestore::begin_heap(false, false, drive.work_mem_kb);
            store.set_eflags(0);
            drive.store = Some(store);
        }
    }
    drive.spooled = 0;
    drive.emit_pos = 0;
    drive.emit_end = 0;
    drive.group_start = 0;
    drive.group_ord = 1;
    drive.partition_open = true;
    drive.partition_done = false;
    drive.boundary_saved = false;

    if let Some(t) = tuple {
        // The row engine's begin_partition copy (first fetched row →
        // first_part_slot).
        let outer_slot = estate.slot_mut(t);
        exectuples::exec_copy_slot(&mut state.first_part_slot, outer_slot, mcx, mcx)?;
    }
    // A row exists here — the deps-hoist cadence of exec_window_agg (C never
    // reads these params on an empty input).
    if !state.deps_hoisted {
        state.hoist_pending_initplans(estate)?;
        state.deps_hoisted = true;
    }
    // Aggregates restart on the partition's first row (the row engine's
    // currentpos == 0 arm), BEFORE its transition.
    if state.numaggs > 0 {
        state.default_agg_partition_init()?;
    }
    {
        let store = drive.store.as_mut().expect("created above");
        store.puttupleslot(&mut state.first_part_slot, mcx)?;
    }
    drive.spooled = 1;
    transition_first_part_row(state, estate)?;
    if state.ord_eq.is_some() {
        // Row 0 heads the open peer group.
        let WindowAggStateData { ref mut agg_row_slot, ref mut first_part_slot, .. } = *state;
        exectuples::exec_copy_slot(agg_row_slot, first_part_slot, mcx, mcx)?;
    }
    Ok(())
}

/// Finalize the open peer group `[group_start, spooled)` — one finalize per
/// group, the row engine's own cadence — and stage it for emission.
fn close_open_group<'mcx>(
    state: &mut WindowAggStateData<'mcx>,
    drive: &mut LaneWindowDrive,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(drive.spooled > drive.group_start, "empty peer group");
    debug_assert!(drive.emit_pos == drive.emit_end, "close while rows pend emission");
    if state.numaggs > 0 {
        state.default_agg_finalize_save(estate)?;
    }
    drive.emit_rank = drive.group_start + 1;
    drive.emit_dense = drive.group_ord;
    drive.emit_end = drive.spooled;
    Ok(())
}

/// Spool one input row and run its evaltrans transition (same compiled
/// program, same row order, per-row tmpcontext reset — the row engine's
/// transition-loop cadence).
fn spool_and_transition<'mcx>(
    state: &mut WindowAggStateData<'mcx>,
    drive: &mut LaneWindowDrive,
    estate: &mut EStateData<'mcx>,
    tuple: ExecSlotId,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    {
        let store = drive.store.as_mut().expect("partition open implies a store");
        let outer_slot = estate.slot_mut(tuple);
        store.puttupleslot(outer_slot, mcx)?;
    }
    drive.spooled += 1;
    if state.numaggs > 0 {
        {
            let et = state.evaltrans.as_mut().expect("numaggs > 0 implies evaltrans");
            if et.has_subplan() {
                ::executils::exec_eval_expr_with_subplans_outer_slot(
                    et,
                    estate,
                    state.tmpcontext,
                    tuple,
                )?;
            } else {
                let outer_slot = estate.slot_mut(tuple);
                let mut slots =
                    EvalSlots { scan: None, inner: None, outer: Some(outer_slot) };
                exec_eval_expr(et, &mut slots)?;
            }
        }
        estate.reset_expr_context(state.tmpcontext);
    }
    Ok(())
}

/// Row-0 transition off first_part_slot (a node-owned slot, so the subplan
/// arm rides the explicit-outer driver like the row engine's agg_row_slot).
fn transition_first_part_row<'mcx>(
    state: &mut WindowAggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if state.numaggs == 0 {
        return Ok(());
    }
    {
        let WindowAggStateData { ref mut evaltrans, ref mut first_part_slot, tmpcontext, .. } =
            *state;
        let et = evaltrans.as_mut().expect("numaggs > 0 implies evaltrans");
        if et.has_subplan() {
            ::executils::exec_eval_expr_with_subplans_outer(
                et,
                first_part_slot,
                estate,
                tmpcontext,
            )?;
        } else {
            let mut slots =
                EvalSlots { scan: None, inner: None, outer: Some(&mut *first_part_slot) };
            exec_eval_expr(et, &mut slots)?;
        }
    }
    estate.reset_expr_context(state.tmpcontext);
    Ok(())
}

// The drive owns a Tuplestore (fd-guard Drop on the spill arm) — droppy.
mcx::forget_safe_struct!(
    LaneWindowDrive { work_mem_kb, spooled, emit_pos, emit_end, emit_rank,
        emit_dense, group_start, group_ord, partition_open, partition_done,
        boundary_saved; store },
);
