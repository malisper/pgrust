//! M3 shared-build hash join — executor bridge (m3-joins.md §3/§5).
//!
//! The per-row halves the runtime arm drives on each helper:
//! - BUILD-ACCEPT: [`shared_build_accept`] — the row path's build hash
//!   (`hash_insert_slot`'s eval, verbatim: non-strict fold keeps NULL-key
//!   tuples; they never match the recheck, so results equal C for every
//!   jointype) + minimal-tuple materialization into the worker's
//!   [`JoinBuildLocal`], instead of `ExecHashTableInsert`.
//! - PROBE: [`shared_probe_outer`] — `exec_hash_join`'s per-outer-row
//!   HJ_NEED_NEW_OUTER → HJ_SCAN_BUCKET → HJ_FILL_OUTER_TUPLE arc over the
//!   [`FrozenJoinTable`], single-batch, no dense seat, no bloom (v1),
//!   emitting joined/projected rows through a callback. Phase-1 join types
//!   only: INNER / LEFT / SEMI / ANTI (all probe-local; no match flags).
//!
//! The leader's HashJoinState is NEVER driven by the runtime arm — each
//! helper probes with its OWN executor's node state; the only cross-thread
//! object is the frozen table. Row order within one outer row's matches is
//! chain order; ACROSS outer rows the runtime interleaves morsels — order-
//! insensitive by contract (the 2026-07-13 directive; tie-normalized
//! oracle).

use std::ptr::NonNull;

use ::exectuples::exec_fetch_slot_minimal_tuple;
use ::executils::{EStateData, ExecSlotId};
use ::execexpr::{exec_qual, EvalSlots};
use ::types_error::PgResult;
use ::types_nodes::JoinType;
use ::types_tuple::MinimalTupleData;

use crate::shared_build::{BudgetExceeded, FrozenJoinTable, JoinBuildLocal};
use crate::{eval_probe_qual, project_result, HashJoinState};
use ::nodehash::HashState;

/// Runtime-join admission: all eight hash-join types (inc-3 added the
/// right-fill family) on top of the serial lane's expression admission
/// (subplan/param-free quals + projection + hash exprs, uninstrumented)
/// and the build hash admission.
///
/// RIGHT_SEMI extra gate (fail-closed): admitted only with NO otherqual.
/// Its exactly-once emission rides `test_and_set_matched`; WHICH
/// (outer,inner) pair wins the flag is racy (and scan-order-dependent in
/// serial C too), so a pair-dependent otherqual deciding emission after
/// the flag would make the emitted SET attribution-dependent — refuse
/// rather than reason about planner scoping. (Projection/joinqual are
/// safe: a semijoin's output scope is the preserved — inner — side.)
pub fn shared_join_admissible(node: &HashJoinState<'_>, hs: &HashState<'_>) -> bool {
    (node.plan.join.jointype != JoinType::JOIN_RIGHT_SEMI || node.otherqual.is_none())
        && crate::lane_join_admissible(node)
        && ::nodehash::lane_build_hash_admissible(hs)
}

/// The build-side row's (hashvalue, minimal-tuple bytes), handed to `f` —
/// the M3.5 batch-routing seam: the caller decides Local push vs batch-file
/// route without this module knowing about spill. Hash exactly as the row
/// path's `hash_insert_slot` (via [`HashState::eval_build_hash`] — the same
/// reset + eval); the byte slice is valid only within `f` (it may borrow
/// the slot's stored image).
pub fn shared_build_hash_tuple<'mcx, R>(
    hs: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot_id: ExecSlotId,
    f: impl FnOnce(u32, &[u8]) -> PgResult<R>,
) -> PgResult<R> {
    let hashvalue = hs.eval_build_hash(estate, slot_id)?;
    let query_mcx = estate.es_query_cxt;
    let (slot, scratch_mcx) = estate.slot_and_per_tuple_mcx(slot_id, hs.ps_ExprContext);
    let fetched = exec_fetch_slot_minimal_tuple(slot, query_mcx, scratch_mcx)?;
    let (ptr, t_len): (*const u8, u32) = match &fetched {
        exectuples::FetchedMinimalTuple::Slot(m, _) => {
            // SAFETY: live stored image; header read.
            (m.as_ptr().cast_const().cast(), unsafe { m.as_ref().t_len })
        }
        exectuples::FetchedMinimalTuple::Copied(t) => (t.as_ptr(), t.t_len()),
    };
    // SAFETY: a minimal tuple image is t_len readable bytes.
    let bytes = unsafe { core::slice::from_raw_parts(ptr, t_len as usize) };
    f(hashvalue, bytes)
}

/// One build-side row into the worker's Local: hash, materialize the
/// minimal tuple, push. `Err(BudgetExceeded)` = the shared envelope crossed
/// (§6): the caller records the refusal and aborts the RG (R5 whole-attempt
/// rerun) — or, with the M3.5 spill arm on, demotes batch 0 (§5.2).
pub fn shared_build_accept<'mcx>(
    hs: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot_id: ExecSlotId,
    local: &mut JoinBuildLocal,
) -> PgResult<Result<(), BudgetExceeded>> {
    shared_build_hash_tuple(hs, estate, slot_id, |hashvalue, bytes| {
        Ok(local.push(hashvalue, bytes))
    })
}

/// Probe one outer row against the frozen table: `exec_hash_join`'s
/// HJ_NEED_NEW_OUTER hash eval + HJ_SCAN_BUCKET candidate loop (hashvalue
/// prefilter, hashclauses recheck, joinqual, per-jointype arm, otherqual,
/// projection) + the HJ_FILL_OUTER_TUPLE null-fill arm — inlined for one
/// outer row, no node-resident cursor (the runtime arm never pauses
/// mid-expansion; the downstream is a sink absorb, not a Volcano pull).
///
/// `emit` receives the join's projected result slot for each output row.
pub fn shared_probe_outer<'mcx>(
    node: &mut HashJoinState<'mcx>,
    hs: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
    table: &FrozenJoinTable,
    outer_slot: ExecSlotId,
    emit: &mut dyn FnMut(&mut HashJoinState<'mcx>, &mut EStateData<'mcx>, ExecSlotId) -> PgResult<()>,
) -> PgResult<()> {
    let hashvalue = shared_probe_outer_hash(node, estate, outer_slot)?;
    shared_probe_outer_hashed(node, hs, estate, table, outer_slot, hashvalue, emit)
}

/// The HJ_NEED_NEW_OUTER hash eval alone (keep-nulls semantics baked into
/// outer_hash_expr at init, per join type — C parity). The M3.5 batched
/// probe evaluates this once, routes non-resident batches to spill files,
/// and probes resident rows via [`shared_probe_outer_hashed`].
pub fn shared_probe_outer_hash<'mcx>(
    node: &mut HashJoinState<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer_slot: ExecSlotId,
) -> PgResult<u32> {
    let ecxt = node.ps_ExprContext;
    {
        let e = estate.ecxt_mut(ecxt);
        e.reset();
        e.ecxt_outertuple = Some(outer_slot);
    }
    Ok(::executils::exec_eval_expr_with_subplans_inner_slot(
        &mut node.outer_hash_expr,
        estate,
        ecxt,
        outer_slot,
    )?
    .value
    .as_u32())
}

/// The slot HashJoin batch reads store saved outer tuples into (C's
/// hj_OuterTupleSlot — `ExecHashJoinGetSavedTuple`'s target). The M3.5
/// leaf-probe drive materializes spilled outer records here.
pub fn shared_saved_outer_slot(node: &HashJoinState<'_>) -> ExecSlotId {
    node.hj_OuterTupleSlot
}

/// [`shared_probe_outer`] with a PRECOMPUTED hashvalue (M3.5 batch probes:
/// the spilled record carries the hash — C's batch-file discipline, never
/// re-evaluated). Performs the full per-row ExprContext setup itself.
pub fn shared_probe_outer_hashed<'mcx>(
    node: &mut HashJoinState<'mcx>,
    hs: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
    table: &FrozenJoinTable,
    outer_slot: ExecSlotId,
    hashvalue: u32,
    emit: &mut dyn FnMut(&mut HashJoinState<'mcx>, &mut EStateData<'mcx>, ExecSlotId) -> PgResult<()>,
) -> PgResult<()> {
    debug_assert!(shared_join_admissible(node, hs), "runtime probe on refused shape");
    let ecxt = node.ps_ExprContext;
    {
        let e = estate.ecxt_mut(ecxt);
        e.reset();
        e.ecxt_outertuple = Some(outer_slot);
    }
    let jointype = node.plan.join.jointype;
    let hslot = hs.hash_tuple_slot;
    let mcx = estate.es_query_cxt;
    estate.ecxt_mut(ecxt).ecxt_innertuple = Some(hslot);

    let mut matched_outer = false;
    for cand in table.chain(hashvalue) {
        // hashvalue prefilter before payload deref (scan_hash_bucket's
        // discipline; the tag word already filtered most misses).
        if cand.hashvalue() != hashvalue {
            continue;
        }
        let payload = cand.payload();
        // SAFETY: the frozen chunk image is a live minimal tuple readable
        // for t_len bytes for the table's lifetime (word-aligned storage);
        // the slot holds it only within this call's emit path.
        unsafe {
            let mtup = NonNull::new_unchecked(payload.as_ptr() as *mut MinimalTupleData);
            exectuples::exec_store_minimal_tuple_ptr(
                &mut estate.es_tupleTable[hslot.0 as usize],
                mcx,
                mtup,
            );
        }
        // hashclauses recheck (ExecScanHashBucket's qual; subplan-bearing
        // shapes are refused at admission).
        let key_eq = {
            let tbl = &mut estate.es_tupleTable[..];
            let [inner, outer] = tbl
                .get_disjoint_mut([hslot.0 as usize, outer_slot.0 as usize])
                .expect("distinct in-range hashjoin slot ids");
            let mut slots = EvalSlots { scan: None, inner: Some(inner), outer: Some(outer) };
            exec_qual(node.hashclauses.as_deref_mut(), &mut slots)?
        };
        if !key_eq {
            continue;
        }
        // RIGHT_SEMI: an already-emitted build tuple is skipped BEFORE the
        // joinqual (HJ_SCAN_BUCKET's has-match skip; racy-stale reads are
        // fine — the authoritative claim is the test_and_set below).
        if jointype == JoinType::JOIN_RIGHT_SEMI && cand.matched() {
            continue;
        }
        // joinqual (HJ_SCAN_BUCKET's arm, verbatim order).
        let matched = eval_probe_qual(node.joinqual.as_deref_mut(), ecxt, hslot, estate)?;
        if !matched {
            estate.instr_count_filtered1(node.js_instr);
            continue;
        }
        matched_outer = true;
        // Match flag (C sets it for every joinqual match): the fill phase
        // (RIGHT/FULL/RIGHT_ANTI) reads it after the probe barrier;
        // RIGHT_SEMI claims it exactly-once here.
        match jointype {
            JoinType::JOIN_RIGHT_SEMI => {
                if !cand.test_and_set_matched() {
                    continue; // another probe task won this build tuple
                }
                // otherqual is None by admission; project + emit once.
                let out = project_result(node, hslot, estate)?;
                emit(node, estate, out)?;
                continue; // keep scanning: other build tuples in this chain
            }
            JoinType::JOIN_RIGHT | JoinType::JOIN_FULL | JoinType::JOIN_RIGHT_ANTI => {
                cand.set_matched();
            }
            _ => {}
        }
        match jointype {
            JoinType::JOIN_ANTI => break, // matched anti-outer emits nothing
            JoinType::JOIN_RIGHT_ANTI => continue, // mark-only probe
            JoinType::JOIN_SEMI => {
                let pass = eval_probe_qual(node.otherqual.as_deref_mut(), ecxt, hslot, estate)?;
                if pass {
                    let out = project_result(node, hslot, estate)?;
                    emit(node, estate, out)?;
                } else {
                    estate.instr_count_filtered2(node.js_instr);
                }
                break; // js_single_match: first joinqual match ends the walk
            }
            JoinType::JOIN_RIGHT_SEMI => unreachable!("handled above"),
            _ => {
                let pass = eval_probe_qual(node.otherqual.as_deref_mut(), ecxt, hslot, estate)?;
                if pass {
                    let out = project_result(node, hslot, estate)?;
                    emit(node, estate, out)?;
                } else {
                    estate.instr_count_filtered2(node.js_instr);
                }
                if node.js_single_match {
                    break; // inner_unique INNER/LEFT
                }
            }
        }
    }
    // HJ_FILL_OUTER_TUPLE: LEFT/FULL/ANTI null-fill unmatched outers
    // (hj_fill_outer covers all three).
    if !matched_outer && node.hj_fill_outer {
        let null_inner = node.hj_NullInnerTupleSlot.expect("null inner slot");
        estate.ecxt_mut(ecxt).ecxt_innertuple = Some(null_inner);
        let pass = eval_probe_qual(node.otherqual.as_deref_mut(), ecxt, null_inner, estate)?;
        if pass {
            let out = project_result(node, null_inner, estate)?;
            emit(node, estate, out)?;
        } else {
            estate.instr_count_filtered2(node.js_instr);
        }
    }
    Ok(())
}

/// The right-fill walk (HJ_FILL_INNER_TUPLES over one frozen partition):
/// never-matched build tuples, null-extended on the outer side, through
/// otherqual + projection into `emit`. Runs on the FILL task set
/// (deps=[probe] — the task-set completion barrier is the flag-visibility
/// edge); partition-exclusive, so no two tasks touch the same tuple.
/// RIGHT/FULL/RIGHT_ANTI only (RIGHT_SEMI never fills).
pub fn shared_fill_partition<'mcx>(
    node: &mut HashJoinState<'mcx>,
    hs: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
    table: &FrozenJoinTable,
    part: u64,
    emit: &mut dyn FnMut(&mut HashJoinState<'mcx>, &mut EStateData<'mcx>, ExecSlotId) -> PgResult<()>,
) -> PgResult<()> {
    debug_assert!(node.hj_fill_inner, "fill walk on a non-fill join type");
    let ecxt = node.ps_ExprContext;
    let hslot = hs.hash_tuple_slot;
    let mcx = estate.es_query_cxt;
    let null_outer = node.hj_NullOuterTupleSlot.expect("null outer slot");
    for tuple in table.unmatched_in_partition(part) {
        let payload = tuple.payload();
        // SAFETY: frozen chunk image, live for the table's lifetime,
        // word-aligned (the probe-path argument, verbatim).
        unsafe {
            let mtup = NonNull::new_unchecked(payload.as_ptr() as *mut MinimalTupleData);
            exectuples::exec_store_minimal_tuple_ptr(
                &mut estate.es_tupleTable[hslot.0 as usize],
                mcx,
                mtup,
            );
        }
        {
            let e = estate.ecxt_mut(ecxt);
            e.reset();
            e.ecxt_outertuple = Some(null_outer);
            e.ecxt_innertuple = Some(hslot);
        }
        let pass = eval_probe_qual(node.otherqual.as_deref_mut(), ecxt, hslot, estate)?;
        if pass {
            let out = project_result(node, hslot, estate)?;
            emit(node, estate, out)?;
        } else {
            estate.instr_count_filtered2(node.js_instr);
        }
    }
    Ok(())
}
