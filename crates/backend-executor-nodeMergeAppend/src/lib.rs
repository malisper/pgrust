//! Port of `src/backend/executor/nodeMergeAppend.c` — routines to handle
//! MergeAppend nodes.
//!
//! INTERFACE ROUTINES
//! - [`ExecInitMergeAppend`]   - initialize the MergeAppend node.
//! - [`ExecMergeAppend`]       - retrieve the next tuple from the node.
//! - [`ExecEndMergeAppend`]    - shut down the MergeAppend node.
//! - [`ExecReScanMergeAppend`] - rescan the MergeAppend node.
//!
//! A MergeAppend node contains a list of one or more subplans, each expected to
//! deliver tuples sorted on a common sort key. The node merges these
//! already-sorted streams by keeping the not-yet-exhausted head tuple of every
//! subplan in a binary heap keyed on the sort columns and repeatedly emitting
//! the heap minimum.
//!
//! The four interface routines, the file-local `heap_compare_slots` comparator,
//! and the node-local binary heap (`lib/binaryheap.c`, specialized to slot
//! indices: a leaf algorithm with no dependency cycle, so it is implemented
//! here rather than seamed) are this crate's owned logic. Operations below the
//! executor-node layer go through the owners' seam crates:
//!
//! - interrupt servicing (`CHECK_FOR_INTERRUPTS`) → tcop/postgres;
//! - child dispatch / init / teardown (`ExecProcNode` / `ExecInitNode` /
//!   `ExecEndNode`) → execProcnode; rescan / changed-param signalling
//!   (`ExecReScan` / `UpdateChangedParamSet`) → execAmi;
//! - result-slot setup and clearing (`ExecInitResultTupleSlotTL` /
//!   `ExecClearTuple`) and attribute access (`slot_getattr`) → execTuples;
//! - run-time partition pruning (`ExecInitPartitionExecPruning` /
//!   `ExecFindMatchingSubPlans`) → execPartition;
//! - the sort-key setup and comparison (`PrepareSortSupportFromOrderingOp` /
//!   `ApplySortComparator`) → sortsupport;
//! - the bitmapset operations (`bms_add_range` / `bms_num_members` /
//!   `bms_next_member` / `bms_overlap`) → nodes/bitmapset.
//!
//! `ExecGetCommonSlotOps` is execUtils and is reached by a direct dependency
//! (no cycle).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use backend_executor_execAmi_seams as execAmi;
use backend_executor_execPartition_seams as execPartition;
use backend_executor_execProcnode_seams as execProcnode;
use backend_executor_execTuples_seams as execTuples;
use backend_nodes_core_seams as bitmapset;
use backend_tcop_postgres_seams as tcop_postgres;
use backend_utils_sort_sortsupport_seams as sortsupport;

use mcx::{alloc_in, Mcx, PgBox};
use types_core::primitive::AttrNumber;
// Migration target: the canonical enum `Datum<'mcx>` from the keystone tuple
// crate. The binary heap's `bh_nodes` storage (owned by `types-nodes`) is still
// the bare-word shim `types_datum::Datum` and carries a plain `SlotNumber`
// (`int32`) word — that dep-owned storage word is the audited ABI/storage edge,
// so the canonical enum is converted to/from the stored slot-index at the
// `bh_nodes` push/read boundary inside the heap helpers below.
use types_datum::Datum as StoredSlotWord;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_nodes::executor::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};
use types_nodes::nodemergeappend::{BinaryHeap, MergeAppend, MergeAppendStateData};
use types_nodes::{
    Bitmapset, EStateData, PlanStateData, PlanStateNode, SlotId, TupleSlotKind,
};
use types_sortsupport::SortSupportData;

/// `SlotNumber` (nodeMergeAppend.c) — `typedef int32 SlotNumber;`. A slot /
/// subplan index stored in the heap. Provides no formal type-safety; it makes
/// the code self-documenting.
type SlotNumber = i32;

/// Install this crate's implementations into its seam slots. nodeMergeAppend
/// has no `<unit>-seams` crate: its functions are reached through the executor
/// dispatch (execProcnode / execAmi), which depend on this crate directly
/// without a cycle.
pub fn init_seams() {}

// ===========================================================================
// Dispatch callback.
// ===========================================================================

/// The `PlanState.ExecProcNode` callback installed by [`ExecInitMergeAppend`]:
/// `castNode(MergeAppendState, pstate)` then run [`ExecMergeAppend`].
fn exec_merge_append_node<'mcx>(
    pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        PlanStateNode::MergeAppend(node) => node,
        other => panic!("castNode(MergeAppendState, pstate) failed: {other:?}"),
    };
    ExecMergeAppend(node, estate)
}

// ===========================================================================
// Interface routines (1:1 with nodeMergeAppend.c).
// ===========================================================================

/// `ExecInitMergeAppend(node, estate, eflags)` — begin all of the subscans of
/// the MergeAppend node.
pub fn ExecInitMergeAppend<'mcx>(
    plan_node: &'mcx types_nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, MergeAppendStateData<'mcx>>> {
    // check for unsupported flags
    //   Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);

    let mcx = estate.es_query_cxt;

    let node: &'mcx MergeAppend<'mcx> = match plan_node {
        types_nodes::nodes::Node::MergeAppend(m) => m,
        other => panic!("castNode(MergeAppend, node) failed: {other:?}"),
    };

    // create new MergeAppendState for our node
    //   MergeAppendState *mergestate = makeNode(MergeAppendState);
    //   mergestate->ps.plan = (Plan *) node;
    //   mergestate->ps.state = estate;
    //   mergestate->ps.ExecProcNode = ExecMergeAppend;
    let mut ps = PlanStateData::default();
    ps.plan = Some(plan_node);
    ps.ExecProcNode = Some(exec_merge_append_node);

    let validsubplans: Option<PgBox<'mcx, Bitmapset<'mcx>>>;
    let ms_prune_state: Option<PgBox<'mcx, types_nodes::PartitionPruneState<'mcx>>>;
    let mut ms_valid_subplans: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None;
    let nplans: i32;

    // If run-time partition pruning is enabled, then set that up now.
    if node.part_prune_index >= 0 {
        // Set up pruning data structure. This also initializes the set of
        // subplans to initialize (validsubplans) by taking into account the
        // result of performing initial pruning if any.
        //   prunestate = ExecInitPartitionExecPruning(&mergestate->ps,
        //       list_length(node->mergeplans), node->part_prune_index,
        //       node->apprelids, &validsubplans);
        let (prunestate, vsubplans) = execPartition::exec_init_partition_exec_pruning::call(
            mcx,
            &mut ps,
            estate,
            list_length(&node.mergeplans),
            node.part_prune_index,
            node.apprelids.as_deref(),
        )?;
        validsubplans = vsubplans;
        //   mergestate->ms_prune_state = prunestate;
        //   nplans = bms_num_members(validsubplans);
        nplans = bitmapset::bms_num_members::call(validsubplans.as_deref());

        // When no run-time pruning is required and there's at least one subplan,
        // we can fill ms_valid_subplans immediately, preventing later calls to
        // ExecFindMatchingSubPlans.
        //   if (!prunestate->do_exec_prune && nplans > 0)
        //       mergestate->ms_valid_subplans = bms_add_range(NULL, 0, nplans - 1);
        if !prunestate.do_exec_prune && nplans > 0 {
            ms_valid_subplans = bitmapset::bms_add_range::call(mcx, None, 0, nplans - 1)?;
        }
        ms_prune_state = Some(prunestate);
    } else {
        //   nplans = list_length(node->mergeplans);
        nplans = list_length(&node.mergeplans);

        // When run-time partition pruning is not enabled we can just mark all
        // subplans as valid; they must also all be initialized.
        //   Assert(nplans > 0);
        //   mergestate->ms_valid_subplans = validsubplans = bms_add_range(NULL, 0, nplans - 1);
        //   mergestate->ms_prune_state = NULL;
        debug_assert!(nplans > 0, "MergeAppend has no subplans");
        validsubplans = bitmapset::bms_add_range::call(mcx, None, 0, nplans - 1)?;
        ms_valid_subplans = clone_bitmapset(mcx, validsubplans.as_deref())?;
        ms_prune_state = None;
    }

    // mergeplanstates = (PlanState **) palloc(nplans * sizeof(PlanState *));
    // mergestate->ms_slots = (TupleTableSlot **) palloc0(sizeof(...) * nplans);
    let nplans_usize =
        usize::try_from(nplans).map_err(|_| elog_error("MergeAppend has a negative subplan count"))?;
    let mut mergeplanstates: mcx::PgVec<'mcx, Option<PgBox<'mcx, PlanStateNode<'mcx>>>> =
        mcx::vec_with_capacity_in(mcx, nplans_usize)?;
    let mut ms_slots: mcx::PgVec<'mcx, Option<SlotId>> = mcx::vec_with_capacity_in(mcx, nplans_usize)?;
    for _ in 0..nplans_usize {
        ms_slots.push(None);
    }

    // ms_heap = binaryheap_allocate(nplans, heap_compare_slots, mergestate);
    let ms_heap = BinaryHeap::allocate(mcx, nplans_usize)?;

    // call ExecInitNode on each of the valid plans to be executed and save the
    // results into the mergeplanstates array.
    //   j = 0; i = -1;
    //   while ((i = bms_next_member(validsubplans, i)) >= 0) {
    //       Plan *initNode = (Plan *) list_nth(node->mergeplans, i);
    //       mergeplanstates[j++] = ExecInitNode(initNode, estate, eflags);
    //   }
    let mut i: i32 = -1;
    loop {
        i = bitmapset::bms_next_member::call(validsubplans.as_deref(), i);
        if i < 0 {
            break;
        }
        let child_index =
            usize::try_from(i).map_err(|_| elog_error("MergeAppend valid-subplan index is negative"))?;
        let init_node = node
            .mergeplans
            .get(child_index)
            .ok_or_else(|| elog_error("MergeAppend mergeplans index out of range"))?;
        let child = execProcnode::exec_init_node::call(mcx, Some(init_node), estate, eflags)?;
        mergeplanstates.push(child);
    }
    let j = i32::try_from(mergeplanstates.len())
        .map_err(|_| elog_error("MergeAppend has too many initialized subplans"))?;

    // Initialize MergeAppend's result tuple type and slot. If the child plans
    // all produce the same fixed slot type, we can use that slot type; otherwise
    // make a virtual slot. (The result slot itself is used only to return a null
    // tuple at end of execution; real tuples are returned in the children's own
    // result slots.)
    //   mergeops = ExecGetCommonSlotOps(mergeplanstates, j);
    let mergeops = {
        let heads: Vec<&PlanStateData<'mcx>> = mergeplanstates
            .iter()
            .take(j as usize)
            .map(|c| {
                c.as_deref()
                    .map(|s| s.ps_head())
                    .expect("ExecGetCommonSlotOps: initialized subplan is missing")
            })
            .collect();
        backend_executor_execUtils::ExecGetCommonSlotOps(&heads, estate)
    };
    match mergeops {
        Some(ops) => {
            // ExecInitResultTupleSlotTL(&mergestate->ps, mergeops);
            execTuples::exec_init_result_tuple_slot_tl::call(&mut ps, estate, ops)?;
        }
        None => {
            // ExecInitResultTupleSlotTL(&mergestate->ps, &TTSOpsVirtual);
            execTuples::exec_init_result_tuple_slot_tl::call(&mut ps, estate, TupleSlotKind::Virtual)?;
            // show that the output slot type is not fixed
            ps.resultopsset = true;
            ps.resultopsfixed = false;
        }
    }

    // Miscellaneous initialization
    //   mergestate->ps.ps_ProjInfo = NULL;
    ps.ps_ProjInfo = None;

    // initialize sort-key information
    //   mergestate->ms_nkeys = node->numCols;
    //   mergestate->ms_sortkeys = palloc0(sizeof(SortSupportData) * node->numCols);
    let num_cols = node.numCols;
    let num_cols_usize =
        usize::try_from(num_cols).map_err(|_| elog_error("MergeAppend has a negative numCols"))?;
    let mut ms_sortkeys: mcx::PgVec<'mcx, SortSupportData<'mcx>> =
        mcx::vec_with_capacity_in(mcx, num_cols_usize)?;

    //   for (i = 0; i < node->numCols; i++) { ... }
    for k in 0..num_cols_usize {
        // SortSupport sortKey = mergestate->ms_sortkeys + i;
        // sortKey->ssup_cxt = CurrentMemoryContext;
        let mut sort_key = SortSupportData::new(mcx);
        sort_key.ssup_collation = *node
            .collations
            .get(k)
            .ok_or_else(|| elog_error("MergeAppend collations array too short"))?;
        sort_key.ssup_nulls_first = *node
            .nullsFirst
            .get(k)
            .ok_or_else(|| elog_error("MergeAppend nullsFirst array too short"))?;
        sort_key.ssup_attno = *node
            .sortColIdx
            .get(k)
            .ok_or_else(|| elog_error("MergeAppend sortColIdx array too short"))?;
        // It isn't feasible to perform abbreviated key conversion, since tuples
        // are pulled into mergestate's binary heap as needed; opt out of that
        // additional optimization entirely.
        //   sortKey->abbreviate = false;
        sort_key.abbreviate = false;

        // PrepareSortSupportFromOrderingOp(node->sortOperators[i], sortKey);
        let ordering_op = *node
            .sortOperators
            .get(k)
            .ok_or_else(|| elog_error("MergeAppend sortOperators array too short"))?;
        sortsupport::prepare_sort_support_from_ordering_op::call(ordering_op, &mut sort_key)?;
        ms_sortkeys.push(sort_key);
    }

    // initialize to show we have not run the subplans yet
    //   mergestate->ms_initialized = false;
    let _ = validsubplans;
    Ok(alloc_in(
        mcx,
        MergeAppendStateData {
            ps,
            mergeplans: mergeplanstates,
            ms_nplans: nplans,
            ms_nkeys: num_cols,
            ms_sortkeys,
            ms_slots,
            ms_heap: Some(alloc_in(mcx, ms_heap)?),
            ms_initialized: false,
            ms_prune_state,
            ms_valid_subplans,
        },
    )?)
}

/// `ExecMergeAppend(pstate)` — the `PlanState.ExecProcNode` callback: handle
/// iteration over multiple subplans, merging their already-sorted streams.
///
/// 1:1 with `static TupleTableSlot *ExecMergeAppend(PlanState *pstate)`. The
/// result slot and the children's slots are ids into `estate.es_tupleTable`, so
/// the `EState` is threaded explicitly. On exhaustion it returns the node's
/// CLEARED result-slot id (the C `return ExecClearTuple(ps_ResultTupleSlot)` —
/// a non-NULL empty slot the caller's `TupIsNull` catches).
pub fn ExecMergeAppend<'mcx>(
    node: &mut MergeAppendStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    tcop_postgres::check_for_interrupts::call()?;

    let mcx = estate.es_query_cxt;

    if !node.ms_initialized {
        // Nothing to do if all subplans were pruned.
        //   if (node->ms_nplans == 0) return ExecClearTuple(node->ps.ps_ResultTupleSlot);
        if node.ms_nplans == 0 {
            clear_result_slot(node, estate)?;
            return Ok(node.ps.ps_ResultTupleSlot);
        }

        // If we've yet to determine the valid subplans then do so now. If
        // run-time pruning is disabled then the valid subplans will always be
        // set to all subplans.
        //   if (node->ms_valid_subplans == NULL)
        //       node->ms_valid_subplans =
        //           ExecFindMatchingSubPlans(node->ms_prune_state, false, NULL);
        if node.ms_valid_subplans.is_none() {
            let prune_state = node
                .ms_prune_state
                .as_deref_mut()
                .ok_or_else(|| elog_error("MergeAppend has no prune state to match subplans"))?;
            node.ms_valid_subplans =
                execPartition::exec_find_matching_subplans::call(mcx, prune_state, estate, false)?;
        }

        // First time through: pull the first tuple from each valid subplan, and
        // set up the heap.
        //   i = -1;
        //   while ((i = bms_next_member(node->ms_valid_subplans, i)) >= 0) {
        //       node->ms_slots[i] = ExecProcNode(node->mergeplans[i]);
        //       if (!TupIsNull(node->ms_slots[i]))
        //           binaryheap_add_unordered(node->ms_heap, Int32GetDatum(i));
        //   }
        let mut i: i32 = -1;
        loop {
            i = bitmapset::bms_next_member::call(node.ms_valid_subplans.as_deref(), i);
            if i < 0 {
                break;
            }
            let idx =
                usize::try_from(i).map_err(|_| elog_error("MergeAppend valid-subplan index is negative"))?;
            let child = node
                .mergeplans
                .get_mut(idx)
                .and_then(|slot| slot.as_deref_mut())
                .ok_or_else(|| elog_error("MergeAppend child plan state is missing"))?;
            let slot = execProcnode::exec_proc_node::call(child, estate)?;
            let not_null = !tup_is_null(slot, estate);
            *node
                .ms_slots
                .get_mut(idx)
                .ok_or_else(|| elog_error("MergeAppend ms_slots index out of range"))? = slot;
            if not_null {
                let heap = heap_mut(node)?;
                binaryheap_add_unordered(heap, Datum::from_i32(i))?;
            }
        }
        // binaryheap_build(node->ms_heap);
        binaryheap_build_node(node, estate)?;
        // node->ms_initialized = true;
        node.ms_initialized = true;
    } else {
        // Otherwise, pull the next tuple from whichever subplan we returned from
        // last time, and reinsert the subplan index into the heap, because it
        // might now compare differently against the existing elements of the
        // heap.
        //   i = DatumGetInt32(binaryheap_first(node->ms_heap));
        let i = binaryheap_first(heap_ref(node)?)?.as_i32();
        let idx = usize::try_from(i).map_err(|_| elog_error("MergeAppend heap slot index is negative"))?;
        // node->ms_slots[i] = ExecProcNode(node->mergeplans[i]);
        let child = node
            .mergeplans
            .get_mut(idx)
            .and_then(|slot| slot.as_deref_mut())
            .ok_or_else(|| elog_error("MergeAppend child plan state is missing"))?;
        let slot = execProcnode::exec_proc_node::call(child, estate)?;
        let not_null = !tup_is_null(slot, estate);
        *node
            .ms_slots
            .get_mut(idx)
            .ok_or_else(|| elog_error("MergeAppend ms_slots index out of range"))? = slot;
        if not_null {
            // binaryheap_replace_first(node->ms_heap, Int32GetDatum(i));
            binaryheap_replace_first_node(node, Datum::from_i32(i), estate)?;
        } else {
            // (void) binaryheap_remove_first(node->ms_heap);
            binaryheap_remove_first_node(node, estate)?;
        }
    }

    if binaryheap_empty(heap_ref(node)?) {
        // All the subplans are exhausted, and so is the heap.
        //   result = ExecClearTuple(node->ps.ps_ResultTupleSlot);
        clear_result_slot(node, estate)?;
        Ok(node.ps.ps_ResultTupleSlot)
    } else {
        // i = DatumGetInt32(binaryheap_first(node->ms_heap));
        // result = node->ms_slots[i];
        let i = binaryheap_first(heap_ref(node)?)?.as_i32();
        let idx = usize::try_from(i).map_err(|_| elog_error("MergeAppend heap slot index is negative"))?;
        let slot = *node
            .ms_slots
            .get(idx)
            .ok_or_else(|| elog_error("MergeAppend ms_slots index out of range"))?;
        Ok(slot)
    }
}

/// `heap_compare_slots(a, b, arg)` — compare the tuples in the two given slots,
/// for the binary heap. Returns the comparison inverted
/// (`INVERT_COMPARE_RESULT`) because the underlying heap is a max-heap but we
/// want the smallest tuple at the top.
///
/// 1:1 with `static int32 heap_compare_slots(Datum a, Datum b, void *arg)`.
/// `slots`/`sortkeys` are borrowed from the node (the C `arg` is the
/// `MergeAppendState *`); the heap operations split this borrow off `ms_heap`.
/// The children's slots are ids into `estate.es_tupleTable` (deform may write).
fn heap_compare_slots<'mcx>(
    slots: &[Option<SlotId>],
    sortkeys: &[SortSupportData<'mcx>],
    a: Datum<'_>,
    b: Datum<'_>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<i32> {
    // SlotNumber slot1 = DatumGetInt32(a); SlotNumber slot2 = DatumGetInt32(b);
    let slot1: SlotNumber = a.as_i32();
    let slot2: SlotNumber = b.as_i32();
    let i1 = usize::try_from(slot1).map_err(|_| elog_error("MergeAppend heap slot index is negative"))?;
    let i2 = usize::try_from(slot2).map_err(|_| elog_error("MergeAppend heap slot index is negative"))?;
    let id1 = slots
        .get(i1)
        .copied()
        .flatten()
        .ok_or_else(|| elog_error("MergeAppend compare slot is empty"))?;
    let id2 = slots
        .get(i2)
        .copied()
        .flatten()
        .ok_or_else(|| elog_error("MergeAppend compare slot is empty"))?;

    //   for (nkey = 0; nkey < node->ms_nkeys; nkey++) { ... }
    for sort_key in sortkeys {
        // AttrNumber attno = sortKey->ssup_attno;
        let attno: AttrNumber = sort_key.ssup_attno;

        // datum1 = slot_getattr(s1, attno, &isNull1);
        let (datum1, is_null1) = execTuples::slot_getattr::call(estate.slot_mut(id1), attno)?;
        // datum2 = slot_getattr(s2, attno, &isNull2);
        let (datum2, is_null2) = execTuples::slot_getattr::call(estate.slot_mut(id2), attno)?;

        // compare = ApplySortComparator(datum1, isNull1, datum2, isNull2, sortKey);
        let mut compare = ApplySortComparator(datum1, is_null1, datum2, is_null2, sort_key)?;
        if compare != 0 {
            // INVERT_COMPARE_RESULT(compare); return compare;
            compare = INVERT_COMPARE_RESULT(compare);
            return Ok(compare);
        }
    }
    Ok(0)
}

/// `ExecEndMergeAppend(node)` — shut down the subscans of the MergeAppend node.
///
/// 1:1 with `void ExecEndMergeAppend(MergeAppendState *node)`.
pub fn ExecEndMergeAppend<'mcx>(
    node: &mut MergeAppendStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // get information from the node
    //   mergeplans = node->mergeplans;
    //   nplans = node->ms_nplans;
    let nplans =
        usize::try_from(node.ms_nplans).map_err(|_| elog_error("MergeAppend has a negative subplan count"))?;

    // shut down each of the subscans
    //   for (i = 0; i < nplans; i++) ExecEndNode(mergeplans[i]);
    for i in 0..nplans {
        if let Some(subnode) = node.mergeplans.get_mut(i).and_then(|s| s.as_deref_mut()) {
            execProcnode::exec_end_node::call(subnode, estate)?;
        }
    }
    Ok(())
}

/// `ExecReScanMergeAppend(node)` — rescan the MergeAppend node.
///
/// 1:1 with `void ExecReScanMergeAppend(MergeAppendState *node)`.
pub fn ExecReScanMergeAppend<'mcx>(
    node: &mut MergeAppendStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;

    // If any PARAM_EXEC Params used in pruning expressions have changed, then
    // we'd better unset the valid subplans so that they are reselected for the
    // new parameter values.
    //   if (node->ms_prune_state &&
    //       bms_overlap(node->ps.chgParam, node->ms_prune_state->execparamids)) {
    //       bms_free(node->ms_valid_subplans);
    //       node->ms_valid_subplans = NULL;
    //   }
    let overlap = match node.ms_prune_state.as_deref() {
        Some(prune_state) => bitmapset::bms_overlap::call(
            node.ps.chgParam.as_deref(),
            prune_state.execparamids.as_deref(),
        ),
        None => false,
    };
    if overlap {
        // bms_free + NULL: in the owned tree, dropping the box is the free.
        node.ms_valid_subplans = None;
    }

    //   for (i = 0; i < node->ms_nplans; i++) { ... }
    let nplans =
        usize::try_from(node.ms_nplans).map_err(|_| elog_error("MergeAppend has a negative subplan count"))?;
    // node->ps.chgParam is read-only throughout the loop; clone it once so the
    // child borrow and the parent read don't alias.
    let parent_chg = clone_bitmapset(mcx, node.ps.chgParam.as_deref())?;
    for i in 0..nplans {
        // PlanState *subnode = node->mergeplans[i];
        let subnode = node
            .mergeplans
            .get_mut(i)
            .and_then(|s| s.as_deref_mut())
            .ok_or_else(|| elog_error("MergeAppend child plan state is missing"))?;

        // ExecReScan doesn't know about my subplans, so I have to do
        // changed-parameter signaling myself.
        //   if (node->ps.chgParam != NULL)
        //       UpdateChangedParamSet(subnode, node->ps.chgParam);
        if parent_chg.is_some() {
            backend_executor_execUtils::UpdateChangedParamSet(
                subnode.ps_head_mut(),
                parent_chg.as_deref(),
                mcx,
            )?;
        }

        // If chgParam of subnode is not null then plan will be re-scanned by
        // first ExecProcNode.
        //   if (subnode->chgParam == NULL) ExecReScan(subnode);
        if subnode.ps_head().chgParam.is_none() {
            execAmi::exec_re_scan::call(subnode, estate)?;
        }
    }

    // binaryheap_reset(node->ms_heap);
    binaryheap_reset(heap_mut(node)?);
    // node->ms_initialized = false;
    node.ms_initialized = false;
    Ok(())
}

// ===========================================================================
// In-crate binary-heap library (lib/binaryheap.c), specialized to the node's
// slot-index comparator. A leaf algorithm: no dependency cycle, so it is
// implemented here rather than seamed.
// ===========================================================================

/// `binaryheap_reset(heap)` — reset the heap to an empty state, keeping the
/// allocated capacity.
fn binaryheap_reset(heap: &mut BinaryHeap<'_>) {
    heap.bh_size = 0;
    heap.bh_has_heap_property = true;
    heap.bh_nodes.clear();
}

/// `binaryheap_empty(h)` — true if the heap has no entries.
fn binaryheap_empty(heap: &BinaryHeap<'_>) -> bool {
    heap.bh_size == 0
}

/// `binaryheap_add_unordered(heap, d)` — add `d` to the end without restoring
/// the heap property (paired with [`binaryheap_build_node`]). The capacity was
/// reserved in `binaryheap_allocate`; an overflow is the C
/// `elog(ERROR, "out of binary heap slots")`.
fn binaryheap_add_unordered(heap: &mut BinaryHeap<'_>, d: Datum<'_>) -> PgResult<()> {
    if heap.bh_size >= heap.bh_space {
        return Err(elog_error("out of binary heap slots"));
    }
    heap.bh_has_heap_property = false;
    heap.bh_nodes.push(slot_word(&d));
    heap.bh_size += 1;
    Ok(())
}

/// `binaryheap_first(heap)` — peek at the heap's top (root) entry. The caller
/// must ensure the heap is non-empty.
fn binaryheap_first<'mcx>(heap: &BinaryHeap<'_>) -> PgResult<Datum<'mcx>> {
    heap.bh_nodes
        .first()
        .copied()
        .map(slot_datum)
        .ok_or_else(|| elog_error("binaryheap_first on empty heap"))
}

/// `binaryheap_remove_first(heap)` over the node — remove and return the heap's
/// top entry, rebalancing with [`sift_down`]. Splits the node borrow for the
/// comparator. The caller must ensure the heap is non-empty.
fn binaryheap_remove_first_node<'mcx>(
    node: &mut MergeAppendStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mut heap = node
        .ms_heap
        .take()
        .ok_or_else(|| elog_error("MergeAppend has no binary heap"))?;
    let result = (|| {
        if binaryheap_empty(&heap) {
            return Err(elog_error("binaryheap_remove_first on empty heap"));
        }
        // extract the root node, which will be the result
        let result = heap.bh_nodes[0];

        // easy if heap contains one element
        if heap.bh_size == 1 {
            heap.bh_size -= 1;
            heap.bh_nodes.pop();
            return Ok(slot_datum(result));
        }

        // Remove the last node, placing it in the vacated root entry, and sift
        // the new root node down to its correct position.
        heap.bh_size -= 1;
        let last = heap
            .bh_nodes
            .pop()
            .ok_or_else(|| elog_error("binaryheap underflow"))?;
        heap.bh_nodes[0] = last;
        sift_down(&mut heap, 0, &node.ms_slots, &node.ms_sortkeys, estate)?;
        Ok(slot_datum(result))
    })();
    node.ms_heap = Some(heap);
    result
}

/// `binaryheap_build(heap)` over the node — assemble a valid heap in O(n) from
/// the nodes added by [`binaryheap_add_unordered`], using [`heap_compare_slots`].
/// Splits the node borrow so the comparator can read the slots/sortkeys while
/// the heap is mutated.
fn binaryheap_build_node<'mcx>(
    node: &mut MergeAppendStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mut heap = node
        .ms_heap
        .take()
        .ok_or_else(|| elog_error("MergeAppend has no binary heap"))?;
    let result = (|| {
        // for (i = parent_offset(heap->bh_size - 1); i >= 0; i--) sift_down(heap, i);
        if heap.bh_size >= 1 {
            let start = parent_offset(heap.bh_size - 1);
            let mut i = start;
            while i >= 0 {
                sift_down(&mut heap, i, &node.ms_slots, &node.ms_sortkeys, estate)?;
                i -= 1;
            }
        }
        heap.bh_has_heap_property = true;
        Ok(())
    })();
    node.ms_heap = Some(heap);
    result
}

/// `binaryheap_replace_first(heap, d)` over the node — replace the topmost
/// element and re-heapify with [`sift_down`]. Splits the node borrow for the
/// comparator.
fn binaryheap_replace_first_node<'mcx>(
    node: &mut MergeAppendStateData<'mcx>,
    d: Datum<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mut heap = node
        .ms_heap
        .take()
        .ok_or_else(|| elog_error("MergeAppend has no binary heap"))?;
    let result = (|| {
        if binaryheap_empty(&heap) {
            return Err(elog_error("binaryheap_replace_first on empty heap"));
        }
        heap.bh_nodes[0] = slot_word(&d);
        if heap.bh_size > 1 {
            sift_down(&mut heap, 0, &node.ms_slots, &node.ms_sortkeys, estate)?;
        }
        Ok(())
    })();
    node.ms_heap = Some(heap);
    result
}

/// Storage-edge converter: the binary heap's `bh_nodes` is owned by
/// `types-nodes` and still carries the bare-word shim (`StoredSlotWord`), into
/// which `binaryheap.c` packs an `int32` `SlotNumber` via `Int32GetDatum`. Pack
/// the canonical [`Datum`] (always a `ByVal` slot index here) back into that
/// stored word. This is the audited ABI/storage edge where a plain word is the
/// faithful representation (`Int32GetDatum`).
fn slot_word(d: &Datum<'_>) -> StoredSlotWord {
    StoredSlotWord::from_i32(d.as_i32())
}

/// Storage-edge converter: lift a stored slot-index word back into the
/// canonical [`Datum`] enum (`DatumGetInt32` -> `Datum::from_i32`).
fn slot_datum<'mcx>(w: StoredSlotWord) -> Datum<'mcx> {
    Datum::from_i32(w.as_i32())
}

/// Offset of the parent of the node at index `i`.
fn parent_offset(i: i32) -> i32 {
    (i - 1) / 2
}

/// Offset of the left child of the node at index `i`.
fn left_offset(i: i32) -> i32 {
    2 * i + 1
}

/// Offset of the right child of the node at index `i`.
fn right_offset(i: i32) -> i32 {
    2 * i + 2
}

/// `sift_down(heap, node_off)` — sift a node down from its current position to
/// satisfy the heap property, using [`heap_compare_slots`] over the node's
/// slots/sortkeys. 1:1 with `lib/binaryheap.c`'s `sift_down`.
fn sift_down<'mcx>(
    heap: &mut BinaryHeap<'mcx>,
    node_off: i32,
    slots: &[Option<SlotId>],
    sortkeys: &[SortSupportData<'mcx>],
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mut node_off = node_off;
    // The heap stores the bare slot-index word (dep-owned `bh_nodes`); the
    // comparator consumes the canonical `Datum` enum, so the word is lifted at
    // each read site.
    let node_val = heap.bh_nodes[node_off as usize];

    loop {
        let left_off = left_offset(node_off);
        let right_off = right_offset(node_off);
        let mut swap_off = left_off;

        // Is the right child larger than the left child?
        if right_off < heap.bh_size {
            let left_val = heap.bh_nodes[left_off as usize];
            let right_val = heap.bh_nodes[right_off as usize];
            if heap_compare_slots(slots, sortkeys, slot_datum(left_val), slot_datum(right_val), estate)?
                < 0
            {
                swap_off = right_off;
            }
        }

        // If no children or parent is >= the larger child, heap condition is
        // satisfied, and we're done.
        if left_off >= heap.bh_size {
            break;
        }
        let swap_val = heap.bh_nodes[swap_off as usize];
        if heap_compare_slots(slots, sortkeys, slot_datum(node_val), slot_datum(swap_val), estate)? >= 0
        {
            break;
        }

        // Otherwise, swap the hole with the child that violates the heap
        // property; then go on to check its children.
        heap.bh_nodes[node_off as usize] = heap.bh_nodes[swap_off as usize];
        node_off = swap_off;
    }
    // Re-fill the hole.
    heap.bh_nodes[node_off as usize] = node_val;
    Ok(())
}

// ===========================================================================
// Small in-crate node-layer helpers.
// ===========================================================================

/// Borrow the node's binary heap for read.
fn heap_ref<'a, 'mcx>(node: &'a MergeAppendStateData<'mcx>) -> PgResult<&'a BinaryHeap<'mcx>> {
    node.ms_heap
        .as_deref()
        .ok_or_else(|| elog_error("MergeAppend has no binary heap"))
}

/// Borrow the node's binary heap for write (used by the comparator-free heap
/// operations: `add_unordered`, `reset`).
fn heap_mut<'a, 'mcx>(node: &'a mut MergeAppendStateData<'mcx>) -> PgResult<&'a mut BinaryHeap<'mcx>> {
    node.ms_heap
        .as_deref_mut()
        .ok_or_else(|| elog_error("MergeAppend has no binary heap"))
}

/// `ExecClearTuple(node->ps.ps_ResultTupleSlot)` — clear the node's result slot
/// (marking it empty). The C returns the slot; here the slot stays in the
/// `EState` pool and the caller observes the cleared slot via
/// `node.ps.ps_ResultTupleSlot`.
fn clear_result_slot<'mcx>(
    node: &mut MergeAppendStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    match node.ps.ps_ResultTupleSlot {
        Some(id) => execTuples::exec_clear_tuple::call(estate.slot_mut(id)),
        // A MergeAppend always has a result slot after init; a missing one is a
        // structural bug, surfaced loudly rather than silently pretended.
        None => Err(elog_error("MergeAppend has no result tuple slot")),
    }
}

/// `TupIsNull(slot)` — true if `slot` is absent or marked empty
/// (`TTS_FLAG_EMPTY`). (`TupIsNull` macro in `executor/tuptable.h`.) The slot is
/// an id into `estate.es_tupleTable` (`None` => true; otherwise resolve and test
/// emptiness).
fn tup_is_null(slot: Option<SlotId>, estate: &EStateData<'_>) -> bool {
    match slot {
        None => true,
        Some(id) => estate.slot(id).is_empty(),
    }
}

/// `INVERT_COMPARE_RESULT(var)` (sortsupport.h) — flip the sign of a three-way
/// comparison result while avoiding the `-INT_MIN` overflow corner case.
///
/// ```c
/// #define INVERT_COMPARE_RESULT(var) ((var) = ((var) < 0) ? 1 : -(var))
/// ```
fn INVERT_COMPARE_RESULT(var: i32) -> i32 {
    if var < 0 {
        1
    } else {
        var.wrapping_neg()
    }
}

/// `list_length(l)` (pg_list.h) — number of cells in `l`. `MergeAppend.mergeplans`
/// is a `List *` of child `Plan` nodes, materialized as a vector; the cell count
/// is the slice length (mirroring C's element-type-agnostic `list_length`).
fn list_length(l: &[types_nodes::nodes::Node<'_>]) -> i32 {
    i32::try_from(l.len()).unwrap_or(i32::MAX)
}

/// `ApplySortComparator(datum1, isNull1, datum2, isNull2, ssup)` (sortsupport.h)
/// — three-way compare two datums with the supplied `SortSupport`, honouring
/// NULL ordering (`ssup_nulls_first`) and reverse ordering (`ssup_reverse`). The
/// per-key datatype comparator is established at init; it is invoked through the
/// sortsupport seam.
///
/// ```c
/// static inline int
/// ApplySortComparator(Datum datum1, bool isNull1, Datum datum2, bool isNull2,
///                     SortSupport ssup) {
///     int compare;
///     if (isNull1) {
///         if (isNull2) compare = 0;
///         else if (ssup->ssup_nulls_first) compare = -1;
///         else compare = 1;
///     } else if (isNull2) {
///         compare = ssup->ssup_nulls_first ? 1 : -1;
///     } else {
///         compare = ApplyUnsignedSortComparator(datum1, datum2, ssup);
///         if (ssup->ssup_reverse) INVERT_COMPARE_RESULT(compare);
///     }
///     return compare;
/// }
/// ```
// `datum1`/`datum2` are real per-column sort values that flow straight from the
// `slot_getattr` seam into the `apply_sort_comparator` seam. Both seams still
// carry the bare-word shim (`StoredSlotWord`) in their contracts and are owned
// by other crates (out of this migration's scope), so this pass-through stays
// on the shim — migrating it here would diverge those seam contracts.
fn ApplySortComparator(
    datum1: StoredSlotWord,
    is_null1: bool,
    datum2: StoredSlotWord,
    is_null2: bool,
    ssup: &SortSupportData<'_>,
) -> PgResult<i32> {
    let compare = if is_null1 {
        if is_null2 {
            0
        } else if ssup.ssup_nulls_first {
            -1
        } else {
            1
        }
    } else if is_null2 {
        if ssup.ssup_nulls_first {
            1
        } else {
            -1
        }
    } else {
        let mut compare = sortsupport::apply_sort_comparator::call(datum1, datum2, ssup)?;
        if ssup.ssup_reverse {
            compare = INVERT_COMPARE_RESULT(compare);
        }
        compare
    };
    Ok(compare)
}

/// Deep-clone an optional bitmapset (the owned-tree stand-in for the C reuse of
/// the same `Bitmapset *` in two fields / re-reading `ps.chgParam` across a
/// child mutation). Copying allocates, so it is fallible.
fn clone_bitmapset<'mcx>(
    mcx: Mcx<'mcx>,
    set: Option<&Bitmapset<'_>>,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    match set {
        Some(s) => Ok(Some(alloc_in(mcx, s.clone_in(mcx)?)?)),
        None => Ok(None),
    }
}

/// `elog(ERROR, msg)` — internal-error text with `ERRCODE_INTERNAL_ERROR`.
fn elog_error(message: &'static str) -> PgError {
    PgError::error(message).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

#[cfg(test)]
mod tests {
    //! Seam-free unit tests for the crate's owned, non-seamed logic. The full
    //! `Exec*MergeAppend` paths drive a live executor through ~8 sibling seams
    //! whose owners are not yet ported (they panic loudly until then), so they
    //! are exercised by the integration suite once those owners land; here we
    //! cover the comparator's NULL ordering, the compare-result inversion, and
    //! the in-crate binary-heap structure.
    use super::*;
    use mcx::MemoryContext;

    #[test]
    fn invert_compare_result_matches_c() {
        assert_eq!(INVERT_COMPARE_RESULT(-7), 1);
        assert_eq!(INVERT_COMPARE_RESULT(7), -7);
        assert_eq!(INVERT_COMPARE_RESULT(0), 0);
        // -INT_MIN corner case avoided: INT_MIN < 0 so result is 1.
        assert_eq!(INVERT_COMPARE_RESULT(i32::MIN), 1);
    }

    #[test]
    fn apply_sort_comparator_null_handling() {
        let ctx = MemoryContext::new("ma-test");
        let mut key = SortSupportData::new(ctx.mcx());
        // `ApplySortComparator` carries the shim column-value word at its seam
        // boundary (see its definition), so these args use `StoredSlotWord`.
        // both null -> 0 (the seam is never consulted on a null branch).
        assert_eq!(
            ApplySortComparator(StoredSlotWord::null(), true, StoredSlotWord::null(), true, &key).unwrap(),
            0
        );
        // null1 only, nulls_first -> -1 ; nulls_last -> 1
        key.ssup_nulls_first = true;
        assert_eq!(
            ApplySortComparator(StoredSlotWord::null(), true, StoredSlotWord::from_i32(5), false, &key)
                .unwrap(),
            -1
        );
        key.ssup_nulls_first = false;
        assert_eq!(
            ApplySortComparator(StoredSlotWord::null(), true, StoredSlotWord::from_i32(5), false, &key)
                .unwrap(),
            1
        );
        // null2 only, nulls_first -> 1 ; nulls_last -> -1
        key.ssup_nulls_first = true;
        assert_eq!(
            ApplySortComparator(StoredSlotWord::from_i32(5), false, StoredSlotWord::null(), true, &key)
                .unwrap(),
            1
        );
        key.ssup_nulls_first = false;
        assert_eq!(
            ApplySortComparator(StoredSlotWord::from_i32(5), false, StoredSlotWord::null(), true, &key)
                .unwrap(),
            -1
        );
    }

    #[test]
    fn binary_heap_allocate_starts_empty_then_add_and_reset() {
        let ctx = MemoryContext::new("ma-heap");
        let mut heap = BinaryHeap::allocate(ctx.mcx(), 4).unwrap();
        assert_eq!(heap.bh_space, 4);
        assert_eq!(heap.bh_size, 0);
        assert!(binaryheap_empty(&heap));

        // add_unordered respects capacity and tracks size; the heap property
        // flag drops until a build (not run here).
        binaryheap_add_unordered(&mut heap, Datum::from_i32(0)).unwrap();
        binaryheap_add_unordered(&mut heap, Datum::from_i32(1)).unwrap();
        assert_eq!(heap.bh_size, 2);
        assert!(!heap.bh_has_heap_property);
        assert_eq!(binaryheap_first(&heap).unwrap().as_i32(), 0);

        binaryheap_reset(&mut heap);
        assert_eq!(heap.bh_size, 0);
        assert!(heap.bh_has_heap_property);
        assert!(heap.bh_nodes.is_empty());
    }

    #[test]
    fn binary_heap_add_unordered_overflows_at_capacity() {
        let ctx = MemoryContext::new("ma-heap-of");
        let mut heap = BinaryHeap::allocate(ctx.mcx(), 1).unwrap();
        binaryheap_add_unordered(&mut heap, Datum::from_i32(0)).unwrap();
        // C: elog(ERROR, "out of binary heap slots").
        assert!(binaryheap_add_unordered(&mut heap, Datum::from_i32(1)).is_err());
    }

    #[test]
    fn heap_offsets_match_c() {
        // Standard array-embedded binary heap index arithmetic.
        assert_eq!(parent_offset(1), 0);
        assert_eq!(parent_offset(2), 0);
        assert_eq!(parent_offset(3), 1);
        assert_eq!(left_offset(0), 1);
        assert_eq!(right_offset(0), 2);
        assert_eq!(left_offset(1), 3);
        assert_eq!(right_offset(1), 4);
    }
}
