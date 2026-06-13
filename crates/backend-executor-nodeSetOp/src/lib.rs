//! Port of `src/backend/executor/nodeSetOp.c` — routines to handle INTERSECT
//! and EXCEPT selection.
//!
//! INTERFACE ROUTINES
//! - [`ExecInitSetOp`]    - initialize the setop node and its subplans.
//! - [`ExecSetOp`]        - the node's `ExecProcNode` body (next group → tuple).
//! - [`ExecEndSetOp`]     - shut down the subplans and free resources.
//! - [`ExecReScanSetOp`]  - rescan the setop node.
//!
//! The input of a SetOp node is two relations (outer = left, inner = right) with
//! identical column sets. In `SETOP_SORTED` mode each input is sorted by all
//! grouping columns and the node performs a merge over the grouping columns,
//! counting how many tuples from each input match; in `SETOP_HASHED` mode the
//! outer relation is read into a tuple hash table (one entry per group, counting
//! tuples), then the inner relation is counted against it and the table scanned
//! to emit the SQL-spec output for INTERSECT/INTERSECT ALL/EXCEPT/EXCEPT ALL.
//! SetOp does no qual checking nor projection: the output tuples are copies of
//! the first-to-arrive tuple in each group.
//!
//! The whole state machine (`ExecSetOp` dispatch, `setop_retrieve_sorted` merge,
//! `setop_load_group`, `setop_compare_slots`, `setop_fill_hash_table`,
//! `setop_retrieve_hash_table`, `set_output_count`, `build_hash_table`) is this
//! crate's owned logic. Operations below the executor-node layer go through the
//! owners' seam crates:
//!
//! - interrupt servicing (`CHECK_FOR_INTERRUPTS`) → tcop/postgres;
//! - child dispatch / init / teardown / rescan (`ExecProcNode` / `ExecInitNode`
//!   / `ExecEndNode` / `ExecReScan`) → execProcnode / execAmi;
//! - econtext / slot setup and slot ops (`ExecAssignExprContext` /
//!   `ExecInitResultTupleSlotTL` / `ExecInitExtraTupleSlot` /
//!   `ExecGetResultType` / `ExecGetCommonChildSlotOps` / `ExecClearTuple` /
//!   `slot_getallattrs` / `ExecCopySlotMinimalTuple` / `ExecStoreMinimalTuple`)
//!   → execUtils / execTuples;
//! - the tuple hash table (`execTuplesHashPrepare` / `BuildTupleHashTable` /
//!   `LookupTupleHashEntry` / `ResetTupleHashTable` / `ResetTupleHashIterator` /
//!   `ScanTupleHashTable`) → execGrouping;
//! - the sort-support setup and comparator (`PrepareSortSupportFromOrderingOp` /
//!   `ApplySortComparator`) → sortsupport.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use backend_executor_execAmi_seams as execAmi;
use backend_executor_execGrouping_seams as execGrouping;
use backend_executor_execProcnode_seams as execProcnode;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils;
use backend_tcop_postgres_seams as tcop_postgres;
use backend_utils_sort_sortsupport_seams as sortsupport;

use mcx::{Mcx, MemoryContext, PgBox};
use types_datum::Datum;
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_nodes::executor::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, EXEC_FLAG_REWIND};
use types_nodes::nodesetop::{
    SetOp, SetOpStateData, SetOpStatePerGroupData, SetOpStatePerInput, TupleHashEntryData,
    SETOPCMD_EXCEPT, SETOPCMD_EXCEPT_ALL, SETOPCMD_INTERSECT, SETOPCMD_INTERSECT_ALL,
    SETOP_HASHED,
};
use types_nodes::{EStateData, PlanStateNode, SlotId, TupleSlotKind};
use types_sortsupport::SortSupportData;
use types_tuple::backend_access_common_heaptuple::TupleValue;

/// Install this crate's seam implementations. nodeSetOp owns no inbound seams:
/// it is reached through the executor dispatch (execProcnode), which can depend
/// on this crate directly without a cycle, so there is nothing to install.
pub fn init_seams() {}

/// Which input of the merge a sorted-mode operation acts on. In C the helpers
/// take a `SetOpStatePerInput *input` plus the matching `PlanState *inputPlan`;
/// both the per-input state (`leftInput`/`rightInput`) and the child plan
/// (`ps.lefttree`/`ps.righttree`) are fields of the state node, so the side is
/// named and the field selected internally. `Left` is the outer relation,
/// `Right` the inner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Side {
    Left,
    Right,
}

// ===========================================================================
// Node state machine (ported 1:1 from nodeSetOp.c).
// ===========================================================================

/// Initialize the hash table to empty.
///
/// ```c
/// static void
/// build_hash_table(SetOpState *setopstate)
/// {
///     SetOp      *node = (SetOp *) setopstate->ps.plan;
///     ExprContext *econtext = setopstate->ps.ps_ExprContext;
///     TupleDesc   desc = ExecGetResultType(outerPlanState(setopstate));
///
///     Assert(node->strategy == SETOP_HASHED);
///     Assert(node->numGroups > 0);
///
///     setopstate->hashtable = BuildTupleHashTable(&setopstate->ps, desc,
///                 ExecGetCommonChildSlotOps(&setopstate->ps), node->numCols,
///                 node->cmpColIdx, setopstate->eqfuncoids,
///                 setopstate->hashfunctions, node->cmpCollations,
///                 node->numGroups, sizeof(SetOpStatePerGroupData),
///                 setopstate->ps.state->es_query_cxt, setopstate->tableContext,
///                 econtext->ecxt_per_tuple_memory, false);
/// }
/// ```
fn build_hash_table<'mcx>(
    setopstate: &mut SetOpStateData<'mcx>,
    plan: &SetOp<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(plan.strategy == SETOP_HASHED);
    debug_assert!(plan.numGroups > 0);

    let mcx = estate.es_query_cxt;

    // desc = ExecGetResultType(outerPlanState(setopstate));
    let outer = setopstate
        .ps
        .lefttree
        .as_deref()
        .expect("build_hash_table: outerPlanState is NULL");
    let desc: types_tuple::heaptuple::TupleDesc<'mcx> =
        match execTuples::exec_get_result_type::call(outer.ps_head()) {
            Some(d) => Some(mcx::alloc_in(mcx, d.clone_in(mcx)?)?),
            None => None,
        };

    // inputOps = ExecGetCommonChildSlotOps(&setopstate->ps)
    let input_ops = execUtils::exec_get_common_child_slot_ops::call(&setopstate.ps, estate)
        .unwrap_or(TupleSlotKind::MinimalTuple);

    let ecxt = setopstate
        .ps
        .ps_ExprContext
        .expect("build_hash_table: ps_ExprContext is NULL");

    // The per-tuple memory context (tempcxt) the hash search uses comes from the
    // node's ExprContext (econtext->ecxt_per_tuple_memory); the table's bucket
    // array (metacxt) lives in the per-query context, entries in tableContext.
    // All three contexts are caller-owned; the seam borrows them.
    let metacxt: &MemoryContext = mcx.context();
    let tablecxt: &MemoryContext = setopstate
        .tableContext
        .as_ref()
        .expect("build_hash_table: tableContext is NULL");
    let tempcxt: &MemoryContext = &estate.ecxt(ecxt).ecxt_per_tuple_memory;

    let table = execGrouping::build_tuple_hash_table::call(
        mcx,
        None,
        desc,
        input_ops,
        plan.numCols,
        plan.cmpColIdx.as_slice(),
        setopstate.eqfuncoids.as_slice(),
        setopstate.hashfunctions.as_slice(),
        plan.cmpCollations.as_slice(),
        plan.numGroups,
        core::mem::size_of::<SetOpStatePerGroupData>(),
        metacxt,
        tablecxt,
        tempcxt,
        false,
    )?;
    setopstate.hashtable = Some(table);
    Ok(())
}

/// We've completed processing a tuple group. Decide how many copies (if any) of
/// its representative row to emit, and store the count into `numOutput`. This
/// logic is straight from the SQL92 specification.
///
/// ```c
/// static void
/// set_output_count(SetOpState *setopstate, SetOpStatePerGroup pergroup)
/// ```
fn set_output_count(
    setopstate: &mut SetOpStateData<'_>,
    plan: &SetOp<'_>,
    pergroup: &SetOpStatePerGroupData,
) -> PgResult<()> {
    let num_left = pergroup.numLeft;
    let num_right = pergroup.numRight;

    let num_output = match plan.cmd {
        SETOPCMD_INTERSECT => {
            if num_left > 0 && num_right > 0 {
                1
            } else {
                0
            }
        }
        SETOPCMD_INTERSECT_ALL => {
            if num_left < num_right {
                num_left
            } else {
                num_right
            }
        }
        SETOPCMD_EXCEPT => {
            if num_left > 0 && num_right == 0 {
                1
            } else {
                0
            }
        }
        SETOPCMD_EXCEPT_ALL => {
            if num_left < num_right {
                0
            } else {
                num_left - num_right
            }
        }
        other => {
            // elog(ERROR, "unrecognized set op: %d", (int) plannode->cmd);
            return Err(elog_error_fmt(alloc::format!("unrecognized set op: {}", other)));
        }
    };
    setopstate.numOutput = num_output;
    Ok(())
}

/// `ExecSetOp(pstate)` — the node's `ExecProcNode` body. Returns a tuple slot id
/// or `None`.
///
/// ```c
/// static TupleTableSlot *
/// ExecSetOp(PlanState *pstate)
/// ```
///
/// The produced row is handed back as the node's result-slot id
/// (`Some(ps_ResultTupleSlot)` — exactly the C returning the `ps_ResultTupleSlot`
/// pointer it keeps), `None` is end-of-scan.
pub fn ExecSetOp<'mcx>(
    node: &mut SetOpStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // CHECK_FOR_INTERRUPTS();
    tcop_postgres::check_for_interrupts::call()?;

    let plan = setop_plan(node);

    // If the previously-returned tuple needs to be returned more than once, keep
    // returning it.
    if node.numOutput > 0 {
        node.numOutput -= 1;
        return Ok(node.ps.ps_ResultTupleSlot);
    }

    // Otherwise, we're done if we are out of groups.
    if node.setop_done {
        return Ok(None);
    }

    // Fetch the next tuple group according to the correct strategy.
    if plan.strategy == SETOP_HASHED {
        if !node.table_filled {
            setop_fill_hash_table(node, estate)?;
        }
        setop_retrieve_hash_table(node, estate)
    } else {
        setop_retrieve_sorted(node, estate)
    }
}

/// The `ExecProcNodeMtd` adapter installed in `ps.ExecProcNode`: `castNode` the
/// dispatch enum back to `SetOpState` and run [`ExecSetOp`].
fn exec_setop_node<'mcx>(
    pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    match pstate {
        PlanStateNode::SetOp(node) => ExecSetOp(node, estate),
        other => panic!(
            "exec_setop_node: ExecProcNode dispatched a non-SetOp node (tag {})",
            other.tag()
        ),
    }
}

/// `ExecSetOp` for non-hashed case.
///
/// ```c
/// static TupleTableSlot *
/// setop_retrieve_sorted(SetOpState *setopstate)
/// ```
fn setop_retrieve_sorted<'mcx>(
    setopstate: &mut SetOpStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // If first time through, establish the invariant that setop_load_group
    // expects: each side's nextTupleSlot is the next output from the child plan,
    // or empty if there is no more output from it.
    //   outerPlan = outerPlanState(setopstate);  -> ps.lefttree
    //   innerPlan = innerPlanState(setopstate);  -> ps.righttree
    if setopstate.need_init {
        setopstate.need_init = false;

        //   setopstate->leftInput.nextTupleSlot = ExecProcNode(outerPlan);
        let left_next = exec_proc_child(setopstate, Side::Left, estate)?;
        setopstate.leftInput.nextTupleSlot = left_next;

        // If the outer relation is empty, then we will emit nothing, and we
        // don't need to read the inner relation at all.
        if tup_is_null(left_next, estate) {
            setopstate.setop_done = true;
            return Ok(None);
        }

        //   setopstate->rightInput.nextTupleSlot = ExecProcNode(innerPlan);
        let right_next = exec_proc_child(setopstate, Side::Right, estate)?;
        setopstate.rightInput.nextTupleSlot = right_next;

        // Set flags that we've not completed either side's group.
        setopstate.leftInput.needGroup = true;
        setopstate.rightInput.needGroup = true;
    }

    // We loop retrieving groups until we find one we should return.
    while !setopstate.setop_done {
        let mut pergroup = SetOpStatePerGroupData {
            numLeft: 0,
            numRight: 0,
        };

        // Fetch the rest of the current outer group, if we didn't already.
        if setopstate.leftInput.needGroup {
            setop_load_group(setopstate, Side::Left, estate)?;
        }

        // If no more outer groups, we're done, and don't need to look at any
        // more of the inner relation.
        if setopstate.leftInput.numTuples == 0 {
            setopstate.setop_done = true;
            break;
        }

        // Fetch the rest of the current inner group, if we didn't already.
        if setopstate.rightInput.needGroup {
            setop_load_group(setopstate, Side::Right, estate)?;
        }

        // Determine whether we have matching groups on both sides (this is
        // basically like the core logic of a merge join).
        let cmpresult = if setopstate.rightInput.numTuples == 0 {
            -1 // as though left input is lesser
        } else {
            // setop_compare_slots(leftInput.firstTupleSlot,
            //                     rightInput.firstTupleSlot, setopstate)
            let s1 = setopstate
                .leftInput
                .firstTupleSlot
                .expect("setop_retrieve_sorted: leftInput.firstTupleSlot is NULL");
            let s2 = setopstate
                .rightInput
                .firstTupleSlot
                .expect("setop_retrieve_sorted: rightInput.firstTupleSlot is NULL");
            setop_compare_slots(setopstate, s1, s2, estate)?
        };

        if cmpresult < 0 {
            // Left group is first, and has no right matches.
            pergroup.numLeft = setopstate.leftInput.numTuples;
            pergroup.numRight = 0;
            // We'll need another left group next time.
            setopstate.leftInput.needGroup = true;
        } else if cmpresult == 0 {
            // We have matching groups.
            pergroup.numLeft = setopstate.leftInput.numTuples;
            pergroup.numRight = setopstate.rightInput.numTuples;
            // We'll need to read from both sides next time.
            setopstate.leftInput.needGroup = true;
            setopstate.rightInput.needGroup = true;
        } else {
            // Right group has no left matches, so we can ignore it.
            setopstate.rightInput.needGroup = true;
            continue;
        }

        // Done scanning these input tuple groups. See if we should emit any
        // copies of result tuple, and if so return the first copy. (The result
        // tuple is the same as the left input's firstTuple slot.)
        let plan = setop_plan(setopstate);
        set_output_count(setopstate, plan, &pergroup)?;

        if setopstate.numOutput > 0 {
            setopstate.numOutput -= 1;
            // C: `return resultTupleSlot` — the node's own result slot's id.
            return Ok(setopstate.ps.ps_ResultTupleSlot);
        }
    }

    // No more groups.
    clear_result_tuple(setopstate, estate)?;
    Ok(None)
}

/// Load next group of tuples from one child plan or the other.
///
/// On entry, we've already read the first tuple of the next group (if there is
/// one) into `input->nextTupleSlot`. This invariant is maintained on exit.
///
/// ```c
/// static void
/// setop_load_group(SetOpStatePerInput *input, PlanState *inputPlan,
///                  SetOpState *setopstate)
/// ```
fn setop_load_group<'mcx>(
    setopstate: &mut SetOpStateData<'mcx>,
    side: Side,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    input_mut(setopstate, side).needGroup = false;

    // If we've exhausted this child plan, report an empty group.
    let next = input_ref(setopstate, side).nextTupleSlot;
    if tup_is_null(next, estate) {
        //   ExecClearTuple(input->firstTupleSlot);
        let first = input_ref(setopstate, side)
            .firstTupleSlot
            .expect("setop_load_group: firstTupleSlot is NULL");
        execTuples::exec_clear_tuple::call(estate.slot_mut(first))?;
        input_mut(setopstate, side).numTuples = 0;
        return Ok(());
    }

    // Make a local copy of the first tuple for comparisons.
    //   ExecStoreMinimalTuple(ExecCopySlotMinimalTuple(input->nextTupleSlot),
    //                         input->firstTupleSlot, true);
    let next = next.expect("setop_load_group: nextTupleSlot checked non-null");
    let mtup = execTuples::exec_copy_slot_minimal_tuple::call(estate.es_query_cxt, estate, next)?;
    let first = input_ref(setopstate, side)
        .firstTupleSlot
        .expect("setop_load_group: firstTupleSlot is NULL");
    execTuples::exec_store_minimal_tuple::call(estate, mtup, first, true)?;
    // and count it
    input_mut(setopstate, side).numTuples = 1;

    // Scan till we find the end-of-group.
    loop {
        // Get next input tuple, if there is one.
        //   input->nextTupleSlot = ExecProcNode(inputPlan);
        let next = exec_proc_child(setopstate, side, estate)?;
        input_mut(setopstate, side).nextTupleSlot = next;
        if tup_is_null(next, estate) {
            break;
        }

        // There is; does it belong to same group as firstTuple?
        //   cmpresult = setop_compare_slots(input->firstTupleSlot,
        //                                   input->nextTupleSlot, setopstate);
        let first = input_ref(setopstate, side)
            .firstTupleSlot
            .expect("setop_load_group: firstTupleSlot is NULL");
        let next = next.expect("setop_load_group: nextTupleSlot checked non-null");
        let cmpresult = setop_compare_slots(setopstate, first, next, estate)?;
        debug_assert!(cmpresult <= 0); // else input is mis-sorted
        if cmpresult != 0 {
            break;
        }

        // Still in same group, so count this tuple.
        input_mut(setopstate, side).numTuples += 1;
    }
    Ok(())
}

/// Compare the tuples in the two given slots.
///
/// ```c
/// static int
/// setop_compare_slots(TupleTableSlot *s1, TupleTableSlot *s2,
///                     SetOpState *setopstate)
/// {
///     slot_getallattrs(s1);
///     slot_getallattrs(s2);
///     for (int nkey = 0; nkey < setopstate->numCols; nkey++)
///     {
///         SortSupport sortKey = setopstate->sortKeys + nkey;
///         AttrNumber  attno = sortKey->ssup_attno;
///         Datum       datum1 = s1->tts_values[attno - 1],
///                     datum2 = s2->tts_values[attno - 1];
///         bool        isNull1 = s1->tts_isnull[attno - 1],
///                     isNull2 = s2->tts_isnull[attno - 1];
///         int         compare = ApplySortComparator(datum1, isNull1,
///                                                    datum2, isNull2, sortKey);
///         if (compare != 0)
///             return compare;
///     }
///     return 0;
/// }
/// ```
fn setop_compare_slots<'mcx>(
    setopstate: &SetOpStateData<'mcx>,
    s1: SlotId,
    s2: SlotId,
    estate: &EStateData<'mcx>,
) -> PgResult<i32> {
    // slot_getallattrs(s1); slot_getallattrs(s2);
    let cols1 = execTuples::slot_getallattrs::call(estate.es_query_cxt, estate.slot(s1))?;
    let cols2 = execTuples::slot_getallattrs::call(estate.es_query_cxt, estate.slot(s2))?;

    for nkey in 0..setopstate.numCols as usize {
        let sort_key = &setopstate.sortKeys[nkey];
        let attno = sort_key.ssup_attno;
        let idx = (attno as usize) - 1;
        // datum1/isNull1 = s1->tts_values/tts_isnull[attno-1]; likewise s2.
        let (datum1, is_null1) = deformed_datum(&cols1[idx]);
        let (datum2, is_null2) = deformed_datum(&cols2[idx]);

        let compare = apply_sort_comparator(datum1, is_null1, datum2, is_null2, sort_key)?;
        if compare != 0 {
            return Ok(compare);
        }
    }
    Ok(0)
}

/// `ExecSetOp` for hashed case: phase 1, read inputs and build hash table.
///
/// ```c
/// static void
/// setop_fill_hash_table(SetOpState *setopstate)
/// ```
fn setop_fill_hash_table<'mcx>(
    setopstate: &mut SetOpStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mut have_tuples = false;

    // Process each outer-plan tuple, and then fetch the next one, until we
    // exhaust the outer plan.
    loop {
        //   outerslot = ExecProcNode(outerPlan);
        let outerslot = exec_proc_child(setopstate, Side::Left, estate)?;
        if tup_is_null(outerslot, estate) {
            break;
        }
        let outerslot = outerslot.expect("setop_fill_hash_table: outerslot checked non-null");
        have_tuples = true;

        // Find or build hashtable entry for this tuple's group.
        //   entry = LookupTupleHashEntry(hashtable, outerslot, &isnew, NULL);
        //   pergroup = TupleHashEntryGetAdditional(hashtable, entry);
        //   if (isnew) { pergroup->numLeft = 0; pergroup->numRight = 0; }
        //   pergroup->numLeft++;
        let hashtable = setopstate
            .hashtable
            .as_mut()
            .expect("setop_fill_hash_table: hashtable is NULL");
        execGrouping::lookup_tuple_hash_entry::call(
            hashtable,
            outerslot,
            true,
            estate,
            &mut |isnew: bool, _entry: &mut TupleHashEntryData<'mcx>, additional: &mut [u8]| {
                // pergroup = TupleHashEntryGetAdditional(hashtable, entry);
                // if (isnew) { pergroup->numLeft = 0; pergroup->numRight = 0; }
                let mut pergroup = if isnew {
                    SetOpStatePerGroupData {
                        numLeft: 0,
                        numRight: 0,
                    }
                } else {
                    read_pergroup(additional)
                };
                // pergroup->numLeft++;
                pergroup.numLeft += 1;
                write_pergroup(additional, &pergroup);
            },
        )?;

        // Must reset expression context after each hashtable lookup.
        //   ResetExprContext(econtext);
        reset_per_tuple_context(setopstate, estate);
    }

    // If the outer relation is empty, then we will emit nothing, and we don't
    // need to read the inner relation at all.
    if have_tuples {
        // Process each inner-plan tuple, and then fetch the next one, until we
        // exhaust the inner plan.
        loop {
            //   innerslot = ExecProcNode(innerPlan);
            let innerslot = exec_proc_child(setopstate, Side::Right, estate)?;
            if tup_is_null(innerslot, estate) {
                break;
            }
            let innerslot =
                innerslot.expect("setop_fill_hash_table: innerslot checked non-null");

            // For tuples not seen previously, do not make hashtable entry.
            //   entry = LookupTupleHashEntry(hashtable, innerslot, NULL, NULL);
            //   if (entry) { pergroup = ...; pergroup->numRight++; }
            let hashtable = setopstate
                .hashtable
                .as_mut()
                .expect("setop_fill_hash_table: hashtable is NULL");
            execGrouping::lookup_tuple_hash_entry::call(
                hashtable,
                innerslot,
                false,
                estate,
                &mut |_isnew: bool, _entry: &mut TupleHashEntryData<'mcx>, additional: &mut [u8]| {
                    // entry present (create == false ⇒ never new): pergroup->numRight++.
                    let mut pergroup = read_pergroup(additional);
                    pergroup.numRight += 1;
                    write_pergroup(additional, &pergroup);
                },
            )?;

            // Must reset expression context after each hashtable lookup.
            reset_per_tuple_context(setopstate, estate);
        }
    }

    setopstate.table_filled = true;
    // Initialize to walk the hash table.
    //   ResetTupleHashIterator(setopstate->hashtable, &setopstate->hashiter);
    let hashtable = setopstate
        .hashtable
        .as_mut()
        .expect("setop_fill_hash_table: hashtable is NULL");
    setopstate.hashiter = execGrouping::reset_tuple_hash_iterator::call(hashtable);
    Ok(())
}

/// `ExecSetOp` for hashed case: phase 2, retrieving groups from hash table.
///
/// ```c
/// static TupleTableSlot *
/// setop_retrieve_hash_table(SetOpState *setopstate)
/// ```
fn setop_retrieve_hash_table<'mcx>(
    setopstate: &mut SetOpStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // We loop retrieving groups until we find one we should return.
    while !setopstate.setop_done {
        // CHECK_FOR_INTERRUPTS();
        tcop_postgres::check_for_interrupts::call()?;

        // Find the next entry in the hash table.
        //   entry = ScanTupleHashTable(hashtable, &setopstate->hashiter);
        let mut pergroup: Option<SetOpStatePerGroupData> = None;
        let result_slot = setopstate
            .ps
            .ps_ResultTupleSlot
            .expect("setop_retrieve_hash_table: ps_ResultTupleSlot is NULL");
        // The entry's firstTuple is stored into the result slot by the owner of
        // ScanTupleHashTable; we capture the per-group counts here, then run
        // set_output_count and the store below.
        let mut entry_tuple: Option<types_tuple::heaptuple::MinimalTuple<'mcx>> = None;
        let found = {
            let mcx = estate.es_query_cxt;
            let mut hashiter = setopstate.hashiter;
            let hashtable = setopstate
                .hashtable
                .as_mut()
                .expect("setop_retrieve_hash_table: hashtable is NULL");
            let found = execGrouping::scan_tuple_hash_table::call(
                hashtable,
                &mut hashiter,
                estate,
                &mut |entry: &mut TupleHashEntryData<'mcx>, additional: &mut [u8]| {
                    pergroup = Some(read_pergroup(additional));
                    // TupleHashEntryGetTuple(entry) — the group's first tuple,
                    // which set_output_count's emit path stores into the result
                    // slot. We capture an owned copy to store below; the entry's
                    // firstTuple lives in the table's tablecxt.
                    entry_tuple = Some(copy_minimal_tuple(&entry.firstTuple, mcx));
                },
            )?;
            setopstate.hashiter = hashiter;
            found
        };

        if !found {
            // No more entries in hashtable, so done.
            setopstate.setop_done = true;
            return Ok(None);
        }

        // See if we should emit any copies of this tuple, and if so return the
        // first copy.
        let pergroup = pergroup.expect("scan callback ran on a found entry");
        let plan = setop_plan(setopstate);
        set_output_count(setopstate, plan, &pergroup)?;

        if setopstate.numOutput > 0 {
            setopstate.numOutput -= 1;
            //   return ExecStoreMinimalTuple(TupleHashEntryGetTuple(entry),
            //                                resultTupleSlot, false);
            let mtup = entry_tuple.expect("scan callback captured the entry tuple");
            execTuples::exec_store_minimal_tuple::call(estate, mtup, result_slot, false)?;
            return Ok(setopstate.ps.ps_ResultTupleSlot);
        }
    }

    // No more groups.
    clear_result_tuple(setopstate, estate)?;
    Ok(None)
}

/// `ExecInitSetOp(node, estate, eflags)` — initialize the setop node state
/// structures and the node's subplan.
///
/// ```c
/// SetOpState *
/// ExecInitSetOp(SetOp *node, EState *estate, int eflags)
/// ```
///
/// The `node`/`estate` back-links of `ps` are reconstructed by the executor when
/// it splices the node into the tree; this routine fills the rest. Returns the
/// state node wrapped in the dispatch enum for the executor's `PlanStateNode`
/// tree.
pub fn ExecInitSetOp<'mcx>(
    plan_node: &'mcx types_nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    mut eflags: i32,
) -> PgResult<PgBox<'mcx, SetOpStateData<'mcx>>> {
    // check for unsupported flags
    //   Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
    debug_assert!(
        eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0,
        "SetOp does not support EXEC_FLAG_BACKWARD/EXEC_FLAG_MARK"
    );

    let mcx = estate.es_query_cxt;

    let node: &'mcx SetOp<'mcx> = match plan_node {
        types_nodes::nodes::Node::SetOp(s) => s,
        other => panic!("castNode(SetOp, node) failed: {other:?}"),
    };

    // create state structure: makeNode(SetOpState)
    let mut setopstate = mcx::alloc_in(mcx, SetOpStateData::new_in(mcx))?;
    //   setopstate->ps.plan = (Plan *) node;
    //   setopstate->ps.state = estate;            (threaded explicitly as estate)
    //   setopstate->ps.ExecProcNode = ExecSetOp;
    setopstate.ps.plan = Some(plan_node);
    setopstate.ps.ExecProcNode = Some(exec_setop_node);

    setopstate.setop_done = false;
    setopstate.numOutput = 0;
    setopstate.numCols = node.numCols;
    setopstate.need_init = true;

    // create expression context
    //   ExecAssignExprContext(estate, &setopstate->ps);
    execUtils::exec_assign_expr_context::call(estate, &mut setopstate.ps)?;

    // If hashing, we also need a longer-lived context to store the hash table.
    // The table can't just be kept in the per-query context because we want to
    // be able to throw it away in ExecReScanSetOp.
    //   setopstate->tableContext = AllocSetContextCreate(CurrentMemoryContext,
    //       "SetOp hash table", ALLOCSET_DEFAULT_SIZES);
    if node.strategy == SETOP_HASHED {
        setopstate.tableContext = Some(mcx.context().new_child("SetOp hash table"));
    }

    // initialize child nodes
    //
    // If we are hashing then the child plans do not need to handle REWIND
    // efficiently; see ExecReScanSetOp.
    if node.strategy == SETOP_HASHED {
        eflags &= !EXEC_FLAG_REWIND;
    }
    //   outerPlanState(setopstate) = ExecInitNode(outerPlan(node), estate, eflags);
    let outer_plan = node.plan.lefttree.as_deref();
    setopstate.ps.lefttree = execProcnode::exec_init_node::call(mcx, outer_plan, estate, eflags)?;
    //   innerPlanState(setopstate) = ExecInitNode(innerPlan(node), estate, eflags);
    let inner_plan = node.plan.righttree.as_deref();
    setopstate.ps.righttree = execProcnode::exec_init_node::call(mcx, inner_plan, estate, eflags)?;

    // Initialize locally-allocated slots. In hashed mode, we just need a result
    // slot. In sorted mode, we need one first-tuple-of-group slot for each
    // input; we use the result slot for the left input's slot and create another
    // for the right input. (The nextTupleSlot slots are not ours, but point to
    // the last slot returned by the input plan node.)
    //   ExecInitResultTupleSlotTL(&setopstate->ps, &TTSOpsMinimalTuple);
    execTuples::exec_init_result_tuple_slot_tl::call(
        &mut setopstate.ps,
        estate,
        TupleSlotKind::MinimalTuple,
    )?;
    if node.strategy != SETOP_HASHED {
        //   setopstate->leftInput.firstTupleSlot = setopstate->ps.ps_ResultTupleSlot;
        // The C aliases the result slot as the left input's first-tuple slot;
        // with `firstTupleSlot: Option<SlotId>` the alias is the plain id copy.
        setopstate.leftInput.firstTupleSlot = setopstate.ps.ps_ResultTupleSlot;
        //   setopstate->rightInput.firstTupleSlot =
        //       ExecInitExtraTupleSlot(estate, ps_ResultTupleDesc, &TTSOpsMinimalTuple);
        let desc = match &setopstate.ps.ps_ResultTupleDesc {
            Some(d) => Some(mcx::alloc_in(mcx, d.clone_in(mcx)?)?),
            None => None,
        };
        let right_first =
            execTuples::exec_init_extra_tuple_slot::call(estate, desc, TupleSlotKind::MinimalTuple)?;
        setopstate.rightInput.firstTupleSlot = Some(right_first);
    }

    // Setop nodes do no projections.
    //   setopstate->ps.ps_ProjInfo = NULL;
    setopstate.ps.ps_ProjInfo = None;

    // Precompute fmgr lookup data for inner loop. We need equality and hashing
    // functions to do it by hashing, while for sorting we need SortSupport data.
    if node.strategy == SETOP_HASHED {
        //   execTuplesHashPrepare(node->numCols, node->cmpOperators,
        //                         &setopstate->eqfuncoids, &setopstate->hashfunctions);
        let (eqfuncoids, hashfunctions) = execGrouping::exec_tuples_hash_prepare::call(
            mcx,
            node.numCols,
            node.cmpOperators.as_slice(),
        )?;
        setopstate.eqfuncoids = eqfuncoids;
        setopstate.hashfunctions = hashfunctions;
    } else {
        //   nkeys = node->numCols;
        //   setopstate->sortKeys = palloc0(nkeys * sizeof(SortSupportData));
        //   for (i = 0; i < nkeys; i++) { ... PrepareSortSupportFromOrderingOp(...); }
        let nkeys = node.numCols;
        for i in 0..nkeys as usize {
            let mut sort_key = SortSupportData::new(mcx);
            //   sortKey->ssup_cxt = CurrentMemoryContext;        (== mcx)
            //   sortKey->ssup_collation = node->cmpCollations[i];
            sort_key.ssup_collation = node.cmpCollations[i];
            //   sortKey->ssup_nulls_first = node->cmpNullsFirst[i];
            sort_key.ssup_nulls_first = node.cmpNullsFirst[i];
            //   sortKey->ssup_attno = node->cmpColIdx[i];
            sort_key.ssup_attno = node.cmpColIdx[i];
            //   sortKey->abbreviate = false;     /* not useful here */
            sort_key.abbreviate = false;

            //   PrepareSortSupportFromOrderingOp(node->cmpOperators[i], sortKey);
            sortsupport::prepare_sort_support_from_ordering_op::call(
                node.cmpOperators[i],
                &mut sort_key,
            )?;

            setopstate.sortKeys.push(sort_key);
        }
    }

    // Create a hash table if needed.
    if node.strategy == SETOP_HASHED {
        build_hash_table(&mut setopstate, node, estate)?;
        setopstate.table_filled = false;
    }

    Ok(setopstate)
}

/// `ExecEndSetOp(node)` — shut down the subplans and free resources allocated to
/// this node.
///
/// ```c
/// void
/// ExecEndSetOp(SetOpState *node)
/// {
///     if (node->tableContext)
///         MemoryContextDelete(node->tableContext);
///     ExecEndNode(outerPlanState(node));
///     ExecEndNode(innerPlanState(node));
/// }
/// ```
pub fn ExecEndSetOp<'mcx>(
    node: &mut SetOpStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // free subsidiary stuff including hashtable
    //   if (node->tableContext) MemoryContextDelete(node->tableContext);
    // `mcx::MemoryContext` frees its allocation domain on drop, so taking it
    // (and dropping the hash table that lives in it) is the MemoryContextDelete.
    node.hashtable = None;
    node.tableContext = None;

    //   ExecEndNode(outerPlanState(node));
    if let Some(outer) = node.ps.lefttree.as_deref_mut() {
        execProcnode::exec_end_node::call(outer, estate)?;
    }
    //   ExecEndNode(innerPlanState(node));
    if let Some(inner) = node.ps.righttree.as_deref_mut() {
        execProcnode::exec_end_node::call(inner, estate)?;
    }
    Ok(())
}

/// `ExecReScanSetOp(node)` — rescan the node.
///
/// ```c
/// void
/// ExecReScanSetOp(SetOpState *node)
/// ```
pub fn ExecReScanSetOp<'mcx>(
    node: &mut SetOpStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    //   ExecClearTuple(node->ps.ps_ResultTupleSlot);
    clear_result_tuple(node, estate)?;
    node.setop_done = false;
    node.numOutput = 0;

    let strategy = setop_plan(node).strategy;
    if strategy == SETOP_HASHED {
        // In the hashed case, if we haven't yet built the hash table then we can
        // just return; nothing done yet, so nothing to undo. If subnode's
        // chgParam is not NULL then it will be re-scanned by ExecProcNode, else
        // no reason to re-scan it at all.
        if !node.table_filled {
            return Ok(());
        }

        // If we do have the hash table and the subplans do not have any
        // parameter changes, then we can just rescan the existing hash table;
        // no need to build it again.
        //   if (outerPlan->chgParam == NULL && innerPlan->chgParam == NULL)
        if child_chgparam_is_null(node, Side::Left) && child_chgparam_is_null(node, Side::Right) {
            //   ResetTupleHashIterator(node->hashtable, &node->hashiter);
            let hashtable = node
                .hashtable
                .as_mut()
                .expect("ExecReScanSetOp: hashtable is NULL");
            node.hashiter = execGrouping::reset_tuple_hash_iterator::call(hashtable);
            return Ok(());
        }

        // Release any hashtable storage.
        //   if (node->tableContext) MemoryContextReset(node->tableContext);
        if let Some(ctx) = node.tableContext.as_mut() {
            ctx.reset();
        }

        // And rebuild an empty hashtable.
        //   ResetTupleHashTable(node->hashtable);
        let hashtable = node
            .hashtable
            .as_mut()
            .expect("ExecReScanSetOp: hashtable is NULL");
        execGrouping::reset_tuple_hash_table::call(hashtable)?;
        node.table_filled = false;
    } else {
        // Need to re-read first input from each side.
        node.need_init = true;
    }

    // if chgParam of subnode is not null then plan will be re-scanned by first
    // ExecProcNode.
    //   if (outerPlan->chgParam == NULL) ExecReScan(outerPlan);
    if child_chgparam_is_null(node, Side::Left) {
        if let Some(outer) = node.ps.lefttree.as_deref_mut() {
            execAmi::exec_re_scan::call(outer, estate)?;
        }
    }
    //   if (innerPlan->chgParam == NULL) ExecReScan(innerPlan);
    if child_chgparam_is_null(node, Side::Right) {
        if let Some(inner) = node.ps.righttree.as_deref_mut() {
            execAmi::exec_re_scan::call(inner, estate)?;
        }
    }
    Ok(())
}

// ===========================================================================
// In-crate helpers.
// ===========================================================================

/// `(SetOp *) setopstate->ps.plan` — the plan node aliased by the state's `ps`.
/// The plan link is the shared, read-only `&'mcx Node`, so the returned borrow
/// is tied to `'mcx`, not to the `&setopstate` borrow.
fn setop_plan<'mcx>(setopstate: &SetOpStateData<'mcx>) -> &'mcx SetOp<'mcx> {
    match setopstate
        .ps
        .plan
        .expect("SetOpState.ps.plan is NULL (executor did not splice the plan link)")
    {
        types_nodes::nodes::Node::SetOp(s) => s,
        other => panic!(
            "SetOpState.ps.plan is not a SetOp node (tag {})",
            other.tag()
        ),
    }
}

/// `ExecProcNode(outerPlanState/innerPlanState(setopstate))` — pull the next
/// tuple from the given child.
fn exec_proc_child<'mcx>(
    setopstate: &mut SetOpStateData<'mcx>,
    side: Side,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let child = match side {
        Side::Left => setopstate.ps.lefttree.as_deref_mut(),
        Side::Right => setopstate.ps.righttree.as_deref_mut(),
    }
    .expect("exec_proc_child: child PlanState is NULL");
    execProcnode::exec_proc_node::call(child, estate)
}

/// `ExecClearTuple(node->ps.ps_ResultTupleSlot)`.
fn clear_result_tuple<'mcx>(
    node: &mut SetOpStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let slot = node
        .ps
        .ps_ResultTupleSlot
        .expect("clear_result_tuple: ps_ResultTupleSlot is NULL");
    execTuples::exec_clear_tuple::call(estate.slot_mut(slot))
}

/// `ResetExprContext(setopstate->ps.ps_ExprContext)` — reset the node's
/// per-tuple memory after each hashtable lookup.
fn reset_per_tuple_context<'mcx>(setopstate: &SetOpStateData<'mcx>, estate: &mut EStateData<'mcx>) {
    if let Some(ecxt) = setopstate.ps.ps_ExprContext {
        estate.ecxt_mut(ecxt).ecxt_per_tuple_memory.reset();
    }
}

/// `outerPlan->chgParam == NULL` / `innerPlan->chgParam == NULL`. A missing
/// child counts as "no params changed" (the recursion is simply skipped, as the
/// C `if (child)` guards do).
fn child_chgparam_is_null(node: &SetOpStateData<'_>, side: Side) -> bool {
    let child = match side {
        Side::Left => node.ps.lefttree.as_deref(),
        Side::Right => node.ps.righttree.as_deref(),
    };
    match child {
        Some(c) => c.ps_head().chgParam.is_none(),
        None => true,
    }
}

/// `&node->leftInput` / `&node->rightInput`.
fn input_ref<'a>(node: &'a SetOpStateData<'_>, side: Side) -> &'a SetOpStatePerInput {
    match side {
        Side::Left => &node.leftInput,
        Side::Right => &node.rightInput,
    }
}

/// `&mut node->leftInput` / `&mut node->rightInput`.
fn input_mut<'a, 'mcx>(
    node: &'a mut SetOpStateData<'mcx>,
    side: Side,
) -> &'a mut SetOpStatePerInput {
    match side {
        Side::Left => &mut node.leftInput,
        Side::Right => &mut node.rightInput,
    }
}

/// `TupIsNull(slot)` — true if `slot` is absent or its resolved arena slot is
/// marked empty (`TTS_FLAG_EMPTY`).
#[inline]
fn tup_is_null(slot: Option<SlotId>, estate: &EStateData<'_>) -> bool {
    match slot {
        None => true,
        Some(id) => estate.slot(id).is_empty(),
    }
}

/// The `Datum` and isnull of a deformed column (`tts_values[i]`/`tts_isnull[i]`).
/// A by-value column yields its scalar `Datum`; a by-reference column's
/// pointer-as-`Datum` is materialized by the slot layer, which is not yet
/// modeled, so it routes through the (still-provisional) slot payload owner.
fn deformed_datum(col: &(TupleValue<'_>, bool)) -> (Datum, bool) {
    let (value, isnull) = col;
    match value {
        TupleValue::ByVal(d) => (*d, *isnull),
        TupleValue::ByRef(_) => panic!(
            "backend-executor-execTuples: tts_values[] Datum for a by-reference column needs \
             the slot payload model (slot_getallattrs is provisional)"
        ),
    }
}

/// `ApplySortComparator(datum1, isNull1, datum2, isNull2, ssup)`
/// (utils/sortsupport.h) — the null/reverse arithmetic is inlined exactly as the
/// C macro; the comparator-function invocation goes through the sortsupport
/// seam.
fn apply_sort_comparator(
    datum1: Datum,
    is_null1: bool,
    datum2: Datum,
    is_null2: bool,
    ssup: &SortSupportData<'_>,
) -> PgResult<i32> {
    let nulls_first = ssup.ssup_nulls_first;
    let reverse = ssup.ssup_reverse;

    let compare = if is_null1 {
        if is_null2 {
            0 // NULL "=" NULL
        } else if nulls_first {
            -1 // NULL "<" NOT_NULL
        } else {
            1 // NULL ">" NOT_NULL
        }
    } else if is_null2 {
        if nulls_first {
            1 // NOT_NULL ">" NULL
        } else {
            -1 // NOT_NULL "<" NULL
        }
    } else {
        // compare = ssup->comparator(datum1, datum2, ssup);
        let mut compare = sortsupport::apply_sort_comparator::call(datum1, datum2, ssup)?;
        if reverse {
            // INVERT_COMPARE_RESULT(compare)
            compare = if compare < 0 { 1 } else { compare.wrapping_neg() };
        }
        compare
    };
    Ok(compare)
}

/// Read a `SetOpStatePerGroupData` out of a hash entry's additional bytes
/// (`TupleHashEntryGetAdditional`). The C stores the struct directly in that
/// MAXALIGN'd space; the two `int64`s are read in native byte order.
fn read_pergroup(additional: &[u8]) -> SetOpStatePerGroupData {
    let mut left = [0u8; 8];
    let mut right = [0u8; 8];
    left.copy_from_slice(&additional[0..8]);
    right.copy_from_slice(&additional[8..16]);
    SetOpStatePerGroupData {
        numLeft: i64::from_ne_bytes(left),
        numRight: i64::from_ne_bytes(right),
    }
}

/// Write a `SetOpStatePerGroupData` back into a hash entry's additional bytes.
fn write_pergroup(additional: &mut [u8], pergroup: &SetOpStatePerGroupData) {
    additional[0..8].copy_from_slice(&pergroup.numLeft.to_ne_bytes());
    additional[8..16].copy_from_slice(&pergroup.numRight.to_ne_bytes());
}

/// Copy a hash entry's stored `firstTuple` (`TupleHashEntryGetTuple`) into a new
/// owned `MinimalTuple` in `mcx` for storing into the result slot.
fn copy_minimal_tuple<'mcx>(
    src: &types_tuple::heaptuple::MinimalTuple<'mcx>,
    mcx: Mcx<'mcx>,
) -> types_tuple::heaptuple::MinimalTuple<'mcx> {
    match src {
        Some(m) => {
            let copied = m
                .clone_in(mcx)
                .expect("copy_minimal_tuple: cloning the hash entry tuple");
            Some(mcx::alloc_in(mcx, copied).expect("copy_minimal_tuple: boxing the clone"))
        }
        None => None,
    }
}

/// `elog(ERROR, fmt, ...)` — formatted internal-error text.
fn elog_error_fmt(message: alloc::string::String) -> PgError {
    PgError::error(message).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

extern crate alloc;

#[cfg(test)]
mod tests;
