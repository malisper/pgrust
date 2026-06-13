//! Port of `src/backend/executor/nodeSetOp.c` — routines to handle INTERSECT
//! and EXCEPT selection.
//!
//! INTERFACE ROUTINES
//! - [`ExecInitSetOp`]   - initialize the node and its subnodes
//! - [`ExecSetOp`]       - retrieve the next output tuple (the `ExecProcNode`
//!   body)
//! - [`ExecEndSetOp`]    - shut down the node and its subnodes
//! - [`ExecReScanSetOp`] - rescan the node
//!
//! In `SETOP_SORTED` mode the node merge-joins its two pre-sorted inputs on the
//! grouping columns, counting per-group duplicates. In `SETOP_HASHED` mode the
//! outer relation is read into a hash table (one entry per group), the inner
//! relation is counted against it, then the table is scanned to emit output.
//! SetOp does no qual checking nor projection — output tuples are copies of the
//! first-to-arrive tuple in each input group.
//!
//! The node state machine is held as an owned [`SetOpStateData`] mutated
//! through `&mut` borrows; the C `PlanState.state` back-pointer is replaced by
//! threading `&mut EStateData` explicitly. `ExecSetOp` returns the produced
//! tuple's [`SlotId`] (the C `return ps_ResultTupleSlot`) or `None`
//! (end-of-scan).
//!
//! Calls into unported owners (execProcnode.c child dispatch, execGrouping.c
//! tuple hash table, execTuples.c slot ops, execUtils.c expr-context/result
//! type, sortsupport.c comparator setup/dispatch, execAmi.c `ExecReScan`,
//! tcop/postgres.c `CHECK_FOR_INTERRUPTS`) go through those owners' seam crates
//! and panic until the owners land.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use backend_executor_execAmi_seams as execAmi;
use backend_executor_execGrouping_seams as execGrouping;
use backend_executor_execProcnode_seams as execProcnode;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils;
use backend_tcop_postgres_seams as tcop_postgres;
use backend_utils_sort_sortsupport_seams as sortsupport;

use mcx::{alloc_in, vec_with_capacity_in, Mcx, MemoryContext, PgBox};
use types_core::primitive::AttrNumber;
use types_datum::Datum;
use types_error::PgResult;
use types_nodes::execnodes::{EStateData, PlanStateData, SlotId};
use types_nodes::executor::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, EXEC_FLAG_REWIND};
use types_nodes::nodes::Node;
use types_nodes::nodesetop::{
    SetOp, SetOpCmd, SetOpStateData, SetOpStatePerGroupData, SetOpStatePerInput, SetOpStrategy,
};
use types_nodes::{PlanStateNode, TupleSlotKind};
use types_sortsupport::SortSupportData;

/// Install this crate's implementations into its seam slots.
///
/// nodeSetOp has no `<unit>-seams` crate: the only crate that will call into
/// it across a cycle is execProcnode's dispatch table, which dispatches
/// through the `PlanState.ExecProcNode` callback stored on the node, not a
/// named seam. Consumers that need `ExecInitSetOp` / `ExecEndSetOp` /
/// `ExecReScanSetOp` (execProcnode / execAmi) can depend on this crate
/// directly, since this crate reaches outward only through per-owner seam
/// crates.
pub fn init_seams() {}

// ===========================================================================
// Side selector for the two merge inputs (outer = left, inner = right).
// ===========================================================================

/// Which input a per-input operation addresses: the outer (left) plan or the
/// inner (right) plan. The C reaches them via `outerPlanState`/`innerPlanState`
/// (`node->ps.lefttree`/`righttree`) and `&node->leftInput`/`&node->rightInput`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    Left,
    Right,
}

// ===========================================================================
// Node state machine (ported 1:1 from nodeSetOp.c).
// ===========================================================================

/// `build_hash_table(setopstate)` — initialize the hash table to empty.
///
/// `additionalsize = sizeof(SetOpStatePerGroupData)` is fixed by this node
/// type; `metacxt = es_query_cxt`, `tablecxt = tableContext`, `tempcxt =
/// econtext->ecxt_per_tuple_memory`.
fn build_hash_table<'mcx>(
    setopstate: &mut SetOpStateData<'mcx>,
    plan: &SetOp<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(plan.strategy == SetOpStrategy::Hashed);
    debug_assert!(plan.numGroups > 0);

    // desc = ExecGetResultType(outerPlanState(setopstate));
    let outer = setopstate
        .ps
        .lefttree
        .as_deref()
        .expect("build_hash_table: no outer plan state");
    let desc = execTuples::exec_get_result_type::call(outer.ps_head())
        .expect("build_hash_table: outer plan has no result type");

    // inputOps = ExecGetCommonChildSlotOps(&setopstate->ps)
    let input_ops = execUtils::exec_get_common_child_slot_ops::call(&setopstate.ps);

    // econtext->ecxt_per_tuple_memory — the temp context for hashing.
    let econtext = setopstate
        .ps
        .ps_ExprContext
        .expect("build_hash_table: ps_ExprContext not created");

    let metacxt = estate.es_query_cxt;

    let tablecxt = setopstate
        .tableContext
        .as_ref()
        .expect("build_hash_table: tableContext not created");
    let tempcxt = &estate.ecxt(econtext).ecxt_per_tuple_memory;

    let hashtable = execGrouping::build_tuple_hash_table::call(
        &setopstate.ps,
        desc,
        input_ops,
        plan.numCols,
        &plan.cmpColIdx,
        &setopstate.eqfuncoids,
        &setopstate.hashfunctions,
        &plan.cmpCollations,
        plan.numGroups,
        core::mem::size_of::<SetOpStatePerGroupData>(),
        metacxt,
        tablecxt,
        tempcxt,
        false,
    )?;
    setopstate.hashtable = hashtable;
    Ok(())
}

/// `set_output_count(setopstate, pergroup)` — decide how many copies (if any)
/// of the group's representative row to emit and store it into `numOutput`.
/// Straight from the SQL92 specification.
fn set_output_count(
    setopstate: &mut SetOpStateData<'_>,
    plan: &SetOp<'_>,
    pergroup: &SetOpStatePerGroupData,
) -> PgResult<()> {
    let num_left = pergroup.numLeft;
    let num_right = pergroup.numRight;

    setopstate.numOutput = match plan.cmd {
        SetOpCmd::Intersect => {
            if num_left > 0 && num_right > 0 {
                1
            } else {
                0
            }
        }
        SetOpCmd::IntersectAll => {
            if num_left < num_right {
                num_left
            } else {
                num_right
            }
        }
        SetOpCmd::Except => {
            if num_left > 0 && num_right == 0 {
                1
            } else {
                0
            }
        }
        SetOpCmd::ExceptAll => {
            if num_left < num_right {
                0
            } else {
                num_left - num_right
            }
        }
    };
    Ok(())
}

/// `ExecSetOp(pstate)` — the node's `ExecProcNode` body. Returns the produced
/// tuple's slot id, or `None` for end-of-scan.
pub fn ExecSetOp<'mcx>(
    node: &mut SetOpStateData<'mcx>,
    plan: &SetOp<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    tcop_postgres::check_for_interrupts::call()?;

    // If the previously-returned tuple needs to be returned more than once,
    // keep returning it.
    if node.numOutput > 0 {
        node.numOutput -= 1;
        return Ok(node.ps.ps_ResultTupleSlot);
    }

    // Otherwise, we're done if we are out of groups.
    if node.setop_done {
        return Ok(None);
    }

    // Fetch the next tuple group according to the correct strategy.
    if plan.strategy == SetOpStrategy::Hashed {
        if !node.table_filled {
            setop_fill_hash_table(node, plan, estate)?;
        }
        setop_retrieve_hash_table(node, plan, estate)
    } else {
        setop_retrieve_sorted(node, plan, estate)
    }
}

/// `setop_retrieve_sorted(setopstate)` — `ExecSetOp` for the non-hashed case.
fn setop_retrieve_sorted<'mcx>(
    setopstate: &mut SetOpStateData<'mcx>,
    plan: &SetOp<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // If first time through, establish the invariant that setop_load_group
    // expects: each side's nextTupleSlot is the next output from the child
    // plan, or empty if there is no more output from it.
    if setopstate.need_init {
        setopstate.need_init = false;

        // setopstate->leftInput.nextTupleSlot = ExecProcNode(outerPlan);
        let left_next = exec_child(setopstate, Side::Left, estate)?;
        setopstate.leftInput.nextTupleSlot = left_next;

        // If the outer relation is empty, then we will emit nothing, and we
        // don't need to read the inner relation at all.
        if tup_is_null(left_next, estate) {
            setopstate.setop_done = true;
            return Ok(None);
        }

        // setopstate->rightInput.nextTupleSlot = ExecProcNode(innerPlan);
        setopstate.rightInput.nextTupleSlot = exec_child(setopstate, Side::Right, estate)?;

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
            let s1 = setopstate
                .leftInput
                .firstTupleSlot
                .expect("setop_retrieve_sorted: left firstTupleSlot not set");
            let s2 = setopstate
                .rightInput
                .firstTupleSlot
                .expect("setop_retrieve_sorted: right firstTupleSlot not set");
            setop_compare_slots(s1, s2, setopstate, estate)?
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
        // copies of the result tuple, and if so return the first copy. (The
        // result tuple is the left input's firstTuple slot.)
        set_output_count(setopstate, plan, &pergroup)?;

        if setopstate.numOutput > 0 {
            setopstate.numOutput -= 1;
            return Ok(setopstate.ps.ps_ResultTupleSlot);
        }
    }

    // No more groups.
    let slot = setopstate
        .ps
        .ps_ResultTupleSlot
        .expect("setop_retrieve_sorted: ps_ResultTupleSlot not initialized");
    execTuples::exec_clear_tuple::call(estate.slot_mut(slot))?;
    Ok(None)
}

/// `setop_load_group(input, inputPlan, setopstate)` — load the next group of
/// tuples from one child plan.
///
/// On entry, the first tuple of the next group (if any) is already in
/// `input->nextTupleSlot`. This invariant is maintained on exit.
fn setop_load_group<'mcx>(
    setopstate: &mut SetOpStateData<'mcx>,
    side: Side,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    input_mut(setopstate, side).needGroup = false;

    // If we've exhausted this child plan, report an empty group.
    let next = input_ref(setopstate, side).nextTupleSlot;
    if tup_is_null(next, estate) {
        // ExecClearTuple(input->firstTupleSlot);
        let first = input_ref(setopstate, side)
            .firstTupleSlot
            .expect("setop_load_group: firstTupleSlot not set");
        execTuples::exec_clear_tuple::call(estate.slot_mut(first))?;
        input_mut(setopstate, side).numTuples = 0;
        return Ok(());
    }

    // Make a local copy of the first tuple for comparisons.
    //   ExecStoreMinimalTuple(ExecCopySlotMinimalTuple(input->nextTupleSlot),
    //                         input->firstTupleSlot, true);
    let next = next.expect("setop_load_group: nextTupleSlot not set");
    let first = input_ref(setopstate, side)
        .firstTupleSlot
        .expect("setop_load_group: firstTupleSlot not set");
    let mcx = estate.es_query_cxt;
    let (dst, src) = estate.slot_pair_mut(first, next);
    execTuples::exec_copy_slot::call(mcx, dst, src)?;
    // and count it
    input_mut(setopstate, side).numTuples = 1;

    // Scan till we find the end-of-group.
    loop {
        // Get next input tuple, if there is one.
        //   input->nextTupleSlot = ExecProcNode(inputPlan);
        let next = exec_child(setopstate, side, estate)?;
        input_mut(setopstate, side).nextTupleSlot = next;
        if tup_is_null(next, estate) {
            break;
        }

        // There is; does it belong to same group as firstTuple?
        let first = input_ref(setopstate, side)
            .firstTupleSlot
            .expect("setop_load_group: firstTupleSlot not set");
        let next = next.expect("setop_load_group: nextTupleSlot not set");
        let cmpresult = setop_compare_slots(first, next, setopstate, estate)?;
        debug_assert!(cmpresult <= 0); // else input is mis-sorted
        if cmpresult != 0 {
            break;
        }

        // Still in same group, so count this tuple.
        input_mut(setopstate, side).numTuples += 1;
    }
    Ok(())
}

/// `setop_compare_slots(s1, s2, setopstate)` — compare the tuples in the two
/// given slots over the grouping columns.
///
/// The C `slot_getallattrs(s1); slot_getallattrs(s2);` materialization plus the
/// `s->tts_values[attno-1]` / `s->tts_isnull[attno-1]` reads are reached
/// through the slot owner's `slot_getattr` seam (the slot payload model lives
/// in execTuples). The per-key `ApplySortComparator` null/reverse arithmetic
/// and the loop are this node's own logic.
fn setop_compare_slots<'mcx>(
    s1: SlotId,
    s2: SlotId,
    setopstate: &SetOpStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<i32> {
    for nkey in 0..setopstate.numCols as usize {
        let sort_key = &setopstate.sortKeys[nkey];
        let attno = sort_key.ssup_attno;
        // datum1 = s1->tts_values[attno-1]; isNull1 = s1->tts_isnull[attno-1];
        let (datum1, is_null1) =
            execTuples::slot_getattr::call(estate.slot(s1), attno as AttrNumber)?;
        let (datum2, is_null2) =
            execTuples::slot_getattr::call(estate.slot(s2), attno as AttrNumber)?;

        let compare = apply_sort_comparator(datum1, is_null1, datum2, is_null2, sort_key)?;
        if compare != 0 {
            return Ok(compare);
        }
    }
    Ok(0)
}

/// `ApplySortComparator(datum1, isNull1, datum2, isNull2, ssup)`
/// (utils/sortsupport.h, inline): null/reverse comparison arithmetic. The
/// non-null comparator invocation `ssup->comparator(...)` dispatches through
/// the sortsupport owner's seam.
fn apply_sort_comparator(
    datum1: Datum,
    is_null1: bool,
    datum2: Datum,
    is_null2: bool,
    ssup: &SortSupportData<'_>,
) -> PgResult<i32> {
    let compare = if is_null1 {
        if is_null2 {
            0 // NULL "=" NULL
        } else if ssup.ssup_nulls_first {
            -1 // NULL "<" NOT_NULL
        } else {
            1 // NULL ">" NOT_NULL
        }
    } else if is_null2 {
        if ssup.ssup_nulls_first {
            1 // NOT_NULL ">" NULL
        } else {
            -1 // NOT_NULL "<" NULL
        }
    } else {
        // compare = ssup->comparator(datum1, datum2, ssup);
        let mut compare = sortsupport::apply_sort_comparator::call(datum1, datum2, ssup)?;
        if ssup.ssup_reverse {
            // INVERT_COMPARE_RESULT(compare)
            compare = -compare;
        }
        compare
    };
    Ok(compare)
}

/// `setop_fill_hash_table(setopstate)` — `ExecSetOp` for the hashed case,
/// phase 1: read inputs and build the hash table.
fn setop_fill_hash_table<'mcx>(
    setopstate: &mut SetOpStateData<'mcx>,
    _plan: &SetOp<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let econtext = setopstate
        .ps
        .ps_ExprContext
        .expect("setop_fill_hash_table: ps_ExprContext not created");
    let mut have_tuples = false;

    // Process each outer-plan tuple, and then fetch the next one, until we
    // exhaust the outer plan.
    loop {
        //   outerslot = ExecProcNode(outerPlan);
        let outerslot = exec_child(setopstate, Side::Left, estate)?;
        // TupIsNull(outerslot)
        let outerslot = match outerslot {
            Some(id) if !estate.slot(id).is_empty() => id,
            _ => break,
        };
        have_tuples = true;

        // Find or build hashtable entry for this tuple's group.
        //   entry = LookupTupleHashEntry(hashtable, outerslot, &isnew, NULL);
        let (entry, isnew) = execGrouping::lookup_tuple_hash_entry::call(
            &mut setopstate.hashtable,
            estate.slot(outerslot),
            true,
        )?
        .expect("LookupTupleHashEntry failed to create an entry");

        //   pergroup = TupleHashEntryGetAdditional(hashtable, entry);
        //   if (isnew) { pergroup->numLeft = 0; pergroup->numRight = 0; }
        let mut pergroup = if isnew {
            SetOpStatePerGroupData {
                numLeft: 0,
                numRight: 0,
            }
        } else {
            execGrouping::tuple_hash_entry_get_additional::call(&setopstate.hashtable, entry)?
        };

        // Advance the counts.
        //   pergroup->numLeft++;
        pergroup.numLeft += 1;
        execGrouping::tuple_hash_entry_set_additional::call(
            &mut setopstate.hashtable,
            entry,
            pergroup,
        )?;

        // Must reset expression context after each hashtable lookup.
        //   ResetExprContext(econtext);
        estate.ecxt_mut(econtext).ecxt_per_tuple_memory.reset();
    }

    // If the outer relation is empty, then we will emit nothing, and we don't
    // need to read the inner relation at all.
    if have_tuples {
        // Process each inner-plan tuple, until we exhaust the inner plan.
        loop {
            //   innerslot = ExecProcNode(innerPlan);
            let innerslot = exec_child(setopstate, Side::Right, estate)?;
            let innerslot = match innerslot {
                Some(id) if !estate.slot(id).is_empty() => id,
                _ => break,
            };

            // For tuples not seen previously, do not make hashtable entry.
            //   entry = LookupTupleHashEntry(hashtable, innerslot, NULL, NULL);
            let entry = execGrouping::lookup_tuple_hash_entry::call(
                &mut setopstate.hashtable,
                estate.slot(innerslot),
                false,
            )?;

            // Advance the counts if entry is already present.
            //   if (entry) { pergroup->numRight++; }
            if let Some((entry, _isnew)) = entry {
                let mut pergroup = execGrouping::tuple_hash_entry_get_additional::call(
                    &setopstate.hashtable,
                    entry,
                )?;
                pergroup.numRight += 1;
                execGrouping::tuple_hash_entry_set_additional::call(
                    &mut setopstate.hashtable,
                    entry,
                    pergroup,
                )?;
            }

            // Must reset expression context after each hashtable lookup.
            estate.ecxt_mut(econtext).ecxt_per_tuple_memory.reset();
        }
    }

    setopstate.table_filled = true;
    // Initialize to walk the hash table.
    //   ResetTupleHashIterator(setopstate->hashtable, &setopstate->hashiter);
    reset_tuple_hash_iterator(setopstate)?;
    Ok(())
}

/// `setop_retrieve_hash_table(setopstate)` — `ExecSetOp` for the hashed case,
/// phase 2: retrieve groups from the hash table.
fn setop_retrieve_hash_table<'mcx>(
    setopstate: &mut SetOpStateData<'mcx>,
    plan: &SetOp<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // We loop retrieving groups until we find one we should return.
    while !setopstate.setop_done {
        tcop_postgres::check_for_interrupts::call()?;

        // Find the next entry in the hash table.
        //   entry = ScanTupleHashTable(hashtable, &setopstate->hashiter);
        let entry = execGrouping::scan_tuple_hash_table::call(
            &mut setopstate.hashtable,
            &mut setopstate.hashiter,
        )?;
        let entry = match entry {
            Some(e) => e,
            None => {
                // No more entries in hashtable, so done.
                setopstate.setop_done = true;
                return Ok(None);
            }
        };

        // See if we should emit any copies of this tuple, and if so return the
        // first copy.
        //   pergroup = TupleHashEntryGetAdditional(hashtable, entry);
        let pergroup =
            execGrouping::tuple_hash_entry_get_additional::call(&setopstate.hashtable, entry)?;
        set_output_count(setopstate, plan, &pergroup)?;

        if setopstate.numOutput > 0 {
            setopstate.numOutput -= 1;
            //   return ExecStoreMinimalTuple(TupleHashEntryGetTuple(entry),
            //                                resultTupleSlot, false);
            let slot = setopstate
                .ps
                .ps_ResultTupleSlot
                .expect("setop_retrieve_hash_table: ps_ResultTupleSlot not initialized");
            execGrouping::store_hash_entry_tuple::call(&setopstate.hashtable, entry, estate, slot)?;
            return Ok(setopstate.ps.ps_ResultTupleSlot);
        }
    }

    // No more groups.
    let slot = setopstate
        .ps
        .ps_ResultTupleSlot
        .expect("setop_retrieve_hash_table: ps_ResultTupleSlot not initialized");
    execTuples::exec_clear_tuple::call(estate.slot_mut(slot))?;
    Ok(None)
}

/// `ExecInitSetOp(node, estate, eflags)` — initialize the setop node state
/// structures and the node's subplan.
///
/// The state tree is allocated in `estate.es_query_cxt` (C: `makeNode` in the
/// per-query context current during `ExecInitNode`), so initialization is
/// fallible on OOM. Takes the enclosing plan-tree [`Node`]; the state's plan
/// back-link aliases the shared, read-only plan tree. Panics if the node is not
/// a `SetOp` (the C `castNode`).
pub fn ExecInitSetOp<'mcx>(
    node: &'mcx Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    mut eflags: i32,
) -> PgResult<PgBox<'mcx, SetOpStateData<'mcx>>> {
    let mcx = estate.es_query_cxt;

    let plan: &'mcx SetOp<'mcx> = match node {
        Node::SetOp(s) => s,
        other => panic!("castNode(SetOp, node) failed: {other:?}"),
    };

    // check for unsupported flags
    //   Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
    debug_assert!(
        eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0,
        "SetOp does not support EXEC_FLAG_BACKWARD/EXEC_FLAG_MARK"
    );

    // create state structure: makeNode(SetOpState)
    let mut setopstate = alloc_in(mcx, empty_setop_state(mcx))?;
    setopstate.ps.plan = Some(node);
    setopstate.ps.ExecProcNode = Some(exec_setop_node);

    setopstate.setop_done = false;
    setopstate.numOutput = 0;
    setopstate.numCols = plan.numCols;
    setopstate.need_init = true;

    // create expression context
    //   ExecAssignExprContext(estate, &setopstate->ps);
    execUtils::exec_assign_expr_context::call(estate, &mut setopstate.ps)?;

    // If hashing, we also need a longer-lived context to store the hash table.
    //   setopstate->tableContext = AllocSetContextCreate(CurrentMemoryContext,
    //       "SetOp hash table", ALLOCSET_DEFAULT_SIZES);
    if plan.strategy == SetOpStrategy::Hashed {
        setopstate.tableContext = Some(MemoryContext::new("SetOp hash table"));
    }

    // initialize child nodes
    //
    // If we are hashing then the child plans do not need to handle REWIND
    // efficiently; see ExecReScanSetOp.
    if plan.strategy == SetOpStrategy::Hashed {
        eflags &= !EXEC_FLAG_REWIND;
    }
    //   outerPlanState(setopstate) = ExecInitNode(outerPlan(node), estate, eflags);
    let outer_plan = node.outer_plan();
    setopstate.ps.lefttree =
        execProcnode::exec_init_node::call(mcx, outer_plan, estate, eflags)?;
    //   innerPlanState(setopstate) = ExecInitNode(innerPlan(node), estate, eflags);
    let inner_plan = node.inner_plan();
    setopstate.ps.righttree =
        execProcnode::exec_init_node::call(mcx, inner_plan, estate, eflags)?;

    // Initialize locally-allocated slots. In hashed mode, we just need a result
    // slot. In sorted mode, we need one first-tuple-of-group slot for each
    // input; we use the result slot for the left input's slot and create
    // another for the right input.
    //   ExecInitResultTupleSlotTL(&setopstate->ps, &TTSOpsMinimalTuple);
    execTuples::exec_init_result_tuple_slot_tl::call(
        &mut setopstate.ps,
        estate,
        TupleSlotKind::MinimalTuple,
    )?;
    if plan.strategy != SetOpStrategy::Hashed {
        //   setopstate->leftInput.firstTupleSlot = setopstate->ps.ps_ResultTupleSlot;
        setopstate.leftInput.firstTupleSlot = setopstate.ps.ps_ResultTupleSlot;
        //   setopstate->rightInput.firstTupleSlot =
        //       ExecInitExtraTupleSlot(estate, ps_ResultTupleDesc, &TTSOpsMinimalTuple);
        let tupdesc_data = setopstate
            .ps
            .ps_ResultTupleDesc
            .as_deref()
            .expect("ExecInitSetOp: ps_ResultTupleDesc not set")
            .clone_in(mcx)?;
        let tupdesc = Some(alloc_in(mcx, tupdesc_data)?);
        let right_first = execTuples::exec_init_extra_tuple_slot::call(
            estate,
            tupdesc,
            TupleSlotKind::MinimalTuple,
        )?;
        setopstate.rightInput.firstTupleSlot = Some(right_first);
    }

    // Setop nodes do no projections.
    //   setopstate->ps.ps_ProjInfo = NULL;
    setopstate.ps.ps_ProjInfo = None;

    // Precompute fmgr lookup data for inner loop. We need equality and hashing
    // functions to do it by hashing, while for sorting we need SortSupport data.
    if plan.strategy == SetOpStrategy::Hashed {
        //   execTuplesHashPrepare(node->numCols, node->cmpOperators,
        //                         &setopstate->eqfuncoids, &setopstate->hashfunctions);
        let (eqfuncoids, hashfunctions) = execGrouping::exec_tuples_hash_prepare::call(
            mcx,
            plan.numCols,
            &plan.cmpOperators,
        )?;
        setopstate.eqfuncoids = eqfuncoids;
        setopstate.hashfunctions = hashfunctions;
    } else {
        //   nkeys = node->numCols;
        //   setopstate->sortKeys = palloc0(nkeys * sizeof(SortSupportData));
        let nkeys = plan.numCols as usize;
        let mut sort_keys = vec_with_capacity_in(mcx, nkeys)?;
        for i in 0..nkeys {
            //   sortKey->ssup_cxt = CurrentMemoryContext;
            //   sortKey->ssup_collation = node->cmpCollations[i];
            //   sortKey->ssup_nulls_first = node->cmpNullsFirst[i];
            //   sortKey->ssup_attno = node->cmpColIdx[i];
            //   sortKey->abbreviate = false;
            let mut sort_key = SortSupportData::new(mcx);
            sort_key.ssup_collation = plan.cmpCollations[i];
            sort_key.ssup_nulls_first = plan.cmpNullsFirst[i];
            sort_key.ssup_attno = plan.cmpColIdx[i];
            sort_key.abbreviate = false;

            //   PrepareSortSupportFromOrderingOp(node->cmpOperators[i], sortKey);
            sortsupport::prepare_sort_support_from_ordering_op::call(
                plan.cmpOperators[i],
                &mut sort_key,
            )?;
            sort_keys.push(sort_key);
        }
        setopstate.sortKeys = sort_keys;
    }

    // Create a hash table if needed.
    if plan.strategy == SetOpStrategy::Hashed {
        build_hash_table(&mut setopstate, plan, estate)?;
        setopstate.table_filled = false;
    }

    Ok(setopstate)
}

/// `ExecEndSetOp(node)` — shut down the subplans and free resources allocated
/// to this node.
pub fn ExecEndSetOp<'mcx>(
    node: &mut SetOpStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // free subsidiary stuff including hashtable
    //   if (node->tableContext) MemoryContextDelete(node->tableContext);
    // Dropping the owned context is MemoryContextDelete.
    node.tableContext = None;

    //   ExecEndNode(outerPlanState(node));
    let outer = node
        .ps
        .lefttree
        .as_deref_mut()
        .expect("ExecEndSetOp: no outer plan state");
    execProcnode::exec_end_node::call(outer, estate)?;
    //   ExecEndNode(innerPlanState(node));
    let inner = node
        .ps
        .righttree
        .as_deref_mut()
        .expect("ExecEndSetOp: no inner plan state");
    execProcnode::exec_end_node::call(inner, estate)?;
    Ok(())
}

/// `ExecReScanSetOp(node)` — rescan the node.
pub fn ExecReScanSetOp<'mcx>(
    node: &mut SetOpStateData<'mcx>,
    plan: &SetOp<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    //   ExecClearTuple(node->ps.ps_ResultTupleSlot);
    let slot = node
        .ps
        .ps_ResultTupleSlot
        .expect("ExecReScanSetOp: ps_ResultTupleSlot not initialized");
    execTuples::exec_clear_tuple::call(estate.slot_mut(slot))?;
    node.setop_done = false;
    node.numOutput = 0;

    if plan.strategy == SetOpStrategy::Hashed {
        // In the hashed case, if we haven't yet built the hash table then we can
        // just return; nothing done yet, so nothing to undo.
        if !node.table_filled {
            return Ok(());
        }

        // If we do have the hash table and the subplans do not have any
        // parameter changes, then we can just rescan the existing hash table.
        //   if (outerPlan->chgParam == NULL && innerPlan->chgParam == NULL)
        if child_chgparam_is_null(node, Side::Left) && child_chgparam_is_null(node, Side::Right) {
            //   ResetTupleHashIterator(node->hashtable, &node->hashiter);
            reset_tuple_hash_iterator(node)?;
            return Ok(());
        }

        // Release any hashtable storage.
        //   if (node->tableContext) MemoryContextReset(node->tableContext);
        if let Some(ctx) = node.tableContext.as_mut() {
            ctx.reset();
        }

        // And rebuild an empty hashtable.
        //   ResetTupleHashTable(node->hashtable);
        execGrouping::reset_tuple_hash_table::call(&mut node.hashtable)?;
        node.table_filled = false;
    } else {
        // Need to re-read first input from each side.
        node.need_init = true;
    }

    // if chgParam of subnode is not null then plan will be re-scanned by first
    // ExecProcNode.
    //   if (outerPlan->chgParam == NULL) ExecReScan(outerPlan);
    if child_chgparam_is_null(node, Side::Left) {
        let outer = node
            .ps
            .lefttree
            .as_deref_mut()
            .expect("ExecReScanSetOp: no outer plan state");
        execAmi::exec_re_scan::call(outer, estate)?;
    }
    //   if (innerPlan->chgParam == NULL) ExecReScan(innerPlan);
    if child_chgparam_is_null(node, Side::Right) {
        let inner = node
            .ps
            .righttree
            .as_deref_mut()
            .expect("ExecReScanSetOp: no inner plan state");
        execAmi::exec_re_scan::call(inner, estate)?;
    }
    Ok(())
}

// ===========================================================================
// In-crate helpers.
// ===========================================================================

/// The `PlanState.ExecProcNode` callback installed by [`ExecInitSetOp`]:
/// `castNode(SetOpState, pstate)` then run [`ExecSetOp`].
fn exec_setop_node<'mcx>(
    pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // SAFETY-free split: the SetOp plan node aliases the shared, read-only plan
    // tree the state's `ps.plan` points at; we re-borrow it as the `SetOp`
    // (castNode) without touching the mutable node state during the call.
    let node = match pstate {
        PlanStateNode::SetOp(node) => node,
        other => panic!("castNode(SetOpState, pstate) failed: {other:?}"),
    };
    let plan: &SetOp = match node.ps.plan {
        Some(Node::SetOp(s)) => s,
        _ => panic!("SetOpState.ps.plan is not a SetOp node"),
    };
    ExecSetOp(node, plan, estate)
}

/// `ExecProcNode(outerPlanState(node))` / `ExecProcNode(innerPlanState(node))`:
/// dispatch into the child plan on the given side.
fn exec_child<'mcx>(
    setopstate: &mut SetOpStateData<'mcx>,
    side: Side,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let child = match side {
        Side::Left => setopstate.ps.lefttree.as_deref_mut(),
        Side::Right => setopstate.ps.righttree.as_deref_mut(),
    }
    .expect("exec_child: child plan state not initialized");
    execProcnode::exec_proc_node::call(child, estate)
}

/// `ResetTupleHashIterator(setopstate->hashtable, &setopstate->hashiter)`.
fn reset_tuple_hash_iterator(setopstate: &mut SetOpStateData<'_>) -> PgResult<()> {
    let SetOpStateData {
        hashtable,
        hashiter,
        ..
    } = setopstate;
    execGrouping::reset_tuple_hash_iterator::call(hashtable, hashiter)
}

/// `outerPlan->chgParam == NULL` / `innerPlan->chgParam == NULL` — true when
/// the child on the given side has no pending parameter changes.
fn child_chgparam_is_null(setopstate: &SetOpStateData<'_>, side: Side) -> bool {
    let child = match side {
        Side::Left => setopstate.ps.lefttree.as_deref(),
        Side::Right => setopstate.ps.righttree.as_deref(),
    }
    .expect("child_chgparam_is_null: child plan state not initialized");
    child.ps_head().chgParam.is_none()
}

/// `TupIsNull(slot)` — true if the slot is absent or its resolved slot is
/// marked empty (`TTS_EMPTY`).
#[inline]
fn tup_is_null(slot: Option<SlotId>, estate: &EStateData<'_>) -> bool {
    match slot {
        None => true,
        Some(id) => estate.slot(id).is_empty(),
    }
}

/// `&node->leftInput` / `&node->rightInput`.
fn input_ref<'a, 'mcx>(
    node: &'a SetOpStateData<'mcx>,
    side: Side,
) -> &'a SetOpStatePerInput {
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

/// `makeNode(SetOpState)` — a zeroed `SetOpState` with its fields at their
/// zero/empty values; `ExecInitSetOp` fills the rest.
fn empty_setop_state<'mcx>(mcx: Mcx<'mcx>) -> SetOpStateData<'mcx> {
    SetOpStateData {
        ps: PlanStateData::default(),
        setop_done: false,
        numOutput: 0,
        numCols: 0,
        sortKeys: mcx::PgVec::new_in(mcx),
        leftInput: SetOpStatePerInput::default(),
        rightInput: SetOpStatePerInput::default(),
        need_init: false,
        eqfuncoids: mcx::PgVec::new_in(mcx),
        hashfunctions: mcx::PgVec::new_in(mcx),
        hashtable: types_execgrouping::TupleHashTable::default(),
        tableContext: None,
        table_filled: false,
        hashiter: types_execgrouping::TupleHashIterator::default(),
    }
}

#[cfg(test)]
mod tests;
