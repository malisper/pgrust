//! Port of `src/backend/executor/nodeRecursiveunion.c` — routines to handle
//! `RecursiveUnion` plan nodes (`WITH RECURSIVE`).
//!
//! A `RecursiveUnion` node evaluates the non-recursive (outer) term once, then
//! repeatedly evaluates the recursive (inner) term against a *working table*
//! until the term produces no new rows. Each iteration's output is stashed in
//! an *intermediate table*, which then becomes the next working table. The
//! recursive term reads the working table through a `WorkTableScan` node that
//! finds this `RecursiveUnionState` via the reserved `wtParam` `Param` slot.
//! For `UNION` (without `ALL`) a tuple hash table of already-seen tuples filters
//! duplicates (the hash key is computed from the grouping columns).
//!
//! INTERFACE ROUTINES
//! - [`ExecInitRecursiveUnion`]   - initialize the node and its subplans.
//! - [`ExecRecursiveUnion`]       - the node's `ExecProcNode` body (next tuple).
//! - [`ExecEndRecursiveUnion`]    - shut down the subplans and free resources.
//! - [`ExecReScanRecursiveUnion`] - rescan the node.
//!
//! The whole state machine (`ExecRecursiveUnion`'s two-phase loop, the
//! `numCols > 0` hashing gate, the working/intermediate table swap, the
//! `recursing`/`intermediate_empty` bookkeeping, and the static helper
//! `build_hash_table`) is this crate's owned logic. Operations below the
//! executor-node layer go through the owners' seam crates:
//!
//! - interrupt servicing (`CHECK_FOR_INTERRUPTS`) → tcop/postgres;
//! - child dispatch / init / teardown / rescan (`ExecProcNode` / `ExecInitNode`
//!   / `ExecEndNode` / `ExecReScan`) → execProcnode / execAmi;
//! - the working / intermediate `tuplestore`s
//!   (`tuplestore_begin_heap` / `_puttupleslot` / `_clear` / `_end`) → sort/storage;
//! - the seen-tuples hash table (`execTuplesHashPrepare` / `BuildTupleHashTable`
//!   / `LookupTupleHashEntry` / `ResetTupleHashTable`) → execGrouping;
//! - result-type setup (`ExecInitResultTypeTL`) and the common child slot-ops
//!   (`ExecGetResultType` / `ExecGetCommonChildSlotOps`) → execTuples / execUtils;
//! - the `bms_add_member` bitmapset helper → nodes/bitmapset;
//! - the `work_mem` GUC → globals;
//! - the `wtParam` `Param`-slot deposit that publishes this state to descendant
//!   `WorkTableScan` nodes → nodeWorktablescan.
//!
//! `mcx::MemoryContext` owns its allocation domain and resets on drop, so the
//! C `AllocSetContextCreate` / `MemoryContextReset` / `MemoryContextDelete` of
//! `tempContext`/`tableContext` are native (`new_child` / `reset` / drop).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use execAmi_seams as execAmi;
use execGrouping_seams as execGrouping;
use execProcnode_seams as execProcnode;
use execTuples_seams as execTuples;
use execUtils_seams as execUtils;
use nodeWorktablescan as worktablescan;
use nodes_core_seams as bitmapset;
use postgres_seams as tcop_postgres;
use init_small_seams as globals;
use sort_storage_seams as tuplestore;

use mcx::PgBox;
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use ::nodes::execnodes::RecursiveUnionSharedState;
use ::nodes::executor::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};
use ::nodes::noderecursiveunion::{RecursiveUnion, RecursiveUnionStateData};
use ::nodes::nodes::Node;
use nodes::{EStateData, PlanStateNode, SlotId, TupleSlotKind};

/// Install this crate's seam implementations. nodeRecursiveunion owns no
/// inbound seams: it is reached through the executor dispatch (execProcnode),
/// which can depend on this crate directly without a cycle, so there is nothing
/// to install.
pub fn init_seams() {}

/// `TupIsNull(slot)` (tuptable.h): `((slot) == NULL || TTS_EMPTY(slot))`. The
/// `ExecProcNode` boundary returns the child's slot id, so `None` is the C
/// `NULL`; `Some(id)` resolves through the estate arena and tests `TTS_EMPTY`.
#[inline]
fn tup_is_null(slot: Option<SlotId>, estate: &EStateData<'_>) -> bool {
    match slot {
        None => true,
        Some(id) => estate.slot(id).is_empty(),
    }
}

/// `&mut es_recursive_shared[wtParam]` — the shared state this RecursiveUnion
/// published at init (`publish_wtparam_slot`), holding the working/intermediate
/// tuplestores and the `recursing`/`intermediate_empty` bookkeeping. In the
/// owned model these live in the `EState.es_recursive_shared` side-table (keyed
/// by `wtParam`) rather than on the node, so the descendant `WorkTableScan` can
/// reach the working table without aliasing this (self-borrowing) node.
fn shared_mut<'a, 'mcx>(
    wt_param: i32,
    estate: &'a mut EStateData<'mcx>,
) -> PgResult<&'a mut RecursiveUnionSharedState<'mcx>> {
    let idx = usize::try_from(wt_param)
        .map_err(|_| internal("RecursiveUnion: invalid wtParam"))?;
    estate
        .es_recursive_shared
        .get_mut(idx)
        .and_then(|s| s.as_mut())
        .ok_or_else(|| internal("RecursiveUnion: shared state not published"))
}

/// `elog(ERROR, ...)` with the default internal SQLSTATE.
fn internal(msg: &str) -> PgError {
    PgError::error(alloc::string::String::from(msg)).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

/// `(RecursiveUnion *) node->ps.plan` — recover the plan node the planner
/// produced from the state's plan back-link. Panics (the C cast can't fail) if
/// the link is missing or the wrong node type.
fn plan_of<'a, 'mcx>(node: &'a RecursiveUnionStateData<'mcx>) -> &'a RecursiveUnion<'mcx> {
    match node.ps.plan {
        Some(p) if p.is_recursiveunion() => p.expect_recursiveunion(),
        Some(other) => panic!(
            "ExecRecursiveUnion: node->ps.plan is not a RecursiveUnion: {:?}",
            other.tag()
        ),
        None => panic!("ExecRecursiveUnion: node->ps.plan is NULL"),
    }
}

// ===========================================================================
// build_hash_table — initialize the hash table to empty.
// ===========================================================================

/// ```c
/// static void
/// build_hash_table(RecursiveUnionState *rustate)
/// {
///     RecursiveUnion *node = (RecursiveUnion *) rustate->ps.plan;
///     TupleDesc   desc = ExecGetResultType(outerPlanState(rustate));
///
///     Assert(node->numCols > 0);
///     Assert(node->numGroups > 0);
///
///     rustate->hashtable = BuildTupleHashTable(&rustate->ps, desc,
///                 ExecGetCommonChildSlotOps(&rustate->ps),
///                 node->numCols, node->dupColIdx, rustate->eqfuncoids,
///                 rustate->hashfunctions, node->dupCollations,
///                 node->numGroups, 0, rustate->ps.state->es_query_cxt,
///                 rustate->tableContext, rustate->tempContext, false);
/// }
/// ```
fn build_hash_table<'mcx>(
    rustate: &mut RecursiveUnionStateData<'mcx>,
    plan: &RecursiveUnion<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(plan.numCols > 0);
    debug_assert!(plan.numGroups > 0);

    let mcx = estate.es_query_cxt;

    // desc = ExecGetResultType(outerPlanState(rustate));
    //
    // If both child plans deliver the same fixed tuple slot type, we can tell
    // BuildTupleHashTable to expect that slot type as input. Otherwise we'll
    // pass NULL denoting that any slot type is possible.
    let outer = rustate
        .ps
        .lefttree
        .as_deref()
        .expect("build_hash_table: outerPlanState is NULL");
    let desc: types_tuple::heaptuple::TupleDesc<'mcx> =
        match execTuples::exec_get_result_type::call(outer.ps_head()) {
            Some(d) => Some(mcx::alloc_in(mcx, d.clone_in(mcx)?)?),
            None => None,
        };

    // inputOps = ExecGetCommonChildSlotOps(&rustate->ps)
    let input_ops = execUtils::exec_get_common_child_slot_ops::call(&rustate.ps, estate)
        .unwrap_or(TupleSlotKind::MinimalTuple);

    // The table's bucket array (metacxt) lives in the per-query context
    // (es_query_cxt), entries in tableContext, temp work in tempContext. All
    // three contexts are caller-owned; the seam borrows them.
    let metacxt = mcx.context();
    let tablecxt = rustate
        .tableContext
        .as_ref()
        .expect("build_hash_table: tableContext is NULL");
    let tempcxt = rustate
        .tempContext
        .as_ref()
        .expect("build_hash_table: tempContext is NULL");

    let table = execGrouping::build_tuple_hash_table::call(
        mcx,
        None,
        desc,
        input_ops,
        plan.numCols,
        plan.dupColIdx.as_slice(),
        rustate.eqfuncoids.as_slice(),
        rustate.hashfunctions.as_slice(),
        plan.dupCollations.as_slice(),
        plan.numGroups,
        0,
        metacxt,
        tablecxt,
        tempcxt,
        false,
    )?;
    rustate.hashtable = Some(table);
    Ok(())
}

// ===========================================================================
// Node state machine (ported 1:1 from nodeRecursiveunion.c).
// ===========================================================================

/// The `PlanState.ExecProcNode` callback installed by
/// [`ExecInitRecursiveUnion`]: `castNode(RecursiveUnionState, pstate)` then run
/// [`ExecRecursiveUnion`], returning the result slot id (the C `return slot`) or
/// `None` (the C `return NULL`).
fn exec_recursive_union_node<'mcx>(
    pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        PlanStateNode::RecursiveUnion(node) => node,
        other => panic!("castNode(RecursiveUnionState, pstate) failed: {other:?}"),
    };
    ExecRecursiveUnion(node, estate)
}

/// `ExecRecursiveUnion(node)` — scans the recursive query sequentially and
/// returns the next qualifying tuple (`None` when the scan is exhausted).
///
/// 1. evaluate non-recursive term and assign the result to RT.
/// 2. execute recursive terms: WT := RT; while WT is not empty, replace the
///    recursive term's input with WT, evaluate it into the intermediate table,
///    append to RT, and loop. When WT is empty return RT.
pub fn ExecRecursiveUnion<'mcx>(
    node: &mut RecursiveUnionStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // RecursiveUnion *plan = (RecursiveUnion *) node->ps.plan;
    //
    // The plan back-link aliases the shared, read-only plan tree; recover it as
    // an owned copy of the fields the loop reads (numCols / wtParam) so the node
    // can be mutated freely below without aliasing the plan borrow.
    let (num_cols, wt_param) = {
        let plan = plan_of(node);
        (plan.numCols, plan.wtParam)
    };

    // CHECK_FOR_INTERRUPTS();
    tcop_postgres::check_for_interrupts::call()?;

    // The working/intermediate tuplestores and the recursing/intermediate_empty
    // bookkeeping live in `EState.es_recursive_shared[wtParam]` (hoisted off the
    // node at init so the descendant WorkTableScan can reach the working table).

    // 1. Evaluate non-recursive term
    //   if (!node->recursing)
    if !shared_mut(wt_param, estate)?.recursing {
        loop {
            // slot = ExecProcNode(outerPlan);
            let slot = {
                let outer = node
                    .ps
                    .lefttree
                    .as_deref_mut()
                    .expect("ExecRecursiveUnion: outerPlanState is NULL");
                execProcnode::exec_proc_node::call(outer, estate)?
            };
            // if (TupIsNull(slot)) break;
            if tup_is_null(slot, estate) {
                break;
            }
            let slot = slot.ok_or_else(|| {
                internal("ExecRecursiveUnion: ExecProcNode returned a null slot")
            })?;
            // if (plan->numCols > 0)
            if num_cols > 0 {
                // Find or build hashtable entry for this tuple's group
                // LookupTupleHashEntry(node->hashtable, slot, &isnew, NULL);
                let isnew = lookup_hash_entry(node, slot, estate)?;
                // Must reset temp context after each hashtable lookup
                // MemoryContextReset(node->tempContext);
                if let Some(temp) = node.tempContext.as_mut() {
                    temp.reset();
                }
                // Ignore tuple if already seen
                if !isnew {
                    continue;
                }
            }
            // Each non-duplicate tuple goes to the working table ...
            // tuplestore_puttupleslot(node->working_table, slot);
            put_into_working(wt_param, slot, estate)?;
            // ... and to the caller
            return Ok(Some(slot));
        }
        // node->recursing = true;
        shared_mut(wt_param, estate)?.recursing = true;
    }

    // 2. Execute recursive term
    loop {
        // slot = ExecProcNode(innerPlan);
        let slot = {
            let inner = node
                .ps
                .righttree
                .as_deref_mut()
                .expect("ExecRecursiveUnion: innerPlanState is NULL");
            execProcnode::exec_proc_node::call(inner, estate)?
        };
        // if (TupIsNull(slot))
        if tup_is_null(slot, estate) {
            // Done if there's nothing in the intermediate table
            // if (node->intermediate_empty) break;
            if shared_mut(wt_param, estate)?.intermediate_empty {
                break;
            }

            // Now we let the intermediate table become the work table. We need
            // a fresh intermediate table, so delete the tuples from the current
            // working table and use that as the new intermediate table. This
            // saves a round of free/malloc from creating a new tuple store.
            {
                let shared = shared_mut(wt_param, estate)?;
                // tuplestore_clear(node->working_table);
                {
                    let working = shared
                        .working_table
                        .as_deref_mut()
                        .ok_or_else(|| internal("ExecRecursiveUnion: working_table is NULL"))?;
                    tuplestore::tuplestore_clear::call(working);
                }
                // swaptemp = node->working_table;
                // node->working_table = node->intermediate_table;
                // node->intermediate_table = swaptemp;
                core::mem::swap(&mut shared.working_table, &mut shared.intermediate_table);
                // mark the intermediate table as empty
                // node->intermediate_empty = true;
                shared.intermediate_empty = true;
            }

            // reset the recursive term
            // innerPlan->chgParam = bms_add_member(innerPlan->chgParam,
            //                                      plan->wtParam);
            inner_chgparam_add_wtparam(node, wt_param, estate)?;

            // and continue fetching from recursive term
            continue;
        }

        let slot = slot.ok_or_else(|| {
            internal("ExecRecursiveUnion: ExecProcNode returned a null slot")
        })?;

        // if (plan->numCols > 0)
        if num_cols > 0 {
            // Find or build hashtable entry for this tuple's group
            // LookupTupleHashEntry(node->hashtable, slot, &isnew, NULL);
            let isnew = lookup_hash_entry(node, slot, estate)?;
            // Must reset temp context after each hashtable lookup
            // MemoryContextReset(node->tempContext);
            if let Some(temp) = node.tempContext.as_mut() {
                temp.reset();
            }
            // Ignore tuple if already seen
            if !isnew {
                continue;
            }
        }

        // Else, tuple is good; stash it in intermediate table ...
        // node->intermediate_empty = false;
        shared_mut(wt_param, estate)?.intermediate_empty = false;
        // tuplestore_puttupleslot(node->intermediate_table, slot);
        put_into_intermediate(wt_param, slot, estate)?;
        // ... and return it
        return Ok(Some(slot));
    }

    // return NULL;
    Ok(None)
}

/// `tuplestore_puttupleslot(node->working_table, slot)` over the side-table:
/// take the `working_table` PgBox out so the put can hold `&mut estate` without a
/// self-alias, then restore it (even on the error path).
fn put_into_working<'mcx>(
    wt_param: i32,
    slot: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mut working = shared_mut(wt_param, estate)?
        .working_table
        .take()
        .ok_or_else(|| internal("ExecRecursiveUnion: working_table is NULL"))?;
    let res = tuplestore::tuplestore_puttupleslot::call(&mut working, slot, estate);
    if let Ok(shared) = shared_mut(wt_param, estate) {
        shared.working_table = Some(working);
    }
    res
}

/// `tuplestore_puttupleslot(node->intermediate_table, slot)` over the side-table
/// (same take/put pattern as [`put_into_working`]).
fn put_into_intermediate<'mcx>(
    wt_param: i32,
    slot: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mut intermediate = shared_mut(wt_param, estate)?
        .intermediate_table
        .take()
        .ok_or_else(|| internal("ExecRecursiveUnion: intermediate_table is NULL"))?;
    let res = tuplestore::tuplestore_puttupleslot::call(&mut intermediate, slot, estate);
    if let Ok(shared) = shared_mut(wt_param, estate) {
        shared.intermediate_table = Some(intermediate);
    }
    res
}

/// `LookupTupleHashEntry(node->hashtable, slot, &isnew, NULL)` — find or build
/// the hashtable entry for this tuple's group; returns `isnew`. The entry
/// contents and its additional bytes are ignored here (the C passes the entry
/// through but only reads `isnew`).
fn lookup_hash_entry<'mcx>(
    node: &mut RecursiveUnionStateData<'mcx>,
    slot: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let hashtable = node
        .hashtable
        .as_deref_mut()
        .expect("ExecRecursiveUnion: hashtable is NULL");
    let (isnew, _hash) =
        execGrouping::lookup_tuple_hash_entry::call(hashtable, slot, estate, &mut |_, _| {})?;
    Ok(isnew)
}

/// `innerPlan->chgParam = bms_add_member(innerPlan->chgParam, plan->wtParam);`
/// — tell the recursive term it has to rescan because the working table changed.
fn inner_chgparam_add_wtparam<'mcx>(
    node: &mut RecursiveUnionStateData<'mcx>,
    wt_param: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let inner = node
        .ps
        .righttree
        .as_deref_mut()
        .expect("ExecRecursiveUnion: innerPlanState is NULL");
    let chg = inner.ps_head_mut().chgParam.take();
    let new = bitmapset::bms_add_member::call(mcx, chg, wt_param)?;
    inner.ps_head_mut().chgParam = Some(new);
    Ok(())
}

// ===========================================================================
// ExecInitRecursiveUnion
// ===========================================================================

/// `ExecInitRecursiveUnion(node, estate, eflags)` — create and initialize a
/// RecursiveUnion node.
///
/// Takes the enclosing plan-tree [`Node`] (the C `RecursiveUnion *` is the same
/// pointer, via struct embedding); the state's plan back-link aliases the
/// shared, read-only plan tree exactly as C's `rustate->ps.plan = (Plan *) node`
/// does. Panics if the node is not a `RecursiveUnion` (the C `castNode`).
pub fn ExecInitRecursiveUnion<'mcx>(
    node: &'mcx Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, RecursiveUnionStateData<'mcx>>> {
    let mcx = estate.es_query_cxt;

    let plan: &'mcx RecursiveUnion<'mcx> = match node.as_recursiveunion() {
        Some(ru) => ru,
        None => panic!("castNode(RecursiveUnion, node) failed: {node:?}"),
    };

    // check for unsupported flags
    // Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);

    // create state structure
    //   rustate = makeNode(RecursiveUnionState);
    //   rustate->ps.plan = (Plan *) node;
    //   rustate->ps.state = estate;
    //   rustate->ps.ExecProcNode = ExecRecursiveUnion;
    //   rustate->eqfuncoids = NULL; rustate->hashfunctions = NULL;
    //   rustate->hashtable = NULL; rustate->tempContext = NULL;
    //   rustate->tableContext = NULL;
    //   rustate->recursing = false; rustate->intermediate_empty = true;
    let mut rustate = RecursiveUnionStateData::alloc_in(mcx)?;
    rustate.ps.plan = Some(node);
    rustate.ps.ExecProcNode = Some(exec_recursive_union_node);

    // node->working_table = tuplestore_begin_heap(false, false, work_mem);
    // node->intermediate_table = tuplestore_begin_heap(false, false, work_mem);
    let work_mem = globals::work_mem::call();
    rustate.working_table = Some(tuplestore::tuplestore_begin_heap::call(
        mcx, false, false, work_mem,
    )?);
    rustate.intermediate_table = Some(tuplestore::tuplestore_begin_heap::call(
        mcx, false, false, work_mem,
    )?);

    // If hashing, we need a per-tuple memory context for comparisons, and a
    // longer-lived context to store the hash table. The table can't just be
    // kept in the per-query context because we want to be able to throw it away
    // when rescanning.
    if plan.numCols > 0 {
        // rustate->tempContext =
        //     AllocSetContextCreate(CurrentMemoryContext, "RecursiveUnion", ...);
        // rustate->tableContext =
        //     AllocSetContextCreate(CurrentMemoryContext,
        //                           "RecursiveUnion hash table", ...);
        rustate.tempContext = Some(mcx.context().new_child("RecursiveUnion"));
        rustate.tableContext = Some(mcx.context().new_child("RecursiveUnion hash table"));
    }

    // Make the state structure available to descendant WorkTableScan nodes via
    // the Param slot reserved for it.
    //   prmdata = &(estate->es_param_exec_vals[node->wtParam]);
    //   Assert(prmdata->execPlan == NULL);
    //   prmdata->value = PointerGetDatum(rustate);
    //   prmdata->isnull = false;
    worktablescan::publish_wtparam_slot(&mut rustate, estate, plan.wtParam)?;

    // Miscellaneous initialization
    //
    // RecursiveUnion plans don't have expression contexts because they never
    // call ExecQual or ExecProject.
    // Assert(node->plan.qual == NIL);
    debug_assert!(plan.plan.qual.is_none());

    // RecursiveUnion nodes still have Result slots, which hold pointers to
    // tuples, so we have to initialize them.
    // ExecInitResultTypeTL(&rustate->ps);
    execTuples::exec_init_result_type_tl::call(&mut rustate.ps, estate)?;

    // Publish the just-established result rowtype to the side-table so the
    // descendant WorkTableScan's deferred ExecAssignScanType (which reads
    // `ExecGetResultType(&rustate->ps)`) can recover it without aliasing this
    // node. (In C the published `rustate` pointer sees this desc set just
    // above; here the side-table holds an owned clone, captured now.)
    {
        let result_tupdesc: types_tuple::heaptuple::TupleDesc<'mcx> =
            match rustate.ps.ps_ResultTupleDesc.as_deref() {
                Some(td) => Some(mcx::alloc_in(mcx, td.clone_in(mcx)?)?),
                None => None,
            };
        shared_mut(plan.wtParam, estate)?.result_tupdesc = result_tupdesc;
    }

    // Initialize result tuple type. (Note: we have to set up the result type
    // before initializing child nodes, because nodeWorktablescan.c expects it
    // to be valid.)
    // rustate->ps.ps_ProjInfo = NULL;
    rustate.ps.ps_ProjInfo = None;

    // initialize child nodes
    // outerPlanState(rustate) = ExecInitNode(outerPlan(node), estate, eflags);
    // innerPlanState(rustate) = ExecInitNode(innerPlan(node), estate, eflags);
    let outer_plan = plan.plan.lefttree.as_deref();
    rustate.ps.lefttree = execProcnode::exec_init_node::call(mcx, outer_plan, estate, eflags)?;
    let inner_plan = plan.plan.righttree.as_deref();
    rustate.ps.righttree = execProcnode::exec_init_node::call(mcx, inner_plan, estate, eflags)?;

    // If hashing, precompute fmgr lookup data for inner loop, and create the
    // hash table.
    if plan.numCols > 0 {
        // execTuplesHashPrepare(node->numCols, node->dupOperators,
        //                       &rustate->eqfuncoids, &rustate->hashfunctions);
        let (eqfuncoids, hashfunctions) = execGrouping::exec_tuples_hash_prepare::call(
            mcx,
            plan.numCols,
            plan.dupOperators.as_slice(),
        )?;
        rustate.eqfuncoids = eqfuncoids;
        rustate.hashfunctions = hashfunctions;
        // build_hash_table(rustate);
        build_hash_table(&mut rustate, plan, estate)?;
    }

    Ok(rustate)
}

// ===========================================================================
// ExecEndRecursiveUnion
// ===========================================================================

/// `ExecEndRecursiveUnion(node)` — frees any storage allocated through C
/// routines.
///
/// ```c
/// void
/// ExecEndRecursiveUnion(RecursiveUnionState *node)
/// {
///     tuplestore_end(node->working_table);
///     tuplestore_end(node->intermediate_table);
///     if (node->tempContext) MemoryContextDelete(node->tempContext);
///     if (node->tableContext) MemoryContextDelete(node->tableContext);
///     ExecEndNode(outerPlanState(node));
///     ExecEndNode(innerPlanState(node));
/// }
/// ```
pub fn ExecEndRecursiveUnion<'mcx>(
    node: &mut RecursiveUnionStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let wt_param = plan_of(node).wtParam;

    // Release tuplestores. They live in the `es_recursive_shared[wtParam]`
    // side-table (hoisted off the node at init); take them out and end them.
    if let Ok(shared) = shared_mut(wt_param, estate) {
        if let Some(working) = shared.working_table.take() {
            tuplestore::tuplestore_end::call(working);
        }
        if let Some(intermediate) = shared.intermediate_table.take() {
            tuplestore::tuplestore_end::call(intermediate);
        }
    }

    // free subsidiary stuff including hashtable
    //
    // MemoryContextDelete is the owned context's drop; taking the Option drops
    // it (and the hash table living in tableContext along with it).
    node.tempContext.take();
    node.tableContext.take();
    node.hashtable.take();

    // close down subplans
    if let Some(outer) = node.ps.lefttree.as_deref_mut() {
        execProcnode::exec_end_node::call(outer, estate)?;
    }
    if let Some(inner) = node.ps.righttree.as_deref_mut() {
        execProcnode::exec_end_node::call(inner, estate)?;
    }
    Ok(())
}

// ===========================================================================
// ExecReScanRecursiveUnion
// ===========================================================================

/// `ExecReScanRecursiveUnion(node)` — rescans the relation.
///
/// ```c
/// void
/// ExecReScanRecursiveUnion(RecursiveUnionState *node)
/// {
///     PlanState  *outerPlan = outerPlanState(node);
///     PlanState  *innerPlan = innerPlanState(node);
///     RecursiveUnion *plan = (RecursiveUnion *) node->ps.plan;
///
///     innerPlan->chgParam = bms_add_member(innerPlan->chgParam, plan->wtParam);
///     if (outerPlan->chgParam == NULL) ExecReScan(outerPlan);
///     if (node->tableContext) MemoryContextReset(node->tableContext);
///     if (plan->numCols > 0) ResetTupleHashTable(node->hashtable);
///     node->recursing = false;
///     node->intermediate_empty = true;
///     tuplestore_clear(node->working_table);
///     tuplestore_clear(node->intermediate_table);
/// }
/// ```
pub fn ExecReScanRecursiveUnion<'mcx>(
    node: &mut RecursiveUnionStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let (num_cols, wt_param) = {
        let plan = plan_of(node);
        (plan.numCols, plan.wtParam)
    };

    // Set recursive term's chgParam to tell it that we'll modify the working
    // table and therefore it has to rescan.
    // innerPlan->chgParam = bms_add_member(innerPlan->chgParam, plan->wtParam);
    inner_chgparam_add_wtparam(node, wt_param, estate)?;

    // if chgParam of subnode is not null then plan will be re-scanned by first
    // ExecProcNode. Because of above, we only have to do this to the
    // non-recursive term.
    // if (outerPlan->chgParam == NULL) ExecReScan(outerPlan);
    let outer_chgparam_is_null = node
        .ps
        .lefttree
        .as_deref()
        .expect("ExecReScanRecursiveUnion: outerPlanState is NULL")
        .ps_head()
        .chgParam
        .is_none();
    if outer_chgparam_is_null {
        let outer = node
            .ps
            .lefttree
            .as_deref_mut()
            .expect("ExecReScanRecursiveUnion: outerPlanState is NULL");
        execAmi::exec_re_scan::call(outer, estate)?;
    }

    // Release any hashtable storage
    // if (node->tableContext) MemoryContextReset(node->tableContext);
    if let Some(table_ctx) = node.tableContext.as_mut() {
        table_ctx.reset();
    }

    // Empty hashtable if needed
    // if (plan->numCols > 0) ResetTupleHashTable(node->hashtable);
    if num_cols > 0 {
        let hashtable = node
            .hashtable
            .as_deref_mut()
            .expect("ExecReScanRecursiveUnion: hashtable is NULL");
        execGrouping::reset_tuple_hash_table::call(hashtable)?;
    }

    // reset processing state (the bookkeeping + tuplestores live in the
    // `es_recursive_shared[wtParam]` side-table).
    let shared = shared_mut(wt_param, estate)?;
    node.recursing = false;
    node.intermediate_empty = true;
    shared.recursing = false;
    shared.intermediate_empty = true;
    // tuplestore_clear(node->working_table);
    if let Some(working) = shared.working_table.as_deref_mut() {
        tuplestore::tuplestore_clear::call(working);
    }
    // tuplestore_clear(node->intermediate_table);
    if let Some(intermediate) = shared.intermediate_table.as_deref_mut() {
        tuplestore::tuplestore_clear::call(intermediate);
    }
    Ok(())
}
