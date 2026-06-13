//! Port of `src/backend/executor/nodeMaterial.c` — routines to handle
//! materialization nodes.
//!
//! INTERFACE ROUTINES
//! - [`ExecMaterial`]       - materialize the result of a subplan
//! - [`ExecInitMaterial`]   - initialize node and subnodes
//! - [`ExecEndMaterial`]    - shutdown node and subnodes
//!
//! The node state machine is held as an owned [`MaterialState`] mutated
//! through `&mut` borrows; the C `PlanState.state` back-pointer is replaced by
//! threading `&mut EStateData` explicitly. `ExecMaterial` returns `Ok(true)`
//! when a tuple is available in `node.ss.ps.ps_ResultTupleSlot` (the C
//! `return slot`) and `Ok(false)` when there is none (the C `return NULL` /
//! `return ExecClearTuple(slot)`).
//!
//! Calls into unported owners (tuplestore.c, execProcnode.c, execAmi.c,
//! execTuples.c, execUtils.c, tcop/postgres.c's `ProcessInterrupts`,
//! globals.c's `work_mem`) go through those owners' seam crates and panic
//! until the owners land.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use backend_executor_execAmi_seams as execAmi;
use backend_executor_execProcnode_seams as execProcnode;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils;
use backend_tcop_postgres_seams as tcop_postgres;
use backend_utils_init_small_seams as globals;
use backend_utils_sort_storage_seams as tuplestore;
use mcx::{alloc_in, PgBox};
use types_error::PgResult;
use types_nodes::execnodes::ScanDirectionIsForward;
use types_nodes::{
    EStateData, Material, MaterialState, PlanStateNode, SlotId, TupleSlotKind,
};

/// `EXEC_FLAG_REWIND` (executor.h) — expect rescan.
const EXEC_FLAG_REWIND: i32 = 0x0004;
/// `EXEC_FLAG_BACKWARD` (executor.h) — need backward scan.
const EXEC_FLAG_BACKWARD: i32 = 0x0008;
/// `EXEC_FLAG_MARK` (executor.h) — need mark/restore.
const EXEC_FLAG_MARK: i32 = 0x0010;

/// Install this crate's implementations into its seam slots.
///
/// nodeMaterial has no `<unit>-seams` crate: callers that will need these
/// functions (execProcnode's dispatch tables) can depend on this crate
/// directly without a cycle, since this crate reaches outward only through
/// per-owner seam crates.
pub fn init_seams() {}

/// `ExecMaterial(pstate)` — materialize the result of a subplan.
///
/// As long as we are at the end of the data collected in the tuplestore, we
/// collect one new row from the subplan on each call, and stash it aside in
/// the tuplestore before returning it. The tuplestore is only read if we are
/// asked to scan backwards, rescan, or mark/restore.
///
/// Returns `Ok(true)` when a tuple is available in
/// `node.ss.ps.ps_ResultTupleSlot`, `Ok(false)` when there is none.
///
/// Allocation (the lazily created tuplestore) happens in
/// `estate.es_query_cxt`, the C `CurrentMemoryContext` while the executor
/// runs.
pub fn ExecMaterial<'mcx>(
    node: &mut MaterialState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    tcop_postgres::check_for_interrupts::call()?;

    // get state info from node
    let dir = estate.es_direction;
    let forward = ScanDirectionIsForward(dir);

    // If first time through, and we need a tuplestore, initialize it.
    if node.tuplestorestate.is_none() && node.eflags != 0 {
        let mut tuplestorestate = tuplestore::tuplestore_begin_heap::call(
            estate.es_query_cxt,
            true,
            false,
            globals::work_mem::call(),
        )?;
        tuplestore::tuplestore_set_eflags::call(&mut tuplestorestate, node.eflags)?;
        if node.eflags & EXEC_FLAG_MARK != 0 {
            // Allocate a second read pointer to serve as the mark. We know it
            // must have index 1, so needn't store that.
            let ptrno =
                tuplestore::tuplestore_alloc_read_pointer::call(&mut tuplestorestate, node.eflags)?;
            debug_assert_eq!(ptrno, 1);
        }
        node.tuplestorestate = Some(tuplestorestate);
    }

    // If we are not at the end of the tuplestore, or are going backwards, try
    // to fetch a tuple from tuplestore.
    let mut eof_tuplestore = match node.tuplestorestate.as_deref() {
        None => true,
        Some(ts) => tuplestore::tuplestore_ateof::call(ts),
    };

    if !forward && eof_tuplestore {
        if !node.eof_underlying {
            // When reversing direction at tuplestore EOF, the first
            // gettupleslot call will fetch the last-added tuple; but we want
            // to return the one before that, if possible. So do an extra
            // fetch.
            //
            // (The C dereferences tuplestorestate unguarded here: a backward
            // scan implies node->eflags included EXEC_FLAG_BACKWARD, so the
            // store was created above.)
            let ts = node
                .tuplestorestate
                .as_deref_mut()
                .expect("ExecMaterial: backward scan with no tuplestore");
            if !tuplestore::tuplestore_advance::call(ts, forward)? {
                return Ok(false); // the tuplestore must be empty
            }
        }
        eof_tuplestore = false;
    }

    // If we can fetch another tuple from the tuplestore, return it.
    let slot = node
        .ss
        .ps
        .ps_ResultTupleSlot
        .expect("ExecMaterial: ps_ResultTupleSlot not initialized");
    if !eof_tuplestore {
        let ts = node
            .tuplestorestate
            .as_deref_mut()
            .expect("ExecMaterial: reading with no tuplestore");
        if tuplestore::tuplestore_gettupleslot::call(ts, forward, false, estate.slot_mut(slot))? {
            return Ok(true);
        }
        if forward {
            eof_tuplestore = true;
        }
    }

    // If necessary, try to fetch another row from the subplan.
    //
    // Note: the eof_underlying state variable exists to short-circuit further
    // subplan calls. It's not optional, unfortunately, because some plan node
    // types are not robust about being called again when they've already
    // returned NULL.
    if eof_tuplestore && !node.eof_underlying {
        // We can only get here with forward==true, so no need to worry about
        // which direction the subplan will go.
        let outerNode = node
            .ss
            .ps
            .lefttree
            .as_deref_mut()
            .expect("ExecMaterial: no outer plan state");
        let outerslot = execProcnode::exec_proc_node::call(outerNode, estate)?;
        // TupIsNull(outerslot) — NULL or empty.
        let outerslot = match outerslot {
            Some(id) if !estate.slot(id).is_empty() => id,
            _ => {
                node.eof_underlying = true;
                return Ok(false);
            }
        };

        // Append a copy of the returned tuple to tuplestore. NOTE: because
        // the tuplestore is certainly in EOF state, its read position will
        // move forward over the added tuple. This is what we want.
        if let Some(ts) = node.tuplestorestate.as_deref_mut() {
            tuplestore::tuplestore_puttupleslot::call(ts, estate.slot(outerslot))?;
        }

        // ExecCopySlot(slot, outerslot); return slot;
        let mcx = estate.es_query_cxt;
        let (dst, src) = estate.slot_pair_mut(slot, outerslot);
        execTuples::exec_copy_slot::call(mcx, dst, src)?;
        return Ok(true);
    }

    // Nothing left ...
    execTuples::exec_clear_tuple::call(estate.slot_mut(slot))?;
    Ok(false)
}

/// The `PlanState.ExecProcNode` callback installed by [`ExecInitMaterial`]:
/// `castNode(MaterialState, pstate)` then run [`ExecMaterial`], returning the
/// result slot's id (the C `return slot`) or `None` (the C `return NULL` /
/// the cleared slot).
fn exec_material_node<'mcx>(
    pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        PlanStateNode::Material(node) => node,
        other => panic!("castNode(MaterialState, pstate) failed: {other:?}"),
    };
    if ExecMaterial(node, estate)? {
        Ok(node.ss.ps.ps_ResultTupleSlot)
    } else {
        Ok(None)
    }
}

/// `ExecInitMaterial(node, estate, eflags)`.
///
/// The state tree is allocated in `estate.es_query_cxt` (C: `makeNode` in the
/// per-query context current during `ExecInitNode`), so initialization is
/// fallible on OOM.
pub fn ExecInitMaterial<'mcx>(
    node: &Material<'_>,
    estate: &mut EStateData<'mcx>,
    mut eflags: i32,
) -> PgResult<PgBox<'mcx, MaterialState<'mcx>>> {
    let mcx = estate.es_query_cxt;

    // create state structure
    //
    // makeNode(MaterialState); matstate->ss.ps.plan = (Plan *) node;
    // matstate->ss.ps.state = estate; matstate->ss.ps.ExecProcNode = ExecMaterial;
    //
    // The plan back-link holds an owned copy of the (read-only at execution
    // time) plan node; the EState back-link is the threaded `estate`
    // parameter.
    let mut matstate = alloc_in(mcx, MaterialState::default())?;
    matstate.ss.ps.plan = Some(alloc_in(
        mcx,
        types_nodes::nodes::Node::Material(node.clone_in(mcx)?),
    )?);
    matstate.ss.ps.ExecProcNode = Some(exec_material_node);

    // We must have a tuplestore buffering the subplan output to do backward
    // scan or mark/restore. We also prefer to materialize the subplan output
    // if we might be called on to rewind and replay it many times. However,
    // if none of these cases apply, we can skip storing the data.
    matstate.eflags = eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);

    // Tuplestore's interpretation of the flag bits is subtly different from
    // the general executor meaning: it doesn't think BACKWARD necessarily
    // means "backwards all the way to start". If told to support BACKWARD we
    // must include REWIND in the tuplestore eflags, else tuplestore_trim
    // might throw away too much.
    if eflags & EXEC_FLAG_BACKWARD != 0 {
        matstate.eflags |= EXEC_FLAG_REWIND;
    }

    matstate.eof_underlying = false;
    matstate.tuplestorestate = None;

    // Miscellaneous initialization
    //
    // Materialization nodes don't need ExprContexts because they never call
    // ExecQual or ExecProject.

    // initialize child nodes
    //
    // We shield the child node from the need to support REWIND, BACKWARD, or
    // MARK/RESTORE.
    eflags &= !(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);

    // outerPlan = outerPlan(node);
    // outerPlanState(matstate) = ExecInitNode(outerPlan, estate, eflags);
    let outerPlan = node.plan.lefttree.as_deref();
    matstate.ss.ps.lefttree = execProcnode::exec_init_node::call(mcx, outerPlan, estate, eflags)?;

    // Initialize result type and slot. No need to initialize projection info
    // because this node doesn't do projections.
    //
    // material nodes only return tuples from their materialized relation.
    execTuples::exec_init_result_tuple_slot_tl::call(
        &mut matstate.ss.ps,
        estate,
        TupleSlotKind::MinimalTuple,
    )?;
    matstate.ss.ps.ps_ProjInfo = None;

    // initialize tuple type.
    execUtils::exec_create_scan_slot_from_outer_plan::call(
        estate,
        &mut matstate.ss,
        TupleSlotKind::MinimalTuple,
    )?;

    Ok(matstate)
}

/// `ExecEndMaterial(node)`.
pub fn ExecEndMaterial<'mcx>(
    node: &mut MaterialState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Release tuplestore resources
    if let Some(tuplestorestate) = node.tuplestorestate.take() {
        tuplestore::tuplestore_end::call(tuplestorestate);
    }

    // shut down the subplan
    let outer = node
        .ss
        .ps
        .lefttree
        .as_deref_mut()
        .expect("ExecEndMaterial: no outer plan state");
    execProcnode::exec_end_node::call(outer, estate)
}

/// `ExecMaterialMarkPos(node)` — calls tuplestore to save the current position
/// in the stored file.
pub fn ExecMaterialMarkPos(node: &mut MaterialState<'_>) -> PgResult<()> {
    debug_assert!(node.eflags & EXEC_FLAG_MARK != 0);

    // if we haven't materialized yet, just return.
    let Some(ts) = node.tuplestorestate.as_deref_mut() else {
        return Ok(());
    };

    // copy the active read pointer to the mark.
    tuplestore::tuplestore_copy_read_pointer::call(ts, 0, 1)?;

    // since we may have advanced the mark, try to truncate the tuplestore.
    tuplestore::tuplestore_trim::call(ts);
    Ok(())
}

/// `ExecMaterialRestrPos(node)` — calls tuplestore to restore the last saved
/// file position.
pub fn ExecMaterialRestrPos(node: &mut MaterialState<'_>) -> PgResult<()> {
    debug_assert!(node.eflags & EXEC_FLAG_MARK != 0);

    // if we haven't materialized yet, just return.
    let Some(ts) = node.tuplestorestate.as_deref_mut() else {
        return Ok(());
    };

    // copy the mark to the active read pointer.
    tuplestore::tuplestore_copy_read_pointer::call(ts, 1, 0)
}

/// `ExecReScanMaterial(node)` — rescans the materialized relation.
pub fn ExecReScanMaterial<'mcx>(
    node: &mut MaterialState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    let slot = node
        .ss
        .ps
        .ps_ResultTupleSlot
        .expect("ExecReScanMaterial: ps_ResultTupleSlot not initialized");
    execTuples::exec_clear_tuple::call(estate.slot_mut(slot))?;

    if node.eflags != 0 {
        // If we haven't materialized yet, just return. If outerplan's
        // chgParam is not NULL then it will be re-scanned by ExecProcNode,
        // else no reason to re-scan it at all.
        if node.tuplestorestate.is_none() {
            return Ok(());
        }

        // If subnode is to be rescanned then we forget previous stored
        // results; we have to re-read the subplan and re-store. Also, if we
        // told tuplestore it needn't support rescan, we lose and must
        // re-read. (This last should not happen in common cases; else our
        // caller lied by not passing EXEC_FLAG_REWIND to us.)
        //
        // Otherwise we can just rewind and rescan the stored output. The
        // state of the subnode does not change.
        //
        // PlanState *outerPlan = outerPlanState(node); the C dereferences it
        // unguarded — ExecInitMaterial splices the outer child before any
        // rescan.
        let outer_chgparam_is_null = node
            .ss
            .ps
            .lefttree
            .as_deref()
            .expect("ExecReScanMaterial: no outer plan state")
            .ps_head()
            .chgParam
            .is_none();

        if !outer_chgparam_is_null || (node.eflags & EXEC_FLAG_REWIND) == 0 {
            // tuplestore_end(node->tuplestorestate);
            // node->tuplestorestate = NULL;
            tuplestore::tuplestore_end::call(
                node.tuplestorestate
                    .take()
                    .expect("checked Some above"),
            );
            if outer_chgparam_is_null {
                let outer = node
                    .ss
                    .ps
                    .lefttree
                    .as_deref_mut()
                    .expect("ExecReScanMaterial: no outer plan state");
                execAmi::exec_re_scan::call(outer, estate)?;
            }
            node.eof_underlying = false;
        } else {
            let ts = node
                .tuplestorestate
                .as_deref_mut()
                .expect("checked Some above");
            tuplestore::tuplestore_rescan::call(ts)?;
        }
    } else {
        // In this case we are just passing on the subquery's output

        // if chgParam of subnode is not null then plan will be re-scanned by
        // first ExecProcNode.
        let outer = node
            .ss
            .ps
            .lefttree
            .as_deref_mut()
            .expect("ExecReScanMaterial: no outer plan state");
        if outer.ps_head().chgParam.is_none() {
            execAmi::exec_re_scan::call(outer, estate)?;
        }
        node.eof_underlying = false;
    }

    Ok(())
}

#[cfg(test)]
mod tests;
