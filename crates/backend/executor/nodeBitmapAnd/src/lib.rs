//! Port of `src/backend/executor/nodeBitmapAnd.c` — routines to handle
//! `BitmapAnd` nodes.
//!
//! `BitmapAnd` nodes don't make use of their left and right subtrees; rather
//! they maintain a list of subplans, much like `Append` nodes. The logic is
//! much simpler than `Append`, however, since we needn't cope with
//! forward/backward execution.
//!
//! INTERFACE ROUTINES
//! - [`ExecInitBitmapAnd`]   - initialize the `BitmapAnd` node
//! - [`MultiExecBitmapAnd`]  - retrieve the result bitmap from the node
//! - [`ExecEndBitmapAnd`]    - shut down the `BitmapAnd` node
//! - [`ExecReScanBitmapAnd`] - rescan the `BitmapAnd` node
//! - [`ExecBitmapAnd`]       - pro-forma `ExecProcNode` stub (always errors)
//!
//! Calls into other crates go through their owners' seam crates and panic until
//! the owner lands: `ExecInitNode`/`MultiExecProcNode`/`ExecEndNode`
//! (execProcnode.c), `ExecReScan` (execAmi.c), `UpdateChangedParamSet`
//! (execUtils.c), `InstrStartNode`/`InstrStopNode` (instrument.c). The
//! `tbm_intersect`/`tbm_is_empty`/`tbm_free` access-method calls go through the
//! tidbitmap owner's seam crate.
//!
//! `backend-executor-execProcnode` / `-execAmi` call back into these node
//! routines (a real cycle), so the four interface routines are also declared in
//! `backend-executor-nodeBitmapAnd-seams` and installed by [`init_seams`].

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use execAmi_seams as execAmi;
use execProcnode_seams as execProcnode;
use execUtils_seams as execUtils;
use instrument_seams as instrument;
use nodeBitmapAnd_seams as self_seams;
use core_tidbitmap_seams as tidbitmap;
use mcx::{Mcx, PgBox};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use ::nodes::executor::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};
use ::nodes::nodebitmapand::{BitmapAnd, BitmapAndState};
use ::nodes::nodes::Node;
use nodes::{EStateData, PlanStateNode, SlotId};
use ::tidbitmap::TIDBitmap;

/// Install this crate's interface routines into its seam slots so the executor
/// dispatch crates (`execProcnode.c` / `execAmi.c`) can reach them across the
/// cycle.
pub fn init_seams() {
    self_seams::exec_init_bitmap_and::set(ExecInitBitmapAnd);
    self_seams::multi_exec_bitmap_and::set(MultiExecBitmapAnd);
    self_seams::exec_end_bitmap_and::set(ExecEndBitmapAnd);
    self_seams::exec_rescan_bitmap_and::set(ExecReScanBitmapAnd);
}

/// `ExecBitmapAnd(pstate)` — stub for pro forma compliance.
///
/// ```c
/// static TupleTableSlot *
/// ExecBitmapAnd(PlanState *pstate)
/// {
///     elog(ERROR, "BitmapAnd node does not support ExecProcNode call convention");
///     return NULL;
/// }
/// ```
///
/// Installed as the node's `ExecProcNode` callback so a wrong `ExecProcNode`
/// dispatch is reported rather than silently mis-executed.
fn ExecBitmapAnd<'mcx>(
    _pstate: &mut PlanStateNode<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    Err(elog_error(
        "BitmapAnd node does not support ExecProcNode call convention",
    ))
}

/// `ExecInitBitmapAnd(node, estate, eflags)` — begin all of the subscans of the
/// `BitmapAnd` node.
///
/// 1:1 with `BitmapAndState *ExecInitBitmapAnd(BitmapAnd *node, EState *estate, int eflags)`.
pub fn ExecInitBitmapAnd<'mcx>(
    mcx: Mcx<'mcx>,
    node: &'mcx Node<'mcx>,
    bitmap_and: &'mcx BitmapAnd<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, PlanStateNode<'mcx>>> {
    // check for unsupported flags
    //   Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
    debug_assert!(
        eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0,
        "BitmapAnd does not support EXEC_FLAG_BACKWARD/EXEC_FLAG_MARK"
    );

    // BitmapAndState *bitmapandstate = makeNode(BitmapAndState);
    let mut bitmapandstate = mcx::alloc_in(mcx, BitmapAndState::new_in(mcx))?;

    // Set up empty vector of subplan states.
    //   nplans = list_length(node->bitmapplans);
    //   bitmapplanstates = (PlanState **) palloc0(nplans * sizeof(PlanState *));
    let nplans = i32::try_from(bitmap_and.bitmapplans.len())
        .map_err(|_| elog_error("BitmapAnd has too many input plans"))?;

    // create new BitmapAndState for our BitmapAnd node
    //   bitmapandstate->ps.plan = (Plan *) node;
    //   bitmapandstate->ps.state = estate;
    //   bitmapandstate->ps.ExecProcNode = ExecBitmapAnd;
    bitmapandstate.ps.plan = Some(node);
    bitmapandstate.ps.ExecProcNode = Some(ExecBitmapAnd);
    bitmapandstate.nplans = nplans;

    // call ExecInitNode on each of the plans to be executed and save the
    // results into the array "bitmapplanstates".
    //   i = 0;
    //   foreach(l, node->bitmapplans) {
    //       initNode = (Plan *) lfirst(l);
    //       bitmapplanstates[i] = ExecInitNode(initNode, estate, eflags);
    //       i++;
    //   }
    for init_node in bitmap_and.bitmapplans.iter() {
        let child = execProcnode::exec_init_node::call(mcx, Some(init_node), estate, eflags)?;
        bitmapandstate.bitmapplans.push(child);
    }

    // Miscellaneous initialization
    //
    // BitmapAnd plans don't have expression contexts because they never call
    // ExecQual or ExecProject.  They don't need any tuple slots either.

    //   return bitmapandstate;
    mcx::alloc_in(mcx, PlanStateNode::BitmapAnd(bitmapandstate))
}

/// `MultiExecBitmapAnd(node)` — retrieve the result bitmap from the node.
///
/// 1:1 with `Node *MultiExecBitmapAnd(BitmapAndState *node)`. Returns the AND of
/// every child bitmap. The first child becomes the running `result`; each
/// subsequent child is intersected into it and then freed. If at any stage the
/// running bitmap is empty, the loop falls out early.
pub fn MultiExecBitmapAnd<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, TIDBitmap>> {
    let node = as_bitmap_and_state(node)?;

    // must provide our own instrumentation support
    //   if (node->ps.instrument) InstrStartNode(node->ps.instrument);
    if let Some(instr) = node.ps.instrument.as_deref_mut() {
        instrument::instr_start_node::call(instr)?;
    }

    // get information from the node
    //   bitmapplans = node->bitmapplans;
    //   nplans = node->nplans;
    let nplans = node.nplans as usize;

    // Scan all the subplans and AND their result bitmaps.
    //   TIDBitmap *result = NULL;
    let mut result: Option<PgBox<'mcx, TIDBitmap>> = None;

    for i in 0..nplans {
        // PlanState *subnode = bitmapplans[i];
        let subnode = node
            .bitmapplans
            .get_mut(i)
            .and_then(|slot| slot.as_deref_mut())
            .ok_or_else(|| elog_error("BitmapAnd child plan state is missing"))?;

        //   subresult = (TIDBitmap *) MultiExecProcNode(subnode);
        //   if (!subresult || !IsA(subresult, TIDBitmap))
        //       elog(ERROR, "unrecognized result from subplan");
        // The recursion seam returns the child's real `TIDBitmap`, so the C
        // `IsA(subresult, TIDBitmap)` tag check is satisfied by the type system.
        let mut subresult = execProcnode::multi_exec_proc_node::call(subnode, estate)?;

        match result.as_deref_mut() {
            // if (result == NULL) result = subresult; /* first subplan */
            None => result = Some(subresult),
            Some(result_ref) => {
                //   tbm_intersect(result, subresult);
                //   tbm_free(subresult);
                tidbitmap::tbm_intersect::call(result_ref, &subresult)?;
                tidbitmap::tbm_free::call(&mut subresult);
            }
        }

        // If at any stage we have a completely empty bitmap, we can fall out
        // without evaluating the remaining subplans, since ANDing them can no
        // longer change the result.  (Note: the fact that indxpath.c orders the
        // subplans by selectivity should make this case more likely to occur.)
        //   if (tbm_is_empty(result)) break;
        let result_ref = result.as_deref().ok_or_else(|| {
            elog_error("MultiExecBitmapAnd: result bitmap not set after the first subplan")
        })?;
        if tidbitmap::tbm_is_empty::call(result_ref)? {
            break;
        }
    }

    //   if (result == NULL) elog(ERROR, "BitmapAnd doesn't support zero inputs");
    let result = result.ok_or_else(|| elog_error("BitmapAnd doesn't support zero inputs"))?;

    // must provide our own instrumentation support
    //   if (node->ps.instrument) InstrStopNode(node->ps.instrument, 0);
    if let Some(instr) = node.ps.instrument.as_deref_mut() {
        instrument::instr_stop_node::call(instr, 0.0)?;
    }

    //   return (Node *) result;
    Ok(result)
}

/// `ExecEndBitmapAnd(node)` — shut down the subscans of the `BitmapAnd` node.
///
/// 1:1 with `void ExecEndBitmapAnd(BitmapAndState *node)`.
pub fn ExecEndBitmapAnd<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let node = as_bitmap_and_state(node)?;

    // get information from the node
    //   bitmapplans = node->bitmapplans;
    //   nplans = node->nplans;
    let nplans = node.nplans as usize;

    // shut down each of the subscans (that we've initialized)
    //   for (i = 0; i < nplans; i++)
    //       if (bitmapplans[i]) ExecEndNode(bitmapplans[i]);
    for i in 0..nplans {
        if let Some(slot) = node.bitmapplans.get_mut(i) {
            if let Some(subnode) = slot.as_deref_mut() {
                execProcnode::exec_end_node::call(subnode, estate)?;
            }
        }
    }

    Ok(())
}

/// `ExecReScanBitmapAnd(node)` — rescan the `BitmapAnd` node.
///
/// 1:1 with `void ExecReScanBitmapAnd(BitmapAndState *node)`.
pub fn ExecReScanBitmapAnd<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let node = as_bitmap_and_state(node)?;
    let nplans = node.nplans as usize;
    let chg_param_present = node.ps.chgParam.is_some();

    for i in 0..nplans {
        // ExecReScan doesn't know about my subplans, so I have to do
        // changed-parameter signaling myself.
        //   if (node->ps.chgParam != NULL)
        //       UpdateChangedParamSet(subnode, node->ps.chgParam);
        if chg_param_present {
            // Split-borrow: clone the parent set so the child borrow and the
            // (read-only) parent chgParam can coexist (the C reads the live
            // set; the cloned copy is identical).
            let newchg = match node.ps.chgParam.as_deref() {
                Some(b) => b.clone_in(mcx)?,
                None => {
                    return Err(elog_error(
                        "ExecReScanBitmapAnd: chgParam present but missing",
                    ))
                }
            };
            let subnode = node
                .bitmapplans
                .get_mut(i)
                .and_then(|slot| slot.as_deref_mut())
                .ok_or_else(|| elog_error("BitmapAnd child plan state is missing"))?;
            execUtils::update_changed_param_set::call(mcx, subnode.ps_head_mut(), &newchg)?;
        }

        // If chgParam of subnode is not null then plan will be re-scanned by
        // first ExecProcNode.
        //   if (subnode->chgParam == NULL) ExecReScan(subnode);
        let subnode = node
            .bitmapplans
            .get_mut(i)
            .and_then(|slot| slot.as_deref_mut())
            .ok_or_else(|| elog_error("BitmapAnd child plan state is missing"))?;
        if subnode.ps_head().chgParam.is_none() {
            execAmi::exec_re_scan::call(subnode, estate)?;
        }
    }

    Ok(())
}

// ===========================================================================
// Small in-crate helpers.
// ===========================================================================

/// `(BitmapAndState *) node` — narrow the dispatched `PlanStateNode` to the
/// concrete `BitmapAndState` (the C `castNode` after the dispatch tag match).
fn as_bitmap_and_state<'a, 'mcx>(
    node: &'a mut PlanStateNode<'mcx>,
) -> PgResult<&'a mut BitmapAndState<'mcx>> {
    match node {
        PlanStateNode::BitmapAnd(b) => Ok(b),
        _ => Err(elog_error("expected a BitmapAndState node")),
    }
}

/// `elog(ERROR, msg)` — internal error with `ERRCODE_INTERNAL_ERROR`.
fn elog_error(message: &'static str) -> PgError {
    PgError::error(message).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}
