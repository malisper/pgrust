//! Port of `src/backend/executor/nodeBitmapOr.c` — routines to handle
//! `BitmapOr` nodes.
//!
//! `BitmapOr` nodes don't make use of their left and right subtrees; rather
//! they maintain a list of subplans, much like `Append` nodes. The logic is
//! much simpler than `Append`, however, since we needn't cope with
//! forward/backward execution.
//!
//! INTERFACE ROUTINES
//! - [`ExecInitBitmapOr`]  - initialize the `BitmapOr` node
//! - [`MultiExecBitmapOr`] - retrieve the result bitmap from the node
//! - [`ExecEndBitmapOr`]   - shut down the `BitmapOr` node
//! - [`ExecReScanBitmapOr`]- rescan the `BitmapOr` node
//!
//! Operations below or beside the node go through their owners' seam crates:
//!
//! - child lifecycle/dispatch (`ExecInitNode` / `MultiExecProcNode` /
//!   `ExecEndNode`) → execProcnode;
//! - rescan (`ExecReScan`) → execAmi;
//! - changed-parameter signaling (`UpdateChangedParamSet`) → execUtils;
//! - per-node instrumentation (`InstrStartNode` / `InstrStopNode`) → instrument;
//! - the running result bitmap (`tbm_create` / `tbm_union`) → tidbitmap
//!   (`backend-nodes-core`);
//! - the `BitmapIndexScan` special-case child run (set `biss_result`, run, OR
//!   in place) → nodeBitmapIndexscan;
//! - `work_mem` (GUC) → init/globals.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

use mcx::{Mcx, PgBox};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_nodes::executor::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};
use types_nodes::execstate_tags::T_BitmapIndexScanState;
use types_nodes::EStateData;

use backend_executor_execAmi_seams as execAmi;
use backend_executor_execProcnode_seams as execProcnode;
use backend_executor_execUtils_seams as execUtils;
use backend_executor_instrument_seams as instrument;
use backend_executor_nodeBitmapIndexscan_seams as nodeBitmapIndexscan;
use backend_nodes_core_tidbitmap_seams as tidbitmap;
use backend_utils_init_small_seams as globals;

pub mod nodes;

use nodes::{BitmapOr, BitmapOrState};

// ===========================================================================
// Interface routines (1:1 with nodeBitmapOr.c).
// ===========================================================================

/// `ExecBitmapOr` — stub for pro forma compliance.
///
/// ```c
/// static TupleTableSlot *
/// ExecBitmapOr(PlanState *pstate)
/// {
///     elog(ERROR, "BitmapOr node does not support ExecProcNode call convention");
///     return NULL;
/// }
/// ```
///
/// The C installs this as the node's `ExecProcNode` callback so an erroneous
/// tuple-at-a-time dispatch is reported rather than silently mis-executed. A
/// `BitmapOr` is only ever run through `MultiExecProcNode`. The C returns
/// `TupleTableSlot *`; on the error path there is no slot to return, so this is
/// `PgResult<()>`.
pub fn ExecBitmapOr() -> PgResult<()> {
    Err(elog_error(
        "BitmapOr node does not support ExecProcNode call convention",
    ))
}

/// `ExecInitBitmapOr` — begin all of the subscans of the `BitmapOr` node.
///
/// ```c
/// BitmapOrState *
/// ExecInitBitmapOr(BitmapOr *node, EState *estate, int eflags)
/// ```
pub fn ExecInitBitmapOr<'mcx>(
    mcx: Mcx<'mcx>,
    node: &'mcx BitmapOr<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, BitmapOrState<'mcx>>> {
    // check for unsupported flags
    //   Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
    debug_assert!(
        eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0,
        "BitmapOr does not support EXEC_FLAG_BACKWARD/EXEC_FLAG_MARK"
    );

    // Set up empty vector of subplan states
    //   nplans = list_length(node->bitmapplans);
    let nplans = node.bitmapplans.len();

    // bitmapplanstates = (PlanState **) palloc0(nplans * sizeof(PlanState *));
    let mut bitmapplans: mcx::PgVec<'mcx, Option<PgBox<'mcx, types_nodes::PlanStateNode<'mcx>>>> =
        mcx::PgVec::new_in(mcx);
    bitmapplans
        .try_reserve(nplans)
        .map_err(|_| mcx.oom(nplans * core::mem::size_of::<usize>()))?;

    // create new BitmapOrState for our BitmapOr node
    //   bitmaporstate->ps.plan = (Plan *) node;
    //   bitmaporstate->ps.state = estate;
    //   bitmaporstate->ps.ExecProcNode = ExecBitmapOr;
    //   bitmaporstate->bitmapplans = bitmapplanstates;
    //   bitmaporstate->nplans = nplans;
    //
    // `makeNode(BitmapOrState)` zeroes the struct; the owned model fills the
    // `ps` head's defaults. The C sets `ps.NodeTag` (T_BitmapOrState) — the
    // owned `BitmapOrState` struct IS its own tag, so there is no tag field to
    // store. `ExecProcNode` is left `None`: the C installs the pro-forma
    // `ExecBitmapOr` stub, which only `elog(ERROR)`s if it is ever dispatched
    // through the tuple-at-a-time convention (a BitmapOr is always run through
    // `MultiExecProcNode`). The owned dispatch never calls a BitmapOr's
    // `ExecProcNode`, so the stub has no slot to occupy.
    let ps = types_nodes::execnodes::PlanStateData::default();

    // call ExecInitNode on each of the plans to be executed and save the
    // results into the array "bitmapplanstates".
    //   i = 0;
    //   foreach(l, node->bitmapplans) {
    //       initNode = (Plan *) lfirst(l);
    //       bitmapplanstates[i] = ExecInitNode(initNode, estate, eflags);
    //       i++;
    //   }
    for init_node in &node.bitmapplans {
        let child = execProcnode::exec_init_node::call(mcx, Some(init_node), estate, eflags)?;
        bitmapplans.push(child);
    }

    // Miscellaneous initialization
    //
    // BitmapOr plans don't have expression contexts because they never call
    // ExecQual or ExecProject.  They don't need any tuple slots either.

    let nplans_i32 =
        i32::try_from(nplans).map_err(|_| elog_error("BitmapOr has too many input plans"))?;

    PgBox::try_new_in(
        BitmapOrState {
            ps,
            bitmapplans,
            nplans: nplans_i32,
            isshared: node.isshared,
        },
        mcx,
    )
    .map_err(|_| mcx.oom(core::mem::size_of::<BitmapOrState>()))
}

/// `MultiExecBitmapOr` — retrieve the result bitmap from the node.
///
/// ```c
/// Node *
/// MultiExecBitmapOr(BitmapOrState *node)
/// ```
///
/// Returns the OR of every child bitmap as the owned `Box<TIDBitmap>` (the C
/// `Node *result`). The `EState` is threaded explicitly (the C
/// `node->ps.state` back-pointer) because the child dispatch and the shared
/// `tbm_create` need it.
pub fn MultiExecBitmapOr<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut BitmapOrState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, types_tidbitmap::TIDBitmap>> {
    // must provide our own instrumentation support
    //   if (node->ps.instrument) InstrStartNode(node->ps.instrument);
    if let Some(instr) = node.ps.instrument.as_deref_mut() {
        instrument::instr_start_node::call(instr)?;
    }

    // get information from the node
    //   bitmapplans = node->bitmapplans;
    //   nplans = node->nplans;
    let nplans = node.nplans as usize;

    // Scan all the subplans and OR their result bitmaps
    //   TIDBitmap *result = NULL;
    let mut result: Option<PgBox<'mcx, types_tidbitmap::TIDBitmap>> = None;

    for i in 0..nplans {
        // PlanState *subnode = bitmapplans[i];
        let subnode = node
            .bitmapplans
            .get_mut(i)
            .and_then(|slot| slot.as_deref_mut())
            .ok_or_else(|| elog_error("BitmapOr child plan state is missing"))?;

        // We can special-case BitmapIndexScan children to avoid an explicit
        // tbm_union step for each child: just pass down the current result
        // bitmap and let the child OR directly into it.
        //   if (IsA(subnode, BitmapIndexScanState))
        if subnode.tag() == T_BitmapIndexScanState {
            // if (result == NULL) /* first subplan */
            if result.is_none() {
                // XXX should we use less than work_mem for this?
                //   result = tbm_create(work_mem * (Size) 1024,
                //       ((BitmapOr *) node->ps.plan)->isshared ?
                //       node->ps.state->es_query_dsa : NULL);
                let maxbytes = (globals::work_mem::call() as usize).wrapping_mul(1024);
                let dsa = if node.isshared {
                    estate.es_query_dsa
                } else {
                    None
                };
                result = Some(tidbitmap::tbm_create::call(mcx, maxbytes, dsa)?);
            }

            //   ((BitmapIndexScanState *) subnode)->biss_result = result;
            //   subresult = (TIDBitmap *) MultiExecProcNode(subnode);
            //   if (subresult != result) elog(ERROR, "unrecognized result from subplan");
            //
            // The child ORs its TIDs directly into the shared `result` bitmap;
            // the identity check is implicit (the seam ORs in place).
            let result_ref = result
                .as_deref_mut()
                .ok_or_else(|| elog_error("MultiExecBitmapOr: result bitmap missing after creation"))?;
            nodeBitmapIndexscan::multi_exec_bitmap_index_child::call(subnode, result_ref, estate)?;
        } else {
            // standard implementation
            //   subresult = (TIDBitmap *) MultiExecProcNode(subnode);
            //   if (!subresult || !IsA(subresult, TIDBitmap))
            //       elog(ERROR, "unrecognized result from subplan");
            //
            // The recursion seam returns the child's real `Box<TIDBitmap>`, so
            // the C `IsA(subresult, TIDBitmap)` tag check is satisfied by the
            // return type (the wrong-type / NULL error paths are statically
            // unreachable).
            let subresult = execProcnode::multi_exec_proc_node::call(subnode, estate)?;

            // if (result == NULL) result = subresult; /* first subplan */
            match result.as_deref_mut() {
                None => result = Some(subresult),
                Some(result_inner) => {
                    //   tbm_union(result, subresult);
                    //   tbm_free(subresult);
                    // `tbm_union` folds `subresult` into `result`; `subresult`
                    // is then freed (the owned `Box` is dropped at end of scope,
                    // the idiomatic `tbm_free`).
                    tidbitmap::tbm_union::call(result_inner, &subresult)?;
                }
            }
        }
    }

    // We could return an empty result set here?
    //   if (result == NULL) elog(ERROR, "BitmapOr doesn't support zero inputs");
    let result = result.ok_or_else(|| elog_error("BitmapOr doesn't support zero inputs"))?;

    // must provide our own instrumentation support
    //   if (node->ps.instrument) InstrStopNode(node->ps.instrument, 0);
    if let Some(instr) = node.ps.instrument.as_deref_mut() {
        instrument::instr_stop_node::call(instr, 0.0)?;
    }

    //   return (Node *) result;
    Ok(result)
}

/// `ExecEndBitmapOr` — shut down the subscans of the `BitmapOr` node.
///
/// ```c
/// void ExecEndBitmapOr(BitmapOrState *node)
/// ```
pub fn ExecEndBitmapOr<'mcx>(
    node: &mut BitmapOrState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
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

/// `ExecReScanBitmapOr` — rescan the `BitmapOr` node.
///
/// ```c
/// void ExecReScanBitmapOr(BitmapOrState *node)
/// ```
pub fn ExecReScanBitmapOr<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut BitmapOrState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let chg_param_present = node.ps.chgParam.is_some();
    let nplans = node.nplans as usize;

    for i in 0..nplans {
        // ExecReScan doesn't know about my subplans, so I have to do
        // changed-parameter signaling myself.
        //   if (node->ps.chgParam != NULL)
        //       UpdateChangedParamSet(subnode, node->ps.chgParam);
        if chg_param_present {
            // Split-borrow: clone the parent set so the child borrow and the
            // (read-only) parent chgParam can coexist (the C reads the live set;
            // the cloned copy is identical).
            let newchg = match node.ps.chgParam.as_deref() {
                Some(b) => b.clone_in(mcx)?,
                None => return Err(elog_error("ExecReScanBitmapOr: chgParam present but missing")),
            };
            let subnode = node
                .bitmapplans
                .get_mut(i)
                .and_then(|slot| slot.as_deref_mut())
                .ok_or_else(|| elog_error("BitmapOr child plan state is missing"))?;
            execUtils::update_changed_param_set::call(mcx, subnode.ps_head_mut(), &newchg)?;
        }

        // If chgParam of subnode is not null then plan will be re-scanned by
        // first ExecProcNode.
        //   if (subnode->chgParam == NULL) ExecReScan(subnode);
        let subnode = node
            .bitmapplans
            .get_mut(i)
            .and_then(|slot| slot.as_deref_mut())
            .ok_or_else(|| elog_error("BitmapOr child plan state is missing"))?;
        if subnode.ps_head().chgParam.is_none() {
            execAmi::exec_re_scan::call(subnode, estate)?;
        }
    }

    Ok(())
}

// ===========================================================================
// Small in-crate helper.
// ===========================================================================

/// `elog(ERROR, msg)` — `errmsg_internal` text with `ERRCODE_INTERNAL_ERROR`.
fn elog_error(message: &'static str) -> PgError {
    PgError::error(message).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

/// Install this crate's inward seams. `nodeBitmapOr` owns no inward seam (no
/// other unit calls into it across a cycle — the executor dispatch reaches it
/// through the `execProcnode` arm that this crate's functions back), so this is
/// empty; the recurrence guard checks it exists and is wired into `init_all`.
pub fn init_seams() {}
