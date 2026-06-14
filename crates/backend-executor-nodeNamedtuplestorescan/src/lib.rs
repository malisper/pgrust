//! Port of `src/backend/executor/nodeNamedtuplestorescan.c` — routines to
//! handle `NamedTuplestoreScan` nodes: a scan over an Ephemeral Named Relation
//! (ENR) backed by a tuplestore, e.g. the transition tables of `AFTER`
//! triggers.
//!
//! INTERFACE ROUTINES
//! - [`ExecNamedTuplestoreScan`]      - scans the named tuplestore
//! - [`ExecInitNamedTuplestoreScan`]  - creates and initializes the node
//! - [`ExecReScanNamedTuplestoreScan`]- rescans the relation
//!
//! plus the file-scope statics `NamedTuplestoreScanNext` (the access method the
//! generic `execScan.c` driver re-enters) and `NamedTuplestoreScanRecheck` (the
//! EvalPlanQual recheck — nothing to check).
//!
//! There is intentionally no `ExecEndNamedTuplestoreScan`: in execProcnode.c's
//! `ExecEndNode`, `T_NamedTuplestoreScanState` is in the "No clean up actions
//! for these nodes" group. The tuplestore is owned by the query environment
//! (`get_ENR`'s `EphemeralNamedRelation`), not by this node, and the extra read
//! pointer is never freed (see the C `XXX` comment), so teardown is a no-op.
//!
//! The node state is the owned [`NamedTuplestoreScanState`] mutated through
//! `&mut` borrows; `node->relation` is a non-owning [`NonNull`] alias of the
//! ENR's tuplestore (the C `Tuplestorestate *`). Calls into unported owners —
//! the generic scan driver (`execScan.c`), the tuplestore read-pointer API
//! (`tuplestore.c`), expression init (`execExpr.c`), tuple-slot ops and
//! result-type setup (`execTuples.c`/`execUtils.c`) — go through those owners'
//! seam crates and panic until the owners land. `get_ENR` /
//! `ENRMetadataGetTupDesc` are already ported, so they are called directly.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use core::ptr::NonNull;

use backend_executor_execExpr_seams as execExpr;
use backend_executor_execScan_seams as execScan;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils;
use backend_utils_misc_queryenvironment as queryenv;
use backend_utils_sort_storage_seams as tuplestore;

use mcx::{alloc_in, PgBox};
use types_error::{PgError, PgResult};
use types_nodes::executor::EXEC_FLAG_REWIND;
use types_nodes::nodenamedtuplestorescan::{NamedTuplestoreScan, NamedTuplestoreScanState};
use types_nodes::queryenvironment::QueryEnvironment;
use types_nodes::{EStateData, SlotId, TupleSlotKind};

/// Install this crate's implementations into its seam slots.
///
/// nodeNamedtuplestorescan has no `<unit>-seams` crate of its own: callers that
/// need these functions (execProcnode's dispatch tables) can depend on this
/// crate directly without a cycle, since this crate reaches outward only
/// through per-owner seam crates and the already-ported queryenvironment unit.
pub fn init_seams() {}

// ===========================================================================
//                              Scan Support
// ===========================================================================

/// `NamedTuplestoreScanNext(node)` — the workhorse for
/// `ExecNamedTuplestoreScan`.
///
/// Selects this node's own read pointer in the named tuplestore and fetches the
/// next tuple into the scan slot. Backward scan is intentionally unsupported
/// (the C `Assert(ScanDirectionIsForward(...))` guards an executor-owned
/// invariant). Returns `Ok(true)` when a tuple was fetched into
/// `node.ss.ss_ScanTupleSlot`, `Ok(false)` when the tuplestore is exhausted
/// (the C returns the scan slot, whose emptiness the boolean reports).
fn NamedTuplestoreScanNext<'mcx>(
    node: &mut NamedTuplestoreScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // We intentionally do not support backward scan.
    //   Assert(ScanDirectionIsForward(node->ss.ps.state->es_direction));

    // Get the next tuple from tuplestore. Return NULL if no more tuples.
    //   slot = node->ss.ss_ScanTupleSlot;
    let scanslot = node
        .ss
        .ss_ScanTupleSlot
        .expect("NamedTuplestoreScanNext: ss_ScanTupleSlot not initialized");
    let readptr = node.readptr;

    //   tuplestore_select_read_pointer(node->relation, node->readptr);
    let relation = relation_mut(node);
    tuplestore::tuplestore_select_read_pointer::call(relation, readptr)?;

    //   (void) tuplestore_gettupleslot(node->relation, true, false, slot);
    let relation = relation_mut(node);
    tuplestore::tuplestore_gettupleslot::call(relation, true, false, estate.slot_mut(scanslot))
}

/// `NamedTuplestoreScanRecheck(node, slot)` — access-method routine to recheck
/// a tuple in EvalPlanQual. Nothing to check, so it always succeeds.
fn NamedTuplestoreScanRecheck<'mcx>(
    _node: &mut NamedTuplestoreScanState<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // nothing to check
    Ok(true)
}

/// `ExecNamedTuplestoreScan(pstate)` — the `PlanState.ExecProcNode` callback.
///
/// Scans the CTE sequentially and returns the next qualifying tuple. Calls the
/// generic [`execScan::exec_scan_namedtuplestore`] driver, passing it the
/// named-tuplestore access-method functions.
///
/// ```c
/// return ExecScan(&node->ss,
///                 (ExecScanAccessMtd) NamedTuplestoreScanNext,
///                 (ExecScanRecheckMtd) NamedTuplestoreScanRecheck);
/// ```
pub fn ExecNamedTuplestoreScan<'mcx>(
    node: &mut NamedTuplestoreScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    execScan::exec_scan_namedtuplestore::call(
        node,
        estate,
        NamedTuplestoreScanNext,
        NamedTuplestoreScanRecheck,
    )
}

/// The `PlanState.ExecProcNode` callback installed by
/// [`ExecInitNamedTuplestoreScan`]: `castNode(NamedTuplestoreScanState,
/// pstate)` then run [`ExecNamedTuplestoreScan`].
fn exec_named_tuplestore_scan_node<'mcx>(
    pstate: &mut types_nodes::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        types_nodes::PlanStateNode::NamedTuplestoreScan(node) => node,
        other => panic!("castNode(NamedTuplestoreScanState, pstate) failed: {other:?}"),
    };
    ExecNamedTuplestoreScan(node, estate)
}

// ===========================================================================
//                          Public node entry points
// ===========================================================================

/// `ExecInitNamedTuplestoreScan(node, estate, eflags)` — create and initialize
/// a `NamedTuplestoreScan` node.
///
/// The state tree is allocated in `estate.es_query_cxt` (C: `makeNode` in the
/// per-query context current during `ExecInitNode`), so initialization is
/// fallible on OOM. The C reads the ENR through `estate->es_queryEnv`; that
/// field is trimmed off `EStateData` here, so the query environment is threaded
/// in explicitly by the executor spine.
pub fn ExecInitNamedTuplestoreScan<'mcx>(
    node: &'mcx types_nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    query_env: &mut QueryEnvironment<'mcx>,
) -> PgResult<PgBox<'mcx, NamedTuplestoreScanState<'mcx>>> {
    let mcx = estate.es_query_cxt;

    // NamedTuplestoreScan *node — the enclosing plan-tree node. Panics if it is
    // not a `NamedTuplestoreScan` (the C `castNode`).
    let ntscan: &'mcx NamedTuplestoreScan<'mcx> = match node {
        types_nodes::nodes::Node::NamedTuplestoreScan(n) => n,
        other => panic!("castNode(NamedTuplestoreScan, node) failed: {other:?}"),
    };

    // check for unsupported flags
    //   Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
    debug_assert!(
        eflags
            & (types_nodes::executor::EXEC_FLAG_BACKWARD | types_nodes::executor::EXEC_FLAG_MARK)
            == 0
    );

    // NamedTuplestoreScan should not have any children.
    //   Assert(outerPlan(node) == NULL); Assert(innerPlan(node) == NULL);
    debug_assert!(ntscan.scan.plan.lefttree.is_none());
    debug_assert!(ntscan.scan.plan.righttree.is_none());

    // create new NamedTuplestoreScanState for node
    //   scanstate = makeNode(NamedTuplestoreScanState);
    //   scanstate->ss.ps.plan = (Plan *) node;
    //   scanstate->ss.ps.state = estate;
    //   scanstate->ss.ps.ExecProcNode = ExecNamedTuplestoreScan;
    let mut scanstate = alloc_in(mcx, NamedTuplestoreScanState::new_in(mcx))?;
    scanstate.ss.ps.plan = Some(node);
    scanstate.ss.ps.ExecProcNode = Some(exec_named_tuplestore_scan_node);

    // enr = get_ENR(estate->es_queryEnv, node->enrname);
    // if (!enr)
    //     elog(ERROR, "executor could not find named tuplestore \"%s\"",
    //          node->enrname);
    //
    // `enr->reldata` is the query-environment-owned tuplestore; this node only
    // aliases it (a non-owning `Tuplestorestate *`). We look the ENR up mutably
    // to obtain that raw pointer, then read its metadata.
    let enrname = ntscan.enrname.as_deref().unwrap_or("");
    let idx = match enr_index(query_env, enrname) {
        Some(i) => i,
        None => return Err(named_tuplestore_not_found(enrname)),
    };

    {
        let enr = &mut query_env.namedRelList[idx];

        // Assert(enr->reldata);
        // scanstate->relation = (Tuplestorestate *) enr->reldata;
        let reldata = enr
            .reldata
            .as_mut()
            .expect("ExecInitNamedTuplestoreScan: enr->reldata is NULL");
        // A non-owning alias of the ENR's tuplestore (C: the raw cast). The
        // query environment outlives the scan, so the pointer stays valid.
        scanstate.relation = Some(NonNull::from(&mut **reldata));

        // scanstate->tupdesc = ENRMetadataGetTupDesc(&(enr->md));
        scanstate.tupdesc = match queryenv::ENRMetadataGetTupDesc(mcx, &enr.md)? {
            Some(td) => Some(alloc_in(mcx, td.clone_in(mcx)?)?),
            None => None,
        };
    }

    // scanstate->readptr =
    //     tuplestore_alloc_read_pointer(scanstate->relation, EXEC_FLAG_REWIND);
    let relation = relation_mut(&mut scanstate);
    scanstate.readptr =
        tuplestore::tuplestore_alloc_read_pointer::call(relation, EXEC_FLAG_REWIND)?;

    // The new read pointer copies its position from read pointer 0, which could
    // be anywhere, so explicitly rewind it.
    //   tuplestore_select_read_pointer(scanstate->relation, scanstate->readptr);
    //   tuplestore_rescan(scanstate->relation);
    let readptr = scanstate.readptr;
    let relation = relation_mut(&mut scanstate);
    tuplestore::tuplestore_select_read_pointer::call(relation, readptr)?;
    let relation = relation_mut(&mut scanstate);
    tuplestore::tuplestore_rescan::call(relation)?;

    // XXX: Should we add a function to free that read pointer when done? This
    // was attempted, but it did not improve performance or memory usage in any
    // tested cases.

    // Miscellaneous initialization: create expression context for node.
    //   ExecAssignExprContext(estate, &scanstate->ss.ps);
    execUtils::exec_assign_expr_context::call(estate, &mut scanstate.ss.ps)?;

    // The scan tuple type is specified for the tuplestore.
    //   ExecInitScanTupleSlot(estate, &scanstate->ss, scanstate->tupdesc,
    //                         &TTSOpsMinimalTuple);
    let tupdesc = clone_tupdesc(&scanstate.tupdesc, mcx)?;
    execTuples::exec_init_scan_tuple_slot::call(
        estate,
        &mut scanstate.ss,
        tupdesc,
        TupleSlotKind::MinimalTuple,
    )?;

    // Initialize result type and projection.
    //   ExecInitResultTypeTL(&scanstate->ss.ps);
    //   ExecAssignScanProjectionInfo(&scanstate->ss);
    execUtils::exec_init_result_type_tl::call(&mut scanstate.ss.ps, estate)?;
    execScan::exec_assign_scan_projection_info::call(&mut scanstate.ss, estate)?;

    // initialize child expressions
    //   scanstate->ss.ps.qual =
    //       ExecInitQual(node->scan.plan.qual, (PlanState *) scanstate);
    let qual = ntscan.scan.plan.qual.as_deref();
    scanstate.ss.ps.qual = execExpr::exec_init_qual::call(qual, &mut scanstate.ss.ps, estate)?;

    Ok(scanstate)
}

/// `ExecReScanNamedTuplestoreScan(node)` — rescans the relation.
///
/// ```c
/// void
/// ExecReScanNamedTuplestoreScan(NamedTuplestoreScanState *node)
/// {
///     Tuplestorestate *tuplestorestate = node->relation;
///
///     if (node->ss.ps.ps_ResultTupleSlot)
///         ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
///
///     ExecScanReScan(&node->ss);
///
///     /* Rewind my own pointer. */
///     tuplestore_select_read_pointer(tuplestorestate, node->readptr);
///     tuplestore_rescan(tuplestorestate);
/// }
/// ```
pub fn ExecReScanNamedTuplestoreScan<'mcx>(
    node: &mut NamedTuplestoreScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Tuplestorestate *tuplestorestate = node->relation; (used below)

    // if (node->ss.ps.ps_ResultTupleSlot)
    //     ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    if let Some(slot) = node.ss.ps.ps_ResultTupleSlot {
        execTuples::exec_clear_tuple::call(estate.slot_mut(slot))?;
    }

    // ExecScanReScan(&node->ss);
    execScan::exec_scan_rescan_ss::call(&mut node.ss, estate)?;

    // Rewind my own pointer.
    //   tuplestore_select_read_pointer(tuplestorestate, node->readptr);
    //   tuplestore_rescan(tuplestorestate);
    let readptr = node.readptr;
    let relation = relation_mut(node);
    tuplestore::tuplestore_select_read_pointer::call(relation, readptr)?;
    let relation = relation_mut(node);
    tuplestore::tuplestore_rescan::call(relation)?;

    Ok(())
}

// ===========================================================================
// Small in-crate helpers
// ===========================================================================

/// `node->relation` dereferenced to a `&mut Tuplestorestate` (the C
/// `Tuplestorestate *`). Panics if the alias was never installed (C would
/// dereference a NULL pointer — a compiler/executor bug).
#[inline]
fn relation_mut<'a, 'mcx>(
    node: &'a mut NamedTuplestoreScanState<'mcx>,
) -> &'a mut types_nodes::Tuplestorestate<'mcx> {
    // SAFETY: the pointer aliases the query-environment-owned ENR tuplestore,
    // which stays live for the scan; the executor drives one node at a time, so
    // no other live `&mut` to it exists during this call.
    unsafe { node.relation_mut() }.expect("NamedTuplestoreScanState.relation is NULL")
}

/// `get_ENR`'s name-match index in the query environment's ENR list (C:
/// `foreach` + `strcmp(enr->md.name, name) == 0`). Mirrors the private
/// `enr_index` of queryenvironment.c, returning the slot so the ENR can be
/// borrowed mutably for the `reldata` alias.
fn enr_index(query_env: &QueryEnvironment<'_>, name: &str) -> Option<usize> {
    query_env
        .namedRelList
        .iter()
        .position(|enr| enr.md.name.as_deref() == Some(name))
}

/// Deep-copy an optional `TupleDesc` into `mcx` (the slot setup needs its own
/// owned descriptor; C shares the pointer).
fn clone_tupdesc<'mcx>(
    td: &types_tuple::heaptuple::TupleDesc<'mcx>,
    mcx: mcx::Mcx<'mcx>,
) -> PgResult<types_tuple::heaptuple::TupleDesc<'mcx>> {
    match td {
        Some(d) => Ok(Some(alloc_in(mcx, d.clone_in(mcx)?)?)),
        None => Ok(None),
    }
}

/// `elog(ERROR, "executor could not find named tuplestore \"%s\"",
/// node->enrname)` — the not-found path of `ExecInitNamedTuplestoreScan`. Uses
/// the default `XX000` SQLSTATE (`ERRCODE_INTERNAL_ERROR`), matching the C
/// `elog(ERROR, ...)`.
fn named_tuplestore_not_found(enrname: &str) -> PgError {
    PgError::error(alloc::format!(
        "executor could not find named tuplestore \"{enrname}\""
    ))
}
