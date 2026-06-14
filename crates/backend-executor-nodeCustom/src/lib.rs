//! Port of `src/backend/executor/nodeCustom.c` — routines to handle execution
//! of a custom-scan node.
//!
//! INTERFACE ROUTINES
//! - [`ExecInitCustomScan`]      - creates and initializes a `CustomScanState`
//! - [`ExecCustomScan`]          - the per-tuple `ExecProcNode` callback
//! - [`ExecEndCustomScan`]       - releases provider resources
//! - [`ExecReScanCustomScan`]    - rescans
//! - [`ExecCustomMarkPos`] / [`ExecCustomRestrPos`] - mark/restore (optional)
//!
//! plus the parallel-scan entry points ([`ExecCustomScanEstimate`] /
//! `InitializeDSM` / `ReInitializeDSM` / `InitializeWorker`) and
//! [`ExecShutdownCustomScan`].
//!
//! The node state machine is held as an owned [`CustomScanState`] mutated
//! through `&mut` borrows; the C `PlanState.state` back-pointer is replaced by
//! threading `&mut EStateData` explicitly. `ExecCustomScan` returns
//! `Ok(Some(slot))` when a tuple is available (the C non-NULL `return slot`)
//! and `Ok(None)` for the C `NULL`.
//!
//! The genuine external boundary is the **custom-scan provider**: the
//! `CustomScanMethods.CreateCustomScanState` and the `CustomExecMethods.*`
//! callbacks an extension installs. The node reads the owned `node.methods`
//! presence flags for the `if (methods->X)` / `Assert(methods->X != NULL)`
//! checks and the `CustomName` for the mark/restore error message — exactly as
//! the C does — and reaches the *invocation* through
//! `backend-nodes-extensible-seams` (where the provider-callback seams live, as
//! the FDW callbacks live in `backend-foreign-foreign-seams`). Those provider
//! seams panic until an extension installs them, which is correct: there is no
//! in-tree custom-scan provider.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

use backend_executor_execExpr_seams as execExpr;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils;
use backend_nodes_extensible_seams as provider;
use backend_storage_ipc_shm_toc_seams as shm_toc;
use backend_tcop_postgres_seams as tcop_postgres;

use mcx::{alloc_in, PgBox};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_nodes::nodes::Node;
use types_nodes::{
    CustomScan, CustomScanState, EStateData, ParallelContext, ParallelWorkerContext, SlotId,
    TupleSlotKind,
};

/// `INDEX_VAR` (primnodes.h) — varno used in a provider-supplied scan tlist.
/// `#define INDEX_VAR (-3)`.
const INDEX_VAR: i32 = -3;

/// Install this crate's implementations of the parallel-scan inward seams (the
/// ones `execParallel` calls through `backend-executor-nodeCustom-seams`).
///
/// `execParallel` holds the custom-scan node as an opaque
/// `PlanStateHandle`/`ParallelContextHandle` (a not-yet-bridged pointer into
/// executor/DSM-owned state). Until the DSM owner can hand this crate the owned
/// `CustomScanState`, the parallel path is unreachable, so these installed
/// implementations panic loudly (mirror-PG-and-panic). The owned parallel entry
/// points ([`ExecCustomScanEstimate`] et al.) carry the real C logic and are
/// callable directly.
pub fn init_seams() {
    use backend_executor_nodeCustom_seams as pq;
    pq::exec_customscan_estimate::set(|_node, _pcxt| {
        panic!(
            "ExecCustomScanEstimate via parallel DSM is unreachable until the \
             DSM owner can pass the owned CustomScanState (the opaque \
             PlanStateHandle cannot be resolved here yet)"
        )
    });
    pq::exec_customscan_initialize_dsm::set(|_node, _pcxt| {
        panic!("ExecCustomScanInitializeDSM via parallel DSM is unreachable until the DSM owner lands")
    });
    pq::exec_customscan_reinitialize_dsm::set(|_node, _pcxt| {
        panic!("ExecCustomScanReInitializeDSM via parallel DSM is unreachable until the DSM owner lands")
    });
    pq::exec_customscan_initialize_worker::set(|_node, _pwcxt| {
        panic!("ExecCustomScanInitializeWorker via parallel DSM is unreachable until the DSM owner lands")
    });
}

// ===========================================================================
//                          Public node entry points
// ===========================================================================

/// The `PlanState.ExecProcNode` callback installed by [`ExecInitCustomScan`]:
/// `castNode(CustomScanState, pstate)` then run [`ExecCustomScan`].
fn exec_custom_scan_node<'mcx>(
    pstate: &mut types_nodes::planstate::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        types_nodes::planstate::PlanStateNode::CustomScan(node) => node,
        other => panic!("castNode(CustomScanState, pstate) failed: {other:?}"),
    };
    ExecCustomScan(node, estate)
}

/// `ExecInitCustomScan(cscan, estate, eflags)` — create and initialize a
/// `CustomScanState`.
///
/// Takes the enclosing plan-tree [`Node`] (the C `CustomScan *`). The provider's
/// `CreateCustomScanState` does the allocation (it may embed `CustomScanState`
/// as its first field); we then fill the standard fields, open the scan
/// relation (if any), build scan/result slots and projection, init the qual,
/// and hand off to the provider's `BeginCustomScan`. Panics if the node is not
/// a `CustomScan` (the C `castNode`).
pub fn ExecInitCustomScan<'mcx>(
    node: &'mcx Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, CustomScanState<'mcx>>> {
    let mcx = estate.es_query_cxt;

    let cscan: &'mcx CustomScan<'mcx> = match node {
        Node::CustomScan(c) => c,
        other => panic!("castNode(CustomScan, node) failed: {other:?}"),
    };

    // Index scanrelid = cscan->scan.scanrelid;
    let scanrelid = cscan.scan.scanrelid;

    // Allocate the CustomScanState object. We let the custom scan provider do
    // the palloc, in case it wants to make a larger object that embeds
    // CustomScanState as the first field. It must set the node tag and the
    // methods field correctly at this time. Other standard fields should be set
    // to zero.
    //   css = castNode(CustomScanState,
    //                  cscan->methods->CreateCustomScanState(cscan));
    let mut css = alloc_in(mcx, provider::create_custom_scan_state::call(mcx, cscan)?)?;

    // ensure flags is filled correctly
    //   css->flags = cscan->flags;
    css.flags = cscan.flags;

    // fill up fields of ScanState
    //   css->ss.ps.plan = &cscan->scan.plan;
    //   css->ss.ps.state = estate;            (estate threaded explicitly)
    //   css->ss.ps.ExecProcNode = ExecCustomScan;
    //
    // The state's plan back-link aliases the shared, read-only plan tree
    // exactly as C's `css->ss.ps.plan = (Plan *) cscan`.
    css.ss.ps.plan = Some(node);
    css.ss.ps.ExecProcNode = Some(exec_custom_scan_node);

    // create expression context for node
    //   ExecAssignExprContext(estate, &css->ss.ps);
    execUtils::exec_assign_expr_context::call(estate, &mut css.ss.ps)?;

    // open the scan relation, if any
    //   if (scanrelid > 0) {
    //       scan_rel = ExecOpenScanRelation(estate, scanrelid, eflags);
    //       css->ss.ss_currentRelation = scan_rel;
    //   }
    if scanrelid > 0 {
        let scan_rel = execUtils::exec_open_scan_relation::call(estate, scanrelid, eflags)?;
        css.ss.ss_currentRelation = Some(scan_rel);
    }
    let scan_rel_is_some = css.ss.ss_currentRelation.is_some();

    // Use a custom slot if specified in CustomScanState or use virtual slot
    // otherwise.
    //   slotOps = css->slotOps;
    //   if (!slotOps) slotOps = &TTSOpsVirtual;
    let slot_ops = css.slotOps.unwrap_or(TupleSlotKind::Virtual);

    // Determine the scan tuple type. If the custom scan provider provided a
    // targetlist describing the scan tuples, use that; else use base relation's
    // rowtype.
    //   if (cscan->custom_scan_tlist != NIL || scan_rel == NULL)
    let custom_scan_tlist_is_nil = cscan
        .custom_scan_tlist
        .as_ref()
        .map(|tl| tl.is_empty())
        .unwrap_or(true);
    let tlistvarno: i32 = if !custom_scan_tlist_is_nil || !scan_rel_is_some {
        // scan_tupdesc = ExecTypeFromTL(cscan->custom_scan_tlist);
        let custom_scan_tlist: &[types_nodes::primnodes::TargetEntry<'mcx>] =
            cscan.custom_scan_tlist.as_deref().unwrap_or(&[]);
        let scan_tupdesc = execTuples::exec_type_from_tl::call(mcx, custom_scan_tlist)?;
        // ExecInitScanTupleSlot(estate, &css->ss, scan_tupdesc, slotOps);
        execTuples::exec_init_scan_tuple_slot::call(estate, &mut css.ss, scan_tupdesc, slot_ops)?;
        // Node's targetlist will contain Vars with varno = INDEX_VAR
        INDEX_VAR
    } else {
        // ExecInitScanTupleSlot(estate, &css->ss, RelationGetDescr(scan_rel),
        //                       slotOps);
        //
        // `ss_currentRelation` is the real owned relcache entry here (this
        // branch is taken only when `scan_rel_is_some`); its `rd_att_clone_in`
        // is `RelationGetDescr(scan_rel)`.
        let scan_tupdesc = {
            let rel = css
                .ss
                .ss_currentRelation
                .as_ref()
                .expect("scan_rel is Some in this branch");
            Some(rel.rd_att_clone_in(mcx)?)
        };
        execTuples::exec_init_scan_tuple_slot::call(estate, &mut css.ss, scan_tupdesc, slot_ops)?;
        // Node's targetlist will contain Vars with varno = scanrelid
        scanrelid as i32
    };

    // Initialize result slot, type and projection.
    //   ExecInitResultTupleSlotTL(&css->ss.ps, &TTSOpsVirtual);
    execTuples::exec_init_result_tuple_slot_tl::call(&mut css.ss.ps, estate, TupleSlotKind::Virtual)?;
    //   ExecAssignScanProjectionInfoWithVarno(&css->ss, tlistvarno);
    execUtils::exec_assign_scan_projection_info_with_varno::call(&mut css.ss, estate, tlistvarno)?;

    // initialize child expressions
    //   css->ss.ps.qual = ExecInitQual(cscan->scan.plan.qual, (PlanState *) css);
    {
        let qual = cscan.scan.plan.qual.as_deref();
        css.ss.ps.qual = execExpr::exec_init_qual::call(qual, &mut css.ss.ps, estate)?;
    }

    // The callback of custom-scan provider applies the final initialization of
    // the custom-scan-state node according to its logic.
    //   css->methods->BeginCustomScan(css, estate, eflags);
    provider::begin_custom_scan::call(&mut css, estate, eflags)?;

    Ok(css)
}

/// `ExecCustomScan(pstate)` — the `ExecProcNode` callback. Checks interrupts and
/// forwards to the provider's `ExecCustomScan` method.
pub fn ExecCustomScan<'mcx>(
    node: &mut CustomScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // CHECK_FOR_INTERRUPTS();
    tcop_postgres::check_for_interrupts::call()?;

    // Assert(node->methods->ExecCustomScan != NULL);
    // return node->methods->ExecCustomScan(node);
    provider::exec_custom_scan::call(node, estate)
}

/// `ExecEndCustomScan(node)` — forward to the provider's `EndCustomScan` method.
pub fn ExecEndCustomScan<'mcx>(
    node: &mut CustomScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Assert(node->methods->EndCustomScan != NULL);
    // node->methods->EndCustomScan(node);
    provider::end_custom_scan::call(node, estate)
}

/// `ExecReScanCustomScan(node)` — forward to the provider's `ReScanCustomScan`
/// method.
pub fn ExecReScanCustomScan<'mcx>(
    node: &mut CustomScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Assert(node->methods->ReScanCustomScan != NULL);
    // node->methods->ReScanCustomScan(node);
    provider::rescan_custom_scan::call(node, estate)
}

/// `ExecCustomMarkPos(node)` — forward to the provider's `MarkPosCustomScan`
/// method, erroring with `ERRCODE_FEATURE_NOT_SUPPORTED` if the provider does
/// not support mark/restore.
pub fn ExecCustomMarkPos<'mcx>(
    node: &mut CustomScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // if (!node->methods->MarkPosCustomScan)
    //     ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
    //             errmsg("custom scan \"%s\" does not support MarkPos",
    //                    node->methods->CustomName)));
    // node->methods->MarkPosCustomScan(node);
    if !has_mark_pos(node) {
        return Err(unsupported_markpos(custom_name(node)));
    }
    provider::mark_pos_custom_scan::call(node, estate)
}

/// `ExecCustomRestrPos(node)` — forward to the provider's `RestrPosCustomScan`
/// method, erroring with `ERRCODE_FEATURE_NOT_SUPPORTED` if the provider does
/// not support mark/restore. (The C message text deliberately says "MarkPos".)
pub fn ExecCustomRestrPos<'mcx>(
    node: &mut CustomScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // if (!node->methods->RestrPosCustomScan)
    //     ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
    //             errmsg("custom scan \"%s\" does not support MarkPos",
    //                    node->methods->CustomName)));
    // node->methods->RestrPosCustomScan(node);
    if !has_restr_pos(node) {
        return Err(unsupported_markpos(custom_name(node)));
    }
    provider::restr_pos_custom_scan::call(node, estate)
}

// ===========================================================================
//                          Parallel Scan Support
// ===========================================================================

/// `ExecCustomScanEstimate(node, pcxt)` — if the provider supports parallel
/// DSM, ask it for its coordination size and reserve the matching shared-memory
/// chunk/key.
pub fn ExecCustomScanEstimate<'mcx>(
    node: &mut CustomScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    pcxt: &mut ParallelContext,
) -> PgResult<()> {
    // const CustomExecMethods *methods = node->methods;
    // if (methods->EstimateDSMCustomScan) {
    if has_estimate_dsm(node) {
        // node->pscan_len = methods->EstimateDSMCustomScan(node, pcxt);
        node.pscan_len = provider::estimate_dsm_custom_scan::call(node, estate, pcxt)?;
        // shm_toc_estimate_chunk(&pcxt->estimator, node->pscan_len);
        let pscan_len = node.pscan_len;
        shm_toc::shm_toc_estimate_chunk::call(pcxt, pscan_len)?;
        // shm_toc_estimate_keys(&pcxt->estimator, 1);
        shm_toc::shm_toc_estimate_keys::call(pcxt, 1)?;
    }
    Ok(())
}

/// `ExecCustomScanInitializeDSM(node, pcxt)` — if the provider supports parallel
/// DSM, allocate its coordination area, let it initialize the area, and publish
/// the area in the TOC keyed by `plan_node_id`.
pub fn ExecCustomScanInitializeDSM<'mcx>(
    node: &mut CustomScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    pcxt: &mut ParallelContext,
) -> PgResult<()> {
    // const CustomExecMethods *methods = node->methods;
    // if (methods->InitializeDSMCustomScan) {
    if has_initialize_dsm(node) {
        // int plan_node_id = node->ss.ps.plan->plan_node_id;
        // coordinate = shm_toc_allocate(pcxt->toc, node->pscan_len);
        // methods->InitializeDSMCustomScan(node, pcxt, coordinate);
        // shm_toc_insert(pcxt->toc, plan_node_id, coordinate);
        //
        // The allocate / provider-init / insert of the DSM chunk are folded into
        // the provider seam (it receives `pcxt` and reads the node's
        // `plan_node_id` / `pscan_len`), since the chunk is a storage-owned
        // `void *` the node only brokers.
        provider::initialize_dsm_custom_scan::call(node, estate, pcxt)?;
    }
    Ok(())
}

/// `ExecCustomScanReInitializeDSM(node, pcxt)` — if the provider supports
/// parallel DSM, re-find its coordination area in the TOC and let it
/// reinitialize.
pub fn ExecCustomScanReInitializeDSM<'mcx>(
    node: &mut CustomScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    pcxt: &mut ParallelContext,
) -> PgResult<()> {
    // const CustomExecMethods *methods = node->methods;
    // if (methods->ReInitializeDSMCustomScan) {
    if has_reinitialize_dsm(node) {
        // coordinate = shm_toc_lookup(pcxt->toc, plan_node_id, false);
        // methods->ReInitializeDSMCustomScan(node, pcxt, coordinate);
        //
        // The TOC lookup of the chunk is folded into the provider seam (see
        // InitializeDSM).
        provider::reinitialize_dsm_custom_scan::call(node, estate, pcxt)?;
    }
    Ok(())
}

/// `ExecCustomScanInitializeWorker(node, pwcxt)` — in a parallel worker, re-find
/// the provider's coordination area in the worker TOC and let it attach.
pub fn ExecCustomScanInitializeWorker<'mcx>(
    node: &mut CustomScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    pwcxt: &mut ParallelWorkerContext,
) -> PgResult<()> {
    // const CustomExecMethods *methods = node->methods;
    // if (methods->InitializeWorkerCustomScan) {
    if has_initialize_worker(node) {
        // coordinate = shm_toc_lookup(pwcxt->toc, plan_node_id, false);
        // methods->InitializeWorkerCustomScan(node, pwcxt->toc, coordinate);
        provider::initialize_worker_custom_scan::call(node, estate, pwcxt)?;
    }
    Ok(())
}

/// `ExecShutdownCustomScan(node)` — if the provider supplies a shutdown
/// callback, invoke it (releases parallel-worker-held resources before the
/// workers exit).
pub fn ExecShutdownCustomScan<'mcx>(
    node: &mut CustomScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // const CustomExecMethods *methods = node->methods;
    // if (methods->ShutdownCustomScan) methods->ShutdownCustomScan(node);
    if has_shutdown(node) {
        provider::shutdown_custom_scan::call(node, estate)?;
    }
    Ok(())
}

// ===========================================================================
// In-crate helpers: owned-data reads (the `if (methods->X)` presence checks and
// the `CustomName` accessor), and the error-message constructor.
// ===========================================================================

/// `ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
/// errmsg("custom scan \"%s\" does not support MarkPos", name)))`. The errmsg
/// string materializes into the `PgError` (the analog of formatting into the
/// `ErrorData`), so the `format!` allocation is the error-message construction
/// at a return-`Err` site.
fn unsupported_markpos(name: &str) -> PgError {
    PgError::error(format!(
        "custom scan \"{name}\" does not support MarkPos"
    ))
    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

/// `node->methods->CustomName`, for error messages. Returns `"?"` if the
/// methods table or the name is absent (never expected for a live node).
#[inline]
fn custom_name<'a>(node: &'a CustomScanState<'_>) -> &'a str {
    node.methods
        .as_ref()
        .and_then(|m| m.CustomName.as_deref())
        .unwrap_or("?")
}

/// `node->methods->MarkPosCustomScan != NULL`.
#[inline]
fn has_mark_pos(node: &CustomScanState<'_>) -> bool {
    node.methods
        .as_ref()
        .is_some_and(|m| m.has_mark_pos_custom_scan)
}

/// `node->methods->RestrPosCustomScan != NULL`.
#[inline]
fn has_restr_pos(node: &CustomScanState<'_>) -> bool {
    node.methods
        .as_ref()
        .is_some_and(|m| m.has_restr_pos_custom_scan)
}

/// `methods->EstimateDSMCustomScan != NULL`.
#[inline]
fn has_estimate_dsm(node: &CustomScanState<'_>) -> bool {
    node.methods
        .as_ref()
        .is_some_and(|m| m.has_estimate_dsm_custom_scan)
}

/// `methods->InitializeDSMCustomScan != NULL`.
#[inline]
fn has_initialize_dsm(node: &CustomScanState<'_>) -> bool {
    node.methods
        .as_ref()
        .is_some_and(|m| m.has_initialize_dsm_custom_scan)
}

/// `methods->ReInitializeDSMCustomScan != NULL`.
#[inline]
fn has_reinitialize_dsm(node: &CustomScanState<'_>) -> bool {
    node.methods
        .as_ref()
        .is_some_and(|m| m.has_reinitialize_dsm_custom_scan)
}

/// `methods->InitializeWorkerCustomScan != NULL`.
#[inline]
fn has_initialize_worker(node: &CustomScanState<'_>) -> bool {
    node.methods
        .as_ref()
        .is_some_and(|m| m.has_initialize_worker_custom_scan)
}

/// `methods->ShutdownCustomScan != NULL`.
#[inline]
fn has_shutdown(node: &CustomScanState<'_>) -> bool {
    node.methods
        .as_ref()
        .is_some_and(|m| m.has_shutdown_custom_scan)
}
