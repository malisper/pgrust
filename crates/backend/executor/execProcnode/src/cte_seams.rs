//! Install bodies for the CteScan leader-aliased `cte_*` seam family
//! (declared in `backend-executor-execMain-seams`, nodeCtescan.c semantics).
//!
//! In C (`nodeCtescan.c`) several `CteScan` nodes can read one CTE. The first to
//! initialize becomes the "leader", owning a shared `Tuplestorestate *cte_table`
//! and the `bool eof_cte` flag; it publishes a pointer to itself in
//! `es_param_exec_vals[cteParam].value`, and every follower
//! (`scanstate->leader = DatumGetPointer(...)`) reaches the shared store through
//! that aliasing back-pointer. `scanstate->cteplanstate` is
//! `list_nth(es_subplanstates, ctePlanId - 1)` — the CTE subplan, shared by all
//! CteScans for the CTE.
//!
//! The owned model cannot hold a live `&mut` alias to another node, nor stash a
//! node pointer in a `Datum`. So — exactly as `ParamExecData.execPlan`'s C
//! `void *` became the [`ExecPlanLink`](::nodes::ExecPlanLink) index — the
//! leader's *shared-per-CTE* state is hoisted out of the leader node into
//! [`EState.es_cte_shared[cteParam]`](::nodes::execnodes::EStateData::es_cte_shared)
//! ([`CteSharedState`]). "Leader" is whoever first creates that entry; leader
//! and followers alike reach the shared `(cte_table, eof_cte)` by `cteParam`
//! index, and the CTE subplan by `ctePlanId` index into `es_subplanstates` (the
//! same precedent `nodeSubplan` uses to reach its child by `plan_id`). No alias,
//! no stub: the shared store is a real `Tuplestorestate`.
//!
//! These are not `execProcnode.c` functions; execMain owns the declarations.
//! They install here because this dispatch crate already owns the
//! `ExecInitCteScan` call site and can run the CTE subplan directly through
//! `exec_proc_node` (the `cteplanstate` taken out of `es_subplanstates`,
//! `nodeSubplan`-style), with the shared store driven through sort-storage's
//! real tuplestore API.

use ::mcx::PgBox;
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use ::nodes::execnodes::CteSharedState;
use ::nodes::nodectescan::{CteScan, CteScanState};
use nodes::{EStateData, PlanStateNode, SlotId, Tuplestorestate};

use execMain_seams as seams;
use execTuples_seams as execTuples;
use ::sort_storage::tuplestore;

fn internal(msg: &str) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

// ---------------------------------------------------------------------------
// Side-table accessors: the shared per-CTE state keyed by `cteParam`.
// ---------------------------------------------------------------------------

/// `node.cteParam` — the index of this node's shared [`CteSharedState`].
#[inline]
fn cte_param_idx(node: &CteScanState<'_>) -> PgResult<usize> {
    node.cte_param
        .and_then(|p| usize::try_from(p).ok())
        .ok_or_else(|| internal("CteScanState has no cteParam"))
}

/// `node.ctePlanId - 1` — the index of this CTE's subplan in `es_subplanstates`.
#[inline]
fn cte_plan_idx(node: &CteScanState<'_>) -> PgResult<usize> {
    node.cte_plan_id
        .and_then(|id| usize::try_from(id - 1).ok())
        .ok_or_else(|| internal("CteScanState has no cteplanstate"))
}

/// Grow `es_cte_shared` to cover `idx` (slots default `None`, the C unclaimed
/// state), then return `&mut es_cte_shared[idx]`.
fn shared_slot_mut<'a, 'mcx>(
    estate: &'a mut EStateData<'mcx>,
    idx: usize,
) -> &'a mut Option<CteSharedState<'mcx>> {
    while estate.es_cte_shared.len() <= idx {
        estate.es_cte_shared.push(None);
    }
    &mut estate.es_cte_shared[idx]
}

/// `&mut es_cte_shared[node.cteParam]`, which must already be claimed (the C
/// shared-state-must-exist invariant once a leader resolved).
fn shared_mut<'a, 'mcx>(
    node: &CteScanState<'mcx>,
    estate: &'a mut EStateData<'mcx>,
) -> PgResult<&'a mut CteSharedState<'mcx>> {
    let idx = cte_param_idx(node)?;
    estate
        .es_cte_shared
        .get_mut(idx)
        .and_then(|s| s.as_mut())
        .ok_or_else(|| internal("CteScan shared state not resolved"))
}

/// `&es_cte_shared[node.cteParam]` (read-only).
fn shared_ref<'a, 'mcx>(
    node: &CteScanState<'mcx>,
    estate: &'a EStateData<'mcx>,
) -> PgResult<&'a CteSharedState<'mcx>> {
    let idx = cte_param_idx(node)?;
    estate
        .es_cte_shared
        .get(idx)
        .and_then(|s| s.as_ref())
        .ok_or_else(|| internal("CteScan shared state not resolved"))
}

/// The shared `Tuplestorestate` (`node->leader->cte_table`), mutably; the leader
/// must have created it (`cte_tuplestore_begin_heap_leader`).
fn store_mut<'a, 'mcx>(
    node: &CteScanState<'mcx>,
    estate: &'a mut EStateData<'mcx>,
) -> PgResult<&'a mut Tuplestorestate<'mcx>> {
    shared_mut(node, estate)?
        .cte_table
        .as_deref_mut()
        .ok_or_else(|| internal("CteScan shared tuplestore is NULL"))
}

/// Move the shared store out of its side-entry (leaving `None`) so a tuplestore
/// op that *also* needs `&mut estate` (gettupleslot / puttupleslot, which form
/// or store a slot's tuple) can run without a self-alias. Pair with
/// [`put_store`]. Mirrors `nodeSubplan`'s `take_subplanstate`/`put_subplanstate`.
fn take_store<'mcx>(
    node: &CteScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, Tuplestorestate<'mcx>>> {
    shared_mut(node, estate)?
        .cte_table
        .take()
        .ok_or_else(|| internal("CteScan shared tuplestore is NULL"))
}

fn put_store<'mcx>(
    node: &CteScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    store: PgBox<'mcx, Tuplestorestate<'mcx>>,
) -> PgResult<()> {
    shared_mut(node, estate)?.cte_table = Some(store);
    Ok(())
}

// ---------------------------------------------------------------------------
// Init: link subplan, resolve leader, create / attach the shared store.
// ---------------------------------------------------------------------------

/// `scanstate->cteplanstate = list_nth(es_subplanstates, node->ctePlanId - 1)`.
/// The owned model records the `ctePlanId` identity on the node; the subplan is
/// reached by index at access time (it stays owned by `es_subplanstates`).
fn cte_link_plan_state<'mcx>(
    scanstate: &mut CteScanState<'mcx>,
    plan: &CteScan<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let idx = usize::try_from(plan.ctePlanId - 1)
        .map_err(|_| internal("CteScan ctePlanId out of range"))?;
    // Verify the subplan-state is present (the C list_nth dereferences it).
    if estate
        .es_subplanstates
        .get(idx)
        .and_then(|b| b.as_ref())
        .is_none()
    {
        return Err(internal("CteScan ctePlanId references no es_subplanstates entry"));
    }
    scanstate.cte_plan_id = Some(plan.ctePlanId);
    Ok(())
}

/// `ExecInitCteScan` Param-slot leader handshake. In C: read
/// `prmdata = &es_param_exec_vals[cteParam]`; if empty, this node is the leader
/// (publish `prmdata->value = PointerGetDatum(scanstate)`,
/// `scanstate->leader = scanstate`, return true); else
/// `scanstate->leader = DatumGetPointer(prmdata->value)`, return false.
///
/// Owned model: claim `es_cte_shared[cteParam]`. If unclaimed, create an empty
/// [`CteSharedState`] (this node is the leader → `is_leader = true`); otherwise
/// it already exists (follower → `is_leader = false`). The leader/follower split
/// is exactly "who created the entry", with no node aliasing.
fn cte_resolve_leader<'mcx>(
    scanstate: &mut CteScanState<'mcx>,
    plan: &CteScan<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let idx = usize::try_from(plan.cteParam)
        .map_err(|_| internal("CteScan cteParam out of range"))?;
    scanstate.cte_param = Some(plan.cteParam);
    let slot = shared_slot_mut(estate, idx);
    let is_leader = slot.is_none();
    if is_leader {
        *slot = Some(CteSharedState::default());
    }
    scanstate.is_leader = is_leader;
    Ok(is_leader)
}

/// Leader-only store creation (`ExecInitCteScan`):
/// `cte_table = tuplestore_begin_heap(true, false, work_mem);
/// tuplestore_set_eflags(cte_table, eflags)`. Stored in the shared side-entry.
fn cte_tuplestore_begin_heap_leader<'mcx>(
    scanstate: &mut CteScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let eflags = scanstate.eflags;
    // tuplestore_begin_heap(randomAccess=true, interXact=false, work_mem)
    let work_mem = init_small_seams::work_mem::call();
    let mut store = tuplestore::tuplestore_begin_heap(mcx, true, false, work_mem)?;
    tuplestore::tuplestore_set_eflags(&mut store, eflags)?;
    shared_mut(scanstate, estate)?.cte_table = Some(store);
    Ok(())
}

/// Follower read-pointer setup (`ExecInitCteScan`):
/// `readptr = tuplestore_alloc_read_pointer(leader->cte_table, eflags);
/// tuplestore_select_read_pointer(leader->cte_table, readptr);
/// tuplestore_rescan(leader->cte_table)`.
fn cte_tuplestore_alloc_read_pointer_follower<'mcx>(
    scanstate: &mut CteScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let eflags = scanstate.eflags;
    let store = store_mut(scanstate, estate)?;
    let readptr = tuplestore::tuplestore_alloc_read_pointer(store, eflags)?;
    tuplestore::tuplestore_select_read_pointer(store, readptr)?;
    tuplestore::tuplestore_rescan(store)?;
    scanstate.readptr = readptr;
    Ok(())
}

// ---------------------------------------------------------------------------
// Leader eof_cte get/set.
// ---------------------------------------------------------------------------

fn cte_leader_eof_cte<'mcx>(node: &CteScanState<'mcx>, estate: &EStateData<'mcx>) -> PgResult<bool> {
    Ok(shared_ref(node, estate)?.eof_cte)
}

fn cte_set_leader_eof_cte<'mcx>(
    node: &mut CteScanState<'mcx>,
    value: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    shared_mut(node, estate)?.eof_cte = value;
    Ok(())
}

fn cte_leader_is_self<'mcx>(node: &CteScanState<'mcx>) -> PgResult<bool> {
    Ok(node.is_leader)
}

// ---------------------------------------------------------------------------
// Shared-store tuplestore ops (`node->leader->cte_table`).
// ---------------------------------------------------------------------------

/// `tuplestore_select_read_pointer(leader->cte_table, node->readptr)`.
fn cte_tuplestore_select_read_pointer<'mcx>(
    node: &mut CteScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let readptr = node.readptr;
    tuplestore::tuplestore_select_read_pointer(store_mut(node, estate)?, readptr)
}

/// `tuplestore_ateof(leader->cte_table)`.
fn cte_tuplestore_ateof<'mcx>(node: &CteScanState<'mcx>, estate: &EStateData<'mcx>) -> PgResult<bool> {
    let store = shared_ref(node, estate)?
        .cte_table
        .as_deref()
        .ok_or_else(|| internal("CteScan shared tuplestore is NULL"))?;
    Ok(tuplestore::tuplestore_ateof(store))
}

/// `tuplestore_advance(leader->cte_table, forward)`.
fn cte_tuplestore_advance<'mcx>(
    node: &mut CteScanState<'mcx>,
    forward: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    tuplestore::tuplestore_advance(store_mut(node, estate)?, forward)
}

/// `tuplestore_gettupleslot(leader->cte_table, forward, /*copy*/true,
/// node->ss.ss_ScanTupleSlot)`. Takes the store out of the side-entry so the
/// real op can hold `&mut estate` (to form the slot tuple) without a self-alias.
fn cte_tuplestore_gettupleslot<'mcx>(
    node: &mut CteScanState<'mcx>,
    forward: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let slot = node
        .ss
        .ss_ScanTupleSlot
        .ok_or_else(|| internal("CteScan has no scan tuple slot"))?;
    let mut store = take_store(node, estate)?;
    let r = tuplestore::tuplestore_gettupleslot(&mut store, forward, true, slot, estate);
    put_store(node, estate, store)?;
    r
}

/// `tuplestore_puttupleslot(leader->cte_table, cteslot)`: append the CTE
/// subplan's just-returned tuple (the slot stashed by `cte_exec_proc_node`).
fn cte_tuplestore_puttupleslot<'mcx>(
    node: &mut CteScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let cteslot = shared_ref(node, estate)?
        .last_cte_slot
        .ok_or_else(|| internal("CteScan has no stashed subplan slot"))?;
    let mut store = take_store(node, estate)?;
    let r = tuplestore::tuplestore_puttupleslot(&mut store, cteslot, estate);
    put_store(node, estate, store)?;
    r
}

/// `tuplestore_rescan(leader->cte_table)`.
fn cte_tuplestore_rescan<'mcx>(
    node: &mut CteScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    tuplestore::tuplestore_rescan(store_mut(node, estate)?)
}

/// `tuplestore_clear(leader->cte_table)`.
fn cte_tuplestore_clear<'mcx>(
    node: &mut CteScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    tuplestore::tuplestore_clear(store_mut(node, estate)?);
    Ok(())
}

/// `tuplestore_end(node->cte_table)` (leader only): drop the shared store and
/// clear the side-entry.
fn cte_tuplestore_end<'mcx>(
    node: &mut CteScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if let Some(store) = shared_mut(node, estate)?.cte_table.take() {
        tuplestore::tuplestore_end(store);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// CTE subplan dispatch (`node->cteplanstate`), via es_subplanstates by index.
// ---------------------------------------------------------------------------

/// `cteslot = ExecProcNode(node->cteplanstate); !TupIsNull(cteslot)`. Runs the
/// CTE subplan one tuple. The subplan plan-state is owned by
/// `es_subplanstates[ctePlanId-1]`; take it out (leaving `None`), run it with a
/// live `&mut estate`, put it back — the `nodeSubplan` precedent. A non-null
/// returned slot is stashed in the shared side-entry's `last_cte_slot` for the
/// following puttupleslot / copy.
fn cte_exec_proc_node<'mcx>(
    node: &mut CteScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let idx = cte_plan_idx(node)?;
    // ExecProcNode(node->cteplanstate)
    let mut sub = estate
        .es_subplanstates
        .get_mut(idx)
        .and_then(|slot| slot.take())
        .ok_or_else(|| internal("CteScan cteplanstate not in es_subplanstates"))?;
    let res = run_subplan(&mut sub, estate);
    estate.es_subplanstates[idx] = Some(sub);
    let slot = res?;

    // TupIsNull(cteslot): ExecProcNode returns a non-NULL but *cleared* slot at
    // end-of-scan (e.g. ExecScan's filtered-out tail), so treat an empty slot as
    // the C NULL.
    match slot {
        Some(s) if !estate.slot(s).is_empty() => {
            shared_mut(node, estate)?.last_cte_slot = Some(s);
            Ok(true)
        }
        _ => Ok(false),
    }
}

/// `ExecProcNode(planstate)` over the CTE subplan tree. execProcnode owns the
/// real `ExecProcNode`, so call it directly rather than through the seam.
#[inline]
fn run_subplan<'mcx>(
    sub: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    crate::execProcnode_run_end::exec_proc_node(sub, estate)
}

/// `ExecCopySlot(node->ss.ss_ScanTupleSlot, cteslot)`: copy the CTE subplan's
/// stashed output tuple into this node's own scan slot (stable across other
/// readers advancing the subplan). Clears the stash after the copy (the C local
/// `cteslot` goes out of scope).
fn cte_copy_tuple_to_scan_slot<'mcx>(
    node: &mut CteScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let dst = node
        .ss
        .ss_ScanTupleSlot
        .ok_or_else(|| internal("CteScan has no scan tuple slot"))?;
    let src = shared_ref(node, estate)?
        .last_cte_slot
        .ok_or_else(|| internal("CteScan has no stashed subplan slot"))?;
    execTuples::exec_copy_slot::call(estate, dst, src)?;
    shared_mut(node, estate)?.last_cte_slot = None;
    Ok(())
}

// ---------------------------------------------------------------------------
// ReScan: `node->leader->cteplanstate->chgParam != NULL`.
// ---------------------------------------------------------------------------

/// `node->leader->cteplanstate->chgParam != NULL` (`ExecReScanCteScan`): does
/// the underlying CTE need a fresh scan? Reads the CTE subplan's `chgParam` via
/// the `ctePlanId` index into `es_subplanstates`.
fn cte_leader_cteplanstate_chgparam_set<'mcx>(
    node: &CteScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> PgResult<bool> {
    let idx = cte_plan_idx(node)?;
    let set = estate
        .es_subplanstates
        .get(idx)
        .and_then(|b| b.as_deref())
        .map(|ps| ps.ps_head().chgParam.is_some())
        .unwrap_or(false);
    Ok(set)
}

// ---------------------------------------------------------------------------
// Install.
// ---------------------------------------------------------------------------

pub(crate) fn init_seams() {
    seams::cte_link_plan_state::set(cte_link_plan_state);
    seams::cte_resolve_leader::set(cte_resolve_leader);
    seams::cte_tuplestore_begin_heap_leader::set(cte_tuplestore_begin_heap_leader);
    seams::cte_tuplestore_alloc_read_pointer_follower::set(cte_tuplestore_alloc_read_pointer_follower);
    seams::cte_leader_eof_cte::set(cte_leader_eof_cte);
    seams::cte_set_leader_eof_cte::set(cte_set_leader_eof_cte);
    seams::cte_leader_is_self::set(cte_leader_is_self);
    seams::cte_tuplestore_select_read_pointer::set(cte_tuplestore_select_read_pointer);
    seams::cte_tuplestore_ateof::set(cte_tuplestore_ateof);
    seams::cte_tuplestore_advance::set(cte_tuplestore_advance);
    seams::cte_tuplestore_gettupleslot::set(cte_tuplestore_gettupleslot);
    seams::cte_tuplestore_puttupleslot::set(cte_tuplestore_puttupleslot);
    seams::cte_tuplestore_rescan::set(cte_tuplestore_rescan);
    seams::cte_tuplestore_clear::set(cte_tuplestore_clear);
    seams::cte_tuplestore_end::set(cte_tuplestore_end);
    seams::cte_exec_proc_node::set(cte_exec_proc_node);
    seams::cte_copy_tuple_to_scan_slot::set(cte_copy_tuple_to_scan_slot);
    seams::cte_leader_cteplanstate_chgparam_set::set(cte_leader_cteplanstate_chgparam_set);
}
