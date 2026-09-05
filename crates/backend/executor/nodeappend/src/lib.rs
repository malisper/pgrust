// nodeAppend.c: sync-sequential, parallel-aware, and async-capable slices
// with runtime partition pruning.
#![allow(non_snake_case)]

use std::sync::{Arc, Mutex, MutexGuard};

use ::execpartition::pruning::PartitionPruneState;
use ::executils::{AsyncRequest, AsyncWaitCtx, EStateData, ExecSlotId};
use ::types_core::PGINVALID_SOCKET;
use ::types_error::PgResult;
use ::types_nodes::bitmapset::Bitmapset;
use ::types_nodes::plannodes::Append;
use ::types_scan::ScanDirection;
use ::types_slot::EXEC_FLAG_MARK;
use ::types_storage::waiteventset::{
    WaitEvent, WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_SOCKET_READABLE,
};

pub fn init_seams() {}

#[cfg(test)]
mod tests;

const INVALID_SUBPLAN_INDEX: i32 = -1;
const EVENT_BUFFER_SIZE: usize = 16;
// wait_event.h PG_WAIT_IPC; AppendReady is IPC row 0 (wait_event_names.txt).
const WAIT_EVENT_APPEND_READY: u32 = 0x0800_0000;

/// execAsync.c's requestee-side surface plus the sync child pull, provided by
/// the host executor (it alone names the child PlanState types); the
/// requestor half (ExecAsyncAppendResponse) runs here after each call.
pub trait AppendAsyncDriver<'mcx> {
    fn fetch_subplan(
        &mut self,
        estate: &mut EStateData<'mcx>,
        i: usize,
    ) -> PgResult<Option<ExecSlotId>>;
    fn async_request(
        &mut self,
        estate: &mut EStateData<'mcx>,
        areq: &mut AsyncRequest,
    ) -> PgResult<()>;
    fn async_configure_wait(
        &mut self,
        estate: &mut EStateData<'mcx>,
        areq: &mut AsyncRequest,
        wait: &AsyncWaitCtx,
    ) -> PgResult<()>;
    fn async_notify(
        &mut self,
        estate: &mut EStateData<'mcx>,
        areq: &mut AsyncRequest,
    ) -> PgResult<()>;
}

/// Adapter for hosts whose Appends never carry async children.
pub struct SyncOnlyDriver<F>(pub F);

impl<'mcx, F> AppendAsyncDriver<'mcx> for SyncOnlyDriver<F>
where
    F: FnMut(&mut EStateData<'mcx>, usize) -> PgResult<Option<ExecSlotId>>,
{
    fn fetch_subplan(
        &mut self,
        estate: &mut EStateData<'mcx>,
        i: usize,
    ) -> PgResult<Option<ExecSlotId>> {
        (self.0)(estate, i)
    }
    fn async_request(&mut self, _: &mut EStateData<'mcx>, _: &mut AsyncRequest) -> PgResult<()> {
        unreachable!("sync-only Append host dispatched an async request")
    }
    fn async_configure_wait(
        &mut self,
        _: &mut EStateData<'mcx>,
        _: &mut AsyncRequest,
        _: &AsyncWaitCtx,
    ) -> PgResult<()> {
        unreachable!("sync-only Append host dispatched an async request")
    }
    fn async_notify(&mut self, _: &mut EStateData<'mcx>, _: &mut AsyncRequest) -> PgResult<()> {
        unreachable!("sync-only Append host dispatched an async request")
    }
}

// ParallelAppendState (nodeAppend.c): the DSM-shared subplan claim table;
// the Mutex is C's pa_lock.
pub struct ParallelAppendState {
    shared: Mutex<PaShared>,
}

struct PaShared {
    pa_next_plan: i32,
    pa_finished: Vec<bool>,
}

impl ParallelAppendState {
    pub fn new(nplans: usize) -> Self {
        ParallelAppendState {
            shared: Mutex::new(PaShared { pa_next_plan: 0, pa_finished: vec![false; nplans] }),
        }
    }
    fn lock(&self) -> MutexGuard<'_, PaShared> {
        self.shared.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[derive(Clone, Copy, PartialEq)]
enum ChooseMode {
    Local,
    Leader,
    Worker,
}

pub struct AppendState<'mcx> {
    pub plan: &'mcx Append<'mcx>,
    as_whichplan: i32,
    as_begun: bool,
    as_nplans: usize,
    as_first_partial_plan: i32,
    as_pstate: Option<Arc<ParallelAppendState>>,
    mode: ChooseMode,
    as_prune_state: Option<Box<PartitionPruneState<'mcx>>>,
    as_valid_subplans_identified: bool,
    as_valid_subplans: Bitmapset<'mcx>,
    as_syncdone: bool,
    as_nasyncplans: i32,
    as_asyncplans: Bitmapset<'mcx>,
    // Indexed by compacted subplan index; None on sync slots.
    as_asyncrequests: ::mcx::PgVec<'mcx, Option<AsyncRequest>>,
    // LIFO of not-yet-returned async results (C as_asyncresults+count).
    as_asyncresults: ::mcx::PgVec<'mcx, ExecSlotId>,
    as_needrequest: Bitmapset<'mcx>,
    as_valid_asyncplans: Bitmapset<'mcx>,
    as_nasyncremain: i32,
}

pub fn exec_init_append<'mcx>(
    node: &'mcx Append<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    nplans: usize,
    first_partial_plan: i32,
    prune_state: Option<Box<PartitionPruneState<'mcx>>>,
    asyncplans: Bitmapset<'mcx>,
    nasyncplans: i32,
) -> PgResult<AppendState<'mcx>> {
    debug_assert!(eflags & EXEC_FLAG_MARK == 0);
    debug_assert!(nasyncplans == asyncplans.num_members());
    let mcx = estate.es_query_cxt;
    let mut st = AppendState {
        plan: node,
        as_whichplan: INVALID_SUBPLAN_INDEX,
        as_begun: false,
        as_nplans: nplans,
        as_first_partial_plan: first_partial_plan,
        as_pstate: None,
        mode: ChooseMode::Local,
        as_prune_state: prune_state,
        as_valid_subplans_identified: false,
        as_valid_subplans: Bitmapset::empty(),
        as_syncdone: false,
        as_nasyncplans: nasyncplans,
        as_asyncplans: asyncplans,
        as_asyncrequests: ::mcx::PgVec::new_in(mcx),
        as_asyncresults: ::mcx::PgVec::new_in(mcx),
        as_needrequest: Bitmapset::empty(),
        as_valid_asyncplans: Bitmapset::empty(),
        as_nasyncremain: 0,
    };
    let do_exec_prune = st.as_prune_state.as_ref().is_some_and(|p| p.do_exec_prune);
    if !do_exec_prune && nplans > 0 {
        ::partprune::bms_add_range(mcx, &mut st.as_valid_subplans, 0, nplans as i32 - 1)?;
        st.as_valid_subplans_identified = true;
    }
    if nasyncplans > 0 {
        st.as_asyncrequests.try_reserve_exact(nplans).map_err(|_| mcx.oom(nplans))?;
        for i in 0..nplans {
            st.as_asyncrequests.push(if st.as_asyncplans.is_member(i as i32) {
                Some(AsyncRequest {
                    requestor_plan_id: node.plan.plan_node_id,
                    request_index: i as i32,
                    callback_pending: false,
                    request_complete: false,
                    result: None,
                })
            } else {
                None
            });
        }
        st.as_asyncresults
            .try_reserve_exact(nasyncplans as usize)
            .map_err(|_| mcx.oom(nasyncplans as usize))?;
        if st.as_valid_subplans_identified {
            classify_matching_subplans(&mut st, mcx)?;
        }
    }
    Ok(st)
}

fn identify_valid_subplans<'mcx>(
    node: &mut AppendState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if !node.as_valid_subplans_identified {
        let ps = node
            .as_prune_state
            .as_mut()
            .expect("unidentified valid set implies an exec prune state");
        node.as_valid_subplans =
            ::execpartition::pruning::exec_find_matching_subplans(ps, estate, false, None)?;
        node.as_valid_subplans_identified = true;
    }
    Ok(())
}

// choose_next_subplan_locally (nodeAppend.c).
fn choose_next_subplan_locally<'mcx>(
    node: &mut AppendState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    debug_assert!(node.as_nplans > 0);
    if node.as_syncdone {
        return Ok(false);
    }
    let mut whichplan = node.as_whichplan;
    if whichplan == INVALID_SUBPLAN_INDEX {
        if node.as_nasyncplans > 0 {
            // ExecAppendAsyncBegin already filled as_valid_subplans.
            debug_assert!(node.as_valid_subplans_identified);
        } else {
            identify_valid_subplans(node, estate)?;
        }
        whichplan = -1;
    }
    debug_assert!(whichplan >= -1 && whichplan <= node.as_nplans as i32);
    let nextplan = if estate.es_direction == ScanDirection::ForwardScanDirection {
        node.as_valid_subplans.next_member(whichplan)
    } else {
        node.as_valid_subplans.prev_member(whichplan)
    };
    if nextplan < 0 {
        if node.as_nasyncplans > 0 {
            node.as_syncdone = true;
        }
        return Ok(false);
    }
    node.as_whichplan = nextplan;
    Ok(true)
}

// mark_invalid_subplans_as_finished (nodeAppend.c).
fn mark_invalid_subplans_as_finished(node: &AppendState<'_>, shared: &mut PaShared) {
    debug_assert!(node.as_prune_state.is_some());
    if node.as_valid_subplans.num_members() as usize == node.as_nplans {
        return;
    }
    for i in 0..node.as_nplans {
        if !node.as_valid_subplans.is_member(i as i32) {
            shared.pa_finished[i] = true;
        }
    }
}

// choose_next_subplan_for_leader (nodeAppend.c): the leader walks backward
// from the cheapest (last) subplan so workers keep the expensive ones.
fn choose_next_subplan_for_leader<'mcx>(
    node: &mut AppendState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    debug_assert!(node.as_nplans > 0);
    let starting = node.as_whichplan == INVALID_SUBPLAN_INDEX;
    if starting {
        // Identify before taking the lock: pruning may error.
        identify_valid_subplans(node, estate)?;
    }
    let pstate = Arc::clone(node.as_pstate.as_ref().expect("parallel append shared state"));
    let mut shared = pstate.lock();
    if !starting {
        shared.pa_finished[node.as_whichplan as usize] = true;
    } else {
        node.as_whichplan = node.as_nplans as i32 - 1;
        if node.as_prune_state.is_some() {
            mark_invalid_subplans_as_finished(node, &mut shared);
        }
    }
    while shared.pa_finished[node.as_whichplan as usize] {
        if node.as_whichplan == 0 {
            shared.pa_next_plan = INVALID_SUBPLAN_INDEX;
            node.as_whichplan = INVALID_SUBPLAN_INDEX;
            return Ok(false);
        }
        node.as_whichplan -= 1;
    }
    if node.as_whichplan < node.as_first_partial_plan {
        shared.pa_finished[node.as_whichplan as usize] = true;
    }
    Ok(true)
}

// choose_next_subplan_for_worker (nodeAppend.c): non-partial plans first in
// descending cost order, then round-robin over the partial plans.
fn choose_next_subplan_for_worker<'mcx>(
    node: &mut AppendState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    debug_assert!(node.as_nplans > 0);
    let starting = node.as_whichplan == INVALID_SUBPLAN_INDEX;
    let newly_identified = starting && !node.as_valid_subplans_identified;
    if newly_identified {
        identify_valid_subplans(node, estate)?;
    }
    let pstate = Arc::clone(node.as_pstate.as_ref().expect("parallel append shared state"));
    let mut shared = pstate.lock();
    if !starting {
        shared.pa_finished[node.as_whichplan as usize] = true;
    } else if newly_identified {
        mark_invalid_subplans_as_finished(node, &mut shared);
    }
    if shared.pa_next_plan == INVALID_SUBPLAN_INDEX {
        return Ok(false);
    }
    node.as_whichplan = shared.pa_next_plan;
    while shared.pa_finished[shared.pa_next_plan as usize] {
        let nextplan = node.as_valid_subplans.next_member(shared.pa_next_plan);
        if nextplan >= 0 {
            shared.pa_next_plan = nextplan;
        } else if node.as_whichplan > node.as_first_partial_plan {
            // Loop back to the first valid partial plan, if any.
            let nextplan =
                node.as_valid_subplans.next_member(node.as_first_partial_plan - 1);
            shared.pa_next_plan = if nextplan < 0 { node.as_whichplan } else { nextplan };
        } else {
            shared.pa_next_plan = node.as_whichplan;
        }
        if shared.pa_next_plan == node.as_whichplan {
            shared.pa_next_plan = INVALID_SUBPLAN_INDEX;
            return Ok(false);
        }
    }
    node.as_whichplan = shared.pa_next_plan;
    shared.pa_next_plan = node.as_valid_subplans.next_member(shared.pa_next_plan);
    if shared.pa_next_plan < 0 {
        let nextplan = node.as_valid_subplans.next_member(node.as_first_partial_plan - 1);
        shared.pa_next_plan = if nextplan >= 0 { nextplan } else { INVALID_SUBPLAN_INDEX };
    }
    if node.as_whichplan < node.as_first_partial_plan {
        shared.pa_finished[node.as_whichplan as usize] = true;
    }
    Ok(true)
}

/// Lane-executor-v2 seam: true iff this Append chooses subplans with the
/// serial in-order chooser (`choose_next_subplan_locally`). The parallel
/// Leader/Worker choosers claim subplans through the shared DSM table in a
/// non-serial order, so the lane refuses them (execmain `lanev2` gate doc).
/// Mode is assigned at DSM/worker init, before the node's first pull.
pub fn lane_choose_local(node: &AppendState<'_>) -> bool {
    matches!(node.mode, ChooseMode::Local)
}

/// Lane-executor-v2 seam: true iff no async-capable children (the lane's
/// SyncOnlyDriver cannot serve async dispatch).
pub fn lane_no_async(node: &AppendState<'_>) -> bool {
    node.as_nasyncplans == 0
}

fn choose_next_subplan<'mcx>(
    node: &mut AppendState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    match node.mode {
        ChooseMode::Local => choose_next_subplan_locally(node, estate),
        ChooseMode::Leader => choose_next_subplan_for_leader(node, estate),
        ChooseMode::Worker => choose_next_subplan_for_worker(node, estate),
    }
}

/// `ExecAppend`.
pub fn exec_append<'mcx, D: AppendAsyncDriver<'mcx>>(
    node: &mut AppendState<'mcx>,
    estate: &mut EStateData<'mcx>,
    driver: &mut D,
) -> PgResult<Option<ExecSlotId>> {
    if estate.es_direction != ScanDirection::ForwardScanDirection
        && !matches!(node.mode, ChooseMode::Local)
    {
        panic!("ExecAppend (nodeAppend.c): backward scan of a parallel Append");
    }
    if !node.as_begun {
        debug_assert!(node.as_whichplan == INVALID_SUBPLAN_INDEX);
        debug_assert!(!node.as_syncdone);
        if node.as_nplans == 0 {
            return Ok(None);
        }
        if node.as_nasyncplans > 0 {
            exec_append_async_begin(node, estate, driver)?;
        }
        if !choose_next_subplan(node, estate)? && node.as_nasyncremain == 0 {
            return Ok(None);
        }
        debug_assert!(
            node.as_syncdone
                || (node.as_whichplan >= 0 && (node.as_whichplan as usize) < node.as_nplans)
        );
        node.as_begun = true;
    }
    loop {
        if init_small::globals::InterruptPending() {
            postgres_seams::check_for_interrupts::call()?;
        }
        if node.as_syncdone || !node.as_needrequest.is_empty() {
            match exec_append_async_get_next(node, estate, driver)? {
                AsyncNext::Tuple(slot) => return Ok(Some(slot)),
                AsyncNext::Done => return Ok(None),
                AsyncNext::SyncContinue => {
                    debug_assert!(!node.as_syncdone);
                    debug_assert!(node.as_needrequest.is_empty());
                }
            }
        }
        let whichplan = node.as_whichplan;
        debug_assert!(whichplan >= 0 && (whichplan as usize) < node.as_nplans);
        if let Some(slot) = driver.fetch_subplan(estate, whichplan as usize)? {
            return Ok(Some(slot));
        }
        // Wait or poll for async events before the end-of-iteration check:
        // it might drain the remaining async subplans.
        if node.as_nasyncremain > 0 {
            exec_append_async_event_wait(node, estate, driver)?;
        }
        if !choose_next_subplan(node, estate)? && node.as_nasyncremain == 0 {
            return Ok(None);
        }
    }
}

/// `ExecAppendInitializeDSM` (leader side; Estimate is a no-op here).
pub fn exec_append_initialize_dsm(node: &mut AppendState<'_>) -> Arc<ParallelAppendState> {
    let ps = Arc::new(ParallelAppendState::new(node.as_nplans));
    node.as_pstate = Some(Arc::clone(&ps));
    node.mode = ChooseMode::Leader;
    ps
}

/// `ExecAppendReInitializeDSM`.
pub fn exec_append_reinitialize_dsm(node: &mut AppendState<'_>) {
    let ps = Arc::clone(node.as_pstate.as_ref().expect("parallel append shared state"));
    let mut shared = ps.lock();
    shared.pa_next_plan = 0;
    shared.pa_finished.iter_mut().for_each(|f| *f = false);
}

/// `ExecAppendInitializeWorker`.
pub fn exec_append_initialize_worker(
    node: &mut AppendState<'_>,
    pstate: Arc<ParallelAppendState>,
) {
    node.as_pstate = Some(pstate);
    node.mode = ChooseMode::Worker;
}

pub fn exec_end_append(node: &mut AppendState<'_>) {
    node.as_prune_state = None;
}

// ExecReScanAppend (nodeAppend.c), node-local half: the host rescans the
// subplans after this returns (C rescans them after the async reset too).
pub fn exec_rescan_append<'mcx, D: AppendAsyncDriver<'mcx>>(
    node: &mut AppendState<'mcx>,
    estate: &mut EStateData<'mcx>,
    driver: &mut D,
) -> PgResult<()> {
    // If there are any async subplans, reset async requests made for them.
    if node.as_nasyncplans > 0 {
        exec_append_async_reset(node, estate, driver)?;
    }
    node.as_whichplan = INVALID_SUBPLAN_INDEX;
    node.as_syncdone = false;
    node.as_begun = false;
    Ok(())
}

pub fn exec_rescan_append_chg<'mcx, D: AppendAsyncDriver<'mcx>>(
    node: &mut AppendState<'mcx>,
    estate: &mut EStateData<'mcx>,
    driver: &mut D,
    chg: &Bitmapset<'mcx>,
) -> PgResult<()> {
    if node.as_nasyncplans > 0 {
        exec_append_async_reset(node, estate, driver)?;
    }
    if let Some(ps) = node.as_prune_state.as_ref() {
        if chg.overlap(&ps.execparamids) {
            node.as_valid_subplans_identified = false;
            node.as_valid_subplans = Bitmapset::empty();
            node.as_valid_asyncplans = Bitmapset::empty();
        }
    }
    node.as_whichplan = INVALID_SUBPLAN_INDEX;
    node.as_syncdone = false;
    node.as_begun = false;
    Ok(())
}

// ExecAppendAsyncReset (nodeAppend.c:1128): drain the in-flight async
// requests (a LIMIT above may have abandoned them mid-fetch) before resetting
// them, so the requestees are left where C leaves them for the rescan.
fn exec_append_async_reset<'mcx, D: AppendAsyncDriver<'mcx>>(
    node: &mut AppendState<'mcx>,
    estate: &mut EStateData<'mcx>,
    driver: &mut D,
) -> PgResult<()> {
    // We should never be called when there are no async subplans.
    debug_assert!(node.as_nasyncplans > 0);
    // Drain pending async requests if any. We force the as_syncdone flag to
    // be true so that exec_append_async_event_wait waits until at least one
    // event occurs.
    node.as_syncdone = true;
    loop {
        // When called from exec_append_async_event_wait, postgres_fdw (and
        // possibly other FDWs) will skip configuration of events for pending
        // requests in some cases if as_needrequest isn't empty. To avoid
        // that, discard results we already have. Note that we need to do
        // this on every iteration, as the call to that function may produce
        // new results.
        node.as_asyncresults.clear();
        node.as_needrequest = Bitmapset::empty();
        let mut found = false;
        let mut i = node.as_asyncplans.next_member(-1);
        while i >= 0 {
            let areq = node.as_asyncrequests[i as usize].expect("async subplan has a request");
            if areq.callback_pending {
                found = true;
                break;
            }
            i = node.as_asyncplans.next_member(i);
        }
        if !found {
            break;
        }
        postgres_seams::check_for_interrupts::call()?;
        // Wait or poll for async events.
        exec_append_async_event_wait(node, estate, driver)?;
    }
    // Reset async requests.
    let mut i = node.as_asyncplans.next_member(-1);
    while i >= 0 {
        let areq = node.as_asyncrequests[i as usize]
            .as_mut()
            .expect("async subplan has a request");
        debug_assert!(!areq.callback_pending);
        areq.request_complete = false;
        areq.result = None;
        i = node.as_asyncplans.next_member(i);
    }
    // Reset state variables.
    debug_assert!(node.as_asyncresults.is_empty());
    debug_assert!(node.as_needrequest.is_empty());
    node.as_nasyncremain = 0;
    Ok(())
}

// ---- async halves (nodeAppend.c "Asynchronous Append Support") ----

enum AsyncNext {
    Tuple(ExecSlotId),
    /// All sync + async subplans exhausted (C's cleared result slot).
    Done,
    /// Async work drained; continue with the current sync subplan.
    SyncContinue,
}

// ExecAsyncRequest (execAsync.c) + the ExecAsyncResponse that follows it.
fn async_dispatch_request<'mcx, D: AppendAsyncDriver<'mcx>>(
    node: &mut AppendState<'mcx>,
    estate: &mut EStateData<'mcx>,
    driver: &mut D,
    i: usize,
) -> PgResult<()> {
    let mut areq = node.as_asyncrequests[i].expect("async subplan has a request");
    debug_assert!(areq.request_index == i as i32);
    driver.async_request(estate, &mut areq)?;
    node.as_asyncrequests[i] = Some(areq);
    exec_async_append_response(node, estate.es_query_cxt, i)
}

// ExecAsyncAppendResponse (nodeAppend.c).
fn exec_async_append_response<'mcx>(
    node: &mut AppendState<'mcx>,
    mcx: ::mcx::Mcx<'mcx>,
    i: usize,
) -> PgResult<()> {
    let areq = node.as_asyncrequests[i].expect("async subplan has a request");
    if !areq.request_complete {
        debug_assert!(areq.callback_pending);
        return Ok(());
    }
    match areq.result {
        None => {
            debug_assert!(!areq.callback_pending);
            node.as_nasyncremain -= 1;
        }
        Some(slot) => {
            debug_assert!((node.as_asyncresults.len() as i32) < node.as_nasyncplans);
            node.as_asyncresults.push(slot);
            // Ready for a new request, but don't launch it here (C's note).
            node.as_needrequest.add_member(mcx, areq.request_index)?;
        }
    }
    Ok(())
}

// ExecAppendAsyncBegin (nodeAppend.c).
fn exec_append_async_begin<'mcx, D: AppendAsyncDriver<'mcx>>(
    node: &mut AppendState<'mcx>,
    estate: &mut EStateData<'mcx>,
    driver: &mut D,
) -> PgResult<()> {
    // Backward scan is not supported by async-aware Appends.
    debug_assert!(estate.es_direction == ScanDirection::ForwardScanDirection);
    debug_assert!(node.as_nplans > 0);
    debug_assert!(node.as_nasyncplans > 0);
    if !node.as_valid_subplans_identified {
        identify_valid_subplans(node, estate)?;
        classify_matching_subplans(node, estate.es_query_cxt)?;
    }
    node.as_syncdone = node.as_valid_subplans.is_empty();
    node.as_nasyncremain = node.as_valid_asyncplans.num_members();
    if node.as_nasyncremain == 0 {
        return Ok(());
    }
    let mut i = node.as_valid_asyncplans.next_member(-1);
    while i >= 0 {
        debug_assert!(!node.as_asyncrequests[i as usize]
            .expect("async subplan has a request")
            .callback_pending);
        async_dispatch_request(node, estate, driver, i as usize)?;
        i = node.as_valid_asyncplans.next_member(i);
    }
    Ok(())
}

// ExecAppendAsyncGetNext (nodeAppend.c).
fn exec_append_async_get_next<'mcx, D: AppendAsyncDriver<'mcx>>(
    node: &mut AppendState<'mcx>,
    estate: &mut EStateData<'mcx>,
    driver: &mut D,
) -> PgResult<AsyncNext> {
    debug_assert!(node.as_nasyncremain > 0);
    if let Some(slot) = exec_append_async_request(node, estate, driver)? {
        return Ok(AsyncNext::Tuple(slot));
    }
    while node.as_nasyncremain > 0 {
        if init_small::globals::InterruptPending() {
            postgres_seams::check_for_interrupts::call()?;
        }
        exec_append_async_event_wait(node, estate, driver)?;
        if let Some(slot) = exec_append_async_request(node, estate, driver)? {
            return Ok(AsyncNext::Tuple(slot));
        }
        // Break from loop if there's any sync subplan that isn't complete.
        if !node.as_syncdone {
            break;
        }
    }
    if node.as_syncdone {
        debug_assert!(node.as_nasyncremain == 0);
        return Ok(AsyncNext::Done);
    }
    Ok(AsyncNext::SyncContinue)
}

// ExecAppendAsyncRequest (nodeAppend.c).
fn exec_append_async_request<'mcx, D: AppendAsyncDriver<'mcx>>(
    node: &mut AppendState<'mcx>,
    estate: &mut EStateData<'mcx>,
    driver: &mut D,
) -> PgResult<Option<ExecSlotId>> {
    if node.as_needrequest.is_empty() {
        debug_assert!(node.as_asyncresults.is_empty());
        return Ok(None);
    }
    if let Some(slot) = node.as_asyncresults.pop() {
        return Ok(Some(slot));
    }
    // Take the set out first: responses re-add ready subplans to a fresh one.
    let needrequest = std::mem::replace(&mut node.as_needrequest, Bitmapset::empty());
    let mut i = needrequest.next_member(-1);
    while i >= 0 {
        async_dispatch_request(node, estate, driver, i as usize)?;
        i = needrequest.next_member(i);
    }
    Ok(node.as_asyncresults.pop())
}

// ExecAppendAsyncEventWait (nodeAppend.c).
fn exec_append_async_event_wait<'mcx, D: AppendAsyncDriver<'mcx>>(
    node: &mut AppendState<'mcx>,
    estate: &mut EStateData<'mcx>,
    driver: &mut D,
) -> PgResult<()> {
    debug_assert!(node.as_nasyncremain > 0);
    let nevents = node.as_nasyncplans + 2;
    // -1: wait until at least one event; 0: poll without waiting.
    let timeout: i64 = if node.as_syncdone { -1 } else { 0 };
    let set = waiteventset::CreateWaitEventSet(nevents)?;
    // The set is an owner resource with no Drop: free on every path.
    let r = event_wait_body(node, estate, driver, set, nevents, timeout);
    waiteventset::FreeWaitEventSet(set);
    r
}

fn event_wait_body<'mcx, D: AppendAsyncDriver<'mcx>>(
    node: &mut AppendState<'mcx>,
    estate: &mut EStateData<'mcx>,
    driver: &mut D,
    set: ::types_storage::waiteventset::WaitEventSetHandle,
    nevents: i32,
    timeout: i64,
) -> PgResult<()> {
    waiteventset::AddWaitEventToSet(set, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, None, None)?;
    // Give each waiting subplan a chance to add an event.
    let mut i = node.as_asyncplans.next_member(-1);
    while i >= 0 {
        let mut areq =
            node.as_asyncrequests[i as usize].expect("async subplan has a request");
        if areq.callback_pending {
            let ctx =
                AsyncWaitCtx { set, needrequest_empty: node.as_needrequest.is_empty() };
            driver.async_configure_wait(estate, &mut areq, &ctx)?;
            node.as_asyncrequests[i as usize] = Some(areq);
            // C's complete_pending_request responds from inside the FDW; the
            // requestor half of that response lands here (no-op if pending).
            exec_async_append_response(node, estate.es_query_cxt, i as usize)?;
        }
        i = node.as_asyncplans.next_member(i);
    }
    if waiteventset::GetNumRegisteredWaitEvents(set) == 1 {
        return Ok(());
    }
    // Latch added AFTER the ConfigureWait calls: postgres_fdw probes
    // GetNumRegisteredWaitEvents==1 for "no other events" (C's order note).
    waiteventset::AddWaitEventToSet(
        set,
        WL_LATCH_SET,
        PGINVALID_SOCKET,
        init_small::globals::MyLatch(),
        None,
    )?;
    let nevents = (nevents as usize).min(EVENT_BUFFER_SIZE);
    let mut occurred = [WaitEvent::default(); EVENT_BUFFER_SIZE];
    let n = waiteventset::WaitEventSetWait(
        set,
        timeout,
        &mut occurred[..nevents],
        WAIT_EVENT_APPEND_READY,
    )?;
    for w in &occurred[..n.max(0) as usize] {
        if w.events & WL_SOCKET_READABLE != 0 {
            let i = w.user_data.expect("async subplan registered its request index") as usize;
            let mut areq = node.as_asyncrequests[i].expect("async subplan has a request");
            if areq.callback_pending {
                // Unset before dispatching: the callback may re-arm it.
                areq.callback_pending = false;
                driver.async_notify(estate, &mut areq)?;
                node.as_asyncrequests[i] = Some(areq);
                exec_async_append_response(node, estate.es_query_cxt, i)?;
            }
        }
        if w.events & WL_LATCH_SET != 0 {
            latch::ResetLatch(init_small::globals::MyLatch().expect("backend latch"));
            postgres_seams::check_for_interrupts::call()?;
        }
    }
    Ok(())
}

// classify_matching_subplans (nodeAppend.c): split as_valid_subplans into
// sync (kept there) and async (as_valid_asyncplans).
fn classify_matching_subplans<'mcx>(
    node: &mut AppendState<'mcx>,
    mcx: ::mcx::Mcx<'mcx>,
) -> PgResult<()> {
    debug_assert!(node.as_valid_subplans_identified);
    debug_assert!(node.as_valid_asyncplans.is_empty());
    if node.as_valid_subplans.is_empty() {
        node.as_syncdone = true;
        node.as_nasyncremain = 0;
        return Ok(());
    }
    if !node.as_valid_subplans.overlap(&node.as_asyncplans) {
        node.as_nasyncremain = 0;
        return Ok(());
    }
    let valid_asyncplans = node.as_asyncplans.intersect(&node.as_valid_subplans, mcx)?;
    // Adjust the valid subplans to contain sync subplans only.
    node.as_valid_subplans.del_members(&valid_asyncplans);
    node.as_valid_asyncplans = valid_asyncplans;
    Ok(())
}

mcx::forget_safe_nodrop!(ChooseMode);
// Exempt: as_prune_state is a droppy owner, released by exec_end_append;
// as_pstate is a plain Arc.
mcx::forget_safe_struct!(
    AppendState<'_> { plan, as_whichplan, as_begun, as_nplans, as_first_partial_plan,
        mode, as_valid_subplans_identified, as_valid_subplans, as_syncdone, as_nasyncplans,
        as_asyncplans, as_asyncrequests, as_asyncresults, as_needrequest,
        as_valid_asyncplans, as_nasyncremain; as_prune_state, as_pstate },
);
