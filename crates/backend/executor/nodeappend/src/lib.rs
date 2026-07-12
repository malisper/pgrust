// nodeAppend.c, sync-sequential + parallel-aware slices with runtime
// partition pruning: async lanes are loud.
#![allow(non_snake_case)]

use std::sync::{Arc, Mutex, MutexGuard};

use ::execpartition::pruning::PartitionPruneState;
use ::executils::{EStateData, ExecSlotId};
use ::types_error::PgResult;
use ::types_nodes::bitmapset::Bitmapset;
use ::types_nodes::plannodes::Append;
use ::types_scan::ScanDirection;
use ::types_slot::EXEC_FLAG_MARK;

pub fn init_seams() {}

const INVALID_SUBPLAN_INDEX: i32 = -1;

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
}

pub fn exec_init_append<'mcx>(
    node: &'mcx Append<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    nplans: usize,
    first_partial_plan: i32,
    prune_state: Option<Box<PartitionPruneState<'mcx>>>,
) -> PgResult<AppendState<'mcx>> {
    debug_assert!(eflags & EXEC_FLAG_MARK == 0);
    if node.nasyncplans != 0 {
        panic!("ExecInitAppend (nodeAppend.c): async-capable subplans not ported");
    }
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
    };
    let do_exec_prune = st.as_prune_state.as_ref().is_some_and(|p| p.do_exec_prune);
    if !do_exec_prune && nplans > 0 {
        ::partprune::bms_add_range(mcx, &mut st.as_valid_subplans, 0, nplans as i32 - 1)?;
        st.as_valid_subplans_identified = true;
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
    let mut whichplan = node.as_whichplan;
    if whichplan == INVALID_SUBPLAN_INDEX {
        identify_valid_subplans(node, estate)?;
        whichplan = -1;
    }
    debug_assert!(whichplan >= -1 && whichplan <= node.as_nplans as i32);
    let nextplan = if estate.es_direction == ScanDirection::ForwardScanDirection {
        node.as_valid_subplans.next_member(whichplan)
    } else {
        node.as_valid_subplans.prev_member(whichplan)
    };
    if nextplan < 0 {
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

/// `ExecAppend` (sync arm).
pub fn exec_append<'mcx, F>(
    node: &mut AppendState<'mcx>,
    estate: &mut EStateData<'mcx>,
    mut fetch_subplan: F,
) -> PgResult<Option<ExecSlotId>>
where
    F: FnMut(&mut EStateData<'mcx>, usize) -> PgResult<Option<ExecSlotId>>,
{
    if estate.es_direction != ScanDirection::ForwardScanDirection
        && !matches!(node.mode, ChooseMode::Local)
    {
        panic!("ExecAppend (nodeAppend.c): backward scan of a parallel Append");
    }
    if !node.as_begun {
        debug_assert!(node.as_whichplan == INVALID_SUBPLAN_INDEX);
        if node.as_nplans == 0 {
            return Ok(None);
        }
        if !choose_next_subplan(node, estate)? {
            return Ok(None);
        }
        node.as_begun = true;
    }
    loop {
        if init_small::globals::InterruptPending() {
            postgres_seams::check_for_interrupts::call()?;
        }
        let whichplan = node.as_whichplan;
        debug_assert!(whichplan >= 0 && (whichplan as usize) < node.as_nplans);
        if let Some(slot) = fetch_subplan(estate, whichplan as usize)? {
            return Ok(Some(slot));
        }
        if !choose_next_subplan(node, estate)? {
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

pub fn exec_rescan_append(node: &mut AppendState<'_>) {
    node.as_whichplan = INVALID_SUBPLAN_INDEX;
    node.as_begun = false;
}

pub fn exec_rescan_append_chg<'mcx>(node: &mut AppendState<'mcx>, chg: &Bitmapset<'mcx>) {
    if let Some(ps) = node.as_prune_state.as_ref() {
        if chg.overlap(&ps.execparamids) {
            node.as_valid_subplans_identified = false;
            node.as_valid_subplans = Bitmapset::empty();
        }
    }
    exec_rescan_append(node);
}

mcx::forget_safe_nodrop!(ChooseMode);
// Exempt: as_prune_state is a droppy owner, released by exec_end_append;
// as_pstate is a plain Arc.
mcx::forget_safe_struct!(
    AppendState<'_> { plan, as_whichplan, as_begun, as_nplans, as_first_partial_plan,
        mode, as_valid_subplans_identified, as_valid_subplans; as_prune_state, as_pstate },
);
