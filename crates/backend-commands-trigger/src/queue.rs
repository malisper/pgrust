//! The AFTER-trigger event-queue machinery of `backend/commands/trigger.c`,
//! ported from PostgreSQL 18.3.
//!
//! The C file-static `afterTriggers` ([`AfterTriggers`]) is a transaction-scoped
//! backend-global; here it is a `thread_local!` (one backend == one thread),
//! reset by `AfterTriggerBeginXact`/`EndXact`, subxact push/pop, and the
//! executor's begin/end-query.  It is NOT owned by any `EState` and NOT a
//! `ResourceOwner` resource.
//!
//! Owned-events model (matching the F0 split):
//!   * Each queued event record carries owned data and re-resolves the
//!     relation/tuples at *fire* time by Oid + `ItemPointer` (never a live
//!     pointer).
//!   * The deferred event list is an owned [`EventList`] = a
//!     `Vec<AfterTriggerEventData>` plus a dedup `Vec<SharedRecord>` table. The
//!     C chunk-arena byte offset to the shared record (`AFTER_TRIGGER_OFFSET`)
//!     becomes the shared record's Vec index in those same low bits.
//!   * Subxact rollback truncates the events Vec back to its saved length.
//!
//! The queue is `'static`-storable: it uses owned `Vec`/`String` and a
//! lifetime-free [`SharedRecord`] (the F0 `AfterTriggerSharedData<'mcx>` carries
//! `'mcx` PgBox fields, which a thread-local global can't hold).  The
//! transition-table (`ats_table`) and FDW substrate are firing-gated, so the
//! reachable queue never sets them; `ats_modifiedcols` is kept as a sorted
//! member `Vec<i32>` so the dedup `bms_equal` comparison is faithful.

use std::cell::RefCell;

use types_core::primitive::Oid;
use types_core::xact::CommandId;
use types_nodes::trigger::{
    AfterTriggerEventData, AFTER_TRIGGER_DEFERRABLE, AFTER_TRIGGER_DONE,
    AFTER_TRIGGER_INITDEFERRED, AFTER_TRIGGER_IN_PROGRESS, AFTER_TRIGGER_OFFSET, TriggerFlags,
};

const INVALID_OID: Oid = 0;
/// `InvalidCommandId` (`c.h`) — `(CommandId) 0xFFFFFFFF`.
const INVALID_COMMAND_ID: CommandId = 0xFFFF_FFFF;

/// The lifetime-free shared-data record for the owned queue — the `'static`
/// analogue of `AfterTriggerSharedData` (`commands/trigger.c`).  `ats_table`
/// (transition-table access) is firing substrate and always absent here; the C
/// `Bitmapset *ats_modifiedcols` is the sorted member set.
#[derive(Clone, Debug, Default)]
pub struct SharedRecord {
    /// `TriggerEvent ats_event` — event type indicator + deferral bits.
    pub ats_event: u32,
    /// `Oid ats_tgoid` — the trigger's ID.
    pub ats_tgoid: Oid,
    /// `Oid ats_relid` — the relation it's on.
    pub ats_relid: Oid,
    /// `Oid ats_rolid` — role to execute the trigger.
    pub ats_rolid: Oid,
    /// `CommandId ats_firing_id` — ID for firing cycle (0 = not yet assigned).
    pub ats_firing_id: CommandId,
    /// `Bitmapset *ats_modifiedcols` — modified columns, as a sorted member set.
    pub ats_modifiedcols: Option<Vec<i32>>,
    /// `true` iff this event references transition-table state (the C
    /// `ats_table != NULL`).  Firing-gated; never set by the reachable queue.
    pub ats_has_table: bool,
}

/// Owned replacement for the C `AfterTriggerEventList` chunk-arena.
///
/// `events` is the flat per-event record array.  `shared` is the deduplicated
/// table of [`SharedRecord`]s; an event references its shared record by the Vec
/// index stored in the `AFTER_TRIGGER_OFFSET` low bits of `ate_flags`.
#[derive(Clone, Debug, Default)]
pub struct EventList {
    /// Per-event records, in queue order.
    pub events: Vec<AfterTriggerEventData>,
    /// Deduplicated shared-data records; indexed by an event's offset bits.
    pub shared: Vec<SharedRecord>,
}

impl EventList {
    /// Read the shared record an event points at (the owned form of the C
    /// `GetTriggerSharedData(event)` offset dereference).
    pub fn shared_of(&self, event: &AfterTriggerEventData) -> &SharedRecord {
        let idx = (event.ate_flags & AFTER_TRIGGER_OFFSET) as usize;
        &self.shared[idx]
    }
}

/// `SetConstraintTriggerData` (`commands/trigger.c`).
#[derive(Clone, Copy, Debug)]
pub struct SetConstraintTriggerData {
    pub sct_tgoid: Oid,
    pub sct_tgisdeferred: bool,
}

/// `SetConstraintStateData` (`commands/trigger.c`) — SET CONSTRAINTS state.
#[derive(Clone, Debug, Default)]
pub struct SetConstraintState {
    pub all_isset: bool,
    pub all_isdeferred: bool,
    pub numstates: i32,
    pub numalloc: i32,
    pub trigstates: Vec<SetConstraintTriggerData>,
}

/// `AfterTriggersQueryData` (`commands/trigger.c`) — per-query-level data.
///
/// `fdw_tuplestore`/`tables` (the transition/FDW subsidiary storage that
/// `AfterTriggerFreeQuery` releases) are firing-substrate; the reachable queue
/// never fills them, but `closed`-style teardown is a no-op `Drop` here.
#[derive(Clone, Debug, Default)]
pub struct QueryLevel {
    /// `AfterTriggerEventList events` — events queued by this query level.
    pub events: EventList,
    /// `true` once this query level's array slot has been initialized (mirrors
    /// `AfterTriggerEnlargeQueryState` zeroing of new slots).
    pub initialized: bool,
}

/// The saved per-subtransaction state pushed by `AfterTriggerBeginSubXact` and
/// consumed by `AfterTriggerEndSubXact`.  The C `AfterTriggersTransData` saved
/// the events head/tail pointers; the owned model restores by truncating to the
/// saved lengths.
#[derive(Clone, Debug, Default)]
pub struct SavedAfterTriggerState {
    /// Saved `SET CONSTRAINTS` state copy, or `None` if unchanged (C `state`).
    pub state: Option<SetConstraintState>,
    /// Saved length of `events.events` (C saved events head/tail pointers).
    pub events_len: usize,
    /// Saved length of `events.shared`.
    pub shared_len: usize,
    /// Saved `query_depth`.
    pub query_depth: i32,
    /// Saved `firing_counter`.
    pub firing_counter: CommandId,
    /// Whether this `trans_stack` slot was actually pushed.
    pub valid: bool,
}

/// `AfterTriggersData` (`commands/trigger.c`) — per-transaction AFTER-trigger
/// state, owned-tree.
#[derive(Clone, Debug, Default)]
pub struct AfterTriggers {
    /// `CommandId firing_counter` — next firing-cycle ID (mustn't be 0).
    pub firing_counter: CommandId,
    /// `SetConstraintState state` — active `SET CONSTRAINTS` state.
    pub state: Option<SetConstraintState>,
    /// `AfterTriggerEventList events` — the deferred (cross-query) event list.
    pub events: EventList,
    /// `AfterTriggersQueryData *query_stack` — per-query-level data.
    pub query_stack: Vec<QueryLevel>,
    /// `int query_depth` — current index into `query_stack` (`-1` if empty).
    pub query_depth: i32,
    /// `AfterTriggersTransData *trans_stack` — per-subtransaction saved data,
    /// indexed by transaction nest level.
    pub trans_stack: Vec<SavedAfterTriggerState>,
}

thread_local! {
    /// `static AfterTriggersData afterTriggers` (`commands/trigger.c`) — the
    /// transaction-scoped after-trigger backend-global.
    static AFTER_TRIGGERS: RefCell<AfterTriggers> = RefCell::new(AfterTriggers {
        firing_counter: INVALID_COMMAND_ID,
        state: None,
        events: EventList::default(),
        query_stack: Vec::new(),
        query_depth: -1,
        trans_stack: Vec::new(),
    });
}

/// Run `f` with mutable access to the thread-local `afterTriggers` global.
pub fn with_after_triggers<R>(f: impl FnOnce(&mut AfterTriggers) -> R) -> R {
    AFTER_TRIGGERS.with(|cell| f(&mut cell.borrow_mut()))
}

// ---------------------------------------------------------------------------
// afterTriggerCheckState (trigger.c:4008) — is this event's trigger DEFERRED?
// ---------------------------------------------------------------------------

/// `afterTriggerCheckState(AfterTriggerShared evtshared)` (trigger.c:4008).
pub fn after_trigger_check_state(at: &AfterTriggers, evtshared: &SharedRecord) -> bool {
    let tgoid = evtshared.ats_tgoid;

    // For not-deferrable triggers the state is always false.
    if (evtshared.ats_event & AFTER_TRIGGER_DEFERRABLE) == 0 {
        return false;
    }

    // If constraint state exists, SET CONSTRAINTS might have been executed
    // either for this trigger or for all triggers.
    if let Some(state) = at.state.as_ref() {
        for i in 0..(state.numstates as usize) {
            if state.trigstates[i].sct_tgoid == tgoid {
                return state.trigstates[i].sct_tgisdeferred;
            }
        }
        if state.all_isset {
            return state.all_isdeferred;
        }
    }

    // Otherwise return the default state for the trigger.
    (evtshared.ats_event & AFTER_TRIGGER_INITDEFERRED) != 0
}

// ---------------------------------------------------------------------------
// afterTriggerAddEvent (trigger.c:4078) — append + dedup into the shared table.
// ---------------------------------------------------------------------------

/// `afterTriggerAddEvent(events, event, evtshared)` (trigger.c:4078).
pub fn after_trigger_add_event(
    events: &mut EventList,
    mut event: AfterTriggerEventData,
    evtshared: &SharedRecord,
) {
    // Try to locate a matching shared-data record already in the list, scanning
    // newest-first (matching C's reverse scan from chunk->endfree).
    let mut found: Option<usize> = None;
    for (idx, newshared) in events.shared.iter().enumerate().rev() {
        if newshared.ats_tgoid == evtshared.ats_tgoid
            && newshared.ats_event == evtshared.ats_event
            && newshared.ats_firing_id == 0
            && newshared.ats_has_table == evtshared.ats_has_table
            && newshared.ats_relid == evtshared.ats_relid
            && newshared.ats_rolid == evtshared.ats_rolid
            && newshared.ats_modifiedcols == evtshared.ats_modifiedcols
        {
            found = Some(idx);
            break;
        }
    }

    let shared_idx = match found {
        Some(idx) => idx,
        None => {
            let mut newshared = evtshared.clone();
            newshared.ats_firing_id = 0; // just to be sure
            events.shared.push(newshared);
            events.shared.len() - 1
        }
    };

    debug_assert!(
        (shared_idx as TriggerFlags) <= AFTER_TRIGGER_OFFSET,
        "shared-record index exceeds AFTER_TRIGGER_OFFSET"
    );
    event.ate_flags &= !AFTER_TRIGGER_OFFSET;
    event.ate_flags |= shared_idx as TriggerFlags;

    events.events.push(event);
}

// ---------------------------------------------------------------------------
// afterTriggerRestoreEventList (trigger.c:4225) — truncate-to-saved-length.
// ---------------------------------------------------------------------------

/// `afterTriggerRestoreEventList(events, old_events)` (trigger.c:4225).
pub fn after_trigger_restore_event_list(
    events: &mut EventList,
    saved_events_len: usize,
    saved_shared_len: usize,
) {
    events.events.truncate(saved_events_len);
    events.shared.truncate(saved_shared_len);
}

// ---------------------------------------------------------------------------
// AfterTriggerBeginXact (trigger.c:5084).
// ---------------------------------------------------------------------------

/// `AfterTriggerBeginXact()` (trigger.c:5084) — called at transaction start.
pub fn after_trigger_begin_xact() {
    with_after_triggers(|at| {
        at.firing_counter = 1; // (CommandId) 1 — mustn't be 0
        at.query_depth = -1;
        at.state = None;
        at.events = EventList::default();
        at.query_stack.clear();
        at.trans_stack.clear();
    });
}

// ---------------------------------------------------------------------------
// AfterTriggerBeginQuery (trigger.c:5116).
// ---------------------------------------------------------------------------

/// `AfterTriggerBeginQuery()` (trigger.c:5116) — bump the query-stack depth.
pub fn after_trigger_begin_query() {
    with_after_triggers(|at| {
        at.query_depth += 1;
    });
}

// ---------------------------------------------------------------------------
// AfterTriggerEnlargeQueryState (trigger.c:5645).
// ---------------------------------------------------------------------------

/// `AfterTriggerEnlargeQueryState()` (trigger.c:5645) — ensure `query_stack`
/// has a slot for the current `query_depth`, initializing new entries to empty.
pub fn after_trigger_enlarge_query_state(at: &mut AfterTriggers) {
    debug_assert!(at.query_depth >= at.query_stack.len() as i32);
    let want = (at.query_depth + 1) as usize;
    while at.query_stack.len() < want {
        at.query_stack.push(QueryLevel {
            events: EventList::default(),
            initialized: true,
        });
    }
}

// ---------------------------------------------------------------------------
// AfterTriggerEndQuery early-exit leg (trigger.c:5136).
// ---------------------------------------------------------------------------

/// The query-depth decrement of the C `AfterTriggerEndQuery` early-exit path
/// (no event stack initialized).  Returns `true` if the fast path applied
/// (depth decremented, no firing needed); `false` if there is a real event
/// stack to fire/promote, which the firing leg handles
/// ([`crate::firing::after_trigger_end_query`]).
pub fn after_trigger_end_query_noevents(at: &mut AfterTriggers) -> bool {
    debug_assert!(at.query_depth >= 0);
    if at.query_depth >= at.query_stack.len() as i32 {
        at.query_depth -= 1;
        return true;
    }
    false
}

// ---------------------------------------------------------------------------
// AfterTriggerEndXact (trigger.c:5343).
// ---------------------------------------------------------------------------

/// `AfterTriggerEndXact(bool isCommit)` (trigger.c:5343) — throw away all state.
pub fn after_trigger_end_xact(_is_commit: bool) {
    with_after_triggers(|at| {
        at.events = EventList::default();
        at.trans_stack.clear();
        at.query_stack.clear();
        at.state = None;
        at.query_depth = -1;
    });
}

// ---------------------------------------------------------------------------
// AfterTriggerBeginSubXact (trigger.c:5391).
// ---------------------------------------------------------------------------

/// `AfterTriggerBeginSubXact()` (trigger.c:5391) — push current info into
/// `trans_stack[my_level]`.
pub fn after_trigger_begin_sub_xact(my_level: i32) {
    with_after_triggers(|at| {
        let lvl = my_level as usize;
        while at.trans_stack.len() <= lvl {
            at.trans_stack.push(SavedAfterTriggerState::default());
        }
        at.trans_stack[lvl] = SavedAfterTriggerState {
            state: None,
            events_len: at.events.events.len(),
            shared_len: at.events.shared.len(),
            query_depth: at.query_depth,
            firing_counter: at.firing_counter,
            valid: true,
        };
    });
}

// ---------------------------------------------------------------------------
// AfterTriggerEndSubXact (trigger.c:5439).
// ---------------------------------------------------------------------------

/// `AfterTriggerEndSubXact(bool isCommit)` (trigger.c:5439).
pub fn after_trigger_end_sub_xact(is_commit: bool, my_level: i32) {
    with_after_triggers(|at| {
        let lvl = my_level as usize;

        if is_commit {
            debug_assert!(lvl < at.trans_stack.len());
            at.trans_stack[lvl].state = None;
            debug_assert_eq!(at.query_depth, at.trans_stack[lvl].query_depth);
            return;
        }

        // Aborting. Subxact start may have failed before BeginSubXact ran.
        if lvl >= at.trans_stack.len() || !at.trans_stack[lvl].valid {
            return;
        }

        let saved = at.trans_stack[lvl].clone();

        // Release query-level storage for aborted queries, restore query_depth.
        while at.query_depth > saved.query_depth {
            if (at.query_depth as usize) < at.query_stack.len() {
                let qd = at.query_depth as usize;
                at.query_stack[qd] = QueryLevel::default();
            }
            at.query_depth -= 1;
        }
        debug_assert_eq!(at.query_depth, saved.query_depth);

        // Restore the global deferred-event list to its former length.
        after_trigger_restore_event_list(&mut at.events, saved.events_len, saved.shared_len);

        // Restore the trigger state if this subxact saved it.
        if let Some(state) = saved.state {
            at.state = Some(state);
        }
        at.trans_stack[lvl].state = None;

        // Un-mark any remaining deferred events marked DONE/IN_PROGRESS by this
        // subxact or a child (firing ID >= the saved firing_counter).
        let subxact_firing_id = saved.firing_counter;
        let n = at.events.events.len();
        for i in 0..n {
            let flags = at.events.events[i].ate_flags;
            if flags & (AFTER_TRIGGER_DONE | AFTER_TRIGGER_IN_PROGRESS) != 0 {
                let sidx = (flags & AFTER_TRIGGER_OFFSET) as usize;
                let firing_id = at.events.shared[sidx].ats_firing_id;
                if firing_id >= subxact_firing_id {
                    at.events.events[i].ate_flags &=
                        !(AFTER_TRIGGER_DONE | AFTER_TRIGGER_IN_PROGRESS);
                }
            }
        }
    });
}

// ---------------------------------------------------------------------------
// SetConstraintState* (trigger.c:5692-5758).
// ---------------------------------------------------------------------------

/// `SetConstraintStateCreate(int numalloc)` (trigger.c:5692).
pub fn set_constraint_state_create(numalloc: i32) -> SetConstraintState {
    let numalloc = if numalloc <= 0 { 1 } else { numalloc };
    SetConstraintState {
        all_isset: false,
        all_isdeferred: false,
        numstates: 0,
        numalloc,
        trigstates: Vec::with_capacity(numalloc as usize),
    }
}

/// `SetConstraintStateCopy(SetConstraintState origstate)` (trigger.c:5717).
pub fn set_constraint_state_copy(origstate: &SetConstraintState) -> SetConstraintState {
    let mut state = set_constraint_state_create(origstate.numstates);
    state.all_isset = origstate.all_isset;
    state.all_isdeferred = origstate.all_isdeferred;
    state.numstates = origstate.numstates;
    state
        .trigstates
        .extend_from_slice(&origstate.trigstates[..origstate.numstates as usize]);
    state
}

/// `SetConstraintStateAddItem(state, tgoid, tgisdeferred)` (trigger.c:5737).
pub fn set_constraint_state_add_item(
    state: &mut SetConstraintState,
    tgoid: Oid,
    tgisdeferred: bool,
) {
    if state.numstates >= state.numalloc {
        let mut newalloc = state.numalloc * 2;
        newalloc = newalloc.max(8);
        if (newalloc as usize) > state.trigstates.len() {
            state.trigstates.reserve(newalloc as usize - state.trigstates.len());
        }
        state.numalloc = newalloc;
        debug_assert!(state.numstates < state.numalloc);
    }
    state.trigstates.push(SetConstraintTriggerData {
        sct_tgoid: tgoid,
        sct_tgisdeferred: tgisdeferred,
    });
    state.numstates += 1;
}

// ---------------------------------------------------------------------------
// AfterTriggerPendingOnRel (trigger.c:6082).
// ---------------------------------------------------------------------------

/// `AfterTriggerPendingOnRel(Oid relid)` (trigger.c:6082).
pub fn after_trigger_pending_on_rel(relid: Oid) -> bool {
    with_after_triggers(|at| {
        if event_list_pending_on_rel(&at.events, relid) {
            return true;
        }
        let max = at.query_stack.len() as i32;
        let mut depth = 0;
        while depth <= at.query_depth && depth < max {
            if event_list_pending_on_rel(&at.query_stack[depth as usize].events, relid) {
                return true;
            }
            depth += 1;
        }
        false
    })
}

fn event_list_pending_on_rel(events: &EventList, relid: Oid) -> bool {
    for event in &events.events {
        if event.ate_flags & AFTER_TRIGGER_DONE != 0 {
            continue;
        }
        if events.shared_of(event).ats_relid == relid {
            return true;
        }
    }
    false
}

// ---------------------------------------------------------------------------
// Subxact lifecycle hooks that resolve the nest level via the xact.c seam.
// ---------------------------------------------------------------------------

/// `AfterTriggerBeginSubXact()` subxact-scope hook — fetch the nest level via
/// the xact.c seam, then push `trans_stack`.
pub fn after_trigger_begin_sub_xact_hook() -> types_error::PgResult<()> {
    let my_level = backend_access_transam_xact_seams::get_current_transaction_nest_level::call();
    after_trigger_begin_sub_xact(my_level);
    Ok(())
}

/// `AfterTriggerEndSubXact(isCommit)` subxact-scope hook.
pub fn after_trigger_end_sub_xact_hook(is_commit: bool) -> types_error::PgResult<()> {
    let my_level = backend_access_transam_xact_seams::get_current_transaction_nest_level::call();
    after_trigger_end_sub_xact(is_commit, my_level);
    Ok(())
}

/// Suppress dead-code warnings for the not-yet-wired SET CONSTRAINTS helpers
/// (`AfterTriggerSetState` is the deferred command leg; these are its parts).
#[allow(dead_code)]
fn _set_constraint_state_used() {
    let _ = (
        set_constraint_state_create as fn(i32) -> SetConstraintState,
        set_constraint_state_copy as fn(&SetConstraintState) -> SetConstraintState,
        set_constraint_state_add_item as fn(&mut SetConstraintState, Oid, bool),
        after_trigger_pending_on_rel as fn(Oid) -> bool,
    );
    let _ = INVALID_OID;
}
