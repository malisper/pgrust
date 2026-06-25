//! Port of `src/backend/postmaster/pmchild.c`: tracking of postmaster child
//! processes.
//!
//! The postmaster keeps track of all child processes so that when a process
//! exits it knows what kind of process it was and can clean up accordingly.
//! Every child process is allocated a [`PMChild`] struct from a fixed pool of
//! structs; the size of the pool is determined by the configured worker/backend
//! counts (`autovacuum_worker_slots`, `max_worker_processes`, `max_wal_senders`,
//! and `max_connections`).
//!
//! Dead-end backends are handled slightly differently: there is no limit on the
//! number of dead-end backends and they do not need unique IDs, so their
//! [`PMChild`] structs are allocated dynamically, not from a pool.
//!
//! The structures and functions in this file are private to the postmaster
//! process. But note that there is an array in shared memory, managed by
//! `pmsignal.c`, that mirrors this — the per-child `PMChildFlags[]` slot states.
//! This crate owns the slot *pools* (the freelists, the active list); pmsignal
//! owns the shared per-slot flag array. We coordinate via the
//! `MarkPostmasterChildSlot{Assigned,Unassigned}` calls into pmsignal, exactly
//! as the C does.
//!
//! ## Process-local model
//!
//! The C uses palloc'd [`PMChild`] structs threaded onto intrusive `dlist`
//! freelists / `ActiveChildList`. Since all of this is postmaster-private (one
//! thread, the postmaster), we mirror it with a process-local `RefCell` holding
//! a slab of [`PMChild`] entries plus `Vec`/`VecDeque` lists of slab indices
//! — the same idiom the bgworker port uses for `BackgroundWorkerList` (the
//! owning collection *is* the C `dlist`; the intrusive link is unused). Pool
//! slots live at fixed slab indices `0..num_pmchild_slots`; dead-end children
//! are appended dynamically.

#![allow(non_snake_case)]

use std::collections::VecDeque;
use std::sync::Mutex;

use ::utils_error::ereport;
use ::types_core::init::{BackendType, BACKEND_NUM_TYPES};
use ::types_error::{ErrorLocation, DEBUG2, ERROR};
use ::types_storage::MAX_IO_WORKERS;

const FILE: &str = "pmchild.c";

/// A struct representing an active postmaster child process
/// (`PMChild`, `postmaster/postmaster.h`).
///
/// This is used mainly to keep track of how many children we have and send them
/// appropriate signals when necessary. All postmaster child processes are
/// assigned a `PMChild` entry. That includes "normal" client sessions, but also
/// autovacuum workers, walsenders, background workers, and aux processes. (Note
/// that at the time of launch, walsenders are labeled `B_BACKEND`; they are
/// relabeled to `B_WAL_SENDER` upon noticing they've changed their
/// `PMChildFlags` entry.)
///
/// "dead-end" children are also allocated a `PMChild` entry: these are children
/// launched just for the purpose of sending a friendly rejection message to a
/// would-be client.
///
/// `child_slot` is an identifier that is unique across all running child
/// processes. It is used as an index into the `PMChildFlags` array. dead-end
/// children are not assigned a `child_slot` and have `child_slot == 0` (valid
/// `child_slot` ids start from 1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PMChild {
    /// `pid_t pid` — process id of backend.
    pub pid: i32,
    /// `int child_slot` — `PMChildSlot` for this backend, if any.
    pub child_slot: i32,
    /// `BackendType bkend_type` — child process flavor.
    pub bkend_type: BackendType,
    /// `struct RegisteredBgWorker *rw` — bgworker info, if this is a bgworker.
    ///
    /// The C field is a pointer into the postmaster-private
    /// `BackgroundWorkerList`. We carry the `rw_index` (the position of the
    /// registration in that list, the same identity the bgworker port uses);
    /// `None` is the C `NULL`. Set/read by postmaster.c (not yet ported).
    pub rw: Option<u32>,
    /// `bool bgworker_notify` — gets bgworker start/stop notifications.
    pub bgworker_notify: bool,
    // `dlist_node elem` — list link in the freelist / ActiveChildList. The
    // owning collections (see `PmChildState`) *are* the C dlists, so the
    // intrusive link is not materialized.
}

/// A freelist for one kind of child process
/// (`PMChildPool`, file-static in pmchild.c).
///
/// Separate pools are maintained for each kind so that, for example, launching
/// a lot of regular backends cannot prevent autovacuum or an aux process from
/// launching.
struct PMChildPool {
    /// `int size` — number of `PMChild` slots reserved for this kind.
    size: i32,
    /// `int first_slotno` — first slot belonging to this pool (1-based
    /// `child_slot`).
    first_slotno: i32,
    /// `dlist_head freelist` — currently unused `PMChild` entries, by slab
    /// index.
    freelist: VecDeque<usize>,
}

impl PMChildPool {
    const fn new() -> Self {
        PMChildPool {
            size: 0,
            first_slotno: 0,
            freelist: VecDeque::new(),
        }
    }
}

/// Postmaster-private state owned by pmchild.c: the slot slab, the per-kind
/// pools, the active-child list, and `num_pmchild_slots`.
struct PmChildState {
    /// The slab backing every live `PMChild`. Indices `0..num_pmchild_slots`
    /// are the fixed pool slots (allocated in `InitPostmasterChildSlots`).
    /// Dead-end children are appended at higher indices and freed on release;
    /// freed dead-end indices are reused.
    slab: Vec<Option<PMChild>>,
    /// `static PMChildPool pmchild_pools[BACKEND_NUM_TYPES]`.
    pools: Vec<PMChildPool>,
    /// `dlist_head ActiveChildList` — slab indices of active children
    /// (including dead-end children), in head-insertion order (head first).
    active_child_list: VecDeque<usize>,
    /// `NON_EXEC_STATIC int num_pmchild_slots = 0`.
    num_pmchild_slots: i32,
    /// Reusable slab indices freed by dead-end-child release (so the slab does
    /// not grow without bound). Pool slots are never returned here.
    free_deadend_slots: Vec<usize>,
}

/// The postmaster's pmchild bookkeeping. This is postmaster-process-private
/// state (the C file-static `pmchild_pools[]`/`num_pmchild_slots`/
/// `ActiveChildList`). Only the postmaster process ever mutates it; the `Mutex`
/// gives us a single process-global owner (and keeps the model sound under the
/// multi-threaded test harness — the real postmaster is single-threaded here).
static PMCHILD: Mutex<PmChildState> = Mutex::new(PmChildState {
    slab: Vec::new(),
    pools: Vec::new(),
    active_child_list: VecDeque::new(),
    num_pmchild_slots: 0,
    free_deadend_slots: Vec::new(),
});

// ---------------------------------------------------------------------------
// GUC / configuration reads (the C reads these globals directly).
// ---------------------------------------------------------------------------

/// `MaxConnections` (globals.c).
fn max_connections() -> i32 {
    init_small_seams::max_connections::call()
}

/// `max_wal_senders` (walsender.c GUC).
fn max_wal_senders() -> i32 {
    walsender_seams::max_wal_senders::call()
}

/// `autovacuum_worker_slots` (autovacuum.c GUC).
fn autovacuum_worker_slots() -> i32 {
    autovacuum_seams::autovacuum_worker_slots::call()
}

/// `max_worker_processes` (globals.c GUC).
fn max_worker_processes() -> i32 {
    init_small_seams::max_worker_processes::call()
}

/// `PostmasterChildName(btype)` (launch_backend.c) — used in DEBUG2 logging.
fn postmaster_child_name(btype: BackendType) -> &'static str {
    launch_backend_seams::postmaster_child_name::call(btype)
}

// ---------------------------------------------------------------------------
// elog helper — pmchild.c uses bare elog() with no errcode(), so these default
// to the level's SQLSTATE + errmsg_internal. `lineno` records the pmchild.c
// source line for parity with C `__LINE__`.
// ---------------------------------------------------------------------------

fn pmchild_loc(lineno: i32) -> ErrorLocation {
    ErrorLocation {
        filename: Some(FILE.to_string()),
        lineno,
        funcname: None,
    }
}

fn elog_error(msg: String, lineno: i32) -> ! {
    let _ = ereport(ERROR)
        .errmsg_internal(msg.clone())
        .finish(pmchild_loc(lineno));
    // ereport(ERROR) does not return in C (longjmp). Mirror with a loud abort
    // in case the error path ever returns here.
    panic!("{msg}");
}

fn elog_debug2(msg: String, lineno: i32) {
    let _ = ereport(DEBUG2)
        .errmsg_internal(msg)
        .finish(pmchild_loc(lineno));
}

// ---------------------------------------------------------------------------
// MaxLivePostmasterChildren
// ---------------------------------------------------------------------------

/// `MaxLivePostmasterChildren()` — reports the number of postmaster child
/// processes that can be active.
///
/// ```c
/// if (num_pmchild_slots == 0)
///     elog(ERROR, "PM child array not initialized yet");
/// return num_pmchild_slots;
/// ```
///
/// Includes all children except for dead-end children. This allows the array
/// in shared memory (`PMChildFlags`) to have a fixed maximum size.
pub fn MaxLivePostmasterChildren() -> i32 {
    let n = PMCHILD.lock().unwrap().num_pmchild_slots;
    if n == 0 {
        elog_error("PM child array not initialized yet".to_string(), 73);
    }
    n
}

// ---------------------------------------------------------------------------
// InitPostmasterChildSlots
// ---------------------------------------------------------------------------

/// `InitPostmasterChildSlots()` — initialize at postmaster startup.
///
/// Note: This is not called on crash restart. We rely on `PMChild` entries to
/// remain valid through the restart process. This is important because the
/// syslogger survives through the crash restart process, so we must not
/// invalidate its `PMChild` slot.
pub fn InitPostmasterChildSlots() {
    // The GUC reads below go through seams (which never re-enter pmchild), so we
    // build the pools/slab in locals first and publish them under the lock at
    // the end.
    {
        // Fresh pool array (BACKEND_NUM_TYPES entries, all zero size).
        let mut pools: Vec<PMChildPool> =
            (0..BACKEND_NUM_TYPES).map(|_| PMChildPool::new()).collect();

        // We allow more connections here than we can have backends because some
        // might still be authenticating; they might fail auth, or some existing
        // backend might exit before the auth cycle is completed. The exact
        // MaxConnections limit is enforced when a new backend tries to join the
        // PGPROC array.
        //
        // WAL senders start out as regular backends, so they share the same
        // pool.
        pools[BackendType::Backend as usize].size = 2 * (max_connections() + max_wal_senders());

        pools[BackendType::AutovacWorker as usize].size = autovacuum_worker_slots();
        pools[BackendType::BgWorker as usize].size = max_worker_processes();
        pools[BackendType::IoWorker as usize].size = MAX_IO_WORKERS;

        // There can be only one of each of these running at a time. They each
        // get their own pool of just one entry.
        pools[BackendType::AutovacLauncher as usize].size = 1;
        pools[BackendType::SlotsyncWorker as usize].size = 1;
        pools[BackendType::Archiver as usize].size = 1;
        pools[BackendType::BgWriter as usize].size = 1;
        pools[BackendType::Checkpointer as usize].size = 1;
        pools[BackendType::Startup as usize].size = 1;
        pools[BackendType::WalReceiver as usize].size = 1;
        pools[BackendType::WalSummarizer as usize].size = 1;
        pools[BackendType::WalWriter as usize].size = 1;
        pools[BackendType::Logger as usize].size = 1;

        // The rest of the pmchild_pools are left at zero size.

        // Count the total number of slots.
        let mut num_pmchild_slots: i32 = 0;
        for pool in &pools {
            num_pmchild_slots += pool.size;
        }

        // Initialize them.
        //
        // C: `slots = palloc(num_pmchild_slots * sizeof(PMChild))`.
        let mut slab: Vec<Option<PMChild>> = Vec::with_capacity(num_pmchild_slots.max(0) as usize);
        let mut slotno: i32 = 0;
        for btype in 0..BACKEND_NUM_TYPES {
            pools[btype].first_slotno = slotno + 1;
            // dlist_init(&pmchild_pools[btype].freelist);
            pools[btype].freelist.clear();

            for _ in 0..pools[btype].size {
                let slab_idx = slotno as usize;
                // slots[slotno].pid = 0; child_slot = slotno + 1; bkend_type =
                // B_INVALID; rw = NULL; bgworker_notify = false;
                slab.push(Some(PMChild {
                    pid: 0,
                    child_slot: slotno + 1,
                    bkend_type: BackendType::Invalid,
                    rw: None,
                    bgworker_notify: false,
                }));
                // dlist_push_tail(&pmchild_pools[btype].freelist, &slots[slotno].elem);
                pools[btype].freelist.push_back(slab_idx);
                slotno += 1;
            }
        }
        debug_assert!(slotno == num_pmchild_slots);

        // Initialize other structures.
        // dlist_init(&ActiveChildList);
        let mut state = PMCHILD.lock().unwrap();
        state.slab = slab;
        state.pools = pools;
        state.active_child_list = VecDeque::new();
        state.num_pmchild_slots = num_pmchild_slots;
        state.free_deadend_slots = Vec::new();
    }
}

// ---------------------------------------------------------------------------
// AssignPostmasterChildSlot
// ---------------------------------------------------------------------------

/// `AssignPostmasterChildSlot(btype)` — allocate a `PMChild` entry for a
/// postmaster child process of given type.
///
/// The entry is taken from the right pool for the type. The returned struct's
/// `child_slot` is unique among all active child processes. Returns `None` (the
/// C `NULL`) if the pool is exhausted.
pub fn AssignPostmasterChildSlot(btype: BackendType) -> Option<PMChild> {
    // The pool bookkeeping runs under the lock; the pmsignal update + DEBUG2 log
    // (which call out to other crates' seams) run after releasing it.
    let result = {
        let mut state = PMCHILD.lock().unwrap();

        // if (pmchild_pools[btype].size == 0)
        //     elog(ERROR, "cannot allocate a PMChild slot for backend type %d", btype);
        if state.pools[btype as usize].size == 0 {
            drop(state);
            elog_error(
                format!(
                    "cannot allocate a PMChild slot for backend type {}",
                    btype as u32
                ),
                168,
            );
        }

        // freelist = &pmchild_pools[btype].freelist;
        // if (dlist_is_empty(freelist)) return NULL;
        match state.pools[btype as usize].freelist.pop_front() {
            None => None,
            // pmchild = dlist_container(..., dlist_pop_head_node(freelist));
            Some(slab_idx) => {
                // pmchild->pid = 0; bkend_type = btype; rw = NULL;
                // bgworker_notify = true;
                {
                    let pmchild = state.slab[slab_idx]
                        .as_mut()
                        .expect("pmchild pool slab slot is live");
                    pmchild.pid = 0;
                    pmchild.bkend_type = btype;
                    pmchild.rw = None;
                    pmchild.bgworker_notify = true;
                }

                // pmchild->child_slot for each entry was initialized when the
                // array of slots was allocated. Sanity check it.
                let child_slot = state.slab[slab_idx].unwrap().child_slot;
                let first_slotno = state.pools[btype as usize].first_slotno;
                let pool_size = state.pools[btype as usize].size;
                if !(child_slot >= first_slotno && child_slot < first_slotno + pool_size) {
                    let bt = state.slab[slab_idx].unwrap().bkend_type;
                    drop(state);
                    elog_error(
                        format!("pmchild freelist for backend type {} is corrupt", bt as u32),
                        188,
                    );
                }

                // dlist_push_head(&ActiveChildList, &pmchild->elem);
                state.active_child_list.push_front(slab_idx);

                Some(state.slab[slab_idx].unwrap())
            }
        }
    };

    let pmchild = result?;

    // Update the status in the shared memory array.
    // MarkPostmasterChildSlotAssigned(pmchild->child_slot);
    pmsignal::MarkPostmasterChildSlotAssigned(pmchild.child_slot)
        .unwrap_or_else(|e| {
            panic!("MarkPostmasterChildSlotAssigned failed for slot {}: {e:?}", pmchild.child_slot)
        });

    elog_debug2(
        format!(
            "assigned pm child slot {} for {}",
            pmchild.child_slot,
            postmaster_child_name(btype)
        ),
        196,
    );

    Some(pmchild)
}

// ---------------------------------------------------------------------------
// AllocDeadEndChild
// ---------------------------------------------------------------------------

/// `AllocDeadEndChild()` — allocate a `PMChild` struct for a dead-end backend.
///
/// Dead-end children are not assigned a `child_slot` number. The struct is
/// allocated dynamically; returns `None` if out of memory (the C
/// `palloc_extended(..., MCXT_ALLOC_NO_OOM)` failure — never observed here, but
/// the signature preserves it).
pub fn AllocDeadEndChild() -> Option<PMChild> {
    // elog(DEBUG2, "allocating dead-end child");
    elog_debug2("allocating dead-end child".to_string(), 212);

    let pmchild = PMChild {
        pid: 0,
        child_slot: 0,
        bkend_type: BackendType::DeadEndBackend,
        rw: None,
        bgworker_notify: false,
    };

    {
        let mut state = PMCHILD.lock().unwrap();
        // pmchild = palloc_extended(...): allocate a fresh slab cell, reusing a
        // freed dead-end index when available.
        let slab_idx = match state.free_deadend_slots.pop() {
            Some(idx) => {
                state.slab[idx] = Some(pmchild);
                idx
            }
            None => {
                state.slab.push(Some(pmchild));
                state.slab.len() - 1
            }
        };
        // dlist_push_head(&ActiveChildList, &pmchild->elem);
        state.active_child_list.push_front(slab_idx);
    }

    Some(pmchild)
}

// ---------------------------------------------------------------------------
// ReleasePostmasterChildSlot
// ---------------------------------------------------------------------------

/// `ReleasePostmasterChildSlot(pmchild)` — release a `PMChild` slot after the
/// child process has exited.
///
/// Returns `true` if the child detached cleanly from shared memory, `false`
/// otherwise (see `MarkPostmasterChildSlotUnassigned`).
///
/// The C takes a `PMChild *` identifying the entry to release. We take the
/// [`PMChild`] by value (it is `Copy`) and locate its slab cell by identity
/// (`child_slot`/`bkend_type`), then run the same freelist/active-list and
/// pmsignal bookkeeping.
pub fn ReleasePostmasterChildSlot(pmchild: PMChild) -> bool {
    // Locate the slab cell + remove from ActiveChildList (dlist_delete).
    let slab_idx = {
        let mut state = PMCHILD.lock().unwrap();
        let idx = find_active_index(&state, &pmchild).unwrap_or_else(|| {
            // NB: `&state` (MutexGuard) derefs to &PmChildState here.
            panic!(
                "ReleasePostmasterChildSlot: PMChild (slot {}, type {:?}) not in ActiveChildList",
                pmchild.child_slot, pmchild.bkend_type
            )
        });
        // dlist_delete(&pmchild->elem);
        state.active_child_list.retain(|&i| i != idx);
        idx
    };

    if pmchild.bkend_type == BackendType::DeadEndBackend {
        // elog(DEBUG2, "releasing dead-end backend"); pfree(pmchild);
        elog_debug2("releasing dead-end backend".to_string(), 241);
        {
            let mut state = PMCHILD.lock().unwrap();
            state.slab[slab_idx] = None;
            state.free_deadend_slots.push(slab_idx);
        }
        return true;
    }

    // elog(DEBUG2, "releasing pm child slot %d", pmchild->child_slot);
    elog_debug2(format!("releasing pm child slot {}", pmchild.child_slot), 249);

    {
        let mut state = PMCHILD.lock().unwrap();

        // WAL senders start out as regular backends, and share the pool.
        let pool_btype = if pmchild.bkend_type == BackendType::WalSender {
            BackendType::Backend
        } else {
            pmchild.bkend_type
        };

        // sanity check that we return the entry to the right pool.
        let first_slotno = state.pools[pool_btype as usize].first_slotno;
        let pool_size = state.pools[pool_btype as usize].size;
        if !(pmchild.child_slot >= first_slotno && pmchild.child_slot < first_slotno + pool_size) {
            let bt = pmchild.bkend_type;
            drop(state);
            elog_error(
                format!("pmchild freelist for backend type {} is corrupt", bt as u32),
                262,
            );
        }

        // dlist_push_head(&pool->freelist, &pmchild->elem);
        state.pools[pool_btype as usize].freelist.push_front(slab_idx);
    }

    // return MarkPostmasterChildSlotUnassigned(pmchild->child_slot);
    pmsignal::MarkPostmasterChildSlotUnassigned(pmchild.child_slot)
}

// ---------------------------------------------------------------------------
// FindPostmasterChildByPid
// ---------------------------------------------------------------------------

/// `FindPostmasterChildByPid(pid)` — find the `PMChild` entry of a running
/// child process by PID.
///
/// ```c
/// dlist_foreach(iter, &ActiveChildList)
///     if (bp->pid == pid) return bp;
/// return NULL;
/// ```
pub fn FindPostmasterChildByPid(pid: i32) -> Option<PMChild> {
    let state = PMCHILD.lock().unwrap();
    for &idx in &state.active_child_list {
        if let Some(bp) = state.slab[idx] {
            if bp.pid == pid {
                return Some(bp);
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// In-place mutators on live ActiveChildList entries
//
// postmaster.c iterates `ActiveChildList` and mutates `PMChild` fields through
// the borrowed `PMChild *`. In this model the list + slab are pmchild-private,
// so the in-place writes are exposed as focused primitives keyed by the live
// entry's identity (its `pid`, unique among running children). These mirror the
// exact field writes postmaster.c performs while holding a `PMChild *`.
// ---------------------------------------------------------------------------

/// Find the active child with the given `pid` and set its `bgworker_notify`
/// flag to `true`, returning whether such a child was found.
///
/// This is the data-touching core of postmaster.c's
/// `PostmasterMarkPIDForWorkerNotify(pid)`:
///
/// ```c
/// dlist_foreach(iter, &ActiveChildList) {
///     bp = dlist_container(PMChild, elem, iter.cur);
///     if (bp->pid == pid) { bp->bgworker_notify = true; return true; }
/// }
/// return false;
/// ```
///
/// `ActiveChildList` is pmchild-private here, so the iterate-find-and-set runs
/// under the pmchild lock; postmaster.c's `PostmasterMarkPIDForWorkerNotify`
/// wrapper delegates to this.
pub fn MarkActiveChildBgworkerNotify(pid: i32) -> bool {
    let mut state = PMCHILD.lock().unwrap();
    // Collect the slab index first to satisfy the borrow checker (the active
    // list and the slab live in the same struct).
    let mut found_idx: Option<usize> = None;
    for &idx in &state.active_child_list {
        if let Some(bp) = state.slab[idx] {
            if bp.pid == pid {
                found_idx = Some(idx);
                break;
            }
        }
    }
    match found_idx {
        Some(idx) => {
            state.slab[idx]
                .as_mut()
                .expect("active child slab slot is live")
                .bgworker_notify = true;
            true
        }
        None => false,
    }
}

// ---------------------------------------------------------------------------
// ActiveChildList walk + in-place live-entry mutators (postmaster.c surface)
// ---------------------------------------------------------------------------
//
// postmaster.c reaches into the *live* `PMChild` entries it gets back from
// `AssignPostmasterChildSlot`/`AllocDeadEndChild` and the ones it walks in
// `ActiveChildList`:
//
//   * after assigning, it does `bn->pid = ...; bn->rw = rw; bn->bkend_type =
//     ...; bn->bgworker_notify = false;` on the entry that lives in the list
//     (StartChildProcess / StartBackgroundWorker / BackendStartup);
//   * `SignalChildren`/`CountChildren` `dlist_foreach(&ActiveChildList)` and may
//     relabel `bp->bkend_type = B_WAL_SENDER` in place;
//   * `PostmasterMarkPIDForWorkerNotify` walks the list and sets
//     `bp->bgworker_notify = true` on the matching entry.
//
// Our `PMChild` is returned by value (`Copy`), so a caller's copy is detached
// from the slab cell on the list. These accessors give the postmaster the
// missing C semantics: locate the live cell by identity and mutate it in place,
// and walk the list yielding a mutable view per entry. Identity mirrors
// `find_active_index`: pool slots key on `child_slot` (> 0); dead-end children
// (`child_slot == 0`) match by full value equality (the per-entry walk visitor
// is the faithful path for mutating those, since it carries the cell itself).

/// A mutable view of one live `ActiveChildList` entry, handed to the
/// [`for_each_active_child`] visitor. Reads expose the C `bp->field`; the
/// `set_*` methods mirror C's in-place `bp->field = ...` on the list entry.
pub struct PMChildRef<'a> {
    entry: &'a mut PMChild,
}

impl PMChildRef<'_> {
    /// `bp->pid`.
    pub fn pid(&self) -> i32 {
        self.entry.pid
    }
    /// `bp->child_slot`.
    pub fn child_slot(&self) -> i32 {
        self.entry.child_slot
    }
    /// `bp->bkend_type`.
    pub fn bkend_type(&self) -> BackendType {
        self.entry.bkend_type
    }
    /// `bp->rw`.
    pub fn rw(&self) -> Option<u32> {
        self.entry.rw
    }
    /// `bp->bgworker_notify`.
    pub fn bgworker_notify(&self) -> bool {
        self.entry.bgworker_notify
    }
    /// A `Copy` snapshot of the live entry (the C `PMChild *` dereferenced).
    pub fn get(&self) -> PMChild {
        *self.entry
    }

    /// `bp->pid = pid`.
    pub fn set_pid(&mut self, pid: i32) {
        self.entry.pid = pid;
    }
    /// `bp->bkend_type = bkend_type`.
    pub fn set_bkend_type(&mut self, bkend_type: BackendType) {
        self.entry.bkend_type = bkend_type;
    }
    /// `bp->rw = rw` (`None` is the C `NULL`).
    pub fn set_rw(&mut self, rw: Option<u32>) {
        self.entry.rw = rw;
    }
    /// `bp->bgworker_notify = bgworker_notify`.
    pub fn set_bgworker_notify(&mut self, bgworker_notify: bool) {
        self.entry.bgworker_notify = bgworker_notify;
    }
}

/// `dlist_foreach(iter, &ActiveChildList)` — walk every live child (pool and
/// dead-end) in head-first order, handing the visitor a [`PMChildRef`] that can
/// read and mutate the live list entry in place. This is the faithful stand-in
/// for the C `dlist_iter`/`dlist_container` walk used by `SignalChildren`,
/// `CountChildren`, and `PostmasterMarkPIDForWorkerNotify`.
///
/// The lock is held across the whole walk (the postmaster is single-threaded,
/// so this matches C exactly); the visitor must not re-enter pmchild.
pub fn for_each_active_child<F: FnMut(PMChildRef<'_>)>(mut f: F) {
    let mut state = PMCHILD.lock().unwrap();
    // Collect the head-first slab indices first to avoid borrowing `slab` and
    // `active_child_list` simultaneously (the order is the C dlist order).
    let indices: Vec<usize> = state.active_child_list.iter().copied().collect();
    for idx in indices {
        if let Some(entry) = state.slab[idx].as_mut() {
            f(PMChildRef { entry });
        }
    }
}

/// Locate the live `ActiveChildList` entry for `pmchild` (by the same identity
/// as [`find_active_index`]) and run `f` on a [`PMChildRef`] for it; returns
/// `true` if the entry was found. The faithful stand-in for mutating the C
/// `PMChild *` the postmaster holds after `AssignPostmasterChildSlot` /
/// `AllocDeadEndChild` (e.g. `bn->rw = rw; bn->bkend_type = ...`).
pub fn with_active_child<F: FnOnce(PMChildRef<'_>)>(pmchild: &PMChild, f: F) -> bool {
    let mut state = PMCHILD.lock().unwrap();
    match find_active_index(&state, pmchild) {
        Some(idx) => {
            if let Some(entry) = state.slab[idx].as_mut() {
                f(PMChildRef { entry });
                true
            } else {
                false
            }
        }
        None => false,
    }
}

/// Set the `pid` of a live active child identified by `child_slot`
/// (postmaster.c writes `bn->pid = pid` after a successful fork in
/// `BackendStartup`/`StartChildProcess`/`StartBackgroundWorker`). Returns
/// whether the entry was found. `child_slot` uniquely identifies a pool entry
/// (dead-end children never have their pid set this way).
pub fn SetActiveChildPid(child_slot: i32, pid: i32) -> bool {
    let mut state = PMCHILD.lock().unwrap();
    let mut found_idx: Option<usize> = None;
    for &idx in &state.active_child_list {
        if let Some(bp) = state.slab[idx] {
            if bp.child_slot != 0 && bp.child_slot == child_slot {
                found_idx = Some(idx);
                break;
            }
        }
    }
    match found_idx {
        Some(idx) => {
            state.slab[idx]
                .as_mut()
                .expect("active child slab slot is live")
                .pid = pid;
            true
        }
        None => false,
    }
}

/// Set the `pid` of a live active child identified by the `PMChild` entry
/// itself (the slab cell still matching `pmchild`), mirroring C's direct
/// `bn->pid = pid` assignment in `BackendStartup`. Unlike [`SetActiveChildPid`],
/// this works for dead-end children, which all carry `child_slot == 0` and so
/// cannot be matched by slot number — the just-forked one is located by
/// [`find_active_index`]'s full-entry equality (it still has `pid == 0` at the
/// call site, exactly matching its slab cell). Returns whether the entry was
/// found.
///
/// This is the load-bearing fix for the crash-restart abnormal-exit bug: if a
/// dead-end child never gets its pid set, it lingers in `ActiveChildList` with
/// `pid == 0`, and the next crash's `TerminateChildren` calls `kill(-0, SIGABRT)`
/// — which signals the postmaster's OWN process group, killing the cluster
/// instead of crash-restarting it.
pub fn SetActiveChildPidByEntry(pmchild: &PMChild, pid: i32) -> bool {
    let mut state = PMCHILD.lock().unwrap();
    match find_active_index(&state, pmchild) {
        Some(idx) => {
            state.slab[idx]
                .as_mut()
                .expect("active child slab slot is live")
                .pid = pid;
            true
        }
        None => false,
    }
}

/// Set the `bkend_type` of a live active child identified by `child_slot`
/// (postmaster.c rewrites `bp->bkend_type = B_WAL_SENDER` in place when it
/// notices a backend has become a walsender, in `SignalChildren`/
/// `CountChildren`; and sets `bn->bkend_type = B_BG_WORKER` in
/// `StartBackgroundWorker`). Returns whether the entry was found.
pub fn SetActiveChildBkendType(child_slot: i32, bkend_type: BackendType) -> bool {
    let mut state = PMCHILD.lock().unwrap();
    let mut found_idx: Option<usize> = None;
    for &idx in &state.active_child_list {
        if let Some(bp) = state.slab[idx] {
            if bp.child_slot != 0 && bp.child_slot == child_slot {
                found_idx = Some(idx);
                break;
            }
        }
    }
    match found_idx {
        Some(idx) => {
            state.slab[idx]
                .as_mut()
                .expect("active child slab slot is live")
                .bkend_type = bkend_type;
            true
        }
        None => false,
    }
}

/// Set the `rw` (bgworker registration index) and `bgworker_notify` flag of a
/// live active child identified by `child_slot`. postmaster.c writes these
/// together when forking a child: `bn->rw = NULL; bn->bgworker_notify = false`
/// for ordinary/autovac backends, or `bn->rw = rw; bn->bgworker_notify = false`
/// for a background worker (`StartBackgroundWorker`). Returns whether found.
pub fn SetActiveChildBgworkerInfo(
    child_slot: i32,
    rw: Option<u32>,
    bgworker_notify: bool,
) -> bool {
    let mut state = PMCHILD.lock().unwrap();
    let mut found_idx: Option<usize> = None;
    for &idx in &state.active_child_list {
        if let Some(bp) = state.slab[idx] {
            if bp.child_slot != 0 && bp.child_slot == child_slot {
                found_idx = Some(idx);
                break;
            }
        }
    }
    match found_idx {
        Some(idx) => {
            let bp = state.slab[idx]
                .as_mut()
                .expect("active child slab slot is live");
            bp.rw = rw;
            bp.bgworker_notify = bgworker_notify;
            true
        }
        None => false,
    }
}

/// Snapshot the current `ActiveChildList` as a `Vec<PMChild>` (head-first
/// order, matching C's `dlist_foreach` over the head-insertion list). Used by
/// postmaster.c's read-only iterations (`SignalChildren`, `CountChildren`'s
/// counting/logging passes); the in-place WAL-sender relabel those passes do is
/// applied via [`SetActiveChildBkendType`].
pub fn ActiveChildListSnapshot() -> Vec<PMChild> {
    let state = PMCHILD.lock().unwrap();
    state
        .active_child_list
        .iter()
        .filter_map(|&idx| state.slab[idx])
        .collect()
}

/// `bp->pid = pid` on the live entry identified by `pmchild`. Returns `true` if
/// the entry was found in `ActiveChildList`.
pub fn SetPostmasterChildPid(pmchild: &PMChild, pid: i32) -> bool {
    with_active_child(pmchild, |mut r| r.set_pid(pid))
}

/// `bp->bkend_type = bkend_type` on the live entry identified by `pmchild`.
pub fn SetPostmasterChildBackendType(pmchild: &PMChild, bkend_type: BackendType) -> bool {
    with_active_child(pmchild, |mut r| r.set_bkend_type(bkend_type))
}

/// `bp->rw = rw` on the live entry identified by `pmchild`.
pub fn SetPostmasterChildRw(pmchild: &PMChild, rw: Option<u32>) -> bool {
    with_active_child(pmchild, |mut r| r.set_rw(rw))
}

/// `bp->bgworker_notify = bgworker_notify` on the live entry identified by
/// `pmchild`.
pub fn SetPostmasterChildBgworkerNotify(pmchild: &PMChild, bgworker_notify: bool) -> bool {
    with_active_child(pmchild, |mut r| r.set_bgworker_notify(bgworker_notify))
}

/// Locate the `ActiveChildList` slab index of `pmchild` by identity. Pool slots
/// are uniquely keyed by `child_slot` (> 0); dead-end children (`child_slot ==
/// 0`) are matched by full value equality.
fn find_active_index(state: &PmChildState, pmchild: &PMChild) -> Option<usize> {
    for &idx in &state.active_child_list {
        if let Some(bp) = state.slab[idx] {
            if pmchild.child_slot != 0 {
                if bp.child_slot == pmchild.child_slot {
                    return Some(idx);
                }
            } else if bp == *pmchild {
                return Some(idx);
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Seam installation
// ---------------------------------------------------------------------------

/// Install this crate's seam implementations.
pub fn init_seams() {
    pmchild_seams::init_postmaster_child_slots::set(InitPostmasterChildSlots);
    pmchild_seams::max_live_postmaster_children::set(MaxLivePostmasterChildren);
}

#[cfg(test)]
mod tests;
