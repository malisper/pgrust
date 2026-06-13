//! Signature types for `backend-storage-lmgr-deadlock` — the deadlock detector
//! (`src/backend/storage/lmgr/deadlock.c`) and the data it walks.
//!
//! # Why an index-handle arena instead of `*mut`
//!
//! deadlock.c is the canonical *shared-memory pointer-identity graph* algorithm:
//! it walks the live `LOCK`/`PROCLOCK`/`PGPROC` graph in shared memory entirely
//! by **address identity** — `visitedProcs[i] == checkProc`,
//! `leader != checkProcLeader`, `waitOrders[i].lock == lock` — and the graph is
//! cyclic (A waits for L2 held by B, B waits for L1 held by A). The C code reaches
//! it through raw `*mut PGPROC` / `*mut LOCK` pointers that *are* absolute shmem
//! addresses, and uses ilist.h's intrusive `dclist`/`dlist` links threaded through
//! the shared structs.
//!
//! The idiomatic surface forbids `*mut`/`*const`/`NonNull`, and owned trees
//! (`Box`/`Option<Box>`) cannot express a cyclic, identity-compared graph. The
//! faithful idiomatic model of *shared memory* is therefore an **arena** of slots
//! (`Vec<…Slot>`) addressed by stable newtype **indices**: [`ProcId`], [`LockId`],
//! [`ProcLockId`]. An index is the exact analogue of "an absolute shmem address" —
//! it is `Copy`, it has identity (`Eq`), and a slot's index never changes for the
//! slot's lifetime. The intrusive wait queues become index-linked lists carried
//! inside the slots, reproducing ilist.h's `dclist`/`dlist` semantics faithfully
//! without raw pointers. [`LockSpace`] is that arena — the shared lock-table
//! substrate the detector walks.
//!
//! The shared structures (the lock/PROCLOCK records, the PGPROC slots, the wait
//! queues, the lock-group links) are modeled field-for-field with the fields the
//! detector consumes; the shmem *allocation* and the LWLock/spinlock primitives
//! that protect the substrate are genuine externals owned by lock.c/proc.c and
//! declared as seams. The detector's own scratch (`visitedProcs`, `topoProcs`, …)
//! is per-backend process-local memory in C and is modeled as owned `Vec`s in the
//! crate, not here.

#![no_std]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use types_storage::lock::{LOCKMASK, LOCKMODE, LOCKTAG, MAX_LOCKMODES};

// ===========================================================================
// DeadLockState (lock.h) — the detector's result.
// ===========================================================================

/// `DeadLockState` (storage/lock.h): the outcome of a deadlock check. Returned by
/// `DeadLockCheck` to `ProcSleep`. (`DS_NOT_YET_CHECKED` is the pre-check value
/// the lock manager initializes a wait with; the detector itself never returns
/// it, but it is part of the vocabulary the proc.c caller uses.)
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeadLockState {
    /// `DS_NOT_YET_CHECKED` — no deadlock check has run yet.
    NotYetChecked,
    /// `DS_NO_DEADLOCK` — no deadlock detected.
    NoDeadlock,
    /// `DS_SOFT_DEADLOCK` — deadlock avoided by queue rearrangement.
    SoftDeadlock,
    /// `DS_HARD_DEADLOCK` — deadlock, no way out but ERROR.
    HardDeadlock,
    /// `DS_BLOCKED_BY_AUTOVACUUM` — no deadlock; queue blocked by an autovacuum
    /// worker that the caller may cancel.
    BlockedByAutovacuum,
}

// ===========================================================================
// LockMethodData (lock.h) — the lock-method conflict table.
// ===========================================================================

/// `LockMethodData` (storage/lock.h): the per-lock-method descriptor the detector
/// consults to decide which lock modes conflict and to name modes in the report.
///
/// In C this is a `const` table addressed by `LOCKMETHODID`; the detector reads
/// `numLockModes`, `conflictTab[mode]` (a `LOCKMASK` of conflicting modes), and
/// `lockModeNames[mode]`. We carry it owned (it is immutable, tiny, and cloned
/// rarely); `conflict_tab[0]` / `lock_mode_names[0]` are the unused NoLock slot.
#[derive(Clone, Debug)]
pub struct LockMethodData {
    /// number of lock modes (modes are numbered `1..=num_lock_modes`)
    pub num_lock_modes: i32,
    /// `conflictTab[i]` — mask of modes that conflict with mode `i`
    pub conflict_tab: [LOCKMASK; MAX_LOCKMODES],
    /// `lockModeNames[i]` — display name of mode `i` (index 0 unused)
    pub lock_mode_names: [&'static str; MAX_LOCKMODES],
}

// ===========================================================================
// Stable identity handles into the shared lock-table arena.
// ===========================================================================

/// Stable handle for a `PGPROC` slot in the shared [`LockSpace`] arena — the
/// idiomatic analogue of a `*mut PGPROC` absolute shmem address. `Copy` + `Eq`,
/// and a slot's id never changes for its lifetime, so equality is identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProcId(pub usize);

/// Stable handle for a `LOCK` slot in [`LockSpace`] — analogue of `*mut LOCK`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LockId(pub usize);

/// Stable handle for a `PROCLOCK` slot in [`LockSpace`] — analogue of
/// `*mut PROCLOCK`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProcLockId(pub usize);

// ===========================================================================
// The shared lock-table substrate the detector walks (the LOCK/PROCLOCK/PGPROC
// graph). Field-for-field with the C structs, but with intrusive ilist links
// modeled as index links inside the arena.
// ===========================================================================

/// One `PGPROC` slot, carrying exactly the fields the deadlock detector reads
/// (storage/proc.h). The detector's view of a process node in the wait-for graph.
#[derive(Clone, Debug)]
pub struct ProcSlot {
    /// `proc->pid` — backend PID (for the report).
    pub pid: i32,
    /// `proc->waitLock` — lock object we're sleeping on, or `None` if not waiting.
    pub wait_lock: Option<LockId>,
    /// `proc->waitProcLock` — per-holder info for the awaited lock (unused by the
    /// detector's traversal but part of the proc's wait state).
    pub wait_proc_lock: Option<ProcLockId>,
    /// `proc->waitLockMode` — type of lock we're waiting for.
    pub wait_lock_mode: LOCKMODE,
    /// `proc->statusFlags` — this backend's status flags (read for
    /// `PROC_IS_AUTOVACUUM`).
    pub status_flags: u8,
    /// `proc->lockGroupLeader` — lock-group leader if I'm a member, else `None`
    /// (the leader points to itself with `None`, i.e. "I am my own leader").
    pub lock_group_leader: Option<ProcId>,
    /// `proc->lockGroupMembers` — the leader's member list (ids of member procs),
    /// in list order. Empty unless I'm a leader.
    pub lock_group_members: Vec<ProcId>,
    /// Whether this proc is currently linked into a lock's wait queue. Models the
    /// C test `proc->links.next != NULL` (a proc whose `links` is threaded onto a
    /// `dclist` has a non-NULL `next`).
    pub is_on_wait_queue: bool,
}

impl ProcSlot {
    /// A fresh idle proc with the given PID: not waiting, no group, not queued.
    pub fn new(pid: i32) -> Self {
        Self {
            pid,
            wait_lock: None,
            wait_proc_lock: None,
            wait_lock_mode: 0,
            status_flags: 0,
            lock_group_leader: None,
            lock_group_members: Vec::new(),
            is_on_wait_queue: false,
        }
    }
}

/// One `PROCLOCK` slot — a per-lock-per-holder record (storage/lock.h). The
/// detector reads `tag.myProc`, `holdMask`, and the group leader.
#[derive(Clone, Debug)]
pub struct ProcLockSlot {
    /// `tag.myLock` — the lock this PROCLOCK is for.
    pub my_lock: LockId,
    /// `tag.myProc` — the holding/awaiting proc.
    pub my_proc: ProcId,
    /// `holdMask` — bitmask for lock modes currently held.
    pub hold_mask: LOCKMASK,
}

/// One `LOCK` slot — the per-locked-object record (storage/lock.h). The detector
/// reads `tag`, walks `procLocks` (the holders) and `waitProcs` (the wait queue),
/// and *rewrites* `waitProcs` to resolve soft deadlocks.
#[derive(Clone, Debug)]
pub struct LockSlot {
    /// `lock->tag` — the lock's identifying key.
    pub tag: LOCKTAG,
    /// `lock->procLocks` — holders of this lock, in dlist order (the `PROCLOCK`
    /// list). Index-linked equivalent of the intrusive `dlist_head`.
    pub proc_locks: Vec<ProcLockId>,
    /// `lock->waitProcs` — the wait queue, in dclist order (the `PGPROC` list).
    /// `DeadLockCheck` reorders this in place when it resolves a soft deadlock.
    pub wait_procs: Vec<ProcId>,
}

impl LockSlot {
    /// A fresh lock with the given tag and empty holder/wait lists.
    pub fn new(tag: LOCKTAG) -> Self {
        Self {
            tag,
            proc_locks: Vec::new(),
            wait_procs: Vec::new(),
        }
    }
}

/// The shared lock-table arena — the substrate the deadlock detector walks. This
/// is the idiomatic model of the shmem `LockMethodLockHash` /
/// `LockMethodProcLockHash` plus the PGPROC array: a set of fixed-identity slots
/// addressed by [`ProcId`]/[`LockId`]/[`ProcLockId`].
///
/// In a real backend this lives in shared memory and is protected by the
/// lock-partition LWLocks; allocation of the slots and the LWLock/spinlock
/// guarding is the genuine external (owned by lock.c/proc.c). The detector itself
/// only reads the graph and rewrites wait queues while holding all partition
/// locks, so within a check it has exclusive access — modeled here by
/// `&mut LockSpace`.
#[derive(Clone, Debug, Default)]
pub struct LockSpace {
    pub procs: Vec<ProcSlot>,
    pub locks: Vec<LockSlot>,
    pub proc_locks: Vec<ProcLockSlot>,
}

impl LockSpace {
    /// An empty arena.
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocate a proc slot, returning its stable id.
    pub fn add_proc(&mut self, proc: ProcSlot) -> ProcId {
        let id = ProcId(self.procs.len());
        self.procs.push(proc);
        id
    }

    /// Allocate a lock slot, returning its stable id.
    pub fn add_lock(&mut self, lock: LockSlot) -> LockId {
        let id = LockId(self.locks.len());
        self.locks.push(lock);
        id
    }

    /// Allocate a proclock slot, returning its stable id.
    pub fn add_proc_lock(&mut self, proc_lock: ProcLockSlot) -> ProcLockId {
        let id = ProcLockId(self.proc_locks.len());
        self.proc_locks.push(proc_lock);
        id
    }

    /// Borrow a proc slot.
    #[inline]
    pub fn proc(&self, id: ProcId) -> &ProcSlot {
        &self.procs[id.0]
    }

    /// Mutably borrow a proc slot.
    #[inline]
    pub fn proc_mut(&mut self, id: ProcId) -> &mut ProcSlot {
        &mut self.procs[id.0]
    }

    /// Borrow a lock slot.
    #[inline]
    pub fn lock(&self, id: LockId) -> &LockSlot {
        &self.locks[id.0]
    }

    /// Mutably borrow a lock slot.
    #[inline]
    pub fn lock_mut(&mut self, id: LockId) -> &mut LockSlot {
        &mut self.locks[id.0]
    }

    /// Borrow a proclock slot.
    #[inline]
    pub fn proc_lock(&self, id: ProcLockId) -> &ProcLockSlot {
        &self.proc_locks[id.0]
    }

    /// `proc->lockGroupLeader ? proc->lockGroupLeader : proc` — the leader of the
    /// lock group `id` belongs to (itself if it has no leader). Pure identity
    /// helper used pervasively by the detector.
    #[inline]
    pub fn group_leader(&self, id: ProcId) -> ProcId {
        self.proc(id).lock_group_leader.unwrap_or(id)
    }
}

// ===========================================================================
// EDGE / WAIT_ORDER / DEADLOCK_INFO (deadlock.c file-scope types).
// ===========================================================================

/// `EDGE` (deadlock.c): one edge in the waits-for graph. `waiter`/`blocker` are
/// lock-group **leaders**. `pred`/`link` are scratch reused by `TopoSort`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Edge {
    /// the leader of the waiting lock group
    pub waiter: ProcId,
    /// the leader of the group it is waiting for
    pub blocker: ProcId,
    /// the lock being waited for
    pub lock: LockId,
    /// workspace for TopoSort
    pub pred: i32,
    /// workspace for TopoSort
    pub link: i32,
}

/// `WAIT_ORDER` (deadlock.c): one potential reordering of a lock's wait queue.
/// `procs_off`/`n_procs` index a run of the shared `wait_order_procs` workspace
/// (the idiomatic equivalent of C's `WAIT_ORDER.procs` pointer into
/// `waitOrderProcs`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WaitOrder {
    /// the lock whose wait queue is described
    pub lock: LockId,
    /// index into `wait_order_procs` where this order's procs begin
    pub procs_off: usize,
    /// number of procs in the new order
    pub n_procs: i32,
}

/// `DEADLOCK_INFO` (deadlock.c): info saved about each edge in a detected cycle,
/// for the diagnostic message. We extract `locktag`/`lockmode`/`pid` (not slot
/// ids) so the report survives after the partition locks are released, exactly as
/// the C comment requires.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DeadlockInfo {
    /// ID of awaited lock object
    pub locktag: LOCKTAG,
    /// type of lock we're waiting for
    pub lockmode: LOCKMODE,
    /// PID of blocked backend
    pub pid: i32,
}

// ===========================================================================
// Report payload — what `DeadLockReport` carries back to the caller.
// ===========================================================================

/// The strings `DeadLockReport` builds before raising its `ereport(ERROR)`. The
/// crate returns these (and the caller raises the error) preserving the C
/// `pg_noreturn` contract; exposed here so the report can be inspected/tested.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DeadlockReport {
    /// `errdetail_internal` — the "Process N waits for …" lines for the client.
    pub client_detail: String,
    /// `errdetail_log` — the client lines plus per-process activity, for the log.
    pub log_detail: String,
}
