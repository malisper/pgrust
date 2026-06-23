//! Faithful idiomatic port of `src/backend/storage/lmgr/deadlock.c`.
//!
//! Every function in the C file is ported, including the `static` helpers
//! `DeadLockCheckRecurse`, `TestConfiguration`,
//! `FindLockCycle`/`Recurse`/`RecurseMember`, `ExpandConstraints`, `TopoSort`,
//! and the `DEBUG_DEADLOCK`-only `PrintLockQueue`.
//!
//! The shared `LOCK`/`PROCLOCK`/`PGPROC` graph the C code walks through raw
//! pointers is modeled as the [`LockSpace`] arena of fixed-identity slots; a raw
//! `*mut PGPROC` becomes a `Copy`, identity-comparable [`ProcId`] (and likewise
//! [`LockId`]/[`ProcLockId`]). The detector takes `&mut LockSpace` because it runs
//! while holding all lock-partition LWLocks. The detector's own scratch (the
//! deadlock.c file-scope `static`s) is per-backend process-local memory, modeled
//! as a thread-local [`DeadlockState`] of owned `Vec`s.

use core::cell::RefCell;

use mcx::{MemoryContext, PgString};

use types_deadlock::{
    DeadLockState, DeadlockInfo, DeadlockReport, Edge, LockId, LockSpace, ProcId, WaitOrder,
};
use types_error::{PgError, PgResult, ERRCODE_T_R_DEADLOCK_DETECTED, ERROR, FATAL};
use types_storage::lock::{LOCKMODE, LOCKTAG, LOCKTAG_RELATION_EXTEND};
use types_storage::storage::{MAX_BACKENDS_BITS, PROC_IS_AUTOVACUUM};

use utils_error::ereport;

use lmgr_seams::describe_lock_tag;
use lock_seams::{get_lock_method_table, get_lockmode_name};
use lmgr_proc_seams::proc_lock_wakeup;
use stat_seams::report_deadlock;
use status_seams::backend_current_activity;
use init_small_seams::max_backends;

/// `LOCKBIT_ON(lockmode)` (lock.h:85) — the conflict-table bit for a lock mode.
#[inline]
fn lockbit_on(lockmode: LOCKMODE) -> i32 {
    1 << lockmode
}

// ===========================================================================
// Per-backend deadlock-detector workspace (the deadlock.c file-scope `static`s).
// ===========================================================================

/// The bundle of per-backend process-local scratch that deadlock.c keeps in
/// file-scope `static` variables. Allocated once by [`init_dead_lock_checking`]
/// (`InitDeadLockChecking`), then reused by every [`dead_lock_check`].
///
/// Process-local, NOT shared memory — see the module docs. These fixed-capacity
/// arrays hold the POD slot-id/edge types; allocation is bounded by `MaxBackends`
/// (a small validated bound) and performed fallibly to mirror the C `palloc`'s
/// out-of-memory `ereport`.
struct DeadlockState {
    // Workspace for FindLockCycle: array of visited procs.
    visited_procs: Vec<ProcId>,
    n_visited_procs: i32,

    // Workspace for TopoSort. C re-uses `visitedProcs`' space (`topoProcs =
    // visitedProcs`); they never run concurrently, so a dedicated buffer of the
    // same size is observably identical. `None` is C's NULL "this slot consumed".
    topo_procs: Vec<Option<ProcId>>,
    before_constraints: Vec<i32>,
    after_constraints: Vec<i32>,

    // Output area for ExpandConstraints.
    wait_orders: Vec<WaitOrder>,
    n_wait_orders: i32,
    wait_order_procs: Vec<ProcId>,

    // Current list of constraints being considered.
    cur_constraints: Vec<Edge>,
    n_cur_constraints: i32,
    max_cur_constraints: i32,

    // Storage space for results from FindLockCycle.
    possible_constraints: Vec<Edge>,
    n_possible_constraints: i32,
    max_possible_constraints: i32,
    deadlock_details: Vec<DeadlockInfo>,
    n_deadlock_details: i32,

    // proc id of any blocking autovacuum worker found.
    blocking_autovacuum_proc: Option<ProcId>,

    // Cached `MaxBackends` the arrays were sized for.
    max_backends: i32,
}

thread_local! {
    /// The single per-backend detector workspace. `None` until
    /// `InitDeadLockChecking` runs, exactly as the C `static`s are NULL until then.
    static STATE: RefCell<Option<DeadlockState>> = const { RefCell::new(None) };
}

/// Run `f` with a mutable borrow of the initialized [`DeadlockState`], panicking
/// if `InitDeadLockChecking` was never called (the C code would dereference NULL
/// workspace pointers).
fn with_state<R>(f: impl FnOnce(&mut DeadlockState) -> R) -> R {
    STATE.with(|cell| {
        let mut borrow = cell.borrow_mut();
        let state = borrow
            .as_mut()
            .expect("InitDeadLockChecking must run before the deadlock detector is used");
        f(state)
    })
}

// `EDGE`'s "null" value (deadlock.c zero-initializes the arrays). The sentinel
// proc/lock ids are index 0; they are never read before being overwritten by a
// real edge, exactly as the C arrays are written before use.
const NULL_EDGE: Edge = Edge {
    waiter: ProcId(0),
    blocker: ProcId(0),
    lock: LockId(0),
    pred: 0,
    link: 0,
};

const NULL_WAIT_ORDER: WaitOrder = WaitOrder {
    lock: LockId(0),
    procs_off: 0,
    n_procs: 0,
};

/// Allocate a `Vec<T>` of length `n` filled with `value`, mirroring a C `palloc`:
/// the reservation is fallible, converting an out-of-memory to the context OOM
/// error (`mcx.oom`), then the infallible fill runs.
fn palloc_vec<T: Clone>(mcx: mcx::Mcx<'_>, n: usize, value: T) -> PgResult<Vec<T>> {
    let mut v: Vec<T> = Vec::new();
    v.try_reserve_exact(n)
        .map_err(|_| mcx.oom(n.saturating_mul(core::mem::size_of::<T>())))?;
    v.resize(n, value);
    Ok(v)
}

// ===========================================================================
// InitDeadLockChecking (deadlock.c:143)
// ===========================================================================

/// `InitDeadLockChecking` — per-backend initialization of the deadlock checker;
/// primarily allocation of working memory for `DeadLockCheck`. Done per-backend
/// and at startup because (a) the checker may run when there's no free memory and
/// (b) it runs inside a signal handler where palloc is dangerous.
///
/// The C `palloc(...)` calls allocate from `TopMemoryContext` (permanent,
/// per-backend). Faithful idiomatic equivalent: owning `Vec`s in the per-backend
/// thread-local workspace — process-local, not shmem. Each allocation is fallible
/// (the C `palloc` out-of-memory `ereport`), bounded by `MaxBackends`.
pub fn init_dead_lock_checking() -> PgResult<()> {
    // The C switches to TopMemoryContext (permanent, per-backend) for these
    // allocations. We own a permanent per-backend context for the OOM channel.
    let top = MemoryContext::new("DeadlockWorkspace");
    let mcx = top.mcx();

    let max_backends = max_backends::call();
    let mb = max_backends as usize;

    // FindLockCycle needs at most MaxBackends entries in visitedProcs[] and
    // deadlockDetails[].
    let visited_procs = palloc_vec(mcx, mb, ProcId(0))?;
    let deadlock_details = palloc_vec(mcx, mb, DeadlockInfo::default())?;

    // TopoSort needs at most MaxBackends wait-queue entries; it needn't run
    // concurrently with FindLockCycle (C re-uses visitedProcs' space here).
    let topo_procs = palloc_vec(mcx, mb, None)?;
    let before_constraints = palloc_vec(mcx, mb, 0i32)?;
    let after_constraints = palloc_vec(mcx, mb, 0i32)?;

    // At most MaxBackends/2 wait queues to rearrange, MaxBackends total waiters.
    let wait_orders = palloc_vec(mcx, mb / 2, NULL_WAIT_ORDER)?;
    let wait_order_procs = palloc_vec(mcx, mb, ProcId(0))?;

    // At most MaxBackends distinct constraints in a configuration.
    let max_cur_constraints = max_backends;
    let cur_constraints = palloc_vec(mcx, max_cur_constraints as usize, NULL_EDGE)?;

    // Up to 3*MaxBackends saved constraints + MaxBackends reserved as the
    // FindLockCycle output workspace == 4*MaxBackends.
    // StaticAssert(MAX_BACKENDS_BITS <= 32 - 3): so 4*MaxBackends cannot overflow
    // an i32 (a compile-time check in C).
    const _: () = assert!(MAX_BACKENDS_BITS <= (32 - 3));
    let max_possible_constraints = max_backends * 4;
    let possible_constraints = palloc_vec(mcx, max_possible_constraints as usize, NULL_EDGE)?;

    let state = DeadlockState {
        visited_procs,
        n_visited_procs: 0,
        topo_procs,
        before_constraints,
        after_constraints,
        wait_orders,
        n_wait_orders: 0,
        wait_order_procs,
        cur_constraints,
        n_cur_constraints: 0,
        max_cur_constraints,
        possible_constraints,
        n_possible_constraints: 0,
        max_possible_constraints,
        deadlock_details,
        n_deadlock_details: 0,
        blocking_autovacuum_proc: None,
        max_backends,
    };

    // `top` is used only as the OOM error channel for the fallible reservations;
    // the workspace `Vec`s own their own heap storage (permanent for the backend's
    // lifetime), so the accounting context can drop here.
    drop(top);

    STATE.with(|cell| *cell.borrow_mut() = Some(state));
    Ok(())
}

// ===========================================================================
// DeadLockCheck (deadlock.c:219)
// ===========================================================================

/// `DeadLockCheck(proc)` — check for deadlocks involving `proc`. If any are found,
/// try to rearrange lock wait queues to resolve them. If resolution is impossible,
/// return [`DeadLockState::HardDeadlock`] (caller must abort `proc`).
///
/// Caller must already have locked all partitions of the lock tables — modeled by
/// the exclusive `&mut LockSpace`.
///
/// On failure, deadlock details are recorded for subsequent printing by
/// [`dead_lock_report`]. The C `elog(FATAL)` consistency checks propagate as
/// `Err` (FATAL maps to `Err(PgError)` per AGENTS.md).
pub fn dead_lock_check(
    space: &mut LockSpace,
    proc: ProcId,
    my_proc: Option<ProcId>,
) -> PgResult<DeadLockState> {
    // Initialize to "no constraints".
    with_state(|s| {
        s.n_cur_constraints = 0;
        s.n_possible_constraints = 0;
        s.n_wait_orders = 0;
        // Initialize to not blocked by an autovacuum worker.
        s.blocking_autovacuum_proc = None;
    });

    // Search for deadlocks and possible fixes.
    if dead_lock_check_recurse(space, proc, my_proc)? {
        // Call FindLockCycle one more time, to record the correct
        // deadlockDetails[] for the basic state with no rearrangements.
        // (TRACE_POSTGRESQL_DEADLOCK_FOUND() is a DTrace probe — a no-op here.)
        with_state(|s| s.n_wait_orders = 0);

        // FindLockCycle(proc, possibleConstraints, &nSoftEdges): the C `softEdges`
        // argument is `possibleConstraints` itself (its base), i.e. offset 0.
        let mut n_soft_edges = 0;
        if !find_lock_cycle(space, proc, my_proc, 0, &mut n_soft_edges) {
            // elog(FATAL, "deadlock seems to have disappeared")
            return Err(PgError::new(FATAL, "deadlock seems to have disappeared"));
        }

        return Ok(DeadLockState::HardDeadlock); // cannot find a non-deadlocked state
    }

    // Apply any needed rearrangements of wait queues.
    let n_wait_orders = with_state(|s| s.n_wait_orders);
    for i in 0..n_wait_orders {
        // Pull out this wait order's lock + the reordered procs.
        let (lock, procs): (LockId, Vec<ProcId>) = with_state(|s| {
            let wo = s.wait_orders[i as usize];
            let n = wo.n_procs as usize;
            let procs = s.wait_order_procs[wo.procs_off..wo.procs_off + n].to_vec();
            (wo.lock, procs)
        });

        // Assert(nProcs == dclist_count(waitQueue))
        debug_assert_eq!(procs.len(), space.lock(lock).wait_procs.len());

        // #ifdef DEBUG_DEADLOCK PrintLockQueue(lock, "DeadLockCheck:");
        #[cfg(feature = "debug_deadlock")]
        print_lock_queue(space, lock, "DeadLockCheck:");

        // Reset the queue and re-add procs in the desired order (the idiomatic
        // equivalent of dclist_init + dclist_push_tail over the intrusive queue).
        space.lock_mut(lock).wait_procs = procs;

        // #ifdef DEBUG_DEADLOCK PrintLockQueue(lock, "rearranged to:");
        #[cfg(feature = "debug_deadlock")]
        print_lock_queue(space, lock, "rearranged to:");

        // See if any waiters for the lock can be woken up now.
        // ProcLockWakeup(GetLocksMethodTable(lock), lock) — proc.c seam.
        proc_lock_wakeup::call(space, lock);
    }

    // Return code tells caller if we had to escape a deadlock or not.
    Ok(with_state(|s| {
        if s.n_wait_orders > 0 {
            DeadLockState::SoftDeadlock
        } else if s.blocking_autovacuum_proc.is_some() {
            DeadLockState::BlockedByAutovacuum
        } else {
            DeadLockState::NoDeadlock
        }
    }))
}

// ===========================================================================
// GetBlockingAutoVacuumPgproc (deadlock.c:289)
// ===========================================================================

/// `GetBlockingAutoVacuumPgproc` — return the proc of the autovacuum that's
/// blocking a process, resetting the saved value as soon as we pass it back.
/// Returns `None` (C's `NULL`) if none was found.
pub fn get_blocking_auto_vacuum_pgproc() -> Option<ProcId> {
    with_state(|s| s.blocking_autovacuum_proc.take())
}

// ===========================================================================
// DeadLockCheckRecurse (deadlock.c:311)
// ===========================================================================

/// `DeadLockCheckRecurse(proc)` — recursively search for valid orderings.
///
/// Returns `Ok(true)` if no solution exists. Returns `Ok(false)` if a
/// deadlock-free state is attainable (in which case `wait_orders[]` shows the
/// required rearrangements). The `elog(FATAL)` consistency check propagates as
/// `Err` (C aborts the backend; FATAL maps to `Err(PgError)` per AGENTS.md).
fn dead_lock_check_recurse(
    space: &mut LockSpace,
    proc: ProcId,
    my_proc: Option<ProcId>,
) -> PgResult<bool> {
    let n_edges = test_configuration(space, proc, my_proc);
    if n_edges < 0 {
        return Ok(true); // hard deadlock --- no solution
    }
    if n_edges == 0 {
        return Ok(false); // good configuration found
    }

    // nCurConstraints >= maxCurConstraints? out of room for active constraints.
    let (out_of_room, old_possible_constraints, saved_list) = with_state(|s| {
        if s.n_cur_constraints >= s.max_cur_constraints {
            return (true, 0, false);
        }
        let old = s.n_possible_constraints;
        let saved =
            if s.n_possible_constraints + n_edges + s.max_backends <= s.max_possible_constraints {
                // We can save the edge list in possibleConstraints[].
                s.n_possible_constraints += n_edges;
                true
            } else {
                // Not room; will need to regenerate the edges on-the-fly.
                false
            };
        (false, old, saved)
    });
    if out_of_room {
        return Ok(true); // out of room for active constraints?
    }

    // Try each available soft edge as an addition to the configuration.
    for i in 0..n_edges {
        if !saved_list && i > 0 {
            // Regenerate the list of possible added constraints.
            if n_edges != test_configuration(space, proc, my_proc) {
                // elog(FATAL, "inconsistent results during deadlock check")
                return Err(PgError::new(
                    FATAL,
                    "inconsistent results during deadlock check",
                ));
            }
        }
        with_state(|s| {
            s.cur_constraints[s.n_cur_constraints as usize] =
                s.possible_constraints[(old_possible_constraints + i) as usize];
            s.n_cur_constraints += 1;
        });
        if !dead_lock_check_recurse(space, proc, my_proc)? {
            return Ok(false); // found a valid solution!
        }
        // give up on that added constraint, try again
        with_state(|s| s.n_cur_constraints -= 1);
    }
    with_state(|s| s.n_possible_constraints = old_possible_constraints);
    Ok(true) // no solution found
}

// ===========================================================================
// TestConfiguration (deadlock.c:377)
// ===========================================================================

/// Test a configuration (current set of constraints) for validity.
///
/// Returns: 0 = good (no deadlocks); -1 = hard deadlock or not self-consistent;
/// `>0` = one or more soft deadlocks (the count of soft edges of an arbitrarily
/// chosen soft cycle, whose edges begin at
/// `possible_constraints[n_possible_constraints..]`).
fn test_configuration(space: &mut LockSpace, start_proc: ProcId, my_proc: Option<ProcId>) -> i32 {
    // softEdges = possibleConstraints + nPossibleConstraints (the output base
    // offset for FindLockCycle this round). Make sure we have room first.
    let soft_edges_base = with_state(|s| {
        if s.n_possible_constraints + s.max_backends > s.max_possible_constraints {
            return -1;
        }
        s.n_possible_constraints
    });
    if soft_edges_base < 0 {
        return -1;
    }

    // Expand current constraint set into wait orderings. Fail if not consistent.
    let (cur_constraints, n_cur) = with_state(|s| (s.cur_constraints.clone(), s.n_cur_constraints));
    if !expand_constraints(space, &cur_constraints, n_cur) {
        return -1;
    }

    let mut soft_found = 0;

    // Check for cycles involving startProc or any proc mentioned in constraints.
    // We check startProc last because if it has a soft cycle still to be dealt
    // with, we want to deal with that first.
    for i in 0..n_cur {
        let (waiter, blocker) = with_state(|s| {
            (
                s.cur_constraints[i as usize].waiter,
                s.cur_constraints[i as usize].blocker,
            )
        });

        let mut n_soft_edges = 0;
        if find_lock_cycle(space, waiter, my_proc, soft_edges_base, &mut n_soft_edges) {
            if n_soft_edges == 0 {
                return -1; // hard deadlock detected
            }
            soft_found = n_soft_edges;
        }
        if find_lock_cycle(space, blocker, my_proc, soft_edges_base, &mut n_soft_edges) {
            if n_soft_edges == 0 {
                return -1; // hard deadlock detected
            }
            soft_found = n_soft_edges;
        }
    }

    let mut n_soft_edges = 0;
    if find_lock_cycle(space, start_proc, my_proc, soft_edges_base, &mut n_soft_edges) {
        if n_soft_edges == 0 {
            return -1; // hard deadlock detected
        }
        soft_found = n_soft_edges;
    }
    soft_found
}

// ===========================================================================
// FindLockCycle / Recurse / RecurseMember (deadlock.c:445)
// ===========================================================================

/// `FindLockCycle(checkProc, softEdges, nSoftEdges)` — basic check for deadlock
/// cycles. `soft_edges_base` is the index into `possible_constraints` where the
/// returned soft-edge list should be written (C's `softEdges` pointer argument).
fn find_lock_cycle(
    space: &mut LockSpace,
    check_proc: ProcId,
    my_proc: Option<ProcId>,
    soft_edges_base: i32,
    n_soft_edges: &mut i32,
) -> bool {
    with_state(|s| {
        s.n_visited_procs = 0;
        s.n_deadlock_details = 0;
    });
    *n_soft_edges = 0;
    find_lock_cycle_recurse(space, check_proc, my_proc, 0, soft_edges_base, n_soft_edges)
}

/// `FindLockCycleRecurse(checkProc, depth, softEdges, nSoftEdges)`.
fn find_lock_cycle_recurse(
    space: &mut LockSpace,
    mut check_proc: ProcId,
    my_proc: Option<ProcId>,
    depth: i32,
    soft_edges_base: i32,
    n_soft_edges: &mut i32,
) -> bool {
    // If this process is a lock group member, check the leader instead. (We might
    // be the leader, in which case this is a no-op.)
    if let Some(leader) = space.proc(check_proc).lock_group_leader {
        check_proc = leader;
    }

    // Have we already seen this proc?
    let already_seen_index = with_state(|s| {
        let mut found = None;
        for i in 0..s.n_visited_procs {
            if s.visited_procs[i as usize] == check_proc {
                found = Some(i);
                break;
            }
        }
        found
    });
    if let Some(i) = already_seen_index {
        // If we return to starting point, we have a deadlock cycle.
        if i == 0 {
            // record total length of cycle --- outer levels will now fill
            // deadlockDetails[].
            with_state(|s| {
                debug_assert!(depth <= s.max_backends);
                s.n_deadlock_details = depth;
            });
            return true;
        }
        // Otherwise, we have a cycle but it does not include the start point, so
        // say "no deadlock".
        return false;
    }

    // Mark proc as seen.
    with_state(|s| {
        debug_assert!(s.n_visited_procs < s.max_backends);
        let idx = s.n_visited_procs as usize;
        s.visited_procs[idx] = check_proc;
        s.n_visited_procs += 1;
    });

    // If the process is waiting, there is an outgoing waits-for edge to each
    // process that blocks it. (`proc->links.next != NULL && proc->waitLock !=
    // NULL` in C; the idiomatic test is "queued on a wait queue and has a
    // waitLock".)
    let cp = space.proc(check_proc);
    if cp.is_on_wait_queue
        && cp.wait_lock.is_some()
        && find_lock_cycle_recurse_member(
            space,
            check_proc,
            check_proc,
            my_proc,
            depth,
            soft_edges_base,
            n_soft_edges,
        )
    {
        return true;
    }

    // If the process is not waiting, there could still be outgoing waits-for edges
    // if it is part of a lock group, because other members might be waiting even
    // though this process is not.
    let members = space.proc(check_proc).lock_group_members.clone();
    for member_proc in members {
        let mp = space.proc(member_proc);
        if mp.is_on_wait_queue
            && mp.wait_lock.is_some()
            && member_proc != check_proc
            && find_lock_cycle_recurse_member(
                space,
                member_proc,
                check_proc,
                my_proc,
                depth,
                soft_edges_base,
                n_soft_edges,
            )
        {
            return true;
        }
    }
    false
}

/// `FindLockCycleRecurseMember(checkProc, checkProcLeader, depth, softEdges,
/// nSoftEdges)`.
fn find_lock_cycle_recurse_member(
    space: &mut LockSpace,
    check_proc: ProcId,
    check_proc_leader: ProcId,
    my_proc: Option<ProcId>,
    depth: i32,
    soft_edges_base: i32,
    n_soft_edges: &mut i32,
) -> bool {
    let lock = space
        .proc(check_proc)
        .wait_lock
        .expect("FindLockCycleRecurseMember: checkProc must be waiting");

    // The relation extension lock can never participate in an actual deadlock
    // cycle. So there's no advantage in checking wait edges from it.
    if space.lock(lock).tag.locktag_type == LOCKTAG_RELATION_EXTEND {
        return false;
    }

    let lock_method_table = get_lock_method_table::call(space, lock);
    let num_lock_modes = lock_method_table.num_lock_modes;
    let wait_lock_mode = space.proc(check_proc).wait_lock_mode;
    let conflict_mask = lock_method_table.conflict_tab[wait_lock_mode as usize];

    // Scan for procs that already hold conflicting locks. These are "hard" edges.
    let proc_locks = space.lock(lock).proc_locks.clone();
    for proclock in proc_locks {
        let proc = space.proc_lock(proclock).my_proc;
        let hold_mask = space.proc_lock(proclock).hold_mask;
        let leader = space.group_leader(proc);

        // A proc never blocks itself or any other lock group member.
        if leader != check_proc_leader {
            let mut lm = 1;
            while lm <= num_lock_modes {
                if (hold_mask & lockbit_on(lm)) != 0 && (conflict_mask & lockbit_on(lm)) != 0 {
                    // This proc hard-blocks checkProc.
                    if find_lock_cycle_recurse(
                        space,
                        proc,
                        my_proc,
                        depth + 1,
                        soft_edges_base,
                        n_soft_edges,
                    ) {
                        // fill deadlockDetails[]
                        let tag = space.lock(lock).tag;
                        let pid = space.proc(check_proc).pid;
                        with_state(|s| {
                            let info = &mut s.deadlock_details[depth as usize];
                            info.locktag = tag;
                            info.lockmode = wait_lock_mode;
                            info.pid = pid;
                        });
                        return true;
                    }

                    // No deadlock here, but see if this proc is an autovacuum that
                    // is directly hard-blocking our own proc. If so, report it so
                    // the caller can cancel it. We don't touch autovacuums that are
                    // *indirectly* blocking us. statusFlags is read without locking,
                    // which is OK for PROC_IS_AUTOVACUUM (set at start, never reset).
                    if Some(check_proc) == my_proc
                        && (space.proc(proc).status_flags & PROC_IS_AUTOVACUUM) != 0
                    {
                        with_state(|s| s.blocking_autovacuum_proc = Some(proc));
                    }

                    // We're done looking at this proclock.
                    break;
                }
                lm += 1;
            }
        }
    }

    // Scan for procs ahead of this one in the lock's wait queue. Those with
    // conflicting requests soft-block this one. Done after the hard-block search,
    // so a proc that both hard- and soft-blocks counts as a hard edge.
    //
    // If there is a proposed re-ordering of the lock's wait order, use that rather
    // than the current wait order.
    let (wait_order_index, n_wait_orders) = with_state(|s| {
        let mut idx = s.n_wait_orders;
        for i in 0..s.n_wait_orders {
            if s.wait_orders[i as usize].lock == lock {
                idx = i;
                break;
            }
        }
        (idx, s.n_wait_orders)
    });

    if wait_order_index < n_wait_orders {
        // Use the given hypothetical wait queue order.
        let (procs_off, queue_size) = with_state(|s| {
            let wo = s.wait_orders[wait_order_index as usize];
            (wo.procs_off, wo.n_procs)
        });

        for i in 0..queue_size {
            let proc = with_state(|s| s.wait_order_procs[procs_off + i as usize]);
            let leader = space.group_leader(proc);

            // TopoSort returns orderings with group members adjacent. So as soon as
            // we reach a process in the same lock group as checkProc, we've found
            // all conflicts that precede any member of checkProcLeader's group.
            if leader == check_proc_leader {
                break;
            }

            // Is there a conflict with this guy's request?
            if (lockbit_on(space.proc(proc).wait_lock_mode) & conflict_mask) != 0 {
                // This proc soft-blocks checkProc.
                if find_lock_cycle_recurse(
                    space,
                    proc,
                    my_proc,
                    depth + 1,
                    soft_edges_base,
                    n_soft_edges,
                ) {
                    let tag = space.lock(lock).tag;
                    let pid = space.proc(check_proc).pid;
                    with_state(|s| {
                        let info = &mut s.deadlock_details[depth as usize];
                        info.locktag = tag;
                        info.lockmode = wait_lock_mode;
                        info.pid = pid;
                        // Add this edge to the list of soft edges in the cycle.
                        debug_assert!(*n_soft_edges < s.max_backends);
                        let se = (soft_edges_base + *n_soft_edges) as usize;
                        s.possible_constraints[se].waiter = check_proc_leader;
                        s.possible_constraints[se].blocker = leader;
                        s.possible_constraints[se].lock = lock;
                    });
                    *n_soft_edges += 1;
                    return true;
                }
            }
        }
    } else {
        // Use the true lock wait queue order. Find the last member of the lock
        // group present in the wait queue; anything after this is not a soft
        // conflict.
        let last_group_member: ProcId = if space.proc(check_proc).lock_group_leader.is_none() {
            check_proc
        } else {
            let mut found: Option<ProcId> = None;
            for &proc in &space.lock(lock).wait_procs {
                if space.proc(proc).lock_group_leader == Some(check_proc_leader) {
                    found = Some(proc);
                }
            }
            found.expect("FindLockCycleRecurseMember: group member must be on wait queue")
        };

        // Now rescan (or scan) the queue to identify the soft conflicts.
        let wait_procs = space.lock(lock).wait_procs.clone();
        for proc in wait_procs {
            let leader = space.group_leader(proc);

            // Done when we reach the target proc.
            if proc == last_group_member {
                break;
            }

            // Is there a conflict with this guy's request?
            if (lockbit_on(space.proc(proc).wait_lock_mode) & conflict_mask) != 0
                && leader != check_proc_leader
            {
                // This proc soft-blocks checkProc.
                if find_lock_cycle_recurse(
                    space,
                    proc,
                    my_proc,
                    depth + 1,
                    soft_edges_base,
                    n_soft_edges,
                ) {
                    let tag = space.lock(lock).tag;
                    let pid = space.proc(check_proc).pid;
                    with_state(|s| {
                        let info = &mut s.deadlock_details[depth as usize];
                        info.locktag = tag;
                        info.lockmode = wait_lock_mode;
                        info.pid = pid;
                        debug_assert!(*n_soft_edges < s.max_backends);
                        let se = (soft_edges_base + *n_soft_edges) as usize;
                        s.possible_constraints[se].waiter = check_proc_leader;
                        s.possible_constraints[se].blocker = leader;
                        s.possible_constraints[se].lock = lock;
                    });
                    *n_soft_edges += 1;
                    return true;
                }
            }
        }
    }

    // No conflict detected here.
    false
}

// ===========================================================================
// ExpandConstraints (deadlock.c:789)
// ===========================================================================

/// `ExpandConstraints(constraints, nConstraints)` — expand a list of soft edges to
/// be reversed into a set of specific new orderings for affected wait queues.
/// Output is `n_wait_orders` `WaitOrder`s in `wait_orders[]`, with proc workspace
/// in `wait_order_procs[]`. Returns true if it built an ordering that satisfies
/// all constraints, false if not (contradictory constraints).
fn expand_constraints(space: &mut LockSpace, constraints: &[Edge], n_constraints: i32) -> bool {
    let mut n_wait_order_procs = 0usize;
    with_state(|s| s.n_wait_orders = 0);

    // Scan constraint list backwards: the last-added constraint is the only one
    // that could fail, so test it for inconsistency first.
    let mut i = n_constraints;
    loop {
        i -= 1;
        if i < 0 {
            break;
        }
        let lock = constraints[i as usize].lock;

        // Did we already make a list for this lock?
        let already = with_state(|s| {
            let mut j = s.n_wait_orders;
            loop {
                j -= 1;
                if j < 0 {
                    break;
                }
                if s.wait_orders[j as usize].lock == lock {
                    return true;
                }
            }
            false
        });
        if already {
            continue;
        }

        // No, so allocate a new list.
        let count = space.lock(lock).wait_procs.len() as i32;
        let procs_off = n_wait_order_procs;
        with_state(|s| {
            let nwo = s.n_wait_orders as usize;
            s.wait_orders[nwo].lock = lock;
            s.wait_orders[nwo].procs_off = procs_off;
            s.wait_orders[nwo].n_procs = count;
        });
        n_wait_order_procs += count as usize;
        debug_assert!(n_wait_order_procs <= with_state(|s| s.max_backends as usize));

        // Do the topo sort. TopoSort need not examine constraints after this one,
        // since they must be for different locks.
        if !topo_sort(space, lock, constraints, i + 1, procs_off) {
            return false;
        }
        with_state(|s| s.n_wait_orders += 1);
    }
    true
}

// ===========================================================================
// TopoSort (deadlock.c:861)
// ===========================================================================

/// `TopoSort(lock, constraints, nConstraints, ordering)` — topological sort of a
/// lock's wait queue satisfying the partial ordering given by `constraints` (each
/// EDGE means "waiter" must appear before "blocker"), minimizing rearrangement.
/// The output is written into `wait_order_procs[ordering_off..]`. Returns false on
/// contradictory constraints.
fn topo_sort(
    space: &mut LockSpace,
    lock: LockId,
    constraints: &[Edge],
    n_constraints: i32,
    ordering_off: usize,
) -> bool {
    let queue_size = space.lock(lock).wait_procs.len() as i32;

    // Work on a *local* copy of the constraints' pred/link workspace so we can
    // mutate it like C mutates constraints[i].pred / .link. (C mutates the
    // caller's array in place; we mirror that with a local mutable copy, since the
    // pred/link fields are pure scratch that no caller reads back.)
    let mut cons: Vec<Edge> = constraints[..n_constraints as usize].to_vec();

    // First, fill topoProcs[] with the procs in their current order.
    {
        let wait_procs = space.lock(lock).wait_procs.clone();
        with_state(|s| {
            for (idx, &proc) in wait_procs.iter().enumerate() {
                s.topo_procs[idx] = Some(proc);
            }
        });
        debug_assert_eq!(wait_procs.len() as i32, queue_size);
    }

    // beforeConstraints / afterConstraints: for each proc, count of "must be
    // before" constraints + list head of "must be after" constraints.
    with_state(|s| {
        for k in 0..queue_size as usize {
            s.before_constraints[k] = 0;
            s.after_constraints[k] = 0;
        }
    });

    for ci in 0..n_constraints {
        // Find a representative process on the lock queue that is part of the
        // waiting lock group. Set other group members' beforeConstraints to -1 so
        // they are emitted with their groupmates. Select the LAST one in topoProcs.
        let proc = cons[ci as usize].waiter;
        let mut jj: i32 = -1;
        {
            let mut j = queue_size;
            loop {
                j -= 1;
                if j < 0 {
                    break;
                }
                let waiter = with_state(|s| s.topo_procs[j as usize])
                    .expect("topo_procs slot populated above");
                if waiter == proc || space.proc(waiter).lock_group_leader == Some(proc) {
                    debug_assert_eq!(space.proc(waiter).wait_lock, Some(lock));
                    if jj == -1 {
                        jj = j;
                    } else {
                        with_state(|s| {
                            debug_assert!(s.before_constraints[j as usize] <= 0);
                            s.before_constraints[j as usize] = -1;
                        });
                    }
                }
            }
        }

        // If no matching waiter, constraint is not relevant to this lock.
        if jj < 0 {
            continue;
        }

        // Similarly, find a representative process waiting for the blocking group.
        let proc = cons[ci as usize].blocker;
        let mut kk: i32 = -1;
        {
            let mut k = queue_size;
            loop {
                k -= 1;
                if k < 0 {
                    break;
                }
                let blocker = with_state(|s| s.topo_procs[k as usize])
                    .expect("topo_procs slot populated above");
                if blocker == proc || space.proc(blocker).lock_group_leader == Some(proc) {
                    debug_assert_eq!(space.proc(blocker).wait_lock, Some(lock));
                    if kk == -1 {
                        kk = k;
                    } else {
                        with_state(|s| {
                            debug_assert!(s.before_constraints[k as usize] <= 0);
                            s.before_constraints[k as usize] = -1;
                        });
                    }
                }
            }
        }

        // If no matching blocker, constraint is not relevant to this lock.
        if kk < 0 {
            continue;
        }

        with_state(|s| {
            debug_assert!(s.before_constraints[jj as usize] >= 0);
            s.before_constraints[jj as usize] += 1; // waiter must come before
                                                     // add this constraint to list of after-constraints for blocker
            cons[ci as usize].pred = jj;
            cons[ci as usize].link = s.after_constraints[kk as usize];
            s.after_constraints[kk as usize] = ci + 1;
        });
    }

    // Scan topoProcs backwards. At each step, output the last proc with no
    // remaining before-constraints plus its lock-group mates, then decrement the
    // beforeConstraints count of each proc it was constrained against.
    let mut last = queue_size - 1;
    let mut i = queue_size - 1;
    while i >= 0 {
        let mut nmatches = 0;

        // Find next candidate to output.
        while with_state(|s| s.topo_procs[last as usize].is_none()) {
            last -= 1;
        }
        let mut j = last;
        while j >= 0 {
            let take = with_state(|s| {
                s.topo_procs[j as usize].is_some() && s.before_constraints[j as usize] == 0
            });
            if take {
                break;
            }
            j -= 1;
        }

        // If no available candidate, topological sort fails.
        if j < 0 {
            return false;
        }

        // Output everything in the lock group. (Members of the same group must be
        // consecutive in any useful ordering — see the C comment.)
        let mut proc =
            with_state(|s| s.topo_procs[j as usize]).expect("candidate slot populated");
        if let Some(leader) = space.proc(proc).lock_group_leader {
            proc = leader;
        }
        for c in 0..=last {
            let matches = with_state(|s| match s.topo_procs[c as usize] {
                Some(tp) => tp == proc || space.proc(tp).lock_group_leader == Some(proc),
                None => false,
            });
            if matches {
                with_state(|s| {
                    let tp = s.topo_procs[c as usize].expect("matched slot is Some");
                    let dst = (i - nmatches) as usize;
                    s.wait_order_procs[ordering_off + dst] = tp;
                    s.topo_procs[c as usize] = None;
                });
                nmatches += 1;
            }
        }
        debug_assert!(nmatches > 0);
        i -= nmatches;

        // Update beforeConstraints counts of its predecessors.
        let mut k = with_state(|s| s.after_constraints[j as usize]);
        while k > 0 {
            with_state(|s| {
                let pred = cons[(k - 1) as usize].pred;
                s.before_constraints[pred as usize] -= 1;
            });
            k = cons[(k - 1) as usize].link;
        }
    }

    true
}

// ===========================================================================
// PrintLockQueue (deadlock.c:1052, DEBUG_DEADLOCK only)
// ===========================================================================

/// `PrintLockQueue(lock, info)` — print a lock's wait queue (the `DEBUG_DEADLOCK`
/// debug aid). Behind the `debug_deadlock` feature, mirroring the C
/// `#ifdef DEBUG_DEADLOCK`.
#[cfg(feature = "debug_deadlock")]
pub fn print_lock_queue(space: &LockSpace, lock: LockId, info: &str) {
    use std::io::Write;
    print!("{info} lock {lock:?} queue ");
    for &proc in &space.lock(lock).wait_procs {
        print!(" {}", space.proc(proc).pid);
    }
    println!();
    let _ = std::io::stdout().flush();
}

// ===========================================================================
// DeadLockReport (deadlock.c:1074)
// ===========================================================================

/// `DeadLockReport` — report a detected deadlock, with available details.
/// `ereport(ERROR, ...)` in C, so it never returns; here it returns the
/// constructed [`PgError`] (the caller raises it, preserving the `pg_noreturn`
/// contract). Uses the `deadlock_details[]` recorded by the last `FindLockCycle`.
pub fn dead_lock_report() -> PgError {
    build_dead_lock_report().0
}

/// The shared body of [`dead_lock_report`], returning both the `PgError` and the
/// rendered [`DeadlockReport`] strings.
///
/// The C original builds two `StringInfo`s (`clientbuf`/`logbuf`) in
/// `CurrentMemoryContext`, copies their data into the `ereport`, and lets the
/// `StringInfo`s be reclaimed when the error context is reset. The idiomatic
/// mirror OWNS a per-call [`MemoryContext`], builds the buffers as `PgString`s
/// charged to it, materializes the owned `String`s the report/`PgError` carry away
/// (the data-copy-into-the-ereport analog), then drops the context (freeing the
/// buffers). An OOM building the report degrades to the bare deadlock error,
/// exactly as a `ereport(ERROR)` from inside the builder would unwind to.
pub(crate) fn build_dead_lock_report() -> (PgError, DeadlockReport) {
    // The crate OWNS this per-call context (the StringInfo accounting context).
    let ctx = MemoryContext::new("DeadLockReport");

    match render_dead_lock_report(ctx.mcx()) {
        Ok((client_detail, log_detail)) => {
            // pgstat_report_deadlock()
            report_deadlock::call();

            let report = DeadlockReport {
                client_detail: client_detail.clone(),
                log_detail: log_detail.clone(),
            };

            // ereport(ERROR, (errcode(ERRCODE_T_R_DEADLOCK_DETECTED),
            //                 errmsg("deadlock detected"),
            //                 errdetail_internal("%s", clientbuf.data),
            //                 errdetail_log("%s", logbuf.data),
            //                 errhint("See server log for query details.")))
            let err = ereport(ERROR)
                .errcode(ERRCODE_T_R_DEADLOCK_DETECTED)
                .errmsg("deadlock detected")
                .errdetail_internal(client_detail)
                .errdetail_log(log_detail)
                .errhint("See server log for query details.")
                .into_error();

            (err, report)
        }
        Err(_oom) => {
            // An OOM building the report degrades to the bare deadlock error.
            report_deadlock::call();
            let err = ereport(ERROR)
                .errcode(ERRCODE_T_R_DEADLOCK_DETECTED)
                .errmsg("deadlock detected")
                .errhint("See server log for query details.")
                .into_error();
            (err, DeadlockReport::default())
        }
    }
}

/// Worker for [`build_dead_lock_report`]: build the client and log detail buffers
/// as `PgString`s charged to `mcx`, then materialize the owned `String`s the
/// report carries away. Returns `Err` on any allocation failure (the caller
/// degrades to the bare deadlock error).
fn render_dead_lock_report(mcx: mcx::Mcx<'_>) -> PgResult<(String, String)> {
    let mut clientbuf = PgString::new_in(mcx); // errdetail for client
    let mut logbuf = PgString::new_in(mcx); // errdetail for server log

    // The recorded cycle. A transient working snapshot of the workspace's
    // deadlock_details[] (the workspace's own array stays in ITS storage).
    let details: Vec<DeadlockInfo> =
        with_state(|s| s.deadlock_details[..s.n_deadlock_details as usize].to_vec());
    let n = details.len();

    // Generate the "waits for" lines sent to the client.
    for i in 0..n {
        let info = &details[i];
        // The last proc waits for the first one...
        let nextpid = if i < n - 1 {
            details[i + 1].pid
        } else {
            details[0].pid
        };

        // reset locktagbuf to hold next object description
        let locktagbuf = describe_lock_tag::call(info.locktag);

        if i > 0 {
            clientbuf.try_push_str("\n")?;
        }

        // _("Process %d waits for %s on %s; blocked by process %d.")
        let modename =
            get_lockmode_name::call(info.locktag.locktag_lockmethodid as u16, info.lockmode);
        let line = format!(
            "Process {} waits for {} on {}; blocked by process {}.",
            info.pid, modename, locktagbuf, nextpid
        );
        clientbuf.try_push_str(&line)?;
    }

    // Duplicate all the above for the server ...
    // (appendBinaryStringInfo(&logbuf, clientbuf.data, clientbuf.len))
    {
        let client_so_far = clientbuf.as_str().to_string();
        logbuf.try_push_str(&client_so_far)?;
    }

    // ... and add info about query strings.
    for info in &details {
        logbuf.try_push_str("\n")?;
        // _("Process %d: %s") — pgstat_get_backend_current_activity(pid, false)
        let activity = backend_current_activity::call(info.pid, false);
        let line = format!("Process {}: {}", info.pid, activity);
        logbuf.try_push_str(&line)?;
    }

    // Materialize the OWNED return strings (escape the context — the
    // data-copy-into-the-ereport analog). The context (and its charged buffers)
    // drops when this function returns up to build_dead_lock_report.
    Ok((clientbuf.as_str().to_string(), logbuf.as_str().to_string()))
}

// ===========================================================================
// RememberSimpleDeadLock (deadlock.c:1146)
// ===========================================================================

/// `RememberSimpleDeadLock(proc1, lockmode, lock, proc2)` — set up info for
/// `DeadLockReport` when `ProcSleep` detects a trivial (two-way) deadlock. `proc1`
/// wants to block for `lockmode` on `lock`, but `proc2` is already waiting and
/// would be blocked by `proc1`.
pub fn remember_simple_dead_lock(
    space: &LockSpace,
    proc1: ProcId,
    lockmode: LOCKMODE,
    lock: LockId,
    proc2: ProcId,
) {
    let lock_tag: LOCKTAG = space.lock(lock).tag;
    let proc1_pid = space.proc(proc1).pid;
    let proc2_wait_lock = space
        .proc(proc2)
        .wait_lock
        .expect("RememberSimpleDeadLock: proc2 must be waiting");
    let proc2_wait_lock_tag: LOCKTAG = space.lock(proc2_wait_lock).tag;
    let proc2_wait_lock_mode = space.proc(proc2).wait_lock_mode;
    let proc2_pid = space.proc(proc2).pid;

    with_state(|s| {
        // info = &deadlockDetails[0]
        s.deadlock_details[0].locktag = lock_tag;
        s.deadlock_details[0].lockmode = lockmode;
        s.deadlock_details[0].pid = proc1_pid;
        // info++ -> deadlockDetails[1]
        s.deadlock_details[1].locktag = proc2_wait_lock_tag;
        s.deadlock_details[1].lockmode = proc2_wait_lock_mode;
        s.deadlock_details[1].pid = proc2_pid;
        s.n_deadlock_details = 2;
    });
}

