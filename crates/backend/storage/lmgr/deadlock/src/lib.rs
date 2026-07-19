#![allow(non_snake_case)]

use std::cell::{Cell, RefCell};
use std::sync::atomic::Ordering::Relaxed;

use init_small::globals::MaxBackends;
use lmgr_proc::{foreach_lock_group_member, GetPGProcByNumber};
use mcx::{Mcx, MemoryContext, PgVec};
use types_core::{ProcNumber, INVALID_PROC_NUMBER};
use types_error::{PgError, PgResult, ERRCODE_T_R_DEADLOCK_DETECTED, ERROR};
use types_storage::lock::{
    DeadLockState, LOCK, LOCKBIT_ON, LOCKMETHODID, LOCKMODE, LOCKTAG, LOCKTAG_RELATION_EXTEND,
};
use types_storage::storage::PROC_IS_AUTOVACUUM;

#[derive(Clone, Copy)]
pub struct Edge {
    pub waiter: ProcNumber,
    pub blocker: ProcNumber,
    pub lock: *mut LOCK,
    pub pred: i32,
    pub link: i32,
}

impl Default for Edge {
    fn default() -> Self {
        Edge {
            waiter: INVALID_PROC_NUMBER,
            blocker: INVALID_PROC_NUMBER,
            lock: std::ptr::null_mut(),
            pred: 0,
            link: 0,
        }
    }
}

#[derive(Clone, Copy, Default)]
pub struct DeadLockInfo {
    pub locktag: LOCKTAG,
    pub lockmode: LOCKMODE,
    pub pid: i32,
}

#[derive(Clone, Copy)]
pub struct WaitOrder {
    pub lock: *mut LOCK,
    pub procs_offset: i32,
    pub nProcs: i32,
}

impl Default for WaitOrder {
    fn default() -> Self {
        WaitOrder {
            lock: std::ptr::null_mut(),
            procs_offset: 0,
            nProcs: 0,
        }
    }
}

// DeadLockCheck workspace, preallocated so the check never allocates (C runs
// it off a timeout with all lock partition LWLocks held). visitedProcs
// doubles as topoProcs per C (FindLockCycle and TopoSort never overlap);
// INVALID_PROC_NUMBER is TopoSort's consumed-slot NULL.
pub struct Workspace {
    visitedProcs: PgVec<'static, ProcNumber>,
    nVisitedProcs: i32,
    deadlockDetails: PgVec<'static, DeadLockInfo>,
    nDeadlockDetails: i32,
    beforeConstraints: PgVec<'static, i32>,
    afterConstraints: PgVec<'static, i32>,
    waitOrders: PgVec<'static, WaitOrder>,
    nWaitOrders: i32,
    waitOrderProcs: PgVec<'static, ProcNumber>,
    curConstraints: PgVec<'static, Edge>,
    nCurConstraints: i32,
    maxCurConstraints: i32,
    possibleConstraints: PgVec<'static, Edge>,
    nPossibleConstraints: i32,
    maxPossibleConstraints: i32,
}

thread_local! {
    static WORKSPACE: RefCell<Option<Workspace>> = const { RefCell::new(None) };
    static BLOCKING_AUTOVACUUM: Cell<ProcNumber> = const { Cell::new(INVALID_PROC_NUMBER) };
}

fn backend_mcx() -> Mcx<'static> {
    thread_local! {
        static CTX: Cell<Option<&'static MemoryContext>> = const { Cell::new(None) };
    }
    CTX.with(|c| match c.get() {
        Some(m) => m.mcx(),
        None => {
            let m: &'static MemoryContext =
                ::mcx::session_root("DeadLockChecking");
            // LIFO: empty the droppy workspace TLS before its context is
            // freed (all workspace vecs allocate from this context).
            ::mcx::register_session_cleanup(Box::new(|| {
                WORKSPACE.with(|w| drop(w.borrow_mut().take()));
            }));
            c.set(Some(m));
            m.mcx()
        }
    })
}

fn filled<T: Copy + Default>(mcx: Mcx<'static>, n: usize) -> PgVec<'static, T> {
    let mut v: PgVec<'static, T> = PgVec::with_capacity_in(n, mcx);
    v.resize(n, T::default());
    v
}

pub fn InitDeadLockChecking() -> PgResult<()> {
    let max_backends = MaxBackends() as usize;
    let mcx = backend_mcx();
    let ws = Workspace {
        visitedProcs: filled(mcx, max_backends),
        nVisitedProcs: 0,
        deadlockDetails: filled(mcx, max_backends),
        nDeadlockDetails: 0,
        beforeConstraints: filled(mcx, max_backends),
        afterConstraints: filled(mcx, max_backends),
        waitOrders: filled(mcx, max_backends / 2),
        nWaitOrders: 0,
        waitOrderProcs: filled(mcx, max_backends),
        curConstraints: filled(mcx, max_backends),
        nCurConstraints: 0,
        maxCurConstraints: MaxBackends(),
        possibleConstraints: filled(mcx, max_backends * 4),
        nPossibleConstraints: 0,
        maxPossibleConstraints: MaxBackends() * 4,
    };
    WORKSPACE.with(|w| *w.borrow_mut() = Some(ws));
    Ok(())
}

fn leader_of(procno: ProcNumber) -> ProcNumber {
    let leader = GetPGProcByNumber(procno).lockGroupLeader.load(Relaxed);
    if leader == INVALID_PROC_NUMBER {
        procno
    } else {
        leader
    }
}

fn is_waiting(procno: ProcNumber) -> bool {
    let proc = GetPGProcByNumber(procno);
    !proc.links.get().is_detached() && !proc.waitLock.get().is_null()
}

/// Caller must hold all lock partition LWLocks (the CheckDeadLock contract).
pub fn DeadLockCheck(procno: ProcNumber) -> DeadLockState {
    WORKSPACE.with(|w| {
        let mut ws = w.borrow_mut();
        let ws = ws.as_mut().expect("InitDeadLockChecking not run");

        ws.nCurConstraints = 0;
        ws.nPossibleConstraints = 0;
        ws.nWaitOrders = 0;
        BLOCKING_AUTOVACUUM.set(INVALID_PROC_NUMBER);

        if DeadLockCheckRecurse(ws, procno) {
            let mut nSoftEdges = 0;
            ws.nWaitOrders = 0;
            if !FindLockCycle(ws, procno, 0, &mut nSoftEdges) {
                panic!("deadlock seems to have disappeared");
            }
            return DeadLockState::HardDeadLock;
        }

        for i in 0..ws.nWaitOrders as usize {
            let lock = ws.waitOrders[i].lock;
            let off = ws.waitOrders[i].procs_offset as usize;
            let n = ws.waitOrders[i].nProcs as usize;
            // SAFETY: all partition locks held exclusive; the lock and queue
            // membership were captured under those locks in this same check.
            unsafe {
                debug_assert_eq!(n, (*lock).waitProcs.count as usize);
                lock::SetWaitQueueOrder(lock, &ws.waitOrderProcs[off..off + n]);
                lock::ProcLockWakeup(lock::GetLocksMethodTable(&*lock), lock);
            }
        }

        if ws.nWaitOrders > 0 {
            DeadLockState::SoftDeadLock
        } else if BLOCKING_AUTOVACUUM.get() != INVALID_PROC_NUMBER {
            DeadLockState::BlockedByAutoVacuum
        } else {
            DeadLockState::NoDeadLock
        }
    })
}

pub fn GetBlockingAutoVacuumPgproc() -> Option<ProcNumber> {
    let procno = BLOCKING_AUTOVACUUM.get();
    BLOCKING_AUTOVACUUM.set(INVALID_PROC_NUMBER);
    (procno != INVALID_PROC_NUMBER).then_some(procno)
}

// Returns true if no solution exists; false if a deadlock-free state is
// attainable (waitOrders[] then holds the required rearrangements).
fn DeadLockCheckRecurse(ws: &mut Workspace, procno: ProcNumber) -> bool {
    let nEdges = TestConfiguration(ws, procno);
    if nEdges < 0 {
        return true;
    }
    if nEdges == 0 {
        return false;
    }
    if ws.nCurConstraints >= ws.maxCurConstraints {
        return true;
    }
    let oldPossibleConstraints = ws.nPossibleConstraints;
    let savedList = ws.nPossibleConstraints + nEdges + MaxBackends() <= ws.maxPossibleConstraints;
    if savedList {
        ws.nPossibleConstraints += nEdges;
    }

    for i in 0..nEdges {
        if !savedList && i > 0 && nEdges != TestConfiguration(ws, procno) {
            panic!("inconsistent results during deadlock check");
        }
        ws.curConstraints[ws.nCurConstraints as usize] =
            ws.possibleConstraints[(oldPossibleConstraints + i) as usize];
        ws.nCurConstraints += 1;
        if !DeadLockCheckRecurse(ws, procno) {
            return false;
        }
        ws.nCurConstraints -= 1;
    }
    ws.nPossibleConstraints = oldPossibleConstraints;
    true
}

// 0: configuration good; -1: hard deadlock or inconsistent constraints; >0:
// number of soft edges of one soft cycle, stored starting at
// possibleConstraints[nPossibleConstraints].
fn TestConfiguration(ws: &mut Workspace, startProc: ProcNumber) -> i32 {
    let mut softFound = 0;
    let softBase = ws.nPossibleConstraints as usize;
    let mut nSoftEdges = 0;

    if ws.nPossibleConstraints + MaxBackends() > ws.maxPossibleConstraints {
        return -1;
    }
    if !ExpandConstraints(ws) {
        return -1;
    }

    for i in 0..ws.nCurConstraints as usize {
        let (waiter, blocker) = (ws.curConstraints[i].waiter, ws.curConstraints[i].blocker);
        if FindLockCycle(ws, waiter, softBase, &mut nSoftEdges) {
            if nSoftEdges == 0 {
                return -1;
            }
            softFound = nSoftEdges;
        }
        if FindLockCycle(ws, blocker, softBase, &mut nSoftEdges) {
            if nSoftEdges == 0 {
                return -1;
            }
            softFound = nSoftEdges;
        }
    }
    if FindLockCycle(ws, startProc, softBase, &mut nSoftEdges) {
        if nSoftEdges == 0 {
            return -1;
        }
        softFound = nSoftEdges;
    }
    softFound
}

fn FindLockCycle(
    ws: &mut Workspace,
    checkProc: ProcNumber,
    softBase: usize,
    nSoftEdges: &mut i32,
) -> bool {
    ws.nVisitedProcs = 0;
    ws.nDeadlockDetails = 0;
    *nSoftEdges = 0;
    FindLockCycleRecurse(ws, checkProc, 0, softBase, nSoftEdges)
}

fn FindLockCycleRecurse(
    ws: &mut Workspace,
    checkProc: ProcNumber,
    depth: i32,
    softBase: usize,
    nSoftEdges: &mut i32,
) -> bool {
    let checkProc = leader_of(checkProc);

    for i in 0..ws.nVisitedProcs as usize {
        if ws.visitedProcs[i] == checkProc {
            if i == 0 {
                debug_assert!(depth <= MaxBackends());
                ws.nDeadlockDetails = depth;
                return true;
            }
            return false;
        }
    }
    debug_assert!(ws.nVisitedProcs < MaxBackends());
    ws.visitedProcs[ws.nVisitedProcs as usize] = checkProc;
    ws.nVisitedProcs += 1;

    if is_waiting(checkProc)
        && FindLockCycleRecurseMember(ws, checkProc, checkProc, depth, softBase, nSoftEdges)
    {
        return true;
    }

    let mut found = false;
    foreach_lock_group_member(GetPGProcByNumber(checkProc), |member| {
        if member != checkProc
            && is_waiting(member)
            && FindLockCycleRecurseMember(ws, member, checkProc, depth, softBase, nSoftEdges)
        {
            found = true;
            return false;
        }
        true
    });
    found
}

fn FindLockCycleRecurseMember(
    ws: &mut Workspace,
    checkProc: ProcNumber,
    checkProcLeader: ProcNumber,
    depth: i32,
    softBase: usize,
    nSoftEdges: &mut i32,
) -> bool {
    let proc = GetPGProcByNumber(checkProc);
    let lock: *mut LOCK = proc.waitLock.get();
    let waitLockMode = proc.waitLockMode.get();

    // SAFETY: all lock partition LWLocks held for the whole check; the shared
    // lock table cannot change under us.
    unsafe {
        if (*lock).tag.locktag_type == LOCKTAG_RELATION_EXTEND {
            return false;
        }

        let lockMethodTable = lock::GetLocksMethodTable(&*lock);
        let numLockModes = lockMethodTable.numLockModes;
        let conflictMask = lockMethodTable.conflictTab[waitLockMode as usize];

        let mut found = false;
        lock::foreach_proclock_on_lock(lock, |proclock| {
            let other = (*proclock).tag.myProc;
            let other_leader = leader_of(other);
            if other_leader != checkProcLeader {
                for lm in 1..=numLockModes {
                    if (*proclock).holdMask & LOCKBIT_ON(lm) != 0
                        && conflictMask & LOCKBIT_ON(lm) != 0
                    {
                        if FindLockCycleRecurse(ws, other, depth + 1, softBase, nSoftEdges) {
                            let info = &mut ws.deadlockDetails[depth as usize];
                            info.locktag = (*lock).tag;
                            info.lockmode = waitLockMode;
                            info.pid = proc.pid.load(Relaxed);
                            found = true;
                            return false;
                        }
                        if lmgr_proc::MyProc() == Some(checkProc)
                            && GetPGProcByNumber(other).statusFlags.load(Relaxed)
                                & PROC_IS_AUTOVACUUM
                                != 0
                        {
                            BLOCKING_AUTOVACUUM.set(other);
                        }
                        break;
                    }
                }
            }
            true
        });
        if found {
            return true;
        }

        // Soft-block only counts when the same proc does not also hard-block
        // (hence after the hard scan). A proposed reordering of this lock's
        // queue is believed over the actual order.
        let mut order_idx = None;
        for i in 0..ws.nWaitOrders as usize {
            if ws.waitOrders[i].lock == lock {
                order_idx = Some(i);
                break;
            }
        }

        if let Some(i) = order_idx {
            let off = ws.waitOrders[i].procs_offset as usize;
            let queue_size = ws.waitOrders[i].nProcs as usize;
            for j in 0..queue_size {
                let other = ws.waitOrderProcs[off + j];
                let other_leader = leader_of(other);
                // TopoSort keeps group members adjacent: reaching our own
                // group means all preceding conflicts have been seen.
                if other_leader == checkProcLeader {
                    break;
                }
                if LOCKBIT_ON(GetPGProcByNumber(other).waitLockMode.get()) & conflictMask != 0
                    && FindLockCycleRecurse(ws, other, depth + 1, softBase, nSoftEdges)
                {
                    let info = &mut ws.deadlockDetails[depth as usize];
                    info.locktag = (*lock).tag;
                    info.lockmode = waitLockMode;
                    info.pid = proc.pid.load(Relaxed);

                    debug_assert!(*nSoftEdges < MaxBackends());
                    let edge = &mut ws.possibleConstraints[softBase + *nSoftEdges as usize];
                    edge.waiter = checkProcLeader;
                    edge.blocker = other_leader;
                    edge.lock = lock;
                    *nSoftEdges += 1;
                    return true;
                }
            }
        } else {
            let mut lastGroupMember = INVALID_PROC_NUMBER;
            if proc.lockGroupLeader.load(Relaxed) == INVALID_PROC_NUMBER {
                lastGroupMember = checkProc;
            } else {
                lock::wq_foreach(lock, |other| {
                    if GetPGProcByNumber(other).lockGroupLeader.load(Relaxed) == checkProcLeader
                    {
                        lastGroupMember = other;
                    }
                    true
                });
                debug_assert!(lastGroupMember != INVALID_PROC_NUMBER);
            }

            let mut found = false;
            lock::wq_foreach(lock, |other| {
                if other == lastGroupMember {
                    return false;
                }
                let other_leader = leader_of(other);
                if LOCKBIT_ON(GetPGProcByNumber(other).waitLockMode.get()) & conflictMask != 0
                    && other_leader != checkProcLeader
                    && FindLockCycleRecurse(ws, other, depth + 1, softBase, nSoftEdges)
                {
                    let info = &mut ws.deadlockDetails[depth as usize];
                    info.locktag = (*lock).tag;
                    info.lockmode = waitLockMode;
                    info.pid = proc.pid.load(Relaxed);

                    debug_assert!(*nSoftEdges < MaxBackends());
                    let edge = &mut ws.possibleConstraints[softBase + *nSoftEdges as usize];
                    edge.waiter = checkProcLeader;
                    edge.blocker = other_leader;
                    edge.lock = lock;
                    *nSoftEdges += 1;
                    found = true;
                    return false;
                }
                true
            });
            if found {
                return true;
            }
        }
    }
    false
}

// Expand curConstraints[0..nCurConstraints] into waitOrders[]. Returns false
// on contradictory constraints.
fn ExpandConstraints(ws: &mut Workspace) -> bool {
    let mut nWaitOrderProcs: i32 = 0;
    ws.nWaitOrders = 0;

    for i in (0..ws.nCurConstraints as usize).rev() {
        let lock = ws.curConstraints[i].lock;
        if (0..ws.nWaitOrders as usize).any(|j| ws.waitOrders[j].lock == lock) {
            continue;
        }
        // SAFETY: partition locks held; lock pointer valid.
        let queue_len = unsafe { (*lock).waitProcs.count as i32 };
        let order = WaitOrder {
            lock,
            procs_offset: nWaitOrderProcs,
            nProcs: queue_len,
        };
        ws.waitOrders[ws.nWaitOrders as usize] = order;
        nWaitOrderProcs += queue_len;
        debug_assert!(nWaitOrderProcs <= MaxBackends());

        if !TopoSort(ws, lock, i + 1, order.procs_offset as usize) {
            return false;
        }
        ws.nWaitOrders += 1;
    }
    true
}

// Reorder lock's wait queue so each of curConstraints[0..nConstraints]'s
// waiters precedes its blocker, minimizing movement. Output written to
// waitOrderProcs[ordering_off..]. Returns false on contradiction.
fn TopoSort(ws: &mut Workspace, lock: *mut LOCK, nConstraints: usize, ordering_off: usize) -> bool {
    let mut queue_size = 0usize;
    // SAFETY: partition locks held.
    unsafe {
        lock::wq_foreach(lock, |procno| {
            ws.visitedProcs[queue_size] = procno;
            queue_size += 1;
            true
        });
        debug_assert_eq!(queue_size, (*lock).waitProcs.count as usize);
    }

    ws.beforeConstraints[..queue_size].fill(0);
    ws.afterConstraints[..queue_size].fill(0);
    for i in 0..nConstraints {
        let waiter = ws.curConstraints[i].waiter;
        debug_assert!(waiter != INVALID_PROC_NUMBER);
        let mut jj: i32 = -1;
        for j in (0..queue_size).rev() {
            let w = ws.visitedProcs[j];
            if w == waiter || GetPGProcByNumber(w).lockGroupLeader.load(Relaxed) == waiter {
                debug_assert!(GetPGProcByNumber(w).waitLock.get() == lock);
                if jj == -1 {
                    jj = j as i32;
                } else {
                    debug_assert!(ws.beforeConstraints[j] <= 0);
                    ws.beforeConstraints[j] = -1;
                }
            }
        }

        if jj < 0 {
            continue;
        }

        let blocker = ws.curConstraints[i].blocker;
        debug_assert!(blocker != INVALID_PROC_NUMBER);
        let mut kk: i32 = -1;
        for k in (0..queue_size).rev() {
            let b = ws.visitedProcs[k];
            if b == blocker || GetPGProcByNumber(b).lockGroupLeader.load(Relaxed) == blocker {
                debug_assert!(GetPGProcByNumber(b).waitLock.get() == lock);
                if kk == -1 {
                    kk = k as i32;
                } else {
                    debug_assert!(ws.beforeConstraints[k] <= 0);
                    ws.beforeConstraints[k] = -1;
                }
            }
        }
        if kk < 0 {
            continue;
        }

        debug_assert!(ws.beforeConstraints[jj as usize] >= 0);
        ws.beforeConstraints[jj as usize] += 1;
        ws.curConstraints[i].pred = jj;
        ws.curConstraints[i].link = ws.afterConstraints[kk as usize];
        ws.afterConstraints[kk as usize] = i as i32 + 1;
    }

    let mut last = queue_size as i32 - 1;
    let mut i = queue_size as i32 - 1;
    while i >= 0 {
        while ws.visitedProcs[last as usize] == INVALID_PROC_NUMBER {
            last -= 1;
        }
        let mut j = last;
        while j >= 0 {
            if ws.visitedProcs[j as usize] != INVALID_PROC_NUMBER
                && ws.beforeConstraints[j as usize] == 0
            {
                break;
            }
            j -= 1;
        }
        if j < 0 {
            return false;
        }

        let proc = leader_of(ws.visitedProcs[j as usize]);
        let mut nmatches = 0i32;
        for c in 0..=last as usize {
            let t = ws.visitedProcs[c];
            if t != INVALID_PROC_NUMBER
                && (t == proc || GetPGProcByNumber(t).lockGroupLeader.load(Relaxed) == proc)
            {
                ws.waitOrderProcs[ordering_off + (i - nmatches) as usize] = t;
                ws.visitedProcs[c] = INVALID_PROC_NUMBER;
                nmatches += 1;
            }
        }
        debug_assert!(nmatches > 0);
        i -= nmatches;

        let mut k = ws.afterConstraints[j as usize];
        while k > 0 {
            ws.beforeConstraints[ws.curConstraints[k as usize - 1].pred as usize] -= 1;
            k = ws.curConstraints[k as usize - 1].link;
        }
    }
    true
}

pub fn DeadLockReport() -> PgResult<()> {
    let details: Vec<DeadLockInfo> = WORKSPACE.with(|w| {
        let ws = w.borrow();
        let ws = ws.as_ref().expect("InitDeadLockChecking not run");
        ws.deadlockDetails[..ws.nDeadlockDetails as usize].to_vec()
    });
    let n = details.len();

    let mut clientbuf = String::new();
    for (i, info) in details.iter().enumerate() {
        let nextpid = if i < n - 1 {
            details[i + 1].pid
        } else {
            details[0].pid
        };
        let locktag_desc = lmgr_seams::describe_lock_tag::call(info.locktag);
        if i > 0 {
            clientbuf.push('\n');
        }
        clientbuf.push_str(&format!(
            "Process {} waits for {} on {}; blocked by process {}.",
            info.pid,
            lock::GetLockmodeName(info.locktag.locktag_lockmethodid as LOCKMETHODID, info.lockmode),
            locktag_desc,
            nextpid
        ));
    }

    let mut logbuf = clientbuf.clone();
    for info in &details {
        let activity = backend_status::pgstat_get_backend_current_activity(info.pid, false)
            .unwrap_or_else(|_| "<backend information not available>".into());
        logbuf.push('\n');
        logbuf.push_str(&format!("Process {}: {}", info.pid, activity));
    }

    pgstat::database::pgstat_report_deadlock();

    Err(Box::new(
        PgError::new(ERROR, "deadlock detected")
            .with_sqlstate(ERRCODE_T_R_DEADLOCK_DETECTED)
            .with_detail(clientbuf)
            .with_detail_log(logbuf)
            .with_hint("See server log for query details."),
    ))
}

/// ProcSleep found a trivial two-way deadlock: `checker` wants `lockmode` on
/// `locktag`, but `blocker` is already waiting and would be blocked by
/// `checker`. Caller holds the lock's partition LWLock.
pub fn RememberSimpleDeadLock(
    checker: ProcNumber,
    lockmode: LOCKMODE,
    locktag: LOCKTAG,
    blocker: ProcNumber,
) {
    WORKSPACE.with(|w| {
        let mut ws = w.borrow_mut();
        let ws = ws.as_mut().expect("InitDeadLockChecking not run");
        let bproc = GetPGProcByNumber(blocker);
        ws.deadlockDetails[0] = DeadLockInfo {
            locktag,
            lockmode,
            pid: GetPGProcByNumber(checker).pid.load(Relaxed),
        };
        // SAFETY: partition lock held; blocker is on this lock's wait queue.
        let blocker_tag = unsafe { (*bproc.waitLock.get()).tag };
        ws.deadlockDetails[1] = DeadLockInfo {
            locktag: blocker_tag,
            lockmode: bproc.waitLockMode.get(),
            pid: bproc.pid.load(Relaxed),
        };
        ws.nDeadlockDetails = 2;
    });
}

pub fn init_seams() {
    deadlock_seams::init_dead_lock_checking::set(InitDeadLockChecking);
    deadlock_seams::dead_lock_check::set(DeadLockCheck);
    deadlock_seams::dead_lock_report::set(DeadLockReport);
    deadlock_seams::remember_simple_deadlock::set(RememberSimpleDeadLock);
    deadlock_seams::get_blocking_autovacuum_procno::set(GetBlockingAutoVacuumPgproc);
}

#[cfg(test)]
mod tests;
