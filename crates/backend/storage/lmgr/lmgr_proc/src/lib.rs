#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod globals;

use std::cell::Cell;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::OnceLock;

use init_small::globals as g;
use types_core::{
    BackendType, InvalidLocalTransactionId, InvalidOid, InvalidTransactionId, ProcNumber,
    TRANSACTION_STATUS_IN_PROGRESS, INVALID_PROC_NUMBER,
};
use types_error::{PgError, PgResult, ERRCODE_TOO_MANY_CONNECTIONS, ERROR, FATAL};
use types_storage::lock::{DeadLockState, DEFAULT_LOCKMETHOD, USER_LOCKMETHOD};
use types_storage::storage::{
    pg_atomic_uint32, proclist_head, proclist_node, FreeListId, LWLockWaitList, PGPROC, PROC_HDR,
    Spinlock, SyncCell, XidCacheStatus, FP_LOCK_GROUPS_PER_BACKEND_MAX, FP_LOCK_SLOTS_PER_GROUP,
    LWTRANCHE_LOCK_FASTPATH, NUM_AUXILIARY_PROCS, NUM_LOCK_PARTITIONS, NUM_SPECIAL_WORKER_PROCS,
    PROC_IS_AUTOVACUUM, PROC_WAIT_STATUS_OK,
};
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET};

const DEFAULT_SPINS_PER_DELAY: i32 = 100;
const SYNC_REP_NOT_WAITING: i32 = 0;
const LW_WS_NOT_WAITING: u8 = 0;

// Fixed at InitProcGlobal like the shmem it sizes; max_wal_senders is also
// the FATAL-message operand at InitProcess time.
#[derive(Clone, Copy, Debug)]
pub struct ProcGlobalConfig {
    pub autovacuum_worker_slots: i32,
    pub max_wal_senders: i32,
    pub max_prepared_xacts: i32,
    pub fastpath_lock_groups_per_backend: i32,
}

static PROC_GLOBAL: OnceLock<&'static PROC_HDR> = OnceLock::new();
static PROC_CONFIG: OnceLock<ProcGlobalConfig> = OnceLock::new();
static ProcStructLock: Spinlock = Spinlock::new();

thread_local! {
    static MY_PROC: Cell<ProcNumber> = const { Cell::new(INVALID_PROC_NUMBER) };
    static DEADLOCK_STATE: Cell<DeadLockState> = const { Cell::new(DeadLockState::NotYetChecked) };
    static GOT_DEADLOCK_TIMEOUT: Cell<bool> = const { Cell::new(false) };
}

pub fn ProcGlobal() -> &'static PROC_HDR {
    PROC_GLOBAL
        .get()
        .unwrap_or_else(|| panic!("proc header uninitialized"))
}

pub fn MyProc() -> Option<ProcNumber> {
    let procno = MY_PROC.get();
    (procno != INVALID_PROC_NUMBER).then_some(procno)
}

fn my_proc_required() -> ProcNumber {
    MyProc().unwrap_or_else(|| panic!("MyProc is not set"))
}

pub fn GetPGProcByNumber(procno: ProcNumber) -> &'static PGPROC {
    &ProcGlobal().allProcs[procno as usize]
}

fn spin_acquire(lock: &Spinlock) {
    if lock.tas() != 0 {
        let mut delay =
            s_lock_seams::SpinDelayStatus::new(file!(), line!() as i32, "ProcStructLock");
        while lock.tas_spin() != 0 {
            s_lock_seams::perform_spin_delay::call(&mut delay);
        }
        s_lock_seams::finish_spin_delay::call(&delay);
    }
}

// C's dlists realized over the index-addressed allProcs arena:
// {head,tail} of ProcNumbers, INVALID_PROC_NUMBER-terminated; the node
// accessor selects which embedded link the list threads through.
type NodeOf = fn(&PGPROC) -> &SyncCell<proclist_node>;

fn links_of(proc: &PGPROC) -> &SyncCell<proclist_node> {
    &proc.links
}

fn group_link_of(proc: &PGPROC) -> &SyncCell<proclist_node> {
    &proc.lockGroupLink
}

fn plist_is_empty(list: &SyncCell<proclist_head>) -> bool {
    list.get().head == INVALID_PROC_NUMBER
}

fn plist_push_head(
    hdr: &PROC_HDR,
    list: &SyncCell<proclist_head>,
    procno: ProcNumber,
    node_of: NodeOf,
) {
    let mut head = list.get();
    let node = proclist_node {
        prev: INVALID_PROC_NUMBER,
        next: head.head,
    };
    if head.head == INVALID_PROC_NUMBER {
        head.tail = procno;
    } else {
        let next_cell = node_of(&hdr.allProcs[head.head as usize]);
        let mut next = next_cell.get();
        next.prev = procno;
        next_cell.set(next);
    }
    head.head = procno;
    node_of(&hdr.allProcs[procno as usize]).set(node);
    list.set(head);
}

fn plist_push_tail(
    hdr: &PROC_HDR,
    list: &SyncCell<proclist_head>,
    procno: ProcNumber,
    node_of: NodeOf,
) {
    let mut head = list.get();
    let node = proclist_node {
        prev: head.tail,
        next: INVALID_PROC_NUMBER,
    };
    if head.tail == INVALID_PROC_NUMBER {
        head.head = procno;
    } else {
        let prev_cell = node_of(&hdr.allProcs[head.tail as usize]);
        let mut prev = prev_cell.get();
        prev.next = procno;
        prev_cell.set(prev);
    }
    head.tail = procno;
    node_of(&hdr.allProcs[procno as usize]).set(node);
    list.set(head);
}

fn plist_delete(
    hdr: &PROC_HDR,
    list: &SyncCell<proclist_head>,
    procno: ProcNumber,
    node_of: NodeOf,
) {
    let mut head = list.get();
    let node = node_of(&hdr.allProcs[procno as usize]).get();
    if node.prev == INVALID_PROC_NUMBER {
        head.head = node.next;
    } else {
        let prev_cell = node_of(&hdr.allProcs[node.prev as usize]);
        let mut prev = prev_cell.get();
        prev.next = node.next;
        prev_cell.set(prev);
    }
    if node.next == INVALID_PROC_NUMBER {
        head.tail = node.prev;
    } else {
        let next_cell = node_of(&hdr.allProcs[node.next as usize]);
        let mut next = next_cell.get();
        next.prev = node.prev;
        next_cell.set(next);
    }
    node_of(&hdr.allProcs[procno as usize]).set(proclist_node::detached());
    list.set(head);
}

fn plist_pop_head(
    hdr: &PROC_HDR,
    list: &SyncCell<proclist_head>,
    node_of: NodeOf,
) -> Option<ProcNumber> {
    let procno = list.get().head;
    if procno == INVALID_PROC_NUMBER {
        return None;
    }
    plist_delete(hdr, list, procno, node_of);
    Some(procno)
}

fn freelist(hdr: &PROC_HDR, id: FreeListId) -> &SyncCell<proclist_head> {
    match id {
        FreeListId::Regular => &hdr.freeProcs,
        FreeListId::Autovac => &hdr.autovacFreeProcs,
        FreeListId::Bgworker => &hdr.bgworkerFreeProcs,
        FreeListId::Walsender => &hdr.walsenderFreeProcs,
    }
}

// InitProcess's freelist choice; must match InitProcGlobal's construction.
fn freelist_id_for(backend_type: BackendType) -> FreeListId {
    match backend_type {
        BackendType::AutovacWorker | BackendType::AutovacLauncher | BackendType::SlotsyncWorker => {
            FreeListId::Autovac
        }
        BackendType::BgWorker => FreeListId::Bgworker,
        BackendType::WalSender => FreeListId::Walsender,
        _ => FreeListId::Regular,
    }
}

const fn maxalign(len: usize) -> usize {
    (len + 7) & !7
}

fn total_procs(cfg: &ProcGlobalConfig) -> PgResult<usize> {
    shmem_seams::add_size::call(
        g::MaxBackends() as usize,
        shmem_seams::add_size::call(NUM_AUXILIARY_PROCS as usize, cfg.max_prepared_xacts as usize)?,
    )
}

fn PGProcShmemSize(cfg: &ProcGlobalConfig) -> PgResult<usize> {
    let per_proc = core::mem::size_of::<PGPROC>()
        + core::mem::size_of::<pg_atomic_uint32>()
        + core::mem::size_of::<SyncCell<XidCacheStatus>>()
        + 1;
    shmem_seams::mul_size::call(total_procs(cfg)?, per_proc)
}

fn FastPathLockShmemSize(cfg: &ProcGlobalConfig) -> PgResult<usize> {
    let groups = cfg.fastpath_lock_groups_per_backend as usize;
    let bits = maxalign(groups * core::mem::size_of::<u64>());
    let relids = maxalign(groups * FP_LOCK_SLOTS_PER_GROUP as usize * core::mem::size_of::<u32>());
    shmem_seams::mul_size::call(total_procs(cfg)?, bits + relids)
}

pub fn ProcGlobalShmemSize(cfg: &ProcGlobalConfig) -> PgResult<usize> {
    let size = core::mem::size_of::<PROC_HDR>() + core::mem::size_of::<Spinlock>();
    let size = shmem_seams::add_size::call(size, PGProcShmemSize(cfg)?)?;
    shmem_seams::add_size::call(size, FastPathLockShmemSize(cfg)?)
}

pub fn ProcGlobalSemas() -> i32 {
    g::MaxBackends() + NUM_AUXILIARY_PROCS
}

pub fn InitProcGlobal(cfg: &ProcGlobalConfig) {
    let max_backends = g::MaxBackends();
    let max_connections = g::MaxConnections();
    let max_worker_processes = g::max_worker_processes();
    assert!(max_backends > 0, "MaxBackends not initialized");
    debug_assert_eq!(
        max_backends,
        max_connections
            + cfg.autovacuum_worker_slots
            + max_worker_processes
            + cfg.max_wal_senders
            + NUM_SPECIAL_WORKER_PROCS
    );
    let groups = cfg.fastpath_lock_groups_per_backend;
    assert!(groups >= 1 && groups <= FP_LOCK_GROUPS_PER_BACKEND_MAX);

    let total = max_backends + NUM_AUXILIARY_PROCS + cfg.max_prepared_xacts;
    let slots_per_backend = groups * FP_LOCK_SLOTS_PER_GROUP;

    let fp_bits: &'static [SyncCell<u64>] = (0..total as usize * groups as usize)
        .map(|_| SyncCell::new(0u64))
        .collect::<Vec<_>>()
        .leak();
    let fp_relids: &'static [SyncCell<u32>] = (0..total as usize * slots_per_backend as usize)
        .map(|_| SyncCell::new(InvalidOid))
        .collect::<Vec<_>>()
        .leak();

    let mut procs = Vec::with_capacity(total as usize);
    for i in 0..total {
        let mut proc = PGPROC::new_zeroed();
        proc.fpLockBits.set(fp_bits[(i * groups) as usize..].as_ptr());
        proc.fpRelId
            .set(fp_relids[(i * slots_per_backend) as usize..].as_ptr());

        // Prepared-xact dummy PGPROCs never run: no sema/latch/fpInfoLock.
        if i < max_backends + NUM_AUXILIARY_PROCS {
            pg_sema_seams::pg_semaphore_create::call(i);
            // InitSharedLatch + LWLockInitialize(fpInfoLock, LOCK_FASTPATH).
            proc.procLatch.is_shared.store(true, Relaxed);
            proc.fpInfoLock.tranche = LWTRANCHE_LOCK_FASTPATH as u16;
            proc.fpInfoLock.state = pg_atomic_uint32::new(lwlock::LW_FLAG_RELEASE_OK);
            proc.fpInfoLock.waiters = LWLockWaitList::new(proclist_head::default());
        }

        proc.procArrayGroupNext
            .value
            .store(INVALID_PROC_NUMBER as u32, Relaxed);
        proc.clogGroupNext
            .value
            .store(INVALID_PROC_NUMBER as u32, Relaxed);

        let list = if i < max_connections {
            Some(FreeListId::Regular)
        } else if i < max_connections + cfg.autovacuum_worker_slots + NUM_SPECIAL_WORKER_PROCS {
            Some(FreeListId::Autovac)
        } else if i < max_connections
            + cfg.autovacuum_worker_slots
            + NUM_SPECIAL_WORKER_PROCS
            + max_worker_processes
        {
            Some(FreeListId::Bgworker)
        } else if i < max_backends {
            Some(FreeListId::Walsender)
        } else {
            None
        };
        proc.procgloballist.set(list);
        procs.push(proc);
    }

    let mut hdr = PROC_HDR::new_zeroed();
    hdr.allProcs = procs.leak();
    hdr.xids = (0..total)
        .map(|_| pg_atomic_uint32::new(0))
        .collect::<Vec<_>>()
        .leak();
    hdr.subxidStates = (0..total)
        .map(|_| SyncCell::new(XidCacheStatus::default()))
        .collect::<Vec<_>>()
        .leak();
    hdr.statusFlags = (0..total)
        .map(|_| std::sync::atomic::AtomicU8::new(0))
        .collect::<Vec<_>>()
        .leak();
    // XXX (as C): allProcCount excludes prepared xacts.
    hdr.allProcCount = (max_backends + NUM_AUXILIARY_PROCS) as u32;
    hdr.fpLockGroupsPerBackend = groups as u32;
    hdr.spins_per_delay.set(DEFAULT_SPINS_PER_DELAY);
    let hdr: &'static PROC_HDR = Box::leak(Box::new(hdr));

    for i in 0..max_backends {
        if let Some(list) = hdr.allProcs[i as usize].procgloballist.get() {
            plist_push_tail(hdr, freelist(hdr, list), i, links_of);
        }
    }

    PROC_CONFIG
        .set(*cfg)
        .unwrap_or_else(|_| panic!("InitProcGlobal called twice"));
    PROC_GLOBAL
        .set(hdr)
        .unwrap_or_else(|_| panic!("InitProcGlobal called twice"));
}

pub fn AuxiliaryProcsBase() -> ProcNumber {
    ProcGlobal().allProcCount as ProcNumber - NUM_AUXILIARY_PROCS
}

pub fn PreparedXactProcsBase() -> ProcNumber {
    ProcGlobal().allProcCount as ProcNumber
}

// The field resets shared verbatim between InitProcess/InitAuxiliaryProcess.
fn init_my_proc_common(proc: &PGPROC, vxid_procno: ProcNumber) {
    proc.links.set(proclist_node::detached());
    proc.waitStatus.store(PROC_WAIT_STATUS_OK, Relaxed);
    proc.fpVXIDLock.store(false, Relaxed);
    proc.fpLocalTransactionId
        .store(InvalidLocalTransactionId, Relaxed);
    proc.xid.value.store(InvalidTransactionId, Relaxed);
    proc.xmin.value.store(InvalidTransactionId, Relaxed);
    proc.pid.store(g::MyProcPid(), Relaxed);
    proc.vxid.procNumber.store(vxid_procno, Relaxed);
    proc.vxid.lxid.store(InvalidLocalTransactionId, Relaxed);
    proc.databaseId.store(InvalidOid, Relaxed);
    proc.roleId.store(InvalidOid, Relaxed);
    proc.tempNamespaceId.store(InvalidOid, Relaxed);
    proc.delayChkptFlags.store(0, Relaxed);
    proc.statusFlags.store(0, Relaxed);
    proc.lwWaiting.store(LW_WS_NOT_WAITING, Relaxed);
    proc.lwWaitMode.store(0, Relaxed);
    proc.waitLock.set(None);
    proc.waitProcLock.set(None);
    proc.waitStart.write(0);
    for i in 0..NUM_LOCK_PARTITIONS as usize {
        // Last owner should have released all locks.
        debug_assert!(proc.myProcLocks[i].get().head.next.is_none());
    }
}

pub fn InitProcess(backend_type: BackendType) -> PgResult<()> {
    let hdr = ProcGlobal();
    if MyProc().is_some() {
        return Err(Box::new(PgError::new(ERROR, "you already exist")));
    }
    if g::IsUnderPostmaster() {
        pmsignal_seams::register_postmaster_child_active::call();
    }

    let list_id = freelist_id_for(backend_type);

    spin_acquire(&ProcStructLock);
    s_lock_seams::set_spins_per_delay::call(hdr.spins_per_delay.get());
    let popped = plist_pop_head(hdr, freelist(hdr, list_id), links_of);
    ProcStructLock.unlock();

    let Some(procno) = popped else {
        if backend_type == BackendType::WalSender {
            let max_wal_senders = PROC_CONFIG.get().map_or(0, |cfg| cfg.max_wal_senders);
            return Err(Box::new(
                PgError::new(
                    FATAL,
                    format!(
                        "number of requested standby connections exceeds \"max_wal_senders\" (currently {max_wal_senders})"
                    ),
                )
                .with_sqlstate(ERRCODE_TOO_MANY_CONNECTIONS),
            ));
        }
        return Err(Box::new(
            PgError::new(FATAL, "sorry, too many clients already")
                .with_sqlstate(ERRCODE_TOO_MANY_CONNECTIONS),
        ));
    };

    MY_PROC.set(procno);
    g::SetMyProcNumber(procno);
    let proc = &hdr.allProcs[procno as usize];
    debug_assert_eq!(proc.procgloballist.get(), Some(list_id));

    init_my_proc_common(proc, procno);
    proc.isRegularBackend
        .store(backend_type == BackendType::Backend, Relaxed);
    // NB -- autovac launcher intentionally does not set IS_AUTOVACUUM.
    if backend_type == BackendType::AutovacWorker {
        proc.statusFlags.store(PROC_IS_AUTOVACUUM, Relaxed);
    }
    proc.recoveryConflictPending.store(false, Relaxed);
    proc.waitLSN.store(0, Relaxed);
    proc.syncRepState.set(SYNC_REP_NOT_WAITING);
    proc.syncRepLinks.set(proclist_node::detached());
    proc.procArrayGroupMember.store(false, Relaxed);
    proc.procArrayGroupMemberXid
        .store(InvalidTransactionId, Relaxed);
    debug_assert_eq!(proc.procArrayGroupNext.read(), INVALID_PROC_NUMBER as u32);
    debug_assert_eq!(proc.lockGroupLeader.load(Relaxed), INVALID_PROC_NUMBER);
    debug_assert!(plist_is_empty(&proc.lockGroupMembers));
    proc.wait_event_info.store(0, Relaxed);
    proc.clogGroupMember.store(false, Relaxed);
    proc.clogGroupMemberXid.store(InvalidTransactionId, Relaxed);
    proc.clogGroupMemberXidStatus
        .store(TRANSACTION_STATUS_IN_PROGRESS, Relaxed);
    proc.clogGroupMemberPage.store(-1, Relaxed);
    proc.clogGroupMemberLsn.store(0, Relaxed);
    debug_assert_eq!(proc.clogGroupNext.read(), INVALID_PROC_NUMBER as u32);

    latch_seams::own_latch::call(&proc.procLatch);
    miscinit_seams::switch_to_shared_latch::call();
    waitevent_seams::pgstat_set_wait_event_storage::call(&proc.wait_event_info);
    pg_sema_seams::pg_semaphore_reset::call(procno);
    ipc_seams::on_shmem_exit::call(ProcKill, 0);
    lwlock::InitLWLockAccess();
    deadlock_seams::init_dead_lock_checking::call()
}

pub fn InitProcessPhase2() -> PgResult<()> {
    let procno = my_proc_required();
    procarray_seams::proc_array_add::call(procno)?;
    ipc_seams::on_shmem_exit::call(RemoveProcFromArray, 0);
    Ok(())
}

pub fn InitAuxiliaryProcess() -> PgResult<()> {
    let hdr = ProcGlobal();
    if MyProc().is_some() {
        return Err(Box::new(PgError::new(ERROR, "you already exist")));
    }
    if g::IsUnderPostmaster() {
        pmsignal_seams::register_postmaster_child_active::call();
    }

    let aux_base = AuxiliaryProcsBase();
    spin_acquire(&ProcStructLock);
    s_lock_seams::set_spins_per_delay::call(hdr.spins_per_delay.get());
    let mut claimed = None;
    for proctype in 0..NUM_AUXILIARY_PROCS {
        let auxproc = &hdr.allProcs[(aux_base + proctype) as usize];
        if auxproc.pid.load(Relaxed) == 0 {
            auxproc.pid.store(g::MyProcPid(), Relaxed);
            claimed = Some(proctype);
            break;
        }
    }
    ProcStructLock.unlock();
    let Some(proctype) = claimed else {
        return Err(Box::new(PgError::new(
            FATAL,
            "all AuxiliaryProcs are in use",
        )));
    };

    let procno = aux_base + proctype;
    MY_PROC.set(procno);
    g::SetMyProcNumber(procno);
    let proc = &hdr.allProcs[procno as usize];

    init_my_proc_common(proc, INVALID_PROC_NUMBER);
    proc.isRegularBackend.store(false, Relaxed);

    latch_seams::own_latch::call(&proc.procLatch);
    miscinit_seams::switch_to_shared_latch::call();
    waitevent_seams::pgstat_set_wait_event_storage::call(&proc.wait_event_info);
    debug_assert_eq!(proc.lockGroupLeader.load(Relaxed), INVALID_PROC_NUMBER);
    debug_assert!(plist_is_empty(&proc.lockGroupMembers));
    pg_sema_seams::pg_semaphore_reset::call(procno);
    ipc_seams::on_shmem_exit::call(AuxiliaryProcKill, proctype as usize);
    lwlock::InitLWLockAccess();
    Ok(())
}

pub fn SetStartupBufferPinWaitBufId(bufid: i32) {
    ProcGlobal().startupBufferPinWaitBufId.store(bufid, Release);
}

pub fn GetStartupBufferPinWaitBufId() -> i32 {
    ProcGlobal().startupBufferPinWaitBufId.load(Acquire)
}

pub fn HaveNFreeProcs(n: i32) -> (bool, i32) {
    debug_assert!(n > 0);
    let hdr = ProcGlobal();
    let mut nfree = 0;
    spin_acquire(&ProcStructLock);
    let mut cur = hdr.freeProcs.get().head;
    while cur != INVALID_PROC_NUMBER {
        nfree += 1;
        if nfree == n {
            break;
        }
        cur = hdr.allProcs[cur as usize].links.get().next;
    }
    ProcStructLock.unlock();
    (nfree == n, nfree)
}

pub fn LockErrorCleanup() -> PgResult<()> {
    g::HoldInterrupts();

    lock_seams::abort_strong_lock_acquire::call();

    let Some(hashcode) = lock_seams::get_awaited_lock_hashcode::call() else {
        g::ResumeInterrupts();
        return Ok(());
    };

    // Preserve the LOCK_TIMEOUT indicator: this runs before ProcessInterrupts
    // on SIGINT and must not lose that the cancel came from a lock timeout.
    timeout_seams::disable_timeouts::call(&[
        timeout_seams::DisableTimeoutParams {
            id: timeout_seams::DEADLOCK_TIMEOUT,
            keep_indicator: false,
        },
        timeout_seams::DisableTimeoutParams {
            id: timeout_seams::LOCK_TIMEOUT,
            keep_indicator: true,
        },
    ]);

    let procno = my_proc_required();
    let proc = GetPGProcByNumber(procno);
    let partition_lock = LockHashPartitionLock(hashcode);
    lwlock::LWLockAcquire(partition_lock, lwlock::LW_EXCLUSIVE, procno)?;

    if !proc.links.get().is_detached() {
        // We could not have been granted the lock yet.
        lock_seams::remove_from_wait_queue::call(procno, hashcode);
    } else if proc.waitStatus.load(Acquire) == PROC_WAIT_STATUS_OK {
        lock_seams::grant_awaited_lock::call();
    }

    lock_seams::reset_awaited_lock::call();
    lwlock::LWLockRelease(partition_lock)?;

    g::ResumeInterrupts();
    Ok(())
}

pub fn ProcReleaseLocks(isCommit: bool) -> PgResult<()> {
    if MyProc().is_none() {
        return Ok(());
    }
    LockErrorCleanup()?;
    lock_seams::lock_release_all::call(DEFAULT_LOCKMETHOD, !isCommit)?;
    lock_seams::lock_release_all::call(USER_LOCKMETHOD, false)
}

pub fn RemoveProcFromArray(_code: i32, _arg: usize) {
    let procno = my_proc_required();
    procarray_seams::proc_array_remove::call(procno, InvalidTransactionId)
        .expect("ProcArrayRemove failed at backend exit");
}

pub fn ProcKill(_code: i32, _arg: usize) {
    let hdr = ProcGlobal();
    let procno = my_proc_required();
    let proc = GetPGProcByNumber(procno);

    // C guards against running in a fork()ed child; the threaded-model
    // invariant is that only the owning backend thread reaches this.
    if proc.pid.load(Relaxed) != g::MyProcPid() {
        panic!("ProcKill() called in child process");
    }

    syncrep_seams::sync_rep_cleanup_at_proc_exit::call();

    for i in 0..NUM_LOCK_PARTITIONS as usize {
        debug_assert!(proc.myProcLocks[i].get().head.next.is_none());
    }

    lwlock::LWLockReleaseAll().expect("LWLockReleaseAll failed in ProcKill");
    condition_variable_seams::condition_variable_cancel_sleep::call();

    let leader_no = proc.lockGroupLeader.load(Relaxed);
    if leader_no != INVALID_PROC_NUMBER {
        let leader = GetPGProcByNumber(leader_no);
        let leader_lwlock = LockHashPartitionLockByProc(leader_no);
        lwlock::LWLockAcquire(leader_lwlock, lwlock::LW_EXCLUSIVE, procno)
            .expect("partition lock in ProcKill");
        debug_assert!(!plist_is_empty(&leader.lockGroupMembers));
        plist_delete(hdr, &leader.lockGroupMembers, procno, group_link_of);
        if plist_is_empty(&leader.lockGroupMembers) {
            leader.lockGroupLeader.store(INVALID_PROC_NUMBER, Relaxed);
            if leader_no != procno {
                // Leader exited first; return its PGPROC.
                let list = leader.procgloballist.get().expect("leader freelist");
                spin_acquire(&ProcStructLock);
                plist_push_head(hdr, freelist(hdr, list), leader_no, links_of);
                ProcStructLock.unlock();
            }
        } else if leader_no != procno {
            proc.lockGroupLeader.store(INVALID_PROC_NUMBER, Relaxed);
        }
        lwlock::LWLockRelease(leader_lwlock).expect("partition unlock in ProcKill");
    }

    miscinit_seams::switch_back_to_local_latch::call();
    waitevent_seams::pgstat_reset_wait_event_storage::call();

    MY_PROC.set(INVALID_PROC_NUMBER);
    g::SetMyProcNumber(INVALID_PROC_NUMBER);
    latch_seams::disown_latch::call(&proc.procLatch);

    proc.pid.store(0, Relaxed);
    proc.vxid.procNumber.store(INVALID_PROC_NUMBER, Relaxed);
    proc.vxid.lxid.store(InvalidLocalTransactionId, Relaxed);

    let list = proc.procgloballist.get().expect("proc freelist");
    spin_acquire(&ProcStructLock);
    // Still being a group leader here means we exited before our children;
    // the last remaining child returns this PGPROC instead.
    if proc.lockGroupLeader.load(Relaxed) == INVALID_PROC_NUMBER {
        debug_assert!(plist_is_empty(&proc.lockGroupMembers));
        plist_push_tail(hdr, freelist(hdr, list), procno, links_of);
    }
    hdr.spins_per_delay
        .set(s_lock_seams::update_spins_per_delay::call(
            hdr.spins_per_delay.get(),
        ));
    ProcStructLock.unlock();

    autovacuum_seams::wake_autovacuum_launcher::call();
}

pub fn AuxiliaryProcKill(_code: i32, arg: usize) {
    let hdr = ProcGlobal();
    let proctype = arg as ProcNumber;
    debug_assert!(proctype >= 0 && proctype < NUM_AUXILIARY_PROCS);
    let procno = my_proc_required();
    let proc = GetPGProcByNumber(procno);

    if proc.pid.load(Relaxed) != g::MyProcPid() {
        panic!("AuxiliaryProcKill() called in child process");
    }
    debug_assert_eq!(procno, AuxiliaryProcsBase() + proctype);

    lwlock::LWLockReleaseAll().expect("LWLockReleaseAll failed in AuxiliaryProcKill");
    condition_variable_seams::condition_variable_cancel_sleep::call();

    miscinit_seams::switch_back_to_local_latch::call();
    waitevent_seams::pgstat_reset_wait_event_storage::call();

    MY_PROC.set(INVALID_PROC_NUMBER);
    g::SetMyProcNumber(INVALID_PROC_NUMBER);
    latch_seams::disown_latch::call(&proc.procLatch);

    spin_acquire(&ProcStructLock);
    proc.pid.store(0, Relaxed);
    proc.vxid.procNumber.store(INVALID_PROC_NUMBER, Relaxed);
    proc.vxid.lxid.store(InvalidLocalTransactionId, Relaxed);
    hdr.spins_per_delay
        .set(s_lock_seams::update_spins_per_delay::call(
            hdr.spins_per_delay.get(),
        ));
    ProcStructLock.unlock();
}

pub fn AuxiliaryPidGetProc(pid: i32) -> Option<ProcNumber> {
    if pid == 0 {
        return None;
    }
    let hdr = ProcGlobal();
    let aux_base = AuxiliaryProcsBase();
    (0..NUM_AUXILIARY_PROCS)
        .map(|i| aux_base + i)
        .find(|&procno| hdr.allProcs[procno as usize].pid.load(Relaxed) == pid)
}

pub fn CheckDeadLockAlert() {
    GOT_DEADLOCK_TIMEOUT.set(true);
    // Have to set the latch again even if handle_sig_alarm already did.
    latch_seams::set_latch_my_latch::call();
}

pub fn GotDeadlockTimeout() -> bool {
    GOT_DEADLOCK_TIMEOUT.get()
}

pub fn DeadlockState() -> DeadLockState {
    DEADLOCK_STATE.get()
}

pub fn ProcWaitForSignal(wait_event_info: u32) {
    latch_seams::wait_latch_my_latch::call(WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0, wait_event_info);
    latch_seams::reset_latch_my_latch::call();
    // C ends with CHECK_FOR_INTERRUPTS(); the interrupt machinery owns that.
}

pub fn ProcSendSignal(procNumber: ProcNumber) -> PgResult<()> {
    let hdr = ProcGlobal();
    if procNumber < 0 || procNumber >= hdr.allProcCount as ProcNumber {
        return Err(Box::new(PgError::new(ERROR, "procNumber out of range")));
    }
    latch_seams::set_latch::call(&hdr.allProcs[procNumber as usize].procLatch);
    Ok(())
}

pub fn LockHashPartitionLock(hashcode: u32) -> &'static lwlock::LWLock {
    let partition = hashcode % NUM_LOCK_PARTITIONS as u32;
    lwlock::main_lock(lwlock::LOCK_MANAGER_LWLOCK_OFFSET as usize + partition as usize)
}

fn LockHashPartitionLockByProc(procno: ProcNumber) -> &'static lwlock::LWLock {
    LockHashPartitionLock(procno as u32)
}

pub fn BecomeLockGroupLeader() -> PgResult<()> {
    let hdr = ProcGlobal();
    let procno = my_proc_required();
    let proc = GetPGProcByNumber(procno);

    if proc.lockGroupLeader.load(Relaxed) == procno {
        return Ok(());
    }
    debug_assert_eq!(proc.lockGroupLeader.load(Relaxed), INVALID_PROC_NUMBER);

    let leader_lwlock = LockHashPartitionLockByProc(procno);
    lwlock::LWLockAcquire(leader_lwlock, lwlock::LW_EXCLUSIVE, procno)?;
    proc.lockGroupLeader.store(procno, Relaxed);
    plist_push_head(hdr, &proc.lockGroupMembers, procno, group_link_of);
    lwlock::LWLockRelease(leader_lwlock)
}

pub fn BecomeLockGroupMember(leader_no: ProcNumber, pid: i32) -> PgResult<bool> {
    let hdr = ProcGlobal();
    let procno = my_proc_required();
    let proc = GetPGProcByNumber(procno);
    let leader = GetPGProcByNumber(leader_no);
    let mut ok = false;

    debug_assert_ne!(procno, leader_no);
    debug_assert_eq!(proc.lockGroupLeader.load(Relaxed), INVALID_PROC_NUMBER);
    debug_assert_ne!(pid, 0);

    // The partition lock derives from the slot number alone, so it is correct
    // even if the leader PGPROC is concurrently being recycled.
    let leader_lwlock = LockHashPartitionLockByProc(leader_no);
    lwlock::LWLockAcquire(leader_lwlock, lwlock::LW_EXCLUSIVE, procno)?;

    if leader.pid.load(Relaxed) == pid && leader.lockGroupLeader.load(Relaxed) == leader_no {
        ok = true;
        proc.lockGroupLeader.store(leader_no, Relaxed);
        plist_push_tail(hdr, &leader.lockGroupMembers, procno, group_link_of);
    }
    lwlock::LWLockRelease(leader_lwlock)?;

    Ok(ok)
}

pub fn init_seams() {
    use lmgr_proc_seams as s;

    s::proc_lw_waiting::set(|procno| GetPGProcByNumber(procno).lwWaiting.load(Relaxed));
    s::set_proc_lw_waiting::set(|procno, state| {
        GetPGProcByNumber(procno).lwWaiting.store(state, Relaxed)
    });
    s::proc_lw_wait_mode::set(|procno| GetPGProcByNumber(procno).lwWaitMode.load(Relaxed));
    s::set_proc_lw_wait_mode::set(|procno, mode| {
        GetPGProcByNumber(procno).lwWaitMode.store(mode, Relaxed)
    });
    s::proc_lw_wait_link::set(|procno| {
        let node = GetPGProcByNumber(procno).lwWaitLink.get();
        s::proclist_node {
            next: node.next,
            prev: node.prev,
        }
    });
    s::set_proc_lw_wait_link::set(|procno, node| {
        GetPGProcByNumber(procno).lwWaitLink.set(proclist_node {
            next: node.next,
            prev: node.prev,
        })
    });
    s::pg_semaphore_lock::set(|procno| pg_sema_seams::pg_semaphore_lock::call(procno));
    s::pg_semaphore_unlock::set(|procno| pg_sema_seams::pg_semaphore_unlock::call(procno));

    use guc_tables::{vars, GucVarAccessors};
    macro_rules! install_var {
        ($($slot:ident: $get:ident / $set:ident;)+) => {
            $(vars::$slot.install(GucVarAccessors {
                get: globals::$get,
                set: globals::$set,
            });)+
        };
    }
    install_var! {
        DeadlockTimeout: DeadlockTimeout / set_DeadlockTimeout;
        StatementTimeout: StatementTimeout / set_StatementTimeout;
        LockTimeout: LockTimeout / set_LockTimeout;
        IdleInTransactionSessionTimeout:
            IdleInTransactionSessionTimeout / set_IdleInTransactionSessionTimeout;
        TransactionTimeout: TransactionTimeout / set_TransactionTimeout;
        IdleSessionTimeout: IdleSessionTimeout / set_IdleSessionTimeout;
        log_lock_waits: log_lock_waits / set_log_lock_waits;
    }
}

#[cfg(test)]
mod tests;
