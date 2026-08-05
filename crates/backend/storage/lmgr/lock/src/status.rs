use std::sync::atomic::Ordering::Relaxed;

use types_core::{TimestampTz, INVALID_PROC_NUMBER};
use types_error::PgResult;
use types_hash::hsearch::HASH_SEQ_STATUS;
use types_storage::lock::{
    ExclusiveLock, LockInstanceData, LOCKBIT_ON, LOCKMASK, LOCKTAG, NoLock, PROCLOCK,
};
use types_storage::storage::{
    VirtualTransactionId, NUM_LOCK_PARTITIONS, PGPROC, PROC_ARRAY_LOCK,
    FP_LOCK_SLOTS_PER_GROUP,
};

use crate::fastpath::{fp_groups_per_backend, fp_info_lock, fp_view, FAST_PATH_LOCKNUMBER_OFFSET};
use crate::shared::{foreach_proclock_on_lock, shared, LockHashPartitionLockByIndex};

pub struct BlockedProcData {
    pub pid: i32,
    pub first_lock: usize,
    pub num_locks: usize,
    pub first_waiter: usize,
    pub num_waiters: usize,
}

pub struct BlockedProcsData {
    pub procs: Vec<BlockedProcData>,
    pub locks: Vec<LockInstanceData>,
    pub waiter_pids: Vec<i32>,
}

fn proc_vxid(proc: &PGPROC) -> VirtualTransactionId {
    VirtualTransactionId {
        procNumber: proc.vxid.procNumber.load(Relaxed),
        localTransactionId: proc.vxid.lxid.load(Relaxed),
    }
}

pub fn GetLockStatusData() -> PgResult<Vec<LockInstanceData>> {
    let procno = crate::my_procno();
    let hdr = lmgr_proc::ProcGlobal();
    let mut locks: Vec<LockInstanceData> =
        Vec::with_capacity(init_small::globals::MaxBackends() as usize);

    // Fast-path arrays first, one backend at a time (C's inconsistent-picture
    // caveat stands: none of these can be involved in conflicts).
    for i in 0..hdr.allProcCount as usize {
        let proc = &hdr.allProcs[i];
        let pid = proc.pid.load(Relaxed);
        if pid == 0 {
            continue;
        }
        lwlock::LWLockAcquire(fp_info_lock(proc), lwlock::LW_SHARED, procno)?;
        // SAFETY: fpInfoLock held shared.
        let view = unsafe { fp_view(proc) };
        for g in 0..fp_groups_per_backend() {
            if view.group_bits(g) == 0 {
                continue;
            }
            for j in 0..FP_LOCK_SLOTS_PER_GROUP as u32 {
                let f = g * FP_LOCK_SLOTS_PER_GROUP as u32 + j;
                let lockbits = view.get_bits(f);
                if lockbits == 0 {
                    continue;
                }
                locks.push(LockInstanceData {
                    locktag: LOCKTAG::relation(proc.databaseId.load(Relaxed), view.relid(f)),
                    holdMask: (lockbits as LOCKMASK) << FAST_PATH_LOCKNUMBER_OFFSET,
                    waitLockMode: NoLock,
                    vxid: proc_vxid(proc),
                    waitStart: 0,
                    pid,
                    leaderPid: pid,
                    fastpath: true,
                });
            }
        }
        if proc.fpVXIDLock.load(Relaxed) {
            locks.push(LockInstanceData {
                locktag: LOCKTAG::virtualtransaction(
                    proc.vxid.procNumber.load(Relaxed) as u32,
                    proc.fpLocalTransactionId.load(Relaxed),
                ),
                holdMask: LOCKBIT_ON(ExclusiveLock),
                waitLockMode: NoLock,
                vxid: proc_vxid(proc),
                waitStart: 0,
                pid,
                leaderPid: pid,
                fastpath: true,
            });
        }
        lwlock::LWLockRelease(fp_info_lock(proc))?;
    }

    for i in 0..NUM_LOCK_PARTITIONS as usize {
        lwlock::LWLockAcquire(LockHashPartitionLockByIndex(i), lwlock::LW_SHARED, procno)?;
    }

    let nelements = locks.len() + dynahash::hash_get_num_entries(shared().proclock_hash) as usize;
    locks.reserve(nelements.saturating_sub(locks.len()));

    let mut seqstat = HASH_SEQ_STATUS::new();
    dynahash::hash_seq_init(&mut seqstat, shared().proclock_hash)?;
    loop {
        let proclock = dynahash::hash_seq_search(&mut seqstat)? as *mut PROCLOCK;
        if proclock.is_null() {
            break;
        }
        // SAFETY: all partition locks held shared; entries, their LOCKs, and
        // waiting PGPROC fields ([PART]) are pinned/stable.
        unsafe {
            let proc = lmgr_proc::GetPGProcByNumber((*proclock).tag.myProc);
            let lock = (*proclock).tag.myLock;
            let wait_mode = if proc.waitLock.get() == lock {
                proc.waitLockMode.get()
            } else {
                NoLock
            };
            let leader = lmgr_proc::GetPGProcByNumber((*proclock).groupLeader);
            locks.push(LockInstanceData {
                locktag: (*lock).tag,
                holdMask: (*proclock).holdMask,
                waitLockMode: wait_mode,
                vxid: proc_vxid(proc),
                waitStart: proc.waitStart.read() as TimestampTz,
                pid: proc.pid.load(Relaxed),
                leaderPid: leader.pid.load(Relaxed),
                fastpath: false,
            });
        }
    }

    for i in (0..NUM_LOCK_PARTITIONS as usize).rev() {
        lwlock::LWLockRelease(LockHashPartitionLockByIndex(i))?;
    }

    debug_assert_eq!(locks.len(), nelements);
    Ok(locks)
}

pub fn GetBlockerStatusData(blocked_pid: i32) -> PgResult<BlockedProcsData> {
    let procno = crate::my_procno();
    let max_backends = init_small::globals::MaxBackends() as usize;
    let mut data = BlockedProcsData {
        procs: Vec::with_capacity(max_backends),
        locks: Vec::with_capacity(max_backends),
        waiter_pids: Vec::with_capacity(max_backends),
    };

    // ProcArrayLock pins the blocked proc's identity; the partition locks pin
    // every lock-grouping field examined below (C's consistency argument).
    let proc_array_lock = lwlock::main_lock(PROC_ARRAY_LOCK);
    lwlock::LWLockAcquire(proc_array_lock, lwlock::LW_SHARED, procno)?;

    // C scans the ProcArray (BackendPidGetProcWithLock); allProcs pid-match is
    // equivalent for live backends and aux procs never wait on heavyweight locks.
    let hdr = lmgr_proc::ProcGlobal();
    let blocked = (blocked_pid != 0)
        .then(|| {
            hdr.allProcs[..hdr.allProcCount as usize]
                .iter()
                .find(|p| p.pid.load(Relaxed) == blocked_pid)
        })
        .flatten();

    if let Some(proc) = blocked {
        for i in 0..NUM_LOCK_PARTITIONS as usize {
            lwlock::LWLockAcquire(LockHashPartitionLockByIndex(i), lwlock::LW_SHARED, procno)?;
        }

        let leader_no = proc.lockGroupLeader.load(Relaxed);
        if leader_no == INVALID_PROC_NUMBER {
            // SAFETY: all partition locks held shared.
            unsafe { single_proc_blocker_status(proc, &mut data) };
        } else {
            let leader = lmgr_proc::GetPGProcByNumber(leader_no);
            lmgr_proc::foreach_lock_group_member(leader, |member_no| {
                let member = lmgr_proc::GetPGProcByNumber(member_no);
                // SAFETY: all partition locks held shared.
                unsafe { single_proc_blocker_status(member, &mut data) };
                true
            });
        }

        for i in (0..NUM_LOCK_PARTITIONS as usize).rev() {
            lwlock::LWLockRelease(LockHashPartitionLockByIndex(i))?;
        }
    }

    lwlock::LWLockRelease(proc_array_lock)?;
    Ok(data)
}

/// SAFETY contract: all lock partition LWLocks held (shared suffices).
unsafe fn single_proc_blocker_status(blocked_proc: &PGPROC, data: &mut BlockedProcsData) {
    let the_lock = blocked_proc.waitLock.get();
    if the_lock.is_null() {
        return;
    }

    let first_lock = data.locks.len();
    let first_waiter = data.waiter_pids.len();

    foreach_proclock_on_lock(the_lock, |proclock| {
        let proc = lmgr_proc::GetPGProcByNumber((*proclock).tag.myProc);
        let lock = (*proclock).tag.myLock;
        let wait_mode = if proc.waitLock.get() == lock {
            proc.waitLockMode.get()
        } else {
            NoLock
        };
        let leader = lmgr_proc::GetPGProcByNumber((*proclock).groupLeader);
        data.locks.push(LockInstanceData {
            locktag: (*lock).tag,
            holdMask: (*proclock).holdMask,
            waitLockMode: wait_mode,
            vxid: proc_vxid(proc),
            waitStart: 0,
            pid: proc.pid.load(Relaxed),
            leaderPid: leader.pid.load(Relaxed),
            fastpath: false,
        });
        true
    });

    crate::waitqueue::wq_foreach(the_lock, |queued_no| {
        if std::ptr::eq(lmgr_proc::GetPGProcByNumber(queued_no), blocked_proc) {
            return false;
        }
        data.waiter_pids
            .push(lmgr_proc::GetPGProcByNumber(queued_no).pid.load(Relaxed));
        true
    });

    data.procs.push(BlockedProcData {
        pid: blocked_proc.pid.load(Relaxed),
        first_lock,
        num_locks: data.locks.len() - first_lock,
        first_waiter,
        num_waiters: data.waiter_pids.len() - first_waiter,
    });
}
