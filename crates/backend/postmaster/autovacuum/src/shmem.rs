// AutoVacuumShmemStruct (autovacuum.c:3383 AutoVacuumShmemInit): the
// ShmemInitStruct("AutoVacuum Data", AutoVacuumShmemSize()) block, C layout —
// the fixed struct followed by autovacuum_worker_slots WorkerInfoData — so
// pg_shmem_allocations lists it with C's size. Worker slots are all-atomic so
// the AutovacuumScheduleLock fields (wi_tableoid/wi_sharedrel) stay readable
// under either lock, exactly as C's locking split allows; the list halves live
// under AV_LOCK (C AutovacuumLock), the claim protocol under AV_SCHEDULE_LOCK.
// Lock order: schedule -> av (C never takes them in the other order).

use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU32};
use std::sync::{Mutex, MutexGuard, OnceLock};

use types_core::{BlockNumber, InvalidOid, Oid, TimestampTz};
use types_error::PgResult;
use types_storage::SyncCell;

use crate::autovacuum_worker_slots;

pub const NUM_WORKITEMS: usize = 256;
pub use autovacuum_seams::AVW_BRIN_SUMMARIZE_RANGE;

pub const AV_FORK_FAILED: usize = 0;
pub const AV_REBALANCE: usize = 1;
const AUTOVAC_NUM_SIGNALS: usize = 2;

// C MAXALIGN (MAXIMUM_ALIGNOF = 8 on every LP64 target pgrust builds for).
const fn MAXALIGN(len: usize) -> usize {
    (len + 7) & !7
}

// WorkerInfoData (autovacuum.c:225), C layout: dlist_node wi_links; Oid
// wi_dboid; Oid wi_tableoid; PGPROC *wi_proc; TimestampTz wi_launchtime;
// pg_atomic_flag wi_dobalance; bool wi_sharedrel — 48 bytes. The thread model
// keeps the list links in the slot-index Vecs under AV_LOCK and the worker's
// pid where C keeps its PGPROC pointer; the padding fields hold C's offsets.
#[repr(C)]
pub struct WorkerInfo {
    _wi_links: [u64; 2],
    pub wi_dboid: AtomicU32,
    pub wi_tableoid: AtomicU32,
    pub wi_proc_pid: AtomicI32,
    _wi_proc_pad: u32,
    pub wi_launchtime: AtomicI64,
    pub wi_dobalance: AtomicBool,
    pub wi_sharedrel: AtomicBool,
}

impl WorkerInfo {
    pub fn reset(&self) {
        self.wi_dboid.store(InvalidOid, Relaxed);
        self.wi_tableoid.store(InvalidOid, Relaxed);
        self.wi_sharedrel.store(false, Relaxed);
        self.wi_proc_pid.store(0, Relaxed);
        self.wi_launchtime.store(0, Relaxed);
        self.wi_dobalance.store(false, Relaxed);
    }
}

// AutoVacuumWorkItem (autovacuum.c:258), C layout: 20 bytes.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct WorkItem {
    pub avw_type: i32,
    pub avw_used: bool,
    pub avw_active: bool,
    pub avw_database: Oid,
    pub avw_relation: Oid,
    pub avw_block_number: BlockNumber,
}

impl WorkItem {
    const EMPTY: WorkItem = WorkItem {
        avw_type: 0,
        avw_used: false,
        avw_active: false,
        avw_database: InvalidOid,
        avw_relation: InvalidOid,
        avw_block_number: 0,
    };
}

// AutoVacuumShmemStruct (autovacuum.c:277), C layout. av_freeWorkers
// (dclist_head), av_runningWorkers (dlist_head) and av_startingWorker are
// pointers into this block in C; the thread model carries those lists as the
// slot-index Vecs of AvLists (under AV_LOCK), so `_av_lists` is only their
// footprint. The work items are AutovacuumLock-guarded (SyncCell: read and
// written through the AvLists guard).
#[repr(C)]
pub struct AutoVacuumShmemStruct {
    av_signal: [AtomicI32; AUTOVAC_NUM_SIGNALS],
    av_launcherpid: AtomicI32,
    _av_lists: [u64; 6],
    av_work_items: [SyncCell<WorkItem>; NUM_WORKITEMS],
    av_nworkers_for_balance: AtomicU32,
}

// C: `static AutoVacuumShmemStruct *AutoVacuumShmem`.
struct ShmemPtr(*const AutoVacuumShmemStruct, &'static [WorkerInfo]);
// SAFETY: every field is an atomic or an AV_LOCK-guarded SyncCell, and the
// slot slice points into the same cluster-lifetime block (shmem is never
// freed).
unsafe impl Sync for ShmemPtr {}
unsafe impl Send for ShmemPtr {}

static AV_SHMEM: OnceLock<ShmemPtr> = OnceLock::new();

fn shmem() -> &'static AutoVacuumShmemStruct {
    let p = AV_SHMEM
        .get()
        .expect("AutoVacuumShmem accessed before AutoVacuumShmemInit");
    // SAFETY: a cluster-lifetime ShmemIndex allocation initialized by
    // AutoVacuumShmemInit before the pointer was published.
    unsafe { &*p.0 }
}

// AutoVacuumShmemSize (autovacuum.c:3365): the fixed struct, MAXALIGNed, plus
// autovacuum_worker_slots WorkerInfoData.
pub fn AutoVacuumShmemSize() -> PgResult<usize> {
    let size = MAXALIGN(core::mem::size_of::<AutoVacuumShmemStruct>());
    shmem::add_size(
        size,
        shmem::mul_size(
            autovacuum_worker_slots().max(0) as usize,
            core::mem::size_of::<WorkerInfo>(),
        )?,
    )
}

/// AutoVacuumShmemInit (autovacuum.c:3383): ShmemInitStruct("AutoVacuum
/// Data", AutoVacuumShmemSize()) registers the block in the ShmemIndex, so
/// pg_shmem_allocations lists it; the WorkerInfo array follows the fixed
/// struct in the same block. First time through (found = false) the launcher
/// pid, signals, work items and balance count are zero (the fresh allocation
/// is zeroed, as C's) and every slot goes on the free list; a re-entry finds
/// the block and leaves the live data alone.
pub fn AutoVacuumShmemInit() -> PgResult<()> {
    const {
        assert!(!core::mem::needs_drop::<AutoVacuumShmemStruct>());
        assert!(!core::mem::needs_drop::<WorkerInfo>());
        // C's sizeof(AutoVacuumShmemStruct) / sizeof(WorkerInfoData) /
        // sizeof(AutoVacuumWorkItem) on LP64.
        assert!(core::mem::size_of::<AutoVacuumShmemStruct>() == 5192);
        assert!(core::mem::size_of::<WorkerInfo>() == 48);
        assert!(core::mem::size_of::<WorkItem>() == 20);
        assert!(core::mem::align_of::<WorkerInfo>() <= 8);
    }
    let nslots = autovacuum_worker_slots().max(0) as usize;
    let size = AutoVacuumShmemSize()?;
    let (raw, found) = shmem::ShmemInitStruct("AutoVacuum Data", size)?;
    let p = raw.cast::<AutoVacuumShmemStruct>();
    // SAFETY: a cache-line-aligned ShmemIndex allocation of
    // MAXALIGN(size_of::<AutoVacuumShmemStruct>()) + nslots *
    // size_of::<WorkerInfo>() bytes; the slot slice covers exactly the tail
    // of that block, at C's MAXALIGN(sizeof(AutoVacuumShmemStruct)) offset.
    // A fresh allocation is zeroed, which is every field's boot value
    // (InvalidOid = 0, pid 0, flags clear, work items unused).
    let slots: &'static [WorkerInfo] = unsafe {
        let base = raw.add(MAXALIGN(core::mem::size_of::<AutoVacuumShmemStruct>()));
        core::slice::from_raw_parts(base.cast::<WorkerInfo>(), nslots)
    };
    let _ = AV_SHMEM.set(ShmemPtr(p, slots));
    if !found {
        // C: dclist_init(&av_freeWorkers) + dclist_push_head of every slot
        // (the last pushed sits at the head); pg_atomic_init_flag per slot.
        let mut l = av_lock();
        l.free_workers = (0..nslots).rev().collect();
        l.running_workers.clear();
        l.starting_worker = None;
    }
    Ok(())
}

/// Crash-cycle reset to the post-AutoVacuumShmemInit boot image (C rebuilds
/// the whole segment after a crash): every child is dead, the postmaster
/// thread is exclusive.
pub fn AutoVacuumShmemResetAfterCrash() {
    let s = shmem();
    for sig in &s.av_signal {
        sig.store(0, Relaxed);
    }
    s.av_launcherpid.store(0, Relaxed);
    s.av_nworkers_for_balance.store(0, Relaxed);
    let mut l = AV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    for wi in &s.av_work_items {
        // SAFETY: AV_LOCK held (the AvLists guard above).
        unsafe { wi.set(WorkItem::EMPTY) };
    }
    let slots = worker_slots();
    for slot in slots {
        slot.reset();
    }
    l.free_workers = (0..slots.len()).rev().collect();
    l.running_workers.clear();
    l.starting_worker = None;
}

// Process-lifetime list image; std Vecs are the C dlists over slot indices.
pub struct AvLists {
    pub free_workers: Vec<usize>,
    pub running_workers: Vec<usize>,
    pub starting_worker: Option<usize>,
}

static AV_LOCK: Mutex<AvLists> = Mutex::new(AvLists {
    free_workers: Vec::new(),
    running_workers: Vec::new(),
    starting_worker: None,
});
static AV_SCHEDULE_LOCK: Mutex<()> = Mutex::new(());

pub fn worker_slots() -> &'static [WorkerInfo] {
    AV_SHMEM
        .get()
        .expect("AutoVacuumShmem accessed before AutoVacuumShmemInit")
        .1
}

// The av_workItems array is AutovacuumLock-guarded: the AvLists guard is the
// proof of holding it.
pub fn work_item(_l: &AvLists, i: usize) -> WorkItem {
    // SAFETY: AV_LOCK held (the caller's AvLists guard).
    unsafe { shmem().av_work_items[i].get() }
}

pub fn set_work_item(_l: &mut AvLists, i: usize, wi: WorkItem) {
    // SAFETY: AV_LOCK held (the caller's AvLists guard, exclusively).
    unsafe { shmem().av_work_items[i].set(wi) }
}

pub fn av_lock() -> MutexGuard<'static, AvLists> {
    AV_LOCK.lock().unwrap()
}

pub fn av_schedule_lock() -> MutexGuard<'static, ()> {
    AV_SCHEDULE_LOCK.lock().unwrap()
}

pub fn launcher_pid() -> i32 {
    shmem().av_launcherpid.load(Relaxed)
}

pub fn set_launcher_pid(pid: i32) {
    shmem().av_launcherpid.store(pid, Relaxed);
}

pub fn get_av_signal(which: usize) -> bool {
    shmem().av_signal[which].swap(0, Relaxed) != 0
}

pub fn set_av_signal(which: usize) {
    shmem().av_signal[which].store(1, Relaxed);
}

pub fn nworkers_for_balance() -> u32 {
    shmem().av_nworkers_for_balance.load(Relaxed)
}

pub fn set_nworkers_for_balance(n: u32) {
    shmem().av_nworkers_for_balance.store(n, Relaxed);
}

pub fn av_worker_available_locked(l: &AvLists) -> bool {
    let free_slots = l.free_workers.len() as i32;
    let reserved_slots =
        (autovacuum_worker_slots() - crate::autovacuum_max_workers()).max(0);
    free_slots > reserved_slots
}

pub fn av_worker_available() -> bool {
    av_worker_available_locked(&av_lock())
}

thread_local! {
    pub static MY_WORKER_INFO: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
    pub static AUTOVACUUM_LAUNCHER_PID: std::cell::Cell<i32> = const { std::cell::Cell::new(0) };
    pub static AV_STORAGE_PARAM_COST_DELAY: std::cell::Cell<f64> =
        const { std::cell::Cell::new(-1.0) };
    pub static AV_STORAGE_PARAM_COST_LIMIT: std::cell::Cell<i32> =
        const { std::cell::Cell::new(-1) };
}

pub fn my_worker_slot() -> Option<&'static WorkerInfo> {
    MY_WORKER_INFO.get().map(|i| &worker_slots()[i])
}

pub fn worker_launchtime(idx: usize) -> TimestampTz {
    worker_slots()[idx].wi_launchtime.load(Relaxed)
}

#[cfg(test)]
pub(crate) fn shmem_for_tests() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| AutoVacuumShmemInit().unwrap());
}

#[cfg(test)]
mod tests {
    use super::*;

    // audit-18.6 b149: AutoVacuumShmemInit is a ShmemInitStruct("AutoVacuum
    // Data", AutoVacuumShmemSize()) allocation (autovacuum.c:3383), so the
    // block is registered in the ShmemIndex (pg_shmem_allocations lists it,
    // size 5960 at the 16-slot default: MAXALIGN(5192) + 16 * 48) and a
    // re-entry finds it (found = true) instead of allocating a second one.
    #[test]
    fn shmem_init_registers_autovacuum_data() {
        shmem_for_tests();
        let size = AutoVacuumShmemSize().unwrap();
        assert_eq!(size, 5192 + autovacuum_worker_slots() as usize * 48);
        let (raw, found) = shmem::ShmemInitStruct("AutoVacuum Data", size).unwrap();
        assert!(found, "AutoVacuum Data is not registered in the ShmemIndex");
        assert!(
            std::ptr::eq(raw.cast::<AutoVacuumShmemStruct>(), shmem()),
            "AutoVacuumShmem does not live in the ShmemIndex block"
        );
        let slots = worker_slots();
        assert_eq!(slots.len(), autovacuum_worker_slots() as usize);
        // The slot array is the block's tail, right after the fixed struct.
        assert_eq!(
            slots.as_ptr() as usize - raw as usize,
            MAXALIGN(core::mem::size_of::<AutoVacuumShmemStruct>())
        );
        let l = av_lock();
        assert_eq!(l.free_workers.len() + l.running_workers.len(), slots.len());
    }
}
