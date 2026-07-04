// AutoVacuumShmemStruct, thread-native: worker slots are all-atomic so the
// AutovacuumScheduleLock fields (wi_tableoid/wi_sharedrel) stay readable under
// either lock, exactly as C's locking split allows; the list halves live under
// AV_LOCK (C AutovacuumLock), the claim protocol under AV_SCHEDULE_LOCK.
// Lock order: schedule -> av (C never takes them in the other order).

use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU32};
use std::sync::{Mutex, MutexGuard, OnceLock};

use types_core::{BlockNumber, InvalidOid, Oid, TimestampTz};

use crate::autovacuum_worker_slots;

pub const NUM_WORKITEMS: usize = 256;
pub const AVW_BRIN_SUMMARIZE_RANGE: i32 = 0;

pub const AV_FORK_FAILED: usize = 0;
pub const AV_REBALANCE: usize = 1;

pub struct WorkerInfo {
    pub wi_dboid: AtomicU32,
    pub wi_tableoid: AtomicU32,
    pub wi_sharedrel: AtomicBool,
    pub wi_proc_pid: AtomicI32,
    pub wi_launchtime: AtomicI64,
    pub wi_dobalance: AtomicBool,
}

impl WorkerInfo {
    fn empty() -> Self {
        WorkerInfo {
            wi_dboid: AtomicU32::new(InvalidOid),
            wi_tableoid: AtomicU32::new(InvalidOid),
            wi_sharedrel: AtomicBool::new(false),
            wi_proc_pid: AtomicI32::new(0),
            wi_launchtime: AtomicI64::new(0),
            wi_dobalance: AtomicBool::new(false),
        }
    }

    pub fn reset(&self) {
        self.wi_dboid.store(InvalidOid, Relaxed);
        self.wi_tableoid.store(InvalidOid, Relaxed);
        self.wi_sharedrel.store(false, Relaxed);
        self.wi_proc_pid.store(0, Relaxed);
        self.wi_launchtime.store(0, Relaxed);
        self.wi_dobalance.store(false, Relaxed);
    }
}

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

// Process-lifetime shmem image; std Vecs are the C dlists over slot indices.
pub struct AvLists {
    pub free_workers: Vec<usize>,
    pub running_workers: Vec<usize>,
    pub starting_worker: Option<usize>,
    pub work_items: [WorkItem; NUM_WORKITEMS],
}

static AV_LOCK: Mutex<AvLists> = Mutex::new(AvLists {
    free_workers: Vec::new(),
    running_workers: Vec::new(),
    starting_worker: None,
    work_items: [WorkItem::EMPTY; NUM_WORKITEMS],
});
static AV_SCHEDULE_LOCK: Mutex<()> = Mutex::new(());

static AV_LAUNCHER_PID: AtomicI32 = AtomicI32::new(0);
static AV_SIGNAL: [AtomicBool; 2] = [AtomicBool::new(false), AtomicBool::new(false)];
static AV_NWORKERS_FOR_BALANCE: AtomicU32 = AtomicU32::new(0);
static WORKER_SLOTS: OnceLock<Box<[WorkerInfo]>> = OnceLock::new();

pub fn worker_slots() -> &'static [WorkerInfo] {
    WORKER_SLOTS.get_or_init(|| {
        (0..autovacuum_worker_slots().max(1) as usize)
            .map(|_| WorkerInfo::empty())
            .collect()
    })
}

// AutoVacuumShmemInit's freelist seeding; idempotent across launcher restarts.
pub fn shmem_init_once() {
    let slots = worker_slots();
    let mut l = av_lock();
    if l.free_workers.is_empty() && l.running_workers.is_empty() && l.starting_worker.is_none() {
        l.free_workers = (0..slots.len()).collect();
    }
}

pub fn av_lock() -> MutexGuard<'static, AvLists> {
    AV_LOCK.lock().unwrap()
}

pub fn av_schedule_lock() -> MutexGuard<'static, ()> {
    AV_SCHEDULE_LOCK.lock().unwrap()
}

pub fn launcher_pid() -> i32 {
    AV_LAUNCHER_PID.load(Relaxed)
}

pub fn set_launcher_pid(pid: i32) {
    AV_LAUNCHER_PID.store(pid, Relaxed);
}

pub fn get_av_signal(which: usize) -> bool {
    AV_SIGNAL[which].swap(false, Relaxed)
}

pub fn set_av_signal(which: usize) {
    AV_SIGNAL[which].store(true, Relaxed);
}

pub fn nworkers_for_balance() -> u32 {
    AV_NWORKERS_FOR_BALANCE.load(Relaxed)
}

pub fn set_nworkers_for_balance(n: u32) {
    AV_NWORKERS_FOR_BALANCE.store(n, Relaxed);
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
