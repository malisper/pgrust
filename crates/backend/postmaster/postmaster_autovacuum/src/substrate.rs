//! Shared-memory substrate for `autovacuum.c`: the `AutoVacuumShmemStruct` +
//! `WorkerInfoData[]` byte layout, modelled as a real Rust struct living in a
//! process-global (the boot is single-user; in C this is one
//! `ShmemInitStruct("AutoVacuum Data", ...)` region).
//!
//! This module owns the layout the rest of `autovacuum.c` defers to via the
//! index-keyed accessor ext-seams. It mirrors `AutoVacuumShmemInit`
//! (`autovacuum.c:3377`) — first creation seeds the `WorkerInfo` free list and
//! zeroes the work-item array — and provides each accessor the launcher/worker
//! scheduling state machine drives.
//!
//! The two intrusive `dlist`/`dclist` worker lists (`av_freeWorkers`,
//! `av_runningWorkers`, threaded through each worker's `wi_links`) are modelled
//! as `VecDeque<usize>` of slot indices: a worker is in exactly one list, and
//! the only operations C performs are push-head / pop-head / delete-by-node,
//! which slot-index queues reproduce exactly.

extern crate alloc;
use alloc::collections::VecDeque;
use alloc::vec::Vec;

use std::sync::{Mutex, OnceLock};

use ::types_core::{BlockNumber, InvalidBlockNumber, InvalidOid, Oid, TimestampTz};

use crate::core::{AutoVacForkFailed, AutoVacRebalance, NUM_WORKITEMS};

/// `AutoVacNumSignals` — number of `av_signal[]` entries.
const AUTO_VAC_NUM_SIGNALS: usize = (AutoVacRebalance + 1) as usize;

/// `WorkerInfoData` (`autovacuum.c:231`). `wi_links` (the intrusive list node)
/// is represented by the worker's membership in `av_freeWorkers` /
/// `av_runningWorkers`; `wi_proc` is modelled as the boolean "this slot's
/// PGPROC is set" the scheduler tests (`worker_proc_is_set`).
#[derive(Clone)]
struct WorkerInfoData {
    wi_dboid: Oid,
    wi_tableoid: Oid,
    /// `PGPROC *wi_proc` — non-NULL ⇔ a worker process owns this slot.
    wi_proc_set: bool,
    wi_launchtime: TimestampTz,
    /// `pg_atomic_flag wi_dobalance` — initialized (clear) by ShmemInit.
    wi_dobalance: bool,
    wi_sharedrel: bool,
}

impl WorkerInfoData {
    fn zeroed() -> Self {
        WorkerInfoData {
            wi_dboid: InvalidOid,
            wi_tableoid: InvalidOid,
            wi_proc_set: false,
            wi_launchtime: 0,
            wi_dobalance: false,
            wi_sharedrel: false,
        }
    }
}

/// `AutoVacuumWorkItem` (`autovacuum.c:263`).
#[derive(Clone)]
struct AutoVacuumWorkItem {
    avw_type: i32,
    avw_used: bool,
    avw_active: bool,
    avw_database: Oid,
    avw_relation: Oid,
    avw_block_number: BlockNumber,
}

impl AutoVacuumWorkItem {
    fn zeroed() -> Self {
        AutoVacuumWorkItem {
            avw_type: 0,
            avw_used: false,
            avw_active: false,
            avw_database: InvalidOid,
            avw_relation: InvalidOid,
            avw_block_number: InvalidBlockNumber,
        }
    }
}

/// `AutoVacuumShmemStruct` (`autovacuum.c:293`).
struct AutoVacuumShmem {
    av_signal: [bool; AUTO_VAC_NUM_SIGNALS],
    av_launcherpid: i32,
    /// `dclist_head av_freeWorkers` — slot indices of free workers.
    av_free_workers: VecDeque<usize>,
    /// `dlist_head av_runningWorkers` — slot indices of running workers.
    av_running_workers: VecDeque<usize>,
    /// `WorkerInfo av_startingWorker` — slot index, or `None` (NULL).
    av_starting_worker: Option<usize>,
    av_work_items: Vec<AutoVacuumWorkItem>,
    av_nworkers_for_balance: u32,
    /// The `WorkerInfoData` array (the trailing shmem region after the struct).
    workers: Vec<WorkerInfoData>,
}

static AUTO_VACUUM_SHMEM: OnceLock<Mutex<AutoVacuumShmem>> = OnceLock::new();

fn shmem() -> std::sync::MutexGuard<'static, AutoVacuumShmem> {
    AUTO_VACUUM_SHMEM
        .get()
        .expect("AutoVacuumShmem not initialized (AutoVacuumShmemInit not called)")
        .lock()
        .expect("AutoVacuumShmem mutex poisoned")
}

/// `AutoVacuumShmemInit` (`autovacuum.c:3377`) — allocate and initialize the
/// autovacuum shmem area. `worker_slots` is `autovacuum_worker_slots`.
///
/// The `!IsUnderPostmaster` branch seeds the `WorkerInfo` free list and zeroes
/// the work-item array; the `else` branch (`IsUnderPostmaster`) just attaches
/// (asserts `found`). Here the process-global is created once either way.
pub fn auto_vacuum_shmem_init(worker_slots: i32) -> types_error::PgResult<()> {
    let under_postmaster = init_small_seams::is_under_postmaster::call();
    AUTO_VACUUM_SHMEM.get_or_init(|| {
        let nslots = worker_slots.max(0) as usize;
        let mut workers = Vec::with_capacity(nslots);
        let mut free_workers = VecDeque::with_capacity(nslots);
        if !under_postmaster {
            // Build the WorkerInfo array and seed the free list (the C
            // !IsUnderPostmaster branch; dclist_push_head walks i upward, so
            // the head ends at the last slot — push_front mirrors that order).
            for i in 0..nslots {
                workers.push(WorkerInfoData::zeroed());
                free_workers.push_front(i);
            }
        } else {
            // Attaching to an already-initialized region; in the single-process
            // model that path is unreachable, but keep the array sized.
            for _ in 0..nslots {
                workers.push(WorkerInfoData::zeroed());
            }
        }
        let work_items = (0..NUM_WORKITEMS)
            .map(|_| AutoVacuumWorkItem::zeroed())
            .collect();
        Mutex::new(AutoVacuumShmem {
            av_signal: [false; AUTO_VAC_NUM_SIGNALS],
            av_launcherpid: 0,
            av_free_workers: free_workers,
            av_running_workers: VecDeque::new(),
            av_starting_worker: None,
            av_work_items: work_items,
            av_nworkers_for_balance: 0,
            workers,
        })
    });
    Ok(())
}

/* ---- launcher pid / signals -------------------------------------------- */

pub fn get_launcher_pid() -> i32 {
    shmem().av_launcherpid
}
pub fn set_launcher_pid(pid: i32) {
    shmem().av_launcherpid = pid;
}
fn signal_index(which: i32) -> usize {
    debug_assert!(which == AutoVacForkFailed || which == AutoVacRebalance);
    which as usize
}
pub fn get_av_signal(which: i32) -> bool {
    shmem().av_signal[signal_index(which)]
}
pub fn set_av_signal(which: i32, value: bool) {
    shmem().av_signal[signal_index(which)] = value;
}

/* ---- worker free / running lists --------------------------------------- */

pub fn free_workers_count() -> u32 {
    shmem().av_free_workers.len() as u32
}
pub fn free_workers_pop_head() -> i32 {
    match shmem().av_free_workers.pop_front() {
        Some(slot) => slot as i32,
        None => -1,
    }
}
pub fn free_workers_push_head(slot: i32) {
    shmem().av_free_workers.push_front(slot as usize);
}
pub fn running_workers_push_head(slot: i32) {
    shmem().av_running_workers.push_front(slot as usize);
}
/// `dlist_delete(&worker->wi_links)` — remove the slot from whichever list it
/// currently belongs to (free or running).
pub fn worker_links_delete(slot: i32) {
    let slot = slot as usize;
    let mut s = shmem();
    if let Some(pos) = s.av_free_workers.iter().position(|&x| x == slot) {
        s.av_free_workers.remove(pos);
    }
    if let Some(pos) = s.av_running_workers.iter().position(|&x| x == slot) {
        s.av_running_workers.remove(pos);
    }
}
pub fn running_workers_slots() -> Vec<i32> {
    shmem().av_running_workers.iter().map(|&x| x as i32).collect()
}

/* ---- per-worker slot fields -------------------------------------------- */

pub fn worker_get_dboid(slot: i32) -> Oid {
    shmem().workers[slot as usize].wi_dboid
}
pub fn worker_set_dboid(slot: i32, dboid: Oid) {
    shmem().workers[slot as usize].wi_dboid = dboid;
}
pub fn worker_get_tableoid(slot: i32) -> Oid {
    shmem().workers[slot as usize].wi_tableoid
}
pub fn worker_set_tableoid(slot: i32, tableoid: Oid) {
    shmem().workers[slot as usize].wi_tableoid = tableoid;
}
pub fn worker_get_sharedrel(slot: i32) -> bool {
    shmem().workers[slot as usize].wi_sharedrel
}
pub fn worker_set_sharedrel(slot: i32, sharedrel: bool) {
    shmem().workers[slot as usize].wi_sharedrel = sharedrel;
}
pub fn worker_get_launchtime(slot: i32) -> TimestampTz {
    shmem().workers[slot as usize].wi_launchtime
}
pub fn worker_set_launchtime(slot: i32, t: TimestampTz) {
    shmem().workers[slot as usize].wi_launchtime = t;
}
pub fn worker_proc_is_set(slot: i32) -> bool {
    shmem().workers[slot as usize].wi_proc_set
}
pub fn worker_set_proc(slot: i32, set_to_myproc: bool) {
    shmem().workers[slot as usize].wi_proc_set = set_to_myproc;
}
pub fn worker_dobalance_unlocked_test(slot: i32) -> bool {
    shmem().workers[slot as usize].wi_dobalance
}
pub fn worker_dobalance_test_set(slot: i32) {
    shmem().workers[slot as usize].wi_dobalance = true;
}
pub fn worker_dobalance_clear(slot: i32) {
    shmem().workers[slot as usize].wi_dobalance = false;
}

/* ---- starting worker / balance ----------------------------------------- */

pub fn starting_worker_slot() -> i32 {
    match shmem().av_starting_worker {
        Some(slot) => slot as i32,
        None => -1,
    }
}
pub fn set_starting_worker_slot(slot: i32) {
    shmem().av_starting_worker = if slot < 0 { None } else { Some(slot as usize) };
}
pub fn nworkers_for_balance_read() -> u32 {
    shmem().av_nworkers_for_balance
}
pub fn nworkers_for_balance_write(n: u32) {
    shmem().av_nworkers_for_balance = n;
}

/* ---- work-item array --------------------------------------------------- */

pub fn workitem_get_used(i: i32) -> bool {
    shmem().av_work_items[i as usize].avw_used
}
pub fn workitem_get_active(i: i32) -> bool {
    shmem().av_work_items[i as usize].avw_active
}
pub fn workitem_set_active(i: i32, v: bool) {
    shmem().av_work_items[i as usize].avw_active = v;
}
pub fn workitem_set_used(i: i32, v: bool) {
    shmem().av_work_items[i as usize].avw_used = v;
}
pub fn workitem_get_database(i: i32) -> Oid {
    shmem().av_work_items[i as usize].avw_database
}
pub fn workitem_get_type(i: i32) -> i32 {
    shmem().av_work_items[i as usize].avw_type
}
pub fn workitem_get_relation(i: i32) -> Oid {
    shmem().av_work_items[i as usize].avw_relation
}
pub fn workitem_get_block_number(i: i32) -> BlockNumber {
    shmem().av_work_items[i as usize].avw_block_number
}
pub fn workitem_fill(i: i32, av_type: i32, database: Oid, relation: Oid, blkno: BlockNumber) {
    let mut s = shmem();
    let item = &mut s.av_work_items[i as usize];
    item.avw_type = av_type;
    item.avw_database = database;
    item.avw_relation = relation;
    item.avw_block_number = blkno;
    item.avw_active = false;
    item.avw_used = true;
}
