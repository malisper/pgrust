#![allow(non_snake_case)]

use std::sync::Mutex;

use elog::{elog as report, ereport};
use types_core::init::{BackendType, BACKEND_NUM_TYPES};
use types_core::pid_t;
use types_error::{DEBUG2, DEBUG4};
use types_storage::storage::MAX_IO_WORKERS;

#[cfg(test)]
mod tests;

// PMChild (postmaster.h). `rw` is C's RegisteredBgWorker*; the bgworker
// registry is unported, so the field is the registration handle placeholder.
#[derive(Clone, Copy, Debug)]
pub struct PMChild {
    pub pid: pid_t,
    pub child_slot: i32,
    pub bkend_type: BackendType,
    pub rw: Option<u32>,
    pub bgworker_notify: bool,
}

struct PMChildPool {
    size: i32,
    first_slotno: i32,
    freelist: Vec<i32>, // LIFO of slot numbers (C dlist push/pop head)
}

// C's slot slab + freelists + ActiveChildList are postmaster-private statics;
// the Mutex is belt-and-suspenders for that single-thread invariant (cold,
// supervisor-only paths). Dead-end children have no pool slot: they get unique
// NEGATIVE synthetic ids so the slot-keyed seam surface can address them
// (C addresses them by struct pointer).
// `active` is the authoritative PMChild store (C's ActiveChildList); pools
// track free slot NUMBERS only — the C slot slab is reconstructible from the
// number, so it is not duplicated here.
struct Registry {
    pools: Vec<PMChildPool>,
    active: Vec<PMChild>,
    next_dead_end_id: i32,
}

static REGISTRY: Mutex<Option<Registry>> = Mutex::new(None);
static NUM_PMCHILD_SLOTS: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(0);

fn with_registry<R>(f: impl FnOnce(&mut Registry) -> R) -> R {
    let mut guard = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    let reg = guard
        .as_mut()
        .unwrap_or_else(|| panic!("PM child array not initialized yet"));
    f(reg)
}

pub fn MaxLivePostmasterChildren() -> i32 {
    let n = NUM_PMCHILD_SLOTS.load(std::sync::atomic::Ordering::Relaxed);
    if n == 0 {
        panic!("PM child array not initialized yet");
    }
    n
}

pub fn InitPostmasterChildSlots() {
    let mut pool_sizes = [0i32; BACKEND_NUM_TYPES];
    // More connections than backends: some may still be authenticating; WAL
    // senders start as regular backends and share that pool.
    pool_sizes[BackendType::Backend as usize] = 2
        * (init_small::globals::MaxConnections() + guc_tables::vars::max_wal_senders.read());
    pool_sizes[BackendType::AutovacWorker as usize] =
        guc_tables::vars::autovacuum_worker_slots.read();
    pool_sizes[BackendType::BgWorker as usize] = init_small::globals::max_worker_processes();
    pool_sizes[BackendType::IoWorker as usize] = MAX_IO_WORKERS;
    for t in [
        BackendType::AutovacLauncher,
        BackendType::SlotsyncWorker,
        BackendType::Archiver,
        BackendType::BgWriter,
        BackendType::Checkpointer,
        BackendType::Startup,
        BackendType::WalReceiver,
        BackendType::WalSummarizer,
        BackendType::WalWriter,
        BackendType::Logger,
    ] {
        pool_sizes[t as usize] = 1;
    }

    let num_slots: i32 = pool_sizes.iter().sum();

    let mut pools = Vec::with_capacity(BACKEND_NUM_TYPES);
    let mut slotno = 0;
    for btype in 0..BACKEND_NUM_TYPES {
        let size = pool_sizes[btype];
        let first_slotno = slotno + 1;
        let mut freelist = Vec::with_capacity(size as usize);
        for _ in 0..size {
            freelist.push(slotno + 1);
            slotno += 1;
        }
        // C dlist_push_tail + dlist_pop_head = FIFO; keep that grant order.
        freelist.reverse();
        pools.push(PMChildPool { size, first_slotno, freelist });
    }
    assert_eq!(slotno, num_slots);

    let mut guard = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    assert!(guard.is_none(), "InitPostmasterChildSlots called twice");
    *guard = Some(Registry { pools, active: Vec::new(), next_dead_end_id: -1 });
    NUM_PMCHILD_SLOTS.store(num_slots, std::sync::atomic::Ordering::Relaxed);
}

pub fn AssignPostmasterChildSlot(btype: BackendType) -> Option<i32> {
    let pmchild = with_registry(|reg| {
        let pool = &mut reg.pools[btype as usize];
        if pool.size == 0 {
            panic!("cannot allocate a PMChild slot for backend type {}", btype as u32);
        }
        let Some(child_slot) = pool.freelist.pop() else {
            return None;
        };
        if !(child_slot >= pool.first_slotno && child_slot < pool.first_slotno + pool.size) {
            panic!("pmchild freelist for backend type {} is corrupt", btype as u32);
        }
        let entry = PMChild {
            pid: 0,
            child_slot,
            bkend_type: btype,
            rw: None,
            bgworker_notify: true,
        };
        reg.active.insert(0, entry);
        Some(entry)
    })?;

    pmsignal::MarkPostmasterChildSlotAssigned(pmchild.child_slot)
        .unwrap_or_else(|e| panic!("MarkPostmasterChildSlotAssigned: {e:?}"));

    let _ = report(
        DEBUG2,
        format!(
            "assigned pm child slot {} for {}",
            pmchild.child_slot,
            launch_backend::postmaster_child_name(btype)
        ),
    );
    Some(pmchild.child_slot)
}

pub fn AllocDeadEndChild() -> Option<i32> {
    let _ = report(DEBUG2, "allocating dead-end child");
    Some(with_registry(|reg| {
        let id = reg.next_dead_end_id;
        reg.next_dead_end_id -= 1;
        reg.active.insert(
            0,
            PMChild {
                pid: 0,
                child_slot: id,
                bkend_type: BackendType::DeadEndBackend,
                rw: None,
                bgworker_notify: false,
            },
        );
        id
    }))
}

pub fn ReleasePostmasterChildSlot(child_slot: i32) -> bool {
    enum Released {
        DeadEnd,
        Pooled,
    }
    let released = with_registry(|reg| {
        let pos = reg
            .active
            .iter()
            .position(|c| c.child_slot == child_slot)
            .unwrap_or_else(|| panic!("releasing unknown pm child slot {child_slot}"));
        let pmchild = reg.active.remove(pos);
        if pmchild.bkend_type == BackendType::DeadEndBackend {
            return Released::DeadEnd;
        }
        // WAL senders start out as regular backends, and share the pool.
        let pool_type = if pmchild.bkend_type == BackendType::WalSender {
            BackendType::Backend
        } else {
            pmchild.bkend_type
        };
        let pool = &mut reg.pools[pool_type as usize];
        if !(pmchild.child_slot >= pool.first_slotno
            && pmchild.child_slot < pool.first_slotno + pool.size)
        {
            panic!(
                "pmchild freelist for backend type {} is corrupt",
                pmchild.bkend_type as u32
            );
        }
        pool.freelist.push(pmchild.child_slot);
        Released::Pooled
    });
    match released {
        Released::DeadEnd => {
            let _ = report(DEBUG2, "releasing dead-end backend");
            true
        }
        Released::Pooled => {
            let _ = report(DEBUG2, format!("releasing pm child slot {child_slot}"));
            pmsignal::MarkPostmasterChildSlotUnassigned(child_slot)
        }
    }
}

pub fn FindPostmasterChildByPid(pid: i32) -> Option<PMChild> {
    with_registry(|reg| reg.active.iter().find(|c| c.pid == pid).copied())
}

pub fn SetChildPid(child_slot: i32, pid: pid_t) {
    with_registry(|reg| {
        let entry = reg
            .active
            .iter_mut()
            .find(|c| c.child_slot == child_slot)
            .unwrap_or_else(|| panic!("set_child_pid: unknown pm child slot {child_slot}"));
        entry.pid = pid;
    });
}

fn btmask_contains(mask: u32, t: BackendType) -> bool {
    mask & (1 << (t as u32)) != 0
}

// The shared walk of CountChildren/SignalChildren (postmaster.c): resolve
// B_BACKENDs that announced themselves WAL senders when the mask
// distinguishes the two.
fn for_each_match(mask: u32, mut f: impl FnMut(&PMChild)) -> i32 {
    with_registry(|reg| {
        let mut matched = 0;
        for i in 0..reg.active.len() {
            let mut bp = reg.active[i];
            if btmask_contains(mask, BackendType::WalSender)
                != btmask_contains(mask, BackendType::Backend)
                && bp.bkend_type == BackendType::Backend
                && pmsignal::IsPostmasterChildWalSender(bp.child_slot)
            {
                bp.bkend_type = BackendType::WalSender;
                reg.active[i].bkend_type = BackendType::WalSender;
            }
            if !btmask_contains(mask, bp.bkend_type) {
                continue;
            }
            matched += 1;
            f(&bp);
        }
        matched
    })
}

pub fn CountChildren(target_mask: u32) -> i32 {
    for_each_match(target_mask, |bp| {
        let _ = ereport(DEBUG4)
            .errmsg_internal(format!(
                "{} process {} is still running",
                launch_backend::postmaster_child_name(bp.bkend_type),
                bp.pid
            ))
            .finish(types_error::ErrorLocation::new("postmaster.c", 0, "CountChildren"));
    })
}

pub fn SignalChildren(signal: i32, target_mask: u32) -> bool {
    let matched = for_each_match(target_mask, |bp| {
        // C signal_child = kill(pid, signal). One address space: per-thread
        // delivery needs a drain the tcop/interrupt lane does not have yet;
        // a silent drop would hang shutdown, so fail with the owner's name.
        panic!(
            "SignalChildren: delivering signal {} to {} thread pid {} — per-thread \
             signal delivery is undesigned (pmchild redesign, tcop/interrupt drain)",
            signal,
            launch_backend::postmaster_child_name(bp.bkend_type),
            bp.pid
        );
    });
    matched > 0
}

pub fn init_seams() {
    pmchild_seams::init_postmaster_child_slots::set(InitPostmasterChildSlots);
    pmchild_seams::max_live_postmaster_children::set(MaxLivePostmasterChildren);
    pmchild_seams::assign_postmaster_child_slot::set(AssignPostmasterChildSlot);
    pmchild_seams::alloc_dead_end_child::set(AllocDeadEndChild);
    pmchild_seams::release_postmaster_child_slot::set(ReleasePostmasterChildSlot);
    pmchild_seams::set_child_pid::set(SetChildPid);
    pmchild_seams::count_children::set(CountChildren);
    pmchild_seams::signal_children::set(SignalChildren);
}
