//! The shared-memory `WalSnd` array operations.
//!
//! These are the per-slot reads/writes that the C code performs directly on
//! `WalSndCtl->walsnds[i]` / `MyWalSnd` under the slot spinlock, ported here
//! over the owned [`crate::core`] shmem struct.  They replace what the
//! src-idiomatic base routed through `walsnd_*` / `my_walsnd_*` seams: the
//! array is owned by this crate, so these are real functions, not seams.

#![allow(non_snake_case)]

use crate::core::{
    proc_get, slot_spin_acquire, slot_spin_release, walsnds_slot, walsnds_slot_mut, with_proc,
    ReplicationKind, WalSnd, WalSndState, XLogRecPtr,
};

/// `&WalSndCtl->walsnds[MyWalSnd]` — the current backend's reserved slot index.
#[inline]
pub fn my_walsnd_index() -> i32 {
    let idx = proc_get(|p| p.my_walsnd);
    assert!(idx >= 0, "MyWalSnd accessed before InitWalSenderSlot");
    idx
}

#[inline]
fn my_slot_index() -> i32 {
    my_walsnd_index()
}

/// Whether this backend has reserved a `WalSnd` slot (`MyWalSnd != NULL`).
pub fn my_walsnd_is_set() -> bool {
    proc_get(|p| p.my_walsnd) >= 0
}

/// Reserve a free `WalSnd` slot for this walsender and initialize its fields
/// under the slot mutex (the body of `InitWalSenderSlot`'s loop).  Returns the
/// reserved index.
pub fn reserve_slot(my_proc_pid: i32, my_database_is_invalid: bool) -> i32 {
    let max = proc_get(|p| p.max_wal_senders);
    let kind = if my_database_is_invalid {
        ReplicationKind::REPLICATION_KIND_PHYSICAL
    } else {
        ReplicationKind::REPLICATION_KIND_LOGICAL
    };

    let mut reserved = -1;
    let mut i: i32 = 0;
    while i < max {
        let slot = walsnds_slot_mut(i);
        slot_spin_acquire(slot);
        if slot.pid != 0 {
            slot_spin_release(slot);
            i += 1;
            continue;
        }
        // Found a free slot: reserve it.
        slot.pid = my_proc_pid;
        slot.state = WalSndState::WALSNDSTATE_STARTUP;
        slot.sentPtr = crate::core::InvalidXLogRecPtr;
        slot.needreload = false;
        slot.write = crate::core::InvalidXLogRecPtr;
        slot.flush = crate::core::InvalidXLogRecPtr;
        slot.apply = crate::core::InvalidXLogRecPtr;
        slot.writeLag = -1;
        slot.flushLag = -1;
        slot.applyLag = -1;
        slot.sync_standby_priority = 0;
        slot.replyTime = 0;
        slot.kind = kind;
        slot_spin_release(slot);
        reserved = i;
        break;
    }

    assert!(reserved >= 0, "no free WalSnd slot");
    with_proc(|p| p.my_walsnd = reserved);
    reserved
}

/// `WalSndKill` body: clear `MyWalSnd` and mark the slot no longer in use.
pub fn release_my_slot() {
    let idx = my_slot_index();
    with_proc(|p| p.my_walsnd = -1);
    let slot = walsnds_slot_mut(idx);
    slot_spin_acquire(slot);
    slot.pid = 0;
    slot_spin_release(slot);
}

/// `WalSnd->pid` for slot `i` under its mutex (used by WalSndInitStopping).
pub fn slot_pid(i: i32) -> i32 {
    let slot = walsnds_slot(i);
    slot_spin_acquire(slot);
    let pid = slot.pid;
    slot_spin_release(slot);
    pid
}

/// A snapshot of slot `i` taken under its mutex (used by WalSndWaitStopping and
/// the stats SRF).
pub fn slot_snapshot(i: i32) -> WalSnd {
    let slot = walsnds_slot(i);
    slot_spin_acquire(slot);
    let snap = WalSnd {
        pid: slot.pid,
        state: slot.state,
        sentPtr: slot.sentPtr,
        write: slot.write,
        flush: slot.flush,
        apply: slot.apply,
        writeLag: slot.writeLag,
        flushLag: slot.flushLag,
        applyLag: slot.applyLag,
        sync_standby_priority: slot.sync_standby_priority,
        replyTime: slot.replyTime,
    };
    slot_spin_release(slot);
    snap
}

// ---------------------------------------------------------------------------
// MyWalSnd field reads/writes (under the slot mutex where C takes it).
// ---------------------------------------------------------------------------

/// `MyWalSnd->state`.
pub fn my_state() -> WalSndState {
    walsnds_slot(my_slot_index()).state
}

/// `MyWalSnd->kind`.
pub fn my_kind() -> ReplicationKind {
    walsnds_slot(my_slot_index()).kind
}

/// `MyWalSnd->write`.
pub fn my_write() -> XLogRecPtr {
    walsnds_slot(my_slot_index()).write
}

/// `MyWalSnd->flush`.
pub fn my_flush() -> XLogRecPtr {
    walsnds_slot(my_slot_index()).flush
}

/// `WalSndSetState`'s shmem write: `SpinLockAcquire; MyWalSnd->state = state;
/// SpinLockRelease;`.
pub fn my_set_state(state: WalSndState) {
    let idx = my_slot_index();
    let slot = walsnds_slot_mut(idx);
    slot_spin_acquire(slot);
    slot.state = state;
    slot_spin_release(slot);
}

/// `SpinLockAcquire(&MyWalSnd->mutex); MyWalSnd->sentPtr = sentPtr;
/// SpinLockRelease;` (the shared-memory status update in XLogSend*).
pub fn my_set_sentptr(sentptr: XLogRecPtr) {
    let idx = my_slot_index();
    let slot = walsnds_slot_mut(idx);
    slot_spin_acquire(slot);
    slot.sentPtr = sentptr;
    slot_spin_release(slot);
}

/// `WalSndRqstFileReload`: set `needreload` on every active walsender slot.
pub fn set_all_needreload() {
    let max = proc_get(|p| p.max_wal_senders);
    let mut i: i32 = 0;
    while i < max {
        let slot = walsnds_slot_mut(i);
        slot_spin_acquire(slot);
        if slot.pid == 0 {
            slot_spin_release(slot);
            i += 1;
            continue;
        }
        slot.needreload = true;
        slot_spin_release(slot);
        i += 1;
    }
}

/// `WalSndCtl->sync_standbys_status` read (no lock; worst case is a stale read,
/// exactly as the C `WalSndUpdateProgress` comment notes).
pub fn ctl_sync_standbys_status() -> u8 {
    crate::core::wal_snd_ctl().sync_standbys_status
}
