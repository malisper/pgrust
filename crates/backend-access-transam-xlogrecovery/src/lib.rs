//! `backend-access-transam-xlogrecovery` — `access/transam/xlogrecovery.c`
//! (PostgreSQL 18.3).
//!
//! This is the **F0 recovery-shmem keystone**: it stands up the
//! `XLogRecoveryCtl` shared-memory region ([`shmem::XLogRecoveryState`]) and
//! the spinlock-protected state accessors that read/write it, following the
//! proven `XLogCtl` pattern from `xlog.c` (task #111). No recovery *driver*
//! (`InitWalRecovery` / `PerformWalRecovery` / `StartupXLOG`) is ported yet;
//! later families fill the replay machinery on top of this region.
//!
//! Ported from `src/backend/access/transam/xlogrecovery.c`.

#![no_std]

extern crate alloc;

pub mod shmem;

#[cfg(test)]
mod shmem_tests;

pub use shmem::{
    get_current_chunk_replay_start_time, get_latest_xtime, get_recovery_pause_state,
    get_xlog_replay_rec_ptr, recovery_wakeup_latch_handle, set_recovery_pause, XLogRecoveryShmemInit,
    XLogRecoveryShmemSize, XLogRecoveryState,
};

use backend_access_transam_xlogrecovery_seams as seams;

/// Install this unit's inward seams.
///
/// F0 installs the recovery-shmem keystone seams (`XLogRecoveryShmemSize` /
/// `XLogRecoveryShmemInit`) and the spinlock-protected shmem-state accessors
/// that read/write the `XLogRecoveryCtl` region 1:1 with the C code. The
/// remaining declared seams (backend-local recovery flags, GUC reads, the
/// replay driver's receipt-time / consistency / promotion state) are owned by
/// xlogrecovery.c but depend on the not-yet-ported recovery driver and its
/// backend-local caches; they stay loud panics until their family lands.
pub fn init_seams() {
    seams::xlog_recovery_shmem_size::set(|| shmem::XLogRecoveryShmemSize());
    seams::xlog_recovery_shmem_init::set(|| shmem::XLogRecoveryShmemInit());

    // Spinlock-protected shmem-state accessors (faithful `info_lck` reads/
    // writes of the `XLogRecoveryCtl` shared struct).
    seams::get_xlog_replay_rec_ptr::set(shmem::get_xlog_replay_rec_ptr);
    seams::get_current_chunk_replay_start_time::set(shmem::get_current_chunk_replay_start_time);
    seams::get_latest_x_time::set(shmem::get_latest_xtime);
    seams::get_recovery_pause_state::set(shmem::get_recovery_pause_state);
    seams::set_recovery_pause::set(shmem::set_recovery_pause);
}
