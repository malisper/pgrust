//! `backend-access-transam-xlogrecovery` — `access/transam/xlogrecovery.c`
//! (PostgreSQL 18.3).
//!
//! # Two state models
//!
//! This crate carries two distinct recovery-state structures, mirroring the C
//! file 1:1:
//!
//! * [`shmem::XLogRecoveryShared`] — the `XLogRecoveryCtlData` *shared-memory*
//!   region (the F0 recovery-shmem keystone): `SharedHotStandbyActive`,
//!   `recoveryWakeupLatch`, the `lastReplayed*` LSNs, `recoveryPauseState`,
//!   guarded by `info_lck`. Stood up by [`shmem::XLogRecoveryShmemInit`] and
//!   read/written under the spinlock by the shmem accessors. The single shared
//!   region every backend attaches to.
//!
//! * [`core::XLogRecoveryState`] — the *backend-local* recovery state: the C
//!   file-scope statics (`StandbyMode`, `ArchiveRecoveryRequested`,
//!   `InArchiveRecovery`, `readSource`/`currentSource`, `minRecoveryPoint`,
//!   `abortedRecPtr`, `current_record`, the recovery-target options, …) that
//!   only the startup process touches. Threaded as `&mut XLogRecoveryState`
//!   through every recovery entry point.
//!
//! # Family status
//!
//! F0 (recovery-shmem keystone, [`shmem`]) has landed: the shared region and
//! its spinlock accessors are real and installed, as have the F1/F2 shmem/GUC/
//! pause-promote accessors, the WAL-page-read driver ([`pageread`]) and the
//! reader/prefetcher holder ([`walrecovery`]). The backend-local model
//! ([`core`]) and carrier types are real.
//!
//! The **replay family is filled** ([`replay`]): `PerformWalRecovery` (the main
//! redo loop), `ApplyWalRecord` (the per-AM `GetRmgr(rmid).rm_redo` dispatch over
//! the held reader), `xlogrecovery_redo` (the RM_XLOG_ID handler),
//! `CheckRecoveryConsistency`, `checkTimeLineSwitch`, `getRecordTimestamp` (xact
//! / restore-point record decode), and `verifyBackupPageConsistency` are ported
//! 1:1 and drive the real rmgr dispatch table. Unported cross-subsystem owners
//! are reached through precise seam-and-panic boundaries (the WAL-driver legs of
//! xlog.c — `ReachedEndOfBackup`/`RemoveNonParentXlogFiles`/
//! `AllowCascadeReplication`, all `needs-decomp` #111). The redo loop calls into
//! the **stop family** ([`stop`]) helpers (`recoveryStopsBefore/After`,
//! `recoveryApplyDelay`, `recoveryPausesHere`), which remain honest panic-stubs
//! pending their own fill, so the loop is reachable only once the stop family
//! lands — exactly the intra-crate seam-and-panic boundary.
//!
//! The **stop / desc / startupxlog families are still scaffold**: honest
//! `panic!("blocked: … pending <family> fill")` stubs naming the unported
//! prerequisite (the rmgr desc dispatch that needs `Mcx`/`PgString` re-signing,
//! the recovery-pause CV sleep, and the `StartupXLOG` process integration into
//! the unported postmaster/startup owners). The crate is CATALOG `in-progress`
//! until those families land; see `audits/`.
//!
//! Ported from `src/backend/access/transam/xlogrecovery.c`.

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
// Scaffold: the backend-local recovery model + module skeletons exist so the
// readrecord / replay / promote / startupxlog family-fills can proceed; their
// `pub(crate)` panic-stubs are intentionally not wired together yet.
#![allow(dead_code)]

extern crate alloc;

pub mod core;
pub mod desc;
pub mod guc;
pub mod pageread;
pub mod promote;
pub mod readrecord;
pub mod replay;
pub mod shmem;
pub mod startupxlog;
pub mod stop;
pub mod walrecovery;

#[cfg(test)]
mod shmem_tests;

pub use shmem::{
    check_promote_signal, get_current_chunk_replay_start_time, get_current_replay_rec_ptr,
    get_latest_xtime, get_recovery_pause_state, get_xlog_receipt_time, get_xlog_replay_rec_ptr,
    hot_standby_active, promote_is_triggered, recovery_wakeup_latch_handle,
    remove_promote_signal_files, set_recovery_pause, startup_request_wal_receiver_restart,
    wakeup_recovery, xlog_request_wal_receiver_reply, XLogRecoveryShared, XLogRecoveryShmemInit,
    XLogRecoveryShmemSize,
};

pub use core::{RecordRef, XLogPageReadResult, XLogRecoveryState, XLogSource};

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
    seams::get_xlog_replay_rec_ptr_tli::set(shmem::get_xlog_replay_rec_ptr);
    seams::get_xlog_replay_recptr::set(shmem::get_xlog_replay_recptr_only);
    seams::get_current_chunk_replay_start_time::set(shmem::get_current_chunk_replay_start_time);
    seams::get_latest_x_time::set(shmem::get_latest_xtime);
    seams::get_recovery_pause_state::set(shmem::get_recovery_pause_state);
    seams::set_recovery_pause::set(shmem::set_recovery_pause);

    // F2: recovery pause / promotion controls + hot-standby + receipt-time
    // accessors. Most are callable from any backend connected to shared memory
    // (they read `XLogRecoveryCtl` under `info_lck` and the per-backend
    // `Local*` caches), so they take no `XLogRecoveryState`.
    seams::get_xlog_receipt_time::set(shmem::get_xlog_receipt_time);
    seams::wakeup_recovery::set(shmem::wakeup_recovery);
    seams::promote_is_triggered::set(shmem::promote_is_triggered);
    seams::hot_standby_active::set(shmem::hot_standby_active);
    seams::startup_request_wal_receiver_restart::set(shmem::startup_request_wal_receiver_restart);

    // `XLogRequestWalReceiverReply()` is owned by xlogrecovery.c; its seam now
    // lives in this unit's own -seams crate (moved off walreceiverfuncs-seams,
    // where it had been parked as a layering convenience + allowlist debt).
    seams::xlog_request_wal_receiver_reply::set(shmem::xlog_request_wal_receiver_reply);

    // The recovery reader/prefetcher holder (InitWalRecovery's reader leg)
    // installs the 5 record/prefetcher seams the `ReadRecord` retry loop drives.
    // Their declarations live in `xlogreader-seams` / `xlogprefetcher-seams` (the
    // C owners of the decoded record / the prefetcher), but only this holder can
    // resolve the recovery driver's `RecordRef` against the live reader, so it is
    // the installer (a sanctioned cross-crate install).
    walrecovery::init_holder_seams();
}
