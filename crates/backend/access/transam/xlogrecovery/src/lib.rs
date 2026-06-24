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
//! The **WAL-recovery orchestrator family has landed** ([`orchestrator`]):
//! `InitWalRecovery` (+ `readRecoverySignalFile` / `validateRecoveryParameters`
//! / `read_backup_label` / `read_tablespace_map`), `FinishWalRecovery`, and
//! `ShutdownWalRecovery` are ported 1:1, operating on a process-lifetime
//! backend-local recovery-state holder (C's file-static globals) and the reader
//! holder ([`walrecovery`]). Their entry seams (`init_wal_recovery` /
//! `finish_wal_recovery` / `shutdown_wal_recovery`) are declared in this unit's
//! `-seams` crate and installed by `init_seams`, so xlog.c's `StartupXLOG` can
//! seam-and-call them around the redo loop. The only seam-and-panic boundary in
//! the family is the `tablespace_map` symlink-creation leg (the unported
//! `tablespace.c` owner); the recovery-target-time conversion bottoms out on the
//! unported `timestamp.c` (`recovery_target_timestamptz_in`).
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
pub mod gucvars;
pub mod orchestrator;
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

use xlogrecovery_seams as seams;

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
    // `RemovePromoteSignalFiles()` / `CheckPromoteSignal()` (xlogrecovery.c) —
    // the postmaster's PostmasterMain + ServerLoop read/remove the promote
    // signal file. The state-free shmem entries match the C `void` signatures
    // (the file unlink/stat needs no recovery state).
    postmaster_seams::remove_promote_signal_files::set(
        shmem::remove_promote_signal_files,
    );
    postmaster_seams::check_promote_signal::set(shmem::check_promote_signal);

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
    seams::with_recovery_wakeup_latch::set(shmem::with_recovery_wakeup_latch);
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

    // The WAL-recovery orchestrator entry seams (InitWalRecovery /
    // FinishWalRecovery / ShutdownWalRecovery), driven by xlog.c's StartupXLOG
    // around the redo loop.
    seams::init_wal_recovery::set(orchestrator::init_wal_recovery);
    seams::finish_wal_recovery::set(orchestrator::finish_wal_recovery);
    seams::perform_wal_recovery::set(orchestrator::perform_wal_recovery);
    seams::shutdown_wal_recovery::set(orchestrator::shutdown_wal_recovery);

    // `InRecovery` (the startup process's backend-local replay flag), now backed
    // by the orchestrator's recovery-state tracking. (xlogrecovery.c global.)
    seams::in_recovery::set(orchestrator::in_recovery_flag);
    seams::end_recovery::set(orchestrator::end_recovery);

    // The tablespace command layer (commands/tablespace.c) reads the same
    // `InRecovery` global through its ambient-globals seam bundle.
    tablespace_globals_seams::InRecovery::set(|| {
        Ok(orchestrator::in_recovery_flag())
    });

    // `ArchiveRecoveryRequested` / `recoveryTargetTLI` (xlogrecovery.c globals),
    // read by the WAL-startup driver (xlog.c `StartupXLOG`). Backed by the
    // startup process's per-backend recovery state.
    seams::archive_recovery_requested::set(orchestrator::archive_recovery_requested);
    seams::in_archive_recovery::set(orchestrator::in_archive_recovery);
    seams::recovery_target_tli::set(orchestrator::recovery_target_tli);
    seams::reached_consistency::set(orchestrator::reached_consistency);
    seams::standby_mode::set(orchestrator::standby_mode);
    // recoveryRestoreCommand (xlogrecovery.c global) — RestoreArchivedFile reads
    // it to build the restore command. Backed by the startup process's recovery
    // state; without this the standby/archive-recovery boot panics on the
    // uninstalled seam.
    seams::recovery_restore_command::set(orchestrator::recovery_restore_command);
    // archiveCleanupCommand (xlogrecovery.c global) — CreateRestartPoint runs it
    // after a successful restartpoint to clean up no-longer-needed WAL.
    seams::archive_cleanup_command::set(orchestrator::archive_cleanup_command);

    // GUC `conf->variable` accessors (`*conf->variable`) for the recovery /
    // streaming GUC globals whose C file-static storage lives in
    // xlogrecovery.c (lines 84-100). Each is a plain GUC global read directly
    // from its slot by the GUC machinery and the recovery code (none come from
    // the ControlFile). The owner holds the backing in [`gucvars`]; here we point
    // the matching `guc_tables::vars` slot at its get/set pair so the GUC engine's
    // `.read()` / `.write()` resolve. `recovery_target_time`'s `conf->variable`
    // is `char *recovery_target_time_string` (xlogrecovery.c:91), an
    // xlogrecovery.c global, so it is installed here too.
    use ::guc_tables::{vars, GucVarAccessors};
    vars::recoveryRestoreCommand.install(GucVarAccessors {
        get: gucvars::recovery_restore_command,
        set: gucvars::set_recovery_restore_command,
    });
    vars::recoveryEndCommand.install(GucVarAccessors {
        get: gucvars::recovery_end_command,
        set: gucvars::set_recovery_end_command,
    });
    vars::archiveCleanupCommand.install(GucVarAccessors {
        get: gucvars::archive_cleanup_command,
        set: gucvars::set_archive_cleanup_command,
    });
    vars::PrimaryConnInfo.install(GucVarAccessors {
        get: gucvars::primary_conn_info,
        set: gucvars::set_primary_conn_info,
    });
    vars::PrimarySlotName.install(GucVarAccessors {
        get: gucvars::primary_slot_name,
        set: gucvars::set_primary_slot_name,
    });
    vars::recoveryTargetInclusive.install(GucVarAccessors {
        get: gucvars::recovery_target_inclusive,
        set: gucvars::set_recovery_target_inclusive,
    });
    vars::recoveryTargetAction.install(GucVarAccessors {
        get: gucvars::recovery_target_action,
        set: gucvars::set_recovery_target_action,
    });
    vars::recovery_min_apply_delay.install(GucVarAccessors {
        get: gucvars::recovery_min_apply_delay,
        set: gucvars::set_recovery_min_apply_delay,
    });
    vars::wal_receiver_create_temp_slot.install(GucVarAccessors {
        get: gucvars::wal_receiver_create_temp_slot,
        set: gucvars::set_wal_receiver_create_temp_slot,
    });
    vars::recovery_target_time_string.install(GucVarAccessors {
        get: gucvars::recovery_target_time_string,
        set: gucvars::set_recovery_target_time_string,
    });

    // GUC check/assign hooks for the recovery-target / streaming-replication
    // parameters (xlogrecovery.c:4782-5105). The `guc_tables` config entries
    // reference these by slot; the GUC engine fires them when parsing the
    // matching GUC. The check hooks parse/validate and produce the `extra`
    // payload; the assign hooks write the recovery-target globals (the
    // `gucvars` cells) that `InitWalRecovery` snapshots into the recovery state.
    use ::guc_tables::hooks;
    hooks::check_primary_slot_name.install(guc::check_primary_slot_name);
    hooks::check_recovery_target.install(guc::check_recovery_target);
    hooks::assign_recovery_target.install(guc::assign_recovery_target);
    hooks::check_recovery_target_lsn.install(guc::check_recovery_target_lsn);
    hooks::assign_recovery_target_lsn.install(guc::assign_recovery_target_lsn);
    hooks::check_recovery_target_name.install(guc::check_recovery_target_name);
    hooks::assign_recovery_target_name.install(guc::assign_recovery_target_name);
    hooks::check_recovery_target_time.install(guc::check_recovery_target_time);
    hooks::assign_recovery_target_time.install(guc::assign_recovery_target_time);
    hooks::check_recovery_target_timeline.install(guc::check_recovery_target_timeline);
    hooks::assign_recovery_target_timeline.install(guc::assign_recovery_target_timeline);
    hooks::check_recovery_target_xid.install(guc::check_recovery_target_xid);
    hooks::assign_recovery_target_xid.install(guc::assign_recovery_target_xid);
}
