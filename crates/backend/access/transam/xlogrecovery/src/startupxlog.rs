//! The recovery driver entry points (`StartupXLOG`'s recovery-side body, split
//! across the init / post-init boundary as `startup_xlog` /
//! `startup_xlog_after_init`).
//!
//! **Scaffold module.** Faithful signatures, honest `panic!` bodies the
//! startupxlog family-fill lane replaces against
//! [`crate::core::XLogRecoveryState`].
//!
//! Ported from `src/backend/access/transam/xlogrecovery.c`.

use ::types_error::PgResult;

use crate::core::XLogRecoveryState;

/// `void StartupXLOG(void)` (xlogrecovery.c) — the recovery-side body of the
/// startup process: read the control file, set up the reader, and (if in
/// recovery) run the redo loop. Split at the `InitWalRecovery` boundary; this is
/// the part up to the redo loop.
pub fn startup_xlog(_st: &mut XLogRecoveryState) -> PgResult<()> {
    panic!(
        "blocked: xlogrecovery::startupxlog::startup_xlog — recovery entry (control-file read, \
         InitWalRecovery, redo loop via replay::perform_wal_recovery) + process integration \
         (StartupProcShutdownHandler, checkpointer signaling) into unported postmaster/startup \
         owners; pending startupxlog+replay family fill"
    )
}

/// The post-init continuation of `StartupXLOG` (the end-of-recovery WAL action
/// and shared-state finalization), reached after the redo loop / init work.
pub fn startup_xlog_after_init(_st: &mut XLogRecoveryState) -> PgResult<()> {
    panic!(
        "blocked: xlogrecovery::startupxlog::startup_xlog_after_init — end-of-recovery WAL action \
         + shared-state finalization, reached after the redo loop; pending startupxlog+replay family fill"
    )
}
