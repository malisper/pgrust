//! GUC check/assign hooks for the recovery-target / streaming parameters
//! (`check_primary_slot_name`, `check_recovery_target*` / `assign_recovery_target*`).
//!
//! **Scaffold module.** Faithful signatures, honest `panic!` bodies the
//! family-fill lanes replace against [`crate::core::XLogRecoveryState`].
//!
//! C threads the GUC "extra" value (`*extra`) by value through
//! [`RecoveryTargetExtra`], rather than through an opaque malloc'd blob.
//!
//! Ported from `src/backend/access/transam/xlogrecovery.c`.

use types_core::{TransactionId, XLogRecPtr};
use types_error::PgError;

use crate::core::{RecoveryTargetTimeLineGoal, XLogRecoveryState};

/// The GUC "extra" value (`*extra`) computed by a `check_*` hook and consumed by
/// the matching `assign_*` hook, threaded by value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RecoveryTargetExtra {
    #[default]
    None,
    Lsn(XLogRecPtr),
    Timeline(RecoveryTargetTimeLineGoal),
    Xid(TransactionId),
}

/// `bool check_primary_slot_name(char **newval, void **extra, GucSource source)`
pub fn check_primary_slot_name(_newval: &str) -> bool {
    panic!("decomp: xlogrecovery::guc::check_primary_slot_name not yet filled")
}

/// `static void error_multiple_recovery_targets(void)` (xlogrecovery.c) — build
/// the "multiple recovery targets specified" error.
pub(crate) fn error_multiple_recovery_targets() -> PgError {
    panic!("decomp: xlogrecovery::guc::error_multiple_recovery_targets not yet filled")
}

/// `bool check_recovery_target(char **newval, void **extra, GucSource source)`
pub fn check_recovery_target(_newval: &str) -> bool {
    panic!("decomp: xlogrecovery::guc::check_recovery_target not yet filled")
}

/// `void assign_recovery_target(const char *newval, void *extra)`
pub fn assign_recovery_target(_st: &mut XLogRecoveryState, _newval: &str) -> Result<(), PgError> {
    panic!("decomp: xlogrecovery::guc::assign_recovery_target not yet filled")
}

/// `bool check_recovery_target_lsn(char **newval, void **extra, GucSource source)`
pub fn check_recovery_target_lsn(_newval: &str) -> Result<RecoveryTargetExtra, ()> {
    panic!("decomp: xlogrecovery::guc::check_recovery_target_lsn not yet filled")
}

/// `void assign_recovery_target_lsn(const char *newval, void *extra)`
pub fn assign_recovery_target_lsn(
    _st: &mut XLogRecoveryState,
    _newval: &str,
    _extra: RecoveryTargetExtra,
) -> Result<(), PgError> {
    panic!("decomp: xlogrecovery::guc::assign_recovery_target_lsn not yet filled")
}

/// `bool check_recovery_target_name(char **newval, void **extra, GucSource source)`
pub fn check_recovery_target_name(_newval: &str) -> bool {
    panic!("decomp: xlogrecovery::guc::check_recovery_target_name not yet filled")
}

/// `void assign_recovery_target_name(const char *newval, void *extra)`
pub fn assign_recovery_target_name(
    _st: &mut XLogRecoveryState,
    _newval: &str,
) -> Result<(), PgError> {
    panic!("decomp: xlogrecovery::guc::assign_recovery_target_name not yet filled")
}

/// `bool check_recovery_target_time(char **newval, void **extra, GucSource source)`
pub fn check_recovery_target_time(_newval: &str) -> bool {
    panic!("decomp: xlogrecovery::guc::check_recovery_target_time not yet filled")
}

/// `void assign_recovery_target_time(const char *newval, void *extra)`
pub fn assign_recovery_target_time(
    _st: &mut XLogRecoveryState,
    _newval: &str,
) -> Result<(), PgError> {
    panic!("decomp: xlogrecovery::guc::assign_recovery_target_time not yet filled")
}

/// `bool check_recovery_target_timeline(char **newval, void **extra, GucSource source)`
pub fn check_recovery_target_timeline(_newval: &str) -> Result<RecoveryTargetExtra, ()> {
    panic!("decomp: xlogrecovery::guc::check_recovery_target_timeline not yet filled")
}

/// `void assign_recovery_target_timeline(const char *newval, void *extra)`
pub fn assign_recovery_target_timeline(
    _st: &mut XLogRecoveryState,
    _newval: &str,
    _extra: RecoveryTargetExtra,
) {
    panic!("decomp: xlogrecovery::guc::assign_recovery_target_timeline not yet filled")
}

/// `bool check_recovery_target_xid(char **newval, void **extra, GucSource source)`
pub fn check_recovery_target_xid(_newval: &str) -> Result<RecoveryTargetExtra, ()> {
    panic!("decomp: xlogrecovery::guc::check_recovery_target_xid not yet filled")
}

/// `void assign_recovery_target_xid(const char *newval, void *extra)`
pub fn assign_recovery_target_xid(
    _st: &mut XLogRecoveryState,
    _newval: &str,
    _extra: RecoveryTargetExtra,
) -> Result<(), PgError> {
    panic!("decomp: xlogrecovery::guc::assign_recovery_target_xid not yet filled")
}
