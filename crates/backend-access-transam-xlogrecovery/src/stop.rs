//! Recovery-target stop-point logic + the pause-state accessors
//! (`recoveryStopsBefore` / `recoveryStopsAfter`, `getRecoveryStopReason`,
//! `recoveryPausesHere`, `recoveryApplyDelay`, `GetRecoveryPauseState` /
//! `SetRecoveryPause` / `ConfirmRecoveryPaused`).
//!
//! **Scaffold module.** Faithful signatures, honest `panic!` bodies the
//! family-fill lanes replace against [`crate::core::XLogRecoveryState`].
//!
//! Note: the *shared* `GetRecoveryPauseState` / `SetRecoveryPause` that read the
//! `XLogRecoveryCtl` shmem region under `info_lck` are the already-landed
//! [`crate::shmem`] accessors; the `&mut XLogRecoveryState` forms here are the
//! startup process's view that the C file threads through replay.
//!
//! Ported from `src/backend/access/transam/xlogrecovery.c`.

use alloc::string::String;

use crate::core::{RecordRef, RecoveryPauseState, XLogRecoveryState};

/// `static bool recoveryStopsBefore(XLogReaderState *record)` (xlogrecovery.c)
pub(crate) fn recovery_stops_before(_st: &mut XLogRecoveryState, _record: RecordRef) -> bool {
    panic!("decomp: xlogrecovery::stop::recovery_stops_before not yet filled")
}

/// `static bool recoveryStopsAfter(XLogReaderState *record)` (xlogrecovery.c)
pub(crate) fn recovery_stops_after(_st: &mut XLogRecoveryState, _record: RecordRef) -> bool {
    panic!("decomp: xlogrecovery::stop::recovery_stops_after not yet filled")
}

/// `static const char *getRecoveryStopReason(void)` (xlogrecovery.c) — the
/// human-readable end-of-recovery reason string.
pub(crate) fn get_recovery_stop_reason(_st: &XLogRecoveryState) -> String {
    panic!("decomp: xlogrecovery::stop::get_recovery_stop_reason not yet filled")
}

/// `static void recoveryPausesHere(bool endOfRecovery)` (xlogrecovery.c) — block
/// here while the recovery pause state is set.
pub(crate) fn recovery_pauses_here(_st: &mut XLogRecoveryState, _end_of_recovery: bool) {
    panic!("decomp: xlogrecovery::stop::recovery_pauses_here not yet filled")
}

/// `static bool recoveryApplyDelay(XLogReaderState *record)` (xlogrecovery.c) —
/// honor `recovery_min_apply_delay` for a commit record.
pub(crate) fn recovery_apply_delay(_st: &mut XLogRecoveryState, _record: RecordRef) -> bool {
    panic!("decomp: xlogrecovery::stop::recovery_apply_delay not yet filled")
}

/// `RecoveryPauseState GetRecoveryPauseState(void)` (xlogrecovery.c:3127) — the
/// startup process's view of the pause state.
pub fn get_recovery_pause_state(_st: &XLogRecoveryState) -> RecoveryPauseState {
    panic!("decomp: xlogrecovery::stop::get_recovery_pause_state not yet filled")
}

/// `void SetRecoveryPause(bool recoveryPause)` (xlogrecovery.c:3094)
pub fn set_recovery_pause(_st: &mut XLogRecoveryState, _recovery_pause: bool) {
    panic!("decomp: xlogrecovery::stop::set_recovery_pause not yet filled")
}

/// `static void ConfirmRecoveryPaused(void)` (xlogrecovery.c) — transition
/// `PauseRequested` -> `Paused` once the redo loop notices the request.
pub(crate) fn confirm_recovery_paused(_st: &mut XLogRecoveryState) {
    panic!("decomp: xlogrecovery::stop::confirm_recovery_paused not yet filled")
}
