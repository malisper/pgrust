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
    panic!(
        "blocked: xlogrecovery::stop::recovery_stops_before — recovery-target comparison depends on \
         replay::get_record_timestamp (xact record decode); pending stop+replay family fill"
    )
}

/// `static bool recoveryStopsAfter(XLogReaderState *record)` (xlogrecovery.c)
pub(crate) fn recovery_stops_after(_st: &mut XLogRecoveryState, _record: RecordRef) -> bool {
    panic!(
        "blocked: xlogrecovery::stop::recovery_stops_after — recovery-target comparison depends on \
         replay::get_record_timestamp + xl_restore_point name decode; pending stop+replay family fill"
    )
}

/// `static const char *getRecoveryStopReason(void)` (xlogrecovery.c) — the
/// human-readable end-of-recovery reason string.
pub(crate) fn get_recovery_stop_reason(_st: &XLogRecoveryState) -> String {
    panic!(
        "blocked: xlogrecovery::stop::get_recovery_stop_reason — reason string built from the \
         recovery_stop_* fields set by recovery_stops_before/after (timestamptz_to_str seam); \
         pending stop-family fill"
    )
}

/// `static void recoveryPausesHere(bool endOfRecovery)` (xlogrecovery.c) — block
/// here while the recovery pause state is set.
pub(crate) fn recovery_pauses_here(_st: &mut XLogRecoveryState, _end_of_recovery: bool) {
    panic!(
        "blocked: xlogrecovery::stop::recovery_pauses_here — pause loop needs \
         ProcessStartupProcInterrupts + recovery-pause CV timed sleep + CheckForStandbyTrigger \
         (unported startup-proc/promote owners); pending stop-family fill"
    )
}

/// `static bool recoveryApplyDelay(XLogReaderState *record)` (xlogrecovery.c) —
/// honor `recovery_min_apply_delay` for a commit record.
pub(crate) fn recovery_apply_delay(_st: &mut XLogRecoveryState, _record: RecordRef) -> bool {
    panic!(
        "blocked: xlogrecovery::stop::recovery_apply_delay — recovery_min_apply_delay wait loop \
         needs replay::get_record_timestamp + recovery-wakeup latch wait + \
         ProcessStartupProcInterrupts; pending stop+replay family fill"
    )
}

/// `RecoveryPauseState GetRecoveryPauseState(void)` (xlogrecovery.c:3091) — the
/// startup process's view of the pause state. Reads `XLogRecoveryCtl` under
/// `info_lck` (the shared-region accessor).
pub fn get_recovery_pause_state(_st: &XLogRecoveryState) -> RecoveryPauseState {
    crate::shmem::get_recovery_pause_state()
}

/// `void SetRecoveryPause(bool recoveryPause)` (xlogrecovery.c:3111)
pub fn set_recovery_pause(_st: &mut XLogRecoveryState, recovery_pause: bool) {
    crate::shmem::set_recovery_pause(recovery_pause);
}

/// `static void ConfirmRecoveryPaused(void)` (xlogrecovery.c:3131) — transition
/// `PauseRequested` -> `Paused` once the redo loop notices the request.
pub(crate) fn confirm_recovery_paused(_st: &mut XLogRecoveryState) {
    crate::shmem::confirm_recovery_paused();
}
