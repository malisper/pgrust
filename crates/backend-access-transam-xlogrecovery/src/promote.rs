//! Standby-promotion + recovery-wakeup helpers
//! (`StartupRequestWalReceiverRestart`, `PromoteIsTriggered`,
//! `CheckForStandbyTrigger`, `RemovePromoteSignalFiles`, `CheckPromoteSignal`,
//! `WakeupRecovery`, `XLogRequestWalReceiverReply`, `HotStandbyActive`).
//!
//! **Scaffold module.** Faithful signatures, honest `panic!` bodies the promote
//! family-fill lane replaces against [`crate::core::XLogRecoveryState`].
//!
//! Ported from `src/backend/access/transam/xlogrecovery.c`.

use crate::core::XLogRecoveryState;

/// `void StartupRequestWalReceiverRestart(void)` (xlogrecovery.c:4421)
pub fn startup_request_wal_receiver_restart(_st: &mut XLogRecoveryState) {
    panic!("decomp: xlogrecovery::promote::startup_request_wal_receiver_restart not yet filled")
}

/// `bool PromoteIsTriggered(void)` (xlogrecovery.c:4436)
pub fn promote_is_triggered(_st: &mut XLogRecoveryState) -> bool {
    panic!("decomp: xlogrecovery::promote::promote_is_triggered not yet filled")
}

/// `static void SetPromoteIsTriggered(void)` (xlogrecovery.c:4454)
pub(crate) fn set_promote_is_triggered(_st: &mut XLogRecoveryState) {
    panic!("decomp: xlogrecovery::promote::set_promote_is_triggered not yet filled")
}

/// `static bool CheckForStandbyTrigger(void)` (xlogrecovery.c:4475)
///
/// Cross-family sibling: called directly (intra-crate) by the replay loop and
/// by `recovery_requires_int_parameter` while paused.
pub(crate) fn check_for_standby_trigger(_st: &mut XLogRecoveryState) -> bool {
    panic!("decomp: xlogrecovery::promote::check_for_standby_trigger not yet filled")
}

/// `void RemovePromoteSignalFiles(void)` (xlogrecovery.c:4496)
pub fn remove_promote_signal_files(_st: &mut XLogRecoveryState) {
    panic!("decomp: xlogrecovery::promote::remove_promote_signal_files not yet filled")
}

/// `bool CheckPromoteSignal(void)` (xlogrecovery.c:4505)
pub fn check_promote_signal(_st: &XLogRecoveryState) -> bool {
    panic!("decomp: xlogrecovery::promote::check_promote_signal not yet filled")
}

/// `void WakeupRecovery(void)` (xlogrecovery.c:4520)
pub fn wakeup_recovery(_st: &XLogRecoveryState) {
    panic!("decomp: xlogrecovery::promote::wakeup_recovery not yet filled")
}

/// `void XLogRequestWalReceiverReply(void)` (xlogrecovery.c:4529)
pub fn xlog_request_wal_receiver_reply(_st: &mut XLogRecoveryState) {
    panic!("decomp: xlogrecovery::promote::xlog_request_wal_receiver_reply not yet filled")
}

/// `bool HotStandbyActive(void)` (xlogrecovery.c:4544)
pub fn hot_standby_active(_st: &mut XLogRecoveryState) -> bool {
    panic!("decomp: xlogrecovery::promote::hot_standby_active not yet filled")
}

/// `static bool HotStandbyActiveInReplay(void)` (xlogrecovery.c:4569)
pub(crate) fn hot_standby_active_in_replay(_st: &XLogRecoveryState) -> bool {
    panic!("decomp: xlogrecovery::promote::hot_standby_active_in_replay not yet filled")
}
