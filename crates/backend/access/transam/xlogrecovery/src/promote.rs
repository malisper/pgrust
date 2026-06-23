//! Standby-promotion + recovery-wakeup helpers
//! (`StartupRequestWalReceiverRestart`, `PromoteIsTriggered`,
//! `SetPromoteIsTriggered`, `CheckForStandbyTrigger`, `RemovePromoteSignalFiles`,
//! `CheckPromoteSignal`, `WakeupRecovery`, `XLogRequestWalReceiverReply`,
//! `HotStandbyActive`, `HotStandbyActiveInReplay`).
//!
//! These functions read/write the `XLogRecoveryCtl` shared region and the
//! per-backend file-static caches (`LocalHotStandbyActive`,
//! `LocalPromoteIsTriggered`, `currentSource`, …). Both live in [`crate::shmem`],
//! the canonical home, because most of these entry points are callable from any
//! backend connected to shared memory (not just the startup process) and so
//! take no `&XLogRecoveryState`. The thin `&XLogRecoveryState` wrappers here are
//! the startup process's typed view, forwarding to the same backend-local state
//! so the replay driver can call them through the state it threads.
//!
//! Ported from `src/backend/access/transam/xlogrecovery.c`.

use crate::core::XLogRecoveryState;

/// `void StartupRequestWalReceiverRestart(void)` (xlogrecovery.c:4416)
pub fn startup_request_wal_receiver_restart(_st: &mut XLogRecoveryState) {
    crate::shmem::startup_request_wal_receiver_restart();
}

/// `bool PromoteIsTriggered(void)` (xlogrecovery.c:4435)
pub fn promote_is_triggered(_st: &mut XLogRecoveryState) -> bool {
    crate::shmem::promote_is_triggered()
}

/// `static void SetPromoteIsTriggered(void)` (xlogrecovery.c:4453)
pub(crate) fn set_promote_is_triggered(_st: &mut XLogRecoveryState) {
    crate::shmem::set_promote_is_triggered();
}

/// `static bool CheckForStandbyTrigger(void)` (xlogrecovery.c:4474)
///
/// Cross-family sibling: called directly (intra-crate) by the replay loop and
/// by `RecoveryRequiresIntParameter` while paused.
pub(crate) fn check_for_standby_trigger(_st: &mut XLogRecoveryState) -> bool {
    crate::shmem::check_for_standby_trigger()
}

/// `void RemovePromoteSignalFiles(void)` (xlogrecovery.c:4495)
pub fn remove_promote_signal_files(_st: &mut XLogRecoveryState) {
    crate::shmem::remove_promote_signal_files();
}

/// `bool CheckPromoteSignal(void)` (xlogrecovery.c:4504)
pub fn check_promote_signal(_st: &XLogRecoveryState) -> bool {
    crate::shmem::check_promote_signal()
}

/// `void WakeupRecovery(void)` (xlogrecovery.c:4519)
pub fn wakeup_recovery(_st: &XLogRecoveryState) {
    crate::shmem::wakeup_recovery();
}

/// `void XLogRequestWalReceiverReply(void)` (xlogrecovery.c:4528)
pub fn xlog_request_wal_receiver_reply(_st: &mut XLogRecoveryState) {
    crate::shmem::xlog_request_wal_receiver_reply();
}

/// `bool HotStandbyActive(void)` (xlogrecovery.c:4543)
pub fn hot_standby_active(_st: &mut XLogRecoveryState) -> bool {
    crate::shmem::hot_standby_active()
}

/// `static bool HotStandbyActiveInReplay(void)` (xlogrecovery.c:4568) — like
/// `HotStandbyActive()`, but for use only in WAL replay code where we don't need
/// to ask any other process: it returns the startup process's local cache
/// directly (`LocalHotStandbyActive`).
pub(crate) fn hot_standby_active_in_replay(st: &XLogRecoveryState) -> bool {
    // Assert(AmStartupProcess() || !IsPostmasterEnvironment) — the caller
    // invariant; in replay the startup process keeps its mirror in
    // `XLogRecoveryState::local_hot_standby_active`.
    st.local_hot_standby_active
}
