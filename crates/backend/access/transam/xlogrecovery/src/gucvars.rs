//! GUC `conf->variable` backing storage for the recovery / streaming GUCs
//! whose C file-static globals live in `xlogrecovery.c` (lines 84-100).
//!
//! In C the GUC machinery reads and writes these globals directly through the
//! variable pointer stored in each `config_generic` (`conf->variable`); the
//! recovery code then reads the same globals. Here the GUC engine reaches the
//! storage through the `GucVarAccessors { get, set }` pair installed on the
//! matching `guc_tables::vars` slot. Each global keeps a `thread_local`
//! backing cell mirroring the C file-static 1:1 (the startup/postmaster process
//! is the single owner), and the accessors read/write it. The boot defaults
//! match `guc_tables.c` (`""` for the string globals, `true`/`false`/`0`/the
//! pause action for the scalars), so a read before the GUC machinery has
//! assigned still yields the C boot value rather than panicking.
//!
//! Ported from `src/backend/access/transam/xlogrecovery.c` (the
//! `recoveryRestoreCommand` / `recoveryEndCommand` / `archiveCleanupCommand` /
//! `recoveryTargetInclusive` / `recoveryTargetAction` / `recovery_min_apply_delay`
//! / `PrimaryConnInfo` / `PrimarySlotName` / `wal_receiver_create_temp_slot`
//! globals) and their `guc_tables.c` bindings.

extern crate std;

use alloc::string::String;
use core::cell::{Cell, RefCell};

// `RECOVERY_TARGET_ACTION_PAUSE` — the boot value of `recoveryTargetAction`
// (an int GUC over the `recovery_target_action` enum; guc_tables.c).
use crate::core::{RecoveryTargetAction, RecoveryTargetTimeLineGoal, RecoveryTargetType};
use ::types_core::{InvalidXLogRecPtr, TimeLineID, TransactionId, XLogRecPtr};

// ---------------------------------------------------------------------------
// `char *` string GUC globals. The C `conf->variable` is a `char **`; the GUC
// slot model is `Option<String>` (NULL stays distinguishable from `""`). The
// boot value installed by guc_tables.c is `Some("")`.
// ---------------------------------------------------------------------------

macro_rules! string_guc {
    ($store:ident, $get:ident, $set:ident, $doc:literal) => {
        std::thread_local! {
            // $doc — the C global / GUC this backing cell mirrors.
            static $store: RefCell<Option<String>> =
                const { RefCell::new(Some(String::new())) };
        }

        /// `*conf->variable` read accessor.
        pub fn $get() -> Option<String> {
            $store.with(|c| c.borrow().clone())
        }

        /// `*conf->variable` write accessor.
        pub fn $set(value: Option<String>) {
            $store.with(|c| *c.borrow_mut() = value);
        }
    };
}

string_guc!(
    RECOVERY_RESTORE_COMMAND,
    recovery_restore_command,
    set_recovery_restore_command,
    "`recoveryRestoreCommand` (`restore_command` GUC; xlogrecovery.c:84)."
);
string_guc!(
    RECOVERY_END_COMMAND,
    recovery_end_command,
    set_recovery_end_command,
    "`recoveryEndCommand` (`recovery_end_command` GUC; xlogrecovery.c:85)."
);
string_guc!(
    ARCHIVE_CLEANUP_COMMAND,
    archive_cleanup_command,
    set_archive_cleanup_command,
    "`archiveCleanupCommand` (`archive_cleanup_command` GUC; xlogrecovery.c:86)."
);
string_guc!(
    PRIMARY_CONN_INFO,
    primary_conn_info,
    set_primary_conn_info,
    "`PrimaryConnInfo` (`primary_conninfo` GUC; xlogrecovery.c:98)."
);
string_guc!(
    PRIMARY_SLOT_NAME,
    primary_slot_name,
    set_primary_slot_name,
    "`PrimarySlotName` (`primary_slot_name` GUC; xlogrecovery.c:99)."
);
string_guc!(
    RECOVERY_TARGET_TIME_STRING,
    recovery_target_time_string,
    set_recovery_target_time_string,
    "`recovery_target_time_string` (`recovery_target_time` GUC; xlogrecovery.c:91)."
);

// ---------------------------------------------------------------------------
// Scalar GUC globals.
// ---------------------------------------------------------------------------

std::thread_local! {
    /// `recoveryTargetInclusive` (`recovery_target_inclusive` GUC; boot `true`;
    /// xlogrecovery.c:88).
    static RECOVERY_TARGET_INCLUSIVE: Cell<bool> = const { Cell::new(true) };

    /// `recoveryTargetAction` (`recovery_target_action` GUC, an int over the
    /// `recovery_target_action` enum; boot `RECOVERY_TARGET_ACTION_PAUSE`;
    /// xlogrecovery.c:89).
    static RECOVERY_TARGET_ACTION: Cell<i32> =
        const { Cell::new(RecoveryTargetAction::Pause as i32) };

    /// `recovery_min_apply_delay` (milliseconds; boot `0`; xlogrecovery.c:95).
    static RECOVERY_MIN_APPLY_DELAY: Cell<i32> = const { Cell::new(0) };

    /// `wal_receiver_create_temp_slot` (boot `false`; xlogrecovery.c:100).
    static WAL_RECEIVER_CREATE_TEMP_SLOT: Cell<bool> = const { Cell::new(false) };
}

/// `*conf->variable` read for `recoveryTargetInclusive`.
pub fn recovery_target_inclusive() -> bool {
    RECOVERY_TARGET_INCLUSIVE.with(Cell::get)
}
/// `*conf->variable` write for `recoveryTargetInclusive`.
pub fn set_recovery_target_inclusive(value: bool) {
    RECOVERY_TARGET_INCLUSIVE.with(|c: &Cell<_>| c.set(value));
}

/// `*conf->variable` read for `recoveryTargetAction`.
pub fn recovery_target_action() -> i32 {
    RECOVERY_TARGET_ACTION.with(Cell::get)
}
/// `*conf->variable` write for `recoveryTargetAction`.
pub fn set_recovery_target_action(value: i32) {
    RECOVERY_TARGET_ACTION.with(|c: &Cell<_>| c.set(value));
}

/// `*conf->variable` read for `recovery_min_apply_delay`.
pub fn recovery_min_apply_delay() -> i32 {
    RECOVERY_MIN_APPLY_DELAY.with(Cell::get)
}
/// `*conf->variable` write for `recovery_min_apply_delay`.
pub fn set_recovery_min_apply_delay(value: i32) {
    RECOVERY_MIN_APPLY_DELAY.with(|c: &Cell<_>| c.set(value));
}

/// `*conf->variable` read for `wal_receiver_create_temp_slot`.
pub fn wal_receiver_create_temp_slot() -> bool {
    WAL_RECEIVER_CREATE_TEMP_SLOT.with(Cell::get)
}
/// `*conf->variable` write for `wal_receiver_create_temp_slot`.
pub fn set_wal_receiver_create_temp_slot(value: bool) {
    WAL_RECEIVER_CREATE_TEMP_SLOT.with(|c: &Cell<_>| c.set(value));
}

// ---------------------------------------------------------------------------
// Recovery-target globals (xlogrecovery.c:87-123). These are NOT `conf->variable`
// of any GUC: the GUC strings (`recovery_target`, `recovery_target_xid`,
// `recovery_target_lsn`, `recovery_target_name`, `recovery_target_time`,
// `recovery_target_timeline`) are parsed by the check/assign hooks, which write
// these decoded extern globals. The startup process snapshots them into its
// `XLogRecoveryState` at `InitWalRecovery`. The hooks fire in any backend
// (PGC_POSTMASTER), so these mirror the C file-statics 1:1 as the assign-hook's
// home; the boot values match the C initializers.
// ---------------------------------------------------------------------------

std::thread_local! {
    /// `RecoveryTargetType recoveryTarget = RECOVERY_TARGET_UNSET;`
    /// (xlogrecovery.c:87).
    static RECOVERY_TARGET: Cell<RecoveryTargetType> =
        const { Cell::new(RecoveryTargetType::Unset) };

    /// `TransactionId recoveryTargetXid;` (xlogrecovery.c:90).
    static RECOVERY_TARGET_XID: Cell<TransactionId> = const { Cell::new(0) };

    /// `XLogRecPtr recoveryTargetLSN;` (xlogrecovery.c:94).
    static RECOVERY_TARGET_LSN: Cell<XLogRecPtr> = const { Cell::new(InvalidXLogRecPtr) };

    /// `const char *recoveryTargetName;` (xlogrecovery.c:93).
    static RECOVERY_TARGET_NAME: RefCell<String> = const { RefCell::new(String::new()) };

    /// `RecoveryTargetTimeLineGoal recoveryTargetTimeLineGoal =
    /// RECOVERY_TARGET_TIMELINE_LATEST;` (xlogrecovery.c:122).
    static RECOVERY_TARGET_TIMELINE_GOAL: Cell<RecoveryTargetTimeLineGoal> =
        const { Cell::new(RecoveryTargetTimeLineGoal::Latest) };

    /// `TimeLineID recoveryTargetTLIRequested = 0;` (xlogrecovery.c:123).
    static RECOVERY_TARGET_TLI_REQUESTED: Cell<TimeLineID> = const { Cell::new(0) };
}

/// `recoveryTarget` read.
pub fn recovery_target() -> RecoveryTargetType {
    RECOVERY_TARGET.with(Cell::get)
}
/// `recoveryTarget` write.
pub fn set_recovery_target(value: RecoveryTargetType) {
    RECOVERY_TARGET.with(|c: &Cell<_>| c.set(value));
}

/// `recoveryTargetXid` read.
pub fn recovery_target_xid() -> TransactionId {
    RECOVERY_TARGET_XID.with(Cell::get)
}
/// `recoveryTargetXid` write.
pub fn set_recovery_target_xid(value: TransactionId) {
    RECOVERY_TARGET_XID.with(|c: &Cell<_>| c.set(value));
}

/// `recoveryTargetLSN` read.
pub fn recovery_target_lsn() -> XLogRecPtr {
    RECOVERY_TARGET_LSN.with(Cell::get)
}
/// `recoveryTargetLSN` write.
pub fn set_recovery_target_lsn(value: XLogRecPtr) {
    RECOVERY_TARGET_LSN.with(|c: &Cell<_>| c.set(value));
}

/// `recoveryTargetName` read.
pub fn recovery_target_name() -> String {
    RECOVERY_TARGET_NAME.with(|c| c.borrow().clone())
}
/// `recoveryTargetName` write.
pub fn set_recovery_target_name(value: String) {
    RECOVERY_TARGET_NAME.with(|c| *c.borrow_mut() = value);
}

/// `recoveryTargetTimeLineGoal` read.
pub fn recovery_target_timeline_goal() -> RecoveryTargetTimeLineGoal {
    RECOVERY_TARGET_TIMELINE_GOAL.with(Cell::get)
}
/// `recoveryTargetTimeLineGoal` write.
pub fn set_recovery_target_timeline_goal(value: RecoveryTargetTimeLineGoal) {
    RECOVERY_TARGET_TIMELINE_GOAL.with(|c: &Cell<_>| c.set(value));
}

/// `recoveryTargetTLIRequested` read.
pub fn recovery_target_tli_requested() -> TimeLineID {
    RECOVERY_TARGET_TLI_REQUESTED.with(Cell::get)
}
/// `recoveryTargetTLIRequested` write.
pub fn set_recovery_target_tli_requested(value: TimeLineID) {
    RECOVERY_TARGET_TLI_REQUESTED.with(|c: &Cell<_>| c.set(value));
}
