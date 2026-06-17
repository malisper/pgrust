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
use crate::core::RecoveryTargetAction;

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
