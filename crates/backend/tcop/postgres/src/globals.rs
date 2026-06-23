//! File-local globals of `tcop/postgres.c`.
//!
//! These are the per-backend variables declared at the top of `postgres.c`
//! (and the two `static struct`s for `ResetUsage`/`ShowUsage`). Each C backend
//! is a process owning its own copy, so each is a `thread_local!` cell here,
//! never a shared static — exactly as `backend-utils-init-small`'s `globals.c`
//! port does for `globals.c`.

#![allow(non_upper_case_globals)]

use std::cell::Cell;

use ::types_dest::dest::CommandDest;
use ::types_storage::ProcSignalReason;
use ::types_storage::storage::NUM_PROCSIGNALS;

thread_local! {
    /// `const char *debug_query_string;` (postgres.c:88) — the client-supplied
    /// query string. `None` mirrors the C `NULL`. Only the reset-to-NULL path
    /// (`reset_debug_query_string`) is in this family's scope; the simple/extended
    /// query loop that sets it is F1/F2 (planner-gated).
    static DEBUG_QUERY_STRING: Cell<Option<&'static str>> = const { Cell::new(None) };


    /// `static bool xact_started = false;` (postgres.c:129) — whether a
    /// `start_xact_command` is in effect (`StartTransactionCommand` has been
    /// issued for the current message). Read by `enable_statement_timeout`'s
    /// assertion.
    static XACT_STARTED: Cell<bool> = const { Cell::new(false) };

    /// `static bool DoingCommandRead = false;` (postgres.c:136) — true while the
    /// backend is in `PostgresMain`'s blocking read for the next client command.
    static DOING_COMMAND_READ: Cell<bool> = const { Cell::new(false) };

    /// `static const char *userDoption = NULL;` (postgres.c:153) — the `-D`
    /// switch value (data directory), captured by `process_postgres_switches`.
    static USER_DOPTION: Cell<Option<&'static str>> = const { Cell::new(None) };

    /// `static bool EchoQuery = false;` (postgres.c:154) — the `-E` switch
    /// (echo queries, single-user mode).
    static ECHO_QUERY: Cell<bool> = const { Cell::new(false) };

    /// `static bool UseSemiNewlineNewline = false;` (postgres.c:155) — the `-j`
    /// switch (use `;\n\n` as the interactive command delimiter).
    static USE_SEMI_NEWLINE_NEWLINE: Cell<bool> = const { Cell::new(false) };

    /// `static bool doing_extended_query_message = false;` (postgres.c:146) —
    /// true while `PostgresMain` is processing an extended-query-protocol
    /// message; controls whether an error initiates skip-till-Sync.
    static DOING_EXTENDED_QUERY_MESSAGE: Cell<bool> = const { Cell::new(false) };

    /// `static bool ignore_till_sync = false;` (postgres.c:147) — true while we
    /// are skipping messages until the next Sync after an extended-query error.
    static IGNORE_TILL_SYNC: Cell<bool> = const { Cell::new(false) };

    /// `static CachedPlanSource *unnamed_stmt_psrc = NULL;` (postgres.c:165) —
    /// the saved `CachedPlanSource` for the unnamed prepared statement (the one
    /// created by a `Parse` message with an empty statement name). A handle of
    /// `0` (`CachedPlanSourceHandle::NULL`) mirrors the C `NULL`.
    static UNNAMED_STMT_PSRC: Cell<nodes::parsestmt::CachedPlanSourceHandle> =
        const { Cell::new(nodes::parsestmt::CachedPlanSourceHandle::NULL) };

    /// `static volatile sig_atomic_t RecoveryConflictPending = false;`
    /// (postgres.c:158).
    static RECOVERY_CONFLICT_PENDING: Cell<bool> = const { Cell::new(false) };

    /// `static volatile sig_atomic_t RecoveryConflictPendingReasons[NUM_PROCSIGNALS];`
    /// (postgres.c:159) — per-reason pending flags for recovery conflicts.
    static RECOVERY_CONFLICT_PENDING_REASONS: Cell<[bool; NUM_PROCSIGNALS]> =
        const { Cell::new([false; NUM_PROCSIGNALS]) };
}

// `debug_query_string`.

#[inline]
pub fn debug_query_string() -> Option<&'static str> {
    DEBUG_QUERY_STRING.get()
}

#[inline]
pub fn set_debug_query_string(value: Option<&'static str>) {
    DEBUG_QUERY_STRING.set(value);
}

// `whereToSendOutput`.

// `whereToSendOutput` (postgres.c:91) has ONE canonical home — the
// `backend-utils-error::config` cell. C declares it once in postgres.c and both
// the error reporter and `ReadCommand` read the same variable; the Rust split
// previously kept a second tcop-local copy, so `BackendInitialize`'s
// `DestRemote` (written to the error-config cell) was invisible to `ReadCommand`
// and a forked client backend wrongly took the interactive (`backend> `) path.
// Delegate to the single cell so every reader/writer agrees.
#[inline]
pub fn where_to_send_output() -> CommandDest {
    utils_error::config::where_to_send_output()
}

#[inline]
pub fn set_where_to_send_output(value: CommandDest) {
    utils_error::config::set_where_to_send_output(value);
}

// `xact_started`.

#[inline]
pub fn xact_started() -> bool {
    XACT_STARTED.get()
}

#[inline]
pub fn set_xact_started(value: bool) {
    XACT_STARTED.set(value);
}

// `DoingCommandRead`.

#[inline]
pub fn doing_command_read() -> bool {
    DOING_COMMAND_READ.get()
}

#[inline]
pub fn set_doing_command_read(value: bool) {
    DOING_COMMAND_READ.set(value);
}

// `userDoption`.

#[inline]
pub fn user_doption() -> Option<&'static str> {
    USER_DOPTION.get()
}

#[inline]
pub fn set_user_doption(value: Option<&'static str>) {
    USER_DOPTION.set(value);
}

// `EchoQuery`.

#[inline]
pub fn echo_query() -> bool {
    ECHO_QUERY.get()
}

#[inline]
pub fn set_echo_query(value: bool) {
    ECHO_QUERY.set(value);
}

// `UseSemiNewlineNewline`.

#[inline]
pub fn use_semi_newline_newline() -> bool {
    USE_SEMI_NEWLINE_NEWLINE.get()
}

#[inline]
pub fn set_use_semi_newline_newline(value: bool) {
    USE_SEMI_NEWLINE_NEWLINE.set(value);
}

// `doing_extended_query_message`.

#[inline]
pub fn doing_extended_query_message() -> bool {
    DOING_EXTENDED_QUERY_MESSAGE.get()
}

#[inline]
pub fn set_doing_extended_query_message(value: bool) {
    DOING_EXTENDED_QUERY_MESSAGE.set(value);
}

// `ignore_till_sync`.

#[inline]
pub fn ignore_till_sync() -> bool {
    IGNORE_TILL_SYNC.get()
}

#[inline]
pub fn set_ignore_till_sync(value: bool) {
    IGNORE_TILL_SYNC.set(value);
}

// `unnamed_stmt_psrc`.

#[inline]
pub fn unnamed_stmt_psrc() -> nodes::parsestmt::CachedPlanSourceHandle {
    UNNAMED_STMT_PSRC.get()
}

#[inline]
pub fn set_unnamed_stmt_psrc(value: nodes::parsestmt::CachedPlanSourceHandle) {
    UNNAMED_STMT_PSRC.set(value);
}

// `RecoveryConflictPending`.

#[inline]
pub fn recovery_conflict_pending() -> bool {
    RECOVERY_CONFLICT_PENDING.get()
}

#[inline]
pub fn set_recovery_conflict_pending(value: bool) {
    RECOVERY_CONFLICT_PENDING.set(value);
}

// `RecoveryConflictPendingReasons[reason]`.

#[inline]
pub fn recovery_conflict_pending_reason(reason: ProcSignalReason) -> bool {
    RECOVERY_CONFLICT_PENDING_REASONS.get()[reason as usize]
}

#[inline]
pub fn set_recovery_conflict_pending_reason(reason: ProcSignalReason, value: bool) {
    let mut arr = RECOVERY_CONFLICT_PENDING_REASONS.get();
    arr[reason as usize] = value;
    RECOVERY_CONFLICT_PENDING_REASONS.set(arr);
}
