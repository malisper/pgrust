//! `tcop/utility.c` — the utility-statement command processor (PostgreSQL 18.3).
//!
//! This crate ports the **parse-tree classifiers** of `utility.c` 1:1 over the
//! repo's owned [`types_nodes::nodes::Node`] tree — the read-only classification,
//! command-tag derivation, log-level derivation, and returns-tuples /
//! tuple-descriptor / contains-query predicates. Each is a pure walk over the
//! `Node` enum, ported branch-for-branch with the C control flow, constants, and
//! (where they raise) error messages / SQLSTATEs:
//!
//!   * [`CommandIsReadOnly`]                  (utility.c:94)
//!   * [`ClassifyUtilityCommandAsReadOnly`]   (utility.c:127, static)
//!   * [`PreventCommandIfReadOnly`] / [`PreventCommandIfParallelMode`] /
//!     [`PreventCommandDuringRecovery`] / [`CheckRestrictedOperation`]
//!     (utility.c:404-467) — consult state seams
//!   * [`UtilityReturnsTuples`]               (utility.c:2028)
//!   * [`UtilityTupleDescriptor`]             (utility.c:2084)
//!   * [`QueryReturnsTuples`]                 (utility.c:2137, `#ifdef NOT_USED`)
//!   * [`UtilityContainsQuery`]               (utility.c:2179)
//!   * [`AlterObjectTypeCommandTag`]          (utility.c:2215, static)
//!   * [`CreateCommandTag`]                   (utility.c:2362)
//!   * [`GetCommandLogLevel`]                 (utility.c:3249)
//!
//! The external state predicates and per-statement-source descriptor lookups the
//! classifiers consult cross [`backend_tcop_utility_out_seams`], defaulting to a
//! loud panic until the owning subsystem installs them.
//!
//! ## Dispatch (`ProcessUtility` / `standard_ProcessUtility` / `ExecDropStmt` /
//! `ProcessUtilityForAlterTable`) — ported in [`crate::dispatch`]
//!
//! The giant `nodeTag` dispatch switch is in [`crate::dispatch`], ported 1:1
//! over the owned [`types_nodes::nodes::Node`] tree (the `readOnlyTree`
//! deep-copy, the recursion / read-only / parallel / recovery guards, the full
//! command switch, and the trailing `CommandCounterIncrement`). The inward
//! `backend_tcop_utility_seams::process_utility` seam now carries the per-utility
//! `mcx: Mcx<'mcx>` working context (the owned analogue of C's per-message
//! `CurrentMemoryContext`): `standard_ProcessUtility` allocates the
//! `copyObject(pstmt)` deep-copy and the `make_parsestate(NULL)` parse state in
//! it, and the caller (pquery `PortalRunUtility`) supplies a per-utility scratch
//! context it drops on return. The seam is installed in [`init_seams`].
//!
//! Every command body the switch routes to lives in another subsystem and is
//! reached through a forwarding seam in [`backend_tcop_utility_out_seams`]
//! (xact-control, portal verbs, every `commands` handler, the event-trigger
//! machinery / `process_utility_slow`, the checkpointer, …). Each defaults to a
//! loud panic until its owning subsystem installs the real handler — so e.g.
//! VACUUM/CLUSTER/CREATE DATABASE/PREPARE/DECLARE CURSOR route to their already-
//! landed owners once those owners install their arm, while DROP/COPY/EXPLAIN/
//! GRANT and the whole `process_utility_slow` DDL fan-out (CREATE/ALTER TABLE,
//! CREATE INDEX, …) seam-and-panic until their owners (or the dedicated
//! `process_utility_slow` wiring point) land. `ProcessUtilitySlow` itself is an
//! outward seam, not an in-crate body: its ~400-line body fans out to ~40 DDL
//! owners plus the `parse_utilcmd.c` transforms and is owned by a dedicated
//! wiring point, exactly as in the established seam architecture.

#![allow(non_snake_case)]
// The classifiers carry the large shared `PgError` enum by value in `PgResult`,
// matching every sibling crate's Result shape.
#![allow(clippy::result_large_err)]

pub mod classify;
pub mod commandtag;
pub mod consts;
pub mod dispatch;
pub mod loglevel;
pub mod returns;
pub mod slow;

pub use classify::{
    CheckRestrictedOperation, ClassifyUtilityCommandAsReadOnly, CommandIsReadOnly,
    PreventCommandDuringRecovery, PreventCommandIfParallelMode, PreventCommandIfReadOnly,
};
pub use commandtag::{AlterObjectTypeCommandTag, CreateCommandTag};
pub use dispatch::{
    ExecDropStmt, ProcessUtility, ProcessUtilityForAlterTable, process_utility_wrapper,
    standard_ProcessUtility,
};
pub use consts::{
    LogStmtLevel, COMMAND_IS_NOT_READ_ONLY, COMMAND_IS_STRICTLY_READ_ONLY,
    COMMAND_OK_IN_PARALLEL_MODE, COMMAND_OK_IN_READ_ONLY_TXN, COMMAND_OK_IN_RECOVERY, LOGSTMT_ALL,
    LOGSTMT_DDL, LOGSTMT_MOD, LOGSTMT_NONE,
};
pub use loglevel::GetCommandLogLevel;
pub use returns::{QueryReturnsTuples, UtilityContainsQuery, UtilityReturnsTuples, UtilityTupleDescriptor};

/// Install this unit's inward seams (`backend-tcop-utility-seams`).
///
/// Installs all of this crate's inward seams: the classifier seams it fully
/// grounds and that are already consumed (`create_command_tag` /
/// `get_command_log_level` / `utility_returns_tuples` /
/// `utility_tuple_descriptor` by pquery / prepare / async / xid8funcs), the
/// `prevent_command_during_recovery` guard, and the `process_utility` dispatch
/// entrypoint (the `Mcx`-carrying per-utility working-context seam landed with
/// the dispatch spine; see [`crate::dispatch::ProcessUtility`]).
pub fn init_seams() {
    backend_tcop_utility_seams::create_command_tag::set(CreateCommandTag);
    backend_tcop_utility_seams::get_command_log_level::set(GetCommandLogLevel);
    backend_tcop_utility_seams::utility_returns_tuples::set(UtilityReturnsTuples);
    backend_tcop_utility_seams::utility_tuple_descriptor::set(UtilityTupleDescriptor);
    backend_tcop_utility_seams::prevent_command_during_recovery::set(PreventCommandDuringRecovery);
    backend_tcop_utility_seams::process_utility::set(ProcessUtility);

    // The event-trigger-fenced DDL fan-out (`ProcessUtilitySlow`). The dispatch's
    // GRANT/DROP/RENAME/ALTER…/COMMENT/SECURITY LABEL fast-path arms and the
    // `_ =>` arm reach it through the `process_utility_slow` outward seam; install
    // it here so CREATE TABLE → `DefineRelation` becomes reachable.
    backend_tcop_utility_out_seams::process_utility_slow::set(slow::process_utility_slow);

    // The recursive `ProcessUtility` re-entry the CREATE-TABLE sub-statement
    // fan-out (implied IndexStmt / AlterTableStmt from PRIMARY KEY / UNIQUE /
    // FOREIGN KEY) and `ProcessUtilityForAlterTable` dispatch through. The body
    // is `ProcessUtility` itself (owned here); it builds the subcommand wrapper
    // `PlannedStmt` + `None` receiver and re-enters the dispatch.
    backend_tcop_utility_out_seams::process_utility_wrapper::set(dispatch::process_utility_wrapper);

    // `CreateSchemaCommand`'s embedded-element loop (schemacmds.c) runs each
    // sub-statement (CREATE TABLE/VIEW/SEQUENCE/INDEX/TRIGGER) through a wrapper
    // `PlannedStmt` + `ProcessUtility(.., PROCESS_UTILITY_SUBCOMMAND, ..,
    // None_Receiver, NULL)` — identical construction to `process_utility_wrapper`.
    // Install the schemacmds-owned subcommand seam pointing at that same body.
    backend_tcop_utility_fc_seams::process_utility_create_schema_subcommand::set(
        |stmt, query_string, stmt_location, stmt_len| {
            let ctx = mcx::MemoryContext::new("process_utility_create_schema_subcommand");
            dispatch::process_utility_wrapper(
                ctx.mcx(),
                stmt,
                query_string,
                stmt_location,
                stmt_len,
            )
        },
    );

    // EventTriggerSupportsObjectType (commands/event_trigger.c): a pure dispatch
    // predicate the GRANT/DROP/RENAME/ALTER…/COMMENT/SECURITY LABEL fast-path
    // arms consult. The real owner `backend-commands-event-trigger` (the
    // `commands/event_trigger.c` port) now installs this seam, so the stopgap
    // install that used to live here is gone — `dispatch::EventTriggerSupportsObjectType`
    // is retained only as the in-crate reference predicate.
}
