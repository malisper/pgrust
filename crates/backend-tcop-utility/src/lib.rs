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
//! ## Dispatch (`ProcessUtility` / `standard_ProcessUtility` /
//! `ProcessUtilitySlow`) — keystone-blocked, not yet ported here
//!
//! The giant `nodeTag` dispatch is **not** in this crate. The installed inward
//! `backend_tcop_utility_seams::process_utility` seam — already consumed by
//! `backend-tcop-pquery`'s `portal_run_utility` — has the C parameter set
//! *minus an `mcx`* (`&PlannedStmt`, `query_string`, `read_only_tree`, `context`,
//! `params`, `dest`, `&mut QueryCompletion`). But `standard_ProcessUtility` must
//! `make_parsestate(NULL)` (allocates a `ParseState<'mcx>`), `copyObject(pstmt)`
//! when `readOnlyTree`, and run the `parse_utilcmd.c` transforms in
//! `ProcessUtilitySlow` — every one of which needs an `Mcx<'mcx>`. There is no
//! ambient memory context in this workspace (by design), and `portal_run_utility`
//! holds none to thread through. Re-signing the inward seam to carry an `mcx`
//! and supplying one from pquery is a prerequisite keystone (thread the
//! per-query `Mcx` into `portal_run_utility` → the `process_utility` seam). Until
//! then the dispatch cannot be installed faithfully, so it is deferred; the
//! `process_utility` seam stays on its panic-until-installed default. The four
//! classifier inward seams (`create_command_tag`, `utility_returns_tuples`,
//! `utility_tuple_descriptor` — pquery already passes it an `mcx` — and
//! `prevent_command_during_recovery`) are fully installable now and are wired in
//! [`init_seams`].

#![allow(non_snake_case)]
// The classifiers carry the large shared `PgError` enum by value in `PgResult`,
// matching every sibling crate's Result shape.
#![allow(clippy::result_large_err)]

pub mod classify;
pub mod commandtag;
pub mod consts;
pub mod loglevel;
pub mod returns;

pub use classify::{
    CheckRestrictedOperation, ClassifyUtilityCommandAsReadOnly, CommandIsReadOnly,
    PreventCommandDuringRecovery, PreventCommandIfParallelMode, PreventCommandIfReadOnly,
};
pub use commandtag::{AlterObjectTypeCommandTag, CreateCommandTag};
pub use consts::{
    LogStmtLevel, COMMAND_IS_NOT_READ_ONLY, COMMAND_IS_STRICTLY_READ_ONLY,
    COMMAND_OK_IN_PARALLEL_MODE, COMMAND_OK_IN_READ_ONLY_TXN, COMMAND_OK_IN_RECOVERY, LOGSTMT_ALL,
    LOGSTMT_DDL, LOGSTMT_MOD, LOGSTMT_NONE,
};
pub use loglevel::GetCommandLogLevel;
pub use returns::{QueryReturnsTuples, UtilityContainsQuery, UtilityReturnsTuples, UtilityTupleDescriptor};

/// Install this unit's inward seams (`backend-tcop-utility-seams`).
///
/// Installs the four classifier seams that this crate fully grounds and that are
/// already consumed (pquery / prepare / async / xid8funcs). The fifth declared
/// inward seam, `process_utility`, is the dispatch entrypoint and is **not**
/// installed here — see the crate-level docs (keystone-blocked on threading an
/// `Mcx` through the inward seam + pquery).
pub fn init_seams() {
    backend_tcop_utility_seams::create_command_tag::set(CreateCommandTag);
    backend_tcop_utility_seams::get_command_log_level::set(GetCommandLogLevel);
    backend_tcop_utility_seams::utility_returns_tuples::set(UtilityReturnsTuples);
    backend_tcop_utility_seams::utility_tuple_descriptor::set(UtilityTupleDescriptor);
    backend_tcop_utility_seams::prevent_command_during_recovery::set(PreventCommandDuringRecovery);
}
