//! Idiomatic port of the **synchronous trigger-firing engine** of
//! `backend/commands/trigger.c` (PostgreSQL 18.3).
//!
//! Scope (the firing engine):
//!   * [`firing::exec_call_trigger_func`] (`ExecCallTriggerFunc`) — dispatch a
//!     trigger function via `fmgr` (`function_call_invoke`), handing the per-call
//!     [`TriggerData`](types_nodes::trigger::TriggerData) to the callee through
//!     the thread-local current-trigger side-channel (the owned analogue of C's
//!     `fcinfo->context = (Node *) &LocTriggerData`).
//!   * The per-statement / per-row `Exec*Triggers` entry points called by
//!     `nodeModifyTable.c`.
//!   * The AFTER-trigger event queue + lifecycle ([`queue`]) and the firing
//!     runtime ([`firing`]: `AfterTriggerExecute`, mark/invoke, end-query).
//!
//! Deferred (the catalog-write DDL leg — a separate family): `CreateTrigger`,
//! `RemoveTriggerById`, `renametrig`, `EnableDisableTrigger`,
//! `RelationBuildTriggers`, `AfterTriggerSetState` (the `SET CONSTRAINTS`
//! command).  Each stays a loud 1:1-named seam-and-panic until its family lands.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

pub mod firing;
pub mod fmgr_builtins;
pub mod queue;

/// Install every implementation in `backend-commands-trigger-seams` (and the
/// trigger-firing seams `nodeModifyTable` consumes through that crate).
pub fn init_seams() {
    firing::init_seams();
    fmgr_builtins::register_trigger_builtins();
}
