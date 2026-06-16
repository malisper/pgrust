//! Outward seams the `tcop/utility.c` **classifiers** consult.
//!
//! `backend-tcop-utility` ports the parse-tree classifiers in-crate
//! (read-only classification, command-tag / log-level derivation, the
//! returns-tuples / tuple-descriptor predicates). The only things that cross a
//! seam are the genuine backend-state predicates the read-only / parallel /
//! recovery / security guards read, and the per-statement-source descriptor
//! lookups (`UtilityReturnsTuples` / `UtilityTupleDescriptor`) which reach into
//! the portal / prepared-statement / explain / SHOW owners. Each owning
//! subsystem installs its real implementation when it lands; until then a call
//! panics loudly with the seam path (mirror-PG-and-panic).
//!
//! (The full `ProcessUtility` dispatch — which fans out to ~70 command owners —
//! is not yet ported in `backend-tcop-utility`; see that crate's docs for the
//! `mcx`-threading keystone that blocks it. Its per-command leaf seams will be
//! added here when the dispatch lands.)

#![allow(non_snake_case)]

use mcx::Mcx;
use seam_core::seam;
use types_nodes::nodes::Node;
use types_tuple::heaptuple::TupleDesc;

/* ===========================================================================
 * backend-state predicates the read-only / parallel / recovery / security
 * guards consult (xact.c / xlog.c / miscinit.c).
 * ======================================================================== */

seam!(
    /// `XactReadOnly` (xact.c) — is the current transaction read-only?
    pub fn xact_read_only() -> bool
);
seam!(
    /// `IsInParallelMode()` (xact.c) — is the current (sub)transaction parallel?
    pub fn is_in_parallel_mode() -> bool
);
seam!(
    /// `RecoveryInProgress()` (xlog.c) — is the server in recovery / hot standby?
    pub fn recovery_in_progress() -> bool
);
seam!(
    /// `InSecurityRestrictedOperation()` (miscinit.c).
    pub fn in_security_restricted_operation() -> bool
);

/* ===========================================================================
 * tuple-returning utility descriptor sources (UtilityReturnsTuples /
 * UtilityTupleDescriptor). A missing portal / prepared statement folds to
 * `false` / `None`, matching the C switches, so these are infallible.
 * ======================================================================== */

seam!(
    /// `GetPortalByName(name) && portal->tupDesc != NULL` (FETCH returns-tuples
    /// predicate; folds the invalid-portal guard).
    pub fn fetch_stmt_portal_tupdesc(stmt: &Node) -> bool
);
seam!(
    /// `FetchPreparedStatement(name, false) && entry->plansource->resultDesc !=
    /// NULL` (EXECUTE returns-tuples predicate).
    pub fn execute_stmt_has_result(stmt: &Node) -> bool
);
seam!(
    /// `CallStmtResultDesc(stmt)` (functioncmds.c).
    pub fn call_stmt_result_desc<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> TupleDesc<'mcx>
);
seam!(
    /// FETCH: `CreateTupleDescCopy(GetPortalByName(name)->tupDesc)` (portalmem.c).
    pub fn fetch_stmt_result_desc<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> TupleDesc<'mcx>
);
seam!(
    /// EXECUTE: `FetchPreparedStatementResultDesc(entry)` (prepare.c).
    pub fn execute_stmt_result_desc<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> TupleDesc<'mcx>
);
seam!(
    /// `ExplainResultDesc(stmt)` (explain.c).
    pub fn explain_result_desc<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> TupleDesc<'mcx>
);
seam!(
    /// `GetPGVariableResultDesc(name)` (guc.c) — SHOW result descriptor.
    pub fn get_pg_variable_result_desc<'mcx>(mcx: Mcx<'mcx>, name: Option<&str>) -> TupleDesc<'mcx>
);

/* ===========================================================================
 * GetCommandLogLevel helpers (define.c / prepare.c).
 * ======================================================================== */

seam!(
    /// `defGetBoolean(opt)` (define.c) — EXPLAIN ANALYZE option scan.
    pub fn def_get_boolean(opt: &Node) -> bool
);
seam!(
    /// EXECUTE: `FetchPreparedStatement(name, false)->plansource->raw_parse_tree`
    /// (prepare.c) — the raw parse tree `GetCommandLogLevel` looks through.
    /// Returns the cached raw parse-tree node, or `None`; a cache read, so
    /// infallible.
    pub fn execute_stmt_raw_parse_tree<'mcx>(stmt: &Node<'mcx>) -> Option<types_nodes::nodes::NodePtr<'mcx>>
);
