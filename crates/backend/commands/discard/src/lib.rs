#![allow(non_snake_case)]
// `PgError` is a large error type shared across the whole tree, so boxing it
// would diverge from every sibling crate's `PgResult` shape.
#![allow(clippy::result_large_err)]

//! `backend/commands/discard.c` — `DISCARD { ALL | PLANS | SEQUENCES | TEMP }`.
//!
//! discard.c is a pure dispatcher with two functions, both ported here
//! branch-for-branch against the owned [`DiscardStmt`] node:
//!
//!  * [`DiscardCommand`] — the command driver. It switches on `stmt.target`.
//!  * [`DiscardAll`] — the `DISCARD ALL` body: the in-transaction-block guard
//!    followed by the fixed, ordered sequence of cross-subsystem resets/drops.
//!
//! Every external is a genuine sibling-subsystem boundary. The already-ported
//! owners (portalmem, prepared-statement cache, plan cache, temp namespace) are
//! called directly; the not-yet-ported owners (xact, GUC, async, lock manager,
//! sequence) are reached through their seam crates and panic loudly until those
//! owners land.

use catalog_namespace::ResetTempTableNamespace;
use async_seams::async_unlisten_all;
use prepare::DropAllPreparedStatements;
use sequence_seams_2::reset_sequence_caches;
use lock_seams::lock_release_all_user;
use cache_plancache::ResetPlanCache;
use guc_seams::{reset_all_options, set_pg_variable_session_authorization_reset};
use portalmem::PortalHashTableDeleteAll;

use transam_xact_seams::prevent_in_transaction_block;
use types_error::PgResult;
// The canonical CALL/DISCARD parse node lives in `nodes::ddlnodes` (the
// node-opaque payload the dispatcher downcasts to). `parsenodes::DiscardMode`
// is a re-export of this same enum, so the two `DiscardStmt` shapes are
// field-identical; we consume the canonical node form directly so the
// ProcessUtility dispatch can hand us the downcast payload with no conversion.
use nodes::ddlnodes::{DiscardMode, DiscardStmt};

/// `DISCARD { ALL | SEQUENCES | TEMP | PLANS }`.
///
/// 1:1 port of `DiscardCommand` (discard.c). The C `switch` carries a
/// `default:` arm that `elog(ERROR)`s on an unrecognized target; the owned
/// [`DiscardMode`] enum is exhaustive (there is no raw/numeric value that could
/// fall outside the four variants), so the match is exhaustive and the
/// corruption-only `default` arm is unrepresentable.
pub fn DiscardCommand(stmt: &DiscardStmt, is_top_level: bool) -> PgResult<()> {
    match stmt.target {
        DiscardMode::DISCARD_ALL => DiscardAll(is_top_level),
        DiscardMode::DISCARD_PLANS => ResetPlanCache(),
        DiscardMode::DISCARD_SEQUENCES => reset_sequence_caches::call(),
        DiscardMode::DISCARD_TEMP => ResetTempTableNamespace(),
    }
}

/// 1:1 port of static `DiscardAll` (discard.c).
fn DiscardAll(is_top_level: bool) -> PgResult<()> {
    // Disallow DISCARD ALL in a transaction block. This is arguably
    // inconsistent (we don't make a similar check in the command sequence
    // that DISCARD ALL is equivalent to), but the idea is to catch mistakes:
    // DISCARD ALL inside a transaction block would leave the transaction
    // still uncommitted.
    prevent_in_transaction_block::call(is_top_level, "DISCARD ALL")?;

    // Closing portals might run user-defined code, so do that first.
    PortalHashTableDeleteAll()?;
    set_pg_variable_session_authorization_reset::call()?;
    reset_all_options::call()?;
    DropAllPreparedStatements()?;
    async_unlisten_all::call()?;
    lock_release_all_user::call()?;
    ResetPlanCache()?;
    ResetTempTableNamespace()?;
    reset_sequence_caches::call()?;

    Ok(())
}

/// Install the outward `ProcessUtility` DISCARD dispatch seam.
///
/// No inward seams: nothing calls into discard across a dependency cycle (its
/// only caller is `ProcessUtility`, unported, which reaches it through
/// `backend-tcop-utility-out-seams::discard_command`). The dispatcher hands us a
/// `&Node` whose payload is the `DiscardStmt` parse node; we downcast to the
/// canonical `ddlnodes::DiscardStmt` and run the command driver.
pub fn init_seams() {
    utility_out_seams::discard_command::set(|stmt, is_top_level| {
        let ds = stmt
            .as_discardstmt()
            .expect("discard_command: parse tree is not a DiscardStmt");
        DiscardCommand(ds, is_top_level)
    });
}

#[cfg(test)]
mod tests;
