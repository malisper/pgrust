//! End-of-(sub)transaction, inplace, and 2PC/recovery processing (inval.c
//! `AtEOXact_Inval`, `AtEOSubXact_Inval`, `CommandEndInvalidationMessages`,
//! the inplace `PreInplace_Inval` / `AtInplace_Inval` / `ForgetInplace_Inval`,
//! `PostPrepare_Inval`, `xactGetCommittedInvalidationMessages`,
//! `inplaceGetInvalidationMessages`, `ProcessCommittedInvalidationMessages`,
//! and `LogLogicalInvalidations`).

use types_core::Oid;
use types_error::PgResult;
use types_storage::SharedInvalidationMessage;

use crate::InvalState;

/// Discard the inplace invalidation info and physically drop its messages from
/// the dense arrays (re-establishing the `nextmsg == len` invariant).
pub(crate) fn forget_inplace_invalidation_state(_state: &mut InvalState<'_>) {
    todo!("forget_inplace_invalidation_state")
}

/// `CommandEndInvalidationMessages` — make the just-completed command's catalog
/// changes visible locally.
pub fn CommandEndInvalidationMessages() -> PgResult<()> {
    todo!("CommandEndInvalidationMessages")
}

/// `AtEOXact_Inval` — process queued invalidation messages at end of main
/// transaction.
pub fn AtEOXact_Inval(_isCommit: bool) -> PgResult<()> {
    todo!("AtEOXact_Inval")
}

/// `AtEOSubXact_Inval` — process queued invalidation messages at subtransaction
/// end.
pub fn AtEOSubXact_Inval(_isCommit: bool) -> PgResult<()> {
    todo!("AtEOSubXact_Inval")
}

/// `PreInplace_Inval`.
pub fn PreInplace_Inval() -> PgResult<()> {
    todo!("PreInplace_Inval")
}

/// `AtInplace_Inval`.
pub fn AtInplace_Inval() -> PgResult<()> {
    todo!("AtInplace_Inval")
}

/// `ForgetInplace_Inval`.
pub fn ForgetInplace_Inval() {
    todo!("ForgetInplace_Inval")
}

/// `PostPrepare_Inval`.
pub fn PostPrepare_Inval() -> PgResult<()> {
    todo!("PostPrepare_Inval")
}

/// `xactGetCommittedInvalidationMessages` — collect all pending messages into a
/// single contiguous array (in `AtEOXact_Inval` processing order) for the
/// commit WAL record; returns the messages and the `RelcacheInitFileInval` flag.
pub fn xactGetCommittedInvalidationMessages() -> PgResult<(Vec<SharedInvalidationMessage>, bool)> {
    todo!("xactGetCommittedInvalidationMessages")
}

/// `inplaceGetInvalidationMessages` — collect the inplace update's pending
/// messages for its WAL record.
pub fn inplaceGetInvalidationMessages() -> PgResult<(Vec<SharedInvalidationMessage>, bool)> {
    todo!("inplaceGetInvalidationMessages")
}

/// `ProcessCommittedInvalidationMessages` — replay invalidation messages during
/// recovery (`xact_redo_commit` / `standby_redo`).
pub fn ProcessCommittedInvalidationMessages(
    _msgs: &[SharedInvalidationMessage],
    _nmsgs: i32,
    _relcache_init_file_inval: bool,
    _dbid: Oid,
    _tsid: Oid,
) -> PgResult<()> {
    todo!("ProcessCommittedInvalidationMessages")
}

/// `LogLogicalInvalidations` — emit WAL for invalidations of the current command.
pub fn LogLogicalInvalidations() -> PgResult<()> {
    todo!("LogLogicalInvalidations")
}
