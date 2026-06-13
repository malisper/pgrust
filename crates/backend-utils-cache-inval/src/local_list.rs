//! Local-list processing (inval.c `LocalExecuteInvalidationMessage`,
//! `AcceptInvalidationMessages`, `InvalidateSystemCaches[Extended]`, and the
//! `ProcessInvalidationMessages[Multi]` public collectors that snapshot a
//! group's messages out before the seam may re-enter).

use mcx::Mcx;
use types_error::PgResult;
use types_storage::SharedInvalidationMessage;

use crate::msgs::{InvalMessageArray, InvalidationMsgsGroup};

/// The leading `id` discriminant common to every SI message variant.
pub(crate) fn msg_id(_msg: &SharedInvalidationMessage) -> i8 {
    todo!("msg_id: discriminant of the SharedInvalidationMessage variant")
}

/// `LocalExecuteInvalidationMessage` — process one inbound SI message, flushing
/// only the local caches (the big id-dispatch switch).
pub fn LocalExecuteInvalidationMessage(_msg: &SharedInvalidationMessage) -> PgResult<()> {
    todo!("LocalExecuteInvalidationMessage")
}

/// `InvalidateSystemCachesExtended`.
pub fn InvalidateSystemCachesExtended(_debug_discard: bool) -> PgResult<()> {
    todo!("InvalidateSystemCachesExtended")
}

/// `InvalidateSystemCaches`.
pub fn InvalidateSystemCaches() -> PgResult<()> {
    todo!("InvalidateSystemCaches")
}

/// `AcceptInvalidationMessages` — read and process the shared invalidation
/// message queue (then the `debug_discard_caches` recursion guard).
pub fn AcceptInvalidationMessages() -> PgResult<()> {
    todo!("AcceptInvalidationMessages")
}

/// `ProcessInvalidationMessages(group, func)` (public) — run `func` for every
/// message in `group`, catcache entries first.
pub fn ProcessInvalidationMessages(
    _group: &InvalidationMsgsGroup,
    _func: &mut dyn FnMut(&SharedInvalidationMessage) -> PgResult<()>,
) -> PgResult<()> {
    todo!("ProcessInvalidationMessages (public)")
}

/// Snapshot a group's messages (catcache subgroup first) into a plain `Vec`
/// before releasing the state borrow and calling a seam.
pub(crate) fn collect_group_messages<'mcx>(
    _mcx: Mcx<'mcx>,
    _arrays: &[InvalMessageArray<'mcx>; 2],
    _group: &InvalidationMsgsGroup,
) -> PgResult<Vec<SharedInvalidationMessage>> {
    todo!("collect_group_messages")
}

/// Snapshot a group's messages as one batch per non-empty subgroup.
pub(crate) fn collect_group_messages_multi<'mcx>(
    _mcx: Mcx<'mcx>,
    _arrays: &[InvalMessageArray<'mcx>; 2],
    _group: &InvalidationMsgsGroup,
) -> PgResult<Vec<Vec<SharedInvalidationMessage>>> {
    todo!("collect_group_messages_multi")
}
