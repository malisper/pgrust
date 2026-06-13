//! invalidate family — clear/rebuild/flush/invalidate/destroy + EOXact cleanup
//! over the real store (OWN logic).
//!
//! SCAFFOLD: signatures mirror the C surface; bodies are `todo!()`. The
//! swap-contents rebuild protocol (`RelationClearRelation`/`Rebuild`/`Flush`/
//! `Invalidate`/`Destroy`), `RelationForgetRelation`,
//! `RelationCacheInvalidate[Entry]`, and `AtEOXact`/`AtEOSubXact_RelationCache`
//! cleanup all operate on the real `RelationIdCache` + per-backend state.

use backend_utils_error::PgResult;
use types_core::primitive::Oid;
use types_core::xact::SubTransactionId;

use crate::core_entry_store::entry::RelationData;

/// `RelationClearRelation(relation, rebuild)` (relcache.c): drop the entry's
/// derived caches and either rebuild it in place (`rebuild`) or remove it from
/// the cache. The C-stable `Relation` pointer is preserved across the in-place
/// content swap. **Own logic** over the real store.
pub fn RelationClearRelation(_relation: *mut RelationData, _rebuild: bool) -> PgResult<()> {
    todo!("relcache-invalidate: RelationClearRelation swap-contents (own logic)")
}

/// `RelationRebuildRelation(relation)` (relcache.c): rebuild a stale entry in
/// place, swapping the freshly built descriptor's contents into the existing
/// allocation so the `Relation` pointer stays valid. **Own logic.**
pub fn RelationRebuildRelation(_relation: *mut RelationData) -> PgResult<()> {
    todo!("relcache-invalidate: RelationRebuildRelation (own logic)")
}

/// `RelationFlushRelation(relation)` (relcache.c): the SI-inval response —
/// clear or rebuild depending on whether the entry is still referenced.
pub fn RelationFlushRelation(_relation: *mut RelationData) -> PgResult<()> {
    todo!("relcache-invalidate: RelationFlushRelation (own logic)")
}

/// `RelationForgetRelation(rid)` (relcache.c): mark the entry for `rid` dropped
/// (or remove it if unreferenced) on relation drop.
pub fn RelationForgetRelation(_rid: Oid) -> PgResult<()> {
    todo!("relcache-invalidate: RelationForgetRelation (own logic)")
}

/// `RelationCacheInvalidateEntry(relationId)` (relcache.c): mark one entry
/// invalid (the per-relation `SHAREDINVALRELCACHE_ID` arm).
pub fn RelationCacheInvalidateEntry(_relationId: Oid) -> PgResult<()> {
    todo!("relcache-invalidate: RelationCacheInvalidateEntry (own logic)")
}

/// `RelationCacheInvalidate(debug_discard)` (relcache.c): blow away the whole
/// relcache (the reset path), rebuilding nailed entries. **Own logic.**
pub fn RelationCacheInvalidate(_debug_discard: bool) -> PgResult<()> {
    todo!("relcache-invalidate: RelationCacheInvalidate (own logic)")
}

/// `swap_relation_contents` helper (relcache.c `RelationRebuildRelation`): move
/// the rebuilt descriptor's fields into the existing entry while preserving the
/// pinned fields (refcnt, the C-stable pointer). **Own logic.**
pub(crate) fn swap_relation_contents(_old: *mut RelationData, _new: Box<RelationData>) {
    todo!("relcache-invalidate: swap_relation_contents (own logic)")
}

/// `AtEOXact_RelationCache(isCommit)` (relcache.c): end-of-transaction relcache
/// cleanup over the `eoxact_list` / whole store. **Own logic.**
pub fn AtEOXact_RelationCache(_isCommit: bool) -> PgResult<()> {
    todo!("relcache-invalidate: AtEOXact_RelationCache (own logic)")
}

/// `AtEOSubXact_RelationCache(isCommit, mySubid, parentSubid)` (relcache.c):
/// end-of-subtransaction relcache cleanup. **Own logic.**
pub fn AtEOSubXact_RelationCache(
    _isCommit: bool,
    _mySubid: SubTransactionId,
    _parentSubid: SubTransactionId,
) -> PgResult<()> {
    todo!("relcache-invalidate: AtEOSubXact_RelationCache (own logic)")
}
