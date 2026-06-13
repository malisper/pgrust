//! derived family — the per-relation derived caches built over the real store
//! (OWN logic).
//!
//! SCAFFOLD: signatures mirror the C surface; bodies are `todo!()`. The
//! derived-list builders (`RelationGetFKeyList`/`IndexList`/`StatExtList`/
//! `PrimaryKeyIndex`/`ReplicaIndex`/`IndexExpressions`/`IndexPredicate`/
//! `IndexAttrBitmap`/`IdentityKeyBitmap`/`ExclusionInfo`,
//! `RelationBuildPublicationDesc`, `RelationBuildRuleLock`) are relcache's OWN
//! logic over the real entry's `rd_indexlist`/`rd_*attr`/… fields. Only the
//! catalog scans and unported node/rewrite vocabulary are seamed.

use backend_utils_error::PgResult;
use types_core::primitive::Oid;

use crate::core_entry_store::entry::RelationData;

/// `IndexAttrBitmapKind` (relcache.h) — which attribute-bitmap to fetch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexAttrBitmapKind {
    Keys,
    PrimaryKey,
    Identity,
    HotBlocking,
    Summarized,
}

/// `RelationGetFKeyList(relation)` (relcache.c): the relation's foreign-key
/// cache-info list, built from `pg_constraint` and cached in `rd_fkeylist`.
pub fn RelationGetFKeyList(_relation: *mut RelationData) -> PgResult<()> {
    todo!("relcache-derived: RelationGetFKeyList (own logic; FK node vocab seamed)")
}

/// `RelationGetIndexList(relation)` (relcache.c): the OIDs of the relation's
/// indexes, built from `pg_index` and cached in `rd_indexlist` (+ `rd_pkindex`/
/// `rd_replidindex`). **Own logic.**
pub fn RelationGetIndexList(_relation: *mut RelationData) -> PgResult<Vec<Oid>> {
    todo!("relcache-derived: RelationGetIndexList (own logic)")
}

/// `RelationGetStatExtList(relation)` (relcache.c): the OIDs of the relation's
/// extended-statistics objects, cached in `rd_statlist`. **Own logic.**
pub fn RelationGetStatExtList(_relation: *mut RelationData) -> PgResult<Vec<Oid>> {
    todo!("relcache-derived: RelationGetStatExtList (own logic)")
}

/// `RelationGetPrimaryKeyIndex(relation, deferrable_ok)` (relcache.c): the
/// primary-key index OID (forces `RelationGetIndexList` first).
pub fn RelationGetPrimaryKeyIndex(
    _relation: *mut RelationData,
    _deferrable_ok: bool,
) -> PgResult<Oid> {
    todo!("relcache-derived: RelationGetPrimaryKeyIndex (own logic)")
}

/// `RelationGetReplicaIndex(relation)` (relcache.c): the replica-identity
/// index OID.
pub fn RelationGetReplicaIndex(_relation: *mut RelationData) -> PgResult<Oid> {
    todo!("relcache-derived: RelationGetReplicaIndex (own logic)")
}

/// `RelationGetIndexExpressions(relation)` (relcache.c): the index's expression
/// trees (node vocabulary — seamed for the tree, own caching).
pub fn RelationGetIndexExpressions(_relation: *mut RelationData) -> PgResult<()> {
    todo!("relcache-derived: RelationGetIndexExpressions (node vocab seamed)")
}

/// `RelationGetIndexPredicate(relation)` (relcache.c): the index's partial
/// predicate tree (node vocabulary — seamed for the tree, own caching).
pub fn RelationGetIndexPredicate(_relation: *mut RelationData) -> PgResult<()> {
    todo!("relcache-derived: RelationGetIndexPredicate (node vocab seamed)")
}

/// `RelationGetIndexAttrBitmap(relation, attrKind)` (relcache.c): the requested
/// attribute bitmap, built (and cached on the entry) from the index list.
/// Returns the offset members. **Own logic.**
pub fn RelationGetIndexAttrBitmap(
    _relation: *mut RelationData,
    _attrKind: IndexAttrBitmapKind,
) -> PgResult<Vec<i32>> {
    todo!("relcache-derived: RelationGetIndexAttrBitmap (own logic)")
}

/// `RelationGetIdentityKeyBitmap(relation)` (relcache.c): the replica-identity
/// index key columns as offset members, or `None` when there is no identity
/// index. **Own logic** (opens the identity index via seam).
pub fn RelationGetIdentityKeyBitmap(_relation: *mut RelationData) -> PgResult<Option<Vec<i32>>> {
    todo!("relcache-derived: RelationGetIdentityKeyBitmap (own logic)")
}

/// `RelationGetExclusionInfo(indexRelation, ...)` (relcache.c): the exclusion
/// operator/proc/strategy arrays for an exclusion-constraint index.
pub fn RelationGetExclusionInfo(_indexRelation: *mut RelationData) -> PgResult<()> {
    todo!("relcache-derived: RelationGetExclusionInfo (own logic)")
}

/// `RelationBuildPublicationDesc(relation)` (relcache.c): build `rd_pubdesc`
/// from `pg_publication*` (publication vocabulary — seamed where unported).
pub fn RelationBuildPublicationDesc(_relation: *mut RelationData) -> PgResult<()> {
    todo!("relcache-derived: RelationBuildPublicationDesc (publication vocab seamed)")
}

/// `RelationBuildRuleLock(relation)` (relcache.c): build `rd_rules` from
/// `pg_rewrite` (rewrite/node vocabulary — seamed where unported).
pub fn RelationBuildRuleLock(_relation: *mut RelationData) -> PgResult<()> {
    todo!("relcache-derived: RelationBuildRuleLock (rewrite vocab seamed)")
}
