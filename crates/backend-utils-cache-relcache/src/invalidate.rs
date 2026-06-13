//! invalidate family — clear/rebuild/flush/invalidate/destroy + EOXact cleanup
//! over the real store (OWN logic).
//!
//! These bodies are the faithful PostgreSQL 18.3 `relcache.c` logic over the
//! REAL [`RelationIdCache`](crate::core_entry_store) + the per-backend
//! `eoxact_list`/`in_progress_list` state. The swap-contents rebuild protocol
//! (`RelationInvalidateRelation`/`RelationClearRelation`/`RelationRebuildRelation`/
//! `RelationFlushRelation`/`RelationDestroyRelation`), `RelationForgetRelation`,
//! `RelationCacheInvalidate[Entry]`, and `AtEOXact`/`AtEOSubXact_RelationCache`
//! all operate on the owned descriptors directly — own logic, nothing deferred.
//!
//! Only genuine cross-unit primitives are routed through owner seams (the
//! [`xunit`] shims below, panic until their owner lands), exactly as C calls
//! out to other modules: `RelationMapInvalidateAll` (relmapper), `smgrreleaseall`
//! (smgr), `IsTransactionState`/`GetCurrentSubTransactionId` (xact),
//! `HistoricSnapshotActive` (snapmgr), `IsBootstrapProcessingMode` (miscinit).
//! `RelationCloseSmgr`/`RelationInitPhysicalAddr`/`RelationReloadIndexInfo`/
//! `RelationReloadNailed` are relcache's own logic, owned by the index family
//! ([`crate::index`]); the build family owns `RelationBuildDesc`.

use backend_utils_error::{elog, PgResult};
use types_core::xact::{InvalidSubTransactionId, SubTransactionId};
use types_core::primitive::Oid;
use types_error::WARNING;

use crate::core_entry_store::{
    self, cache_delete, cache_find_reldesc, cache_lookup, cache_seq_reldescs, with_state,
    RelationDecrementReferenceCount, RelationIncrementReferenceCount,
};
use crate::core_entry_store::entry::RelationData;
use crate::relation_get_relid;

/// `RELKIND_INDEX` / `RELKIND_PARTITIONED_INDEX` (pg_class.h) as `i8` (the
/// owned `relkind` field's type). The byte values match `types_tuple::access`.
const RELKIND_INDEX: i8 = b'i' as i8;
const RELKIND_PARTITIONED_INDEX: i8 = b'I' as i8;
const RELKIND_RELATION: i8 = b'r' as i8;

/* ==========================================================================
 * Genuine cross-unit primitives. relcache.c calls these out to other backend
 * modules; here they route through their owner's seam. Until each owner lands
 * the shim panics (the documented seam-and-panic boundary — "mirror PG and
 * panic"). They are NOT own logic and are NOT stubbed silently.
 * ======================================================================== */
mod xunit {
    /// `RelationMapInvalidateAll()` (relmapper.c) — reload the relation map.
    pub(super) fn relation_map_invalidate_all() {
        todo!("relcache-invalidate xunit seam: RelationMapInvalidateAll (relmapper owner)")
    }

    /// `smgrreleaseall()` (smgr.c) — close all open relation files.
    pub(super) fn smgrreleaseall() {
        todo!("relcache-invalidate xunit seam: smgrreleaseall (smgr owner)")
    }

    /// `RelationCloseSmgr(relation)` (relcache.h inline) — close the relation's
    /// smgr handle. The smgr layer is a genuine cross-unit owner; the entry
    /// holds no `rd_smgr` field in the owned mirror, so this is the smgr-close
    /// side effect routed to its owner.
    pub(super) fn relation_close_smgr(_relation: *mut super::RelationData) {
        todo!("relcache-invalidate xunit seam: RelationCloseSmgr (smgr owner)")
    }

    /// `IsTransactionState()` (xact.c) — true when in a live transaction.
    pub(super) fn is_transaction_state() -> bool {
        todo!("relcache-invalidate xunit seam: IsTransactionState (xact owner)")
    }

    /// `HistoricSnapshotActive()` (snapmgr.c) — true during logical decoding.
    pub(super) fn historic_snapshot_active() -> bool {
        todo!("relcache-invalidate xunit seam: HistoricSnapshotActive (snapmgr owner)")
    }

    /// `GetCurrentSubTransactionId()` (xact.c).
    pub(super) fn get_current_sub_transaction_id() -> types_core::xact::SubTransactionId {
        todo!("relcache-invalidate xunit seam: GetCurrentSubTransactionId (xact owner)")
    }
}

/* ==========================================================================
 * RelationInvalidateRelation / RelationClearRelation / RelationDestroyRelation
 * ======================================================================== */

/// `RelationInvalidateRelation(relation)` (relcache.c): mark an entry invalid so
/// it's reloaded on next access. Closes smgr (so the next access reopens the
/// files), drops AM cached data, and clears `rd_isvalid`. **Own logic.**
#[allow(unsafe_code)]
pub(crate) fn RelationInvalidateRelation(relation: *mut RelationData) -> PgResult<()> {
    // Make sure smgr and lower levels close the relation's files, if they
    // weren't closed already. (smgr is a cross-unit owner.)
    xunit::relation_close_smgr(relation);

    // SAFETY: live `Relation` pointer into a cache-owned (or in-build)
    // descriptor.
    let rd = unsafe { &mut *relation };

    // Free AM cached data, if any. The owned entry models the AM cache as the
    // resolved `rd_amcache`-equivalent state on the descriptor; clearing it is
    // part of the index family's AM-init lifecycle, so there is no separate
    // heap block to free here (the C `pfree(rd_amcache)` is subsumed by the
    // owned descriptor). Nothing to do beyond marking invalid.

    rd.rd_isvalid = false;
    Ok(())
}

/// `RelationClearRelation(relation)` (relcache.c): physically blow away a
/// relation cache entry. Caller must ensure refcount is zero and the rel/its
/// storage was not created in the current transaction. **Own logic** over the
/// real store. (In PG 18.3 the `rebuild` parameter was removed; rebuild is now
/// the separate [`RelationRebuildRelation`].)
#[allow(unsafe_code)]
pub fn RelationClearRelation(relation: *mut RelationData) -> PgResult<()> {
    // SAFETY: live `Relation` pointer.
    let rd = unsafe { &*relation };
    debug_assert!(rd.rd_refcnt == 0, "RelationHasReferenceCountZero");
    debug_assert!(!rd.rd_isnailed);
    // Relations created in the same transaction must never be removed (see
    // RelationFlushRelation).
    debug_assert!(rd.rd_createSubid == InvalidSubTransactionId);
    debug_assert!(rd.rd_firstRelfilelocatorSubid == InvalidSubTransactionId);
    debug_assert!(rd.rd_droppedSubid == InvalidSubTransactionId);
    let relid = rd.rd_id;

    // first mark it as invalid
    RelationInvalidateRelation(relation)?;

    // Remove it from the hash table AND release storage. In C this is
    // RelationCacheDelete(relation) followed by RelationDestroyRelation(...,
    // false); here `cache_delete` unhooks the entry and reclaims the owned
    // `Box<RelationData>` in one step — the single `Box` drop frees the whole
    // owned subsidiary tree (rd_rel/rd_att/the derived lists/bitmaps), which is
    // exactly the C `RelationDestroyRelation` `pfree` cascade.
    cache_delete(relid)
}

/// `RelationDestroyRelation(relation, remember_tupdesc)` (relcache.c):
/// physically delete a relation cache entry and all subsidiary data, for an
/// entry the caller has *already unhooked* from the hash table (the
/// swap-path `newrel`). For the owned model this is a single `Box` drop: the
/// owned descriptor carries all its subsidiary data inline, so reclaiming the
/// `Box` frees the whole tree exactly once. `remember_tupdesc` is the C hack to
/// defer freeing a still-shared `TupleDesc` to end-of-transaction; the owned
/// `rd_att` is not reference-counted or shared, so it is freed with the `Box`.
/// **Own logic.**
#[allow(unsafe_code)]
pub(crate) fn RelationDestroyRelation(newrel: Box<RelationData>, _remember_tupdesc: bool) {
    debug_assert!(newrel.rd_refcnt == 0, "RelationHasReferenceCountZero");
    // The `Box` drop frees rd_rel, rd_att, rd_fkeylist, rd_indexlist,
    // rd_statlist, the attr bitmaps, rd_pubdesc, rd_options, rd_indextuple,
    // rd_amcache, and the per-entry contexts — the C `pfree` cascade.
    drop(newrel);
}

/* ==========================================================================
 * RelationRebuildRelation — in-place rebuild with content swap.
 * ======================================================================== */

/// `RelationRebuildRelation(relation)` (relcache.c): rebuild a stale, still-open
/// entry (refcount > 0) in place, swapping a freshly built descriptor's
/// contents into the existing allocation so the C-stable `Relation` pointer
/// stays valid. **Own logic.**
#[allow(unsafe_code)]
pub fn RelationRebuildRelation(relation: *mut RelationData) -> PgResult<()> {
    // SAFETY: live `Relation` pointer; the caller holds a positive refcount.
    {
        let rd = unsafe { &*relation };
        debug_assert!(rd.rd_refcnt != 0, "!RelationHasReferenceCountZero");
        // there is no reason to ever rebuild a dropped relation
        debug_assert!(rd.rd_droppedSubid == InvalidSubTransactionId);
    }

    // Close and mark it as invalid until we've finished the rebuild.
    RelationInvalidateRelation(relation)?;

    // SAFETY: live `Relation` pointer.
    let rd = unsafe { &*relation };
    let relkind = rd.rd_rel.relkind;
    let is_index =
        relkind == RELKIND_INDEX || relkind == RELKIND_PARTITIONED_INDEX;
    // The C condition is `rd_indexcxt != NULL`, i.e. the index access info has
    // been initialized. In the owned model that state is exactly `rd_indam`
    // being set (the resolved index-AM routine), so use it as the proxy.
    let index_info_initialized = rd.rd_indam.is_some();
    let is_nailed = rd.rd_isnailed;

    // Indexes only have a limited number of possible schema changes; use the
    // light reload unless the index access info hasn't been initialized yet
    // (index creation relies on the full procedure in that window).
    if is_index && index_info_initialized {
        return crate::index::RelationReloadIndexInfo(relation);
    }
    // Nailed relations are handled separately.
    if is_nailed {
        return crate::index::RelationReloadNailed(relation);
    }

    // Build a new entry from scratch, swap its contents with the old entry,
    // and finally destroy the new entry. This avoids trouble if an error
    // occurs partway through: the old entry stays !rd_isvalid and is no less
    // valid than before.
    let save_relid = relation_get_relid(rd);

    // Build temporary entry, but don't link it into the hashtable.
    let newrel_ptr = crate::build::RelationBuildDesc(save_relid, false)?;

    // Between here and the end of the swap, don't do anything that could read
    // system catalogs (it must be free from invalidation processing).

    if newrel_ptr.is_null() {
        // We can validly get here if using a historic snapshot in which the
        // relation is still invisible; just leave it invalid and return.
        if xunit::historic_snapshot_active() {
            return Ok(());
        }
        // Otherwise this shouldn't happen: dropping a still-referenced relation
        // is supposed to be impossible.
        return elog_relation_deleted_in_use(save_relid);
    }

    // SAFETY: `RelationBuildDesc(_, false)` returns a freshly leaked
    // `Box<RelationData>` that is not linked into the cache; reclaim it so we
    // own it for the swap and final destroy.
    let mut newrel: Box<RelationData> = unsafe { Box::from_raw(newrel_ptr) };

    // SAFETY: live `Relation` pointer; mutated in place for the swap.
    let old = unsafe { &mut *relation };

    // If we were to have cases of the relkind changing, pgstats would get
    // confused.
    debug_assert!(old.rd_rel.relkind == newrel.rd_rel.relkind);

    // keep_tupdesc/keep_rules/keep_policies decide whether to preserve the old
    // substructures in place (various places assume they won't move). The
    // node/rewrite-vocabulary payloads (rules, policies) are presence-only in
    // the owned mirror; preserve based on the structural comparisons we can do.
    let keep_tupdesc = equal_tuple_descs(&old.rd_att, &newrel.rd_att);
    let keep_rules = old.rd_has_rules == newrel.rd_has_rules;
    let keep_policies = old.rd_has_rsdesc == newrel.rd_has_rsdesc;
    // partkey is immutable once set up, so we can always keep it.
    let keep_partkey = old.rd_has_partkey;

    // Perform the swap. C swaps the whole structs then re-swaps the few fields
    // that must be preserved. We do the equivalent: snapshot the preserved
    // fields from `old`, move all of `newrel`'s contents into `old`, then
    // restore the preserved fields. There is no CHECK_FOR_INTERRUPTS in this
    // sequence (it's straight-line code), matching the C requirement.

    // Capture fields that must be preserved (taken from the *old* entry).
    let save_refcnt = old.rd_refcnt;
    let save_isnailed = old.rd_isnailed;
    let save_createSubid = old.rd_createSubid;
    let save_newRelfilelocatorSubid = old.rd_newRelfilelocatorSubid;
    let save_firstRelfilelocatorSubid = old.rd_firstRelfilelocatorSubid;
    let save_droppedSubid = old.rd_droppedSubid;
    let save_toastoid = old.rd_toastoid;
    let save_pgstat_enabled = old.pgstat_enabled;

    // Preserve old substructures by moving them out of `old` first (so the
    // wholesale content move below doesn't clobber them); we move them back
    // after, conditionally.
    let old_att = std::mem::take(&mut old.rd_att);
    let old_has_rules = old.rd_has_rules;
    let old_has_rsdesc = old.rd_has_rsdesc;
    let old_has_partkey = old.rd_has_partkey;
    let old_has_partdesc = old.rd_has_partdesc;

    // isnailed shouldn't change.
    debug_assert!(newrel.rd_isnailed == save_isnailed);

    // Wholesale content move: replace `old`'s contents with `newrel`'s. We then
    // restore the preserved fields. `newrel` is left holding what *was* in
    // `old` for those fields we explicitly swap back (so its `Box` drop frees
    // the discarded contents — the C `RelationDestroyRelation(newrel, ...)`).
    let discarded = std::mem::replace(old, *newrel);
    // `discarded` now holds the old contents (minus `old_att`, taken above).
    // Stash it back into `newrel` so the final destroy frees it.
    newrel = Box::new(discarded);

    // Restore preserved scalar fields onto the rebuilt entry.
    old.rd_refcnt = save_refcnt;
    old.rd_createSubid = save_createSubid;
    old.rd_newRelfilelocatorSubid = save_newRelfilelocatorSubid;
    old.rd_firstRelfilelocatorSubid = save_firstRelfilelocatorSubid;
    old.rd_droppedSubid = save_droppedSubid;
    old.rd_toastoid = save_toastoid;
    old.pgstat_enabled = save_pgstat_enabled;
    // rd_rel contents are always copied from newrel (C: SWAPFIELD then memcpy
    // the CLASS_TUPLE_SIZE back); after the wholesale move `old.rd_rel` already
    // holds the freshly built pg_class form, which is the intended result.

    // preserve old tupledesc/rules/policies if no logical change.
    if keep_tupdesc {
        // Put the preserved old descriptor back; the freshly built one (now in
        // `old.rd_att` after the move) is discarded with `newrel`.
        let freshly_built_att = std::mem::replace(&mut old.rd_att, old_att);
        newrel.rd_att = freshly_built_att;
    } else {
        // Keep the freshly built `rd_att`; `old_att` is dropped here (the C
        // `RelationDestroyRelation(newrel, !keep_tupdesc)` frees the old one).
        drop(old_att);
    }
    if keep_rules {
        old.rd_has_rules = old_has_rules;
    }
    if keep_policies {
        old.rd_has_rsdesc = old_has_rsdesc;
    }
    // preserve old partition key/descriptor presence if we have one.
    if keep_partkey {
        old.rd_has_partkey = old_has_partkey;
        old.rd_has_partdesc = old_has_partdesc;
    }

    // And now throw away the temporary entry (the C
    // `RelationDestroyRelation(newrel, !keep_tupdesc)`).
    RelationDestroyRelation(newrel, !keep_tupdesc);
    Ok(())
}

/// `equalTupleDescs(d1, d2)` over the owned descriptors — the rebuild's
/// keep-tupdesc decision. Compares the structural fields the rebuild cares
/// about (column shape, type identity); matches the C `equalTupleDescs`
/// coverage that's representable on the owned mirror.
fn equal_tuple_descs(
    d1: &crate::core_entry_store::entry::OwnedTupleDesc,
    d2: &crate::core_entry_store::entry::OwnedTupleDesc,
) -> bool {
    if d1.natts != d2.natts || d1.tdtypeid != d2.tdtypeid || d1.tdtypmod != d2.tdtypmod {
        return false;
    }
    if d1.attrs.len() != d2.attrs.len() {
        return false;
    }
    for (a1, a2) in d1.attrs.iter().zip(d2.attrs.iter()) {
        if a1.attname != a2.attname
            || a1.atttypid != a2.atttypid
            || a1.attlen != a2.attlen
            || a1.attnum != a2.attnum
            || a1.atttypmod != a2.atttypmod
            || a1.attbyval != a2.attbyval
            || a1.attalign != a2.attalign
            || a1.attnotnull != a2.attnotnull
            || a1.attisdropped != a2.attisdropped
            || a1.attcollation != a2.attcollation
        {
            return false;
        }
    }
    true
}

/// `RelationIsMapped(relation)` (utils/rel.h) over the owned entry:
/// `RELKIND_HAS_STORAGE(relkind) && relfilenode == InvalidRelFileNumber`.
/// **Own logic.**
fn relation_is_mapped(rd: &RelationData) -> bool {
    let relkind = rd.rd_rel.relkind;
    let has_storage = relkind == RELKIND_RELATION
        || relkind == RELKIND_INDEX
        || relkind == (b's' as i8) // RELKIND_SEQUENCE
        || relkind == (b't' as i8) // RELKIND_TOASTVALUE
        || relkind == (b'm' as i8); // RELKIND_MATVIEW
    has_storage && rd.rd_rel.relfilenode == types_core::primitive::InvalidRelFileNumber
}

/// `elog(ERROR, "relation %u deleted while still in use", relid)`.
fn elog_relation_deleted_in_use(relid: Oid) -> PgResult<()> {
    Err(backend_utils_error::ereport(types_error::ERROR)
        .errmsg_internal(format!("relation {relid} deleted while still in use"))
        .into_error())
}

/* ==========================================================================
 * RelationFlushRelation — SI-inval response.
 * ======================================================================== */

/// `RelationFlushRelation(relation)` (relcache.c): rebuild the relation if it is
/// open (refcount > 0), else blow it away. Used on a cache invalidation event.
/// **Own logic.**
#[allow(unsafe_code)]
pub fn RelationFlushRelation(relation: *mut RelationData) -> PgResult<()> {
    // SAFETY: live `Relation` pointer.
    let rd = unsafe { &*relation };
    let created_in_xact = rd.rd_createSubid != InvalidSubTransactionId
        || rd.rd_firstRelfilelocatorSubid != InvalidSubTransactionId;
    let dropped = rd.rd_droppedSubid != InvalidSubTransactionId;
    let refcnt_zero = rd.rd_refcnt == 0;
    let is_nailed = rd.rd_isnailed;
    let refcnt_one = rd.rd_refcnt == 1;

    if created_in_xact {
        // New relcache entries are always rebuilt, not flushed; else we'd
        // forget the "new" status of the relation. Ditto for new-relfilenumber.
        if xunit::is_transaction_state() && !dropped {
            // The rel could have zero refcnt here, so temporarily increment the
            // refcnt to ensure it's safe to rebuild it.
            RelationIncrementReferenceCount(relation)?;
            RelationRebuildRelation(relation)?;
            RelationDecrementReferenceCount(relation)?;
        } else {
            RelationInvalidateRelation(relation)?;
        }
    } else {
        // Pre-existing rels can be dropped from the relcache if not open.
        if refcnt_zero {
            RelationClearRelation(relation)?;
        } else if !xunit::is_transaction_state() {
            // Can't do catalog access to rebuild; mark invalid for next open.
            RelationInvalidateRelation(relation)?;
        } else if is_nailed && refcnt_one {
            // A nailed relation with refcnt == 1 is unused; can't clear it, and
            // no need to rebuild immediately.
            RelationInvalidateRelation(relation)?;
        } else {
            RelationRebuildRelation(relation)?;
        }
    }
    Ok(())
}

/* ==========================================================================
 * RelationForgetRelation — relation drop.
 * ======================================================================== */

/// `RelationForgetRelation(rid)` (relcache.c): caller reports it dropped the
/// relation. **Own logic.**
#[allow(unsafe_code)]
pub fn RelationForgetRelation(rid: Oid) -> PgResult<()> {
    let relation = match cache_lookup(rid) {
        Some(r) => r,
        None => return Ok(()), // not in cache, nothing to do
    };

    // SAFETY: live cache-owned descriptor.
    let rd = unsafe { &mut *relation };

    if rd.rd_refcnt != 0 {
        return Err(backend_utils_error::ereport(types_error::ERROR)
            .errmsg_internal(format!("relation {rid} is still open"))
            .into_error());
    }

    debug_assert!(rd.rd_droppedSubid == InvalidSubTransactionId);
    if rd.rd_createSubid != InvalidSubTransactionId
        || rd.rd_firstRelfilelocatorSubid != InvalidSubTransactionId
    {
        // In the event of subtransaction rollback, we must not forget
        // rd_*Subid. Mark the entry "dropped" and invalidate it, instead of
        // destroying it right away.
        rd.rd_droppedSubid = xunit::get_current_sub_transaction_id();
        RelationInvalidateRelation(relation)
    } else {
        RelationClearRelation(relation)
    }
}

/* ==========================================================================
 * RelationCacheInvalidateEntry / RelationCacheInvalidate — SI cache flush.
 * ======================================================================== */

/// `RelationCacheInvalidateEntry(relationId)` (relcache.c): invoked for SI cache
/// flush messages — flush any relcache entry matching `relationId`, or mark
/// matching in-progress builds invalidated. **Own logic.**
pub fn RelationCacheInvalidateEntry(relationId: Oid) -> PgResult<()> {
    if let Some(relation) = cache_lookup(relationId) {
        with_state(|st| st.relcache_invals_received += 1);
        RelationFlushRelation(relation)
    } else {
        with_state(|st| {
            for ent in st.in_progress_list.iter_mut() {
                if ent.reloid == relationId {
                    ent.invalidated = true;
                }
            }
        });
        Ok(())
    }
}

/// `RelationCacheInvalidate(debug_discard)` (relcache.c): blow away cached
/// descriptors with zero refcounts and rebuild those with positive refcounts;
/// also reset relation-mapping data and the smgr cache. The SI-overflow reset
/// path. Two phases for `hash_seq_search` safety. **Own logic.**
#[allow(unsafe_code)]
pub fn RelationCacheInvalidate(debug_discard: bool) -> PgResult<()> {
    // Reload relation mapping data before reconstructing the cache.
    xunit::relation_map_invalidate_all();

    // Phase 1: walk the cache, deleting deletable items and collecting the
    // rebuildable ones. We snapshot the descriptor pointers first (own
    // `cache_seq_reldescs`), which is equivalent to — and safer than — C's
    // delete-during-`hash_seq_search`, since `hash_seq_search` only copes with
    // deletion of the element currently being visited.
    let mut rebuild_first_list: Vec<*mut RelationData> = Vec::new();
    let mut rebuild_list: Vec<*mut RelationData> = Vec::new();

    for relation in cache_seq_reldescs() {
        // SAFETY: live cache-owned descriptor.
        let rd = unsafe { &*relation };

        // Ignore new relations; no other backend will manipulate them before
        // we commit. Likewise new-relfilelocator relations.
        if rd.rd_createSubid != InvalidSubTransactionId
            || rd.rd_firstRelfilelocatorSubid != InvalidSubTransactionId
        {
            continue;
        }

        with_state(|st| st.relcache_invals_received += 1);

        if rd.rd_refcnt == 0 {
            // Delete this entry immediately.
            RelationClearRelation(relation)?;
        } else {
            // If it's a mapped relation, immediately update its rd_locator in
            // case its relfilenumber changed (must happen in phase 1 in case
            // the relation is consulted during phase-2 rebuilds).
            if relation_is_mapped(rd) {
                xunit::relation_close_smgr(relation);
                crate::index::RelationInitPhysicalAddr(relation)?;
            }

            // Order: pg_class to the front of rebuildFirstList, pg_class_oid_index
            // to its back; other nailed rels to the front of rebuildList,
            // everything else to the back.
            let relid = relation_get_relid(rd);
            if relid == types_catalog::catalog::RELATION_RELATION_ID {
                rebuild_first_list.insert(0, relation);
            } else if relid == types_catalog::catalog::CLASS_OID_INDEX_ID {
                rebuild_first_list.push(relation);
            } else if rd.rd_isnailed {
                rebuild_list.insert(0, relation);
            } else {
                rebuild_list.push(relation);
            }
        }
    }

    // We cannot destroy the SMgrRelations (still referenced) but close their FDs.
    xunit::smgrreleaseall();

    // Phase 2: rebuild (or invalidate) the items found in phase 1.
    let in_xact = xunit::is_transaction_state();
    for relation in rebuild_first_list.into_iter().chain(rebuild_list.into_iter()) {
        // SAFETY: live cache-owned descriptor (held by positive refcount).
        let rd = unsafe { &*relation };
        if !in_xact || (rd.rd_isnailed && rd.rd_refcnt == 1) {
            RelationInvalidateRelation(relation)?;
        } else {
            RelationRebuildRelation(relation)?;
        }
    }

    if !debug_discard {
        // Any RelationBuildDesc() on the stack must start over.
        with_state(|st| {
            for ent in st.in_progress_list.iter_mut() {
                ent.invalidated = true;
            }
        });
    }
    Ok(())
}

/* ==========================================================================
 * AtEOXact_RelationCache / AtEOXact_cleanup — end-of-transaction cleanup.
 * ======================================================================== */

/// `AtEOXact_RelationCache(isCommit)` (relcache.c): clean up the relcache at
/// main-transaction commit or abort. Must run *before* processing invalidation
/// messages. **Own logic.**
pub fn AtEOXact_RelationCache(isCommit: bool) -> PgResult<()> {
    // Forget in_progress_list (relevant when aborting due to an error during
    // RelationBuildDesc()).
    with_state(|st| {
        debug_assert!(st.in_progress_list.is_empty() || !isCommit);
        st.in_progress_list.clear();
    });

    // Unless eoxact_list[] overflowed, we only need to examine the rels listed
    // in it; otherwise fall back on a whole-cache scan.
    let (overflowed, list) =
        with_state(|st| (st.eoxact_list_overflowed, st.eoxact_list.clone()));

    if overflowed {
        for relation in cache_seq_reldescs() {
            AtEOXact_cleanup(relation, isCommit)?;
        }
    } else {
        for relid in list {
            // List entries may not be found in the hashtable; that's fine.
            if let Some(relation) = cache_find_reldesc(relid) {
                AtEOXact_cleanup(relation, isCommit)?;
            }
        }
    }

    // The owned model has no separate EOXactTupleDescArray: rd_att is owned
    // (not reference-counted/shared), so it is freed when its entry's `Box` is
    // dropped. The C deferred-free array exists only to manage shared TupleDesc
    // refcounts, which the owned mirror does not have.

    // Now we're out of the transaction and can clear the lists.
    with_state(|st| core_entry_store::eoxact_list_reset(st));
    Ok(())
}

/// `AtEOXact_cleanup(relation, isCommit)` (relcache.c): clean up a single rel at
/// main-transaction commit or abort. Idempotent (eoxact_list may hold dups).
/// **Own logic.**
#[allow(unsafe_code)]
fn AtEOXact_cleanup(relation: *mut RelationData, isCommit: bool) -> PgResult<()> {
    // SAFETY: live cache-owned descriptor.
    let rd = unsafe { &mut *relation };

    // Is the relation live after this transaction ends? During commit, clear
    // the entry if it is preserved after relation drop; during rollback, clear
    // it if created in the current transaction.
    let clear_relcache = if isCommit {
        rd.rd_droppedSubid != InvalidSubTransactionId
    } else {
        rd.rd_createSubid != InvalidSubTransactionId
    };

    // Reset the subids to zero now that we're out of the transaction (also lets
    // RelationClearRelation drop the entry).
    rd.rd_createSubid = InvalidSubTransactionId;
    rd.rd_newRelfilelocatorSubid = InvalidSubTransactionId;
    rd.rd_firstRelfilelocatorSubid = InvalidSubTransactionId;
    rd.rd_droppedSubid = InvalidSubTransactionId;

    if clear_relcache {
        if rd.rd_refcnt == 0 {
            RelationClearRelation(relation)?;
            return Ok(());
        }
        // Hmm, there's a (leaked?) reference. Don't remove the entry; bleat and
        // leave it. Just a WARNING to avoid error-during-error-recovery loops.
        elog(
            WARNING,
            format!(
                "cannot remove relcache entry for \"{}\" because it has nonzero refcount",
                rd.rd_rel.relname
            ),
        )?;
    }
    Ok(())
}

/* ==========================================================================
 * AtEOSubXact_RelationCache / AtEOSubXact_cleanup — end-of-subxact cleanup.
 * ======================================================================== */

/// `AtEOSubXact_RelationCache(isCommit, mySubid, parentSubid)` (relcache.c):
/// clean up the relcache at sub-transaction commit or abort. Must run *before*
/// processing invalidation messages. **Own logic.**
pub fn AtEOSubXact_RelationCache(
    isCommit: bool,
    mySubid: SubTransactionId,
    parentSubid: SubTransactionId,
) -> PgResult<()> {
    // Forget in_progress_list (we don't commit subtransactions during
    // RelationBuildDesc()).
    with_state(|st| {
        debug_assert!(st.in_progress_list.is_empty() || !isCommit);
        st.in_progress_list.clear();
    });

    let (overflowed, list) =
        with_state(|st| (st.eoxact_list_overflowed, st.eoxact_list.clone()));

    if overflowed {
        for relation in cache_seq_reldescs() {
            AtEOSubXact_cleanup(relation, isCommit, mySubid, parentSubid)?;
        }
    } else {
        for relid in list {
            if let Some(relation) = cache_find_reldesc(relid) {
                AtEOSubXact_cleanup(relation, isCommit, mySubid, parentSubid)?;
            }
        }
    }

    // Don't reset the list; we still need more cleanup later.
    Ok(())
}

/// `AtEOSubXact_cleanup(relation, isCommit, mySubid, parentSubid)` (relcache.c):
/// clean up a single rel at subtransaction commit or abort. Idempotent.
/// **Own logic.**
#[allow(unsafe_code)]
fn AtEOSubXact_cleanup(
    relation: *mut RelationData,
    isCommit: bool,
    mySubid: SubTransactionId,
    parentSubid: SubTransactionId,
) -> PgResult<()> {
    // SAFETY: live cache-owned descriptor.
    let rd = unsafe { &mut *relation };

    // Is it a relation created in the current subtransaction? During subcommit,
    // mark it as belonging to the parent (unless dropped); otherwise delete it.
    if rd.rd_createSubid == mySubid {
        debug_assert!(
            rd.rd_droppedSubid == mySubid || rd.rd_droppedSubid == InvalidSubTransactionId
        );
        if isCommit && rd.rd_droppedSubid == InvalidSubTransactionId {
            rd.rd_createSubid = parentSubid;
        } else if rd.rd_refcnt == 0 {
            // allow the entry to be removed
            rd.rd_createSubid = InvalidSubTransactionId;
            rd.rd_newRelfilelocatorSubid = InvalidSubTransactionId;
            rd.rd_firstRelfilelocatorSubid = InvalidSubTransactionId;
            rd.rd_droppedSubid = InvalidSubTransactionId;
            RelationClearRelation(relation)?;
            return Ok(());
        } else {
            // (leaked?) reference; transfer it to the parent and bleat.
            rd.rd_createSubid = parentSubid;
            elog(
                WARNING,
                format!(
                    "cannot remove relcache entry for \"{}\" because it has nonzero refcount",
                    rd.rd_rel.relname
                ),
            )?;
        }
    }

    // Likewise, update or drop any new-relfilenumber-in-subtransaction record
    // or drop record.
    if rd.rd_newRelfilelocatorSubid == mySubid {
        rd.rd_newRelfilelocatorSubid = if isCommit { parentSubid } else { InvalidSubTransactionId };
    }
    if rd.rd_firstRelfilelocatorSubid == mySubid {
        rd.rd_firstRelfilelocatorSubid =
            if isCommit { parentSubid } else { InvalidSubTransactionId };
    }
    if rd.rd_droppedSubid == mySubid {
        rd.rd_droppedSubid = if isCommit { parentSubid } else { InvalidSubTransactionId };
    }
    Ok(())
}
