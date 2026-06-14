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
    self, cache_delete, cache_find_reldesc, cache_lookup, cache_seq_reldescs, with_rel,
    with_rel_mut, with_state, RelationDecrementReferenceCount, RelationIncrementReferenceCount,
};
use crate::core_entry_store::entry::RelationData;

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
    use backend_utils_error::PgResult;

    /// `RelationMapInvalidateAll()` (relmapper.c) — reload the relation map.
    /// C reloads both the shared and local maps (`RelationMapInvalidate(true)`
    /// then `RelationMapInvalidate(false)`); the relmapper owner exposes the
    /// per-map `relation_map_invalidate(shared)` seam, so compose the two.
    /// Reading the on-disk map file can `ereport(ERROR)`, carried on `Err`.
    pub(super) fn relation_map_invalidate_all() -> PgResult<()> {
        backend_utils_cache_relmapper_seams::relation_map_invalidate::call(true)?;
        backend_utils_cache_relmapper_seams::relation_map_invalidate::call(false)?;
        Ok(())
    }

    /// `smgrreleaseall()` (smgr.c) — close all open relation files. Genuine
    /// cross-unit owner (smgr is still unported, CATALOG status=todo); the seam
    /// panics until smgr installs it.
    pub(super) fn smgrreleaseall() {
        backend_storage_smgr_seams::smgrreleaseall::call()
    }

    /// `RelationCloseSmgr(relation)` (rel.h inline) — close the relation's smgr
    /// handle. The smgr layer is a genuine cross-unit owner; the entry holds no
    /// `rd_smgr` field in the owned mirror, so this routes the relation's
    /// `RelFileLocatorBackend` to its owner (panics until smgr lands).
    pub(super) fn relation_close_smgr(relation: types_core::primitive::Oid) {
        let rlocator = crate::core_entry_store::with_rel(relation, |rd| {
            types_storage::RelFileLocatorBackend {
                locator: rd.rd_locator,
                backend: rd.rd_backend,
            }
        });
        backend_storage_smgr_seams::relation_close_smgr::call(rlocator)
    }

    /// `IsTransactionState()` (xact.c) — true when in a live transaction.
    pub(super) fn is_transaction_state() -> bool {
        backend_access_transam_xact_seams::is_transaction_state::call()
    }

    /// `HistoricSnapshotActive()` (snapmgr.c) — true during logical decoding.
    pub(super) fn historic_snapshot_active() -> bool {
        backend_utils_time_snapmgr_seams::historic_snapshot_active::call()
    }

    /// `GetCurrentSubTransactionId()` (xact.c).
    pub(super) fn get_current_sub_transaction_id() -> types_core::xact::SubTransactionId {
        backend_access_transam_xact_seams::get_current_sub_transaction_id::call()
    }
}

/* ==========================================================================
 * RelationInvalidateRelation / RelationClearRelation / RelationDestroyRelation
 * ======================================================================== */

/// `RelationInvalidateRelation(relation)` (relcache.c): mark an entry invalid so
/// it's reloaded on next access. Closes smgr (so the next access reopens the
/// files), drops AM cached data, and clears `rd_isvalid`. **Own logic.**
pub(crate) fn RelationInvalidateRelation(relation: Oid) -> PgResult<()> {
    // Make sure smgr and lower levels close the relation's files, if they
    // weren't closed already. (smgr is a cross-unit owner.)
    xunit::relation_close_smgr(relation);

    // Free AM cached data, if any. The owned entry models the AM cache as the
    // resolved `rd_amcache`-equivalent state on the descriptor; clearing it is
    // part of the index family's AM-init lifecycle, so there is no separate
    // heap block to free here (the C `pfree(rd_amcache)` is subsumed by the
    // owned descriptor). Nothing to do beyond marking invalid.

    with_rel_mut(relation, |rd| rd.rd_isvalid = false);
    Ok(())
}

/// `RelationClearRelation(relation)` (relcache.c): physically blow away a
/// relation cache entry. Caller must ensure refcount is zero and the rel/its
/// storage was not created in the current transaction. **Own logic** over the
/// real store. (In PG 18.3 the `rebuild` parameter was removed; rebuild is now
/// the separate [`RelationRebuildRelation`].)
pub fn RelationClearRelation(relation: Oid) -> PgResult<()> {
    let relid = with_rel(relation, |rd| {
        debug_assert!(rd.rd_refcnt == 0, "RelationHasReferenceCountZero");
        debug_assert!(!rd.rd_isnailed);
        // Relations created in the same transaction must never be removed (see
        // RelationFlushRelation).
        debug_assert!(rd.rd_createSubid == InvalidSubTransactionId);
        debug_assert!(rd.rd_firstRelfilelocatorSubid == InvalidSubTransactionId);
        debug_assert!(rd.rd_droppedSubid == InvalidSubTransactionId);
        rd.rd_id
    });

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
pub fn RelationRebuildRelation(relation: Oid) -> PgResult<()> {
    let (relkind, index_info_initialized, is_nailed, save_relid) = with_rel(relation, |rd| {
        debug_assert!(rd.rd_refcnt != 0, "!RelationHasReferenceCountZero");
        // there is no reason to ever rebuild a dropped relation
        debug_assert!(rd.rd_droppedSubid == InvalidSubTransactionId);
        // The C condition is `rd_indexcxt != NULL`, i.e. the index access info
        // has been initialized. In the owned model that state is exactly
        // `rd_indam` being set (the resolved index-AM routine), so use it as the
        // proxy.
        (rd.rd_rel.relkind, rd.rd_indam.is_some(), rd.rd_isnailed, rd.rd_id)
    });
    let is_index = relkind == RELKIND_INDEX || relkind == RELKIND_PARTITIONED_INDEX;

    // Close and mark it as invalid until we've finished the rebuild.
    RelationInvalidateRelation(relation)?;

    // Indexes only have a limited number of possible schema changes; use the
    // light reload unless the index access info hasn't been initialized yet
    // (index creation relies on the full procedure in that window). The reload
    // mutates the stored entry in place (`with_rel_mut`), keeping its `Box`
    // allocation — and any `RelationRef` pinned to it — stable.
    if is_index && index_info_initialized {
        return with_rel_mut(relation, crate::index::RelationReloadIndexInfo);
    }
    // Nailed relations are handled separately.
    if is_nailed {
        return with_rel_mut(relation, crate::index::RelationReloadNailed);
    }

    // Build a new entry from scratch, swap its contents with the old entry,
    // and finally destroy the new entry. This avoids trouble if an error
    // occurs partway through: the old entry stays !rd_isvalid and is no less
    // valid than before.

    // Build temporary entry, but don't link it into the hashtable: it is parked
    // in the build family's SCRATCH slot (the C `newrel` local pointer).
    let newrel_built = crate::build::RelationBuildDesc(save_relid, false)?;

    // Between here and the end of the swap, don't do anything that could read
    // system catalogs (it must be free from invalidation processing).

    if newrel_built == types_core::InvalidOid {
        // We can validly get here if using a historic snapshot in which the
        // relation is still invisible; just leave it invalid and return.
        if xunit::historic_snapshot_active() {
            return Ok(());
        }
        // Otherwise this shouldn't happen: dropping a still-referenced relation
        // is supposed to be impossible.
        return elog_relation_deleted_in_use(save_relid);
    }

    // Reclaim the freshly-built, not-yet-linked descriptor for the swap + final
    // destroy (the build family parked it in SCRATCH).
    let mut newrel: Box<RelationData> = crate::build::take_scratch()
        .expect("RelationBuildDesc(.., false) parked a scratch descriptor");

    // The swap mutates the stored `old` descriptor IN PLACE behind the `Box`'s
    // `&mut RelationData`, so its heap allocation never moves — any `RelationRef`
    // pinned to it (`rd_refcnt > 0`) stays valid, exactly like the C pointer.
    // There is no CHECK_FOR_INTERRUPTS in this sequence (straight-line code). The
    // closure returns the temporary entry (now holding the OLD contents) so the
    // caller can destroy it after the borrow is released.
    let to_destroy = with_rel_mut(relation, move |old| {
        // If we were to have cases of the relkind changing, pgstats would get
        // confused.
        debug_assert!(old.rd_rel.relkind == newrel.rd_rel.relkind);

        // keep_tupdesc/keep_rules/keep_policies decide whether to preserve the
        // old substructures in place (various places assume they won't move).
        // The node/rewrite-vocabulary payloads (rules, policies) are
        // presence-only in the owned mirror; preserve on the structural
        // comparisons we can do.
        let keep_tupdesc = equal_tuple_descs(&old.rd_att, &newrel.rd_att);
        let keep_rules = old.rd_has_rules == newrel.rd_has_rules;
        let keep_policies = old.rd_has_rsdesc == newrel.rd_has_rsdesc;
        // partkey is immutable once set up, so we can always keep it.
        let keep_partkey = old.rd_has_partkey;

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
        // wholesale content move below doesn't clobber them); moved back
        // conditionally after.
        let old_att = std::mem::take(&mut old.rd_att);
        let old_has_rules = old.rd_has_rules;
        let old_has_rsdesc = old.rd_has_rsdesc;
        let old_has_partkey = old.rd_has_partkey;
        let old_has_partdesc = old.rd_has_partdesc;

        // isnailed shouldn't change.
        debug_assert!(newrel.rd_isnailed == save_isnailed);

        // Wholesale content move: replace `old`'s contents with `newrel`'s, then
        // restore the preserved fields. `newrel` is left holding what *was* in
        // `old` so its `Box` drop frees the discarded contents (the C
        // `RelationDestroyRelation(newrel, ...)`).
        let discarded = std::mem::replace(old, *newrel);
        newrel = Box::new(discarded);

        // Restore preserved scalar fields onto the rebuilt entry.
        old.rd_refcnt = save_refcnt;
        old.rd_createSubid = save_createSubid;
        old.rd_newRelfilelocatorSubid = save_newRelfilelocatorSubid;
        old.rd_firstRelfilelocatorSubid = save_firstRelfilelocatorSubid;
        old.rd_droppedSubid = save_droppedSubid;
        old.rd_toastoid = save_toastoid;
        old.pgstat_enabled = save_pgstat_enabled;
        // rd_rel contents are always copied from newrel; after the wholesale
        // move `old.rd_rel` already holds the freshly built pg_class form.

        // preserve old tupledesc/rules/policies if no logical change.
        if keep_tupdesc {
            // Put the preserved old descriptor back; the freshly built one (now
            // in `old.rd_att`) is discarded with `newrel`.
            let freshly_built_att = std::mem::replace(&mut old.rd_att, old_att);
            newrel.rd_att = freshly_built_att;
        } else {
            // Keep the freshly built `rd_att`; `old_att` is dropped here.
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

        // Hand the temporary entry (now holding the OLD contents) back out so the
        // caller destroys it once the store borrow is released.
        newrel
    });

    // And now throw away the temporary entry (the C
    // `RelationDestroyRelation(newrel, !keep_tupdesc)`). In the owned model the
    // `remember_tupdesc` deferred-free is unnecessary (no shared tupdesc), so we
    // just drop the returned box.
    RelationDestroyRelation(to_destroy, false);
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
        // C disregards attrelid/attnum (placement keys); we keep attnum here as
        // the owned mirror uses it as the row's identity, which is benign for a
        // same-shape comparison.
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
        // When the column has a not-null constraint, its validity aspect lives
        // only in attnullability, so compare it too (C equalTupleDescs).
        if a1.attnotnull && a1.attnullability != a2.attnullability {
            return false;
        }
        // C also compares attndims/attstorage/attcompression/atthasdef/
        // attidentity/attgenerated/attislocal/attinhcount; those fields are not
        // carried on the trimmed OwnedAttr mirror (they are not consumed
        // elsewhere in this crate), so they are not representable here.
    }

    // Compare the constr substructure (C equalTupleDescs constr block): the
    // has_* flags + the defval array (assumed adnum-sorted) + the check array
    // (assumed name-sorted). The `missing`/AttrMissing array is node/datum
    // vocabulary not carried on the owned mirror and so is not compared.
    match (&d1.constr, &d2.constr) {
        (Some(c1), Some(c2)) => {
            if c1.has_not_null != c2.has_not_null
                || c1.has_generated_stored != c2.has_generated_stored
                || c1.has_generated_virtual != c2.has_generated_virtual
                || c1.defval.len() != c2.defval.len()
                || c1.check.len() != c2.check.len()
            {
                return false;
            }
            for (dv1, dv2) in c1.defval.iter().zip(c2.defval.iter()) {
                if dv1.adnum != dv2.adnum || dv1.adbin != dv2.adbin {
                    return false;
                }
            }
            for (ck1, ck2) in c1.check.iter().zip(c2.check.iter()) {
                if ck1.ccname != ck2.ccname
                    || ck1.ccbin != ck2.ccbin
                    || ck1.ccenforced != ck2.ccenforced
                    || ck1.ccvalid != ck2.ccvalid
                    || ck1.ccnoinherit != ck2.ccnoinherit
                {
                    return false;
                }
            }
            true
        }
        (None, None) => true,
        _ => false,
    }
}

/// `RelationIsMapped(relation)` (utils/rel.h) over the owned entry:
/// `RELKIND_HAS_STORAGE(relkind) && relfilenode == InvalidRelFileNumber`.
/// **Own logic.**
fn relation_is_mapped(rd: &RelationData) -> bool {
    let relkind = rd.rd_rel.relkind;
    let has_storage = relkind == RELKIND_RELATION
        || relkind == RELKIND_INDEX
        || relkind == (b'S' as i8) // RELKIND_SEQUENCE
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
pub fn RelationFlushRelation(relation: Oid) -> PgResult<()> {
    let (created_in_xact, dropped, refcnt_zero, is_nailed, refcnt_one) = with_rel(relation, |rd| {
        (
            rd.rd_createSubid != InvalidSubTransactionId
                || rd.rd_firstRelfilelocatorSubid != InvalidSubTransactionId,
            rd.rd_droppedSubid != InvalidSubTransactionId,
            rd.rd_refcnt == 0,
            rd.rd_isnailed,
            rd.rd_refcnt == 1,
        )
    });

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
pub fn RelationForgetRelation(rid: Oid) -> PgResult<()> {
    let relation = match cache_lookup(rid) {
        Some(r) => r,
        None => return Ok(()), // not in cache, nothing to do
    };

    let (refcnt_nonzero, preserve) = with_rel(relation, |rd| {
        debug_assert!(rd.rd_droppedSubid == InvalidSubTransactionId);
        (
            rd.rd_refcnt != 0,
            rd.rd_createSubid != InvalidSubTransactionId
                || rd.rd_firstRelfilelocatorSubid != InvalidSubTransactionId,
        )
    });

    if refcnt_nonzero {
        return Err(backend_utils_error::ereport(types_error::ERROR)
            .errmsg_internal(format!("relation {rid} is still open"))
            .into_error());
    }

    if preserve {
        // In the event of subtransaction rollback, we must not forget
        // rd_*Subid. Mark the entry "dropped" and invalidate it, instead of
        // destroying it right away.
        let subid = xunit::get_current_sub_transaction_id();
        with_rel_mut(relation, |rd| rd.rd_droppedSubid = subid);
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
pub fn RelationCacheInvalidate(debug_discard: bool) -> PgResult<()> {
    // Reload relation mapping data before reconstructing the cache.
    xunit::relation_map_invalidate_all()?;

    // Phase 1: walk the cache, deleting deletable items and collecting the
    // rebuildable ones. We snapshot the OID handles first (own
    // `cache_seq_reldescs`), which is equivalent to — and safer than — C's
    // delete-during-`hash_seq_search`, since `hash_seq_search` only copes with
    // deletion of the element currently being visited.
    let mut rebuild_first_list: Vec<Oid> = Vec::new();
    let mut rebuild_list: Vec<Oid> = Vec::new();

    for relation in cache_seq_reldescs() {
        let (skip, refcnt_zero, mapped, relid, isnailed) = with_rel(relation, |rd| {
            (
                // Ignore new relations; no other backend will manipulate them
                // before we commit. Likewise new-relfilelocator relations.
                rd.rd_createSubid != InvalidSubTransactionId
                    || rd.rd_firstRelfilelocatorSubid != InvalidSubTransactionId,
                rd.rd_refcnt == 0,
                relation_is_mapped(rd),
                rd.rd_id,
                rd.rd_isnailed,
            )
        });

        if skip {
            continue;
        }

        with_state(|st| st.relcache_invals_received += 1);

        if refcnt_zero {
            // Delete this entry immediately.
            RelationClearRelation(relation)?;
        } else {
            // If it's a mapped relation, immediately update its rd_locator in
            // case its relfilenumber changed (must happen in phase 1 in case
            // the relation is consulted during phase-2 rebuilds).
            if mapped {
                xunit::relation_close_smgr(relation);
                with_rel_mut(relation, crate::index::RelationInitPhysicalAddr)?;
            }

            // Order: pg_class to the front of rebuildFirstList, pg_class_oid_index
            // to its back; other nailed rels to the front of rebuildList,
            // everything else to the back.
            if relid == types_catalog::catalog::RELATION_RELATION_ID {
                rebuild_first_list.insert(0, relation);
            } else if relid == types_catalog::catalog::CLASS_OID_INDEX_ID {
                rebuild_first_list.push(relation);
            } else if isnailed {
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
        let (isnailed, refcnt_one) =
            with_rel(relation, |rd| (rd.rd_isnailed, rd.rd_refcnt == 1));
        if !in_xact || (isnailed && refcnt_one) {
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
fn AtEOXact_cleanup(relation: Oid, isCommit: bool) -> PgResult<()> {
    let (clear_relcache, refcnt_zero, relname) = with_rel_mut(relation, |rd| {
        // Is the relation live after this transaction ends? During commit, clear
        // the entry if it is preserved after relation drop; during rollback,
        // clear it if created in the current transaction.
        let clear_relcache = if isCommit {
            rd.rd_droppedSubid != InvalidSubTransactionId
        } else {
            rd.rd_createSubid != InvalidSubTransactionId
        };

        // Reset the subids to zero now that we're out of the transaction (also
        // lets RelationClearRelation drop the entry).
        rd.rd_createSubid = InvalidSubTransactionId;
        rd.rd_newRelfilelocatorSubid = InvalidSubTransactionId;
        rd.rd_firstRelfilelocatorSubid = InvalidSubTransactionId;
        rd.rd_droppedSubid = InvalidSubTransactionId;

        (clear_relcache, rd.rd_refcnt == 0, rd.rd_rel.relname.clone())
    });

    if clear_relcache {
        if refcnt_zero {
            RelationClearRelation(relation)?;
            return Ok(());
        }
        // Hmm, there's a (leaked?) reference. Don't remove the entry; bleat and
        // leave it. Just a WARNING to avoid error-during-error-recovery loops.
        elog(
            WARNING,
            format!(
                "cannot remove relcache entry for \"{relname}\" because it has nonzero refcount"
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
fn AtEOSubXact_cleanup(
    relation: Oid,
    isCommit: bool,
    mySubid: SubTransactionId,
    parentSubid: SubTransactionId,
) -> PgResult<()> {
    // Is it a relation created in the current subtransaction? During subcommit,
    // mark it as belonging to the parent (unless dropped); otherwise delete it.
    // `clear` and `warn_name` are decided inside the borrow; the clear happens
    // after (it re-enters the store via RelationClearRelation).
    enum Action {
        None,
        Clear,
        Warn(String),
    }
    let action = with_rel_mut(relation, |rd| {
        let mut action = Action::None;
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
                action = Action::Clear;
            } else {
                // (leaked?) reference; transfer it to the parent and bleat.
                rd.rd_createSubid = parentSubid;
                action = Action::Warn(rd.rd_rel.relname.clone());
            }
        }

        // Likewise, update or drop any new-relfilenumber-in-subtransaction
        // record or drop record.
        if rd.rd_newRelfilelocatorSubid == mySubid {
            rd.rd_newRelfilelocatorSubid =
                if isCommit { parentSubid } else { InvalidSubTransactionId };
        }
        if rd.rd_firstRelfilelocatorSubid == mySubid {
            rd.rd_firstRelfilelocatorSubid =
                if isCommit { parentSubid } else { InvalidSubTransactionId };
        }
        if rd.rd_droppedSubid == mySubid {
            rd.rd_droppedSubid = if isCommit { parentSubid } else { InvalidSubTransactionId };
        }
        action
    });

    match action {
        Action::None => Ok(()),
        Action::Clear => RelationClearRelation(relation),
        Action::Warn(relname) => elog(
            WARNING,
            format!(
                "cannot remove relcache entry for \"{relname}\" because it has nonzero refcount"
            ),
        ),
    }
}
