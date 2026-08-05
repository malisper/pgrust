use core::cell::Cell;
use std::rc::Rc;

use types_core::{InvalidSubTransactionId, Oid, SubTransactionId};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR, WARNING};
use types_rel::{RelationData, RELKIND_INDEX, RELKIND_PARTITIONED_INDEX, RELKIND_RELATION};

use crate::schemapg::CLASS_OID_INDEX_ID;
use crate::{build, cache_mcx, store, with_state, RelCacheEnt};

const RELATION_RELATION_ID: Oid = types_core::RELATION_RELATION_ID;

// RelationInvalidateRelation; C also frees rd_amcache (absent clearing, prior gap).
// rd_indexlist cleared here = C freeing it in RelationClearRelation/rebuild;
// every inval arm passes through here before the entry is dropped or replaced.
pub(crate) fn RelationInvalidateRelation(rel: &RelationData<'static>) {
    // C RelationCloseSmgr is void; mdclose cannot fail.
    let _ = smgr::RelationCloseSmgr(rel);
    rel.rd_isvalid.set(false);
    rel.rd_amcache.set(None);
    *rel.rd_amcache_hash.borrow_mut() = None;
    rel.rd_amcache_gin.set(None);
    *rel.rd_indexlist.borrow_mut() = None;
    *rel.rd_trigdesc.borrow_mut() = None;
    crate::rules::forget(rel.rd_id);
    crate::rowsecurity::forget(rel.rd_id);
    crate::indexattr::forget(rel.rd_id);
    crate::statextlist::forget(rel.rd_id);
    crate::fkeylist::forget(rel.rd_id);
    crate::deform_jit::forget(rel.rd_id);
}

// RelationClearRelation: caller has verified refcount-zero, not nailed, and
// no in-transaction subids.
pub(crate) fn RelationClearRelation(relid: Oid, rel: &RelationData<'static>) -> PgResult<()> {
    debug_assert_eq!(rel.rd_createSubid.get(), InvalidSubTransactionId);
    debug_assert_eq!(rel.rd_firstRelfilelocatorSubid.get(), InvalidSubTransactionId);
    debug_assert_eq!(rel.rd_droppedSubid.get(), InvalidSubTransactionId);
    RelationInvalidateRelation(rel);
    store::delete(relid)
}

fn copy_preserved(from: &RelationData<'static>, to: &RelationData<'static>) {
    to.rd_createSubid.set(from.rd_createSubid.get());
    to.rd_newRelfilelocatorSubid.set(from.rd_newRelfilelocatorSubid.get());
    to.rd_firstRelfilelocatorSubid.set(from.rd_firstRelfilelocatorSubid.get());
    to.rd_droppedSubid.set(from.rd_droppedSubid.get());
    to.pgstat_enabled.set(from.pgstat_enabled.get());
}

fn replace_entry(relid: Oid, newrel: &Rc<RelationData<'static>>) {
    with_state(|st| {
        let old = match st.id_cache.get_mut(&relid) {
            Some(ent) => Some(core::mem::replace(&mut ent.rel, Rc::clone(newrel))),
            None => {
                st.id_cache.insert(relid, RelCacheEnt { rel: Rc::clone(newrel), nailed: false });
                None
            }
        };
        if let Some(old) = old {
            crate::note_stale(st, &old);
        }
    });
}

#[track_caller]
#[cold]
#[inline(never)]
fn deleted_while_in_use(relid: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("relation {relid} deleted while still in use"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

// RelationRebuildRelation. C swaps the rebuilt contents in place; here the
// entry Rc is replaced and live holders keep their invalidated snapshot until
// they reopen. The swap's keep_* preservation set maps to copying the Cell
// fields and reusing an equal tupdesc Rc.
pub(crate) fn RelationRebuildRelation(
    relid: Oid,
    held: &Rc<RelationData<'static>>,
) -> PgResult<Rc<RelationData<'static>>> {
    debug_assert!(!store::refcount_zero(held, 0));
    debug_assert_eq!(held.rd_droppedSubid.get(), InvalidSubTransactionId);

    RelationInvalidateRelation(held);

    if matches!(held.rd_rel.relkind, RELKIND_INDEX | RELKIND_PARTITIONED_INDEX)
        && held.rd_index.is_some()
    {
        return RelationReloadIndexInfo(relid, held);
    }
    if store::is_nailed(relid) {
        return RelationReloadNailed(relid, held);
    }

    let Some(newdata) = build::build_desc_data(relid)? else {
        if snapmgr::HistoricSnapshotActive() {
            return Ok(Rc::clone(held));
        }
        return Err(deleted_while_in_use(relid));
    };
    debug_assert_eq!(held.rd_rel.relkind, newdata.rd_rel.relkind);

    let mut newdata = newdata;
    if tupdesc::equalTupleDescs(&held.rd_att, &newdata.rd_att) {
        // keep_tupdesc: preserve descriptor identity for pointer-compare users
        newdata.rd_att = Rc::clone(&held.rd_att);
    }
    let newrel = Rc::new(newdata);
    copy_preserved(held, &newrel);
    newrel.rd_isvalid.set(true);
    replace_entry(relid, &newrel);
    Ok(newrel)
}

// RelationReloadIndexInfo. C refreshes rd_rel + the mutable pg_index bools in
// place; here it's a fresh index-info build, holders keep the old snapshot.
fn RelationReloadIndexInfo(
    relid: Oid,
    held: &Rc<RelationData<'static>>,
) -> PgResult<Rc<RelationData<'static>>> {
    debug_assert!(!held.rd_isvalid.get());
    debug_assert_eq!(held.rd_droppedSubid.get(), InvalidSubTransactionId);

    let critical = crate::criticalRelcachesBuilt();
    if held.rd_rel.relisshared && !critical {
        // Shared index before database selection: no pg_class to read, no
        // significant schema change possible — but its physical
        // relfilenumber might have changed (relcache.c:2297-2300).
        build::RelationInitPhysicalAddr(held)?;
        held.rd_isvalid.set(true);
        return Ok(Rc::clone(held));
    }

    let index_ok = relid != CLASS_OID_INDEX_ID && critical;
    let Some(scanned) = relcache_build_seams::scan_pg_relation::call(relid, index_ok, false)?
    else {
        return Err(Box::new(
            PgError::error(format!("could not find pg_class tuple for index {relid}"))
                .with_sqlstate(ERRCODE_INTERNAL_ERROR),
        ));
    };
    let mcx = cache_mcx();
    // System indexes keep their access info without re-reading pg_index — the
    // INDEXRELID syscache load can recurse back into this reload (C gates the
    // refresh on !IsSystemRelation for the same reason, relcache.c:2444).
    let is_system = scanned.form.relnamespace == types_core::PG_CATALOG_NAMESPACE
        || scanned.form.relnamespace == types_core::PG_TOAST_NAMESPACE;
    let (index, opcintype, opfamily, indoption, indcollation, support) = if is_system {
        let held_index = held.rd_index.as_ref().expect("system index rd_index");
        (
            clone_pg_index(mcx, held_index)?,
            vec_clone_in(mcx, &held.rd_opcintype)?,
            vec_clone_in(mcx, &held.rd_opfamily)?,
            vec_clone_in(mcx, &held.rd_indoption)?,
            vec_clone_in(mcx, &held.rd_indcollation)?,
            vec_clone_in(mcx, &held.rd_support)?,
        )
    } else {
        let ii =
            relcache_build_seams::relation_init_index_access_info::call(mcx, relid, &scanned.form)?;
        (ii.index, ii.opcintype, ii.opfamily, ii.indoption, ii.indcollation, ii.support)
    };

    let newrel = Rc::new(RelationData { rd_locator: Default::default(), rd_smgr: Default::default(),
        rd_id: relid,
        rd_backend: held.rd_backend,
        rd_islocaltemp: held.rd_islocaltemp,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(InvalidSubTransactionId),
        rd_newRelfilelocatorSubid: Cell::new(InvalidSubTransactionId),
        rd_firstRelfilelocatorSubid: Cell::new(InvalidSubTransactionId),
        rd_droppedSubid: Cell::new(InvalidSubTransactionId),
        rd_lockInfo: lmgr::RelationInitLockInfo(relid, scanned.form.relisshared),
        rd_rel: scanned.form,
        rd_att: Rc::clone(&held.rd_att),
        rd_index: Some(index),
        rd_opcintype: opcintype,
        rd_opfamily: opfamily,
        rd_indoption: indoption,
        rd_indcollation: indcollation,
        rd_options: scanned.options,
        pgstat_enabled: Cell::new(false),
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
        rd_support: support,
        // Preserved like rd_support: resolving a support proc scans
        // pg_amproc, which needs these very indexes searchable.
        rd_supportinfo: core::cell::RefCell::new(held.rd_supportinfo.borrow().clone()),
        // Preserved like rd_supportinfo: attoptions changes swap the whole
        // entry, and holders keep their Rc.
        rd_opcoptions: core::cell::RefCell::new(held.rd_opcoptions.borrow().clone()),
        rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
    });
    build::RelationInitPhysicalAddr(&newrel)?;
    copy_preserved(held, &newrel);
    replace_entry(relid, &newrel);
    Ok(newrel)
}

fn vec_clone_in<'mcx, T: Copy>(
    mcx: mcx::Mcx<'mcx>,
    src: &mcx::PgVec<'_, T>,
) -> PgResult<mcx::PgVec<'mcx, T>> {
    let mut out: mcx::PgVec<'mcx, T> = mcx::vec_with_capacity_in(mcx, src.len())?;
    out.extend(src.iter().copied());
    Ok(out)
}

fn clone_pg_index<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    src: &types_rel::FormData_pg_index<'_>,
) -> PgResult<types_rel::FormData_pg_index<'mcx>> {
    Ok(types_rel::FormData_pg_index {
        indexrelid: src.indexrelid,
        indrelid: src.indrelid,
        indnatts: src.indnatts,
        indnkeyatts: src.indnkeyatts,
        indisunique: src.indisunique,
        indnullsnotdistinct: src.indnullsnotdistinct,
        indisprimary: src.indisprimary,
        indisexclusion: src.indisexclusion,
        indimmediate: src.indimmediate,
        indisvalid: src.indisvalid,
        indisready: src.indisready,
        indkey: vec_clone_in(mcx, &src.indkey)?,
        has_indpred: src.has_indpred,
        indexprs_src: match &src.indexprs_src {
            Some(s) => Some(mcx::PgString::from_str_in(s.as_str(), mcx)?),
            None => None,
        },
        indpred_src: match &src.indpred_src {
            Some(s) => Some(mcx::PgString::from_str_in(s.as_str(), mcx)?),
            None => None,
        },
    })
}

// RelationReloadNailed: only rd_rel content (relfrozenxid etc.) can change.
fn RelationReloadNailed(
    relid: Oid,
    held: &Rc<RelationData<'static>>,
) -> PgResult<Rc<RelationData<'static>>> {
    debug_assert!(!held.rd_isvalid.get());
    debug_assert_eq!(held.rd_rel.relkind, RELKIND_RELATION);

    // Redo RelationInitPhysicalAddr in case it is a mapped relation whose
    // mapping changed (relcache.c:2394-2398). In place, before the scan: the
    // pg_class self-scan below must read the post-swap file, and error paths
    // must not strand the stale locator on a valid-again entry.
    build::RelationInitPhysicalAddr(held)?;

    if !crate::criticalRelcachesBuilt() {
        // Can't scan pg_class yet: leave invalid but usable.
        return Ok(Rc::clone(held));
    }

    // Valid before scanning: the scan re-enters the relcache for pg_class and
    // must not recurse into this reload.
    held.rd_isvalid.set(true);
    let scanned = relcache_build_seams::scan_pg_relation::call(relid, true, false)?
        .ok_or_else(|| deleted_while_in_use(relid))?;

    let newrel = Rc::new(RelationData { rd_locator: Default::default(), rd_smgr: Default::default(),
        rd_id: relid,
        rd_backend: held.rd_backend,
        rd_islocaltemp: held.rd_islocaltemp,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(InvalidSubTransactionId),
        rd_newRelfilelocatorSubid: Cell::new(InvalidSubTransactionId),
        rd_firstRelfilelocatorSubid: Cell::new(InvalidSubTransactionId),
        rd_droppedSubid: Cell::new(InvalidSubTransactionId),
        rd_lockInfo: held.rd_lockInfo,
        rd_rel: scanned.form,
        rd_att: Rc::clone(&held.rd_att),
        rd_index: None,
        rd_opcintype: mcx::PgVec::new_in(cache_mcx()),
        rd_opfamily: mcx::PgVec::new_in(cache_mcx()),
        rd_indoption: mcx::PgVec::new_in(cache_mcx()),
        rd_indcollation: mcx::PgVec::new_in(cache_mcx()),
        rd_options: scanned.options,
        pgstat_enabled: Cell::new(false),
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
        rd_support: mcx::PgVec::new_in(cache_mcx()),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
    });
    build::RelationInitPhysicalAddr(&newrel)?;
    copy_preserved(held, &newrel);
    debug_assert!(store::is_nailed(relid));
    replace_entry(relid, &newrel);
    Ok(newrel)
}

// RelationFlushRelation: rebuild if open, else blow away.
pub(crate) fn RelationFlushRelation(relid: Oid) -> PgResult<()> {
    let Some((rel, nailed)) = store::lookup_ent(relid) else {
        return Ok(());
    };
    let in_xact = xact_seams::is_transaction_state::call();

    if rel.rd_createSubid.get() != InvalidSubTransactionId
        || rel.rd_firstRelfilelocatorSubid.get() != InvalidSubTransactionId
    {
        // New-in-transaction rels are rebuilt, never flushed, to keep their
        // "new" status; our held clone is C's temporary refcount bump.
        if in_xact && rel.rd_droppedSubid.get() == InvalidSubTransactionId {
            RelationRebuildRelation(relid, &rel)?;
        } else {
            RelationInvalidateRelation(&rel);
        }
        return Ok(());
    }

    if !nailed && store::refcount_zero(&rel, 1) {
        RelationClearRelation(relid, &rel)
    } else if !in_xact || (nailed && store::refcount_zero(&rel, 1)) {
        // No catalog access possible, or an unused nailed rel: defer.
        RelationInvalidateRelation(&rel);
        Ok(())
    } else {
        RelationRebuildRelation(relid, &rel).map(|_| ())
    }
}

// RelationForgetRelation: caller reports that it dropped the relation.
pub fn RelationForgetRelation(rid: Oid) -> PgResult<()> {
    let Some((rel, _nailed)) = store::lookup_ent(rid) else {
        return Ok(());
    };
    // relcache.c:2903. The session must not have this relation open on ANY
    // lineage: refcount_zero would miss a handle taken before a rebuild and
    // let the drop through. Our lookup_ent clone is C's temporary bump.
    if !store::user_refcount_zero(rid, 1) {
        return Err(Box::new(
            PgError::error(format!("relation {rid} is still open"))
                .with_sqlstate(ERRCODE_INTERNAL_ERROR),
        ));
    }
    debug_assert_eq!(rel.rd_droppedSubid.get(), InvalidSubTransactionId);
    if rel.rd_createSubid.get() != InvalidSubTransactionId
        || rel.rd_firstRelfilelocatorSubid.get() != InvalidSubTransactionId
    {
        // Preserve rd_*Subid for subxact rollback: mark dropped, keep entry.
        rel.rd_droppedSubid.set(xact_seams::get_current_sub_transaction_id::call());
        RelationInvalidateRelation(&rel);
        Ok(())
    } else {
        RelationClearRelation(rid, &rel)
    }
}

pub fn RelationCacheInvalidateEntry(relationId: Oid) -> PgResult<()> {
    // The rules side-cache can hold relids that never entered id_cache.
    crate::rules::forget(relationId);
    crate::rowsecurity::forget(relationId);
    crate::indexattr::forget(relationId);
    crate::statextlist::forget(relationId);
    crate::fkeylist::forget(relationId);
    crate::deform_jit::forget(relationId);
    let cached = with_state(|st| st.id_cache.contains_key(&relationId));
    if cached {
        with_state(|st| st.invals_received += 1);
        RelationFlushRelation(relationId)
    } else {
        with_state(|st| {
            for ent in st.in_progress.iter_mut() {
                if ent.reloid == relationId {
                    ent.invalidated = true;
                }
            }
        });
        Ok(())
    }
}

// Two phases: deletions first (safe against re-entry), then rebuilds ordered
// pg_class, pg_class_oid_index, other nailed, rest — catalogs must be current
// before they reload the rest. Phase lists are transient per call, as in C.
pub fn RelationCacheInvalidate(debug_discard: bool) -> PgResult<()> {
    relmapper_seams::relation_map_invalidate_all::call()?;
    with_state(|st| {
        st.rules_cache.clear();
        st.policies_cache.clear();
        st.indexattr_cache.clear();
        st.statext_cache.clear();
        st.fkey_cache.clear();
        st.deform_jit_cache.clear();
    });

    let snapshot: Vec<(Oid, Rc<RelationData<'static>>, bool)> = with_state(|st| {
        st.id_cache.iter().map(|(k, e)| (*k, Rc::clone(&e.rel), e.nailed)).collect()
    });

    // Oids, not entry clones: see the re-resolve note on the phase-2 loop.
    let mut rebuild_first: Vec<Oid> = Vec::new();
    let mut rebuild: Vec<Oid> = Vec::new();

    for (relid, rel, nailed) in snapshot {
        // New-in-transaction rels can't be targets of cross-backend inval.
        if rel.rd_createSubid.get() != InvalidSubTransactionId
            || rel.rd_firstRelfilelocatorSubid.get() != InvalidSubTransactionId
        {
            continue;
        }
        with_state(|st| st.invals_received += 1);

        if !nailed && store::refcount_zero(&rel, 1) {
            RelationClearRelation(relid, &rel)?;
            continue;
        }
        if rel.is_mapped() {
            build::RelationInitPhysicalAddr(&rel)?;
        }
        if relid == RELATION_RELATION_ID {
            rebuild_first.insert(0, relid);
        } else if relid == CLASS_OID_INDEX_ID {
            rebuild_first.push(relid);
        } else if nailed {
            rebuild.insert(0, relid);
        } else {
            rebuild.push(relid);
        }
    }

    // FDs must be re-opened after possible relfilenumber changes.
    smgr::smgrreleaseall();

    let in_xact = xact_seams::is_transaction_state::call();
    for relid in rebuild_first.into_iter().chain(rebuild) {
        // C's phase-2 list carries a pointer that is guaranteed to still BE the
        // cache's entry when its turn comes: refcount > 0 forbids deletion and
        // the rebuild is in place (relcache.c:2570-2582, 2971-2980). The list
        // is also not itself a reference, so it does not perturb rd_refcnt.
        // Our rebuild REPLACES the entry Rc, so a clone held across this loop
        // would (a) be orphaned by a nested invalidation arriving during an
        // earlier entry's catalog access, its strong count no longer counting
        // the cache and so reading one below C's rd_refcnt in the arm test
        // below, and (b) inflate what that nested arm test reads by one. Hold
        // nothing; re-resolve, as the EOXact cleanups already do.
        let Some((rel, nailed)) = store::lookup_ent(relid) else {
            continue;
        };
        if !in_xact || (nailed && store::refcount_zero(&rel, 1)) {
            RelationInvalidateRelation(&rel);
        } else {
            RelationRebuildRelation(relid, &rel)?;
        }
    }

    if !debug_discard {
        with_state(|st| {
            for ent in st.in_progress.iter_mut() {
                ent.invalidated = true;
            }
        });
    }
    Ok(())
}

// Copied out so cleanup can re-enter the store (cold, per-EOXact).
fn eoxact_targets() -> Vec<Oid> {
    with_state(|st| {
        if st.eoxact_list_overflowed {
            st.id_cache.keys().copied().collect()
        } else {
            st.eoxact_list[..st.eoxact_list_len].to_vec()
        }
    })
}

// RelationAssumeNewRelfilelocator (relcache.c): stamp the subid Cells and
// flag the entry for eoxact cleanup.
pub fn RelationAssumeNewRelfilelocator(rel: &RelationData<'_>) {
    let subid = xact_seams::get_current_sub_transaction_id::call();
    rel.rd_newRelfilelocatorSubid.set(subid);
    if rel.rd_firstRelfilelocatorSubid.get() == InvalidSubTransactionId {
        rel.rd_firstRelfilelocatorSubid.set(subid);
    }
    store::eoxact_list_add(rel.rd_id);
}

pub fn AtEOXact_RelationCache(isCommit: bool) -> PgResult<()> {
    with_state(|st| {
        debug_assert!(st.in_progress.is_empty() || !isCommit);
        st.in_progress.clear();
    });

    // Duplicates possible in eoxact_list; cleanup is idempotent. No
    // EOXactTupleDescArray: rd_att is Rc-shared, freed by the last holder.
    for relid in eoxact_targets() {
        AtEOXact_cleanup(relid, isCommit)?;
    }

    with_state(|st| {
        st.eoxact_list_len = 0;
        st.eoxact_list_overflowed = false;
    });
    Ok(())
}

fn AtEOXact_cleanup(relid: Oid, isCommit: bool) -> PgResult<()> {
    let Some((rel, nailed)) = store::lookup_ent(relid) else {
        return Ok(());
    };
    let _ = nailed;
    #[cfg(debug_assertions)]
    if !miscinit_seams::is_bootstrap_processing_mode::call() {
        // relcache.c:3319-3320: rd_refcnt == (rd_isnailed ? 1 : 0), the nail
        // living in the flag here. All lineages: the leak shape this cache
        // makes possible is a handle taken before a rebuild and never dropped,
        // which no count on the current entry can see.
        debug_assert!(store::user_refcount_zero(relid, 1), "relcache reference leak at EOXact");
    }

    let clear_relcache = if isCommit {
        rel.rd_droppedSubid.get() != InvalidSubTransactionId
    } else {
        rel.rd_createSubid.get() != InvalidSubTransactionId
    };

    rel.rd_createSubid.set(InvalidSubTransactionId);
    rel.rd_newRelfilelocatorSubid.set(InvalidSubTransactionId);
    rel.rd_firstRelfilelocatorSubid.set(InvalidSubTransactionId);
    rel.rd_droppedSubid.set(InvalidSubTransactionId);

    if clear_relcache {
        // relcache.c:3348-3366. The WARNING is a leak diagnostic, so it has to
        // see holders of superseded lineages too or it is silently suppressed
        // in exactly the case this cache adds.
        if store::user_refcount_zero(relid, 1) {
            return RelationClearRelation(relid, &rel);
        }
        elog::elog(
            WARNING,
            format!(
                "cannot remove relcache entry for \"{}\" because it has nonzero refcount",
                rel.name()
            ),
        )?;
    }
    Ok(())
}

pub fn AtEOSubXact_RelationCache(
    isCommit: bool,
    mySubid: SubTransactionId,
    parentSubid: SubTransactionId,
) -> PgResult<()> {
    with_state(|st| {
        debug_assert!(st.in_progress.is_empty() || !isCommit);
        st.in_progress.clear();
    });

    for relid in eoxact_targets() {
        AtEOSubXact_cleanup(relid, isCommit, mySubid, parentSubid)?;
    }
    // Keep eoxact_list: more cleanup at higher levels and EOXact.
    Ok(())
}

fn AtEOSubXact_cleanup(
    relid: Oid,
    isCommit: bool,
    mySubid: SubTransactionId,
    parentSubid: SubTransactionId,
) -> PgResult<()> {
    let Some((rel, _nailed)) = store::lookup_ent(relid) else {
        return Ok(());
    };

    if rel.rd_createSubid.get() == mySubid {
        debug_assert!(
            rel.rd_droppedSubid.get() == mySubid
                || rel.rd_droppedSubid.get() == InvalidSubTransactionId
        );
        if isCommit && rel.rd_droppedSubid.get() == InvalidSubTransactionId {
            rel.rd_createSubid.set(parentSubid);
        } else if store::user_refcount_zero(relid, 1) {
            // relcache.c:3452-3475, matched pair with AtEOXact_cleanup: same
            // diagnostic, and the else arm additionally transfers the entry to
            // the parent subxact so cleanup can retry.
            rel.rd_createSubid.set(InvalidSubTransactionId);
            rel.rd_newRelfilelocatorSubid.set(InvalidSubTransactionId);
            rel.rd_firstRelfilelocatorSubid.set(InvalidSubTransactionId);
            rel.rd_droppedSubid.set(InvalidSubTransactionId);
            return RelationClearRelation(relid, &rel);
        } else {
            rel.rd_createSubid.set(parentSubid);
            elog::elog(
                WARNING,
                format!(
                    "cannot remove relcache entry for \"{}\" because it has nonzero refcount",
                    rel.name()
                ),
            )?;
        }
    }

    let transfer = |cell: &Cell<SubTransactionId>| {
        if cell.get() == mySubid {
            cell.set(if isCommit { parentSubid } else { InvalidSubTransactionId });
        }
    };
    transfer(&rel.rd_newRelfilelocatorSubid);
    transfer(&rel.rd_firstRelfilelocatorSubid);
    transfer(&rel.rd_droppedSubid);
    Ok(())
}
