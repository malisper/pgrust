use std::rc::Rc;

use types_core::{InvalidSubTransactionId, Oid};
use types_error::PgResult;
use types_rel::RelationData;

use crate::{invalidate, with_state, RelCacheEnt, MAX_EOXACT_LIST};

pub(crate) enum Probe {
    Miss,
    Dropped,
    Valid(Rc<RelationData<'static>>),
    Invalid(Rc<RelationData<'static>>),
}

pub(crate) fn probe(relation_id: Oid) -> Probe {
    with_state(|st| match st.id_cache.get(&relation_id) {
        None => Probe::Miss,
        Some(ent) => {
            let rel = &ent.rel;
            if rel.rd_droppedSubid.get() != InvalidSubTransactionId {
                debug_assert!(!rel.rd_isvalid.get());
                Probe::Dropped
            } else if rel.rd_isvalid.get() {
                Probe::Valid(Rc::clone(rel))
            } else {
                Probe::Invalid(Rc::clone(rel))
            }
        }
    })
}

// RelationIdCacheLookup.
pub(crate) fn lookup_ent(relation_id: Oid) -> Option<(Rc<RelationData<'static>>, bool)> {
    with_state(|st| st.id_cache.get(&relation_id).map(|e| (Rc::clone(&e.rel), e.nailed)))
}

pub(crate) fn is_nailed(relation_id: Oid) -> bool {
    with_state(|st| st.id_cache.get(&relation_id).is_some_and(|e| e.nailed))
}

// NOT RelationHasReferenceCountZero. A rebuild replaces the entry Rc, so this
// counts holders of the CURRENT lineage only: it answers "if I replace this
// entry now, does anybody get orphaned?" -- the right question at the
// arm-selection sites, and it reads BELOW C's rd_refcnt whenever a holder of a
// superseded lineage exists. `held` = probe clones the caller holds, per frame.
#[inline]
pub(crate) fn refcount_zero(rel: &Rc<RelationData<'static>>, held: usize) -> bool {
    Rc::strong_count(rel) == 1 + held
}

// RelationHasReferenceCountZero (rel.h:500) for real: all lineages of `relid`,
// which is what C's rd_refcnt counts. Required at every leak-detection and
// user-visible-semantics site -- those ask "does the session still have this
// relation open anywhere", and refcount_zero cannot answer it. `held` = probe
// clones the caller holds on the current entry, same per-frame convention.
#[inline]
pub(crate) fn user_refcount_zero(relid: Oid, held: usize) -> bool {
    crate::RelationUserRefcount(relid) == held
}

pub fn RelationIdGetRelation(relationId: Oid) -> PgResult<Option<Rc<RelationData<'static>>>> {
    debug_assert!(
        !xact_seams::is_transaction_state::is_installed()
            || xact_seams::is_transaction_state::call()
    );

    match probe(relationId) {
        Probe::Dropped => Ok(None),
        Probe::Valid(rel) => Ok(Some(rel)),
        Probe::Invalid(stale) => {
            // The live clone is C's positive refcount: RelationCacheInvalidate
            // cannot evict the entry mid-rebuild.
            let rebuilt = invalidate::RelationRebuildRelation(relationId, &stale)?;
            debug_assert!(
                rebuilt.rd_isvalid.get()
                    || (is_nailed(relationId) && !crate::criticalRelcachesBuilt())
            );
            Ok(Some(rebuilt))
        }
        Probe::Miss => crate::build::RelationBuildDesc(relationId, true),
    }
}

// RelationCacheInsert. Dropping a replaced zero-ref entry is
// RelationDestroyRelation; a still-referenced one survives in its holders.
pub(crate) fn insert(
    rel: Rc<RelationData<'static>>,
    nailed: bool,
    replace_allowed: bool,
) -> PgResult<()> {
    let relid = rel.rd_id;
    let leaked = with_state(|st| match st.id_cache.insert(relid, RelCacheEnt { rel, nailed }) {
        Some(old) => {
            debug_assert!(replace_allowed);
            crate::note_stale(st, &old.rel);
            (!refcount_zero(&old.rel, 0)).then(|| String::from(old.rel.name()))
        }
        None => None,
    });
    if let Some(name) = leaked {
        if !miscinit_seams::is_bootstrap_processing_mode::call() {
            elog::elog(
                types_error::WARNING,
                format!("leaking still-referenced relcache entry for \"{name}\""),
            )?;
        }
    }
    Ok(())
}

// RelationCacheDelete + RelationDestroyRelation: removal drops the cache's
// strong ref; the payload frees when the last holder drops.
pub(crate) fn delete(relation_id: Oid) -> PgResult<()> {
    let missing = with_state(|st| st.id_cache.remove(&relation_id).is_none());
    if missing {
        elog::elog(
            types_error::WARNING,
            format!("failed to delete relcache entry for OID {relation_id}"),
        )?;
    }
    Ok(())
}

pub(crate) fn eoxact_list_add(relid: Oid) {
    with_state(|st| {
        if st.eoxact_list_len < MAX_EOXACT_LIST {
            st.eoxact_list[st.eoxact_list_len] = relid;
            st.eoxact_list_len += 1;
        } else {
            st.eoxact_list_overflowed = true;
        }
    });
}
