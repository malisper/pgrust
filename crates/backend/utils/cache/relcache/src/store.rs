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
            st.lru_clock += 1;
            ent.last_used.set(st.lru_clock);
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
        Probe::Miss => {
            // D3.2: the shared-core L2 sits under the miss; the hit paths
            // above are untouched.
            let built = if crate::l2core::l2_active() {
                crate::l2core::miss_via_l2(relationId)?
            } else {
                crate::build::RelationBuildDesc(relationId, true)?
            };
            // D3.1: the miss path is the only place user queries grow the
            // cache; enforce the entry cap here, never mid-build (nested
            // opens see a non-empty in_progress list and skip).
            enforce_cap()?;
            Ok(built)
        }
    }
}

/// D3.1 cap enforcement: when `id_cache` exceeds the cap, clear the
/// least-recently-used eligible entries. Eligibility mirrors exactly the
/// conditions under which RelationFlushRelation would CLEAR (not rebuild) an
/// entry on an incoming invalidation: not nailed, refcount zero on the
/// current lineage, and no in-transaction subid state (new-in-transaction /
/// new-relfilelocator / dropped entries are exempt, as in C's flush logic) —
/// so an eviction is indistinguishable from an invalidation that arrived
/// while the entry was unpinned, minus the rebuild the next open performs.
pub(crate) fn enforce_cap() -> PgResult<()> {
    enforce_cap_at(crate::relcache_cap())
}

pub(crate) fn enforce_cap_at(cap: usize) -> PgResult<()> {
    if cap == 0 {
        return Ok(());
    }
    let victims: Vec<Oid> = with_state(|st| {
        // in_progress non-empty = we are inside somebody's RelationBuildDesc;
        // defer to the top-level miss that triggered it. Pre-critical phases
        // (bootstrap, init file load) never evict.
        if st.id_cache.len() <= cap
            || !st.in_progress.is_empty()
            || !st.critical_relcaches_built
        {
            return Vec::new();
        }
        let excess = st.id_cache.len() - cap;
        let mut eligible: Vec<(u64, Oid)> = st
            .id_cache
            .iter()
            .filter_map(|(oid, ent)| {
                (!ent.nailed
                    && Rc::strong_count(&ent.rel) == 1
                    && ent.rel.rd_createSubid.get() == InvalidSubTransactionId
                    && ent.rel.rd_newRelfilelocatorSubid.get() == InvalidSubTransactionId
                    && ent.rel.rd_firstRelfilelocatorSubid.get() == InvalidSubTransactionId
                    && ent.rel.rd_droppedSubid.get() == InvalidSubTransactionId)
                    .then(|| (ent.last_used.get(), *oid))
            })
            .collect();
        eligible.sort_unstable();
        eligible.truncate(excess);
        eligible.into_iter().map(|(_, oid)| oid).collect()
    });
    for oid in victims {
        // Re-verify per victim: clearing entry N can run side-cache forgets
        // but never takes new references; still, stay in lockstep with the
        // live state exactly like RelationCacheInvalidate's phase-2 loop.
        let Some((rel, nailed)) = lookup_ent(oid) else { continue };
        if nailed
            || !refcount_zero(&rel, 1)
            || rel.rd_createSubid.get() != InvalidSubTransactionId
            || rel.rd_firstRelfilelocatorSubid.get() != InvalidSubTransactionId
            || rel.rd_droppedSubid.get() != InvalidSubTransactionId
        {
            continue;
        }
        crate::invalidate::RelationClearRelation(oid, &rel)?;
    }
    Ok(())
}

/// D3.4 idle passivation: clear EVERY eligible entry — the enforce_cap_at
/// eligibility (evicting an unpinned, un-nailed, no-subxact-state entry ≡ an
/// invalidation arriving while it was unpinned); nailed, pinned, and
/// in-transaction entries survive. Returns the number cleared. Caller
/// guarantees idle-not-in-transaction, where nothing should be pinned — the
/// debug assert checks exactly that.
pub(crate) fn passivate_all() -> PgResult<usize> {
    let victims: Vec<Oid> = with_state(|st| {
        if !st.in_progress.is_empty() || !st.critical_relcaches_built {
            return Vec::new();
        }
        st.id_cache
            .iter()
            .filter_map(|(oid, ent)| {
                let eligible = !ent.nailed
                    && Rc::strong_count(&ent.rel) == 1
                    && ent.rel.rd_createSubid.get() == InvalidSubTransactionId
                    && ent.rel.rd_newRelfilelocatorSubid.get() == InvalidSubTransactionId
                    && ent.rel.rd_firstRelfilelocatorSubid.get() == InvalidSubTransactionId
                    && ent.rel.rd_droppedSubid.get() == InvalidSubTransactionId;
                debug_assert!(
                    eligible || ent.nailed,
                    "pinned/in-transaction relcache entry at idle passivation: {oid}"
                );
                eligible.then_some(*oid)
            })
            .collect()
    });
    let mut cleared = 0usize;
    for oid in victims {
        // Re-verify per victim, exactly as enforce_cap_at does.
        let Some((rel, nailed)) = lookup_ent(oid) else { continue };
        if nailed
            || !refcount_zero(&rel, 1)
            || rel.rd_createSubid.get() != InvalidSubTransactionId
            || rel.rd_firstRelfilelocatorSubid.get() != InvalidSubTransactionId
            || rel.rd_droppedSubid.get() != InvalidSubTransactionId
        {
            continue;
        }
        crate::invalidate::RelationClearRelation(oid, &rel)?;
        cleared += 1;
    }
    Ok(cleared)
}

// RelationCacheInsert. Dropping a replaced zero-ref entry is
// RelationDestroyRelation; a still-referenced one survives in its holders.
pub(crate) fn insert(
    rel: Rc<RelationData<'static>>,
    nailed: bool,
    replace_allowed: bool,
) -> PgResult<()> {
    let relid = rel.rd_id;
    let leaked = with_state(|st| {
        st.lru_clock += 1;
        let last_used = core::cell::Cell::new(st.lru_clock);
        match st.id_cache.insert(relid, RelCacheEnt { rel, nailed, last_used }) {
        Some(old) => {
            debug_assert!(replace_allowed);
            crate::note_stale(st, &old.rel);
            (!refcount_zero(&old.rel, 0)).then(|| String::from(old.rel.name()))
        }
        None => None,
        }
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
            // relcache.c:1475 RelationCacheDelete
            "trying to delete a reldesc that does not exist",
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
