#![allow(non_snake_case)]

pub mod build;
pub mod deform_jit;
pub mod indexlist;
mod trigdesc;
pub mod initfile;
pub mod invalidate;
pub mod local;
pub mod indexattr;
pub mod fkeylist;
pub mod statextlist;
pub mod rowsecurity;
pub mod rules;
pub mod schemapg;
pub mod store;
#[cfg(test)]
mod tests;

use core::cell::RefCell;
use core::mem::ManuallyDrop;
use std::rc::{Rc, Weak};

use mcx::{Mcx, MemoryContext, PgHashMap, PgVec};
use types_core::Oid;
use types_rel::RelationData;

pub use build::{formrdesc, RelationBuildDesc};
pub use indexlist::RelationGetIndexList;
pub use trigdesc::RelationGetTriggerDesc;
pub use initfile::{
    RelationCacheInitFilePostInvalidate, RelationCacheInitFilePreInvalidate,
    RelationCacheInitFileRemove, RelationCacheInitialize, RelationCacheInitializePhase2,
    RelationCacheInitializePhase3, RelationIdIsInInitFile,
};
pub use invalidate::{
    AtEOSubXact_RelationCache, AtEOXact_RelationCache, RelationCacheInvalidate,
    RelationCacheInvalidateEntry, RelationForgetRelation,
};
pub use rowsecurity::RelationGetRowSecurityDesc;
pub use rules::RelationGetRules;
pub use store::RelationIdGetRelation;

pub const MAX_EOXACT_LIST: usize = 32;
const INITRELCACHESIZE: usize = 400;

// C rd_refcnt does NOT map onto the entry Rc: a rebuild replaces this Rc, so
// one oid can have several live allocations and strong_count on the current one
// counts only holders whose snapshot is current. C's question -- "does the
// session have this relation open at all" -- is RelationUserRefcount, which
// sums the current lineage and the still-held predecessors in `stale_refs`. A
// nailed entry's permanent rd_refcnt=1 is the `nailed` flag, so
// RelationUserRefcount == rd_refcnt - (nailed ? 1 : 0). See
// scratchpad/night/relcache-invariant-contract.md for the per-site table.
pub(crate) struct RelCacheEnt {
    pub(crate) rel: Rc<RelationData<'static>>,
    pub(crate) nailed: bool,
}

#[derive(Clone, Copy)]
pub(crate) struct InProgressEnt {
    pub(crate) reloid: Oid,
    pub(crate) invalidated: bool,
}

pub(crate) struct RelcacheState {
    pub(crate) mcx: Mcx<'static>,
    pub(crate) id_cache: PgHashMap<'static, Oid, RelCacheEnt>,
    pub(crate) rules_cache: PgHashMap<'static, Oid, std::rc::Rc<rules::RdRules>>,
    pub(crate) policies_cache: PgHashMap<'static, Oid, std::rc::Rc<rowsecurity::RdRowSecurity>>,
    pub(crate) indexattr_cache:
        PgHashMap<'static, Oid, std::rc::Rc<relcache_seams::IndexAttrBitmaps>>,
    pub(crate) statext_cache: PgHashMap<'static, Oid, std::rc::Rc<[Oid]>>,
    pub(crate) fkey_cache: PgHashMap<'static, Oid, std::rc::Rc<[types_rel::ForeignKeyCacheInfo]>>,
    pub(crate) deform_jit_cache:
        PgHashMap<'static, (Oid, u16), std::rc::Rc<jit_deform::DeformKernel>>,
    // C rebuilds swap entry contents in place, preserving rd_refcnt identity;
    // our rebuild replaces the Rc, so still-held predecessors are tracked here
    // (weak, pruned on read) to keep per-oid refcounts C-exact.
    pub(crate) stale_refs: PgHashMap<'static, Oid, PgVec<'static, Weak<RelationData<'static>>>>,
    pub(crate) in_progress: PgVec<'static, InProgressEnt>,
    pub(crate) eoxact_list: [Oid; MAX_EOXACT_LIST],
    pub(crate) eoxact_list_len: usize,
    pub(crate) eoxact_list_overflowed: bool,
    pub(crate) invals_received: i64,
    pub(crate) critical_relcaches_built: bool,
    pub(crate) critical_shared_relcaches_built: bool,
}

thread_local! {
    static STATE: RefCell<Option<ManuallyDrop<RelcacheState>>> = const { RefCell::new(None) };
}

// INVARIANT: `f` must not call back into any seam or re-entrant relcache path;
// the borrow is held for its whole extent (loud RefCell panic otherwise).
// CacheMemoryContext is leaked: C never resets or deletes it.
pub(crate) fn with_state<R>(f: impl FnOnce(&mut RelcacheState) -> R) -> R {
    STATE.with(|cell| {
        let mut slot = cell.borrow_mut();
        let st = slot.get_or_insert_with(|| {
            let mcx = ::mcx::session_root("CacheMemoryContext").mcx();
            // LIFO: drop the state PROPERLY (running the hash maps' drop
            // glue) before the context is freed wholesale. The rel entries
            // are `Rc<RelationData>` on the GLOBAL heap — a context-only
            // free skips the refcount decrements and leaks every entry
            // (~0.6 MiB/session measured, the dominant FPBUDGET-1 tail).
            ::mcx::register_session_cleanup(Box::new(|| {
                STATE.with(|cell| {
                    if let Some(st) = cell.borrow_mut().take() {
                        drop(ManuallyDrop::into_inner(st));
                    }
                });
            }));
            ManuallyDrop::new(RelcacheState {
                mcx,
                id_cache: PgHashMap::with_capacity_in(INITRELCACHESIZE, mcx),
                rules_cache: PgHashMap::new_in(mcx),
                policies_cache: PgHashMap::new_in(mcx),
                indexattr_cache: PgHashMap::new_in(mcx),
                statext_cache: PgHashMap::new_in(mcx),
                fkey_cache: PgHashMap::new_in(mcx),
                deform_jit_cache: PgHashMap::new_in(mcx),
                stale_refs: PgHashMap::new_in(mcx),
                in_progress: PgVec::new_in(mcx),
                eoxact_list: [0; MAX_EOXACT_LIST],
                eoxact_list_len: 0,
                eoxact_list_overflowed: false,
                invals_received: 0,
                critical_relcaches_built: false,
                critical_shared_relcaches_built: false,
            })
        });
        f(st)
    })
}

pub(crate) fn cache_mcx() -> Mcx<'static> {
    with_state(|st| st.mcx)
}

// Record a replaced entry allocation that still has holders; weak so pruning
// is automatic once the last holder drops.
pub(crate) fn note_stale(st: &mut RelcacheState, old: &Rc<RelationData<'static>>) {
    if Rc::strong_count(old) <= 1 {
        return;
    }
    let mcx = st.mcx;
    st.stale_refs
        .entry(old.rd_id)
        .or_insert_with(|| PgVec::new_in(mcx))
        .push(Rc::downgrade(old));
}

// C rd_refcnt vs (rd_isnailed ? 2 : 1): user refs across the current entry and
// any still-held rebuilt-away predecessors. The nail is a flag here, so the
// caller's expected count is 1 in both cases.
pub fn RelationUserRefcount(relid: Oid) -> usize {
    with_state(|st| {
        let mut total = 0usize;
        if let Some(ent) = st.id_cache.get(&relid) {
            total += Rc::strong_count(&ent.rel) - 1;
        }
        if let Some(v) = st.stale_refs.get_mut(&relid) {
            v.retain(|w| w.strong_count() > 0);
            for w in v.iter() {
                total += w.strong_count();
            }
            if v.is_empty() {
                st.stale_refs.remove(&relid);
            }
        }
        total
    })
}

pub fn criticalRelcachesBuilt() -> bool {
    with_state(|st| st.critical_relcaches_built)
}

pub fn criticalSharedRelcachesBuilt() -> bool {
    with_state(|st| st.critical_shared_relcaches_built)
}

pub fn init_seams() {
    relcache_seams::critical_relcaches_built::set(criticalRelcachesBuilt);
    relcache_seams::critical_shared_relcaches_built::set(criticalSharedRelcachesBuilt);
    relcache_seams::relation_id_get_relation::set(store::RelationIdGetRelation);
    relcache_seams::relation_get_index_list::set(indexlist::RelationGetIndexList);
    relcache_seams::relation_get_trigger_desc::set(trigdesc::RelationGetTriggerDesc);
    relcache_seams::relation_get_stat_ext_list::set(statextlist::RelationGetStatExtList);
    relcache_seams::relation_get_fkey_list::set(fkeylist::RelationGetFKeyList);
    relcache_seams::relation_get_index_attr_bitmap::set(indexattr::RelationGetIndexAttrBitmap);
    relcache_seams::relation_cache_invalidate::set(invalidate::RelationCacheInvalidate);
    relcache_seams::relation_cache_invalidate_entry::set(invalidate::RelationCacheInvalidateEntry);
    relcache_seams::relation_id_is_in_init_file::set(initfile::RelationIdIsInInitFile);
    relcache_seams::relation_cache_init_file_remove::set(initfile::RelationCacheInitFileRemove);
    relcache_seams::relation_cache_init_file_pre_invalidate::set(
        initfile::RelationCacheInitFilePreInvalidate,
    );
    relcache_seams::relation_cache_init_file_post_invalidate::set(
        initfile::RelationCacheInitFilePostInvalidate,
    );
    relcache_seams::at_eoxact_relation_cache::set(invalidate::AtEOXact_RelationCache);
    relcache_seams::at_eosubxact_relation_cache::set(invalidate::AtEOSubXact_RelationCache);
    relcache_seams::relation_get_rules::set(rules::RelationGetRulesShapes);
    relcache_seams::relation_get_deform_kernel::set(deform_jit::RelationGetDeformKernel);
}
