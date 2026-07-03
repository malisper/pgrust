#![allow(non_snake_case)]

pub mod build;
pub mod indexlist;
mod trigdesc;
pub mod initfile;
pub mod invalidate;
pub mod local;
pub mod indexattr;
pub mod rules;
pub mod schemapg;
pub mod store;
#[cfg(test)]
mod tests;

use core::cell::RefCell;
use core::mem::ManuallyDrop;
use std::rc::Rc;

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
pub use rules::RelationGetRules;
pub use store::RelationIdGetRelation;

pub const MAX_EOXACT_LIST: usize = 32;
const INITRELCACHESIZE: usize = 400;

// C rd_refcnt maps onto the entry Rc: user refs = strong_count - 1 (the cache
// itself holds one), and a nailed entry's permanent rd_refcnt=1 is the
// `nailed` flag. RelationHasReferenceCountZero(rel) == strong_count is 1 plus
// the probe clones the caller currently holds.
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
    pub(crate) indexattr_cache:
        PgHashMap<'static, Oid, std::rc::Rc<relcache_seams::IndexAttrBitmaps>>,
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
            let mcx = Box::leak(Box::new(MemoryContext::new("CacheMemoryContext"))).mcx();
            ManuallyDrop::new(RelcacheState {
                mcx,
                id_cache: PgHashMap::with_capacity_in(INITRELCACHESIZE, mcx),
                rules_cache: PgHashMap::new_in(mcx),
                indexattr_cache: PgHashMap::new_in(mcx),
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
}
