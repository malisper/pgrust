use std::rc::Rc;
use types_core::Oid;
use types_error::PgResult;
use types_rel::RelationData;

seam_core::seam!(
    pub fn relation_id_get_relation(
        relation_id: Oid,
    ) -> PgResult<Option<Rc<RelationData<'static>>>>
);

seam_core::seam!(
    // RelationGetIndexList (relcache.c), OID list form; the caller holds the
    // relation open. The rd_indexlist cache lives with the implementation.
    pub fn relation_get_index_list<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relid: Oid,
    ) -> PgResult<mcx::PgVec<'mcx, Oid>>
);

seam_core::seam!(
    pub fn relation_get_stat_ext_list<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relid: Oid,
    ) -> PgResult<mcx::PgVec<'mcx, Oid>>
);

seam_core::seam!(
    // RelationGetFKeyList (relcache.c): rd_fkeylist; shared read-only slice,
    // scan (index) order — C promises no particular order.
    pub fn relation_get_fkey_list(
        relid: Oid,
    ) -> PgResult<Rc<[types_rel::ForeignKeyCacheInfo]>>
);

seam_core::seam!(
    pub fn relation_get_trigger_desc(
        relid: Oid,
    ) -> PgResult<Option<Rc<types_trigger::TriggerDesc<'static>>>>
);

seam_core::seam!(
    pub fn relation_cache_invalidate(debug_discard: bool) -> PgResult<()>
);

seam_core::seam!(
    pub fn relation_cache_invalidate_entry(relid: Oid) -> PgResult<()>
);

seam_core::seam!(
    pub fn relation_id_is_in_init_file(relid: Oid) -> bool
);

seam_core::seam!(
    pub fn relation_cache_init_file_pre_invalidate() -> PgResult<()>
);

seam_core::seam!(
    pub fn relation_cache_init_file_post_invalidate() -> PgResult<()>
);

seam_core::seam!(
    pub fn at_eoxact_relation_cache(is_commit: bool) -> types_error::PgResult<()>
);

seam_core::seam!(
    // criticalRelcachesBuilt (relcache.c file-scope flag; read by IndexScanOK).
    pub fn critical_relcaches_built() -> bool
);

seam_core::seam!(
    // criticalSharedRelcachesBuilt (relcache.c; read by IndexScanOK).
    pub fn critical_shared_relcaches_built() -> bool
);

seam_core::seam!(
    pub fn at_eosubxact_relation_cache(
        is_commit: bool,
        my_subid: types_core::SubTransactionId,
        parent_subid: types_core::SubTransactionId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    // RelationCacheInitFileRemove() (relcache.c init file half).
    pub fn relation_cache_init_file_remove()
);

// Attnum lists stand in for C's rd_attrsvalid bitmapsets (indexed attnums are
// few); sorted ascending, deduplicated.
pub struct IndexAttrBitmaps {
    pub hot_blocking: mcx::PgVec<'static, i16>,
    pub summarized: mcx::PgVec<'static, i16>,
    pub key: mcx::PgVec<'static, i16>,
    pub pk: mcx::PgVec<'static, i16>,
    pub identity: mcx::PgVec<'static, i16>,
}

seam_core::seam!(
    // RelationGetIndexAttrBitmap (relcache.c), all five kinds at once; cached
    // on the implementation side, invalidation clears it.
    pub fn relation_get_index_attr_bitmap(
        relid: Oid,
    ) -> PgResult<Rc<IndexAttrBitmaps>>
);

// RewriteRuleMeta marshal shape (relcache::rules), owned copies for the seam
// boundary (cold path: currtid_for_view).
pub struct RuleShape {
    pub event: i32,
    pub is_instead: bool,
    pub action_src: String,
}

seam_core::seam!(
    // RelationGetRules (relcache.c rd_rules), SELECT-event rules only need
    // this shape; empty Vec == rd_rules == NULL.
    pub fn relation_get_rules(relid: Oid) -> PgResult<Vec<RuleShape>>
);

seam_core::seam!(
    // Deform-JIT kernel for the current relcache entry's leading `ncols`
    // fixed columns (docs/optimizations/jit-deform.md); cached on the
    // implementation side, dropped by relcache invalidation. None = refused
    // (shape, arch, kill switch, arena) — callers keep the interpreted path.
    // Callers holding a possibly-stale relation MUST check
    // kernel.matches(rel.rd_att) before arming.
    pub fn relation_get_deform_kernel(
        relid: Oid,
        ncols: u16,
    ) -> Option<Rc<jit_deform::DeformKernel>>
);
