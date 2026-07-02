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
