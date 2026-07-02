use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;
use types_rel::RelationData;
use types_storage::PrepareToInvalidateCacheTuple;
use types_tuple::HeapTupleData;

seam_core::seam!(
    // PrepareToInvalidateCacheTuple (catcache.c) with the per-catcache
    // callback inverted into returned requests (it may lazily initialize a
    // catcache and re-enter inval, so the caller replays outside its borrow).
    pub fn prepare_to_invalidate_cache_tuple<'mcx>(
        mcx: Mcx<'mcx>,
        relation: &RelationData<'_>,
        tuple: &HeapTupleData<'_>,
        newtuple: Option<&HeapTupleData<'_>>,
    ) -> PgResult<PgVec<'mcx, PrepareToInvalidateCacheTuple>>
);

seam_core::seam!(
    pub fn catalog_cache_flush_catalog(cat_id: Oid) -> PgResult<()>
);

seam_core::seam!(
    pub fn reset_catalog_caches_ext(debug_discard: bool) -> PgResult<()>
);
