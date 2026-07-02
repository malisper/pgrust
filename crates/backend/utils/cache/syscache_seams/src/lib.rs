use types_core::Oid;
use types_error::PgResult;
use types_storage::PgClassShape;
use types_tuple::{HeapTupleData, PgTypeShape};

seam_core::seam!(
    pub fn search_syscache_exists_reloid(reloid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    pub fn sys_cache_invalidate(cache_id: i32, hash_value: u32) -> PgResult<()>
);

seam_core::seam!(
    // RelationInvalidatesSnapshotsOnly (syscache.c).
    pub fn relation_invalidates_snapshots_only(relid: Oid) -> bool
);

seam_core::seam!(
    // SearchSysCache1(RELOID, relid) projected to (oid, relisshared);
    // None mirrors !HeapTupleIsValid(tup).
    pub fn lookup_pg_class_by_relid(relid: Oid) -> PgResult<Option<PgClassShape>>
);

seam_core::seam!(
    // GETSTRUCT(tuple) as Form_pg_class, projected to (oid, relisshared).
    pub fn pg_class_shape(tuple: &HeapTupleData<'_>) -> PgClassShape
);

seam_core::seam!(
    pub fn pg_attribute_attrelid(tuple: &HeapTupleData<'_>) -> Oid
);

seam_core::seam!(
    pub fn pg_index_indexrelid(tuple: &HeapTupleData<'_>) -> Oid
);

seam_core::seam!(
    // Some(conrelid) iff contype == CONSTRAINT_FOREIGN && OidIsValid(conrelid).
    pub fn pg_constraint_fk_target(tuple: &HeapTupleData<'_>) -> Option<Oid>
);

seam_core::seam!(
    // SearchSysCache1(TYPEOID, typid) projected to TupleDescInitEntry's reads;
    // None mirrors !HeapTupleIsValid(tup).
    pub fn lookup_pg_type_shape(typid: Oid) -> PgResult<Option<PgTypeShape>>
);
