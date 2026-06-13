//! initfile family — backend relcache bring-up, local-relation creation,
//! relfilenumber assignment, and the relcache init-file BINARY CODEC
//! (reclaimed in-crate) (OWN logic).
//!
//! SCAFFOLD: signatures mirror the C surface; bodies are `todo!()` EXCEPT
//! [`RelationCacheInitialize`], whose dynahash-creation half is real substrate
//! (the C `RelationIdCache` setup) and delegates to
//! [`crate::core_entry_store::create_id_cache`]. The init-file codec
//! (`load`/`write_relcache_init_file`) was previously seamed out of the crate;
//! it is reclaimed here and lands in full when this family is filled.

use backend_utils_error::PgResult;
use types_core::primitive::Oid;

use crate::core_entry_store::entry::RelationData;

/// `RelationCacheInitialize()` (relcache.c): create the `RelationIdCache`
/// dynahash and reserve `in_progress_list` (no catalog access). The dynahash
/// creation is real substrate; the relation-map init is seamed.
pub fn RelationCacheInitialize() -> PgResult<()> {
    // Real substrate: create the RelationIdCache (the C
    // `hash_create("Relcache by OID", ...)`). `in_progress_list` is a `Vec`
    // that grows on demand, so no fixed pre-reservation is required.
    crate::core_entry_store::create_id_cache()?;
    // RelationMapInitialize(): relation-map owner seam (lands with that owner).
    Ok(())
}

/// `RelationCacheInitializePhase2()` (relcache.c): load relcache entries for the
/// shared system catalogs (from the shared init file, else hardcoded). **Own
/// logic**; catalog access is seamed.
pub fn RelationCacheInitializePhase2() -> PgResult<()> {
    todo!("relcache-initfile: RelationCacheInitializePhase2 (own logic)")
}

/// `RelationCacheInitializePhase3()` (relcache.c): load the nailed-in
/// system-catalog entries (real catalog access). **Own logic.**
pub fn RelationCacheInitializePhase3() -> PgResult<()> {
    todo!("relcache-initfile: RelationCacheInitializePhase3 (own logic)")
}

/// `load_critical_index(indexoid, heapoid)` (relcache.c): nail one critical
/// system-catalog index into the cache during phase 3. **Own logic.**
pub fn load_critical_index(_indexoid: Oid, _heapoid: Oid) -> PgResult<()> {
    todo!("relcache-initfile: load_critical_index (own logic)")
}

/// `RelationBuildLocalRelation(relname, relnamespace, tupDesc, relid,
/// accessmtd, relfilenumber, reltablespace, shared_relation, mapped_relation,
/// relpersistence, relkind)` (relcache.c): build a relcache entry for a
/// brand-new relation without catalog access. **Own logic.**
pub fn RelationBuildLocalRelation(
    _relname: &str,
    _relnamespace: Oid,
    _relid: Oid,
    _reltablespace: Oid,
    _shared_relation: bool,
    _mapped_relation: bool,
    _relpersistence: i8,
    _relkind: i8,
) -> PgResult<*mut RelationData> {
    todo!("relcache-initfile: RelationBuildLocalRelation (own logic)")
}

/// `RelationSetNewRelfilenumber(relation, persistence)` (relcache.c): assign a
/// new relfilenumber/storage to an existing relation. **Own logic.**
pub fn RelationSetNewRelfilenumber(_relation: *mut RelationData, _persistence: i8) -> PgResult<()> {
    todo!("relcache-initfile: RelationSetNewRelfilenumber (own logic)")
}

/// `RelationAssumeNewRelfilelocator(relation)` (relcache.c): update the
/// `rd_*Subid` tracking after an external relfilenumber change. **Own logic.**
pub fn RelationAssumeNewRelfilelocator(_relation: *mut RelationData) -> PgResult<()> {
    todo!("relcache-initfile: RelationAssumeNewRelfilelocator (own logic)")
}

/// `write_relcache_init_file(shared)` (relcache.c): serialize the nailed
/// relcache entries to the on-disk init file. The BINARY CODEC is reclaimed
/// in-crate (it was seamed out before). **Own logic.**
pub fn write_relcache_init_file(_shared: bool) -> PgResult<()> {
    todo!("relcache-initfile: write_relcache_init_file BINARY CODEC (own logic)")
}

/// `load_relcache_init_file(shared)` (relcache.c): deserialize the nailed
/// relcache entries from the on-disk init file; returns `false` to signal a
/// rebuild-from-catalog. The BINARY CODEC is reclaimed in-crate. **Own logic.**
pub fn load_relcache_init_file(_shared: bool) -> PgResult<bool> {
    todo!("relcache-initfile: load_relcache_init_file BINARY CODEC (own logic)")
}

/// `RelationIdIsInInitFile(relationId)` (relcache.c): is the relation one whose
/// entry is cached in the init file? **Own logic.**
pub fn RelationIdIsInInitFile(_relationId: Oid) -> bool {
    todo!("relcache-initfile: RelationIdIsInInitFile (own logic)")
}

/// `RelationCacheInitFilePreInvalidate()` (relcache.c): take `RelCacheInitLock`
/// and unlink the init file before sending invalidations. **Own logic.**
pub fn RelationCacheInitFilePreInvalidate() -> PgResult<()> {
    todo!("relcache-initfile: RelationCacheInitFilePreInvalidate (own logic)")
}

/// `RelationCacheInitFilePostInvalidate()` (relcache.c): release
/// `RelCacheInitLock` after invalidations are sent. **Own logic.**
pub fn RelationCacheInitFilePostInvalidate() -> PgResult<()> {
    todo!("relcache-initfile: RelationCacheInitFilePostInvalidate (own logic)")
}

/// `RelationCacheInitFileRemove()` (relcache.c): remove stale init files at
/// startup. **Own logic.**
pub fn RelationCacheInitFileRemove() -> PgResult<()> {
    todo!("relcache-initfile: RelationCacheInitFileRemove (own logic)")
}
