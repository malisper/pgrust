//! index family — index/table access-method initialization (OWN logic).
//!
//! SCAFFOLD: signatures mirror the C surface; bodies are `todo!()`. The
//! index-AM-init tree (`RelationInitIndexAccessInfo`, `IndexSupportInitialize`,
//! `LookupOpclassInfo` + opclass cache, `InitIndexAmRoutine`,
//! `InitTableAmRoutine`, `RelationInitTableAccessMethod`,
//! `RelationReloadIndexInfo`/`Nailed`, `RelationInitPhysicalAddr`) is relcache's
//! OWN logic. Only `SearchSysCache`/`fmgr` handler lookups are seamed.

use backend_utils_error::PgResult;
use types_core::primitive::Oid;

use crate::core_entry_store::entry::RelationData;

/// `OpClassCacheEnt` (relcache.c): the per-(opclass,am) cached support-proc OID
/// array + supportProcs valid mask. Held in the relcache-owned `OpClassCache`
/// dynahash. **Own logic** (filled when this family lands).
pub struct OpClassCacheEnt {
    pub opclassoid: Oid,
    pub supportProcs: Vec<Oid>,
    pub valid: bool,
}

/// `RelationInitIndexAccessInfo(relation)` (relcache.c): set up an index
/// relation's `rd_indam`/`rd_opfamily`/`rd_opcintype`/`rd_support`/
/// `rd_indoption`/`rd_indcollation` from `pg_index`/`pg_opclass`/`pg_am`.
pub fn RelationInitIndexAccessInfo(_relation: *mut RelationData) -> PgResult<()> {
    todo!("relcache-index: RelationInitIndexAccessInfo (own logic)")
}

/// `IndexSupportInitialize(indclass, indfamily, opcintype, support, ...)`
/// (relcache.c): fill the support-proc OID arrays from the opclass cache.
pub fn IndexSupportInitialize(_relation: *mut RelationData) -> PgResult<()> {
    todo!("relcache-index: IndexSupportInitialize (own logic)")
}

/// `LookupOpclassInfo(operatorClassOid, numSupport)` (relcache.c): the
/// `OpClassCache` lookup/build for an opclass's default support procs.
pub fn LookupOpclassInfo(_operatorClassOid: Oid, _numSupport: i32) -> PgResult<OpClassCacheEnt> {
    todo!("relcache-index: LookupOpclassInfo + opclass cache (own logic)")
}

/// `InitIndexAmRoutine(relation)` (relcache.c): resolve and cache the index
/// AM's `IndexAmRoutine` vtable into `rd_indam` (handler call is seamed).
pub fn InitIndexAmRoutine(_relation: *mut RelationData) -> PgResult<()> {
    todo!("relcache-index: InitIndexAmRoutine (own logic)")
}

/// `InitTableAmRoutine(relation)` (relcache.c): resolve and cache the table
/// AM's `TableAmRoutine` vtable into `rd_tableam` (handler call is seamed).
pub fn InitTableAmRoutine(_relation: *mut RelationData) -> PgResult<()> {
    todo!("relcache-index: InitTableAmRoutine (own logic)")
}

/// `RelationInitTableAccessMethod(relation)` (relcache.c): set `rd_tableam`
/// for a table-like relation (or leave it `None` for views/foreign tables).
pub fn RelationInitTableAccessMethod(_relation: *mut RelationData) -> PgResult<()> {
    todo!("relcache-index: RelationInitTableAccessMethod (own logic)")
}

/// `RelationReloadIndexInfo(relation)` (relcache.c): refresh a non-nailed
/// index entry's `pg_class`/`pg_index` fields in place during rebuild.
pub fn RelationReloadIndexInfo(_relation: *mut RelationData) -> PgResult<()> {
    todo!("relcache-index: RelationReloadIndexInfo (own logic)")
}

/// `RelationReloadNailed(relation)` (relcache.c): refresh a nailed entry's
/// `pg_class` fields in place during rebuild.
pub fn RelationReloadNailed(_relation: *mut RelationData) -> PgResult<()> {
    todo!("relcache-index: RelationReloadNailed (own logic)")
}

/// `RelationInitPhysicalAddr(relation)` (relcache.c): compute `rd_locator`
/// from `pg_class.reltablespace`/`relfilenode` (or the relation map for mapped
/// relations). **Own logic** (relation-map read is seamed).
pub fn RelationInitPhysicalAddr(_relation: *mut RelationData) -> PgResult<()> {
    todo!("relcache-index: RelationInitPhysicalAddr (own logic)")
}
