//! build family — `RelationBuildDesc` orchestration (IN-CRATE) and the
//! descriptor-assembly subroutines.
//!
//! SCAFFOLD: signatures mirror the C surface; bodies are `todo!()`. The
//! orchestration (`RelationBuildDesc`, `AllocateRelationDesc`,
//! `RelationBuildTupleDesc`, `RelationParseRelOptions`, `formrdesc` +
//! `BuildHardcodedDescriptor`, `AttrDefaultFetch`, `CheckNNConstraintFetch`)
//! is relcache's OWN logic and lands here in full when this family is filled.
//! ONLY the catalog-scan primitive (`ScanPgRelation`: `systable_beginscan`/
//! `getnext` via genam, `SearchSysCache` via catcache) is a genuine cross-unit
//! seam, routed through its owner.

use backend_utils_error::PgResult;
use mcx::Mcx;
use types_core::primitive::Oid;

use crate::core_entry_store::entry::RelationData;

/// Project the owned relcache entry into the cross-unit
/// [`types_rel::RelationData`] value-slice, copied into `mcx` (the C "copy the
/// consumed slice of the entry into the caller's memory context"). This is the
/// build family's projection half, used by the `relation_id_get_relation`
/// seam. **Own logic.**
pub(crate) fn project_relation_data<'mcx>(
    _mcx: Mcx<'mcx>,
    _rd: *mut RelationData,
) -> PgResult<types_rel::RelationData<'mcx>> {
    todo!("relcache-build: project owned entry into cross-unit value-slice (own logic)")
}

/// `RelationBuildDesc(targetRelId, insertIt)` (relcache.c): assemble a fresh
/// relcache entry for `targetRelId` by reading `pg_class` (via
/// [`ScanPgRelation`]), build its tuple descriptor, parse reloptions,
/// initialize index/table access info, and (if `insertIt`) install it in the
/// `RelationIdCache`. Returns the built `Relation` (the C pointer), or `null`
/// when no `pg_class` row exists. **Own orchestration.**
pub fn RelationBuildDesc(_targetRelId: Oid, _insertIt: bool) -> PgResult<*mut RelationData> {
    todo!("relcache-build: RelationBuildDesc orchestration (own logic)")
}

/// `ScanPgRelation(targetRelId, indexOK, force_non_historic)` (relcache.c):
/// fetch the `pg_class` heap tuple for `targetRelId`. The scan itself
/// (`systable_beginscan`/`systable_getnext` or `SearchSysCache`) is the genuine
/// cross-unit seam (genam/catcache owners); this routine's caller orchestration
/// is own logic. Returns the owned `pg_class` form for the found row.
pub fn ScanPgRelation(
    _targetRelId: Oid,
    _indexOK: bool,
    _force_non_historic: bool,
) -> PgResult<Option<crate::core_entry_store::entry::FormPgClass>> {
    todo!("relcache-build: ScanPgRelation (catalog scan seamed via genam/catcache)")
}

/// `AllocateRelationDesc(relp)` (relcache.c): `palloc0` a fresh descriptor and
/// copy the `pg_class` form into `rd_rel`. **Own logic.**
pub fn AllocateRelationDesc(
    _relp: crate::core_entry_store::entry::FormPgClass,
) -> PgResult<Box<RelationData>> {
    todo!("relcache-build: AllocateRelationDesc (own logic)")
}

/// `RelationBuildTupleDesc(relation)` (relcache.c): build `rd_att` from
/// `pg_attribute` (+ attrdef/notnull constraint fetches). **Own logic**; the
/// `pg_attribute` scan is the seamed catalog primitive.
pub fn RelationBuildTupleDesc(_relation: *mut RelationData) -> PgResult<()> {
    todo!("relcache-build: RelationBuildTupleDesc (own logic)")
}

/// `RelationParseRelOptions(relation, tuple)` (relcache.c): parse
/// `pg_class.reloptions` into `rd_options`. **Own logic.**
pub fn RelationParseRelOptions(_relation: *mut RelationData) -> PgResult<()> {
    todo!("relcache-build: RelationParseRelOptions (own logic)")
}

/// `formrdesc(relationName, relationReltype, isshared, natts, attrs)`
/// (relcache.c): build a hardcoded bootstrap relcache entry for a nailed
/// system catalog without catalog access. **Own logic.**
pub fn formrdesc(
    _relationName: &str,
    _relationReltype: Oid,
    _isshared: bool,
    _natts: i32,
) -> PgResult<*mut RelationData> {
    todo!("relcache-build: formrdesc + BuildHardcodedDescriptor (own logic)")
}

/// `AttrDefaultFetch(relation, ndef)` (relcache.c): load column default
/// expressions from `pg_attrdef`. **Own logic**; the scan is seamed.
pub fn AttrDefaultFetch(_relation: *mut RelationData, _ndef: i32) -> PgResult<()> {
    todo!("relcache-build: AttrDefaultFetch (own logic)")
}

/// `CheckNNConstraintFetch(relation)` (relcache.c): load not-null constraint
/// info from `pg_constraint`. **Own logic**; the scan is seamed.
pub fn CheckNNConstraintFetch(_relation: *mut RelationData) -> PgResult<()> {
    todo!("relcache-build: CheckNNConstraintFetch (own logic)")
}
