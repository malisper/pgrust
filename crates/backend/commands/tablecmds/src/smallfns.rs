//! `CheckTableNotInUse` (tablecmds.c:4416), `SetRelationHasSubclass` (3647),
//! `CheckRelationTableSpaceMove` (3693), `SetRelationTableSpace` (3750).

#![allow(non_snake_case)]

use ::utils_error::ereport;
use ::mcx::Mcx;

use ::types_core::primitive::Oid;
use types_error::{
    PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_OBJECT_IN_USE,
    ERROR,
};
use ::rel::Relation;
use ::types_tuple::access::{RELKIND_INDEX, RELKIND_PARTITIONED_INDEX};

use tablecmds_seams as seam;

use crate::helpers::{here, GLOBALTABLESPACE_OID};

/// `CheckTableNotInUse(rel, stmt)` (tablecmds.c:4416). Installed as the inward
/// `check_table_not_in_use` seam.
/// `RELATION_IS_OTHER_TEMP(rel)` (rel.h:669): `rd_rel->relpersistence ==
/// RELPERSISTENCE_TEMP && !rel->rd_islocaltemp` — a temp table belonging to some
/// *other* session.
pub fn relation_is_other_temp(rel: &Relation<'_>) -> PgResult<bool> {
    Ok(rel.rd_rel.relpersistence == ::types_tuple::access::RELPERSISTENCE_TEMP
        && !relcache_seams::rd_islocaltemp::call(rel)?)
}

/// `RelationIsLogicallyLogged(relation)` (rel.h:712):
/// ```c
/// (XLogLogicalInfoActive() &&
///  RelationNeedsWAL(relation) &&
///  (relation)->rd_rel->relkind != RELKIND_FOREIGN_TABLE &&
///  !IsCatalogRelation(relation))
/// ```
/// `XLogLogicalInfoActive()` is `wal_level >= WAL_LEVEL_LOGICAL`; `RelationNeedsWAL`
/// reads `rd_createSubid`/`rd_firstRelfilelocatorSubid` (not on the trimmed
/// `RelationData`) plus the `wal_level` GUC, so it is the relcache owner's seam;
/// `IsCatalogRelation` is the catalog owner's. All three short-circuit, so the
/// in-hand `Relation` carries everything the predicate needs.
pub fn relation_is_logically_logged(rel: &Relation<'_>) -> PgResult<bool> {
    const RELKIND_FOREIGN_TABLE: u8 = b'f';
    Ok(transam_xlog_seams::xlog_logical_info_active::call()
        && relcache_seams::relation_needs_wal::call(rel)
        && rel.rd_rel.relkind != RELKIND_FOREIGN_TABLE
        && !catalog_seams::is_catalog_relation::call(rel))
}

pub fn check_table_not_in_use(rel: &Relation<'_>, stmt: &str) -> PgResult<()> {
    let expected_refcnt = if seam::relation_is_nailed::call(rel)? { 2 } else { 1 };
    if seam::relation_get_refcount::call(rel)? != expected_refcnt {
        return ereport(ERROR)
            .errcode(ERRCODE_OBJECT_IN_USE)
            .errmsg(format!(
                "cannot {stmt} \"{}\" because it is being used by active queries in this session",
                rel.name()
            ))
            .finish(here("CheckTableNotInUse"));
    }

    if rel.rd_rel.relkind != RELKIND_INDEX
        && rel.rd_rel.relkind != RELKIND_PARTITIONED_INDEX
        && seam::after_trigger_pending_on_rel::call(rel.rd_id)?
    {
        return ereport(ERROR)
            .errcode(ERRCODE_OBJECT_IN_USE)
            .errmsg(format!(
                "cannot {stmt} \"{}\" because it has pending trigger events",
                rel.name()
            ))
            .finish(here("CheckTableNotInUse"));
    }

    Ok(())
}

/// `SetRelationHasSubclass(relationId, relhassubclass)` (tablecmds.c:3647).
/// Installed as the inward `set_relation_has_subclass` seam. The syscache
/// modifiable-copy + GETSTRUCT mutation body crosses the outward
/// `set_relation_has_subclass_catalog` seam (see the seam crate).
pub fn set_relation_has_subclass<'mcx>(
    _mcx: Mcx<'mcx>,
    relation_id: Oid,
    relhassubclass: bool,
) -> PgResult<()> {
    seam::set_relation_has_subclass_catalog::call(relation_id, relhassubclass)
}

/// `CheckRelationTableSpaceMove(rel, newTableSpaceId)` (tablecmds.c:3693).
/// Returns true if a move is required, false for a no-op; raises otherwise.
/// Installed as the inward `check_relation_tablespace_move` seam.
pub fn check_relation_tablespace_move<'mcx>(
    rel: &Relation<'mcx>,
    new_tablespace_id: Oid,
) -> PgResult<bool> {
    /*
     * No work if no change in tablespace.  Note that MyDatabaseTableSpace is
     * stored as 0.
     */
    let old_tablespace_id = rel.rd_rel.reltablespace;
    if new_tablespace_id == old_tablespace_id
        || (new_tablespace_id == tablespace_globals_seams::MyDatabaseTableSpace::call()?
            && old_tablespace_id == ::types_core::primitive::InvalidOid)
    {
        return Ok(false);
    }

    /*
     * We cannot support moving mapped relations into different tablespaces.
     */
    if rel.is_mapped() {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot move system relation \"{}\"", rel.name()))
            .finish(here("CheckRelationTableSpaceMove"))
            .map(|()| false);
    }

    /* Cannot move a non-shared relation into pg_global */
    if new_tablespace_id == GLOBALTABLESPACE_OID {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("only shared relations can be placed in pg_global tablespace")
            .finish(here("CheckRelationTableSpaceMove"))
            .map(|()| false);
    }

    /*
     * Do not allow moving temp tables of other backends.
     */
    if seam::relation_is_other_temp::call(rel)? {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("cannot move temporary tables of other sessions")
            .finish(here("CheckRelationTableSpaceMove"))
            .map(|()| false);
    }

    Ok(true)
}

/// `SetRelationTableSpace(rel, newTableSpaceId, newRelFilenumber)`
/// (tablecmds.c:3750). Installed as the inward `set_relation_tablespace` seam.
/// The syscache modifiable-copy + CatalogTupleUpdate + changeDependencyOnTablespace
/// body crosses the outward `set_relation_tablespace_catalog` seam.
pub fn set_relation_tablespace<'mcx>(
    _mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    new_tablespace_id: Oid,
    new_relfilenumber: Oid,
) -> PgResult<()> {
    let reloid = rel.rd_id;

    debug_assert!(check_relation_tablespace_move(rel, new_tablespace_id)?);

    seam::set_relation_tablespace_catalog::call(
        reloid,
        rel.rd_rel.relkind,
        new_tablespace_id,
        new_relfilenumber,
    )
}

/// `SearchSysCache1(RELOID, relid)` projection of the `pg_class` fields the DROP
/// / TRUNCATE `RangeVarGetRelidExtended` callbacks read
/// (`RangeVarCallbackForDropRelation` / `RangeVarCallbackForTruncate`,
/// tablecmds.c). `Ok(None)` mirrors the C `!HeapTupleIsValid` (the relation was
/// concurrently dropped). Installed as the inward `get_pg_class_drop_info` seam.
///
/// C does one `SearchSysCache1(RELOID)` and reads every field off that tuple;
/// here the same `RELOID`-keyed syscache projections are read in turn. They all
/// resolve against the one cached pg_class tuple, so the cache-miss behavior is
/// consistent: the first read decides `None` (concurrently dropped) before any
/// field is consumed.
pub fn get_pg_class_drop_info(
    relid: Oid,
) -> PgResult<Option<seam::PgClassDropInfo>> {
    use syscache_seams as sc;

    // First read gates the whole projection on the tuple's presence (the C
    // `!HeapTupleIsValid(tuple)` early return).
    let Some(relkind) = sc::rel_relkind::call(relid)? else {
        return Ok(None);
    };
    let relpersistence = sc::pg_class_extra::call(relid)?
        .map(|e| e.relpersistence)
        .ok_or_else(|| {
            ::types_error::PgError::error("get_pg_class_drop_info: pg_class tuple vanished mid-read")
        })?;
    let relispartition = sc::rel_relispartition::call(relid)?.ok_or_else(|| {
        ::types_error::PgError::error("get_pg_class_drop_info: pg_class tuple vanished mid-read")
    })?;
    let relnamespace = sc::rel_namespace::call(relid)?.ok_or_else(|| {
        ::types_error::PgError::error("get_pg_class_drop_info: pg_class tuple vanished mid-read")
    })?;

    let scratch = ::mcx::MemoryContext::new("get_pg_class_drop_info relname");
    let relname = sc::rel_name::call(scratch.mcx(), relid)?
        .map(|s| s.as_str().to_string())
        .ok_or_else(|| {
            ::types_error::PgError::error("get_pg_class_drop_info: pg_class tuple vanished mid-read")
        })?;

    Ok(Some(seam::PgClassDropInfo {
        relkind,
        relpersistence,
        relispartition,
        relnamespace,
        relname,
    }))
}

/// `IsSystemClass(relid, classform)` (catalog.c) for the DROP / TRUNCATE / RENAME
/// `RangeVarGetRelidExtended` callbacks, which hold only a `Form_pg_class`
/// projection (no open relation). `IsSystemClass` reads the form solely for its
/// `relnamespace` (via `IsToastClass`), so this is exactly
/// `IsSystemClassByNamespace(relid, relnamespace)`; `relkind` is part of the C
/// signature but unread. Installed as the inward `is_system_class_relid` seam.
pub fn is_system_class_relid(relid: Oid, _relkind: u8, relnamespace: Oid) -> PgResult<bool> {
    Ok(catalog_catalog::IsSystemClassByNamespace(relid, relnamespace))
}

/// The inline `pg_index` lookup in `RangeVarCallbackForDropRelation`
/// (tablecmds.c): for a system index that might have been invalidated by a
/// failed concurrent process, fetch its `indisvalid` flag.
///
/// ```c
/// locTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(relOid));
/// if (!HeapTupleIsValid(locTuple))
/// {
///     /* Index relation is gone (concurrent drop), so we can just return. */
///     ReleaseSysCache(tuple);
///     return;
/// }
/// indexform = (Form_pg_index) GETSTRUCT(locTuple);
/// /* Mark object as being an invalid index of system catalogs */
/// if (!indexform->indisvalid)
///     state->invalid_system_index = true;
/// ReleaseSysCache(locTuple);
/// ```
///
/// Returns `Ok(None)` when the `pg_index` row is gone (the C early `return`,
/// signalling the caller to bypass the drop), or `Ok(Some(indisvalid))`.
pub fn get_index_isvalid(relid: Oid) -> PgResult<Option<bool>> {
    use syscache_seams as sc;
    Ok(sc::pg_index_flags::call(relid)?.map(|flags| flags.indisvalid))
}
