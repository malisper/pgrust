//! `CheckTableNotInUse` (tablecmds.c:4416), `SetRelationHasSubclass` (3647),
//! `CheckRelationTableSpaceMove` (3693), `SetRelationTableSpace` (3750).

#![allow(non_snake_case)]

use backend_utils_error::ereport;
use mcx::Mcx;

use types_core::primitive::Oid;
use types_error::{
    PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_OBJECT_IN_USE,
    ERROR,
};
use types_rel::Relation;
use types_tuple::access::{RELKIND_INDEX, RELKIND_PARTITIONED_INDEX};

use backend_commands_tablecmds_seams as seam;

use crate::helpers::{here, GLOBALTABLESPACE_OID};

/// `CheckTableNotInUse(rel, stmt)` (tablecmds.c:4416). Installed as the inward
/// `check_table_not_in_use` seam.
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
        || (new_tablespace_id == backend_commands_tablespace_globals_seams::MyDatabaseTableSpace::call()?
            && old_tablespace_id == types_core::primitive::InvalidOid)
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
