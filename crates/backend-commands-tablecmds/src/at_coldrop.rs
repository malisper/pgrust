//! `commands/tablecmds.c` — ALTER TABLE DROP COLUMN executed family.
//!
//! PORTED here (faithful, 100% C logic):
//!   - `ATPrepDropColumn` (tablecmds.c:9256)
//!   - `ATExecDropColumn` (tablecmds.c:9283) — marks the attribute dropped by
//!     deleting it (and everything depending on it) through the dependency
//!     machinery (`performMultipleDeletions`); `RemoveAttributeById` (reached by
//!     the deletion) is what flips `attisdropped`. Inheritance children are
//!     recursed one level at a time, decrementing `attinhcount` (and setting
//!     `attislocal` for ONLY drops) on surviving child columns.

#![allow(non_snake_case)]

extern crate alloc;

use mcx::{Mcx, PgVec};

use types_catalog::catalog_dependency::ObjectAddress;
use types_catalog::pg_attribute::{
    AttributeRelationId, Anum_pg_attribute_attinhcount, Anum_pg_attribute_attislocal,
    Anum_pg_attribute_attnum, PgAttributeUpdateRow,
};
use types_core::primitive::AttrNumber;
use types_error::{
    PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_TABLE_DEFINITION, ERRCODE_UNDEFINED_COLUMN,
    ERROR, NOTICE,
};
use types_nodes::ddlnodes::AlterTableType::AT_DropColumn;
use types_nodes::ddlnodes::AlterTableCmd;
use types_nodes::parsenodes::DropBehavior;
use types_rel::Relation;
use types_storage::lock::{NoLock, RowExclusiveLock, LOCKMODE};
use types_tuple::access::{RELKIND_COMPOSITE_TYPE, RELKIND_PARTITIONED_TABLE};
use types_tuple::heaptuple::FirstLowInvalidHeapAttributeNumber;

use backend_access_common_relation::relation_open;
use backend_catalog_dependency_seams as dep_seam;
use backend_catalog_indexing_seams as indexing_seam;
use backend_catalog_pg_inherits::find_inheritance_children;
use backend_utils_cache_syscache::SearchSysCacheAttName;

use crate::at_phase::{
    ATSimplePermissions, AlterTableUtilityContext, CheckAlterTableIsSafe, ATT_FOREIGN_TABLE,
    ATT_PARTITIONED_TABLE, ATT_TABLE,
};
use crate::helpers::{here, RelationRelationId};

use backend_access_common_heaptuple::FormedTuple;
use backend_utils_cache_syscache::{SysCacheGetAttrNotNull, ATTNAME};

const BITS_PER_BITMAPWORD: i32 = 64;

/// `bms_make_singleton(x)` (bitmapset.c) in the `types_pathnodes::Bitmapset`
/// (`bitmapword[]`) layout `has_partition_attrs` consumes.
pub(crate) fn bms_make_singleton(x: i32) -> types_pathnodes::Bitmapset {
    let wordnum = (x / BITS_PER_BITMAPWORD) as usize;
    let bitnum = (x % BITS_PER_BITMAPWORD) as u32;
    let mut words = alloc::vec![0u64; wordnum + 1];
    words[wordnum] = 1u64 << bitnum;
    types_pathnodes::Bitmapset { words }
}

/// `GETSTRUCT(tuple)->field` for a non-null `int2` `pg_attribute` column.
fn att_i16(mcx: Mcx<'_>, tup: &FormedTuple<'_>, anum: i16) -> PgResult<i16> {
    Ok(SysCacheGetAttrNotNull(mcx, ATTNAME, tup, anum as i32)?.as_i16())
}

/// `GETSTRUCT(tuple)->field` for a non-null `bool` `pg_attribute` column.
fn att_bool(mcx: Mcx<'_>, tup: &FormedTuple<'_>, anum: i16) -> PgResult<bool> {
    Ok(SysCacheGetAttrNotNull(mcx, ATTNAME, tup, anum as i32)?.as_bool())
}

/// `ATPrepDropColumn(wqueue, rel, recurse, recursing, cmd, lockmode, context)`
/// (tablecmds.c:9256).
pub fn ATPrepDropColumn<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, crate::at_phase::AlteredTableInfo<'mcx>>,
    rel: &Relation<'mcx>,
    recurse: bool,
    recursing: bool,
    cmd: &mut AlterTableCmd<'mcx>,
    lockmode: LOCKMODE,
    context: &AlterTableUtilityContext<'_>,
) -> PgResult<()> {
    // if (rel->rd_rel->reloftype && !recursing) ereport(cannot drop column from
    // typed table). reloftype is not carried on the relcache rd_rel; read it
    // through the syscache projection (InvalidOid for an ordinary table).
    let reloftype =
        backend_utils_cache_syscache_seams::search_relation_reloftype::call(rel.rd_id)?
            .unwrap_or(types_core::InvalidOid);
    if reloftype != types_core::InvalidOid && !recursing {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(types_error::ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("cannot drop column from typed table".to_string())
            .into_error());
    }

    if rel.rd_rel.relkind == RELKIND_COMPOSITE_TYPE {
        crate::at_phase::ATTypedTableRecursion(mcx, wqueue, rel, cmd, lockmode, context)?;
    }

    if recurse {
        cmd.recurse = true;
    }
    Ok(())
}

/// `ATExecDropColumn(wqueue, rel, colName, behavior, recurse, recursing,
/// missing_ok, lockmode, addrs)` (tablecmds.c:9283). Returns the address of the
/// dropped column.
#[allow(clippy::too_many_arguments)]
pub fn ATExecDropColumn<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    colName: &str,
    behavior: DropBehavior,
    recurse: bool,
    recursing: bool,
    missing_ok: bool,
) -> PgResult<ObjectAddress> {
    // The C threads an `ObjectAddresses *addrs` through the recursion to collect
    // every (parent + child) column address and delete them all at the end. The
    // owned model builds the accumulator at the top-level invocation and passes
    // it down; here we express the same with an explicit accumulator.
    let mut addrs = dep_seam::new_object_addresses::call()?;
    let object = drop_column_recurse(
        mcx, rel, colName, behavior, recurse, recursing, missing_ok, &mut addrs,
    )?;

    // Recursion has ended, drop everything that was collected.
    dep_seam::perform_multiple_deletions::call(&addrs.refs, behavior, 0)?;
    dep_seam::free_object_addresses::call(addrs)?;

    Ok(object)
}

/// The recursive core of `ATExecDropColumn`. Collects column addresses into
/// `addrs`; the top-level caller performs the deletions.
#[allow(clippy::too_many_arguments)]
fn drop_column_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    colName: &str,
    behavior: DropBehavior,
    recurse: bool,
    recursing: bool,
    missing_ok: bool,
    addrs: &mut types_catalog::catalog_dependency::ObjectAddresses,
) -> PgResult<ObjectAddress> {
    // At top level, permission check was done in ATPrepCmd, else do it.
    if recursing {
        ATSimplePermissions(
            AT_DropColumn,
            rel,
            ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
        )?;
    }

    // get the number of the attribute.
    let tuple = match SearchSysCacheAttName(mcx, rel.rd_id, colName)? {
        Some(t) => t,
        None => {
            if !missing_ok {
                return Err(backend_utils_error::ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_COLUMN)
                    .errmsg(format!(
                        "column \"{}\" of relation \"{}\" does not exist",
                        colName,
                        rel.name()
                    ))
                    .into_error());
            } else {
                backend_utils_error::ereport(NOTICE)
                    .errmsg(format!(
                        "column \"{}\" of relation \"{}\" does not exist, skipping",
                        colName,
                        rel.name()
                    ))
                    .finish(here("ATExecDropColumn"))?;
                return Ok(ObjectAddress {
                    classId: types_core::InvalidOid,
                    objectId: types_core::InvalidOid,
                    objectSubId: 0,
                });
            }
        }
    };

    let attnum: AttrNumber = att_i16(mcx, &tuple, Anum_pg_attribute_attnum)?;
    let attinhcount = att_i16(mcx, &tuple, Anum_pg_attribute_attinhcount)?;

    // Can't drop a system attribute.
    if attnum <= 0 {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot drop system column \"{colName}\""))
            .into_error());
    }

    // Don't drop inherited columns, unless recursing.
    if attinhcount > 0 && !recursing {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
            .errmsg(format!("cannot drop inherited column \"{colName}\""))
            .into_error());
    }

    // Don't drop columns used in the partition key, either.
    // bms_make_singleton(attnum - FirstLowInvalidHeapAttributeNumber).
    let singleton = bms_make_singleton((attnum - FirstLowInvalidHeapAttributeNumber) as i32);
    let (is_part_attr, _is_expr) =
        backend_catalog_partition_seams::has_partition_attrs::call(mcx, rel, Some(&singleton))?;
    if is_part_attr {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
            .errmsg(format!(
                "cannot drop column \"{}\" because it is part of the partition key of relation \"{}\"",
                colName,
                rel.name()
            ))
            .into_error());
    }

    // ReleaseSysCache(tuple) — drop the held tuple.
    drop(tuple);

    // Propagate to children as appropriate (one level of recursion at a time).
    let children = find_inheritance_children(mcx, rel.rd_id, RowExclusiveLock)?;

    if !children.is_empty() {
        // In a partitioned table, the column must be dropped from partitions too.
        if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE && !recurse {
            return Err(backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg(
                    "cannot drop column from only the partitioned table when partitions exist"
                        .to_string(),
                )
                .errhint("Do not specify the ONLY keyword.".to_string())
                .into_error());
        }

        let attr_rel = relation_open(mcx, AttributeRelationId, RowExclusiveLock)?;
        for &childrelid in children.iter() {
            // find_inheritance_children already got lock.
            let childrel = relation_open(mcx, childrelid, NoLock)?;
            CheckAlterTableIsSafe(&childrel)?;

            let tuple = SearchSysCacheAttName(mcx, childrelid, colName)?.ok_or_else(|| {
                backend_utils_error::PgError::error(format!(
                    "cache lookup failed for attribute \"{colName}\" of relation {childrelid}"
                ))
            })?;
            let childinhcount = att_i16(mcx, &tuple, Anum_pg_attribute_attinhcount)?;
            let childislocal = att_bool(mcx, &tuple, Anum_pg_attribute_attislocal)?;

            if childinhcount <= 0 {
                return Err(backend_utils_error::PgError::error(format!(
                    "relation {childrelid} has non-inherited attribute \"{colName}\""
                )));
            }

            if recurse {
                // If the child column has other definition sources, just
                // decrement its inheritance count; else recurse to delete it.
                if childinhcount == 1 && !childislocal {
                    drop(tuple);
                    drop_column_recurse(
                        mcx, &childrel, colName, behavior, true, true, false, addrs,
                    )?;
                } else {
                    // Child column must survive my deletion.
                    let row = PgAttributeUpdateRow {
                        attinhcount: Some(childinhcount - 1),
                        ..Default::default()
                    };
                    indexing_seam::catalog_tuple_update_pg_attribute::call(
                        mcx, &attr_rel, &tuple, &row,
                    )?;
                    backend_access_transam_xact::CommandCounterIncrement()?;
                }
            } else {
                // ONLY in this table: mark inheritors' attributes as locally
                // defined rather than inherited.
                let row = PgAttributeUpdateRow {
                    attinhcount: Some(childinhcount - 1),
                    attislocal: Some(true),
                    ..Default::default()
                };
                indexing_seam::catalog_tuple_update_pg_attribute::call(mcx, &attr_rel, &tuple, &row)?;
                backend_access_transam_xact::CommandCounterIncrement()?;
            }

            // heap_freetuple(tuple) + table_close(childrel, NoLock).
            drop(childrel);
        }
        drop(attr_rel);
    }

    // Add object to delete.
    let object = ObjectAddress {
        classId: RelationRelationId,
        objectId: rel.rd_id,
        objectSubId: attnum as i32,
    };
    dep_seam::add_exact_object_address::call(object, addrs)?;

    Ok(object)
}
