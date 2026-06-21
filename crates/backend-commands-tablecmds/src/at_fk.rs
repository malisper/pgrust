//! `commands/tablecmds.c` — the ADD FOREIGN KEY subcommand family.
//!
//! Ported faithfully from PostgreSQL 18.3:
//!   - `ATAddForeignKeyConstraint` (tablecmds.c:10067) — validate the FK
//!     (columns exist, referenced columns form a unique index, types match via
//!     the PK/FK opclass operators), create the `pg_constraint` 'f' entry, and
//!     install the RI enforcement triggers.
//!   - `validateFkOnDeleteSetColumns` (tablecmds.c:10646).
//!   - `addFkConstraint` (tablecmds.c:10722) — `CreateConstraintEntry` for the
//!     FK, plus the partitioning dependency entries.
//!   - `addFkRecurseReferenced` (tablecmds.c:10900) / `addFkRecurseReferencing`
//!     (tablecmds.c:11038) — create the action / check triggers and recurse to
//!     partitions (the partitioned-table recursion is fully ported, including
//!     `tryAttachPartitionForeignKey` / `AttachPartitionForeignKey` and the
//!     `CloneForeignKeyConstraints` / `CloneFk{Referenced,Referencing}`
//!     ATTACH/CREATE-partition cloners).
//!   - `transformColumnNameList` (tablecmds.c:13327),
//!     `transformFkeyGetPrimaryKey` (tablecmds.c:13382),
//!     `transformFkeyCheckAttrs` (tablecmds.c:13485),
//!     `findFkeyCast` (tablecmds.c:13636),
//!     `checkFkeyPermissions` (tablecmds.c:13665).
//!   - `CreateFKCheckTrigger` (tablecmds.c:13790),
//!     `createForeignKeyActionTriggers` (tablecmds.c:13857),
//!     `createForeignKeyCheckTriggers` (tablecmds.c:13992).

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

use mcx::{Mcx, PgString, PgVec};

use types_catalog::catalog_dependency::{
    ObjectAddress, DEPENDENCY_INTERNAL, DEPENDENCY_PARTITION_PRI, DEPENDENCY_PARTITION_SEC,
};
use types_catalog::pg_constraint::CONSTRAINT_FOREIGN;
use types_core::primitive::{AttrNumber, InvalidOid, Oid, OidIsValid};
use types_error::{
    PgResult, ERRCODE_COLLATION_MISMATCH, ERRCODE_DATATYPE_MISMATCH, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_COLUMN_REFERENCE, ERRCODE_INVALID_FOREIGN_KEY, ERRCODE_INVALID_TABLE_DEFINITION,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_SYNTAX_ERROR, ERRCODE_TOO_MANY_COLUMNS,
    ERRCODE_UNDEFINED_COLUMN, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_OBJECT_DEFINITION, ERROR,
};
use types_nodes::ddlnodes::{Constraint, ConstrType, CoercionContext, CreateTrigStmt};
use types_nodes::rawnodes::RangeVar as DdlRangeVar;
use types_nodes::nodes::{Node, NodePtr};
use types_rel::Relation;
use types_storage::lock::LOCKMODE;
use types_tuple::access::{
    ATTRIBUTE_GENERATED_VIRTUAL, RELKIND_FOREIGN_TABLE, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
    RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP, RELPERSISTENCE_UNLOGGED,
};

use backend_access_transam_xact::CommandCounterIncrement;
use backend_catalog_objectaddress::consts::{ConstraintRelationId, RelationRelationId};
use types_catalog::pg_trigger::{
    Anum_pg_trigger_oid, Anum_pg_trigger_tgconstraint, Anum_pg_trigger_tgconstrrelid,
    Anum_pg_trigger_tgfoid, Anum_pg_trigger_tgrelid, Anum_pg_trigger_tgtype,
    TriggerConstraintIndexId, TriggerRelationId, TRIGGER_FOR_DELETE, TRIGGER_FOR_INSERT,
    TRIGGER_FOR_UPDATE,
};
use backend_utils_adt_ri_triggers::checks::ri_fkey_trigger_type;
use backend_utils_adt_ri_triggers::{RI_TRIGGER_FK, RI_TRIGGER_PK};
use backend_access_common_scankey::ScanKeyInit;
use backend_access_common_heaptuple::heap_deform_tuple;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::{AccessExclusiveLock, RowExclusiveLock, RowShareLock};

use crate::at_phase::{AlteredTableInfo, NewConstraint, ATGetQueueEntry, CheckAlterTableIsSafe};

/// `INDEX_MAX_KEYS` (pg_config_manual.h).
const INDEX_MAX_KEYS: usize = 32;

/// FK action codes (`parsenodes.h`).
const FKCONSTR_ACTION_NOACTION: i8 = b'a' as i8;
const FKCONSTR_ACTION_RESTRICT: i8 = b'r' as i8;
const FKCONSTR_ACTION_CASCADE: i8 = b'c' as i8;
const FKCONSTR_ACTION_SETNULL: i8 = b'n' as i8;
const FKCONSTR_ACTION_SETDEFAULT: i8 = b'd' as i8;

const ShareRowExclusiveLock: LOCKMODE = 6;
const NoLock: LOCKMODE = 0;

/// `COMPARE_EQ` / `COMPARE_OVERLAP` (`cmptype.h`) — the AM-independent compare
/// types we pass to `IndexAmTranslateCompareType`.
const COMPARE_EQ: i32 = 3;
const COMPARE_OVERLAP: i32 = 8;
const InvalidStrategy: i16 = 0;

// ===========================================================================
// transformColumnNameList (tablecmds.c:13327)
// ===========================================================================

/// `transformColumnNameList(relId, colList, attnums, atttypids, attcollids)`
/// — look up each named referencing/referenced column, recording its attnum,
/// type, and collation.  Returns the number of columns.
fn transformColumnNameList(
    mcx: Mcx<'_>,
    rel_id: Oid,
    col_list: &PgVec<'_, NodePtr<'_>>,
    attnums: &mut [AttrNumber],
    mut atttypids: Option<&mut [Oid]>,
    mut attcollids: Option<&mut [Oid]>,
) -> PgResult<i32> {
    let mut attnum: usize = 0;
    for col in col_list.iter() {
        let attname = col.expect_string().sval.as_str();

        // atttuple = SearchSysCacheAttName(relId, attname); — resolved through
        // the lsyscache attribute helpers (attnum + type/collation).
        let att = backend_utils_cache_lsyscache::attribute::get_attnum(rel_id, attname)?;
        if att == InvalidOid as AttrNumber {
            return Err(backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!(
                    "column \"{attname}\" referenced in foreign key constraint does not exist"
                ))
                .into_error());
        }
        if att < 0 {
            return Err(backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("system columns cannot be used in foreign keys".to_string())
                .into_error());
        }
        if attnum >= INDEX_MAX_KEYS {
            return Err(backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_TOO_MANY_COLUMNS)
                .errmsg(format!(
                    "cannot have more than {INDEX_MAX_KEYS} keys in a foreign key"
                ))
                .into_error());
        }
        attnums[attnum] = att;
        if let Some(t) = atttypids.as_deref_mut() {
            t[attnum] = backend_utils_cache_lsyscache::attribute::get_atttype(rel_id, att)?;
        }
        if let Some(c) = attcollids.as_deref_mut() {
            let (_typ, _typmod, coll) =
                backend_utils_cache_lsyscache::attribute::get_atttypetypmodcoll(rel_id, att)?;
            c[attnum] = coll;
        }
        attnum += 1;
    }
    Ok(attnum as i32)
}

// ===========================================================================
// transformFkeyGetPrimaryKey (tablecmds.c:13382)
// ===========================================================================

/// `transformFkeyGetPrimaryKey(...)` — when REFERENCES omits the column list,
/// look up the referenced table's primary key: its attnums/types/collations,
/// the index OID, the index opclasses, and the attribute-name list to store
/// back on `fkconstraint->pk_attrs`.  Returns the number of PK attributes.
fn transformFkeyGetPrimaryKey<'mcx>(
    mcx: Mcx<'mcx>,
    pkrel: &Relation<'mcx>,
    index_oid: &mut Oid,
    attnamelist: &mut PgVec<'mcx, NodePtr<'mcx>>,
    attnums: &mut [AttrNumber],
    atttypids: &mut [Oid],
    attcollids: &mut [Oid],
    opclasses: &mut [Oid],
    pk_has_without_overlaps: &mut bool,
) -> PgResult<i32> {
    *index_oid = InvalidOid;

    let indexoidlist =
        backend_utils_cache_relcache::derived::RelationGetIndexList(pkrel.rd_id)?;

    let mut pk_info: Option<types_cache::PgIndexInfo<'mcx>> = None;
    for &indexoid in indexoidlist.iter() {
        let info = backend_utils_cache_syscache_seams::search_pg_index_info::call(mcx, indexoid)?
            .ok_or_else(|| {
                backend_utils_error::ereport(ERROR)
                    .errmsg_internal(format!("cache lookup failed for index {indexoid}"))
                    .into_error()
            })?;
        if info.indisprimary && info.indisvalid {
            // Refuse a deferrable primary key (per SQL spec).
            if !info.indimmediate {
                return Err(backend_utils_error::ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(format!(
                        "cannot use a deferrable primary key for referenced table \"{}\"",
                        pkrel.name()
                    ))
                    .into_error());
            }
            *index_oid = indexoid;
            pk_info = Some(info);
            break;
        }
    }

    if !OidIsValid(*index_oid) {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "there is no primary key for referenced table \"{}\"",
                pkrel.name()
            ))
            .into_error());
    }

    let info = pk_info.unwrap();
    let i = info.indnkeyatts as usize;
    for k in 0..i {
        let pkattno = info.indkey[k];
        attnums[k] = pkattno;
        atttypids[k] = backend_parser_relation::attnumTypeId(pkrel, pkattno as i32)?;
        attcollids[k] = backend_parser_relation::attnumCollationId(pkrel, pkattno as i32)?;
        opclasses[k] = info.indclass[k];
        let name = backend_parser_relation::attnumAttName(mcx, pkrel, pkattno as i32)?;
        attnamelist.push(make_string(mcx, name.as_str())?);
    }

    *pk_has_without_overlaps = info.indisexclusion;

    Ok(i as i32)
}

// ===========================================================================
// transformFkeyCheckAttrs (tablecmds.c:13485)
// ===========================================================================

/// `transformFkeyCheckAttrs(pkrel, numattrs, attnums, with_period, opclasses,
/// pk_has_without_overlaps)` — validate that the referenced columns form a
/// unique (or, for temporal, exclusion) index, returning that index's OID and
/// filling in the per-column opclasses.
fn transformFkeyCheckAttrs<'mcx>(
    mcx: Mcx<'mcx>,
    pkrel: &Relation<'mcx>,
    numattrs: i32,
    attnums: &[AttrNumber],
    with_period: bool,
    opclasses: &mut [Oid],
    pk_has_without_overlaps: &mut bool,
) -> PgResult<Oid> {
    let numattrs = numattrs as usize;
    let mut indexoid = InvalidOid;
    let mut found = false;
    let mut found_deferrable = false;

    // Reject duplicate appearances of columns in the referenced-columns list.
    for i in 0..numattrs {
        for j in (i + 1)..numattrs {
            if attnums[i] == attnums[j] {
                return Err(backend_utils_error::ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FOREIGN_KEY)
                    .errmsg(
                        "foreign key referenced-columns list must not contain duplicates"
                            .to_string(),
                    )
                    .into_error());
            }
        }
    }

    let indexoidlist =
        backend_utils_cache_relcache::derived::RelationGetIndexList(pkrel.rd_id)?;

    for &cand in indexoidlist.iter() {
        indexoid = cand;
        let info = backend_utils_cache_syscache_seams::search_pg_index_info::call(mcx, indexoid)?
            .ok_or_else(|| {
                backend_utils_error::ereport(ERROR)
                    .errmsg_internal(format!("cache lookup failed for index {indexoid}"))
                    .into_error()
            })?;

        let has_pred =
            backend_utils_cache_syscache_seams::pg_index_has_predicate::call(indexoid)?
                .unwrap_or(false);
        let (_tid, has_exprs) =
            backend_utils_cache_syscache_seams::pg_index_tid_and_hasexprs::call(indexoid)?
                .unwrap_or((Default::default(), false));

        let unique_ok = if with_period {
            info.indisexclusion
        } else {
            info.indisunique
        };

        if info.indnkeyatts as usize == numattrs
            && unique_ok
            && info.indisvalid
            && !has_pred
            && !has_exprs
        {
            // Match the attnum list against the index columns in any order,
            // extracting the opclasses while we're at it.
            found = true;
            for i in 0..numattrs {
                let mut col_found = false;
                for j in 0..numattrs {
                    if attnums[i] == info.indkey[j] {
                        opclasses[i] = info.indclass[j];
                        col_found = true;
                        break;
                    }
                }
                if !col_found {
                    found = false;
                    break;
                }
            }
            // The last attribute in the index must be the PERIOD FK part.
            if found && with_period {
                let periodattnum = attnums[numattrs - 1];
                found = periodattnum == info.indkey[numattrs - 1];
            }
            // Refuse a deferrable unique/primary key (per SQL spec).
            if found && !info.indimmediate {
                found_deferrable = true;
                found = false;
            }
            if found {
                *pk_has_without_overlaps = info.indisexclusion;
            }
        }
        if found {
            break;
        }
    }

    if !found {
        if found_deferrable {
            return Err(backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "cannot use a deferrable unique constraint for referenced table \"{}\"",
                    pkrel.name()
                ))
                .into_error());
        }
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_INVALID_FOREIGN_KEY)
            .errmsg(format!(
                "there is no unique constraint matching given keys for referenced table \"{}\"",
                pkrel.name()
            ))
            .into_error());
    }

    Ok(indexoid)
}

// ===========================================================================
// findFkeyCast (tablecmds.c:13636)
// ===========================================================================

/// `findFkeyCast(targetTypeId, sourceTypeId, &funcid)` — wrapper around
/// `find_coercion_pathway` for the constraint-revalidation path; treats an
/// exact match as a relabel.
fn findFkeyCast(
    target_type_id: Oid,
    source_type_id: Oid,
) -> PgResult<(backend_parser_coerce_seams::CoercionPathType, Oid)> {
    use backend_parser_coerce_seams::CoercionPathType;
    if target_type_id == source_type_id {
        Ok((CoercionPathType::Relabeltype, InvalidOid))
    } else {
        let (ret, funcid) = backend_parser_coerce::find_coercion_pathway(
            target_type_id,
            source_type_id,
            CoercionContext::COERCION_IMPLICIT,
        )?;
        if ret == CoercionPathType::None {
            return Err(backend_utils_error::ereport(ERROR)
                .errmsg_internal(format!(
                    "could not find cast from {source_type_id} to {target_type_id}"
                ))
                .into_error());
        }
        Ok((ret, funcid))
    }
}

// ===========================================================================
// checkFkeyPermissions (tablecmds.c:13665)
// ===========================================================================

/// `checkFkeyPermissions(rel, attnums, natts)` — REFERENCES privilege check on
/// the referenced table (relation-level, or per-column).
fn checkFkeyPermissions(rel: &Relation<'_>, attnums: &[AttrNumber], natts: i32) -> PgResult<()> {
    use types_acl::acl::{ACLCHECK_OK, ACL_REFERENCES};
    let roleid = backend_utils_init_miscinit::GetUserId();

    // Okay if we have relation-level REFERENCES permission.
    let r = backend_catalog_aclchk_seams::pg_class_aclcheck::call(rel.rd_id, roleid, ACL_REFERENCES)?;
    if r == ACLCHECK_OK {
        return Ok(());
    }
    // Else we must have REFERENCES on each column.
    for &att in attnums.iter().take(natts as usize) {
        let r = backend_catalog_aclchk_seams::pg_attribute_aclcheck::call(
            rel.rd_id, att, roleid, ACL_REFERENCES,
        )?;
        if r != ACLCHECK_OK {
            backend_catalog_aclchk_seams::aclcheck_error::call(
                r,
                backend_catalog_objectaddress::resolve::get_relkind_objtype(rel.rd_rel.relkind as u8),
                Some(rel.name().to_string()),
            )?;
        }
    }
    Ok(())
}

// ===========================================================================
// validateFkOnDeleteSetColumns (tablecmds.c:10646)
// ===========================================================================

/// `validateFkOnDeleteSetColumns(...)` — verify that columns named in an ON
/// DELETE SET NULL/DEFAULT (...) list are part of the FK; drops duplicates and
/// returns the deduplicated count.
fn validateFkOnDeleteSetColumns(
    mcx: Mcx<'_>,
    numfks: i32,
    fkattnums: &[AttrNumber],
    numfksetcols: i32,
    fksetcolsattnums: &mut [AttrNumber],
    fksetcols: &PgVec<'_, NodePtr<'_>>,
) -> PgResult<i32> {
    let _ = mcx;
    let mut numcolsout = 0usize;
    for i in 0..numfksetcols as usize {
        let setcol_attnum = fksetcolsattnums[i];

        // Make sure it's in fkattnums[].
        let mut seen = false;
        for j in 0..numfks as usize {
            if fkattnums[j] == setcol_attnum {
                seen = true;
                break;
            }
        }
        if !seen {
            let col = fksetcols[i].expect_string().sval.as_str();
            return Err(backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
                .errmsg(format!(
                    "column \"{col}\" referenced in ON DELETE SET action must be part of foreign key"
                ))
                .into_error());
        }

        // Now check for dups.
        seen = false;
        for j in 0..numcolsout {
            if fksetcolsattnums[j] == setcol_attnum {
                seen = true;
                break;
            }
        }
        if !seen {
            fksetcolsattnums[numcolsout] = setcol_attnum;
            numcolsout += 1;
        }
    }
    Ok(numcolsout as i32)
}

// ===========================================================================
// ATAddForeignKeyConstraint (tablecmds.c:10067)
// ===========================================================================

/// `ATAddForeignKeyConstraint(wqueue, tab, rel, fkconstraint, recurse,
/// recursing, lockmode)` — the ADD FOREIGN KEY entry.  Ports the
/// non-self-referential, non-temporal common case in full; the partitioned
/// recursion bottoms out in the addFkRecurse* helpers below.
pub fn ATAddForeignKeyConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    ti: usize,
    rel: &Relation<'mcx>,
    fkconstraint: &Constraint<'mcx>,
    recurse: bool,
    _recursing: bool,
    lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    let mut pkattnum = [0 as AttrNumber; INDEX_MAX_KEYS];
    let mut fkattnum = [0 as AttrNumber; INDEX_MAX_KEYS];
    let mut pktypoid = [InvalidOid; INDEX_MAX_KEYS];
    let mut fktypoid = [InvalidOid; INDEX_MAX_KEYS];
    let mut pkcolloid = [InvalidOid; INDEX_MAX_KEYS];
    let mut fkcolloid = [InvalidOid; INDEX_MAX_KEYS];
    let mut opclasses = [InvalidOid; INDEX_MAX_KEYS];
    let mut pfeqoperators = [InvalidOid; INDEX_MAX_KEYS];
    let mut ppeqoperators = [InvalidOid; INDEX_MAX_KEYS];
    let mut ffeqoperators = [InvalidOid; INDEX_MAX_KEYS];
    let mut fkdelsetcols = [0 as AttrNumber; INDEX_MAX_KEYS];

    // We work with an owned, mutable copy of the constraint: C scribbles
    // conname / pk_attrs onto fkconstraint as it goes.
    let mut fkconstraint = fkconstraint.clone_in(mcx)?;

    // Grab ShareRowExclusiveLock on the pk table.
    let pkrel = if OidIsValid(fkconstraint.old_pktable_oid) {
        backend_access_table_table_seams::table_open::call(
            mcx,
            fkconstraint.old_pktable_oid,
            ShareRowExclusiveLock,
        )?
    } else {
        let rv_node = fkconstraint.pktable.as_deref().ok_or_else(|| {
            backend_utils_error::ereport(ERROR)
                .errmsg_internal("ADD FOREIGN KEY: no pktable RangeVar")
                .into_error()
        })?;
        let rv = rv_node.as_rangevar().ok_or_else(|| {
            backend_utils_error::ereport(ERROR)
                .errmsg_internal("ADD FOREIGN KEY: pktable is not a RangeVar")
                .into_error()
        })?;
        backend_access_table_table::table_openrv(mcx, &to_access_range_var(rv), ShareRowExclusiveLock)?
    };

    // Validity checks.
    if !recurse && rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "cannot use ONLY for foreign key on partitioned table \"{}\" referencing relation \"{}\"",
                rel.name(), pkrel.name()
            ))
            .into_error());
    }

    if pkrel.rd_rel.relkind != RELKIND_RELATION
        && pkrel.rd_rel.relkind != RELKIND_PARTITIONED_TABLE
    {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "referenced relation \"{}\" is not a table",
                pkrel.name()
            ))
            .into_error());
    }

    if !backend_commands_tablespace_globals_seams::allowSystemTableMods::call()?
        && backend_catalog_catalog::IsSystemRelation(&pkrel)
    {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "permission denied: \"{}\" is a system catalog",
                pkrel.name()
            ))
            .into_error());
    }

    // Persistence cross-checks.
    match rel.rd_rel.relpersistence {
        RELPERSISTENCE_PERMANENT => {
            if pkrel.rd_rel.relpersistence != RELPERSISTENCE_PERMANENT {
                return persistence_err("constraints on permanent tables may reference only permanent tables");
            }
        }
        RELPERSISTENCE_UNLOGGED => {
            if pkrel.rd_rel.relpersistence != RELPERSISTENCE_PERMANENT
                && pkrel.rd_rel.relpersistence != RELPERSISTENCE_UNLOGGED
            {
                return persistence_err("constraints on unlogged tables may reference only permanent or unlogged tables");
            }
        }
        RELPERSISTENCE_TEMP => {
            if pkrel.rd_rel.relpersistence != RELPERSISTENCE_TEMP {
                return persistence_err("constraints on temporary tables may reference only temporary tables");
            }
            // C: !pkrel->rd_islocaltemp || !rel->rd_islocaltemp — reject if
            // either side is another session's temp table.  rd_islocaltemp is
            // not modeled; `relation_is_other_temp` is its inverse for a temp.
            if backend_commands_tablecmds_seams::relation_is_other_temp::call(&pkrel)?
                || backend_commands_tablecmds_seams::relation_is_other_temp::call(rel)?
            {
                return persistence_err("constraints on temporary tables must involve temporary tables of this session");
            }
        }
        _ => {}
    }

    // Look up the referencing attributes.
    let numfks = transformColumnNameList(
        mcx,
        rel.rd_id,
        &fkconstraint.fk_attrs,
        &mut fkattnum,
        Some(&mut fktypoid),
        Some(&mut fkcolloid),
    )?;

    let with_period = fkconstraint.fk_with_period || fkconstraint.pk_with_period;
    if with_period && !fkconstraint.fk_with_period {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_INVALID_FOREIGN_KEY)
            .errmsg("foreign key uses PERIOD on the referenced table but not the referencing table".to_string())
            .into_error());
    }

    let mut numfkdelsetcols = transformColumnNameList(
        mcx,
        rel.rd_id,
        &fkconstraint.fk_del_set_cols,
        &mut fkdelsetcols,
        None,
        None,
    )?;
    numfkdelsetcols = validateFkOnDeleteSetColumns(
        mcx,
        numfks,
        &fkattnum,
        numfkdelsetcols,
        &mut fkdelsetcols,
        &fkconstraint.fk_del_set_cols,
    )?;

    // Resolve the referenced index / opclasses / pk attnums.
    let index_oid;
    let mut pk_has_without_overlaps = false;
    let numpks;
    if fkconstraint.pk_attrs.is_empty() {
        let mut pk_attrs: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
        let mut idx = InvalidOid;
        numpks = transformFkeyGetPrimaryKey(
            mcx,
            &pkrel,
            &mut idx,
            &mut pk_attrs,
            &mut pkattnum,
            &mut pktypoid,
            &mut pkcolloid,
            &mut opclasses,
            &mut pk_has_without_overlaps,
        )?;
        index_oid = idx;
        fkconstraint.pk_attrs = pk_attrs;
        if pk_has_without_overlaps && !fkconstraint.fk_with_period {
            return Err(backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_INVALID_FOREIGN_KEY)
                .errmsg("foreign key uses PERIOD on the referenced table but not the referencing table".to_string())
                .into_error());
        }
    } else {
        numpks = transformColumnNameList(
            mcx,
            pkrel.rd_id,
            &fkconstraint.pk_attrs,
            &mut pkattnum,
            Some(&mut pktypoid),
            Some(&mut pkcolloid),
        )?;
        if with_period && !fkconstraint.pk_with_period {
            return Err(backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_INVALID_FOREIGN_KEY)
                .errmsg("foreign key uses PERIOD on the referencing table but not the referenced table".to_string())
                .into_error());
        }
        index_oid = transformFkeyCheckAttrs(
            mcx,
            &pkrel,
            numpks,
            &pkattnum,
            with_period,
            &mut opclasses,
            &mut pk_has_without_overlaps,
        )?;
    }

    if pk_has_without_overlaps && !with_period {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_INVALID_FOREIGN_KEY)
            .errmsg("foreign key must use PERIOD when referencing a primary key using WITHOUT OVERLAPS".to_string())
            .into_error());
    }

    // Permissions on the referenced table.
    checkFkeyPermissions(&pkrel, &pkattnum, numpks)?;

    // Generated-column restrictions on the referencing columns.
    for i in 0..numfks as usize {
        let attgenerated = rel.rd_att.attr((fkattnum[i] - 1) as usize).attgenerated;
        if attgenerated != 0 {
            if fkconstraint.fk_upd_action == FKCONSTR_ACTION_SETNULL
                || fkconstraint.fk_upd_action == FKCONSTR_ACTION_SETDEFAULT
                || fkconstraint.fk_upd_action == FKCONSTR_ACTION_CASCADE
            {
                return generated_action_err("ON UPDATE");
            }
            if fkconstraint.fk_del_action == FKCONSTR_ACTION_SETNULL
                || fkconstraint.fk_del_action == FKCONSTR_ACTION_SETDEFAULT
            {
                return generated_action_err("ON DELETE");
            }
        }
        if attgenerated == ATTRIBUTE_GENERATED_VIRTUAL {
            return Err(backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("foreign key constraints on virtual generated columns are not supported".to_string())
                .into_error());
        }
    }

    // PERIOD action restrictions.
    if fkconstraint.fk_with_period {
        let bad = |a: i8| {
            a == FKCONSTR_ACTION_RESTRICT
                || a == FKCONSTR_ACTION_CASCADE
                || a == FKCONSTR_ACTION_SETNULL
                || a == FKCONSTR_ACTION_SETDEFAULT
        };
        if bad(fkconstraint.fk_upd_action) {
            return period_action_err("ON UPDATE");
        }
        if bad(fkconstraint.fk_del_action) {
            return period_action_err("ON DELETE");
        }
    }

    // Look up the equality operators to use in the constraint.
    if numfks != numpks {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_INVALID_FOREIGN_KEY)
            .errmsg("number of referencing and referenced columns for foreign key disagree".to_string())
            .into_error());
    }

    // old_check_ok = (fkconstraint->old_conpfeqop != NIL).
    let old_check_ok = !fkconstraint.old_conpfeqop.is_empty();
    debug_assert!(!old_check_ok || numfks as usize == fkconstraint.old_conpfeqop.len());
    let mut old_check_ok = old_check_ok;
    let mut old_pfeqop_idx = 0usize;

    for i in 0..numpks as usize {
        let pktype = pktypoid[i];
        let fktype = fktypoid[i];
        let pkcoll = pkcolloid[i];
        let fkcoll = fkcolloid[i];

        // pg_opclass fields: opcmethod / opcfamily / opcintype.
        let amid = backend_utils_cache_lsyscache::opclass::get_opclass_method(opclasses[i])?;
        let (opfamily, opcintype) =
            backend_utils_cache_lsyscache::opclass::get_opclass_opfamily_and_input_type(
                opclasses[i],
            )?
            .ok_or_else(|| {
                backend_utils_error::ereport(ERROR)
                    .errmsg_internal(format!("cache lookup failed for opclass {}", opclasses[i]))
                    .into_error()
            })?;

        // Strategy number from the index AM.
        let for_overlaps = with_period && i == numpks as usize - 1;
        let cmptype = if for_overlaps { COMPARE_OVERLAP } else { COMPARE_EQ };
        let eqstrategy = backend_access_index_amapi_seams::index_am_translate_cmptype::call(
            cmptype, amid, opfamily, true,
        )?;
        if eqstrategy == InvalidStrategy {
            let amname = backend_utils_cache_lsyscache::namespace_range_index_pubsub::get_am_name(mcx, amid)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            let opfname = backend_utils_cache_lsyscache::opclass::get_opfamily_name(mcx, opfamily, false)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            return Err(backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(if for_overlaps {
                    "could not identify an overlaps operator for foreign key".to_string()
                } else {
                    "could not identify an equality operator for foreign key".to_string()
                })
                .errdetail(format!(
                    "Could not translate compare type {cmptype} for operator family \"{opfname}\" of access method \"{amname}\"."
                ))
                .into_error());
        }

        // ppeqop: PK = PK.
        let ppeqop = backend_utils_cache_lsyscache_seams::get_opfamily_member::call(
            opfamily, opcintype, opcintype, eqstrategy,
        )?;
        if !OidIsValid(ppeqop) {
            return Err(backend_utils_error::ereport(ERROR)
                .errmsg_internal(format!(
                    "missing operator {eqstrategy}({opcintype},{opcintype}) in opfamily {opfamily}"
                ))
                .into_error());
        }

        // pfeqop / ffeqop: equality operators that take the FK type.
        let fktyped = backend_utils_cache_lsyscache_seams::get_base_type::call(fktype)?;
        let mut pfeqop = backend_utils_cache_lsyscache_seams::get_opfamily_member::call(
            opfamily, opcintype, fktyped, eqstrategy,
        )?;
        let mut pfeqop_right;
        let mut ffeqop;
        if OidIsValid(pfeqop) {
            pfeqop_right = fktyped;
            ffeqop = backend_utils_cache_lsyscache_seams::get_opfamily_member::call(
                opfamily, fktyped, fktyped, eqstrategy,
            )?;
        } else {
            pfeqop_right = InvalidOid;
            ffeqop = InvalidOid;
        }

        if !(OidIsValid(pfeqop) && OidIsValid(ffeqop)) {
            // Look for an implicit cast from the FK type to the opcintype.
            let input_typeids = [pktype, fktype];
            let target_typeids = [opcintype, opcintype];
            if backend_parser_coerce_seams::can_coerce_type::call(
                2,
                &input_typeids,
                &target_typeids,
                CoercionContext::COERCION_IMPLICIT,
            )? {
                pfeqop = ppeqop;
                ffeqop = ppeqop;
                pfeqop_right = opcintype;
            }
        }

        if !(OidIsValid(pfeqop) && OidIsValid(ffeqop)) {
            let fkname = format_type(mcx, fktype)?;
            let pkname = format_type(mcx, pktype)?;
            let fkcolname = fkconstraint.fk_attrs[i].expect_string().sval.as_str().to_string();
            let pkcolname = fkconstraint.pk_attrs[i].expect_string().sval.as_str().to_string();
            return Err(backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!(
                    "foreign key constraint \"{}\" cannot be implemented",
                    fkconstraint.conname.as_ref().map(|s| s.as_str()).unwrap_or("")
                ))
                .errdetail(format!(
                    "Key columns \"{fkcolname}\" of the referencing table and \"{pkcolname}\" of the referenced table are of incompatible types: {fkname} and {pkname}."
                ))
                .into_error());
        }

        // Collation consistency.
        if (OidIsValid(pkcoll) && !OidIsValid(fkcoll))
            || (!OidIsValid(pkcoll) && OidIsValid(fkcoll))
        {
            return Err(backend_utils_error::ereport(ERROR)
                .errmsg_internal("key columns are not both collatable")
                .into_error());
        }
        if OidIsValid(pkcoll) && OidIsValid(fkcoll) {
            let pkcolldet =
                backend_utils_cache_lsyscache::collation_constraint_language_cast::get_collation_isdeterministic(pkcoll)?;
            let fkcolldet =
                backend_utils_cache_lsyscache::collation_constraint_language_cast::get_collation_isdeterministic(fkcoll)?;
            if (!pkcolldet || !fkcolldet) && pkcoll != fkcoll {
                let fkc = coll_name(mcx, fkcoll)?;
                let pkc = coll_name(mcx, pkcoll)?;
                let fkcolname = fkconstraint.fk_attrs[i].expect_string().sval.as_str().to_string();
                let pkcolname = fkconstraint.pk_attrs[i].expect_string().sval.as_str().to_string();
                return Err(backend_utils_error::ereport(ERROR)
                    .errcode(ERRCODE_COLLATION_MISMATCH)
                    .errmsg(format!(
                        "foreign key constraint \"{}\" cannot be implemented",
                        fkconstraint.conname.as_ref().map(|s| s.as_str()).unwrap_or("")
                    ))
                    .errdetail(format!(
                        "Key columns \"{fkcolname}\" of the referencing table and \"{pkcolname}\" of the referenced table have incompatible collations: \"{fkc}\" and \"{pkc}\".  If either collation is nondeterministic, then both collations have to be the same."
                    ))
                    .into_error());
            }
        }

        // Constraint-revalidation fast path (ALTER COLUMN TYPE rebuild). Only
        // reached when fkconstraint->old_conpfeqop was non-empty.
        if old_check_ok {
            // C: lfirst_oid(old_pfeqop_item) — the old pfeqop carried as an
            // Integer-node oid in the (cold) ALTER COLUMN TYPE revalidation list.
            let old_pfeqop = match fkconstraint.old_conpfeqop[old_pfeqop_idx].as_integer() {
                Some(i) => i.ival as Oid,
                None => InvalidOid,
            };
            old_check_ok = pfeqop == old_pfeqop;
            old_pfeqop_idx += 1;
        }
        if old_check_ok {
            let attr = wqueue[ti].oldDesc.attr((fkattnum[i] - 1) as usize);
            let old_fktype = attr.atttypid;
            let new_fktype = fktype;
            let (old_pathtype, old_castfunc) = findFkeyCast(pfeqop_right, old_fktype)?;
            let (new_pathtype, new_castfunc) = findFkeyCast(pfeqop_right, new_fktype)?;
            let old_fkcoll = attr.attcollation;
            let new_fkcoll = fkcoll;
            let poly = is_polymorphic_type(pfeqop_right);
            old_check_ok = new_pathtype == old_pathtype
                && new_castfunc == old_castfunc
                && (!poly || new_fktype == old_fktype)
                && (new_fkcoll == old_fkcoll
                    || (collation_is_det(old_fkcoll)? && collation_is_det(new_fkcoll)?));
        }

        pfeqoperators[i] = pfeqop;
        ppeqoperators[i] = ppeqop;
        ffeqoperators[i] = ffeqop;
    }

    // Temporal-FK PERIOD operator lookup (validated even if unused here).
    if with_period {
        let _ = backend_catalog_pg_constraint_seams::find_fk_period_opers::call(
            opclasses[numpks as usize - 1],
            numpks,
        )?;
    }

    let numfks = numfks as usize;

    // First, create the constraint catalog entry itself.
    let address = addFkConstraint(
        mcx,
        AddFkSides::Both,
        &mut fkconstraint,
        rel,
        &pkrel,
        index_oid,
        InvalidOid, // no parent constraint
        numfks,
        &pkattnum,
        &fkattnum,
        &pfeqoperators,
        &ppeqoperators,
        &ffeqoperators,
        numfkdelsetcols as usize,
        &fkdelsetcols,
        false,
        with_period,
    )?;

    // Referenced-side action triggers and recurse.
    addFkRecurseReferenced(
        mcx,
        &fkconstraint,
        rel,
        &pkrel,
        index_oid,
        address.objectId,
        numfks,
        &pkattnum,
        &fkattnum,
        &pfeqoperators,
        &ppeqoperators,
        &ffeqoperators,
        numfkdelsetcols as usize,
        &fkdelsetcols,
        old_check_ok,
        InvalidOid,
        InvalidOid,
        with_period,
    )?;

    // Referencing-side check triggers and recurse + phase-3 scheduling.
    addFkRecurseReferencing(
        mcx,
        wqueue,
        &fkconstraint,
        rel,
        &pkrel,
        index_oid,
        address.objectId,
        numfks,
        &pkattnum,
        &fkattnum,
        &pfeqoperators,
        &ppeqoperators,
        &ffeqoperators,
        numfkdelsetcols as usize,
        &fkdelsetcols,
        old_check_ok,
        lockmode,
        InvalidOid,
        InvalidOid,
        with_period,
    )?;

    // Close pk table, keep lock until commit.
    pkrel.close(NoLock)?;

    Ok(address)
}

#[derive(Clone, Copy, PartialEq)]
enum AddFkSides {
    Both,
    Referenced,
    Referencing,
}

// ===========================================================================
// addFkConstraint (tablecmds.c:10722)
// ===========================================================================

fn addFkConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    fkside: AddFkSides,
    fkconstraint: &mut Constraint<'mcx>,
    rel: &Relation<'mcx>,
    pkrel: &Relation<'mcx>,
    index_oid: Oid,
    parent_constr: Oid,
    numfks: usize,
    pkattnum: &[AttrNumber],
    fkattnum: &[AttrNumber],
    pfeqoperators: &[Oid],
    ppeqoperators: &[Oid],
    ffeqoperators: &[Oid],
    numfkdelsetcols: usize,
    fkdelsetcols: &[AttrNumber],
    is_internal: bool,
    with_period: bool,
) -> PgResult<ObjectAddress> {
    // Verify relkind for each referenced partition (redundant at top level).
    if pkrel.rd_rel.relkind != RELKIND_RELATION
        && pkrel.rd_rel.relkind != RELKIND_PARTITIONED_TABLE
    {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "referenced relation \"{}\" is not a table",
                pkrel.name()
            ))
            .into_error());
    }

    // Pick a constraint name (unique within rel).
    let constraintname = fkconstraint
        .conname
        .as_ref()
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();
    let conname = if backend_catalog_pg_constraint::ConstraintNameIsUsed(
        mcx,
        types_catalog::pg_constraint::ConstraintCategory::Relation,
        rel.rd_id,
        &constraintname,
    )? {
        backend_catalog_pg_constraint::ChooseConstraintName(
            mcx,
            &constraintname,
            "",
            "",
            rel.rd_rel.relnamespace,
            &[],
        )?
    } else {
        constraintname.clone()
    };

    if fkconstraint.conname.is_none() {
        fkconstraint.conname = Some(PgString::from_str_in(&conname, mcx)?);
    }

    let (conislocal, coninhcount, connoinherit) = if OidIsValid(parent_constr) {
        (false, 1i16, false)
    } else {
        (
            true,
            0i16,
            rel.rd_rel.relkind != RELKIND_PARTITIONED_TABLE,
        )
    };

    // Record the FK constraint in pg_constraint.
    let constr_oid = backend_catalog_pg_constraint::CreateConstraintEntry(
        mcx,
        &conname,
        rel.rd_rel.relnamespace,
        CONSTRAINT_FOREIGN,
        fkconstraint.deferrable,
        fkconstraint.initdeferred,
        fkconstraint.is_enforced,
        fkconstraint.initially_valid,
        parent_constr,
        rel.rd_id,
        &fkattnum[..numfks],
        numfks as i32,
        numfks as i32,
        InvalidOid, // not a domain constraint
        index_oid,
        pkrel.rd_id,
        &pkattnum[..numfks],
        &pfeqoperators[..numfks],
        &ppeqoperators[..numfks],
        &ffeqoperators[..numfks],
        numfks as i32,
        fkconstraint.fk_upd_action,
        fkconstraint.fk_del_action,
        &fkdelsetcols[..numfkdelsetcols],
        numfkdelsetcols as i32,
        fkconstraint.fk_matchtype,
        None, // no exclusion constraint
        None, // no check constraint expr
        None,
        conislocal,
        coninhcount,
        connoinherit,
        with_period,
        is_internal,
    )?;

    let address = ObjectAddress {
        classId: ConstraintRelationId,
        objectId: constr_oid,
        objectSubId: 0,
    };

    // Partitioning dependency entries (only for subsidiary partition rows).
    if OidIsValid(parent_constr) {
        let referenced = ObjectAddress {
            classId: ConstraintRelationId,
            objectId: parent_constr,
            objectSubId: 0,
        };
        debug_assert!(fkside != AddFkSides::Both);
        if fkside == AddFkSides::Referenced {
            backend_catalog_pg_depend_seams::recordDependencyOn::call(
                mcx, &address, &referenced, DEPENDENCY_INTERNAL,
            )?;
        } else {
            backend_catalog_pg_depend_seams::recordDependencyOn::call(
                mcx, &address, &referenced, DEPENDENCY_PARTITION_PRI,
            )?;
            let rel_ref = ObjectAddress {
                classId: RelationRelationId,
                objectId: rel.rd_id,
                objectSubId: 0,
            };
            backend_catalog_pg_depend_seams::recordDependencyOn::call(
                mcx, &address, &rel_ref, DEPENDENCY_PARTITION_SEC,
            )?;
        }
    }

    // Make the new constraint visible.
    CommandCounterIncrement()?;

    Ok(address)
}

// ===========================================================================
// addFkRecurseReferenced (tablecmds.c:10900)
// ===========================================================================

fn addFkRecurseReferenced<'mcx>(
    mcx: Mcx<'mcx>,
    fkconstraint: &Constraint<'mcx>,
    rel: &Relation<'mcx>,
    pkrel: &Relation<'mcx>,
    index_oid: Oid,
    parent_constr: Oid,
    numfks: usize,
    pkattnum: &[AttrNumber],
    fkattnum: &[AttrNumber],
    pfeqoperators: &[Oid],
    ppeqoperators: &[Oid],
    ffeqoperators: &[Oid],
    numfkdelsetcols: usize,
    fkdelsetcols: &[AttrNumber],
    old_check_ok: bool,
    parent_del_trigger: Oid,
    parent_upd_trigger: Oid,
    with_period: bool,
) -> PgResult<()> {
    // Create action triggers to enforce the constraint (unless NOT ENFORCED).
    // The C captures the resulting delete/update trigger OIDs to pass as the
    // parent trigger OIDs when recursing to partitions.
    let mut delete_trigger_oid = InvalidOid;
    let mut update_trigger_oid = InvalidOid;
    if fkconstraint.is_enforced {
        let (del, upd) = createForeignKeyActionTriggers(
            mcx,
            rel.rd_id,
            pkrel.rd_id,
            fkconstraint,
            parent_constr,
            index_oid,
            parent_del_trigger,
            parent_upd_trigger,
        )?;
        delete_trigger_oid = del;
        update_trigger_oid = upd;
    }

    // If the referenced table is partitioned, recurse on ourselves to handle
    // each partition.  We need one pg_constraint row created for each partition
    // in addition to the pg_constraint row for the parent table.
    if pkrel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        let pd = backend_partitioning_partdesc::RelationGetPartitionDesc(mcx, pkrel, true)?;

        for i in 0..pd.nparts as usize {
            // XXX would it be better to acquire these locks beforehand?
            let part_rel = backend_access_table_table_seams::table_open::call(
                mcx,
                pd.oids[i],
                ShareRowExclusiveLock,
            )?;

            // Map the attribute numbers in the referenced side of the FK
            // definition to match the partition's column layout.
            let map = backend_access_common_next::attmap::build_attrmap_by_name_if_req(
                mcx,
                &part_rel.rd_att,
                &pkrel.rd_att,
                false,
            )?;
            let mut mapped_storage = [0 as AttrNumber; INDEX_MAX_KEYS];
            let mapped_pkattnum: &[AttrNumber] = if let Some(m) = map.as_ref() {
                for j in 0..numfks {
                    mapped_storage[j] = m.attnums[(pkattnum[j] - 1) as usize];
                }
                &mapped_storage[..numfks]
            } else {
                &pkattnum[..numfks]
            };

            // Determine the index to use at this level.
            let part_index_id =
                backend_catalog_partition::index_get_partition(&part_rel, index_oid)?;
            if !OidIsValid(part_index_id) {
                return Err(backend_utils_error::ereport(ERROR)
                    .errmsg_internal(format!(
                        "index for {index_oid} not found in partition {}",
                        part_rel.name()
                    ))
                    .into_error());
            }

            // Create entry at this level ...
            let address = addFkConstraint(
                mcx,
                AddFkSides::Referenced,
                &mut fkconstraint.clone_in(mcx)?,
                rel,
                &part_rel,
                part_index_id,
                parent_constr,
                numfks,
                mapped_pkattnum,
                fkattnum,
                pfeqoperators,
                ppeqoperators,
                ffeqoperators,
                numfkdelsetcols,
                fkdelsetcols,
                true,
                with_period,
            )?;

            // ... and recurse to our children.
            addFkRecurseReferenced(
                mcx,
                fkconstraint,
                rel,
                &part_rel,
                part_index_id,
                address.objectId,
                numfks,
                mapped_pkattnum,
                fkattnum,
                pfeqoperators,
                ppeqoperators,
                ffeqoperators,
                numfkdelsetcols,
                fkdelsetcols,
                old_check_ok,
                delete_trigger_oid,
                update_trigger_oid,
                with_period,
            )?;

            // Done -- clean up (but keep the lock).
            part_rel.close(NoLock)?;
        }
    }

    Ok(())
}

// ===========================================================================
// addFkRecurseReferencing (tablecmds.c:11038)
// ===========================================================================

fn addFkRecurseReferencing<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    fkconstraint: &Constraint<'mcx>,
    rel: &Relation<'mcx>,
    pkrel: &Relation<'mcx>,
    index_oid: Oid,
    parent_constr: Oid,
    numfks: usize,
    pkattnum: &[AttrNumber],
    fkattnum: &[AttrNumber],
    pfeqoperators: &[Oid],
    ppeqoperators: &[Oid],
    ffeqoperators: &[Oid],
    numfkdelsetcols: usize,
    fkdelsetcols: &[AttrNumber],
    old_check_ok: bool,
    lockmode: LOCKMODE,
    parent_ins_trigger: Oid,
    parent_upd_trigger: Oid,
    with_period: bool,
) -> PgResult<()> {
    debug_assert!(OidIsValid(parent_constr));

    if rel.rd_rel.relkind == RELKIND_FOREIGN_TABLE {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("foreign key constraints are not supported on foreign tables".to_string())
            .into_error());
    }

    // Add check triggers (unless NOT ENFORCED).  Capture the resulting
    // insert/update trigger OIDs to pass as parent OIDs when recursing.
    let mut insert_trigger_oid = InvalidOid;
    let mut update_trigger_oid = InvalidOid;
    if fkconstraint.is_enforced {
        let (ins, upd) = createForeignKeyCheckTriggers(
            mcx,
            rel.rd_id,
            pkrel.rd_id,
            fkconstraint,
            parent_constr,
            index_oid,
            parent_ins_trigger,
            parent_upd_trigger,
        )?;
        insert_trigger_oid = ins;
        update_trigger_oid = upd;
    }

    if rel.rd_rel.relkind == RELKIND_RELATION {
        // Tell Phase 3 to check the constraint against existing rows.
        if !old_check_ok && !fkconstraint.skip_validation && fkconstraint.is_enforced {
            let tab = ATGetQueueEntry(mcx, wqueue, rel)?;
            let name = backend_utils_cache_lsyscache::collation_constraint_language_cast::get_constraint_name(
                mcx, parent_constr,
            )?;
            // C carries fkconstraint on newcon->qual for the phase-3 validator;
            // the owned NewConstraint rides the constraint Node on `qual`.
            let qual = mcx::alloc_in(
                mcx,
                Node::mk_constraint(mcx, fkconstraint.clone_in(mcx)?)?,
            )?;
            let newcon = NewConstraint {
                name,
                contype: ConstrType::CONSTR_FOREIGN as i32,
                refrelid: pkrel.rd_id,
                refindid: index_oid,
                conid: parent_constr,
                qual: Some(qual),
            };
            wqueue[tab].constraints.push(newcon);
        }
    } else if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        let pd = backend_partitioning_partdesc::RelationGetPartitionDesc(mcx, rel, true)?;

        // Triggers of the foreign keys will be manipulated a bunch of times in
        // the loop below.  Open the trigger catalog once and pass it down.
        let trigrel = backend_access_table_table_seams::table_open::call(
            mcx,
            TriggerRelationId,
            RowExclusiveLock,
        )?;

        // Recurse to take appropriate action on each partition; either we find
        // an existing constraint to reparent to ours, or we create a new one.
        for i in 0..pd.nparts as usize {
            let partition = backend_access_table_table_seams::table_open::call(
                mcx,
                pd.oids[i],
                lockmode,
            )?;

            CheckAlterTableIsSafe(&partition)?;

            let attmap = backend_access_common_next::attmap::build_attrmap_by_name(
                mcx,
                &partition.rd_att,
                &rel.rd_att,
                false,
            )?;
            let mut mapped_fkattnum = [0 as AttrNumber; INDEX_MAX_KEYS];
            for j in 0..numfks {
                mapped_fkattnum[j] = attmap.attnums[(fkattnum[j] - 1) as usize];
            }

            // Check whether an existing constraint can be repurposed.
            let part_fks = backend_utils_cache_relcache::derived::RelationGetFKeyList(partition.rd_id)?;
            let mut attached = false;
            for fk in part_fks.iter() {
                if tryAttachPartitionForeignKey(
                    mcx,
                    wqueue,
                    fk,
                    &partition,
                    parent_constr,
                    numfks,
                    &mapped_fkattnum,
                    pkattnum,
                    pfeqoperators,
                    insert_trigger_oid,
                    update_trigger_oid,
                    &trigrel,
                )? {
                    attached = true;
                    break;
                }
            }
            if attached {
                partition.close(NoLock)?;
                continue;
            }

            // No luck finding a good constraint to reuse; create our own.
            let address = addFkConstraint(
                mcx,
                AddFkSides::Referencing,
                &mut fkconstraint.clone_in(mcx)?,
                &partition,
                pkrel,
                index_oid,
                parent_constr,
                numfks,
                pkattnum,
                &mapped_fkattnum[..numfks],
                pfeqoperators,
                ppeqoperators,
                ffeqoperators,
                numfkdelsetcols,
                fkdelsetcols,
                true,
                with_period,
            )?;

            // Call ourselves to finalize the creation and we're done.
            addFkRecurseReferencing(
                mcx,
                wqueue,
                fkconstraint,
                &partition,
                pkrel,
                index_oid,
                address.objectId,
                numfks,
                pkattnum,
                &mapped_fkattnum[..numfks],
                pfeqoperators,
                ppeqoperators,
                ffeqoperators,
                numfkdelsetcols,
                fkdelsetcols,
                old_check_ok,
                lockmode,
                insert_trigger_oid,
                update_trigger_oid,
                with_period,
            )?;

            partition.close(NoLock)?;
        }

        trigrel.close(RowExclusiveLock)?;
    }

    Ok(())
}

// ===========================================================================
// CreateFKCheckTrigger (tablecmds.c:13790)
// ===========================================================================

/// `CreateFKCheckTrigger(myRelOid, refRelOid, fkconstraint, constraintOid,
/// indexOid, parentTrigOid, on_insert)` — the referencing-side INSERT/UPDATE
/// "check" trigger (RI_FKey_check_ins / _upd).
fn CreateFKCheckTrigger<'mcx>(
    mcx: Mcx<'mcx>,
    my_rel_oid: Oid,
    ref_rel_oid: Oid,
    fkconstraint: &Constraint<'mcx>,
    constraint_oid: Oid,
    index_oid: Oid,
    parent_trig_oid: Oid,
    on_insert: bool,
) -> PgResult<Oid> {
    let (funcname, events) = if on_insert {
        (system_func_name(mcx, "RI_FKey_check_ins")?, TRIGGER_TYPE_INSERT)
    } else {
        (system_func_name(mcx, "RI_FKey_check_upd")?, TRIGGER_TYPE_UPDATE)
    };

    let fk_trigger = CreateTrigStmt {
        replace: false,
        isconstraint: true,
        trigname: Some(PgString::from_str_in("RI_ConstraintTrigger_c", mcx)?),
        relation: None,
        funcname,
        args: PgVec::new_in(mcx),
        row: true,
        timing: TRIGGER_TYPE_AFTER,
        events,
        columns: PgVec::new_in(mcx),
        whenClause: None,
        transitionRels: PgVec::new_in(mcx),
        deferrable: fkconstraint.deferrable,
        initdeferred: fkconstraint.initdeferred,
        constrrel: None,
    };

    let trig_address = backend_commands_trigger::create::CreateTrigger(
        mcx,
        &fk_trigger,
        "",
        my_rel_oid,
        ref_rel_oid,
        constraint_oid,
        index_oid,
        InvalidOid,
        parent_trig_oid,
        true,  // is_internal
        false, // in_partition
    )?;

    CommandCounterIncrement()?;
    Ok(trig_address.objectId)
}

// ===========================================================================
// createForeignKeyActionTriggers (tablecmds.c:13857)
// ===========================================================================

pub(crate) fn createForeignKeyActionTriggers<'mcx>(
    mcx: Mcx<'mcx>,
    my_rel_oid: Oid,
    ref_rel_oid: Oid,
    fkconstraint: &Constraint<'mcx>,
    constraint_oid: Oid,
    index_oid: Oid,
    parent_del_trigger: Oid,
    parent_upd_trigger: Oid,
) -> PgResult<(Oid, Oid)> {
    // ON DELETE action trigger.
    let (del_funcname, del_deferrable, del_initdeferred) =
        action_trigger_func(mcx, fkconstraint, fkconstraint.fk_del_action, false)?;
    let del_trigger = CreateTrigStmt {
        replace: false,
        isconstraint: true,
        trigname: Some(PgString::from_str_in("RI_ConstraintTrigger_a", mcx)?),
        relation: None,
        funcname: del_funcname,
        args: PgVec::new_in(mcx),
        row: true,
        timing: TRIGGER_TYPE_AFTER,
        events: TRIGGER_TYPE_DELETE,
        columns: PgVec::new_in(mcx),
        whenClause: None,
        transitionRels: PgVec::new_in(mcx),
        deferrable: del_deferrable,
        initdeferred: del_initdeferred,
        constrrel: None,
    };
    let del_addr = backend_commands_trigger::create::CreateTrigger(
        mcx, &del_trigger, "", ref_rel_oid, my_rel_oid, constraint_oid, index_oid, InvalidOid,
        parent_del_trigger, true, false,
    )?;
    let delete_trig_oid = del_addr.objectId;
    CommandCounterIncrement()?;

    // ON UPDATE action trigger.
    let (upd_funcname, upd_deferrable, upd_initdeferred) =
        action_trigger_func(mcx, fkconstraint, fkconstraint.fk_upd_action, true)?;
    let upd_trigger = CreateTrigStmt {
        replace: false,
        isconstraint: true,
        trigname: Some(PgString::from_str_in("RI_ConstraintTrigger_a", mcx)?),
        relation: None,
        funcname: upd_funcname,
        args: PgVec::new_in(mcx),
        row: true,
        timing: TRIGGER_TYPE_AFTER,
        events: TRIGGER_TYPE_UPDATE,
        columns: PgVec::new_in(mcx),
        whenClause: None,
        transitionRels: PgVec::new_in(mcx),
        deferrable: upd_deferrable,
        initdeferred: upd_initdeferred,
        constrrel: None,
    };
    let upd_addr = backend_commands_trigger::create::CreateTrigger(
        mcx, &upd_trigger, "", ref_rel_oid, my_rel_oid, constraint_oid, index_oid, InvalidOid,
        parent_upd_trigger, true, false,
    )?;
    let update_trig_oid = upd_addr.objectId;

    Ok((delete_trig_oid, update_trig_oid))
}

/// Map an FK action code to the RI action proc name and its
/// deferrable/initdeferred flags (only NO ACTION inherits the constraint's
/// deferral; the other actions fire immediately).  `on_update` selects the
/// `_upd` vs `_del` proc family.
fn action_trigger_func<'mcx>(
    mcx: Mcx<'mcx>,
    fkconstraint: &Constraint<'mcx>,
    action: i8,
    on_update: bool,
) -> PgResult<(PgVec<'mcx, NodePtr<'mcx>>, bool, bool)> {
    let suffix = if on_update { "upd" } else { "del" };
    let (name, deferrable, initdeferred) = match action {
        FKCONSTR_ACTION_NOACTION => (
            format!("RI_FKey_noaction_{suffix}"),
            fkconstraint.deferrable,
            fkconstraint.initdeferred,
        ),
        FKCONSTR_ACTION_RESTRICT => (format!("RI_FKey_restrict_{suffix}"), false, false),
        FKCONSTR_ACTION_CASCADE => (format!("RI_FKey_cascade_{suffix}"), false, false),
        FKCONSTR_ACTION_SETNULL => (format!("RI_FKey_setnull_{suffix}"), false, false),
        FKCONSTR_ACTION_SETDEFAULT => (format!("RI_FKey_setdefault_{suffix}"), false, false),
        other => {
            return Err(backend_utils_error::ereport(ERROR)
                .errmsg_internal(format!("unrecognized FK action type: {}", other as i32))
                .into_error());
        }
    };
    Ok((system_func_name(mcx, &name)?, deferrable, initdeferred))
}

// ===========================================================================
// createForeignKeyCheckTriggers (tablecmds.c:13992)
// ===========================================================================

pub(crate) fn createForeignKeyCheckTriggers<'mcx>(
    mcx: Mcx<'mcx>,
    my_rel_oid: Oid,
    ref_rel_oid: Oid,
    fkconstraint: &Constraint<'mcx>,
    constraint_oid: Oid,
    index_oid: Oid,
    parent_ins_trigger: Oid,
    parent_upd_trigger: Oid,
) -> PgResult<(Oid, Oid)> {
    let insert_trig_oid = CreateFKCheckTrigger(
        mcx, my_rel_oid, ref_rel_oid, fkconstraint, constraint_oid, index_oid,
        parent_ins_trigger, true,
    )?;
    let update_trig_oid = CreateFKCheckTrigger(
        mcx, my_rel_oid, ref_rel_oid, fkconstraint, constraint_oid, index_oid,
        parent_upd_trigger, false,
    )?;
    Ok((insert_trig_oid, update_trig_oid))
}

// ===========================================================================
// pg_trigger scan helpers (tablecmds.c)
// ===========================================================================

/// One scanned `pg_trigger` row's scalar columns, as the FK partition helpers
/// read them off a `tgconstraint`-keyed scan.
struct FkTriggerScanRow {
    tgoid: Oid,
    tgrelid: Oid,
    tgconstrrelid: Oid,
    tgfoid: Oid,
    tgtype: i16,
}

/// Scan `pg_trigger` for all rows whose `tgconstraint == conoid`, returning the
/// scalar columns the FK helpers inspect.  `trigrel` is an already-open
/// `pg_trigger` relation.
fn scan_fk_triggers_by_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    trigrel: &Relation<'mcx>,
    conoid: Oid,
) -> PgResult<Vec<FkTriggerScanRow>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        Anum_pg_trigger_tgconstraint,
        BTEqualStrategyNumber,
        types_core::fmgr::F_OIDEQ,
        types_tuple::backend_access_common_heaptuple::Datum::from_oid(conoid),
    )?;
    let keys = [key];

    let mut scan = backend_access_index_genam_seams::systable_beginscan::call(
        trigrel,
        TriggerConstraintIndexId,
        true,
        None,
        &keys,
    )?;

    let mut out = Vec::new();
    while let Some(tup) =
        backend_access_index_genam_seams::systable_getnext::call(mcx, scan.desc_mut())?
    {
        let cols = heap_deform_tuple(mcx, &tup.tuple, &trigrel.rd_att, &tup.data)?;
        let col = |attno: i16| cols[attno as usize - 1].0.clone();
        out.push(FkTriggerScanRow {
            tgoid: col(Anum_pg_trigger_oid).as_oid(),
            tgrelid: col(Anum_pg_trigger_tgrelid).as_oid(),
            tgconstrrelid: col(Anum_pg_trigger_tgconstrrelid).as_oid(),
            tgfoid: col(Anum_pg_trigger_tgfoid).as_oid(),
            tgtype: col(Anum_pg_trigger_tgtype).as_i16(),
        });
    }
    let _ = scan;
    Ok(out)
}

/// `GetForeignKeyActionTriggers(trigrel, conoid, confrelid, conrelid, ...)`
/// (tablecmds.c:12066) — the delete/update "action" triggers of the given
/// constraint on the PK side.
fn GetForeignKeyActionTriggers<'mcx>(
    mcx: Mcx<'mcx>,
    trigrel: &Relation<'mcx>,
    conoid: Oid,
    confrelid: Oid,
    conrelid: Oid,
) -> PgResult<(Oid, Oid)> {
    let mut delete_trigger_oid = InvalidOid;
    let mut update_trigger_oid = InvalidOid;

    for row in scan_fk_triggers_by_constraint(mcx, trigrel, conoid)? {
        if row.tgconstrrelid != conrelid {
            continue;
        }
        if row.tgrelid != confrelid {
            continue;
        }
        // Only ever look at "action" triggers on the PK side.
        if ri_fkey_trigger_type(row.tgfoid) != RI_TRIGGER_PK {
            continue;
        }
        if TRIGGER_FOR_DELETE(row.tgtype) {
            delete_trigger_oid = row.tgoid;
        } else if TRIGGER_FOR_UPDATE(row.tgtype) {
            update_trigger_oid = row.tgoid;
        }
        if OidIsValid(delete_trigger_oid) && OidIsValid(update_trigger_oid) {
            break;
        }
    }

    if !OidIsValid(delete_trigger_oid) {
        return Err(backend_utils_error::ereport(ERROR)
            .errmsg_internal(format!(
                "could not find ON DELETE action trigger of foreign key constraint {conoid}"
            ))
            .into_error());
    }
    if !OidIsValid(update_trigger_oid) {
        return Err(backend_utils_error::ereport(ERROR)
            .errmsg_internal(format!(
                "could not find ON UPDATE action trigger of foreign key constraint {conoid}"
            ))
            .into_error());
    }
    Ok((delete_trigger_oid, update_trigger_oid))
}

/// `GetForeignKeyCheckTriggers(trigrel, conoid, confrelid, conrelid, ...)`
/// (tablecmds.c:12131) — the insert/update "check" triggers of the given
/// constraint on the FK side.
fn GetForeignKeyCheckTriggers<'mcx>(
    mcx: Mcx<'mcx>,
    trigrel: &Relation<'mcx>,
    conoid: Oid,
    confrelid: Oid,
    conrelid: Oid,
) -> PgResult<(Oid, Oid)> {
    let mut insert_trigger_oid = InvalidOid;
    let mut update_trigger_oid = InvalidOid;

    for row in scan_fk_triggers_by_constraint(mcx, trigrel, conoid)? {
        if row.tgconstrrelid != confrelid {
            continue;
        }
        if row.tgrelid != conrelid {
            continue;
        }
        // Only ever look at "check" triggers on the FK side.
        if ri_fkey_trigger_type(row.tgfoid) != RI_TRIGGER_FK {
            continue;
        }
        if TRIGGER_FOR_INSERT(row.tgtype) {
            insert_trigger_oid = row.tgoid;
        } else if TRIGGER_FOR_UPDATE(row.tgtype) {
            update_trigger_oid = row.tgoid;
        }
        if OidIsValid(insert_trigger_oid) && OidIsValid(update_trigger_oid) {
            break;
        }
    }

    if !OidIsValid(insert_trigger_oid) {
        return Err(backend_utils_error::ereport(ERROR)
            .errmsg_internal(format!(
                "could not find ON INSERT check triggers of foreign key constraint {conoid}"
            ))
            .into_error());
    }
    if !OidIsValid(update_trigger_oid) {
        return Err(backend_utils_error::ereport(ERROR)
            .errmsg_internal(format!(
                "could not find ON UPDATE check triggers of foreign key constraint {conoid}"
            ))
            .into_error());
    }
    Ok((insert_trigger_oid, update_trigger_oid))
}

/// `DropForeignKeyConstraintTriggers(trigrel, conoid, confrelid, conrelid)`
/// (tablecmds.c:12003) — drop the action triggers for the FK constraint that
/// become redundant once a partition is attached to its parent's constraint.
fn DropForeignKeyConstraintTriggers<'mcx>(
    mcx: Mcx<'mcx>,
    trigrel: &Relation<'mcx>,
    conoid: Oid,
    confrelid: Oid,
    conrelid: Oid,
) -> PgResult<()> {
    for row in scan_fk_triggers_by_constraint(mcx, trigrel, conoid)? {
        // Invalid if trigger is not for a referential integrity constraint.
        if !OidIsValid(row.tgconstrrelid) {
            continue;
        }
        if OidIsValid(conrelid) && row.tgconstrrelid != conrelid {
            continue;
        }
        if OidIsValid(confrelid) && row.tgrelid != confrelid {
            continue;
        }

        // The constraint owns this trigger via an internal dependency record;
        // remove the dependency so the trigger can be dropped while keeping the
        // constraint intact.
        backend_catalog_pg_depend_seams::deleteDependencyRecordsFor::call(
            TriggerRelationId,
            row.tgoid,
            false,
        )?;
        // Make dependency deletion visible to performDeletion.
        CommandCounterIncrement()?;
        backend_catalog_dependency_seams::perform_deletion::call(
            TriggerRelationId,
            row.tgoid,
            0,
            types_nodes::parsenodes::DROP_RESTRICT,
            0,
        )?;
        // Make trigger drop visible, in case the loop iterates.
        CommandCounterIncrement()?;
    }
    Ok(())
}

// ===========================================================================
// tryAttachPartitionForeignKey / AttachPartitionForeignKey (tablecmds.c)
// ===========================================================================

/// `tryAttachPartitionForeignKey(wqueue, fk, partition, parentConstrOid, ...)`
/// (tablecmds.c:11629) — compare the partition's existing FK (`fk`) to the FK
/// defined by the parent's parameters; if equivalent, link the two constraints
/// and return `true`.
fn tryAttachPartitionForeignKey<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    fk: &backend_utils_cache_relcache::derived::ForeignKeyCacheInfo,
    partition: &Relation<'mcx>,
    parent_constr_oid: Oid,
    numfks: usize,
    mapped_conkey: &[AttrNumber],
    confkey: &[AttrNumber],
    conpfeqop: &[Oid],
    parent_ins_trigger: Oid,
    parent_upd_trigger: Oid,
    trigrel: &Relation<'mcx>,
) -> PgResult<bool> {
    let parent_constr =
        backend_utils_cache_syscache_seams::search_constraint_form_by_oid::call(parent_constr_oid)?
            .ok_or_else(|| {
                backend_utils_error::ereport(ERROR)
                    .errmsg_internal(format!(
                        "cache lookup failed for constraint {parent_constr_oid}"
                    ))
                    .into_error()
            })?
            .form;

    // Quick & easy initial checks.
    if fk.confrelid != parent_constr.confrelid || fk.nkeys as usize != numfks {
        return Ok(false);
    }
    for i in 0..numfks {
        if fk.conkey[i] != mapped_conkey[i]
            || fk.confkey[i] != confkey[i]
            || fk.conpfeqop[i] != conpfeqop[i]
        {
            return Ok(false);
        }
    }

    // Looks good so far; perform more extensive checks.
    let part_constr =
        backend_utils_cache_syscache_seams::search_constraint_form_by_oid::call(fk.conoid)?
            .ok_or_else(|| {
                backend_utils_error::ereport(ERROR)
                    .errmsg_internal(format!("cache lookup failed for constraint {}", fk.conoid))
                    .into_error()
            })?
            .form;

    // Enforceability must match, else raise an error (per the C comment).
    if part_constr.conenforced != parent_constr.conenforced {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "constraint \"{}\" enforceability conflicts with constraint \"{}\" on relation \"{}\"",
                parent_constr.conname_str(),
                part_constr.conname_str(),
                partition.name()
            ))
            .into_error());
    }

    if OidIsValid(part_constr.conparentid)
        || part_constr.condeferrable != parent_constr.condeferrable
        || part_constr.condeferred != parent_constr.condeferred
        || part_constr.confupdtype != parent_constr.confupdtype
        || part_constr.confdeltype != parent_constr.confdeltype
        || part_constr.confmatchtype != parent_constr.confmatchtype
    {
        return Ok(false);
    }

    // Looks good!  Attach this constraint.
    AttachPartitionForeignKey(
        mcx,
        wqueue,
        partition,
        fk.conoid,
        parent_constr_oid,
        parent_ins_trigger,
        parent_upd_trigger,
        trigrel,
    )?;

    Ok(true)
}

/// `AttachPartitionForeignKey(wqueue, partition, partConstrOid, parentConstrOid,
/// parentInsTrigger, parentUpdTrigger, trigrel)` (tablecmds.c:11801) — finalize
/// the attach: drop the partition's now-redundant rows/triggers, set the
/// constraint's parent, reparent the check triggers, and queue validation if
/// needed.
fn AttachPartitionForeignKey<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    partition: &Relation<'mcx>,
    part_constr_oid: Oid,
    parent_constr_oid: Oid,
    parent_ins_trigger: Oid,
    parent_upd_trigger: Oid,
    trigrel: &Relation<'mcx>,
) -> PgResult<()> {
    let parent_constr =
        backend_utils_cache_syscache_seams::search_constraint_form_by_oid::call(parent_constr_oid)?
            .ok_or_else(|| {
                backend_utils_error::ereport(ERROR)
                    .errmsg_internal(format!(
                        "cache lookup failed for constraint {parent_constr_oid}"
                    ))
                    .into_error()
            })?
            .form;
    let parent_constr_is_enforced = parent_constr.conenforced;
    let parent_convalidated = parent_constr.convalidated;

    let part_constr =
        backend_utils_cache_syscache_seams::search_constraint_form_by_oid::call(part_constr_oid)?
            .ok_or_else(|| {
                backend_utils_error::ereport(ERROR)
                    .errmsg_internal(format!("cache lookup failed for constraint {part_constr_oid}"))
                    .into_error()
            })?
            .form;
    let part_constr_frelid = part_constr.confrelid;
    let part_constr_relid = part_constr.conrelid;
    let part_convalidated = part_constr.convalidated;

    // If the referenced table is partitioned, the partition we're attaching now
    // has extra pg_constraint rows and action triggers that are no longer
    // needed.  Remove those.
    if backend_utils_cache_lsyscache_seams::get_rel_relkind::call(part_constr_frelid)?
        == RELKIND_PARTITIONED_TABLE
    {
        let pg_constraint = backend_access_table_table_seams::table_open::call(
            mcx,
            ConstraintRelationId,
            RowShareLock,
        )?;
        RemoveInheritedConstraint(mcx, &pg_constraint, trigrel, part_constr_oid, part_constr_relid)?;
        pg_constraint.close(RowShareLock)?;
    }

    // Will we need to validate this constraint?  A valid parent constraint
    // implies all child constraints have been validated, so if this one isn't,
    // trigger phase-3 validation.
    let queue_validation = parent_convalidated && !part_convalidated;

    // The action triggers in the new partition become redundant; remove them.
    DropForeignKeyConstraintTriggers(
        mcx,
        trigrel,
        part_constr_oid,
        part_constr_frelid,
        part_constr_relid,
    )?;

    backend_catalog_pg_constraint::ConstraintSetParentConstraint(
        mcx,
        part_constr_oid,
        parent_constr_oid,
        partition.rd_id,
    )?;

    // Attach the partition's "check" triggers to the parent triggers, if the
    // constraint is ENFORCED.
    if parent_constr_is_enforced {
        let (insert_trigger_oid, update_trigger_oid) = GetForeignKeyCheckTriggers(
            mcx,
            trigrel,
            part_constr_oid,
            part_constr_frelid,
            part_constr_relid,
        )?;
        debug_assert!(OidIsValid(insert_trigger_oid) && OidIsValid(parent_ins_trigger));
        backend_commands_trigger::set_parent::TriggerSetParentTrigger(
            mcx,
            trigrel,
            insert_trigger_oid,
            parent_ins_trigger,
            partition.rd_id,
        )?;
        debug_assert!(OidIsValid(update_trigger_oid) && OidIsValid(parent_upd_trigger));
        backend_commands_trigger::set_parent::TriggerSetParentTrigger(
            mcx,
            trigrel,
            update_trigger_oid,
            parent_upd_trigger,
            partition.rd_id,
        )?;
    }

    // We changed this pg_constraint row's parent; validating will flip
    // convalidated, so CCI here (also needed for the duplicate-constraint case).
    CommandCounterIncrement()?;

    // If validation is needed, put it in the queue now.
    if queue_validation {
        let conrel = backend_access_table_table_seams::table_open::call(
            mcx,
            ConstraintRelationId,
            RowExclusiveLock,
        )?;
        let part_constr2 =
            backend_utils_cache_syscache_seams::search_constraint_form_by_oid::call(part_constr_oid)?
                .ok_or_else(|| {
                    backend_utils_error::ereport(ERROR)
                        .errmsg_internal(format!(
                            "cache lookup failed for constraint {part_constr_oid}"
                        ))
                        .into_error()
                })?
                .form;
        let confrelid = part_constr2.confrelid;
        let constr_name = part_constr2.conname_str().to_string();
        // Use the same lock as for AT_ValidateConstraint.
        crate::at_dropvalidate::QueueFKConstraintValidation(
            mcx,
            wqueue,
            partition,
            confrelid,
            part_constr_oid,
            &part_constr2,
            &constr_name,
            ShareUpdateExclusiveLock,
        )?;
        conrel.close(RowExclusiveLock)?;
    }

    Ok(())
}

const ShareUpdateExclusiveLock: LOCKMODE = 4;

/// `RemoveInheritedConstraint(conrel, trigrel, conoid, conrelid)`
/// (tablecmds.c:11913) — remove the inherited constraint and its trigger from
/// `conrelid` (the partition), which inherited `conoid`.
fn RemoveInheritedConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    conrel: &Relation<'mcx>,
    trigrel: &Relation<'mcx>,
    conoid: Oid,
    conrelid: Oid,
) -> PgResult<()> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        types_catalog::pg_constraint::Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        types_core::fmgr::F_OIDEQ,
        types_tuple::backend_access_common_heaptuple::Datum::from_oid(conrelid),
    )?;
    let keys = [key];

    let mut scan = backend_access_index_genam_seams::systable_beginscan::call(
        conrel,
        types_catalog::pg_constraint::ConstraintRelidTypidNameIndexId,
        true,
        None,
        &keys,
    )?;

    let mut objs: Vec<ObjectAddress> = Vec::new();
    while let Some(tup) =
        backend_access_index_genam_seams::systable_getnext::call(mcx, scan.desc_mut())?
    {
        let cols = heap_deform_tuple(mcx, &tup.tuple, &conrel.rd_att, &tup.data)?;
        let conform_oid =
            cols[types_catalog::pg_constraint::Anum_pg_constraint_oid as usize - 1].0.as_oid();
        let conform_parentid = cols
            [types_catalog::pg_constraint::Anum_pg_constraint_conparentid as usize - 1]
            .0
            .as_oid();
        if conform_parentid != conoid {
            continue;
        }

        objs.push(ObjectAddress {
            classId: ConstraintRelationId,
            objectId: conform_oid,
            objectSubId: 0,
        });

        // Delete the dependency record that binds the two constraints together.
        backend_catalog_pg_depend_seams::deleteDependencyRecordsForSpecific::call(
            ConstraintRelationId,
            conform_oid,
            DEPENDENCY_INTERNAL.0,
            ConstraintRelationId,
            conoid,
        )?;

        // Search for the triggers of this constraint and set them up for
        // deletion too.
        for row in scan_fk_triggers_by_constraint(mcx, trigrel, conform_oid)? {
            objs.push(ObjectAddress {
                classId: TriggerRelationId,
                objectId: row.tgoid,
                objectSubId: 0,
            });
            backend_catalog_pg_depend_seams::deleteDependencyRecordsForSpecific::call(
                TriggerRelationId,
                row.tgoid,
                DEPENDENCY_INTERNAL.0,
                ConstraintRelationId,
                conform_oid,
            )?;
        }
    }
    let _ = scan;

    // Must make the dependency deletions visible before performDeletion.
    CommandCounterIncrement()?;
    backend_catalog_dependency_seams::perform_multiple_deletions::call(
        &objs,
        types_nodes::parsenodes::DROP_RESTRICT,
        0,
    )?;

    Ok(())
}

// ===========================================================================
// CloneForeignKeyConstraints (tablecmds.c:11198)
// ===========================================================================

/// `CloneForeignKeyConstraints(wqueue, parentRel, partitionRel)`
/// (tablecmds.c:11198) — clone the parent's foreign keys onto a newly-acquired
/// partition (both the referencing side and the referenced side).
pub(crate) fn CloneForeignKeyConstraints<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    parent_rel: &Relation<'mcx>,
    partition_rel: &Relation<'mcx>,
) -> PgResult<()> {
    // This only works for declarative partitioning.
    debug_assert!(parent_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE);

    // First, clone constraints where the parent is on the referencing side.
    CloneFkReferencing(mcx, wqueue, parent_rel, partition_rel)?;

    // Clone constraints for which the parent is on the referenced side.
    CloneFkReferenced(mcx, parent_rel, partition_rel)?;

    Ok(())
}

/// `CloneFkReferenced(parentRel, partitionRel)` (tablecmds.c:11233) — find all
/// FKs that have `parentRel` on the referenced side, and clone them to the
/// partition.  No phase-3 verification is needed for the referenced side.
fn CloneFkReferenced<'mcx>(
    mcx: Mcx<'mcx>,
    parent_rel: &Relation<'mcx>,
    partition_rel: &Relation<'mcx>,
) -> PgResult<()> {
    // Search pg_constraint for FK rows whose confrelid == parentRel.  We must
    // not clone any constraint whose parent constraint is also going to be
    // cloned; so collect the candidate list first, then clone the roots.
    let pg_constraint = backend_access_table_table_seams::table_open::call(
        mcx,
        ConstraintRelationId,
        RowShareLock,
    )?;

    let mut key0 = ScanKeyData::empty();
    ScanKeyInit(
        &mut key0,
        types_catalog::pg_constraint::Anum_pg_constraint_confrelid,
        BTEqualStrategyNumber,
        types_core::fmgr::F_OIDEQ,
        types_tuple::backend_access_common_heaptuple::Datum::from_oid(parent_rel.rd_id),
    )?;
    let mut key1 = ScanKeyData::empty();
    ScanKeyInit(
        &mut key1,
        types_catalog::pg_constraint::Anum_pg_constraint_contype,
        BTEqualStrategyNumber,
        types_core::fmgr::F_CHAREQ,
        types_tuple::backend_access_common_heaptuple::Datum::from_char(CONSTRAINT_FOREIGN),
    )?;
    let keys = [key0, key1];

    // This is a seqscan, as we don't have a usable index (InvalidOid).
    let mut scan = backend_access_index_genam_seams::systable_beginscan::call(
        &pg_constraint,
        InvalidOid,
        true,
        None,
        &keys,
    )?;

    let mut clone: Vec<Oid> = Vec::new();
    while let Some(tup) =
        backend_access_index_genam_seams::systable_getnext::call(mcx, scan.desc_mut())?
    {
        let cols = heap_deform_tuple(mcx, &tup.tuple, &pg_constraint.rd_att, &tup.data)?;
        let conoid = cols
            [types_catalog::pg_constraint::Anum_pg_constraint_oid as usize - 1]
            .0
            .as_oid();
        clone.push(conoid);
    }
    let _ = scan;
    pg_constraint.close(RowShareLock)?;

    // Open the trigger catalog once for the loop's subroutines.
    let trigrel = backend_access_table_table_seams::table_open::call(
        mcx,
        TriggerRelationId,
        RowExclusiveLock,
    )?;

    let attmap = backend_access_common_next::attmap::build_attrmap_by_name(
        mcx,
        &partition_rel.rd_att,
        &parent_rel.rd_att,
        false,
    )?;

    for &constr_oid in clone.iter() {
        let row = backend_utils_cache_syscache_seams::search_constraint_form_by_oid::call(
            constr_oid,
        )?
        .ok_or_else(|| {
            backend_utils_error::ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for constraint {constr_oid}"))
                .into_error()
        })?;
        let constr_form = row.form;

        // Don't clone a constraint for which we're going to clone the parent.
        if clone.contains(&constr_form.conparentid) {
            continue;
        }

        // We need the same lock level that CreateTrigger will acquire.
        let fk_rel = backend_access_table_table_seams::table_open::call(
            mcx,
            constr_form.conrelid,
            ShareRowExclusiveLock,
        )?;

        let index_oid = constr_form.conindid;
        let tuple = backend_utils_cache_syscache_seams::search_constraint_tuple_by_oid::call(
            mcx, constr_oid,
        )?
        .ok_or_else(|| {
            backend_utils_error::ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for constraint {constr_oid}"))
                .into_error()
        })?;
        let fk = backend_catalog_pg_constraint::DeconstructFkConstraintRow(
            mcx, &tuple, true, true, true, true,
        )?;
        let numfks = fk.numfks as usize;
        let conkey = &fk.conkey;
        let confkey = &fk.confkey;
        let conpfeqop = fk.pf_eq_oprs.as_deref().unwrap_or(&[]);
        let conppeqop = fk.pp_eq_oprs.as_deref().unwrap_or(&[]);
        let conffeqop = fk.ff_eq_oprs.as_deref().unwrap_or(&[]);
        let numfkdelsetcols = fk.num_fk_del_set_cols as usize;
        let confdelsetcols = fk.fk_del_set_cols.as_deref().unwrap_or(&[]);

        let mut mapped_confkey = [0 as AttrNumber; INDEX_MAX_KEYS];
        for i in 0..numfks {
            mapped_confkey[i] = attmap.attnums[(confkey[i] - 1) as usize];
        }

        let mut fkconstraint = build_clone_fkconstraint(mcx, &constr_form, &fk_rel, conkey, numfks)?;

        // Add the new FK pointing to the new partition.  Because the new
        // partition is on the referenced side, no phase-3 check is needed.
        let part_index_id = backend_catalog_partition::index_get_partition(partition_rel, index_oid)?;
        if !OidIsValid(part_index_id) {
            return Err(backend_utils_error::ereport(ERROR)
                .errmsg_internal(format!(
                    "index for {index_oid} not found in partition {}",
                    partition_rel.name()
                ))
                .into_error());
        }

        // Get the "action" triggers to pass as parent OIDs.
        let (delete_trigger_oid, update_trigger_oid) = if constr_form.conenforced {
            GetForeignKeyActionTriggers(
                mcx,
                &trigrel,
                constr_oid,
                constr_form.confrelid,
                constr_form.conrelid,
            )?
        } else {
            (InvalidOid, InvalidOid)
        };

        let address = addFkConstraint(
            mcx,
            AddFkSides::Referenced,
            &mut fkconstraint,
            &fk_rel,
            partition_rel,
            part_index_id,
            constr_oid,
            numfks,
            &mapped_confkey[..numfks],
            conkey,
            conpfeqop,
            conppeqop,
            conffeqop,
            numfkdelsetcols,
            confdelsetcols,
            false,
            constr_form.conperiod,
        )?;

        addFkRecurseReferenced(
            mcx,
            &fkconstraint,
            &fk_rel,
            partition_rel,
            part_index_id,
            address.objectId,
            numfks,
            &mapped_confkey[..numfks],
            conkey,
            conpfeqop,
            conppeqop,
            conffeqop,
            numfkdelsetcols,
            confdelsetcols,
            true,
            delete_trigger_oid,
            update_trigger_oid,
            constr_form.conperiod,
        )?;

        fk_rel.close(NoLock)?;
    }

    trigrel.close(RowExclusiveLock)?;
    Ok(())
}

/// `CloneFkReferencing(wqueue, parentRel, partRel)` (tablecmds.c:11343) — for
/// each FK of the parent on the referencing side, find a reparentable
/// equivalent in the partition, else create a new child constraint.
fn CloneFkReferencing<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    parent_rel: &Relation<'mcx>,
    part_rel: &Relation<'mcx>,
) -> PgResult<()> {
    // Obtain the list of constraints to clone.
    let parent_fks =
        backend_utils_cache_relcache::derived::RelationGetFKeyList(parent_rel.rd_id)?;
    let mut clone: Vec<Oid> = Vec::new();
    for fk in parent_fks.iter() {
        // Refuse to attach a table as partition that this partitioned table
        // already has a foreign key to.
        if fk.confrelid == part_rel.rd_id {
            let name = backend_utils_cache_lsyscache::collation_constraint_language_cast::get_constraint_name(
                mcx, fk.conoid,
            )?
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
            return Err(backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "cannot attach table \"{}\" as a partition because it is referenced by foreign key \"{name}\"",
                    part_rel.name()
                ))
                .into_error());
        }
        clone.push(fk.conoid);
    }

    // Silently do nothing if there's nothing to do.
    if clone.is_empty() {
        return Ok(());
    }

    if part_rel.rd_rel.relkind == RELKIND_FOREIGN_TABLE {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("foreign key constraints are not supported on foreign tables".to_string())
            .into_error());
    }

    let trigrel = backend_access_table_table_seams::table_open::call(
        mcx,
        TriggerRelationId,
        RowExclusiveLock,
    )?;

    let attmap = backend_access_common_next::attmap::build_attrmap_by_name(
        mcx,
        &part_rel.rd_att,
        &parent_rel.rd_att,
        false,
    )?;

    let part_fks = backend_utils_cache_relcache::derived::RelationGetFKeyList(part_rel.rd_id)?;

    for &parent_constr_oid in clone.iter() {
        let row = backend_utils_cache_syscache_seams::search_constraint_form_by_oid::call(
            parent_constr_oid,
        )?
        .ok_or_else(|| {
            backend_utils_error::ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for constraint {parent_constr_oid}"))
                .into_error()
        })?;
        let constr_form = row.form;

        // Don't clone constraints whose parents are being cloned.
        if clone.contains(&constr_form.conparentid) {
            continue;
        }

        // Need to prevent concurrent deletions.  If pkrel is partitioned, lock
        // all partitions.
        let pkrel = backend_access_table_table_seams::table_open::call(
            mcx,
            constr_form.confrelid,
            ShareRowExclusiveLock,
        )?;
        if pkrel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
            let _ = backend_catalog_pg_inherits::find_all_inheritors(
                mcx,
                pkrel.rd_id,
                ShareRowExclusiveLock,
                false,
            )?;
        }

        let tuple = backend_utils_cache_syscache_seams::search_constraint_tuple_by_oid::call(
            mcx, parent_constr_oid,
        )?
        .ok_or_else(|| {
            backend_utils_error::ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for constraint {parent_constr_oid}"))
                .into_error()
        })?;
        let fk = backend_catalog_pg_constraint::DeconstructFkConstraintRow(
            mcx, &tuple, true, true, true, true,
        )?;
        let numfks = fk.numfks as usize;
        let conkey = &fk.conkey;
        let confkey = &fk.confkey;
        let conpfeqop = fk.pf_eq_oprs.as_deref().unwrap_or(&[]);
        let conppeqop = fk.pp_eq_oprs.as_deref().unwrap_or(&[]);
        let conffeqop = fk.ff_eq_oprs.as_deref().unwrap_or(&[]);
        let numfkdelsetcols = fk.num_fk_del_set_cols as usize;
        let confdelsetcols = fk.fk_del_set_cols.as_deref().unwrap_or(&[]);

        let mut mapped_conkey = [0 as AttrNumber; INDEX_MAX_KEYS];
        for i in 0..numfks {
            mapped_conkey[i] = attmap.attnums[(conkey[i] - 1) as usize];
        }

        // Get the parent's "check" triggers, if ENFORCED, to pass as parent
        // OIDs (and to reparent the partition's existing triggers on attach).
        let (insert_trigger_oid, update_trigger_oid) = if constr_form.conenforced {
            GetForeignKeyCheckTriggers(
                mcx,
                &trigrel,
                constr_form.oid,
                constr_form.confrelid,
                constr_form.conrelid,
            )?
        } else {
            (InvalidOid, InvalidOid)
        };

        // See whether any existing FK is fit for the purpose; if so, attach.
        let mut attached = false;
        for pfk in part_fks.iter() {
            if tryAttachPartitionForeignKey(
                mcx,
                wqueue,
                pfk,
                part_rel,
                parent_constr_oid,
                numfks,
                &mapped_conkey,
                confkey,
                conpfeqop,
                insert_trigger_oid,
                update_trigger_oid,
                &trigrel,
            )? {
                attached = true;
                break;
            }
        }
        if attached {
            pkrel.close(NoLock)?;
            continue;
        }

        // No dice.  Set up to create our own constraint.
        let mut fkconstraint = build_clone_fkconstraint_referencing(
            mcx,
            &constr_form,
            part_rel,
            &mapped_conkey,
            numfks,
        )?;
        let index_oid = constr_form.conindid;
        let with_period = constr_form.conperiod;

        let address = addFkConstraint(
            mcx,
            AddFkSides::Referencing,
            &mut fkconstraint,
            part_rel,
            &pkrel,
            index_oid,
            parent_constr_oid,
            numfks,
            confkey,
            &mapped_conkey[..numfks],
            conpfeqop,
            conppeqop,
            conffeqop,
            numfkdelsetcols,
            confdelsetcols,
            false,
            with_period,
        )?;

        addFkRecurseReferencing(
            mcx,
            wqueue,
            &fkconstraint,
            part_rel,
            &pkrel,
            index_oid,
            address.objectId,
            numfks,
            confkey,
            &mapped_conkey[..numfks],
            conpfeqop,
            conppeqop,
            conffeqop,
            numfkdelsetcols,
            confdelsetcols,
            false, // no old check exists
            AccessExclusiveLock,
            insert_trigger_oid,
            update_trigger_oid,
            with_period,
        )?;

        pkrel.close(NoLock)?;
    }

    trigrel.close(RowExclusiveLock)?;
    Ok(())
}

/// Build the `Constraint` node for a referenced-side clone from the parent
/// constraint's form, naming `fk_attrs` from the FK relation's columns.
fn build_clone_fkconstraint<'mcx>(
    mcx: Mcx<'mcx>,
    constr_form: &types_catalog::pg_constraint::FormData_pg_constraint,
    fk_rel: &Relation<'mcx>,
    conkey: &[AttrNumber],
    numfks: usize,
) -> PgResult<Constraint<'mcx>> {
    let mut c = crate::mergeattr::empty_constraint(mcx, ConstrType::CONSTR_FOREIGN)?;
    c.conname = Some(PgString::from_str_in(constr_form.conname_str(), mcx)?);
    c.deferrable = constr_form.condeferrable;
    c.initdeferred = constr_form.condeferred;
    c.location = -1;
    c.pktable = None;
    c.fk_matchtype = constr_form.confmatchtype;
    c.fk_upd_action = constr_form.confupdtype;
    c.fk_del_action = constr_form.confdeltype;
    c.old_pktable_oid = InvalidOid;
    c.is_enforced = constr_form.conenforced;
    c.skip_validation = false;
    c.initially_valid = constr_form.convalidated;
    // fk_attrs are the FK relation's column names for the key columns.
    for i in 0..numfks {
        let att = fk_rel.rd_att.attr((conkey[i] - 1) as usize);
        let name = String::from_utf8_lossy(att.attname.name_str()).into_owned();
        c.fk_attrs.push(make_string(mcx, &name)?);
    }
    Ok(c)
}

/// Build the `Constraint` node for a referencing-side clone — `fk_attrs` are
/// the partition's column names (via the mapped conkey).
fn build_clone_fkconstraint_referencing<'mcx>(
    mcx: Mcx<'mcx>,
    constr_form: &types_catalog::pg_constraint::FormData_pg_constraint,
    part_rel: &Relation<'mcx>,
    mapped_conkey: &[AttrNumber],
    numfks: usize,
) -> PgResult<Constraint<'mcx>> {
    let mut c = crate::mergeattr::empty_constraint(mcx, ConstrType::CONSTR_FOREIGN)?;
    // conname is determined inside addFkConstraint (from the parent name).
    c.conname = Some(PgString::from_str_in(constr_form.conname_str(), mcx)?);
    c.deferrable = constr_form.condeferrable;
    c.initdeferred = constr_form.condeferred;
    c.location = -1;
    c.pktable = None;
    c.fk_matchtype = constr_form.confmatchtype;
    c.fk_upd_action = constr_form.confupdtype;
    c.fk_del_action = constr_form.confdeltype;
    c.old_pktable_oid = InvalidOid;
    c.is_enforced = constr_form.conenforced;
    c.skip_validation = false;
    c.initially_valid = constr_form.convalidated;
    for i in 0..numfks {
        let att = part_rel.rd_att.attr((mapped_conkey[i] - 1) as usize);
        let name = String::from_utf8_lossy(att.attname.name_str()).into_owned();
        c.fk_attrs.push(make_string(mcx, &name)?);
    }
    Ok(c)
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

use types_catalog::pg_trigger::{
    TRIGGER_TYPE_AFTER, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_UPDATE,
};

/// `makeString(s)` as a `Node*`.
fn make_string<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<NodePtr<'mcx>> {
    let sval = PgString::from_str_in(s, mcx)?;
    mcx::alloc_in(mcx, Node::mk_string(mcx, types_nodes::value::StringNode { sval })?)
}

/// `SystemFuncName(name)` — a `pg_catalog`-qualified function-name list.
fn system_func_name<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    let mut names: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    names.push(make_string(mcx, "pg_catalog")?);
    names.push(make_string(mcx, name)?);
    Ok(names)
}

fn to_access_range_var(rv: &DdlRangeVar<'_>) -> types_tuple::access::RangeVar {
    types_tuple::access::RangeVar {
        catalogname: rv.catalogname.as_deref().map(|s| s.into()),
        schemaname: rv.schemaname.as_deref().map(|s| s.into()),
        relname: rv.relname.as_deref().unwrap_or("").into(),
        inh: rv.inh,
        relpersistence: rv.relpersistence as u8,
        location: rv.location,
    }
}

fn format_type<'mcx>(mcx: Mcx<'mcx>, oid: Oid) -> PgResult<String> {
    Ok(backend_utils_adt_format_type::format_type_be(mcx, oid)?.as_str().to_string())
}

fn coll_name<'mcx>(mcx: Mcx<'mcx>, oid: Oid) -> PgResult<String> {
    Ok(backend_utils_cache_lsyscache::collation_constraint_language_cast::get_collation_name(mcx, oid)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_default())
}

fn collation_is_det(oid: Oid) -> PgResult<bool> {
    if !OidIsValid(oid) {
        // C: get_collation_isdeterministic(InvalidOid) treats it as deterministic.
        return Ok(true);
    }
    backend_utils_cache_lsyscache::collation_constraint_language_cast::get_collation_isdeterministic(oid)
}

fn is_polymorphic_type(oid: Oid) -> bool {
    // ANYARRAY/ANYELEMENT/ANYENUM/ANYRANGE/ANYNONARRAY/ANY*/etc. — pg_type.h OIDs.
    matches!(
        oid,
        2276 | 2277 | 2283 | 2776 | 3500 | 3831 | 4537 | 5077 | 5078 | 5079 | 5080
    )
}

fn persistence_err<T>(msg: &str) -> PgResult<T> {
    Err(backend_utils_error::ereport(ERROR)
        .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
        .errmsg(msg.to_string())
        .into_error())
}

fn generated_action_err<T>(which: &str) -> PgResult<T> {
    Err(backend_utils_error::ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg(format!(
            "invalid {which} action for foreign key constraint containing generated column"
        ))
        .into_error())
}

fn period_action_err<T>(which: &str) -> PgResult<T> {
    Err(backend_utils_error::ereport(ERROR)
        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
        .errmsg(format!(
            "unsupported {which} action for foreign key constraint using PERIOD"
        ))
        .into_error())
}
