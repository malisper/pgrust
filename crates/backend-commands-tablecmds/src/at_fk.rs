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
//!     partitions.  The partitioned-table recursion is a faithful seam-and-stop
//!     (it needs `RelationGetPartitionDesc` / `index_get_partition` /
//!     `tryAttachPartitionForeignKey`, none reachable here); the
//!     non-partitioned common case is fully ported.
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
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERROR,
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

use crate::at_phase::{AlteredTableInfo, NewConstraint, ATGetQueueEntry};
use crate::helpers::here;

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

fn fk_unported(what: &str) -> ! {
    panic!("{what} is not yet ported in backend-commands-tablecmds (faithful seam-and-panic)");
}

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
            let old_pfeqop = match &*fkconstraint.old_conpfeqop[old_pfeqop_idx] {
                Node::Integer(i) => i.ival as Oid,
                _ => InvalidOid,
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
    _numfks: usize,
    _pkattnum: &[AttrNumber],
    _fkattnum: &[AttrNumber],
    _pfeqoperators: &[Oid],
    _ppeqoperators: &[Oid],
    _ffeqoperators: &[Oid],
    _numfkdelsetcols: usize,
    _fkdelsetcols: &[AttrNumber],
    _old_check_ok: bool,
    parent_del_trigger: Oid,
    parent_upd_trigger: Oid,
    _with_period: bool,
) -> PgResult<()> {
    // Create action triggers to enforce the constraint (unless NOT ENFORCED).
    if fkconstraint.is_enforced {
        createForeignKeyActionTriggers(
            mcx,
            rel.rd_id,
            pkrel.rd_id,
            fkconstraint,
            parent_constr,
            index_oid,
            parent_del_trigger,
            parent_upd_trigger,
        )?;
    }

    // Partitioned referenced table: recurse to each partition.
    if pkrel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        fk_unported("addFkRecurseReferenced partitioned-table recursion (needs RelationGetPartitionDesc / index_get_partition)");
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
    _numfks: usize,
    _pkattnum: &[AttrNumber],
    _fkattnum: &[AttrNumber],
    _pfeqoperators: &[Oid],
    _ppeqoperators: &[Oid],
    _ffeqoperators: &[Oid],
    _numfkdelsetcols: usize,
    _fkdelsetcols: &[AttrNumber],
    old_check_ok: bool,
    _lockmode: LOCKMODE,
    parent_ins_trigger: Oid,
    parent_upd_trigger: Oid,
    _with_period: bool,
) -> PgResult<()> {
    debug_assert!(OidIsValid(parent_constr));

    if rel.rd_rel.relkind == RELKIND_FOREIGN_TABLE {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("foreign key constraints are not supported on foreign tables".to_string())
            .into_error());
    }

    // Add check triggers (unless NOT ENFORCED).
    if fkconstraint.is_enforced {
        createForeignKeyCheckTriggers(
            mcx,
            rel.rd_id,
            pkrel.rd_id,
            fkconstraint,
            parent_constr,
            index_oid,
            parent_ins_trigger,
            parent_upd_trigger,
        )?;
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
                Node::mk_constraint(mcx, fkconstraint.clone_in(mcx)?),
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
        fk_unported("addFkRecurseReferencing partitioned-table recursion (needs RelationGetPartitionDesc / tryAttachPartitionForeignKey)");
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

fn createForeignKeyActionTriggers<'mcx>(
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
        mcx, &del_trigger, ref_rel_oid, my_rel_oid, constraint_oid, index_oid, InvalidOid,
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
        mcx, &upd_trigger, ref_rel_oid, my_rel_oid, constraint_oid, index_oid, InvalidOid,
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

fn createForeignKeyCheckTriggers<'mcx>(
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

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

use types_catalog::pg_trigger::{
    TRIGGER_TYPE_AFTER, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_UPDATE,
};

/// `makeString(s)` as a `Node*`.
fn make_string<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<NodePtr<'mcx>> {
    let sval = PgString::from_str_in(s, mcx)?;
    mcx::alloc_in(mcx, Node::mk_string(mcx, types_nodes::value::StringNode { sval }))
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
