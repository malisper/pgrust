//! `commands/tablecmds.c` — the execute-phase RENAME verticals, ported 1:1 from
//! PostgreSQL 18.3:
//!
//! | C function (tablecmds.c line) | Rust |
//! |---|---|
//! | `renameatt_check` (3795) | [`renameatt_check`] |
//! | `renameatt_internal` (3844) | [`renameatt_internal`] |
//! | `RangeVarCallbackForRenameAttribute` (3989) | [`RangeVarCallbackForRenameAttribute`] |
//! | `renameatt` (4009) | [`renameatt`] |
//! | `RenameRelation` (4206) | [`RenameRelation`] |
//! | `RenameRelationInternal` (4270) | [`RenameRelationInternal`] |
//!
//! These mutate the on-disk `pg_class.relname` / `pg_attribute.attname`
//! catalogs and rename the associated rowtype / index constraint.  The catalog
//! reads/writes go through the same substrate the rest of this crate uses:
//!   * `relation_open` + RAII close (the lmgr lock is transaction-scoped).
//!   * `search_syscache_copy_pg_class` → a writable [`types_cluster::PgClassForm`]
//!     (which carries `relname`), mutated and re-stored via
//!     `catalog_tuple_update_pg_class` (a read-modify-write that only replaces
//!     the carried columns, so re-storing the unchanged fields is a no-op).
//!   * `SearchSysCacheCopyAttName` → a writable `pg_attribute` tuple, the new
//!     `attname` applied via the [`PgAttributeUpdateRow`] carrier and the
//!     `catalog_tuple_update_pg_attribute` write leaf.
//!   * `find_all_inheritors` / `find_inheritance_children` for the inheritance
//!     recursion (`stmt->relation->inh`), ported faithfully (including the C
//!     `forboth(child_oids, child_numparents)` per-child `expected_parents`).
//!
//! `rename_constraint_internal` / `RenameConstraint` (the RENAME CONSTRAINT
//! forms) are NOT ported here: they read `pg_constraint.contype` / `conindid` /
//! `coninhcount` / `connoinherit` off the constraint tuple, and no
//! `pg_constraint` form-reader seam exposes those fields yet. `RenameConstraint`
//! stays its declared `backend-commands-tablecmds-seams` loud panic until that
//! reader lands.

#![allow(non_snake_case)]

use mcx::Mcx;

use types_acl::ACLCHECK_NOT_OWNER;
use types_catalog::catalog_dependency::ObjectAddress;
use types_catalog::pg_attribute::{
    Anum_pg_attribute_attinhcount, Anum_pg_attribute_attnum, PgAttributeUpdateRow,
};
use types_core::primitive::{AttrNumber, InvalidOid, Oid, OidIsValid};
use types_error::{
    PgResult, ERRCODE_DUPLICATE_TABLE, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_TABLE_DEFINITION, ERRCODE_UNDEFINED_COLUMN,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR, NOTICE,
};
use types_nodes::parsenodes::OBJECT_INDEX;
use types_parsenodes::RenameStmt;
use types_storage::lock::{AccessExclusiveLock, NoLock, RowExclusiveLock, ShareUpdateExclusiveLock};
use types_tuple::access::{
    RangeVar as AccessRangeVar, RELKIND_COMPOSITE_TYPE, RELKIND_FOREIGN_TABLE, RELKIND_INDEX,
    RELKIND_MATVIEW, RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
    RELKIND_VIEW,
};
use types_tuple::backend_access_common_heaptuple::FormedTuple;

use backend_access_common_relation::relation_open;
use backend_utils_error::ereport;

use backend_catalog_aclchk_seams as aclchk_seam;
use backend_catalog_indexing_seams as indexing_seam;
use backend_catalog_namespace::{RangeVarGetRelidExtended, RVR_MISSING_OK};
use backend_catalog_pg_constraint::RenameConstraintById;
use backend_storage_lmgr_lmgr::UnlockRelationOid;
use backend_catalog_pg_depend_seams as depend_seam;
use backend_catalog_pg_inherits::{find_all_inheritors, find_inheritance_children};
use backend_catalog_pg_type_seams as pg_type_seam;
use backend_commands_tablespace_globals_seams as ts_globals_seam;
use backend_utils_cache_lsyscache_seams as lsyscache_seam;
use backend_utils_cache_syscache::cacheinfo::RELOID;
use backend_utils_cache_syscache::{
    SearchSysCacheCopyAttName, SysCacheGetAttrNotNull, ATTNAME,
};
use backend_utils_cache_syscache_seams as syscache_seam;
use backend_utils_init_miscinit_seams as miscinit_seam;
use types_catalog::pg_class::Anum_pg_class_reloftype;

use backend_commands_tablecmds_seams as seam;

use crate::helpers::{here, object_address_set, RelationRelationId};

/// `ObjectAddressSubSet(addr, classId, objectId, subId)` (objectaddress.h).
fn object_address_subset(class_id: Oid, object_id: Oid, sub_id: i32) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: sub_id,
    }
}

/// `namestrcpy(&name, src)` — produce the 64-byte NUL-padded `NameData` image.
fn namestrcpy_image(src: &str) -> [u8; 64] {
    let mut name = [0u8; 64];
    for (i, &b) in src.as_bytes().iter().take(64).enumerate() {
        name[i] = b;
    }
    name[64 - 1] = 0;
    name
}

// ===========================================================================
// renameatt (tablecmds.c:4009) — ALTER TABLE RENAME COLUMN.
// ===========================================================================

/// `renameatt_check(myrelid, classform, recursing)` (tablecmds.c:3795) — sanity
/// + permission checks before an attribute rename.  Here `relkind` /
/// `relnamespace` / `reloftype` are projected off the relcache `rd_rel` of the
/// already-opened (or to-be-opened) relation rather than a `Form_pg_class`
/// pointer, matching the rest of this crate's `rd_rel` field-read convention.
pub(crate) fn renameatt_check(
    myrelid: Oid,
    relname: &str,
    relkind: u8,
    relnamespace: Oid,
    reloftype: Oid,
    recursing: bool,
) -> PgResult<()> {
    // if (classform->reloftype && !recursing)
    //     ereport(ERROR, ERRCODE_WRONG_OBJECT_TYPE, "cannot rename column of typed table");
    if OidIsValid(reloftype) && !recursing {
        return ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("cannot rename column of typed table")
            .finish(here("renameatt_check"))
            .map(|()| unreachable!());
    }

    /*
     * Renaming the columns of sequences or toast tables doesn't actually break
     * anything from the system's point of view, since internal references are
     * by attnum.  But it doesn't seem right to allow users to change names that
     * are hardcoded into the system, hence the following restriction.
     */
    if relkind != RELKIND_RELATION
        && relkind != RELKIND_VIEW
        && relkind != RELKIND_MATVIEW
        && relkind != RELKIND_COMPOSITE_TYPE
        && relkind != RELKIND_INDEX
        && relkind != RELKIND_PARTITIONED_INDEX
        && relkind != RELKIND_FOREIGN_TABLE
        && relkind != RELKIND_PARTITIONED_TABLE
    {
        return ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("cannot rename columns of relation \"{relname}\""))
            .finish(here("renameatt_check"))
            .map(|()| unreachable!());
    }

    /*
     * permissions checking.  only the owner of a class can change its schema.
     */
    if !aclchk_seam::object_ownercheck::call(
        RelationRelationId,
        myrelid,
        miscinit_seam::get_user_id::call(),
    )? {
        let actual_relkind = lsyscache_seam::get_rel_relkind::call(myrelid)?;
        aclchk_seam::aclcheck_error::call(
            ACLCHECK_NOT_OWNER,
            backend_catalog_objectaddress_seams::get_relkind_objtype::call(actual_relkind),
            Some(relname.to_string()),
        )?;
    }
    if !ts_globals_seam::allowSystemTableMods::call()?
        && seam::is_system_class_relid::call(myrelid, relkind, relnamespace)?
    {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "permission denied: \"{relname}\" is a system catalog"
            ))
            .finish(here("renameatt_check"))
            .map(|()| unreachable!());
    }

    Ok(())
}

/// `renameatt_internal(myrelid, oldattname, newattname, recurse, recursing,
/// expected_parents, behavior)` (tablecmds.c:3844) — the workhorse for
/// [`renameatt`].  Returns the renamed attribute's attnum.
fn renameatt_internal<'mcx>(
    mcx: Mcx<'mcx>,
    myrelid: Oid,
    oldattname: &str,
    newattname: &str,
    recurse: bool,
    recursing: bool,
    expected_parents: i32,
    behavior: types_nodes::parsenodes::DropBehavior,
) -> PgResult<AttrNumber> {
    /*
     * Grab an exclusive lock on the target table, which we will NOT release
     * until end of transaction.
     */
    // targetrelation = relation_open(myrelid, AccessExclusiveLock);
    let targetrelation = relation_open(mcx, myrelid, AccessExclusiveLock)?;
    // renameatt_check(myrelid, RelationGetForm(targetrelation), recursing);
    let target_relkind = targetrelation.rd_rel.relkind;
    let target_reltype = targetrelation.rd_rel.reltype;
    // `reloftype` is not carried by the trimmed relcache `rd_rel`; read it off
    // the same RELOID pg_class tuple the C `RelationGetForm` deref exposes.
    let target_reloftype = pg_class_reloftype(mcx, myrelid)?;
    renameatt_check(
        myrelid,
        &targetrelation.name(),
        target_relkind,
        targetrelation.rd_rel.relnamespace,
        target_reloftype,
        recursing,
    )?;

    /*
     * if the 'recurse' flag is set then we are supposed to rename this
     * attribute in all classes that inherit from 'relname' (as well as in
     * 'relname').
     */
    if recurse {
        // child_oids = find_all_inheritors(myrelid, AccessExclusiveLock, &child_numparents);
        let (child_oids, child_numparents) =
            find_all_inheritors(mcx, myrelid, AccessExclusiveLock, true)?;
        let child_numparents = child_numparents
            .expect("find_all_inheritors(want_numparents=true) returns numparents");

        // forboth(lo, child_oids, li, child_numparents)
        for (childrelid, &numparents) in child_oids.iter().copied().zip(child_numparents.iter()) {
            if childrelid == myrelid {
                continue;
            }
            // note we need not recurse again
            renameatt_internal(
                mcx,
                childrelid,
                oldattname,
                newattname,
                false,
                true,
                numparents,
                behavior,
            )?;
        }
    } else {
        /*
         * If we are told not to recurse, there had better not be any child
         * tables; else the rename would put them out of step.
         */
        if expected_parents == 0
            && !find_inheritance_children(mcx, myrelid, NoLock)?.is_empty()
        {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg(format!(
                    "inherited column \"{oldattname}\" must be renamed in child tables too"
                ))
                .finish(here("renameatt_internal"))
                .map(|()| unreachable!());
        }
    }

    /* rename attributes in typed tables of composite type */
    if target_relkind == RELKIND_COMPOSITE_TYPE {
        // child_oids = find_typed_table_dependencies(targetrelation->rd_rel->reltype,
        //                  RelationGetRelationName(targetrelation), behavior);
        let child_oids = crate::at_phase::find_typed_table_dependencies(
            mcx,
            target_reltype,
            &targetrelation.name(),
            behavior,
        )?;

        // foreach(lo, child_oids)
        //   renameatt_internal(lfirst_oid(lo), oldattname, newattname, true,
        //                      true, 0, behavior);
        for childrelid in child_oids.iter().copied() {
            renameatt_internal(
                mcx,
                childrelid,
                oldattname,
                newattname,
                true,
                true,
                0,
                behavior,
            )?;
        }
    }

    // attrelation = table_open(AttributeRelationId, RowExclusiveLock);
    let attrelation = relation_open(
        mcx,
        types_catalog::pg_attribute::AttributeRelationId,
        RowExclusiveLock,
    )?;

    // atttup = SearchSysCacheCopyAttName(myrelid, oldattname);
    let atttup = match SearchSysCacheCopyAttName(mcx, myrelid, oldattname)? {
        Some(t) => t,
        None => {
            return ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!("column \"{oldattname}\" does not exist"))
                .finish(here("renameatt_internal"))
                .map(|()| unreachable!());
        }
    };

    // attnum = attform->attnum;
    let attnum: AttrNumber = att_field_i16(mcx, &atttup, Anum_pg_attribute_attnum)?;
    if attnum <= 0 {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot rename system column \"{oldattname}\""))
            .finish(here("renameatt_internal"))
            .map(|()| unreachable!());
    }

    /*
     * if the attribute is inherited, forbid the renaming (unless we already
     * have all the expected parents in the hierarchy being processed).
     */
    let attinhcount: i16 = att_field_i16(mcx, &atttup, Anum_pg_attribute_attinhcount)?;
    if (attinhcount as i32) > expected_parents {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
            .errmsg(format!("cannot rename inherited column \"{oldattname}\""))
            .finish(here("renameatt_internal"))
            .map(|()| unreachable!());
    }

    /* new name should not already exist */
    // (void) check_for_column_name_collision(targetrelation, newattname, false);
    crate::at_coladd::check_for_column_name_collision(mcx, &targetrelation, newattname, false)?;

    /* apply the update */
    // namestrcpy(&(attform->attname), newattname);
    // CatalogTupleUpdate(attrelation, &atttup->t_self, atttup);
    let row = PgAttributeUpdateRow {
        attname: Some(namestrcpy_image(newattname)),
        ..Default::default()
    };
    indexing_seam::catalog_tuple_update_pg_attribute::call(mcx, &attrelation, &atttup, &row)?;

    // InvokeObjectPostAlterHook(RelationRelationId, myrelid, attnum); -- no-op.

    // table_close(attrelation, RowExclusiveLock); -- RAII drop.
    drop(attrelation);
    // relation_close(targetrelation, NoLock); -- close rel but keep lock.
    drop(targetrelation);

    Ok(attnum)
}

/// `RangeVarCallbackForRenameAttribute(rv, relid, oldrelid, arg)`
/// (tablecmds.c:3989) — pre-lock permission/integrity checks for the
/// `renameatt` / RENAME CONSTRAINT resolution.  A concurrently-dropped relation
/// (`None` drop-info) is a no-op.
pub(crate) fn RangeVarCallbackForRenameAttribute(mcx: Mcx<'_>, relid: Oid) -> PgResult<()> {
    // tuple = SearchSysCache1(RELOID, relid); if (!valid) return; /* dropped */
    // form = (Form_pg_class) GETSTRUCT(tuple);
    // The drop-info projection carries relkind / relnamespace / relname off the
    // same RELOID tuple; a None projection is the concurrently-dropped no-op.
    let Some(info) = seam::get_pg_class_drop_info::call(relid)? else {
        return Ok(()); /* concurrently dropped */
    };
    let reloftype = pg_class_reloftype(mcx, relid)?;
    // renameatt_check(relid, form, false);
    renameatt_check(
        relid,
        &info.relname,
        info.relkind,
        info.relnamespace,
        reloftype,
        false,
    )
}

/// `renameatt(stmt)` (tablecmds.c:4009) — ALTER TABLE RENAME COLUMN driver.
/// The returned ObjectAddress is that of the renamed column.
pub fn renameatt<'mcx>(mcx: Mcx<'mcx>, stmt: &RenameStmt) -> PgResult<ObjectAddress> {
    let relation = stmt
        .relation
        .as_ref()
        .expect("RenameStmt.relation must be set for RENAME COLUMN");

    /* lock level taken here should match renameatt_internal */
    // relid = RangeVarGetRelidExtended(stmt->relation, AccessExclusiveLock,
    //             stmt->missing_ok ? RVR_MISSING_OK : 0,
    //             RangeVarCallbackForRenameAttribute, NULL);
    let flags = if stmt.missing_ok { RVR_MISSING_OK } else { 0 };
    let mut cb = |_rv: &AccessRangeVar, relid: Oid, _oldrelid: Oid| {
        RangeVarCallbackForRenameAttribute(mcx, relid)
    };
    let relid = RangeVarGetRelidExtended(
        mcx,
        relation,
        AccessExclusiveLock,
        flags,
        Some(&mut cb),
    )?;

    if !OidIsValid(relid) {
        ereport(NOTICE)
            .errmsg(format!(
                "relation \"{}\" does not exist, skipping",
                relation.relname
            ))
            .finish(here("renameatt"))?;
        return Ok(crate::helpers::object_address_set(InvalidOid, InvalidOid));
    }

    // attnum = renameatt_internal(relid, stmt->subname, stmt->newname,
    //              stmt->relation->inh, false, 0, stmt->behavior);
    let attnum = renameatt_internal(
        mcx,
        relid,
        stmt.subname.as_deref().unwrap_or(""),
        stmt.newname.as_deref().unwrap_or(""),
        relation.inh, /* recursive? */
        false,        /* recursing? */
        0,            /* expected inhcount */
        stmt.behavior,
    )?;

    // ObjectAddressSubSet(address, RelationRelationId, relid, attnum);
    Ok(object_address_subset(RelationRelationId, relid, attnum as i32))
}

// ===========================================================================
// RenameRelation (tablecmds.c:4206) — ALTER TABLE/INDEX/SEQUENCE/VIEW/MATVIEW/
// FOREIGN TABLE RENAME TO.
// ===========================================================================

/// `RenameRelation(stmt)` (tablecmds.c:4206) — resolve+lock the target relation
/// (retrying with the correct lock level on an index/non-index statement
/// mismatch), then [`RenameRelationInternal`].
pub fn RenameRelation<'mcx>(mcx: Mcx<'mcx>, stmt: &RenameStmt) -> PgResult<ObjectAddress> {
    let mut is_index_stmt = stmt.renameType == OBJECT_INDEX;
    let relation = stmt
        .relation
        .as_ref()
        .expect("RenameStmt.relation must be set for RENAME TO");

    let relid;
    loop {
        // lockmode = is_index_stmt ? ShareUpdateExclusiveLock : AccessExclusiveLock;
        let lockmode = if is_index_stmt {
            ShareUpdateExclusiveLock
        } else {
            AccessExclusiveLock
        };

        // relid = RangeVarGetRelidExtended(stmt->relation, lockmode,
        //             stmt->missing_ok ? RVR_MISSING_OK : 0,
        //             RangeVarCallbackForAlterRelation, stmt);
        let flags = if stmt.missing_ok { RVR_MISSING_OK } else { 0 };
        // RangeVarCallbackForAlterRelation, stmt — the Rename-aware callback:
        // it applies the namespace ACL_CREATE recheck and the
        // `!IsA(stmt, RenameStmt)` relaxation of the ALTER INDEX rule (so
        // `ALTER INDEX <table> RENAME` / `ALTER TABLE <index> RENAME` are allowed),
        // unlike the ALTER TABLE phase variant in `at_phase`.
        let mut cb = |rv: &AccessRangeVar, candidate: Oid, oldrelid: Oid| {
            crate::rename_schema::range_var_callback_for_alter_relation_rename(
                mcx, rv, candidate, oldrelid, stmt,
            )
        };
        let candidate =
            RangeVarGetRelidExtended(mcx, relation, lockmode, flags, Some(&mut cb))?;

        if !OidIsValid(candidate) {
            ereport(NOTICE)
                .errmsg(format!(
                    "relation \"{}\" does not exist, skipping",
                    relation.relname
                ))
                .finish(here("RenameRelation"))?;
            return Ok(crate::helpers::object_address_set(InvalidOid, InvalidOid));
        }

        /*
         * We allow mismatched statement and object types (e.g., ALTER INDEX to
         * rename a table), but we might've used the wrong lock level.  If that
         * happens, retry with the correct lock level.
         */
        let relkind = lsyscache_seam::get_rel_relkind::call(candidate)?;
        let obj_is_index = relkind == RELKIND_INDEX || relkind == RELKIND_PARTITIONED_INDEX;
        if obj_is_index || is_index_stmt == obj_is_index {
            relid = candidate;
            break;
        }

        // UnlockRelationOid(relid, lockmode); is_index_stmt = obj_is_index;
        UnlockRelationOid(candidate, lockmode)?;
        is_index_stmt = obj_is_index;
    }

    /* Do the work */
    // RenameRelationInternal(relid, stmt->newname, false, is_index_stmt);
    RenameRelationInternal(
        mcx,
        relid,
        stmt.newname.as_deref().unwrap_or(""),
        false,
        is_index_stmt,
    )?;

    // ObjectAddressSet(address, RelationRelationId, relid);
    Ok(object_address_set(RelationRelationId, relid))
}

/// `RenameRelationInternal(myrelid, newrelname, is_internal, is_index)`
/// (tablecmds.c:4270) — change the name of a relation: pin it (keeping the
/// lock), update its `pg_class.relname`, and rename the associated rowtype and
/// index constraint if any.
pub fn RenameRelationInternal<'mcx>(
    mcx: Mcx<'mcx>,
    myrelid: Oid,
    newrelname: &str,
    _is_internal: bool,
    is_index: bool,
) -> PgResult<()> {
    /*
     * Grab a lock on the target relation, which we will NOT release until end
     * of transaction.
     */
    // targetrelation = relation_open(myrelid, is_index ? ShareUpdateExclusiveLock
    //                                                  : AccessExclusiveLock);
    let lockmode = if is_index {
        ShareUpdateExclusiveLock
    } else {
        AccessExclusiveLock
    };
    let targetrelation = relation_open(mcx, myrelid, lockmode)?;
    // namespaceId = RelationGetNamespace(targetrelation);
    let namespace_id = targetrelation.rd_rel.relnamespace;
    let reltype = targetrelation.rd_rel.reltype;
    let relkind = targetrelation.rd_rel.relkind;

    /*
     * Find relation's pg_class tuple, and make sure newrelname isn't in use.
     */
    // relrelation = table_open(RelationRelationId, RowExclusiveLock);
    let relrelation = relation_open(mcx, RelationRelationId, RowExclusiveLock)?;

    // reltup = SearchSysCacheLockedCopy1(RELOID, myrelid);
    let (otid, mut relform) = match syscache_seam::search_syscache_copy_pg_class::call(mcx, myrelid)?
    {
        Some(t) => t,
        None => {
            // elog(ERROR, "cache lookup failed for relation %u", myrelid);
            return ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for relation {myrelid}"))
                .finish(here("RenameRelationInternal"))
                .map(|()| unreachable!());
        }
    };

    // if (get_relname_relid(newrelname, namespaceId) != InvalidOid)
    //     ereport(ERROR, ERRCODE_DUPLICATE_TABLE, "relation ... already exists");
    if OidIsValid(lsyscache_seam::get_relname_relid::call(newrelname, namespace_id)?) {
        return ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_TABLE)
            .errmsg(format!("relation \"{newrelname}\" already exists"))
            .finish(here("RenameRelationInternal"))
            .map(|()| unreachable!());
    }

    /*
     * Update pg_class tuple with new relname.  (Scribbling on relform is OK
     * because it's a copy...)
     */
    // namestrcpy(&(relform->relname), newrelname);
    relform.relname = newrelname.to_string();
    // CatalogTupleUpdate(relrelation, &otid, reltup);
    indexing_seam::catalog_tuple_update_pg_class::call(mcx, &relrelation, otid, &relform)?;

    // InvokeObjectPostAlterHookArg(...); -- no-op.
    // table_close(relrelation, RowExclusiveLock); -- RAII drop.
    drop(relrelation);

    /*
     * Also rename the associated type, if any.
     */
    // if (OidIsValid(targetrelation->rd_rel->reltype))
    //     RenameTypeInternal(reltype, newrelname, namespaceId);
    if OidIsValid(reltype) {
        pg_type_seam::rename_type_internal::call(reltype, newrelname.to_string(), namespace_id)?;
    }

    /*
     * Also rename the associated constraint, if any (index relkinds).
     */
    if relkind == RELKIND_INDEX || relkind == RELKIND_PARTITIONED_INDEX {
        // Oid constraintId = get_index_constraint(myrelid);
        let constraint_id = depend_seam::get_index_constraint::call(myrelid)?;
        if OidIsValid(constraint_id) {
            RenameConstraintById(mcx, constraint_id, newrelname)?;
        }
    }

    /*
     * Close rel, but keep lock!
     */
    // relation_close(targetrelation, NoLock);
    drop(targetrelation);

    Ok(())
}

// ---------------------------------------------------------------------------
// Form_pg_attribute field readers (GETSTRUCT off a syscache tuple).
// ---------------------------------------------------------------------------

/// `GETSTRUCT(tuple)->field` for a non-null `int2` `pg_attribute` column.
fn att_field_i16(mcx: Mcx<'_>, tup: &FormedTuple<'_>, anum: i16) -> PgResult<i16> {
    Ok(SysCacheGetAttrNotNull(mcx, ATTNAME, tup, anum as i32)?.as_i16())
}

/// `((Form_pg_class) GETSTRUCT(SearchSysCache1(RELOID, relid)))->reloftype` —
/// the `reloftype` column the trimmed relcache `rd_rel` does not carry. Returns
/// `InvalidOid` for a concurrently-dropped relation.
pub(crate) fn pg_class_reloftype(mcx: Mcx<'_>, relid: Oid) -> PgResult<Oid> {
    let Some(tuple) = syscache_seam::search_syscache_copy_pg_class_tuple::call(mcx, relid)? else {
        return Ok(InvalidOid);
    };
    Ok(SysCacheGetAttrNotNull(mcx, RELOID, &tuple, Anum_pg_class_reloftype as i32)?.as_oid())
}
