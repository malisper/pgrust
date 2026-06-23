//! `backend/commands/tablecmds.c` — the RENAME CONSTRAINT + SET SCHEMA
//! execute-phase drivers (generic ALTER dispatch targets driven by
//! `commands/alter.c`), the companion of `rename.rs` (which owns RENAME TO /
//! RENAME COLUMN):
//!
//! | C function (line) | Rust |
//! |---|---|
//! | `rename_constraint_internal` (4046) | [`rename_constraint_internal`] |
//! | `RenameConstraint` (4156) | [`RenameConstraint`] |
//! | `AlterTableNamespace` (18946) | [`AlterTableNamespace`] |
//! | `AlterTableNamespaceInternal` (19017) | [`AlterTableNamespaceInternal`] |
//! | `AlterRelationNamespaceInternal` (19054) | [`AlterRelationNamespaceInternal`] |
//! | `AlterIndexNamespaces` (19127) | [`alter_index_namespaces`] |
//! | `AlterSeqNamespaces` (19166) | [`alter_seq_namespaces`] |
//! | `RangeVarCallbackForAlterRelation` (19586) | [`range_var_callback_for_alter_relation`] |
//!
//! The shared `renameatt_check` / `RangeVarCallbackForRenameAttribute` /
//! `RenameRelationInternal` / `pg_class_reloftype` helpers live in
//! [`crate::rename`] and are reused here.
//!
//! # Honest deferrals (loud, not silent)
//!
//! * **Inheritance recursion** (`rename_constraint_internal` `recurse` via
//!   `find_all_inheritors`): unported `find_*`; a *recursive* RENAME CONSTRAINT
//!   loud-errors, the non-recursive path is fully ported and the children-exist
//!   guard is consulted (errors as C does).
//! * **Domain-constraint RENAME** (`RenameConstraint` `OBJECT_DOMCONSTRAINT`
//!   leg): `typenameTypeId` + `checkDomainOwner` live in parse-type/typecmds
//!   (a tablecmds dep would cycle); crosses the uninstalled
//!   `rename_constraint_domain_typid` outward seam.
//! * **Row-type SET SCHEMA** (`AlterTableNamespaceInternal` rowtype leg via
//!   `AlterTypeNamespaceInternal`): crosses the `alter_type_namespace_internal`
//!   outward seam (owner `typecmds`, installed by that crate).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use ::mcx::Mcx;

use ::types_catalog::catalog_dependency::{InvalidObjectAddress, ObjectAddress, ObjectAddresses};
use ::types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_error::{
    PgResult, ERRCODE_DUPLICATE_TABLE, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_TABLE_DEFINITION, ERRCODE_WRONG_OBJECT_TYPE, ERROR, NOTICE,
};
use ::nodes::parsenodes::{
    OBJECT_DOMCONSTRAINT, OBJECT_FOREIGN_TABLE, OBJECT_INDEX, OBJECT_MATVIEW, OBJECT_SCHEMA,
    OBJECT_SEQUENCE, OBJECT_TYPE, OBJECT_VIEW,
};
use parsenodes::{AlterObjectSchemaStmt, Node, RenameStmt};
use ::types_storage::lock::{LOCKMODE, AccessExclusiveLock, NoLock, RowExclusiveLock};
use ::types_tuple::access::RangeVar as AccessRangeVar;
use ::types_tuple::access::{
    RELKIND_COMPOSITE_TYPE, RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_PARTITIONED_INDEX,
    RELKIND_SEQUENCE, RELKIND_TOASTVALUE,
};

use ::common_relation::relation_open;
use ::utils_error::ereport;

use aclchk_seams as aclchk_seam;
use dependency_seams as dep_seam;
use indexing_seams as indexing_seam;
use objectaddress_seams as objaddr_seam;
use pg_constraint_seams as pgcon_seam;
use pg_depend_seams as pgdepend_seam;
use tablespace_globals_seams as ts_globals_seam;
use inval_seams as inval_seam;
use lsyscache_seams as lsyscache_seam;
use syscache_seams as syscache_seam;

use catalog_namespace::{
    CheckSetNamespace, RangeVarGetAndCheckCreationNamespace, RangeVarGetRelidExtended, RVR_MISSING_OK,
};
use pg_constraint::{AlterConstraintNamespaces, RenameConstraintById};
use ::relcache::derived::RelationGetIndexList;

use tablecmds_seams as seam;

use ::types_acl::acl::ACL_CREATE;
use types_acl::{ACLCHECK_NOT_OWNER, ACLCHECK_OK};

use crate::helpers::{
    here, object_address_set, NamespaceRelationId, RelationRelationId, TypeRelationId,
};
use crate::rename::{
    pg_class_reloftype, renameatt_check, RangeVarCallbackForRenameAttribute, RenameRelationInternal,
};

/// `pg_constraint.contype` codes (catalog/pg_constraint.h).
const CONSTRAINT_CHECK: i8 = b'c' as i8;
const CONSTRAINT_PRIMARY: i8 = b'p' as i8;
const CONSTRAINT_UNIQUE: i8 = b'u' as i8;
const CONSTRAINT_EXCLUSION: i8 = b'x' as i8;
const CONSTRAINT_NOTNULL: i8 = b'n' as i8;

/// `ConstraintRelationId` — `pg_constraint` OID.
const ConstraintRelationId: Oid = ::types_core::catalog::CONSTRAINT_RELATION_ID;

/// The owned `access::RangeVar` carried by `stmt->relation` (the relation
/// rename / SET SCHEMA forms always have it set).
fn stmt_relation(rv: &Option<AccessRangeVar>) -> &AccessRangeVar {
    rv.as_ref()
        .expect("RenameStmt/AlterObjectSchemaStmt.relation must be set for the relation forms")
}

/// `find_inheritance_children(myrelid, NoLock) != NIL` (pg_inherits.c): does the
/// relation have any direct inheritance children?
fn has_inheritance_children(mcx: Mcx<'_>, relid: Oid) -> PgResult<bool> {
    Ok(!pg_inherits::find_inheritance_children(mcx, relid, NoLock)?.is_empty())
}

/// `castNode(List, stmt->object)` decoded into the qualified-name `String`
/// components (the `object` field carries a `List` of `String` value nodes).
fn decode_name_list(stmt: &RenameStmt) -> PgResult<alloc_vec_string> {
    let object = stmt
        .object
        .as_deref()
        .expect("RenameStmt.object must be set for a domain constraint");
    let list: &[Node] = object
        .as_list()
        .expect("castNode(List): List node expected for RenameStmt.object");
    let mut names = Vec::with_capacity(list.len());
    for elem in list {
        let s = elem
            .as_string()
            .expect("strVal: String node expected in RenameStmt.object list");
        names.push(s.sval.clone().unwrap_or_default());
    }
    Ok(names)
}

type alloc_vec_string = Vec<String>;

// ===========================================================================
// RenameConstraint (tablecmds.c:4156)
// ===========================================================================

/// `rename_constraint_internal(myrelid, mytypid, oldconname, newconname,
/// recurse, recursing, expected_parents)` (tablecmds.c:4046).
fn rename_constraint_internal<'mcx>(
    mcx: Mcx<'mcx>,
    myrelid: Oid,
    mytypid: Oid,
    oldconname: &str,
    newconname: &str,
    recurse: bool,
    _recursing: bool,
    expected_parents: i32,
) -> PgResult<ObjectAddress> {
    debug_assert!(!OidIsValid(myrelid) || !OidIsValid(mytypid));

    let has_target_relation = OidIsValid(myrelid);

    // targetrelation kept open for the relation branch (so the lock is held and
    // we can fire CacheInvalidateRelcache at the end).
    let mut targetrelation = None;

    let constraint_oid = if OidIsValid(mytypid) {
        // constraintOid = get_domain_constraint_oid(mytypid, oldconname, false);
        pgcon_seam::get_domain_constraint_oid::call(mcx, mytypid, oldconname, false)?
    } else {
        // targetrelation = relation_open(myrelid, AccessExclusiveLock);
        let rel = relation_open(mcx, myrelid, AccessExclusiveLock)?;
        // renameatt_check(myrelid, RelationGetForm(targetrelation), false);
        let reloftype = pg_class_reloftype(mcx, myrelid)?;
        renameatt_check(
            myrelid,
            rel.name(),
            rel.rd_rel.relkind,
            rel.rd_rel.relnamespace,
            reloftype,
            false,
        )?;
        targetrelation = Some(rel);
        // constraintOid = get_relation_constraint_oid(myrelid, oldconname, false);
        pgcon_seam::get_relation_constraint_oid::call(mcx, myrelid, oldconname, false)?
    };

    // tuple = SearchSysCache1(CONSTROID, constraintOid); con = GETSTRUCT(tuple);
    let Some(con_tup) = cache_syscache::SearchSysCache1(
        mcx,
        cache_syscache::CONSTROID,
        cache::SysCacheKey::Value(datum::Datum::from_oid(constraint_oid)),
    )?
    else {
        return Err(ereport(ERROR)
            .errmsg_internal(format!("cache lookup failed for constraint {constraint_oid}"))
            .into_error());
    };
    let con = syscache_seam::read_constraint_form::call(&con_tup)?;
    drop(con_tup);

    // if (myrelid && (contype == CHECK || contype == NOTNULL) && !connoinherit)
    if has_target_relation
        && (con.contype == CONSTRAINT_CHECK || con.contype == CONSTRAINT_NOTNULL)
        && !con.connoinherit
    {
        if recurse {
            // child_oids = find_all_inheritors(myrelid, AccessExclusiveLock,
            //                                  &child_numparents);
            let (child_oids, child_numparents) = pg_inherits::find_all_inheritors(
                mcx,
                myrelid,
                AccessExclusiveLock,
                true,
            )?;
            // forboth(lo, child_oids, li, child_numparents)
            let child_numparents = child_numparents
                .expect("find_all_inheritors(want_numparents=true) returns child_numparents");
            for (lo, li) in child_oids.iter().zip(child_numparents.iter()) {
                let childrelid = *lo;
                let numparents = *li;
                // if (childrelid == myrelid) continue;
                if childrelid == myrelid {
                    continue;
                }
                // rename_constraint_internal(childrelid, InvalidOid, oldconname,
                //                            newconname, false, true, numparents);
                rename_constraint_internal(
                    mcx,
                    childrelid,
                    InvalidOid,
                    oldconname,
                    newconname,
                    false,
                    true,
                    numparents,
                )?;
            }
        } else {
            // if (expected_parents == 0 && find_inheritance_children(myrelid) != NIL)
            //     ereport(ERROR, "inherited constraint ... must be renamed in child tables too");
            if expected_parents == 0 && has_inheritance_children(mcx, myrelid)? {
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                    .errmsg(format!(
                        "inherited constraint \"{oldconname}\" must be renamed in child tables too"
                    ))
                    .finish(here("rename_constraint_internal"))
                    .map(|()| unreachable!());
            }
        }

        // if (con->coninhcount > expected_parents)
        //     ereport(ERROR, "cannot rename inherited constraint ...");
        if (con.coninhcount as i32) > expected_parents {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg(format!("cannot rename inherited constraint \"{oldconname}\""))
                .finish(here("rename_constraint_internal"))
                .map(|()| unreachable!());
        }
    }

    // if (con->conindid && (contype == PRIMARY || UNIQUE || EXCLUSION))
    //     RenameRelationInternal(con->conindid, newconname, false, true);
    // else
    //     RenameConstraintById(constraintOid, newconname);
    if OidIsValid(con.conindid)
        && (con.contype == CONSTRAINT_PRIMARY
            || con.contype == CONSTRAINT_UNIQUE
            || con.contype == CONSTRAINT_EXCLUSION)
    {
        RenameRelationInternal(mcx, con.conindid, newconname, false, true)?;
    } else {
        RenameConstraintById(mcx, constraint_oid, newconname)?;
    }

    // ObjectAddressSet(address, ConstraintRelationId, constraintOid);
    let address = object_address_set(ConstraintRelationId, constraint_oid);

    // if (targetrelation) { CacheInvalidateRelcache(targetrelation);
    //                       relation_close(targetrelation, NoLock); }
    if let Some(rel) = targetrelation {
        inval_seam::cache_invalidate_relcache::call(rel.rd_id)?;
        rel.close(NoLock)?;
    }

    Ok(address)
}

/// `RenameConstraint(stmt)` (tablecmds.c:4156).
pub fn RenameConstraint<'mcx>(mcx: Mcx<'mcx>, stmt: &RenameStmt) -> PgResult<ObjectAddress> {
    let mut relid = InvalidOid;
    let mut typid = InvalidOid;

    if stmt.renameType == OBJECT_DOMCONSTRAINT {
        // typid = typenameTypeId(NULL, makeTypeNameFromNameList(castNode(List, stmt->object)));
        // rel = table_open(TypeRelationId, RowExclusiveLock); tup = SearchSysCache1(TYPEOID, typid);
        // checkDomainOwner(tup); ReleaseSysCache(tup); table_close(rel, NoLock);
        //
        // The typenameTypeId + checkDomainOwner resolution lives in
        // parse-type + typecmds (a tablecmds dependency would cycle); crosses
        // the (uninstalled) rename_constraint_domain_typid outward seam.
        let names = decode_name_list(stmt)?;
        typid = seam::rename_constraint_domain_typid::call(&names)?;
        let _ = TypeRelationId;
    } else {
        // relid = RangeVarGetRelidExtended(stmt->relation, AccessExclusiveLock,
        //             stmt->missing_ok ? RVR_MISSING_OK : 0,
        //             RangeVarCallbackForRenameAttribute, NULL);
        let relation = stmt_relation(&stmt.relation);
        let flags = if stmt.missing_ok { RVR_MISSING_OK } else { 0 };
        let mut cb = |_rv: &AccessRangeVar, rel_id: Oid, _old: Oid| {
            RangeVarCallbackForRenameAttribute(mcx, rel_id)
        };
        relid = RangeVarGetRelidExtended(mcx, relation, AccessExclusiveLock, flags, Some(&mut cb))?;
        if !OidIsValid(relid) {
            // ereport(NOTICE, "relation ... does not exist, skipping");
            ereport(NOTICE)
                .errmsg(format!(
                    "relation \"{}\" does not exist, skipping",
                    relation.relname
                ))
                .finish(here("RenameConstraint"))?;
            return Ok(InvalidObjectAddress);
        }
    }

    // return rename_constraint_internal(relid, typid, stmt->subname, stmt->newname,
    //            (stmt->relation && stmt->relation->inh), false, 0);
    let recurse = stmt.relation.as_ref().map(|rv| rv.inh).unwrap_or(false);

    rename_constraint_internal(
        mcx,
        relid,
        typid,
        stmt.subname.as_deref().unwrap_or(""),
        stmt.newname.as_deref().unwrap_or(""),
        recurse,
        false,
        0,
    )
}

// ===========================================================================
// AlterTableNamespace (SET SCHEMA) — tablecmds.c:18946
// ===========================================================================

/// `AlterTableNamespace(stmt, oldschema)` (tablecmds.c:18946). When
/// `want_oldschema` is true the previous schema OID is returned in the tuple's
/// second slot (the C `*oldschema` out-parameter); else `InvalidOid`.
pub fn AlterTableNamespace<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterObjectSchemaStmt,
    want_oldschema: bool,
) -> PgResult<(ObjectAddress, Oid)> {
    let relation = stmt_relation(&stmt.relation);

    // relid = RangeVarGetRelidExtended(stmt->relation, AccessExclusiveLock,
    //             stmt->missing_ok ? RVR_MISSING_OK : 0,
    //             RangeVarCallbackForAlterRelation, stmt);
    let flags = if stmt.missing_ok { RVR_MISSING_OK } else { 0 };
    let mut cb = |rv: &AccessRangeVar, rel_id: Oid, old: Oid| {
        range_var_callback_for_alter_relation(mcx, rv, rel_id, old, AlterArg::Schema(stmt))
    };
    let relid = RangeVarGetRelidExtended(mcx, relation, AccessExclusiveLock, flags, Some(&mut cb))?;

    if !OidIsValid(relid) {
        ereport(NOTICE)
            .errmsg(format!(
                "relation \"{}\" does not exist, skipping",
                relation.relname
            ))
            .finish(here("AlterTableNamespace"))?;
        return Ok((InvalidObjectAddress, InvalidOid));
    }

    // rel = relation_open(relid, NoLock);
    let rel = relation_open(mcx, relid, NoLock)?;
    // oldNspOid = RelationGetNamespace(rel);
    let old_nsp_oid = rel.rd_rel.relnamespace;
    let relname = rel.name().to_string();

    // If it's an owned sequence, disallow moving it by itself.
    if rel.rd_rel.relkind == RELKIND_SEQUENCE {
        // sequenceIsOwned(relid, DEPENDENCY_AUTO/INTERNAL, &tableId, &colId)
        let owned = dep_seam::sequence_is_owned::call(
            relid,
            ::types_catalog::catalog_dependency::DEPENDENCY_AUTO,
        )?
        .or(dep_seam::sequence_is_owned::call(
            relid,
            ::types_catalog::catalog_dependency::DEPENDENCY_INTERNAL,
        )?);
        if let Some((table_id, _col_id)) = owned {
            let table_name = lsyscache_seam::get_rel_name::call(mcx, table_id)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("cannot move an owned sequence into another schema".to_string())
                .errdetail(format!(
                    "Sequence \"{relname}\" is linked to table \"{table_name}\"."
                ))
                .finish(here("AlterTableNamespace"))
                .map(|()| unreachable!());
        }
    }

    // newrv = makeRangeVar(stmt->newschema, RelationGetRelationName(rel), -1);
    // nspOid = RangeVarGetAndCheckCreationNamespace(newrv, NoLock, NULL);
    let mut newrv = AccessRangeVar {
        catalogname: None,
        schemaname: stmt.newschema.clone(),
        relname: relname.clone(),
        inh: true,
        relpersistence: b'p',
        location: -1,
    };
    let nsp_oid = RangeVarGetAndCheckCreationNamespace(mcx, &mut newrv, NoLock, None)?;

    // common checks on switching namespaces
    CheckSetNamespace(mcx, old_nsp_oid, nsp_oid)?;

    // objsMoved = new_object_addresses();
    let mut objs_moved = dep_seam::new_object_addresses::call()?;
    // AlterTableNamespaceInternal(rel, oldNspOid, nspOid, objsMoved);
    AlterTableNamespaceInternal(mcx, &rel, old_nsp_oid, nsp_oid, &mut objs_moved)?;
    // free_object_addresses(objsMoved);
    dep_seam::free_object_addresses::call(objs_moved)?;

    // ObjectAddressSet(myself, RelationRelationId, relid);
    let myself = object_address_set(RelationRelationId, relid);

    // close rel, but keep lock until commit
    rel.close(NoLock)?;

    let oldschema = if want_oldschema { old_nsp_oid } else { InvalidOid };
    Ok((myself, oldschema))
}

/// `AlterTableNamespaceInternal(rel, oldNspOid, nspOid, objsMoved)`
/// (tablecmds.c:19017): besides moving the relation itself, its dependent
/// objects (row type, indexes, owned sequences, constraints) are relocated.
pub fn AlterTableNamespaceInternal<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &rel::Relation<'mcx>,
    old_nsp_oid: Oid,
    nsp_oid: Oid,
    objs_moved: &mut ObjectAddresses,
) -> PgResult<()> {
    // classRel = table_open(RelationRelationId, RowExclusiveLock);
    let class_rel = relation_open(mcx, RelationRelationId, RowExclusiveLock)?;

    // AlterRelationNamespaceInternal(classRel, RelationGetRelid(rel), oldNspOid,
    //                                nspOid, true, objsMoved);
    AlterRelationNamespaceInternal(mcx, &class_rel, rel.rd_id, old_nsp_oid, nsp_oid, true, objs_moved)?;

    // Fix the table's row type too, if it has one.
    if OidIsValid(rel.rd_rel.reltype) {
        // AlterTypeNamespaceInternal(reltype, nspOid, false, false, false, objsMoved);
        // Owner is typecmds (installed there).
        seam::alter_type_namespace_internal::call(
            rel.rd_rel.reltype,
            nsp_oid,
            false, /* isImplicitArray */
            false, /* ignoreDependent */
            false, /* errorOnTableType */
            objs_moved,
        )?;
    }

    // Fix other dependent stuff.
    alter_index_namespaces(mcx, &class_rel, rel, old_nsp_oid, nsp_oid, objs_moved)?;
    alter_seq_namespaces(
        mcx,
        &class_rel,
        rel,
        old_nsp_oid,
        nsp_oid,
        objs_moved,
        AccessExclusiveLock,
    )?;
    AlterConstraintNamespaces(rel.rd_id, old_nsp_oid, nsp_oid, false, objs_moved)?;

    // table_close(classRel, RowExclusiveLock);
    class_rel.close(RowExclusiveLock)?;

    Ok(())
}

/// `AlterRelationNamespaceInternal(classRel, relOid, oldNspOid, newNspOid,
/// hasDependEntry, objsMoved)` (tablecmds.c:19054): fix the pg_class entry and
/// the pg_depend entry if any. Caller must have opened+write-locked pg_class.
pub fn AlterRelationNamespaceInternal<'mcx>(
    mcx: Mcx<'mcx>,
    class_rel: &rel::Relation<'mcx>,
    rel_oid: Oid,
    old_nsp_oid: Oid,
    new_nsp_oid: Oid,
    has_depend_entry: bool,
    objs_moved: &mut ObjectAddresses,
) -> PgResult<()> {
    // classTup = SearchSysCacheLockedCopy1(RELOID, ObjectIdGetDatum(relOid));
    let Some((tid, mut class_form)) =
        syscache_seam::search_syscache_copy_pg_class::call(mcx, rel_oid)?
    else {
        return Err(ereport(ERROR)
            .errmsg_internal(format!("cache lookup failed for relation {rel_oid}"))
            .into_error());
    };

    debug_assert_eq!(class_form.relnamespace, old_nsp_oid);

    let thisobj = object_address_set(RelationRelationId, rel_oid);

    // If the object has already been moved, don't move it again. If it's already
    // in the right place, don't move it (but still fire the access hook).
    let already_done = dep_seam::object_address_present::call(thisobj.clone(), objs_moved)?;
    if !already_done && old_nsp_oid != new_nsp_oid {
        // check for duplicate name (friendlier than unique-index failure)
        if OidIsValid(lsyscache_seam::get_relname_relid::call(&class_form.relname, new_nsp_oid)?) {
            let nspname = lsyscache_seam::get_namespace_name::call(mcx, new_nsp_oid)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            return ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_TABLE)
                .errmsg(format!(
                    "relation \"{}\" already exists in schema \"{}\"",
                    class_form.relname, nspname
                ))
                .finish(here("AlterRelationNamespaceInternal"))
                .map(|()| unreachable!());
        }

        // classForm->relnamespace = newNspOid;
        // CatalogTupleUpdate(classRel, &otid, classTup);
        class_form.relnamespace = new_nsp_oid;
        indexing_seam::catalog_tuple_update_pg_class::call(mcx, class_rel, tid, &class_form)?;

        // Update dependency on schema if caller said so.
        if has_depend_entry {
            let n = pgdepend_seam::changeDependencyFor::call(
                mcx,
                RelationRelationId,
                rel_oid,
                NamespaceRelationId,
                old_nsp_oid,
                new_nsp_oid,
            )?;
            if n != 1 {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!(
                        "could not change schema dependency for relation \"{}\"",
                        class_form.relname
                    ))
                    .into_error());
            }
        }
    }

    if !already_done {
        dep_seam::add_exact_object_address::call(thisobj, objs_moved)?;
        // InvokeObjectPostAlterHook(RelationRelationId, relOid, 0);
    }

    Ok(())
}

/// Seam wrapper for `AlterRelationNamespaceInternal`, reached by
/// `AlterTypeNamespaceInternal` (typecmds.c) for composite types by OID. The
/// composite-type path has no caller-side `mcx` or open `classRel`, so the
/// wrapper opens+write-locks pg_class itself, performs the pg_class/pg_depend
/// move, then closes pg_class keeping the lock until commit.
pub fn alter_relation_namespace_internal_seam(
    rel_oid: Oid,
    old_nsp_oid: Oid,
    new_nsp_oid: Oid,
    has_depend_entry: bool,
    objs_moved: &mut ObjectAddresses,
) -> PgResult<()> {
    let ctx = ::mcx::MemoryContext::new("AlterRelationNamespaceInternal");
    let mcx = ctx.mcx();

    // classRel = table_open(RelationRelationId, RowExclusiveLock);
    let class_rel = relation_open(mcx, RelationRelationId, RowExclusiveLock)?;

    let res = AlterRelationNamespaceInternal(
        mcx,
        &class_rel,
        rel_oid,
        old_nsp_oid,
        new_nsp_oid,
        has_depend_entry,
        objs_moved,
    );

    // table_close(classRel, RowExclusiveLock); -- keep lock until commit.
    class_rel.close(RowExclusiveLock)?;

    res
}

/// `AlterIndexNamespaces(classRel, rel, oldNspOid, newNspOid, objsMoved)`
/// (tablecmds.c:19127): move all of the relation's indexes to the new schema.
fn alter_index_namespaces<'mcx>(
    mcx: Mcx<'mcx>,
    class_rel: &rel::Relation<'mcx>,
    rel: &rel::Relation<'mcx>,
    old_nsp_oid: Oid,
    new_nsp_oid: Oid,
    objs_moved: &mut ObjectAddresses,
) -> PgResult<()> {
    // indexList = RelationGetIndexList(rel);
    let index_list = RelationGetIndexList(rel.rd_id)?;

    for index_oid in index_list {
        let thisobj = object_address_set(RelationRelationId, index_oid);

        // The index has no dependency on the namespace and no row type, so no
        // changeDependencyFor / AlterTypeNamespaceInternal (hasDependEntry=false).
        if !dep_seam::object_address_present::call(thisobj.clone(), objs_moved)? {
            AlterRelationNamespaceInternal(
                mcx,
                class_rel,
                index_oid,
                old_nsp_oid,
                new_nsp_oid,
                false,
                objs_moved,
            )?;
            dep_seam::add_exact_object_address::call(thisobj, objs_moved)?;
        }
    }

    Ok(())
}

/// `AlterSeqNamespaces(classRel, rel, oldNspOid, newNspOid, objsMoved, lockmode)`
/// (tablecmds.c:19166): move all identity / SERIAL-column sequences of the
/// relation to the new schema. SERIAL/identity sequences are those with an
/// auto/internal dependency on one of the table's columns — exactly the set
/// `getOwnedSequences` (pg_depend.c) returns (it applies the same
/// `classid == pg_class && objsubid == 0 && refobjsubid != 0 && deptype in
/// {AUTO, INTERNAL} && relkind == SEQUENCE` filter the C scan does inline). We
/// take `lockmode` on each before moving and keep it until commit, as C does.
fn alter_seq_namespaces<'mcx>(
    mcx: Mcx<'mcx>,
    class_rel: &rel::Relation<'mcx>,
    rel: &rel::Relation<'mcx>,
    old_nsp_oid: Oid,
    new_nsp_oid: Oid,
    objs_moved: &mut ObjectAddresses,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let seqlist = pgdepend_seam::getOwnedSequences::call(mcx, rel.rd_id)?;

    for seq_oid in seqlist.iter().copied() {
        // seqRel = relation_open(objid, lockmode); -- take + hold the lock.
        let seq_rel = relation_open(mcx, seq_oid, lockmode)?;

        // getOwnedSequences already filtered to RELKIND_SEQUENCE; assert it.
        debug_assert_eq!(seq_rel.rd_rel.relkind, RELKIND_SEQUENCE);

        // Fix the pg_class and pg_depend entries.
        AlterRelationNamespaceInternal(
            mcx,
            class_rel,
            seq_oid,
            old_nsp_oid,
            new_nsp_oid,
            true,
            objs_moved,
        )?;

        // Sequences no longer have pg_type entries: Assert(reltype == InvalidOid).
        debug_assert_eq!(seq_rel.rd_rel.reltype, InvalidOid);

        // close it, keep the lock till end of transaction
        seq_rel.close(NoLock)?;
    }

    Ok(())
}

// ===========================================================================
// RangeVarCallbackForAlterRelation (tablecmds.c:19586)
// ===========================================================================

/// The `arg` passed to `RangeVarCallbackForAlterRelation`: the originating
/// statement, used to derive `reltype` and the RENAME / SET SCHEMA extra
/// checks.
enum AlterArg<'a> {
    Rename(&'a RenameStmt),
    Schema(&'a AlterObjectSchemaStmt),
}

/// `RangeVarCallbackForAlterRelation` invoked with a `RenameStmt` arg — the
/// callback `RenameRelation` (tablecmds.c:4206) passes to
/// `RangeVarGetRelidExtended`. This is the Rename-aware path: it applies the
/// namespace `ACL_CREATE` recheck and the `!IsA(stmt, RenameStmt)` relaxation of
/// the ALTER INDEX "is not an index" rule (so `ALTER INDEX <table> RENAME` is
/// permitted, matching C).
pub(crate) fn range_var_callback_for_alter_relation_rename<'mcx>(
    mcx: Mcx<'mcx>,
    rv: &AccessRangeVar,
    relid: Oid,
    oldrelid: Oid,
    stmt: &RenameStmt,
) -> PgResult<()> {
    range_var_callback_for_alter_relation(mcx, rv, relid, oldrelid, AlterArg::Rename(stmt))
}

/// `RangeVarCallbackForAlterRelation(rv, relid, oldrelid, arg)`
/// (tablecmds.c:19586) — the `RangeVarGetRelidExtended` callback shared by
/// RenameStmt / AlterObjectSchemaStmt / AlterTableStmt. (This is the
/// Rename/SET-SCHEMA-aware variant; the ALTER TABLE phase machinery carries its
/// own callback in `at_phase`.)
fn range_var_callback_for_alter_relation<'mcx>(
    mcx: Mcx<'mcx>,
    rv: &AccessRangeVar,
    relid: Oid,
    _oldrelid: Oid,
    arg: AlterArg<'_>,
) -> PgResult<()> {
    // tuple = SearchSysCache1(RELOID, relid); if (!valid) return; (concurrently dropped)
    let Some(info) = seam::get_pg_class_drop_info::call(relid)? else {
        return Ok(());
    };
    let relkind = info.relkind;

    // Must own relation.
    if !aclchk_seam::object_ownercheck::call(
        RelationRelationId,
        relid,
        miscinit_seams::get_user_id::call(),
    )? {
        let actual_kind = lsyscache_seam::get_rel_relkind::call(relid)?;
        aclchk_seam::aclcheck_error::call(
            ACLCHECK_NOT_OWNER,
            objaddr_seam::get_relkind_objtype::call(actual_kind),
            Some(rv.relname.clone()),
        )?;
    }

    // No system table modifications unless explicitly allowed.
    if !ts_globals_seam::allowSystemTableMods::call()?
        && seam::is_system_class_relid::call(relid, relkind, info.relnamespace)?
    {
        return ereport(ERROR)
            .errcode(::types_error::ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "permission denied: \"{}\" is a system catalog",
                rv.relname
            ))
            .finish(here("RangeVarCallbackForAlterRelation"));
    }

    // Extract the specified relation type from the statement; for ALTER .. RENAME
    // additionally require CREATE on the containing namespace.
    let (reltype, is_rename) = match &arg {
        AlterArg::Rename(stmt) => {
            let aclresult = aclchk_seam::object_aclcheck::call(
                NamespaceRelationId,
                info.relnamespace,
                miscinit_seams::get_user_id::call(),
                ACL_CREATE,
            )?;
            if aclresult != ACLCHECK_OK {
                let nspname = lsyscache_seam::get_namespace_name::call(mcx, info.relnamespace)?
                    .map(|s| s.as_str().to_string());
                aclchk_seam::aclcheck_error::call(aclresult, OBJECT_SCHEMA, nspname)?;
            }
            (stmt.renameType, true)
        }
        AlterArg::Schema(stmt) => (stmt.objectType, false),
    };

    // For compatibility, ALTER TABLE works on most relation types; the explicit
    // forms must match the relkind. ALTER INDEX is relaxed for RENAME.
    if reltype == OBJECT_SEQUENCE && relkind != RELKIND_SEQUENCE {
        return wrong_object_type(&rv.relname, "is not a sequence");
    }
    if reltype == OBJECT_VIEW && relkind != ::types_tuple::access::RELKIND_VIEW {
        return wrong_object_type(&rv.relname, "is not a view");
    }
    if reltype == OBJECT_MATVIEW && relkind != RELKIND_MATVIEW {
        return wrong_object_type(&rv.relname, "is not a materialized view");
    }
    if reltype == OBJECT_FOREIGN_TABLE && relkind != ::types_tuple::access::RELKIND_FOREIGN_TABLE {
        return wrong_object_type(&rv.relname, "is not a foreign table");
    }
    if reltype == OBJECT_TYPE && relkind != RELKIND_COMPOSITE_TYPE {
        return wrong_object_type(&rv.relname, "is not a composite type");
    }
    if reltype == OBJECT_INDEX
        && relkind != RELKIND_INDEX
        && relkind != RELKIND_PARTITIONED_INDEX
        && !is_rename
    {
        return wrong_object_type(&rv.relname, "is not an index");
    }

    // Don't allow ALTER TABLE on composite types — use ALTER TYPE instead.
    if reltype != OBJECT_TYPE && relkind == RELKIND_COMPOSITE_TYPE {
        return ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("\"{}\" is a composite type", rv.relname))
            .errhint("Use ALTER TYPE instead.".to_string())
            .finish(here("RangeVarCallbackForAlterRelation"));
    }

    // Don't allow SET SCHEMA on relations that can't be moved (indexes, composite
    // types, TOAST tables).
    if let AlterArg::Schema(_) = &arg {
        if relkind == RELKIND_INDEX || relkind == RELKIND_PARTITIONED_INDEX {
            return ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!("cannot change schema of index \"{}\"", rv.relname))
                .errhint("Change the schema of the table instead.".to_string())
                .finish(here("RangeVarCallbackForAlterRelation"));
        } else if relkind == RELKIND_COMPOSITE_TYPE {
            return ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!(
                    "cannot change schema of composite type \"{}\"",
                    rv.relname
                ))
                .errhint("Use ALTER TYPE instead.".to_string())
                .finish(here("RangeVarCallbackForAlterRelation"));
        } else if relkind == RELKIND_TOASTVALUE {
            return ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!(
                    "cannot change schema of TOAST table \"{}\"",
                    rv.relname
                ))
                .errhint("Change the schema of the table instead.".to_string())
                .finish(here("RangeVarCallbackForAlterRelation"));
        }
    }

    Ok(())
}

/// The `ereport(ERROR, ERRCODE_WRONG_OBJECT_TYPE, "\"%s\" <suffix>")` raised by
/// the relkind/objtype mismatch checks.
fn wrong_object_type(relname: &str, suffix: &str) -> PgResult<()> {
    ereport(ERROR)
        .errcode(ERRCODE_WRONG_OBJECT_TYPE)
        .errmsg(format!("\"{relname}\" {suffix}"))
        .finish(here("RangeVarCallbackForAlterRelation"))
}
