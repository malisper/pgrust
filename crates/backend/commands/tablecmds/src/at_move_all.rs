//! `commands/tablecmds.c` — ALTER (TABLE|INDEX|MATERIALIZED VIEW) ALL IN
//! TABLESPACE ... [OWNED BY ...] SET TABLESPACE ... (`AlterTableMoveAll`,
//! tablecmds.c:16985).
//!
//! Scans `pg_class` for every relation in the source tablespace whose relkind
//! matches the requested object type, skipping catalog/shared/temp/toast
//! relations and (if `OWNED BY` was given) relations not owned by one of the
//! named roles, then drives `ATExecSetTableSpace` per relation via
//! `AlterTableInternal` inside an event-trigger fence. The per-relation work
//! (`ATExecSetTableSpace`) is already ported in `at_tablespace.rs`.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::vec::Vec;

use mcx::{Mcx, PgVec, PgString};
use types_core::primitive::{Oid, InvalidOid, OidIsValid};
use types_error::PgResult;
use types_error::{ERROR, NOTICE, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_NO_DATA_FOUND};
use nodes::nodes::{Node, NodePtr};
use nodes::ddlnodes::{AlterTableCmd, AlterTableType};
use nodes::parsenodes::{
    DropBehavior, OBJECT_TABLE, OBJECT_INDEX, OBJECT_MATVIEW, OBJECT_TABLESPACE,
};
use types_tuple::heaptuple::Datum;
use types_acl::{ACLCHECK_OK, ACL_CREATE, ACLCHECK_NOT_OWNER};
use types_catalog::catalog::{
    RELKIND_RELATION, RELKIND_PARTITIONED_TABLE, RELKIND_INDEX, RELKIND_PARTITIONED_INDEX,
    RELKIND_MATVIEW, GLOBALTABLESPACE_OID,
};
use types_catalog::pg_class::{
    Anum_pg_class_oid, Anum_pg_class_relname, Anum_pg_class_relnamespace,
    Anum_pg_class_relowner, Anum_pg_class_relisshared, Anum_pg_class_relkind,
    Anum_pg_class_reltablespace,
};
use types_storage::lock::{AccessShareLock, AccessExclusiveLock};

use heaptuple::heap_deform_tuple;
use genam_seams as genam;
use table::{table_open, table_close};
use miscinit::GetUserId;
use utils_error::ereport;

use aclchk_seams as aclchk_seam;
use catalog_seams as catalog_seam;
use namespace_seams as namespace_seam;
use tablespace_seams as tablespace_seam;
use tablespace_globals_seams as tablespace_globals_seam;
use lsyscache_seams as lsyscache_seam;
use acl_seams as acl_seam;
use lmgr::{LockRelationOid, ConditionalLockRelationOid};

use crate::helpers::{here, RelationRelationId, TableSpaceRelationId};

/// `roleSpecsToIds(memberNames)` (user.c:1651) — given a list of `RoleSpec`
/// nodes, generate a list of role OIDs in the same order. `ROLESPEC_PUBLIC`
/// rejection is handled inside `get_rolespec_oid`.
fn roleSpecsToIds<'mcx>(
    mcx: Mcx<'mcx>,
    memberNames: &PgVec<'_, NodePtr<'_>>,
) -> PgResult<Vec<Oid>> {
    let mut result: Vec<Oid> = Vec::new();
    for n in memberNames.iter() {
        let rolespec = n
            .as_rolespec()
            .expect("AlterTableMoveAllStmt.roles element is a RoleSpec");
        // `get_rolespec_oid` reads the `parsenodes::RoleSpec` view (same
        // `roletype` / `rolename`); reproject the `ddlnodes::RoleSpec` node.
        let view = nodes::parsenodes::RoleSpec {
            roletype: rolespec.roletype,
            rolename: match &rolespec.rolename {
                Some(s) => Some(PgString::from_str_in(s.as_str(), mcx)?),
                None => None,
            },
        };
        let roleid = acl_seam::get_rolespec_oid::call(&view, false)?;
        result.push(roleid);
    }
    Ok(result)
}

/// `NameStr` of a `pg_class.relname` deformed column (NameData, by-reference).
fn name_str(col: &(Datum<'_>, bool)) -> String {
    match &col.0 {
        Datum::ByRef(b) => {
            let len = b.iter().position(|&c| c == 0).unwrap_or(b.len());
            String::from_utf8_lossy(&b[..len]).into_owned()
        }
        _ => String::new(),
    }
}

/// `AlterTableMoveAll(stmt)` (tablecmds.c:16985). Returns the destination
/// tablespace OID.
pub fn AlterTableMoveAll<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &nodes::ddlnodes::AlterTableMoveAllStmt<'mcx>,
) -> PgResult<Oid> {
    let role_oids = roleSpecsToIds(mcx, &stmt.roles)?;

    // Ensure we were not asked to move something we can't.
    if stmt.objtype != OBJECT_TABLE
        && stmt.objtype != OBJECT_INDEX
        && stmt.objtype != OBJECT_MATVIEW
    {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("only tables, indexes, and materialized views exist in tablespaces".to_string())
            .finish(here("AlterTableMoveAll"))
            .map(|()| unreachable!());
    }

    // Get the orig and new tablespace OIDs.
    let orig_name = stmt
        .orig_tablespacename
        .as_ref()
        .expect("AlterTableMoveAllStmt.orig_tablespacename is non-NULL")
        .as_str();
    let new_name = stmt
        .new_tablespacename
        .as_ref()
        .expect("AlterTableMoveAllStmt.new_tablespacename is non-NULL")
        .as_str();
    let mut orig_tablespaceoid = tablespace_seam::get_tablespace_oid::call(orig_name, false)?;
    let mut new_tablespaceoid = tablespace_seam::get_tablespace_oid::call(new_name, false)?;

    // Can't move shared relations in to or out of pg_global. (This is also
    // checked by ATExecSetTableSpace, but nice to stop earlier.)
    if orig_tablespaceoid == GLOBALTABLESPACE_OID || new_tablespaceoid == GLOBALTABLESPACE_OID {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("cannot move relations in to or out of pg_global tablespace".to_string())
            .finish(here("AlterTableMoveAll"))
            .map(|()| unreachable!());
    }

    // Must have CREATE rights on the new tablespace, unless it is the database
    // default tablespace (which all users implicitly have CREATE rights on).
    let my_db_tablespace = tablespace_globals_seam::MyDatabaseTableSpace::call()?;
    if OidIsValid(new_tablespaceoid) && new_tablespaceoid != my_db_tablespace {
        let aclresult = aclchk_seam::object_aclcheck::call(
            TableSpaceRelationId,
            new_tablespaceoid,
            GetUserId(),
            ACL_CREATE,
        )?;
        if aclresult != ACLCHECK_OK {
            let ts_name = tablespace_seam::get_tablespace_name::call(mcx, new_tablespaceoid)?
                .map(|s| s.as_str().to_string());
            aclchk_seam::aclcheck_error::call(aclresult, OBJECT_TABLESPACE, ts_name)?;
        }
    }

    // Now that the checks are done, check if we should set either to InvalidOid
    // because it is our database's default tablespace.
    if orig_tablespaceoid == my_db_tablespace {
        orig_tablespaceoid = InvalidOid;
    }
    if new_tablespaceoid == my_db_tablespace {
        new_tablespaceoid = InvalidOid;
    }

    // no-op
    if orig_tablespaceoid == new_tablespaceoid {
        return Ok(new_tablespaceoid);
    }

    // Walk the list of objects in the tablespace and move them. This will only
    // find objects in our database, of course.
    //
    //   ScanKeyInit(&key[0], Anum_pg_class_reltablespace,
    //               BTEqualStrategyNumber, F_OIDEQ,
    //               ObjectIdGetDatum(orig_tablespaceoid));
    //   rel = table_open(RelationRelationId, AccessShareLock);
    //   scan = table_beginscan_catalog(rel, 1, key);
    let mut key = types_scan::scankey::ScanKeyData::empty();
    scankey::ScanKeyInit(
        &mut key,
        Anum_pg_class_reltablespace,
        types_scan::scankey::BTEqualStrategyNumber,
        types_core::fmgr::F_OIDEQ,
        Datum::from_oid(orig_tablespaceoid),
    )?;
    let keys = [key];

    let rel = table_open(mcx, RelationRelationId, AccessShareLock)?;
    let mut scan = genam::systable_beginscan::call(&rel, Oid::default(), false, None, &keys)?;

    let mut relations: Vec<Oid> = Vec::new();
    while let Some(tup) = genam::systable_getnext::call(mcx, scan.desc_mut())? {
        let cols = heap_deform_tuple(mcx, &tup.tuple, &rel.rd_att, &tup.data)?;
        let relOid = cols[(Anum_pg_class_oid - 1) as usize].0.as_oid();
        let relnamespace = cols[(Anum_pg_class_relnamespace - 1) as usize].0.as_oid();
        let relisshared = cols[(Anum_pg_class_relisshared - 1) as usize].0.as_bool();
        let relkind = cols[(Anum_pg_class_relkind - 1) as usize].0.as_u8();
        let relowner = cols[(Anum_pg_class_relowner - 1) as usize].0.as_oid();
        let relname = name_str(&cols[(Anum_pg_class_relname - 1) as usize]);

        // Do not move objects in pg_catalog as part of this, if an admin really
        // wishes to do so, they can issue the individual ALTER commands directly.
        //
        // Also, explicitly avoid any shared tables, temp tables, or TOAST (TOAST
        // will be moved with the main table).
        if catalog_seam::is_catalog_namespace::call(relnamespace)
            || relisshared
            || namespace_seam::is_any_temp_namespace::call(mcx, relnamespace)?
            || catalog_seam::is_toast_namespace::call(relnamespace)
        {
            continue;
        }

        // Only move the object type requested.
        if (stmt.objtype == OBJECT_TABLE
            && relkind != RELKIND_RELATION
            && relkind != RELKIND_PARTITIONED_TABLE)
            || (stmt.objtype == OBJECT_INDEX
                && relkind != RELKIND_INDEX
                && relkind != RELKIND_PARTITIONED_INDEX)
            || (stmt.objtype == OBJECT_MATVIEW && relkind != RELKIND_MATVIEW)
        {
            continue;
        }

        // Check if we are only moving objects owned by certain roles.
        if !role_oids.is_empty() && !role_oids.contains(&relowner) {
            continue;
        }

        // Handle permissions-checking here since we are locking the tables and
        // also to avoid doing a bunch of work only to fail part-way. Note that
        // permissions will also be checked by AlterTableInternal().
        //
        // Caller must be considered an owner on the table to move it.
        if !aclchk_seam::object_ownercheck::call(RelationRelationId, relOid, GetUserId())? {
            let actual_relkind = lsyscache_seam::get_rel_relkind::call(relOid)?;
            let objtype = objectaddress::resolve::get_relkind_objtype(actual_relkind);
            aclchk_seam::aclcheck_error::call(ACLCHECK_NOT_OWNER, objtype, Some(relname.clone()))?;
        }

        if stmt.nowait && !ConditionalLockRelationOid(relOid, AccessExclusiveLock)? {
            let nspname = lsyscache_seam::get_namespace_name::call(mcx, relnamespace)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            return ereport(ERROR)
                .errcode(types_error::ERRCODE_OBJECT_IN_USE)
                .errmsg(format!(
                    "aborting because lock on relation \"{}.{}\" is not available",
                    nspname, relname
                ))
                .finish(here("AlterTableMoveAll"))
                .map(|()| unreachable!());
        } else {
            LockRelationOid(relOid, AccessExclusiveLock)?;
        }

        // Add to our list of objects to move.
        relations.push(relOid);
    }

    scan.end()?;
    table_close(rel, AccessShareLock)?;

    if relations.is_empty() {
        let where_name = if orig_tablespaceoid == InvalidOid {
            "(database default)".to_string()
        } else {
            tablespace_seam::get_tablespace_name::call(mcx, orig_tablespaceoid)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default()
        };
        ereport(NOTICE)
            .errcode(ERRCODE_NO_DATA_FOUND)
            .errmsg(format!(
                "no matching relations in tablespace \"{}\" found",
                where_name
            ))
            .finish(here("AlterTableMoveAll"))?;
    }

    // Everything is locked, loop through and move all of the relations.
    for &relOid in relations.iter() {
        let cmd = AlterTableCmd {
            subtype: AlterTableType::AT_SetTableSpace,
            name: Some(PgString::from_str_in(new_name, mcx)?),
            num: 0,
            newowner: None,
            def: None,
            behavior: DropBehavior::Restrict,
            missing_ok: false,
            recurse: false,
        };
        let cmdnode = mcx::alloc_in(mcx, Node::mk_alter_table_cmd(mcx, cmd)?)?;
        let mut cmds: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
        cmds.push(cmdnode);

        // EventTriggerAlterTableStart((Node *) stmt) — pass the original
        // AlterTableMoveAllStmt parse node.
        let stmt_node =
            mcx::alloc_in(mcx, Node::mk_alter_table_move_all_stmt(mcx, stmt.clone_in(mcx)?)?)?;
        utility_out_seams::event_trigger_alter_table_start::call(&stmt_node)?;
        // OID is set by AlterTableInternal.
        crate::at_phase::AlterTableInternal(mcx, relOid, &cmds, false)?;
        utility_out_seams::event_trigger_alter_table_end::call()?;
    }

    Ok(new_tablespaceoid)
}
