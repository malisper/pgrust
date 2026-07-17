//! ALTER EXTENSION ADD/DROP (extension.c ExecAlterExtensionContentsStmt +
//! ExecAlterExtensionContentsRecurse). Type-recursion (array/multirange/
//! rowtype members) and extconfig removal stay loud; pg_init_privs
//! record/remove is a no-op repo-wide (see aclchk grant.rs).

use mcx::Mcx;
use types_core::{
    Oid, EXTENSION_RELATION_ID, NAMESPACE_RELATION_ID, RELATION_RELATION_ID, TYPE_RELATION_ID,
};
use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_OBJECT_DEFINITION,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
};
use types_nodes::parsenodes::ObjectType;
use types_nodes::rawnodes::AlterExtensionContentsStmt;
use types_nodes::Node;
use types_rel::{AccessShareLock, ShareUpdateExclusiveLock};

use pg_depend::{DependencyType, ObjectAddress};

use crate::unported;

/// `ExecAlterExtensionContentsStmt`.
pub fn ExecAlterExtensionContentsStmt<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterExtensionContentsStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let extname = stmt.extname.expect("grammar always supplies extname");

    match stmt.objtype {
        ObjectType::OBJECT_DATABASE
        | ObjectType::OBJECT_EXTENSION
        | ObjectType::OBJECT_INDEX
        | ObjectType::OBJECT_PUBLICATION
        | ObjectType::OBJECT_ROLE
        | ObjectType::OBJECT_STATISTIC_EXT
        | ObjectType::OBJECT_SUBSCRIPTION
        | ObjectType::OBJECT_TABLESPACE => {
            return Err(Box::new(
                PgError::error("cannot add an object of this type to an extension")
                    .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
            ));
        }
        _ => {}
    }

    // Shared lock on the extension so it can't be dropped concurrently.
    let (extension, ext_rel) = objectaddress_seams::get_object_address::call(
        mcx,
        ObjectType::OBJECT_EXTENSION,
        Node::mk_string(mcx, extname)?,
        AccessShareLock,
        false,
    )?;
    debug_assert!(ext_rel.is_none());

    if !aclchk::object_ownercheck(EXTENSION_RELATION_ID, extension.objectId, miscinit::GetUserId())?
    {
        return Err(Box::new(
            PgError::error(format!("must be owner of extension {extname}"))
                .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }

    let object_node = stmt.object.expect("grammar always supplies the member object");
    let (object, relation) = objectaddress_seams::get_object_address::call(
        mcx,
        stmt.objtype,
        object_node,
        ShareUpdateExclusiveLock,
        false,
    )?;
    debug_assert!(object.objectSubId == 0);

    objectaddress_seams::check_object_ownership::call(
        mcx,
        miscinit::GetUserId(),
        stmt.objtype,
        object,
        object_node,
        relation.as_ref(),
    )?;

    let extension =
        ObjectAddress { classId: extension.classId, objectId: extension.objectId, objectSubId: 0 };
    let object =
        ObjectAddress { classId: object.classId, objectId: object.objectId, objectSubId: 0 };
    alter_contents_recurse(mcx, stmt, extname, &extension, &object)?;

    // C: InvokeObjectPostAlterHook (no object_access hooks in this port),
    // then relation_close(NoLock) — the Relation guard drop is that close.
    drop(relation);

    Ok(extension)
}

/// `ExecAlterExtensionContentsRecurse`.
fn alter_contents_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterExtensionContentsStmt<'mcx>,
    extname: &str,
    extension: &ObjectAddress,
    object: &ObjectAddress,
) -> PgResult<()> {
    let old_extension = pg_depend::getExtensionOfObject(mcx, object.classId, object.objectId)?;

    if stmt.action > 0 {
        if old_extension != 0 {
            let desc = objectaddress_seams::get_object_description::call(
                mcx,
                object.classId,
                object.objectId,
                0,
                false,
            )?
            .unwrap_or_else(|| "object".into());
            let owner = crate::get_extension_name(mcx, old_extension)?
                .map(|s| s.to_string())
                .unwrap_or_default();
            return Err(Box::new(
                PgError::error(format!("{desc} is already a member of extension \"{owner}\""))
                    .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            ));
        }
        if object.classId == NAMESPACE_RELATION_ID
            && object.objectId == crate::get_extension_schema(extension.objectId)?
        {
            let nsp = lsyscache::get_namespace_name(mcx, object.objectId)?
                .map(|s| s.to_string())
                .unwrap_or_default();
            return Err(Box::new(
                PgError::error(format!(
                    "cannot add schema \"{nsp}\" to extension \"{extname}\" because the schema contains the extension"
                ))
                .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            ));
        }
        pg_depend::recordDependencyOn(mcx, object, extension, DependencyType::Extension)?;
        // recordExtObjInitPriv: pg_init_privs is a repo-wide no-op.
    } else {
        if old_extension != extension.objectId {
            let desc = objectaddress_seams::get_object_description::call(
                mcx,
                object.classId,
                object.objectId,
                0,
                false,
            )?
            .unwrap_or_else(|| "object".into());
            return Err(Box::new(
                PgError::error(format!("{desc} is not a member of extension \"{extname}\""))
                    .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            ));
        }
        if pg_depend::deleteDependencyRecordsForClass(
            mcx,
            object.classId,
            object.objectId,
            EXTENSION_RELATION_ID,
            DependencyType::Extension,
        )? != 1
        {
            return Err(PgError::error("unexpected number of extension dependency records").into());
        }
        if object.classId == RELATION_RELATION_ID {
            extension_config_remove(extension.objectId)?;
        }
        // removeExtObjInitPriv: pg_init_privs is a repo-wide no-op.
    }

    if object.classId == TYPE_RELATION_ID {
        // unported: ALTER EXTENSION ADD/DROP TYPE dependent-object recursion
        return Err(Box::new(
            types_error::PgError::error(
                "ALTER EXTENSION ... ADD/DROP TYPE is not supported yet",
            )
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    Ok(())
}

/// `extension_config_remove` (extension.c): drop the table from the
/// extension's extconfig/extcondition arrays. pg_extension_config_dump is
/// unported, so a populated array cannot exist yet — the null fast path is
/// the whole live surface, anything else is loud.
fn extension_config_remove(ext_oid: Oid) -> PgResult<()> {
    use cache_syscache::{ReleaseSysCache, SearchSysCache1, SysCacheGetAttr, SysCacheKey, EXTENSIONOID};
    let Some(tuple) =
        SearchSysCache1(EXTENSIONOID, SysCacheKey::Value(datum::Datum::from_oid(ext_oid)))?
    else {
        return Ok(());
    };
    let (_, isnull) = SysCacheGetAttr(EXTENSIONOID, &tuple, crate::Anum_pg_extension_extconfig)?;
    ReleaseSysCache(tuple);
    if !isnull {
        unported("extension_config_remove with a populated extconfig array");
    }
    Ok(())
}
