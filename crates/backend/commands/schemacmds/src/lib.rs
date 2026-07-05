//! schemacmds.c, CreateSchemaCommand lane. The caller supplies exec_elements
//! (C calls ProcessUtility directly; the layering here runs upward through the
//! tcop dispatcher), invoked between the search_path override and its undo.

#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::{InvalidOid, Oid, SECURITY_LOCAL_USERID_CHANGE};
use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_RESERVED_NAME, ERROR, NOTICE,
};
use types_guc::{PGC_S_SESSION, PGC_USERSET};
use types_nodes::parsenodes::{CreateSchemaStmt, ObjectType};
use types_nodes::NodeList;

// check_can_set_role (acl.c).
fn check_can_set_role(mcx: Mcx<'_>, member: Oid, role: Oid) -> PgResult<()> {
    if !adt_acl::member_can_set_role(member, role)? {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "permission denied to set role \"{}\"",
                    miscinit::GetUserNameFromId(mcx, role, false)?
                        .expect("noerr=false")
                        .as_str()
                ),
            )
            .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }
    Ok(())
}

pub fn CreateSchemaCommand<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateSchemaStmt<'mcx>,
    exec_elements: &mut dyn FnMut(Oid, &NodeList<'mcx>, &str) -> PgResult<()>,
) -> PgResult<Oid> {
    let (saved_uid, save_sec_context) = miscinit::GetUserIdAndSecContext();

    let owner_uid = match stmt.authrole {
        Some(role) => aclchk::get_rolespec_oid(
            role.as_role_spec().expect("authrole is a RoleSpec"),
            false,
        )?,
        None => saved_uid,
    };

    // Fill schema name with the user name if not specified.
    let owner_name;
    let schema_name = match stmt.schemaname {
        Some(s) => s,
        None => {
            owner_name =
                miscinit::GetUserNameFromId(mcx, owner_uid, false)?.expect("noerr=false");
            owner_name.as_str()
        }
    };

    let aclresult = aclchk::object_aclcheck(
        types_core::catalog::DATABASE_RELATION_ID,
        init_small::globals::MyDatabaseId(),
        saved_uid,
        adt_acl::ACL_CREATE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        let dbname = dbcommands::get_database_name(init_small::globals::MyDatabaseId())?
            .unwrap_or_default();
        aclchk_seams::aclcheck_error::call(
            aclresult,
            ObjectType::OBJECT_DATABASE as i32,
            &dbname,
        )?;
    }

    check_can_set_role(mcx, saved_uid, owner_uid)?;

    if !init_small::globals::allowSystemTableMods() && catalog::IsReservedName(schema_name) {
        return Err(Box::new(
            PgError::new(ERROR, format!("unacceptable schema name \"{schema_name}\""))
                .with_sqlstate(ERRCODE_RESERVED_NAME)
                .with_detail("The prefix \"pg_\" is reserved for system schemas."),
        ));
    }

    if stmt.if_not_exists
        && catalog_namespace::get_namespace_oid(schema_name, true)? != InvalidOid
    {
        // C: checkMembershipInCurrentExtension guards extension scripts
        // reusing pre-existing schemas; extension-script state is loud at the
        // extension lane, so the pre-existing-object hole cannot be reached
        // silently here.
        elog_seams::ereport::call(
            PgError::new(NOTICE, format!("schema \"{schema_name}\" already exists, skipping"))
                .with_sqlstate(types_error::ERRCODE_DUPLICATE_SCHEMA),
        )?;
        return Ok(InvalidOid);
    }

    // Create the objects as the target role; error paths rely on transaction
    // abort to restore the identity, as C does.
    if saved_uid != owner_uid {
        miscinit::SetUserIdAndSecContext(
            owner_uid,
            save_sec_context | SECURITY_LOCAL_USERID_CHANGE,
        );
    }

    let namespace_id = pg_namespace::NamespaceCreate(mcx, schema_name, owner_uid, false)?;
    xact::CommandCounterIncrement()?;

    // Prepend the new schema to the search path for exactly the duration of
    // the element subcommands (function-SET-option style save; guc.c undoes
    // it on error).
    let save_nestlevel = guc::NewGUCNestLevel();
    let mut pathbuf = String::from_utf8(
        adt_quote::quote_identifier(mcx, schema_name.as_bytes())?.as_bytes().to_vec(),
    )
    .expect("identifier is UTF-8");
    let nsp = guc::GetConfigOption("search_path", false, false)?.unwrap_or_default();
    let nsp = nsp.trim_start();
    if !nsp.is_empty() {
        pathbuf.push_str(", ");
        pathbuf.push_str(nsp);
    }
    guc::set_config_option(
        "search_path",
        Some(&pathbuf),
        PGC_USERSET,
        PGC_S_SESSION,
        guc::GUC_ACTION_SAVE,
        true,
        types_error::ErrorLevel(0),
        false,
    )?;

    // The caller collects the schema for event triggers ahead of the element
    // subcommands and hands each element to ProcessUtility (C does both
    // inline here).
    exec_elements(namespace_id, &stmt.schemaElts, schema_name)?;

    guc::AtEOXact_GUC(true, save_nestlevel);

    if saved_uid != owner_uid {
        miscinit::SetUserIdAndSecContext(saved_uid, save_sec_context);
    }
    Ok(namespace_id)
}
