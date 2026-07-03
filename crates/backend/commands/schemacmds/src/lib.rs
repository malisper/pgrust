//! schemacmds.c, CreateSchemaCommand lane. The search_path GUC save/set around
//! element processing is skipped: schema elements are loud at the parser, so
//! the saved path could never be observed.

#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_RESERVED_NAME, ERROR, NOTICE};
use types_nodes::parsenodes::{CreateSchemaStmt, ObjectType};

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: schemacmds {what}")
}

pub fn CreateSchemaCommand<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateSchemaStmt<'mcx>,
) -> PgResult<Oid> {
    if stmt.authrole.is_some() {
        unported("CreateSchemaCommand: AUTHORIZATION");
    }
    if !stmt.schemaElts.is_nil() {
        unported("CreateSchemaCommand: schema elements");
    }
    let schema_name = stmt.schemaname.expect("grammar always supplies schemaname");

    let (saved_uid, _sec_context) = miscinit::GetUserIdAndSecContext();
    let owner_uid = saved_uid;

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
        elog_seams::ereport::call(
            PgError::new(NOTICE, format!("schema \"{schema_name}\" already exists, skipping"))
                .with_sqlstate(types_error::ERRCODE_DUPLICATE_SCHEMA),
        )?;
        return Ok(InvalidOid);
    }

    let namespace_id = pg_namespace::NamespaceCreate(mcx, schema_name, owner_uid, false)?;
    xact::CommandCounterIncrement()?;
    Ok(namespace_id)
}
