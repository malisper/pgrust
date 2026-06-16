#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

//! `backend/commands/schemacmds.c` — CREATE / ALTER / RENAME SCHEMA.
//!
//! The four exported drivers ([`CreateSchemaCommand`], [`RenameSchema`],
//! [`AlterSchemaOwner`], [`AlterSchemaOwner_oid`]) plus the file-static
//! `AlterSchemaOwner_internal` reproduce the C branch order, permission
//! ordering, error codes / messages / SQLSTATEs, lock levels, the
//! reserved-name / IF NOT EXISTS / same-owner short-circuits, the search-path
//! string assembly, and the invalidation / event-trigger hooks.
//!
//! `CreateSchemaStmt` is a real owned node, so its fields (`schemaname`,
//! `authrole`, `if_not_exists`, `schemaElts`) are read directly. The
//! cross-subsystem externals cross through their owners' `-seams` crates;
//! `IsReservedName`, `get_namespace_oid`, `NamespaceCreate`, and
//! `checkMembershipInCurrentExtension` are reused directly from the ported
//! foundation crates.

use mcx::Mcx;

use backend_utils_error::ereport;
use types_acl::acl::{ACL_CREATE, ACLCHECK_NOT_OWNER, ACLCHECK_OK};
use types_catalog::catalog::{DATABASE_RELATION_ID, NAMESPACE_RELATION_ID};
use types_catalog::catalog_dependency::ObjectAddress;
use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_core::init::SECURITY_LOCAL_USERID_CHANGE;
use types_error::pg_error::{ErrorLocation, PgError};
use types_error::{PgResult, ERRCODE_DUPLICATE_SCHEMA, ERRCODE_RESERVED_NAME, ERRCODE_UNDEFINED_SCHEMA, ERROR, NOTICE};
use types_nodes::nodes::Node;
use types_nodes::ddlnodes::CreateSchemaStmt;
use types_nodes::parsenodes::{OBJECT_DATABASE, OBJECT_SCHEMA};

use backend_catalog_catalog::IsReservedName;
use backend_catalog_namespace::get_namespace_oid;
use backend_catalog_pg_depend::checkMembershipInCurrentExtension;
use backend_catalog_pg_namespace::NamespaceCreate;

use backend_catalog_aclchk_seams as aclchk;
use backend_catalog_indexing_seams as indexing;
use backend_catalog_objectaccess_seams as objectaccess;
use backend_catalog_pg_shdepend_seams as shdepend;
use backend_commands_dbcommands_seams as dbcommands;
use backend_commands_event_trigger_seams as event_trigger;
use backend_access_transam_xact_seams as xact;
use backend_parser_parse_utilcmd_seams as parse_utilcmd;
use backend_tcop_utility_fc_seams as utility;
use backend_utils_adt_acl_seams as acl;
use backend_utils_adt_ruleutils_seams as ruleutils;
use backend_utils_cache_syscache_seams as syscache;
use backend_utils_init_miscinit_seams as miscinit;
use backend_utils_init_small_seams as init_small;
use backend_utils_misc_guc_seams as guc;

const NamespaceRelationId: Oid = NAMESPACE_RELATION_ID;
const DatabaseRelationId: Oid = DATABASE_RELATION_ID;

/// `ErrorLocation` for `ereport(...).finish(...)` in this module.
fn here(lineno: i32, funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/commands/schemacmds.c", lineno, funcname)
}

/// `ObjectAddressSet(addr, class, object)` — sets `objectSubId = 0`.
fn object_address_set(class_id: Oid, object_id: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: 0,
    }
}

/// `InvalidObjectAddress` (`objectaddress.c`) — the all-`InvalidOid` /
/// `objectSubId = 0` sentinel address.
fn invalid_object_address() -> ObjectAddress {
    ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    }
}

/// `aclcheck_error(aclresult, OBJECT_DATABASE, get_database_name(MyDatabaseId))`
/// — the database CREATE-privilege failure shared by `CreateSchemaCommand`,
/// `RenameSchema`, and `AlterSchemaOwner_internal`. `aclcheck_error` always
/// raises; this returns its `Err`.
fn aclcheck_error_database(
    mcx: Mcx<'_>,
    aclresult: types_acl::acl::AclResult,
) -> PgResult<()> {
    let dbname = dbcommands::get_database_name::call(mcx, init_small::my_database_id::call())?
        .map(|s| s.as_str().to_string());
    aclchk::aclcheck_error::call(aclresult, OBJECT_DATABASE, dbname)
}

/// `scanner_isspace(ch)` (`parser/scansup.c`): space, tab, newline, carriage
/// return, or form feed.
fn scanner_isspace(ch: char) -> bool {
    matches!(ch, ' ' | '\t' | '\n' | '\r' | '\x0c')
}

/* =========================================================================
 * CreateSchemaCommand (schemacmds.c:51-242) — CREATE SCHEMA
 * ========================================================================= */

/// `CreateSchemaCommand(CreateSchemaStmt *stmt, const char *queryString,
/// int stmt_location, int stmt_len)` — CREATE SCHEMA.
pub fn CreateSchemaCommand<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateSchemaStmt<'_>,
    query_string: &str,
    stmt_location: i32,
    stmt_len: i32,
) -> PgResult<Oid> {
    let mut schemaName: Option<String> = stmt.schemaname.as_ref().map(|s| s.as_str().to_string());
    let namespaceId: Oid;
    let owner_uid: Oid;

    /* GetUserIdAndSecContext(&saved_uid, &save_sec_context); */
    let (saved_uid, save_sec_context) = miscinit::get_user_id_and_sec_context::call();

    /*
     * Who is supposed to own the new schema?
     */
    if let Some(authrole) = stmt_authrole(mcx, stmt)? {
        owner_uid = acl::get_rolespec_oid::call(&authrole, false)?;
    } else {
        owner_uid = saved_uid;
    }

    /* fill schema name with the user name if not specified */
    if schemaName.is_none() {
        /*
         * tuple = SearchSysCache1(AUTHOID, owner_uid);
         * if (!HeapTupleIsValid(tuple)) elog(ERROR, "cache lookup failed for role %u", owner_uid);
         * schemaName = pstrdup(NameStr(((Form_pg_authid) GETSTRUCT(tuple))->rolname));
         */
        match syscache::authid_rolname::call(mcx, owner_uid)? {
            Some(rolname) => schemaName = Some(rolname.as_str().to_string()),
            None => {
                return Err(PgError::error(format!(
                    "cache lookup failed for role {owner_uid}"
                )))
            }
        }
    }
    let schemaName = schemaName.expect("schemaName filled above");

    /*
     * To create a schema, must have schema-create privilege on the current
     * database and must be able to become the target role.
     */
    let aclresult = aclchk::object_aclcheck::call(
        DatabaseRelationId,
        init_small::my_database_id::call(),
        saved_uid,
        ACL_CREATE,
    )?;
    if aclresult != ACLCHECK_OK {
        aclcheck_error_database(mcx, aclresult)?;
    }

    acl::check_can_set_role::call(saved_uid, owner_uid)?;

    /* Additional check to protect reserved schema names */
    if !guc::allow_system_table_mods::call() && IsReservedName(&schemaName) {
        ereport(ERROR)
            .errcode(ERRCODE_RESERVED_NAME)
            .errmsg(format!("unacceptable schema name \"{schemaName}\""))
            .errdetail("The prefix \"pg_\" is reserved for system schemas.")
            .finish(here(108, "CreateSchemaCommand"))?;
    }

    /*
     * If if_not_exists was given and the schema already exists, bail out.
     */
    if stmt.if_not_exists {
        let existing = get_namespace_oid(&schemaName, true)?;
        if OidIsValid(existing) {
            /*
             * If we are in an extension script, insist that the pre-existing
             * object be a member of the extension, to avoid security risks.
             */
            let address = object_address_set(NamespaceRelationId, existing);
            checkMembershipInCurrentExtension(mcx, &address)?;

            /* OK to skip */
            ereport(NOTICE)
                .errcode(ERRCODE_DUPLICATE_SCHEMA)
                .errmsg(format!("schema \"{schemaName}\" already exists, skipping"))
                .finish(here(132, "CreateSchemaCommand"))?;
            return Ok(InvalidOid);
        }
    }

    /*
     * If the requested authorization is different from the current user,
     * temporarily set the current user so that the object(s) will be created
     * with the correct ownership.
     */
    if saved_uid != owner_uid {
        miscinit::set_user_id_and_sec_context::call(
            owner_uid,
            save_sec_context | SECURITY_LOCAL_USERID_CHANGE,
        );
    }

    /* Create the schema's namespace */
    namespaceId = NamespaceCreate(&schemaName, owner_uid, false)?;

    /* Advance cmd counter to make the namespace visible */
    xact::command_counter_increment::call()?;

    /*
     * Prepend the new schema to the current search path.
     */
    let save_nestlevel = guc::new_guc_nest_level::call();

    /*
     * initStringInfo(&pathbuf);
     * appendStringInfoString(&pathbuf, quote_identifier(schemaName));
     */
    let mut pathbuf = ruleutils::quote_identifier::call(mcx, &schemaName)?
        .as_str()
        .to_string();

    /*
     * char *nsp = namespace_search_path;
     * while (scanner_isspace(*nsp)) nsp++;
     */
    let nsp_full = backend_catalog_namespace::namespace_search_path();
    let nsp = nsp_full.trim_start_matches(scanner_isspace);

    /* if (*nsp != '\0') appendStringInfo(&pathbuf, ", %s", nsp); */
    if !nsp.is_empty() {
        pathbuf.push_str(", ");
        pathbuf.push_str(nsp);
    }

    /*
     * (void) set_config_option("search_path", pathbuf.data, PGC_USERSET,
     *                          PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);
     */
    guc::set_search_path_save::call(&pathbuf)?;

    /*
     * Report the new schema to possibly interested event triggers.
     */
    let address = object_address_set(NamespaceRelationId, namespaceId);
    /* EventTriggerCollectSimpleCommand(address, InvalidObjectAddress, (Node *) stmt); */
    event_trigger::event_trigger_collect_simple_command_create_schema::call(
        address,
        invalid_object_address(),
        stmt,
    )?;

    /*
     * Examine the list of commands embedded in the CREATE SCHEMA command, and
     * reorganize them into a sequentially executable order.
     */
    let schema_elts: Vec<Node<'_>> = stmt
        .schemaElts
        .iter()
        .map(|n| (**n).clone_in(mcx))
        .collect::<PgResult<Vec<_>>>()?;
    let parsetree_list =
        parse_utilcmd::transformCreateSchemaStmtElements::call(mcx, &schema_elts, &schemaName)?;

    /*
     * Execute each command contained in the CREATE SCHEMA. The grammar allows
     * only utility commands, so hand them straight to ProcessUtility.
     */
    for substmt in parsetree_list.iter() {
        /*
         * makeNode(PlannedStmt): commandType = CMD_UTILITY; canSetTag = false;
         * utilityStmt = stmt; stmt_location; stmt_len; then
         * ProcessUtility(wrapper, queryString, false,
         *                PROCESS_UTILITY_SUBCOMMAND, NULL, NULL,
         *                None_Receiver, NULL);
         */
        utility::process_utility_create_schema_subcommand::call(
            substmt,
            query_string,
            stmt_location,
            stmt_len,
        )?;

        /* make sure later steps can see the object created here */
        xact::command_counter_increment::call()?;
    }

    /* Restore the GUC variable search_path we set above. */
    guc::at_eoxact_guc::call(true, save_nestlevel)?;

    /* Reset current user and security context */
    miscinit::set_user_id_and_sec_context::call(saved_uid, save_sec_context);

    Ok(namespaceId)
}

/* =========================================================================
 * RenameSchema (schemacmds.c:248-304) — Rename schema
 * ========================================================================= */

/// `RenameSchema(const char *oldname, const char *newname)` — rename schema.
pub fn RenameSchema<'mcx>(
    mcx: Mcx<'mcx>,
    oldname: &str,
    newname: &str,
) -> PgResult<ObjectAddress> {
    /*
     * rel = table_open(NamespaceRelationId, RowExclusiveLock);
     * tup = SearchSysCacheCopy1(NAMESPACENAME, oldname);
     *
     * The relation open + RowExclusiveLock-protected tuple read/write are
     * encapsulated by the indexing-owned `rename_namespace_tuple` seam; here
     * we read the row through the syscache for the existence + oid.
     */
    let Some((nspOid, _nspowner, _nspname)) =
        syscache::namespace_owner_row_by_name::call(mcx, oldname)?
    else {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_SCHEMA)
            .errmsg(format!("schema \"{oldname}\" does not exist"))
            .finish(here(262, "RenameSchema"))
            .map(|()| invalid_object_address());
    };

    /* make sure the new name doesn't exist */
    if OidIsValid(get_namespace_oid(newname, true)?) {
        return ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_SCHEMA)
            .errmsg(format!("schema \"{newname}\" already exists"))
            .finish(here(271, "RenameSchema"))
            .map(|()| invalid_object_address());
    }

    /* must be owner */
    if !aclchk::object_ownercheck::call(NamespaceRelationId, nspOid, miscinit::get_user_id::call())? {
        aclchk::aclcheck_error::call(ACLCHECK_NOT_OWNER, OBJECT_SCHEMA, Some(oldname.to_string()))?;
    }

    /* must have CREATE privilege on database */
    let aclresult = aclchk::object_aclcheck::call(
        DatabaseRelationId,
        init_small::my_database_id::call(),
        miscinit::get_user_id::call(),
        ACL_CREATE,
    )?;
    if aclresult != ACLCHECK_OK {
        aclcheck_error_database(mcx, aclresult)?;
    }

    if !guc::allow_system_table_mods::call() && IsReservedName(newname) {
        return ereport(ERROR)
            .errcode(ERRCODE_RESERVED_NAME)
            .errmsg(format!("unacceptable schema name \"{newname}\""))
            .errdetail("The prefix \"pg_\" is reserved for system schemas.")
            .finish(here(287, "RenameSchema"))
            .map(|()| invalid_object_address());
    }

    /*
     * rename:
     *   namestrcpy(&nspform->nspname, newname);
     *   CatalogTupleUpdate(rel, &tup->t_self, tup);
     */
    indexing::rename_namespace_tuple::call(nspOid, newname)?;

    objectaccess::invoke_object_post_alter_hook::call(NamespaceRelationId, nspOid, 0)?;

    let address = object_address_set(NamespaceRelationId, nspOid);

    /* table_close(rel, NoLock); heap_freetuple(tup); — owned by the seam. */

    Ok(address)
}

/* =========================================================================
 * AlterSchemaOwner_oid (schemacmds.c:306-323)
 * ========================================================================= */

/// `AlterSchemaOwner_oid(Oid schemaoid, Oid newOwnerId)` — change schema owner,
/// by OID.
pub fn AlterSchemaOwner_oid<'mcx>(
    mcx: Mcx<'mcx>,
    schemaoid: Oid,
    newOwnerId: Oid,
) -> PgResult<()> {
    /*
     * rel = table_open(NamespaceRelationId, RowExclusiveLock);
     * tup = SearchSysCache1(NAMESPACEOID, schemaoid);
     * if (!HeapTupleIsValid(tup)) elog(ERROR, "cache lookup failed for schema %u", schemaoid);
     */
    let Some((nsp_oid, nsp_owner, nspname)) =
        syscache::namespace_owner_row_by_oid::call(mcx, schemaoid)?
    else {
        return Err(PgError::error(format!(
            "cache lookup failed for schema {schemaoid}"
        )));
    };

    AlterSchemaOwner_internal(mcx, nsp_oid, &nspname.as_str().to_string(), nsp_owner, newOwnerId)?;

    /* ReleaseSysCache(tup); table_close(rel, RowExclusiveLock); — owned by seams. */

    Ok(())
}

/* =========================================================================
 * AlterSchemaOwner (schemacmds.c:329-358) — Change schema owner
 * ========================================================================= */

/// `AlterSchemaOwner(const char *name, Oid newOwnerId)` — change schema owner,
/// by name.
pub fn AlterSchemaOwner<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    newOwnerId: Oid,
) -> PgResult<ObjectAddress> {
    let Some((nsp_oid, nsp_owner, nspname)) =
        syscache::namespace_owner_row_by_name::call(mcx, name)?
    else {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_SCHEMA)
            .errmsg(format!("schema \"{name}\" does not exist"))
            .finish(here(342, "AlterSchemaOwner"))
            .map(|()| invalid_object_address());
    };

    AlterSchemaOwner_internal(mcx, nsp_oid, &nspname.as_str().to_string(), nsp_owner, newOwnerId)?;

    let address = object_address_set(NamespaceRelationId, nsp_oid);

    Ok(address)
}

/* =========================================================================
 * AlterSchemaOwner_internal (schemacmds.c:360-442)
 * ========================================================================= */

/// `AlterSchemaOwner_internal(HeapTuple tup, Relation rel, Oid newOwnerId)` —
/// static helper applying the owner change. The `(nsp_oid, nspname, nsp_owner)`
/// triple is what `GETSTRUCT(tup)` reads.
fn AlterSchemaOwner_internal(
    mcx: Mcx<'_>,
    nsp_oid: Oid,
    nspname: &str,
    nsp_owner: Oid,
    newOwnerId: Oid,
) -> PgResult<()> {
    /*
     * If the new owner is the same as the existing owner, consider the command
     * to have succeeded.  This is for dump restoration purposes.
     */
    if nsp_owner != newOwnerId {
        /* Otherwise, must be owner of the existing object */
        if !aclchk::object_ownercheck::call(NamespaceRelationId, nsp_oid, miscinit::get_user_id::call())? {
            aclchk::aclcheck_error::call(
                ACLCHECK_NOT_OWNER,
                OBJECT_SCHEMA,
                Some(nspname.to_string()),
            )?;
        }

        /* Must be able to become new owner */
        acl::check_can_set_role::call(miscinit::get_user_id::call(), newOwnerId)?;

        /*
         * must have create-schema rights
         *
         * NOTE: the current user is checked for create privileges instead of
         * the destination owner.  This is consistent with the CREATE case.
         */
        let aclresult = aclchk::object_aclcheck::call(
            DatabaseRelationId,
            init_small::my_database_id::call(),
            miscinit::get_user_id::call(),
            ACL_CREATE,
        )?;
        if aclresult != ACLCHECK_OK {
            aclcheck_error_database(mcx, aclresult)?;
        }

        /*
         * repl_repl[nspowner] = true; repl_val[nspowner] = newOwnerId;
         * aclDatum = SysCacheGetAttr(NAMESPACENAME, tup, nspacl, &isNull);
         * if (!isNull) newAcl = aclnewowner(..., nspForm->nspowner, newOwnerId);
         * newtuple = heap_modify_tuple(...); CatalogTupleUpdate(...);
         * heap_freetuple(newtuple);
         */
        indexing::update_namespace_owner_tuple::call(nsp_oid, nsp_owner, newOwnerId)?;

        /* Update owner dependency reference */
        shdepend::changeDependencyOnOwner::call(NamespaceRelationId, nsp_oid, newOwnerId)?;
    }

    objectaccess::invoke_object_post_alter_hook::call(NamespaceRelationId, nsp_oid, 0)?;

    Ok(())
}

/* -------------------------------------------------------------------------
 * Small helpers
 * ------------------------------------------------------------------------- */

/// `if (stmt->authrole)` → the owned `authrole` node, downcast to its
/// `RoleSpec`. `CREATE SCHEMA AUTHORIZATION` always parses a `RoleSpec`; any
/// other node tag is a malformed parse tree (`errmsg_internal`-class bug).
///
/// The `Node::RoleSpec` arena variant carries `ddlnodes::RoleSpec`; the
/// `get_rolespec_oid` seam reads the `parsenodes::RoleSpec` view (same
/// `roletype`/`rolename`). The view is built in `mcx` (C: the same node, read
/// directly).
fn stmt_authrole<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateSchemaStmt<'_>,
) -> PgResult<Option<types_nodes::parsenodes::RoleSpec<'mcx>>> {
    match stmt.authrole.as_ref() {
        None => Ok(None),
        Some(node) => match &**node {
            Node::RoleSpec(rs) => Ok(Some(types_nodes::parsenodes::RoleSpec {
                roletype: rs.roletype,
                rolename: match &rs.rolename {
                    Some(s) => Some(mcx::PgString::from_str_in(s.as_str(), mcx)?),
                    None => None,
                },
            })),
            other => Err(ereport(ERROR)
                .errmsg_internal(format!(
                    "CreateSchemaStmt.authrole is not a RoleSpec (got {:?})",
                    other.tag()
                ))
                .into_error()),
        },
    }
}

/// Install this crate's inward seam ([`backend_commands_schemacmds_seams`]).
pub fn init_seams() {
    backend_commands_schemacmds_seams::alter_schema_owner_oid::set(|schema_oid, new_owner_id| {
        let ctx = mcx::MemoryContext::new("alter_schema_owner_oid");
        AlterSchemaOwner_oid(ctx.mcx(), schema_oid, new_owner_id)
    });
}
