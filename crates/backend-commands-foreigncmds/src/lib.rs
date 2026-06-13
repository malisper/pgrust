#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

//! `backend/commands/foreigncmds.c` — CREATE / ALTER / DROP of FOREIGN DATA
//! WRAPPER / SERVER / USER MAPPING / FOREIGN TABLE, and IMPORT FOREIGN SCHEMA.
//!
//! This crate owns the SET/ADD/DROP `DefElem` merge of
//! `transformGenericOptions`, the HANDLER/VALIDATOR option parse, the
//! superuser/owner/USAGE permission decisions, the IF NOT EXISTS / duplicate
//! checks, the WARNING/NOTICE emissions, and the dependency-recording
//! orchestration. In the owned tree the C `Datum`/`HeapTuple`/`values[]`/
//! `nulls[]`/`repl_*[]` plumbing dissolves: an option set is a `Vec<DefElem>`,
//! and each catalog-row insert/update/syscache lookup is one by-value seam to
//! the `pg_foreign_*` catalog-access layer (`backend-foreign-foreign-seams`).
//! All other externals (ACL/ownership, dependency recording, func/type
//! lookup, namespace/role resolution, hooks, the option-array validator, the
//! IMPORT FDW-callback / parse-execute machinery) cross their owner's seam.

use backend_utils_error::ereport;
use mcx::{Mcx, PgVec};
use types_acl::{ACLCHECK_OK, ACL_USAGE};
use types_catalog::catalog_dependency::{
    InvalidObjectAddress, ObjectAddress, DEPENDENCY_NORMAL,
};
use types_core::primitive::{InvalidOid, Oid};
use types_error::error::{
    ERRCODE_DUPLICATE_OBJECT, ERRCODE_FDW_NO_SCHEMAS, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE,
    ERROR, NOTICE, WARNING,
};
use types_error::pg_error::{ErrorLocation, PgError};
use types_error::PgResult;
use types_nodes::parsenodes::{
    RoleSpec, ROLESPEC_PUBLIC, DROP_CASCADE, OBJECT_FDW, OBJECT_FOREIGN_SERVER,
};
use types_foreigncmds::{
    AlterFdwStmt, AlterForeignServerStmt, AlterUserMappingStmt, CreateFdwStmt,
    CreateForeignServerStmt, CreateForeignTableStmt, CreateUserMappingStmt, DefElem, DefElemArg,
    DefElemAction, DropUserMappingStmt, FdwOwnerRow, ImportForeignSchemaStmt, ImportPlannedStmt,
    ImportRawStmt, ServerOwnerRow,
    ACL_ID_PUBLIC, CMD_UTILITY,
    FDW_HANDLEROID, ForeignDataWrapperRelationId, ForeignServerRelationId, ForeignTableRelationId,
    OIDOID, ProcedureRelationId, TEXTARRAYOID, UserMappingRelationId,
};

use backend_catalog_aclchk_seams as aclchk_seams;
use backend_catalog_dependency_seams as dependency_seams;
use backend_catalog_namespace_seams as namespace_seams;
use backend_catalog_objectaccess_seams as objectaccess_seams;
use backend_catalog_pg_depend_seams as pg_depend_seams;
use backend_catalog_pg_shdepend_seams as shdepend_seams;
use backend_foreign_foreign_seams as foreign_seams;
use backend_tcop_postgres_seams as postgres_seams;
use backend_tcop_utility_fc_seams as utility_seams;
use backend_parser_parse_func_seams as parse_func_seams;
use backend_parser_parse_type_seams as parse_type_seams;
use backend_utils_adt_acl_seams as acl_seams;
use backend_utils_cache_lsyscache_seams as lsyscache_seams;
use backend_utils_init_miscinit_seams as miscinit_seams;

/// `OidIsValid(oid)` — true when `oid != InvalidOid`.
fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

/// Source location for ereport (this file mirrors commands/foreigncmds.c).
fn errloc(lineno: i32, funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/commands/foreigncmds.c", lineno, funcname)
}

/// `ObjectAddressSet(addr, class, object)`.
fn object_address_set(class_id: Oid, object_id: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: 0,
    }
}

/* ===========================================================================
 * optionListToArray / transformGenericOptions (foreigncmds.c:65-206)
 * ======================================================================== */

/// `transformGenericOptions` — merge SET/ADD/DROP `DefElem` actions into
/// `old_options`, run `optionListToArray`'s `"="`-in-name validation, and (if
/// `fdwvalidator` is valid) call the validator. Returns the merged option
/// list. An empty list models the C `PointerGetDatum(NULL)` "no options"
/// array; the catalog seams store SQL NULL for it.
///
/// In the owned tree the C `optionListToArray` text[]-encoding /
/// `untransformRelOptions` decoding is performed by the catalog seams; the
/// only piece of `optionListToArray` that is foreigncmds.c's own logic — the
/// `"="`-in-name rejection — is performed here, during the merge that precedes
/// the encode.
pub fn transformGenericOptions<'mcx>(
    mcx: Mcx<'mcx>,
    catalog_id: Oid,
    old_options: PgVec<'mcx, DefElem<'mcx>>,
    options: &[DefElem<'mcx>],
    fdwvalidator: Oid,
) -> PgResult<PgVec<'mcx, DefElem<'mcx>>> {
    let mut result_options = old_options;

    for od in options {
        /*
         * Find the element in resultOptions.  We need this for validation in
         * all cases.  `cell_index` is `Some(i)` when found (the C `cell`),
         * `None` when not found (the C `!cell`).
         */
        let od_name = od.defname.as_str();
        let mut cell_index: Option<usize> = None;
        for (i, def) in result_options.iter().enumerate() {
            if def.defname.as_str() == od_name {
                cell_index = Some(i);
                break;
            }
        }

        match od.defaction {
            DefElemAction::Drop => {
                let Some(idx) = cell_index else {
                    return ereport(ERROR)
                        .errcode(ERRCODE_UNDEFINED_OBJECT)
                        .errmsg(format!("option \"{od_name}\" not found"))
                        .finish(errloc(159, "transformGenericOptions"))
                        .map(|()| PgVec::new_in(mcx));
                };
                result_options.remove(idx);
            }
            DefElemAction::Set => {
                let Some(idx) = cell_index else {
                    return ereport(ERROR)
                        .errcode(ERRCODE_UNDEFINED_OBJECT)
                        .errmsg(format!("option \"{od_name}\" not found"))
                        .finish(errloc(168, "transformGenericOptions"))
                        .map(|()| PgVec::new_in(mcx));
                };
                result_options[idx] = od.clone_in(mcx)?;
            }
            DefElemAction::Add | DefElemAction::Unspec => {
                if cell_index.is_some() {
                    return ereport(ERROR)
                        .errcode(ERRCODE_DUPLICATE_OBJECT)
                        .errmsg(format!("option \"{od_name}\" provided more than once"))
                        .finish(errloc(178, "transformGenericOptions"))
                        .map(|()| PgVec::new_in(mcx));
                }
                result_options.try_reserve(1).map_err(|_| mcx.oom(1))?;
                result_options.push(od.clone_in(mcx)?);
            }
        }
    }

    /*
     * `optionListToArray`'s own validation: insist that no option name contains
     * "=", else "a=b=c" would be ambiguous (ERRCODE_INVALID_PARAMETER_VALUE).
     * The actual text[] encoding is performed by the catalog seam on store.
     */
    for def in result_options.iter() {
        let name = def.defname.as_str();
        if name.contains('=') {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "invalid option name \"{name}\": must not contain \"=\""
                ))
                .finish(errloc(86, "optionListToArray"))
                .map(|()| PgVec::new_in(mcx));
        }
    }

    if OidIsValid(fdwvalidator) {
        /*
         * Pass a null options list as an empty array, so that validators don't
         * have to be declared non-strict to handle the case.  An empty
         * `result_options` is exactly that case, and the validator seam builds
         * the empty array on the runtime side.
         */
        foreign_seams::validate_options::call(fdwvalidator, &result_options, catalog_id)?;
    }

    Ok(result_options)
}

/// Convert a merged option list into the catalog seam's NULL-vs-present form:
/// `None` (store SQL NULL) for the empty list, else `Some(&list)`. Mirrors the
/// C `if (PointerIsValid(DatumGetPointer(opts))) values[..]=opts; else
/// nulls[..]`.
fn options_for_store<'a, 'mcx>(options: &'a [DefElem<'mcx>]) -> Option<&'a [DefElem<'mcx>]> {
    if options.is_empty() {
        None
    } else {
        Some(options)
    }
}

/* ===========================================================================
 * AlterForeignDataWrapperOwner_internal (foreigncmds.c:215-278)
 * ======================================================================== */

/// `AlterForeignDataWrapperOwner_internal` — change a FDW's owner.  Allowed
/// only for superusers; the new owner must also be a superuser.  `row` is the
/// syscache-fetched `(fdwid, fdwname, fdwowner)`.
fn AlterForeignDataWrapperOwner_internal(
    row: &FdwOwnerRow<'_>,
    new_owner_id: Oid,
) -> PgResult<()> {
    /* Must be a superuser to change a FDW owner */
    if !miscinit_seams::superuser::call() {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "permission denied to change owner of foreign-data wrapper \"{}\"",
                row.fdwname
            ))
            .errhint("Must be superuser to change owner of a foreign-data wrapper.")
            .finish(errloc(230, "AlterForeignDataWrapperOwner_internal"));
    }

    /* New owner must also be a superuser */
    if !miscinit_seams::superuser_arg::call(new_owner_id) {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "permission denied to change owner of foreign-data wrapper \"{}\"",
                row.fdwname
            ))
            .errhint("The owner of a foreign-data wrapper must be a superuser.")
            .finish(errloc(238, "AlterForeignDataWrapperOwner_internal"));
    }

    if row.fdwowner != new_owner_id {
        /*
         * Set `fdwowner = new_owner`, plus `aclnewowner(fdwacl, old, new)` when
         * the ACL is non-NULL, then `CatalogTupleUpdate`. The genuine
         * catalog/ACL plumbing crosses one seam.
         */
        foreign_seams::fdw_set_owner::call(row.fdwid, row.fdwowner, new_owner_id)?;

        /* Update owner dependency reference */
        shdepend_seams::changeDependencyOnOwner::call(
            ForeignDataWrapperRelationId,
            row.fdwid,
            new_owner_id,
        )?;
    }

    objectaccess_seams::invoke_object_post_alter_hook::call(
        ForeignDataWrapperRelationId,
        row.fdwid,
        0,
    )?;

    Ok(())
}

/// `AlterForeignDataWrapperOwner` — change FDW owner by name.
pub fn AlterForeignDataWrapperOwner<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    new_owner_id: Oid,
) -> PgResult<ObjectAddress> {
    let Some(row) = foreign_seams::fdw_owner_row_by_name::call(mcx, name)? else {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("foreign-data wrapper \"{name}\" does not exist"))
            .finish(errloc(300, "AlterForeignDataWrapperOwner"))
            .map(|()| InvalidObjectAddress);
    };

    let fdw_id = row.fdwid;

    AlterForeignDataWrapperOwner_internal(&row, new_owner_id)?;

    Ok(object_address_set(ForeignDataWrapperRelationId, fdw_id))
}

/// `AlterForeignDataWrapperOwner_oid` — change FDW owner by OID.
pub fn AlterForeignDataWrapperOwner_oid<'mcx>(
    mcx: Mcx<'mcx>,
    fwd_id: Oid,
    new_owner_id: Oid,
) -> PgResult<()> {
    let Some(row) = foreign_seams::fdw_owner_row_by_oid::call(mcx, fwd_id)? else {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "foreign-data wrapper with OID {fwd_id} does not exist"
            ))
            .finish(errloc(335, "AlterForeignDataWrapperOwner_oid"));
    };

    AlterForeignDataWrapperOwner_internal(&row, new_owner_id)
}

/* ===========================================================================
 * AlterForeignServerOwner_internal (foreigncmds.c:348-420)
 * ======================================================================== */

/// `AlterForeignServerOwner_internal` — change a foreign server's owner.
/// `row` is the syscache-fetched `(serverid, srvname, srvowner, srvfdw)`.
fn AlterForeignServerOwner_internal<'mcx>(
    mcx: Mcx<'mcx>,
    row: &ServerOwnerRow<'_>,
    new_owner_id: Oid,
) -> PgResult<()> {
    if row.srvowner != new_owner_id {
        /* Superusers can always do it */
        if !miscinit_seams::superuser::call() {
            let srv_id = row.serverid;

            /* Must be owner */
            if !aclchk_seams::object_ownercheck::call(
                ForeignServerRelationId,
                srv_id,
                miscinit_seams::get_user_id::call(),
            )? {
                return aclchk_seams::aclcheck_error::call(
                    types_acl::ACLCHECK_NOT_OWNER,
                    OBJECT_FOREIGN_SERVER,
                    Some(row.srvname.as_str().to_string()),
                );
            }

            /* Must be able to become new owner */
            acl_seams::check_can_set_role::call(miscinit_seams::get_user_id::call(), new_owner_id)?;

            /* New owner must have USAGE privilege on foreign-data wrapper */
            let aclresult = aclchk_seams::object_aclcheck::call(
                ForeignDataWrapperRelationId,
                row.srvfdw,
                new_owner_id,
                ACL_USAGE,
            )?;
            if aclresult != ACLCHECK_OK {
                let fdw = foreign_seams::get_foreign_data_wrapper::call(mcx, row.srvfdw)?;
                return aclchk_seams::aclcheck_error::call(
                    aclresult,
                    OBJECT_FDW,
                    Some(fdw.fdwname.as_str().to_string()),
                );
            }
        }

        /*
         * Set `srvowner = new_owner`, plus `aclnewowner(srvacl, old, new)` when
         * non-NULL, `CatalogTupleUpdate`, then update the owner dependency.
         */
        foreign_seams::server_set_owner::call(row.serverid, row.srvowner, new_owner_id)?;

        /* Update owner dependency reference */
        shdepend_seams::changeDependencyOnOwner::call(
            ForeignServerRelationId,
            row.serverid,
            new_owner_id,
        )?;
    }

    objectaccess_seams::invoke_object_post_alter_hook::call(
        ForeignServerRelationId,
        row.serverid,
        0,
    )?;

    Ok(())
}

/// `AlterForeignServerOwner` — change foreign server owner by name.
pub fn AlterForeignServerOwner<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    new_owner_id: Oid,
) -> PgResult<ObjectAddress> {
    let Some(row) = foreign_seams::server_owner_row_by_name::call(mcx, name)? else {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("server \"{name}\" does not exist"))
            .finish(errloc(441, "AlterForeignServerOwner"))
            .map(|()| InvalidObjectAddress);
    };

    let serv_oid = row.serverid;

    AlterForeignServerOwner_internal(mcx, &row, new_owner_id)?;

    Ok(object_address_set(ForeignServerRelationId, serv_oid))
}

/// `AlterForeignServerOwner_oid` — change foreign server owner by OID.
pub fn AlterForeignServerOwner_oid<'mcx>(
    mcx: Mcx<'mcx>,
    srv_id: Oid,
    new_owner_id: Oid,
) -> PgResult<()> {
    let Some(row) = foreign_seams::server_owner_row_by_oid::call(mcx, srv_id)? else {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("foreign server with OID {srv_id} does not exist"))
            .finish(errloc(472, "AlterForeignServerOwner_oid"));
    };

    AlterForeignServerOwner_internal(mcx, &row, new_owner_id)
}

/* ===========================================================================
 * lookup_fdw_handler_func / lookup_fdw_validator_func / parse_func_options
 * (foreigncmds.c:485-563)
 * ======================================================================== */

/// The `(List *) def->arg` qualified-name components of a HANDLER/VALIDATOR
/// option (a `T_List` of `String` nodes).  A non-list arg is a parser
/// invariant violation; the C casts unconditionally, so a loud internal error
/// is the faithful surface.
fn func_name_arg<'a, 'mcx>(def: &'a DefElem<'mcx>) -> PgResult<Option<&'a [mcx::PgString<'mcx>]>> {
    match &def.arg {
        None => Ok(None),
        Some(arg) => match &**arg {
            DefElemArg::NameList(names) => Ok(Some(names.as_slice())),
            _ => ereport(ERROR)
                .errmsg_internal("foreigncmds: HANDLER/VALIDATOR option arg is not a name list")
                .finish(errloc(0, "func_name_arg"))
                .map(|()| None),
        },
    }
}

/// `lookup_fdw_handler_func` — resolve a HANDLER function name to an OID,
/// checking it returns `fdw_handler`.
fn lookup_fdw_handler_func<'mcx>(mcx: Mcx<'mcx>, handler: &DefElem<'mcx>) -> PgResult<Oid> {
    let Some(arg) = func_name_arg(handler)? else {
        return Ok(InvalidOid);
    };

    /* handlers have no arguments */
    let handler_oid = parse_func_seams::lookup_func_name::call(arg, 0, &[], false)?;

    /* check that handler has correct return type */
    if lsyscache_seams::get_func_rettype::call(handler_oid)? != FDW_HANDLEROID {
        let funcname_str = parse_type_seams::name_list_to_string::call(mcx, arg)?;
        return ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "function {funcname_str} must return type {}",
                "fdw_handler"
            ))
            .finish(errloc(498, "lookup_fdw_handler_func"))
            .map(|()| InvalidOid);
    }

    Ok(handler_oid)
}

/// `lookup_fdw_validator_func` — resolve a VALIDATOR function name to an OID;
/// validators take `(text[], oid)`.
fn lookup_fdw_validator_func<'mcx>(validator: &DefElem<'mcx>) -> PgResult<Oid> {
    let Some(arg) = func_name_arg(validator)? else {
        return Ok(InvalidOid);
    };

    /* validators take text[], oid */
    let funcargtypes = [TEXTARRAYOID, OIDOID];

    parse_func_seams::lookup_func_name::call(arg, 2, &funcargtypes, false)
    /* validator's return value is ignored, so we don't check the type */
}

/// `parse_func_options` — process the HANDLER/VALIDATOR options of CREATE/ALTER
/// FDW.  Returns `(handler_given, fdwhandler, validator_given, fdwvalidator)`.
fn parse_func_options<'mcx>(
    mcx: Mcx<'mcx>,
    func_options: &[DefElem<'mcx>],
) -> PgResult<(bool, Oid, bool, Oid)> {
    let mut handler_given = false;
    let mut validator_given = false;
    /* return InvalidOid if not given */
    let mut fdwhandler = InvalidOid;
    let mut fdwvalidator = InvalidOid;

    for def in func_options {
        let dn = def.defname.as_str();
        if dn == "handler" {
            if handler_given {
                return aclchk_seams::error_conflicting_def_elem::call(dn.to_string())
                    .map(|()| (false, InvalidOid, false, InvalidOid));
            }
            handler_given = true;
            fdwhandler = lookup_fdw_handler_func(mcx, def)?;
        } else if dn == "validator" {
            if validator_given {
                return aclchk_seams::error_conflicting_def_elem::call(dn.to_string())
                    .map(|()| (false, InvalidOid, false, InvalidOid));
            }
            validator_given = true;
            fdwvalidator = lookup_fdw_validator_func(def)?;
        } else {
            return ereport(ERROR)
                .errmsg_internal(format!("option \"{dn}\" not recognized"))
                .finish(errloc(560, "parse_func_options"))
                .map(|()| (false, InvalidOid, false, InvalidOid));
        }
    }

    Ok((handler_given, fdwhandler, validator_given, fdwvalidator))
}

/* ===========================================================================
 * CreateForeignDataWrapper (foreigncmds.c:568-678)
 * ======================================================================== */

/// `CreateForeignDataWrapper` — CREATE FOREIGN DATA WRAPPER.
pub fn CreateForeignDataWrapper<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateFdwStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let fdwname = stmt.fdwname.as_str();

    /* Must be superuser */
    if !miscinit_seams::superuser::call() {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "permission denied to create foreign-data wrapper \"{fdwname}\""
            ))
            .errhint("Must be superuser to create a foreign-data wrapper.")
            .finish(errloc(589, "CreateForeignDataWrapper"))
            .map(|()| InvalidObjectAddress);
    }

    /* For now the owner cannot be specified on create. Use effective user ID. */
    let owner_id = miscinit_seams::get_user_id::call();

    /*
     * Check that there is no other foreign-data wrapper by this name.
     */
    if foreign_seams::get_foreign_data_wrapper_by_name::call(mcx, fdwname, true)?.is_some() {
        return ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!("foreign-data wrapper \"{fdwname}\" already exists"))
            .finish(errloc(602, "CreateForeignDataWrapper"))
            .map(|()| InvalidObjectAddress);
    }

    /* Lookup handler and validator functions, if given */
    let (_handler_given, fdwhandler, _validator_given, fdwvalidator) =
        parse_func_options(mcx, &stmt.func_options)?;

    let fdwoptions = transformGenericOptions(
        mcx,
        ForeignDataWrapperRelationId,
        PgVec::new_in(mcx),
        &stmt.options,
        fdwvalidator,
    )?;

    /*
     * Insert tuple into pg_foreign_data_wrapper (the GetNewOidWithIndex +
     * heap_form_tuple + CatalogTupleInsert).
     */
    let fdw_id = foreign_seams::insert_fdw::call(
        fdwname,
        owner_id,
        fdwhandler,
        fdwvalidator,
        options_for_store(&fdwoptions),
    )?;

    /* record dependencies */
    let myself = object_address_set(ForeignDataWrapperRelationId, fdw_id);

    if OidIsValid(fdwhandler) {
        let referenced = object_address_set(ProcedureRelationId, fdwhandler);
        pg_depend_seams::recordDependencyOn::call(mcx, &myself, &referenced, DEPENDENCY_NORMAL)?;
    }

    if OidIsValid(fdwvalidator) {
        let referenced = object_address_set(ProcedureRelationId, fdwvalidator);
        pg_depend_seams::recordDependencyOn::call(mcx, &myself, &referenced, DEPENDENCY_NORMAL)?;
    }

    shdepend_seams::recordDependencyOnOwner::call(ForeignDataWrapperRelationId, fdw_id, owner_id)?;

    /* dependency on extension */
    pg_depend_seams::recordDependencyOnCurrentExtension::call(mcx, &myself, false)?;

    /* Post creation hook for new foreign data wrapper */
    objectaccess_seams::invoke_object_post_create_hook::call(
        ForeignDataWrapperRelationId,
        fdw_id,
        0,
    )?;

    Ok(myself)
}

/* ===========================================================================
 * AlterForeignDataWrapper (foreigncmds.c:684-842)
 * ======================================================================== */

/// `AlterForeignDataWrapper` — ALTER FOREIGN DATA WRAPPER.
pub fn AlterForeignDataWrapper<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterFdwStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let fdwname = stmt.fdwname.as_str();

    /* Must be superuser */
    if !miscinit_seams::superuser::call() {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "permission denied to alter foreign-data wrapper \"{fdwname}\""
            ))
            .errhint("Must be superuser to alter a foreign-data wrapper.")
            .finish(errloc(706, "AlterForeignDataWrapper"))
            .map(|()| InvalidObjectAddress);
    }

    let Some(fdw_row) = foreign_seams::fdw_lookup_by_name::call(fdwname)? else {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("foreign-data wrapper \"{fdwname}\" does not exist"))
            .finish(errloc(716, "AlterForeignDataWrapper"))
            .map(|()| InvalidObjectAddress);
    };
    let fdw_id = fdw_row.fdwid;

    let (handler_given, fdwhandler, validator_given, fdwvalidator0) =
        parse_func_options(mcx, &stmt.func_options)?;

    let mut repl_handler: Option<Oid> = None;
    let mut repl_validator: Option<Oid> = None;
    let mut repl_options: Option<Option<PgVec<'mcx, DefElem<'mcx>>>> = None;

    if handler_given {
        repl_handler = Some(fdwhandler);

        /*
         * It could be that the behavior of accessing foreign table changes with
         * the new handler.  Warn about this.
         */
        ereport(WARNING)
            .errmsg("changing the foreign-data wrapper handler can change behavior of existing foreign tables")
            .finish(errloc(740, "AlterForeignDataWrapper"))?;
    }

    let fdwvalidator = if validator_given {
        repl_validator = Some(fdwvalidator0);

        /*
         * It could be that existing options for the FDW or dependent SERVER,
         * USER MAPPING or FOREIGN TABLE objects are no longer valid according to
         * the new validator.  Warn about this.
         */
        if OidIsValid(fdwvalidator0) {
            ereport(WARNING)
                .errmsg("changing the foreign-data wrapper validator can cause the options for dependent objects to become invalid")
                .finish(errloc(755, "AlterForeignDataWrapper"))?;
        }
        fdwvalidator0
    } else {
        /*
         * Validator is not changed, but we need it for validating options.
         */
        fdw_row.fdwvalidator
    };

    /*
     * If options specified, validate and update.
     */
    if !stmt.options.is_empty() {
        /* Extract the current options */
        let old_options = foreign_seams::fdw_options::call(mcx, fdw_id)?;

        /* Transform the options */
        let merged = transformGenericOptions(
            mcx,
            ForeignDataWrapperRelationId,
            old_options,
            &stmt.options,
            fdwvalidator,
        )?;

        repl_options = Some(if merged.is_empty() { None } else { Some(merged) });
    }

    /* Everything looks good - update the tuple */
    let repl_options_ref = repl_options
        .as_ref()
        .map(|inner| inner.as_ref().map(|v| v.as_slice()));
    foreign_seams::update_fdw::call(fdw_id, repl_handler, repl_validator, repl_options_ref)?;

    let myself = object_address_set(ForeignDataWrapperRelationId, fdw_id);

    /* Update function dependencies if we changed them */
    if handler_given || validator_given {
        /*
         * Flush all existing dependency records of this FDW on functions; we
         * assume there can be none other than the ones we are fixing.
         */
        pg_depend_seams::deleteDependencyRecordsForClass::call(
            ForeignDataWrapperRelationId,
            fdw_id,
            ProcedureRelationId,
            DEPENDENCY_NORMAL.as_char(),
        )?;

        /* And build new ones. */

        if OidIsValid(fdwhandler) {
            let referenced = object_address_set(ProcedureRelationId, fdwhandler);
            pg_depend_seams::recordDependencyOn::call(mcx, &myself, &referenced, DEPENDENCY_NORMAL)?;
        }

        if OidIsValid(fdwvalidator) {
            let referenced = object_address_set(ProcedureRelationId, fdwvalidator);
            pg_depend_seams::recordDependencyOn::call(mcx, &myself, &referenced, DEPENDENCY_NORMAL)?;
        }
    }

    objectaccess_seams::invoke_object_post_alter_hook::call(
        ForeignDataWrapperRelationId,
        fdw_id,
        0,
    )?;

    Ok(myself)
}

/* ===========================================================================
 * CreateForeignServer (foreigncmds.c:848-978)
 * ======================================================================== */

/// `CreateForeignServer` — CREATE SERVER.
pub fn CreateForeignServer<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateForeignServerStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let servername = stmt.servername.as_str();

    /* For now the owner cannot be specified on create. Use effective user ID. */
    let owner_id = miscinit_seams::get_user_id::call();

    /*
     * Check that there is no other foreign server by this name.  If there is
     * one, do nothing if IF NOT EXISTS was specified.
     */
    let srv_id = foreign_seams::get_foreign_server_oid::call(servername, true)?;
    if OidIsValid(srv_id) {
        if stmt.if_not_exists {
            /*
             * If we are in an extension script, insist that the pre-existing
             * object be a member of the extension, to avoid security risks.
             */
            let myself = object_address_set(ForeignServerRelationId, srv_id);
            pg_depend_seams::checkMembershipInCurrentExtension::call(mcx, &myself)?;

            /* OK to skip */
            ereport(NOTICE)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!("server \"{servername}\" already exists, skipping"))
                .finish(errloc(885, "CreateForeignServer"))?;
            return Ok(InvalidObjectAddress);
        } else {
            return ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!("server \"{servername}\" already exists"))
                .finish(errloc(893, "CreateForeignServer"))
                .map(|()| InvalidObjectAddress);
        }
    }

    /*
     * Check that the FDW exists and that we have USAGE on it. Also get the
     * actual FDW for option validation etc.
     */
    let fdwname = stmt.fdwname.as_str();
    let fdw = foreign_seams::get_foreign_data_wrapper_by_name::call(mcx, fdwname, false)?
        // missing_ok=false ⇒ the seam errors rather than returning None;
        // defensively reject None (the C returns a non-NULL pointer here).
        .ok_or_else(|| {
            ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("foreign-data wrapper \"{fdwname}\" does not exist"))
                .into_error()
                .with_error_location(errloc(903, "CreateForeignServer"))
        })?;

    let aclresult = aclchk_seams::object_aclcheck::call(
        ForeignDataWrapperRelationId,
        fdw.fdwid,
        owner_id,
        ACL_USAGE,
    )?;
    if aclresult != ACLCHECK_OK {
        return aclchk_seams::aclcheck_error::call(
            aclresult,
            OBJECT_FDW,
            Some(fdw.fdwname.as_str().to_string()),
        )
        .map(|()| InvalidObjectAddress);
    }

    /* Add server options */
    let srvoptions = transformGenericOptions(
        mcx,
        ForeignServerRelationId,
        PgVec::new_in(mcx),
        &stmt.options,
        fdw.fdwvalidator,
    )?;

    /*
     * Insert tuple into pg_foreign_server.
     */
    let srv_id = foreign_seams::insert_server::call(
        servername,
        owner_id,
        fdw.fdwid,
        stmt.servertype.as_ref().map(|s| s.as_str()),
        stmt.version.as_ref().map(|s| s.as_str()),
        options_for_store(&srvoptions),
    )?;

    /* record dependencies */
    let myself = object_address_set(ForeignServerRelationId, srv_id);

    let referenced = object_address_set(ForeignDataWrapperRelationId, fdw.fdwid);
    pg_depend_seams::recordDependencyOn::call(mcx, &myself, &referenced, DEPENDENCY_NORMAL)?;

    shdepend_seams::recordDependencyOnOwner::call(ForeignServerRelationId, srv_id, owner_id)?;

    /* dependency on extension */
    pg_depend_seams::recordDependencyOnCurrentExtension::call(mcx, &myself, false)?;

    /* Post creation hook for new foreign server */
    objectaccess_seams::invoke_object_post_create_hook::call(ForeignServerRelationId, srv_id, 0)?;

    Ok(myself)
}

/* ===========================================================================
 * AlterForeignServer (foreigncmds.c:984-1077)
 * ======================================================================== */

/// `AlterForeignServer` — ALTER SERVER.
pub fn AlterForeignServer<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterForeignServerStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let servername = stmt.servername.as_str();

    let Some(srv_row) = foreign_seams::server_lookup_by_name::call(servername)? else {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("server \"{servername}\" does not exist"))
            .finish(errloc(1002, "AlterForeignServer"))
            .map(|()| InvalidObjectAddress);
    };
    let srv_id = srv_row.serverid;

    /*
     * Only owner or a superuser can ALTER a SERVER.
     */
    if !aclchk_seams::object_ownercheck::call(
        ForeignServerRelationId,
        srv_id,
        miscinit_seams::get_user_id::call(),
    )? {
        return aclchk_seams::aclcheck_error::call(
            types_acl::ACLCHECK_NOT_OWNER,
            OBJECT_FOREIGN_SERVER,
            Some(servername.to_string()),
        )
        .map(|()| InvalidObjectAddress);
    }

    let mut repl_version: Option<Option<&str>> = None;
    let mut repl_options: Option<Option<PgVec<'mcx, DefElem<'mcx>>>> = None;

    if stmt.has_version {
        /*
         * Change the server VERSION string.
         */
        repl_version = Some(stmt.version.as_ref().map(|s| s.as_str()));
    }

    if !stmt.options.is_empty() {
        let fdw = foreign_seams::get_foreign_data_wrapper::call(mcx, srv_row.srvfdw)?;

        /* Extract the current srvoptions */
        let old_options = foreign_seams::server_options::call(mcx, srv_id)?;

        /* Prepare the options array */
        let merged = transformGenericOptions(
            mcx,
            ForeignServerRelationId,
            old_options,
            &stmt.options,
            fdw.fdwvalidator,
        )?;

        repl_options = Some(if merged.is_empty() { None } else { Some(merged) });
    }

    /* Everything looks good - update the tuple */
    let repl_options_ref = repl_options
        .as_ref()
        .map(|inner| inner.as_ref().map(|v| v.as_slice()));
    foreign_seams::update_server::call(srv_id, repl_version, repl_options_ref)?;

    objectaccess_seams::invoke_object_post_alter_hook::call(ForeignServerRelationId, srv_id, 0)?;

    Ok(object_address_set(ForeignServerRelationId, srv_id))
}

/* ===========================================================================
 * user_mapping_ddl_aclcheck (foreigncmds.c:1085-1104)
 * ======================================================================== */

/// `user_mapping_ddl_aclcheck` — permission check for user-mapping DDL.  Server
/// owners may operate on any mapping; users may operate on their own mapping.
fn user_mapping_ddl_aclcheck(umuserid: Oid, serverid: Oid, servername: &str) -> PgResult<()> {
    let curuserid = miscinit_seams::get_user_id::call();

    if !aclchk_seams::object_ownercheck::call(ForeignServerRelationId, serverid, curuserid)? {
        if umuserid == curuserid {
            let aclresult = aclchk_seams::object_aclcheck::call(
                ForeignServerRelationId,
                serverid,
                curuserid,
                ACL_USAGE,
            )?;
            if aclresult != ACLCHECK_OK {
                return aclchk_seams::aclcheck_error::call(
                    aclresult,
                    OBJECT_FOREIGN_SERVER,
                    Some(servername.to_string()),
                );
            }
        } else {
            return aclchk_seams::aclcheck_error::call(
                types_acl::ACLCHECK_NOT_OWNER,
                OBJECT_FOREIGN_SERVER,
                Some(servername.to_string()),
            );
        }
    }

    Ok(())
}

/* ===========================================================================
 * CreateUserMapping (foreigncmds.c:1110-1230)
 * ======================================================================== */

/// `CreateUserMapping` — CREATE USER MAPPING.
pub fn CreateUserMapping<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateUserMappingStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let use_id = rolespec_oid(&stmt.user, false)?;

    let servername = stmt.servername.as_str();

    /* Check that the server exists. */
    let srv = foreign_seams::get_foreign_server_by_name::call(mcx, servername, false)?.ok_or_else(
        || {
            ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("server \"{servername}\" does not exist"))
                .into_error()
                .with_error_location(errloc(1134, "CreateUserMapping"))
        },
    )?;

    user_mapping_ddl_aclcheck(use_id, srv.serverid, servername)?;

    /*
     * Check that the user mapping is unique within server.
     */
    let um_id = foreign_seams::usermapping_oid::call(use_id, srv.serverid)?;

    if OidIsValid(um_id) {
        if stmt.if_not_exists {
            /*
             * Since user mappings aren't members of extensions (see comments
             * below), no need for checkMembershipInCurrentExtension here.
             */
            let username = foreign_seams::mapping_user_name::call(mcx, use_id)?;
            ereport(NOTICE)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!(
                    "user mapping for \"{username}\" already exists for server \"{servername}\", skipping"
                ))
                .finish(errloc(1153, "CreateUserMapping"))?;

            return Ok(InvalidObjectAddress);
        } else {
            let username = foreign_seams::mapping_user_name::call(mcx, use_id)?;
            return ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!(
                    "user mapping for \"{username}\" already exists for server \"{servername}\""
                ))
                .finish(errloc(1163, "CreateUserMapping"))
                .map(|()| InvalidObjectAddress);
        }
    }

    let fdw = foreign_seams::get_foreign_data_wrapper::call(mcx, srv.fdwid)?;

    /* Add user options */
    let useoptions = transformGenericOptions(
        mcx,
        UserMappingRelationId,
        PgVec::new_in(mcx),
        &stmt.options,
        fdw.fdwvalidator,
    )?;

    /*
     * Insert tuple into pg_user_mapping.
     */
    let um_id = foreign_seams::insert_usermapping::call(
        use_id,
        srv.serverid,
        options_for_store(&useoptions),
    )?;

    /* Add dependency on the server */
    let myself = object_address_set(UserMappingRelationId, um_id);

    let referenced = object_address_set(ForeignServerRelationId, srv.serverid);
    pg_depend_seams::recordDependencyOn::call(mcx, &myself, &referenced, DEPENDENCY_NORMAL)?;

    if OidIsValid(use_id) {
        /* Record the mapped user dependency */
        shdepend_seams::recordDependencyOnOwner::call(UserMappingRelationId, um_id, use_id)?;
    }

    /*
     * Perhaps someday there should be a recordDependencyOnCurrentExtension call
     * here; but since roles aren't members of extensions, it seems like user
     * mappings shouldn't be either.
     */

    /* Post creation hook for new user mapping */
    objectaccess_seams::invoke_object_post_create_hook::call(UserMappingRelationId, um_id, 0)?;

    Ok(myself)
}

/* ===========================================================================
 * AlterUserMapping (foreigncmds.c:1236-1328)
 * ======================================================================== */

/// `AlterUserMapping` — ALTER USER MAPPING.
pub fn AlterUserMapping<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterUserMappingStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let use_id = rolespec_oid(&stmt.user, false)?;

    let servername = stmt.servername.as_str();

    let srv = foreign_seams::get_foreign_server_by_name::call(mcx, servername, false)?.ok_or_else(
        || {
            ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("server \"{servername}\" does not exist"))
                .into_error()
                .with_error_location(errloc(1257, "AlterUserMapping"))
        },
    )?;

    let um_id = foreign_seams::usermapping_oid::call(use_id, srv.serverid)?;
    if !OidIsValid(um_id) {
        let username = foreign_seams::mapping_user_name::call(mcx, use_id)?;
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "user mapping for \"{username}\" does not exist for server \"{servername}\""
            ))
            .finish(errloc(1263, "AlterUserMapping"))
            .map(|()| InvalidObjectAddress);
    }

    user_mapping_ddl_aclcheck(use_id, srv.serverid, servername)?;

    let mut repl_options: Option<Option<PgVec<'mcx, DefElem<'mcx>>>> = None;

    if !stmt.options.is_empty() {
        /*
         * Process the options.
         */
        let fdw = foreign_seams::get_foreign_data_wrapper::call(mcx, srv.fdwid)?;

        let old_options = foreign_seams::usermapping_options::call(mcx, um_id)?;

        /* Prepare the options array */
        let merged = transformGenericOptions(
            mcx,
            UserMappingRelationId,
            old_options,
            &stmt.options,
            fdw.fdwvalidator,
        )?;

        repl_options = Some(if merged.is_empty() { None } else { Some(merged) });
    }

    /* Everything looks good - update the tuple */
    let repl_options_ref = repl_options
        .as_ref()
        .map(|inner| inner.as_ref().map(|v| v.as_slice()));
    foreign_seams::update_usermapping::call(um_id, repl_options_ref)?;

    objectaccess_seams::invoke_object_post_alter_hook::call(UserMappingRelationId, um_id, 0)?;

    Ok(object_address_set(UserMappingRelationId, um_id))
}

/* ===========================================================================
 * RemoveUserMapping (foreigncmds.c:1334-1407)
 * ======================================================================== */

/// `RemoveUserMapping` — DROP USER MAPPING.  Returns the dropped mapping's OID,
/// or `InvalidOid` when skipped via IF EXISTS.
pub fn RemoveUserMapping<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &DropUserMappingStmt<'mcx>,
) -> PgResult<Oid> {
    let role = &stmt.user;

    let use_id = if rolespec_is_public(role) {
        ACL_ID_PUBLIC
    } else {
        let use_id = rolespec_oid(role, stmt.missing_ok)?;
        if !OidIsValid(use_id) {
            /*
             * IF EXISTS specified, role not found and not public. Notice this
             * and leave.
             */
            let rolename = rolespec_name(role);
            ereport(NOTICE)
                .errmsg_internal(format!("role \"{rolename}\" does not exist, skipping"))
                .finish(errloc(1354, "RemoveUserMapping"))?;
            return Ok(InvalidOid);
        }
        use_id
    };

    let servername = stmt.servername.as_str();

    let Some(srv) = foreign_seams::get_foreign_server_by_name::call(mcx, servername, true)? else {
        if !stmt.missing_ok {
            return ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("server \"{servername}\" does not exist"))
                .finish(errloc(1365, "RemoveUserMapping"))
                .map(|()| InvalidOid);
        }
        /* IF EXISTS, just note it */
        ereport(NOTICE)
            .errmsg(format!("server \"{servername}\" does not exist, skipping"))
            .finish(errloc(1370, "RemoveUserMapping"))?;
        return Ok(InvalidOid);
    };

    let um_id = foreign_seams::usermapping_oid::call(use_id, srv.serverid)?;

    if !OidIsValid(um_id) {
        if !stmt.missing_ok {
            let username = foreign_seams::mapping_user_name::call(mcx, use_id)?;
            return ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "user mapping for \"{username}\" does not exist for server \"{servername}\""
                ))
                .finish(errloc(1383, "RemoveUserMapping"))
                .map(|()| InvalidOid);
        }

        /* IF EXISTS specified, just note it */
        let username = foreign_seams::mapping_user_name::call(mcx, use_id)?;
        ereport(NOTICE)
            .errmsg(format!(
                "user mapping for \"{username}\" does not exist for server \"{servername}\", skipping"
            ))
            .finish(errloc(1389, "RemoveUserMapping"))?;
        return Ok(InvalidOid);
    }

    user_mapping_ddl_aclcheck(use_id, srv.serverid, srv.servername.as_str())?;

    /*
     * Do the deletion
     */
    let object = object_address_set(UserMappingRelationId, um_id);

    dependency_seams::perform_deletion::call(
        object.classId,
        object.objectId,
        object.objectSubId,
        DROP_CASCADE,
        0,
    )?;

    Ok(um_id)
}

/* ===========================================================================
 * CreateForeignTable (foreigncmds.c:1414-1489)
 * ======================================================================== */

/// `CreateForeignTable` — CREATE FOREIGN TABLE; called after `DefineRelation`.
pub fn CreateForeignTable<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateForeignTableStmt<'mcx>,
    relid: Oid,
) -> PgResult<()> {
    /*
     * Advance command counter to ensure the pg_attribute tuple is visible; the
     * tuple might be updated to add constraints in previous step.
     */
    backend_access_transam_xact_seams::command_counter_increment::call()?;

    /*
     * For now the owner cannot be specified on create. Use effective user ID.
     */
    let owner_id = miscinit_seams::get_user_id::call();

    /*
     * Check that the foreign server exists and that we have USAGE on it. Also
     * get the actual FDW for option validation etc.
     */
    let servername = stmt.servername.as_str();
    let server = foreign_seams::get_foreign_server_by_name::call(mcx, servername, false)?
        .ok_or_else(|| {
            ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("server \"{servername}\" does not exist"))
                .into_error()
                .with_error_location(errloc(1446, "CreateForeignTable"))
        })?;
    let aclresult = aclchk_seams::object_aclcheck::call(
        ForeignServerRelationId,
        server.serverid,
        owner_id,
        ACL_USAGE,
    )?;
    if aclresult != ACLCHECK_OK {
        return aclchk_seams::aclcheck_error::call(
            aclresult,
            OBJECT_FOREIGN_SERVER,
            Some(server.servername.as_str().to_string()),
        );
    }

    let fdw = foreign_seams::get_foreign_data_wrapper::call(mcx, server.fdwid)?;

    /* Add table generic options */
    let ftoptions = transformGenericOptions(
        mcx,
        ForeignTableRelationId,
        PgVec::new_in(mcx),
        &stmt.options,
        fdw.fdwvalidator,
    )?;

    /*
     * Insert tuple into pg_foreign_table + add the pg_class -> pg_foreign_server
     * dependency (the C records `recordDependencyOn(pg_class:relid ->
     * pg_foreign_server:serverid)` inside CreateForeignTable, after the insert;
     * the insert seam performs both, matching `RelationRelationId` as
     * `myself.classId`).
     */
    foreign_seams::insert_foreign_table::call(
        relid,
        server.serverid,
        options_for_store(&ftoptions),
    )?;

    Ok(())
}

/* ===========================================================================
 * ImportForeignSchema (foreigncmds.c:1494-1605)
 * ======================================================================== */

/// `import_error_callback_arg` (foreigncmds.c:42-47) — the state the IMPORT
/// error-context callback reads: the failing command text and (once known) the
/// foreign table being imported.
struct ImportErrorCallbackArg<'a> {
    /// `tablename` — the current table's name, or `None` (not known yet).
    tablename: Option<String>,
    /// `cmd` — the SQL string being parsed/executed.
    cmd: &'a str,
}

/// `import_error_callback(arg)` (foreigncmds.c:1610, static) — error-context
/// callback supplying the failing SQL statement's text. In the owned tree C's
/// `error_context_stack` push/pop is the attach-on-propagation idiom
/// (docs/query-lifecycle-raii.md): this transforms the in-flight `PgError` the
/// same way the C callback edits the `errordata` at `errfinish` time.
fn import_error_callback(mut err: PgError, callback_arg: &ImportErrorCallbackArg<'_>) -> PgError {
    /* If it's a syntax error, convert to internal syntax error report */
    let syntaxerrposition = err.cursor_position().unwrap_or(0);
    if syntaxerrposition > 0 {
        err = err
            .with_cursor_position(0)
            .with_internal_position(syntaxerrposition)
            .with_internal_query(callback_arg.cmd.to_string());
    }

    if let Some(tablename) = &callback_arg.tablename {
        err.add_context_line(format!("importing foreign table \"{tablename}\""));
    }
    err
}

/// `ImportForeignSchema` — IMPORT FOREIGN SCHEMA.  The USAGE/CREATE permission
/// checks and the no-handler guard, the FDW-routine IMPORT-support guard, and
/// the per-command parse/execute loop — including the statement-type check, the
/// `IsImportableForeignTable` filter, the schema-name rewrite, the wrapper
/// `PlannedStmt` construction, the inter-subcommand `CommandCounterIncrement`,
/// and the error-context callback — are all owned here. `GetFdwRoutine` + the
/// FDW callback, `pg_parse_query`, the filter predicate, and `ProcessUtility`
/// cross their owners' seams.
pub fn ImportForeignSchema<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &ImportForeignSchemaStmt<'mcx>,
) -> PgResult<()> {
    let server_name = stmt.server_name.as_str();
    let local_schema = stmt.local_schema.as_str();

    /* Check that the foreign server exists and that we have USAGE on it */
    let server = foreign_seams::get_foreign_server_by_name::call(mcx, server_name, false)?
        .ok_or_else(|| {
            ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("server \"{server_name}\" does not exist"))
                .into_error()
                .with_error_location(errloc(1505, "ImportForeignSchema"))
        })?;
    let aclresult = aclchk_seams::object_aclcheck::call(
        ForeignServerRelationId,
        server.serverid,
        miscinit_seams::get_user_id::call(),
        ACL_USAGE,
    )?;
    if aclresult != ACLCHECK_OK {
        return aclchk_seams::aclcheck_error::call(
            aclresult,
            OBJECT_FOREIGN_SERVER,
            Some(server.servername.as_str().to_string()),
        );
    }

    /* Check that the schema exists and we have CREATE permissions on it */
    let _ = namespace_seams::lookup_creation_namespace::call(local_schema)?;

    /* Get the FDW and check it supports IMPORT */
    let fdw = foreign_seams::get_foreign_data_wrapper::call(mcx, server.fdwid)?;
    if !OidIsValid(fdw.fdwhandler) {
        return ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "foreign-data wrapper \"{}\" has no handler",
                fdw.fdwname
            ))
            .finish(errloc(1516, "ImportForeignSchema"));
    }

    /*
     * GetFdwRoutine + the `fdw_routine->ImportForeignSchema == NULL` guard and
     * the FDW callback (`fdw_routine->ImportForeignSchema(stmt, server->serverid)`)
     * cross the seam: a `None` result is the C NULL callback, raising
     * ERRCODE_FDW_NO_SCHEMAS here; a `Some(cmd_list)` is the FDW's command list.
     */
    let cmd_list = match foreign_seams::fdw_import_foreign_schema::call(
        mcx,
        stmt,
        server.serverid,
        fdw.fdwhandler,
    )? {
        Some(cmds) => cmds,
        None => {
            return ereport(ERROR)
                .errcode(ERRCODE_FDW_NO_SCHEMAS)
                .errmsg(format!(
                    "foreign-data wrapper \"{}\" does not support IMPORT FOREIGN SCHEMA",
                    fdw.fdwname
                ))
                .finish(errloc(1522, "ImportForeignSchema"));
        }
    };

    /* Parse and execute each command */
    for cmd in cmd_list.iter() {
        let cmd = cmd.as_str();
        import_one_command(mcx, stmt, &fdw.fdwname, local_schema, cmd)?;
    }

    Ok(())
}

/// One iteration of `ImportForeignSchema`'s `foreach (lc, cmd_list)` body: set
/// up the error-context callback for this command, parse it, and process each
/// resulting `CREATE FOREIGN TABLE`. Split out so the error-context callback
/// (C's `error_context_stack` push for the duration of the command) attaches to
/// every `PgError` raised while this command is in flight, then unwinds at the
/// end of the scope — mirroring `error_context_stack = sqlerrcontext.previous`.
fn import_one_command<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &ImportForeignSchemaStmt<'mcx>,
    fdwname: &str,
    local_schema: &str,
    cmd: &str,
) -> PgResult<()> {
    /*
     * Setup error traceback support for ereport(). This is so that any error
     * in the generated SQL will be displayed nicely. The current table name is
     * not known yet.
     */
    let mut callback_arg = ImportErrorCallbackArg {
        tablename: None,
        cmd,
    };

    /*
     * Parse the SQL string into a list of raw parse trees. The error-context
     * callback applies to this parse step as well.
     */
    let raw_parsetree_list = postgres_seams::pg_parse_query::call(mcx, cmd)
        .map_err(|e| import_error_callback(e, &callback_arg))?;

    /*
     * Process each parse tree (we allow the FDW to put more than one command
     * per string, though this isn't really advised).
     */
    for rs in raw_parsetree_list.iter() {
        /*
         * Because we only allow CreateForeignTableStmt, we can skip parse
         * analysis, rewrite, and planning steps here.
         */
        let info = foreign_seams::import_classify_raw_stmt::call(*rs)
            .map_err(|e| import_error_callback(e, &callback_arg))?;
        let (relname, stmt_location, stmt_len, utility_stmt) = match info {
            ImportRawStmt::CreateForeignTable {
                relname,
                stmt_location,
                stmt_len,
                utility_stmt,
            } => (relname, stmt_location, stmt_len, utility_stmt),
            ImportRawStmt::Other { node_tag } => {
                let err = ereport(ERROR)
                    .errcode(ERRCODE_INTERNAL_ERROR)
                    .errmsg(format!(
                        "foreign-data wrapper \"{fdwname}\" returned incorrect statement type {node_tag}"
                    ))
                    .into_error()
                    .with_error_location(errloc(1570, "ImportForeignSchema"));
                return Err(import_error_callback(err, &callback_arg));
            }
        };

        /* Ignore commands for tables excluded by filter options */
        if !foreign_seams::is_importable_foreign_table::call(&relname, stmt)
            .map_err(|e| import_error_callback(e, &callback_arg))?
        {
            continue;
        }

        /* Enable reporting of current table's name on error */
        callback_arg.tablename = Some(relname);

        /* Ensure creation schema is the one given in IMPORT statement */
        foreign_seams::import_set_schemaname::call(*rs, local_schema)
            .map_err(|e| import_error_callback(e, &callback_arg))?;

        /* No planning needed, just make a wrapper PlannedStmt */
        let pstmt = ImportPlannedStmt {
            command_type: CMD_UTILITY,
            can_set_tag: false,
            utility_stmt,
            stmt_location,
            stmt_len,
        };

        /* Execute statement */
        utility_seams::process_utility_import_subcommand::call(pstmt, cmd)
            .map_err(|e| import_error_callback(e, &callback_arg))?;

        /* Be sure to advance the command counter between subcommands */
        backend_access_transam_xact_seams::command_counter_increment::call()
            .map_err(|e| import_error_callback(e, &callback_arg))?;

        callback_arg.tablename = None;
    }

    Ok(())
}

/* ===========================================================================
 * RoleSpec helpers
 *
 * In C, `stmt->user` is a `RoleSpec *`; the public/`get_rolespec_oid` split is
 * threaded inline.
 * ======================================================================== */

/// True when `role->roletype == ROLESPEC_PUBLIC`.
fn rolespec_is_public(role: &RoleSpec<'_>) -> bool {
    role.roletype == ROLESPEC_PUBLIC
}

/// `role->rolename` for the IF-EXISTS "role does not exist" notice.
fn rolespec_name(role: &RoleSpec<'_>) -> String {
    role.rolename
        .as_ref()
        .map(|r| r.as_str().to_string())
        .unwrap_or_default()
}

/// The C `if (role->roletype == ROLESPEC_PUBLIC) useId = ACL_ID_PUBLIC; else
/// useId = get_rolespec_oid(stmt->user, missing_ok);` idiom (the
/// CreateUserMapping / AlterUserMapping callers pass `missing_ok = false`).
fn rolespec_oid(role: &RoleSpec<'_>, missing_ok: bool) -> PgResult<Oid> {
    if rolespec_is_public(role) {
        Ok(ACL_ID_PUBLIC)
    } else {
        acl_seams::get_rolespec_oid::call(role, missing_ok)
    }
}

/* ===========================================================================
 * Seam installation
 *
 * Most of this crate's public command drivers are reached from utility
 * processing (`ProcessUtility`), which is not yet ported. But the
 * `*_oid` owner-change entry points are reached from REASSIGN OWNED in
 * `backend-catalog-pg-shdepend`, a direct dependency cycle, so they are
 * declared as inward seams in `backend-commands-foreigncmds-seams` and must be
 * installed here.
 *
 * The seam contract for the REASSIGN-OWNED owner-oid family is `Mcx`-free
 * (matching the neighbor `alter_type_owner_oid` / `alter_schema_owner_oid` /
 * `at_exec_change_owner` seams, all called from the same shdepend dispatch
 * with no `Mcx` in scope). The ported functions need an `Mcx` for the
 * syscache-row allocation, so each installer wrapper creates a local
 * `MemoryContext` and runs the ported function in it — the established
 * bridging idiom (cf. backend-commands-matview::init_seams).
 * ======================================================================== */

/// Install this crate's inward seams: the two REASSIGN-OWNED owner-change
/// entry points reached from `backend-catalog-pg-shdepend`.
pub fn init_seams() {
    use backend_commands_foreigncmds_seams as s;

    s::alter_foreign_server_owner_oid::set(|srv_id, new_owner_id| {
        let ctx = mcx::MemoryContext::new("AlterForeignServerOwner_oid");
        AlterForeignServerOwner_oid(ctx.mcx(), srv_id, new_owner_id)
    });
    s::alter_foreign_data_wrapper_owner_oid::set(|fdw_id, new_owner_id| {
        let ctx = mcx::MemoryContext::new("AlterForeignDataWrapperOwner_oid");
        AlterForeignDataWrapperOwner_oid(ctx.mcx(), fdw_id, new_owner_id)
    });
}

#[cfg(test)]
mod tests;
