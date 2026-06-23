#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

//! `backend/commands/operatorcmds.c` — CREATE / ALTER / DROP OPERATOR support
//! (PostgreSQL 18.3).
//!
//! Every C function — public ([`DefineOperator`], [`RemoveOperatorById`],
//! [`AlterOperator`]) and static ([`ValidateRestrictionEstimator`],
//! [`ValidateJoinEstimator`], [`ValidateOperatorReference`]) — is implemented
//! in-crate with identical branch order, permission checks, error
//! codes/messages/SQLSTATEs, lock levels, and the argument bundle handed to the
//! catalog routines.
//!
//! `QualifiedNameGetCreationNamespace`, the `defGet*` accessors, and
//! `op_signature_string` are called through their ported owner crates directly.
//! The ACL machinery, the type/function/operator lookups, and the
//! catalog-munging helpers (`OperatorCreate`/`OperatorUpd`/
//! `OperatorValidateParams`/`makeOperatorDependencies` plus the raw operator
//! tuple I/O) cross to their still-unported owners through `-seams` crates,
//! each of which panics until the owner lands. No silent stubs.

use mcx::Mcx;

use utils_error::ereport;
use types_error::pg_error::ErrorLocation;
use types_error::{
    PgResult, ERRCODE_AMBIGUOUS_FUNCTION, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_FUNCTION_DEFINITION, ERRCODE_INVALID_OBJECT_DEFINITION,
    ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_FUNCTION, ERROR, WARNING,
};

use catalog_namespace::QualifiedNameGetCreationNamespace;
use define::{defGetBoolean, defGetQualifiedName, defGetTypeName};
use parse_oper::op_signature_string;

use aclchk_seams::{aclcheck_error, object_aclcheck, object_ownercheck};
use pg_operator_seams::{
    alter_operator_apply, fetch_operator_form, invoke_object_post_alter_hook, operator_create,
    operator_lookup, operator_upd, operator_validate_params, remove_operator_tuple,
    OperatorAttrUpdate, OperatorCreateArgs, OperatorValidateParamsArgs,
};
use functioncmds_seams::{lookup_func_name, name_list_to_string};
use parse_type_seams::typename_type_id;
use format_type_seams::format_type_be_owned;
use lsyscache_seams::{get_element_type, get_func_rettype, get_namespace_name};
use miscinit_seams::get_user_id;
use superuser_seams::superuser;

use types_acl::{ACLCHECK_NOT_OWNER, ACLCHECK_OK, ACL_CREATE, ACL_EXECUTE, ACL_USAGE};
use types_catalog::catalog::{
    NAMESPACE_RELATION_ID, OPERATOR_RELATION_ID, PROCEDURE_RELATION_ID, TYPE_RELATION_ID,
};
use types_catalog::catalog_dependency::ObjectAddress;
use types_core::catalog::{FirstGenbkiObjectId, INT4OID, INTERNALOID, OIDOID};
use types_core::primitive::{InvalidOid, Oid};
use nodes::parsenodes::{OBJECT_FUNCTION, OBJECT_OPERATOR, OBJECT_SCHEMA, OBJECT_TYPE};
use parsenodes::{AlterOperatorStmt, DefElem, Node, TypeName};

/// `INT2OID` / `FLOAT8OID` (catalog/pg_type.h) — used in the estimator-function
/// argument-type templates and the float8-return check.
const INT2OID: Oid = 21;
const FLOAT8OID: Oid = 701;

/// `OidIsValid(objectId)` (`c.h`).
#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

/// `errstart`/`errfinish` source location — `src/backend/commands/operatorcmds.c`.
fn errloc(lineno: i32, funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/commands/operatorcmds.c", lineno, funcname)
}

/// `defel->defname` — the attribute name of a `DefElem`, or `""` when absent.
fn def_name(defel: &DefElem) -> &str {
    defel.defname.as_deref().unwrap_or("")
}

/// Flatten a qualified-name `List *` of `String` value nodes (the form
/// `defGetQualifiedName` returns) to the bare name components.
fn qualified_name_strings(nodes: &[Node]) -> Vec<String> {
    nodes
        .iter()
        .map(|n| match n.as_string() {
            Some(s) => s.sval.clone().unwrap_or_default(),
            None => String::new(),
        })
        .collect()
}

/// Convert the raw-parser `TypeName` (`parsenodes`, returned by
/// `defGetTypeName`) into the resolver-facing `TypeName` (`opclass`) that
/// `typenameTypeId` consumes.
fn to_resolver_typename(tn: &TypeName) -> opclass::TypeName {
    opclass::TypeName {
        names: qualified_name_strings(&tn.names),
        typeOid: tn.typeOid,
        setof: tn.setof,
        pct_type: tn.pct_type,
        typemod: tn.typemod,
        arrayBounds: tn
            .arrayBounds
            .iter()
            .map(|n| n.as_integer().map(|i| i.ival).unwrap_or(-1))
            .collect(),
        location: tn.location,
    }
}

/// `aclcheck_error_type(aclerr, typeOid)` (aclchk.c): for a type whose ACL
/// check failed, render the (element-)type name and raise. Always raises (the
/// only way callers reach it).
fn aclcheck_error_type(aclerr: types_acl::AclResult, type_oid: Oid) -> PgResult<()> {
    let element_type = get_element_type::call(type_oid)?.unwrap_or(InvalidOid);
    let name = format_type_be_owned::call(if OidIsValid(element_type) {
        element_type
    } else {
        type_oid
    })?;
    aclcheck_error::call(aclerr, OBJECT_TYPE, Some(name))
}

/// `DefineOperator` (operatorcmds.c) — extracts everything from the parameter
/// list and lets `OperatorCreate()` do the actual work.
///
/// `names` is the (possibly-qualified) operator name (a `List *` of `String`);
/// `parameters` is a `List *` of `DefElem` nodes.
pub fn DefineOperator(
    mcx: Mcx<'_>,
    names: &[Option<String>],
    parameters: &[Node],
) -> PgResult<ObjectAddress> {
    let mut canMerge = false; /* operator merges */
    let mut canHash = false; /* operator hashes */
    let mut functionName: Option<Vec<String>> = None; /* function for operator */
    let mut typeName1: Option<TypeName> = None; /* first type name */
    let mut typeName2: Option<TypeName> = None; /* second type name */
    let mut typeId1: Oid = InvalidOid; /* types converted to OID */
    let mut typeId2: Oid = InvalidOid;
    let mut commutatorName: Vec<String> = Vec::new(); /* optional commutator operator name */
    let mut negatorName: Vec<String> = Vec::new(); /* optional negator operator name */
    let mut restrictionName: Vec<String> = Vec::new(); /* optional restrict. sel. function */
    let mut joinName: Vec<String> = Vec::new(); /* optional join sel. function */
    let restrictionOid: Oid;
    let joinOid: Oid;
    let mut typeId: [Oid; 2] = [InvalidOid; 2]; /* to hold left and right arg */
    let nargs: i32;

    /* Convert list of names to a name and namespace */
    let (oprNamespace, oprName) = QualifiedNameGetCreationNamespace(mcx, names)?;
    let oprName = oprName.to_string();

    /* Check we have creation rights in target namespace */
    let aclresult =
        object_aclcheck::call(NAMESPACE_RELATION_ID, oprNamespace, get_user_id::call(), ACL_CREATE)?;
    if aclresult != ACLCHECK_OK {
        aclcheck_error::call(
            aclresult,
            OBJECT_SCHEMA,
            get_namespace_name::call(mcx, oprNamespace)?.map(|s| s.as_str().to_string()),
        )?;
    }

    /*
     * loop over the definition list and extract the information we need.
     */
    for node in parameters {
        let Some(defel) = node.as_defelem() else {
            return Err(ereport(ERROR)
                .errmsg_internal("DefineOperator: parameter list element is not a DefElem")
                .into_error());
        };
        let defname = def_name(defel);

        if defname == "leftarg" {
            let tn = defGetTypeName(defel)?;
            if tn.setof {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg("SETOF type not allowed for operator argument")
                    .into_error());
            }
            typeName1 = Some(tn);
        } else if defname == "rightarg" {
            let tn = defGetTypeName(defel)?;
            if tn.setof {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg("SETOF type not allowed for operator argument")
                    .into_error());
            }
            typeName2 = Some(tn);
        }
        /* "function" and "procedure" are equivalent here */
        else if defname == "function" || defname == "procedure" {
            functionName = Some(qualified_name_strings(&defGetQualifiedName(defel)?));
        } else if defname == "commutator" {
            commutatorName = qualified_name_strings(&defGetQualifiedName(defel)?);
        } else if defname == "negator" {
            negatorName = qualified_name_strings(&defGetQualifiedName(defel)?);
        } else if defname == "restrict" {
            restrictionName = qualified_name_strings(&defGetQualifiedName(defel)?);
        } else if defname == "join" {
            joinName = qualified_name_strings(&defGetQualifiedName(defel)?);
        } else if defname == "hashes" {
            canHash = defGetBoolean(defel)?;
        } else if defname == "merges" {
            canMerge = defGetBoolean(defel)?;
        }
        /* These obsolete options are taken as meaning canMerge */
        else if defname == "sort1" || defname == "sort2" || defname == "ltcmp"
            || defname == "gtcmp"
        {
            canMerge = true;
        } else {
            /* WARNING, not ERROR, for historical backwards-compatibility */
            ereport(WARNING)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("operator attribute \"{defname}\" not recognized"))
                .finish(errloc(152, "DefineOperator"))?;
        }
    }

    /*
     * make sure we have our required definitions
     */
    let functionName = match functionName {
        Some(f) => f,
        None => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg("operator function must be specified")
                .into_error());
        }
    };

    /* Transform type names to type OIDs */
    if let Some(tn) = &typeName1 {
        typeId1 = typename_type_id::call(&to_resolver_typename(tn))?;
    }
    if let Some(tn) = &typeName2 {
        typeId2 = typename_type_id::call(&to_resolver_typename(tn))?;
    }

    /*
     * If only the right argument is missing, the user is likely trying to
     * create a postfix operator, so give them a hint about why that does not
     * work.  But if both arguments are missing, do not mention postfix
     * operators, as the user most likely simply neglected to mention the
     * arguments.
     */
    if !OidIsValid(typeId1) && !OidIsValid(typeId2) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
            .errmsg("operator argument types must be specified")
            .into_error());
    }
    if !OidIsValid(typeId2) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
            .errmsg("operator right argument type must be specified")
            .errdetail("Postfix operators are not supported.")
            .into_error());
    }

    if typeName1.is_some() {
        let aclresult = object_aclcheck::call(TYPE_RELATION_ID, typeId1, get_user_id::call(), ACL_USAGE)?;
        if aclresult != ACLCHECK_OK {
            aclcheck_error_type(aclresult, typeId1)?;
        }
    }

    if typeName2.is_some() {
        let aclresult = object_aclcheck::call(TYPE_RELATION_ID, typeId2, get_user_id::call(), ACL_USAGE)?;
        if aclresult != ACLCHECK_OK {
            aclcheck_error_type(aclresult, typeId2)?;
        }
    }

    /*
     * Look up the operator's underlying function.
     */
    if !OidIsValid(typeId1) {
        typeId[0] = typeId2;
        nargs = 1;
    } else if !OidIsValid(typeId2) {
        typeId[0] = typeId1;
        nargs = 1;
    } else {
        typeId[0] = typeId1;
        typeId[1] = typeId2;
        nargs = 2;
    }
    let functionOid =
        lookup_func_name::call(functionName.clone(), nargs, typeId[..nargs as usize].to_vec(), false)?;

    /*
     * We require EXECUTE rights for the function.  This isn't strictly
     * necessary, since EXECUTE will be checked at any attempted use of the
     * operator, but it seems like a good idea anyway.
     */
    let aclresult = object_aclcheck::call(PROCEDURE_RELATION_ID, functionOid, get_user_id::call(), ACL_EXECUTE)?;
    if aclresult != ACLCHECK_OK {
        aclcheck_error::call(
            aclresult,
            OBJECT_FUNCTION,
            Some(name_list_to_string::call(functionName.clone())?),
        )?;
    }

    let rettype = get_func_rettype::call(functionOid)?;
    let aclresult = object_aclcheck::call(TYPE_RELATION_ID, rettype, get_user_id::call(), ACL_USAGE)?;
    if aclresult != ACLCHECK_OK {
        aclcheck_error_type(aclresult, rettype)?;
    }

    /*
     * Look up restriction and join estimators if specified
     */
    if !restrictionName.is_empty() {
        restrictionOid = ValidateRestrictionEstimator(restrictionName)?;
    } else {
        restrictionOid = InvalidOid;
    }
    if !joinName.is_empty() {
        joinOid = ValidateJoinEstimator(joinName)?;
    } else {
        joinOid = InvalidOid;
    }

    /*
     * now have OperatorCreate do all the work..
     */
    operator_create::call(OperatorCreateArgs {
        operator_name: oprName,           /* operator name */
        operator_namespace: oprNamespace, /* namespace */
        left_type: typeId1,               /* left type id */
        right_type: typeId2,              /* right type id */
        proc: functionOid,                /* function for operator */
        commutator_name: commutatorName,  /* optional commutator operator name */
        negator_name: negatorName,        /* optional negator operator name */
        restriction_oid: restrictionOid,  /* optional restrict. sel. function */
        join_oid: joinOid,                /* optional join sel. function name */
        can_merge: canMerge,              /* operator merges */
        can_hash: canHash,                /* operator hashes */
    })
}

/// `ValidateRestrictionEstimator` (operatorcmds.c) — look up a restriction
/// estimator by name and verify signature + permissions.
fn ValidateRestrictionEstimator(restrictionName: Vec<String>) -> PgResult<Oid> {
    let mut typeId: [Oid; 4] = [InvalidOid; 4];

    typeId[0] = INTERNALOID; /* PlannerInfo */
    typeId[1] = OIDOID; /* operator OID */
    typeId[2] = INTERNALOID; /* args list */
    typeId[3] = INT4OID; /* varRelid */

    let restrictionOid = lookup_func_name::call(restrictionName.clone(), 4, typeId.to_vec(), false)?;

    /* estimators must return float8 */
    if get_func_rettype::call(restrictionOid)? != FLOAT8OID {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "restriction estimator function {} must return type {}",
                name_list_to_string::call(restrictionName)?,
                "float8"
            ))
            .into_error());
    }

    /*
     * If the estimator is not a built-in function, require superuser privilege
     * to install it.  If it is built-in, only require EXECUTE rights.
     */
    if restrictionOid >= FirstGenbkiObjectId {
        if !superuser::call()? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg(
                    "must be superuser to specify a non-built-in restriction estimator function",
                )
                .into_error());
        }
    } else {
        let aclresult =
            object_aclcheck::call(PROCEDURE_RELATION_ID, restrictionOid, get_user_id::call(), ACL_EXECUTE)?;
        if aclresult != ACLCHECK_OK {
            aclcheck_error::call(
                aclresult,
                OBJECT_FUNCTION,
                Some(name_list_to_string::call(restrictionName)?),
            )?;
        }
    }

    Ok(restrictionOid)
}

/// `ValidateJoinEstimator` (operatorcmds.c) — look up a join estimator by name
/// and verify signature + permissions.
fn ValidateJoinEstimator(joinName: Vec<String>) -> PgResult<Oid> {
    let mut typeId: [Oid; 5] = [InvalidOid; 5];
    let mut joinOid: Oid;

    typeId[0] = INTERNALOID; /* PlannerInfo */
    typeId[1] = OIDOID; /* operator OID */
    typeId[2] = INTERNALOID; /* args list */
    typeId[3] = INT2OID; /* jointype */
    typeId[4] = INTERNALOID; /* SpecialJoinInfo */

    /*
     * As of Postgres 8.4, the preferred signature for join estimators has 5
     * arguments, but we still allow the old 4-argument form.  Whine about
     * ambiguity if both forms exist.
     */
    joinOid = lookup_func_name::call(joinName.clone(), 5, typeId.to_vec(), true)?;
    let joinOid2 = lookup_func_name::call(joinName.clone(), 4, typeId[..4].to_vec(), true)?;
    if OidIsValid(joinOid) {
        if OidIsValid(joinOid2) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_AMBIGUOUS_FUNCTION)
                .errmsg(format!(
                    "join estimator function {} has multiple matches",
                    name_list_to_string::call(joinName)?
                ))
                .into_error());
        }
    } else {
        joinOid = joinOid2;
        /* If not found, reference the 5-argument signature in error msg */
        if !OidIsValid(joinOid) {
            joinOid = lookup_func_name::call(joinName.clone(), 5, typeId.to_vec(), false)?;
        }
    }

    /* estimators must return float8 */
    if get_func_rettype::call(joinOid)? != FLOAT8OID {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "join estimator function {} must return type {}",
                name_list_to_string::call(joinName)?,
                "float8"
            ))
            .into_error());
    }

    /* privilege checks are the same as in ValidateRestrictionEstimator */
    if joinOid >= FirstGenbkiObjectId {
        if !superuser::call()? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("must be superuser to specify a non-built-in join estimator function")
                .into_error());
        }
    } else {
        let aclresult =
            object_aclcheck::call(PROCEDURE_RELATION_ID, joinOid, get_user_id::call(), ACL_EXECUTE)?;
        if aclresult != ACLCHECK_OK {
            aclcheck_error::call(
                aclresult,
                OBJECT_FUNCTION,
                Some(name_list_to_string::call(joinName)?),
            )?;
        }
    }

    Ok(joinOid)
}

/// `ValidateOperatorReference` (operatorcmds.c) — look up an operator by name +
/// arg types, verifying it is defined (not a shell) and owned by the current
/// user.
fn ValidateOperatorReference(name: Vec<String>, leftTypeId: Oid, rightTypeId: Oid) -> PgResult<Oid> {
    let (oid, defined) = operator_lookup::call(name.clone(), leftTypeId, rightTypeId)?;

    /* These message strings are chosen to match parse_oper.c */
    if !OidIsValid(oid) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "operator does not exist: {}",
                op_signature_string(&name, leftTypeId, rightTypeId)?
            ))
            .into_error());
    }

    if !defined {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "operator is only a shell: {}",
                op_signature_string(&name, leftTypeId, rightTypeId)?
            ))
            .into_error());
    }

    if !object_ownercheck::call(OPERATOR_RELATION_ID, oid, get_user_id::call())? {
        aclcheck_error::call(
            ACLCHECK_NOT_OWNER,
            OBJECT_OPERATOR,
            Some(name_list_to_string::call(name)?),
        )?;
    }

    Ok(oid)
}

/// `RemoveOperatorById` (operatorcmds.c) — guts of operator deletion.
///
/// Reads the form in-crate to decide whether the commutator/negator back-links
/// must be reset; the syscache fetch/release, the optional `OperatorUpd`, and
/// the final `CatalogTupleDelete` (under `RowExclusiveLock`) cross the seam
/// together so the self-commutator/self-negator re-fetch is performed
/// faithfully.
pub fn RemoveOperatorById(operOid: Oid) -> PgResult<()> {
    let op = match fetch_operator_form::call(operOid)? {
        Some(op) => op,
        None => {
            /* should not happen */
            return Err(ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for operator {operOid}"))
                .into_error());
        }
    };

    /*
     * Reset links from commutator and negator, if any.  In case of a
     * self-commutator or self-negator, this means we have to re-fetch the
     * updated tuple.
     */
    let do_operator_upd = OidIsValid(op.oprcom) || OidIsValid(op.oprnegate);

    remove_operator_tuple::call(operOid, op.oprcom, op.oprnegate, do_operator_upd)
}

/// `AlterOperator` (operatorcmds.c) — ALTER OPERATOR … SET (…).
///
/// Only RESTRICT and JOIN estimator functions can be changed; COMMUTATOR,
/// NEGATOR, MERGES and HASHES can be set if not set previously.
pub fn AlterOperator(stmt: &AlterOperatorStmt) -> PgResult<ObjectAddress> {
    let address: ObjectAddress;
    let mut restrictionName: Vec<String> = Vec::new(); /* optional restrict. sel. function */
    let mut updateRestriction = false;
    let restrictionOid: Oid;
    let mut joinName: Vec<String> = Vec::new(); /* optional join sel. function */
    let mut updateJoin = false;
    let joinOid: Oid;
    let mut commutatorName: Vec<String> = Vec::new(); /* optional commutator operator name */
    let commutatorOid: Oid;
    let mut negatorName: Vec<String> = Vec::new(); /* optional negator operator name */
    let negatorOid: Oid;
    let mut canMerge = false;
    let mut updateMerges = false;
    let mut canHash = false;
    let mut updateHashes = false;

    /* Look up the operator */
    let Some(opername) = &stmt.opername else {
        return Err(ereport(ERROR)
            .errmsg_internal("AlterOperator: stmt->opername is not an ObjectWithArgs")
            .into_error());
    };
    let oprId = parse_oper_seams::lookup_oper_with_args_node::call(opername, false)?;
    /* table_open(OperatorRelationId, RowExclusiveLock) + SearchSysCacheCopy1 */
    let oprForm = match fetch_operator_form::call(oprId)? {
        Some(f) => f,
        None => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for operator {oprId}"))
                .into_error());
        }
    };

    /* Process options */
    for node in &stmt.options {
        let Some(defel) = node.as_defelem() else {
            return Err(ereport(ERROR)
                .errmsg_internal("AlterOperator: option list element is not a DefElem")
                .into_error());
        };
        let defname = def_name(defel);

        let param: Vec<String> = if defel.arg.is_none() {
            Vec::new() /* NONE, removes the function */
        } else {
            qualified_name_strings(&defGetQualifiedName(defel)?)
        };

        if defname == "restrict" {
            restrictionName = param;
            updateRestriction = true;
        } else if defname == "join" {
            joinName = param;
            updateJoin = true;
        } else if defname == "commutator" {
            commutatorName = qualified_name_strings(&defGetQualifiedName(defel)?);
        } else if defname == "negator" {
            negatorName = qualified_name_strings(&defGetQualifiedName(defel)?);
        } else if defname == "merges" {
            canMerge = defGetBoolean(defel)?;
            updateMerges = true;
        } else if defname == "hashes" {
            canHash = defGetBoolean(defel)?;
            updateHashes = true;
        }
        /*
         * The rest of the options that CREATE accepts cannot be changed.
         * Check for them so that we can give a meaningful error message.
         */
        else if defname == "leftarg"
            || defname == "rightarg"
            || defname == "function"
            || defname == "procedure"
        {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("operator attribute \"{defname}\" cannot be changed"))
                .into_error());
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("operator attribute \"{defname}\" not recognized"))
                .into_error());
        }
    }

    /* Check permissions. Must be owner. */
    if !object_ownercheck::call(OPERATOR_RELATION_ID, oprId, get_user_id::call())? {
        aclcheck_error::call(
            ACLCHECK_NOT_OWNER,
            OBJECT_OPERATOR,
            Some(oprForm.oprname.clone()),
        )?;
    }

    /*
     * Look up OIDs for any parameters specified
     */
    if !restrictionName.is_empty() {
        restrictionOid = ValidateRestrictionEstimator(restrictionName)?;
    } else {
        restrictionOid = InvalidOid;
    }
    if !joinName.is_empty() {
        joinOid = ValidateJoinEstimator(joinName)?;
    } else {
        joinOid = InvalidOid;
    }

    if !commutatorName.is_empty() {
        /* commutator has reversed arg types */
        commutatorOid = ValidateOperatorReference(commutatorName, oprForm.oprright, oprForm.oprleft)?;

        /*
         * We don't need to do anything extra for a self commutator as in
         * OperatorCreate, since the operator surely exists already.
         */
    } else {
        commutatorOid = InvalidOid;
    }

    if !negatorName.is_empty() {
        negatorOid = ValidateOperatorReference(negatorName, oprForm.oprleft, oprForm.oprright)?;

        /* Must reject self-negation */
        if negatorOid == oprForm.oid {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg("operator cannot be its own negator")
                .into_error());
        }
    } else {
        negatorOid = InvalidOid;
    }

    /*
     * Check that we're not changing any attributes that might be depended on
     * by plans, while allowing no-op updates.
     */
    if OidIsValid(commutatorOid) && OidIsValid(oprForm.oprcom) && commutatorOid != oprForm.oprcom {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
            .errmsg(format!(
                "operator attribute \"{}\" cannot be changed if it has already been set",
                "commutator"
            ))
            .into_error());
    }

    if OidIsValid(negatorOid) && OidIsValid(oprForm.oprnegate) && negatorOid != oprForm.oprnegate {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
            .errmsg(format!(
                "operator attribute \"{}\" cannot be changed if it has already been set",
                "negator"
            ))
            .into_error());
    }

    if updateMerges && oprForm.oprcanmerge && !canMerge {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
            .errmsg(format!(
                "operator attribute \"{}\" cannot be changed if it has already been set",
                "merges"
            ))
            .into_error());
    }

    if updateHashes && oprForm.oprcanhash && !canHash {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
            .errmsg(format!(
                "operator attribute \"{}\" cannot be changed if it has already been set",
                "hashes"
            ))
            .into_error());
    }

    /* Perform additional checks, like OperatorCreate does */
    operator_validate_params::call(OperatorValidateParamsArgs {
        oprleft: oprForm.oprleft,
        oprright: oprForm.oprright,
        oprresult: oprForm.oprresult,
        has_commutator: OidIsValid(commutatorOid),
        has_negator: OidIsValid(negatorOid),
        has_restriction_selectivity: OidIsValid(restrictionOid),
        has_join_selectivity: OidIsValid(joinOid),
        can_merge: canMerge,
        can_hash: canHash,
    })?;

    /* Update the tuple */
    let mut updates: Vec<OperatorAttrUpdate> = Vec::new();
    if updateRestriction {
        updates.push(OperatorAttrUpdate::Restriction(restrictionOid));
    }
    if updateJoin {
        updates.push(OperatorAttrUpdate::Join(joinOid));
    }
    if OidIsValid(commutatorOid) {
        updates.push(OperatorAttrUpdate::Commutator(commutatorOid));
    }
    if OidIsValid(negatorOid) {
        updates.push(OperatorAttrUpdate::Negator(negatorOid));
    }
    if updateMerges {
        updates.push(OperatorAttrUpdate::Merges(canMerge));
    }
    if updateHashes {
        updates.push(OperatorAttrUpdate::Hashes(canHash));
    }

    /*
     * heap_modify_tuple + CatalogTupleUpdate + makeOperatorDependencies(tup,
     * false, true).
     */
    address = alter_operator_apply::call(oprId, updates)?;

    if OidIsValid(commutatorOid) || OidIsValid(negatorOid) {
        operator_upd::call(oprId, commutatorOid, negatorOid, false)?;
    }

    invoke_object_post_alter_hook::call(oprId)?;

    /* table_close(catalog, NoLock) */

    Ok(address)
}

/// Install the seams `operatorcmds.c` owns. `RemoveOperatorById` is declared in
/// `backend-catalog-pg-operator-seams` (so dependency.c can call it across a
/// cycle) but its C lives here, so this crate is its installer.
pub fn init_seams() {
    pg_operator_seams::RemoveOperatorById::set(RemoveOperatorById);

    // ProcessUtilitySlow dispatch target (utility.c): ALTER OPERATOR. Decode the
    // rich `AlterOperatorStmt` into the flat parsenodes form the ported
    // `AlterOperator` body consumes.
    utility_out_seams::alter_operator::set(alter_operator_seam);
}

/// Outward-seam adapter for `AlterOperator(stmt)` (utility.c `ProcessUtilitySlow`
/// `T_AlterOperatorStmt`): decode the rich `AlterOperatorStmt` into the flat
/// [`parsenodes::AlterOperatorStmt`] and run the ported [`AlterOperator`]
/// body.
fn alter_operator_seam<'mcx>(
    _mcx: Mcx<'mcx>,
    stmt: &nodes::nodes::Node<'mcx>,
) -> PgResult<ObjectAddress> {
    use parse_type::{rich_node_to_parse, rich_objectwithargs_to_parse};

    let aos = match stmt.as_alteroperatorstmt() {
        Some(s) => s,
        None => {
            return Err(ereport(ERROR)
                .errmsg_internal("alter_operator_seam: statement is not an AlterOperatorStmt")
                .into_error())
        }
    };

    let opername = match aos.opername.as_deref() {
        Some(n) => match n.as_objectwithargs() {
            Some(owa) => Some(rich_objectwithargs_to_parse(owa)?),
            None => {
                return Err(ereport(ERROR)
                    .errmsg_internal("ALTER OPERATOR: opername is not an ObjectWithArgs")
                    .into_error())
            }
        },
        None => None,
    };

    let mut options: Vec<parsenodes::Node> = Vec::with_capacity(aos.options.len());
    for n in aos.options.iter() {
        options.push(rich_node_to_parse(n)?);
    }

    let pn = parsenodes::AlterOperatorStmt { opername, options };

    AlterOperator(&pn)
}
