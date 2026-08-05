//! operatorcmds.c — CREATE/ALTER OPERATOR and RemoveOperatorById.
#![allow(non_snake_case, non_upper_case_globals)]

use cache_syscache::{SearchSysCacheCopy, SysCacheKey, OPEROID};
use datum::Datum;
use elog::ereport;
use mcx::Mcx;
use pg_depend::ObjectAddress;
use types_core::{
    FirstGenbkiObjectId, InvalidOid, Oid, FLOAT8OID, INT2OID, INT4OID, INTERNALOID,
    NAMESPACE_RELATION_ID, OIDOID, OPERATOR_RELATION_ID, PROCEDURE_RELATION_ID, TYPE_RELATION_ID,
};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_AMBIGUOUS_FUNCTION, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_FUNCTION_DEFINITION, ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_SYNTAX_ERROR,
    ERRCODE_UNDEFINED_FUNCTION, ERROR, WARNING,
};
use types_nodes::parsenodes::{AlterOperatorStmt, ObjectType, ObjectWithArgs};
use types_nodes::rawnodes::TypeName;
use types_nodes::NodeList;
use types_rel::RowExclusiveLock;

use pg_operator::{
    form_of_tuple, makeOperatorDependencies, Anum_pg_operator_oprcanhash,
    Anum_pg_operator_oprcanmerge, Anum_pg_operator_oprcom, Anum_pg_operator_oprjoin,
    Anum_pg_operator_oprnegate, Anum_pg_operator_oprrest, Natts_pg_operator, OperatorCreate,
    OperatorLookup, OperatorUpd, OperatorValidateParams,
};

fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

#[track_caller]
#[cold]
fn err(sqlstate: types_error::SqlState, msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(sqlstate))
}

fn typename_type_id(mcx: Mcx<'_>, tn: &TypeName<'_>) -> PgResult<Oid> {
    parse_utilcmd::LookupTypeNameOid(mcx, tn)
}

fn type_acl_check(typeId: Oid) -> PgResult<()> {
    let aclresult = aclchk::object_aclcheck(
        TYPE_RELATION_ID,
        typeId,
        miscinit::GetUserId(),
        adt_acl::ACL_USAGE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        // aclcheck_error_type
        let name = format_type::format_type_be(typeId)?;
        aclchk::aclcheck_error(aclresult, ObjectType::OBJECT_TYPE, &name)?;
    }
    Ok(())
}

// DefineOperator (operatorcmds.c): decode the CREATE OPERATOR DefElem list
// and hand off to OperatorCreate.
pub fn DefineOperator<'mcx>(
    mcx: Mcx<'mcx>,
    names: &NodeList<'mcx>,
    parameters: &NodeList<'mcx>,
) -> PgResult<ObjectAddress> {
    let mut buf = [""; 4];
    let parts = name_parts(names, &mut buf);
    let (oprNamespace, oprName) =
        catalog_namespace::QualifiedNameGetCreationNamespace(mcx, parts)?;

    let aclresult = aclchk::object_aclcheck(
        NAMESPACE_RELATION_ID,
        oprNamespace,
        miscinit::GetUserId(),
        adt_acl::ACL_CREATE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        let nspname = lsyscache::get_namespace_name(mcx, oprNamespace)?;
        aclchk::aclcheck_error(
            aclresult,
            ObjectType::OBJECT_SCHEMA,
            nspname.as_ref().map(|s| s.as_str()).unwrap_or(""),
        )?;
    }

    let mut canMerge = false;
    let mut canHash = false;
    let mut typeName1: Option<&TypeName<'mcx>> = None;
    let mut typeName2: Option<&TypeName<'mcx>> = None;
    let mut functionName: Option<&NodeList<'mcx>> = None;
    let mut commutatorName: Option<&NodeList<'mcx>> = None;
    let mut negatorName: Option<&NodeList<'mcx>> = None;
    let mut restrictionName: Option<&NodeList<'mcx>> = None;
    let mut joinName: Option<&NodeList<'mcx>> = None;

    for n in parameters.iter() {
        let defel = n.as_def_elem().expect("CREATE OPERATOR definition: DefElem list");
        match defel.defname.unwrap_or("") {
            "leftarg" => {
                let tn = commands_define::defGetTypeName(mcx, defel)?;
                if tn.setof {
                    return Err(err(
                        ERRCODE_INVALID_FUNCTION_DEFINITION,
                        "SETOF type not allowed for operator argument".into(),
                    ));
                }
                typeName1 = Some(tn);
            }
            "rightarg" => {
                let tn = commands_define::defGetTypeName(mcx, defel)?;
                if tn.setof {
                    return Err(err(
                        ERRCODE_INVALID_FUNCTION_DEFINITION,
                        "SETOF type not allowed for operator argument".into(),
                    ));
                }
                typeName2 = Some(tn);
            }
            "function" | "procedure" => {
                functionName = Some(commands_define::defGetQualifiedName(mcx, defel)?)
            }
            "commutator" => {
                commutatorName = Some(commands_define::defGetQualifiedName(mcx, defel)?)
            }
            "negator" => negatorName = Some(commands_define::defGetQualifiedName(mcx, defel)?),
            "restrict" => {
                restrictionName = Some(commands_define::defGetQualifiedName(mcx, defel)?)
            }
            "join" => joinName = Some(commands_define::defGetQualifiedName(mcx, defel)?),
            "hashes" => canHash = commands_define::defGetBoolean(defel)?,
            "merges" => canMerge = commands_define::defGetBoolean(defel)?,
            // Obsolete options taken as meaning canMerge.
            "sort1" | "sort2" | "ltcmp" | "gtcmp" => canMerge = true,
            other => {
                // WARNING, not ERROR, for historical backwards-compatibility.
                ereport(WARNING)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("operator attribute \"{other}\" not recognized"))
                    .finish(ErrorLocation::new(file!(), line!() as i32, "DefineOperator"))?;
            }
        }
    }

    let Some(functionName) = functionName else {
        return Err(err(
            ERRCODE_INVALID_FUNCTION_DEFINITION,
            "operator function must be specified".into(),
        ));
    };

    let typeId1 = match typeName1 {
        Some(tn) => typename_type_id(mcx, tn)?,
        None => InvalidOid,
    };
    let typeId2 = match typeName2 {
        Some(tn) => typename_type_id(mcx, tn)?,
        None => InvalidOid,
    };

    if !OidIsValid(typeId1) && !OidIsValid(typeId2) {
        return Err(err(
            ERRCODE_INVALID_FUNCTION_DEFINITION,
            "operator argument types must be specified".into(),
        ));
    }
    if !OidIsValid(typeId2) {
        return Err(Box::new(
            ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg("operator right argument type must be specified")
                .errdetail("Postfix operators are not supported.")
                .into_error(),
        ));
    }

    if typeName1.is_some() {
        type_acl_check(typeId1)?;
    }
    if typeName2.is_some() {
        type_acl_check(typeId2)?;
    }

    let (typeId, nargs): ([Oid; 2], i16) = if !OidIsValid(typeId1) {
        ([typeId2, InvalidOid], 1)
    } else if !OidIsValid(typeId2) {
        ([typeId1, InvalidOid], 1)
    } else {
        ([typeId1, typeId2], 2)
    };
    let functionOid = parse_func::LookupFuncName(functionName, nargs, &typeId, false)?;

    let aclresult = aclchk::object_aclcheck(
        PROCEDURE_RELATION_ID,
        functionOid,
        miscinit::GetUserId(),
        adt_acl::ACL_EXECUTE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        let name = commands_define::NameListToString(mcx, functionName)?;
        aclchk::aclcheck_error(aclresult, ObjectType::OBJECT_FUNCTION, name.as_str())?;
    }

    let rettype = lsyscache::get_func_rettype(functionOid)?;
    type_acl_check(rettype)?;

    let restrictionOid = match restrictionName {
        Some(n) => ValidateRestrictionEstimator(mcx, n)?,
        None => InvalidOid,
    };
    let joinOid = match joinName {
        Some(n) => ValidateJoinEstimator(mcx, n)?,
        None => InvalidOid,
    };

    OperatorCreate(
        mcx,
        oprName,
        oprNamespace,
        typeId1,
        typeId2,
        functionOid,
        commutatorName,
        negatorName,
        restrictionOid,
        joinOid,
        canMerge,
        canHash,
    )
}

fn estimator_priv_check(mcx: Mcx<'_>, oid: Oid, name: &NodeList<'_>, what: &str) -> PgResult<()> {
    if oid >= FirstGenbkiObjectId {
        if !superuser::superuser()? {
            return Err(err(
                ERRCODE_INSUFFICIENT_PRIVILEGE,
                format!("must be superuser to specify a non-built-in {what} estimator function"),
            ));
        }
    } else {
        let aclresult = aclchk::object_aclcheck(
            PROCEDURE_RELATION_ID,
            oid,
            miscinit::GetUserId(),
            adt_acl::ACL_EXECUTE,
        )?;
        if aclresult != aclchk::ACLCHECK_OK {
            let s = commands_define::NameListToString(mcx, name)?;
            aclchk::aclcheck_error(aclresult, ObjectType::OBJECT_FUNCTION, s.as_str())?;
        }
    }
    Ok(())
}

fn ValidateRestrictionEstimator(mcx: Mcx<'_>, restrictionName: &NodeList<'_>) -> PgResult<Oid> {
    let typeId = [INTERNALOID, OIDOID, INTERNALOID, INT4OID];
    let restrictionOid = parse_func::LookupFuncName(restrictionName, 4, &typeId, false)?;

    if lsyscache::get_func_rettype(restrictionOid)? != FLOAT8OID {
        let s = commands_define::NameListToString(mcx, restrictionName)?;
        return Err(err(
            ERRCODE_INVALID_OBJECT_DEFINITION,
            format!(
                "restriction estimator function {} must return type {}",
                s.as_str(),
                "float8"
            ),
        ));
    }

    estimator_priv_check(mcx, restrictionOid, restrictionName, "restriction")?;
    Ok(restrictionOid)
}

fn ValidateJoinEstimator(mcx: Mcx<'_>, joinName: &NodeList<'_>) -> PgResult<Oid> {
    let typeId = [INTERNALOID, OIDOID, INTERNALOID, INT2OID, INTERNALOID];

    // The 5-arg form is preferred since 8.4; the 4-arg form is still allowed.
    let mut joinOid = parse_func::LookupFuncName(joinName, 5, &typeId, true)?;
    let joinOid2 = parse_func::LookupFuncName(joinName, 4, &typeId, true)?;
    if OidIsValid(joinOid) {
        if OidIsValid(joinOid2) {
            let s = commands_define::NameListToString(mcx, joinName)?;
            return Err(err(
                ERRCODE_AMBIGUOUS_FUNCTION,
                format!("join estimator function {} has multiple matches", s.as_str()),
            ));
        }
    } else {
        joinOid = joinOid2;
        if !OidIsValid(joinOid) {
            // Reference the 5-argument signature in the error message.
            joinOid = parse_func::LookupFuncName(joinName, 5, &typeId, false)?;
        }
    }

    if lsyscache::get_func_rettype(joinOid)? != FLOAT8OID {
        let s = commands_define::NameListToString(mcx, joinName)?;
        return Err(err(
            ERRCODE_INVALID_OBJECT_DEFINITION,
            format!("join estimator function {} must return type {}", s.as_str(), "float8"),
        ));
    }

    estimator_priv_check(mcx, joinOid, joinName, "join")?;
    Ok(joinOid)
}

// Message strings chosen to match parse_oper.c.
fn ValidateOperatorReference(
    name: &NodeList<'_>,
    leftTypeId: Oid,
    rightTypeId: Oid,
) -> PgResult<Oid> {
    let (oid, defined) = OperatorLookup(name, leftTypeId, rightTypeId)?;

    let mut buf = [""; 4];
    let parts = name_parts(name, &mut buf);
    if !OidIsValid(oid) {
        return Err(err(
            ERRCODE_UNDEFINED_FUNCTION,
            format!(
                "operator does not exist: {}",
                parse_oper::op_signature_string(parts, leftTypeId, rightTypeId)?
            ),
        ));
    }
    if !defined {
        return Err(err(
            ERRCODE_UNDEFINED_FUNCTION,
            format!(
                "operator is only a shell: {}",
                parse_oper::op_signature_string(parts, leftTypeId, rightTypeId)?
            ),
        ));
    }
    if !aclchk::object_ownercheck(OPERATOR_RELATION_ID, oid, miscinit::GetUserId())? {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NOT_OWNER,
            ObjectType::OBJECT_OPERATOR,
            &parts.join("."),
        )?;
    }
    Ok(oid)
}

// Guts of operator deletion (invoked through the dependency machinery).
pub fn RemoveOperatorById(mcx: Mcx<'_>, operOid: Oid) -> PgResult<()> {
    let rel = table::table_open(mcx, OPERATOR_RELATION_ID, RowExclusiveLock)?;

    let fetch = || {
        Ok::<_, Box<types_error::PgError>>(SearchSysCacheCopy(
            mcx,
            OPEROID,
            SysCacheKey::Value(Datum::from_oid(operOid)),
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
        )?
        .unwrap_or_else(|| panic!("cache lookup failed for operator {operOid}")))
    };

    let mut tup = fetch()?;
    let op = form_of_tuple(&rel, tup.as_tuple());

    // Reset links from commutator and negator; a self-link means the tuple
    // just changed under us and must be re-fetched.
    if OidIsValid(op.oprcom) || OidIsValid(op.oprnegate) {
        OperatorUpd(mcx, operOid, op.oprcom, op.oprnegate, true)?;
        if operOid == op.oprcom || operOid == op.oprnegate {
            tup = fetch()?;
        }
    }

    let tid = tup.as_tuple().t_self;
    catalog_indexing::CatalogTupleDelete(&rel, &tid)?;

    rel.close(RowExclusiveLock)
}

// AlterOperator (operatorcmds.c): ALTER OPERATOR <op> SET (option = ...).
pub fn AlterOperator<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterOperatorStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let owa = stmt
        .opername
        .and_then(|n| n.as_variant::<ObjectWithArgs>())
        .expect("AlterOperatorStmt.opername is ObjectWithArgs");
    let oprId = parse_oper::LookupOperWithArgs(&owa.objname, &owa.objargs, false)?;

    let catalog = table::table_open(mcx, OPERATOR_RELATION_ID, RowExclusiveLock)?;
    let tup = SearchSysCacheCopy(
        mcx,
        OPEROID,
        SysCacheKey::Value(Datum::from_oid(oprId)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?
    .unwrap_or_else(|| panic!("cache lookup failed for operator {oprId}"));
    let oprForm = form_of_tuple(&catalog, tup.as_tuple());

    let mut restrictionName: Option<&NodeList<'mcx>> = None;
    let mut updateRestriction = false;
    let mut joinName: Option<&NodeList<'mcx>> = None;
    let mut updateJoin = false;
    let mut commutatorName: Option<&NodeList<'mcx>> = None;
    let mut negatorName: Option<&NodeList<'mcx>> = None;
    let mut canMerge = false;
    let mut updateMerges = false;
    let mut canHash = false;
    let mut updateHashes = false;

    for n in stmt.options.iter() {
        let defel = n.as_def_elem().expect("ALTER OPERATOR options: DefElem list");
        let param = match defel.arg {
            None => None, // NONE removes the function
            Some(_) => Some(commands_define::defGetQualifiedName(mcx, defel)?),
        };
        match defel.defname.unwrap_or("") {
            "restrict" => {
                restrictionName = param;
                updateRestriction = true;
            }
            "join" => {
                joinName = param;
                updateJoin = true;
            }
            "commutator" => {
                commutatorName = Some(commands_define::defGetQualifiedName(mcx, defel)?)
            }
            "negator" => negatorName = Some(commands_define::defGetQualifiedName(mcx, defel)?),
            "merges" => {
                canMerge = commands_define::defGetBoolean(defel)?;
                updateMerges = true;
            }
            "hashes" => {
                canHash = commands_define::defGetBoolean(defel)?;
                updateHashes = true;
            }
            other @ ("leftarg" | "rightarg" | "function" | "procedure") => {
                return Err(err(
                    ERRCODE_SYNTAX_ERROR,
                    format!("operator attribute \"{other}\" cannot be changed"),
                ));
            }
            other => {
                return Err(err(
                    ERRCODE_SYNTAX_ERROR,
                    format!("operator attribute \"{other}\" not recognized"),
                ));
            }
        }
    }

    // Permission check: must be owner.
    if !aclchk::object_ownercheck(OPERATOR_RELATION_ID, oprId, miscinit::GetUserId())? {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NOT_OWNER,
            ObjectType::OBJECT_OPERATOR,
            core::str::from_utf8(oprForm.oprname.name_str()).expect("non-UTF-8 oprname"),
        )?;
    }

    let restrictionOid = match restrictionName {
        Some(n) => ValidateRestrictionEstimator(mcx, n)?,
        None => InvalidOid,
    };
    let joinOid = match joinName {
        Some(n) => ValidateJoinEstimator(mcx, n)?,
        None => InvalidOid,
    };

    let commutatorOid = if let Some(commutatorName) = commutatorName {
        // commutator has reversed arg types; a self-commutator surely exists.
        ValidateOperatorReference(commutatorName, oprForm.oprright, oprForm.oprleft)?
    } else {
        InvalidOid
    };

    let negatorOid = if let Some(negatorName) = negatorName {
        let oid = ValidateOperatorReference(negatorName, oprForm.oprleft, oprForm.oprright)?;
        if oid == oprForm.oid {
            return Err(err(
                ERRCODE_INVALID_FUNCTION_DEFINITION,
                "operator cannot be its own negator".into(),
            ));
        }
        oid
    } else {
        InvalidOid
    };

    // Only no-op updates are allowed for plan-affecting attributes.
    fn cannot_change(attr: &str) -> Box<PgError> {
        err(
            ERRCODE_INVALID_FUNCTION_DEFINITION,
            format!("operator attribute \"{attr}\" cannot be changed if it has already been set"),
        )
    }
    if OidIsValid(commutatorOid) && OidIsValid(oprForm.oprcom) && commutatorOid != oprForm.oprcom {
        return Err(cannot_change("commutator"));
    }
    if OidIsValid(negatorOid) && OidIsValid(oprForm.oprnegate) && negatorOid != oprForm.oprnegate {
        return Err(cannot_change("negator"));
    }
    if updateMerges && oprForm.oprcanmerge && !canMerge {
        return Err(cannot_change("merges"));
    }
    if updateHashes && oprForm.oprcanhash && !canHash {
        return Err(cannot_change("hashes"));
    }

    OperatorValidateParams(
        oprForm.oprleft,
        oprForm.oprright,
        oprForm.oprresult,
        OidIsValid(commutatorOid),
        OidIsValid(negatorOid),
        OidIsValid(restrictionOid),
        OidIsValid(joinOid),
        canMerge,
        canHash,
    )?;

    let mut values = [Datum::null(); Natts_pg_operator];
    let nulls = [false; Natts_pg_operator];
    let mut replaces = [false; Natts_pg_operator];
    {
        let mut set = |attnum: i32, value: Datum| {
            values[attnum as usize - 1] = value;
            replaces[attnum as usize - 1] = true;
        };
        if updateRestriction {
            set(Anum_pg_operator_oprrest, Datum::from_oid(restrictionOid));
        }
        if updateJoin {
            set(Anum_pg_operator_oprjoin, Datum::from_oid(joinOid));
        }
        if OidIsValid(commutatorOid) {
            set(Anum_pg_operator_oprcom, Datum::from_oid(commutatorOid));
        }
        if OidIsValid(negatorOid) {
            set(Anum_pg_operator_oprnegate, Datum::from_oid(negatorOid));
        }
        if updateMerges {
            set(Anum_pg_operator_oprcanmerge, Datum::from_bool(canMerge));
        }
        if updateHashes {
            set(Anum_pg_operator_oprcanhash, Datum::from_bool(canHash));
        }
    }

    let mut newtup = heaptuple::heap_modify_tuple(
        mcx,
        tup.as_tuple(),
        catalog.descr(),
        &values,
        &nulls,
        &replaces,
    )?;
    let otid = tup.as_tuple().t_self;
    catalog_indexing::CatalogTupleUpdate(mcx, &catalog, &otid, &mut newtup)?;

    let newform = form_of_tuple(&catalog, newtup.as_tuple());
    let address = makeOperatorDependencies(mcx, &newform, false, true)?;

    if OidIsValid(commutatorOid) || OidIsValid(negatorOid) {
        OperatorUpd(mcx, oprId, commutatorOid, negatorOid, false)?;
    }

    catalog.close(RowExclusiveLock)?;

    Ok(address)
}

fn name_parts<'a, 'mcx>(names: &NodeList<'mcx>, buf: &'a mut [&'mcx str; 4]) -> &'a [&'mcx str] {
    let n = names.len().min(buf.len());
    for (i, slot) in buf.iter_mut().enumerate().take(n) {
        *slot = names.nth(i).as_string().expect("name list holds String nodes").sval;
    }
    &buf[..n]
}

pub fn init_seams() {
    dependency_seams::remove_operator_by_id::set(RemoveOperatorById);
}
