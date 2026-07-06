// typecmds.c DefineRange + findRange* helpers + range/multirange
// constructor creation.

use mcx::Mcx;
use parser_small1::ParseState;
use types_core::{
    InvalidOid, Oid, OidIsValid, BOOTSTRAP_SUPERUSERID, BTREE_AM_OID, FLOAT8OID,
    PROCEDURE_RELATION_ID, TEXTOID, TYPE_RELATION_ID,
};
use types_error::{
    PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_INVALID_OBJECT_DEFINITION,
    ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_nodes::parsenodes::DefElem;
use types_nodes::rawnodes::CreateRangeStmt;
use types_nodes::NodeList;

use pg_depend::DependencyType;
use pg_proc::{
    INTERNALlanguageId, PROARGMODE_VARIADIC, PROKIND_FUNCTION, PROPARALLEL_SAFE,
    PROVOLATILE_IMMUTABLE, ProcedureCreateArgs,
};
use pg_type::{ObjectAddress, TypeCreateParams, DEFAULT_TYPDELIM, TYPCATEGORY_ARRAY, TYPTYPE_BASE};

use crate::{
    domain_err, function_does_not_exist, type_already_exists, TYPALIGN_DOUBLE, TYPALIGN_INT,
    TYPSTORAGE_EXTENDED, TYPTYPE_MULTIRANGE, TYPTYPE_PSEUDO, TYPTYPE_RANGE,
};

const F_RANGE_IN: Oid = 3834;
const F_RANGE_OUT: Oid = 3835;
const F_RANGE_RECV: Oid = 3836;
const F_RANGE_SEND: Oid = 3837;
const F_RANGE_TYPANALYZE: Oid = 3916;
const F_MULTIRANGE_IN: Oid = 4231;
const F_MULTIRANGE_OUT: Oid = 4232;
const F_MULTIRANGE_RECV: Oid = 4233;
const F_MULTIRANGE_SEND: Oid = 4234;
const F_MULTIRANGE_TYPANALYZE: Oid = 4242;
const F_FMGR_INTERNAL_VALIDATOR: Oid = 2246;
const TYPCATEGORY_RANGE: i8 = b'R' as i8;
const COERCION_CODE_EXPLICIT: i8 = b'e' as i8;
const COERCION_METHOD_FUNCTION: i8 = b'f' as i8;

pub fn DefineRange<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    stmt: &CreateRangeStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let (typeNamespace, typeName) =
        crate::creation_namespace(mcx, &stmt.typeName, "DefineRange")?;

    let mut typoid = syscache_seams::lookup_pg_type_oid_by_name::call(typeName, typeNamespace)?;
    if OidIsValid(typoid) && lsyscache::get_typisdefined(typoid)? {
        if pg_type::moveArrayTypeName(mcx, typoid, typeName, typeNamespace)? {
            typoid = InvalidOid;
        } else {
            return Err(type_already_exists(typeName));
        }
    }

    let mut rangeSubtype = InvalidOid;
    let mut rangeSubOpclassName: Option<&NodeList<'mcx>> = None;
    let mut rangeCollationName: Option<&NodeList<'mcx>> = None;
    let mut rangeCanonicalName: Option<&NodeList<'mcx>> = None;
    let mut rangeSubtypeDiffName: Option<&NodeList<'mcx>> = None;
    let mut multirangeTypeName: Option<&str> = None;
    let mut multirangeNamespace = InvalidOid;

    for n in stmt.params.iter() {
        let defel = n.as_def_elem().expect("CREATE TYPE AS RANGE params: DefElem list");
        let conflicting = |defel: &DefElem<'_>| {
            domain_err(pstate, ERRCODE_SYNTAX_ERROR, "conflicting or redundant options", defel.location)
        };
        match defel.defname.unwrap_or("") {
            "subtype" => {
                if OidIsValid(rangeSubtype) {
                    return Err(conflicting(defel));
                }
                let tn = commands_define::defGetTypeName(mcx, defel)?;
                rangeSubtype = parse_utilcmd::LookupTypeNameOid(mcx, tn)?;
            }
            "subtype_opclass" => {
                if rangeSubOpclassName.is_some() {
                    return Err(conflicting(defel));
                }
                rangeSubOpclassName = Some(commands_define::defGetQualifiedName(mcx, defel)?);
            }
            "collation" => {
                if rangeCollationName.is_some() {
                    return Err(conflicting(defel));
                }
                rangeCollationName = Some(commands_define::defGetQualifiedName(mcx, defel)?);
            }
            "canonical" => {
                if rangeCanonicalName.is_some() {
                    return Err(conflicting(defel));
                }
                rangeCanonicalName = Some(commands_define::defGetQualifiedName(mcx, defel)?);
            }
            "subtype_diff" => {
                if rangeSubtypeDiffName.is_some() {
                    return Err(conflicting(defel));
                }
                rangeSubtypeDiffName = Some(commands_define::defGetQualifiedName(mcx, defel)?);
            }
            "multirange_type_name" => {
                if multirangeTypeName.is_some() {
                    return Err(conflicting(defel));
                }
                let names = commands_define::defGetQualifiedName(mcx, defel)?;
                let mut buf = [""; 4];
                let nnames = names.len();
                assert!((1..=3).contains(&nnames), "improper qualified name");
                for (i, n) in names.iter().enumerate() {
                    buf[i] = n.as_string().expect("qualified name").sval;
                }
                let (nsp, name) =
                    catalog_namespace::QualifiedNameGetCreationNamespace(mcx, &buf[..nnames])?;
                multirangeNamespace = nsp;
                multirangeTypeName = Some(name);
            }
            other => {
                return Err(Box::new(
                    PgError::new(ERROR, format!("type attribute \"{other}\" not recognized"))
                        .with_sqlstate(ERRCODE_SYNTAX_ERROR),
                ));
            }
        }
    }

    if !OidIsValid(rangeSubtype) {
        return Err(Box::new(
            PgError::new(ERROR, "type attribute \"subtype\" is required".to_string())
                .with_sqlstate(ERRCODE_SYNTAX_ERROR),
        ));
    }
    if lsyscache::get_typtype(rangeSubtype)? == TYPTYPE_PSEUDO {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "range subtype cannot be {}",
                    format_type::format_type_be(rangeSubtype)?
                ),
            )
            .with_sqlstate(ERRCODE_DATATYPE_MISMATCH),
        ));
    }

    let rangeSubOpclass = findRangeSubOpclass(mcx, rangeSubOpclassName, rangeSubtype)?;

    let rangeCollation = if lsyscache::type_is_collatable(rangeSubtype)? {
        match rangeCollationName {
            Some(names) => {
                let mut buf = [""; 4];
                let nnames = names.len();
                assert!((1..=3).contains(&nnames), "improper qualified name");
                for (i, n) in names.iter().enumerate() {
                    buf[i] = n.as_string().expect("qualified name").sval;
                }
                catalog_namespace::get_collation_oid(&buf[..nnames], false)?
            }
            None => lsyscache::get_typcollation(rangeSubtype)?,
        }
    } else {
        if rangeCollationName.is_some() {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    "range collation specified but subtype does not support collation"
                        .to_string(),
                )
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
            ));
        }
        InvalidOid
    };

    let rangeCanonical = match rangeCanonicalName {
        Some(names) => {
            if !OidIsValid(typoid) {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        "cannot specify a canonical function without a pre-created shell type"
                            .to_string(),
                    )
                    .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .with_hint(
                        "Create the type as a shell type, then create its canonicalization \
                         function, then do a full CREATE TYPE.",
                    ),
                ));
            }
            findRangeCanonicalFunction(mcx, names, typoid)?
        }
        None => InvalidOid,
    };

    let rangeSubtypeDiff = match rangeSubtypeDiffName {
        Some(names) => findRangeSubtypeDiffFunction(mcx, names, rangeSubtype)?,
        None => InvalidOid,
    };

    let (_subtyplen, _subtypbyval, subtypalign) = lsyscache::get_typlenbyvalalign(rangeSubtype)?;
    let alignment = if subtypalign == TYPALIGN_DOUBLE { TYPALIGN_DOUBLE } else { TYPALIGN_INT };

    let rangeArrayOid = pg_type::AssignTypeArrayOid(mcx)?;
    let multirangeOid = pg_type::AssignTypeMultirangeOid(mcx)?;
    let multirangeArrayOid = pg_type::AssignTypeMultirangeArrayOid(mcx)?;

    let user_id = miscinit::GetUserId();

    let address = pg_type::TypeCreate(
        mcx,
        &TypeCreateParams {
            newTypeOid: InvalidOid,
            typeName,
            typeNamespace,
            relationOid: InvalidOid,
            relationKind: 0,
            ownerId: user_id,
            internalSize: -1,
            typeType: TYPTYPE_RANGE,
            typeCategory: TYPCATEGORY_RANGE,
            typePreferred: false,
            typDelim: DEFAULT_TYPDELIM,
            inputProcedure: F_RANGE_IN,
            outputProcedure: F_RANGE_OUT,
            receiveProcedure: F_RANGE_RECV,
            sendProcedure: F_RANGE_SEND,
            typmodinProcedure: InvalidOid,
            typmodoutProcedure: InvalidOid,
            analyzeProcedure: F_RANGE_TYPANALYZE,
            subscriptProcedure: InvalidOid,
            elementType: InvalidOid,
            isImplicitArray: false,
            arrayType: rangeArrayOid,
            baseType: InvalidOid,
            passedByValue: false,
            alignment,
            storage: TYPSTORAGE_EXTENDED,
            typeMod: -1,
            typNDims: 0,
            typeNotNull: false,
            typeCollation: InvalidOid,
            defaultValue: None,
            defaultTypeBin: None,
        },
    )?;
    debug_assert!(typoid == InvalidOid || typoid == address.objectId);
    let typoid = address.objectId;

    let mut mrng_name_buf = None;
    let (multirangeTypeName, multirangeNamespace) = match multirangeTypeName {
        Some(name) => {
            let old_typoid =
                syscache_seams::lookup_pg_type_oid_by_name::call(name, multirangeNamespace)?;
            if OidIsValid(old_typoid)
                && lsyscache::get_typisdefined(old_typoid)?
                && !pg_type::moveArrayTypeName(mcx, old_typoid, name, multirangeNamespace)?
            {
                return Err(type_already_exists(name));
            }
            (name, multirangeNamespace)
        }
        None => {
            let name = pg_type::makeMultirangeTypeName(typeName, typeNamespace)?;
            let name = mrng_name_buf.insert(name);
            (
                core::str::from_utf8(name.name_str()).expect("multirange type name"),
                typeNamespace,
            )
        }
    };

    let mltrng_address = pg_type::TypeCreate(
        mcx,
        &TypeCreateParams {
            newTypeOid: multirangeOid,
            typeName: multirangeTypeName,
            typeNamespace: multirangeNamespace,
            relationOid: InvalidOid,
            relationKind: 0,
            ownerId: user_id,
            internalSize: -1,
            typeType: TYPTYPE_MULTIRANGE,
            typeCategory: TYPCATEGORY_RANGE,
            typePreferred: false,
            typDelim: DEFAULT_TYPDELIM,
            inputProcedure: F_MULTIRANGE_IN,
            outputProcedure: F_MULTIRANGE_OUT,
            receiveProcedure: F_MULTIRANGE_RECV,
            sendProcedure: F_MULTIRANGE_SEND,
            typmodinProcedure: InvalidOid,
            typmodoutProcedure: InvalidOid,
            analyzeProcedure: F_MULTIRANGE_TYPANALYZE,
            subscriptProcedure: InvalidOid,
            elementType: InvalidOid,
            isImplicitArray: false,
            arrayType: multirangeArrayOid,
            baseType: InvalidOid,
            passedByValue: false,
            alignment,
            storage: TYPSTORAGE_EXTENDED,
            typeMod: -1,
            typNDims: 0,
            typeNotNull: false,
            typeCollation: InvalidOid,
            defaultValue: None,
            defaultTypeBin: None,
        },
    )?;
    debug_assert!(multirangeOid == mltrng_address.objectId);

    pg_range::RangeCreate(
        mcx,
        typoid,
        rangeSubtype,
        rangeCollation,
        rangeSubOpclass,
        rangeCanonical,
        rangeSubtypeDiff,
        multirangeOid,
    )?;

    let rangeArrayName = pg_type::makeArrayTypeName(typeName, typeNamespace)?;
    pg_type::TypeCreate(
        mcx,
        &TypeCreateParams {
            newTypeOid: rangeArrayOid,
            typeName: core::str::from_utf8(rangeArrayName.name_str()).expect("array type name"),
            typeNamespace,
            relationOid: InvalidOid,
            relationKind: 0,
            ownerId: user_id,
            internalSize: -1,
            typeType: TYPTYPE_BASE,
            typeCategory: TYPCATEGORY_ARRAY,
            typePreferred: false,
            typDelim: DEFAULT_TYPDELIM,
            inputProcedure: pg_type::F_ARRAY_IN,
            outputProcedure: pg_type::F_ARRAY_OUT,
            receiveProcedure: pg_type::F_ARRAY_RECV,
            sendProcedure: pg_type::F_ARRAY_SEND,
            typmodinProcedure: InvalidOid,
            typmodoutProcedure: InvalidOid,
            analyzeProcedure: pg_type::F_ARRAY_TYPANALYZE,
            subscriptProcedure: pg_type::F_ARRAY_SUBSCRIPT_HANDLER,
            elementType: typoid,
            isImplicitArray: true,
            arrayType: InvalidOid,
            baseType: InvalidOid,
            passedByValue: false,
            alignment,
            storage: TYPSTORAGE_EXTENDED,
            typeMod: -1,
            typNDims: 0,
            typeNotNull: false,
            typeCollation: InvalidOid,
            defaultValue: None,
            defaultTypeBin: None,
        },
    )?;

    let multirangeArrayName =
        pg_type::makeArrayTypeName(multirangeTypeName, typeNamespace)?;
    pg_type::TypeCreate(
        mcx,
        &TypeCreateParams {
            newTypeOid: multirangeArrayOid,
            typeName: core::str::from_utf8(multirangeArrayName.name_str())
                .expect("array type name"),
            typeNamespace: multirangeNamespace,
            relationOid: InvalidOid,
            relationKind: 0,
            ownerId: user_id,
            internalSize: -1,
            typeType: TYPTYPE_BASE,
            typeCategory: TYPCATEGORY_ARRAY,
            typePreferred: false,
            typDelim: DEFAULT_TYPDELIM,
            inputProcedure: pg_type::F_ARRAY_IN,
            outputProcedure: pg_type::F_ARRAY_OUT,
            receiveProcedure: pg_type::F_ARRAY_RECV,
            sendProcedure: pg_type::F_ARRAY_SEND,
            typmodinProcedure: InvalidOid,
            typmodoutProcedure: InvalidOid,
            analyzeProcedure: pg_type::F_ARRAY_TYPANALYZE,
            subscriptProcedure: pg_type::F_ARRAY_SUBSCRIPT_HANDLER,
            elementType: multirangeOid,
            isImplicitArray: true,
            arrayType: InvalidOid,
            baseType: InvalidOid,
            passedByValue: false,
            alignment,
            storage: TYPSTORAGE_EXTENDED,
            typeMod: -1,
            typNDims: 0,
            typeNotNull: false,
            typeCollation: InvalidOid,
            defaultValue: None,
            defaultTypeBin: None,
        },
    )?;

    makeRangeConstructors(mcx, typeName, typeNamespace, typoid, rangeSubtype)?;
    let castFuncOid = makeMultirangeConstructors(
        mcx,
        multirangeTypeName,
        typeNamespace,
        multirangeOid,
        typoid,
        rangeArrayOid,
    )?;

    pg_cast::CastCreate(
        mcx,
        typoid,
        multirangeOid,
        castFuncOid,
        InvalidOid,
        InvalidOid,
        COERCION_CODE_EXPLICIT,
        COERCION_METHOD_FUNCTION,
        DependencyType::Internal,
    )?;

    Ok(address)
}

fn makeRangeConstructors<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    namespace: Oid,
    rangeOid: Oid,
    subtype: Oid,
) -> PgResult<()> {
    const PROSRC: [&str; 2] = ["range_constructor2", "range_constructor3"];
    const PRONARGS: [usize; 2] = [2, 3];

    let constructorArgTypes = [subtype, subtype, TEXTOID];
    let referenced = pg_depend::ObjectAddress::set(TYPE_RELATION_ID, rangeOid);

    for i in 0..PROSRC.len() {
        let myself = pg_proc::ProcedureCreate(
            mcx,
            &ProcedureCreateArgs {
                procedureName: name,
                procNamespace: namespace,
                replace: false,
                returnsSet: false,
                returnType: rangeOid,
                proowner: BOOTSTRAP_SUPERUSERID,
                languageObjectId: INTERNALlanguageId,
                languageValidator: F_FMGR_INTERNAL_VALIDATOR,
                prosrc: PROSRC[i],
                probin: None,
                prosqlbody: None,
                prokind: PROKIND_FUNCTION,
                security_definer: false,
                isLeakProof: false,
                isStrict: false,
                volatility: PROVOLATILE_IMMUTABLE,
                parallel: PROPARALLEL_SAFE,
                parameterTypes: &constructorArgTypes[..PRONARGS[i]],
                allParameterTypes: None,
                parameterModes: None,
                parameterNames: None,
                proconfig: None,
                procost: 1.0,
                prorows: 0.0,
                prosupport: InvalidOid,
            parameterDefaults: None,
                numDefaults: 0,
            },
        )?;
        // C: constructors are internally dependent on the range type so they
        // drop silently with it (pg_dump relies on this).
        pg_depend::recordDependencyOn(mcx, &myself, &referenced, DependencyType::Internal)?;
    }
    Ok(())
}

fn makeMultirangeConstructors<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    namespace: Oid,
    multirangeOid: Oid,
    rangeOid: Oid,
    rangeArrayOid: Oid,
) -> PgResult<Oid> {
    let referenced = pg_depend::ObjectAddress::set(TYPE_RELATION_ID, multirangeOid);

    let myself = pg_proc::ProcedureCreate(
        mcx,
        &ProcedureCreateArgs {
            procedureName: name,
            procNamespace: namespace,
            replace: false,
            returnsSet: false,
            returnType: multirangeOid,
            proowner: BOOTSTRAP_SUPERUSERID,
            languageObjectId: INTERNALlanguageId,
            languageValidator: F_FMGR_INTERNAL_VALIDATOR,
            prosrc: "multirange_constructor0",
            probin: None,
            prosqlbody: None,
            prokind: PROKIND_FUNCTION,
            security_definer: false,
            isLeakProof: false,
            isStrict: true,
            volatility: PROVOLATILE_IMMUTABLE,
            parallel: PROPARALLEL_SAFE,
            parameterTypes: &[],
            allParameterTypes: None,
            parameterModes: None,
            parameterNames: None,
            proconfig: None,
            procost: 1.0,
            prorows: 0.0,
            prosupport: InvalidOid,
            parameterDefaults: None,
            numDefaults: 0,
        },
    )?;
    pg_depend::recordDependencyOn(mcx, &myself, &referenced, DependencyType::Internal)?;

    let myself = pg_proc::ProcedureCreate(
        mcx,
        &ProcedureCreateArgs {
            procedureName: name,
            procNamespace: namespace,
            replace: false,
            returnsSet: false,
            returnType: multirangeOid,
            proowner: BOOTSTRAP_SUPERUSERID,
            languageObjectId: INTERNALlanguageId,
            languageValidator: F_FMGR_INTERNAL_VALIDATOR,
            prosrc: "multirange_constructor1",
            probin: None,
            prosqlbody: None,
            prokind: PROKIND_FUNCTION,
            security_definer: false,
            isLeakProof: false,
            isStrict: true,
            volatility: PROVOLATILE_IMMUTABLE,
            parallel: PROPARALLEL_SAFE,
            parameterTypes: &[rangeOid],
            allParameterTypes: None,
            parameterModes: None,
            parameterNames: None,
            proconfig: None,
            procost: 1.0,
            prorows: 0.0,
            prosupport: InvalidOid,
            parameterDefaults: None,
            numDefaults: 0,
        },
    )?;
    pg_depend::recordDependencyOn(mcx, &myself, &referenced, DependencyType::Internal)?;
    let castFuncOid = myself.objectId;

    let myself = pg_proc::ProcedureCreate(
        mcx,
        &ProcedureCreateArgs {
            procedureName: name,
            procNamespace: namespace,
            replace: false,
            returnsSet: false,
            returnType: multirangeOid,
            proowner: BOOTSTRAP_SUPERUSERID,
            languageObjectId: INTERNALlanguageId,
            languageValidator: F_FMGR_INTERNAL_VALIDATOR,
            prosrc: "multirange_constructor2",
            probin: None,
            prosqlbody: None,
            prokind: PROKIND_FUNCTION,
            security_definer: false,
            isLeakProof: false,
            isStrict: true,
            volatility: PROVOLATILE_IMMUTABLE,
            parallel: PROPARALLEL_SAFE,
            parameterTypes: &[rangeArrayOid],
            allParameterTypes: Some(&[rangeArrayOid]),
            parameterModes: Some(&[PROARGMODE_VARIADIC]),
            parameterNames: None,
            proconfig: None,
            procost: 1.0,
            prorows: 0.0,
            prosupport: InvalidOid,
            parameterDefaults: None,
            numDefaults: 0,
        },
    )?;
    pg_depend::recordDependencyOn(mcx, &myself, &referenced, DependencyType::Internal)?;

    Ok(castFuncOid)
}

fn findRangeSubOpclass<'mcx>(
    mcx: Mcx<'mcx>,
    opcname: Option<&NodeList<'mcx>>,
    subtype: Oid,
) -> PgResult<Oid> {
    match opcname {
        Some(names) => {
            let opcid = opclasscmds::get_opclass_oid(BTREE_AM_OID, names, false)?;
            let opInputType = lsyscache::get_opclass_input_type(opcid)?;
            if !coerce::IsBinaryCoercible(subtype, opInputType)? {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!(
                            "operator class \"{}\" does not accept data type {}",
                            commands_define::NameListToString(mcx, names)?.as_str(),
                            format_type::format_type_be(subtype)?
                        ),
                    )
                    .with_sqlstate(ERRCODE_DATATYPE_MISMATCH),
                ));
            }
            Ok(opcid)
        }
        None => {
            let opcid = indexcmds_seams::get_default_opclass::call(subtype, BTREE_AM_OID)?;
            if !OidIsValid(opcid) {
                // C: spelled identically to ResolveOpClass.
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!(
                            "data type {} has no default operator class for access method \"btree\"",
                            format_type::format_type_be(subtype)?
                        ),
                    )
                    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT)
                    .with_hint(
                        "You must specify an operator class for the range type or define a \
                         default operator class for the subtype.",
                    ),
                ));
            }
            Ok(opcid)
        }
    }
}

fn range_func_check<'mcx>(
    mcx: Mcx<'mcx>,
    proc_oid: Oid,
    procname: &NodeList<'mcx>,
    argtypes: &[Oid],
    what: &str,
) -> PgResult<()> {
    if lsyscache::func_volatile(proc_oid)? != PROVOLATILE_IMMUTABLE {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("{what} function {} must be immutable", func_sig(mcx, procname, argtypes)?),
            )
            .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
        ));
    }
    if aclchk::object_aclcheck(
        PROCEDURE_RELATION_ID,
        proc_oid,
        miscinit::GetUserId(),
        adt_acl::ACL_EXECUTE,
    )? != aclchk::ACLCHECK_OK
    {
        let name = lsyscache::get_func_name(mcx, proc_oid)?
            .map(|n| n.as_str().to_string())
            .unwrap_or_else(|| proc_oid.to_string());
        return Err(Box::new(
            PgError::new(ERROR, format!("permission denied for function {name}"))
                .with_sqlstate(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }
    Ok(())
}

fn func_sig<'mcx>(
    mcx: Mcx<'mcx>,
    procname: &NodeList<'mcx>,
    argtypes: &[Oid],
) -> PgResult<String> {
    let mut sig = commands_define::NameListToString(mcx, procname)?.as_str().to_string();
    sig.push('(');
    for (i, &t) in argtypes.iter().enumerate() {
        if i > 0 {
            sig.push_str(", ");
        }
        sig.push_str(&format_type::format_type_be(t)?);
    }
    sig.push(')');
    Ok(sig)
}

fn findRangeCanonicalFunction<'mcx>(
    mcx: Mcx<'mcx>,
    procname: &NodeList<'mcx>,
    typeOid: Oid,
) -> PgResult<Oid> {
    let argList = [typeOid];
    let procOid = parse_func::LookupFuncName(procname, 1, &argList, true)?;
    if !OidIsValid(procOid) {
        return Err(function_does_not_exist(mcx, procname, &argList)?);
    }
    if lsyscache::get_func_rettype(procOid)? != typeOid {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "range canonical function {} must return range type",
                    func_sig(mcx, procname, &argList)?
                ),
            )
            .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
        ));
    }
    range_func_check(mcx, procOid, procname, &argList, "range canonical")?;
    Ok(procOid)
}

fn findRangeSubtypeDiffFunction<'mcx>(
    mcx: Mcx<'mcx>,
    procname: &NodeList<'mcx>,
    subtype: Oid,
) -> PgResult<Oid> {
    let argList = [subtype, subtype];
    let procOid = parse_func::LookupFuncName(procname, 2, &argList, true)?;
    if !OidIsValid(procOid) {
        return Err(function_does_not_exist(mcx, procname, &argList)?);
    }
    if lsyscache::get_func_rettype(procOid)? != FLOAT8OID {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "range subtype diff function {} must return type {}",
                    func_sig(mcx, procname, &argList)?,
                    "double precision"
                ),
            )
            .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
        ));
    }
    range_func_check(mcx, procOid, procname, &argList, "range subtype diff")?;
    Ok(procOid)
}
