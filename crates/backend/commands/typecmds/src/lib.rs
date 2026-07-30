// typecmds.c DefineDomain lane (CREATE DOMAIN with NOT NULL/CHECK/NULL
// constraints) + enum lane (DefineEnum/AlterEnum/checkEnumOwner) + ALTER
// DOMAIN/ALTER TYPE lane (alter.rs). COLLATE and inherited base-type
// defaults are loud.
#![allow(non_snake_case, non_upper_case_globals)]

mod alter;
mod range;

pub fn init_seams() {
    typecmds_seams::alter_type_owner_internal::set(alter::AlterTypeOwnerInternal);
    typecmds_seams::alter_domain_add_constraint::set(alter::AlterDomainAddConstraint);
    typecmds_seams::alter_type_namespace_internal::set(alter::AlterTypeNamespaceInternal);
    pg_shdepend::alter_type_owner_oid::set(alter::AlterTypeOwner_oid);
}
pub use range::DefineRange;
pub use alter::{
    checkDomainOwner, AlterDomain, AlterType, AlterTypeNamespace, AlterTypeNamespace_oid,
    AlterTypeNamespaceInternal, AlterTypeOwner, AlterTypeOwner_oid, AlterTypeOwnerInternal,
    RenameDomainConstraint, RenameType,
};

use datum::Datum;
use mcx::{Mcx, PgVec};
use parser_small1::{make_parsestate, ParseExprKind, ParseState, PreColumnRefHook};
use types_core::{AttrNumber, InvalidOid, Oid, OidIsValid, NAMESPACE_RELATION_ID, TYPE_RELATION_ID};
use types_error::{
    PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_DUPLICATE_OBJECT,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_COLUMN_REFERENCE, ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_SYNTAX_ERROR,
    ERRCODE_WRONG_OBJECT_TYPE,
    ERROR,
};
use types_nodes::primnodes::CoerceToDomainValue;
use types_nodes::rawnodes::{AlterEnumStmt, Constraint, ConstrType, CreateDomainStmt, CreateEnumStmt, TypeName};
use types_nodes::NodeTag;
use types_rel::AccessShareLock;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

use pg_type::{
    ObjectAddress, TypeCreateParams, DEFAULT_TYPDELIM, TYPCATEGORY_ARRAY, TYPTYPE_BASE,
    TYPTYPE_DOMAIN,
};

const F_DOMAIN_IN: Oid = 2597;
const F_DOMAIN_RECV: Oid = 2598;
pub(crate) const TYPTYPE_COMPOSITE: i8 = b'c' as i8;
const TYPTYPE_ENUM: i8 = b'e' as i8;
pub(crate) const TYPTYPE_RANGE: i8 = b'r' as i8;
pub(crate) const TYPTYPE_MULTIRANGE: i8 = b'm' as i8;
const TYPSTORAGE_EXTENDED: i8 = b'x' as i8;
const TYPCATEGORY_ENUM: i8 = b'E' as i8;
const TYPALIGN_INT: i8 = b'i' as i8;
const TYPSTORAGE_PLAIN: i8 = b'p' as i8;

const F_ENUM_IN: Oid = 3506;
const F_ENUM_OUT: Oid = 3507;
const F_ENUM_RECV: Oid = 3532;
const F_ENUM_SEND: Oid = 3533;

struct BaseTypeRow {
    typlen: i16,
    typbyval: bool,
    typtype: i8,
    typcategory: i8,
    typdelim: i8,
    typoutput: Oid,
    typsend: Oid,
    typanalyze: Oid,
    typalign: i8,
    typstorage: i8,
    typcollation: Oid,
    has_default: bool,
}

fn base_type_row<'mcx>(mcx: Mcx<'mcx>, typeoid: Oid) -> PgResult<BaseTypeRow> {
    const Anum_pg_type_typlen: AttrNumber = 5;
    const Anum_pg_type_typbyval: AttrNumber = 6;
    const Anum_pg_type_typtype: AttrNumber = 7;
    const Anum_pg_type_typcategory: AttrNumber = 8;
    const Anum_pg_type_typdelim: AttrNumber = 11;
    const Anum_pg_type_typoutput: AttrNumber = 17;
    const Anum_pg_type_typsend: AttrNumber = 19;
    const Anum_pg_type_typanalyze: AttrNumber = 22;
    const Anum_pg_type_typalign: AttrNumber = 23;
    const Anum_pg_type_typstorage: AttrNumber = 24;
    const Anum_pg_type_typcollation: AttrNumber = 29;
    const Anum_pg_type_typdefaultbin: AttrNumber = 30;
    const Anum_pg_type_typdefault: AttrNumber = 31;

    let rel = table::table_open(mcx, TYPE_RELATION_ID, AccessShareLock)?;
    let mut key = ScanKeyData::empty();
    key.sk_attno = pg_type::Anum_pg_type_oid;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(typeoid);
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        pg_type::TypeOidIndexId,
        true,
        None,
        core::slice::from_ref(&key),
    )?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for type {typeoid}"));
    let descr = rel.descr();
    let mut isnull = false;
    // SAFETY (each): fixed NOT NULL pg_type columns of the declared types.
    let get = |attno: AttrNumber, isnull: &mut bool| unsafe {
        types_tuple::heap_getattr(tup, attno as i32, descr, isnull)
    };
    let row = BaseTypeRow {
        typlen: get(Anum_pg_type_typlen, &mut isnull).as_i16(),
        typbyval: get(Anum_pg_type_typbyval, &mut isnull).as_bool(),
        typtype: get(Anum_pg_type_typtype, &mut isnull).as_i8(),
        typcategory: get(Anum_pg_type_typcategory, &mut isnull).as_i8(),
        typdelim: get(Anum_pg_type_typdelim, &mut isnull).as_i8(),
        typoutput: get(Anum_pg_type_typoutput, &mut isnull).as_oid(),
        typsend: get(Anum_pg_type_typsend, &mut isnull).as_oid(),
        typanalyze: get(Anum_pg_type_typanalyze, &mut isnull).as_oid(),
        typalign: get(Anum_pg_type_typalign, &mut isnull).as_i8(),
        typstorage: get(Anum_pg_type_typstorage, &mut isnull).as_i8(),
        typcollation: get(Anum_pg_type_typcollation, &mut isnull).as_oid(),
        has_default: {
            let mut null_bin = false;
            let mut null_def = false;
            get(Anum_pg_type_typdefaultbin, &mut null_bin);
            get(Anum_pg_type_typdefault, &mut null_def);
            !(null_bin && null_def)
        },
    };
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(row)
}

pub(crate) fn type_name_to_string<'mcx>(mcx: Mcx<'mcx>, tn: &TypeName<'_>) -> PgResult<mcx::PgString<'mcx>> {
    let mut s = mcx::PgString::new_in(mcx);
    for (i, n) in tn.names.iter().enumerate() {
        if i > 0 {
            s.try_push_str(".")?;
        }
        s.try_push_str(n.as_string().expect("TypeName names").sval)?;
    }
    Ok(s)
}

pub fn DefineDomain<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    stmt: &CreateDomainStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let mut names: [&str; 4] = [""; 4];
    let nnames = stmt.domainname.len();
    assert!((1..=3).contains(&nnames), "improper qualified name");
    for (i, n) in stmt.domainname.iter().enumerate() {
        names[i] = n.as_string().expect("domainname names").sval;
    }
    let (domain_namespace, domain_name) =
        catalog_namespace::QualifiedNameGetCreationNamespace(mcx, &names[..nnames])?;

    let user_id = miscinit::GetUserId();
    if aclchk::object_aclcheck(
        NAMESPACE_RELATION_ID,
        domain_namespace,
        user_id,
        adt_acl::ACL_CREATE,
    )? != aclchk::ACLCHECK_OK
    {
        return Err(permission_denied_schema(domain_namespace)?);
    }

    let old_type_oid =
        syscache_seams::lookup_pg_type_oid_by_name::call(domain_name, domain_namespace)?;
    if old_type_oid != InvalidOid
        && !pg_type::moveArrayTypeName(mcx, old_type_oid, domain_name, domain_namespace)?
    {
        return Err(type_already_exists(domain_name));
    }

    let type_name = stmt
        .typeName
        .expect("CreateDomainStmt.typeName")
        .as_type_name()
        .expect("TypeName");
    let typ_ndims = type_name.arrayBounds.len() as i32;
    // C typenameType(pstate, ...) applies no typtype gate (typecmds.c:764);
    // the typtype check below owns pseudo-type rejection.
    let (basetypeoid, basetype_mod) =
        parse_utilcmd::typenameTypeIdAndModAllowComposite(mcx, Some(pstate), type_name)?;
    let base = base_type_row(mcx, basetypeoid)?;

    let typtype = base.typtype;
    if typtype != TYPTYPE_BASE
        && typtype != TYPTYPE_COMPOSITE
        && typtype != TYPTYPE_DOMAIN
        && typtype != TYPTYPE_ENUM
        && typtype != TYPTYPE_RANGE
        && typtype != TYPTYPE_MULTIRANGE
    {
        return Err(invalid_base_type(mcx, pstate, type_name)?);
    }

    if aclchk::object_aclcheck(TYPE_RELATION_ID, basetypeoid, user_id, adt_acl::ACL_USAGE)?
        != aclchk::ACLCHECK_OK
    {
        return Err(permission_denied_type(basetypeoid));
    }

    let base_coll = base.typcollation;
    let domaincoll = if let Some(cc) = stmt.collClause {
        let cc = cc.as_collate_clause().expect("CollateClause");
        catalog_namespace::get_collation_oid_list(&cc.collname, false)?
    } else {
        base_coll
    };
    if OidIsValid(domaincoll) && !OidIsValid(base_coll) {
        return Err(domain_err(
            pstate,
            ERRCODE_DATATYPE_MISMATCH,
            &format!(
                "collations are not supported by type {}",
                format_type::format_type_be(basetypeoid)?
            ),
            type_name.location,
        ));
    }

    if base.has_default {
        // unported: DefineDomain inherited base-type typdefault
        return Err(Box::new(
            types_error::PgError::error(
                "CREATE DOMAIN over a type with a default value is not supported yet",
            )
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }

    let mut typ_not_null = false;
    let mut null_defined = false;
    let mut saw_default = false;
    let mut default_value: Option<String> = None;
    let mut default_value_bin: Option<mcx::PgString<'mcx>> = None;
    let mut default_expr_node: Option<types_nodes::Node<'mcx>> = None;
    for cnode in stmt.constraints.iter() {
        if cnode.node_tag() != NodeTag::T_Constraint {
            panic!("unrecognized node type: {:?}", cnode.node_tag());
        }
        let constr = cnode.as_variant::<Constraint>().expect("Constraint");
        match constr.contype {
            ConstrType::CONSTR_DEFAULT => {
                if saw_default {
                    return Err(domain_err(
                        pstate,
                        ERRCODE_SYNTAX_ERROR,
                        "multiple default expressions",
                        constr.location,
                    ));
                }
                saw_default = true;
                if let Some(raw) = constr.raw_expr {
                    let default_expr = tablecmds::cook_default(
                        mcx,
                        pstate,
                        raw,
                        basetypeoid,
                        basetype_mod,
                        domain_name,
                        0,
                        None,
                    )?;
                    // A plain NULL constant is no default; a CoerceToDomain
                    // over a base domain is kept so this default overrides
                    // the base domain's (typecmds.c:864-880).
                    let is_null_const =
                        default_expr.as_const().is_some_and(|c| c.constisnull);
                    if !is_null_const {
                        default_value = Some(ruleutils::deparse_expression_pretty(
                            mcx,
                            default_expr,
                            InvalidOid,
                            false,
                            0,
                        )?);
                        default_value_bin = Some(outfuncs::nodeToString(mcx, default_expr)?);
                        default_expr_node = Some(default_expr);
                    }
                }
            }
            ConstrType::CONSTR_NOTNULL => {
                if null_defined {
                    if !typ_not_null {
                        return Err(domain_err(
                            pstate,
                            ERRCODE_SYNTAX_ERROR,
                            "conflicting NULL/NOT NULL constraints",
                            constr.location,
                        ));
                    }
                    return Err(domain_err(
                        pstate,
                        ERRCODE_INVALID_OBJECT_DEFINITION,
                        "redundant NOT NULL constraint definition",
                        constr.location,
                    ));
                }
                if constr.is_no_inherit {
                    return Err(domain_err(
                        pstate,
                        ERRCODE_INVALID_OBJECT_DEFINITION,
                        "not-null constraints for domains cannot be marked NO INHERIT",
                        constr.location,
                    ));
                }
                typ_not_null = true;
                null_defined = true;
            }
            ConstrType::CONSTR_NULL => {
                if null_defined && typ_not_null {
                    return Err(domain_err(
                        pstate,
                        ERRCODE_SYNTAX_ERROR,
                        "conflicting NULL/NOT NULL constraints",
                        constr.location,
                    ));
                }
                typ_not_null = false;
                null_defined = true;
            }
            ConstrType::CONSTR_CHECK => {
                if constr.is_no_inherit {
                    return Err(domain_err(
                        pstate,
                        ERRCODE_INVALID_OBJECT_DEFINITION,
                        "check constraints for domains cannot be marked NO INHERIT",
                        constr.location,
                    ));
                }
            }
            ConstrType::CONSTR_UNIQUE => {
                return Err(domain_err(
                    pstate,
                    ERRCODE_SYNTAX_ERROR,
                    "unique constraints not possible for domains",
                    constr.location,
                ))
            }
            ConstrType::CONSTR_PRIMARY => {
                return Err(domain_err(
                    pstate,
                    ERRCODE_SYNTAX_ERROR,
                    "primary key constraints not possible for domains",
                    constr.location,
                ))
            }
            ConstrType::CONSTR_EXCLUSION => {
                return Err(domain_err(
                    pstate,
                    ERRCODE_SYNTAX_ERROR,
                    "exclusion constraints not possible for domains",
                    constr.location,
                ))
            }
            ConstrType::CONSTR_FOREIGN => {
                return Err(domain_err(
                    pstate,
                    ERRCODE_SYNTAX_ERROR,
                    "foreign key constraints not possible for domains",
                    constr.location,
                ))
            }
            ConstrType::CONSTR_ATTR_DEFERRABLE
            | ConstrType::CONSTR_ATTR_NOT_DEFERRABLE
            | ConstrType::CONSTR_ATTR_DEFERRED
            | ConstrType::CONSTR_ATTR_IMMEDIATE => {
                return Err(domain_err(
                    pstate,
                    ERRCODE_FEATURE_NOT_SUPPORTED,
                    "specifying constraint deferrability not supported for domains",
                    constr.location,
                ))
            }
            ConstrType::CONSTR_GENERATED | ConstrType::CONSTR_IDENTITY => {
                return Err(domain_err(
                    pstate,
                    ERRCODE_FEATURE_NOT_SUPPORTED,
                    "specifying GENERATED not supported for domains",
                    constr.location,
                ))
            }
            ConstrType::CONSTR_ATTR_ENFORCED | ConstrType::CONSTR_ATTR_NOT_ENFORCED => {
                return Err(domain_err(
                    pstate,
                    ERRCODE_INVALID_OBJECT_DEFINITION,
                    "specifying constraint enforceability not supported for domains",
                    constr.location,
                ))
            }
        }
    }

    let domain_array_oid = pg_type::AssignTypeArrayOid(mcx)?;

    let address = pg_type::TypeCreate(
        mcx,
        &TypeCreateParams {
            newTypeOid: InvalidOid,
            typeName: domain_name,
            typeNamespace: domain_namespace,
            relationOid: InvalidOid,
            relationKind: 0,
            ownerId: user_id,
            internalSize: base.typlen,
            typeType: TYPTYPE_DOMAIN,
            typeCategory: base.typcategory,
            typePreferred: false,
            typDelim: base.typdelim,
            inputProcedure: F_DOMAIN_IN,
            outputProcedure: base.typoutput,
            receiveProcedure: F_DOMAIN_RECV,
            sendProcedure: base.typsend,
            typmodinProcedure: InvalidOid,
            typmodoutProcedure: InvalidOid,
            analyzeProcedure: base.typanalyze,
            subscriptProcedure: InvalidOid,
            elementType: InvalidOid,
            isImplicitArray: false,
            arrayType: domain_array_oid,
            baseType: basetypeoid,
            passedByValue: base.typbyval,
            alignment: base.typalign,
            storage: base.typstorage,
            typeMod: basetype_mod,
            typNDims: typ_ndims,
            typeNotNull: typ_not_null,
            typeCollation: domaincoll,
            defaultValue: default_value.as_deref(),
            defaultTypeBin: default_value_bin.as_ref().map(|s| s.as_str()),
        },
    )?;
    // C records the typdefaultbin expression's dependencies inside
    // GenerateTypeDependencies (pg_type.c:576-581,710-711); pg_type cannot
    // depend on catalog_dependency, so the same records are written here.
    if let Some(expr) = default_expr_node {
        catalog_dependency::recordDependencyOnExpr(
            mcx,
            &ObjectAddress::set(TYPE_RELATION_ID, address.objectId),
            expr,
            &types_nodes::NodeList::nil(),
            pg_depend::DependencyType::Normal,
        )?;
    }

    let domain_array_name = pg_type::makeArrayTypeName(domain_name, domain_namespace)?;
    let array_alignment = if base.typalign == b'd' as i8 { b'd' as i8 } else { b'i' as i8 };
    pg_type::TypeCreate(
        mcx,
        &TypeCreateParams {
            newTypeOid: domain_array_oid,
            typeName: core::str::from_utf8(domain_array_name.name_str()).expect("array type name"),
            typeNamespace: domain_namespace,
            relationOid: InvalidOid,
            relationKind: 0,
            ownerId: user_id,
            internalSize: -1,
            typeType: TYPTYPE_BASE,
            typeCategory: TYPCATEGORY_ARRAY,
            typePreferred: false,
            typDelim: base.typdelim,
            inputProcedure: pg_type::F_ARRAY_IN,
            outputProcedure: pg_type::F_ARRAY_OUT,
            receiveProcedure: pg_type::F_ARRAY_RECV,
            sendProcedure: pg_type::F_ARRAY_SEND,
            typmodinProcedure: InvalidOid,
            typmodoutProcedure: InvalidOid,
            analyzeProcedure: pg_type::F_ARRAY_TYPANALYZE,
            subscriptProcedure: pg_type::F_ARRAY_SUBSCRIPT_HANDLER,
            elementType: address.objectId,
            isImplicitArray: true,
            arrayType: InvalidOid,
            baseType: InvalidOid,
            passedByValue: false,
            alignment: array_alignment,
            storage: TYPSTORAGE_EXTENDED,
            typeMod: -1,
            typNDims: 0,
            typeNotNull: false,
            typeCollation: domaincoll,
            defaultValue: None,
            defaultTypeBin: None,
        },
    )?;

    for cnode in stmt.constraints.iter() {
        let constr = cnode.as_variant::<Constraint>().expect("Constraint");
        match constr.contype {
            ConstrType::CONSTR_CHECK => {
                domainAddCheckConstraint(
                    mcx,
                    address.objectId,
                    domain_namespace,
                    basetypeoid,
                    basetype_mod,
                    constr,
                    domain_name,
                )?;
            }
            ConstrType::CONSTR_NOTNULL => {
                domainAddNotNullConstraint(
                    mcx,
                    address.objectId,
                    domain_namespace,
                    constr,
                    domain_name,
                )?;
            }
            _ => {}
        }
        xact::CommandCounterIncrement()?;
    }

    Ok(address)
}

// QualifiedNameGetCreationNamespace + the CREATE aclcheck, shared shape with
// the domains lane's DefineDomain preamble.
fn creation_namespace<'mcx, 'a>(
    mcx: Mcx<'mcx>,
    qualified: &types_nodes::NodeList<'a>,
    _what: &str,
) -> PgResult<(Oid, &'a str)> {
    let mut names: [&str; 4] = [""; 4];
    let nnames = qualified.len();
    assert!((1..=3).contains(&nnames), "improper qualified name");
    for (i, n) in qualified.iter().enumerate() {
        names[i] = n.as_string().expect("qualified name").sval;
    }
    let (namespace, name) =
        catalog_namespace::QualifiedNameGetCreationNamespace(mcx, &names[..nnames])?;
    if aclchk::object_aclcheck(
        NAMESPACE_RELATION_ID,
        namespace,
        miscinit::GetUserId(),
        adt_acl::ACL_CREATE,
    )? != aclchk::ACLCHECK_OK
    {
        return Err(permission_denied_schema(namespace)?);
    }
    Ok((namespace, name))
}

const TYPALIGN_DOUBLE: i8 = b'd' as i8;
const TYPALIGN_SHORT: i8 = b's' as i8;
const TYPALIGN_CHAR: i8 = b'c' as i8;
const TYPSTORAGE_EXTERNAL: i8 = b'e' as i8;
const TYPSTORAGE_MAIN: i8 = b'm' as i8;
const TYPTYPE_PSEUDO: i8 = b'p' as i8;
const PROVOLATILE_VOLATILE: i8 = b'v' as i8;
const CSTRINGARRAYOID: Oid = 1263;

#[track_caller]
#[cold]
#[inline(never)]
fn objdef_err(msg: String) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION))
}

#[track_caller]
#[cold]
#[inline(never)]
fn param_err(msg: String) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE))
}

fn function_does_not_exist<'mcx>(
    mcx: Mcx<'mcx>,
    procname: &types_nodes::NodeList<'mcx>,
    argtypes: &[Oid],
) -> PgResult<Box<PgError>> {
    let mut sig = commands_define::NameListToString(mcx, procname)?.as_str().to_string();
    sig.push('(');
    for (i, &t) in argtypes.iter().enumerate() {
        if i > 0 {
            sig.push_str(", ");
        }
        sig.push_str(&format_type::format_type_be(t)?);
    }
    sig.push(')');
    Ok(Box::new(
        PgError::new(ERROR, format!("function {sig} does not exist"))
            .with_sqlstate(types_error::ERRCODE_UNDEFINED_FUNCTION),
    ))
}

fn volatile_warning<'mcx>(
    mcx: Mcx<'mcx>,
    what: &str,
    procname: &types_nodes::NodeList<'mcx>,
) -> PgResult<()> {
    let name = commands_define::NameListToString(mcx, procname)?;
    elog::ereport(types_error::WARNING)
        .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
        .errmsg(format!("type {} function {} should not be volatile", what, name.as_str()))
        .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "DefineType"))
}

fn io_func_rettype_check<'mcx>(
    mcx: Mcx<'mcx>,
    proc_oid: Oid,
    want: Oid,
    what: &str,
    want_name: Option<&str>,
    procname: &types_nodes::NodeList<'mcx>,
) -> PgResult<()> {
    if lsyscache::get_func_rettype(proc_oid)? != want {
        let name = commands_define::NameListToString(mcx, procname)?;
        let tname = match want_name {
            Some(s) => s.to_string(),
            None => format_type::format_type_be(want)?,
        };
        return Err(objdef_err(format!(
            "{what} function {} must return type {tname}",
            name.as_str()
        )));
    }
    Ok(())
}

fn findTypeInputFunction<'mcx>(
    mcx: Mcx<'mcx>,
    procname: &types_nodes::NodeList<'mcx>,
    typeOid: Oid,
    receive: bool,
) -> PgResult<Oid> {
    let first = if receive { types_core::INTERNALOID } else { types_core::CSTRINGOID };
    let arglist = [first, types_core::OIDOID, types_core::INT4OID];
    let proc1 = parse_func::LookupFuncName(procname, 1, &arglist, true)?;
    let proc3 = parse_func::LookupFuncName(procname, 3, &arglist, true)?;
    let what = if receive { "receive" } else { "input" };
    let proc_oid = if proc1 != InvalidOid {
        if proc3 != InvalidOid {
            let name = commands_define::NameListToString(mcx, procname)?;
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!("type {what} function {} has multiple matches", name.as_str()),
                )
                .with_sqlstate(types_error::ERRCODE_AMBIGUOUS_FUNCTION),
            ));
        }
        proc1
    } else {
        if proc3 == InvalidOid {
            return Err(function_does_not_exist(mcx, procname, &arglist[..1])?);
        }
        proc3
    };
    io_func_rettype_check(mcx, proc_oid, typeOid, &format!("type {what}"), None, procname)?;
    if lsyscache::func_volatile(proc_oid)? == PROVOLATILE_VOLATILE {
        volatile_warning(mcx, what, procname)?;
    }
    Ok(proc_oid)
}

fn findTypeOutputFunction<'mcx>(
    mcx: Mcx<'mcx>,
    procname: &types_nodes::NodeList<'mcx>,
    typeOid: Oid,
    send: bool,
) -> PgResult<Oid> {
    let arglist = [typeOid];
    let proc_oid = parse_func::LookupFuncName(procname, 1, &arglist, true)?;
    if proc_oid == InvalidOid {
        return Err(function_does_not_exist(mcx, procname, &arglist)?);
    }
    let (what, want, want_name) = if send {
        ("send", types_core::BYTEAOID, "bytea")
    } else {
        ("output", types_core::CSTRINGOID, "cstring")
    };
    io_func_rettype_check(mcx, proc_oid, want, &format!("type {what}"), Some(want_name), procname)?;
    if lsyscache::func_volatile(proc_oid)? == PROVOLATILE_VOLATILE {
        volatile_warning(mcx, what, procname)?;
    }
    Ok(proc_oid)
}

fn findTypeTypmodFunction<'mcx>(
    mcx: Mcx<'mcx>,
    procname: &types_nodes::NodeList<'mcx>,
    output: bool,
) -> PgResult<Oid> {
    let arglist = [if output { types_core::INT4OID } else { CSTRINGARRAYOID }];
    let proc_oid = parse_func::LookupFuncName(procname, 1, &arglist, true)?;
    if proc_oid == InvalidOid {
        return Err(function_does_not_exist(mcx, procname, &arglist)?);
    }
    let (tag, want, want_name, warnwhat) = if output {
        ("typmod_out", types_core::CSTRINGOID, "cstring", "modifier output")
    } else {
        ("typmod_in", types_core::INT4OID, "integer", "modifier input")
    };
    io_func_rettype_check(mcx, proc_oid, want, tag, Some(want_name), procname)?;
    if lsyscache::func_volatile(proc_oid)? == PROVOLATILE_VOLATILE {
        volatile_warning(mcx, warnwhat, procname)?;
    }
    Ok(proc_oid)
}

fn findTypeAnalyzeFunction<'mcx>(
    mcx: Mcx<'mcx>,
    procname: &types_nodes::NodeList<'mcx>,
) -> PgResult<Oid> {
    let arglist = [types_core::INTERNALOID];
    let proc_oid = parse_func::LookupFuncName(procname, 1, &arglist, true)?;
    if proc_oid == InvalidOid {
        return Err(function_does_not_exist(mcx, procname, &arglist)?);
    }
    io_func_rettype_check(
        mcx,
        proc_oid,
        types_core::BOOLOID,
        "type analyze",
        Some("boolean"),
        procname,
    )?;
    Ok(proc_oid)
}

fn findTypeSubscriptingFunction<'mcx>(
    mcx: Mcx<'mcx>,
    procname: &types_nodes::NodeList<'mcx>,
) -> PgResult<Oid> {
    let arglist = [types_core::INTERNALOID];
    let proc_oid = parse_func::LookupFuncName(procname, 1, &arglist, true)?;
    if proc_oid == InvalidOid {
        return Err(function_does_not_exist(mcx, procname, &arglist)?);
    }
    io_func_rettype_check(
        mcx,
        proc_oid,
        types_core::INTERNALOID,
        "type subscripting",
        Some("internal"),
        procname,
    )?;
    if proc_oid == pg_type::F_ARRAY_SUBSCRIPT_HANDLER {
        let name = commands_define::NameListToString(mcx, procname)?;
        return Err(objdef_err(format!(
            "user-defined types cannot use subscripting function {}",
            name.as_str()
        )));
    }
    Ok(proc_oid)
}

// DefineType (typecmds.c): CREATE TYPE base-type arm + the shell two-phase
// dance.
pub fn DefineType<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    names: &types_nodes::NodeList<'mcx>,
    parameters: &types_nodes::NodeList<'mcx>,
) -> PgResult<ObjectAddress> {
    use types_nodes::parsenodes::DefElem;

    if !superuser::superuser()? {
        return Err(Box::new(
            PgError::new(ERROR, "must be superuser to create a base type".to_string())
                .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }

    let mut buf = [""; 4];
    let nnames = names.len();
    assert!((1..=3).contains(&nnames), "improper qualified name");
    for (i, n) in names.iter().enumerate() {
        buf[i] = n.as_string().expect("qualified name").sval;
    }
    let (typeNamespace, typeName) =
        catalog_namespace::QualifiedNameGetCreationNamespace(mcx, &buf[..nnames])?;

    let mut typoid = syscache_seams::lookup_pg_type_oid_by_name::call(typeName, typeNamespace)?;
    if typoid != InvalidOid && lsyscache::get_typisdefined(typoid)? {
        if pg_type::moveArrayTypeName(mcx, typoid, typeName, typeNamespace)? {
            typoid = InvalidOid;
        } else {
            return Err(type_already_exists(typeName));
        }
    }

    if parameters.is_nil() {
        if typoid != InvalidOid {
            return Err(type_already_exists(typeName));
        }
        return pg_type::TypeShellMake(mcx, typeName, typeNamespace, miscinit::GetUserId());
    }

    if typoid == InvalidOid {
        return Err(Box::new(
            PgError::new(ERROR, format!("type \"{typeName}\" does not exist"))
                .with_sqlstate(ERRCODE_DUPLICATE_OBJECT)
                .with_hint(
                    "Create the type as a shell type, then create its I/O functions, \
                     then do a full CREATE TYPE.",
                ),
        ));
    }

    let mut likeTypeEl: Option<&DefElem<'mcx>> = None;
    let mut internalLengthEl: Option<&DefElem<'mcx>> = None;
    let mut inputNameEl: Option<&DefElem<'mcx>> = None;
    let mut outputNameEl: Option<&DefElem<'mcx>> = None;
    let mut receiveNameEl: Option<&DefElem<'mcx>> = None;
    let mut sendNameEl: Option<&DefElem<'mcx>> = None;
    let mut typmodinNameEl: Option<&DefElem<'mcx>> = None;
    let mut typmodoutNameEl: Option<&DefElem<'mcx>> = None;
    let mut analyzeNameEl: Option<&DefElem<'mcx>> = None;
    let mut subscriptNameEl: Option<&DefElem<'mcx>> = None;
    let mut categoryEl: Option<&DefElem<'mcx>> = None;
    let mut preferredEl: Option<&DefElem<'mcx>> = None;
    let mut delimiterEl: Option<&DefElem<'mcx>> = None;
    let mut elemTypeEl: Option<&DefElem<'mcx>> = None;
    let mut defaultValueEl: Option<&DefElem<'mcx>> = None;
    let mut byValueEl: Option<&DefElem<'mcx>> = None;
    let mut alignmentEl: Option<&DefElem<'mcx>> = None;
    let mut storageEl: Option<&DefElem<'mcx>> = None;
    let mut collatableEl: Option<&DefElem<'mcx>> = None;

    for n in parameters.iter() {
        let defel = n.as_def_elem().expect("CREATE TYPE definition: DefElem list");
        let slot: &mut Option<&DefElem<'mcx>> = match defel.defname.unwrap_or("") {
            "like" => &mut likeTypeEl,
            "internallength" => &mut internalLengthEl,
            "input" => &mut inputNameEl,
            "output" => &mut outputNameEl,
            "receive" => &mut receiveNameEl,
            "send" => &mut sendNameEl,
            "typmod_in" => &mut typmodinNameEl,
            "typmod_out" => &mut typmodoutNameEl,
            "analyze" | "analyse" => &mut analyzeNameEl,
            "subscript" => &mut subscriptNameEl,
            "category" => &mut categoryEl,
            "preferred" => &mut preferredEl,
            "delimiter" => &mut delimiterEl,
            "element" => &mut elemTypeEl,
            "default" => &mut defaultValueEl,
            "passedbyvalue" => &mut byValueEl,
            "alignment" => &mut alignmentEl,
            "storage" => &mut storageEl,
            "collatable" => &mut collatableEl,
            other => {
                // C: WARNING, not ERROR, for historical backwards-compatibility.
                let pos = parser_small1::parser_errposition(
                    pstate,
                    defel.location,
                    mbutils::GetDatabaseEncoding(),
                );
                elog::ereport(types_error::WARNING)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("type attribute \"{other}\" not recognized"))
                    .errposition(pos)
                    .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "DefineType"))?;
                continue;
            }
        };
        if slot.is_some() {
            return Err(domain_err(
                pstate,
                ERRCODE_SYNTAX_ERROR,
                "conflicting or redundant options",
                defel.location,
            ));
        }
        *slot = Some(defel);
    }

    let mut internalLength: i16 = -1;
    let mut byValue = false;
    let mut alignment = TYPALIGN_INT;
    let mut storage = TYPSTORAGE_PLAIN;
    if let Some(defel) = likeTypeEl {
        let tn = commands_define::defGetTypeName(mcx, defel)?;
        let (likeoid, _typmod) = parse_utilcmd::typenameTypeIdAndMod(mcx, Some(pstate), tn)?;
        if !lsyscache::get_typisdefined(likeoid)? {
            let name = type_name_to_string(mcx, tn)?;
            return Err(Box::new(
                PgError::new(ERROR, format!("type \"{}\" is only a shell", name.as_str()))
                    .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
            ));
        }
        let like = base_type_row(mcx, likeoid)?;
        internalLength = like.typlen;
        byValue = like.typbyval;
        alignment = like.typalign;
        storage = like.typstorage;
    }
    if let Some(defel) = internalLengthEl {
        internalLength = commands_define::defGetTypeLength(defel)? as i16;
    }
    let inputName = match inputNameEl {
        Some(d) => Some(commands_define::defGetQualifiedName(mcx, d)?),
        None => None,
    };
    let outputName = match outputNameEl {
        Some(d) => Some(commands_define::defGetQualifiedName(mcx, d)?),
        None => None,
    };
    let receiveName = match receiveNameEl {
        Some(d) => Some(commands_define::defGetQualifiedName(mcx, d)?),
        None => None,
    };
    let sendName = match sendNameEl {
        Some(d) => Some(commands_define::defGetQualifiedName(mcx, d)?),
        None => None,
    };
    let typmodinName = match typmodinNameEl {
        Some(d) => Some(commands_define::defGetQualifiedName(mcx, d)?),
        None => None,
    };
    let typmodoutName = match typmodoutNameEl {
        Some(d) => Some(commands_define::defGetQualifiedName(mcx, d)?),
        None => None,
    };
    let analyzeName = match analyzeNameEl {
        Some(d) => Some(commands_define::defGetQualifiedName(mcx, d)?),
        None => None,
    };
    let subscriptName = match subscriptNameEl {
        Some(d) => Some(commands_define::defGetQualifiedName(mcx, d)?),
        None => None,
    };
    let mut category = pg_type::TYPCATEGORY_USER;
    if let Some(defel) = categoryEl {
        let p = commands_define::defGetString(mcx, defel)?;
        let c = p.as_bytes().first().copied().unwrap_or(0);
        if !(32..=126).contains(&c) {
            return Err(param_err(format!("invalid type category \"{p}\": must be simple ASCII")));
        }
        category = c as i8;
    }
    let preferred = match preferredEl {
        Some(d) => commands_define::defGetBoolean(d)?,
        None => false,
    };
    let mut delimiter = DEFAULT_TYPDELIM;
    if let Some(defel) = delimiterEl {
        let p = commands_define::defGetString(mcx, defel)?;
        delimiter = p.as_bytes().first().copied().unwrap_or(0) as i8;
    }
    let mut elemType = InvalidOid;
    if let Some(defel) = elemTypeEl {
        elemType =
            parse_utilcmd::LookupTypeNameOid(mcx, commands_define::defGetTypeName(mcx, defel)?)?;
        if lsyscache::get_typtype(elemType)? == TYPTYPE_PSEUDO {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "array element type cannot be {}",
                        format_type::format_type_be(elemType)?
                    ),
                )
                .with_sqlstate(ERRCODE_DATATYPE_MISMATCH),
            ));
        }
    }
    let defaultValue = match defaultValueEl {
        Some(d) => Some(commands_define::defGetString(mcx, d)?),
        None => None,
    };
    if let Some(defel) = byValueEl {
        byValue = commands_define::defGetBoolean(defel)?;
    }
    if let Some(defel) = alignmentEl {
        let a = commands_define::defGetString(mcx, defel)?;
        alignment = if a.eq_ignore_ascii_case("double")
            || a.eq_ignore_ascii_case("float8")
            || a.eq_ignore_ascii_case("pg_catalog.float8")
        {
            TYPALIGN_DOUBLE
        } else if a.eq_ignore_ascii_case("int4") || a.eq_ignore_ascii_case("pg_catalog.int4") {
            TYPALIGN_INT
        } else if a.eq_ignore_ascii_case("int2") || a.eq_ignore_ascii_case("pg_catalog.int2") {
            TYPALIGN_SHORT
        } else if a.eq_ignore_ascii_case("char") || a.eq_ignore_ascii_case("pg_catalog.bpchar") {
            TYPALIGN_CHAR
        } else {
            return Err(param_err(format!("alignment \"{a}\" not recognized")));
        };
    }
    if let Some(defel) = storageEl {
        let a = commands_define::defGetString(mcx, defel)?;
        storage = if a.eq_ignore_ascii_case("plain") {
            TYPSTORAGE_PLAIN
        } else if a.eq_ignore_ascii_case("external") {
            TYPSTORAGE_EXTERNAL
        } else if a.eq_ignore_ascii_case("extended") {
            TYPSTORAGE_EXTENDED
        } else if a.eq_ignore_ascii_case("main") {
            TYPSTORAGE_MAIN
        } else {
            return Err(param_err(format!("storage \"{a}\" not recognized")));
        };
    }
    let collation = match collatableEl {
        Some(d) => {
            if commands_define::defGetBoolean(d)? {
                types_core::DEFAULT_COLLATION_OID
            } else {
                InvalidOid
            }
        }
        None => InvalidOid,
    };

    let Some(inputName) = inputName else {
        return Err(objdef_err("type input function must be specified".into()));
    };
    let Some(outputName) = outputName else {
        return Err(objdef_err("type output function must be specified".into()));
    };
    if typmodinName.is_none() && typmodoutName.is_some() {
        return Err(objdef_err(
            "type modifier output function is useless without a type modifier input function"
                .into(),
        ));
    }

    let inputOid = findTypeInputFunction(mcx, inputName, typoid, false)?;
    let outputOid = findTypeOutputFunction(mcx, outputName, typoid, false)?;
    let receiveOid = match receiveName {
        Some(n) => findTypeInputFunction(mcx, n, typoid, true)?,
        None => InvalidOid,
    };
    let sendOid = match sendName {
        Some(n) => findTypeOutputFunction(mcx, n, typoid, true)?,
        None => InvalidOid,
    };
    let typmodinOid = match typmodinName {
        Some(n) => findTypeTypmodFunction(mcx, n, false)?,
        None => InvalidOid,
    };
    let typmodoutOid = match typmodoutName {
        Some(n) => findTypeTypmodFunction(mcx, n, true)?,
        None => InvalidOid,
    };
    let analyzeOid = match analyzeName {
        Some(n) => findTypeAnalyzeFunction(mcx, n)?,
        None => InvalidOid,
    };
    let subscriptOid = match subscriptName {
        Some(n) => findTypeSubscriptingFunction(mcx, n)?,
        None => {
            if elemType != InvalidOid {
                if internalLength > 0 && !byValue && lsyscache::get_typlen(elemType)? > 0 {
                    pg_type::F_RAW_ARRAY_SUBSCRIPT_HANDLER
                } else {
                    return Err(param_err(
                        "element type cannot be specified without a subscripting function".into(),
                    ));
                }
            } else {
                InvalidOid
            }
        }
    };

    let array_oid = pg_type::AssignTypeArrayOid(mcx)?;
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
            internalSize: internalLength,
            typeType: TYPTYPE_BASE,
            typeCategory: category,
            typePreferred: preferred,
            typDelim: delimiter,
            inputProcedure: inputOid,
            outputProcedure: outputOid,
            receiveProcedure: receiveOid,
            sendProcedure: sendOid,
            typmodinProcedure: typmodinOid,
            typmodoutProcedure: typmodoutOid,
            analyzeProcedure: analyzeOid,
            subscriptProcedure: subscriptOid,
            elementType: elemType,
            isImplicitArray: false,
            arrayType: array_oid,
            baseType: InvalidOid,
            passedByValue: byValue,
            alignment,
            storage,
            typeMod: -1,
            typNDims: 0,
            typeNotNull: false,
            typeCollation: collation,
            defaultValue,
            defaultTypeBin: None,
        },
    )?;
    debug_assert!(typoid == address.objectId);

    let array_name = pg_type::makeArrayTypeName(typeName, typeNamespace)?;
    let array_alignment =
        if alignment == TYPALIGN_DOUBLE { TYPALIGN_DOUBLE } else { TYPALIGN_INT };
    pg_type::TypeCreate(
        mcx,
        &TypeCreateParams {
            newTypeOid: array_oid,
            typeName: core::str::from_utf8(array_name.name_str()).expect("array type name"),
            typeNamespace,
            relationOid: InvalidOid,
            relationKind: 0,
            ownerId: user_id,
            internalSize: -1,
            typeType: TYPTYPE_BASE,
            typeCategory: TYPCATEGORY_ARRAY,
            typePreferred: false,
            typDelim: delimiter,
            inputProcedure: pg_type::F_ARRAY_IN,
            outputProcedure: pg_type::F_ARRAY_OUT,
            receiveProcedure: pg_type::F_ARRAY_RECV,
            sendProcedure: pg_type::F_ARRAY_SEND,
            typmodinProcedure: typmodinOid,
            typmodoutProcedure: typmodoutOid,
            analyzeProcedure: pg_type::F_ARRAY_TYPANALYZE,
            subscriptProcedure: pg_type::F_ARRAY_SUBSCRIPT_HANDLER,
            elementType: address.objectId,
            isImplicitArray: true,
            arrayType: InvalidOid,
            baseType: InvalidOid,
            passedByValue: false,
            alignment: array_alignment,
            storage: TYPSTORAGE_EXTENDED,
            typeMod: -1,
            typNDims: 0,
            typeNotNull: false,
            typeCollation: collation,
            defaultValue: None,
            defaultTypeBin: None,
        },
    )?;

    Ok(address)
}


pub fn DefineEnum<'mcx>(mcx: Mcx<'mcx>, stmt: &CreateEnumStmt<'mcx>) -> PgResult<ObjectAddress> {
    let (enum_namespace, enum_name) = creation_namespace(mcx, &stmt.typeName, "DefineEnum")?;
    let user_id = miscinit::GetUserId();

    let old_type_oid =
        syscache_seams::lookup_pg_type_oid_by_name::call(enum_name, enum_namespace)?;
    if old_type_oid != InvalidOid
        && !pg_type::moveArrayTypeName(mcx, old_type_oid, enum_name, enum_namespace)?
    {
        return Err(type_already_exists(enum_name));
    }

    let enum_array_oid = pg_type::AssignTypeArrayOid(mcx)?;

    let enum_type_addr = pg_type::TypeCreate(
        mcx,
        &TypeCreateParams {
            newTypeOid: InvalidOid,
            typeName: enum_name,
            typeNamespace: enum_namespace,
            relationOid: InvalidOid,
            relationKind: 0,
            ownerId: user_id,
            internalSize: core::mem::size_of::<Oid>() as i16,
            typeType: TYPTYPE_ENUM,
            typeCategory: TYPCATEGORY_ENUM,
            typePreferred: false,
            typDelim: DEFAULT_TYPDELIM,
            inputProcedure: F_ENUM_IN,
            outputProcedure: F_ENUM_OUT,
            receiveProcedure: F_ENUM_RECV,
            sendProcedure: F_ENUM_SEND,
            typmodinProcedure: InvalidOid,
            typmodoutProcedure: InvalidOid,
            analyzeProcedure: InvalidOid,
            subscriptProcedure: InvalidOid,
            elementType: InvalidOid,
            isImplicitArray: false,
            arrayType: enum_array_oid,
            baseType: InvalidOid,
            passedByValue: true,
            alignment: TYPALIGN_INT,
            storage: TYPSTORAGE_PLAIN,
            typeMod: -1,
            typNDims: 0,
            typeNotNull: false,
            typeCollation: InvalidOid,
            defaultValue: None,
            defaultTypeBin: None,
        },
    )?;

    let mut vals: PgVec<'mcx, &str> = PgVec::with_capacity_in(stmt.vals.len(), mcx);
    for v in stmt.vals.iter() {
        vals.push(v.as_string().expect("enum_val_list String").sval);
    }
    pg_enum::EnumValuesCreate(mcx, enum_type_addr.objectId, &vals)?;

    let enum_array_name = pg_type::makeArrayTypeName(enum_name, enum_namespace)?;
    pg_type::TypeCreate(
        mcx,
        &TypeCreateParams {
            newTypeOid: enum_array_oid,
            typeName: core::str::from_utf8(enum_array_name.name_str()).expect("array type name"),
            typeNamespace: enum_namespace,
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
            elementType: enum_type_addr.objectId,
            isImplicitArray: true,
            arrayType: InvalidOid,
            baseType: InvalidOid,
            passedByValue: false,
            alignment: TYPALIGN_INT,
            storage: TYPSTORAGE_EXTENDED,
            typeMod: -1,
            typNDims: 0,
            typeNotNull: false,
            typeCollation: InvalidOid,
            defaultValue: None,
            defaultTypeBin: None,
        },
    )?;

    Ok(enum_type_addr)
}

pub fn AlterEnum<'mcx>(mcx: Mcx<'mcx>, stmt: &AlterEnumStmt<'mcx>) -> PgResult<ObjectAddress> {
    // C shares the list pointer (makeTypeNameFromNameList); cold DDL copy.
    let typename = TypeName {
        names: stmt.typeName.clone_in(mcx)?,
        typemod: -1,
        location: -1,
        ..Default::default()
    };
    let (enum_type_oid, _typmod) = parse_utilcmd::typenameTypeIdAndMod(mcx, None, &typename)?;

    checkEnumOwner(enum_type_oid)?;

    match stmt.oldVal {
        Some(old_val) => {
            pg_enum::RenameEnumLabel(
                mcx,
                enum_type_oid,
                old_val,
                stmt.newVal.expect("AlterEnumStmt.newVal"),
            )?;
        }
        None => {
            pg_enum::AddEnumLabel(
                mcx,
                enum_type_oid,
                stmt.newVal.expect("AlterEnumStmt.newVal"),
                stmt.newValNeighbor,
                stmt.newValIsAfter,
                stmt.skipIfNewValExists,
            )?;
        }
    }

    Ok(ObjectAddress::set(TYPE_RELATION_ID, enum_type_oid))
}

fn checkEnumOwner(type_oid: Oid) -> PgResult<()> {
    let typtype = syscache_seams::pg_type_typtype::call(type_oid)?
        .unwrap_or_else(|| panic!("cache lookup failed for type {type_oid}"));
    if typtype != TYPTYPE_ENUM {
        return Err(not_an_enum(type_oid)?);
    }
    if !aclchk::object_ownercheck(TYPE_RELATION_ID, type_oid, miscinit::GetUserId())? {
        return Err(alter::must_be_owner_of_type(type_oid)?);
    }
    Ok(())
}

fn constraint_name<'mcx>(
    mcx: Mcx<'mcx>,
    domain_oid: Oid,
    domain_namespace: Oid,
    domain_name: &str,
    constr: &Constraint<'_>,
    label: &str,
) -> PgResult<mcx::PgString<'mcx>> {
    match constr.conname {
        Some(name) => {
            if pg_constraint::ConstraintNameIsUsed(
                mcx,
                pg_constraint::ConstraintCategory::Domain,
                domain_oid,
                name,
            )? {
                return Err(constraint_already_exists(name, domain_name));
            }
            mcx::PgString::from_str_in(name, mcx)
        }
        None => {
            pg_constraint::ChooseConstraintName(mcx, domain_name, None, label, domain_namespace, &[])
        }
    }
}

pub(crate) fn domainAddCheckConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    domain_oid: Oid,
    domain_namespace: Oid,
    base_type_oid: Oid,
    typ_mod: i32,
    constr: &Constraint<'mcx>,
    domain_name: &str,
) -> PgResult<(Oid, mcx::PgString<'mcx>)> {
    debug_assert!(constr.contype == ConstrType::CONSTR_CHECK);
    let conname =
        constraint_name(mcx, domain_oid, domain_namespace, domain_name, constr, "check")?;

    let mut cpstate = make_parsestate(mcx, None);
    cpstate.p_pre_columnref_hook = PreColumnRefHook::DomainValue(CoerceToDomainValue {
        typeId: base_type_oid,
        typeMod: typ_mod,
        collation: lsyscache::get_typcollation(base_type_oid)?,
        location: -1,
    });

    let raw_expr = constr.raw_expr.expect("CHECK constraint raw_expr");
    let expr = parse_expr::transformExpr(
        mcx,
        &mut cpstate,
        raw_expr,
        ParseExprKind::EXPR_KIND_DOMAIN_CHECK,
    )?;
    let expr = coerce::coerce_to_boolean(
        mcx,
        &cpstate,
        expr,
        parse_expr::expr_type(expr),
        parse_expr::expr_location(expr),
        "CHECK",
    )?;
    parse_collate::assign_expr_collations(mcx, &cpstate, expr)?;

    if !cpstate.p_rtable.is_nil() || vars::contain_var_clause(expr)? {
        return Err(table_refs_in_domain_check());
    }

    let ccbin = outfuncs::nodeToString(mcx, expr)?;
    let mut entry = pg_constraint::ConstraintEntry::base(
        conname.as_str(),
        domain_namespace,
        pg_constraint::CONSTRAINT_CHECK,
        InvalidOid,
    );
    entry.is_validated = !constr.skip_validation;
    entry.domain_id = domain_oid;
    entry.conbin = Some(ccbin.as_str());
    // C passes the expr tree too (typecmds.c domainAddCheckConstraint), so
    // CreateConstraintEntry records column deps for (VALUE).field references
    // — ALTER TYPE ... ALTER/DROP ATTRIBUTE must see this constraint.
    entry.con_expr = Some(expr);
    let ccoid = pg_constraint::CreateConstraintEntry(mcx, &entry)?;
    parser_small1::free_parsestate(cpstate)?;
    Ok((ccoid, ccbin))
}

pub(crate) fn domainAddNotNullConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    domain_oid: Oid,
    domain_namespace: Oid,
    constr: &Constraint<'_>,
    domain_name: &str,
) -> PgResult<Oid> {
    debug_assert!(constr.contype == ConstrType::CONSTR_NOTNULL);
    let conname =
        constraint_name(mcx, domain_oid, domain_namespace, domain_name, constr, "not_null")?;
    let mut entry = pg_constraint::ConstraintEntry::base(
        conname.as_str(),
        domain_namespace,
        pg_constraint::CONSTRAINT_NOTNULL,
        InvalidOid,
    );
    entry.is_validated = !constr.skip_validation;
    entry.domain_id = domain_oid;
    pg_constraint::CreateConstraintEntry(mcx, &entry)
}

#[track_caller]
#[cold]
#[inline(never)]
fn domain_err(
    pstate: &ParseState<'_, '_>,
    sqlstate: types_error::SqlState,
    msg: &str,
    location: i32,
) -> Box<PgError> {
    let pos = parser_small1::parser_errposition(pstate, location, mbutils::GetDatabaseEncoding());
    Box::new(PgError::new(ERROR, msg.to_string()).with_sqlstate(sqlstate).with_cursor_position(pos))
}

#[track_caller]
#[cold]
#[inline(never)]
fn table_refs_in_domain_check() -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, "cannot use table references in domain check constraint".to_string())
            .with_sqlstate(ERRCODE_INVALID_COLUMN_REFERENCE),
    )
}

#[cold]
#[inline(never)]
fn permission_denied_schema(nsp: Oid) -> PgResult<Box<PgError>> {
    let name = syscache_seams::pg_namespace_nspname::call(nsp)?
        .map(|n| String::from_utf8_lossy(n.name_str()).into_owned())
        .unwrap_or_else(|| nsp.to_string());
    Ok(Box::new(
        PgError::new(ERROR, format!("permission denied for schema {name}"))
            .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
    ))
}

#[track_caller]
#[cold]
#[inline(never)]
fn permission_denied_type(typeoid: Oid) -> Box<PgError> {
    let name = format_type::format_type_be(typeoid).unwrap_or_else(|_| "???".into());
    Box::new(
        PgError::new(ERROR, format!("permission denied for type {name}"))
            .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn type_already_exists(name: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, format!("type \"{name}\" already exists"))
            .with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn constraint_already_exists(conname: &str, domain_name: &str) -> Box<PgError> {
    Box::new(
        PgError::new(
            ERROR,
            format!("constraint \"{conname}\" for domain \"{domain_name}\" already exists"),
        )
        .with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
    )
}

#[cold]
#[inline(never)]
fn invalid_base_type<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, '_>,
    tn: &TypeName<'_>,
) -> PgResult<Box<PgError>> {
    let name = type_name_to_string(mcx, tn)?;
    let pos =
        parser_small1::parser_errposition(pstate, tn.location, mbutils::GetDatabaseEncoding());
    Ok(Box::new(
        PgError::new(ERROR, format!("\"{}\" is not a valid base type for a domain", name.as_str()))
            .with_sqlstate(ERRCODE_DATATYPE_MISMATCH)
            .with_cursor_position(pos),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;
    use types_nodes::{Node, NodeList};

    #[test]
    fn type_name_to_string_joins_qualified_names() {
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let names = NodeList::make2(
            mcx,
            Node::mk_string(mcx, "pg_catalog").unwrap(),
            Node::mk_string(mcx, "int4").unwrap(),
        )
        .unwrap();
        let tn = TypeName { names, ..Default::default() };
        assert_eq!(type_name_to_string(mcx, &tn).unwrap().as_str(), "pg_catalog.int4");
    }

    #[test]
    fn domain_err_carries_sqlstate_without_sourcetext() {
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let pstate = make_parsestate(mcx, None);
        let e = domain_err(&pstate, ERRCODE_SYNTAX_ERROR, "multiple default expressions", 10);
        assert_eq!(e.sqlstate(), ERRCODE_SYNTAX_ERROR);
        assert_eq!(e.message(), "multiple default expressions");
    }
}

#[cold]
#[inline(never)]
fn not_an_enum(type_oid: Oid) -> PgResult<Box<PgError>> {
    let name = format_type::format_type_be(type_oid)?;
    Ok(Box::new(
        PgError::new(ERROR, format!("{name} is not an enum"))
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
    ))
}

/// `DefineCompositeType` (typecmds.c): CREATE TYPE name AS (coldeflist).
pub fn DefineCompositeType<'mcx>(
    mcx: Mcx<'mcx>,
    typevar: &'mcx types_nodes::primnodes::RangeVar<'mcx>,
    coldeflist: types_nodes::NodeList<'mcx>,
    query_string: &str,
) -> PgResult<ObjectAddress> {
    let relname = typevar.relname.expect("RangeVar.relname");
    let creation_rv = rel_vocab::RangeVar {
        catalogname: typevar.catalogname,
        schemaname: typevar.schemaname,
        relname,
        inh: typevar.inh,
        relpersistence: typevar.relpersistence,
        location: typevar.location,
    };
    // typecmds.c:2582: resolve + ACL_CREATE + namespace lock, then C's explicit
    // (idempotent) RangeVarAdjustRelationPersistence call, which the helper
    // already applied to the returned persistence.
    let (type_namespace, _existing_relid, _relpersistence) =
        catalog_namespace::RangeVarGetAndCheckCreationNamespace(
            mcx,
            &creation_rv,
            types_rel::NoLock,
            false,
        )?;
    let old_type_oid =
        syscache_seams::lookup_pg_type_oid_by_name::call(relname, type_namespace)?;
    if old_type_oid != InvalidOid
        && !pg_type::moveArrayTypeName(mcx, old_type_oid, relname, type_namespace)?
    {
        return Err(Box::new(
            PgError::new(ERROR, format!("type \"{relname}\" already exists"))
                .with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
        ));
    }
    let create_stmt = types_nodes::rawnodes::CreateStmt {
        relation: Some(typevar),
        tableElts: coldeflist,
        ..Default::default()
    };
    let relid = tablecmds::DefineRelation(
        mcx,
        &create_stmt,
        types_rel::RELKIND_COMPOSITE_TYPE,
        InvalidOid,
        query_string,
    )?;
    Ok(ObjectAddress::set(types_core::RELATION_RELATION_ID, relid))
}
