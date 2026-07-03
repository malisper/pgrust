// typecmds.c DefineDomain lane (CREATE DOMAIN with NOT NULL/CHECK/NULL
// constraints). ALTER DOMAIN, COLLATE, DEFAULT expressions, and inherited
// base-type defaults are loud.
#![allow(non_snake_case)]

use datum::Datum;
use mcx::Mcx;
use parser_small1::{make_parsestate, ParseExprKind, ParseState, PreColumnRefHook};
use types_core::{AttrNumber, InvalidOid, Oid, NAMESPACE_RELATION_ID, TYPE_RELATION_ID};
use types_error::{
    PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_DUPLICATE_OBJECT,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_COLUMN_REFERENCE, ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_SYNTAX_ERROR,
    ERROR,
};
use types_nodes::primnodes::CoerceToDomainValue;
use types_nodes::rawnodes::{Constraint, ConstrType, CreateDomainStmt, TypeName};
use types_nodes::NodeTag;
use types_rel::AccessShareLock;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

use pg_type::{ObjectAddress, TypeCreateParams, TYPCATEGORY_ARRAY, TYPTYPE_BASE, TYPTYPE_DOMAIN};

const F_DOMAIN_IN: Oid = 2597;
const F_DOMAIN_RECV: Oid = 2598;
const TYPTYPE_COMPOSITE: i8 = b'c' as i8;
const TYPTYPE_ENUM: i8 = b'e' as i8;
const TYPTYPE_RANGE: i8 = b'r' as i8;
const TYPTYPE_MULTIRANGE: i8 = b'm' as i8;
const TYPSTORAGE_EXTENDED: i8 = b'x' as i8;

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("{what} unported — unit backend-commands-typecmds")
}

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

fn type_name_to_string<'mcx>(mcx: Mcx<'mcx>, tn: &TypeName<'_>) -> PgResult<mcx::PgString<'mcx>> {
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
    let (schemaname, domain_name) = catalog_namespace::DeconstructQualifiedName(&names[..nnames])?;
    let domain_namespace = match schemaname {
        Some(schemaname) => catalog_namespace::get_namespace_oid(schemaname, false)?,
        None => {
            let path = catalog_namespace::fetch_search_path(mcx, false)?;
            match path.first() {
                Some(&ns) => ns,
                None => return Err(no_creation_schema()),
            }
        }
    };
    if catalog_namespace::isAnyTempNamespace(domain_namespace)? {
        unported("DefineDomain (typecmds.c): temp-namespace domain creation");
    }

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
        && !pg_type::moveArrayTypeName(old_type_oid, domain_name, domain_namespace)?
    {
        return Err(type_already_exists(domain_name));
    }

    let type_name = stmt
        .typeName
        .expect("CreateDomainStmt.typeName")
        .as_type_name()
        .expect("TypeName");
    let typ_ndims = type_name.arrayBounds.len() as i32;
    let (basetypeoid, basetype_mod) = parse_utilcmd::typenameTypeIdAndMod(mcx, type_name)?;
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
    if stmt.collClause.is_some() {
        unported("DefineDomain (typecmds.c): COLLATE clause (get_collation_oid)");
    }
    let domaincoll = base_coll;

    if base.has_default {
        unported("DefineDomain (typecmds.c): inherited base-type typdefault");
    }

    let mut typ_not_null = false;
    let mut null_defined = false;
    let mut saw_default = false;
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
                if constr.raw_expr.is_some() {
                    unported("DefineDomain (typecmds.c): DEFAULT expression (deparse_expression)");
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
        },
    )?;

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

fn domainAddCheckConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    domain_oid: Oid,
    domain_namespace: Oid,
    base_type_oid: Oid,
    typ_mod: i32,
    constr: &Constraint<'mcx>,
    domain_name: &str,
) -> PgResult<Oid> {
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
    let ccoid = pg_constraint::CreateConstraintEntry(mcx, &entry)?;
    parser_small1::free_parsestate(cpstate)?;
    Ok(ccoid)
}

fn domainAddNotNullConstraint<'mcx>(
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

#[cold]
#[inline(never)]
fn no_creation_schema() -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, "no schema has been selected to create in".to_string())
            .with_sqlstate(types_error::ERRCODE_UNDEFINED_SCHEMA),
    )
}

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

#[cold]
#[inline(never)]
fn permission_denied_type(typeoid: Oid) -> Box<PgError> {
    let name = format_type::format_type_be(typeoid).unwrap_or_else(|_| "???".into());
    Box::new(
        PgError::new(ERROR, format!("permission denied for type {name}"))
            .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
    )
}

#[cold]
#[inline(never)]
fn type_already_exists(name: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, format!("type \"{name}\" already exists"))
            .with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
    )
}

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
