// ALTER DOMAIN / ALTER TYPE lane (typecmds.c:2614-4290). Hosted here from
// tablecmds.c: RenameConstraint's domain arm (crate cycle with tablecmds).
use datum::Datum;
use mcx::{Mcx, PgVec};
use pg_depend::{DependencyType, ObjectAddress};
use types_core::{
    AttrNumber, InvalidOid, Oid, DEFAULT_COLLATION_OID, RELATION_RELATION_ID, TYPE_RELATION_ID,
};
use types_error::{
    PgError, PgResult, ERRCODE_CHECK_VIOLATION, ERRCODE_DUPLICATE_OBJECT,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_NOT_NULL_VIOLATION, ERRCODE_UNDEFINED_OBJECT,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR, NOTICE, PG_DIAG_COLUMN_NAME,
};
use types_nodes::parsenodes::{AlterDomainStmt, ObjectType, RenameStmt};
use types_nodes::rawnodes::{Constraint, ConstrType, TypeName};
use types_nodes::{Node, NodeList, NodeTag};
use types_rel::{
    AccessShareLock, NoLock, Relation, RowExclusiveLock, ShareLock, LOCKMODE, RELKIND_MATVIEW,
    RELKIND_RELATION,
};

use pg_type::TypeOidIndexId;

use crate::{
    domainAddCheckConstraint, domainAddNotNullConstraint, type_name_to_string,
    TYPTYPE_COMPOSITE, TYPTYPE_MULTIRANGE, TYPTYPE_RANGE,
};
use pg_type::TYPTYPE_DOMAIN;

const Anum_pg_type_typname: AttrNumber = 2;
const Anum_pg_type_typnamespace: AttrNumber = 3;
const Anum_pg_type_typowner: AttrNumber = 4;
const Anum_pg_type_typlen: AttrNumber = 5;
const Anum_pg_type_typtype: AttrNumber = 7;
const Anum_pg_type_typrelid: AttrNumber = 12;
const Anum_pg_type_typsubscript: AttrNumber = 13;
const Anum_pg_type_typelem: AttrNumber = 14;
const Anum_pg_type_typarray: AttrNumber = 15;
const Anum_pg_type_typinput: AttrNumber = 16;
const Anum_pg_type_typoutput: AttrNumber = 17;
const Anum_pg_type_typreceive: AttrNumber = 18;
const Anum_pg_type_typsend: AttrNumber = 19;
const Anum_pg_type_typmodin: AttrNumber = 20;
const Anum_pg_type_typmodout: AttrNumber = 21;
const Anum_pg_type_typanalyze: AttrNumber = 22;
const Anum_pg_type_typstorage: AttrNumber = 24;
const Anum_pg_type_typnotnull: AttrNumber = 25;
const Anum_pg_type_typbasetype: AttrNumber = 26;
const Anum_pg_type_typtypmod: AttrNumber = 27;
const Anum_pg_type_typcollation: AttrNumber = 29;
const Anum_pg_type_typdefaultbin: AttrNumber = 30;
const Anum_pg_type_typdefault: AttrNumber = 31;
const Anum_pg_type_typacl: AttrNumber = 32;

use pg_type::F_ARRAY_SUBSCRIPT_HANDLER;

const Anum_pg_constraint_oid: AttrNumber = 1;
const Anum_pg_constraint_contype: AttrNumber = 4;
const Anum_pg_constraint_convalidated: AttrNumber = 8;
const Anum_pg_constraint_conrelid: AttrNumber = 9;
const Anum_pg_constraint_contypid: AttrNumber = 10;
const Anum_pg_constraint_conname: AttrNumber = 2;
const Anum_pg_constraint_conbin: AttrNumber = 28;

struct TypeRow {
    typname: String,
    typnamespace: Oid,
    typowner: Oid,
    typlen: i16,
    typtype: i8,
    typstorage: i8,
    typrelid: Oid,
    typsubscript: Oid,
    typelem: Oid,
    typarray: Oid,
    typinput: Oid,
    typoutput: Oid,
    typreceive: Oid,
    typsend: Oid,
    typmodin: Oid,
    typmodout: Oid,
    typanalyze: Oid,
    typnotnull: bool,
    typbasetype: Oid,
    typtypmod: i32,
    typcollation: Oid,
    typdefaultbin: Option<String>,
    typacl_isnull: bool,
}

fn oid_key(attno: AttrNumber, oid: Oid) -> types_scan::scankey::ScanKeyData {
    let mut k = types_scan::scankey::ScanKeyData::empty();
    k.sk_attno = attno;
    k.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    k.sk_collation = 0;
    k.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    k.sk_argument = Datum::from_oid(oid);
    k
}

fn name_key<'mcx>(
    mcx: Mcx<'mcx>,
    attno: AttrNumber,
    name: &str,
) -> PgResult<(types_scan::scankey::ScanKeyData, PgVec<'mcx, u8>)> {
    assert!(name.len() < 64, "name key truncation unported");
    let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, 64)?;
    mcx::vec_append_bytes(&mut buf, name.as_bytes())?;
    mcx::vec_append_bytes(&mut buf, &[0u8; 64][..64 - name.len()])?;
    let mut k = types_scan::scankey::ScanKeyData::empty();
    k.sk_attno = attno;
    k.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    k.sk_collation = types_core::C_COLLATION_OID;
    k.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_NAMEEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_NAMEEQ) failed: {e:?}"));
    k.sk_argument = Datum::from_usize(buf.as_ptr() as usize);
    Ok((k, buf))
}

fn fetch_type_row<'mcx>(mcx: Mcx<'mcx>, typeoid: Oid) -> PgResult<TypeRow> {
    let rel = table::table_open(mcx, TYPE_RELATION_ID, AccessShareLock)?;
    let keys = [oid_key(pg_type::Anum_pg_type_oid, typeoid)];
    let mut scan = genam::systable_beginscan(mcx, &rel, TypeOidIndexId, true, None, &keys)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for type {typeoid}"));
    let desc = rel.descr();
    let get = |attno: AttrNumber, isnull: &mut bool| {
        // SAFETY: pg_type columns of the declared types under its descriptor.
        unsafe { types_tuple::heap_getattr(tup, attno as i32, desc, isnull) }
    };
    let mut isnull = false;
    let namedatum = get(Anum_pg_type_typname, &mut isnull);
    // SAFETY: NameData column is a 64-byte NUL-padded in-tuple buffer.
    let namebytes =
        unsafe { core::slice::from_raw_parts(namedatum.as_usize() as *const u8, 64) };
    let end = namebytes.iter().position(|&b| b == 0).unwrap_or(64);
    let typname = core::str::from_utf8(&namebytes[..end]).expect("typname UTF-8").to_string();
    let mut acl_isnull = false;
    get(Anum_pg_type_typacl, &mut acl_isnull);
    let mut bin_isnull = false;
    let bin_datum = get(Anum_pg_type_typdefaultbin, &mut bin_isnull);
    let typdefaultbin = if bin_isnull {
        None
    } else {
        let p = bin_datum.as_usize() as *const u8;
        // SAFETY: live text varlena image through its extent.
        let image = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
        let payload = varlena::open_image(mcx, image)?;
        Some(core::str::from_utf8(payload.as_bytes()).expect("typdefaultbin UTF-8").to_string())
    };
    let row = TypeRow {
        typname,
        typnamespace: get(Anum_pg_type_typnamespace, &mut isnull).as_oid(),
        typowner: get(Anum_pg_type_typowner, &mut isnull).as_oid(),
        typlen: get(Anum_pg_type_typlen, &mut isnull).as_i16(),
        typtype: get(Anum_pg_type_typtype, &mut isnull).as_i8(),
        typstorage: get(Anum_pg_type_typstorage, &mut isnull).as_i8(),
        typrelid: get(Anum_pg_type_typrelid, &mut isnull).as_oid(),
        typsubscript: get(Anum_pg_type_typsubscript, &mut isnull).as_oid(),
        typelem: get(Anum_pg_type_typelem, &mut isnull).as_oid(),
        typarray: get(Anum_pg_type_typarray, &mut isnull).as_oid(),
        typinput: get(Anum_pg_type_typinput, &mut isnull).as_oid(),
        typoutput: get(Anum_pg_type_typoutput, &mut isnull).as_oid(),
        typreceive: get(Anum_pg_type_typreceive, &mut isnull).as_oid(),
        typsend: get(Anum_pg_type_typsend, &mut isnull).as_oid(),
        typmodin: get(Anum_pg_type_typmodin, &mut isnull).as_oid(),
        typmodout: get(Anum_pg_type_typmodout, &mut isnull).as_oid(),
        typanalyze: get(Anum_pg_type_typanalyze, &mut isnull).as_oid(),
        typnotnull: get(Anum_pg_type_typnotnull, &mut isnull).as_bool(),
        typbasetype: get(Anum_pg_type_typbasetype, &mut isnull).as_oid(),
        typtypmod: get(Anum_pg_type_typtypmod, &mut isnull).as_i32(),
        typcollation: get(Anum_pg_type_typcollation, &mut isnull).as_oid(),
        typdefaultbin,
        typacl_isnull: acl_isnull,
    };
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(row)
}

// Scan-modify-update on the domain's pg_type row; apply fills (values,
// nulls, replace) indexed by Anum-1.
fn update_type_row<'mcx>(
    mcx: Mcx<'mcx>,
    typeoid: Oid,
    apply: impl FnOnce(&mut [Datum], &mut [bool], &mut [bool]) -> PgResult<()>,
) -> PgResult<()> {
    let rel = table::table_open(mcx, TYPE_RELATION_ID, RowExclusiveLock)?;
    let keys = [oid_key(pg_type::Anum_pg_type_oid, typeoid)];
    let mut scan = genam::systable_beginscan(mcx, &rel, TypeOidIndexId, true, None, &keys)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for type {typeoid}"));
    let desc = rel.descr();
    let n = desc.natts as usize;
    let mut values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, n)?;
    let mut nulls: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, n)?;
    let mut replace: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, n)?;
    values.resize(n, Datum::null());
    nulls.resize(n, false);
    replace.resize(n, false);
    apply(&mut values, &mut nulls, &mut replace)?;
    let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
    let otid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut newtup)?;
    rel.close(RowExclusiveLock)
}

// The manual sinval C sends when a command changes domain semantics without
// touching the pg_type row (typecmds.c "We must send out an sinval message").
fn cache_inval_type_tuple<'mcx>(mcx: Mcx<'mcx>, typeoid: Oid) -> PgResult<()> {
    let rel = table::table_open(mcx, TYPE_RELATION_ID, RowExclusiveLock)?;
    let keys = [oid_key(pg_type::Anum_pg_type_oid, typeoid)];
    let mut scan = genam::systable_beginscan(mcx, &rel, TypeOidIndexId, true, None, &keys)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for type {typeoid}"));
    inval::invalidate::CacheInvalidateHeapTuple(&rel, tup, None)?;
    genam::systable_endscan(mcx, scan)?;
    rel.close(RowExclusiveLock)
}

fn typename_from_list<'mcx>(mcx: Mcx<'mcx>, names: &NodeList<'mcx>) -> PgResult<TypeName<'mcx>> {
    Ok(TypeName { names: names.clone_in(mcx)?, typemod: -1, location: -1, ..Default::default() })
}

pub fn checkDomainOwner(typtype: i8, type_oid: Oid) -> PgResult<()> {
    if typtype != TYPTYPE_DOMAIN {
        return Err(not_a_domain(type_oid)?);
    }
    if !aclchk::object_ownercheck(TYPE_RELATION_ID, type_oid, miscinit::GetUserId())? {
        return Err(must_be_owner_of_type(type_oid)?);
    }
    Ok(())
}

pub fn AlterDomain<'mcx>(mcx: Mcx<'mcx>, stmt: &AlterDomainStmt<'mcx>) -> PgResult<()> {
    match stmt.subtype {
        b'T' => AlterDomainDefault(mcx, &stmt.typeName, stmt.def),
        b'N' => AlterDomainNotNull(mcx, &stmt.typeName, false),
        b'O' => AlterDomainNotNull(mcx, &stmt.typeName, true),
        b'C' => AlterDomainAddConstraint(mcx, &stmt.typeName, stmt.def.expect("constraint def")),
        b'X' => AlterDomainDropConstraint(
            mcx,
            &stmt.typeName,
            stmt.name.expect("constraint name"),
            stmt.behavior,
            stmt.missing_ok,
        ),
        b'V' => AlterDomainValidateConstraint(
            mcx,
            &stmt.typeName,
            stmt.name.expect("constraint name"),
        ),
        other => panic!("unrecognized alter domain type: {other}"),
    }
}

fn AlterDomainDefault<'mcx>(
    mcx: Mcx<'mcx>,
    names: &NodeList<'mcx>,
    default_raw: Option<Node<'mcx>>,
) -> PgResult<()> {
    let typename = typename_from_list(mcx, names)?;
    let (domainoid, _) = parse_utilcmd::typenameTypeIdAndMod(mcx, None, &typename)?;
    let row = fetch_type_row(mcx, domainoid)?;
    checkDomainOwner(row.typtype, domainoid)?;

    let mut default_expr: Option<Node<'mcx>> = None;
    let mut defaultbin_text: Option<datum::Varlena<'mcx>> = None;
    let mut default_text: Option<datum::Varlena<'mcx>> = None;
    if let Some(raw) = default_raw {
        let mut pstate = parser_small1::make_parsestate(mcx, None);
        let expr = tablecmds::cook_default(
            mcx,
            &mut pstate,
            raw,
            row.typbasetype,
            row.typtypmod,
            &row.typname,
            0,
            None,
        )?;
        let is_null_const = expr
            .as_variant::<types_nodes::primnodes::Const>()
            .map(|c| c.constisnull)
            .unwrap_or(false);
        if !is_null_const {
            let default_value = ruleutils::deparse_expression_pretty(mcx, expr, InvalidOid, false, 0)?;
            let binstr = outfuncs::nodeToString(mcx, expr)?;
            defaultbin_text = Some(varlena::cstring_to_text(mcx, binstr.as_str().as_bytes())?);
            default_text = Some(varlena::cstring_to_text(mcx, default_value.as_bytes())?);
            default_expr = Some(expr);
        }
    }

    update_type_row(mcx, domainoid, |values, nulls, replace| {
        let bin_ix = (Anum_pg_type_typdefaultbin - 1) as usize;
        let def_ix = (Anum_pg_type_typdefault - 1) as usize;
        replace[bin_ix] = true;
        replace[def_ix] = true;
        match (&defaultbin_text, &default_text) {
            (Some(bin), Some(def)) => {
                values[bin_ix] = Datum::from_usize(bin.as_bytes().as_ptr() as usize);
                values[def_ix] = Datum::from_usize(def.as_bytes().as_ptr() as usize);
            }
            _ => {
                nulls[bin_ix] = true;
                nulls[def_ix] = true;
            }
        }
        Ok(())
    })?;

    rebuild_domain_dependencies(mcx, domainoid, &row, default_expr)
}

// GenerateTypeDependencies (pg_type.c) rebuild arm, domain shape: delete +
// re-record namespace/owner/extension/procs/basetype/collation, then the
// default expression's normal deps.
fn rebuild_domain_dependencies<'mcx>(
    mcx: Mcx<'mcx>,
    domainoid: Oid,
    row: &TypeRow,
    default_expr: Option<Node<'mcx>>,
) -> PgResult<()> {
    pg_depend::deleteDependencyRecordsFor(mcx, TYPE_RELATION_ID, domainoid, true)?;
    pg_shdepend::deleteSharedDependencyRecordsFor(mcx, TYPE_RELATION_ID, domainoid, 0)?;

    let myself = ObjectAddress::set(TYPE_RELATION_ID, domainoid);
    let mut addrs_normal = [ObjectAddress::set(InvalidOid, InvalidOid); 12];
    let mut n = 0;
    addrs_normal[n] = ObjectAddress::set(types_core::NAMESPACE_RELATION_ID, row.typnamespace);
    n += 1;
    pg_depend::recordDependencyOnOwner(mcx, TYPE_RELATION_ID, domainoid, row.typowner)?;
    pg_depend::recordDependencyOnCurrentExtension(mcx, &myself, true)?;
    const PROCEDURE_RELATION_ID: Oid = 1255;
    for proc in [
        row.typinput,
        row.typoutput,
        row.typreceive,
        row.typsend,
        row.typmodin,
        row.typmodout,
        row.typanalyze,
        row.typsubscript,
    ] {
        if proc != InvalidOid {
            addrs_normal[n] = ObjectAddress::set(PROCEDURE_RELATION_ID, proc);
            n += 1;
        }
    }
    if row.typbasetype != InvalidOid {
        addrs_normal[n] = ObjectAddress::set(TYPE_RELATION_ID, row.typbasetype);
        n += 1;
    }
    const COLLATION_RELATION_ID: Oid = 3456;
    if row.typcollation != InvalidOid && row.typcollation != DEFAULT_COLLATION_OID {
        addrs_normal[n] = ObjectAddress::set(COLLATION_RELATION_ID, row.typcollation);
        n += 1;
    }
    pg_depend::record_object_address_dependencies(
        mcx,
        &myself,
        &mut addrs_normal[..n],
        DependencyType::Normal,
    )?;
    if let Some(expr) = default_expr {
        catalog_dependency::recordDependencyOnExpr(
            mcx,
            &myself,
            expr,
            &NodeList::nil(),
            DependencyType::Normal,
        )?;
    }
    Ok(())
}

fn AlterDomainNotNull<'mcx>(
    mcx: Mcx<'mcx>,
    names: &NodeList<'mcx>,
    not_null: bool,
) -> PgResult<()> {
    let typename = typename_from_list(mcx, names)?;
    let (domainoid, _) = parse_utilcmd::typenameTypeIdAndMod(mcx, None, &typename)?;
    let row = fetch_type_row(mcx, domainoid)?;
    checkDomainOwner(row.typtype, domainoid)?;

    if row.typnotnull == not_null {
        return Ok(());
    }

    if not_null {
        let constr = Constraint {
            contype: ConstrType::CONSTR_NOTNULL,
            initially_valid: true,
            location: -1,
            ..Default::default()
        };
        domainAddNotNullConstraint(mcx, domainoid, row.typnamespace, &constr, &row.typname)?;
        validateDomainNotNullConstraint(mcx, domainoid)?;
    } else {
        let con_oid = pg_constraint::findDomainNotNullConstraint(mcx, domainoid)?
            .unwrap_or_else(|| {
                panic!("could not find not-null constraint on domain \"{}\"", row.typname)
            });
        catalog_dependency::performDeletion(
            mcx,
            &ObjectAddress::set(types_core::CONSTRAINT_RELATION_ID, con_oid),
            types_nodes::parsenodes::DropBehavior::DROP_RESTRICT,
            0,
        )?;
    }

    update_type_row(mcx, domainoid, |values, _nulls, replace| {
        let ix = (Anum_pg_type_typnotnull - 1) as usize;
        values[ix] = Datum::from_bool(not_null);
        replace[ix] = true;
        Ok(())
    })
}

fn AlterDomainDropConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    names: &NodeList<'mcx>,
    constr_name: &str,
    behavior: types_nodes::parsenodes::DropBehavior,
    missing_ok: bool,
) -> PgResult<()> {
    let typename = typename_from_list(mcx, names)?;
    let (domainoid, _) = parse_utilcmd::typenameTypeIdAndMod(mcx, None, &typename)?;
    let row = fetch_type_row(mcx, domainoid)?;
    checkDomainOwner(row.typtype, domainoid)?;

    let con_rel = table::table_open(mcx, types_core::CONSTRAINT_RELATION_ID, RowExclusiveLock)?;
    let (nkey, _namebuf) = name_key(mcx, Anum_pg_constraint_conname, constr_name)?;
    let keys = [
        oid_key(Anum_pg_constraint_conrelid, InvalidOid),
        oid_key(Anum_pg_constraint_contypid, domainoid),
        nkey,
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &con_rel,
        pg_constraint::ConstraintRelidTypidNameIndexId,
        true,
        None,
        &keys,
    )?;
    let mut found: Option<(Oid, u8)> = None;
    if let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let desc = con_rel.descr();
        let mut isnull = false;
        // SAFETY (each): fixed NOT NULL pg_constraint columns under its descriptor.
        let oid = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_constraint_oid as i32, desc, &mut isnull)
        }
        .as_oid();
        // SAFETY: as above.
        let contype = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_constraint_contype as i32, desc, &mut isnull)
        }
        .as_i8() as u8;
        found = Some((oid, contype));
    }
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(RowExclusiveLock)?;

    match found {
        Some((con_oid, contype)) => {
            if contype == pg_constraint::CONSTRAINT_NOTNULL {
                update_type_row(mcx, domainoid, |values, _nulls, replace| {
                    let ix = (Anum_pg_type_typnotnull - 1) as usize;
                    values[ix] = Datum::from_bool(false);
                    replace[ix] = true;
                    Ok(())
                })?;
            }
            catalog_dependency::performDeletion(
                mcx,
                &ObjectAddress::set(types_core::CONSTRAINT_RELATION_ID, con_oid),
                behavior,
                0,
            )?;
        }
        None => {
            let dname = type_name_to_string(mcx, &typename)?;
            if !missing_ok {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!(
                            "constraint \"{constr_name}\" of domain \"{}\" does not exist",
                            dname.as_str()
                        ),
                    )
                    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
                ));
            }
            elog_seams::ereport_msg::call(
                NOTICE,
                format!(
                    "constraint \"{constr_name}\" of domain \"{}\" does not exist, skipping",
                    dname.as_str()
                ),
                None,
            )?;
        }
    }

    cache_inval_type_tuple(mcx, domainoid)
}

pub(crate) fn AlterDomainAddConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    names: &NodeList<'mcx>,
    new_constraint: Node<'mcx>,
) -> PgResult<()> {
    let typename = typename_from_list(mcx, names)?;
    let (domainoid, _) = parse_utilcmd::typenameTypeIdAndMod(mcx, None, &typename)?;
    let row = fetch_type_row(mcx, domainoid)?;
    checkDomainOwner(row.typtype, domainoid)?;

    if new_constraint.node_tag() != NodeTag::T_Constraint {
        panic!("unrecognized node type: {:?}", new_constraint.node_tag());
    }
    let constr = new_constraint.as_variant::<Constraint>().expect("Constraint");

    match constr.contype {
        ConstrType::CONSTR_CHECK => {
            let (_ccoid, ccbin) = domainAddCheckConstraint(
                mcx,
                domainoid,
                row.typnamespace,
                row.typbasetype,
                row.typtypmod,
                constr,
                &row.typname,
            )?;
            if !constr.skip_validation {
                validateDomainCheckConstraint(mcx, domainoid, ccbin.as_str())?;
            }
            cache_inval_type_tuple(mcx, domainoid)?;
        }
        ConstrType::CONSTR_NOTNULL => {
            if row.typnotnull {
                return Ok(());
            }
            domainAddNotNullConstraint(mcx, domainoid, row.typnamespace, constr, &row.typname)?;
            if !constr.skip_validation {
                validateDomainNotNullConstraint(mcx, domainoid)?;
            }
            update_type_row(mcx, domainoid, |values, _nulls, replace| {
                let ix = (Anum_pg_type_typnotnull - 1) as usize;
                values[ix] = Datum::from_bool(true);
                replace[ix] = true;
                Ok(())
            })?;
        }
        other => panic!("AlterDomainAddConstraint: parser let through {other:?}"),
    }
    Ok(())
}

fn AlterDomainValidateConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    names: &NodeList<'mcx>,
    constr_name: &str,
) -> PgResult<()> {
    let typename = typename_from_list(mcx, names)?;
    let (domainoid, _) = parse_utilcmd::typenameTypeIdAndMod(mcx, None, &typename)?;
    let row = fetch_type_row(mcx, domainoid)?;
    checkDomainOwner(row.typtype, domainoid)?;

    let con_rel = table::table_open(mcx, types_core::CONSTRAINT_RELATION_ID, RowExclusiveLock)?;
    let (nkey, _namebuf) = name_key(mcx, Anum_pg_constraint_conname, constr_name)?;
    let keys = [
        oid_key(Anum_pg_constraint_conrelid, InvalidOid),
        oid_key(Anum_pg_constraint_contypid, domainoid),
        nkey,
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &con_rel,
        pg_constraint::ConstraintRelidTypidNameIndexId,
        true,
        None,
        &keys,
    )?;
    let desc = con_rel.descr();
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        let dname = type_name_to_string(mcx, &typename)?;
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "constraint \"{constr_name}\" of domain \"{}\" does not exist",
                    dname.as_str()
                ),
            )
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
        ));
    };
    let mut isnull = false;
    // SAFETY: fixed NOT NULL pg_constraint column under its descriptor.
    let contype = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_constraint_contype as i32, desc, &mut isnull)
    }
    .as_i8() as u8;
    if contype != pg_constraint::CONSTRAINT_CHECK {
        let dname = type_name_to_string(mcx, &typename)?;
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "constraint \"{constr_name}\" of domain \"{}\" is not a check constraint",
                    dname.as_str()
                ),
            )
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }
    // SAFETY: conbin read under the constraint tuple's descriptor; NULL is a
    // corruption panic below.
    let conbin_datum = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_constraint_conbin as i32, desc, &mut isnull)
    };
    assert!(!isnull, "null conbin for constraint \"{constr_name}\"");
    let p = conbin_datum.as_usize() as *const u8;
    // SAFETY: live text varlena image through its extent.
    let image = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    let payload = varlena::open_image(mcx, image)?;
    let conbin = core::str::from_utf8(payload.as_bytes()).expect("conbin UTF-8").to_string();

    let n = desc.natts as usize;
    let mut values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, n)?;
    let mut nulls: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, n)?;
    let mut replace: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, n)?;
    values.resize(n, Datum::null());
    nulls.resize(n, false);
    replace.resize(n, false);
    values[(Anum_pg_constraint_convalidated - 1) as usize] = Datum::from_bool(true);
    replace[(Anum_pg_constraint_convalidated - 1) as usize] = true;
    let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
    let otid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;

    validateDomainCheckConstraint(mcx, domainoid, &conbin)?;

    catalog_indexing::CatalogTupleUpdate(mcx, &con_rel, &otid, &mut newtup)?;
    con_rel.close(RowExclusiveLock)
}

struct RelToCheck<'mcx> {
    rel: Relation<'mcx>,
    atts: PgVec<'mcx, i32>,
}

fn get_rels_with_domain<'mcx>(
    mcx: Mcx<'mcx>,
    domain_oid: Oid,
    lockmode: LOCKMODE,
) -> PgResult<PgVec<'mcx, RelToCheck<'mcx>>> {
    let domain_type_name = format_type::format_type_be(domain_oid)?;
    let mut result: PgVec<'mcx, RelToCheck<'mcx>> = PgVec::new_in(mcx);
    let dep_rel = table::table_open(mcx, pg_depend::DependRelationId, AccessShareLock)?;
    const Anum_pg_depend_classid: AttrNumber = 1;
    const Anum_pg_depend_objid: AttrNumber = 2;
    const Anum_pg_depend_objsubid: AttrNumber = 3;
    const Anum_pg_depend_refclassid: AttrNumber = 4;
    const Anum_pg_depend_refobjid: AttrNumber = 5;
    let keys = [
        oid_key(Anum_pg_depend_refclassid, TYPE_RELATION_ID),
        oid_key(Anum_pg_depend_refobjid, domain_oid),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &dep_rel,
        pg_depend::DependReferenceIndexId,
        true,
        None,
        &keys,
    )?;
    let desc = dep_rel.descr();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let get = |anum: AttrNumber| {
            let mut isnull = false;
            // SAFETY: fixed NOT NULL pg_depend columns under its descriptor.
            unsafe { types_tuple::heap_getattr(tup, anum as i32, desc, &mut isnull) }
        };
        let classid = get(Anum_pg_depend_classid).as_oid();
        let objid = get(Anum_pg_depend_objid).as_oid();
        let objsubid = get(Anum_pg_depend_objsubid).as_i32();
        if classid == TYPE_RELATION_ID {
            if lsyscache::get_typtype(objid)? == TYPTYPE_DOMAIN {
                let mut sub = get_rels_with_domain(mcx, objid, lockmode)?;
                for rtc in sub.drain(..) {
                    result.push(rtc);
                }
            } else {
                tablecmds::find_composite_type_dependencies(mcx, objid, &domain_type_name)?;
            }
            continue;
        }
        if classid != RELATION_RELATION_ID || objsubid <= 0 {
            continue;
        }
        let mut rtc_ix = result.iter().position(|r| r.rel.rd_id == objid);
        if rtc_ix.is_none() {
            let rel = relation_seams::relation_open::call(mcx, objid, lockmode)?;
            if rel.rd_rel.reltype != InvalidOid {
                tablecmds::find_composite_type_dependencies(
                    mcx,
                    rel.rd_rel.reltype,
                    &domain_type_name,
                )?;
            }
            if rel.rd_rel.relkind != RELKIND_RELATION && rel.rd_rel.relkind != RELKIND_MATVIEW {
                rel.close(lockmode)?;
                continue;
            }
            result.push(RelToCheck { rel, atts: PgVec::new_in(mcx) });
            rtc_ix = Some(result.len() - 1);
        }
        let rtc = &mut result[rtc_ix.expect("entry exists")];
        let natts = rtc.rel.rd_att.natts as i32;
        if objsubid > natts {
            continue;
        }
        let att = rtc.rel.rd_att.attr(objsubid as usize - 1);
        if att.attisdropped || att.atttypid != domain_oid {
            continue;
        }
        let mut ptr = rtc.atts.len();
        rtc.atts.push(objsubid);
        while ptr > 0 && rtc.atts[ptr - 1] > objsubid {
            rtc.atts[ptr] = rtc.atts[ptr - 1];
            ptr -= 1;
        }
        rtc.atts[ptr] = objsubid;
    }
    genam::systable_endscan(mcx, scan)?;
    dep_rel.close(AccessShareLock)?;
    Ok(result)
}

fn validateDomainNotNullConstraint<'mcx>(mcx: Mcx<'mcx>, domainoid: Oid) -> PgResult<()> {
    let rels = get_rels_with_domain(mcx, domainoid, ShareLock)?;
    for rtc in rels.iter() {
        let testrel = &rtc.rel;
        let snapshot = snapmgr::GetLatestSnapshot()?;
        let snapshot = snapmgr::RegisterSnapshot(Some(&snapshot))?.expect("registered snapshot");
        let mut scan =
            tableam::table_beginscan(mcx, testrel, Some(snapshot.clone()), 0, PgVec::new_in(mcx))?;
        let mut slot = tableam::table_slot_create(mcx, testrel)?;
        while tableam::table_scan_getnextslot(
            mcx,
            &mut scan,
            types_scan::ScanDirection::ForwardScanDirection,
            &mut slot,
        )? {
            for &attnum in rtc.atts.iter() {
                if exectuples::slot_attisnull(&mut slot, attnum) {
                    let att = testrel.rd_att.attr(attnum as usize - 1);
                    return Err(domain_column_violation(
                        mcx,
                        testrel,
                        att.attname.name_str(),
                        ERRCODE_NOT_NULL_VIOLATION,
                        "contains null values",
                    )?);
                }
            }
        }
        tableam::table_endscan(scan)?;
        snapmgr::UnregisterSnapshot(Some(&snapshot));
    }
    Ok(())
}

fn validateDomainCheckConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    domainoid: Oid,
    ccbin: &str,
) -> PgResult<()> {
    let expr = readfuncs::stringToNode(mcx, ccbin)?;
    let planned = clauses::eval_const_expressions(mcx, expr)?;
    let mut program = execexpr::domain::prepare_domain_check_expr(mcx, planned)?;

    let rels = get_rels_with_domain(mcx, domainoid, ShareLock)?;
    for rtc in rels.iter() {
        let testrel = &rtc.rel;
        let snapshot = snapmgr::GetLatestSnapshot()?;
        let snapshot = snapmgr::RegisterSnapshot(Some(&snapshot))?.expect("registered snapshot");
        let mut scan =
            tableam::table_beginscan(mcx, testrel, Some(snapshot.clone()), 0, PgVec::new_in(mcx))?;
        let mut slot = tableam::table_slot_create(mcx, testrel)?;
        while tableam::table_scan_getnextslot(
            mcx,
            &mut scan,
            types_scan::ScanDirection::ForwardScanDirection,
            &mut slot,
        )? {
            for &attnum in rtc.atts.iter() {
                let mut isnull = false;
                let d = exectuples::slot_getattr(&mut slot, attnum, &mut isnull);
                let r = program.eval(d, isnull)?;
                if !r.isnull && !r.value.as_bool() {
                    let att = testrel.rd_att.attr(attnum as usize - 1);
                    return Err(domain_column_violation(
                        mcx,
                        testrel,
                        att.attname.name_str(),
                        ERRCODE_CHECK_VIOLATION,
                        "contains values that violate the new constraint",
                    )?);
                }
            }
        }
        tableam::table_endscan(scan)?;
        snapmgr::UnregisterSnapshot(Some(&snapshot));
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn domain_column_violation<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attname_bytes: &[u8],
    sqlstate: types_error::SqlState,
    tail: &str,
) -> PgResult<Box<PgError>> {
    let colname = core::str::from_utf8(attname_bytes).expect("attname UTF-8").to_string();
    let relname = rel.name().to_string();
    let schema = lsyscache::get_namespace_name(mcx, rel.rd_rel.relnamespace)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();
    Ok(Box::new(
        PgError::new(
            ERROR,
            format!("column \"{colname}\" of table \"{relname}\" {tail}"),
        )
        .with_sqlstate(sqlstate)
        .with_schema_name(schema)
        .with_table_name(relname)
        .with_error_field(PG_DIAG_COLUMN_NAME, colname)?,
    ))
}

pub fn RenameType<'mcx>(mcx: Mcx<'mcx>, stmt: &RenameStmt<'mcx>) -> PgResult<()> {
    let names = stmt.object.expect("RenameStmt.object").as_list().expect("name list");
    let new_type_name = stmt.newname.expect("RenameStmt.newname");
    let typename = typename_from_list(mcx, names)?;
    let (type_oid, _) = parse_utilcmd::typenameTypeIdAndMod(mcx, None, &typename)?;
    let row = fetch_type_row(mcx, type_oid)?;

    if !aclchk::object_ownercheck(TYPE_RELATION_ID, type_oid, miscinit::GetUserId())? {
        return Err(must_be_owner_of_type(type_oid)?);
    }
    if stmt.renameType == ObjectType::OBJECT_DOMAIN && row.typtype != TYPTYPE_DOMAIN {
        return Err(not_a_domain(type_oid)?);
    }
    if row.typtype == TYPTYPE_COMPOSITE {
        // unported: RenameType composite types (RenameRelationInternal chase)
        return Err(Box::new(
            PgError::error("renaming a composite type is not supported yet")
                .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    if row.typelem != InvalidOid && row.typsubscript == F_ARRAY_SUBSCRIPT_HANDLER {
        return Err(cannot_alter_array_type(type_oid, row.typelem)?);
    }
    pg_type::RenameTypeInternal(mcx, type_oid, new_type_name, row.typnamespace)
}

// RenameConstraint (tablecmds.c) OBJECT_DOMCONSTRAINT arm: domain constraints
// have no index/inheritance legs, so the rename collapses to
// get_domain_constraint_oid + RenameConstraintById.
pub fn RenameDomainConstraint<'mcx>(mcx: Mcx<'mcx>, stmt: &RenameStmt<'mcx>) -> PgResult<()> {
    let names = stmt.object.expect("RenameStmt.object").as_list().expect("name list");
    let typename = typename_from_list(mcx, names)?;
    let (typid, _) = parse_utilcmd::typenameTypeIdAndMod(mcx, None, &typename)?;
    let row = fetch_type_row(mcx, typid)?;
    checkDomainOwner(row.typtype, typid)?;
    let con_oid = pg_constraint::get_domain_constraint_oid(
        mcx,
        typid,
        stmt.subname.expect("RenameStmt.subname"),
        false,
    )?;
    pg_constraint::RenameConstraintById(mcx, con_oid, stmt.newname.expect("RenameStmt.newname"))
}

pub fn AlterTypeOwner<'mcx>(
    mcx: Mcx<'mcx>,
    names: &NodeList<'mcx>,
    new_owner_id: Oid,
    objecttype: ObjectType,
) -> PgResult<()> {
    let typename = typename_from_list(mcx, names)?;
    let (type_oid, _) = parse_utilcmd::typenameTypeIdAndMod(mcx, None, &typename)?;
    let row = fetch_type_row(mcx, type_oid)?;

    if objecttype == ObjectType::OBJECT_DOMAIN && row.typtype != TYPTYPE_DOMAIN {
        return Err(not_a_domain(type_oid)?);
    }
    if row.typtype == TYPTYPE_COMPOSITE
        && lsyscache::get_rel_relkind(row.typrelid)? != b'c' as i8
    {
        return Err(is_a_table_row_type(type_oid)?);
    }
    if row.typelem != InvalidOid && row.typsubscript == F_ARRAY_SUBSCRIPT_HANDLER {
        return Err(cannot_alter_array_type(type_oid, row.typelem)?);
    }
    if row.typtype == TYPTYPE_MULTIRANGE {
        return Err(cannot_alter_multirange_type(type_oid)?);
    }

    if row.typowner != new_owner_id {
        if !superuser::superuser_arg(miscinit::GetUserId())? {
            // unported: AlterTypeOwner non-superuser checks
            // (check_can_set_role/ACL_CREATE)
            return Err(Box::new(
                PgError::error(
                    "changing the owner of a type as a non-superuser is not supported yet",
                )
                .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
        AlterTypeOwner_oid(mcx, type_oid, new_owner_id, true)?;
    }
    Ok(())
}

pub fn AlterTypeOwner_oid<'mcx>(
    mcx: Mcx<'mcx>,
    type_oid: Oid,
    new_owner_id: Oid,
    has_depend_entry: bool,
) -> PgResult<()> {
    let row = fetch_type_row(mcx, type_oid)?;
    if row.typtype == TYPTYPE_COMPOSITE {
        // ATExecChangeOwner fixes up the pg_class entry and calls back to
        // AlterTypeOwnerInternal for the pg_type entry(s).
        pg_shdepend::at_exec_change_owner::call(
            mcx,
            row.typrelid,
            new_owner_id,
            true,
            types_rel::AccessExclusiveLock,
        )?;
    } else {
        AlterTypeOwnerInternal(mcx, type_oid, new_owner_id)?;
    }
    if has_depend_entry {
        pg_shdepend::changeDependencyOnOwner(mcx, TYPE_RELATION_ID, type_oid, new_owner_id)?;
    }
    Ok(())
}

pub fn AlterTypeOwnerInternal<'mcx>(
    mcx: Mcx<'mcx>,
    type_oid: Oid,
    new_owner_id: Oid,
) -> PgResult<()> {
    let row = fetch_type_row(mcx, type_oid)?;
    {
        let rel = table::table_open(mcx, TYPE_RELATION_ID, RowExclusiveLock)?;
        let keys = [oid_key(pg_type::Anum_pg_type_oid, type_oid)];
        let mut scan = genam::systable_beginscan(mcx, &rel, TypeOidIndexId, true, None, &keys)?;
        let tup = genam::systable_getnext(mcx, &mut scan)?
            .unwrap_or_else(|| panic!("cache lookup failed for type {type_oid}"));
        let desc = rel.descr();
        let n = desc.natts as usize;
        let mut values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, n)?;
        let mut nulls: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, n)?;
        let mut replace: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, n)?;
        values.resize(n, Datum::null());
        nulls.resize(n, false);
        replace.resize(n, false);
        values[(Anum_pg_type_typowner - 1) as usize] = Datum::from_oid(new_owner_id);
        replace[(Anum_pg_type_typowner - 1) as usize] = true;

        let mut acl_null = false;
        // SAFETY: typacl read under the open scan's held tuple.
        let acl_datum = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_type_typacl as i32, desc, &mut acl_null)
        };
        let acl_img;
        if !acl_null {
            let new_acl = aclchk::with_acl_datum(acl_datum, |acl| {
                adt_acl::aclnewowner(mcx, acl, row.typowner, new_owner_id)
            })?;
            acl_img = adt_acl::varlena::acl_image(mcx, &new_acl)?;
            values[(Anum_pg_type_typacl - 1) as usize] =
                Datum::from_usize(acl_img.as_ptr() as usize);
            replace[(Anum_pg_type_typacl - 1) as usize] = true;
        }

        let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
        let otid = tup.t_self;
        genam::systable_endscan(mcx, scan)?;
        catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut newtup)?;
        rel.close(RowExclusiveLock)?;
    }
    if row.typarray != InvalidOid {
        AlterTypeOwnerInternal(mcx, row.typarray, new_owner_id)?;
    }
    if row.typtype == TYPTYPE_RANGE {
        let multirange = lsyscache::get_range_multirange(type_oid)?;
        if multirange == InvalidOid {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "could not find multirange type for data type {}",
                        format_type::format_type_be(type_oid)?
                    ),
                )
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
            ));
        }
        AlterTypeOwnerInternal(mcx, multirange, new_owner_id)?;
    }
    Ok(())
}

pub fn AlterTypeNamespace<'mcx>(
    mcx: Mcx<'mcx>,
    names: &NodeList<'mcx>,
    newschema: &str,
    objecttype: ObjectType,
) -> PgResult<()> {
    let typename = typename_from_list(mcx, names)?;
    let (type_oid, _) = parse_utilcmd::typenameTypeIdAndMod(mcx, None, &typename)?;
    if objecttype == ObjectType::OBJECT_DOMAIN && lsyscache::get_typtype(type_oid)? != TYPTYPE_DOMAIN
    {
        return Err(not_a_domain(type_oid)?);
    }
    let nsp_oid = catalog_namespace::LookupCreationNamespace(mcx, newschema)?;
    let mut objs_moved: PgVec<'mcx, ObjectAddress> = PgVec::new_in(mcx);
    AlterTypeNamespace_oid(mcx, type_oid, nsp_oid, false, &mut objs_moved)?;
    Ok(())
}

pub fn AlterTypeNamespace_oid<'mcx>(
    mcx: Mcx<'mcx>,
    type_oid: Oid,
    nsp_oid: Oid,
    ignore_dependent: bool,
    objs_moved: &mut PgVec<'mcx, ObjectAddress>,
) -> PgResult<Oid> {
    if !aclchk::object_ownercheck(TYPE_RELATION_ID, type_oid, miscinit::GetUserId())? {
        return Err(must_be_owner_of_type(type_oid)?);
    }
    let elem_oid = lsyscache::get_element_type(type_oid)?;
    if elem_oid != InvalidOid && lsyscache::get_array_type(elem_oid)? == type_oid {
        if ignore_dependent {
            return Ok(InvalidOid);
        }
        return Err(cannot_alter_array_type(type_oid, elem_oid)?);
    }
    AlterTypeNamespaceInternal(mcx, type_oid, nsp_oid, false, ignore_dependent, true, objs_moved)
}

pub fn AlterTypeNamespaceInternal<'mcx>(
    mcx: Mcx<'mcx>,
    type_oid: Oid,
    nsp_oid: Oid,
    is_implicit_array: bool,
    ignore_dependent: bool,
    error_on_table_type: bool,
    objs_moved: &mut PgVec<'mcx, ObjectAddress>,
) -> PgResult<Oid> {
    let thisobj = ObjectAddress::set(TYPE_RELATION_ID, type_oid);
    if objs_moved.iter().any(|a| a.classId == thisobj.classId && a.objectId == thisobj.objectId) {
        return Ok(InvalidOid);
    }
    let row = fetch_type_row(mcx, type_oid)?;
    let old_nsp_oid = row.typnamespace;
    let array_oid = row.typarray;

    if old_nsp_oid != nsp_oid {
        catalog_namespace::CheckSetNamespace(old_nsp_oid, nsp_oid)?;
        if syscache_seams::lookup_pg_type_oid_by_name::call(&row.typname, nsp_oid)? != InvalidOid {
            let nspname = lsyscache::get_namespace_name(mcx, nsp_oid)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "type \"{}\" already exists in schema \"{nspname}\"",
                        row.typname
                    ),
                )
                .with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
            ));
        }
    }

    let is_composite_type = row.typtype == TYPTYPE_COMPOSITE
        && lsyscache::get_rel_relkind(row.typrelid)? == b'c' as i8;
    if row.typtype == TYPTYPE_COMPOSITE && !is_composite_type {
        if ignore_dependent {
            return Ok(InvalidOid);
        }
        if error_on_table_type {
            return Err(is_a_table_row_type(type_oid)?);
        }
    }

    if old_nsp_oid != nsp_oid {
        update_type_row(mcx, type_oid, |values, _nulls, replace| {
            let ix = (Anum_pg_type_typnamespace - 1) as usize;
            values[ix] = Datum::from_oid(nsp_oid);
            replace[ix] = true;
            Ok(())
        })?;
    }

    if is_composite_type {
        // typecmds.c: relocate the composite type's pg_class entry (no
        // pg_depend entry of its own).
        let class_rel =
            table::table_open(mcx, types_core::RELATION_RELATION_ID, RowExclusiveLock)?;
        tablecmds::AlterRelationNamespaceInternal(
            mcx,
            &class_rel,
            row.typrelid,
            old_nsp_oid,
            nsp_oid,
            false,
            objs_moved,
        )?;
        class_rel.close(RowExclusiveLock)?;
    } else if row.typtype == TYPTYPE_DOMAIN {
        pg_constraint::AlterConstraintNamespaces(
            mcx,
            type_oid,
            old_nsp_oid,
            nsp_oid,
            true,
            objs_moved,
        )?;
    }

    if old_nsp_oid != nsp_oid
        && (is_composite_type || row.typtype != TYPTYPE_COMPOSITE)
        && !is_implicit_array
        && pg_depend::changeDependencyFor(
            mcx,
            TYPE_RELATION_ID,
            type_oid,
            types_core::NAMESPACE_RELATION_ID,
            old_nsp_oid,
            nsp_oid,
        )? != 1
    {
        panic!(
            "could not change schema dependency for type \"{}\"",
            format_type::format_type_be(type_oid)?
        );
    }

    objs_moved.push(thisobj);

    if array_oid != InvalidOid {
        AlterTypeNamespaceInternal(mcx, array_oid, nsp_oid, true, true, true, objs_moved)?;
    }

    Ok(old_nsp_oid)
}

#[cold]
#[inline(never)]
fn not_a_domain(type_oid: Oid) -> PgResult<Box<PgError>> {
    let name = format_type::format_type_be(type_oid)?;
    Ok(Box::new(
        PgError::new(ERROR, format!("{name} is not a domain"))
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
    ))
}

#[cold]
#[inline(never)]
pub(crate) fn must_be_owner_of_type(type_oid: Oid) -> PgResult<Box<PgError>> {
    let name = format_type::format_type_be(type_oid)?;
    Ok(Box::new(
        PgError::new(ERROR, format!("must be owner of type {name}"))
            .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
    ))
}

#[cold]
#[inline(never)]
fn cannot_alter_array_type(type_oid: Oid, elem_oid: Oid) -> PgResult<Box<PgError>> {
    let name = format_type::format_type_be(type_oid)?;
    let elem = format_type::format_type_be(elem_oid)?;
    Ok(Box::new(
        PgError::new(ERROR, format!("cannot alter array type {name}"))
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
            .with_hint(format!(
                "You can alter type {elem}, which will alter the array type as well."
            )),
    ))
}

#[cold]
#[inline(never)]
fn cannot_alter_multirange_type(type_oid: Oid) -> PgResult<Box<PgError>> {
    let name = format_type::format_type_be(type_oid)?;
    let rangetype = lsyscache::misc::get_multirange_range(type_oid)?;
    let mut e = PgError::new(ERROR, format!("cannot alter multirange type {name}"))
        .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE);
    if rangetype != InvalidOid {
        e = e.with_hint(format!(
            "You can alter type {}, which will alter the multirange type as well.",
            format_type::format_type_be(rangetype)?
        ));
    }
    Ok(Box::new(e))
}

#[cold]
#[inline(never)]
fn is_a_table_row_type(type_oid: Oid) -> PgResult<Box<PgError>> {
    let name = format_type::format_type_be(type_oid)?;
    Ok(Box::new(
        PgError::new(ERROR, format!("{name} is a table's row type"))
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
            .with_hint("Use ALTER TABLE instead."),
    ))
}

#[derive(Default)]
struct AlterTypeRecurseParams {
    update_storage: bool,
    update_receive: bool,
    update_send: bool,
    update_typmodin: bool,
    update_typmodout: bool,
    update_analyze: bool,
    update_subscript: bool,
    storage: i8,
    receive_oid: Oid,
    send_oid: Oid,
    typmodin_oid: Oid,
    typmodout_oid: Oid,
    analyze_oid: Oid,
    subscript_oid: Oid,
}

pub fn AlterType<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &types_nodes::rawnodes::AlterTypeStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    use crate::{
        findTypeAnalyzeFunction, findTypeInputFunction, findTypeOutputFunction,
        findTypeSubscriptingFunction, findTypeTypmodFunction, objdef_err, param_err,
        TYPSTORAGE_EXTENDED, TYPSTORAGE_EXTERNAL, TYPSTORAGE_MAIN, TYPSTORAGE_PLAIN,
    };

    let typename = typename_from_list(mcx, &stmt.typeName)?;
    let (type_oid, _) = parse_utilcmd::typenameTypeIdAndMod(mcx, None, &typename)?;
    let row = fetch_type_row(mcx, type_oid)?;

    let mut require_super = false;
    let mut p = AlterTypeRecurseParams::default();
    for n in stmt.options.iter() {
        let defel = n.as_def_elem().expect("ALTER TYPE options: DefElem list");
        match defel.defname.unwrap_or("") {
            "storage" => {
                let a = commands_define::defGetString(mcx, defel)?;
                p.storage = if a.eq_ignore_ascii_case("plain") {
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
                if p.storage != TYPSTORAGE_PLAIN && row.typlen != -1 {
                    return Err(objdef_err("fixed-size types must have storage PLAIN".into()));
                }
                if p.storage != TYPSTORAGE_PLAIN && row.typstorage == TYPSTORAGE_PLAIN {
                    require_super = true;
                } else if p.storage == TYPSTORAGE_PLAIN && row.typstorage != TYPSTORAGE_PLAIN {
                    return Err(objdef_err("cannot change type's storage to PLAIN".into()));
                }
                p.update_storage = true;
            }
            "receive" => {
                p.receive_oid = match defel.arg {
                    Some(_) => findTypeInputFunction(
                        mcx,
                        commands_define::defGetQualifiedName(mcx, defel)?,
                        type_oid,
                        true,
                    )?,
                    None => InvalidOid,
                };
                p.update_receive = true;
                require_super = true;
            }
            "send" => {
                p.send_oid = match defel.arg {
                    Some(_) => findTypeOutputFunction(
                        mcx,
                        commands_define::defGetQualifiedName(mcx, defel)?,
                        type_oid,
                        true,
                    )?,
                    None => InvalidOid,
                };
                p.update_send = true;
                require_super = true;
            }
            "typmod_in" => {
                p.typmodin_oid = match defel.arg {
                    Some(_) => findTypeTypmodFunction(
                        mcx,
                        commands_define::defGetQualifiedName(mcx, defel)?,
                        false,
                    )?,
                    None => InvalidOid,
                };
                p.update_typmodin = true;
                require_super = true;
            }
            "typmod_out" => {
                p.typmodout_oid = match defel.arg {
                    Some(_) => findTypeTypmodFunction(
                        mcx,
                        commands_define::defGetQualifiedName(mcx, defel)?,
                        true,
                    )?,
                    None => InvalidOid,
                };
                p.update_typmodout = true;
                require_super = true;
            }
            "analyze" => {
                p.analyze_oid = match defel.arg {
                    Some(_) => findTypeAnalyzeFunction(
                        mcx,
                        commands_define::defGetQualifiedName(mcx, defel)?,
                    )?,
                    None => InvalidOid,
                };
                p.update_analyze = true;
                require_super = true;
            }
            "subscript" => {
                p.subscript_oid = match defel.arg {
                    Some(_) => findTypeSubscriptingFunction(
                        mcx,
                        commands_define::defGetQualifiedName(mcx, defel)?,
                    )?,
                    None => InvalidOid,
                };
                p.update_subscript = true;
                require_super = true;
            }
            attr @ ("input" | "output" | "internallength" | "passedbyvalue" | "alignment"
            | "like" | "category" | "preferred" | "default" | "element" | "delimiter"
            | "collatable") => {
                return Err(type_attribute_error(attr, "cannot be changed"));
            }
            attr => return Err(type_attribute_error(attr, "not recognized")),
        }
    }

    if require_super {
        if !superuser::superuser()? {
            return Err(Box::new(
                PgError::new(ERROR, "must be superuser to alter a type".to_string())
                    .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
            ));
        }
    } else if !aclchk::object_ownercheck(TYPE_RELATION_ID, type_oid, miscinit::GetUserId())? {
        return Err(must_be_owner_of_type(type_oid)?);
    }

    if row.typtype != pg_type::TYPTYPE_BASE {
        return Err(not_a_base_type(type_oid)?);
    }
    if row.typelem != InvalidOid && row.typsubscript == F_ARRAY_SUBSCRIPT_HANDLER {
        return Err(not_a_base_type(type_oid)?);
    }

    AlterTypeRecurse(mcx, type_oid, false, &mut p)?;

    Ok(ObjectAddress::set(TYPE_RELATION_ID, type_oid))
}

fn AlterTypeRecurse<'mcx>(
    mcx: Mcx<'mcx>,
    type_oid: Oid,
    is_implicit_array: bool,
    p: &mut AlterTypeRecurseParams,
) -> PgResult<()> {
    stack_depth::check_stack_depth()?;

    let mut row = fetch_type_row(mcx, type_oid)?;
    update_type_row(mcx, type_oid, |values, _nulls, replace| {
        let mut set = |anum: AttrNumber, d: Datum| {
            let ix = (anum - 1) as usize;
            values[ix] = d;
            replace[ix] = true;
        };
        if p.update_storage {
            set(Anum_pg_type_typstorage, Datum::from_char(p.storage));
        }
        if p.update_receive {
            set(Anum_pg_type_typreceive, Datum::from_oid(p.receive_oid));
        }
        if p.update_send {
            set(Anum_pg_type_typsend, Datum::from_oid(p.send_oid));
        }
        if p.update_typmodin {
            set(Anum_pg_type_typmodin, Datum::from_oid(p.typmodin_oid));
        }
        if p.update_typmodout {
            set(Anum_pg_type_typmodout, Datum::from_oid(p.typmodout_oid));
        }
        if p.update_analyze {
            set(Anum_pg_type_typanalyze, Datum::from_oid(p.analyze_oid));
        }
        if p.update_subscript {
            set(Anum_pg_type_typsubscript, Datum::from_oid(p.subscript_oid));
        }
        Ok(())
    })?;
    if p.update_receive {
        row.typreceive = p.receive_oid;
    }
    if p.update_send {
        row.typsend = p.send_oid;
    }
    if p.update_typmodin {
        row.typmodin = p.typmodin_oid;
    }
    if p.update_typmodout {
        row.typmodout = p.typmodout_oid;
    }
    if p.update_analyze {
        row.typanalyze = p.analyze_oid;
    }
    if p.update_subscript {
        row.typsubscript = p.subscript_oid;
    }

    rebuild_alter_type_dependencies(mcx, type_oid, &row, is_implicit_array)?;

    if !is_implicit_array
        && (p.update_typmodin || p.update_typmodout)
        && row.typarray != InvalidOid
    {
        let mut arrparams = AlterTypeRecurseParams {
            update_typmodin: p.update_typmodin,
            update_typmodout: p.update_typmodout,
            typmodin_oid: p.typmodin_oid,
            typmodout_oid: p.typmodout_oid,
            ..Default::default()
        };
        AlterTypeRecurse(mcx, row.typarray, true, &mut arrparams)?;
    }

    // Domains inherit neither typreceive (F_DOMAIN_RECV), typmods, nor
    // subscripting.
    p.update_receive = false;
    p.update_typmodin = false;
    p.update_typmodout = false;
    p.update_subscript = false;
    if !(p.update_storage || p.update_send || p.update_analyze) {
        return Ok(());
    }

    let rel = table::table_open(mcx, TYPE_RELATION_ID, RowExclusiveLock)?;
    let keys = [oid_key(Anum_pg_type_typbasetype, type_oid)];
    let mut scan = genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &keys)?;
    loop {
        let desc = rel.descr();
        let (domain_oid, typtype) = {
            let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else { break };
            let mut isnull = false;
            // SAFETY: fixed NOT NULL pg_type columns under its descriptor.
            unsafe {
                (
                    types_tuple::heap_getattr(tup, pg_type::Anum_pg_type_oid as i32, desc, &mut isnull)
                        .as_oid(),
                    types_tuple::heap_getattr(tup, Anum_pg_type_typtype as i32, desc, &mut isnull)
                        .as_i8(),
                )
            }
        };
        if typtype != TYPTYPE_DOMAIN {
            continue;
        }
        AlterTypeRecurse(mcx, domain_oid, false, p)?;
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(RowExclusiveLock)
}

// GenerateTypeDependencies (pg_type.c) rebuild arm as invoked from
// AlterTypeRecurse: relationKind 0 (composites rejected), dependent iff
// implicit array, defaultExpr/typacl re-read from the row.
fn rebuild_alter_type_dependencies<'mcx>(
    mcx: Mcx<'mcx>,
    type_oid: Oid,
    row: &TypeRow,
    is_implicit_array: bool,
) -> PgResult<()> {
    pg_depend::deleteDependencyRecordsFor(mcx, TYPE_RELATION_ID, type_oid, true)?;
    pg_shdepend::deleteSharedDependencyRecordsFor(mcx, TYPE_RELATION_ID, type_oid, 0)?;

    let myself = ObjectAddress::set(TYPE_RELATION_ID, type_oid);
    let mut addrs_normal = [ObjectAddress::set(InvalidOid, InvalidOid); 12];
    let mut n = 0;
    if !is_implicit_array {
        addrs_normal[n] = ObjectAddress::set(types_core::NAMESPACE_RELATION_ID, row.typnamespace);
        n += 1;
        pg_depend::recordDependencyOnOwner(mcx, TYPE_RELATION_ID, type_oid, row.typowner)?;
        assert!(
            row.typacl_isnull,
            "AlterTypeRecurse: non-null typacl dependency rebuild (recordDependencyOnNewAcl) unported"
        );
    }
    pg_depend::recordDependencyOnCurrentExtension(mcx, &myself, true)?;
    const PROCEDURE_RELATION_ID: Oid = 1255;
    for proc in [
        row.typinput,
        row.typoutput,
        row.typreceive,
        row.typsend,
        row.typmodin,
        row.typmodout,
        row.typanalyze,
        row.typsubscript,
    ] {
        if proc != InvalidOid {
            addrs_normal[n] = ObjectAddress::set(PROCEDURE_RELATION_ID, proc);
            n += 1;
        }
    }
    if row.typbasetype != InvalidOid {
        addrs_normal[n] = ObjectAddress::set(TYPE_RELATION_ID, row.typbasetype);
        n += 1;
    }
    const COLLATION_RELATION_ID: Oid = 3456;
    if row.typcollation != InvalidOid && row.typcollation != DEFAULT_COLLATION_OID {
        addrs_normal[n] = ObjectAddress::set(COLLATION_RELATION_ID, row.typcollation);
        n += 1;
    }
    pg_depend::record_object_address_dependencies(
        mcx,
        &myself,
        &mut addrs_normal[..n],
        DependencyType::Normal,
    )?;
    if row.typelem != InvalidOid {
        let referenced = ObjectAddress::set(TYPE_RELATION_ID, row.typelem);
        let behavior = if is_implicit_array {
            DependencyType::Internal
        } else {
            DependencyType::Normal
        };
        pg_depend::recordDependencyOn(mcx, &myself, &referenced, behavior)?;
    }
    if let Some(bin) = &row.typdefaultbin {
        let expr = readfuncs::stringToNode(mcx, bin)?;
        catalog_dependency::recordDependencyOnExpr(
            mcx,
            &myself,
            expr,
            &NodeList::nil(),
            DependencyType::Normal,
        )?;
    }
    Ok(())
}

#[track_caller]
#[cold]
#[inline(never)]
fn type_attribute_error(attr: &str, tail: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, format!("type attribute \"{attr}\" {tail}"))
            .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR),
    )
}

#[cold]
#[inline(never)]
fn not_a_base_type(type_oid: Oid) -> PgResult<Box<PgError>> {
    let name = format_type::format_type_be(type_oid)?;
    Ok(Box::new(
        PgError::new(ERROR, format!("{name} is not a base type"))
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
    ))
}
