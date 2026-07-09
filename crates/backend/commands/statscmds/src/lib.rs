#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::fmgr::{F_BOOLEQ, F_OIDEQ};
use types_core::primitive::RegProcedure;
use types_core::{AttrNumber, InvalidOid, Oid};
use types_error::{ErrorLocation, PgError, PgResult, ERROR, NOTICE};
use types_nodes::rawnodes::{AlterStatsStmt, CreateStatsStmt, StatsElem};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::HeapTupleData;

use elog::ereport;

pub const StatisticExtRelationId: Oid = 3381;
pub const StatisticExtDataRelationId: Oid = 3429;
pub const StatisticExtOidIndexId: Oid = 3380;
pub const StatisticExtNameIndexId: Oid = 3997;
pub const StatisticExtDataStxoidInhIndexId: Oid = 3433;

const RELATION_RELATION_ID: Oid = 1259;
const NAMESPACE_RELATION_ID: Oid = 2615;
const INT2OID: Oid = 21;
const CHAROID: Oid = 18;

const Natts_pg_statistic_ext: usize = 9;
const Anum_oid: usize = 1;
const Anum_stxrelid: usize = 2;
const Anum_stxname: usize = 3;
const Anum_stxnamespace: usize = 4;
const Anum_stxowner: usize = 5;
const Anum_stxkeys: usize = 6;
const Anum_stxstattarget: usize = 7;
const Anum_stxkind: usize = 8;
const Anum_stxexprs: usize = 9;

const STATS_MAX_DIMENSIONS: usize = 8;
const NAMEDATALEN: usize = 64;

const STATS_EXT_NDISTINCT: u8 = b'd';
const STATS_EXT_DEPENDENCIES: u8 = b'f';
const STATS_EXT_MCV: u8 = b'm';
const STATS_EXT_EXPRESSIONS: u8 = b'e';

use types_rel::{RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION};

const ShareUpdateExclusiveLock: types_rel::LOCKMODE = 4;
const RowExclusiveLock: types_rel::LOCKMODE = 3;
const NoLock: types_rel::LOCKMODE = 0;

use cache_syscache::cacheinfo::STATEXTNAMENSP;

// get_relkind_objtype (objectaddress.c)
fn get_relkind_objtype(relkind: u8) -> types_nodes::parsenodes::ObjectType {
    use types_nodes::parsenodes::ObjectType::*;
    match relkind {
        RELKIND_RELATION | RELKIND_PARTITIONED_TABLE => OBJECT_TABLE,
        types_rel::RELKIND_INDEX | types_rel::RELKIND_PARTITIONED_INDEX => OBJECT_INDEX,
        types_rel::RELKIND_SEQUENCE => OBJECT_SEQUENCE,
        types_rel::RELKIND_VIEW => OBJECT_VIEW,
        RELKIND_MATVIEW => OBJECT_MATVIEW,
        RELKIND_FOREIGN_TABLE => OBJECT_FOREIGN_TABLE,
        _ => OBJECT_TABLE,
    }
}

fn loc(funcname: &str) -> ErrorLocation {
    ErrorLocation {
        filename: Some("statscmds.c".into()),
        lineno: 0,
        funcname: Some(funcname.into()),
    }
}

fn err(code: types_error::SqlState, msg: String) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(code))
}

#[cold]
fn unported(what: &str) -> ! {
    panic!("statscmds.c: {what}")
}

fn eq_key(attno: usize, func: RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}

fn name_arg<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<PgVec<'mcx, u8>> {
    assert!(name.len() < NAMEDATALEN, "statistics name too long: {name:?}");
    let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, NAMEDATALEN)?;
    mcx::vec_append_bytes(&mut buf, name.as_bytes())?;
    mcx::vec_append_bytes(&mut buf, &[0u8; 64][..NAMEDATALEN - name.len()])?;
    Ok(buf)
}

fn statext_name_exists(name: &str, namespaceid: Oid) -> PgResult<bool> {
    cache_syscache::SearchSysCacheExists(
        STATEXTNAMENSP,
        cache_syscache::SysCacheKey::Str(name),
        cache_syscache::SysCacheKey::Value(Datum::from_oid(namespaceid)),
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
    )
}

// int2vector image: 1-D int2 array, lbound 0 (buildint2vector).
fn int2vector_image<'mcx>(mcx: Mcx<'mcx>, vals: &[i16]) -> PgResult<PgVec<'mcx, u8>> {
    let len = 4 + 20 + vals.len() * 2;
    let mut out: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, len)?;
    mcx::vec_append_bytes(&mut out, &((len as u32) << 2).to_ne_bytes())?;
    mcx::vec_append_bytes(&mut out, &1i32.to_ne_bytes())?;
    mcx::vec_append_bytes(&mut out, &0i32.to_ne_bytes())?;
    mcx::vec_append_bytes(&mut out, &INT2OID.to_ne_bytes())?;
    mcx::vec_append_bytes(&mut out, &(vals.len() as i32).to_ne_bytes())?;
    mcx::vec_append_bytes(&mut out, &0i32.to_ne_bytes())?;
    for &v in vals {
        mcx::vec_append_bytes(&mut out, &v.to_ne_bytes())?;
    }
    Ok(out)
}

// text varlena image (CStringGetTextDatum).
fn text_image<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<PgVec<'mcx, u8>> {
    let len = 4 + s.len();
    let mut out: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, len)?;
    mcx::vec_append_bytes(&mut out, &((len as u32) << 2).to_ne_bytes())?;
    mcx::vec_append_bytes(&mut out, s.as_bytes())?;
    Ok(out)
}

// 1-D "char"[] image, lbound 1 (construct_array_builtin CHAROID).
fn chararray_image<'mcx>(mcx: Mcx<'mcx>, vals: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let len = 4 + 20 + vals.len();
    let mut out: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, len)?;
    mcx::vec_append_bytes(&mut out, &((len as u32) << 2).to_ne_bytes())?;
    mcx::vec_append_bytes(&mut out, &1i32.to_ne_bytes())?;
    mcx::vec_append_bytes(&mut out, &0i32.to_ne_bytes())?;
    mcx::vec_append_bytes(&mut out, &CHAROID.to_ne_bytes())?;
    mcx::vec_append_bytes(&mut out, &(vals.len() as i32).to_ne_bytes())?;
    mcx::vec_append_bytes(&mut out, &1i32.to_ne_bytes())?;
    mcx::vec_append_bytes(&mut out, vals)?;
    Ok(out)
}

// check_rights=false on ALTER TABLE's AT_ReAddStatistics rebuild
// (tablecmds.c:9693 passes !is_rebuild).
pub fn CreateStatistics<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateStatsStmt<'mcx>,
    check_rights: bool,
) -> PgResult<pg_depend::ObjectAddress> {
    let mut attnums: [i16; STATS_MAX_DIMENSIONS] = [0; STATS_MAX_DIMENSIONS];
    let mut nattnums = 0usize;
    let stxowner = miscinit_seams::get_user_id::call();

    if stmt.relations.iter().count() != 1 {
        return Err(err(
            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
            "only a single relation is allowed in CREATE STATISTICS".into(),
        ));
    }
    let rln = stmt.relations.iter().next().expect("relation");
    let Some(rv) = rln.as_range_var() else {
        return Err(err(
            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
            "only a single relation is allowed in CREATE STATISTICS".into(),
        ));
    };
    let rvv = rel_vocab::RangeVar {
        catalogname: rv.catalogname,
        schemaname: rv.schemaname,
        relname: rv.relname.expect("relname"),
        inh: rv.inh,
        relpersistence: rv.relpersistence as u8,
        location: rv.location,
    };
    let rel = relation_seams::relation_openrv::call(mcx, &rvv, ShareUpdateExclusiveLock)?;
    let relid = rel.rd_id;

    let relkind = rel.rd_rel.relkind;
    if !matches!(
        relkind,
        RELKIND_RELATION | RELKIND_MATVIEW | RELKIND_FOREIGN_TABLE | RELKIND_PARTITIONED_TABLE
    ) {
        let detail = pg_class_seams::errdetail_relkind_not_supported::call(relkind)?;
        return Err(Box::new(
            PgError::new(ERROR, format!("cannot define statistics for relation \"{}\"", rel.name()))
                .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                .with_detail(detail),
        ));
    }

    if !aclchk::object_ownercheck(RELATION_RELATION_ID, relid, stxowner)? {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NOT_OWNER,
            get_relkind_objtype(relkind),
            rel.name(),
        )?;
    }

    if !init_small::globals::allowSystemTableMods()
        && (catalog::IsCatalogRelationOid(relid)
            || catalog::IsToastNamespace(rel.rd_rel.relnamespace))
    {
        return Err(err(
            types_error::ERRCODE_INSUFFICIENT_PRIVILEGE,
            format!("permission denied: \"{}\" is a system catalog", rel.name()),
        ));
    }

    let (namespace_id, namestr): (Oid, String) = if !stmt.defnames.is_nil() {
        let mut parts: [&str; 4] = [""; 4];
        let nparts = stmt.defnames.len().min(4);
        for (i, n) in stmt.defnames.iter().take(4).enumerate() {
            parts[i] = n.as_string().expect("name String").sval;
        }
        let (schemaname, name) = catalog_namespace::DeconstructQualifiedName(&parts[..nparts])?;
        let nsp_rv = rel_vocab::RangeVar {
            catalogname: None,
            schemaname,
            relname: name,
            inh: true,
            relpersistence: b'p',
            location: -1,
        };
        // QualifiedNameGetCreationNamespace == the RangeVar leg for a
        // permanent object with no catalog name.
        let nsp = catalog_namespace::RangeVarGetCreationNamespace(mcx, &nsp_rv)?;
        (nsp, name.to_string())
    } else {
        let nsp = rel.rd_rel.relnamespace;
        let name2 = ChooseExtendedStatisticNameAddition(&stmt.exprs);
        let name = ChooseExtendedStatisticName(rel.name(), &name2, "stat", nsp)?;
        (nsp, name)
    };

    if check_rights {
        let aclresult = aclchk::object_aclcheck(
            types_core::catalog::NAMESPACE_RELATION_ID,
            namespace_id,
            miscinit_seams::get_user_id::call(),
            types_nodes::parsenodes::ACL_CREATE,
        )?;
        if aclresult != aclchk::ACLCHECK_OK {
            let nspname = lsyscache::get_namespace_name(mcx, namespace_id)?;
            aclchk::aclcheck_error(
                aclresult,
                types_nodes::parsenodes::ObjectType::OBJECT_SCHEMA,
                nspname.as_ref().map(|s| s.as_str()).unwrap_or(""),
            )?;
        }
    }

    if statext_name_exists(&namestr, namespace_id)? {
        if stmt.if_not_exists {
            ereport(NOTICE)
                .errcode(types_error::ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!("statistics object \"{namestr}\" already exists, skipping"))
                .finish(loc("CreateStatistics"))?;
            rel.close(NoLock)?;
            return Ok(pg_depend::ObjectAddress::set(InvalidOid, InvalidOid));
        }
        return Err(err(
            types_error::ERRCODE_DUPLICATE_OBJECT,
            format!("statistics object \"{namestr}\" already exists"),
        ));
    }

    let numcols = stmt.exprs.iter().count();
    if numcols > STATS_MAX_DIMENSIONS {
        return Err(err(
            types_error::ERRCODE_TOO_MANY_COLUMNS,
            format!("cannot have more than {STATS_MAX_DIMENSIONS} columns in statistics"),
        ));
    }

    let tupdesc = rel.descr();
    let mut stxexprs: PgVec<'_, types_nodes::Node<'_>> = PgVec::new_in(mcx);
    for selem_node in stmt.exprs.iter() {
        let selem: &StatsElem<'_> =
            selem_node.as_variant::<StatsElem>().expect("stats_param is a StatsElem");
        if let Some(attname) = selem.name {
            let i = (1..=tupdesc.natts).find(|&i| {
                let a = tupdesc.attr(i as usize - 1);
                !a.attisdropped && a.attname.name_str() == attname.as_bytes()
            });
            let Some(i) = i else {
                if catalog_heap::SystemAttributeByName(attname).is_some() {
                    return Err(err(
                        types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                        "statistics creation on system columns is not supported".into(),
                    ));
                }
                return Err(err(
                    types_error::ERRCODE_UNDEFINED_COLUMN,
                    format!("column \"{attname}\" does not exist"),
                ));
            };
            let att = tupdesc.attr(i as usize - 1);
            if att.attgenerated == b'v' as i8 {
                return Err(err(
                    types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                    "statistics creation on virtual generated columns is not supported".into(),
                ));
            }
            let entry = typcache::lookup_type_cache(att.atttypid, typcache::TYPECACHE_LT_OPR)?;
            if entry.lt_opr() == InvalidOid {
                let typname = format_type::format_type_be(att.atttypid)?;
                return Err(err(
                    types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                    format!(
                        "column \"{attname}\" cannot be used in statistics because its type {typname} has no default btree operator class"
                    ),
                ));
            }
            attnums[nattnums] = i as i16;
            nattnums += 1;
        } else if let Some(var) = selem.expr.expect("StatsElem without name or expr").as_var() {
            if var.varattno <= 0 {
                return Err(err(
                    types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                    "statistics creation on system columns is not supported".into(),
                ));
            }
            if lsyscache::get_attgenerated(relid, var.varattno)? == b'v' as i8 {
                return Err(err(
                    types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                    "statistics creation on virtual generated columns is not supported".into(),
                ));
            }
            let entry = typcache::lookup_type_cache(var.vartype, typcache::TYPECACHE_LT_OPR)?;
            if entry.lt_opr() == InvalidOid {
                let attname = tupdesc.attr(var.varattno as usize - 1).attname;
                let typname = format_type::format_type_be(var.vartype)?;
                return Err(err(
                    types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                    format!(
                        "column \"{}\" cannot be used in statistics because its type {typname} has no default btree operator class",
                        core::str::from_utf8(attname.name_str()).unwrap_or("?")
                    ),
                ));
            }
            attnums[nattnums] = var.varattno;
            nattnums += 1;
        } else {
            let expr = selem.expr.expect("StatsElem expr");
            let mut exprattnums = types_nodes::Bitmapset::empty();
            vars::pull_varattnos(mcx, expr, 1, &mut exprattnums)?;
            for (w, word) in exprattnums.as_words().iter().enumerate() {
                let mut word = *word;
                while word != 0 {
                    let k = (w * 64) as i32 + word.trailing_zeros() as i32;
                    word &= word - 1;
                    let attnum = k + types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
                    if attnum <= 0 {
                        return Err(err(
                            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                            "statistics creation on system columns is not supported".into(),
                        ));
                    }
                    if lsyscache::get_attgenerated(relid, attnum as i16)? == b'v' as i8 {
                        return Err(err(
                            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                            "statistics creation on virtual generated columns is not supported"
                                .into(),
                        ));
                    }
                }
            }
            if stmt.exprs.len() > 1 {
                let atttype = nodes_core::node_funcs::expr_type(expr);
                let entry = typcache::lookup_type_cache(atttype, typcache::TYPECACHE_LT_OPR)?;
                if entry.lt_opr() == InvalidOid {
                    let typname = format_type::format_type_be(atttype)?;
                    return Err(err(
                        types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                        format!(
                            "expression cannot be used in multivariate statistics because its type {typname} has no default btree operator class"
                        ),
                    ));
                }
            }
            stxexprs.push(expr);
        }
    }

    if numcols == 1 && stxexprs.len() == 1 && !stmt.stat_types.is_nil() {
        return Err(err(
            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
            "when building statistics on a single expression, statistics kinds may not be specified"
                .into(),
        ));
    }

    let mut build_ndistinct = false;
    let mut build_dependencies = false;
    let mut build_mcv = false;
    let mut requested_type = false;
    for type_node in stmt.stat_types.iter() {
        let t = type_node.as_string().expect("statistics kind String").sval;
        match t {
            "ndistinct" => {
                build_ndistinct = true;
                requested_type = true;
            }
            "dependencies" => {
                build_dependencies = true;
                requested_type = true;
            }
            "mcv" => {
                build_mcv = true;
                requested_type = true;
            }
            other => {
                return Err(err(
                    types_error::ERRCODE_SYNTAX_ERROR,
                    format!("unrecognized statistics kind \"{other}\""),
                ));
            }
        }
    }
    if !requested_type && numcols >= 2 {
        build_ndistinct = true;
        build_dependencies = true;
        build_mcv = true;
    }
    let build_expressions = !stxexprs.is_empty();
    if numcols < 2 && stxexprs.len() != 1 {
        return Err(err(
            types_error::ERRCODE_INVALID_OBJECT_DEFINITION,
            "extended statistics require at least 2 columns".into(),
        ));
    }

    attnums[..nattnums].sort_unstable();
    for i in 1..nattnums {
        if attnums[i] == attnums[i - 1] {
            return Err(err(
                types_error::ERRCODE_DUPLICATE_COLUMN,
                "duplicate column name in statistics definition".into(),
            ));
        }
    }
    for (i, &e1) in stxexprs.iter().enumerate() {
        for (j, &e2) in stxexprs.iter().enumerate() {
            if i != j && types_nodes::equal(e1, e2) {
                return Err(err(
                    types_error::ERRCODE_DUPLICATE_COLUMN,
                    "duplicate expression in statistics definition".into(),
                ));
            }
        }
    }

    let mut stxkind: PgVec<'_, u8> = mcx::vec_with_capacity_in(mcx, 4)?;
    if build_ndistinct {
        stxkind.push(STATS_EXT_NDISTINCT);
    }
    if build_dependencies {
        stxkind.push(STATS_EXT_DEPENDENCIES);
    }
    if build_mcv {
        stxkind.push(STATS_EXT_MCV);
    }
    if build_expressions {
        stxkind.push(STATS_EXT_EXPRESSIONS);
    }

    let exprs_img: Option<PgVec<'_, u8>> = if stxexprs.is_empty() {
        None
    } else {
        let list = types_nodes::NodeList::from_slice(mcx, &stxexprs)?;
        let list_node = types_nodes::Node::mk_list(mcx, list)?;
        let s = outfuncs::nodeToString(mcx, list_node)?;
        Some(text_image(mcx, s.as_str())?)
    };

    let statrel = table::table_open(mcx, StatisticExtRelationId, RowExclusiveLock)?;

    let statoid = catalog::GetNewOidWithIndex(mcx, &statrel, StatisticExtOidIndexId, Anum_oid as AttrNumber)?;

    let stxname = name_arg(mcx, &namestr)?;
    let stxkeys = int2vector_image(mcx, &attnums[..nattnums])?;
    let stxkind_img = chararray_image(mcx, &stxkind)?;

    let mut values = [Datum::null(); Natts_pg_statistic_ext];
    let mut nulls = [false; Natts_pg_statistic_ext];
    values[Anum_oid - 1] = Datum::from_oid(statoid);
    values[Anum_stxrelid - 1] = Datum::from_oid(relid);
    values[Anum_stxname - 1] = Datum::from_usize(stxname.as_ptr() as usize);
    values[Anum_stxnamespace - 1] = Datum::from_oid(namespace_id);
    values[Anum_stxowner - 1] = Datum::from_oid(stxowner);
    values[Anum_stxkeys - 1] = Datum::from_usize(stxkeys.as_ptr() as usize);
    nulls[Anum_stxstattarget - 1] = true;
    values[Anum_stxkind - 1] = Datum::from_usize(stxkind_img.as_ptr() as usize);
    match &exprs_img {
        Some(img) => values[Anum_stxexprs - 1] = Datum::from_usize(img.as_ptr() as usize),
        None => nulls[Anum_stxexprs - 1] = true,
    }

    let mut htup = heaptuple::heap_form_tuple(mcx, statrel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &statrel, &mut htup)?;

    statrel.close(RowExclusiveLock)?;

    inval::invalidate::CacheInvalidateRelcache(&rel)?;

    rel.close(NoLock)?;

    let myself = pg_depend::ObjectAddress::set(StatisticExtRelationId, statoid);
    for &attnum in &attnums[..nattnums] {
        let parent = pg_depend::ObjectAddress::sub_set(RELATION_RELATION_ID, relid, attnum as i32);
        pg_depend::recordDependencyOn(mcx, &myself, &parent, pg_depend::DependencyType::Auto)?;
    }
    if nattnums == 0 {
        let parent = pg_depend::ObjectAddress::set(RELATION_RELATION_ID, relid);
        pg_depend::recordDependencyOn(mcx, &myself, &parent, pg_depend::DependencyType::Auto)?;
    }
    if !stxexprs.is_empty() {
        let list = types_nodes::NodeList::from_slice(mcx, &stxexprs)?;
        let exprs_node = types_nodes::Node::mk_list(mcx, list)?;
        pg_depend::recordDependencyOnSingleRelExpr(
            mcx,
            &myself,
            exprs_node,
            relid,
            pg_depend::DependencyType::Normal,
            pg_depend::DependencyType::Auto,
            false,
        )?;
    }
    let parent = pg_depend::ObjectAddress::set(NAMESPACE_RELATION_ID, namespace_id);
    pg_depend::recordDependencyOn(mcx, &myself, &parent, pg_depend::DependencyType::Normal)?;
    pg_depend::recordDependencyOnOwner(mcx, StatisticExtRelationId, statoid, stxowner)?;

    if let Some(comment) = stmt.stxcomment {
        commands_comment::CreateComments(mcx, statoid, StatisticExtRelationId, 0, Some(comment))?;
    }

    Ok(myself)
}

const MAX_STATISTICS_TARGET: i64 = 10000;

// AlterStatistics (statscmds.c:638). DIVERGENCE: the old tuple comes from a
// systable scan on the oid index instead of SearchSysCache1(STATEXTOID); the
// update is identical.
pub fn AlterStatistics<'mcx>(mcx: Mcx<'mcx>, stmt: &AlterStatsStmt<'_>) -> PgResult<()> {
    let mut newtarget: i64 = 0;
    let mut newtarget_default = true;
    if let Some(t) = stmt.stxstattarget {
        let ival = i64::from(t.as_integer().expect("stxstattarget is an Integer").ival);
        // -1 was used in previous versions for the default setting
        if ival != -1 {
            newtarget = ival;
            newtarget_default = false;
        }
    }
    if !newtarget_default {
        if newtarget < 0 {
            return Err(err(
                types_error::ERRCODE_INVALID_PARAMETER_VALUE,
                format!("statistics target {newtarget} is too low"),
            ));
        } else if newtarget > MAX_STATISTICS_TARGET {
            newtarget = MAX_STATISTICS_TARGET;
            ereport(types_error::WARNING)
                .errcode(types_error::ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!("lowering statistics target to {newtarget}"))
                .finish(loc("AlterStatistics"))?;
        }
    }

    let parts: PgVec<'_, &str> = {
        let mut v = PgVec::new_in(mcx);
        for n in stmt.defnames.iter() {
            v.push(n.as_string().expect("name String").sval);
        }
        v
    };
    let stxoid = get_statistics_object_oid(&parts, stmt.missing_ok)?;

    if stxoid == InvalidOid {
        debug_assert!(stmt.missing_ok);
        let (schemaname, statname) = catalog_namespace::DeconstructQualifiedName(&parts)?;
        let msg = match schemaname {
            Some(s) => {
                format!("statistics object \"{s}.{statname}\" does not exist, skipping")
            }
            None => format!("statistics object \"{statname}\" does not exist, skipping"),
        };
        ereport(NOTICE).errmsg(msg).finish(loc("AlterStatistics"))?;
        return Ok(());
    }

    let rel = table::table_open(mcx, StatisticExtRelationId, RowExclusiveLock)?;

    let keys = [eq_key(Anum_oid, F_OIDEQ, Datum::from_oid(stxoid))];
    let mut scan =
        genam::systable_beginscan(mcx, &rel, StatisticExtOidIndexId, true, None, &keys)?;
    let Some(oldtup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(err(
            types_error::ERRCODE_INTERNAL_ERROR,
            format!("cache lookup failed for extended statistics object {stxoid}"),
        ));
    };

    if !aclchk::object_ownercheck(
        StatisticExtRelationId,
        stxoid,
        miscinit_seams::get_user_id::call(),
    )? {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NOT_OWNER,
            types_nodes::parsenodes::ObjectType::OBJECT_STATISTIC_EXT,
            &parts.join("."),
        )?;
    }

    let mut repl_val = [Datum::null(); Natts_pg_statistic_ext];
    let mut repl_null = [false; Natts_pg_statistic_ext];
    let mut repl_repl = [false; Natts_pg_statistic_ext];
    repl_repl[Anum_stxstattarget - 1] = true;
    if !newtarget_default {
        repl_val[Anum_stxstattarget - 1] = Datum::from_i16(newtarget as i16);
    } else {
        repl_null[Anum_stxstattarget - 1] = true;
    }

    let desc = rel.descr();
    let mut newtup = heaptuple::heap_modify_tuple(mcx, oldtup, desc, &repl_val, &repl_null, &repl_repl)?;
    let otid = oldtup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut newtup)?;

    rel.close(RowExclusiveLock)?;
    Ok(())
}

fn ChooseExtendedStatisticName(
    name1: &str,
    name2: &str,
    label: &str,
    namespaceid: Oid,
) -> PgResult<String> {
    let mut pass = 0;
    let mut modlabel = label.to_string();
    loop {
        let candidate = make_object_name(name1, name2, &modlabel);
        if !statext_name_exists(&candidate, namespaceid)? {
            return Ok(candidate);
        }
        pass += 1;
        modlabel = format!("{label}{pass}");
    }
}

// makeObjectName (indexcmds.c); names are valid UTF-8, so pg_mbcliplen is a
// char-boundary clip.
fn make_object_name(name1: &str, name2: &str, label: &str) -> String {
    let mut overhead = label.len() + 1;
    if !name2.is_empty() {
        overhead += 1;
    }
    let availchars = NAMEDATALEN - 1 - overhead;
    let mut name1chars = name1.len();
    let mut name2chars = name2.len();
    while name1chars + name2chars > availchars {
        if name1chars > name2chars {
            name1chars -= 1;
        } else {
            name2chars -= 1;
        }
    }
    fn clip(s: &str, mut n: usize) -> &str {
        while !s.is_char_boundary(n) {
            n -= 1;
        }
        &s[..n]
    }
    let mut s = String::with_capacity(NAMEDATALEN);
    s.push_str(clip(name1, name1chars));
    if !name2.is_empty() {
        s.push('_');
        s.push_str(clip(name2, name2chars));
    }
    s.push('_');
    s.push_str(label);
    s
}

fn ChooseExtendedStatisticNameAddition(exprs: &types_nodes::NodeList<'_>) -> String {
    let mut buf = String::new();
    for selem_node in exprs.iter() {
        let Some(selem) = selem_node.as_variant::<StatsElem>() else {
            continue;
        };
        let name = selem.name.unwrap_or("expr");
        if !buf.is_empty() {
            buf.push('_');
        }
        let mut copy_len = name.len().min(NAMEDATALEN - 1);
        while !name.is_char_boundary(copy_len) {
            copy_len -= 1;
        }
        buf.push_str(&name[..copy_len]);
        if buf.len() >= NAMEDATALEN {
            break;
        }
    }
    buf
}

fn find_data_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    statsOid: Oid,
    inh: bool,
) -> PgResult<Option<types_tuple::ItemPointerData>> {
    let keys = [
        eq_key(1, F_OIDEQ, Datum::from_oid(statsOid)),
        eq_key(2, F_BOOLEQ, Datum::from_bool(inh)),
    ];
    let mut scan =
        genam::systable_beginscan(mcx, rel, StatisticExtDataStxoidInhIndexId, true, None, &keys)?;
    let found = genam::systable_getnext(mcx, &mut scan)?.map(|t| t.t_self);
    genam::systable_endscan(mcx, scan)?;
    Ok(found)
}

// StatisticsGetRelation (statscmds.c:937).
pub fn StatisticsGetRelation(statId: Oid, missing_ok: bool) -> PgResult<Oid> {
    use cache_syscache::{cacheinfo::STATEXTOID, ReleaseSysCache, SysCacheGetAttr, SysCacheKey};
    let Some(tup) = cache_syscache::SearchSysCache1(
        STATEXTOID,
        SysCacheKey::Value(Datum::from_oid(statId)),
    )?
    else {
        if missing_ok {
            return Ok(InvalidOid);
        }
        return Err(err(
            types_error::ERRCODE_INTERNAL_ERROR,
            format!("cache lookup failed for statistics object {statId}"),
        ));
    };
    let (d, isnull) = SysCacheGetAttr(STATEXTOID, &tup, Anum_stxrelid as i32)?;
    debug_assert!(!isnull);
    let result = d.as_oid();
    ReleaseSysCache(tup);
    Ok(result)
}

pub fn RemoveStatisticsDataById(mcx: Mcx<'_>, statsOid: Oid, inh: bool) -> PgResult<()> {
    let relation = table::table_open(mcx, StatisticExtDataRelationId, RowExclusiveLock)?;
    if let Some(tid) = find_data_tuple(mcx, &relation, statsOid, inh)? {
        catalog_indexing::CatalogTupleDelete(&relation, &tid)?;
    }
    relation.close(RowExclusiveLock)?;
    Ok(())
}

pub fn RemoveStatisticsById(mcx: Mcx<'_>, statsOid: Oid) -> PgResult<()> {
    let relation = table::table_open(mcx, StatisticExtRelationId, RowExclusiveLock)?;

    let keys = [eq_key(Anum_oid, F_OIDEQ, Datum::from_oid(statsOid))];
    let mut scan =
        genam::systable_beginscan(mcx, &relation, StatisticExtOidIndexId, true, None, &keys)?;
    let found: Option<(types_tuple::ItemPointerData, Oid)> = {
        let desc = relation.descr();
        genam::systable_getnext(mcx, &mut scan)?.map(|t: &HeapTupleData<'_>| {
            let mut isnull = false;
            // SAFETY: pg_statistic_ext tuple under its own descriptor.
            let relid_d =
                unsafe { types_tuple::heap_getattr(t, Anum_stxrelid as i32, desc, &mut isnull) };
            (t.t_self, relid_d.as_oid())
        })
    };
    genam::systable_endscan(mcx, scan)?;
    let Some((tid, relid)) = found else {
        return Err(err(
            types_error::ERRCODE_INTERNAL_ERROR,
            format!("cache lookup failed for statistics object {statsOid}"),
        ));
    };

    let rel = table::table_open(mcx, relid, ShareUpdateExclusiveLock)?;

    RemoveStatisticsDataById(mcx, statsOid, true)?;
    RemoveStatisticsDataById(mcx, statsOid, false)?;

    inval::invalidate::CacheInvalidateRelcacheByRelid(relid)?;

    catalog_indexing::CatalogTupleDelete(&relation, &tid)?;

    rel.close(NoLock)?;
    relation.close(RowExclusiveLock)?;
    Ok(())
}

// get_statistics_object_oid (statscmds.c): explicit schema or first
// search-path hit.
pub fn get_statistics_object_oid(names: &[&str], missing_ok: bool) -> PgResult<Oid> {
    let (schemaname, stats_name) = catalog_namespace::DeconstructQualifiedName(names)?;
    let mut stats_oid = InvalidOid;
    if let Some(schemaname) = schemaname {
        let namespace_id = catalog_namespace::LookupExplicitNamespace(schemaname, missing_ok)?;
        if namespace_id != InvalidOid {
            stats_oid = statext_name_lookup(stats_name, namespace_id)?;
        }
    } else {
        let mut path = [InvalidOid; 64];
        let n = catalog_namespace::fetch_search_path_array(&mut path)?;
        for &nsp in &path[..n] {
            stats_oid = statext_name_lookup(stats_name, nsp)?;
            if stats_oid != InvalidOid {
                break;
            }
        }
    }
    if stats_oid == InvalidOid && !missing_ok {
        return Err(Box::new(
            PgError::error(format!(
                "statistics object \"{}\" does not exist",
                names.join(".")
            ))
            .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
        ));
    }
    Ok(stats_oid)
}

fn statext_name_lookup(name: &str, namespace_id: Oid) -> PgResult<Oid> {
    cache_syscache::GetSysCacheOid(
        cache_syscache::cacheinfo::STATEXTNAMENSP,
        Anum_oid as i32,
        cache_syscache::SysCacheKey::Str(name),
        cache_syscache::SysCacheKey::Value(Datum::from_oid(namespace_id)),
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
    )
}
