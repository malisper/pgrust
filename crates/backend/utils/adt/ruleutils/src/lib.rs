//! ruleutils.c introspection slice for psql \d, \dt, \l: pg_get_userbyid,
//! pg_get_indexdef (plain btree), pg_get_constraintdef, pg_get_expr.
//! Unported arms are loud named panics, never wrong output.

#![allow(non_snake_case)]

pub mod builtins;
mod deparse;
mod functiondef;
mod plan;
mod query;
mod ruledef;
mod triggerdef;
mod viewdef;
#[cfg(test)]
mod tests;

pub use builtins::RULEUTILS_BUILTINS;

fn deparse_expression_for_seam<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    expr: types_nodes::Node<'mcx>,
    relid: types_core::Oid,
) -> types_error::PgResult<String> {
    deparse::deparse_expression_pretty(mcx, expr, relid, false, 0)
}

fn deparse_partbound_const_for_seam<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    expr: types_nodes::Node<'mcx>,
) -> types_error::PgResult<String> {
    deparse::deparse_partbound_const(mcx, expr)
}
pub use deparse::deparse_expression_pretty;
pub use plan::{
    deparse_context_for_plan_tree, deparse_expression, select_rtable_names_for_explain,
    set_deparse_context_plan, AncestorEntry, PlanDeparse,
};
pub use functiondef::{
    pg_get_function_arg_default_worker, pg_get_function_arguments_worker,
    pg_get_function_identity_arguments_worker, pg_get_function_result_worker,
    pg_get_function_sqlbody_worker, pg_get_functiondef_worker,
};
pub use ruledef::pg_get_ruledef_worker;
pub use triggerdef::pg_get_triggerdef_worker;
pub use viewdef::pg_get_viewdef_worker;
pub use format_type::quote_identifier;

use cache_syscache::{
    ReleaseSysCache, SearchSysCache1, SysCacheKey, AMOID, AUTHOID, CONSTROID, INDEXRELID, OPEROID,
    PARTRELID, PROCOID, RELOID, STATEXTOID, TABLESPACEOID, TYPEOID,
};
use datum::Datum;
use mcx::Mcx;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use types_nodes::{Node, NodeList, NodeTag};
use types_tuple::{HeapTupleData, NameData, TupleDescData};

pub const PRETTYFLAG_PAREN: i32 = 0x0001;
pub const PRETTYFLAG_INDENT: i32 = 0x0002;
pub const PRETTYFLAG_SCHEMA: i32 = 0x0004;

pub fn get_pretty_flags(pretty: bool) -> i32 {
    if pretty {
        PRETTYFLAG_PAREN | PRETTYFLAG_INDENT | PRETTYFLAG_SCHEMA
    } else {
        PRETTYFLAG_INDENT
    }
}

#[cold]
#[inline(never)]
pub(crate) fn gap(func: &str, what: &str) -> ! {
    panic!("ruleutils ({func}): {what} unported")
}

#[cold]
#[inline(never)]
pub(crate) fn cache_lookup_failed(what: &str, oid: Oid) -> Box<PgError> {
    PgError::error(format!("cache lookup failed for {what} {oid}")).into()
}

fn tupdesc_for(cache_id: i32) -> &'static TupleDescData<'static> {
    match catcache::cache_tupdesc(cache_id) {
        Some(td) => td,
        None => {
            catcache::InitCatCachePhase2(cache_id, false)
                .expect("catcache phase-2 init for ruleutils projection");
            catcache::cache_tupdesc(cache_id).expect("phase-2 init left no tupdesc")
        }
    }
}

/// GETSTRUCT-shape read: fixed-width NOT NULL leading column.
pub(crate) fn getattr(tuple: &HeapTupleData<'_>, cache_id: i32, attnum: i32) -> Datum {
    // SAFETY: callers pass a tuple of this catalog's row type and a fixed
    // NOT NULL leading attnum (GETSTRUCT invariant).
    unsafe { types_tuple::fastgetattr_fixed(tuple, attnum, tupdesc_for(cache_id)) }
}

pub(crate) fn getattr_null(
    tuple: &HeapTupleData<'_>,
    cache_id: i32,
    attnum: i32,
) -> Option<Datum> {
    let mut isnull = false;
    // SAFETY: callers pass a tuple of this catalog's row type.
    let d = unsafe { types_tuple::heap_getattr(tuple, attnum, tupdesc_for(cache_id), &mut isnull) };
    if isnull { None } else { Some(d) }
}

pub(crate) fn name_at(d: Datum) -> String {
    // SAFETY: NameData column datums point at the 64-byte in-tuple buffer.
    let n = unsafe { *(d.as_usize() as *const NameData) };
    String::from_utf8_lossy(n.name_str()).into_owned()
}

// Body bytes of a catalog varlena datum, live while the tuple is pinned;
// external/compressed images detoast into a scratch context.
pub(crate) fn varlena_body_at(d: Datum) -> Vec<u8> {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena attr datum; length read from its own header.
    unsafe {
        let b0 = *p;
        if b0 == 0x01 || (b0 & 0x03) == 0x02 {
            let len = if b0 == 0x01 {
                detoast::varsize_any(core::slice::from_raw_parts(p, 2))
            } else {
                (u32::from_ne_bytes(*(p as *const [u8; 4])) >> 2) as usize
            };
            let raw = core::slice::from_raw_parts(p, len);
            let scratch = mcx::MemoryContext::new("ruleutils detoast");
            let image =
                detoast::detoast_attr(scratch.mcx(), raw).expect("detoast catalog varlena");
            image[datum::varlena::VARHDRSZ..].to_vec()
        } else {
            types_fmgr::PackedVarlena::from_ptr(p).data().to_vec()
        }
    }
}

pub(crate) fn text_at(d: Datum) -> String {
    String::from_utf8(varlena_body_at(d)).expect("non-UTF-8 catalog text")
}

// One-dimensional no-null int16 array body (int2vector or int2[]).
pub(crate) fn i16_array_at(d: Datum) -> Vec<i16> {
    array_body(d, 2).chunks_exact(2).map(|c| i16::from_ne_bytes([c[0], c[1]])).collect()
}

pub(crate) fn oid_array_at(d: Datum) -> Vec<Oid> {
    array_body(d, 4).chunks_exact(4).map(|c| u32::from_ne_bytes([c[0], c[1], c[2], c[3]])).collect()
}

// One-dimensional no-null text array (TYPALIGN_INT elements).
pub(crate) fn text_array_at(d: Datum) -> Vec<String> {
    let b = varlena_body_at(d);
    let ndim = i32::from_ne_bytes(b[0..4].try_into().unwrap());
    if ndim == 0 {
        return Vec::new();
    }
    let dataoffset = i32::from_ne_bytes(b[4..8].try_into().unwrap());
    assert!(ndim == 1 && dataoffset == 0, "ruleutils: unexpected catalog text[] shape");
    let dim1 = i32::from_ne_bytes(b[12..16].try_into().unwrap()) as usize;
    let mut out = Vec::with_capacity(dim1);
    let mut off = 20usize;
    for _ in 0..dim1 {
        // att_align_pointer: a zero pad byte means the element was aligned.
        if b[off] == 0 {
            off = (off + 3) & !3;
        }
        let b0 = b[off];
        let (hdr, len) = if b0 & 0x01 != 0 {
            (1usize, (((b0 >> 1) & 0x7F) as usize).saturating_sub(1))
        } else {
            let raw = u32::from_ne_bytes(b[off..off + 4].try_into().unwrap());
            (4usize, (raw >> 2) as usize - 4)
        };
        out.push(String::from_utf8(b[off + hdr..off + hdr + len].to_vec())
            .expect("non-UTF-8 text[] element"));
        off += hdr + len;
    }
    out
}

pub(crate) fn array_body(d: Datum, elem_width: usize) -> Vec<u8> {
    // Header fields read bytewise (short varlena headers leave the body
    // unaligned).
    let b = varlena_body_at(d);
    let ndim = i32::from_ne_bytes(b[0..4].try_into().unwrap());
    if ndim == 0 {
        return Vec::new();
    }
    let dataoffset = i32::from_ne_bytes(b[4..8].try_into().unwrap());
    assert!(ndim == 1 && dataoffset == 0, "ruleutils: unexpected catalog array shape");
    let dim1 = i32::from_ne_bytes(b[12..16].try_into().unwrap()) as usize;
    b[20..20 + elem_width * dim1].to_vec()
}

pub(crate) fn str_in<'m>(mcx: Mcx<'m>, s: &str) -> PgResult<&'m str> {
    let v = mcx::PgString::from_str_in(s, mcx)?.into_bytes().leak();
    // SAFETY: bytes came from a str.
    Ok(unsafe { core::str::from_utf8_unchecked(v) })
}

pub fn quote_qualified_identifier(qualifier: Option<&str>, ident: &str) -> String {
    match qualifier {
        Some(q) => format!("{}.{}", quote_identifier(q), quote_identifier(ident)),
        None => quote_identifier(ident).into_owned(),
    }
}

pub fn generate_operator_clause(
    mcx: Mcx<'_>,
    buf: &mut String,
    leftop: &str,
    leftoptype: Oid,
    opoid: Oid,
    rightop: &str,
    rightoptype: Oid,
) -> PgResult<()> {
    use core::fmt::Write;
    let shape = syscache_seams::lookup_pg_operator_shape::call(opoid)?
        .ok_or_else(|| cache_lookup_failed("operator", opoid))?;
    let oprname = syscache_seams::pg_operator_oprname::call(opoid)?
        .ok_or_else(|| cache_lookup_failed("operator", opoid))?;
    let oprname = String::from_utf8_lossy(oprname.name_str()).into_owned();
    let nspname = lsyscache::get_namespace_name(mcx, shape.oprnamespace)?
        .ok_or_else(|| cache_lookup_failed("namespace", shape.oprnamespace))?;

    buf.push_str(leftop);
    if leftoptype != shape.oprleft {
        add_cast_to(mcx, buf, shape.oprleft)?;
    }
    write!(buf, " OPERATOR({}.", quote_identifier(nspname.as_str())).expect("String write");
    buf.push_str(&oprname);
    write!(buf, ") {rightop}").expect("String write");
    if rightoptype != shape.oprright {
        add_cast_to(mcx, buf, shape.oprright)?;
    }
    Ok(())
}

fn add_cast_to(mcx: Mcx<'_>, buf: &mut String, typid: Oid) -> PgResult<()> {
    use core::fmt::Write;
    let (typname, typnamespace) = syscache_seams::pg_type_name_namespace::call(typid)?
        .ok_or_else(|| cache_lookup_failed("type", typid))?;
    let typname = String::from_utf8_lossy(typname.name_str()).into_owned();
    let nspname = namespace_name_or_temp(mcx, typnamespace)?
        .ok_or_else(|| cache_lookup_failed("namespace", typnamespace))?;
    write!(buf, "::{}.{}", quote_identifier(&nspname), quote_identifier(&typname))
        .expect("String write");
    Ok(())
}

pub(crate) fn namespace_name_or_temp(mcx: Mcx<'_>, nspid: Oid) -> PgResult<Option<String>> {
    if catalog_namespace::isTempNamespace(nspid) {
        return Ok(Some("pg_temp".into()));
    }
    Ok(lsyscache::get_namespace_name(mcx, nspid)?.map(|s| s.as_str().to_owned()))
}

struct PgClassRow {
    relname: String,
    relnamespace: Oid,
    relam: Oid,
    relkind: i8,
}

const ANUM_PG_CLASS_RELNAME: i32 = 2;
const ANUM_PG_CLASS_RELNAMESPACE: i32 = 3;
const ANUM_PG_CLASS_RELAM: i32 = 7;
const ANUM_PG_CLASS_RELKIND: i32 = 18;
const ANUM_PG_CLASS_RELOPTIONS: i32 = 33;

fn pg_class_row(relid: Oid) -> PgResult<Option<PgClassRow>> {
    let Some(ht) = SearchSysCache1(RELOID, SysCacheKey::Value(Datum::from_oid(relid)))? else {
        return Ok(None);
    };
    let t = ht.tuple();
    let row = PgClassRow {
        relname: name_at(getattr(&t, RELOID, ANUM_PG_CLASS_RELNAME)),
        relnamespace: getattr(&t, RELOID, ANUM_PG_CLASS_RELNAMESPACE).as_oid(),
        relam: getattr(&t, RELOID, ANUM_PG_CLASS_RELAM).as_oid(),
        relkind: getattr(&t, RELOID, ANUM_PG_CLASS_RELKIND).as_i8(),
    };
    drop(t);
    ReleaseSysCache(ht);
    Ok(Some(row))
}

// catalog_namespace::RelationIsVisible(relid: Oid) -> PgResult<bool> is the
// contract this mirrors (RelationIsVisibleExt, namespace.c): visible iff the
// unqualified name resolves to this relation in the active search path.
fn relation_is_visible(relid: Oid, relname: &str) -> PgResult<bool> {
    Ok(catalog_namespace::RelnameGetRelid(relname)? == relid)
}

pub fn generate_relation_name(mcx: Mcx<'_>, relid: Oid) -> PgResult<String> {
    let row = pg_class_row(relid)?.ok_or_else(|| cache_lookup_failed("relation", relid))?;
    let nspname = if relation_is_visible(relid, &row.relname)? {
        None
    } else {
        namespace_name_or_temp(mcx, row.relnamespace)?
    };
    Ok(quote_qualified_identifier(nspname.as_deref(), &row.relname))
}

pub fn generate_qualified_relation_name(mcx: Mcx<'_>, relid: Oid) -> PgResult<String> {
    let row = pg_class_row(relid)?.ok_or_else(|| cache_lookup_failed("relation", relid))?;
    let nspname = namespace_name_or_temp(mcx, row.relnamespace)?
        .ok_or_else(|| cache_lookup_failed("namespace", row.relnamespace))?;
    Ok(quote_qualified_identifier(Some(&nspname), &row.relname))
}

const ANUM_PG_TYPE_TYPNAME: i32 = 2;
const ANUM_PG_TYPE_TYPNAMESPACE: i32 = 3;

pub fn generate_qualified_type_name(mcx: Mcx<'_>, typid: Oid) -> PgResult<String> {
    let Some(ht) = SearchSysCache1(TYPEOID, SysCacheKey::Value(Datum::from_oid(typid)))? else {
        return Err(cache_lookup_failed("type", typid));
    };
    let t = ht.tuple();
    let typname = name_at(getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPNAME));
    let typnamespace = getattr(&t, TYPEOID, ANUM_PG_TYPE_TYPNAMESPACE).as_oid();
    drop(t);
    ReleaseSysCache(ht);
    let nspname = namespace_name_or_temp(mcx, typnamespace)?
        .ok_or_else(|| cache_lookup_failed("namespace", typnamespace))?;
    Ok(quote_qualified_identifier(Some(&nspname), &typname))
}

const ANUM_PG_OPERATOR_OPRNAME: i32 = 2;
const ANUM_PG_OPERATOR_OPRNAMESPACE: i32 = 3;
const ANUM_PG_OPERATOR_OPRKIND: i32 = 5;

pub(crate) fn generate_operator_name(
    mcx: Mcx<'_>,
    operid: Oid,
    arg1: Oid,
    arg2: Oid,
) -> PgResult<String> {
    let Some(ht) = SearchSysCache1(OPEROID, SysCacheKey::Value(Datum::from_oid(operid)))? else {
        return Err(cache_lookup_failed("operator", operid));
    };
    let t = ht.tuple();
    let oprname = name_at(getattr(&t, OPEROID, ANUM_PG_OPERATOR_OPRNAME));
    let oprnamespace = getattr(&t, OPEROID, ANUM_PG_OPERATOR_OPRNAMESPACE).as_oid();
    let oprkind = getattr(&t, OPEROID, ANUM_PG_OPERATOR_OPRKIND).as_i8();
    drop(t);
    ReleaseSysCache(ht);

    let resolved = match oprkind as u8 {
        b'b' => {
            let pstate = parser_small1::make_parsestate(mcx, None);
            let mut opname: NodeList<'_> = NodeList::nil();
            opname.lappend(mcx, Node::mk_string(mcx, str_in(mcx, &oprname)?)?)?;
            parse_oper::oper(&pstate, &opname, arg1, arg2, true, -1)?.map(|op| op.oid)
        }
        b'l' => {
            let pstate = parser_small1::make_parsestate(mcx, None);
            let mut opname: NodeList<'_> = NodeList::nil();
            opname.lappend(mcx, Node::mk_string(mcx, str_in(mcx, &oprname)?)?)?;
            parse_oper::left_oper(&pstate, &opname, arg2, true, -1)?.map(|op| op.oid)
        }
        other => panic!("unrecognized oprkind: {other}"),
    };
    if resolved == Some(operid) {
        return Ok(oprname);
    }
    let nspname = namespace_name_or_temp(mcx, oprnamespace)?
        .ok_or_else(|| cache_lookup_failed("namespace", oprnamespace))?;
    Ok(format!("OPERATOR({}.{oprname})", quote_identifier(&nspname)))
}

// CollationIsVisibleExt reduced to the lookup_collation probe pair
// (encoding-exact, then any-encoding) over the search path.
fn collation_is_visible(collid: Oid, collname: &str, collnamespace: Oid) -> PgResult<bool> {
    let encoding = mbutils::GetDatabaseEncoding() as i32;
    let mut path = [InvalidOid; 64];
    let n = catalog_namespace::fetch_search_path_array(&mut path)?;
    for &nsp in &path[..n] {
        if nsp == collnamespace {
            return Ok(true);
        }
        for enc in [encoding, -1] {
            let found = cache_syscache::GetSysCacheOid(
                cache_syscache::COLLNAMEENCNSP,
                1,
                SysCacheKey::Str(collname),
                SysCacheKey::Value(Datum::from_i32(enc)),
                SysCacheKey::Value(Datum::from_oid(nsp)),
                SysCacheKey::UNUSED,
            )?;
            if found != InvalidOid {
                return Ok(found == collid);
            }
        }
    }
    Ok(false)
}

const ANUM_PG_COLLATION_COLLNAME: i32 = 2;
const ANUM_PG_COLLATION_COLLNAMESPACE: i32 = 3;

pub fn generate_collation_name(mcx: Mcx<'_>, collid: Oid) -> PgResult<String> {
    let Some(ht) = SearchSysCache1(
        cache_syscache::COLLOID,
        SysCacheKey::Value(Datum::from_oid(collid)),
    )?
    else {
        return Err(cache_lookup_failed("collation", collid));
    };
    let t = ht.tuple();
    let collname = name_at(getattr(&t, cache_syscache::COLLOID, ANUM_PG_COLLATION_COLLNAME));
    let collnamespace =
        getattr(&t, cache_syscache::COLLOID, ANUM_PG_COLLATION_COLLNAMESPACE).as_oid();
    drop(t);
    ReleaseSysCache(ht);

    let nspname = if collation_is_visible(collid, &collname, collnamespace)? {
        None
    } else {
        namespace_name_or_temp(mcx, collnamespace)?
    };
    Ok(quote_qualified_identifier(nspname.as_deref(), &collname))
}

pub(crate) fn generate_function_name(
    mcx: Mcx<'_>,
    funcid: Oid,
    argtypes: &[Oid],
    argnames: &[&str],
    has_variadic: bool,
) -> PgResult<String> {
    let proname = lsyscache::get_func_name(mcx, funcid)?
        .ok_or_else(|| cache_lookup_failed("function", funcid))?;
    let proname = proname.as_str().to_owned();
    // C threads use_variadic into func_get_detail: expand_variadic is off
    // when the call prints with the VARIADIC keyword.
    let cands = catalog_namespace::FuncnameGetCandidatesExtended(
        mcx,
        &[&proname],
        argtypes.len() as i16,
        argnames,
        !has_variadic,
        true,
        false,
        false,
    )?;
    let mut best = cands.iter().find(|c| c.args.as_slice() == argtypes).map(|c| c.oid);
    if best.is_none() && !cands.is_empty() {
        let matched = parse_func::func_match_argtypes(mcx, argtypes, cands.as_slice())?;
        best = match matched.len() {
            0 => None,
            1 => Some(matched[0].oid),
            _ => parse_func::func_select_candidate(argtypes, matched)?.map(|c| c.oid),
        };
    }
    // C's FuncNameAsType coercion arm returns FUNCDETAIL_COERCION with
    // funcid = InvalidOid; like NOTFOUND it lands in the qualify branch, so
    // resolution failure falls through unconditionally.
    if best == Some(funcid) {
        return Ok(quote_identifier(&proname).into_owned());
    }
    let Some(sht) = SearchSysCache1(PROCOID, SysCacheKey::Value(Datum::from_oid(funcid)))? else {
        return Err(cache_lookup_failed("function", funcid));
    };
    const ANUM_PG_PROC_PRONAMESPACE: i32 = 3;
    let pronamespace = getattr(&sht.tuple(), PROCOID, ANUM_PG_PROC_PRONAMESPACE).as_oid();
    ReleaseSysCache(sht);
    let nspname = namespace_name_or_temp(mcx, pronamespace)?
        .ok_or_else(|| cache_lookup_failed("namespace", pronamespace))?;
    Ok(quote_qualified_identifier(Some(&nspname), &proname))
}

const ANUM_PG_AUTHID_ROLNAME: i32 = 2;

pub fn pg_get_userbyid_core(roleid: Oid) -> PgResult<NameData> {
    let mut result = NameData::default();
    match SearchSysCache1(AUTHOID, SysCacheKey::Value(Datum::from_oid(roleid)))? {
        Some(ht) => {
            let d = getattr(&ht.tuple(), AUTHOID, ANUM_PG_AUTHID_ROLNAME);
            // SAFETY: rolname NameData column inside the pinned tuple image.
            result = unsafe { *(d.as_usize() as *const NameData) };
            ReleaseSysCache(ht);
        }
        None => result.namestrcpy(&format!("unknown (OID={roleid})")),
    }
    Ok(result)
}

const RELKIND_PARTITIONED_INDEX: i8 = b'I' as i8;
const INDOPTION_DESC: i16 = 0x0001;
const INDOPTION_NULLS_FIRST: i16 = 0x0002;

const ANUM_PG_INDEX_INDRELID: i32 = 2;
const ANUM_PG_INDEX_INDNATTS: i32 = 3;
const ANUM_PG_INDEX_INDNKEYATTS: i32 = 4;
const ANUM_PG_INDEX_INDISUNIQUE: i32 = 5;
const ANUM_PG_INDEX_INDNULLSNOTDISTINCT: i32 = 6;
const ANUM_PG_INDEX_INDKEY: i32 = 16;
const ANUM_PG_INDEX_INDCOLLATION: i32 = 17;
const ANUM_PG_INDEX_INDCLASS: i32 = 18;
const ANUM_PG_INDEX_INDOPTION: i32 = 19;
const ANUM_PG_INDEX_INDEXPRS: i32 = 20;
const ANUM_PG_INDEX_INDPRED: i32 = 21;

struct PgIndexRow {
    indrelid: Oid,
    indnatts: i16,
    indnkeyatts: i16,
    indisunique: bool,
    indnullsnotdistinct: bool,
    indkey: Vec<i16>,
    indcollation: Vec<Oid>,
    indclass: Vec<Oid>,
    indoption: Vec<i16>,
    has_exprs: bool,
    indexprs_src: Option<String>,
    indpred: Option<String>,
}

fn pg_index_row(indexrelid: Oid) -> PgResult<Option<PgIndexRow>> {
    let Some(ht) = SearchSysCache1(INDEXRELID, SysCacheKey::Value(Datum::from_oid(indexrelid)))?
    else {
        return Ok(None);
    };
    let t = ht.tuple();
    let notnull =
        |anum: i32| getattr_null(&t, INDEXRELID, anum).expect("NOT NULL pg_index column");
    let row = PgIndexRow {
        indrelid: getattr(&t, INDEXRELID, ANUM_PG_INDEX_INDRELID).as_oid(),
        indnatts: getattr(&t, INDEXRELID, ANUM_PG_INDEX_INDNATTS).as_i16(),
        indnkeyatts: getattr(&t, INDEXRELID, ANUM_PG_INDEX_INDNKEYATTS).as_i16(),
        indisunique: getattr(&t, INDEXRELID, ANUM_PG_INDEX_INDISUNIQUE).as_bool(),
        indnullsnotdistinct: getattr(&t, INDEXRELID, ANUM_PG_INDEX_INDNULLSNOTDISTINCT).as_bool(),
        indkey: i16_array_at(notnull(ANUM_PG_INDEX_INDKEY)),
        indcollation: oid_array_at(notnull(ANUM_PG_INDEX_INDCOLLATION)),
        indclass: oid_array_at(notnull(ANUM_PG_INDEX_INDCLASS)),
        indoption: i16_array_at(notnull(ANUM_PG_INDEX_INDOPTION)),
        has_exprs: getattr_null(&t, INDEXRELID, ANUM_PG_INDEX_INDEXPRS).is_some(),
        indexprs_src: getattr_null(&t, INDEXRELID, ANUM_PG_INDEX_INDEXPRS).map(text_at),
        indpred: getattr_null(&t, INDEXRELID, ANUM_PG_INDEX_INDPRED).map(text_at),
    };
    drop(t);
    ReleaseSysCache(ht);
    Ok(Some(row))
}

// get_reloptions (ruleutils.c): name=value pairs; value kept bare only when
// it is an identifier that needs no quoting (C tests quote_identifier ptr
// identity), else single-quoted.
fn get_reloptions(buf: &mut String, options: &[String]) {
    for (i, option) in options.iter().enumerate() {
        let (name, value) = match option.find('=') {
            Some(p) => (&option[..p], &option[p + 1..]),
            None => (option.as_str(), ""),
        };
        if i > 0 {
            buf.push_str(", ");
        }
        buf.push_str(&quote_identifier(name));
        buf.push('=');
        match quote_identifier(value) {
            std::borrow::Cow::Borrowed(v) => buf.push_str(v),
            std::borrow::Cow::Owned(_) => deparse::simple_quote_literal(buf, value),
        }
    }
}

fn flatten_reloptions(relid: Oid) -> PgResult<Option<String>> {
    let Some(ht) = SearchSysCache1(RELOID, SysCacheKey::Value(Datum::from_oid(relid)))? else {
        return Err(cache_lookup_failed("relation", relid));
    };
    let t = ht.tuple();
    let opts = getattr_null(&t, RELOID, ANUM_PG_CLASS_RELOPTIONS).map(text_array_at);
    drop(t);
    ReleaseSysCache(ht);
    Ok(opts.map(|o| {
        let mut buf = String::new();
        get_reloptions(&mut buf, &o);
        buf
    }))
}

const ANUM_PG_AM_AMNAME: i32 = 2;

fn pg_am_name(amid: Oid) -> PgResult<String> {
    let Some(ht) = SearchSysCache1(AMOID, SysCacheKey::Value(Datum::from_oid(amid)))? else {
        return Err(cache_lookup_failed("access method", amid));
    };
    let name = name_at(getattr(&ht.tuple(), AMOID, ANUM_PG_AM_AMNAME));
    ReleaseSysCache(ht);
    Ok(name)
}

// get_opclass_name (ruleutils.c): emit " opclass" only when not the default
// for actual_datatype.
pub(crate) fn get_opclass_name(
    mcx: Mcx<'_>,
    opclass: Oid,
    actual_datatype: Oid,
    buf: &mut String,
) -> PgResult<()> {
    let Some((opcname, opcnamespace, opcmethod)) = pg_opclass_row(opclass)? else {
        return Err(cache_lookup_failed("opclass", opclass));
    };
    if actual_datatype == InvalidOid
        || indexcmds_seams::get_default_opclass::call(actual_datatype, opcmethod)? != opclass
    {
        buf.push(' ');
        if !opclass_is_visible(opclass, &opcname, opcmethod)? {
            let nspname = namespace_name_or_temp(mcx, opcnamespace)?
                .ok_or_else(|| cache_lookup_failed("namespace", opcnamespace))?;
            buf.push_str(&quote_identifier(&nspname));
            buf.push('.');
        }
        buf.push_str(&quote_identifier(&opcname));
    }
    Ok(())
}

const ANUM_PG_OPCLASS_OPCMETHOD: i32 = 2;
const ANUM_PG_OPCLASS_OPCNAME: i32 = 3;
const ANUM_PG_OPCLASS_OPCNAMESPACE: i32 = 4;

fn pg_opclass_row(opclass: Oid) -> PgResult<Option<(String, Oid, Oid)>> {
    let Some(ht) =
        SearchSysCache1(cache_syscache::CLAOID, SysCacheKey::Value(Datum::from_oid(opclass)))?
    else {
        return Ok(None);
    };
    let t = ht.tuple();
    let out = (
        name_at(getattr(&t, cache_syscache::CLAOID, ANUM_PG_OPCLASS_OPCNAME)),
        getattr(&t, cache_syscache::CLAOID, ANUM_PG_OPCLASS_OPCNAMESPACE).as_oid(),
        getattr(&t, cache_syscache::CLAOID, ANUM_PG_OPCLASS_OPCMETHOD).as_oid(),
    );
    drop(t);
    ReleaseSysCache(ht);
    Ok(Some(out))
}

// OpclassIsVisible (namespace.c): first same-name/same-AM opclass in the
// search path wins.
fn opclass_is_visible(opclass: Oid, opcname: &str, opcmethod: Oid) -> PgResult<bool> {
    let mut path = [InvalidOid; 64];
    let n = catalog_namespace::fetch_search_path_array(&mut path)?;
    for &nsp in &path[..n] {
        let found = cache_syscache::GetSysCacheOid(
            cache_syscache::CLAAMNAMENSP,
            1,
            SysCacheKey::Value(Datum::from_oid(opcmethod)),
            SysCacheKey::Str(opcname),
            SysCacheKey::Value(Datum::from_oid(nsp)),
            SysCacheKey::UNUSED,
        )?;
        if found != InvalidOid {
            return Ok(found == opclass);
        }
    }
    Ok(false)
}

// generate_opclass_name (ruleutils.c:12898).
pub fn generate_opclass_name(mcx: Mcx<'_>, opclass: Oid) -> PgResult<String> {
    let mut buf = String::new();
    get_opclass_name(mcx, opclass, InvalidOid, &mut buf)?;
    Ok(buf.split_off(1))
}

#[allow(clippy::too_many_arguments)]
pub fn pg_get_indexdef_worker(
    mcx: Mcx<'_>,
    indexrelid: Oid,
    colno: i32,
    exclude_ops: Option<&[Oid]>,
    attrs_only: bool,
    keys_only: bool,
    show_tbl_spc: bool,
    inherits: bool,
    pretty_flags: i32,
    missing_ok: bool,
) -> PgResult<Option<String>> {
    pg_get_indexdef_worker_extended(
        mcx,
        indexrelid,
        colno,
        exclude_ops,
        attrs_only,
        keys_only,
        show_tbl_spc,
        inherits,
        pretty_flags,
        missing_ok,
    )
}

/// pg_get_indexdef_columns_extended's RULE_INDEXDEF_KEYS_ONLY arm
/// (BuildIndexValueDescription's column list).
pub fn pg_get_indexdef_columns_keys_only(
    mcx: Mcx<'_>,
    indexrelid: Oid,
) -> PgResult<Option<String>> {
    pg_get_indexdef_worker_extended(
        mcx,
        indexrelid,
        0,
        None,
        true,
        true,
        false,
        false,
        PRETTYFLAG_PAREN | PRETTYFLAG_INDENT | PRETTYFLAG_SCHEMA,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
fn pg_get_indexdef_worker_extended(
    mcx: Mcx<'_>,
    indexrelid: Oid,
    colno: i32,
    exclude_ops: Option<&[Oid]>,
    attrs_only: bool,
    keys_only: bool,
    show_tbl_spc: bool,
    inherits: bool,
    pretty_flags: i32,
    missing_ok: bool,
) -> PgResult<Option<String>> {
    let is_constraint = exclude_ops.is_some();
    let Some(idx) = pg_index_row(indexrelid)? else {
        if missing_ok {
            return Ok(None);
        }
        return Err(cache_lookup_failed("index", indexrelid));
    };
    let idxrel =
        pg_class_row(indexrelid)?.ok_or_else(|| cache_lookup_failed("relation", indexrelid))?;
    let amname = pg_am_name(idxrel.relam)?;
    let amcanorder = amapi::GetIndexAmRoutineByAmId(idxrel.relam, false)?
        .expect("noerror=false returned Some")
        .amcanorder();
    let mut indexpr_items: std::vec::Vec<types_nodes::Node<'_>> = std::vec::Vec::new();
    if let Some(src) = idx.indexprs_src.as_deref() {
        let node = readfuncs::stringToNode(mcx, src)?;
        for e in node.as_list().expect("indexprs is a List").iter() {
            indexpr_items.push(e);
        }
    }
    let mut indexpr_next = 0usize;

    let mut buf = String::new();
    if !attrs_only {
        if !is_constraint {
            let relname = if pretty_flags & PRETTYFLAG_SCHEMA != 0 {
                generate_relation_name(mcx, idx.indrelid)?
            } else {
                generate_qualified_relation_name(mcx, idx.indrelid)?
            };
            buf.push_str(&format!(
                "CREATE {}INDEX {} ON {}{} USING {} (",
                if idx.indisunique { "UNIQUE " } else { "" },
                quote_identifier(&idxrel.relname),
                if idxrel.relkind == RELKIND_PARTITIONED_INDEX && !inherits { "ONLY " } else { "" },
                relname,
                quote_identifier(&amname),
            ));
        } else {
            buf.push_str(&format!("EXCLUDE USING {} (", quote_identifier(&amname)));
        }
    }

    let mut sep = "";
    let natts = if keys_only { idx.indnkeyatts as usize } else { idx.indnatts as usize };
    for keyno in 0..natts {
        let attnum = idx.indkey[keyno];
        if keys_only && keyno >= idx.indnkeyatts as usize {
            break;
        }
        if colno == 0 && keyno == idx.indnkeyatts as usize {
            buf.push_str(") INCLUDE (");
            sep = "";
        }
        if colno == 0 {
            buf.push_str(sep);
        }
        sep = ", ";
        let (keycoltype, keycolcollation);
        if attnum != 0 {
            let attname = lsyscache::get_attname(mcx, idx.indrelid, attnum, false)?
                .expect("get_attname missing_ok=false");
            if colno == 0 || colno == keyno as i32 + 1 {
                buf.push_str(&quote_identifier(attname.as_str()));
            }
            let (t, _, c) = lsyscache::get_atttypetypmodcoll(idx.indrelid, attnum)?;
            keycoltype = t;
            keycolcollation = c;
        } else {
            let indexkey = *indexpr_items
                .get(indexpr_next)
                .expect("too few entries in indexprs list");
            indexpr_next += 1;
            keycoltype = parse_expr::expr_type(indexkey);
            keycolcollation = parse_expr::expr_collation(indexkey);
            if colno == 0 || colno == keyno as i32 + 1 {
                let str = deparse::deparse_expression_pretty(
                    mcx, indexkey, idx.indrelid, false, pretty_flags,
                )?;
                if looks_like_function(indexkey) {
                    buf.push_str(&str);
                } else {
                    buf.push('(');
                    buf.push_str(&str);
                    buf.push(')');
                }
            }
        }
        let _ = keycolcollation;

        if !attrs_only
            && keyno < idx.indnkeyatts as usize
            && (colno == 0 || colno == keyno as i32 + 1)
        {
            let opt = idx.indoption[keyno];
            let indcoll = idx.indcollation[keyno];
            let attoptions = lsyscache::get_attoptions(mcx, indexrelid, keyno as i16 + 1)?;
            let has_options = attoptions != Datum::null();
            if indcoll != InvalidOid && indcoll != keycolcollation {
                buf.push_str(" COLLATE ");
                buf.push_str(&generate_collation_name(mcx, indcoll)?);
            }
            get_opclass_name(
                mcx,
                idx.indclass[keyno],
                if has_options { InvalidOid } else { keycoltype },
                &mut buf,
            )?;
            if has_options {
                buf.push_str(" (");
                get_reloptions(&mut buf, &text_array_at(attoptions));
                buf.push(')');
            }
            if amcanorder {
                if opt & INDOPTION_DESC != 0 {
                    buf.push_str(" DESC");
                    if opt & INDOPTION_NULLS_FIRST == 0 {
                        buf.push_str(" NULLS LAST");
                    }
                } else if opt & INDOPTION_NULLS_FIRST != 0 {
                    buf.push_str(" NULLS FIRST");
                }
            }
            if let Some(ops) = exclude_ops {
                let opname = generate_operator_name(mcx, ops[keyno], keycoltype, keycoltype)?;
                buf.push_str(&format!(" WITH {opname}"));
            }
        }
    }

    if !attrs_only {
        buf.push(')');
        if idx.indnullsnotdistinct {
            buf.push_str(" NULLS NOT DISTINCT");
        }
        if let Some(options) = flatten_reloptions(indexrelid)? {
            buf.push_str(&format!(" WITH ({options})"));
        }
        if show_tbl_spc {
            let tblspc = lsyscache::get_rel_tablespace(indexrelid)?;
            if tblspc != InvalidOid {
                if is_constraint {
                    buf.push_str(" USING INDEX");
                }
                let spcname = get_tablespace_name(tblspc)?
                    .unwrap_or_else(|| panic!("cache lookup failed for tablespace {tblspc}"));
                buf.push_str(&format!(" TABLESPACE {}", quote_identifier(&spcname)));
            }
        }
        if let Some(predsrc) = &idx.indpred {
            let node = readfuncs::stringToNode(mcx, predsrc)?;
            let predstr =
                deparse::deparse_expression_pretty(mcx, node, idx.indrelid, false, pretty_flags)?;
            if is_constraint {
                buf.push_str(&format!(" WHERE ({predstr})"));
            } else {
                buf.push_str(&format!(" WHERE {predstr}"));
            }
        }
    }
    Ok(Some(buf))
}

pub fn init_seams() {
    genam_seams::pg_get_indexdef_columns_keys_only::set(pg_get_indexdef_columns_keys_only);
    ruleutils_seams::deparse_expression::set(deparse_expression_for_seam);
    ruleutils_seams::pg_get_partkeydef_columns::set(pg_get_partkeydef_columns_for_seam);
    ruleutils_seams::deparse_partbound_const::set(deparse_partbound_const_for_seam);
}

// pg_get_partkeydef_columns (ruleutils.c).
// C pg_get_partkeydef_columns(relid, pretty=true): GET_PRETTY_FLAGS(true),
// so PRETTYFLAG_PAREN elides the redundant parens inside the expression-key
// wrap ("(b + 0)", not "((b + 0))") in routing-error DETAIL lines.
fn pg_get_partkeydef_columns_for_seam(mcx: Mcx<'_>, relid: Oid) -> PgResult<Option<String>> {
    pg_get_partkeydef_worker(mcx, relid, get_pretty_flags(true), true, false)
}

// looks_like_function (ruleutils.c): node types that deparse as func(...).
fn looks_like_function(node: types_nodes::Node<'_>) -> bool {
    use types_nodes::NodeTag::*;
    match node.node_tag() {
        T_FuncExpr => node
            .as_func_expr()
            .map(|f| f.funcformat == types_nodes::CoercionForm::COERCE_EXPLICIT_CALL)
            .unwrap_or(false),
        T_NullIfExpr | T_CoalesceExpr | T_MinMaxExpr | T_SQLValueFunction | T_XmlExpr => true,
        _ => false,
    }
}

pub fn pg_get_indexdef_string(mcx: Mcx<'_>, indexrelid: Oid) -> PgResult<String> {
    Ok(pg_get_indexdef_worker(mcx, indexrelid, 0, None, false, false, true, true, 0, false)?
        .expect("missing_ok=false returns Some"))
}

const ANUM_PG_TABLESPACE_SPCNAME: i32 = 2;

// Divergence from C: get_tablespace_name (tablespace.c) seq-scans
// pg_tablespace; this reads the TABLESPACEOID syscache.
fn get_tablespace_name(spc_oid: Oid) -> PgResult<Option<String>> {
    let Some(ht) = SearchSysCache1(TABLESPACEOID, SysCacheKey::Value(Datum::from_oid(spc_oid)))?
    else {
        return Ok(None);
    };
    let name = name_at(getattr(&ht.tuple(), TABLESPACEOID, ANUM_PG_TABLESPACE_SPCNAME));
    ReleaseSysCache(ht);
    Ok(Some(name))
}

const CONSTRAINT_FOREIGN: i8 = b'f' as i8;
const CONSTRAINT_PRIMARY: i8 = b'p' as i8;
const CONSTRAINT_UNIQUE: i8 = b'u' as i8;
const CONSTRAINT_CHECK: i8 = b'c' as i8;
const CONSTRAINT_NOTNULL: i8 = b'n' as i8;
const CONSTRAINT_TRIGGER: i8 = b't' as i8;
const CONSTRAINT_EXCLUSION: i8 = b'x' as i8;

const FKCONSTR_MATCH_FULL: i8 = b'f' as i8;
const FKCONSTR_MATCH_PARTIAL: i8 = b'p' as i8;
const FKCONSTR_MATCH_SIMPLE: i8 = b's' as i8;
const FKCONSTR_ACTION_NOACTION: i8 = b'a' as i8;
const FKCONSTR_ACTION_RESTRICT: i8 = b'r' as i8;
const FKCONSTR_ACTION_CASCADE: i8 = b'c' as i8;
const FKCONSTR_ACTION_SETNULL: i8 = b'n' as i8;
const FKCONSTR_ACTION_SETDEFAULT: i8 = b'd' as i8;

const ANUM_PG_CONSTRAINT_CONTYPE: i32 = 4;
const ANUM_PG_CONSTRAINT_CONDEFERRABLE: i32 = 5;
const ANUM_PG_CONSTRAINT_CONDEFERRED: i32 = 6;
const ANUM_PG_CONSTRAINT_CONENFORCED: i32 = 7;
const ANUM_PG_CONSTRAINT_CONVALIDATED: i32 = 8;
const ANUM_PG_CONSTRAINT_CONRELID: i32 = 9;
const ANUM_PG_CONSTRAINT_CONTYPID: i32 = 10;
const ANUM_PG_CONSTRAINT_CONINDID: i32 = 11;
const ANUM_PG_CONSTRAINT_CONFRELID: i32 = 13;
const ANUM_PG_CONSTRAINT_CONFUPDTYPE: i32 = 14;
const ANUM_PG_CONSTRAINT_CONFDELTYPE: i32 = 15;
const ANUM_PG_CONSTRAINT_CONFMATCHTYPE: i32 = 16;
const ANUM_PG_CONSTRAINT_CONNOINHERIT: i32 = 19;
const ANUM_PG_CONSTRAINT_CONPERIOD: i32 = 20;
const ANUM_PG_CONSTRAINT_CONKEY: i32 = 21;
const ANUM_PG_CONSTRAINT_CONFKEY: i32 = 22;
const ANUM_PG_CONSTRAINT_CONFDELSETCOLS: i32 = 26;
const ANUM_PG_CONSTRAINT_CONEXCLOP: i32 = 27;
const ANUM_PG_CONSTRAINT_CONBIN: i32 = 28;

fn decompile_column_index_array(
    mcx: Mcx<'_>,
    keys: &[i16],
    relid: Oid,
    with_period: bool,
    buf: &mut String,
) -> PgResult<usize> {
    for (j, &attnum) in keys.iter().enumerate() {
        let colname = lsyscache::get_attname(mcx, relid, attnum, false)?
            .expect("get_attname missing_ok=false");
        if j > 0 {
            buf.push_str(", ");
            if with_period && j == keys.len() - 1 {
                buf.push_str("PERIOD ");
            }
        }
        buf.push_str(&quote_identifier(colname.as_str()));
    }
    Ok(keys.len())
}

const ANUM_PG_CONSTRAINT_CONNAME: i32 = 2;

pub fn pg_get_constraintdef_command(mcx: Mcx<'_>, constraint_id: Oid) -> PgResult<String> {
    let Some(ht) = SearchSysCache1(CONSTROID, SysCacheKey::Value(Datum::from_oid(constraint_id)))?
    else {
        return Err(PgError::error(format!(
            "could not find tuple for constraint {constraint_id}"
        ))
        .into());
    };
    let t = ht.tuple();
    let conname = name_at(getattr(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONNAME));
    let conrelid = getattr(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONRELID).as_oid();
    let contypid = getattr(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONTYPID).as_oid();
    drop(t);
    ReleaseSysCache(ht);
    // C emits ALTER TABLE without ONLY: CHECK re-add wants recursion and the
    // other contypes never inherit.
    let prefix = if conrelid != InvalidOid {
        format!(
            "ALTER TABLE {} ADD CONSTRAINT {} ",
            generate_qualified_relation_name(mcx, conrelid)?,
            quote_identifier(&conname)
        )
    } else {
        debug_assert!(contypid != InvalidOid);
        format!(
            "ALTER DOMAIN {} ADD CONSTRAINT {} ",
            generate_qualified_type_name(mcx, contypid)?,
            quote_identifier(&conname)
        )
    };
    let body = pg_get_constraintdef_worker_full(mcx, constraint_id, true, 0, false)?
        .expect("missing_ok=false returns Some");
    Ok(prefix + &body)
}

pub fn pg_get_constraintdef_worker(
    mcx: Mcx<'_>,
    constraint_id: Oid,
    pretty_flags: i32,
    missing_ok: bool,
) -> PgResult<Option<String>> {
    pg_get_constraintdef_worker_full(mcx, constraint_id, false, pretty_flags, missing_ok)
}

// Divergence from C: pg_get_constraintdef_worker scans pg_constraint under a
// fresh MVCC snapshot; this reads the CONSTROID syscache.
fn pg_get_constraintdef_worker_full(
    mcx: Mcx<'_>,
    constraint_id: Oid,
    full_command: bool,
    pretty_flags: i32,
    missing_ok: bool,
) -> PgResult<Option<String>> {
    let Some(ht) = SearchSysCache1(CONSTROID, SysCacheKey::Value(Datum::from_oid(constraint_id)))?
    else {
        if missing_ok {
            return Ok(None);
        }
        return Err(PgError::error(format!(
            "could not find tuple for constraint {constraint_id}"
        ))
        .into());
    };
    let t = ht.tuple();
    let contype = getattr(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONTYPE).as_i8();
    let conrelid = getattr(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONRELID).as_oid();
    let contypid = getattr(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONTYPID).as_oid();
    let conindid = getattr(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONINDID).as_oid();
    let confrelid = getattr(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONFRELID).as_oid();
    let confupdtype = getattr(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONFUPDTYPE).as_i8();
    let confdeltype = getattr(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONFDELTYPE).as_i8();
    let confmatchtype = getattr(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONFMATCHTYPE).as_i8();
    let condeferrable = getattr(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONDEFERRABLE).as_bool();
    let condeferred = getattr(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONDEFERRED).as_bool();
    let conenforced = getattr(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONENFORCED).as_bool();
    let convalidated = getattr(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONVALIDATED).as_bool();
    let connoinherit = getattr(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONNOINHERIT).as_bool();
    let conperiod = getattr(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONPERIOD).as_bool();
    let conkey = getattr_null(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONKEY).map(i16_array_at);
    let confkey = getattr_null(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONFKEY).map(i16_array_at);
    let confdelsetcols =
        getattr_null(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONFDELSETCOLS).map(i16_array_at);
    let conexclop = getattr_null(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONEXCLOP).map(oid_array_at);
    let conbin = getattr_null(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONBIN).map(text_at);
    drop(t);
    ReleaseSysCache(ht);

    let mut buf = String::new();
    match contype {
        CONSTRAINT_FOREIGN => {
            buf.push_str("FOREIGN KEY (");
            let conkey = conkey.expect("FK constraint has conkey");
            decompile_column_index_array(mcx, &conkey, conrelid, conperiod, &mut buf)?;
            buf.push_str(&format!(") REFERENCES {}(", generate_relation_name(mcx, confrelid)?));
            let confkey = confkey.expect("FK constraint has confkey");
            decompile_column_index_array(mcx, &confkey, confrelid, conperiod, &mut buf)?;
            buf.push(')');
            match confmatchtype {
                FKCONSTR_MATCH_FULL => buf.push_str(" MATCH FULL"),
                FKCONSTR_MATCH_PARTIAL => buf.push_str(" MATCH PARTIAL"),
                FKCONSTR_MATCH_SIMPLE => {}
                other => panic!("unrecognized confmatchtype: {other}"),
            }
            let action = |t: i8| match t {
                FKCONSTR_ACTION_NOACTION => None,
                FKCONSTR_ACTION_RESTRICT => Some("RESTRICT"),
                FKCONSTR_ACTION_CASCADE => Some("CASCADE"),
                FKCONSTR_ACTION_SETNULL => Some("SET NULL"),
                FKCONSTR_ACTION_SETDEFAULT => Some("SET DEFAULT"),
                other => panic!("unrecognized FK action: {other}"),
            };
            if let Some(s) = action(confupdtype) {
                buf.push_str(&format!(" ON UPDATE {s}"));
            }
            if let Some(s) = action(confdeltype) {
                buf.push_str(&format!(" ON DELETE {s}"));
            }
            if let Some(cols) = confdelsetcols {
                buf.push_str(" (");
                decompile_column_index_array(mcx, &cols, conrelid, false, &mut buf)?;
                buf.push(')');
            }
        }
        CONSTRAINT_PRIMARY | CONSTRAINT_UNIQUE => {
            buf.push_str(if contype == CONSTRAINT_PRIMARY { "PRIMARY KEY " } else { "UNIQUE " });
            let idx =
                pg_index_row(conindid)?.ok_or_else(|| cache_lookup_failed("index", conindid))?;
            if contype == CONSTRAINT_UNIQUE && idx.indnullsnotdistinct {
                buf.push_str("NULLS NOT DISTINCT ");
            }
            buf.push('(');
            let conkey = conkey.expect("index constraint has conkey");
            let keyatts = decompile_column_index_array(mcx, &conkey, conrelid, false, &mut buf)?;
            if conperiod {
                buf.push_str(" WITHOUT OVERLAPS");
            }
            buf.push(')');
            if (idx.indnatts as usize) > keyatts {
                buf.push_str(" INCLUDE (");
                for (j, &attnum) in idx.indkey.iter().enumerate().skip(keyatts) {
                    if j > keyatts {
                        buf.push_str(", ");
                    }
                    let colname = lsyscache::get_attname(mcx, conrelid, attnum, false)?
                        .expect("get_attname missing_ok=false");
                    buf.push_str(&quote_identifier(colname.as_str()));
                }
                buf.push(')');
            }
            if full_command && conindid != InvalidOid {
                if let Some(options) = flatten_reloptions(conindid)? {
                    buf.push_str(&format!(" WITH ({options})"));
                }
                // The tablespace, unless database default: ALTER TABLE's
                // re-add path needs it to recreate exact catalog state.
                let tblspc = lsyscache::get_rel_tablespace(conindid)?;
                if tblspc != InvalidOid {
                    let spcname = get_tablespace_name(tblspc)?
                        .unwrap_or_else(|| panic!("cache lookup failed for tablespace {tblspc}"));
                    buf.push_str(&format!(
                        " USING INDEX TABLESPACE {}",
                        quote_identifier(&spcname)
                    ));
                }
            }
        }
        CONSTRAINT_CHECK => {
            let conbin = conbin.expect("CHECK constraint has conbin");
            let expr = readfuncs::stringToNode(mcx, &conbin)?;
            let consrc = deparse_expression_pretty(mcx, expr, conrelid, false, pretty_flags)?;
            buf.push_str(&format!(
                "CHECK ({consrc}){}",
                if connoinherit { " NO INHERIT" } else { "" }
            ));
        }
        CONSTRAINT_NOTNULL => {
            if conrelid != InvalidOid {
                let conkey = conkey.expect("NOT NULL constraint has conkey");
                assert!(conkey.len() == 1, "NOT NULL constraint has one column");
                let colname = lsyscache::get_attname(mcx, conrelid, conkey[0], false)?
                    .expect("get_attname missing_ok=false");
                buf.push_str(&format!("NOT NULL {}", quote_identifier(colname.as_str())));
                if connoinherit {
                    buf.push_str(" NO INHERIT");
                }
            } else if contypid != InvalidOid {
                buf.push_str("NOT NULL");
            }
        }
        CONSTRAINT_TRIGGER => buf.push_str("TRIGGER"),
        CONSTRAINT_EXCLUSION => {
            let operators = conexclop.expect("EXCLUDE constraint has conexclop");
            // C suppresses the tablespace here (pg_dump wants it that way).
            let indexdef = pg_get_indexdef_worker(
                mcx,
                conindid,
                0,
                Some(&operators),
                false,
                false,
                false,
                false,
                pretty_flags,
                false,
            )?
            .expect("missing_ok=false");
            buf.push_str(&indexdef);
        }
        other => gap(
            "pg_get_constraintdef",
            &format!("constraint type '{}'", (other as u8) as char),
        ),
    }

    if condeferrable {
        buf.push_str(" DEFERRABLE");
    }
    if condeferred {
        buf.push_str(" INITIALLY DEFERRED");
    }
    if !conenforced {
        buf.push_str(" NOT ENFORCED");
    } else if !convalidated {
        buf.push_str(" NOT VALID");
    }
    Ok(Some(buf))
}

pub fn pg_get_expr_worker(
    mcx: Mcx<'_>,
    expr_text: &str,
    relid: Oid,
    pretty_flags: i32,
) -> PgResult<Option<String>> {
    // stringToNode (read.c) returns NULL for the "<>" null-node marker that
    // pg_rewrite.ev_qual carries on every unconditional rule; every later
    // step must tolerate the NULL node exactly as C does (public issue #18).
    let node = readfuncs::stringToNodeNullable(mcx, expr_text)?;
    let mut tst = node;
    while let Some(n) = tst {
        if n.node_tag() != NodeTag::T_List {
            break;
        }
        let list = n.as_list().expect("List tag");
        tst = if list.is_nil() { None } else { Some(list.nth(0)) };
    }
    if tst.is_some_and(|n| n.node_tag() == NodeTag::T_Query) {
        return Err(PgError::error("input is a query, not an expression".to_string())
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .into());
    }

    // C: bms_is_subset(pull_varnos(NULL, node), {1}) / bms_is_empty.
    // pull_varnos of the NULL node is the empty set, which passes both arms.
    if let Some(n) = node {
        let relids = vars::pull_varnos(mcx, n)?;
        if relid != InvalidOid {
            if relids.iter().any(|v| v != 1) {
                return Err(PgError::error(
                    "expression contains variables of more than one relation".to_string(),
                )
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .into());
            }
        } else if !relids.is_empty() {
            return Err(PgError::error("expression contains variables".to_string())
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .into());
        }
    }

    if relid != InvalidOid {
        // Divergence from C: try_relation_open existence probe without the
        // AccessShareLock (relation_open machinery is another lane). The
        // probe stays BEFORE deparse: C returns SQL NULL for a vanished
        // relation even when the node itself is NULL.
        if pg_class_row(relid)?.is_none() {
            return Ok(None);
        }
    }
    match node {
        Some(n) => Ok(Some(deparse_expression_pretty(mcx, n, relid, false, pretty_flags)?)),
        // get_rule_expr (ruleutils.c): "if (node == NULL) return;" — the
        // deparse of the NULL node is the EMPTY STRING, not SQL NULL
        // (verified against live C 18.3: is_null=f, is_empty=t).
        None => Ok(Some(String::new())),
    }
}

const PARTITION_STRATEGY_HASH: i8 = b'h' as i8;
const PARTITION_STRATEGY_LIST: i8 = b'l' as i8;
const PARTITION_STRATEGY_RANGE: i8 = b'r' as i8;

const ANUM_PG_PARTITIONED_TABLE_PARTSTRAT: i32 = 2;
const ANUM_PG_PARTITIONED_TABLE_PARTNATTS: i32 = 3;
const ANUM_PG_PARTITIONED_TABLE_PARTATTRS: i32 = 5;
const ANUM_PG_PARTITIONED_TABLE_PARTCLASS: i32 = 6;
const ANUM_PG_PARTITIONED_TABLE_PARTCOLLATION: i32 = 7;
const ANUM_PG_PARTITIONED_TABLE_PARTEXPRS: i32 = 8;

// pg_get_partconstrdef_string (ruleutils.c): the partition constraint
// deparsed with a table alias, for RI_PartitionRemove_Check.
pub fn pg_get_partconstrdef_string<'mcx>(
    mcx: Mcx<'mcx>,
    partition_id: Oid,
    aliasname: &str,
) -> PgResult<Option<String>> {
    let rel = table::table_open(mcx, partition_id, types_rel::AccessShareLock)?;
    let mut quals: NodeList<'mcx> = NodeList::nil();
    for q in partdesc::RelationGetPartitionQual(mcx, &rel)?.iter() {
        quals.lappend(mcx, q)?;
    }
    // C keeps the AccessShareLock so the caller can deparse safely.
    rel.close(types_rel::NoLock)?;
    if quals.is_nil() {
        return Ok(None);
    }
    let constr_expr = partbounds::make_ands_explicit(mcx, quals)?;
    let mut ctx = deparse::DeparseContext::new(mcx, 0);
    ctx.varprefix = true;
    ctx.namespaces
        .push(std::rc::Rc::new(query::deparse_context_for(mcx, aliasname, partition_id)?));
    deparse::get_rule_expr(constr_expr, &mut ctx, false)?;
    Ok(Some(ctx.buf))
}

pub fn pg_get_partkeydef_worker(
    mcx: Mcx<'_>,
    relid: Oid,
    pretty_flags: i32,
    attrs_only: bool,
    missing_ok: bool,
) -> PgResult<Option<String>> {
    let Some(ht) = SearchSysCache1(PARTRELID, SysCacheKey::Value(Datum::from_oid(relid)))? else {
        if missing_ok {
            return Ok(None);
        }
        return Err(PgError::error(format!(
            "cache lookup failed for partition key of {relid}"
        ))
        .into());
    };
    let t = ht.tuple();
    let notnull =
        |anum: i32| getattr_null(&t, PARTRELID, anum).expect("NOT NULL pg_partitioned_table column");
    let partstrat = getattr(&t, PARTRELID, ANUM_PG_PARTITIONED_TABLE_PARTSTRAT).as_i8();
    let partnatts = getattr(&t, PARTRELID, ANUM_PG_PARTITIONED_TABLE_PARTNATTS).as_i16();
    let partattrs = i16_array_at(notnull(ANUM_PG_PARTITIONED_TABLE_PARTATTRS));
    let partclass = oid_array_at(notnull(ANUM_PG_PARTITIONED_TABLE_PARTCLASS));
    let partcollation = oid_array_at(notnull(ANUM_PG_PARTITIONED_TABLE_PARTCOLLATION));
    let partexprs_text =
        getattr_null(&t, PARTRELID, ANUM_PG_PARTITIONED_TABLE_PARTEXPRS).map(text_at);
    drop(t);
    ReleaseSysCache(ht);

    let mut partexprs: Vec<Node<'_>> = Vec::new();
    if let Some(s) = &partexprs_text {
        let node = readfuncs::stringToNode(mcx, s)?;
        let list = node.as_list().expect("unexpected node type found in partexprs");
        partexprs = list.iter().collect();
    }
    let mut partexpr_item = 0usize;

    let strategy = match partstrat {
        PARTITION_STRATEGY_HASH => "HASH",
        PARTITION_STRATEGY_LIST => "LIST",
        PARTITION_STRATEGY_RANGE => "RANGE",
        other => panic!("unexpected partition strategy: {other}"),
    };
    let mut buf = String::new();
    if !attrs_only {
        buf.push_str(strategy);
        buf.push_str(" (");
    }
    let mut sep = "";
    for keyno in 0..partnatts as usize {
        let attnum = partattrs[keyno];
        buf.push_str(sep);
        sep = ", ";
        let (keycoltype, keycolcollation);
        if attnum != 0 {
            let attname = lsyscache::get_attname(mcx, relid, attnum, false)?
                .expect("get_attname missing_ok=false");
            buf.push_str(&quote_identifier(attname.as_str()));
            let (ty, _, coll) = lsyscache::get_atttypetypmodcoll(relid, attnum)?;
            keycoltype = ty;
            keycolcollation = coll;
        } else {
            assert!(partexpr_item < partexprs.len(), "too few entries in partexprs list");
            let partkey = partexprs[partexpr_item];
            partexpr_item += 1;
            let s = deparse_expression_pretty(mcx, partkey, relid, false, pretty_flags)?;
            if query::looks_like_function(partkey) {
                buf.push_str(&s);
            } else {
                buf.push_str(&format!("({s})"));
            }
            keycoltype = parse_expr::expr_type(partkey);
            keycolcollation = parse_expr::expr_collation(partkey);
        }
        let partcoll = partcollation[keyno];
        if !attrs_only && partcoll != InvalidOid && partcoll != keycolcollation {
            buf.push_str(&format!(" COLLATE {}", generate_collation_name(mcx, partcoll)?));
        }
        if !attrs_only {
            get_opclass_name(mcx, partclass[keyno], keycoltype, &mut buf)?;
        }
    }
    if !attrs_only {
        buf.push(')');
    }
    Ok(Some(buf))
}

const STATS_EXT_NDISTINCT: u8 = b'd';
const STATS_EXT_DEPENDENCIES: u8 = b'f';
const STATS_EXT_MCV: u8 = b'm';

const ANUM_PG_STATISTIC_EXT_STXRELID: i32 = 2;
const ANUM_PG_STATISTIC_EXT_STXNAME: i32 = 3;
const ANUM_PG_STATISTIC_EXT_STXNAMESPACE: i32 = 4;
const ANUM_PG_STATISTIC_EXT_STXKEYS: i32 = 6;
const ANUM_PG_STATISTIC_EXT_STXKIND: i32 = 8;
const ANUM_PG_STATISTIC_EXT_STXEXPRS: i32 = 9;

pub fn pg_get_statisticsobj_worker(
    mcx: Mcx<'_>,
    statextid: Oid,
    columns_only: bool,
    missing_ok: bool,
) -> PgResult<Option<String>> {
    let Some(ht) = SearchSysCache1(STATEXTOID, SysCacheKey::Value(Datum::from_oid(statextid)))?
    else {
        if missing_ok {
            return Ok(None);
        }
        return Err(PgError::error(format!(
            "cache lookup failed for statistics object {statextid}"
        ))
        .into());
    };
    let t = ht.tuple();
    let notnull =
        |anum: i32| getattr_null(&t, STATEXTOID, anum).expect("NOT NULL pg_statistic_ext column");
    let stxrelid = getattr(&t, STATEXTOID, ANUM_PG_STATISTIC_EXT_STXRELID).as_oid();
    let stxname = name_at(getattr(&t, STATEXTOID, ANUM_PG_STATISTIC_EXT_STXNAME));
    let stxnamespace = getattr(&t, STATEXTOID, ANUM_PG_STATISTIC_EXT_STXNAMESPACE).as_oid();
    let stxkeys = i16_array_at(notnull(ANUM_PG_STATISTIC_EXT_STXKEYS));
    let stxkind = array_body(notnull(ANUM_PG_STATISTIC_EXT_STXKIND), 1);
    let exprs_text = getattr_null(&t, STATEXTOID, ANUM_PG_STATISTIC_EXT_STXEXPRS).map(text_at);
    drop(t);
    ReleaseSysCache(ht);

    let mut exprs: Vec<Node<'_>> = Vec::new();
    if let Some(s) = &exprs_text {
        let node = readfuncs::stringToNode(mcx, s)?;
        exprs = node.as_list().expect("stxexprs is a List").iter().collect();
    }
    let ncolumns = stxkeys.len() + exprs.len();

    let mut buf = String::new();
    if !columns_only {
        let nsp = namespace_name_or_temp(mcx, stxnamespace)?;
        buf.push_str(&format!(
            "CREATE STATISTICS {}",
            quote_qualified_identifier(nsp.as_deref(), &stxname)
        ));
        let ndistinct_enabled = stxkind.contains(&STATS_EXT_NDISTINCT);
        let dependencies_enabled = stxkind.contains(&STATS_EXT_DEPENDENCIES);
        let mcv_enabled = stxkind.contains(&STATS_EXT_MCV);
        if (!ndistinct_enabled || !dependencies_enabled || !mcv_enabled) && ncolumns > 1 {
            let mut gotone = false;
            buf.push_str(" (");
            if ndistinct_enabled {
                buf.push_str("ndistinct");
                gotone = true;
            }
            if dependencies_enabled {
                buf.push_str(if gotone { ", dependencies" } else { "dependencies" });
                gotone = true;
            }
            if mcv_enabled {
                buf.push_str(if gotone { ", mcv" } else { "mcv" });
            }
            buf.push(')');
        }
        buf.push_str(" ON ");
    }

    let mut colno = 0usize;
    for &attnum in &stxkeys {
        if colno > 0 {
            buf.push_str(", ");
        }
        let attname = lsyscache::get_attname(mcx, stxrelid, attnum, false)?
            .expect("get_attname missing_ok=false");
        buf.push_str(&quote_identifier(attname.as_str()));
        colno += 1;
    }
    for &expr in &exprs {
        let s = deparse_expression_pretty(mcx, expr, stxrelid, false, PRETTYFLAG_PAREN)?;
        if colno > 0 {
            buf.push_str(", ");
        }
        if query::looks_like_function(expr) {
            buf.push_str(&s);
        } else {
            buf.push_str(&format!("({s})"));
        }
        colno += 1;
    }

    if !columns_only {
        buf.push_str(&format!(" FROM {}", generate_relation_name(mcx, stxrelid)?));
    }
    Ok(Some(buf))
}

// pg_get_statisticsobjdef_expressions (ruleutils.c:1838); the fc wrapper
// builds the text[] result.
pub fn pg_get_statisticsobjdef_expressions_worker(
    mcx: Mcx<'_>,
    statextid: Oid,
) -> PgResult<Option<Vec<String>>> {
    let Some(ht) = SearchSysCache1(STATEXTOID, SysCacheKey::Value(Datum::from_oid(statextid)))?
    else {
        return Ok(None);
    };
    let t = ht.tuple();
    let stxrelid = getattr(&t, STATEXTOID, ANUM_PG_STATISTIC_EXT_STXRELID).as_oid();
    let exprs_text = getattr_null(&t, STATEXTOID, ANUM_PG_STATISTIC_EXT_STXEXPRS).map(text_at);
    drop(t);
    ReleaseSysCache(ht);
    let Some(src) = exprs_text else {
        return Ok(None);
    };
    let node = readfuncs::stringToNode(mcx, &src)?;
    let mut out = Vec::new();
    for expr in node.as_list().expect("stxexprs is a List").iter() {
        out.push(deparse_expression_pretty(mcx, expr, stxrelid, false, PRETTYFLAG_INDENT)?);
    }
    Ok(Some(out))
}

const RELKIND_SEQUENCE: i8 = b'S' as i8;

// pg_get_serial_sequence (ruleutils.c:2833).
pub fn pg_get_serial_sequence_worker(
    mcx: Mcx<'_>,
    tablename: &str,
    columnname: &str,
) -> PgResult<Option<String>> {
    let table_oid = viewdef::qualified_name_to_relid(mcx, tablename)?;
    let attnum = lsyscache::get_attnum(table_oid, columnname)?;
    if attnum == 0 {
        return Err(PgError::error(format!(
            "column \"{columnname}\" of relation \"{tablename}\" does not exist"
        ))
        .with_sqlstate(types_error::ERRCODE_UNDEFINED_COLUMN)
        .into());
    }
    for cand in pg_depend::get_serial_sequence_candidates(mcx, table_oid, attnum as i32)?.iter() {
        if lsyscache::get_rel_relkind(*cand)? == RELKIND_SEQUENCE {
            return Ok(Some(generate_qualified_relation_name(mcx, *cand)?));
        }
    }
    Ok(None)
}

// pg_get_partition_constraintdef (ruleutils.c:2096) over
// get_partition_qual_relid (partcache.c:299).
pub fn pg_get_partition_constraintdef_worker(
    mcx: Mcx<'_>,
    relation_id: Oid,
) -> PgResult<Option<String>> {
    if !lsyscache::get_rel_relispartition(relation_id)? {
        return Ok(None);
    }
    // C holds AccessShareLock through the deparse; lock machinery is another
    // lane (matches the pg_get_expr divergence above).
    // relation_open, not table_open: index partitions are legal inputs
    // (get_partition_qual_relid, partcache.c:306).
    let rel = relation_seams::relation_open::call(mcx, relation_id, types_rel::AccessShareLock)?;
    let and_args = partdesc::RelationGetPartitionQual(mcx, &rel)?;
    rel.close(types_rel::AccessShareLock)?;
    // The cached qual list is 'static (List is invariant); copy into mcx as
    // C's generate_partition_qual copyObject does.
    let expr = match and_args.len() {
        0 => return Ok(None),
        1 => copyfuncs::copy_object(mcx, and_args.nth(0))?,
        _ => {
            let mut args = NodeList::nil();
            for a in and_args.iter() {
                args.lappend(mcx, copyfuncs::copy_object(mcx, a)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::BoolExpr {
                    boolop: types_nodes::primnodes::BoolExprType::AND_EXPR,
                    args,
                    location: -1,
                },
            )?
        }
    };
    Ok(Some(deparse_expression_pretty(mcx, expr, relation_id, false, PRETTYFLAG_INDENT)?))
}

// pg_get_querydef (ruleutils.c:1588) — extension-facing entry point.
pub fn pg_get_querydef<'mcx>(
    mcx: Mcx<'mcx>,
    query: &'mcx types_nodes::Query<'mcx>,
    pretty: bool,
) -> PgResult<String> {
    let mut ctx = deparse::DeparseContext::new(mcx, get_pretty_flags(pretty));
    query::get_query_def(query, &mut ctx, None, true)?;
    Ok(ctx.buf)
}
