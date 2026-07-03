//! ruleutils.c introspection slice for psql \d, \dt, \l: pg_get_userbyid,
//! pg_get_indexdef (plain btree), pg_get_constraintdef, pg_get_expr.
//! Unported arms are loud named panics, never wrong output.

#![allow(non_snake_case)]

pub mod builtins;
mod deparse;
mod functiondef;
mod query;
mod ruledef;
mod viewdef;
#[cfg(test)]
mod tests;

pub use builtins::RULEUTILS_BUILTINS;
pub use deparse::deparse_expression_pretty;
pub use functiondef::{
    pg_get_function_arguments_worker, pg_get_function_identity_arguments_worker,
    pg_get_function_result_worker, pg_get_functiondef_worker,
};
pub use ruledef::pg_get_ruledef_worker;
pub use viewdef::pg_get_viewdef_worker;
pub use format_type::quote_identifier;

use cache_syscache::{
    ReleaseSysCache, SearchSysCache1, SysCacheKey, AMOID, AUTHOID, CONSTROID, INDEXRELID, OPEROID,
    PROCOID, RELOID,
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

pub(crate) fn text_at(d: Datum) -> String {
    // SAFETY: catalog text/pg_node_tree datum, live while the tuple is pinned;
    // PackedVarlena panics loudly on external/compressed images.
    let v = unsafe { types_fmgr::PackedVarlena::from_ptr(d.as_usize() as *const u8) };
    String::from_utf8(v.data().to_vec()).expect("non-UTF-8 pg_node_tree")
}

// One-dimensional no-null int16 array body (int2vector or int2[]).
fn i16_array_at(d: Datum) -> Vec<i16> {
    array_body(d, 2).chunks_exact(2).map(|c| i16::from_ne_bytes([c[0], c[1]])).collect()
}

pub(crate) fn oid_array_at(d: Datum) -> Vec<Oid> {
    array_body(d, 4).chunks_exact(4).map(|c| u32::from_ne_bytes([c[0], c[1], c[2], c[3]])).collect()
}

pub(crate) fn array_body(d: Datum, elem_width: usize) -> Vec<u8> {
    // SAFETY: as text_at; header fields read bytewise (short varlena headers
    // leave the body unaligned).
    let v = unsafe { types_fmgr::PackedVarlena::from_ptr(d.as_usize() as *const u8) };
    let b = v.data();
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
    has_reloptions: bool,
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
        has_reloptions: getattr_null(&t, RELOID, ANUM_PG_CLASS_RELOPTIONS).is_some(),
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
        b'l' => gap("generate_operator_name", "prefix (left) operator resolution"),
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
    has_variadic: bool,
) -> PgResult<String> {
    let proname = lsyscache::get_func_name(mcx, funcid)?
        .ok_or_else(|| cache_lookup_failed("function", funcid))?;
    let proname = proname.as_str().to_owned();
    if has_variadic {
        gap("generate_function_name", "VARIADIC call deparse");
    }
    let cands = catalog_namespace::FuncnameGetCandidates(
        mcx,
        &[&proname],
        argtypes.len() as i16,
        true,
        true,
    )?;
    let mut best = cands.iter().find(|c| c.args.as_slice() == argtypes).map(|c| c.oid);
    if best.is_none() && !cands.is_empty() {
        if argtypes.len() == 1 {
            // C consults the FuncNameAsType coercion arm before fuzzy matching.
            gap("generate_function_name", "single-arg fuzzy resolution (coercion arm)");
        }
        let matched = parse_func::func_match_argtypes(mcx, argtypes, cands.as_slice())?;
        best = match matched.len() {
            0 => None,
            1 => Some(matched[0].oid),
            _ => parse_func::func_select_candidate(argtypes, matched)?.map(|c| c.oid),
        };
    }
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

const BTREE_AM_OID: Oid = 403;
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
    has_pred: bool,
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
        has_pred: getattr_null(&t, INDEXRELID, ANUM_PG_INDEX_INDPRED).is_some(),
    };
    drop(t);
    ReleaseSysCache(ht);
    Ok(Some(row))
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
fn get_opclass_name(
    mcx: Mcx<'_>,
    opclass: Oid,
    actual_datatype: Oid,
    buf: &mut String,
) -> PgResult<()> {
    let Some((opcname, opcnamespace, opcmethod)) = pg_opclass_row(opclass)? else {
        return Err(cache_lookup_failed("opclass", opclass));
    };
    if actual_datatype == InvalidOid
        || indexcmds::GetDefaultOpClass(actual_datatype, opcmethod)? != opclass
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

pub fn pg_get_indexdef_worker(
    mcx: Mcx<'_>,
    indexrelid: Oid,
    colno: i32,
    attrs_only: bool,
    pretty_flags: i32,
    missing_ok: bool,
) -> PgResult<Option<String>> {
    let Some(idx) = pg_index_row(indexrelid)? else {
        if missing_ok {
            return Ok(None);
        }
        return Err(cache_lookup_failed("index", indexrelid));
    };
    let idxrel =
        pg_class_row(indexrelid)?.ok_or_else(|| cache_lookup_failed("relation", indexrelid))?;
    let amname = pg_am_name(idxrel.relam)?;
    if idxrel.relam != BTREE_AM_OID {
        gap("pg_get_indexdef", &format!("non-btree index (am \"{amname}\")"));
    }
    if idx.has_exprs {
        gap("pg_get_indexdef", "expression index columns");
    }

    let mut buf = String::new();
    if !attrs_only {
        let relname = if pretty_flags & PRETTYFLAG_SCHEMA != 0 {
            generate_relation_name(mcx, idx.indrelid)?
        } else {
            generate_qualified_relation_name(mcx, idx.indrelid)?
        };
        buf.push_str(&format!(
            "CREATE {}INDEX {} ON {}{} USING {} (",
            if idx.indisunique { "UNIQUE " } else { "" },
            quote_identifier(&idxrel.relname),
            if idxrel.relkind == RELKIND_PARTITIONED_INDEX { "ONLY " } else { "" },
            relname,
            quote_identifier(&amname),
        ));
    }

    let mut sep = "";
    for keyno in 0..idx.indnatts as usize {
        let attnum = idx.indkey[keyno];
        if colno == 0 && keyno == idx.indnkeyatts as usize {
            buf.push_str(") INCLUDE (");
            sep = "";
        }
        if colno == 0 {
            buf.push_str(sep);
        }
        sep = ", ";
        if attnum == 0 {
            gap("pg_get_indexdef", "expression index column");
        }
        let attname = lsyscache::get_attname(mcx, idx.indrelid, attnum, false)?
            .expect("get_attname missing_ok=false");
        if colno == 0 || colno == keyno as i32 + 1 {
            buf.push_str(&quote_identifier(attname.as_str()));
        }
        let (keycoltype, _, keycolcollation) =
            lsyscache::get_atttypetypmodcoll(idx.indrelid, attnum)?;

        if !attrs_only
            && keyno < idx.indnkeyatts as usize
            && (colno == 0 || colno == keyno as i32 + 1)
        {
            let opt = idx.indoption[keyno];
            let indcoll = idx.indcollation[keyno];
            if lsyscache::get_attoptions(mcx, indexrelid, keyno as i16 + 1)? != Datum::null() {
                gap("pg_get_indexdef", "per-column attoptions");
            }
            if indcoll != InvalidOid && indcoll != keycolcollation {
                gap("pg_get_indexdef", "non-default column collation");
            }
            get_opclass_name(mcx, idx.indclass[keyno], keycoltype, &mut buf)?;
            if opt & INDOPTION_DESC != 0 {
                buf.push_str(" DESC");
                if opt & INDOPTION_NULLS_FIRST == 0 {
                    buf.push_str(" NULLS LAST");
                }
            } else if opt & INDOPTION_NULLS_FIRST != 0 {
                buf.push_str(" NULLS FIRST");
            }
        }
    }

    if !attrs_only {
        buf.push(')');
        if idx.indnullsnotdistinct {
            buf.push_str(" NULLS NOT DISTINCT");
        }
        if idxrel.has_reloptions {
            gap("pg_get_indexdef", "index reloptions (WITH ...)");
        }
        if idx.has_pred {
            gap("pg_get_indexdef", "partial index predicate");
        }
    }
    Ok(Some(buf))
}

const CONSTRAINT_FOREIGN: i8 = b'f' as i8;
const CONSTRAINT_PRIMARY: i8 = b'p' as i8;
const CONSTRAINT_UNIQUE: i8 = b'u' as i8;
const CONSTRAINT_CHECK: i8 = b'c' as i8;
const CONSTRAINT_NOTNULL: i8 = b'n' as i8;
const CONSTRAINT_TRIGGER: i8 = b't' as i8;

const FKCONSTR_MATCH_FULL: i8 = b'f' as i8;
const FKCONSTR_MATCH_PARTIAL: i8 = b'p' as i8;
const FKCONSTR_MATCH_SIMPLE: i8 = b's' as i8;
const FKCONSTR_ACTION_NOACTION: i8 = b'a' as i8;
const FKCONSTR_ACTION_RESTRICT: i8 = b'r' as i8;
const FKCONSTR_ACTION_CASCADE: i8 = b'c' as i8;
const FKCONSTR_ACTION_SETNULL: i8 = b'n' as i8;
const FKCONSTR_ACTION_SETDEFAULT: i8 = b'd' as i8;

const ANUM_PG_CONSTRAINT_CONTYPE: i32 = 4;
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

// Divergence from C: pg_get_constraintdef_worker scans pg_constraint under a
// fresh MVCC snapshot; this reads the CONSTROID syscache.
pub fn pg_get_constraintdef_worker(
    mcx: Mcx<'_>,
    constraint_id: Oid,
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
    let connoinherit = getattr(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONNOINHERIT).as_bool();
    let conperiod = getattr(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONPERIOD).as_bool();
    let conkey = getattr_null(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONKEY).map(i16_array_at);
    let confkey = getattr_null(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONFKEY).map(i16_array_at);
    let confdelsetcols =
        getattr_null(&t, CONSTROID, ANUM_PG_CONSTRAINT_CONFDELSETCOLS).map(i16_array_at);
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
        other => gap(
            "pg_get_constraintdef",
            &format!("constraint type '{}'", (other as u8) as char),
        ),
    }
    Ok(Some(buf))
}

pub fn pg_get_expr_worker(
    mcx: Mcx<'_>,
    expr_text: &str,
    relid: Oid,
    pretty_flags: i32,
) -> PgResult<Option<String>> {
    let node = readfuncs::stringToNode(mcx, expr_text)?;
    let mut tst = Some(node);
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

    let mut bad_var = false;
    let mut has_var = false;
    deparse::walk_varnos(node, &mut |varno, levelsup| {
        has_var = true;
        if varno != 1 || levelsup != 0 {
            bad_var = true;
        }
    });
    if relid != InvalidOid {
        if bad_var {
            return Err(PgError::error(
                "expression contains variables of more than one relation".to_string(),
            )
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .into());
        }
    } else if has_var {
        return Err(PgError::error("expression contains variables".to_string())
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .into());
    }

    if relid != InvalidOid {
        // Divergence from C: try_relation_open existence probe without the
        // AccessShareLock (relation_open machinery is another lane).
        if pg_class_row(relid)?.is_none() {
            return Ok(None);
        }
    }
    Ok(Some(deparse_expression_pretty(mcx, node, relid, false, pretty_flags)?))
}
