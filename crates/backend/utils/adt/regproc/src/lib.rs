//! regproc.c reg* I/O slice: regproc/regclass/regtype/regnamespace/regrole.
//! regtypein's type-name arm needs parseTypeString (loud named panic);
//! regprocedure/regoper/regoperator/regconfig/regdictionary/regcollation
//! stay unregistered. Namespace access rides the existing namespace_seams
//! (direct catalog_namespace dep cycles through fmgr_core); the nargs=-1
//! FuncnameGetCandidates lane and LookupExplicitNamespace's lookup+ACL steps
//! are transcribed here from namespace.c until seams for them exist. The
//! *IsVisible probes use the would-regNNNin-find-it lookups C documents as
//! equivalent.

pub mod builtins;
#[cfg(test)]
mod tests;

use mcx::{Mcx, PgVec};
use types_core::{InvalidOid, Oid, OidIsValid, RELPERSISTENCE_PERMANENT};
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_AMBIGUOUS_FUNCTION,
    ERRCODE_INVALID_NAME, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERRCODE_SYNTAX_ERROR,
    ERRCODE_UNDEFINED_FUNCTION, ERRCODE_UNDEFINED_OBJECT, ERRCODE_UNDEFINED_SCHEMA,
    ERRCODE_UNDEFINED_TABLE,
};
use types_rel::NoLock;

// parsenodes.h ObjectType / acl.h, verified against REL_18_3 (the
// catalog_namespace lookup.rs constants).
const OBJECT_SCHEMA: i32 = 36;
const ACL_USAGE: u64 = 1 << 8;
const ACLCHECK_OK: i32 = 0;

pub type Esc<'a> = Option<&'a mut SoftErrorContext>;
pub type RegName<'mcx> = PgVec<'mcx, u8>;

#[cold]
#[inline(never)]
fn invalid_name_syntax() -> PgError {
    PgError::error("invalid name syntax").with_sqlstate(ERRCODE_INVALID_NAME)
}

#[cold]
#[inline(never)]
fn oid_out_of_range(s: &str) -> PgError {
    PgError::error(format!("value \"{s}\" is out of range for type oid"))
        .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

#[cold]
#[inline(never)]
fn undefined_function(s: &str) -> PgError {
    PgError::error(format!("function \"{s}\" does not exist"))
        .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION)
}

#[cold]
#[inline(never)]
fn ambiguous_function(s: &str) -> PgError {
    PgError::error(format!("more than one function named \"{s}\""))
        .with_sqlstate(ERRCODE_AMBIGUOUS_FUNCTION)
}

#[cold]
#[inline(never)]
fn undefined_relation(names: &[String]) -> PgError {
    PgError::error(format!("relation \"{}\" does not exist", names.join(".")))
        .with_sqlstate(ERRCODE_UNDEFINED_TABLE)
}

#[cold]
#[inline(never)]
fn undefined_schema(name: &str) -> PgError {
    PgError::error(format!("schema \"{name}\" does not exist"))
        .with_sqlstate(ERRCODE_UNDEFINED_SCHEMA)
}

#[cold]
#[inline(never)]
fn undefined_role(name: &str) -> PgError {
    PgError::error(format!("role \"{name}\" does not exist"))
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT)
}

fn parse_numeric_oid(s: &str, esc: Esc) -> PgResult<Option<Option<Oid>>> {
    let b = s.as_bytes();
    let all_digits =
        b.first().is_some_and(|c| c.is_ascii_digit()) && b.iter().all(|c| c.is_ascii_digit());
    if !all_digits {
        return Ok(None);
    }
    // oidin's uint32in_subr on all-digit input: only the overflow arm is reachable.
    let mut v: u64 = 0;
    for &c in b {
        v = v * 10 + (c - b'0') as u64;
        if v > u32::MAX as u64 {
            return ereturn(esc, Some(None), oid_out_of_range(s));
        }
    }
    Ok(Some(Some(v as Oid)))
}

/// C parseDashOrOid: outer None = not handled (name lookup proceeds);
/// inner None = handled but soft-failed (caller returns Datum 0).
fn parse_dash_or_oid(s: &str, esc: Esc) -> PgResult<Option<Option<Oid>>> {
    if s == "-" {
        return Ok(Some(Some(InvalidOid)));
    }
    parse_numeric_oid(s, esc)
}

/// C stringToQualifiedNameList; None = soft-reported failure (caller returns SQL NULL).
pub fn string_to_qualified_name_list(
    mcx: Mcx<'_>,
    s: &str,
    esc: Esc,
) -> PgResult<Option<Vec<String>>> {
    // Vec<String>: split_identifier_string's justified owned-string shape (cold I/O path).
    match varlena::split_identifier_string(mcx, s, b'.', mbutils::GetDatabaseEncoding())? {
        Some(names) if !names.is_empty() => Ok(Some(names)),
        _ => ereturn(esc, None, invalid_name_syntax()),
    }
}

fn make_range_var<'a>(names: &'a [String]) -> PgResult<rel_vocab::RangeVar<'a>> {
    let (catalogname, schemaname, relname) = match names {
        [r] => (None, None, r.as_str()),
        [s, r] => (None, Some(s.as_str()), r.as_str()),
        [c, s, r] => (Some(c.as_str()), Some(s.as_str()), r.as_str()),
        _ => {
            return Err(Box::new(
                PgError::error(format!(
                    "improper relation name (too many dotted names): {}",
                    names.join(".")
                ))
                .with_sqlstate(ERRCODE_SYNTAX_ERROR),
            ))
        }
    };
    Ok(rel_vocab::RangeVar {
        catalogname,
        schemaname,
        relname,
        inh: true,
        relpersistence: RELPERSISTENCE_PERMANENT,
        location: -1,
    })
}

/// C DeconstructQualifiedName, function-name callers only (the catalogname
/// arm needs get_database_name — loud until a consumer shows up).
fn deconstruct_qualified_name<'a>(names: &[&'a str]) -> PgResult<(Option<&'a str>, &'a str)> {
    match names {
        [objname] => Ok((None, objname)),
        [schemaname, objname] => Ok((Some(schemaname), objname)),
        [_, _, _] => panic!(
            "DeconstructQualifiedName (namespace.c): catalog-qualified name arm \
             (cross-database check) unported"
        ),
        _ => Err(Box::new(
            PgError::error(format!(
                "improper qualified name (too many dotted names): {}",
                names.join(".")
            ))
            .with_sqlstate(ERRCODE_SYNTAX_ERROR),
        )),
    }
}

/// C LookupExplicitNamespace(missing_ok=false): lookup + ACL_USAGE check.
/// The pg_temp alias arm needs myTempNamespace — loud.
fn lookup_explicit_namespace(nspname: &str) -> PgResult<Oid> {
    if nspname == "pg_temp" {
        panic!("LookupExplicitNamespace (namespace.c): pg_temp alias arm unported in adt_regproc");
    }
    let namespace_id = syscache_seams::lookup_pg_namespace_oid_by_name::call(nspname)?;
    if !OidIsValid(namespace_id) {
        return Err(Box::new(undefined_schema(nspname)));
    }
    let aclresult = aclchk_seams::object_aclcheck::call(
        types_core::catalog::NAMESPACE_RELATION_ID,
        namespace_id,
        miscinit_seams::get_user_id::call(),
        ACL_USAGE,
    )?;
    if aclresult != ACLCHECK_OK {
        aclchk_seams::aclcheck_error::call(aclresult, OBJECT_SCHEMA, nspname)?;
    }
    Ok(namespace_id)
}

struct FuncCand {
    oid: Oid,
    pathpos: usize,
    raw_index: usize,
}

/// C FuncnameGetCandidates(names, -1, NIL, false, false, false, _): every
/// arity, path-ordered, same-signature shadowing resolved to the earlier
/// namespace.
fn funcname_candidates_any(mcx: Mcx<'_>, names: &[&str]) -> PgResult<Vec<Oid>> {
    let (schemaname, funcname) = deconstruct_qualified_name(names)?;
    let raw = syscache_seams::lookup_pg_proc_name_candidates::call(mcx, funcname)?;
    let ns_filter = match schemaname {
        Some(name) => Some(lookup_explicit_namespace(name)?),
        None => None,
    };
    let path = match ns_filter {
        Some(_) => None,
        None => Some(namespace_seams::fetch_search_path::call(mcx, true)?),
    };
    let mut kept: Vec<FuncCand> = Vec::new();
    for (i, cand) in raw.iter().enumerate() {
        let pathpos = match (&ns_filter, &path) {
            (Some(id), _) => {
                if cand.pronamespace != *id {
                    continue;
                }
                0
            }
            (None, Some(p)) => match p.iter().position(|&n| n == cand.pronamespace) {
                Some(pos) => pos,
                None => continue,
            },
            (None, None) => unreachable!(),
        };
        match kept.iter_mut().find(|prev| {
            raw[prev.raw_index].proargtypes.as_slice() == cand.proargtypes.as_slice()
        }) {
            Some(prev) => {
                if pathpos < prev.pathpos {
                    *prev = FuncCand { oid: cand.oid, pathpos, raw_index: i };
                }
            }
            None => kept.push(FuncCand { oid: cand.oid, pathpos, raw_index: i }),
        }
    }
    Ok(kept.into_iter().map(|c| c.oid).collect())
}

fn range_var_get_relid(
    mcx: Mcx<'_>,
    rv: &rel_vocab::RangeVar<'_>,
    missing_ok: bool,
) -> PgResult<Oid> {
    namespace_seams::range_var_get_relid::call(mcx, rv, NoLock, missing_ok)
}

fn unqualified_rv(relname: &str) -> rel_vocab::RangeVar<'_> {
    rel_vocab::RangeVar {
        catalogname: None,
        schemaname: None,
        relname,
        inh: true,
        relpersistence: RELPERSISTENCE_PERMANENT,
        location: -1,
    }
}

pub fn regprocin(mcx: Mcx<'_>, s: &str, mut esc: Esc) -> PgResult<Option<Oid>> {
    if let Some(handled) = parse_dash_or_oid(s, esc.as_deref_mut())? {
        return Ok(Some(handled.unwrap_or(InvalidOid)));
    }
    let Some(names) = string_to_qualified_name_list(mcx, s, esc.as_deref_mut())? else {
        return Ok(None);
    };
    let refs: Vec<&str> = names.iter().map(|n| n.as_str()).collect();
    let cands = funcname_candidates_any(mcx, &refs)?;
    match cands.as_slice() {
        [] => ereturn(esc, Some(InvalidOid), undefined_function(s)),
        [oid] => Ok(Some(*oid)),
        _ => ereturn(esc, Some(InvalidOid), ambiguous_function(s)),
    }
}

pub fn regclassin(mcx: Mcx<'_>, s: &str, mut esc: Esc) -> PgResult<Option<Oid>> {
    if let Some(handled) = parse_dash_or_oid(s, esc.as_deref_mut())? {
        return Ok(Some(handled.unwrap_or(InvalidOid)));
    }
    let Some(names) = string_to_qualified_name_list(mcx, s, esc.as_deref_mut())? else {
        return Ok(None);
    };
    let rv = make_range_var(&names)?;
    let result = range_var_get_relid(mcx, &rv, true)?;
    if !OidIsValid(result) {
        return ereturn(esc, Some(InvalidOid), undefined_relation(&names));
    }
    Ok(Some(result))
}

pub fn regnamespacein(mcx: Mcx<'_>, s: &str, mut esc: Esc) -> PgResult<Option<Oid>> {
    if let Some(handled) = parse_dash_or_oid(s, esc.as_deref_mut())? {
        return Ok(Some(handled.unwrap_or(InvalidOid)));
    }
    let Some(names) = string_to_qualified_name_list(mcx, s, esc.as_deref_mut())? else {
        return Ok(None);
    };
    let [name] = names.as_slice() else {
        return ereturn(esc, Some(InvalidOid), invalid_name_syntax());
    };
    let result = syscache_seams::lookup_pg_namespace_oid_by_name::call(name)?;
    if !OidIsValid(result) {
        return ereturn(esc, Some(InvalidOid), undefined_schema(name));
    }
    Ok(Some(result))
}

pub fn regrolein(mcx: Mcx<'_>, s: &str, mut esc: Esc) -> PgResult<Option<Oid>> {
    if let Some(handled) = parse_dash_or_oid(s, esc.as_deref_mut())? {
        return Ok(Some(handled.unwrap_or(InvalidOid)));
    }
    let Some(names) = string_to_qualified_name_list(mcx, s, esc.as_deref_mut())? else {
        return Ok(None);
    };
    let [name] = names.as_slice() else {
        return ereturn(esc, Some(InvalidOid), invalid_name_syntax());
    };
    match syscache_seams::lookup_authid_by_rolname::call(name)? {
        Some((oid, _)) => Ok(Some(oid)),
        None => ereturn(esc, Some(InvalidOid), undefined_role(name)),
    }
}

pub fn regtypein(_mcx: Mcx<'_>, s: &str, mut esc: Esc) -> PgResult<Option<Oid>> {
    if let Some(handled) = parse_dash_or_oid(s, esc.as_deref_mut())? {
        return Ok(Some(handled.unwrap_or(InvalidOid)));
    }
    panic!(
        "regtypein (regproc.c): type-name arm requires parseTypeString (raw parser) — \
         unported; input {s:?}"
    );
}

fn cstr_in<'mcx>(mcx: Mcx<'mcx>, parts: &[&[u8]]) -> PgResult<RegName<'mcx>> {
    let len: usize = parts.iter().map(|p| p.len()).sum();
    let mut v = mcx::vec_with_capacity_in(mcx, len + 1)?;
    for p in parts {
        mcx::vec_append_bytes(&mut v, p)?;
    }
    mcx::vec_append_bytes(&mut v, &[0])?;
    Ok(v)
}

fn oid_numeric_cstr(mcx: Mcx<'_>, oid: Oid) -> PgResult<RegName<'_>> {
    let mut buf = [0u8; 10];
    let mut n = oid;
    let mut i = buf.len();
    loop {
        i -= 1;
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    cstr_in(mcx, &[&buf[i..]])
}

fn quote_qualified<'mcx>(
    mcx: Mcx<'mcx>,
    nspname: Option<&str>,
    ident: &str,
) -> PgResult<RegName<'mcx>> {
    let quoted = format_type::quote_identifier(ident);
    match nspname {
        Some(nsp) => {
            let qnsp = format_type::quote_identifier(nsp);
            cstr_in(mcx, &[qnsp.as_bytes(), b".", quoted.as_bytes()])
        }
        None => cstr_in(mcx, &[quoted.as_bytes()]),
    }
}

pub fn regprocout(mcx: Mcx<'_>, proid: Oid) -> PgResult<RegName<'_>> {
    if proid == InvalidOid {
        return cstr_in(mcx, &[b"-"]);
    }
    let Some(namedata) = syscache_seams::pg_proc_proname::call(proid)? else {
        return oid_numeric_cstr(mcx, proid);
    };
    let proname = core::str::from_utf8(namedata.name_str())
        .map_err(|_| Box::new(PgError::error("pg_proc.proname is not UTF-8")))?;
    let cands = funcname_candidates_any(mcx, &[proname])?;
    if matches!(cands.as_slice(), [oid] if *oid == proid) {
        return quote_qualified(mcx, None, proname);
    }
    let nspname = match syscache_seams::lookup_pg_proc_shape::call(proid)? {
        Some(shape) => lsyscache::get_namespace_name(mcx, shape.pronamespace)?,
        None => None,
    };
    quote_qualified(mcx, nspname.as_deref(), proname)
}

pub fn regclassout(mcx: Mcx<'_>, classid: Oid) -> PgResult<RegName<'_>> {
    if classid == InvalidOid {
        return cstr_in(mcx, &[b"-"]);
    }
    let Some(relname) = lsyscache::get_rel_name(mcx, classid)? else {
        return oid_numeric_cstr(mcx, classid);
    };
    // C RelationIsVisible == "would regclassin find it unqualified".
    let visible = range_var_get_relid(mcx, &unqualified_rv(&relname), true)? == classid;
    let nspname = if visible {
        None
    } else {
        lsyscache::get_namespace_name(mcx, lsyscache::get_rel_namespace(classid)?)?
    };
    quote_qualified(mcx, nspname.as_deref(), &relname)
}

// ts_cache's TSConfig/TSDictionary name resolution (missing_ok shape), local
// copy: a ts_cache dep would cycle (ts_cache -> adt_regproc for name parsing).
fn ts_search_path_lookup(
    names: &[&str],
    by_name: fn(&str, Oid) -> PgResult<Oid>,
) -> PgResult<Oid> {
    match names {
        [name] => {
            let scratch = mcx::MemoryContext::new("ts_search_path_lookup");
            for nsp in namespace_seams::fetch_search_path::call(scratch.mcx(), true)?.iter() {
                if namespace_seams::is_temp_namespace::call(*nsp) {
                    continue;
                }
                let oid = by_name(name, *nsp)?;
                if OidIsValid(oid) {
                    return Ok(oid);
                }
            }
            Ok(InvalidOid)
        }
        [schemaname, name] => {
            let nsp = namespace_seams::lookup_explicit_namespace::call(schemaname, true)?;
            if !OidIsValid(nsp) {
                return Ok(InvalidOid);
            }
            by_name(name, nsp)
        }
        _ => Ok(InvalidOid),
    }
}

fn ts_config_by_name(n: &str, nsp: Oid) -> PgResult<Oid> {
    syscache_seams::lookup_pg_ts_config_oid_by_name::call(n, nsp)
}

fn ts_dict_by_name(n: &str, nsp: Oid) -> PgResult<Oid> {
    syscache_seams::lookup_pg_ts_dict_oid_by_name::call(n, nsp)
}

pub fn regconfigin(mcx: Mcx<'_>, s: &str, mut esc: Esc) -> PgResult<Option<Oid>> {
    if let Some(handled) = parse_dash_or_oid(s, esc.as_deref_mut())? {
        return Ok(Some(handled.unwrap_or(InvalidOid)));
    }
    let Some(names) = string_to_qualified_name_list(mcx, s, esc.as_deref_mut())? else {
        return Ok(None);
    };
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let result = ts_search_path_lookup(&refs, ts_config_by_name)?;
    if !OidIsValid(result) {
        return ereturn(
            esc,
            Some(InvalidOid),
            PgError::error(format!("text search configuration \"{s}\" does not exist"))
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
        );
    }
    Ok(Some(result))
}

pub fn regdictionaryin(mcx: Mcx<'_>, s: &str, mut esc: Esc) -> PgResult<Option<Oid>> {
    if let Some(handled) = parse_dash_or_oid(s, esc.as_deref_mut())? {
        return Ok(Some(handled.unwrap_or(InvalidOid)));
    }
    let Some(names) = string_to_qualified_name_list(mcx, s, esc.as_deref_mut())? else {
        return Ok(None);
    };
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let result = ts_search_path_lookup(&refs, ts_dict_by_name)?;
    if !OidIsValid(result) {
        return ereturn(
            esc,
            Some(InvalidOid),
            PgError::error(format!("text search dictionary \"{s}\" does not exist"))
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
        );
    }
    Ok(Some(result))
}

pub fn regconfigout(mcx: Mcx<'_>, cfgid: Oid) -> PgResult<RegName<'_>> {
    if cfgid == InvalidOid {
        return cstr_in(mcx, &[b"-"]);
    }
    let Some(shape) = syscache_seams::lookup_pg_ts_config_shape::call(cfgid)? else {
        return oid_numeric_cstr(mcx, cfgid);
    };
    let name = core::str::from_utf8(shape.cfgname.name_str())
        .map_err(|_| Box::new(PgError::error("pg_ts_config.cfgname is not UTF-8")))?;
    let visible = ts_search_path_lookup(&[name], ts_config_by_name)? == cfgid;
    let nspname = if visible {
        None
    } else {
        lsyscache::get_namespace_name(mcx, shape.cfgnamespace)?
    };
    quote_qualified(mcx, nspname.as_deref(), name)
}

pub fn regdictionaryout(mcx: Mcx<'_>, dictid: Oid) -> PgResult<RegName<'_>> {
    if dictid == InvalidOid {
        return cstr_in(mcx, &[b"-"]);
    }
    let Some(shape) = syscache_seams::lookup_pg_ts_dict_shape::call(mcx, dictid)? else {
        return oid_numeric_cstr(mcx, dictid);
    };
    let name = core::str::from_utf8(shape.dictname.name_str())
        .map_err(|_| Box::new(PgError::error("pg_ts_dict.dictname is not UTF-8")))?;
    let visible = ts_search_path_lookup(&[name], ts_dict_by_name)? == dictid;
    let nspname = if visible {
        None
    } else {
        lsyscache::get_namespace_name(mcx, shape.dictnamespace)?
    };
    quote_qualified(mcx, nspname.as_deref(), name)
}

pub fn regtypeout(mcx: Mcx<'_>, typid: Oid) -> PgResult<RegName<'_>> {
    if typid == InvalidOid {
        return cstr_in(mcx, &[b"-"]);
    }
    if syscache_seams::lookup_pg_type_typcache_shape::call(typid)?.is_none() {
        return oid_numeric_cstr(mcx, typid);
    }
    let name = format_type::format_type_be(typid)?;
    cstr_in(mcx, &[name.as_bytes()])
}

pub fn regnamespaceout(mcx: Mcx<'_>, nspid: Oid) -> PgResult<RegName<'_>> {
    if nspid == InvalidOid {
        return cstr_in(mcx, &[b"-"]);
    }
    match lsyscache::get_namespace_name(mcx, nspid)? {
        Some(name) => quote_qualified(mcx, None, &name),
        None => oid_numeric_cstr(mcx, nspid),
    }
}

pub fn regroleout(mcx: Mcx<'_>, roleoid: Oid) -> PgResult<RegName<'_>> {
    if roleoid == InvalidOid {
        return cstr_in(mcx, &[b"-"]);
    }
    match syscache_seams::lookup_authid_rolname::call(mcx, roleoid)? {
        Some(name) => quote_qualified(mcx, None, &name),
        None => oid_numeric_cstr(mcx, roleoid),
    }
}

pub fn text_regclass(mcx: Mcx<'_>, s: &str) -> PgResult<Oid> {
    let Some(names) = string_to_qualified_name_list(mcx, s, None)? else {
        unreachable!("hard-error path returns Err");
    };
    let rv = make_range_var(&names)?;
    range_var_get_relid(mcx, &rv, false)
}
