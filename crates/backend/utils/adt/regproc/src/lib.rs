//! regproc.c reg* I/O slice: regproc/regclass/regtype/regnamespace/regrole,
//! plus regprocedurein/parseNameAndArgTypes for the has_function_privilege
//! family; regoper/regoperator/regconfig/regdictionary/regcollation stay
//! unregistered. Namespace access rides the existing namespace_seams
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

pub fn regtypein(mcx: Mcx<'_>, s: &str, mut esc: Esc) -> PgResult<Option<Oid>> {
    if let Some(handled) = parse_dash_or_oid(s, esc.as_deref_mut())? {
        return Ok(Some(handled.unwrap_or(InvalidOid)));
    }
    match parse_utilcmd_seams::parseTypeString::call(mcx, s) {
        Ok((oid, _typmod)) => Ok(Some(oid)),
        // C plumbs the escontext into parseTypeString, so soft callers see
        // every parse/lookup failure as a soft error.
        Err(e) => match esc {
            Some(esc) => ereturn(Some(esc), Some(InvalidOid), *e),
            None => Err(e),
        },
    }
}

#[cold]
#[inline(never)]
fn improper_arg_list(msg: &'static str) -> PgError {
    PgError::error(msg).with_sqlstate(types_error::ERRCODE_INVALID_TEXT_REPRESENTATION)
}

const fn scanner_isspace(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r' | b'\x0b' | b'\x0c')
}

// parseNameAndArgTypes (regproc.c), allowNone=false slice (the operator
// callers stay unregistered).
fn parse_name_and_arg_types(
    mcx: Mcx<'_>,
    s: &str,
    mut esc: Esc,
) -> PgResult<Option<(Vec<String>, Vec<Oid>)>> {
    const FUNC_MAX_ARGS: usize = 100;
    let bytes = s.as_bytes();
    let mut in_quote = false;
    let mut lparen = None;
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'"' {
            in_quote = !in_quote;
        } else if b == b'(' && !in_quote {
            lparen = Some(i);
            break;
        }
    }
    let Some(lp) = lparen else {
        return ereturn(esc, None, improper_arg_list("expected a left parenthesis"));
    };
    let Some(names) = string_to_qualified_name_list(mcx, &s[..lp], esc.as_deref_mut())? else {
        return Ok(None);
    };

    let rest = &bytes[lp + 1..];
    let mut end = rest.len();
    while end > 0 && scanner_isspace(rest[end - 1]) {
        end -= 1;
    }
    if end == 0 || rest[end - 1] != b')' {
        return ereturn(esc, None, improper_arg_list("expected a right parenthesis"));
    }
    let args = &rest[..end - 1];

    let mut argtypes: Vec<Oid> = Vec::new();
    let mut ptr = 0usize;
    let mut had_comma = false;
    loop {
        while ptr < args.len() && scanner_isspace(args[ptr]) {
            ptr += 1;
        }
        if ptr >= args.len() {
            if had_comma {
                return ereturn(esc, None, improper_arg_list("expected a type name"));
            }
            break;
        }
        let start = ptr;
        let mut in_quote = false;
        let mut paren_count = 0i32;
        while ptr < args.len() {
            let b = args[ptr];
            if b == b'"' {
                in_quote = !in_quote;
            } else if b == b',' && !in_quote && paren_count == 0 {
                break;
            } else if !in_quote {
                match b {
                    b'(' | b'[' => paren_count += 1,
                    b')' | b']' => paren_count -= 1,
                    _ => {}
                }
            }
            ptr += 1;
        }
        if in_quote || paren_count != 0 {
            return ereturn(esc, None, improper_arg_list("improper type name"));
        }
        let mut tend = ptr;
        if ptr < args.len() {
            had_comma = true;
            ptr += 1;
        } else {
            had_comma = false;
        }
        while tend > start && scanner_isspace(args[tend - 1]) {
            tend -= 1;
        }
        let typename = core::str::from_utf8(&args[start..tend])
            .map_err(|_| Box::new(PgError::error("invalid UTF-8 in type name")))?;
        let typeid = match parse_utilcmd_seams::parseTypeString::call(mcx, typename) {
            Ok((oid, _typmod)) => oid,
            Err(e) => match esc {
                Some(esc) => return ereturn(Some(esc), None, *e),
                None => return Err(e),
            },
        };
        if argtypes.len() >= FUNC_MAX_ARGS {
            return ereturn(
                esc,
                None,
                PgError::error("too many arguments")
                    .with_sqlstate(types_error::ERRCODE_TOO_MANY_ARGUMENTS),
            );
        }
        argtypes.push(typeid);
    }

    Ok(Some((names, argtypes)))
}

// regprocedurein (regproc.c).
pub fn regprocedurein(mcx: Mcx<'_>, s: &str, mut esc: Esc) -> PgResult<Option<Oid>> {
    if let Some(handled) = parse_dash_or_oid(s, esc.as_deref_mut())? {
        return Ok(Some(handled.unwrap_or(InvalidOid)));
    }
    let Some((names, argtypes)) = parse_name_and_arg_types(mcx, s, esc.as_deref_mut())? else {
        return Ok(None);
    };
    let refs: Vec<&str> = names.iter().map(|n| n.as_str()).collect();
    // FuncnameGetCandidates(names, nargs, ...) + the exact-argtype scan.
    let cands = funcname_candidates_exact(mcx, &refs, &argtypes)?;
    match cands {
        Some(oid) => Ok(Some(oid)),
        None => ereturn(esc, Some(InvalidOid), undefined_function(s)),
    }
}

fn funcname_candidates_exact(
    mcx: Mcx<'_>,
    names: &[&str],
    argtypes: &[Oid],
) -> PgResult<Option<Oid>> {
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
    let mut best: Option<(usize, Oid)> = None;
    for cand in raw.iter() {
        if cand.proargtypes.as_slice() != argtypes {
            continue;
        }
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
        if best.map(|(bp, _)| pathpos < bp).unwrap_or(true) {
            best = Some((pathpos, cand.oid));
        }
    }
    Ok(best.map(|(_, oid)| oid))
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


// FunctionIsVisible / OperatorIsVisible search-path walk, local to this
// crate (a catalog_namespace dep cycles through fmgr_core).
// format_procedure (regproc.c): "name(argtype,argtype)", schema-qualified
// when not visible on the search path.
pub fn format_procedure(mcx: Mcx<'_>, procedure_oid: Oid) -> PgResult<String> {
    let Some(namedata) = syscache_seams::pg_proc_proname::call(procedure_oid)? else {
        return Ok(procedure_oid.to_string());
    };
    let proname = core::str::from_utf8(namedata.name_str())
        .map_err(|_| Box::new(PgError::error("pg_proc.proname is not UTF-8")))?;
    let (_rettype, argtypes) = lsyscache::get_func_signature(mcx, procedure_oid)?;

    // FunctionIsVisible: only an identical-argtype candidate earlier on the
    // path shadows this oid (FuncnameGetCandidates dedups by argument list).
    let raw = syscache_seams::lookup_pg_proc_name_candidates::call(mcx, proname)?;
    let path = namespace_seams::fetch_search_path::call(mcx, true)?;
    let pos = |nsp: Oid| path.iter().position(|&p| p == nsp);
    let mut visible = false;
    let mut best: Option<(usize, Oid)> = None;
    for cand in raw.iter() {
        if cand.proargtypes.as_slice() != argtypes.as_slice() {
            continue;
        }
        if let Some(p) = pos(cand.pronamespace) {
            if best.map(|(bp, _)| p < bp).unwrap_or(true) {
                best = Some((p, cand.oid));
            }
        }
    }
    if let Some((_, oid)) = best {
        visible = oid == procedure_oid;
    }
    let nspname = if visible {
        None
    } else {
        match syscache_seams::lookup_pg_proc_shape::call(procedure_oid)? {
            Some(shape) => lsyscache::get_namespace_name(mcx, shape.pronamespace)?,
            None => None,
        }
    };

    let mut buf = String::new();
    if let Some(nsp) = &nspname {
        buf.push_str(&format_type::quote_identifier(nsp.as_str()));
        buf.push('.');
    }
    buf.push_str(&format_type::quote_identifier(proname));
    buf.push('(');
    for (i, &t) in argtypes.iter().enumerate() {
        if i > 0 {
            buf.push(',');
        }
        buf.push_str(&format_type::format_type_be(t)?);
    }
    buf.push(')');
    Ok(buf)
}

// format_operator (regproc.c): "name(lefttype,righttype)" with NONE for a
// missing side; schema-qualified when not visible.
pub fn format_operator(mcx: Mcx<'_>, operator_oid: Oid) -> PgResult<String> {
    let Some(namedata) = syscache_seams::pg_operator_oprname::call(operator_oid)? else {
        return Ok(operator_oid.to_string());
    };
    let oprname = core::str::from_utf8(namedata.name_str())
        .map_err(|_| Box::new(PgError::error("pg_operator.oprname is not UTF-8")))?;
    let (oprleft, oprright) = lsyscache::op_input_types(operator_oid)?;

    // OperatorIsVisible: first exact (name,left,right) match on the path.
    let cands =
        syscache_seams::lookup_pg_operator_candidates::call(mcx, oprname, oprleft, oprright)?;
    let path = namespace_seams::fetch_search_path::call(mcx, true)?;
    let mut visible = false;
    let mut my_nsp = InvalidOid;
    'outer: for &nsp in path.iter() {
        for &(oid, oprnamespace) in cands.iter() {
            if oid == operator_oid {
                my_nsp = oprnamespace;
            }
            if oprnamespace == nsp {
                visible = oid == operator_oid;
                break 'outer;
            }
        }
    }
    if my_nsp == InvalidOid {
        for &(oid, oprnamespace) in cands.iter() {
            if oid == operator_oid {
                my_nsp = oprnamespace;
            }
        }
    }

    let mut buf = String::new();
    if !visible {
        if let Some(nsp) = lsyscache::get_namespace_name(mcx, my_nsp)? {
            buf.push_str(&format_type::quote_identifier(nsp.as_str()));
            buf.push('.');
        }
    }
    buf.push_str(oprname);
    buf.push('(');
    if oprleft != InvalidOid {
        buf.push_str(&format_type::format_type_be(oprleft)?);
    } else {
        buf.push_str("NONE");
    }
    buf.push(',');
    if oprright != InvalidOid {
        buf.push_str(&format_type::format_type_be(oprright)?);
    } else {
        buf.push_str("NONE");
    }
    buf.push(')');
    Ok(buf)
}
