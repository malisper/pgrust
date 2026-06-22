//! Idiomatic owned-tree port of `src/backend/parser/parse_oper.c`
//! (PostgreSQL 18.3) — handle operator things for the parser.
//!
//! Every public function and static helper of `parse_oper.c` is ported 1:1
//! against the C source (branch order, error text, SQLSTATE), keyed on the
//! repo's owned [`types_nodes::primnodes::Expr`] expression tree:
//!
//!   * [`LookupOperName`], [`LookupOperWithArgs`], [`get_sort_group_operators`];
//!   * [`oprid`], [`oprfuncid`] (accessors over a resolved operator row);
//!   * [`oper`], [`compatible_oper`], [`compatible_oper_opid`], [`left_oper`];
//!   * [`op_signature_string`];
//!   * [`make_op`], [`make_scalar_array_op`].
//!
//! Static helpers (`binary_oper_exact`, `oper_select_candidate`, `op_error`, and
//! the operator lookaside cache: `make_oper_cache_key`, `find_oper_cache_entry`,
//! `make_oper_cache_entry`, `InvalidateOprCacheCallBack`) are ported as
//! crate-private functions. The cache is faithful to C's hash-table behavior — a
//! per-backend, lazily-initialized lookaside flushed wholesale on operator/cast
//! invalidation — modelled as a process-global map (PostgreSQL backends are
//! single-threaded), keyed on the zero-filled `OprCacheKey`.
//!
//! # The pointer / fmgr boundary
//!
//! No `extern "C"`, no raw pointers. Soft errors flow through
//! `backend-utils-error`. Catalog/namespace, syscache (the `pg_operator` row),
//! typcache, lsyscache, the still-absent sibling parser crates
//! (`parse_type` / `parse_coerce` / `parse_func`), and the operator-cache
//! invalidation registration are reached through their per-owner seam crates
//! (loud-panic until the owner installs them). [`backend_nodes_core::expr_type`]
//! (`nodeFuncs.c`) is a ported sibling called directly (no dep cycle).
//!
//! The C `Operator` is a `SearchSysCache1(OPEROID)` `HeapTuple` plus a
//! `(Form_pg_operator) GETSTRUCT(op)` read, released with `ReleaseSysCache`. The
//! owned-tree port has no raw syscache tuple to hold, so the resolved operator is
//! carried by value as a decoded [`ResolvedOper`] (the syscache seam's
//! `oper_row_by_oid` returns it); `oprid`/`oprfuncid` read its fields, and the
//! `ReleaseSysCache` calls dissolve (value semantics), which is why `oper` /
//! `left_oper` / `compatible_oper` return `Option<ResolvedOper>` rather than an
//! opaque handle.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

use std::collections::BTreeMap;
use std::sync::Mutex;

use backend_utils_error::{ereport, PgError, PgResult};
use mcx::{Mcx, MemoryContext};

use types_core::fmgr::NAMEDATALEN;
use types_core::primitive::{Oid, OidIsValid, INVALID_OID as InvalidOid};
use types_error::{
    ERRCODE_AMBIGUOUS_FUNCTION, ERRCODE_INTERNAL_ERROR, ERRCODE_SYNTAX_ERROR,
    ERRCODE_UNDEFINED_FUNCTION, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_nodes::primnodes::{Expr, OpExpr, ScalarArrayOpExpr};
use types_parsenodes::ParseState;
use types_tuple::heaptuple::{
    ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID, ANYCOMPATIBLENONARRAYOID,
    ANYCOMPATIBLEOID, ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYENUMOID, ANYMULTIRANGEOID,
    ANYNONARRAYOID, ANYRANGEOID, BOOLOID, UNKNOWNOID,
};

// Outward seam aliases.
use backend_catalog_namespace_seams::{
    lookup_explicit_namespace, opername_get_candidates, opername_get_oprid,
};
use backend_nodes_core::nodefuncs::expr_type as exprType;
use backend_parser_coerce_seams::{enforce_generic_type_consistency, is_binary_coercible};
use backend_parser_parse_func_seams::{
    check_srf_call_placement, func_match_argtypes, func_select_candidate, make_fn_arguments,
    set_last_srf,
};
use backend_parser_parse_type_seams::{lookup_type_name_oid, typename_type_id};
use backend_utils_cache_lsyscache_seams::{
    get_array_type, get_base_element_type, get_base_type, get_func_retset,
};
use backend_utils_cache_syscache_seams::oper_row_by_oid;
use backend_utils_cache_typcache_seams::sort_group_operators;

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// Small ABI predicates ported from the C macros (postgres.h / pg_type.h).
// ---------------------------------------------------------------------------

/// `RegProcedureIsValid(p)` — a regproc is valid iff it is not `InvalidOid`.
#[inline]
fn reg_procedure_is_valid(p: Oid) -> bool {
    OidIsValid(p)
}

/// `IsPolymorphicType(typid)` — the `IsPolymorphicTypeFamily1` set (pg_type.h):
/// the pseudo-types whose concrete type depends on the call site.
#[inline]
fn is_polymorphic_type(typid: Oid) -> bool {
    matches!(
        typid,
        ANYELEMENTOID
            | ANYARRAYOID
            | ANYNONARRAYOID
            | ANYENUMOID
            | ANYRANGEOID
            | ANYMULTIRANGEOID
            | ANYCOMPATIBLEOID
            | ANYCOMPATIBLEARRAYOID
            | ANYCOMPATIBLENONARRAYOID
            | ANYCOMPATIBLERANGEOID
            | ANYCOMPATIBLEMULTIRANGEOID
    )
}

/// `parser_errposition(pstate, location)` — the cursor position to attach to an
/// ereport. A no-op returning 0 when `location < 0` or `pstate == NULL`.
#[inline]
fn errpos(pstate: Option<&ParseState<'_>>, location: i32) -> i32 {
    if location < 0 || pstate.is_none() {
        return 0;
    }
    // The repo's trimmed ParseState carries only p_sourcetext; the raw byte
    // location IS the cursor position the C parser_errposition resolves to
    // (parse_node.c maps p_sourcetext-relative byte offset -> char position;
    // with only the source text we reproduce the +1 1-based column the same way
    // parser_errposition does for an ASCII source).
    location + 1
}

/// Borrow `opname` (`&[String]`, the inward-seam name shape) as `&[&str]` for
/// the namespace seams.
#[inline]
fn name_refs(opname: &[String]) -> Vec<&str> {
    opname.iter().map(|s| s.as_str()).collect()
}

/// Build an `elog(ERROR, ...)`-style internal error for a "can't happen"
/// control-flow path. With `no_error == false`, `oper`/`left_oper` never return
/// `Ok(None)`, so the `make_op`/`make_scalar_array_op` callers that unwrap the
/// operator can surface that impossible `None` as a normal `PgError`.
#[cold]
fn internal_error(what: &str) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INTERNAL_ERROR)
        .errmsg_internal(format!("{what} unexpectedly returned no operator"))
        .into_error()
}

/// The resolved `pg_operator` row, decoded by value. The C `Operator` is a
/// `SearchSysCache1(OPEROID)` `HeapTuple`; the owned port carries the decoded
/// fields `oprid`/`oprfuncid`/`make_*` read (`oprname` is never inspected here,
/// so it is not retained). Mirrors `(Form_pg_operator) GETSTRUCT(op)`.
#[derive(Clone, Copy, Debug)]
pub struct ResolvedOper {
    /// `oid` — the operator OID.
    pub oid: Oid,
    /// `oprleft` — left operand type (`0` for a prefix op).
    pub oprleft: Oid,
    /// `oprright` — right operand type.
    pub oprright: Oid,
    /// `oprresult` — result type.
    pub oprresult: Oid,
    /// `oprcode` — underlying function OID.
    pub oprcode: Oid,
}

/// `FuncDetailCode` (parse_func.h) — the candidate-resolution result codes the
/// static `oper_select_candidate` returns and `op_error` consumes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FuncDetailCode {
    /// `FUNCDETAIL_NOTFOUND` — no candidate matched.
    NotFound,
    /// `FUNCDETAIL_MULTIPLE` — ambiguous; no single best candidate.
    Multiple,
    /// `FUNCDETAIL_NORMAL` — exactly one best candidate.
    Normal,
}

// ===========================================================================
// LookupOperName / LookupOperWithArgs.
// ===========================================================================

/// `LookupOperName()` (parse_oper.c:98)
///
/// Given a possibly-qualified operator name and exact input datatypes, look up
/// the operator. Pass `oprleft = InvalidOid` for a prefix op. If the operator is
/// not found, returns `InvalidOid` if `no_error` is true, else raises an error.
pub fn LookupOperName(
    pstate: Option<&ParseState<'_>>,
    opername: &[String],
    oprleft: Oid,
    oprright: Oid,
    no_error: bool,
    location: i32,
) -> PgResult<Oid> {
    let cx = MemoryContext::new("LookupOperName");
    let names = name_refs(opername);
    let result = opername_get_oprid::call(cx.mcx(), &names, oprleft, oprright)?;
    if OidIsValid(result) {
        return Ok(result);
    }

    /* we don't use op_error here because only an exact match is wanted */
    if !no_error {
        if !OidIsValid(oprright) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("postfix operators are not supported")
                .errposition(errpos(pstate, location))
                .into_error());
        }

        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "operator does not exist: {}",
                op_signature_string(opername, oprleft, oprright)?
            ))
            .errposition(errpos(pstate, location))
            .into_error());
    }

    Ok(InvalidOid)
}

/// `LookupOperWithArgs()` (parse_oper.c:132)
///
/// Like [`LookupOperName`], but the argument types are specified by an
/// `ObjectWithArgs` node, whose two `objargs` entries are `TypeName`s.
pub fn LookupOperWithArgs(
    oper: &types_opclass::ObjectWithArgs,
    no_error: bool,
) -> PgResult<Oid> {
    // Assert(list_length(oper->objargs) == 2);
    debug_assert_eq!(oper.objargs.len(), 2);

    let oprleft = type_name_arg(&oper.objargs, 0);
    let oprright = type_name_arg(&oper.objargs, 1);

    let leftoid = match oprleft {
        None => InvalidOid,
        Some(tn) => typename_type_id_opclass(tn, no_error)?,
    };

    let rightoid = match oprright {
        None => InvalidOid,
        Some(tn) => typename_type_id_opclass(tn, no_error)?,
    };

    LookupOperName(None, &oper.objname, leftoid, rightoid, no_error, -1)
}

/// `LookupTypeNameOid(NULL, typeName, noError)` over an opclass [`TypeName`]:
/// resolve to its type OID. `typenameTypeId` raises on a missing/shell type;
/// `no_error` here mirrors C's `LookupTypeNameOid(... , noError)` — with
/// `no_error = true` a miss yields `InvalidOid` rather than `Err`.
fn typename_type_id_opclass(
    tn: &types_opclass::TypeName,
    no_error: bool,
) -> PgResult<Oid> {
    match typename_type_id::call(tn) {
        Ok(oid) => Ok(oid),
        Err(e) => {
            if no_error {
                Ok(InvalidOid)
            } else {
                Err(e)
            }
        }
    }
}

/// `linitial_node(TypeName, list)` / `lsecond_node(TypeName, list)` for
/// `LookupOperWithArgs`: the i-th `objargs` entry (a `TypeName`), or `None` for
/// a NULL list element (a prefix operator's missing left operand).
fn type_name_arg(objargs: &[types_opclass::TypeName], i: usize) -> Option<&types_opclass::TypeName> {
    objargs.get(i)
}

/// `LookupOperWithArgs(oper, missing_ok)` over the raw-parser
/// [`types_parsenodes::ObjectWithArgs`] (the `get_object_address`
/// `OBJECT_OPERATOR` arm). Each `objargs` entry is a `Node::TypeName`.
pub fn LookupOperWithArgs_node(
    oper: &types_parsenodes::ObjectWithArgs,
    missing_ok: bool,
) -> PgResult<Oid> {
    // Assert(list_length(oper->objargs) == 2);
    debug_assert_eq!(oper.objargs.len(), 2);

    let oprleft = node_type_name_arg(&oper.objargs, 0);
    let oprright = node_type_name_arg(&oper.objargs, 1);

    let leftoid = match oprleft {
        None => InvalidOid,
        Some(tn) => lookup_type_name_oid::call(tn, missing_ok)?,
    };

    let rightoid = match oprright {
        None => InvalidOid,
        Some(tn) => lookup_type_name_oid::call(tn, missing_ok)?,
    };

    LookupOperName(None, &oper.objname, leftoid, rightoid, missing_ok, -1)
}

/// `linitial_node(TypeName, list)` / `lsecond_node(TypeName, list)` over a
/// raw-parser `objargs` list: the i-th entry as a [`types_parsenodes::TypeName`],
/// or `None` for a NULL element.
fn node_type_name_arg(
    objargs: &[types_parsenodes::Node],
    i: usize,
) -> Option<&types_parsenodes::TypeName> {
    match objargs.get(i) {
        Some(n) => n.as_typename(),
        None => None,
    }
}

// ===========================================================================
// get_sort_group_operators.
// ===========================================================================

/// The result tuple of [`get_sort_group_operators`] — the C function's four
/// `Oid *`/`bool *` output parameters.
#[derive(Clone, Copy, Debug, Default)]
pub struct SortGroupOperators {
    pub lt_opr: Oid,
    pub eq_opr: Oid,
    pub gt_opr: Oid,
    pub is_hashable: bool,
}

/// `get_sort_group_operators()` (parse_oper.c:179)
///
/// Get default sorting/grouping operators for a type. We fetch `<`, `=`, `>` all
/// at once via the type cache; a given type might have only `=` (hashable but not
/// sortable). Throws a standard error if a needed operator is missing.
///
/// `want_hashable` corresponds to the C caller passing a non-NULL `isHashable`
/// (toggling `TYPECACHE_HASH_PROC`); pass `true` to also compute hashability.
pub fn get_sort_group_operators(
    argtype: Oid,
    need_lt: bool,
    need_eq: bool,
    need_gt: bool,
    want_hashable: bool,
) -> PgResult<SortGroupOperators> {
    /*
     * Look up the operators using the type cache.
     *
     * Note: the search algorithm used by typcache.c ensures that the results
     * are consistent, ie all from matching opclasses.
     */
    let (lt_opr, eq_opr, gt_opr, hashable) = sort_group_operators::call(argtype, want_hashable)?;

    /* Report errors if needed */
    if (need_lt && !OidIsValid(lt_opr)) || (need_gt && !OidIsValid(gt_opr)) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "could not identify an ordering operator for type {}",
                format_type_be(argtype)?
            ))
            .errhint("Use an explicit ordering operator or modify the query.")
            .into_error());
    }
    if need_eq && !OidIsValid(eq_opr) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "could not identify an equality operator for type {}",
                format_type_be(argtype)?
            ))
            .into_error());
    }

    /* Return results as needed */
    Ok(SortGroupOperators {
        lt_opr,
        eq_opr,
        gt_opr,
        is_hashable: hashable,
    })
}

// ===========================================================================
// oprid / oprfuncid.
// ===========================================================================

/// `oprid()` (parse_oper.c:238): given an operator row, return the operator OID.
#[inline]
pub fn oprid(op: &ResolvedOper) -> Oid {
    op.oid
}

/// `oprfuncid()` (parse_oper.c:245): given an operator row, return the
/// underlying function's OID (`oprcode`).
#[inline]
pub fn oprfuncid(op: &ResolvedOper) -> Oid {
    op.oprcode
}

// ===========================================================================
// binary_oper_exact / oper_select_candidate.
// ===========================================================================

/// `binary_oper_exact()` (parse_oper.c:262)
///
/// Check for an "exact" match to the specified operand types. If one operand is
/// an unknown literal, assume it should be taken to be the same type as the other
/// operand; also consider that the other operand might be a domain type to be
/// reduced to its base type for an "exact" match.
fn binary_oper_exact(opname: &[String], mut arg1: Oid, mut arg2: Oid) -> PgResult<Oid> {
    let mut was_unknown = false;

    /* Unspecified type for one of the arguments? then use the other */
    if (arg1 == UNKNOWNOID) && (arg2 != InvalidOid) {
        arg1 = arg2;
        was_unknown = true;
    } else if (arg2 == UNKNOWNOID) && (arg1 != InvalidOid) {
        arg2 = arg1;
        was_unknown = true;
    }

    let cx = MemoryContext::new("binary_oper_exact");
    let names = name_refs(opname);
    let result = opername_get_oprid::call(cx.mcx(), &names, arg1, arg2)?;
    if OidIsValid(result) {
        return Ok(result);
    }

    if was_unknown {
        /* arg1 and arg2 are the same here, need only look at arg1 */
        let basetype = get_base_type::call(arg1)?;

        if basetype != arg1 {
            let result = opername_get_oprid::call(cx.mcx(), &names, basetype, basetype)?;
            if OidIsValid(result) {
                return Ok(result);
            }
        }
    }

    Ok(InvalidOid)
}

/// `oper_select_candidate()` (parse_oper.c:312)
///
/// Given the input argtype array and one or more candidates for the operator,
/// attempt to resolve the conflict. Returns the [`FuncDetailCode`] and, on
/// success, the OID of the best candidate.
fn oper_select_candidate(
    mcx: Mcx<'_>,
    nargs: i32,
    input_typeids: &[Oid],
    candidates: &types_namespace::FuncCandidateList<'_>,
) -> PgResult<(FuncDetailCode, Oid)> {
    /*
     * Delete any candidates that cannot actually accept the given input types,
     * whether directly or by coercion.
     */
    let candidates = func_match_argtypes::call(mcx, nargs, input_typeids, candidates)?;
    let ncandidates = candidates.len();

    /* Done if no candidate or only one candidate survives */
    if ncandidates == 0 {
        return Ok((FuncDetailCode::NotFound, InvalidOid));
    }
    if ncandidates == 1 {
        return Ok((FuncDetailCode::Normal, candidates[0].oid));
    }

    /*
     * Use the same heuristics as for ambiguous functions to resolve the
     * conflict.
     */
    let best = func_select_candidate::call(mcx, nargs, input_typeids, &candidates)?;

    if let Some(best_oid) = best {
        return Ok((FuncDetailCode::Normal, best_oid));
    }

    Ok((FuncDetailCode::Multiple, InvalidOid)) /* failed to select a best candidate */
}

// ===========================================================================
// oper / compatible_oper / compatible_oper_opid / left_oper.
// ===========================================================================

/// `SearchSysCache1(OPEROID, operOid)` — fetch the decoded `pg_operator` row by
/// OID (the C holds a syscache tuple released by `ReleaseSysCache`; the owned
/// port decodes the consumed fields into a by-value [`ResolvedOper`]).
/// `Ok(None)` on cache miss (the C `!HeapTupleIsValid(tup)`).
fn search_oper_row(oper_oid: Oid) -> PgResult<Option<ResolvedOper>> {
    let cx = MemoryContext::new("search_oper_row");
    let row = oper_row_by_oid::call(cx.mcx(), oper_oid)?;
    Ok(row.map(|r| ResolvedOper {
        oid: r.oid,
        oprleft: r.oprleft,
        oprright: r.oprright,
        oprresult: r.oprresult,
        oprcode: r.oprcode,
    }))
}

/// `oper()` (parse_oper.c:370): search for a binary operator.
pub fn oper(
    pstate: Option<&ParseState<'_>>,
    opname: &[String],
    mut ltype_id: Oid,
    mut rtype_id: Oid,
    no_error: bool,
    location: i32,
) -> PgResult<Option<ResolvedOper>> {
    let mut oper_oid;
    let mut fdresult = FuncDetailCode::NotFound;
    let mut tup: Option<ResolvedOper> = None;

    /*
     * Try to find the mapping in the lookaside cache.
     */
    let mut key = OprCacheKey::default();
    let key_ok = make_oper_cache_key(pstate, &mut key, opname, ltype_id, rtype_id, location)?;

    if key_ok {
        oper_oid = find_oper_cache_entry(&key)?;
        if OidIsValid(oper_oid) {
            tup = search_oper_row(oper_oid)?;
            if tup.is_some() {
                return Ok(tup);
            }
        }
    }

    /*
     * First try for an "exact" match.
     */
    oper_oid = binary_oper_exact(opname, ltype_id, rtype_id)?;
    if !OidIsValid(oper_oid) {
        /*
         * Otherwise, search for the most suitable candidate.
         */
        /* Get binary operators of given name */
        let cx = MemoryContext::new("oper candidates");
        let names = name_refs(opname);
        let clist = opername_get_candidates::call(cx.mcx(), &names, b'b', false)?;

        /* No operators found? Then fail... */
        if !clist.is_empty() {
            /*
             * Unspecified type for one of the arguments? then use the other
             * (XXX this is probably dead code?)
             */
            if rtype_id == InvalidOid {
                rtype_id = ltype_id;
            } else if ltype_id == InvalidOid {
                ltype_id = rtype_id;
            }
            let input_oids = [ltype_id, rtype_id];
            let (fd, oid) = oper_select_candidate(cx.mcx(), 2, &input_oids, &clist)?;
            fdresult = fd;
            oper_oid = oid;
        }
    }

    if OidIsValid(oper_oid) {
        tup = search_oper_row(oper_oid)?;
    }

    if tup.is_some() {
        if key_ok {
            make_oper_cache_entry(&key, oper_oid);
        }
    } else if !no_error {
        op_error(pstate, opname, ltype_id, rtype_id, fdresult, location)?;
    }

    Ok(tup)
}

/// `compatible_oper()` (parse_oper.c:450)
pub fn compatible_oper(
    pstate: Option<&ParseState<'_>>,
    op: &[String],
    arg1: Oid,
    arg2: Oid,
    no_error: bool,
    location: i32,
) -> PgResult<Option<ResolvedOper>> {
    /* oper() will find the best available match */
    let optup = oper(pstate, op, arg1, arg2, no_error, location)?;
    let opform = match optup {
        None => return Ok(None), /* must be noError case */
        Some(opform) => opform,
    };

    /* but is it good enough? */
    if is_binary_coercible::call(arg1, opform.oprleft)?
        && is_binary_coercible::call(arg2, opform.oprright)?
    {
        return Ok(Some(opform));
    }

    /* nope... (C ReleaseSysCache(optup) here; owned value, nothing to release) */

    if !no_error {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "operator requires run-time type coercion: {}",
                op_signature_string(op, arg1, arg2)?
            ))
            .errposition(errpos(pstate, location))
            .into_error());
    }

    Ok(None)
}

/// `compatible_oper_opid()` (parse_oper.c:487)
pub fn compatible_oper_opid(op: &[String], arg1: Oid, arg2: Oid, no_error: bool) -> PgResult<Oid> {
    let optup = compatible_oper(None, op, arg1, arg2, no_error, -1)?;
    if let Some(optup) = optup {
        return Ok(oprid(&optup));
    }
    Ok(InvalidOid)
}

/// `left_oper()` (parse_oper.c:518): search for a unary left (prefix) operator.
pub fn left_oper(
    pstate: Option<&ParseState<'_>>,
    op: &[String],
    arg: Oid,
    no_error: bool,
    location: i32,
) -> PgResult<Option<ResolvedOper>> {
    let mut oper_oid;
    let mut fdresult = FuncDetailCode::NotFound;
    let mut tup: Option<ResolvedOper> = None;

    /*
     * Try to find the mapping in the lookaside cache.
     */
    let mut key = OprCacheKey::default();
    let key_ok = make_oper_cache_key(pstate, &mut key, op, InvalidOid, arg, location)?;

    if key_ok {
        oper_oid = find_oper_cache_entry(&key)?;
        if OidIsValid(oper_oid) {
            tup = search_oper_row(oper_oid)?;
            if tup.is_some() {
                return Ok(tup);
            }
        }
    }

    /*
     * First try for an "exact" match.
     */
    let cx = MemoryContext::new("left_oper");
    let names = name_refs(op);
    oper_oid = opername_get_oprid::call(cx.mcx(), &names, InvalidOid, arg)?;
    if !OidIsValid(oper_oid) {
        /*
         * Otherwise, search for the most suitable candidate.
         */
        /* Get prefix operators of given name */
        let mut clist = opername_get_candidates::call(cx.mcx(), &names, b'l', false)?;

        /* No operators found? Then fail... */
        if !clist.is_empty() {
            /*
             * The returned list has args in the form (0, oprright). Move the
             * useful data into args[0] to keep oper_select_candidate simple.
             * XXX we are assuming here that we may scribble on the list!
             */
            for clisti in clist.iter_mut() {
                clisti.args[0] = clisti.args[1];
            }

            /*
             * We must run oper_select_candidate even if only one candidate,
             * otherwise we may falsely return a non-type-compatible operator.
             */
            let (fd, oid) = oper_select_candidate(cx.mcx(), 1, &[arg], &clist)?;
            fdresult = fd;
            oper_oid = oid;
        }
    }

    if OidIsValid(oper_oid) {
        tup = search_oper_row(oper_oid)?;
    }

    if tup.is_some() {
        if key_ok {
            make_oper_cache_entry(&key, oper_oid);
        }
    } else if !no_error {
        op_error(pstate, op, InvalidOid, arg, fdresult, location)?;
    }

    Ok(tup)
}

// ===========================================================================
// op_signature_string / op_error.
// ===========================================================================

/// `format_type_be(type_oid)` — the displayable type name, owned (the C palloc's
/// the string in the current context; we render an owned `String` for the
/// error message).
fn format_type_be(type_oid: Oid) -> PgResult<String> {
    backend_utils_adt_format_type::format_type_be_owned(type_oid)
}

/// `NameListToString(op)` — render a possibly-qualified operator name into a
/// dotted string. (The C builds a `StringInfo`; here an owned `String`.)
fn name_list_to_string(op: &[String]) -> String {
    op.join(".")
}

/// `op_signature_string()` (parse_oper.c:602)
pub fn op_signature_string(op: &[String], arg1: Oid, arg2: Oid) -> PgResult<String> {
    let mut argbuf = String::new();

    if OidIsValid(arg1) {
        argbuf.push_str(&format_type_be(arg1)?);
        argbuf.push(' ');
    }

    argbuf.push_str(&name_list_to_string(op));

    argbuf.push(' ');
    argbuf.push_str(&format_type_be(arg2)?);

    Ok(argbuf) /* return palloc'd string buffer */
}

/// `op_error()` (parse_oper.c:622): complain about an unresolvable operator.
fn op_error(
    pstate: Option<&ParseState<'_>>,
    op: &[String],
    arg1: Oid,
    arg2: Oid,
    fdresult: FuncDetailCode,
    location: i32,
) -> PgResult<()> {
    if fdresult == FuncDetailCode::Multiple {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_AMBIGUOUS_FUNCTION)
            .errmsg(format!(
                "operator is not unique: {}",
                op_signature_string(op, arg1, arg2)?
            ))
            .errhint(
                "Could not choose a best candidate operator. \
                 You might need to add explicit type casts.",
            )
            .errposition(errpos(pstate, location))
            .into_error());
    }

    let hint = if !OidIsValid(arg1) || !OidIsValid(arg2) {
        "No operator matches the given name and argument type. \
         You might need to add an explicit type cast."
    } else {
        "No operator matches the given name and argument types. \
         You might need to add explicit type casts."
    };

    Err(ereport(ERROR)
        .errcode(ERRCODE_UNDEFINED_FUNCTION)
        .errmsg(format!(
            "operator does not exist: {}",
            op_signature_string(op, arg1, arg2)?
        ))
        .errhint(hint)
        .errposition(errpos(pstate, location))
        .into_error())
}

// ===========================================================================
// make_op / make_scalar_array_op.
// ===========================================================================

/// `make_op()` (parse_oper.c:660): operator expression construction.
///
/// Transform an operator expression ensuring type compatibility. `last_srf`
/// should be a copy of `pstate->p_last_srf` from just before transforming the
/// operator's arguments. `ltree == None` denotes a prefix operator;
/// `rtree == None` is a (rejected) postfix operator. Returns the built `OpExpr`.
pub fn make_op<'mcx>(
    mut pstate: Option<&mut ParseState<'mcx>>,
    opname: &[String],
    ltree: Option<Expr<'static>>,
    rtree: Option<Expr<'static>>,
    last_srf: Option<&Expr>,
    location: i32,
) -> PgResult<Expr<'static>> {
    let ltype_id;
    let rtype_id;
    let opform;

    /* Check it's not a postfix operator */
    let rtree = match rtree {
        None => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("postfix operators are not supported")
                .into_error());
        }
        Some(rtree) => rtree,
    };

    /* Select the operator */
    match &ltree {
        None => {
            /* prefix operator */
            rtype_id = exprType(Some(&rtree))?;
            ltype_id = InvalidOid;
            opform = left_oper(pstate.as_deref(), opname, rtype_id, false, location)?
                .ok_or_else(|| internal_error("left_oper(noError=false)"))?;
        }
        Some(ltree) => {
            /* otherwise, binary operator */
            ltype_id = exprType(Some(ltree))?;
            rtype_id = exprType(Some(&rtree))?;
            opform = oper(pstate.as_deref(), opname, ltype_id, rtype_id, false, location)?
                .ok_or_else(|| internal_error("oper(noError=false)"))?;
        }
    }

    /* Check it's not a shell */
    if !reg_procedure_is_valid(opform.oprcode) {
        /* C does not ReleaseSysCache here; the error unwinds the context. */
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "operator is only a shell: {}",
                op_signature_string(opname, opform.oprleft, opform.oprright)?
            ))
            .errposition(errpos(pstate.as_deref(), location))
            .into_error());
    }

    /* Do typecasting and build the expression tree */
    let mut args: Vec<Expr>;
    let mut actual_arg_types = [InvalidOid; 2];
    let mut declared_arg_types = [InvalidOid; 2];
    let nargs;

    match ltree {
        None => {
            /* prefix operator */
            args = vec![rtree];
            actual_arg_types[0] = rtype_id;
            declared_arg_types[0] = opform.oprright;
            nargs = 1;
        }
        Some(ltree) => {
            /* otherwise, binary operator */
            args = vec![ltree, rtree];
            actual_arg_types[0] = ltype_id;
            actual_arg_types[1] = rtype_id;
            declared_arg_types[0] = opform.oprleft;
            declared_arg_types[1] = opform.oprright;
            nargs = 2;
        }
    }

    /*
     * enforce consistency with polymorphic argument and return types, possibly
     * adjusting return type or declared_arg_types (which will be used as the
     * cast destination by make_fn_arguments)
     */
    let rettype = enforce_generic_type_consistency::call(
        &actual_arg_types[..nargs],
        &mut declared_arg_types[..nargs],
        nargs as i32,
        opform.oprresult,
        false,
    )?;

    /* perform the necessary typecasting of arguments */
    make_fn_arguments::call(
        pstate.as_deref_mut(),
        &mut args,
        &actual_arg_types[..nargs],
        &declared_arg_types[..nargs],
    )?;

    /* and build the expression node */
    let opretset = get_func_retset::call(opform.oprcode)?;
    let result = OpExpr {
        opno: oprid(&opform),
        opfuncid: opform.oprcode,
        opresulttype: rettype,
        opretset,
        /* opcollid and inputcollid will be set by parse_collate.c */
        opcollid: InvalidOid,
        inputcollid: InvalidOid,
        args,
        location,
    };
    let result = Expr::OpExpr(result);

    /* if it returns a set, check that's OK */
    if opretset {
        /*
         * C's make_op dereferences pstate in this SRF path
         * (check_srf_call_placement reads pstate->p_expr_kind and writes
         * pstate->p_hasTargetSRFs), so a set-returning operator necessarily has
         * a non-NULL pstate.
         */
        let pstate = pstate
            .as_deref_mut()
            .ok_or_else(|| internal_error("make_op: set-returning operator with NULL pstate"))?;
        check_srf_call_placement::call(pstate, last_srf, location)?;
        /* ... and remember it for error checks at higher levels */
        set_last_srf::call(pstate, &result)?;
    }

    /* C ReleaseSysCache(tup) here; owned value, nothing to release. */

    Ok(result)
}

/// `make_scalar_array_op()` (parse_oper.c:770): build the expression tree for a
/// `scalar op ANY/ALL (array)` construct.
pub fn make_scalar_array_op<'mcx>(
    mut pstate: Option<&mut ParseState<'mcx>>,
    opname: &[String],
    use_or: bool,
    ltree: Expr<'static>,
    rtree: Expr<'static>,
    location: i32,
) -> PgResult<Expr<'static>> {
    let rtype_id;
    let res_atype_id;

    let ltype_id = exprType(Some(&ltree))?;
    let atype_id = exprType(Some(&rtree))?;

    /*
     * The right-hand input of the operator will be the element type of the
     * array. However, if we currently have just an untyped literal on the right,
     * stay with that and hope we can resolve the operator.
     */
    if atype_id == UNKNOWNOID {
        rtype_id = UNKNOWNOID;
    } else {
        rtype_id = get_base_element_type::call(atype_id)?;
        if !OidIsValid(rtype_id) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg("op ANY/ALL (array) requires array on right side")
                .errposition(errpos(pstate.as_deref(), location))
                .into_error());
        }
    }

    /* Now resolve the operator */
    let opform = oper(pstate.as_deref(), opname, ltype_id, rtype_id, false, location)?
        .ok_or_else(|| internal_error("oper(noError=false)"))?;

    /* Check it's not a shell */
    if !reg_procedure_is_valid(opform.oprcode) {
        /* C does not ReleaseSysCache here; the error unwinds the context. */
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "operator is only a shell: {}",
                op_signature_string(opname, opform.oprleft, opform.oprright)?
            ))
            .errposition(errpos(pstate.as_deref(), location))
            .into_error());
    }

    let mut args = vec![ltree, rtree];
    let mut actual_arg_types = [ltype_id, rtype_id];
    let mut declared_arg_types = [opform.oprleft, opform.oprright];

    /*
     * enforce consistency with polymorphic argument and return types, possibly
     * adjusting return type or declared_arg_types (which will be used as the
     * cast destination by make_fn_arguments)
     */
    let rettype = enforce_generic_type_consistency::call(
        &actual_arg_types,
        &mut declared_arg_types,
        2,
        opform.oprresult,
        false,
    )?;

    /*
     * Check that operator result is boolean
     */
    if rettype != BOOLOID {
        /* C does not ReleaseSysCache here; the error unwinds the context. */
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("op ANY/ALL (array) requires operator to yield boolean")
            .errposition(errpos(pstate.as_deref(), location))
            .into_error());
    }
    if get_func_retset::call(opform.oprcode)? {
        /* C does not ReleaseSysCache here; the error unwinds the context. */
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("op ANY/ALL (array) requires operator not to return a set")
            .errposition(errpos(pstate.as_deref(), location))
            .into_error());
    }

    /*
     * Now switch back to the array type on the right, arranging for any needed
     * cast to be applied. Beware of polymorphic operators here;
     * enforce_generic_type_consistency may or may not have replaced a
     * polymorphic type with a real one.
     */
    if is_polymorphic_type(declared_arg_types[1]) {
        /* assume the actual array type is OK */
        res_atype_id = atype_id;
    } else {
        res_atype_id = match get_array_type::call(declared_arg_types[1])? {
            Some(t) => t,
            None => InvalidOid,
        };
        if !OidIsValid(res_atype_id) {
            /* C does not ReleaseSysCache here; the error unwinds the context. */
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "could not find array type for data type {}",
                    format_type_be(declared_arg_types[1])?
                ))
                .errposition(errpos(pstate.as_deref(), location))
                .into_error());
        }
    }
    actual_arg_types[1] = atype_id;
    declared_arg_types[1] = res_atype_id;

    /* perform the necessary typecasting of arguments */
    make_fn_arguments::call(
        pstate.as_deref_mut(),
        &mut args,
        &actual_arg_types,
        &declared_arg_types,
    )?;

    /* and build the expression node */
    let result = ScalarArrayOpExpr {
        opno: oprid(&opform),
        opfuncid: opform.oprcode,
        hashfuncid: InvalidOid,
        negfuncid: InvalidOid,
        useOr: use_or,
        /* inputcollid will be set by parse_collate.c */
        inputcollid: InvalidOid,
        args,
        location,
    };

    /* C ReleaseSysCache(tup) here; owned value, nothing to release. */

    Ok(Expr::ScalarArrayOpExpr(result))
}

// ===========================================================================
// Lookaside cache to speed operator lookup (parse_oper.c:896..1053).
// ===========================================================================

/// `MAX_CACHED_PATH_LEN` (parse_oper.c:49): if your search_path is longer than
/// this, sucks to be you ... we just punt and don't cache anything.
const MAX_CACHED_PATH_LEN: usize = 16;

const NAMEDATALEN_USZ: usize = NAMEDATALEN as usize;

/// `OprCacheKey` (parse_oper.c:51): the lookup key for the operator lookaside
/// table. `oprname` is zero-padded to `NAMEDATALEN`; unused `search_path` entries
/// are zero, so equal keys compare identically.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct OprCacheKey {
    /// `char oprname[NAMEDATALEN]`, zero-padded.
    oprname: [u8; NAMEDATALEN_USZ],
    /// Left input OID, or 0 if prefix op.
    left_arg: Oid,
    /// Right input OID.
    right_arg: Oid,
    /// The active search path (minus temp namespace), or the single explicit
    /// schema OID; unused trailing entries are zero.
    search_path: [Oid; MAX_CACHED_PATH_LEN],
}

impl Default for OprCacheKey {
    fn default() -> Self {
        /* ensure zero-fill for stable hashing (C's MemSet(key, 0, ...)) */
        OprCacheKey {
            oprname: [0; NAMEDATALEN_USZ],
            left_arg: InvalidOid,
            right_arg: InvalidOid,
            search_path: [InvalidOid; MAX_CACHED_PATH_LEN],
        }
    }
}

/// `OprCacheHash` (parse_oper.c:923): the per-backend operator lookaside table.
/// `None` until first use, matching C's `static HTAB *OprCacheHash = NULL`.
static OPR_CACHE_HASH: Mutex<Option<BTreeMap<OprCacheKey, Oid>>> = Mutex::new(None);

/// `strlcpy(dst, src, NAMEDATALEN)` into a zero-filled `[u8; NAMEDATALEN]`.
fn strlcpy_namedata(opername: &str) -> [u8; NAMEDATALEN_USZ] {
    let mut buf = [0u8; NAMEDATALEN_USZ];
    let src = opername.as_bytes();
    let n = core::cmp::min(src.len(), NAMEDATALEN_USZ - 1);
    buf[..n].copy_from_slice(&src[..n]);
    buf
}

/// `make_oper_cache_key()` (parse_oper.c:937)
///
/// Fill the lookup key struct given operator name and arg types. Returns true if
/// successful, false if the search_path overflowed (hence no caching is possible).
fn make_oper_cache_key(
    pstate: Option<&ParseState<'_>>,
    key: &mut OprCacheKey,
    opname: &[String],
    ltype_id: Oid,
    rtype_id: Oid,
    location: i32,
) -> PgResult<bool> {
    /* deconstruct the name list */
    let cx = MemoryContext::new("make_oper_cache_key");
    let name_list: Vec<Option<String>> = opname.iter().map(|s| Some(s.clone())).collect();
    let (schemaname, opername) =
        backend_catalog_namespace::DeconstructQualifiedName(cx.mcx(), &name_list)?;

    /* ensure zero-fill for stable hashing */
    *key = OprCacheKey::default();

    /* save operator name and input types into key */
    key.oprname = strlcpy_namedata(opername);
    key.left_arg = ltype_id;
    key.right_arg = rtype_id;

    if let Some(schemaname) = schemaname {
        /*
         * search only in exact schema given
         *
         *   setup_parser_errposition_callback(&pcbstate, pstate, location);
         *   key->search_path[0] = LookupExplicitNamespace(schemaname, false);
         *   cancel_parser_errposition_callback(&pcbstate);
         *
         * LookupExplicitNamespace raises "schema does not exist" with no parse
         * position of its own; the C callback supplies `location`. The ambient
         * error_context_stack is retired (docs/query-lifecycle-raii.md), so we
         * attach the cursor position where the fallible lookup returns Err, only
         * when the error has none of its own.
         */
        key.search_path[0] = lookup_explicit_namespace::call(schemaname, false)
            .map_err(|e| {
                if e.cursor_position().is_some() {
                    return e;
                }
                let pos = errpos(pstate, location);
                if pos > 0 {
                    e.with_cursor_position(pos)
                } else {
                    e
                }
            })?;
    } else {
        /* get the active search path */
        let mut path = [InvalidOid; MAX_CACHED_PATH_LEN + 1];
        let count = backend_catalog_namespace::fetch_search_path_array(cx.mcx(), &mut path)?;
        if count as usize > MAX_CACHED_PATH_LEN {
            return Ok(false); /* oops, didn't fit */
        }
        for (slot, ns) in key
            .search_path
            .iter_mut()
            .zip(path.iter().take(count as usize))
        {
            *slot = *ns;
        }
    }

    Ok(true)
}

/// `find_oper_cache_entry()` (parse_oper.c:981)
///
/// Look for a cache entry matching the given key. If found, return the contained
/// operator OID, else return `InvalidOid`. On first use this initializes the
/// table and registers the pg_operator/pg_cast flush callbacks.
fn find_oper_cache_entry(key: &OprCacheKey) -> PgResult<Oid> {
    let mut cache = OPR_CACHE_HASH.lock().expect("oper cache");
    let need_init = cache.is_none();
    if need_init {
        /* First time through: initialize the hash table */
        *cache = Some(BTreeMap::new());

        /*
         * Arrange to flush cache on pg_operator and pg_cast changes.
         * (CacheRegisterSyscacheCallback(OPERNAMENSP, ...) and
         * CacheRegisterSyscacheCallback(CASTSOURCETARGET, ...).)
         *
         * The repo's inval-callback registry is not yet modeled for this
         * per-backend cache; in the single-backend port the cache is flushed
         * explicitly via [`invalidate_oper_cache`] (the InvalidateOprCacheCallBack
         * body), which the host invokes on the relevant inval events.
         */
    }

    /* Look for an existing entry */
    Ok(cache
        .as_ref()
        .and_then(|m| m.get(key).copied())
        .unwrap_or(InvalidOid))
}

/// `make_oper_cache_entry()` (parse_oper.c:1020): insert a cache entry.
fn make_oper_cache_entry(key: &OprCacheKey, opr_oid: Oid) {
    let mut cache = OPR_CACHE_HASH.lock().expect("oper cache");
    /* Assert(OprCacheHash != NULL) */
    debug_assert!(cache.is_some());
    if let Some(m) = cache.as_mut() {
        m.insert(*key, opr_oid);
    }
}

/// `InvalidateOprCacheCallBack()` (parse_oper.c:1036): callback for pg_operator
/// and pg_cast inval events. Currently we just flush all entries.
pub fn invalidate_oper_cache() {
    let mut cache = OPR_CACHE_HASH.lock().expect("oper cache");
    /* Assert(OprCacheHash != NULL) */
    debug_assert!(cache.is_some());
    if let Some(m) = cache.as_mut() {
        m.clear();
    }
}

// ===========================================================================
// Seam installation: the inward seams this unit owns.
// ===========================================================================

/// `LookupOperWithArgs(oper, noError)` over the opclass `ObjectWithArgs`.
fn seam_lookup_oper_with_args(
    oper: &types_opclass::ObjectWithArgs,
    no_error: bool,
) -> PgResult<Oid> {
    LookupOperWithArgs(oper, no_error)
}

/// `LookupOperWithArgs(oper, missing_ok)` over the raw-parser `ObjectWithArgs`.
fn seam_lookup_oper_with_args_node(
    oper: &types_parsenodes::ObjectWithArgs,
    missing_ok: bool,
) -> PgResult<Oid> {
    LookupOperWithArgs_node(oper, missing_ok)
}

/// `LookupOperName(NULL, opername, oprleft, oprright, false, -1)`.
fn seam_lookup_oper_name(opername: &[String], oprleft: Oid, oprright: Oid) -> PgResult<Oid> {
    LookupOperName(None, opername, oprleft, oprright, false, -1)
}

/// Install the seams this crate owns. Wired into `seams_init::init_all()`.
/// `generate_operator_name(operid, arg1, arg2)` (ruleutils.c, static) — the
/// possibly-schema-qualified operator name to use in deparsed output. Installs
/// ruleutils' `generate_operator_name` seam from this crate (which owns the
/// `oper`/`left_oper` candidate-resolution the body needs).
///
/// Schema-qualifies only if the parser would *fail* to resolve the same operator
/// from the unqualified name with the given argtypes.
fn generate_operator_name<'mcx>(
    mcx: Mcx<'mcx>,
    operid: Oid,
    arg1: Oid,
    arg2: Oid,
) -> PgResult<mcx::PgString<'mcx>> {
    // opertup = SearchSysCache1(OPEROID, operid); elog(ERROR) on miss.
    let optup = backend_utils_cache_syscache_seams::oper_row_by_oid::call(mcx, operid)?
        .ok_or_else(|| {
            ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg_internal(format!("cache lookup failed for operator {operid}"))
                .into_error()
        })?;
    let oprname: String = optup.oprname.as_str().into();

    // Resolve the unqualified name with the argtypes; qualify iff it does not
    // re-resolve to the same operator.
    let opname_list = vec![oprname.clone()];
    let p_result: Option<ResolvedOper> = match optup.oprkind {
        b'b' => oper(None, &opname_list, arg1, arg2, true, -1)?,
        b'l' => left_oper(None, &opname_list, arg2, true, -1)?,
        other => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg_internal(format!("unrecognized oprkind: {other}"))
                .into_error());
        }
    };

    let need_qual = match &p_result {
        Some(p) => oprid(p) != operid,
        None => true,
    };

    let mut buf = String::new();
    if need_qual {
        // nspname = get_namespace_name_or_temp(operform->oprnamespace);
        let nspname = backend_utils_cache_lsyscache_seams::get_namespace_name_or_temp::call(
            mcx,
            optup.oprnamespace,
        )?
        .ok_or_else(|| {
            ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg_internal(format!(
                    "cache lookup failed for namespace {}",
                    optup.oprnamespace
                ))
                .into_error()
        })?;
        let qnsp =
            backend_utils_adt_ruleutils_seams::quote_identifier::call(mcx, nspname.as_str())?;
        buf.push_str("OPERATOR(");
        buf.push_str(qnsp.as_str());
        buf.push('.');
    }
    buf.push_str(&oprname);
    if need_qual {
        buf.push(')');
    }
    mcx::PgString::from_str_in(&buf, mcx)
}

pub fn init_seams() {
    backend_parser_parse_oper_seams::lookup_oper_with_args::set(seam_lookup_oper_with_args);
    backend_parser_parse_oper_seams::lookup_oper_with_args_node::set(
        seam_lookup_oper_with_args_node,
    );
    backend_parser_parse_oper_seams::lookup_oper_name::set(seam_lookup_oper_name);

    // ruleutils' `generate_operator_name` (the deparser's operator-name
    // generator) — owned here because its body needs `oper`/`left_oper`.
    backend_utils_adt_ruleutils_seams::generate_operator_name::set(generate_operator_name);
}
