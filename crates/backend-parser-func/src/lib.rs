//! Owned-tree port of `src/backend/parser/parse_func.c` (PostgreSQL 18.3) —
//! handle function calls in the parser.
//!
//! Every public function and static helper of `parse_func.c` is ported 1:1
//! against the C source (branch order, error text, SQLSTATE, return values),
//! keyed on the repo's owned [`types_nodes::primnodes::Expr`] expression tree
//! and the trimmed [`types_nodes::parsestmt::ParseState`].
//!
//! The function name (`List *funcname` of `String` nodes in C) crosses as the
//! `String`-component slice [`&[PgString]`], matching the inward-seam contract
//! (`lookup_func_name`) and the `NameListToString`/`name_list_to_string`
//! convention used across the ported parser.
//!
//! No raw pointers, no `extern "C"`. Catalog/namespace, syscache (the pg_proc /
//! pg_aggregate rows), lsyscache, the type-coercion engine (`parse_coerce.c`),
//! the namespace candidate search, the aggregate/window transforms
//! (`parse_agg.c`), `parse_relation.c`, `funcapi.c`, `parse_type.c`,
//! `format_type.c`, and `parse_expr.c`'s `ParseExprKindName` are reached through
//! their per-owner seam crates (loud-panic until the owner installs them).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_range_loop)]

use backend_utils_error::{ereport, PgError};
use mcx::{Mcx, MemoryContext, PgString, PgVec};

use types_core::primitive::{
    AttrNumber, Oid, OidIsValid, FUNC_MAX_ARGS, InvalidAttrNumber, INVALID_OID as InvalidOid,
};
use types_error::{
    PgResult, ERRCODE_AMBIGUOUS_FUNCTION, ERRCODE_DATATYPE_MISMATCH, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_FUNCTION_DEFINITION, ERRCODE_SYNTAX_ERROR, ERRCODE_TOO_MANY_ARGUMENTS,
    ERRCODE_UNDEFINED_FUNCTION, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_nodes::nodes::Node;
use types_nodes::parsestmt::{ParseExprKind, ParseState};
use types_nodes::primnodes::{
    Aggref, ArrayExpr, CoercionForm, Expr, FieldSelect, FuncExpr, WindowFunc,
};
use types_nodes::rawnodes::FuncCall;
use types_opclass::ObjectWithArgs;
use types_nodes::parsenodes::{
    ObjectType, OBJECT_AGGREGATE, OBJECT_FUNCTION, OBJECT_PROCEDURE, OBJECT_ROUTINE,
};
use types_parsenodes::CoercionContext;
use types_parsenodes::ObjectWithArgs as ParseObjectWithArgs;

use types_tuple::heaptuple::{RECORDOID, UNKNOWNOID, VOIDOID};

// Outward seam aliases.
use backend_catalog_namespace_seams::funcname_get_candidates;
use backend_nodes_core::nodefuncs::{expr_location as exprLocation, expr_type as exprType};
use backend_parser_coerce_seams::{
    can_coerce_type, coerce_type, enforce_generic_type_consistency, find_coercion_pathway_explicit,
    select_common_type, CoercionPathType,
};
use backend_parser_parse_agg_seams::{transform_aggregate_call, transform_window_func_call};
use backend_parser_parse_expr_seams::parse_expr_kind_name;
use backend_parser_parse_func_seams as me;
use backend_parser_parse_type_seams::{
    func_name_as_type, lookup_type_name_oid, lookup_type_name_oid_owa,
};
use backend_parser_relation_seams::{expand_record_variable, scan_ns_item_for_column_by_posn};
use backend_utils_adt_format_type_seams::format_type_be_owned;
use backend_utils_cache_lsyscache_seams::{
    get_array_type, get_base_element_type, get_base_type, get_func_prokind,
    get_type_category_preferred,
};
use backend_utils_cache_syscache_seams::{agg_row_by_oid, proc_argdefaults, proc_row_by_oid};
use backend_utils_fmgr_funcapi_seams::get_expr_result_tupdesc;

use types_namespace::{FuncCandidate, FuncCandidateList};

#[cfg(test)]
mod tests;

// ===========================================================================
// Catalog / type-system constants used directly by parse_func.c.
// ===========================================================================

/// `ANYOID` — the polymorphic "any" pseudo-type (`catalog/pg_type.h`).
const ANYOID: Oid = 2276;

// `prokind` codes (`catalog/pg_proc.h`). `get_func_prokind` returns a `u8`.
const PROKIND_FUNCTION: u8 = b'f';
const PROKIND_AGGREGATE: u8 = b'a';
const PROKIND_WINDOW: u8 = b'w';
const PROKIND_PROCEDURE: u8 = b'p';

// `aggkind` codes (`catalog/pg_aggregate.h`).
const AGGKIND_NORMAL: i8 = b'n' as i8;
const AGGKIND_HYPOTHETICAL: i8 = b'h' as i8;

/// `AGGKIND_IS_ORDERED_SET(kind)` (`catalog/pg_aggregate.h`).
#[inline]
fn AGGKIND_IS_ORDERED_SET(kind: i8) -> bool {
    kind != AGGKIND_NORMAL
}

/// `TYPCATEGORY_STRING` — the string category (`'S'`) (`catalog/pg_type.h`).
const TYPCATEGORY_STRING: u8 = b'S';
/// `TYPCATEGORY_INVALID` — sentinel meaning "no category yet".
const TYPCATEGORY_INVALID: u8 = 0;

// `CoercionForm` codes (`nodes/primnodes.h`).
const COERCE_EXPLICIT_CALL: CoercionForm = CoercionForm::COERCE_EXPLICIT_CALL;
const COERCE_IMPLICIT_CAST: CoercionForm = CoercionForm::COERCE_IMPLICIT_CAST;

/// `FuncDetailCode` (`parser/parse_func.h`). Mirrors the C enum order/values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FuncDetailCode {
    NotFound,
    Multiple,
    Normal,
    Procedure,
    Aggregate,
    WindowFunc,
    Coercion,
}

/// Possible error codes from `LookupFuncNameInternal` (`parse_func.c:39`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FuncLookupError {
    NoSuchFunc,
    Ambiguous,
}

// ===========================================================================
// Small node helpers (faithful to the C macros used below).
// ===========================================================================

/// `ISCOMPLEX(typeid)` == `(typeOrDomainTypeRelid(typeid) != InvalidOid)`
/// (`parser/parse_type.h`): the type (resolving a domain to its base) has a
/// backing pg_class relid, i.e. it is a composite/rowtype. We model this via
/// `get_typ_typrelid(getBaseType(typeid))` — the same lookup `typeOrDomain...`
/// performs (lsyscache).
#[inline]
fn ISCOMPLEX(typeid: Oid) -> PgResult<bool> {
    let base = get_base_type::call(typeid)?;
    Ok(OidIsValid(get_typ_typrelid(base)?))
}

/// `get_typ_typrelid(typid)` (lsyscache) — `typrelid`, or `InvalidOid`.
#[inline]
fn get_typ_typrelid(typid: Oid) -> PgResult<Oid> {
    backend_utils_cache_lsyscache_seams::get_typ_typrelid::call(typid)
}

/// `parser_errposition(pstate, location)` — the cursor position to attach to an
/// ereport (the C macro returns a `errposition()`-ready offset). A no-op
/// returning 0 when `location < 0`; otherwise the byte offset's 1-based column.
/// Mirrors the trimmed-ParseState convention used across the ported parser.
#[inline]
fn parser_errposition(pstate: Option<&ParseState<'_>>, location: i32) -> i32 {
    let _ = pstate;
    if location < 0 {
        return 0;
    }
    location + 1
}

/// `exprLocation` over a `Node *` (here the `p_last_srf` carrier). The SRF node
/// is always a `Node::Expr`, so we read its inner `Expr`.
fn node_expr_location(node: Option<&Node<'_>>) -> PgResult<i32> {
    match node.and_then(|n| n.as_expr()) {
        Some(e) => exprLocation(Some(e)),
        None => Ok(-1),
    }
}

/// Whether `pstate.p_last_srf == last_srf` (pointer identity in C).
///
/// `last_srf` is the value `pstate->p_last_srf` held when the caller started
/// transforming this call's arguments; the test detects whether a *new* SRF was
/// recorded during that transformation. The owned tree holds nodes by value and
/// `Expr` is not `PartialEq`, so identity is modeled as: both `None` ⇒ same;
/// both `Some` ⇒ same node tag *and* same parse location (`exprLocation`). An
/// SRF call node's `(tag, location)` uniquely identifies it within a single
/// query parse (locations are distinct byte offsets), so this is a faithful
/// stand-in for the pointer equality the C performs.
fn p_last_srf_eq(pstate: &ParseState<'_>, last_srf: Option<&Expr>) -> PgResult<bool> {
    let cur = pstate.p_last_srf.as_deref().and_then(|n| n.as_expr());
    Ok(match (cur, last_srf) {
        (None, None) => true,
        (Some(a), Some(b)) => {
            core::mem::discriminant(a) == core::mem::discriminant(b)
                && exprLocation(Some(a))? == exprLocation(Some(b))?
        }
        _ => false,
    })
}

/// Set `pstate->p_last_srf = (Node *) result` (boxed `Node::Expr` in `mcx`).
fn set_p_last_srf<'mcx>(pstate: &mut ParseState<'mcx>, result: &Expr) -> PgResult<()> {
    let mcx = pstate_mcx(pstate);
    pstate.p_last_srf = Some(mcx::alloc_in(mcx, Node::Expr(result.clone()))?);
    Ok(())
}

/// The allocation context to use for nodes we attach to `pstate`. The trimmed
/// `ParseState` does not carry an explicit arena; new nodes share the lifetime
/// of the values already in `pstate`, so we use the ambient query Mcx the
/// existing `p_last_srf`/rtable allocations live in.
#[inline]
fn pstate_mcx<'mcx>(pstate: &ParseState<'mcx>) -> Mcx<'mcx> {
    // Recover the Mcx from an existing mcx-allocated field of pstate. p_rtable
    // is a PgVec whose allocator is the query context.
    *pstate.p_rtable.allocator()
}

// ===========================================================================
// func_match_argtypes (parse_func.c:923)
// ===========================================================================

/// Port target: `func_match_argtypes` (parse_func.c:923).
///
/// Given the `raw_candidates` and the `input_typeids`, produce the shortlist of
/// candidates whose declared arg types the inputs can be coerced to (directly or
/// via implicit cast). C returns the surviving count + the new list via the
/// out-param; the owned port returns the filtered list (whose `len()` is that
/// count), allocated in `mcx`.
pub fn func_match_argtypes<'mcx>(
    mcx: Mcx<'mcx>,
    nargs: i32,
    input_typeids: &[Oid],
    raw_candidates: &FuncCandidateList<'mcx>,
) -> PgResult<FuncCandidateList<'mcx>> {
    let mut candidates: FuncCandidateList<'mcx> = PgVec::new_in(mcx);

    // C prepends each match to the result (reversing order), then the consumer
    // reverses again on use; the matching *set* is what matters and the
    // single-match / func_select_candidate paths are order-insensitive for
    // correctness. Build it in original order.
    for current_candidate in raw_candidates.iter() {
        if can_coerce_type::call(
            nargs,
            input_typeids,
            &current_candidate.args,
            CoercionContext::COERCION_IMPLICIT,
        )? {
            candidates.push(clone_candidate(mcx, current_candidate)?);
        }
    }

    Ok(candidates)
}

// ===========================================================================
// func_select_candidate (parse_func.c:1008)
// ===========================================================================

/// Port target: `func_select_candidate` (parse_func.c:1008).
///
/// Given the input argtype array and more than one candidate, apply the
/// ambiguous-function resolution heuristics to pick a single best candidate.
/// Returns the selected candidate's OID (`Ok(Some(oid))`), or `Ok(None)` when
/// no unique best candidate can be chosen (the C NULL return).
pub fn func_select_candidate<'mcx>(
    mcx: Mcx<'mcx>,
    nargs: i32,
    input_typeids: &[Oid],
    candidates: &FuncCandidateList<'mcx>,
) -> PgResult<Option<Oid>> {
    // protect local fixed-size arrays.
    if nargs as usize > FUNC_MAX_ARGS {
        return Err(too_many_arguments_error(None, -1)?);
    }

    let na = nargs as usize;
    let mut input_base_typeids = vec![InvalidOid; na];
    let mut slot_category = vec![TYPCATEGORY_INVALID; na];
    let mut slot_has_preferred_type = vec![false; na];

    // Reduce domains to base types; count unknowns.
    let mut nunknowns = 0;
    for i in 0..na {
        let it = input_typeids[i];
        if it != UNKNOWNOID {
            input_base_typeids[i] = get_base_type::call(it)?;
        } else {
            input_base_typeids[i] = UNKNOWNOID;
            nunknowns += 1;
        }
    }

    // Keep candidates with the most exact-type matches.
    let mut candidates: Vec<&FuncCandidate<'mcx>> = candidates.iter().collect();
    {
        let mut nbest_match = 0;
        let mut kept: Vec<&FuncCandidate<'mcx>> = Vec::new();
        for cand in &candidates {
            let mut nmatch = 0;
            for i in 0..na {
                if input_base_typeids[i] != UNKNOWNOID
                    && cand.args[i] == input_base_typeids[i]
                {
                    nmatch += 1;
                }
            }
            if nmatch > nbest_match || kept.is_empty() {
                nbest_match = nmatch;
                kept.clear();
                kept.push(cand);
            } else if nmatch == nbest_match {
                kept.push(cand);
            }
        }
        candidates = kept;
    }
    if candidates.len() == 1 {
        return Ok(Some(candidates[0].oid));
    }

    // Look for candidates with exact matches or preferred types at coercion args.
    for i in 0..na {
        slot_category[i] = type_category(input_base_typeids[i])?;
    }
    {
        let mut nbest_match = 0;
        let mut kept: Vec<&FuncCandidate<'mcx>> = Vec::new();
        for cand in &candidates {
            let mut nmatch = 0;
            for i in 0..na {
                if input_base_typeids[i] != UNKNOWNOID
                    && (cand.args[i] == input_base_typeids[i]
                        || is_preferred_type(slot_category[i], cand.args[i])?)
                {
                    nmatch += 1;
                }
            }
            if nmatch > nbest_match || kept.is_empty() {
                nbest_match = nmatch;
                kept.clear();
                kept.push(cand);
            } else if nmatch == nbest_match {
                kept.push(cand);
            }
        }
        candidates = kept;
    }
    if candidates.len() == 1 {
        return Ok(Some(candidates[0].oid));
    }

    // Try assigning types for the unknown inputs.
    if nunknowns == 0 {
        return Ok(None); // failed to select a best candidate
    }

    let mut resolved_unknowns = false;
    for i in 0..na {
        if input_base_typeids[i] != UNKNOWNOID {
            continue;
        }
        resolved_unknowns = true; // assume we can do it
        slot_category[i] = TYPCATEGORY_INVALID;
        slot_has_preferred_type[i] = false;
        let mut have_conflict = false;
        for cand in &candidates {
            let current_type = cand.args[i];
            let (current_category, current_is_preferred) =
                get_type_category_preferred::call(current_type)?;
            if slot_category[i] == TYPCATEGORY_INVALID {
                // first candidate
                slot_category[i] = current_category;
                slot_has_preferred_type[i] = current_is_preferred;
            } else if current_category == slot_category[i] {
                // more candidates in same category
                slot_has_preferred_type[i] |= current_is_preferred;
            } else if current_category == TYPCATEGORY_STRING {
                // STRING always wins if available
                slot_category[i] = current_category;
                slot_has_preferred_type[i] = current_is_preferred;
            } else {
                // Remember conflict, but keep going (might find STRING).
                have_conflict = true;
            }
        }
        if have_conflict && slot_category[i] != TYPCATEGORY_STRING {
            // Failed to resolve category conflict at this position.
            resolved_unknowns = false;
            break;
        }
    }

    if resolved_unknowns {
        // Strip non-matching candidates.
        let mut kept: Vec<&FuncCandidate<'mcx>> = Vec::new();
        for cand in &candidates {
            let mut keepit = true;
            for i in 0..na {
                if input_base_typeids[i] != UNKNOWNOID {
                    continue;
                }
                let current_type = cand.args[i];
                let (current_category, current_is_preferred) =
                    get_type_category_preferred::call(current_type)?;
                if current_category != slot_category[i] {
                    keepit = false;
                    break;
                }
                if slot_has_preferred_type[i] && !current_is_preferred {
                    keepit = false;
                    break;
                }
            }
            if keepit {
                kept.push(cand);
            }
        }
        // if we found any matches, restrict our attention to those.
        if !kept.is_empty() {
            candidates = kept;
        }
        if candidates.len() == 1 {
            return Ok(Some(candidates[0].oid));
        }
    }

    // Last gasp: if there are both known- and unknown-type inputs and all the
    // known types are the same, assume the unknowns are also that type.
    if nunknowns < nargs {
        let mut known_type = UNKNOWNOID;
        for i in 0..na {
            if input_base_typeids[i] == UNKNOWNOID {
                continue;
            }
            if known_type == UNKNOWNOID {
                known_type = input_base_typeids[i];
            } else if known_type != input_base_typeids[i] {
                known_type = UNKNOWNOID;
                break;
            }
        }

        if known_type != UNKNOWNOID {
            for i in 0..na {
                input_base_typeids[i] = known_type;
            }
            let mut ncandidates = 0;
            let mut last_candidate: Option<&FuncCandidate<'mcx>> = None;
            for cand in &candidates {
                if can_coerce_type::call(
                    nargs,
                    &input_base_typeids,
                    &cand.args,
                    CoercionContext::COERCION_IMPLICIT,
                )? {
                    ncandidates += 1;
                    if ncandidates > 1 {
                        break; // not unique, give up
                    }
                    last_candidate = Some(cand);
                }
            }
            if ncandidates == 1 {
                return Ok(last_candidate.map(|c| c.oid));
            }
        }
    }

    let _ = mcx;
    Ok(None) // failed to select a best candidate
}

/// `TypeCategory(typid)` (lsyscache) — the `typcategory` only.
#[inline]
fn type_category(typid: Oid) -> PgResult<u8> {
    Ok(get_type_category_preferred::call(typid)?.0)
}

/// `IsPreferredType(category, type)` (lsyscache): is `type`'s category equal to
/// `category` and is it preferred within it?
#[inline]
fn is_preferred_type(category: u8, typid: Oid) -> PgResult<bool> {
    let (cat, pref) = get_type_category_preferred::call(typid)?;
    Ok((category == TYPCATEGORY_INVALID || category == cat) && pref)
}

/// Deep-copy a candidate into `mcx` (the C list is rebuilt in place; the owned
/// vector model copies the surviving entries).
fn clone_candidate<'mcx>(
    mcx: Mcx<'mcx>,
    c: &FuncCandidate<'mcx>,
) -> PgResult<FuncCandidate<'mcx>> {
    let mut argnumbers = PgVec::new_in(mcx);
    argnumbers.extend_from_slice(&c.argnumbers);
    let mut args = PgVec::new_in(mcx);
    args.extend_from_slice(&c.args);
    Ok(FuncCandidate {
        pathpos: c.pathpos,
        oid: c.oid,
        nominalnargs: c.nominalnargs,
        nargs: c.nargs,
        nvargs: c.nvargs,
        ndargs: c.ndargs,
        argnumbers,
        args,
    })
}

// ===========================================================================
// make_fn_arguments (parse_func.c:1824)
// ===========================================================================

/// Port target: `make_fn_arguments` (parse_func.c:1824).
///
/// Given the actual argument expressions and the desired input types, add any
/// necessary typecasting to the expression tree (modified in place). `pstate`
/// is `None` when the C `pstate == NULL`.
pub fn make_fn_arguments<'mcx>(
    mut pstate: Option<&mut ParseState<'mcx>>,
    fargs: &mut [Expr],
    actual_arg_types: &[Oid],
    declared_arg_types: &[Oid],
) -> PgResult<()> {
    for i in 0..fargs.len() {
        // types don't match? then force coercion using a function call...
        if actual_arg_types[i] != declared_arg_types[i] {
            // If arg is a NamedArgExpr, coerce its input expr instead --- we
            // want the NamedArgExpr to stay at the top level of the list.
            if let Some(na) = fargs[i].as_namedargexpr() {
                let inner = na
                    .arg
                    .as_deref()
                    .cloned()
                    .ok_or_else(|| internal_error("make_fn_arguments: NamedArgExpr.arg is NULL"))?;
                let coerced = coerce_type::call(
                    pstate.as_deref_mut(),
                    inner,
                    actual_arg_types[i],
                    declared_arg_types[i],
                    -1,
                    CoercionContext::COERCION_IMPLICIT,
                    COERCE_IMPLICIT_CAST,
                    -1,
                )?;
                if let Some(na) = fargs[i].as_namedargexpr_mut() {
                    na.arg = Some(Box::new(coerced));
                }
            } else {
                let node = core::mem::replace(&mut fargs[i], Expr::Const(dummy_const()));
                let coerced = coerce_type::call(
                    pstate.as_deref_mut(),
                    node,
                    actual_arg_types[i],
                    declared_arg_types[i],
                    -1,
                    CoercionContext::COERCION_IMPLICIT,
                    COERCE_IMPLICIT_CAST,
                    -1,
                )?;
                fargs[i] = coerced;
            }
        }
    }
    Ok(())
}

// ===========================================================================
// funcname_signature_string (parse_func.c:1992) / func_signature_string (2030)
// ===========================================================================

/// Port target: `funcname_signature_string` (parse_func.c:1992).
///
/// Build a string like `"foo(integer)"`. `argnames` (if any) is the list of
/// arg names for the trailing named-notation arguments.
pub fn funcname_signature_string(
    funcname: &str,
    nargs: i32,
    argnames: &[PgString<'_>],
    argtypes: &[Oid],
) -> PgResult<String> {
    let mut argbuf = String::new();

    argbuf.push_str(funcname);
    argbuf.push('(');

    let numposargs = nargs - argnames.len() as i32;
    let mut lc: usize = 0;

    for i in 0..nargs {
        if i != 0 {
            argbuf.push_str(", ");
        }
        if i >= numposargs {
            argbuf.push_str(argnames[lc].as_str());
            argbuf.push_str(" => ");
            lc += 1;
        }
        argbuf.push_str(&format_type_be_owned::call(argtypes[i as usize])?);
    }

    argbuf.push(')');
    Ok(argbuf)
}

/// Port target: `func_signature_string` (parse_func.c:2030).
pub fn func_signature_string(
    funcname: &[PgString<'_>],
    nargs: i32,
    argnames: &[PgString<'_>],
    argtypes: &[Oid],
) -> PgResult<String> {
    funcname_signature_string(&name_list_to_string_str(funcname)?, nargs, argnames, argtypes)
}

/// `NameListToString(funcname)` — render a qualified name as a dotted string
/// (parse_type.c), returning an owned `String`.
fn name_list_to_string_str(funcname: &[PgString<'_>]) -> PgResult<String> {
    // The seam allocates in an mcx; render to an owned String for error text.
    // Reproduce NameListToString directly (dotted join) to avoid threading an
    // mcx here — identical output for the String-component case.
    let mut out = String::new();
    for (i, comp) in funcname.iter().enumerate() {
        if i > 0 {
            out.push('.');
        }
        out.push_str(comp.as_str());
    }
    Ok(out)
}

// ===========================================================================
// LookupFuncNameInternal (parse_func.c:2048)
// ===========================================================================

/// Port target: `LookupFuncNameInternal` (parse_func.c:2048). Returns the found
/// OID (or `InvalidOid`) and sets `*lookup_error`.
fn LookupFuncNameInternal<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    funcname: &[PgString<'_>],
    nargs: i32,
    argtypes: &[Oid],
    include_out_arguments: bool,
    missing_ok: bool,
    lookup_error: &mut FuncLookupError,
) -> PgResult<Oid> {
    let mut result = InvalidOid;

    // NULL argtypes allowed for nullary functions only.
    debug_assert!(!argtypes.is_empty() || nargs <= 0);

    // Always set *lookupError.
    *lookup_error = FuncLookupError::NoSuchFunc;

    // Get list of candidate objects.
    let names: Vec<&str> = funcname.iter().map(|s| s.as_str()).collect();
    let clist = funcname_get_candidates::call(
        mcx,
        &names,
        nargs,
        &[],
        false,
        false,
        include_out_arguments,
        missing_ok,
    )?;

    // Scan list for a match to the arg types and the objtype.
    for cand in clist.iter() {
        // Check arg type match, if specified.
        if nargs >= 0 {
            // if nargs==0, argtypes can be null; don't pass that to memcmp.
            if nargs > 0 && !oid_slices_eq(argtypes, &cand.args, nargs as usize) {
                continue;
            }
        }

        // Check for duplicates reported by FuncnameGetCandidates.
        if !OidIsValid(cand.oid) {
            *lookup_error = FuncLookupError::Ambiguous;
            return Ok(InvalidOid);
        }

        // Check objtype match, if specified.
        match objtype {
            OBJECT_FUNCTION | OBJECT_AGGREGATE => {
                // Ignore procedures.
                if get_func_prokind::call(cand.oid)? == PROKIND_PROCEDURE {
                    continue;
                }
            }
            OBJECT_PROCEDURE => {
                // Ignore non-procedures.
                if get_func_prokind::call(cand.oid)? != PROKIND_PROCEDURE {
                    continue;
                }
            }
            OBJECT_ROUTINE => {
                // no restriction
            }
            _ => {
                debug_assert!(false);
            }
        }

        // Check for multiple matches.
        if OidIsValid(result) {
            *lookup_error = FuncLookupError::Ambiguous;
            return Ok(InvalidOid);
        }

        // OK, we have a candidate.
        result = cand.oid;
    }

    Ok(result)
}

// ===========================================================================
// LookupFuncName (parse_func.c:2143)
// ===========================================================================

/// Port target: `LookupFuncName` (parse_func.c:2143).
pub fn LookupFuncName<'mcx>(
    mcx: Mcx<'mcx>,
    funcname: &[PgString<'_>],
    nargs: i32,
    argtypes: &[Oid],
    missing_ok: bool,
) -> PgResult<Oid> {
    let mut lookup_error = FuncLookupError::NoSuchFunc;

    let funcoid = LookupFuncNameInternal(
        mcx,
        OBJECT_FUNCTION,
        funcname,
        nargs,
        argtypes,
        false,
        missing_ok,
        &mut lookup_error,
    )?;

    if OidIsValid(funcoid) {
        return Ok(funcoid);
    }

    match lookup_error {
        FuncLookupError::NoSuchFunc => {
            // Let the caller deal with it when missing_ok is true.
            if missing_ok {
                return Ok(InvalidOid);
            }

            if nargs < 0 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_FUNCTION)
                    .errmsg(format!(
                        "could not find a function named \"{}\"",
                        name_list_to_string_str(funcname)?
                    ))
                    .into_error());
            } else {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_FUNCTION)
                    .errmsg(format!(
                        "function {} does not exist",
                        func_signature_string(funcname, nargs, &[], argtypes)?
                    ))
                    .into_error());
            }
        }
        FuncLookupError::Ambiguous => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_AMBIGUOUS_FUNCTION)
                .errmsg(format!(
                    "function name \"{}\" is not unique",
                    name_list_to_string_str(funcname)?
                ))
                .errhint("Specify the argument list to select the function unambiguously.")
                .into_error());
        }
    }
}

// ===========================================================================
// LookupFuncWithArgs (parse_func.c:2205)
// ===========================================================================

/// Port target: `LookupFuncWithArgs` (parse_func.c:2205) for a plain
/// [`ObjectWithArgs`] describing a function (the `OBJECT_FUNCTION` form
/// opclasscmds.c uses).
pub fn lookup_func_with_args<'mcx>(
    mcx: Mcx<'mcx>,
    func: &ObjectWithArgs,
    missing_ok: bool,
) -> PgResult<Oid> {
    lookup_func_with_args_impl(
        mcx,
        OBJECT_FUNCTION,
        &func.objname,
        &func.objargs,
        func.args_unspecified,
        &[],
        missing_ok,
    )
}

/// Port target: `LookupFuncWithArgs` (parse_func.c:2205), the object-type-aware
/// form over the parser's own [`ParseObjectWithArgs`].
pub fn lookup_func_with_args_for_objtype<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    func: &ParseObjectWithArgs,
    missing_ok: bool,
) -> PgResult<Oid> {
    // Resolve the input arg TypeNames (Node::TypeName carriers) up front.
    let mut typenames: Vec<&types_parsenodes::TypeName> = Vec::new();
    for n in &func.objargs {
        let t = n
            .as_typename()
            .ok_or_else(|| internal_error("lookup_func_with_args: expected TypeName node"))?;
        typenames.push(t);
    }
    lookup_func_with_args_objtype_inner(
        mcx,
        objtype,
        &func.objname,
        &typenames,
        func.args_unspecified,
        &func.objfuncargs,
        missing_ok,
    )
}

/// Shared body for the `types_opclass::TypeName` (objargs) variant.
fn lookup_func_with_args_impl<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    objname: &[String],
    objargs: &[types_opclass::TypeName],
    args_unspecified: bool,
    objfuncargs: &[types_parsenodes::Node],
    missing_ok: bool,
) -> PgResult<Oid> {
    let funcname: Vec<PgString<'mcx>> = objname
        .iter()
        .map(|s| PgString::from_str_in(s, mcx))
        .collect::<Result<_, _>>()?;

    let argcount = objargs.len() as i32;
    check_arg_count(objtype, argcount)?;

    // First, perform a lookup considering only input arguments.
    let mut argoids: Vec<Oid> = Vec::new();
    for t in objargs {
        let oid = lookup_type_name_oid_owa::call(t, missing_ok)?;
        if !OidIsValid(oid) {
            return Ok(InvalidOid); // missing_ok must be true
        }
        argoids.push(oid);
    }

    lookup_func_with_args_finish(
        mcx,
        objtype,
        &funcname,
        argcount,
        &argoids,
        args_unspecified,
        objfuncargs,
        missing_ok,
    )
}

/// Shared body for the `types_parsenodes::TypeName` (objargs) variant.
fn lookup_func_with_args_objtype_inner<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    objname: &[String],
    objargs: &[&types_parsenodes::TypeName],
    args_unspecified: bool,
    objfuncargs: &[types_parsenodes::Node],
    missing_ok: bool,
) -> PgResult<Oid> {
    let funcname: Vec<PgString<'mcx>> = objname
        .iter()
        .map(|s| PgString::from_str_in(s, mcx))
        .collect::<Result<_, _>>()?;

    let argcount = objargs.len() as i32;
    check_arg_count(objtype, argcount)?;

    let mut argoids: Vec<Oid> = Vec::new();
    for t in objargs {
        let oid = lookup_type_name_oid::call(t, missing_ok)?;
        if !OidIsValid(oid) {
            return Ok(InvalidOid);
        }
        argoids.push(oid);
    }

    lookup_func_with_args_finish(
        mcx,
        objtype,
        &funcname,
        argcount,
        &argoids,
        args_unspecified,
        objfuncargs,
        missing_ok,
    )
}

/// The lookup + objtype-validation tail shared by both `LookupFuncWithArgs`
/// argument representations (parse_func.c:2255 onward).
fn lookup_func_with_args_finish<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    funcname: &[PgString<'_>],
    argcount: i32,
    argoids: &[Oid],
    args_unspecified: bool,
    objfuncargs: &[types_parsenodes::Node],
    missing_ok: bool,
) -> PgResult<Oid> {
    // -1 means no args were specified.
    let nargs = if args_unspecified { -1 } else { argcount };

    let mut lookup_error = FuncLookupError::NoSuchFunc;
    let mut oid = LookupFuncNameInternal(
        mcx,
        if args_unspecified {
            objtype
        } else {
            OBJECT_ROUTINE
        },
        funcname,
        nargs,
        argoids,
        false,
        missing_ok,
        &mut lookup_error,
    )?;

    // If PROCEDURE or ROUTINE with a no-mode-marker arg list and no ambiguity,
    // perform a lookup considering all arguments.
    if (objtype == OBJECT_PROCEDURE || objtype == OBJECT_ROUTINE)
        && !objfuncargs.is_empty()
        && lookup_error != FuncLookupError::Ambiguous
    {
        let mut have_param_mode = false;
        for fp in objfuncargs {
            let fp = fp
                .as_functionparameter()
                .ok_or_else(|| internal_error("LookupFuncWithArgs: expected FunctionParameter"))?;
            if fp.mode != types_parsenodes::FUNC_PARAM_DEFAULT {
                have_param_mode = true;
                break;
            }
        }

        if !have_param_mode {
            // Without mode marks, objargs surely includes all params.
            debug_assert!(objfuncargs.len() as i32 == argcount);

            // For OBJECT_PROCEDURE, ignore non-procedures.
            let poid = LookupFuncNameInternal(
                mcx,
                objtype,
                funcname,
                argcount,
                argoids,
                true,
                missing_ok,
                &mut lookup_error,
            )?;

            // Combine results, handling ambiguity.
            if OidIsValid(poid) {
                if OidIsValid(oid) && oid != poid {
                    // got hits both ways, on different objects.
                    oid = InvalidOid;
                    lookup_error = FuncLookupError::Ambiguous;
                } else {
                    oid = poid;
                }
            } else if lookup_error == FuncLookupError::Ambiguous {
                oid = InvalidOid;
            }
        }
    }

    if OidIsValid(oid) {
        // Validate that the objtype matches the prokind of the found function.
        match objtype {
            OBJECT_FUNCTION => {
                // Only complain if it's a procedure.
                if get_func_prokind::call(oid)? == PROKIND_PROCEDURE {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                        .errmsg(format!(
                            "{} is not a function",
                            func_signature_string(funcname, argcount, &[], argoids)?
                        ))
                        .into_error());
                }
            }
            OBJECT_PROCEDURE => {
                if get_func_prokind::call(oid)? != PROKIND_PROCEDURE {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                        .errmsg(format!(
                            "{} is not a procedure",
                            func_signature_string(funcname, argcount, &[], argoids)?
                        ))
                        .into_error());
                }
            }
            OBJECT_AGGREGATE => {
                if get_func_prokind::call(oid)? != PROKIND_AGGREGATE {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                        .errmsg(format!(
                            "function {} is not an aggregate",
                            func_signature_string(funcname, argcount, &[], argoids)?
                        ))
                        .into_error());
                }
            }
            _ => {
                // OBJECT_ROUTINE accepts anything.
            }
        }

        Ok(oid) // All good
    } else {
        // Deal with cases where the lookup failed.
        match lookup_error {
            FuncLookupError::NoSuchFunc => {
                // Suppress no-such-func errors when missing_ok is true.
                if missing_ok {
                    return Ok(InvalidOid);
                }

                match objtype {
                    OBJECT_PROCEDURE => {
                        if args_unspecified {
                            Err(ereport(ERROR)
                                .errcode(ERRCODE_UNDEFINED_FUNCTION)
                                .errmsg(format!(
                                    "could not find a procedure named \"{}\"",
                                    name_list_to_string_str(funcname)?
                                ))
                                .into_error())
                        } else {
                            Err(ereport(ERROR)
                                .errcode(ERRCODE_UNDEFINED_FUNCTION)
                                .errmsg(format!(
                                    "procedure {} does not exist",
                                    func_signature_string(funcname, argcount, &[], argoids)?
                                ))
                                .into_error())
                        }
                    }
                    OBJECT_AGGREGATE => {
                        if args_unspecified {
                            Err(ereport(ERROR)
                                .errcode(ERRCODE_UNDEFINED_FUNCTION)
                                .errmsg(format!(
                                    "could not find an aggregate named \"{}\"",
                                    name_list_to_string_str(funcname)?
                                ))
                                .into_error())
                        } else if argcount == 0 {
                            Err(ereport(ERROR)
                                .errcode(ERRCODE_UNDEFINED_FUNCTION)
                                .errmsg(format!(
                                    "aggregate {}(*) does not exist",
                                    name_list_to_string_str(funcname)?
                                ))
                                .into_error())
                        } else {
                            Err(ereport(ERROR)
                                .errcode(ERRCODE_UNDEFINED_FUNCTION)
                                .errmsg(format!(
                                    "aggregate {} does not exist",
                                    func_signature_string(funcname, argcount, &[], argoids)?
                                ))
                                .into_error())
                        }
                    }
                    _ => {
                        // FUNCTION and ROUTINE.
                        if args_unspecified {
                            Err(ereport(ERROR)
                                .errcode(ERRCODE_UNDEFINED_FUNCTION)
                                .errmsg(format!(
                                    "could not find a function named \"{}\"",
                                    name_list_to_string_str(funcname)?
                                ))
                                .into_error())
                        } else {
                            Err(ereport(ERROR)
                                .errcode(ERRCODE_UNDEFINED_FUNCTION)
                                .errmsg(format!(
                                    "function {} does not exist",
                                    func_signature_string(funcname, argcount, &[], argoids)?
                                ))
                                .into_error())
                        }
                    }
                }
            }
            FuncLookupError::Ambiguous => {
                let what = match objtype {
                    OBJECT_FUNCTION => "function",
                    OBJECT_PROCEDURE => "procedure",
                    OBJECT_AGGREGATE => "aggregate",
                    OBJECT_ROUTINE => "routine",
                    _ => {
                        debug_assert!(false);
                        return Ok(InvalidOid);
                    }
                };
                let mut b = ereport(ERROR).errcode(ERRCODE_AMBIGUOUS_FUNCTION).errmsg(format!(
                    "{what} name \"{}\" is not unique",
                    name_list_to_string_str(funcname)?
                ));
                if args_unspecified {
                    b = b.errhint(format!(
                        "Specify the argument list to select the {what} unambiguously."
                    ));
                }
                Err(b.into_error())
            }
        }
    }
}

/// `argcount > FUNC_MAX_ARGS` check shared by both `LookupFuncWithArgs` forms
/// (parse_func.c:2222).
fn check_arg_count(objtype: ObjectType, argcount: i32) -> PgResult<()> {
    if argcount as usize > FUNC_MAX_ARGS {
        if objtype == OBJECT_PROCEDURE {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_TOO_MANY_ARGUMENTS)
                .errmsg_plural(
                    format!("procedures cannot have more than {FUNC_MAX_ARGS} argument"),
                    format!("procedures cannot have more than {FUNC_MAX_ARGS} arguments"),
                    FUNC_MAX_ARGS as u64,
                )
                .into_error());
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_TOO_MANY_ARGUMENTS)
                .errmsg_plural(
                    format!("functions cannot have more than {FUNC_MAX_ARGS} argument"),
                    format!("functions cannot have more than {FUNC_MAX_ARGS} arguments"),
                    FUNC_MAX_ARGS as u64,
                )
                .into_error());
        }
    }
    Ok(())
}

// ===========================================================================
// check_srf_call_placement (parse_func.c:2510)
// ===========================================================================

/// Port target: `check_srf_call_placement` (parse_func.c:2510).
///
/// Verify that a set-returning function is called in a valid place. A side
/// effect is to set `pstate->p_hasTargetSRFs` true if appropriate.
pub fn check_srf_call_placement<'mcx>(
    pstate: &mut ParseState<'mcx>,
    last_srf: Option<&Expr>,
    location: i32,
) -> PgResult<()> {
    use ParseExprKind::*;

    let mut err: Option<&'static str> = None;
    let mut errkind = false;

    match pstate.p_expr_kind {
        EXPR_KIND_NONE => {
            debug_assert!(false); // can't happen
        }
        EXPR_KIND_OTHER => {
            // Accept SRF here; caller must throw error if wanted.
        }
        EXPR_KIND_JOIN_ON | EXPR_KIND_JOIN_USING => {
            err = Some("set-returning functions are not allowed in JOIN conditions");
        }
        EXPR_KIND_FROM_SUBSELECT => {
            // can't get here, but just in case, throw an error.
            errkind = true;
        }
        EXPR_KIND_FROM_FUNCTION => {
            // okay, but we don't allow nested SRFs here.
            if !p_last_srf_eq(pstate, last_srf)? {
                let loc = node_expr_location(pstate.p_last_srf.as_deref())?;
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("set-returning functions must appear at top level of FROM")
                    .errposition(parser_errposition(Some(pstate), loc))
                    .into_error());
            }
        }
        EXPR_KIND_WHERE => errkind = true,
        EXPR_KIND_POLICY => {
            err = Some("set-returning functions are not allowed in policy expressions");
        }
        EXPR_KIND_HAVING => errkind = true,
        EXPR_KIND_FILTER => errkind = true,
        EXPR_KIND_WINDOW_PARTITION | EXPR_KIND_WINDOW_ORDER => {
            // okay, these are effectively GROUP BY/ORDER BY.
            pstate.p_hasTargetSRFs = true;
        }
        EXPR_KIND_WINDOW_FRAME_RANGE
        | EXPR_KIND_WINDOW_FRAME_ROWS
        | EXPR_KIND_WINDOW_FRAME_GROUPS => {
            err = Some("set-returning functions are not allowed in window definitions");
        }
        EXPR_KIND_SELECT_TARGET | EXPR_KIND_INSERT_TARGET => {
            // okay
            pstate.p_hasTargetSRFs = true;
        }
        EXPR_KIND_UPDATE_SOURCE | EXPR_KIND_UPDATE_TARGET => {
            // disallowed because it would be ambiguous what to do.
            errkind = true;
        }
        EXPR_KIND_GROUP_BY | EXPR_KIND_ORDER_BY => {
            // okay
            pstate.p_hasTargetSRFs = true;
        }
        EXPR_KIND_DISTINCT_ON => {
            // okay
            pstate.p_hasTargetSRFs = true;
        }
        EXPR_KIND_LIMIT | EXPR_KIND_OFFSET => errkind = true,
        EXPR_KIND_RETURNING | EXPR_KIND_MERGE_RETURNING => errkind = true,
        EXPR_KIND_VALUES => {
            // SRFs are presently not supported by nodeValuesscan.c.
            errkind = true;
        }
        EXPR_KIND_VALUES_SINGLE => {
            // okay, since we process this like a SELECT tlist.
            pstate.p_hasTargetSRFs = true;
        }
        EXPR_KIND_MERGE_WHEN => {
            err = Some("set-returning functions are not allowed in MERGE WHEN conditions");
        }
        EXPR_KIND_CHECK_CONSTRAINT | EXPR_KIND_DOMAIN_CHECK => {
            err = Some("set-returning functions are not allowed in check constraints");
        }
        EXPR_KIND_COLUMN_DEFAULT | EXPR_KIND_FUNCTION_DEFAULT => {
            err = Some("set-returning functions are not allowed in DEFAULT expressions");
        }
        EXPR_KIND_INDEX_EXPRESSION => {
            err = Some("set-returning functions are not allowed in index expressions");
        }
        EXPR_KIND_INDEX_PREDICATE => {
            err = Some("set-returning functions are not allowed in index predicates");
        }
        EXPR_KIND_STATS_EXPRESSION => {
            err = Some("set-returning functions are not allowed in statistics expressions");
        }
        EXPR_KIND_ALTER_COL_TRANSFORM => {
            err = Some("set-returning functions are not allowed in transform expressions");
        }
        EXPR_KIND_EXECUTE_PARAMETER => {
            err = Some("set-returning functions are not allowed in EXECUTE parameters");
        }
        EXPR_KIND_TRIGGER_WHEN => {
            err = Some("set-returning functions are not allowed in trigger WHEN conditions");
        }
        EXPR_KIND_PARTITION_BOUND => {
            err = Some("set-returning functions are not allowed in partition bound");
        }
        EXPR_KIND_PARTITION_EXPRESSION => {
            err = Some("set-returning functions are not allowed in partition key expressions");
        }
        EXPR_KIND_CALL_ARGUMENT => {
            err = Some("set-returning functions are not allowed in CALL arguments");
        }
        EXPR_KIND_COPY_WHERE => {
            err = Some("set-returning functions are not allowed in COPY FROM WHERE conditions");
        }
        EXPR_KIND_GENERATED_COLUMN => {
            err = Some("set-returning functions are not allowed in column generation expressions");
        }
        EXPR_KIND_CYCLE_MARK => errkind = true,
    }

    if let Some(err) = err {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg_internal(err)
            .errposition(parser_errposition(Some(pstate), location))
            .into_error());
    }
    if errkind {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "set-returning functions are not allowed in {}",
                parse_expr_kind_name::call(pstate.p_expr_kind)
            ))
            .errposition(parser_errposition(Some(pstate), location))
            .into_error());
    }

    Ok(())
}

// ===========================================================================
// Inline helpers.
// ===========================================================================

/// `memcmp(a, b, n * sizeof(Oid)) == 0`.
#[inline]
fn oid_slices_eq(a: &[Oid], b: &[Oid], n: usize) -> bool {
    if a.len() < n || b.len() < n {
        return false;
    }
    a[..n] == b[..n]
}

/// A throwaway placeholder `Const` used only as a temporary while we move an
/// `Expr` out of a `&mut [Expr]` slot during in-place coercion. It is always
/// overwritten before the function returns.
fn dummy_const() -> types_nodes::primnodes::Const {
    types_nodes::primnodes::Const {
        consttype: InvalidOid,
        consttypmod: -1,
        constcollid: InvalidOid,
        constlen: 0,
        constvalue: types_tuple::Datum::null(),
        constisnull: true,
        constbyval: false,
        location: -1,
    }
}

/// `errmsg_internal` `ereport(ERROR)` (an "internal error" — `elog(ERROR)`).
fn internal_error(msg: &'static str) -> PgError {
    ereport(ERROR).errmsg_internal(msg).into_error()
}

/// The `errcode(ERRCODE_TOO_MANY_ARGUMENTS)` `cannot pass more than N` error
/// shared by several call sites.
fn too_many_arguments_error(pstate: Option<&ParseState<'_>>, location: i32) -> PgResult<PgError> {
    Ok(ereport(ERROR)
        .errcode(ERRCODE_TOO_MANY_ARGUMENTS)
        .errmsg_plural(
            format!("cannot pass more than {FUNC_MAX_ARGS} argument to a function"),
            format!("cannot pass more than {FUNC_MAX_ARGS} arguments to a function"),
            FUNC_MAX_ARGS as u64,
        )
        .errposition(parser_errposition(pstate, location))
        .into_error())
}

// ===========================================================================
// Seam installation.
// ===========================================================================

/// Install the seams this crate owns. Wired into `seams_init::init_all()`.
pub fn init_seams() {
    me::lookup_func_name::set(seam_lookup_func_name);
    me::lookup_func_with_args::set(seam_lookup_func_with_args);
    me::lookup_func_with_args_for_objtype::set(seam_lookup_func_with_args_for_objtype);
    me::func_match_argtypes::set(func_match_argtypes);
    me::func_select_candidate::set(func_select_candidate);
    me::make_fn_arguments::set(seam_make_fn_arguments);
    me::check_srf_call_placement::set(check_srf_call_placement);
    me::set_last_srf::set(seam_set_last_srf);
    me::func_get_detail::set(seam_func_get_detail);

    // Cross-crate install: `funcname_signature_string` (parse_func.c, body here)
    // is consumed by functioncmds for `ereport` argument-signature text; its
    // decl lives on `backend-commands-functioncmds-seams` (owned `String`
    // proname, `Vec<Oid>` argtypes, no named-arg list — pass an empty slice).
    backend_commands_functioncmds_seams::funcname_signature_string::set(
        |proname, pronargs, arg_types| {
            funcname_signature_string(&proname, pronargs, &[], &arg_types)
        },
    );

    // Cross-crate install: `func_signature_string` (parse_func.c, body here)
    // consumed by typecmds/pg_aggregate/functioncmds for `ereport` signature
    // text. The decl carries owned `Vec<String>` name components; the body
    // wants `&[PgString]`. The names are read only to render the (owned)
    // result string, so materialise them in a throwaway scratch context.
    backend_commands_functioncmds_seams::func_signature_string::set(
        |funcname, nargs, argtypes| {
            let scratch = mcx::MemoryContext::new("func_signature_string scratch");
            let mcx = scratch.mcx();
            let names: Vec<PgString<'_>> = funcname
                .iter()
                .map(|s| PgString::from_str_in(s, mcx))
                .collect::<PgResult<_>>()?;
            func_signature_string(&names, nargs, &[], &argtypes)
        },
    );

    // Cross-crate install: `LookupFuncName(funcname, nargs, argtypes,
    // missing_ok)` (parse_func.c, body here) is reached by functioncmds.c
    // (CreateCast / SUPPORT lookup) and by typecmds.c's I/O-function resolution
    // through the functioncmds-seams channel. The decl carries owned
    // `Vec<String>` name components; the body wants `&[PgString]`, materialised
    // in a scratch context.
    backend_commands_functioncmds_seams::lookup_func_name::set(
        |funcname, nargs, argtypes, missing_ok| {
            let scratch = mcx::MemoryContext::new("functioncmds lookup_func_name");
            let mcx = scratch.mcx();
            let names: Vec<PgString<'_>> = funcname
                .iter()
                .map(|s| PgString::from_str_in(s, mcx))
                .collect::<PgResult<_>>()?;
            LookupFuncName(mcx, &names, nargs, &argtypes, missing_ok)
        },
    );

    // Cross-crate install: `recheck_cast_function_args` (clauses.c:4382, the
    // const-fold simplify path) re-runs the parser's type resolution over the
    // (reordered / default-expanded) argument list. The decl lives on
    // `backend-optimizer-util-clauses-seams`; the body needs the parser's
    // `enforce_generic_type_consistency` + `make_fn_arguments`, both reachable
    // here.
    backend_optimizer_util_clauses_seams::recheck_cast_function_args::set(
        seam_recheck_cast_function_args,
    );
}

/// `recheck_cast_function_args` (clauses.c:4382): recheck function args and
/// typecast as needed. Re-derives the actual argument types via `exprType`,
/// re-runs `enforce_generic_type_consistency` against the declared (possibly
/// polymorphic) `proargtypes` to resolve polymorphism and verify the resolved
/// result type still matches what the parser produced, then applies any needed
/// casts via `make_fn_arguments`. Returns the (possibly cast) argument list.
fn seam_recheck_cast_function_args(
    mut args: Vec<Expr>,
    result_type: Oid,
    proargtypes: Vec<Oid>,
    prorettype: Oid,
) -> PgResult<Vec<Expr>> {
    if args.len() > FUNC_MAX_ARGS {
        return Err(internal_error("too many function arguments"));
    }

    let nargs = args.len() as i32;
    let actual_arg_types: Vec<Oid> = args
        .iter()
        .map(|a| exprType(Some(a)))
        .collect::<PgResult<_>>()?;

    debug_assert_eq!(args.len(), proargtypes.len());
    let mut declared_arg_types = proargtypes;

    let rettype = enforce_generic_type_consistency::call(
        &actual_arg_types,
        &mut declared_arg_types,
        nargs,
        prorettype,
        false,
    )?;

    // let's just check we got the same answer as the parser did ...
    if rettype != result_type {
        return Err(internal_error(
            "function's resolved result type changed during planning",
        ));
    }

    // perform any necessary typecasting of arguments (pstate == NULL)
    make_fn_arguments(None, &mut args, &actual_arg_types, &declared_arg_types)?;

    Ok(args)
}

/// Seam entry for `func_get_detail`. The sole cross-crate caller is
/// `pg_aggregate.c`'s `lookup_agg_function`, which calls
/// `func_get_detail(fnName, NIL, NIL, nargs, input_types, false, false, false,
/// ..., NULL)` — i.e. no argument expressions or names, no variadic/default
/// expansion, no OUT arguments, and a NULL `argdefaults` out-param. This
/// wrapper threads those fixed flags into the full private port and maps the
/// owner's local `FuncDetail`/`FuncDetailCode` onto the seam contract's
/// (`mcx`-allocated `true_typeids`).
fn seam_func_get_detail<'mcx>(
    mcx: Mcx<'mcx>,
    funcname: &[String],
    nargs: i32,
    argtypes: &[Oid],
) -> PgResult<me::FuncDetail<'mcx>> {
    let names: Vec<PgString<'mcx>> = funcname
        .iter()
        .map(|s| PgString::from_str_in(s, mcx))
        .collect::<Result<_, _>>()?;
    let detail = func_get_detail(
        mcx,
        &names,
        &[],   // fargs = NIL
        &[],   // fargnames = NIL
        nargs,
        argtypes,
        false, // expand_variadic
        false, // expand_defaults
        false, // include_out_arguments
        false, // argdefaults out-param is NULL
    )?;

    let mut true_typeids = PgVec::new_in(mcx);
    for &t in &detail.true_typeids {
        true_typeids.push(t);
    }

    Ok(me::FuncDetail {
        fdresult: func_detail_code_to_seam(detail.fdresult),
        funcid: detail.funcid,
        rettype: detail.rettype,
        retset: detail.retset,
        nvargs: detail.nvargs,
        vatype: detail.vatype,
        true_typeids,
    })
}

/// Map the owner's local [`FuncDetailCode`] onto the seam contract's enum.
fn func_detail_code_to_seam(code: FuncDetailCode) -> me::FuncDetailCode {
    match code {
        FuncDetailCode::NotFound => me::FuncDetailCode::NotFound,
        FuncDetailCode::Multiple => me::FuncDetailCode::Multiple,
        FuncDetailCode::Normal => me::FuncDetailCode::Normal,
        FuncDetailCode::Procedure => me::FuncDetailCode::Procedure,
        FuncDetailCode::Aggregate => me::FuncDetailCode::Aggregate,
        FuncDetailCode::WindowFunc => me::FuncDetailCode::WindowFunc,
        FuncDetailCode::Coercion => me::FuncDetailCode::Coercion,
    }
}

fn seam_lookup_func_name(
    funcname: &[PgString<'_>],
    nargs: i32,
    argtypes: &[Oid],
    missing_ok: bool,
) -> PgResult<Oid> {
    let cx = MemoryContext::new("LookupFuncName");
    LookupFuncName(cx.mcx(), funcname, nargs, argtypes, missing_ok)
}

fn seam_lookup_func_with_args(func: &ObjectWithArgs, missing_ok: bool) -> PgResult<Oid> {
    let cx = MemoryContext::new("LookupFuncWithArgs");
    lookup_func_with_args(cx.mcx(), func, missing_ok)
}

fn seam_lookup_func_with_args_for_objtype(
    objtype: ObjectType,
    func: &ParseObjectWithArgs,
    missing_ok: bool,
) -> PgResult<Oid> {
    let cx = MemoryContext::new("LookupFuncWithArgs");
    lookup_func_with_args_for_objtype(cx.mcx(), objtype, func, missing_ok)
}

fn seam_make_fn_arguments<'mcx>(
    pstate: Option<&mut ParseState<'mcx>>,
    fargs: &mut [Expr],
    actual_arg_types: &[Oid],
    declared_arg_types: &[Oid],
) -> PgResult<()> {
    make_fn_arguments(pstate, fargs, actual_arg_types, declared_arg_types)
}

fn seam_set_last_srf<'mcx>(pstate: &mut ParseState<'mcx>, result: &Expr) -> PgResult<()> {
    set_p_last_srf(pstate, result)
}

include!("parse_func_or_column.rs");
