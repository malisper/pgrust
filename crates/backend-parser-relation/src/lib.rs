//! Port of `src/backend/parser/parse_relation.c` (PostgreSQL 18.3) — the parser
//! support routines that deal with relations: range-table-entry lookup/creation
//! (`addRangeTableEntry*`), namespace-item searches (`refnameNamespaceItem`,
//! `scanNameSpaceFor*`), column lookup (`scanRTEForColumn`, `colNameToVar`,
//! `searchRangeTableForCol`), alias construction (`buildRelationAliases`),
//! RTE/nsitem expansion (`expandRTE`, `expandNSItem*`), and the various `get_*` /
//! `attnum*` helpers.
//!
//! Logic, branch order, error-message text and SQLSTATE values follow PG 18.3
//! one-for-one.
//!
//! # Owned-tree representation
//!
//! The C `ParseState *` becomes `&ParseState` (mutating routines take
//! `&mut ParseState`). Each `ParseNamespaceItem` carries its own owned copy of
//! the RTE (`p_rte`) plus the 1-based `p_rtindex` into `p_rtable`; the C
//! `nsitem->p_rte` aliasing is modeled by indexing back into `p_rtable` /
//! `p_rteperminfos` when mutation is needed. Scan/search helpers return indices
//! (`Option<usize>` / coordinates) rather than borrows so callers can re-borrow.
//!
//! # Seam-and-panic deferrals (matching src-idiomatic)
//!
//! The function-RTE composite/RECORD machinery (`get_expr_result_type`,
//! `CreateTemplateTupleDesc`, `typenameTypeIdAndMod`, `GetColumnDefCollation`,
//! `CheckAttributeNamesTypes`, `format_type_be`, `get_func_result_name`,
//! `get_expr_result_tupdesc`) is unported funcapi/parse_type — those arms
//! mirror-PG-and-panic. The overall structure and every other arm are ported.

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use alloc::format;

use mcx::{Mcx, PgString, PgVec};

use types_core::{AttrNumber, Index, InvalidAttrNumber, InvalidOid, Oid, OidIsValid};
use types_core::catalog::INT4OID;
use types_tuple::heaptuple::RECORDOID;

/// `RECORDARRAYOID` (`catalog/pg_type_d.h`) — OID of the `record[]` type. Not
/// re-exported by the type-OID crates yet; carried locally (used by the CTE
/// SEARCH/CYCLE extra-column arms).
const RECORDARRAYOID: Oid = 2287;

use types_acl::acl::ACL_SELECT;

use types_storage::lock::{AccessShareLock, RowExclusiveLock, RowShareLock, NoLock, LOCKMODE};

use types_tuple::heaptuple::{
    FirstLowInvalidHeapAttributeNumber,
    TableOidAttributeNumber, TupleDescData,
};
use types_tuple::access::RELKIND_COMPOSITE_TYPE;
use types_tuple::access::RELPERSISTENCE_TEMP;

use types_nodes::nodes::{Node, NodePtr};
use types_nodes::nodes::CmdType::CMD_SELECT;
use types_nodes::value::StringNode;
use types_nodes::primnodes::{Var, VarReturningType};
use types_nodes::primnodes::VarReturningType::VAR_RETURNING_DEFAULT;
use types_nodes::rawnodes::{Alias, RangeTblRef, RangeFunction, RangeVar, RowMarkClause};
use types_nodes::rawnodes::CommonTableExpr;
use types_nodes::primnodes::TableFunc;
use types_nodes::parsenodes::{
    RangeTblEntry, RTEPermissionInfo, RTE_CTE, RTE_FUNCTION, RTE_GROUP, RTE_JOIN,
    RTE_NAMEDTUPLESTORE, RTE_RELATION, RTE_RESULT, RTE_SUBQUERY, RTE_TABLEFUNC, RTE_VALUES,
};
use types_nodes::parsestmt::{ParseNamespaceColumn, ParseNamespaceItem, ParseState};
use types_nodes::parsestmt::ParseExprKind::{
    EXPR_KIND_CHECK_CONSTRAINT, EXPR_KIND_GENERATED_COLUMN, EXPR_KIND_MERGE_WHEN,
};
use types_nodes::copy_query::Query;
use types_nodes::queryenvironment::ENR_NAMED_TUPLESTORE;

use types_error::error::{
    ERRCODE_AMBIGUOUS_ALIAS, ERRCODE_AMBIGUOUS_COLUMN, ERRCODE_DUPLICATE_ALIAS,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_COLUMN_REFERENCE, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
    ERRCODE_UNDEFINED_COLUMN, ERRCODE_UNDEFINED_TABLE,
};
use types_error::{PgResult, ERROR};

use backend_utils_error::ereport;

use backend_nodes_core::makefuncs::{make_alias, make_null_const, make_target_entry, make_var};

use backend_parser_small1_seams as small1_seam;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_cache_syscache_seams as syscache;
use backend_optimizer_util_plancat_ext_seams as plancat_ext;

/// `MAX_FUZZY_DISTANCE` (parse_relation.c).
const MAX_FUZZY_DISTANCE: i32 = 3;

/// `MaxAttrNumber` (`access/attnum.h`).
const MaxAttrNumber: i32 = 32767;

// ===========================================================================
// Small node helpers.
// ===========================================================================

/// `makeString(pstrdup(s))` as a central `Node::String` boxed for a colname list.
fn make_string_node<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<NodePtr<'mcx>> {
    let node = Node::String(StringNode {
        sval: PgString::from_str_in(s, mcx)?,
    });
    mcx::alloc_in(mcx, node)
}

/// `strVal(node)` — read a `String` node's contents.
fn str_val<'a>(node: &'a Node<'_>) -> &'a str {
    match node {
        Node::String(s) => s.sval.as_str(),
        _ => panic!("strVal: node is not a String"),
    }
}

/// `(Node *) makeVar(...)` — wrap a `Var` as the central `Node`.
fn var_node<'mcx>(var: Var) -> Node<'mcx> {
    Node::Expr(types_nodes::primnodes::Expr::Var(var))
}

/// `NameStr(attr->attname)` (a `NameData` carries NUL-terminated bytes).
fn attname_str(attr: &types_tuple::heaptuple::FormData_pg_attribute) -> &str {
    let bytes = attr.attname.name_str();
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    core::str::from_utf8(&bytes[..end]).unwrap_or("")
}

/// Convert a parse-tree [`RangeVar`] (`types_nodes::rawnodes::RangeVar`) into the
/// access-layer `types_tuple::access::RangeVar` that `table_openrv_extended` /
/// `RangeVarGetRelid` consume. (The two RangeVar models are an inherited split;
/// the access layer's variant carries no alias.)
fn to_access_range_var(rv: &RangeVar<'_>) -> types_tuple::access::RangeVar {
    types_tuple::access::RangeVar {
        catalogname: rv.catalogname.as_deref().map(|s| s.into()),
        schemaname: rv.schemaname.as_deref().map(|s| s.into()),
        relname: rv.relname.as_deref().unwrap_or("").into(),
        inh: rv.inh,
        relpersistence: rv.relpersistence as u8,
        location: rv.location,
    }
}

/// `parser_errposition(pstate, location)` via the small1 seam (infallible in C).
fn parser_errposition(pstate: &ParseState<'_>, location: i32) -> i32 {
    small1_seam::parser_errposition::call(pstate, location).unwrap_or(0)
}

/// `rte->eref->aliasname` (eref is always present once an RTE is built).
fn rte_eref_aliasname<'a>(rte: &'a RangeTblEntry<'_>) -> &'a str {
    rte.eref
        .as_deref()
        .and_then(|a| a.aliasname.as_deref())
        .unwrap_or("")
}

/// `nsitem->p_names->aliasname`.
fn nsitem_aliasname<'a>(nsitem: &'a ParseNamespaceItem<'_>) -> &'a str {
    nsitem
        .p_names
        .as_deref()
        .and_then(|a| a.aliasname.as_deref())
        .unwrap_or("")
}

/// `nsitem->p_rte`.
fn nsitem_rte<'a, 'mcx>(nsitem: &'a ParseNamespaceItem<'mcx>) -> &'a RangeTblEntry<'mcx> {
    nsitem
        .p_rte
        .as_deref()
        .expect("ParseNamespaceItem.p_rte is always set")
}

// ===========================================================================
// refnameNamespaceItem + scanNameSpaceFor* family.
// ===========================================================================

/// `refnameNamespaceItem` — return the index `(sublevels_up, ns_idx)` of the
/// matching visible namespace item, or `None`. Returning coordinates (rather
/// than a borrow) mirrors the established index-returning pattern so callers can
/// re-borrow `pstate`. When the caller wants only the current level (C
/// `sublevels_up == NULL`), pass `want_uplevels = false`.
pub fn refnameNamespaceItem(
    pstate: &ParseState<'_>,
    schemaname: Option<&str>,
    refname: &str,
    location: i32,
    want_uplevels: bool,
) -> PgResult<Option<(i32, usize)>> {
    let mut rel_id = InvalidOid;

    if let Some(schemaname) = schemaname {
        // LookupNamespaceNoError: only finding existing RTEs; no USAGE check.
        let namespace_id = backend_catalog_namespace::LookupNamespaceNoError(schemaname)?;
        if !OidIsValid(namespace_id) {
            return Ok(None);
        }
        rel_id = lsyscache::get_relname_relid::call(refname, namespace_id)?;
        if !OidIsValid(rel_id) {
            return Ok(None);
        }
    }

    let mut levelsup: i32 = 0;
    let mut cur: Option<&ParseState> = Some(pstate);
    while let Some(ps) = cur {
        let result = if OidIsValid(rel_id) {
            scanNameSpaceForRelid(ps, rel_id, location)?
        } else {
            scanNameSpaceForRefname(ps, refname, location)?
        };

        if let Some(idx) = result {
            return Ok(Some((levelsup, idx)));
        }

        if want_uplevels {
            levelsup += 1;
        } else {
            break;
        }
        cur = ps.parentParseState.as_deref();
    }
    Ok(None)
}

/// `scanNameSpaceForRefname` — return the namespace index matching `refname`, or
/// `None`. Raises on multiple matches.
fn scanNameSpaceForRefname(
    pstate: &ParseState<'_>,
    refname: &str,
    location: i32,
) -> PgResult<Option<usize>> {
    let mut result: Option<usize> = None;

    for (idx, nsitem) in pstate.p_namespace.iter().enumerate() {
        // Ignore columns-only items
        if !nsitem.p_rel_visible {
            continue;
        }
        // If not inside LATERAL, ignore lateral-only items
        if nsitem.p_lateral_only && !pstate.p_lateral_active {
            continue;
        }

        if nsitem_aliasname(nsitem) == refname {
            if result.is_some() {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_AMBIGUOUS_ALIAS)
                    .errmsg(format!("table reference \"{refname}\" is ambiguous"))
                    .errposition(parser_errposition(pstate, location))
                    .into_error());
            }
            check_lateral_ref_ok(pstate, nsitem, location)?;
            result = Some(idx);
        }
    }
    Ok(result)
}

/// `scanNameSpaceForRelid` — return the index of the relation item matching
/// `relid`, or `None`. Raises on multiple matches.
fn scanNameSpaceForRelid(
    pstate: &ParseState<'_>,
    relid: Oid,
    location: i32,
) -> PgResult<Option<usize>> {
    let mut result: Option<usize> = None;

    for (idx, nsitem) in pstate.p_namespace.iter().enumerate() {
        let rte = nsitem_rte(nsitem);

        // Ignore columns-only items
        if !nsitem.p_rel_visible {
            continue;
        }
        // If not inside LATERAL, ignore lateral-only items
        if nsitem.p_lateral_only && !pstate.p_lateral_active {
            continue;
        }
        // Ignore OLD/NEW namespace items that can appear in RETURNING
        if nsitem.p_returning_type != VAR_RETURNING_DEFAULT {
            continue;
        }

        // yes, the test for alias == NULL should be there...
        if rte.rtekind == RTE_RELATION && rte.relid == relid && rte.alias.is_none() {
            if result.is_some() {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_AMBIGUOUS_ALIAS)
                    .errmsg(format!("table reference {relid} is ambiguous"))
                    .errposition(parser_errposition(pstate, location))
                    .into_error());
            }
            check_lateral_ref_ok(pstate, nsitem, location)?;
            result = Some(idx);
        }
    }
    Ok(result)
}

/// `scanNameSpaceForCTE` — return the CTE matching `refname` (cloned) and its
/// levelsup count, or `None`.
pub fn scanNameSpaceForCTE<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'mcx>,
    refname: &str,
) -> PgResult<Option<(CommonTableExpr<'mcx>, Index)>> {
    let mut levelsup: Index = 0;
    let mut cur: Option<&ParseState<'mcx>> = Some(pstate);
    while let Some(ps) = cur {
        for cte in ps.p_ctenamespace.iter() {
            if cte.ctename.as_deref() == Some(refname) {
                // C returns the CTE pointer; the owned model returns a copy.
                return Ok(Some((cte.clone_in(mcx)?, levelsup)));
            }
        }
        cur = ps.parentParseState.as_deref();
        levelsup += 1;
    }
    Ok(None)
}

/// `isFutureCTE` — true iff `refname` names a not-yet-in-scope CTE.
fn isFutureCTE(pstate: &ParseState<'_>, refname: &str) -> bool {
    let mut cur: Option<&ParseState> = Some(pstate);
    while let Some(ps) = cur {
        for cte in ps.p_future_ctes.iter() {
            if cte.ctename.as_deref() == Some(refname) {
                return true;
            }
        }
        cur = ps.parentParseState.as_deref();
    }
    false
}

/// `scanNameSpaceForENR` — true iff `refname` matches a visible ENR.
pub fn scanNameSpaceForENR(pstate: &ParseState<'_>, refname: &str) -> bool {
    backend_parser_small1::name_matches_visible_ENR(pstate, refname)
}

/// `searchRangeTableForRel` — see if any RTE could possibly match the RangeVar.
/// Returns the matching RTE as `(depth, rte_index)` coordinates, or `None`.
/// Heuristic: used only by `errorMissingRTE`.
fn searchRangeTableForRel<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'mcx>,
    relation: &RangeVar<'_>,
) -> PgResult<Option<(usize, usize)>> {
    let refname = relation.relname.as_deref().unwrap_or("");
    let mut rel_id = InvalidOid;
    let mut cte: Option<CommonTableExpr> = None;
    let mut isenr = false;
    let mut ctelevelsup: Index = 0;

    // Unqualified name: check CTE, then ENR; else look up the relation.
    if relation.schemaname.is_none() {
        if let Some((c, lvl)) = scanNameSpaceForCTE(mcx, pstate, refname)? {
            cte = Some(c);
            ctelevelsup = lvl;
        } else {
            isenr = scanNameSpaceForENR(pstate, refname);
        }
    }

    if cte.is_none() && !isenr {
        rel_id = backend_catalog_namespace::RangeVarGetRelid(mcx, &to_access_range_var(relation), NoLock, true)?;
    }

    let mut levelsup: Index = 0;
    let mut depth: usize = 0;
    let mut cur: Option<&ParseState> = Some(pstate);
    while let Some(ps) = cur {
        for (rte_index, rte) in ps.p_rtable.iter().enumerate() {
            if rte.rtekind == RTE_RELATION && OidIsValid(rel_id) && rte.relid == rel_id {
                return Ok(Some((depth, rte_index)));
            }
            if rte.rtekind == RTE_CTE
                && cte.is_some()
                && rte.ctelevelsup + levelsup == ctelevelsup
                && rte.ctename.as_deref() == Some(refname)
            {
                return Ok(Some((depth, rte_index)));
            }
            if rte.rtekind == RTE_NAMEDTUPLESTORE
                && isenr
                && rte.enrname.as_deref() == Some(refname)
            {
                return Ok(Some((depth, rte_index)));
            }
            if rte_eref_aliasname(rte) == refname {
                return Ok(Some((depth, rte_index)));
            }
        }
        cur = ps.parentParseState.as_deref();
        levelsup += 1;
        depth += 1;
    }
    Ok(None)
}

// ===========================================================================
// checkNameSpaceConflicts.
// ===========================================================================

/// `checkNameSpaceConflicts` — raise on relation-name conflicts between two
/// namespace lists.
pub fn checkNameSpaceConflicts(
    _pstate: &ParseState<'_>,
    namespace1: &[ParseNamespaceItem<'_>],
    namespace2: &[ParseNamespaceItem<'_>],
) -> PgResult<()> {
    for nsitem1 in namespace1.iter() {
        if !nsitem1.p_rel_visible {
            continue;
        }
        let rte1 = nsitem_rte(nsitem1);
        let aliasname1 = nsitem_aliasname(nsitem1);

        for nsitem2 in namespace2.iter() {
            if !nsitem2.p_rel_visible {
                continue;
            }
            let rte2 = nsitem_rte(nsitem2);
            let aliasname2 = nsitem_aliasname(nsitem2);

            if aliasname2 != aliasname1 {
                continue; // definitely no conflict
            }
            if rte1.rtekind == RTE_RELATION
                && rte1.alias.is_none()
                && rte2.rtekind == RTE_RELATION
                && rte2.alias.is_none()
                && rte1.relid != rte2.relid
            {
                continue; // no conflict per SQL rule
            }
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_ALIAS)
                .errmsg(format!("table name \"{aliasname1}\" specified more than once"))
                .into_error());
        }
    }
    Ok(())
}

// ===========================================================================
// check_lateral_ref_ok.
// ===========================================================================

/// `check_lateral_ref_ok` — complain if a namespace item is currently disallowed
/// as a LATERAL reference.
fn check_lateral_ref_ok(
    pstate: &ParseState<'_>,
    nsitem: &ParseNamespaceItem<'_>,
    location: i32,
) -> PgResult<()> {
    if nsitem.p_lateral_only && !nsitem.p_lateral_ok {
        let rte = nsitem_rte(nsitem);
        let refname = nsitem_aliasname(nsitem);

        // errhint vs errdetail per whether this is the UPDATE/DELETE target.
        let is_target = match pstate.p_target_nsitem.as_deref() {
            Some(t) => core::ptr::eq(
                nsitem_rte(t) as *const RangeTblEntry,
                rte as *const RangeTblEntry,
            ),
            None => false,
        };

        let mut b = ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg(format!(
                "invalid reference to FROM-clause entry for table \"{refname}\""
            ));
        if is_target {
            b = b.errhint(format!(
                "There is an entry for table \"{refname}\", but it cannot be referenced from this part of the query."
            ));
        } else {
            b = b.errdetail(format!(
                "The combining JOIN type must be INNER or LEFT for a LATERAL reference."
            ));
        }
        return Err(b
            .errposition(parser_errposition(pstate, location))
            .into_error());
    }
    Ok(())
}

// ===========================================================================
// GetNSItemByRangeTablePosn / GetRTEByRangeTablePosn / GetCTEForRTE.
// ===========================================================================

/// `GetNSItemByRangeTablePosn` — find the nsitem with the given RT index at the
/// given nesting depth (there must be one).
pub fn GetNSItemByRangeTablePosn<'a, 'mcx>(
    pstate: &'a ParseState<'mcx>,
    varno: i32,
    sublevels_up: i32,
) -> PgResult<&'a ParseNamespaceItem<'mcx>> {
    let mut ps = pstate;
    let mut su = sublevels_up;
    while su > 0 {
        su -= 1;
        ps = ps
            .parentParseState
            .as_deref()
            .expect("GetNSItemByRangeTablePosn: pstate stack underflow");
    }
    for nsitem in ps.p_namespace.iter() {
        if nsitem.p_rtindex == varno {
            return Ok(nsitem);
        }
    }
    Err(ereport(ERROR)
        .errmsg_internal(format!("nsitem not found (internal error)"))
        .into_error())
}

/// `GetRTEByRangeTablePosn` — find the RTE with the given RT index at the given
/// nesting depth. (Need not be in the namespace.)
pub fn GetRTEByRangeTablePosn<'a, 'mcx>(
    pstate: &'a ParseState<'mcx>,
    varno: i32,
    sublevels_up: i32,
) -> &'a RangeTblEntry<'mcx> {
    let mut ps = pstate;
    let mut su = sublevels_up;
    while su > 0 {
        su -= 1;
        ps = ps
            .parentParseState
            .as_deref()
            .expect("GetRTEByRangeTablePosn: pstate stack underflow");
    }
    debug_assert!(varno > 0 && (varno as usize) <= ps.p_rtable.len());
    &ps.p_rtable[(varno - 1) as usize]
}

/// `GetCTEForRTE` — fetch the CTE (cloned) for a CTE-reference RTE.
pub fn GetCTEForRTE<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    rtelevelsup: i32,
) -> PgResult<CommonTableExpr<'mcx>> {
    debug_assert!(rte.rtekind == RTE_CTE);
    let mut levelsup = rte.ctelevelsup as i32 + rtelevelsup;
    let mut ps = pstate;
    while levelsup > 0 {
        levelsup -= 1;
        ps = match ps.parentParseState.as_deref() {
            Some(p) => p,
            None => {
                let name = rte.ctename.as_deref().unwrap_or("");
                return Err(ereport(ERROR)
                    .errmsg_internal(format!("bad levelsup for CTE \"{name}\""))
                    .into_error());
            }
        };
    }
    let ctename = rte.ctename.as_deref().unwrap_or("");
    for cte in ps.p_ctenamespace.iter() {
        if cte.ctename.as_deref() == Some(ctename) {
            return cte.clone_in(mcx);
        }
    }
    Err(ereport(ERROR)
        .errmsg_internal(format!("could not find CTE \"{ctename}\""))
        .into_error())
}

// ===========================================================================
// Fuzzy column-name matching.
// ===========================================================================

/// `FuzzyAttrMatchState` (parse_relation.c): records the best fuzzy and exact
/// column-name matches seen so far. The C struct stores `RangeTblEntry *`
/// pointers; here each match is `(depth, rte_index)` — the pstate-stack depth and
/// the RTE's index in that level's `p_rtable`.
#[derive(Clone, Debug)]
struct FuzzyAttrMatchState {
    distance: i32,
    rfirst: Option<(usize, usize)>,
    first: AttrNumber,
    rsecond: Option<(usize, usize)>,
    second: AttrNumber,
    rexact1: Option<(usize, usize)>,
    exact1: AttrNumber,
    rexact2: Option<(usize, usize)>,
    exact2: AttrNumber,
}

impl FuzzyAttrMatchState {
    fn new() -> Self {
        FuzzyAttrMatchState {
            distance: MAX_FUZZY_DISTANCE + 1,
            rfirst: None,
            first: 0,
            rsecond: None,
            second: 0,
            rexact1: None,
            exact1: 0,
            rexact2: None,
            exact2: 0,
        }
    }
}

/// `updateFuzzyAttrMatchState` — using Levenshtein distance, consider if column
/// `actual` (at `rte_ref`, attribute `attnum`) is the best fuzzy match for
/// `match_`.
fn updateFuzzyAttrMatchState(
    fuzzy_rte_penalty: i32,
    fuzzystate: &mut FuzzyAttrMatchState,
    rte_ref: (usize, usize),
    actual: &str,
    match_: &str,
    attnum: i32,
) -> PgResult<()> {
    // Bail before computing the Levenshtein distance if there's no hope.
    if fuzzy_rte_penalty > fuzzystate.distance {
        return Ok(());
    }

    // Outright reject dropped columns (empty actual names).
    if actual.is_empty() {
        return Ok(());
    }

    // Use Levenshtein to compute match distance.
    let matchlen = match_.len() as i32;
    let mut columndistance = backend_utils_adt_varlena::misc_encoding::varstr_levenshtein_less_equal(
        actual.as_bytes(),
        actual.len() as i32,
        match_.as_bytes(),
        matchlen,
        1,
        1,
        1,
        fuzzystate.distance + 1 - fuzzy_rte_penalty,
        true,
    )?;

    // If more than half the characters are different, don't treat it as a
    // match, to avoid making ridiculous suggestions.
    if columndistance > matchlen / 2 {
        return Ok(());
    }

    // From here on we can ignore the RTE-name vs column-name distance split.
    columndistance += fuzzy_rte_penalty;

    if columndistance < fuzzystate.distance {
        // Store new lowest observed distance as first/only match.
        fuzzystate.distance = columndistance;
        fuzzystate.rfirst = Some(rte_ref);
        fuzzystate.first = attnum as AttrNumber;
        fuzzystate.rsecond = None;
    } else if columndistance == fuzzystate.distance {
        if fuzzystate.rsecond.is_some() {
            // Too many matches at same distance: drop these, keep the distance.
            fuzzystate.rfirst = None;
            fuzzystate.rsecond = None;
        } else if fuzzystate.rfirst.is_some() {
            // Record as provisional second match.
            fuzzystate.rsecond = Some(rte_ref);
            fuzzystate.second = attnum as AttrNumber;
        } else {
            // rfirst is NULL → distance too high; ignore this match.
        }
    }
    Ok(())
}

// ===========================================================================
// scanRTEForColumn / scanNSItemForColumn / colNameToVar / searchRangeTableForCol.
// ===========================================================================

/// `specialAttNum` — if `attname` could be a system column, return its attnum.
fn specialAttNum(attname: &str) -> PgResult<i32> {
    match plancat_ext::system_attribute_by_name::call(attname)? {
        Some(attnum) => Ok(attnum),
        None => Ok(InvalidAttrNumber as i32),
    }
}

/// `scanRTEForColumn` — search the column names listed in `eref` of one RTE for
/// `colname`. Return the attnum (possibly negative for a system column), else
/// `InvalidAttrNumber`. Raises on ambiguity. If `fuzzystate` is `Some`, updates
/// it (with `rte_ref` recording the RTE's coordinates).
fn scanRTEForColumn<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_>,
    rte: &RangeTblEntry<'_>,
    eref: &Alias<'_>,
    colname: &str,
    location: i32,
    fuzzy_rte_penalty: i32,
    rte_ref: (usize, usize),
    mut fuzzystate: Option<&mut FuzzyAttrMatchState>,
) -> PgResult<i32> {
    let mut result: i32 = InvalidAttrNumber as i32;
    let mut attnum: i32 = 0;

    // Scan the user column names (or aliases) for a match. Complain if multiple.
    for c in eref.colnames.iter() {
        let attcolname: &str = str_val(c);
        attnum += 1;
        if attcolname == colname {
            if result != 0 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_AMBIGUOUS_COLUMN)
                    .errmsg(format!("column reference \"{colname}\" is ambiguous"))
                    .errposition(parser_errposition(pstate, location))
                    .into_error());
            }
            result = attnum;
        }

        // Update fuzzy match state, if provided.
        if let Some(fs) = fuzzystate.as_deref_mut() {
            updateFuzzyAttrMatchState(fuzzy_rte_penalty, fs, rte_ref, attcolname, colname, attnum)?;
        }
    }

    // If we have a unique match, return it (a user alias overrides a system
    // column name without error).
    if result != 0 {
        return Ok(result);
    }

    // If the RTE represents a real relation, consider system column names.
    if rte.rtekind == RTE_RELATION && rte.relkind != RELKIND_COMPOSITE_TYPE as i8 {
        // quick check to see if name could be a system column
        let sysattnum = specialAttNum(colname)?;
        if sysattnum != InvalidAttrNumber as i32 {
            // now check to see if column actually is defined
            if syscache::search_attnum_attname::call(mcx, rte.relid, sysattnum as AttrNumber)?.is_some() {
                result = sysattnum;
            }
        }
    }

    Ok(result)
}

/// `scanNSItemForColumn` — search the column names of the namespace item at
/// `nsitem_index` for `colname`. If found, build and return the appropriate `Var`
/// (wrapped in `Node`), else `None`. Side effect: marks the RTE for SELECT.
pub fn scanNSItemForColumn<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    nsitem_index: usize,
    sublevels_up: i32,
    colname: &str,
    location: i32,
) -> PgResult<Option<Node<'mcx>>> {
    // Read what we need out of the nsitem (cloning the RTE / names).
    let (rte, p_names, p_nscolumns, p_rtindex, p_returning_type, aliasname) = {
        let nsitem = &pstate.p_namespace[nsitem_index];
        (
            nsitem_rte(nsitem).clone_in(mcx)?,
            nsitem.p_names.as_deref().expect("p_names set").clone_in(mcx)?,
            clone_nscolumns(&nsitem.p_nscolumns, mcx)?,
            nsitem.p_rtindex,
            nsitem.p_returning_type,
            {
                // need an owned aliasname for the dropped-column error
                let a = nsitem_aliasname(nsitem);
                PgString::from_str_in(a, mcx)?
            },
        )
    };

    let attnum = scanRTEForColumn(mcx, pstate, &rte, &p_names, colname, location, 0, (0, 0), None)?;

    if attnum == InvalidAttrNumber as i32 {
        return Ok(None); // Return NULL if no match
    }

    // In constraint check, no system column is allowed except tableOid.
    if pstate.p_expr_kind == EXPR_KIND_CHECK_CONSTRAINT
        && attnum < InvalidAttrNumber as i32
        && attnum != TableOidAttributeNumber as i32
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg(format!(
                "system column \"{colname}\" reference in check constraint is invalid"
            ))
            .errposition(parser_errposition(pstate, location))
            .into_error());
    }

    // In generated column, no system column is allowed except tableOid.
    if pstate.p_expr_kind == EXPR_KIND_GENERATED_COLUMN
        && attnum < InvalidAttrNumber as i32
        && attnum != TableOidAttributeNumber as i32
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg(format!(
                "cannot use system column \"{colname}\" in column generation expression"
            ))
            .errposition(parser_errposition(pstate, location))
            .into_error());
    }

    // In a MERGE WHEN condition, no system column is allowed except tableOid.
    if pstate.p_expr_kind == EXPR_KIND_MERGE_WHEN
        && attnum < InvalidAttrNumber as i32
        && attnum != TableOidAttributeNumber as i32
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg(format!(
                "cannot use system column \"{colname}\" in MERGE WHEN condition"
            ))
            .errposition(parser_errposition(pstate, location))
            .into_error());
    }

    let mut var: Var;
    if attnum > InvalidAttrNumber as i32 {
        // Get attribute data from the ParseNamespaceColumn array.
        let nscol = &p_nscolumns[(attnum - 1) as usize];

        // Complain if dropped column.  See notes in scanRTEForColumn.
        if nscol.p_varno == 0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!(
                    "column \"{colname}\" of relation \"{}\" does not exist",
                    aliasname.as_str()
                ))
                .into_error());
        }

        var = make_var(
            nscol.p_varno as i32,
            nscol.p_varattno,
            nscol.p_vartype,
            nscol.p_vartypmod,
            nscol.p_varcollid,
            sublevels_up as Index,
        );
        // makeVar doesn't offer parameters for these, so set them by hand:
        var.varnosyn = nscol.p_varnosyn;
        var.varattnosyn = nscol.p_varattnosyn;
    } else {
        // System column, so use predetermined type data.
        let (atttypid, atttypmod, attcollation) =
            plancat_ext::system_attribute_definition::call(attnum)?;
        var = make_var(
            p_rtindex,
            attnum as AttrNumber,
            atttypid,
            atttypmod,
            attcollation,
            sublevels_up as Index,
        );
    }
    var.location = location;

    // Mark Var for RETURNING OLD/NEW, as necessary.
    var.varreturningtype = p_returning_type;

    // Mark Var if it's nulled by any outer joins.
    markNullableIfNeeded(pstate, &mut var)?;

    // Require read access to the column.
    markVarForSelectPriv(mcx, pstate, &var)?;

    Ok(Some(var_node(var)))
}

/// `colNameToVar` — search for an unqualified column name across the namespace.
/// Returns the `Var` node (wrapped) if found, else `None`. Raises on ambiguity.
pub fn colNameToVar<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    colname: &str,
    localonly: bool,
    location: i32,
) -> PgResult<Option<Node<'mcx>>> {
    let mut result: Option<Node<'mcx>> = None;
    let mut sublevels_up: i32 = 0;

    let max_depth = pstate_depth(pstate);
    for depth in 0..=max_depth {
        let (ns_len, lateral_active) = {
            let ps = pstate_at_depth(pstate, depth);
            (ps.p_namespace.len(), ps.p_lateral_active)
        };

        for ns_idx in 0..ns_len {
            let (cols_visible, lateral_only) = {
                let ps = pstate_at_depth(pstate, depth);
                let nsitem = &ps.p_namespace[ns_idx];
                (nsitem.p_cols_visible, nsitem.p_lateral_only)
            };
            // Ignore table-only items.
            if !cols_visible {
                continue;
            }
            // If not inside LATERAL, ignore lateral-only items.
            if lateral_only && !lateral_active {
                continue;
            }

            let newresult = scan_nsitem_for_column_at_depth(
                mcx, pstate, depth, ns_idx, sublevels_up, colname, location,
            )?;

            if newresult.is_some() {
                if result.is_some() {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_AMBIGUOUS_COLUMN)
                        .errmsg(format!("column reference \"{colname}\" is ambiguous"))
                        .errposition(parser_errposition(
                            pstate_at_depth(pstate, depth),
                            location,
                        ))
                        .into_error());
                }
                {
                    let ps = pstate_at_depth(pstate, depth);
                    check_lateral_ref_ok(ps, &ps.p_namespace[ns_idx], location)?;
                }
                result = newresult;
            }
        }

        if result.is_some() || localonly {
            break; // found, or don't want to look at parent
        }
        sublevels_up += 1;
    }

    Ok(result)
}

// --- pstate-stack navigation helpers (owned tree) -------------------------

/// Number of parent levels above `pstate` (0 if `pstate` is the outermost).
fn pstate_depth(pstate: &ParseState<'_>) -> usize {
    let mut n = 0;
    let mut cur = pstate.parentParseState.as_deref();
    while let Some(p) = cur {
        n += 1;
        cur = p.parentParseState.as_deref();
    }
    n
}

/// Borrow the pstate `depth` levels up from `pstate` (depth 0 == `pstate`).
fn pstate_at_depth<'a, 'mcx>(pstate: &'a ParseState<'mcx>, depth: usize) -> &'a ParseState<'mcx> {
    let mut cur = pstate;
    for _ in 0..depth {
        cur = cur
            .parentParseState
            .as_deref()
            .expect("pstate_at_depth: stack underflow");
    }
    cur
}

/// `scanNSItemForColumn` performed against the nsitem at `(depth, ns_idx)`. The
/// mark-for-select side effects descend by `var.varlevelsup` from `pstate`,
/// exactly as the C does.
pub fn scan_nsitem_for_column_at_depth<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    depth: usize,
    ns_idx: usize,
    sublevels_up: i32,
    colname: &str,
    location: i32,
) -> PgResult<Option<Node<'mcx>>> {
    let (rte, p_names, p_nscolumns, p_rtindex, p_returning_type, aliasname, expr_kind) = {
        let ps = pstate_at_depth(pstate, depth);
        let nsitem = &ps.p_namespace[ns_idx];
        (
            nsitem_rte(nsitem).clone_in(mcx)?,
            nsitem.p_names.as_deref().expect("p_names set").clone_in(mcx)?,
            clone_nscolumns(&nsitem.p_nscolumns, mcx)?,
            nsitem.p_rtindex,
            nsitem.p_returning_type,
            PgString::from_str_in(nsitem_aliasname(nsitem), mcx)?,
            pstate.p_expr_kind,
        )
    };

    let attnum = {
        let ps = pstate_at_depth(pstate, depth);
        scanRTEForColumn(mcx, ps, &rte, &p_names, colname, location, 0, (0, 0), None)?
    };

    if attnum == InvalidAttrNumber as i32 {
        return Ok(None);
    }

    if expr_kind == EXPR_KIND_CHECK_CONSTRAINT
        && attnum < InvalidAttrNumber as i32
        && attnum != TableOidAttributeNumber as i32
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg(format!(
                "system column \"{colname}\" reference in check constraint is invalid"
            ))
            .errposition(parser_errposition(pstate_at_depth(pstate, depth), location))
            .into_error());
    }
    if expr_kind == EXPR_KIND_GENERATED_COLUMN
        && attnum < InvalidAttrNumber as i32
        && attnum != TableOidAttributeNumber as i32
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg(format!(
                "cannot use system column \"{colname}\" in column generation expression"
            ))
            .errposition(parser_errposition(pstate_at_depth(pstate, depth), location))
            .into_error());
    }
    if expr_kind == EXPR_KIND_MERGE_WHEN
        && attnum < InvalidAttrNumber as i32
        && attnum != TableOidAttributeNumber as i32
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg(format!(
                "cannot use system column \"{colname}\" in MERGE WHEN condition"
            ))
            .errposition(parser_errposition(pstate_at_depth(pstate, depth), location))
            .into_error());
    }

    let mut var: Var;
    if attnum > InvalidAttrNumber as i32 {
        let nscol = &p_nscolumns[(attnum - 1) as usize];
        if nscol.p_varno == 0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!(
                    "column \"{colname}\" of relation \"{}\" does not exist",
                    aliasname.as_str()
                ))
                .into_error());
        }
        var = make_var(
            nscol.p_varno as i32,
            nscol.p_varattno,
            nscol.p_vartype,
            nscol.p_vartypmod,
            nscol.p_varcollid,
            sublevels_up as Index,
        );
        var.varnosyn = nscol.p_varnosyn;
        var.varattnosyn = nscol.p_varattnosyn;
    } else {
        let (atttypid, atttypmod, attcollation) =
            plancat_ext::system_attribute_definition::call(attnum)?;
        var = make_var(
            p_rtindex,
            attnum as AttrNumber,
            atttypid,
            atttypmod,
            attcollation,
            sublevels_up as Index,
        );
    }
    var.location = location;
    var.varreturningtype = p_returning_type;

    markNullableIfNeeded(pstate, &mut var)?;
    markVarForSelectPriv(mcx, pstate, &var)?;

    Ok(Some(var_node(var)))
}

/// `searchRangeTableForCol` — heuristic search over the entire rangetable(s) for
/// `colname` (optionally under alias `alias`), recording exact and fuzzy matches.
/// Used only by `errorMissingColumn`.
fn searchRangeTableForCol<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_>,
    alias: Option<&str>,
    colname: &str,
    location: i32,
) -> PgResult<FuzzyAttrMatchState> {
    let mut fuzzystate = FuzzyAttrMatchState::new();

    let max_depth = pstate_depth(pstate);
    for depth in 0..=max_depth {
        let ps = pstate_at_depth(pstate, depth);
        for (rte_index, rte) in ps.p_rtable.iter().enumerate() {
            // Typically not useful to look for matches within join RTEs.
            if rte.rtekind == RTE_JOIN {
                continue;
            }

            let eref = rte.eref.as_deref().expect("eref set");
            let mut fuzzy_rte_penalty = 0;
            if let Some(alias) = alias {
                let aliasname = rte_eref_aliasname(rte);
                fuzzy_rte_penalty = backend_utils_adt_varlena::misc_encoding::varstr_levenshtein_less_equal(
                    alias.as_bytes(),
                    alias.len() as i32,
                    aliasname.as_bytes(),
                    aliasname.len() as i32,
                    1,
                    1,
                    1,
                    MAX_FUZZY_DISTANCE + 1,
                    true,
                )?;
            }

            let attnum = scanRTEForColumn(
                mcx,
                pstate,
                rte,
                eref,
                colname,
                location,
                fuzzy_rte_penalty,
                (depth, rte_index),
                Some(&mut fuzzystate),
            )?;
            if attnum != InvalidAttrNumber as i32 && fuzzy_rte_penalty == 0 {
                if fuzzystate.rexact1.is_none() {
                    fuzzystate.rexact1 = Some((depth, rte_index));
                    fuzzystate.exact1 = attnum as AttrNumber;
                } else {
                    fuzzystate.rexact2 = Some((depth, rte_index));
                    fuzzystate.exact2 = attnum as AttrNumber;
                }
            }
        }
    }

    Ok(fuzzystate)
}

// ===========================================================================
// markNullableIfNeeded / markRTEForSelectPriv / markVarForSelectPriv.
// ===========================================================================

/// `markNullableIfNeeded` — if the RTE referenced by `var` is nullable by outer
/// join(s) at this point, set `var.varnullingrels` to show that.
pub fn markNullableIfNeeded(pstate: &ParseState<'_>, var: &mut Var) -> PgResult<()> {
    let rtindex = var.varno;

    // Find the appropriate pstate.
    let mut ps = pstate;
    for _ in 0..var.varlevelsup {
        ps = ps
            .parentParseState
            .as_deref()
            .expect("markNullableIfNeeded: pstate stack underflow");
    }

    // Find currently-relevant join relids for the Var's rel.
    if rtindex > 0 && (rtindex as usize) <= ps.p_nullingrels.len() {
        let relids = &ps.p_nullingrels[(rtindex - 1) as usize];
        // Merge with any already-declared nulling rels (bms_union). The node
        // field carries the lifetime-free word storage (`ExprRelids`); the
        // pstate's `Bitmapset` carries `PgVec<bitmapword>`. Union is a flat
        // bitwise OR over the word arrays (C's bms_union).
        let other = relids.words.as_slice();
        if !other.is_empty() {
            let n = core::cmp::max(var.varnullingrels.words.len(), other.len());
            let mut merged = alloc::vec::Vec::with_capacity(n);
            for i in 0..n {
                let a = var.varnullingrels.words.get(i).copied().unwrap_or(0);
                let b = other.get(i).copied().unwrap_or(0);
                merged.push(a | b);
            }
            // Normalize trailing zero words (bms storage invariant).
            while merged.last() == Some(&0) {
                merged.pop();
            }
            var.varnullingrels.words = merged;
        }
    }
    Ok(())
}

/// `markRTEForSelectPriv` — mark column `col` of the RTE at `rtindex` as
/// requiring SELECT privilege. `col == InvalidAttrNumber` is a whole-row ref.
fn markRTEForSelectPriv<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    rtindex: i32,
    col: AttrNumber,
) -> PgResult<()> {
    let rtekind = pstate.p_rtable[(rtindex - 1) as usize].rtekind;

    if rtekind == RTE_RELATION {
        // Make sure the rel as a whole is marked for SELECT access.
        let perminfo_index = {
            let rte = &pstate.p_rtable[(rtindex - 1) as usize];
            getRTEPermissionInfo(&pstate.p_rteperminfos, rte)?
        };
        let perminfo = &mut pstate.p_rteperminfos[perminfo_index];
        perminfo.requiredPerms |= ACL_SELECT;
        // Must offset the attnum to fit in a bitmapset.
        let existing = perminfo.selectedCols.take();
        let added = backend_nodes_core::bitmapset::bms_add_member(
            mcx,
            existing,
            col as i32 - FirstLowInvalidHeapAttributeNumber as i32,
        )?;
        perminfo.selectedCols = Some(added);
    } else if rtekind == RTE_JOIN {
        if col == InvalidAttrNumber {
            // A whole-row reference to a join has to be treated as whole-row
            // references to the two inputs.
            let (larg_varno, rarg_varno) = {
                let j = if rtindex > 0 && (rtindex as usize) <= pstate.p_joinexprs.len() {
                    pstate.p_joinexprs[(rtindex - 1) as usize].as_deref()
                } else {
                    None
                };
                let j = match j {
                    Some(j) => j,
                    None => {
                        return Err(ereport(ERROR)
                            .errmsg_internal(format!(
                                "could not find JoinExpr for whole-row reference"
                            ))
                            .into_error());
                    }
                };
                // Note: we can't see FromExpr here.
                let larg = join_input_rtindex(j.larg.as_deref())?;
                let rarg = join_input_rtindex(j.rarg.as_deref())?;
                (larg, rarg)
            };
            markRTEForSelectPriv(mcx, pstate, larg_varno, InvalidAttrNumber)?;
            markRTEForSelectPriv(mcx, pstate, rarg_varno, InvalidAttrNumber)?;
        }
        // else: join alias Vars for ordinary columns refer to merged JOIN USING
        // columns; the join input columns are also referenced in the join's qual
        // and get marked there, so nothing to do here.
    }
    // other RTE types don't require privilege marking
    Ok(())
}

/// The `IsA(j->larg, RangeTblRef) ? rtindex : IsA(j->larg, JoinExpr) ? rtindex`
/// dispatch from `markRTEForSelectPriv`'s whole-row JOIN branch.
fn join_input_rtindex(node: Option<&Node<'_>>) -> PgResult<i32> {
    match node {
        Some(Node::RangeTblRef(rtr)) => Ok(rtr.rtindex),
        Some(Node::JoinExpr(j)) => Ok(j.rtindex),
        other => Err(ereport(ERROR)
            .errmsg_internal(format!(
                "unrecognized node type: {}",
                other.map(|n| n.node_tag().0).unwrap_or(0)
            ))
            .into_error()),
    }
}

/// `markVarForSelectPriv` — mark the RTE referenced by `var` as requiring SELECT
/// privilege for the Var's column.
pub fn markVarForSelectPriv<'mcx>(mcx: Mcx<'mcx>, pstate: &mut ParseState<'mcx>, var: &Var) -> PgResult<()> {
    mark_var_for_select_priv_at(mcx, pstate, var.varlevelsup as usize, var.varno, var.varattno)
}

/// Apply `markRTEForSelectPriv` at the pstate `levelsup` levels above `pstate`.
fn mark_var_for_select_priv_at<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    levelsup: usize,
    varno: i32,
    varattno: AttrNumber,
) -> PgResult<()> {
    if levelsup == 0 {
        return markRTEForSelectPriv(mcx, pstate, varno, varattno);
    }
    match pstate.parentParseState.as_deref_mut() {
        Some(parent) => mark_var_for_select_priv_at(mcx, parent, levelsup - 1, varno, varattno),
        None => Err(ereport(ERROR)
            .errmsg_internal(format!("markVarForSelectPriv: pstate stack underflow"))
            .into_error()),
    }
}

// ===========================================================================
// buildRelationAliases / chooseScalarFunctionAlias.
// ===========================================================================

/// `buildRelationAliases` — construct the eref column name list for a relation
/// (or function) RTE. `eref->colnames` is filled; `alias->colnames` is rebuilt
/// with empty strings for dropped columns so it is 1-to-1 with physical columns.
fn buildRelationAliases<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: &TupleDescData<'mcx>,
    alias: Option<&mut Alias<'mcx>>,
    eref: &mut Alias<'mcx>,
) -> PgResult<()> {
    let maxattrs = tupdesc.attrs.len();
    debug_assert!(eref.colnames.is_empty());

    // The C iterates the user alias colnames with a cursor while rebuilding the
    // alias colname list. We snapshot the supplied aliases, then rewrite.
    let (mut alias, supplied): (Option<&mut Alias<'mcx>>, alloc::vec::Vec<NodePtr<'mcx>>) =
        match alias {
            Some(a) => {
                // Take the old colnames; we'll rebuild a->colnames below.
                let old = core::mem::replace(&mut a.colnames, PgVec::new_in(mcx));
                let snap: alloc::vec::Vec<NodePtr<'mcx>> = old.into_iter().collect();
                (Some(a), snap)
            }
            None => (None, alloc::vec::Vec::new()),
        };
    let numaliases = supplied.len();
    let mut aliasidx = 0usize;
    let mut numdropped = 0usize;

    for varattno in 0..maxattrs {
        let attr = tupdesc.attr(varattno);

        let attrname: NodePtr<'mcx>;
        if attr.attisdropped {
            // Always insert an empty string for a dropped column.
            attrname = make_string_node(mcx, "")?;
            if aliasidx < numaliases {
                if let Some(a) = alias.as_deref_mut() {
                    a.colnames.push(make_string_node(mcx, "")?);
                }
            }
            numdropped += 1;
        } else if aliasidx < numaliases {
            // Use the next user-supplied alias.
            let s = str_val(&supplied[aliasidx]);
            attrname = make_string_node(mcx, s)?;
            aliasidx += 1;
            if let Some(a) = alias.as_deref_mut() {
                a.colnames.push(make_string_node(mcx, s)?);
            }
        } else {
            attrname = make_string_node(mcx, attname_str(attr))?;
            // we're done with the alias if any
        }

        eref.colnames.push(attrname);
    }

    // Too many user-supplied aliases?
    if aliasidx < numaliases {
        let aliasname = eref.aliasname.as_deref().unwrap_or("");
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg(format!(
                "table \"{}\" has {} columns available but {} columns specified",
                aliasname,
                maxattrs - numdropped,
                numaliases
            ))
            .into_error());
    }

    Ok(())
}

/// `chooseScalarFunctionAlias` — select the column alias for a scalar function in
/// a function RTE. (Faithful static-fn port; its only caller is the function-RTE
/// adder's scalar arm, which is deferred — see `addRangeTableEntryForFunction`.)
#[allow(dead_code)]
fn chooseScalarFunctionAlias<'mcx>(
    mcx: Mcx<'mcx>,
    _funcexpr: Option<&Node<'mcx>>,
    funcname: &str,
    alias: Option<&Alias<'mcx>>,
    nfuncs: i32,
) -> PgResult<PgString<'mcx>> {
    // If the expression is a simple function call, and the function has a single
    // OUT parameter that is named, use the parameter's name. That branch needs
    // get_func_result_name(funcid), which is unported funcapi here.
    if let Some(Node::Expr(types_nodes::primnodes::Expr::FuncExpr(_))) = _funcexpr {
        panic!(
            "chooseScalarFunctionAlias: the FuncExpr OUT-parameter-name branch needs \
             get_func_result_name(funcid) (funcapi), unported here (parse_relation.c:1280)"
        );
    }

    // If there's just one function and the user gave an RTE alias name, use it.
    if nfuncs == 1 {
        if let Some(a) = alias {
            if let Some(name) = a.aliasname.as_deref() {
                return PgString::from_str_in(name, mcx);
            }
        }
    }

    // Otherwise use the function name.
    PgString::from_str_in(funcname, mcx)
}

// ===========================================================================
// buildNSItemFromTupleDesc / buildNSItemFromLists.
// ===========================================================================

/// `buildNSItemFromTupleDesc` — build a ParseNamespaceItem given a tupdesc. The
/// nsitem carries an owned copy of the RTE (`p_rte`) plus its `p_rtindex`.
fn buildNSItemFromTupleDesc<'mcx>(
    mcx: Mcx<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    rtindex: Index,
    perminfo: Option<&RTEPermissionInfo<'mcx>>,
    tupdesc: &TupleDescData<'mcx>,
) -> PgResult<ParseNamespaceItem<'mcx>> {
    let maxattrs = tupdesc.attrs.len();
    let eref = rte.eref.as_deref().expect("eref set");
    debug_assert!(maxattrs == eref.colnames.len());

    let mut nscolumns: PgVec<'mcx, ParseNamespaceColumn> = mcx::vec_with_capacity_in(mcx, maxattrs)?;
    for varattno in 0..maxattrs {
        let attr = tupdesc.attr(varattno);
        let mut nscol = ParseNamespaceColumn::default();
        // For a dropped column, just leave the entry as zeroes.
        if !attr.attisdropped {
            nscol.p_varno = rtindex;
            nscol.p_varattno = (varattno + 1) as AttrNumber;
            nscol.p_vartype = attr.atttypid;
            nscol.p_vartypmod = attr.atttypmod;
            nscol.p_varcollid = attr.attcollation;
            nscol.p_varnosyn = rtindex;
            nscol.p_varattnosyn = (varattno + 1) as AttrNumber;
        }
        nscolumns.push(nscol);
    }

    Ok(ParseNamespaceItem {
        p_names: Some(mcx::alloc_in(mcx, eref.clone_in(mcx)?)?),
        p_rte: Some(mcx::alloc_in(mcx, rte.clone_in(mcx)?)?),
        p_rtindex: rtindex as i32,
        p_perminfo: match perminfo {
            Some(p) => Some(mcx::alloc_in(mcx, p.clone_in(mcx)?)?),
            None => None,
        },
        p_nscolumns: nscolumns,
        p_rel_visible: true,
        p_cols_visible: true,
        p_lateral_only: false,
        p_lateral_ok: true,
        p_returning_type: VAR_RETURNING_DEFAULT,
    })
}

/// `buildNSItemFromLists` — build a ParseNamespaceItem from per-column type lists.
fn buildNSItemFromLists<'mcx>(
    mcx: Mcx<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    rtindex: Index,
    coltypes: &[Oid],
    coltypmods: &[i32],
    colcollations: &[Oid],
) -> PgResult<ParseNamespaceItem<'mcx>> {
    let maxattrs = coltypes.len();
    let eref = rte.eref.as_deref().expect("eref set");
    debug_assert!(maxattrs == eref.colnames.len());
    debug_assert!(maxattrs == coltypmods.len());
    debug_assert!(maxattrs == colcollations.len());

    let mut nscolumns: PgVec<'mcx, ParseNamespaceColumn> = mcx::vec_with_capacity_in(mcx, maxattrs)?;
    for varattno in 0..maxattrs {
        let mut nscol = ParseNamespaceColumn::default();
        nscol.p_varno = rtindex;
        nscol.p_varattno = (varattno + 1) as AttrNumber;
        nscol.p_vartype = coltypes[varattno];
        nscol.p_vartypmod = coltypmods[varattno];
        nscol.p_varcollid = colcollations[varattno];
        nscol.p_varnosyn = rtindex;
        nscol.p_varattnosyn = (varattno + 1) as AttrNumber;
        nscolumns.push(nscol);
    }

    Ok(ParseNamespaceItem {
        p_names: Some(mcx::alloc_in(mcx, eref.clone_in(mcx)?)?),
        p_rte: Some(mcx::alloc_in(mcx, rte.clone_in(mcx)?)?),
        p_rtindex: rtindex as i32,
        p_perminfo: None,
        p_nscolumns: nscolumns,
        p_rel_visible: true,
        p_cols_visible: true,
        p_lateral_only: false,
        p_lateral_ok: true,
        p_returning_type: VAR_RETURNING_DEFAULT,
    })
}

// ===========================================================================
// parserOpenTable.
// ===========================================================================

/// `parserOpenTable` — open a table during parse analysis, with parser-specific
/// error reporting. Returns the opened `Relation`.
pub fn parserOpenTable<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_>,
    relation: &RangeVar<'_>,
    lockmode: LOCKMODE,
) -> PgResult<types_rel::Relation<'mcx>> {
    // setup/cancel_parser_errposition_callback are no-ops in this repo's error
    // model (location tags attach on propagation).
    let rel = backend_access_table_table::table_openrv_extended(mcx, &to_access_range_var(relation), lockmode, true)?;
    match rel {
        Some(rel) => Ok(rel),
        None => {
            if relation.schemaname.is_some() {
                let sn = relation.schemaname.as_deref().unwrap_or("");
                let rn = relation.relname.as_deref().unwrap_or("");
                Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_TABLE)
                    .errmsg(format!("relation \"{sn}.{rn}\" does not exist"))
                    .into_error())
            } else {
                let rn = relation.relname.as_deref().unwrap_or("");
                if isFutureCTE(pstate, rn) {
                    Err(ereport(ERROR)
                        .errcode(ERRCODE_UNDEFINED_TABLE)
                        .errmsg(format!("relation \"{rn}\" does not exist"))
                        .errdetail(format!(
                            "There is a WITH item named \"{rn}\", but it cannot be referenced from this part of the query."
                        ))
                        .errhint(format!(
                            "Use WITH RECURSIVE, or re-order the WITH items to remove forward references."
                        ))
                        .into_error())
                } else {
                    Err(ereport(ERROR)
                        .errcode(ERRCODE_UNDEFINED_TABLE)
                        .errmsg(format!("relation \"{rn}\" does not exist"))
                        .into_error())
                }
            }
        }
    }
}

// ===========================================================================
// addRangeTableEntry* family.
// ===========================================================================

/// `addRangeTableEntry` — add a relation RTE to `p_rtable` and return its nsitem.
pub fn addRangeTableEntry<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    relation: &RangeVar<'mcx>,
    alias: Option<Alias<'mcx>>,
    inh: bool,
    in_from_cl: bool,
) -> PgResult<ParseNamespaceItem<'mcx>> {
    let refname = match alias.as_ref().and_then(|a| a.aliasname.as_deref()) {
        Some(s) => s,
        None => relation.relname.as_deref().unwrap_or(""),
    };
    let refname_owned = PgString::from_str_in(refname, mcx)?;

    // Lock level: RowShareLock if locked FOR UPDATE/SHARE, else AccessShareLock.
    let lockmode = if isLockedRefname(pstate, Some(refname)) {
        RowShareLock
    } else {
        AccessShareLock
    };

    let rel = parserOpenTable(mcx, pstate, relation, lockmode)?;

    let mut rte = RangeTblEntry::new_in(mcx);
    rte.rtekind = RTE_RELATION;
    rte.alias = match &alias {
        Some(a) => Some(mcx::alloc_in(mcx, a.clone_in(mcx)?)?),
        None => None,
    };
    rte.relid = rel.rd_id;
    rte.inh = inh;
    rte.relkind = rel.rd_rel.relkind as i8;
    rte.rellockmode = lockmode;

    // Build the eref column-name list.
    let mut eref = make_alias(mcx, refname_owned.as_str(), PgVec::new_in(mcx))?;
    let mut alias_for_build = match alias {
        Some(a) => Some(a),
        None => None,
    };
    buildRelationAliases(mcx, &rel.rd_att, alias_for_build.as_mut(), &mut eref)?;
    rte.eref = Some(mcx::alloc_in(mcx, eref)?);
    // The user alias's colnames may have been rebuilt; re-store the (rebuilt)
    // user alias on the RTE so it matches C (which stored the same pointer).
    if let Some(a) = alias_for_build {
        rte.alias = Some(mcx::alloc_in(mcx, a)?);
    }

    rte.lateral = false;
    rte.inFromCl = in_from_cl;

    // Initialize access permissions.
    let perminfo_idx = addRTEPermissionInfo(&mut pstate.p_rteperminfos, &mut rte)?;
    pstate.p_rteperminfos[perminfo_idx].requiredPerms = ACL_SELECT;
    let perminfo_snapshot = pstate.p_rteperminfos[perminfo_idx].clone_in(mcx)?;

    // Add the completed RTE.
    pstate.p_rtable.push(rte);
    let rtindex = pstate.p_rtable.len() as Index;
    let rte_ref = &pstate.p_rtable[(rtindex - 1) as usize];

    let nsitem =
        buildNSItemFromTupleDesc(mcx, rte_ref, rtindex, Some(&perminfo_snapshot), &rel.rd_att)?;

    // Drop the rel refcount, keep the lock till end of xact.
    backend_access_table_table::table_close(rel, NoLock)?;

    Ok(nsitem)
}

/// `addRangeTableEntryForRelation` — like addRangeTableEntry but from an open rel.
pub fn addRangeTableEntryForRelation<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    rel: &types_rel::RelationData<'mcx>,
    lockmode: LOCKMODE,
    alias: Option<Alias<'mcx>>,
    inh: bool,
    in_from_cl: bool,
) -> PgResult<ParseNamespaceItem<'mcx>> {
    debug_assert!(
        lockmode == AccessShareLock || lockmode == RowShareLock || lockmode == RowExclusiveLock
    );

    let refname = match alias.as_ref().and_then(|a| a.aliasname.as_deref()) {
        Some(s) => s,
        None => rel.name(),
    };
    let refname_owned = PgString::from_str_in(refname, mcx)?;

    let mut rte = RangeTblEntry::new_in(mcx);
    rte.rtekind = RTE_RELATION;
    rte.alias = match &alias {
        Some(a) => Some(mcx::alloc_in(mcx, a.clone_in(mcx)?)?),
        None => None,
    };
    rte.relid = rel.rd_id;
    rte.inh = inh;
    rte.relkind = rel.rd_rel.relkind as i8;
    rte.rellockmode = lockmode;

    let mut eref = make_alias(mcx, refname_owned.as_str(), PgVec::new_in(mcx))?;
    let mut alias_for_build = alias;
    buildRelationAliases(mcx, &rel.rd_att, alias_for_build.as_mut(), &mut eref)?;
    rte.eref = Some(mcx::alloc_in(mcx, eref)?);
    if let Some(a) = alias_for_build {
        rte.alias = Some(mcx::alloc_in(mcx, a)?);
    }

    rte.lateral = false;
    rte.inFromCl = in_from_cl;

    let perminfo_idx = addRTEPermissionInfo(&mut pstate.p_rteperminfos, &mut rte)?;
    pstate.p_rteperminfos[perminfo_idx].requiredPerms = ACL_SELECT;
    let perminfo_snapshot = pstate.p_rteperminfos[perminfo_idx].clone_in(mcx)?;

    pstate.p_rtable.push(rte);
    let rtindex = pstate.p_rtable.len() as Index;
    let rte_ref = &pstate.p_rtable[(rtindex - 1) as usize];

    buildNSItemFromTupleDesc(mcx, rte_ref, rtindex, Some(&perminfo_snapshot), &rel.rd_att)
}

/// `addRangeTableEntryForSubquery` — make a subquery RTE.
pub fn addRangeTableEntryForSubquery<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    subquery: Query<'mcx>,
    alias: Option<Alias<'mcx>>,
    lateral: bool,
    in_from_cl: bool,
) -> PgResult<ParseNamespaceItem<'mcx>> {
    let mut rte = RangeTblEntry::new_in(mcx);
    rte.rtekind = RTE_SUBQUERY;

    // eref = alias ? copyObject(alias) : makeAlias("unnamed_subquery", NIL)
    let mut eref = match &alias {
        Some(a) => a.clone_in(mcx)?,
        None => make_alias(mcx, "unnamed_subquery", PgVec::new_in(mcx))?,
    };
    let numaliases = eref.colnames.len();

    // Fill unspecified alias columns and extract column type info.
    let mut coltypes: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let mut coltypmods: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    let mut colcollations: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let mut varattno: usize = 0;
    for te in subquery.targetList.iter() {
        if te.resjunk {
            continue;
        }
        varattno += 1;
        debug_assert!(varattno as AttrNumber == te.resno);
        if varattno > numaliases {
            let attrname = te.resname.as_deref().unwrap_or("");
            eref.colnames.push(make_string_node(mcx, attrname)?);
        }
        let expr = te.expr.as_deref();
        coltypes.push(backend_nodes_core::nodefuncs::expr_type(expr)?);
        coltypmods.push(backend_nodes_core::nodefuncs::expr_typmod(expr)?);
        colcollations.push(backend_nodes_core::nodefuncs::expr_collation(expr)?);
    }
    if varattno < numaliases {
        let aliasname = eref.aliasname.as_deref().unwrap_or("");
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg(format!(
                "table \"{aliasname}\" has {varattno} columns available but {numaliases} columns specified"
            ))
            .into_error());
    }

    rte.subquery = Some(mcx::alloc_in(mcx, subquery)?);
    rte.alias = match &alias {
        Some(a) => Some(mcx::alloc_in(mcx, a.clone_in(mcx)?)?),
        None => None,
    };
    rte.eref = Some(mcx::alloc_in(mcx, eref)?);
    rte.lateral = lateral;
    rte.inFromCl = in_from_cl;

    pstate.p_rtable.push(rte);
    let rtindex = pstate.p_rtable.len() as Index;
    let rte_ref = &pstate.p_rtable[(rtindex - 1) as usize];

    let mut nsitem =
        buildNSItemFromLists(mcx, rte_ref, rtindex, &coltypes, &coltypmods, &colcollations)?;

    // Visible as a relation name only if it had a user-written alias.
    nsitem.p_rel_visible = alias.is_some();

    Ok(nsitem)
}

/// `addRangeTableEntryForFunction` — make a function RTE.
///
/// The composite/RECORD/scalar tupdesc machinery needs `get_expr_result_type`,
/// `CreateTemplateTupleDesc`, `TupleDescInitEntry`, `typenameTypeIdAndMod`,
/// `GetColumnDefCollation`, `CheckAttributeNamesTypes`, `format_type_be` — all
/// unported funcapi/parse_type here. Mirror-PG-and-panic (matching
/// src-idiomatic) once we reach the per-function type resolution.
pub fn addRangeTableEntryForFunction<'mcx>(
    _mcx: Mcx<'mcx>,
    _pstate: &mut ParseState<'mcx>,
    _funcnames: &[PgString<'mcx>],
    _funcexprs: &[NodePtr<'mcx>],
    _coldeflists: &[PgVec<'mcx, NodePtr<'mcx>>],
    _rangefunc: &RangeFunction<'mcx>,
    _lateral: bool,
    _in_from_cl: bool,
) -> PgResult<ParseNamespaceItem<'mcx>> {
    panic!(
        "addRangeTableEntryForFunction: the per-function type resolution \
         (get_expr_result_type / CreateTemplateTupleDesc / TupleDescInitEntry / \
         typenameTypeIdAndMod / GetColumnDefCollation / CheckAttributeNamesTypes / \
         format_type_be) needs the unported funcapi + parse_type owners \
         (parse_relation.c:1751)"
    )
}

/// `addRangeTableEntryForTableFunc` — make a tablefunc RTE.
///
/// The RTE's `tablefunc` field (`Option<NodePtr>`) has no `Node::TableFunc`
/// central-enum variant to carry the `TableFunc` value, so the constructed RTE
/// cannot store the table function faithfully. Mirror-PG-and-panic until the
/// central `Node` enum gains a `TableFunc` arm (this is the same modeling gap
/// that blocks the VALUES adder's row lists).
pub fn addRangeTableEntryForTableFunc<'mcx>(
    _mcx: Mcx<'mcx>,
    _pstate: &mut ParseState<'mcx>,
    _tf: TableFunc<'mcx>,
    _alias: Option<Alias<'mcx>>,
    _lateral: bool,
    _in_from_cl: bool,
) -> PgResult<ParseNamespaceItem<'mcx>> {
    panic!(
        "addRangeTableEntryForTableFunc: RTE.tablefunc (Option<NodePtr>) has no \
         Node::TableFunc central-enum variant to carry the TableFunc value \
         (parse_relation.c:2065)"
    )
}

/// `addRangeTableEntryForValues` — make a values RTE.
///
/// `exprs` is a list of rows; each row is a `Node::List` of the row's column
/// expressions (carried in `RTE.values_lists`).
pub fn addRangeTableEntryForValues<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    exprs: PgVec<'mcx, NodePtr<'mcx>>,
    coltypes: PgVec<'mcx, Oid>,
    coltypmods: PgVec<'mcx, i32>,
    colcollations: PgVec<'mcx, Oid>,
    alias: Option<Alias<'mcx>>,
    lateral: bool,
    in_from_cl: bool,
) -> PgResult<ParseNamespaceItem<'mcx>> {
    let refname: &str = match &alias {
        Some(a) => a.aliasname.as_deref().unwrap_or("*VALUES*"),
        None => "*VALUES*",
    };

    let mut rte = RangeTblEntry::new_in(mcx);
    rte.rtekind = RTE_VALUES;

    // eref = alias ? copyObject(alias) : makeAlias(refname, NIL)
    let mut eref = match &alias {
        Some(a) => a.clone_in(mcx)?,
        None => make_alias(mcx, refname, PgVec::new_in(mcx))?,
    };

    // numcolumns = list_length(linitial(exprs))
    let numcolumns = match exprs.first().map(|n| n.as_ref()) {
        Some(Node::List(items)) => items.len(),
        _ => {
            return Err(ereport(ERROR)
                .errmsg(format!("VALUES exprs first row is not a List"))
                .into_error())
        }
    };
    let mut numaliases = eref.colnames.len();
    while numaliases < numcolumns {
        numaliases += 1;
        let attrname = format!("column{numaliases}");
        eref.colnames.push(make_string_node(mcx, attrname.as_str())?);
    }
    if numcolumns < numaliases {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg(format!(
                "VALUES lists \"{refname}\" have {numcolumns} columns available but {numaliases} columns specified"
            ))
            .into_error());
    }

    rte.values_lists = exprs;
    rte.coltypes = coltypes;
    rte.coltypmods = coltypmods;
    rte.colcollations = colcollations;
    rte.alias = match &alias {
        Some(a) => Some(mcx::alloc_in(mcx, a.clone_in(mcx)?)?),
        None => None,
    };
    rte.eref = Some(mcx::alloc_in(mcx, eref)?);
    rte.lateral = lateral;
    rte.inFromCl = in_from_cl;

    // Snapshot the column lists for buildNSItemFromLists.
    let coltypes_snap: PgVec<'mcx, Oid> = {
        let mut v = mcx::vec_with_capacity_in(mcx, rte.coltypes.len())?;
        for t in rte.coltypes.iter() { v.push(*t); }
        v
    };
    let coltypmods_snap: PgVec<'mcx, i32> = {
        let mut v = mcx::vec_with_capacity_in(mcx, rte.coltypmods.len())?;
        for t in rte.coltypmods.iter() { v.push(*t); }
        v
    };
    let colcollations_snap: PgVec<'mcx, Oid> = {
        let mut v = mcx::vec_with_capacity_in(mcx, rte.colcollations.len())?;
        for t in rte.colcollations.iter() { v.push(*t); }
        v
    };

    pstate.p_rtable.push(rte);
    let rtindex = pstate.p_rtable.len() as Index;
    let rte_ref = &pstate.p_rtable[(rtindex - 1) as usize];

    buildNSItemFromLists(
        mcx,
        rte_ref,
        rtindex,
        &coltypes_snap,
        &coltypmods_snap,
        &colcollations_snap,
    )
}

/// `addRangeTableEntryForJoin` — make a join RTE. The caller supplies the
/// `ParseNamespaceColumn` array.
pub fn addRangeTableEntryForJoin<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    colnames: &[NodePtr<'mcx>],
    nscolumns: PgVec<'mcx, ParseNamespaceColumn>,
    jointype: types_nodes::jointype::JoinType,
    nummergedcols: i32,
    aliasvars: PgVec<'mcx, NodePtr<'mcx>>,
    leftcols: PgVec<'mcx, i32>,
    rightcols: PgVec<'mcx, i32>,
    join_using_alias: Option<Alias<'mcx>>,
    alias: Option<Alias<'mcx>>,
    in_from_cl: bool,
) -> PgResult<ParseNamespaceItem<'mcx>> {
    // Fail if join has too many columns.
    if aliasvars.len() > MaxAttrNumber as usize {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(format!("joins can have at most {MaxAttrNumber} columns"))
            .into_error());
    }

    let mut rte = RangeTblEntry::new_in(mcx);
    rte.rtekind = RTE_JOIN;
    rte.jointype = jointype;
    rte.joinmergedcols = nummergedcols;
    rte.joinaliasvars = aliasvars;
    rte.joinleftcols = leftcols;
    rte.joinrightcols = rightcols;
    rte.join_using_alias = match &join_using_alias {
        Some(a) => Some(mcx::alloc_in(mcx, a.clone_in(mcx)?)?),
        None => None,
    };
    rte.alias = match &alias {
        Some(a) => Some(mcx::alloc_in(mcx, a.clone_in(mcx)?)?),
        None => None,
    };

    let mut eref = match &alias {
        Some(a) => a.clone_in(mcx)?,
        None => make_alias(mcx, "unnamed_join", PgVec::new_in(mcx))?,
    };
    let numaliases = eref.colnames.len();

    if numaliases < colnames.len() {
        for c in colnames[numaliases..].iter() {
            eref.colnames.push(make_string_node(mcx, str_val(c))?);
        }
    }
    if numaliases > colnames.len() {
        let aliasname = eref.aliasname.as_deref().unwrap_or("");
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg(format!(
                "join expression \"{}\" has {} columns available but {} columns specified",
                aliasname,
                colnames.len(),
                numaliases
            ))
            .into_error());
    }

    rte.eref = Some(mcx::alloc_in(mcx, eref)?);
    rte.lateral = false;
    rte.inFromCl = in_from_cl;

    pstate.p_rtable.push(rte);
    let rtindex = pstate.p_rtable.len() as Index;
    let rte_ref = &pstate.p_rtable[(rtindex - 1) as usize];
    let eref_clone = rte_ref.eref.as_deref().expect("eref set").clone_in(mcx)?;

    Ok(ParseNamespaceItem {
        p_names: Some(mcx::alloc_in(mcx, eref_clone)?),
        p_rte: Some(mcx::alloc_in(mcx, rte_ref.clone_in(mcx)?)?),
        p_perminfo: None,
        p_rtindex: rtindex as i32,
        p_nscolumns: nscolumns,
        p_rel_visible: true,
        p_cols_visible: true,
        p_lateral_only: false,
        p_lateral_ok: true,
        p_returning_type: VAR_RETURNING_DEFAULT,
    })
}

/// `addRangeTableEntryForCTE` — make a CTE RTE.
pub fn addRangeTableEntryForCTE<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    cte: &mut CommonTableExpr<'mcx>,
    levelsup: Index,
    rv: &RangeVar<'mcx>,
    in_from_cl: bool,
) -> PgResult<ParseNamespaceItem<'mcx>> {
    let alias = rv.alias.as_deref();
    let refname = match alias.and_then(|a| a.aliasname.as_deref()) {
        Some(s) => s,
        None => cte.ctename.as_deref().unwrap_or(""),
    };
    let refname_owned = PgString::from_str_in(refname, mcx)?;

    let mut rte = RangeTblEntry::new_in(mcx);
    rte.rtekind = RTE_CTE;
    rte.ctename = match &cte.ctename {
        Some(s) => Some(s.clone_in(mcx)?),
        None => None,
    };
    rte.ctelevelsup = levelsup;

    // Self-reference iff CTE's parse analysis isn't completed.
    rte.self_reference = !matches!(cte.ctequery.as_deref(), Some(Node::Query(_)));
    debug_assert!(cte.cterecursive || !rte.self_reference);
    if !rte.self_reference {
        cte.cterefcount += 1;
    }

    // Error if the CTE is data-modifying without RETURNING.
    if let Some(Node::Query(ctequery)) = cte.ctequery.as_deref() {
        if ctequery.commandType != CMD_SELECT && ctequery.returningList.is_empty() {
            let name = cte.ctename.as_deref().unwrap_or("");
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "WITH query \"{name}\" does not have a RETURNING clause"
                ))
                .errposition(parser_errposition(pstate, rv.location))
                .into_error());
        }
    }

    rte.coltypes = mcx::slice_in(mcx, &cte.ctecoltypes)?;
    rte.coltypmods = mcx::slice_in(mcx, &cte.ctecoltypmods)?;
    rte.colcollations = mcx::slice_in(mcx, &cte.ctecolcollations)?;

    rte.alias = match alias {
        Some(a) => Some(mcx::alloc_in(mcx, a.clone_in(mcx)?)?),
        None => None,
    };
    let mut eref = match alias {
        Some(a) => a.clone_in(mcx)?,
        None => make_alias(mcx, refname_owned.as_str(), PgVec::new_in(mcx))?,
    };
    let numaliases = eref.colnames.len();

    // Fill in any unspecified alias columns from cte->ctecolnames.
    let mut varattno = 0usize;
    for lc in cte.ctecolnames.iter() {
        varattno += 1;
        if varattno > numaliases {
            eref.colnames.push(mcx::alloc_in(mcx, lc.clone_in(mcx)?)?);
        }
    }
    if varattno < numaliases {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg(format!(
                "table \"{refname}\" has {varattno} columns available but {numaliases} columns specified"
            ))
            .into_error());
    }

    rte.eref = Some(mcx::alloc_in(mcx, eref)?);

    let mut n_dontexpand_columns = 0usize;

    if let Some(sc) = cte.search_clause.as_deref() {
        let eref_mut = rte.eref.as_deref_mut().unwrap();
        let col = sc.search_seq_column.as_deref().unwrap_or("");
        eref_mut.colnames.push(make_string_node(mcx, col)?);
        if sc.search_breadth_first {
            rte.coltypes.push(RECORDOID);
        } else {
            rte.coltypes.push(RECORDARRAYOID);
        }
        rte.coltypmods.push(-1);
        rte.colcollations.push(InvalidOid);
        n_dontexpand_columns += 1;
    }

    if let Some(cc) = cte.cycle_clause.as_deref().and_then(|n| n.as_cte_cycle_clause()) {
        let eref_mut = rte.eref.as_deref_mut().unwrap();
        let mark_col = cc.cycle_mark_column.as_deref().unwrap_or("");
        eref_mut.colnames.push(make_string_node(mcx, mark_col)?);
        rte.coltypes.push(cc.cycle_mark_type);
        rte.coltypmods.push(cc.cycle_mark_typmod);
        rte.colcollations.push(cc.cycle_mark_collation);

        let eref_mut = rte.eref.as_deref_mut().unwrap();
        let path_col = cc.cycle_path_column.as_deref().unwrap_or("");
        eref_mut.colnames.push(make_string_node(mcx, path_col)?);
        rte.coltypes.push(RECORDARRAYOID);
        rte.coltypmods.push(-1);
        rte.colcollations.push(InvalidOid);

        n_dontexpand_columns += 2;
    }

    rte.lateral = false;
    rte.inFromCl = in_from_cl;

    // Snapshot the column lists for buildNSItemFromLists (matches C reading the
    // appended lists).
    let coltypes_copy = mcx::slice_in(mcx, &rte.coltypes)?;
    let coltypmods_copy = mcx::slice_in(mcx, &rte.coltypmods)?;
    let colcollations_copy = mcx::slice_in(mcx, &rte.colcollations)?;
    let ctelevelsup = rte.ctelevelsup;

    pstate.p_rtable.push(rte);
    let rtindex = pstate.p_rtable.len() as Index;
    let rte_ref = &pstate.p_rtable[(rtindex - 1) as usize];

    let mut psi = buildNSItemFromLists(
        mcx,
        rte_ref,
        rtindex,
        &coltypes_copy,
        &coltypmods_copy,
        &colcollations_copy,
    )?;

    // The columns added by search and cycle clauses are not included in star
    // expansion in queries contained in the CTE.
    if ctelevelsup > 0 {
        let total = psi.p_names.as_deref().map(|a| a.colnames.len()).unwrap_or(0);
        for i in 0..n_dontexpand_columns {
            let idx = total - 1 - i;
            psi.p_nscolumns[idx].p_dontexpand = true;
        }
    }

    Ok(psi)
}

/// `addRangeTableEntryForENR` — make an RTE for an ephemeral named relation.
pub fn addRangeTableEntryForENR<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    rv: &RangeVar<'mcx>,
    in_from_cl: bool,
) -> PgResult<ParseNamespaceItem<'mcx>> {
    let alias = rv.alias.as_deref();
    let refname = match alias.and_then(|a| a.aliasname.as_deref()) {
        Some(s) => s,
        None => rv.relname.as_deref().unwrap_or(""),
    };
    let refname_owned = PgString::from_str_in(refname, mcx)?;
    let rv_relname = rv.relname.as_deref().unwrap_or("");

    // get_visible_ENR + read its metadata, then build a fully owned tupdesc so
    // we no longer borrow `pstate` before mutating its rangetable below.
    let (enrtype, reliddesc, enrname, enrtuples, tupdesc_owned) = {
        let enrmd = backend_parser_small1::get_visible_ENR(pstate, rv_relname)
            .expect("addRangeTableEntryForENR: ENR must be visible");
        let td = backend_utils_misc_queryenvironment::ENRMetadataGetTupDesc(mcx, enrmd)?
            .expect("ENRMetadataGetTupDesc returns Some");
        let td_owned: TupleDescData = (*td).clone_in(mcx)?;
        (
            enrmd.enrtype,
            enrmd.reliddesc,
            PgString::from_str_in(enrmd.name.as_deref().unwrap_or(""), mcx)?,
            enrmd.enrtuples,
            td_owned,
        )
    };
    let tupdesc: &TupleDescData = &tupdesc_owned;

    let mut rte = RangeTblEntry::new_in(mcx);
    match enrtype {
        t if t == ENR_NAMED_TUPLESTORE => {
            rte.rtekind = RTE_NAMEDTUPLESTORE;
        }
        other => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("unexpected enrtype: {}", other as i32))
                .into_error());
        }
    }

    rte.relid = reliddesc;

    // Build the eref column-name list.
    let mut eref = make_alias(mcx, refname_owned.as_str(), PgVec::new_in(mcx))?;
    let mut alias_for_build = match alias {
        Some(a) => Some(a.clone_in(mcx)?),
        None => None,
    };
    buildRelationAliases(mcx, tupdesc, alias_for_build.as_mut(), &mut eref)?;
    rte.eref = Some(mcx::alloc_in(mcx, eref)?);

    // Record additional data for ENR, including column type info.
    rte.enrname = Some(enrname);
    rte.enrtuples = enrtuples;
    for attno in 1..=tupdesc.attrs.len() {
        let att = tupdesc.attr(attno - 1);
        if att.attisdropped {
            rte.coltypes.push(InvalidOid);
            rte.coltypmods.push(0);
            rte.colcollations.push(InvalidOid);
        } else {
            if att.atttypid == InvalidOid {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!(
                        "atttypid is invalid for non-dropped column in \"{rv_relname}\""
                    ))
                    .into_error());
            }
            rte.coltypes.push(att.atttypid);
            rte.coltypmods.push(att.atttypmod);
            rte.colcollations.push(att.attcollation);
        }
    }

    rte.lateral = false;
    rte.inFromCl = in_from_cl;

    pstate.p_rtable.push(rte);
    let rtindex = pstate.p_rtable.len() as Index;
    let rte_ref = &pstate.p_rtable[(rtindex - 1) as usize];

    buildNSItemFromTupleDesc(mcx, rte_ref, rtindex, None, tupdesc)
}

/// `addRangeTableEntryForGroup` — make a GROUP RTE.
pub fn addRangeTableEntryForGroup<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    group_clauses: &[types_nodes::primnodes::TargetEntry<'mcx>],
) -> PgResult<ParseNamespaceItem<'mcx>> {
    let mut rte = RangeTblEntry::new_in(mcx);
    rte.rtekind = RTE_GROUP;

    let mut eref = make_alias(mcx, "*GROUP*", PgVec::new_in(mcx))?;

    let mut groupexprs: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    let mut coltypes: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let mut coltypmods: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    let mut colcollations: PgVec<'mcx, Oid> = PgVec::new_in(mcx);

    for te in group_clauses.iter() {
        let colname = te.resname.as_deref().unwrap_or("?column?");
        eref.colnames.push(make_string_node(mcx, colname)?);

        // groupexprs = lappend(groupexprs, copyObject(te->expr))
        let expr = te.expr.as_deref();
        let expr_node = match expr {
            Some(e) => Node::Expr(e.clone()),
            None => panic!("addRangeTableEntryForGroup: group clause has no expr"),
        };
        groupexprs.push(mcx::alloc_in(mcx, expr_node)?);

        coltypes.push(backend_nodes_core::nodefuncs::expr_type(expr)?);
        coltypmods.push(backend_nodes_core::nodefuncs::expr_typmod(expr)?);
        colcollations.push(backend_nodes_core::nodefuncs::expr_collation(expr)?);
    }

    rte.eref = Some(mcx::alloc_in(mcx, eref)?);
    rte.groupexprs = groupexprs;
    rte.lateral = false;
    rte.inFromCl = false;

    pstate.p_rtable.push(rte);
    let rtindex = pstate.p_rtable.len() as Index;
    let rte_ref = &pstate.p_rtable[(rtindex - 1) as usize];

    buildNSItemFromLists(mcx, rte_ref, rtindex, &coltypes, &coltypmods, &colcollations)
}

// ===========================================================================
// isLockedRefname / addNSItemToQuery.
// ===========================================================================

/// `isLockedRefname` — has `refname` been selected FOR UPDATE/SHARE?
pub fn isLockedRefname(pstate: &ParseState<'_>, refname: Option<&str>) -> bool {
    // Locked from parent ⇒ treat as a generic FOR UPDATE here.
    if pstate.p_locked_from_parent {
        return true;
    }

    // The C iterates pstate->p_locking_clause (a list of LockingClause nodes),
    // reading each lc->lockedRels. The LockingClause node type is not yet
    // modeled in this repo's central Node enum (rawnodes.rs carries it only as a
    // comment), so the per-clause loop can't be ported faithfully. An empty
    // p_locking_clause (no FOR UPDATE/SHARE) is handled correctly (returns
    // false); only a populated list — which can't be produced until the
    // parse_clause owner that builds LockingClause nodes lands — would require
    // it, so mirror-PG-and-panic there.
    if !pstate.p_locking_clause.is_empty() {
        let _ = refname;
        panic!(
            "isLockedRefname: iterating pstate.p_locking_clause needs the LockingClause \
             node (lockedRels list), not yet modeled in the central Node enum \
             (parse_relation.c:2677)"
        );
    }
    false
}

/// `addNSItemToQuery` — add the nsitem/RTE as a top-level entry in the pstate's
/// join list and/or namespace list.
pub fn addNSItemToQuery<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    mut nsitem: ParseNamespaceItem<'mcx>,
    add_to_join_list: bool,
    add_to_rel_namespace: bool,
    add_to_var_namespace: bool,
) -> PgResult<()> {
    if add_to_join_list {
        let rtr = Node::RangeTblRef(RangeTblRef {
            rtindex: nsitem.p_rtindex,
        });
        pstate.p_joinlist.push(mcx::alloc_in(mcx, rtr)?);
    }
    if add_to_rel_namespace || add_to_var_namespace {
        nsitem.p_rel_visible = add_to_rel_namespace;
        nsitem.p_cols_visible = add_to_var_namespace;
        nsitem.p_lateral_only = false;
        nsitem.p_lateral_ok = true;
        pstate.p_namespace.push(nsitem);
    }
    Ok(())
}

// ===========================================================================
// expandRTE / expandRelation / expandTupleDesc.
// ===========================================================================

/// `expandRTE` — expand the columns of a rangetable entry into name and/or Var
/// lists. Pass `None` for an output list that's not wanted.
pub fn expandRTE<'mcx>(
    mcx: Mcx<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    rtindex: i32,
    sublevels_up: i32,
    returning_type: VarReturningType,
    location: i32,
    include_dropped: bool,
    mut colnames: Option<&mut PgVec<'mcx, NodePtr<'mcx>>>,
    mut colvars: Option<&mut PgVec<'mcx, NodePtr<'mcx>>>,
) -> PgResult<()> {
    match rte.rtekind {
        RTE_RELATION => {
            expandRelation(
                mcx,
                rte.relid,
                rte.eref.as_deref().expect("eref set"),
                rtindex,
                sublevels_up,
                returning_type,
                location,
                include_dropped,
                colnames,
                colvars,
            )?;
        }
        RTE_SUBQUERY => {
            let eref = rte.eref.as_deref().expect("eref set");
            let mut aliasp_idx = 0usize;
            let subquery = rte.subquery.as_deref().expect("subquery set");
            let mut varattno = 0i32;
            for te in subquery.targetList.iter() {
                if te.resjunk {
                    continue;
                }
                varattno += 1;
                debug_assert!(varattno as AttrNumber == te.resno);

                if aliasp_idx >= eref.colnames.len() {
                    let an = eref.aliasname.as_deref().unwrap_or("");
                    return Err(ereport(ERROR)
                        .errmsg_internal(format!("too few column names for subquery {an}"))
                        .into_error());
                }

                if let Some(cn) = colnames.as_deref_mut() {
                    let label = str_val(&eref.colnames[aliasp_idx]);
                    cn.push(make_string_node(mcx, label)?);
                }
                if let Some(cv) = colvars.as_deref_mut() {
                    let expr = te.expr.as_deref();
                    let mut varnode = make_var(
                        rtindex,
                        varattno as AttrNumber,
                        backend_nodes_core::nodefuncs::expr_type(expr)?,
                        backend_nodes_core::nodefuncs::expr_typmod(expr)?,
                        backend_nodes_core::nodefuncs::expr_collation(expr)?,
                        sublevels_up as Index,
                    );
                    varnode.varreturningtype = returning_type;
                    varnode.location = location;
                    cv.push(mcx::alloc_in(mcx, var_node(varnode))?);
                }
                aliasp_idx += 1;
            }
        }
        RTE_FUNCTION => {
            // The RTE_FUNCTION expansion needs get_expr_result_type /
            // expandTupleDesc over a funcapi tupdesc — unported funcapi here.
            panic!(
                "expandRTE RTE_FUNCTION arm needs get_expr_result_type + the funcapi \
                 tupdesc expansion (unported here) (parse_relation.c:2825)"
            );
        }
        RTE_JOIN => {
            let eref = rte.eref.as_deref().expect("eref set");
            debug_assert!(eref.colnames.len() == rte.joinaliasvars.len());
            let mut varattno = 0i32;
            for (colname_node, aliasvar) in eref.colnames.iter().zip(rte.joinaliasvars.iter()) {
                varattno += 1;
                let avar = aliasvar.as_ref();

                // During ordinary parsing there are never deleted columns in the
                // join. This dead-code arm handles a NULL aliasvar for safety; in
                // the owned model a NULL pointer is a `Node::Null`-like marker we
                // don't have, so we only treat the C NULL case via include_dropped
                // when the aliasvar is an explicit null Const is not applicable —
                // expandRTE is not called on JOIN RTEs during parsing.
                let _ = avar;

                if let Some(cn) = colnames.as_deref_mut() {
                    let label = str_val(colname_node);
                    cn.push(make_string_node(mcx, label)?);
                }
                if let Some(cv) = colvars.as_deref_mut() {
                    let varnode = if let Some(v) = avar.as_var() {
                        // copyObject + adjust varlevelsup/location.
                        let mut nv = v.clone();
                        nv.varlevelsup = sublevels_up as Index;
                        nv.varreturningtype = returning_type;
                        nv.location = location;
                        var_node(nv)
                    } else {
                        let oe = avar.as_expr();
                        let mut nv = make_var(
                            rtindex,
                            varattno as AttrNumber,
                            backend_nodes_core::nodefuncs::expr_type(oe)?,
                            backend_nodes_core::nodefuncs::expr_typmod(oe)?,
                            backend_nodes_core::nodefuncs::expr_collation(oe)?,
                            sublevels_up as Index,
                        );
                        nv.varreturningtype = returning_type;
                        nv.location = location;
                        var_node(nv)
                    };
                    cv.push(mcx::alloc_in(mcx, varnode)?);
                }
            }
        }
        RTE_TABLEFUNC | RTE_VALUES | RTE_CTE | RTE_NAMEDTUPLESTORE => {
            let eref = rte.eref.as_deref().expect("eref set");
            let mut aliasp_idx = 0usize;
            let mut varattno = 0i32;
            for i in 0..rte.coltypes.len() {
                let coltype = rte.coltypes[i];
                let coltypmod = rte.coltypmods[i];
                let colcoll = rte.colcollations[i];
                varattno += 1;

                if let Some(cn) = colnames.as_deref_mut() {
                    // Assume there is one alias per output column.
                    if OidIsValid(coltype) {
                        let label = str_val(&eref.colnames[aliasp_idx]);
                        cn.push(make_string_node(mcx, label)?);
                    } else if include_dropped {
                        cn.push(make_string_node(mcx, "")?);
                    }
                    aliasp_idx += 1;
                }

                if let Some(cv) = colvars.as_deref_mut() {
                    if OidIsValid(coltype) {
                        let mut varnode = make_var(
                            rtindex,
                            varattno as AttrNumber,
                            coltype,
                            coltypmod,
                            colcoll,
                            sublevels_up as Index,
                        );
                        varnode.varreturningtype = returning_type;
                        varnode.location = location;
                        cv.push(mcx::alloc_in(mcx, var_node(varnode))?);
                    } else if include_dropped {
                        let nc = make_null_const(mcx, INT4OID, -1, InvalidOid)?;
                        cv.push(mcx::alloc_in(mcx, Node::Expr(types_nodes::primnodes::Expr::Const(nc)))?);
                    }
                }
            }
        }
        RTE_RESULT | RTE_GROUP => {
            // These expose no columns, so nothing to do.
        }
        other => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("unrecognized RTE kind: {}", other as i32))
                .into_error());
        }
    }
    Ok(())
}

/// `expandRelation` — expandRTE subroutine: open the rel and expand its tupdesc.
fn expandRelation<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    eref: &Alias<'mcx>,
    rtindex: i32,
    sublevels_up: i32,
    returning_type: VarReturningType,
    location: i32,
    include_dropped: bool,
    colnames: Option<&mut PgVec<'mcx, NodePtr<'mcx>>>,
    colvars: Option<&mut PgVec<'mcx, NodePtr<'mcx>>>,
) -> PgResult<()> {
    let rel = backend_access_table_table::table_open(mcx, relid, AccessShareLock)?;
    let natts = rel.rd_att.attrs.len() as i32;
    expandTupleDesc(
        mcx,
        &rel.rd_att,
        eref,
        natts,
        0,
        rtindex,
        sublevels_up,
        returning_type,
        location,
        include_dropped,
        colnames,
        colvars,
    )?;
    backend_access_table_table::table_close(rel, AccessShareLock)?;
    Ok(())
}

/// `expandTupleDesc` — generate names/Vars for the first `count` attributes of
/// the tupdesc (offset into eref->colnames and the varattno by `offset`).
fn expandTupleDesc<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: &TupleDescData<'mcx>,
    eref: &Alias<'mcx>,
    count: i32,
    offset: i32,
    rtindex: i32,
    sublevels_up: i32,
    returning_type: VarReturningType,
    location: i32,
    include_dropped: bool,
    mut colnames: Option<&mut PgVec<'mcx, NodePtr<'mcx>>>,
    mut colvars: Option<&mut PgVec<'mcx, NodePtr<'mcx>>>,
) -> PgResult<()> {
    let mut aliasidx: Option<usize> = if (offset as usize) < eref.colnames.len() {
        Some(offset as usize)
    } else {
        None
    };

    debug_assert!(count <= tupdesc.attrs.len() as i32);
    for varattno in 0..count {
        let attr = tupdesc.attr(varattno as usize);

        if attr.attisdropped {
            if include_dropped {
                if let Some(cn) = colnames.as_deref_mut() {
                    cn.push(make_string_node(mcx, "")?);
                }
                if let Some(cv) = colvars.as_deref_mut() {
                    let nc = make_null_const(mcx, INT4OID, -1, InvalidOid)?;
                    cv.push(mcx::alloc_in(mcx, Node::Expr(types_nodes::primnodes::Expr::Const(nc)))?);
                }
            }
            if let Some(ai) = aliasidx {
                aliasidx = if ai + 1 < eref.colnames.len() {
                    Some(ai + 1)
                } else {
                    None
                };
            }
            continue;
        }

        if let Some(cn) = colnames.as_deref_mut() {
            let label = if let Some(ai) = aliasidx {
                str_val(&eref.colnames[ai])
            } else {
                attname_str(attr)
            };
            cn.push(make_string_node(mcx, label)?);
        }
        if let Some(ai) = aliasidx {
            aliasidx = if ai + 1 < eref.colnames.len() {
                Some(ai + 1)
            } else {
                None
            };
        }

        if let Some(cv) = colvars.as_deref_mut() {
            let mut varnode = make_var(
                rtindex,
                (varattno + offset + 1) as AttrNumber,
                attr.atttypid,
                attr.atttypmod,
                attr.attcollation,
                sublevels_up as Index,
            );
            varnode.varreturningtype = returning_type;
            varnode.location = location;
            cv.push(mcx::alloc_in(mcx, var_node(varnode))?);
        }
    }
    Ok(())
}

// ===========================================================================
// expandNSItemVars / expandNSItemAttrs.
// ===========================================================================

/// `expandNSItemVars` — produce a list of Vars (and optionally column names) for
/// the non-dropped columns of the nsitem.
pub fn expandNSItemVars<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'mcx>,
    nsitem: &ParseNamespaceItem<'mcx>,
    sublevels_up: i32,
    location: i32,
    mut colnames: Option<&mut PgVec<'mcx, NodePtr<'mcx>>>,
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    let mut result: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    let names = nsitem.p_names.as_deref().expect("p_names set");

    for (colindex, colnameval) in names.colnames.iter().enumerate() {
        let colname = str_val(colnameval);
        let nscol = &nsitem.p_nscolumns[colindex];

        if nscol.p_dontexpand {
            // skip
        } else if !colname.is_empty() {
            debug_assert!(nscol.p_varno > 0);
            let mut var = make_var(
                nscol.p_varno as i32,
                nscol.p_varattno,
                nscol.p_vartype,
                nscol.p_vartypmod,
                nscol.p_varcollid,
                sublevels_up as Index,
            );
            var.varreturningtype = nscol.p_varreturningtype;
            var.varnosyn = nscol.p_varnosyn;
            var.varattnosyn = nscol.p_varattnosyn;
            var.location = location;

            markNullableIfNeeded(pstate, &mut var)?;

            result.push(mcx::alloc_in(mcx, var_node(var))?);
            if let Some(cn) = colnames.as_deref_mut() {
                cn.push(make_string_node(mcx, colname)?);
            }
        } else {
            // dropped column, ignore
            debug_assert!(nscol.p_varno == 0);
        }
    }
    Ok(result)
}

/// `expandNSItemAttrs` — produce a list of TargetEntries for the attributes of
/// the nsitem; the referenced columns are marked for SELECT if requested.
pub fn expandNSItemAttrs<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    nsitem_index: usize,
    sublevels_up: i32,
    require_col_privs: bool,
    location: i32,
) -> PgResult<PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>> {
    // Snapshot what we need (the nsitem) so we can mutate pstate below.
    let (nsitem_snapshot, rtekind, perminfo_present) = {
        let nsitem = &pstate.p_namespace[nsitem_index];
        let rte = nsitem_rte(nsitem);
        (
            clone_nsitem(nsitem, mcx)?,
            rte.rtekind,
            nsitem.p_perminfo.is_some(),
        )
    };

    let mut names: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    let vars = expandNSItemVars(mcx, pstate, &nsitem_snapshot, sublevels_up, location, Some(&mut names))?;

    // Require read access to the table (handles zero-column relations).
    if rtekind == RTE_RELATION {
        debug_assert!(perminfo_present);
        let rtindex = nsitem_snapshot.p_rtindex;
        let perminfo_index = {
            let rte = &pstate.p_rtable[(rtindex - 1) as usize];
            getRTEPermissionInfo(&pstate.p_rteperminfos, rte)?
        };
        pstate.p_rteperminfos[perminfo_index].requiredPerms |= ACL_SELECT;
    }

    let mut te_list: PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>> = PgVec::new_in(mcx);
    debug_assert!(names.len() == vars.len());
    for (name, varnode) in names.iter().zip(vars.iter()) {
        let label = str_val(name);
        let var = match varnode.as_ref().as_var() {
            Some(v) => v.clone(),
            None => panic!("expandNSItemAttrs: expansion produced a non-Var"),
        };
        let resno = pstate.p_next_resno as AttrNumber;
        pstate.p_next_resno += 1;
        let te = make_target_entry(
            mcx,
            types_nodes::primnodes::Expr::Var(var.clone()),
            resno,
            Some(label),
            false,
        )?;
        te_list.push(te);

        if require_col_privs {
            markVarForSelectPriv(mcx, pstate, &var)?;
        }
    }

    Ok(te_list)
}

// ===========================================================================
// get_rte_attribute_name / get_rte_attribute_is_dropped / get_tle_by_resno /
// get_parse_rowmark.
// ===========================================================================

/// `get_rte_attribute_name` — get an attribute name from a RangeTblEntry (using
/// aliases if available). Returns "*" for InvalidAttrNumber.
pub fn get_rte_attribute_name<'mcx>(
    mcx: Mcx<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    attnum: AttrNumber,
) -> PgResult<PgString<'mcx>> {
    if attnum == InvalidAttrNumber {
        return PgString::from_str_in("*", mcx);
    }

    // If there is a user-written column alias, use it.
    if let Some(alias) = rte.alias.as_deref() {
        if attnum > 0 && (attnum as usize) <= alias.colnames.len() {
            return PgString::from_str_in(str_val(&alias.colnames[(attnum - 1) as usize]), mcx);
        }
    }

    // If the RTE is a relation, go to the catalogs (handles renames).
    if rte.rtekind == RTE_RELATION {
        return match lsyscache::get_attname::call(mcx, rte.relid, attnum, false)? {
            Some(s) => Ok(s),
            None => Err(ereport(ERROR)
                .errmsg_internal(format!(
                    "cache lookup failed for attribute {} of relation {}",
                    attnum, rte.relid
                ))
                .into_error()),
        };
    }

    // Otherwise use the column name from eref.
    let eref = rte.eref.as_deref().expect("eref set");
    if attnum > 0 && (attnum as usize) <= eref.colnames.len() {
        return PgString::from_str_in(str_val(&eref.colnames[(attnum - 1) as usize]), mcx);
    }

    Err(ereport(ERROR)
        .errmsg_internal(format!(
            "invalid attnum {} for rangetable entry {}",
            attnum,
            rte_eref_aliasname(rte)
        ))
        .into_error())
}

/// `get_rte_attribute_is_dropped` — check whether an attribute ref is to a
/// dropped column.
pub fn get_rte_attribute_is_dropped(
    rte: &RangeTblEntry<'_>,
    attnum: AttrNumber,
) -> PgResult<bool> {
    let result;
    match rte.rtekind {
        RTE_RELATION => {
            // Plain relation RTE: look up the attribute catalog entry.
            match syscache::search_attnum_attisdropped::call(rte.relid, attnum)? {
                Some(attisdropped) => result = attisdropped,
                None => {
                    return Err(ereport(ERROR)
                        .errmsg_internal(format!(
                            "cache lookup failed for attribute {} of relation {}",
                            attnum, rte.relid
                        ))
                        .into_error());
                }
            }
        }
        RTE_SUBQUERY | RTE_TABLEFUNC | RTE_VALUES | RTE_CTE | RTE_GROUP => {
            // These never have dropped columns.
            result = false;
        }
        RTE_NAMEDTUPLESTORE => {
            if attnum <= 0 || (attnum as usize) > rte.coltypes.len() {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!("invalid varattno {attnum}"))
                    .into_error());
            }
            result = !OidIsValid(rte.coltypes[(attnum - 1) as usize]);
        }
        RTE_JOIN => {
            if attnum <= 0 || (attnum as usize) > rte.joinaliasvars.len() {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!("invalid varattno {attnum}"))
                    .into_error());
            }
            // A NULL pointer in joinaliasvars signals a dropped column; in the
            // owned model that maps to a null Const placeholder. expandRTE is not
            // called on JOIN RTEs during parsing; for stored rules, a dropped
            // column shows up as a null Const, which we detect here.
            result = matches!(
                rte.joinaliasvars[(attnum - 1) as usize].as_ref(),
                Node::Expr(types_nodes::primnodes::Expr::Const(_))
                    if is_null_const(rte.joinaliasvars[(attnum - 1) as usize].as_ref())
            );
        }
        RTE_FUNCTION => {
            // RTE_FUNCTION composite arm needs get_expr_result_tupdesc — unported.
            panic!(
                "get_rte_attribute_is_dropped RTE_FUNCTION composite arm needs \
                 get_expr_result_tupdesc (funcapi), unported here (parse_relation.c:3487)"
            );
        }
        RTE_RESULT => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!(
                    "column {} of relation \"{}\" does not exist",
                    attnum,
                    rte_eref_aliasname(rte)
                ))
                .into_error());
        }
        other => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("unrecognized RTE kind: {}", other as i32))
                .into_error());
        }
    }
    Ok(result)
}

/// Is this aliasvar an all-NULL Const (dropped join column placeholder)?
fn is_null_const(node: &Node<'_>) -> bool {
    matches!(
        node,
        Node::Expr(types_nodes::primnodes::Expr::Const(c)) if c.constisnull
    )
}

/// `get_tle_by_resno` — find the TargetEntry with the given resno (search).
pub fn get_tle_by_resno<'a, 'mcx>(
    tlist: &'a [types_nodes::primnodes::TargetEntry<'mcx>],
    resno: AttrNumber,
) -> Option<&'a types_nodes::primnodes::TargetEntry<'mcx>> {
    tlist.iter().find(|tle| tle.resno == resno)
}

/// `get_parse_rowmark` — return the relation's RowMarkClause if any.
pub fn get_parse_rowmark<'a, 'mcx>(
    qry: &'a Query<'mcx>,
    rtindex: Index,
) -> Option<&'a RowMarkClause> {
    for rc_node in qry.rowMarks.iter() {
        if let Node::RowMarkClause(rc) = rc_node.as_ref() {
            if rc.rti == rtindex {
                return Some(rc);
            }
        }
    }
    None
}

// ===========================================================================
// attnameAttNum / attnumAttName / attnumTypeId / attnumCollationId.
// ===========================================================================

/// `attnameAttNum` — given an open relation and attribute name, return the
/// attnum (InvalidAttrNumber if not found or dropped).
pub fn attnameAttNum(
    rd: &types_rel::RelationData<'_>,
    attname: &str,
    sys_col_ok: bool,
) -> PgResult<i32> {
    let natts = rd.rd_att.attrs.len();
    for i in 0..natts {
        let att = rd.rd_att.attr(i);
        if attname_str(att) == attname && !att.attisdropped {
            return Ok((i + 1) as i32);
        }
    }

    if sys_col_ok {
        let i = specialAttNum(attname)?;
        if i != InvalidAttrNumber as i32 {
            return Ok(i);
        }
    }

    Ok(InvalidAttrNumber as i32)
}

/// `attnumAttName` — given an open relation and attid, return the attribute name.
pub fn attnumAttName<'mcx>(
    mcx: Mcx<'mcx>,
    rd: &types_rel::RelationData<'mcx>,
    attid: i32,
) -> PgResult<PgString<'mcx>> {
    if attid <= 0 {
        // C returns &SystemAttributeDefinition(attid)->attname. The owned
        // system-attribute seam only carries type info; the by-name table is the
        // SystemAttributeByName/Definition catalog data not exposed here.
        panic!(
            "attnumAttName: the system-column name table (SystemAttributeDefinition\
             ->attname) is not exposed by the system-attribute seam (parse_relation.c:3645)"
        );
    }
    if attid > rd.rd_att.attrs.len() as i32 {
        return Err(ereport(ERROR)
            .errmsg_internal(format!("invalid attribute number {attid}"))
            .into_error());
    }
    PgString::from_str_in(attname_str(rd.rd_att.attr((attid - 1) as usize)), mcx)
}

/// `attnumTypeId` — given an open relation and attid, return the attribute type.
pub fn attnumTypeId(rd: &types_rel::RelationData<'_>, attid: i32) -> PgResult<Oid> {
    if attid <= 0 {
        let (atttypid, _atttypmod, _attcollation) =
            plancat_ext::system_attribute_definition::call(attid)?;
        return Ok(atttypid);
    }
    if attid > rd.rd_att.attrs.len() as i32 {
        return Err(ereport(ERROR)
            .errmsg_internal(format!("invalid attribute number {attid}"))
            .into_error());
    }
    Ok(rd.rd_att.attr((attid - 1) as usize).atttypid)
}

/// `attnumCollationId` — given an open relation and attid, return the collation.
pub fn attnumCollationId(rd: &types_rel::RelationData<'_>, attid: i32) -> PgResult<Oid> {
    if attid <= 0 {
        // All system attributes are of noncollatable types.
        return Ok(InvalidOid);
    }
    if attid > rd.rd_att.attrs.len() as i32 {
        return Err(ereport(ERROR)
            .errmsg_internal(format!("invalid attribute number {attid}"))
            .into_error());
    }
    Ok(rd.rd_att.attr((attid - 1) as usize).attcollation)
}

// ===========================================================================
// errorMissingRTE / errorMissingColumn / findNSItemForRTE /
// rte_visible_if_lateral / rte_visible_if_qualified.
// ===========================================================================

/// `errorMissingRTE` — generate a helpful error about a missing RTE.
pub fn errorMissingRTE<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'mcx>,
    relation: &RangeVar<'_>,
) -> PgResult<core::convert::Infallible> {
    let relname = relation.relname.as_deref().unwrap_or("");
    let rte_coords = searchRangeTableForRel(mcx, pstate, relation)?;
    let mut bad_alias: Option<alloc::string::String> = None;

    // If we found a match that has an alias and the alias is visible in the
    // namespace, suggest using the alias.
    if let Some((depth, rte_index)) = rte_coords {
        let ps = pstate_at_depth(pstate, depth);
        let rte = &ps.p_rtable[rte_index];
        if rte.alias.is_some() && rte_eref_aliasname(rte) != relname {
            let aliasname = rte_eref_aliasname(rte);
            // refnameNamespaceItem(pstate, NULL, aliasname, ...) — and check the
            // found item is for the same RTE.
            if let Some((su, ns_idx)) =
                refnameNamespaceItem(pstate, None, aliasname, relation.location, true)?
            {
                let found_ps = pstate_at_depth(pstate, su as usize);
                let found_rte = nsitem_rte(&found_ps.p_namespace[ns_idx]);
                // p_rte == rte (same RTE): compare by depth + index identity.
                if su as usize == depth
                    && rte_eref_aliasname(found_rte) == aliasname
                    && found_ps.p_namespace[ns_idx].p_rtindex as usize == rte_index + 1
                {
                    bad_alias = Some(aliasname.into());
                }
            }
        }
    }

    if let Some(bad_alias) = bad_alias {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_TABLE)
            .errmsg(format!(
                "invalid reference to FROM-clause entry for table \"{relname}\""
            ))
            .errhint(format!(
                "Perhaps you meant to reference the table alias \"{bad_alias}\"."
            ))
            .errposition(parser_errposition(pstate, relation.location))
            .into_error());
    } else if let Some((depth, rte_index)) = rte_coords {
        let ps = pstate_at_depth(pstate, depth);
        let rte = &ps.p_rtable[rte_index];
        let aliasname = rte_eref_aliasname(rte);
        let mut b = ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_TABLE)
            .errmsg(format!(
                "invalid reference to FROM-clause entry for table \"{relname}\""
            ))
            .errdetail(format!(
                "There is an entry for table \"{aliasname}\", but it cannot be referenced from this part of the query."
            ));
        if rte_visible_if_lateral(pstate, depth, rte_index) {
            b = b.errhint(format!(
                "To reference that table, you must mark this subquery with LATERAL."
            ));
        }
        return Err(b
            .errposition(parser_errposition(pstate, relation.location))
            .into_error());
    } else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_TABLE)
            .errmsg(format!("missing FROM-clause entry for table \"{relname}\""))
            .errposition(parser_errposition(pstate, relation.location))
            .into_error());
    }
}

/// `errorMissingColumn` — generate a helpful error about a missing column.
pub fn errorMissingColumn<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_>,
    relname: Option<&str>,
    colname: &str,
    location: i32,
) -> PgResult<core::convert::Infallible> {
    let state = searchRangeTableForCol(mcx, pstate, relname, colname, location)?;

    // helper to render "column rel.col" vs "column \"col\"".
    let col_msg = |relname: Option<&str>| -> alloc::string::String {
        match relname {
            Some(r) => format!("column {r}.{colname} does not exist"),
            None => format!("column \"{colname}\" does not exist"),
        }
    };

    // Read an RTE's aliasname / colname by coordinates.
    let rte_alias = |coord: (usize, usize)| -> alloc::string::String {
        let ps = pstate_at_depth(pstate, coord.0);
        rte_eref_aliasname(&ps.p_rtable[coord.1]).into()
    };
    let rte_colname = |coord: (usize, usize), col: AttrNumber| -> alloc::string::String {
        let ps = pstate_at_depth(pstate, coord.0);
        let rte = &ps.p_rtable[coord.1];
        let eref = rte.eref.as_deref().expect("eref set");
        str_val(&eref.colnames[(col - 1) as usize]).into()
    };

    if let Some(rexact1) = state.rexact1 {
        if state.rexact2.is_some() {
            let mut b = ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_COLUMN)
                .errmsg(col_msg(relname))
                .errdetail(format!(
                    "There are columns named \"{colname}\", but they are in tables that cannot be referenced from this part of the query."
                ));
            if relname.is_none() {
                b = b.errhint(format!("Try using a table-qualified name."));
            }
            return Err(b.errposition(parser_errposition(pstate, location)).into_error());
        }
        // Single exact match; try to determine why it's inaccessible.
        let a = rte_alias(rexact1);
        let mut b = ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_COLUMN)
            .errmsg(col_msg(relname))
            .errdetail(format!(
                "There is a column named \"{colname}\" in table \"{a}\", but it cannot be referenced from this part of the query."
            ));
        if rte_visible_if_lateral(pstate, rexact1.0, rexact1.1) {
            b = b.errhint(format!(
                "To reference that column, you must mark this subquery with LATERAL."
            ));
        } else if relname.is_none() && rte_visible_if_qualified(pstate, rexact1.0, rexact1.1) {
            b = b.errhint(format!(
                "To reference that column, you must use a table-qualified name."
            ));
        }
        return Err(b.errposition(parser_errposition(pstate, location)).into_error());
    }

    if state.rsecond.is_none() {
        if state.rfirst.is_none() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_COLUMN)
                .errmsg(col_msg(relname))
                .errposition(parser_errposition(pstate, location))
                .into_error());
        }
        let rfirst = state.rfirst.unwrap();
        let a = rte_alias(rfirst);
        let c = rte_colname(rfirst, state.first);
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_COLUMN)
            .errmsg(col_msg(relname))
            .errhint(format!(
                "Perhaps you meant to reference the column \"{a}.{c}\"."
            ))
            .errposition(parser_errposition(pstate, location))
            .into_error());
    } else {
        let rfirst = state.rfirst.unwrap();
        let rsecond = state.rsecond.unwrap();
        let a1 = rte_alias(rfirst);
        let c1 = rte_colname(rfirst, state.first);
        let a2 = rte_alias(rsecond);
        let c2 = rte_colname(rsecond, state.second);
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_COLUMN)
            .errmsg(col_msg(relname))
            .errhint(format!(
                "Perhaps you meant to reference the column \"{a1}.{c1}\" or the column \"{a2}.{c2}\"."
            ))
            .errposition(parser_errposition(pstate, location))
            .into_error());
    }
}

/// `findNSItemForRTE` — find the nsitem `(depth, ns_idx)` for the RTE at
/// `(rte_depth, rte_index)`, if visible at all.
fn findNSItemForRTE(
    pstate: &ParseState<'_>,
    rte_depth: usize,
    rte_index: usize,
) -> Option<(usize, usize)> {
    // The RTE's identity in the owned model is its (depth, p_rtable index). An
    // nsitem matches when its p_rtindex points at the same RTE at the same level.
    let max_depth = pstate_depth(pstate);
    for depth in 0..=max_depth {
        let ps = pstate_at_depth(pstate, depth);
        for (ns_idx, nsitem) in ps.p_namespace.iter().enumerate() {
            if depth == rte_depth && (nsitem.p_rtindex as usize) == rte_index + 1 {
                return Some((depth, ns_idx));
            }
        }
    }
    None
}

/// `rte_visible_if_lateral` — would this RTE be visible if the user had written
/// LATERAL?
fn rte_visible_if_lateral(pstate: &ParseState<'_>, rte_depth: usize, rte_index: usize) -> bool {
    if pstate.p_lateral_active {
        return false;
    }
    if let Some((depth, ns_idx)) = findNSItemForRTE(pstate, rte_depth, rte_index) {
        let nsitem = &pstate_at_depth(pstate, depth).p_namespace[ns_idx];
        return nsitem.p_lateral_only && nsitem.p_lateral_ok;
    }
    false
}

/// `rte_visible_if_qualified` — would columns in this RTE be visible if qualified?
fn rte_visible_if_qualified(pstate: &ParseState<'_>, rte_depth: usize, rte_index: usize) -> bool {
    if let Some((depth, ns_idx)) = findNSItemForRTE(pstate, rte_depth, rte_index) {
        let nsitem = &pstate_at_depth(pstate, depth).p_namespace[ns_idx];
        return nsitem.p_rel_visible && !nsitem.p_cols_visible;
    }
    false
}

// ===========================================================================
// isQueryUsingTempRelation / addRTEPermissionInfo / getRTEPermissionInfo.
// ===========================================================================

/// `isQueryUsingTempRelation` — true iff any relation underlying the query is a
/// temporary relation.
pub fn isQueryUsingTempRelation(mcx: Mcx<'_>, query: &Query<'_>) -> PgResult<bool> {
    isQueryUsingTempRelation_walker_query(mcx, query)
}

fn isQueryUsingTempRelation_walker(mcx: Mcx<'_>, node: &Node<'_>) -> PgResult<bool> {
    if let Node::Query(query) = node {
        return isQueryUsingTempRelation_walker_query(mcx, query);
    }
    // C: `return expression_tree_walker(node, walker, context)`. In the owned
    // model the central `query_tree_walker` (driven from the Query arm below)
    // already descends into every nested `Query` node (RTE subqueries, sublink
    // subqueries, CTEs), which are the only nodes this probe acts on — it opens
    // relations only from a Query's rtable. A non-Query leaf therefore yields no
    // temp relation directly; its sub-Query recursion is the query walker's job.
    Ok(false)
}

fn isQueryUsingTempRelation_walker_query(mcx: Mcx<'_>, query: &Query<'_>) -> PgResult<bool> {
    for rte in query.rtable.iter() {
        if rte.rtekind == RTE_RELATION {
            let rel = backend_access_table_table::table_open(mcx, rte.relid, AccessShareLock)?;
            let relpersistence = rel.rd_rel.relpersistence;
            backend_access_table_table::table_close(rel, AccessShareLock)?;
            if relpersistence == RELPERSISTENCE_TEMP {
                return Ok(true);
            }
        }
    }

    // query_tree_walker(query, walker, ..., QTW_IGNORE_JOINALIASES). The owned
    // query walker visits sub-Querys (in sublink/CTE/RTE subquery positions).
    let mut found = false;
    let mut err: Option<types_error::PgError> = None;
    backend_nodes_core::node_walker::query_tree_walker(
        query,
        &mut |node: &Node| {
            if found || err.is_some() {
                return true;
            }
            match isQueryUsingTempRelation_walker(mcx, node) {
                Ok(true) => {
                    found = true;
                    true
                }
                Ok(false) => false,
                Err(e) => {
                    err = Some(e);
                    true
                }
            }
        },
        0,
    );
    if let Some(e) = err {
        return Err(e);
    }
    Ok(found)
}

/// `addRTEPermissionInfo` — create an RTEPermissionInfo for the RTE and append it
/// to the list, setting `rte->perminfoindex`. Returns the 0-based index.
pub fn addRTEPermissionInfo<'mcx>(
    rteperminfos: &mut PgVec<'mcx, RTEPermissionInfo<'mcx>>,
    rte: &mut RangeTblEntry<'mcx>,
) -> PgResult<usize> {
    debug_assert!(OidIsValid(rte.relid));
    debug_assert!(rte.perminfoindex == 0);

    let mut perminfo = RTEPermissionInfo::default();
    perminfo.relid = rte.relid;
    perminfo.inh = rte.inh;

    rteperminfos.push(perminfo);
    let idx = rteperminfos.len(); // 1-based
    rte.perminfoindex = idx as Index;
    Ok(idx - 1)
}

/// `getRTEPermissionInfo` — find the RTEPermissionInfo index (0-based) for the
/// given RTE in the list. C returns the pointer; the owned model returns the
/// 0-based index (the inward seam this crate owns).
pub fn getRTEPermissionInfo(
    rteperminfos: &[RTEPermissionInfo<'_>],
    rte: &RangeTblEntry<'_>,
) -> PgResult<usize> {
    if rte.perminfoindex == 0 || rte.perminfoindex as usize > rteperminfos.len() {
        return Err(ereport(ERROR)
            .errmsg_internal(format!(
                "invalid perminfoindex {} in RTE with relid {}",
                rte.perminfoindex, rte.relid
            ))
            .into_error());
    }
    let idx = (rte.perminfoindex - 1) as usize;
    let perminfo = &rteperminfos[idx];
    if perminfo.relid != rte.relid {
        return Err(ereport(ERROR)
            .errmsg_internal(format!(
                "permission info at index {} (with relid={}) does not match provided RTE (with relid={})",
                rte.perminfoindex, perminfo.relid, rte.relid
            ))
            .into_error());
    }
    Ok(idx)
}

// ===========================================================================
// Small clone helpers + bitmapset field algebra.
// ===========================================================================

/// Deep-copy a `ParseNamespaceColumn` slice into `mcx` (it is `Copy`).
fn clone_nscolumns<'mcx>(
    cols: &[ParseNamespaceColumn],
    mcx: Mcx<'mcx>,
) -> PgResult<PgVec<'mcx, ParseNamespaceColumn>> {
    mcx::slice_in(mcx, cols)
}

/// Deep-copy a `ParseNamespaceItem` into `mcx`.
fn clone_nsitem<'mcx>(
    nsitem: &ParseNamespaceItem<'_>,
    mcx: Mcx<'mcx>,
) -> PgResult<ParseNamespaceItem<'mcx>> {
    Ok(ParseNamespaceItem {
        p_names: match nsitem.p_names.as_deref() {
            Some(a) => Some(mcx::alloc_in(mcx, a.clone_in(mcx)?)?),
            None => None,
        },
        p_rte: match nsitem.p_rte.as_deref() {
            Some(r) => Some(mcx::alloc_in(mcx, r.clone_in(mcx)?)?),
            None => None,
        },
        p_rtindex: nsitem.p_rtindex,
        p_perminfo: match nsitem.p_perminfo.as_deref() {
            Some(p) => Some(mcx::alloc_in(mcx, p.clone_in(mcx)?)?),
            None => None,
        },
        p_nscolumns: clone_nscolumns(&nsitem.p_nscolumns, mcx)?,
        p_rel_visible: nsitem.p_rel_visible,
        p_cols_visible: nsitem.p_cols_visible,
        p_lateral_only: nsitem.p_lateral_only,
        p_lateral_ok: nsitem.p_lateral_ok,
        p_returning_type: nsitem.p_returning_type,
    })
}

// ===========================================================================
// Seam installation.
// ===========================================================================

/// `GetNSItemByRangeTablePosn(pstate, varno, sublevels_up)` followed by
/// `scanNSItemForColumn(pstate, nsitem, sublevels_up, colname, location)`
/// (parse_relation.c), as used by `ParseComplexProjection` (parse_func.c) for
/// the whole-row-Var fast path `(foo.*).bar`. The seam crosses the resolved
/// `ParseNamespaceItem *` by its `(varno, sublevels_up)` identity (exactly the
/// key `GetNSItemByRangeTablePosn` looks up); the owner re-resolves the nsitem
/// at depth `sublevels_up` and performs the column scan. `Ok(None)` is the C
/// `NULL` (column name does not match).
fn scan_ns_item_for_column_by_posn<'mcx>(
    pstate: &mut ParseState<'mcx>,
    varno: i32,
    sublevels_up: i32,
    colname: &str,
    location: i32,
) -> PgResult<Option<types_nodes::primnodes::Expr>> {
    // Recover the ambient query Mcx from an existing mcx-allocated pstate field
    // (p_rtable's allocator is the query context), as parse_func.c's pstate_mcx.
    let mcx = *pstate.p_rtable.allocator();
    let depth = sublevels_up as usize;

    // GetNSItemByRangeTablePosn: at the given nesting depth, find the nsitem
    // whose p_rtindex matches varno (there must be one).
    let ns_idx = {
        let ps = pstate_at_depth(pstate, depth);
        ps.p_namespace
            .iter()
            .position(|nsitem| nsitem.p_rtindex == varno)
            .ok_or_else(|| {
                ereport(ERROR)
                    .errmsg_internal(format!("nsitem not found (internal error)"))
                    .into_error()
            })?
    };

    // scanNSItemForColumn against the resolved nsitem at that depth.
    let node = scan_nsitem_for_column_at_depth(
        mcx,
        pstate,
        depth,
        ns_idx,
        sublevels_up,
        colname,
        location,
    )?;

    // The C returns a Node* that is a Var; unwrap to the Expr the caller wants.
    Ok(match node {
        Some(Node::Expr(expr)) => Some(expr),
        Some(other) => panic!(
            "scan_ns_item_for_column_by_posn: scanNSItemForColumn returned non-Expr node (tag {})",
            other.node_tag().0
        ),
        None => None,
    })
}

/// Install this unit's inward seams.
pub fn init_seams() {
    backend_parser_relation_seams::get_rte_permission_info::set(getRTEPermissionInfo);
    backend_parser_relation_seams::scan_ns_item_for_column_by_posn::set(
        scan_ns_item_for_column_by_posn,
    );
}
