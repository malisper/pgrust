//! Port of `src/backend/parser/parse_clause.c` (PostgreSQL 18.3) — the
//! expression-clause core of the parser.
//!
//! # Scope (F1 — clause core)
//!
//! Ported 1:1 over the repo's split raw-[`Node`]/typed-[`Expr`] model with owned
//! `Vec` lists and `Mcx<'mcx>`-threaded allocation:
//!
//!   * [`transformWhereClause`] / [`transformLimitClause`] (`checkExprIsVarFree`).
//!   * `findTargetlistEntrySQL92` / `findTargetlistEntrySQL99` /
//!     `checkTargetlistEntrySQL92`.
//!   * [`transformGroupClause`] + the grouping-set engine
//!     (`flatten_grouping_sets`, `transformGroupClauseExpr`,
//!     `transformGroupClauseList`, `transformGroupingSet`).
//!   * [`transformSortClause`] / [`addTargetToSortList`] / `addTargetToGroupList`
//!     / [`targetIsInSortList`] / [`assignSortGroupRef`].
//!   * [`transformDistinctClause`] / [`transformDistinctOnClause`] /
//!     `get_matching_location`.
//!
//! # Seams (panic-until-owner-lands)
//!
//! `transformTargetEntry` (parse_target — `backend-parser-target-seams`),
//! `contain_aggs_of_level` / `locate_agg_of_level` (parse_agg —
//! `backend-parser-parse-agg-seams`), `contain_windowfuncs` /
//! `locate_windowfunc` (rewriteManip — `backend-rewrite-rewritemanip-seams`),
//! and `equal` over `Expr` (equalfuncs — `backend-nodes-equalfuncs-seams`).
//!
//! Merged sibling owners called directly (cycle-free): `transformExpr`
//! (parse_expr), `coerce_type`/`coerce_to_boolean`/`coerce_to_specific_type`
//! (parse_coerce), `compatible_oper_opid`/`get_sort_group_operators`
//! (parse_oper), `colNameToVar` (parse_relation),
//! `contain_vars_of_level`/`locate_var_of_level` (var.c). The lsyscache lookups
//! (`get_equality_op_for_ordering_op`, `op_hashjoinable`, `get_commutator`) and
//! `parser_errposition` (parse_node.c) go through their installed seams.
//!
//! # F2 — FROM clause / JOIN (in `from_clause`)
//!
//! `transformFromClause` / `setTargetTable` / `transformFromClauseItem` and the
//! JOIN machinery live in the [`from_clause`] submodule.
//!
//! # Deferred to follow-on families (NOT in this crate)
//!
//!   * F3a: `transformWindowDefinitions` / on-conflict.
//!   * F3b: tablefunc (XMLTABLE) / `JSON_TABLE`.

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use mcx::{alloc_in, Mcx};

use types_core::{Index, InvalidOid, Oid, OidIsValid};
use types_error::{
    PgError, PgResult, ERRCODE_AMBIGUOUS_COLUMN, ERRCODE_GROUPING_ERROR, ERRCODE_INTERNAL_ERROR,
    ERRCODE_INVALID_COLUMN_REFERENCE, ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE,
    ERRCODE_SYNTAX_ERROR, ERRCODE_TOO_MANY_COLUMNS, ERRCODE_WINDOWING_ERROR,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use backend_utils_error::ereport;
use types_tuple::heaptuple::{INT8OID, TEXTOID, UNKNOWNOID};

use types_nodes::nodes::{Node, NodePtr};
use types_nodes::parsestmt::{ParseExprKind, ParseState};
use types_nodes::nodelimit::{LimitOption, LIMIT_OPTION_WITH_TIES};

use ParseExprKind::{EXPR_KIND_DISTINCT_ON, EXPR_KIND_GROUP_BY, EXPR_KIND_LIMIT};
use types_nodes::primnodes::{CoercionForm, Expr, TargetEntry};
use types_nodes::rawnodes::{
    GroupingSet, GroupingSetKind, SortBy, SortByDir, SortByNulls, SortGroupClause,
};
use types_nodes::value::Integer;

use types_parsenodes::CoercionContext;

use backend_nodes_core::makefuncs::make_grouping_set;
use backend_nodes_core::nodefuncs::{expr_location, expr_type, strip_implicit_coercions};

use backend_optimizer_util_vars::var::{contain_vars_of_level, locate_var_of_level};
use backend_parser_parse_expr::transformExpr;
use backend_parser_parse_oper::{compatible_oper_opid, get_sort_group_operators};

use backend_nodes_equalfuncs_seams as equalfuncs;
use backend_parser_parse_agg_seams as parse_agg;
use backend_parser_small1_seams as parse_node;
use backend_parser_target_seams as parse_target;
use backend_rewrite_rewritemanip_seams as rewritemanip;
use backend_utils_cache_lsyscache_seams as lsyscache;

// ===========================================================================
// CoercionForm / CoercionContext values transmitted to coerce_type.
// ===========================================================================

/// `COERCION_IMPLICIT` (parser/parse_coerce.h, first `CoercionContext` value).
const COERCION_IMPLICIT: CoercionContext = CoercionContext::COERCION_IMPLICIT;
/// `COERCE_IMPLICIT_CAST` (nodes/primnodes.h).
const COERCE_IMPLICIT_CAST: CoercionForm = CoercionForm::COERCE_IMPLICIT_CAST;

// ===========================================================================
// IsA helpers + value-node accessors (nodes.h / value.h).
// ===========================================================================

/// `strVal(node)` if `node` is a `T_String` value node, else `None`.
pub(crate) fn str_val<'a>(node: &'a Node<'_>) -> Option<&'a str> {
    match node {
        Node::String(s) => Some(s.sval.as_str()),
        _ => None,
    }
}

/// `elog(ERROR, ...)` equivalent — an internal "can't happen" error.
pub(crate) fn elog_error(msg: impl Into<String>) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INTERNAL_ERROR)
        .errmsg_internal(msg)
        .into_error()
}

/// `parser_errposition(pstate, location)` — best-effort cursor position; the C
/// is infallible here (it reads `pstate->p_sourcetext`), so unwrap to 0 on the
/// catcache-free path, matching the parse_expr.c port's helper.
pub(crate) fn errpos(pstate: &ParseState<'_>, location: i32) -> i32 {
    parse_node::parser_errposition::call(pstate, location).unwrap_or(0)
}

/// `(Node *) tle->expr` — the C reads the `TargetEntry.expr` subtree as a
/// `Node *` at the var/agg/windowfunc walk sites. In the split model that is an
/// owned [`Expr`] wrapped as [`Node::Expr`]. An absent expr is an internal error
/// (the C code always has a non-NULL `tle->expr` at these call sites).
fn tle_expr_node<'mcx>(tle: &TargetEntry<'mcx>) -> Node<'mcx> {
    let expr = tle
        .expr
        .as_deref()
        .cloned()
        .expect("TargetEntry.expr must be present");
    Node::Expr(expr)
}

/// `tle->expr` as a borrowed [`Expr`] for `exprType`/`equal`/`strip_*`.
fn tle_expr<'a, 'mcx>(tle: &'a TargetEntry<'mcx>) -> &'a Expr {
    tle.expr
        .as_deref()
        .expect("TargetEntry.expr must be present")
}

// ===========================================================================
// transformWhereClause — parse_clause.c:1830
// ===========================================================================

/// Transform the qualification and make sure it is of type boolean. Used for
/// WHERE and allied clauses.
pub fn transformWhereClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    clause: Option<Node<'mcx>>,
    exprKind: ParseExprKind,
    constructName: &str,
) -> PgResult<Option<Expr>> {
    let Some(clause) = clause else {
        return Ok(None);
    };

    let qual = transformExpr(pstate, Some(clause), exprKind)?
        .ok_or_else(|| elog_error("transformWhereClause: transformExpr returned NULL"))?;
    let qual =
        backend_parser_coerce::coerce_to_boolean(mcx, Some(pstate), qual, constructName)?;
    Ok(Some(qual))
}

// ===========================================================================
// transformLimitClause — parse_clause.c:1880
// ===========================================================================

/// Transform the expression and make sure it is of type bigint. Used for LIMIT
/// and allied clauses.
pub fn transformLimitClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    clause: Option<Node<'mcx>>,
    exprKind: ParseExprKind,
    constructName: &str,
    limitOption: LimitOption,
) -> PgResult<Option<Expr>> {
    let Some(clause) = clause else {
        return Ok(None);
    };

    // The C re-reads the original `clause` pointer (unchanged by transformExpr)
    // for the WITH TIES NULL check below.
    let raw_is_null_const = match &clause {
        Node::A_Const(ac) => ac.isnull,
        _ => false,
    };

    let qual = transformExpr(pstate, Some(clause), exprKind)?
        .ok_or_else(|| elog_error("transformLimitClause: transformExpr returned NULL"))?;
    let qual = backend_parser_coerce::coerce_to_specific_type(
        mcx,
        Some(pstate),
        qual,
        INT8OID,
        constructName,
    )?;

    /* LIMIT can't refer to any variables of the current query */
    checkExprIsVarFree(pstate, &qual, constructName)?;

    /*
     * Don't allow NULLs in FETCH FIRST .. WITH TIES.  This test is ugly and
     * extensible only with great pain, but seems better than no test at all.
     */
    if exprKind == EXPR_KIND_LIMIT && limitOption == LIMIT_OPTION_WITH_TIES && raw_is_null_const {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE)
            .errmsg("row count cannot be null in FETCH FIRST ... WITH TIES clause")
            .into_error());
    }

    Ok(Some(qual))
}

// ===========================================================================
// checkExprIsVarFree — parse_clause.c:1924
// ===========================================================================

/// Check that given expr has no Vars of the current query level.
pub(crate) fn checkExprIsVarFree(
    pstate: &mut ParseState<'_>,
    n: &Expr,
    constructName: &str,
) -> PgResult<()> {
    let node = Node::Expr(n.clone());
    if contain_vars_of_level(&node, 0) {
        let location = locate_var_of_level(&node, 0);
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            // translator: %s is name of a SQL construct, eg LIMIT
            .errmsg(alloc::format!(
                "argument of {constructName} must not contain variables"
            ))
            .errposition(errpos(pstate, location))
            .into_error());
    }
    Ok(())
}

// ===========================================================================
// checkTargetlistEntrySQL92 — parse_clause.c:1949
// ===========================================================================

/// Validate a targetlist entry found by findTargetlistEntrySQL92.
fn checkTargetlistEntrySQL92(
    pstate: &mut ParseState<'_>,
    tle: &TargetEntry<'_>,
    exprKind: ParseExprKind,
) -> PgResult<()> {
    if exprKind == EXPR_KIND_GROUP_BY {
        let tle_expr = tle_expr_node(tle);
        /* reject aggregates and window functions */
        if pstate.p_hasAggs && parse_agg::contain_aggs_of_level::call(&tle_expr, 0) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_GROUPING_ERROR)
                // translator: %s is name of a SQL construct, eg GROUP BY
                .errmsg(alloc::format!(
                    "aggregate functions are not allowed in {}",
                    parse_expr_kind_name(exprKind)
                ))
                .errposition(errpos(
                    pstate,
                    parse_agg::locate_agg_of_level::call(&tle_expr, 0),
                ))
                .into_error());
        }
        if pstate.p_hasWindowFuncs && rewritemanip::contain_windowfuncs::call(&tle_expr) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_WINDOWING_ERROR)
                // translator: %s is name of a SQL construct, eg GROUP BY
                .errmsg(alloc::format!(
                    "window functions are not allowed in {}",
                    parse_expr_kind_name(exprKind)
                ))
                .errposition(errpos(
                    pstate,
                    rewritemanip::locate_windowfunc::call(&tle_expr),
                ))
                .into_error());
        }
    }
    /*
     * The other exprKinds (ORDER BY, DISTINCT ON) impose no restriction here
     * (the C `switch` has only the GROUP BY case plus a `default: break`).
     */
    Ok(())
}

/// `ParseExprKindName(exprKind)` — the SQL construct name for error text.
/// Delegated to the parse_expr.c port (the canonical owner of the table).
fn parse_expr_kind_name(exprKind: ParseExprKind) -> &'static str {
    backend_parser_parse_expr::ParseExprKindName(exprKind)
}

// ===========================================================================
// findTargetlistEntrySQL92 — parse_clause.c:2050
// ===========================================================================

/// Returns the targetlist index matching the given (untransformed) node per the
/// SQL92 interpretation (a bare column name or an ordinal position number),
/// falling back to the SQL99 interpretation otherwise.
fn findTargetlistEntrySQL92<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    node: &Node<'mcx>,
    tlist: &mut Vec<TargetEntry<'mcx>>,
    exprKind: ParseExprKind,
) -> PgResult<usize> {
    /*
     * Check for a bare column reference of the form `colname` (a single-field
     * ColumnRef whose field is a String).
     */
    if let Node::ColumnRef(cref) = node {
        if cref.fields.len() == 1 {
            if let Some(field0_name) = str_val(&cref.fields[0]) {
                let location = cref.location;
                let mut name: Option<String> = Some(String::from(field0_name));

                if exprKind == EXPR_KIND_GROUP_BY {
                    /*
                     * In GROUP BY, we must prefer a match against a FROM-clause
                     * column to one against the targetlist.  If FROM exposes a
                     * matching column, fall through to SQL99 rules.  (colNameToVar
                     * ereports on ambiguity.)
                     */
                    let n = name.as_deref().unwrap();
                    if backend_parser_relation::colNameToVar(mcx, pstate, n, true, location)?
                        .is_some()
                    {
                        name = None;
                    }
                }

                if let Some(name) = name {
                    let mut target_result: Option<usize> = None;

                    for idx in 0..tlist.len() {
                        let is_match = !tlist[idx].resjunk
                            && tlist[idx].resname.as_deref() == Some(name.as_str());
                        if is_match {
                            if let Some(prev) = target_result {
                                if !equalfuncs::equal_expr::call(
                                    tle_expr(&tlist[prev]),
                                    tle_expr(&tlist[idx]),
                                ) {
                                    return Err(ereport(ERROR)
                                        .errcode(ERRCODE_AMBIGUOUS_COLUMN)
                                        // translator: first %s is name of a SQL construct, eg ORDER BY
                                        .errmsg(alloc::format!(
                                            "{} \"{}\" is ambiguous",
                                            parse_expr_kind_name(exprKind),
                                            name
                                        ))
                                        .errposition(errpos(pstate, location))
                                        .into_error());
                                }
                            } else {
                                target_result = Some(idx);
                            }
                            /* Stay in loop to check for ambiguity */
                        }
                    }
                    if let Some(idx) = target_result {
                        /* return the first match, after suitable validation */
                        checkTargetlistEntrySQL92(pstate, &tlist[idx], exprKind)?;
                        return Ok(idx);
                    }
                }
            }
        }
    }

    /*
     * Check for a constant-integer ordinal reference of the form `n`.
     */
    if let Node::A_Const(aconst) = node {
        let aconst_location = aconst.location;
        let target_pos = match aconst.val.as_deref() {
            Some(Node::Integer(i)) => i.ival,
            _ => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    // translator: %s is name of a SQL construct, eg ORDER BY
                    .errmsg(alloc::format!(
                        "non-integer constant in {}",
                        parse_expr_kind_name(exprKind)
                    ))
                    .errposition(errpos(pstate, aconst_location))
                    .into_error());
            }
        };

        let mut targetlist_pos: i32 = 0;
        for idx in 0..tlist.len() {
            if !tlist[idx].resjunk {
                targetlist_pos += 1;
                if targetlist_pos == target_pos {
                    /* return the unique match, after suitable validation */
                    checkTargetlistEntrySQL92(pstate, &tlist[idx], exprKind)?;
                    return Ok(idx);
                }
            }
        }
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            // translator: %s is name of a SQL construct, eg ORDER BY
            .errmsg(alloc::format!(
                "{} position {} is not in select list",
                parse_expr_kind_name(exprKind),
                target_pos
            ))
            .errposition(errpos(pstate, aconst_location))
            .into_error());
    }

    /*
     * Otherwise, we have an expression, so process it per SQL99 rules.
     */
    findTargetlistEntrySQL99(mcx, pstate, node, tlist, exprKind)
}

// ===========================================================================
// findTargetlistEntrySQL99 — parse_clause.c:2171
// ===========================================================================

/// Returns the targetlist index matching the given (untransformed) node per the
/// SQL99 interpretation; appends a resjunk target if none matches.
fn findTargetlistEntrySQL99<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    node: &Node<'mcx>,
    tlist: &mut Vec<TargetEntry<'mcx>>,
    exprKind: ParseExprKind,
) -> PgResult<usize> {
    /*
     * Convert the untransformed node to a transformed expression, and search
     * for a match in the tlist.
     */
    let expr = transformExpr(pstate, Some(node.clone_in(mcx)?), exprKind)?
        .ok_or_else(|| elog_error("findTargetlistEntrySQL99: transformExpr returned NULL"))?;

    for idx in 0..tlist.len() {
        /* Ignore any implicit cast on the existing tlist expression. */
        let texpr = strip_implicit_coercions(tle_expr(&tlist[idx]));
        if equalfuncs::equal_expr::call(&expr, texpr) {
            return Ok(idx);
        }
    }

    /*
     * If no matches, construct a new target entry which is appended to the end
     * of the target list, with resjunk = true.
     */
    let target_result =
        parse_target::transform_target_entry::call(mcx, pstate, node, expr, exprKind, None, true)?;

    tlist.push(target_result);
    Ok(tlist.len() - 1)
}

// ===========================================================================
// transformGroupClause — parse_clause.c:2257 .. :2719 (grouping-set engine)
// ===========================================================================

/// The three shapes a C `flatten_grouping_sets()` can return through its
/// `Node *` result: `(Node *) NIL`, a single non-`List` node, or a `List *`.
enum Flattened<'mcx> {
    Nil,
    One(NodePtr<'mcx>),
    Many(Vec<NodePtr<'mcx>>),
}

/// `flatten_grouping_sets(expr, toplevel, hasGroupingSets)` — parse_clause.c:2257
///
/// Flatten out parenthesized sublists in grouping lists, and some cases of
/// nested grouping sets.  As a side effect, set `*hasGroupingSets` if the list
/// has any `GroupingSet` nodes.
fn flatten_grouping_sets<'mcx>(
    mcx: Mcx<'mcx>,
    expr: &Node<'mcx>,
    toplevel: bool,
    hasGroupingSets: Option<&mut bool>,
) -> PgResult<Flattened<'mcx>> {
    /* check_stack_depth handled by the host runtime */

    match expr {
        Node::RowExpr(r) if r.row_format == COERCE_IMPLICIT_CAST => {
            /* Recurse into the implicit RowExpr's arguments (a `T_List`). */
            return flatten_grouping_sets_list(mcx, &r.args, false, None);
        }
        Node::GroupingSet(gset) => {
            let mut result_set: Vec<NodePtr<'mcx>> = Vec::new();

            if let Some(flag) = hasGroupingSets {
                *flag = true;
            }

            /*
             * At the top level, skip over all empty grouping sets; the caller
             * can supply the canonical GROUP BY () if nothing is left.
             */
            if toplevel && gset.kind == GroupingSetKind::GROUPING_SET_EMPTY {
                return Ok(Flattened::Nil);
            }

            for n1 in gset.content.iter() {
                let n2 = flatten_grouping_sets(mcx, n1, false, None)?;

                let n1_is_sets = matches!(
                    &**n1,
                    Node::GroupingSet(g) if g.kind == GroupingSetKind::GROUPING_SET_SETS
                );
                if n1_is_sets {
                    /* n2 is a `List *` (the flattened nested SETS content) */
                    match n2 {
                        Flattened::Nil => {}
                        Flattened::One(node) => result_set.push(node),
                        Flattened::Many(mut nodes) => result_set.append(&mut nodes),
                    }
                } else {
                    /* lappend(result_set, n2): n2 is appended as a single cell */
                    result_set.push(flattened_to_node(mcx, n2)?);
                }
            }

            /*
             * At top level, keep the grouping set node; but if we're in a
             * simply-nested grouping set, concat the flattened result into the
             * outer list.
             */
            if toplevel || gset.kind != GroupingSetKind::GROUPING_SET_SETS {
                let gs = make_grouping_set(
                    gset.kind,
                    nodes_into_pgvec(mcx, result_set)?,
                    gset.location,
                );
                return Ok(Flattened::One(alloc_in(mcx, Node::GroupingSet(gs))?));
            } else {
                return Ok(Flattened::Many(result_set));
            }
        }
        _ => {}
    }

    Ok(Flattened::One(alloc_in(mcx, expr.clone_in(mcx)?)?))
}

/// The C `T_List` arm of `flatten_grouping_sets`: the grouping list (and an
/// implicit RowExpr's `args`) reaches the recursion as a `List *`.
fn flatten_grouping_sets_list<'mcx>(
    mcx: Mcx<'mcx>,
    list: &[NodePtr<'mcx>],
    toplevel: bool,
    hasGroupingSets: Option<&mut bool>,
) -> PgResult<Flattened<'mcx>> {
    let mut result: Vec<NodePtr<'mcx>> = Vec::new();
    let mut flag_holder = hasGroupingSets;
    for l in list.iter() {
        let n = flatten_grouping_sets(mcx, l, toplevel, flag_holder.as_deref_mut())?;
        match n {
            Flattened::Nil => { /* if (n != NIL) skips */ }
            Flattened::Many(mut nodes) => {
                /* IsA(n, List): list_concat */
                result.append(&mut nodes);
            }
            Flattened::One(node) => {
                /* lappend */
                result.push(node);
            }
        }
    }
    Ok(Flattened::Many(result))
}

/// Reduce a [`Flattened`] to a single node for the `lappend(result_set, n2)`
/// call site in `flatten_grouping_sets`'s `T_GroupingSet` arm.
fn flattened_to_node<'mcx>(
    mcx: Mcx<'mcx>,
    f: Flattened<'mcx>,
) -> PgResult<NodePtr<'mcx>> {
    match f {
        Flattened::One(n) => Ok(n),
        Flattened::Nil => {
            /* The grouping-set walk never appends a NIL cell. */
            unreachable!("flatten_grouping_sets: NIL node appended to a grouping list")
        }
        Flattened::Many(nodes) => {
            /*
             * A parenthesized expression sublist (implicit RowExpr) inside a
             * grouping set: the C appends the flattened sublist (a `List *`) as
             * a single `T_List` cell, which transformGroupingSet later reaches
             * via `IsA(n, List)`.
             */
            Ok(alloc_in(mcx, Node::List(nodes_into_pgvec(mcx, nodes)?))?)
        }
    }
}

/// `transformGroupClauseExpr(...)` — parse_clause.c:2366
///
/// Transform a single expression within a `GROUP BY` clause or grouping set.
/// Returns the ressortgroupref of the expression.
fn transformGroupClauseExpr<'mcx>(
    mcx: Mcx<'mcx>,
    flatresult: &mut Vec<SortGroupClause>,
    seen_local: &[Index],
    pstate: &mut ParseState<'mcx>,
    gexpr: &Node<'mcx>,
    targetlist: &mut Vec<TargetEntry<'mcx>>,
    sortClause: &[SortGroupClause],
    exprKind: ParseExprKind,
    useSQL99: bool,
    toplevel: bool,
) -> PgResult<Index> {
    let tle_idx = if useSQL99 {
        findTargetlistEntrySQL99(mcx, pstate, gexpr, targetlist, exprKind)?
    } else {
        findTargetlistEntrySQL92(mcx, pstate, gexpr, targetlist, exprKind)?
    };

    let mut found = false;

    if targetlist[tle_idx].ressortgroupref > 0 {
        let ressortgroupref = targetlist[tle_idx].ressortgroupref;

        /*
         * Eliminate duplicates (GROUP BY x, x) but only at local level.
         * (Duplicates in grouping sets can affect the number of returned rows,
         * so can't be dropped indiscriminately.)
         */
        if seen_local.contains(&ressortgroupref) {
            return Ok(0);
        }

        /*
         * If we're already in the flat clause list, no need to add ourselves
         * again.
         */
        found = targetIsInSortList(&targetlist[tle_idx], InvalidOid, flatresult);
        if found {
            return Ok(ressortgroupref);
        }

        /*
         * If the GROUP BY tlist entry also appears in ORDER BY, copy operator
         * info from the (first) matching ORDER BY item.  In a grouping set, we
         * force NULLS LAST.
         */
        for sc in sortClause.iter() {
            if sc.tleSortGroupRef == ressortgroupref {
                let mut grpc = *sc;
                if !toplevel {
                    grpc.nulls_first = false;
                }
                flatresult.push(grpc);
                found = true;
                break;
            }
        }
    }

    /*
     * If no match in ORDER BY, just add it to the result using default
     * sort/group semantics.
     */
    if !found {
        addTargetToGroupList(mcx, pstate, tle_idx, flatresult, targetlist)?;
    }

    /* _something_ must have assigned us a sortgroupref by now... */
    Ok(targetlist[tle_idx].ressortgroupref)
}

/// `transformGroupClauseList(...)` — parse_clause.c:2474
///
/// Transform a list of expressions within a single grouping-set clause, where
/// duplicates can be safely eliminated.  Returns the list of ressortgrouprefs.
fn transformGroupClauseList<'mcx>(
    mcx: Mcx<'mcx>,
    flatresult: &mut Vec<SortGroupClause>,
    pstate: &mut ParseState<'mcx>,
    list: &[NodePtr<'mcx>],
    targetlist: &mut Vec<TargetEntry<'mcx>>,
    sortClause: &[SortGroupClause],
    exprKind: ParseExprKind,
    useSQL99: bool,
    toplevel: bool,
) -> PgResult<Vec<Index>> {
    let mut seen_local: Vec<Index> = Vec::new();
    let mut result: Vec<Index> = Vec::new();

    for gexpr in list.iter() {
        let ref_ = transformGroupClauseExpr(
            mcx,
            flatresult,
            &seen_local,
            pstate,
            gexpr,
            targetlist,
            sortClause,
            exprKind,
            useSQL99,
            toplevel,
        )?;

        if ref_ > 0 {
            seen_local.push(ref_);
            result.push(ref_);
        }
    }

    Ok(result)
}

/// `transformGroupingSet(...)` — parse_clause.c:2527
///
/// Transform a grouping set and (recursively) its content.  Returns the
/// transformed node (SIMPLE nodes holding lists of ressortgrouprefs).
fn transformGroupingSet<'mcx>(
    mcx: Mcx<'mcx>,
    flatresult: &mut Vec<SortGroupClause>,
    pstate: &mut ParseState<'mcx>,
    gset: &GroupingSet<'mcx>,
    targetlist: &mut Vec<TargetEntry<'mcx>>,
    sortClause: &[SortGroupClause],
    exprKind: ParseExprKind,
    useSQL99: bool,
    toplevel: bool,
) -> PgResult<Node<'mcx>> {
    debug_assert!(toplevel || gset.kind != GroupingSetKind::GROUPING_SET_SETS);

    let mut content: Vec<NodePtr<'mcx>> = Vec::new();

    for n in gset.content.iter() {
        match &**n {
            /*
             * A parenthesized sublist of expressions: transform the whole list
             * (duplicates within it can be eliminated locally) into a SIMPLE
             * grouping set of ressortgrouprefs.
             */
            Node::List(sublist) => {
                let l = transformGroupClauseList(
                    mcx, flatresult, pstate, sublist, targetlist, sortClause, exprKind, useSQL99,
                    false,
                )?;
                let loc = list_exprLocation(sublist)?;
                let gs = make_grouping_set(
                    GroupingSetKind::GROUPING_SET_SIMPLE,
                    refs_to_int_pgvec(mcx, &l)?,
                    loc,
                );
                content.push(alloc_in(mcx, Node::GroupingSet(gs))?);
            }
            Node::GroupingSet(gset2) => {
                let tg = transformGroupingSet(
                    mcx, flatresult, pstate, gset2, targetlist, sortClause, exprKind, useSQL99,
                    false,
                )?;
                content.push(alloc_in(mcx, tg)?);
            }
            _ => {
                let ref_ = transformGroupClauseExpr(
                    mcx,
                    flatresult,
                    &[],
                    pstate,
                    n,
                    targetlist,
                    sortClause,
                    exprKind,
                    useSQL99,
                    false,
                )?;
                let loc = node_expr_location(n)?;
                let gs = make_grouping_set(
                    GroupingSetKind::GROUPING_SET_SIMPLE,
                    refs_to_int_pgvec(mcx, &[ref_])?,
                    loc,
                );
                content.push(alloc_in(mcx, Node::GroupingSet(gs))?);
            }
        }
    }

    /* Arbitrarily cap the size of CUBE, which has exponential growth */
    if gset.kind == GroupingSetKind::GROUPING_SET_CUBE && content.len() > 12 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_TOO_MANY_COLUMNS)
            .errmsg("CUBE is limited to 12 elements")
            .errposition(errpos(pstate, gset.location))
            .into_error());
    }

    let gs = make_grouping_set(gset.kind, nodes_into_pgvec(mcx, content)?, gset.location);
    Ok(Node::GroupingSet(gs))
}

/// `transformGroupClause(...)` — parse_clause.c:2631
///
/// Transform a `GROUP BY` clause (also used for window `PARTITION BY`, always
/// SQL99).  Returns `(groupClause, groupingSets)`.
pub fn transformGroupClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    grouplist: &[NodePtr<'mcx>],
    targetlist: &mut Vec<TargetEntry<'mcx>>,
    sortClause: &[SortGroupClause],
    exprKind: ParseExprKind,
    useSQL99: bool,
) -> PgResult<(Vec<SortGroupClause>, Vec<NodePtr<'mcx>>)> {
    let mut result: Vec<SortGroupClause> = Vec::new();
    let mut gsets: Vec<NodePtr<'mcx>> = Vec::new();
    let mut hasGroupingSets = false;
    let mut seen_local: Vec<Index> = Vec::new();

    /*
     * Recursively flatten implicit RowExprs.  The grouplist reaches the
     * `T_List` arm.
     */
    let flat = flatten_grouping_sets_list(mcx, grouplist, true, Some(&mut hasGroupingSets))?;
    let mut flat_grouplist: Vec<NodePtr<'mcx>> = match flat {
        Flattened::Nil => Vec::new(),
        Flattened::One(n) => alloc::vec![n],
        Flattened::Many(nodes) => nodes,
    };

    /*
     * If the list is now empty but hasGroupingSets is true, restore a single
     * empty grouping set: GROUP BY ()
     */
    if flat_grouplist.is_empty() && hasGroupingSets {
        let loc = list_exprLocation(grouplist)?;
        let gs = make_grouping_set(GroupingSetKind::GROUPING_SET_EMPTY, empty_pgvec(mcx)?, loc);
        flat_grouplist.push(alloc_in(mcx, Node::GroupingSet(gs))?);
    }

    for gexpr in flat_grouplist.iter() {
        if let Node::GroupingSet(gset) = &**gexpr {
            match gset.kind {
                GroupingSetKind::GROUPING_SET_EMPTY => {
                    gsets.push(alloc_in(mcx, gexpr.clone_in(mcx)?)?);
                }
                GroupingSetKind::GROUPING_SET_SIMPLE => {
                    /* can't happen */
                    debug_assert!(false, "GROUPING_SET_SIMPLE at top level");
                }
                GroupingSetKind::GROUPING_SET_SETS
                | GroupingSetKind::GROUPING_SET_CUBE
                | GroupingSetKind::GROUPING_SET_ROLLUP => {
                    let tg = transformGroupingSet(
                        mcx, &mut result, pstate, gset, targetlist, sortClause, exprKind, useSQL99,
                        true,
                    )?;
                    gsets.push(alloc_in(mcx, tg)?);
                }
            }
        } else {
            let ref_ = transformGroupClauseExpr(
                mcx,
                &mut result,
                &seen_local,
                pstate,
                gexpr,
                targetlist,
                sortClause,
                exprKind,
                useSQL99,
                true,
            )?;

            if ref_ > 0 {
                seen_local.push(ref_);
                if hasGroupingSets {
                    let loc = node_expr_location(gexpr)?;
                    let gs = make_grouping_set(
                        GroupingSetKind::GROUPING_SET_SIMPLE,
                        refs_to_int_pgvec(mcx, &[ref_])?,
                        loc,
                    );
                    gsets.push(alloc_in(mcx, Node::GroupingSet(gs))?);
                }
            }
        }
    }

    Ok((result, gsets))
}

// ===========================================================================
// transformSortClause — parse_clause.c:2731
// ===========================================================================

/// Transform an ORDER BY clause (a list of `SortBy` nodes); returns a list of
/// `SortGroupClause` nodes, growing `targetlist` as needed.
pub fn transformSortClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    orderlist: &[SortBy<'mcx>],
    targetlist: &mut Vec<TargetEntry<'mcx>>,
    exprKind: ParseExprKind,
    useSQL99: bool,
) -> PgResult<Vec<SortGroupClause>> {
    let mut sortlist: Vec<SortGroupClause> = Vec::new();

    for sortby in orderlist.iter() {
        let sortby_node = sortby
            .node
            .as_deref()
            .ok_or_else(|| elog_error("transformSortClause: SortBy.node is NULL"))?;
        let tle_idx = if useSQL99 {
            findTargetlistEntrySQL99(mcx, pstate, sortby_node, targetlist, exprKind)?
        } else {
            findTargetlistEntrySQL92(mcx, pstate, sortby_node, targetlist, exprKind)?
        };

        addTargetToSortList(mcx, pstate, tle_idx, &mut sortlist, targetlist, sortby)?;
    }

    Ok(sortlist)
}

// ===========================================================================
// transformDistinctClause — parse_clause.c:2984
// ===========================================================================

/// Transform a DISTINCT clause.  `is_agg` only affects error phrasing.
pub fn transformDistinctClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    targetlist: &mut Vec<TargetEntry<'mcx>>,
    sortClause: &[SortGroupClause],
    is_agg: bool,
) -> PgResult<Vec<SortGroupClause>> {
    let mut result: Vec<SortGroupClause> = Vec::new();

    /*
     * The distinctClause is all ORDER BY items followed by all other
     * non-resjunk targetlist items.  No resjunk ORDER BY items allowed.
     */
    for scl in sortClause.iter() {
        let tle_idx = get_sortgroupclause_tle_idx(scl, targetlist)?;

        if targetlist[tle_idx].resjunk {
            let msg = if is_agg {
                "in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list"
            } else {
                "for SELECT DISTINCT, ORDER BY expressions must appear in select list"
            };
            let loc = expr_location(Some(tle_expr(&targetlist[tle_idx])))?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
                .errmsg(msg)
                .errposition(errpos(pstate, loc))
                .into_error());
        }
        result.push(*scl);
    }

    /*
     * Now add any remaining non-resjunk tlist items.
     */
    for idx in 0..targetlist.len() {
        if targetlist[idx].resjunk {
            continue; /* ignore junk */
        }
        addTargetToGroupList(mcx, pstate, idx, &mut result, targetlist)?;
    }

    /*
     * Complain if we found nothing to make DISTINCT.
     */
    if result.is_empty() {
        let msg = if is_agg {
            "an aggregate with DISTINCT must have at least one argument"
        } else {
            "SELECT DISTINCT must have at least one column"
        };
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(msg)
            .into_error());
    }

    Ok(result)
}

// ===========================================================================
// transformDistinctOnClause — parse_clause.c:3068
// ===========================================================================

/// Transform a DISTINCT ON clause.  `distinctlist` is a list of (untransformed)
/// expression nodes.
pub fn transformDistinctOnClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    distinctlist: &[NodePtr<'mcx>],
    targetlist: &mut Vec<TargetEntry<'mcx>>,
    sortClause: &[SortGroupClause],
) -> PgResult<Vec<SortGroupClause>> {
    let mut result: Vec<SortGroupClause> = Vec::new();
    let mut sortgrouprefs: Vec<Index> = Vec::new();

    /*
     * Add all the DISTINCT ON expressions to the tlist, assign sortgroupref
     * numbers, and make a list of them (in DISTINCT ON list order).
     */
    for dexpr in distinctlist.iter() {
        let tle_idx = findTargetlistEntrySQL92(mcx, pstate, dexpr, targetlist, EXPR_KIND_DISTINCT_ON)?;
        let sortgroupref = assignSortGroupRef(tle_idx, targetlist);
        sortgrouprefs.push(sortgroupref);
    }

    /*
     * If both DISTINCT ON and ORDER BY are written, adopt sorting semantics and
     * column ordering from the matching ORDER BY items, which must precede the
     * rest.
     */
    let mut skipped_sortitem = false;
    for scl in sortClause.iter() {
        if sortgrouprefs.contains(&scl.tleSortGroupRef) {
            if skipped_sortitem {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
                    .errmsg("SELECT DISTINCT ON expressions must match initial ORDER BY expressions")
                    .errposition(errpos(
                        pstate,
                        get_matching_location(scl.tleSortGroupRef, &sortgrouprefs, distinctlist)?,
                    ))
                    .into_error());
            } else {
                result.push(*scl);
            }
        } else {
            skipped_sortitem = true;
        }
    }

    /*
     * Now add any remaining DISTINCT ON items, using default sort/group
     * semantics.
     */
    for (dexpr, &sortgroupref) in distinctlist.iter().zip(sortgrouprefs.iter()) {
        let tle_idx = targetlist
            .iter()
            .position(|t| t.ressortgroupref == sortgroupref)
            .ok_or_else(|| elog_error("DISTINCT ON: sortgroupref not in targetlist"))?;

        if targetIsInSortList(&targetlist[tle_idx], InvalidOid, &result) {
            continue; /* already in list (with some semantics) */
        }
        if skipped_sortitem {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
                .errmsg("SELECT DISTINCT ON expressions must match initial ORDER BY expressions")
                .errposition(errpos(pstate, node_expr_location(dexpr)?))
                .into_error());
        }
        addTargetToGroupList(mcx, pstate, tle_idx, &mut result, targetlist)?;
    }

    /* An empty result list is impossible here because of grammar restrictions. */
    debug_assert!(!result.is_empty());

    Ok(result)
}

// ===========================================================================
// get_matching_location — parse_clause.c:3175
// ===========================================================================

/// Get the exprLocation of the exprs member corresponding to the (first) member
/// of sortgrouprefs that equals sortgroupref.
fn get_matching_location<'mcx>(
    sortgroupref: Index,
    sortgrouprefs: &[Index],
    exprs: &[NodePtr<'mcx>],
) -> PgResult<i32> {
    for (lcs, lce) in sortgrouprefs.iter().zip(exprs.iter()) {
        if *lcs == sortgroupref {
            return node_expr_location(lce);
        }
    }
    /* if no match, caller blew it */
    Err(elog_error("get_matching_location: no matching sortgroupref"))
}

// ===========================================================================
// addTargetToSortList — parse_clause.c:3458
// ===========================================================================

/// If the given targetlist entry isn't already in the SortGroupClause list,
/// add it, using the given sort ordering info.
pub fn addTargetToSortList<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    tle_idx: usize,
    sortlist: &mut Vec<SortGroupClause>,
    targetlist: &mut Vec<TargetEntry<'mcx>>,
    sortby: &SortBy<'mcx>,
) -> PgResult<()> {
    let mut restype = expr_type(Some(tle_expr(&targetlist[tle_idx])))?;

    /* if tlist item is an UNKNOWN literal, change it to TEXT */
    if restype == UNKNOWNOID {
        let expr = tle_expr(&targetlist[tle_idx]).clone();
        let coerced = backend_parser_coerce::coerce_type(
            mcx,
            Some(pstate),
            Some(expr),
            restype,
            TEXTOID,
            -1,
            COERCION_IMPLICIT,
            COERCE_IMPLICIT_CAST,
            -1,
        )?
        .ok_or_else(|| elog_error("addTargetToSortList: coerce_type returned NULL"))?;
        targetlist[tle_idx].expr = Some(alloc_in(mcx, coerced)?);
        restype = TEXTOID;
    }

    /* determine the sortop, eqop, and directionality */
    let sortop: Oid;
    let eqop: Oid;
    let hashable: bool;
    let reverse: bool;

    match sortby.sortby_dir {
        SortByDir::SORTBY_DEFAULT | SortByDir::SORTBY_ASC => {
            let ops = get_sort_group_operators(restype, true, true, false, false)?;
            sortop = ops.lt_opr;
            eqop = ops.eq_opr;
            hashable = ops.is_hashable;
            reverse = false;
        }
        SortByDir::SORTBY_DESC => {
            let ops = get_sort_group_operators(restype, false, true, true, false)?;
            sortop = ops.gt_opr;
            eqop = ops.eq_opr;
            hashable = ops.is_hashable;
            reverse = true;
        }
        SortByDir::SORTBY_USING => {
            debug_assert!(!sortby.useOp.is_empty());
            let useop = opname_strings(&sortby.useOp);
            sortop = compatible_oper_opid(&useop, restype, restype, false)?;

            /*
             * Verify it's a valid ordering operator, fetch the equality
             * operator, and decide ASC-vs-DESC handling.
             */
            let (e, rev) = lsyscache::get_equality_op_for_ordering_op::call(sortop)?
                .unwrap_or((InvalidOid, false));
            eqop = e;
            reverse = rev;
            if !OidIsValid(eqop) {
                let opname = String::from(sortby.useOp.last().and_then(|n| str_val(n)).unwrap_or(""));
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg(alloc::format!(
                        "operator {} is not a valid ordering operator",
                        opname
                    ))
                    .errhint(
                        "Ordering operators must be \"<\" or \">\" members of btree operator families.",
                    )
                    .into_error());
            }

            /* Also see if the equality operator is hashable. */
            hashable = lsyscache::op_hashjoinable::call(eqop, restype)?;
        }
    }

    /* avoid making duplicate sortlist entries */
    if !targetIsInSortList(&targetlist[tle_idx], sortop, sortlist) {
        let tle_sort_group_ref = assignSortGroupRef(tle_idx, targetlist);
        let nulls_first = match sortby.sortby_nulls {
            // NULLS FIRST is default for DESC; other way for ASC
            SortByNulls::SORTBY_NULLS_DEFAULT => reverse,
            SortByNulls::SORTBY_NULLS_FIRST => true,
            SortByNulls::SORTBY_NULLS_LAST => false,
        };

        let sortcl = SortGroupClause {
            tleSortGroupRef: tle_sort_group_ref,
            eqop,
            sortop,
            reverse_sort: reverse,
            nulls_first,
            hashable,
        };

        sortlist.push(sortcl);
    }

    Ok(())
}

// ===========================================================================
// addTargetToGroupList — parse_clause.c:3536
// ===========================================================================

/// If the given targetlist entry isn't already in the SortGroupClause list, add
/// it, using default sort/group semantics.
fn addTargetToGroupList<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    tle_idx: usize,
    grouplist: &mut Vec<SortGroupClause>,
    targetlist: &mut Vec<TargetEntry<'mcx>>,
) -> PgResult<()> {
    let mut restype = expr_type(Some(tle_expr(&targetlist[tle_idx])))?;

    /* if tlist item is an UNKNOWN literal, change it to TEXT */
    if restype == UNKNOWNOID {
        let expr = tle_expr(&targetlist[tle_idx]).clone();
        let coerced = backend_parser_coerce::coerce_type(
            mcx,
            Some(pstate),
            Some(expr),
            restype,
            TEXTOID,
            -1,
            COERCION_IMPLICIT,
            COERCE_IMPLICIT_CAST,
            -1,
        )?
        .ok_or_else(|| elog_error("addTargetToGroupList: coerce_type returned NULL"))?;
        targetlist[tle_idx].expr = Some(alloc_in(mcx, coerced)?);
        restype = TEXTOID;
    }

    /* avoid making duplicate grouplist entries */
    if !targetIsInSortList(&targetlist[tle_idx], InvalidOid, grouplist) {
        /* determine the eqop and optional sortop */
        let ops = get_sort_group_operators(restype, false, true, false, false)?;

        let tle_sort_group_ref = assignSortGroupRef(tle_idx, targetlist);
        let grpcl = SortGroupClause {
            tleSortGroupRef: tle_sort_group_ref,
            eqop: ops.eq_opr,
            sortop: ops.lt_opr,
            reverse_sort: false, /* sortop is "less than", or InvalidOid */
            nulls_first: false,  /* OK with or without sortop */
            hashable: ops.is_hashable,
        };

        grouplist.push(grpcl);
    }

    Ok(())
}

// ===========================================================================
// assignSortGroupRef — parse_clause.c:3593
// ===========================================================================

/// Assign the targetentry an unused ressortgroupref if it doesn't already have
/// one.  Return the assigned or pre-existing refnumber.
pub fn assignSortGroupRef(tle_idx: usize, targetlist: &mut [TargetEntry<'_>]) -> Index {
    if targetlist[tle_idx].ressortgroupref != 0 {
        return targetlist[tle_idx].ressortgroupref;
    }

    /* easiest way to pick an unused refnumber: max used + 1 */
    let mut max_ref: Index = 0;
    for tle in targetlist.iter() {
        if tle.ressortgroupref > max_ref {
            max_ref = tle.ressortgroupref;
        }
    }

    targetlist[tle_idx].ressortgroupref = max_ref + 1;
    targetlist[tle_idx].ressortgroupref
}

// ===========================================================================
// targetIsInSortList — parse_clause.c:3634
// ===========================================================================

/// Is the given target item already in the sortlist?  If sortop is not
/// InvalidOid, also test for a match to the sortop (or its commutator).
///
/// It is not an oversight that this function ignores the nulls_first flag.
pub fn targetIsInSortList(
    tle: &TargetEntry<'_>,
    sortop: Oid,
    sortList: &[SortGroupClause],
) -> bool {
    let r = tle.ressortgroupref;

    /* no need to scan list if tle has no marker */
    if r == 0 {
        return false;
    }

    for scl in sortList.iter() {
        if scl.tleSortGroupRef == r
            && (sortop == InvalidOid
                || sortop == scl.sortop
                || sortop == get_commutator_or_invalid(scl.sortop))
        {
            return true;
        }
    }
    false
}

/// `get_commutator(sortop)` — the C compares against it inside the `||` chain;
/// a cache-path error there is unexpected, so collapse to `InvalidOid` (which
/// never matches a valid `sortop`), preserving the C short-circuit behavior.
fn get_commutator_or_invalid(opno: Oid) -> Oid {
    lsyscache::get_commutator::call(opno).unwrap_or(InvalidOid)
}

// ===========================================================================
// Internal node / list helpers
// ===========================================================================

/// `leftmostLoc(loc1, loc2)` (nodeFuncs.c).
fn leftmost_loc(loc1: i32, loc2: i32) -> i32 {
    if loc1 < 0 {
        loc2
    } else if loc2 < 0 {
        loc1
    } else {
        loc1.min(loc2)
    }
}

/// `exprLocation((Node *) node)` (nodeFuncs.c) over a raw-grammar [`Node`].
///
/// The repo splits `exprLocation` so that
/// [`backend_nodes_core::nodefuncs::expr_location`] only handles the typed
/// [`Expr`] arms; the GROUP BY / DISTINCT ON items reaching the clause core are
/// still raw grammar nodes, so the raw-`Node` arms of the C `exprLocation`
/// switch are ported here directly (delegating to `expr_location` for an already
/// typed [`Node::Expr`]).
fn node_expr_location(node: &Node<'_>) -> PgResult<i32> {
    let loc = match node {
        Node::ColumnRef(c) => c.location,
        Node::ParamRef(p) => p.location,
        Node::A_Const(a) => a.location,
        Node::A_Expr(a) => {
            // leftmost of operator or left operand (if any)
            leftmost_loc(a.location, opt_node_expr_location(a.lexpr.as_deref())?)
        }
        Node::FuncCall(fc) => {
            // consider both function name and leftmost arg
            leftmost_loc(fc.location, list_exprLocation(&fc.args)?)
        }
        Node::A_ArrayExpr(a) => a.location,
        Node::TypeCast(tc) => {
            let mut loc = opt_node_expr_location(tc.arg.as_deref())?;
            if let Some(tn) = tc.typeName.as_deref() {
                loc = leftmost_loc(loc, tn.location);
            }
            leftmost_loc(loc, tc.location)
        }
        Node::CollateClause(c) => opt_node_expr_location(c.arg.as_deref())?,
        Node::SortBy(s) => opt_node_expr_location(s.node.as_deref())?,
        Node::A_Indirection(a) => opt_node_expr_location(a.arg.as_deref())?,
        Node::GroupingSet(g) => g.location,
        Node::TypeName(t) => t.location,
        Node::RowExpr(r) => r.location,
        Node::List(list) => list_exprLocation(list)?,
        // Typed expression leaf: delegate to the nodefuncs implementation.
        Node::Expr(e) => expr_location(Some(e))?,
        // Value nodes and other locationless raw kinds: unknown (C `-1`).
        _ => -1,
    };
    Ok(loc)
}

/// `exprLocation((Node *) opt)` for an optional child pointer.
fn opt_node_expr_location(node: Option<&Node<'_>>) -> PgResult<i32> {
    match node {
        Some(n) => node_expr_location(n),
        None => Ok(-1),
    }
}

/// `exprLocation((Node *) list)` over a `List *` (the `T_List` arm of
/// `exprLocation`: the first member with `loc >= 0`).
fn list_exprLocation(list: &[NodePtr<'_>]) -> PgResult<i32> {
    let mut loc: i32 = -1;
    for n in list.iter() {
        loc = node_expr_location(n)?;
        if loc >= 0 {
            break;
        }
    }
    Ok(loc)
}

/// Convert a raw `List *opname` (a list of `String` value nodes) into the
/// `Vec<String>` `compatible_oper_opid` expects.
fn opname_strings(name: &mcx::PgVec<'_, NodePtr<'_>>) -> Vec<String> {
    let mut out = Vec::with_capacity(name.len());
    for n in name.iter() {
        if let Some(s) = str_val(n) {
            out.push(String::from(s));
        }
    }
    out
}

/// `get_sortgroupclause_tle(scl, targetList)` (tlist.c) reduced to the index:
/// find the targetlist entry whose `ressortgroupref` matches the clause's.
fn get_sortgroupclause_tle_idx(
    scl: &SortGroupClause,
    targetlist: &[TargetEntry<'_>],
) -> PgResult<usize> {
    targetlist
        .iter()
        .position(|t| t.ressortgroupref == scl.tleSortGroupRef && t.ressortgroupref != 0)
        .ok_or_else(|| elog_error("ORDER BY position not found in targetlist"))
}

/// `list_make1_int(ref)` for a SIMPLE grouping set's `content`: a `List *` of
/// `Integer` value nodes holding the ressortgrouprefs.
fn refs_to_int_pgvec<'mcx>(
    mcx: Mcx<'mcx>,
    refs: &[Index],
) -> PgResult<mcx::PgVec<'mcx, NodePtr<'mcx>>> {
    let mut v = mcx::vec_with_capacity_in::<NodePtr<'mcx>>(mcx, refs.len())?;
    for &r in refs.iter() {
        let cell = alloc_in(mcx, Node::Integer(Integer { ival: r as i32 }))?;
        v.try_reserve(1).map_err(|_| mcx.oom(0))?;
        v.push(cell);
    }
    Ok(v)
}

/// Move a `Vec<NodePtr>` into a context-allocated `PgVec<NodePtr>` (a `List *`).
fn nodes_into_pgvec<'mcx>(
    mcx: Mcx<'mcx>,
    nodes: Vec<NodePtr<'mcx>>,
) -> PgResult<mcx::PgVec<'mcx, NodePtr<'mcx>>> {
    let mut v = mcx::vec_with_capacity_in::<NodePtr<'mcx>>(mcx, nodes.len())?;
    for n in nodes.into_iter() {
        v.try_reserve(1).map_err(|_| mcx.oom(0))?;
        v.push(n);
    }
    Ok(v)
}

/// An empty `PgVec<NodePtr>` (the C `NIL`).
fn empty_pgvec<'mcx>(mcx: Mcx<'mcx>) -> PgResult<mcx::PgVec<'mcx, NodePtr<'mcx>>> {
    mcx::vec_with_capacity_in::<NodePtr<'mcx>>(mcx, 0)
}

/// This crate owns no inward seam: every `parse_clause.c` function in the F1
/// scope is a leaf consumer called directly by its (still-unported) callers
/// (`analyze.c`). The aggregator still invokes this so the crate participates in
/// the wiring discipline; it installs nothing.
pub fn init_seams() {
    backend_parser_clause_seams::transform_where_clause::set(transformWhereClause);
}

mod from_clause;
pub use from_clause::{
    setNamespaceLateralState, setTargetTable, transformFromClause,
};

mod window_conflict;
pub use window_conflict::{transformOnConflictArbiter, transformWindowDefinitions};

#[cfg(test)]
mod tests;
