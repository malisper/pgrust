//! `parser/analyze.c` — transform a raw parse tree into an analyzed
//! `Query` tree.
//!
//! Milestone scope (Workstream-A): the SELECT path end to end — the
//! `parse_analyze_*` drivers, `transformStmt` dispatch, `transformTopLevelStmt`,
//! `transformOptionalSelectInto`, `parse_sub_analyze`, `transformSelectStmt`,
//! the FOR UPDATE/SHARE locking family, and the `*_requires_*` predicates.
//! SQL text -> `raw_parser` -> `transformStmt` -> an owned, walkable
//! `types_nodes::copy_query::Query<'mcx>`.
//!
//! VALUES, set-operations, and the DML statements (INSERT/UPDATE/DELETE/MERGE,
//! RETURN, PL/pgSQL assignment, DECLARE CURSOR, EXPLAIN, CREATE TABLE AS, CALL)
//! are a follow-on family — they dispatch through `transformStmt` to a
//! seam-and-panic until their decomposition lands (see the crate notes).

#![allow(non_snake_case)]

extern crate alloc;

use alloc::vec::Vec;

use backend_utils_error::ereport;
use mcx::{Mcx, PgBox, PgVec};
use types_error::{PgResult, ERROR};
use types_nodes::copy_query::{Query, QuerySource};
use types_nodes::nodes::{CmdType, Node, NodePtr};
use types_nodes::parsestmt::{ParseState, RawStmt};
use types_nodes::rawnodes::SelectStmt;

mod locking;
mod select;
mod setop;

pub use locking::{applyLockingClause, transformLockingClause, CheckSelectLocking, LCS_asString};

/// `ereport(ERROR, errmsg_internal(...))` shorthand for the panics-as-errors in
/// logic this unit owns.
fn elog_error(msg: impl Into<alloc::string::String>) -> types_error::PgError {
    ereport(ERROR).errmsg_internal(msg.into()).into_error()
}

/* ===========================================================================
 * Entry points: parse_analyze_*
 * =========================================================================== */

/// `parse_analyze_fixedparams(parseTree, sourceText, paramTypes, numParams,
/// queryEnv)` — analyze a raw statement with the given fixed parameter types.
///
/// In the milestone scope the COPY/PREPARE drivers pass no parameters and a
/// `None` query environment; `setup_parse_fixed_parameters` is applied when
/// `param_types` is non-empty (delegated to the small1 param owner).
pub fn parse_analyze_fixedparams<'mcx>(
    mcx: Mcx<'mcx>,
    parse_tree: &RawStmt<'mcx>,
    source_text: &str,
    param_types: &[types_core::primitive::Oid],
) -> PgResult<Query<'mcx>> {
    let mut pstate = backend_parser_small1::make_parsestate(mcx, None)?;

    pstate.p_sourcetext = Some(mcx::PgString::from_str_in(source_text, mcx)?);

    if !param_types.is_empty() {
        // setup_parse_fixed_parameters(pstate, paramTypes, numParams) installs
        // the fixed paramref hook + ref-hook state on the ParseState. small1's
        // owned-model port returns a `FixedParamState` carrier instead of
        // mutating the ParseState (the owned ParseState cannot hold the
        // borrowing hook); wiring that carrier into the owned ParseState is the
        // small1 param-hook follow-on (cf. small1's F3 var-param mirror note).
        // No milestone consumer passes parameters (COPY/PREPARE SELECT pass an
        // empty paramTypes), so mirror-PG-and-panic until that follow-on lands.
        let _ = backend_parser_small1::setup_parse_fixed_parameters(param_types);
        panic!(
            "parse_analyze_fixedparams with parameters needs the small1 \
             owned-model param-hook wiring (setup_parse_fixed_parameters returns \
             a carrier, not a ParseState mutation)"
        );
    }

    let query = transformTopLevelStmt(mcx, &mut pstate, parse_tree)?;

    // IsQueryIdEnabled() -> JumbleQuery(query): query-id jumbling is a separate
    // unported subsystem; the hook (post_parse_analyze_hook) is NULL by default.
    // pgstat_report_query_id is a no-op for queryId == 0. None of these change
    // the returned Query in the default configuration.

    backend_parser_small1::free_parsestate(pstate)?;

    Ok(query)
}

/* ===========================================================================
 * parse_sub_analyze
 * =========================================================================== */

/// `parse_sub_analyze(parseTree, parentParseState, parentCTE,
/// locked_from_parent, resolve_unknowns)` — recursively analyze a sub-statement
/// in a child `ParseState` built off `parent_pstate`. Returns the resulting
/// `Query` wrapped as `Node::Query` (C `(Node *) query`), the contract the
/// parse_cte / parse_clause consumers read.
pub fn parse_sub_analyze<'mcx>(
    mcx: Mcx<'mcx>,
    parse_tree: &Node<'mcx>,
    parent_pstate: &mut ParseState<'mcx>,
    parent_cte: Option<&types_nodes::rawnodes::CommonTableExpr<'mcx>>,
    locked_from_parent: bool,
    resolve_unknowns: bool,
) -> PgResult<PgBox<'mcx, Node<'mcx>>> {
    let mut pstate = backend_parser_small1::make_parsestate(mcx, Some(parent_pstate))?;

    pstate.p_parent_cte = match parent_cte {
        Some(c) => Some(mcx::alloc_in(mcx, c.clone_in(mcx)?)?),
        None => None,
    };
    pstate.p_locked_from_parent = locked_from_parent;
    pstate.p_resolve_unknowns = resolve_unknowns;

    let query = transformStmt(mcx, &mut pstate, parse_tree)?;

    backend_parser_small1::free_parsestate(pstate)?;

    mcx::alloc_in(mcx, Node::Query(query))
}

/* ===========================================================================
 * transformTopLevelStmt / transformOptionalSelectInto
 * =========================================================================== */

/// `transformTopLevelStmt(pstate, parseTree)` — transform a `RawStmt` into a
/// `Query`, transferring statement-location data from the `RawStmt`.
pub fn transformTopLevelStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    parse_tree: &RawStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    /* We're at top level, so allow SELECT INTO */
    let mut result = transformOptionalSelectInto(mcx, pstate, &parse_tree.stmt)?;

    result.stmt_location = parse_tree.stmt_location;
    result.stmt_len = parse_tree.stmt_len;

    Ok(result)
}

/// `transformOptionalSelectInto(pstate, parseTree)` — if a top-level SELECT has
/// INTO, rewrite it to CREATE TABLE AS; otherwise transform unchanged.
fn transformOptionalSelectInto<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    parse_tree: &Node<'mcx>,
) -> PgResult<Query<'mcx>> {
    if let Node::SelectStmt(stmt) = parse_tree {
        /* drill down to leftmost SelectStmt of a set-op tree */
        let mut leaf = stmt;
        while leaf.op != types_nodes::rawnodes::SETOP_NONE {
            match leaf.larg.as_deref() {
                Some(l) => leaf = l,
                None => break,
            }
        }
        debug_assert!(leaf.larg.is_none());

        if leaf.intoClause.is_some() {
            // Build a CREATE TABLE AS wrapping a copy of the SELECT with the
            // INTO clause removed from its leftmost leaf, mirroring the C
            // in-place edit (we deep-copy because the input is borrowed).
            let mut select_copy = stmt.clone_in(mcx)?;
            clear_leftmost_into(&mut select_copy);

            let into = leaf.intoClause.as_ref().map(|i| i.clone_in(mcx)).transpose()?;
            let into = match into {
                Some(n) => Some(mcx::alloc_in(mcx, n)?),
                None => None,
            };

            let ctas = types_nodes::ddlnodes::CreateTableAsStmt {
                query: Some(mcx::alloc_in(mcx, Node::SelectStmt(select_copy))?),
                into,
                objtype: types_nodes::parsenodes::OBJECT_TABLE,
                is_select_into: true,
                if_not_exists: false,
            };
            let ctas_node = Node::CreateTableAsStmt(ctas);
            return transformStmt(mcx, pstate, &ctas_node);
        }
    }

    transformStmt(mcx, pstate, parse_tree)
}

/// Helper for the INTO rewrite: clear `intoClause` on the leftmost leaf of a
/// (possibly set-op) `SelectStmt`, matching the C `stmt->intoClause = NULL`.
fn clear_leftmost_into(stmt: &mut SelectStmt<'_>) {
    let mut cur = stmt;
    while cur.op != types_nodes::rawnodes::SETOP_NONE {
        match cur.larg.as_deref_mut() {
            Some(l) => cur = l,
            None => break,
        }
    }
    cur.intoClause = None;
}

/* ===========================================================================
 * transformStmt dispatch
 * =========================================================================== */

/// `transformStmt(pstate, parseTree)` — recursively transform a parse tree into
/// a `Query` tree.
pub fn transformStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    parse_tree: &Node<'mcx>,
) -> PgResult<Query<'mcx>> {
    let mut result: Query<'mcx> = match parse_tree {
        Node::SelectStmt(n) => {
            if !n.valuesLists.is_empty() {
                select::transformValuesClause(mcx, pstate, n)?
            } else if n.op == types_nodes::rawnodes::SETOP_NONE {
                select::transformSelectStmt(mcx, pstate, n)?
            } else {
                setop::transformSetOperationStmt(mcx, pstate, n)?
            }
        }
        Node::InsertStmt(_)
        | Node::DeleteStmt(_)
        | Node::UpdateStmt(_)
        | Node::MergeStmt(_)
        | Node::ReturnStmt(_)
        | Node::PLAssignStmt(_)
        | Node::DeclareCursorStmt(_)
        | Node::ExplainStmt(_)
        | Node::CreateTableAsStmt(_)
        | Node::CallStmt(_) => {
            // The DML / special-statement transforms are a follow-on family;
            // they are not reachable on the SELECT-milestone path. Mirror the C
            // dispatch and panic loudly until the family lands.
            panic!(
                "transformStmt: DML/special statement (tag {:?}) is in the \
                 follow-on family (transformInsert/Update/Delete/Merge/Return/\
                 PLAssign/DeclareCursor/Explain/CreateTableAs/Call) — not yet \
                 ported (analyze.c:312)",
                parse_tree.tag()
            );
        }
        other => {
            // Other statements don't require transformation: wrap a CMD_UTILITY
            // Query around the original parse tree.
            let mut q = Query::new(mcx);
            q.commandType = CmdType::CMD_UTILITY;
            q.utilityStmt = Some(mcx::alloc_in(mcx, other.clone_in(mcx)?)?);
            q
        }
    };

    /* Mark as original query until we learn differently */
    result.querySource = QuerySource::QSRC_ORIGINAL;
    result.canSetTag = true;

    Ok(result)
}

/* ===========================================================================
 * stmt_requires_parse_analysis / analyze_requires_snapshot /
 * query_requires_rewrite_plan
 * =========================================================================== */

/// `stmt_requires_parse_analysis(parseTree)` — true if parse analysis does
/// anything non-trivial (more than wrapping a CMD_UTILITY Query).
pub fn stmt_requires_parse_analysis(parse_tree: &RawStmt<'_>) -> bool {
    match parse_tree.stmt.as_ref() {
        Node::InsertStmt(_)
        | Node::DeleteStmt(_)
        | Node::UpdateStmt(_)
        | Node::MergeStmt(_)
        | Node::SelectStmt(_)
        | Node::ReturnStmt(_)
        | Node::PLAssignStmt(_)
        | Node::DeclareCursorStmt(_)
        | Node::ExplainStmt(_)
        | Node::CreateTableAsStmt(_)
        | Node::CallStmt(_) => true,
        _ => false,
    }
}

/// `analyze_requires_snapshot(parseTree)` — true if parse analysis requires a
/// snapshot to be set.
pub fn analyze_requires_snapshot(parse_tree: &RawStmt<'_>) -> bool {
    // The C function: result = stmt_requires_parse_analysis(parseTree). (The
    // historical special-casing of A_Expr etc. was removed; it now exactly
    // tracks stmt_requires_parse_analysis.)
    stmt_requires_parse_analysis(parse_tree)
}

/// `query_requires_rewrite_plan(query)` — true unless the Query is a no-op
/// CMD_UTILITY that the rewriter/planner ignore.
pub fn query_requires_rewrite_plan(query: &Query<'_>) -> bool {
    if query.commandType == CmdType::CMD_UTILITY {
        match query.utilityStmt.as_deref() {
            // These utility statements are optimizable through the
            // rewriter/planner (they embed an optimizable query).
            Some(Node::CreateTableAsStmt(_))
            | Some(Node::DeclareCursorStmt(_))
            | Some(Node::ExplainStmt(_))
            | Some(Node::CallStmt(_)) => true,
            _ => false,
        }
    } else {
        true
    }
}

/* ===========================================================================
 * Seam installation
 * =========================================================================== */

/// Install this crate's inward seams. Currently the cross-cycle consumer
/// contract is `parse_sub_analyze` (consumed by parse_cte and parse_clause).
pub fn init_seams() {
    backend_parser_analyze_seams::parse_sub_analyze::set(parse_sub_analyze);
}

/* ---- shared assembly helpers ---------------------------------------------- */

/// Wrap a `Vec<SortGroupClause>` (a dep's typed return) into the `List *` of
/// `Node`s the `Query` carries (`PgVec<NodePtr>`).
pub(crate) fn sgc_vec_to_nodes<'mcx>(
    mcx: Mcx<'mcx>,
    v: Vec<types_nodes::rawnodes::SortGroupClause>,
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    let mut out = mcx::vec_with_capacity_in(mcx, v.len())?;
    for sgc in v {
        out.push(mcx::alloc_in(mcx, Node::SortGroupClause(sgc))?);
    }
    Ok(out)
}

/// Wrap a `Vec<NodePtr>` (e.g. groupingSets) — already nodes — into a `PgVec`.
pub(crate) fn node_vec_to_pgvec<'mcx>(
    mcx: Mcx<'mcx>,
    v: Vec<NodePtr<'mcx>>,
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    let mut out = mcx::vec_with_capacity_in(mcx, v.len())?;
    for n in v {
        out.push(n);
    }
    Ok(out)
}

/// Wrap an optional `Expr` (a dep's typed clause return) into the `Node *`
/// (`Option<NodePtr>`) a `Query` carries.
pub(crate) fn opt_expr_to_node<'mcx>(
    mcx: Mcx<'mcx>,
    e: Option<types_nodes::primnodes::Expr>,
) -> PgResult<Option<NodePtr<'mcx>>> {
    match e {
        Some(expr) => Ok(Some(mcx::alloc_in(mcx, Node::Expr(expr))?)),
        None => Ok(None),
    }
}

/// Wrap an optional `Expr` (a dep's typed clause return) into the
/// concretely-typed `Option<PgBox<Expr>>` an expression-only `Query` field
/// (`havingQual`/`limitOffset`/`limitCount`/`mergeJoinCondition`) carries.
pub(crate) fn opt_expr_to_box<'mcx>(
    mcx: Mcx<'mcx>,
    e: Option<types_nodes::primnodes::Expr>,
) -> PgResult<Option<PgBox<'mcx, types_nodes::primnodes::Expr>>> {
    match e {
        Some(expr) => Ok(Some(mcx::alloc_in(mcx, expr)?)),
        None => Ok(None),
    }
}

/// Wrap a `PgVec<CommonTableExpr>` (transformWithClause return) into the
/// `cteList` (`PgVec<NodePtr>`).
pub(crate) fn cte_vec_to_nodes<'mcx>(
    mcx: Mcx<'mcx>,
    v: PgVec<'mcx, types_nodes::rawnodes::CommonTableExpr<'mcx>>,
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    let mut out = mcx::vec_with_capacity_in(mcx, v.len())?;
    for cte in v {
        out.push(mcx::alloc_in(mcx, Node::CommonTableExpr(cte))?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests;
