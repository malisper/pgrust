//! `transformIndexStmt` / `transformStatsStmt` (`parse_utilcmd.c`).
//!
//! Both open the parent relation by OID, add it to the rtable so expressions can
//! refer to its columns without qualification, transform the WHERE predicate /
//! index-element / stat expressions, fix their collations, and then check that
//! only the base rel is mentioned. The relation is threaded as an owned
//! [`Relation<'mcx>`] carrier (RAII drop releases the relcache reference; the
//! lock is the caller's responsibility, so we open with `NoLock`).

use mcx::{Mcx, PgString};

use utils_error::ereport;
use types_core::Oid;
use types_error::{PgResult, ERRCODE_INVALID_COLUMN_REFERENCE, ERROR};
use types_storage::lock::{AccessShareLock, NoLock};

use nodes::ddlnodes::{IndexElem, StatsElem};
use nodes::nodes::Node;
use nodes::parsestmt::ParseExprKind::{
    EXPR_KIND_INDEX_EXPRESSION, EXPR_KIND_INDEX_PREDICATE, EXPR_KIND_STATS_EXPRESSION,
};

use common_relation::relation_open;
use table::table_close;
use clause::transformWhereClause;
use parse_collate::assign_expr_collations;
use parse_expr::transformExpr;
use parse_target::FigureIndexColname;
use parser_relation::{addNSItemToQuery, addRangeTableEntryForRelation};
use small1::{free_parsestate, make_parsestate};

use crate::core::NodePtr;

/// `transformIndexStmt(relid, stmt, queryString)` — parse analysis for CREATE
/// INDEX / ALTER TABLE.
pub fn transformIndexStmt<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    stmt: NodePtr<'mcx>,
    query_string: &str,
) -> PgResult<NodePtr<'mcx>> {
    let stmt_node = mcx::PgBox::into_inner(stmt);
    let stmt_tag = stmt_node.node_tag();
    let mut stmt = match stmt_node.into_indexstmt() {
        Some(s) => s,
        None => unreachable!("transformIndexStmt: not an IndexStmt node: {}", stmt_tag),
    };

    // Nothing to do if statement already transformed.
    if stmt.transformed {
        return mcx::alloc_in(mcx, Node::mk_index_stmt(mcx, stmt)?);
    }

    // Set up pstate.
    let mut pstate = make_parsestate(mcx, None)?;
    pstate.p_sourcetext = Some(PgString::from_str_in(query_string, mcx)?);

    // Put the parent table into the rtable so that the expressions can refer to
    // its fields without qualification. Caller is responsible for locking the
    // relation, but we still need to open it.
    let rel = relation_open(mcx, relid, NoLock)?;
    let nsitem =
        addRangeTableEntryForRelation(mcx, &mut pstate, &rel, AccessShareLock, None, false, true)?;

    // no to join list, yes to namespaces
    addNSItemToQuery(mcx, &mut pstate, nsitem, false, true, true)?;

    // take care of the where clause
    if stmt.whereClause.is_some() {
        let clause = stmt.whereClause.take().map(|n| mcx::PgBox::into_inner(n));
        let where_expr =
            transformWhereClause(mcx, &mut pstate, clause, EXPR_KIND_INDEX_PREDICATE, "WHERE")?;
        // Bring the parser-arena `'static` qual into `mcx` for the in-place
        // collation pass and the `'mcx` Node wrap (`Expr` is invariant).
        let mut where_expr: Option<nodes::primnodes::Expr<'mcx>> = match where_expr {
            Some(e) => Some(e.clone_in(mcx)?),
            None => None,
        };
        // we have to fix its collations too
        if let Some(e) = where_expr.as_mut() {
            assign_expr_collations(Some(&pstate), e)?;
        }
        stmt.whereClause = match where_expr {
            Some(e) => Some(mcx::alloc_in(mcx, Node::mk_expr(mcx, e)?)?),
            None => None,
        };
    }

    // take care of any index expressions
    for param in stmt.indexParams.iter_mut() {
        let Some(ielem) = param.as_mut().as_indexelem_mut() else {
            continue;
        };
        transform_index_elem_expr(mcx, &mut pstate, ielem)?;
    }

    // Check that only the base rel is mentioned. (This should be dead code now
    // that add_missing_from is history.)
    if pstate.p_rtable.len() != 1 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg(
                "index expressions and predicates can refer only to the table being indexed",
            )
            .into_error());
    }

    free_parsestate(pstate)?;

    // Close relation. (The owned carrier closes with NoLock on drop.)
    table_close(rel, NoLock)?;

    // Mark statement as successfully transformed.
    stmt.transformed = true;

    mcx::alloc_in(mcx, Node::mk_index_stmt(mcx, stmt)?)
}

/// The per-`IndexElem` expression transform from `transformIndexStmt`.
fn transform_index_elem_expr<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut nodes::parsestmt::ParseState<'mcx>,
    ielem: &mut IndexElem<'mcx>,
) -> PgResult<()> {
    if ielem.expr.is_none() {
        return Ok(());
    }

    // Extract preliminary index col name before transforming expr.
    if ielem.indexcolname.is_none() {
        if let Some(name) = FigureIndexColname(ielem.expr.as_deref()) {
            ielem.indexcolname = Some(PgString::from_str_in(&name, mcx)?);
        }
    }

    // Now do parse transformation of the expression.
    let expr = ielem.expr.take().map(|n| mcx::PgBox::into_inner(n));
    let t = transformExpr(pstate, expr, EXPR_KIND_INDEX_EXPRESSION)?;
    // Bring the parser-arena `'static` result into `mcx` for the in-place
    // collation pass and the `'mcx` Node wrap (`Expr` is invariant).
    let mut t: Option<nodes::primnodes::Expr<'mcx>> = match t {
        Some(e) => Some(e.clone_in(mcx)?),
        None => None,
    };

    // We have to fix its collations too.
    if let Some(e) = t.as_mut() {
        assign_expr_collations(Some(pstate), e)?;
    }
    ielem.expr = match t {
        Some(e) => Some(mcx::alloc_in(mcx, Node::mk_expr(mcx, e)?)?),
        None => None,
    };
    Ok(())
}

/// `transformStatsStmt(relid, stmt, queryString)` — parse analysis for CREATE
/// STATISTICS.
pub fn transformStatsStmt<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    stmt: NodePtr<'mcx>,
    query_string: &str,
) -> PgResult<NodePtr<'mcx>> {
    let stmt_node = mcx::PgBox::into_inner(stmt);
    let stmt_tag = stmt_node.node_tag();
    let mut stmt = match stmt_node.into_createstatsstmt() {
        Some(s) => s,
        None => {
            unreachable!("transformStatsStmt: not a CreateStatsStmt node: {}", stmt_tag)
        }
    };

    // Nothing to do if statement already transformed.
    if stmt.transformed {
        return mcx::alloc_in(mcx, Node::mk_create_stats_stmt(mcx, stmt)?);
    }

    // Set up pstate.
    let mut pstate = make_parsestate(mcx, None)?;
    pstate.p_sourcetext = Some(PgString::from_str_in(query_string, mcx)?);

    // Put the parent table into the rtable. Caller is responsible for locking
    // the relation, but we still need to open it.
    let rel = relation_open(mcx, relid, NoLock)?;
    let nsitem =
        addRangeTableEntryForRelation(mcx, &mut pstate, &rel, AccessShareLock, None, false, true)?;

    // no to join list, yes to namespaces
    addNSItemToQuery(mcx, &mut pstate, nsitem, false, true, true)?;

    // take care of any expressions
    for expr_node in stmt.exprs.iter_mut() {
        let Some(selem) = expr_node.as_mut().as_statselem_mut() else {
            continue;
        };
        transform_stats_elem_expr(mcx, &mut pstate, selem)?;
    }

    // Check that only the base rel is mentioned.
    if pstate.p_rtable.len() != 1 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg("statistics expressions can refer only to the table being referenced")
            .into_error());
    }

    free_parsestate(pstate)?;

    // Close relation.
    table_close(rel, NoLock)?;

    // Mark statement as successfully transformed.
    stmt.transformed = true;

    mcx::alloc_in(mcx, Node::mk_create_stats_stmt(mcx, stmt)?)
}

/// The per-`StatsElem` expression transform from `transformStatsStmt`.
fn transform_stats_elem_expr<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut nodes::parsestmt::ParseState<'mcx>,
    selem: &mut StatsElem<'mcx>,
) -> PgResult<()> {
    if selem.expr.is_none() {
        return Ok(());
    }
    // Now do parse transformation of the expression.
    let expr = selem.expr.take().map(|n| mcx::PgBox::into_inner(n));
    let t = transformExpr(pstate, expr, EXPR_KIND_STATS_EXPRESSION)?;
    // Bring the parser-arena `'static` result into `mcx` for the in-place
    // collation pass and the `'mcx` Node wrap (`Expr` is invariant).
    let mut t: Option<nodes::primnodes::Expr<'mcx>> = match t {
        Some(e) => Some(e.clone_in(mcx)?),
        None => None,
    };

    // We have to fix its collations too.
    if let Some(e) = t.as_mut() {
        assign_expr_collations(Some(pstate), e)?;
    }
    selem.expr = match t {
        Some(e) => Some(mcx::alloc_in(mcx, Node::mk_expr(mcx, e)?)?),
        None => None,
    };
    Ok(())
}
