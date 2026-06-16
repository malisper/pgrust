//! Returns-tuples / tuple-descriptor / contains-query predicates
//! (utility.c:2027-2206).
//!
//! The tag switches + the `ismove` / NULL guards are ported in-crate; the
//! portal / prepared-statement / explain / variable-show descriptor *sources*
//! cross into unported owners and are routed through
//! [`backend_tcop_utility_out_seams`] (aliased `rt`).

use backend_tcop_utility_out_seams as rt;
use types_error::PgResult;
use types_nodes::nodes::Node;
use types_nodes::nodes::{CMD_DELETE, CMD_INSERT, CMD_MERGE, CMD_SELECT, CMD_UTILITY};
use types_tuple::heaptuple::TupleDesc;

use crate::consts::RECORDOID;

/// `UtilityReturnsTuples` (utility.c:2027-2073) — true if this utility statement
/// will send output to the destination.
///
/// The tag switch + the `ismove` / NULL guards are in-crate; the portal /
/// prepared-statement lookups cross [`backend_tcop_utility_out_seams`] seams.
/// Pure classification over a well-formed node; cannot `ereport` (matches the
/// `utility_returns_tuples` inward-seam contract of `PgResult<bool>`).
pub fn UtilityReturnsTuples(parsetree: &Node) -> PgResult<bool> {
    let result = match parsetree {
        // case T_CallStmt: return (stmt->funcexpr->funcresulttype == RECORDOID);
        Node::CallStmt(stmt) => {
            // stmt->funcexpr is a FuncExpr once analyzed; reach it through the
            // Node::Expr(Expr::FuncExpr) arm. Any other shape is not a
            // record-returning CALL.
            match stmt.funcexpr.as_deref() {
                Some(Node::Expr(expr)) => match expr.as_funcexpr() {
                    Some(func) => func.funcresulttype == RECORDOID,
                    None => false,
                },
                _ => false,
            }
        }
        // case T_FetchStmt:
        Node::FetchStmt(stmt) => {
            if stmt.ismove {
                return Ok(false);
            }
            // portal = GetPortalByName(stmt->portalname);
            // if (!PortalIsValid(portal)) return false;
            // return portal->tupDesc ? true : false;
            // The lookup folds both the invalid-portal and the null-tupDesc
            // guards: a present, valid portal with a tuple descriptor yields
            // Some(desc); everything else yields None.
            match &stmt.portalname {
                Some(_) => rt::fetch_stmt_portal_tupdesc::call(parsetree),
                None => false,
            }
        }
        // case T_ExecuteStmt:
        //   entry = FetchPreparedStatement(stmt->name, false);
        //   if (!entry) return false;
        //   if (entry->plansource->resultDesc) return true;
        //   return false;
        Node::ExecuteStmt(_) => rt::execute_stmt_has_result::call(parsetree),
        // case T_ExplainStmt: return true;
        Node::ExplainStmt(_) => true,
        // case T_VariableShowStmt: return true;
        Node::VariableShowStmt(_) => true,
        // default: return false;
        _ => false,
    };
    Ok(result)
}

/// `UtilityTupleDescriptor` (utility.c:2083-2128) — the actual output tuple
/// descriptor for a utility statement for which [`UtilityReturnsTuples`]
/// previously returned `true` (or a NULL descriptor).
///
/// The returned descriptor is created in (or copied into) `mcx`. The repo's
/// `TupleDesc<'mcx>` is itself an `Option<PgBox<…>>`, so the C `NULL` return
/// maps directly to `None` — no out-of-band channel is needed.
///
/// The tag switch + the `ismove` guard are in-crate; each descriptor *source*
/// is external and crosses a seam (allocating in `mcx` where required).
pub fn UtilityTupleDescriptor<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    parsetree: &Node<'mcx>,
) -> PgResult<TupleDesc<'mcx>> {
    let desc: TupleDesc<'mcx> = match parsetree {
        // case T_CallStmt: return CallStmtResultDesc((CallStmt *) parsetree);
        Node::CallStmt(_) => rt::call_stmt_result_desc::call(mcx, parsetree),
        // case T_FetchStmt:
        Node::FetchStmt(stmt) => {
            if stmt.ismove {
                return Ok(None);
            }
            // portal = GetPortalByName(stmt->portalname);
            // if (!PortalIsValid(portal)) return NULL;
            // return CreateTupleDescCopy(portal->tupDesc);
            match &stmt.portalname {
                Some(_) => rt::fetch_stmt_result_desc::call(mcx, parsetree),
                None => None,
            }
        }
        // case T_ExecuteStmt:
        //   entry = FetchPreparedStatement(stmt->name, false);
        //   if (!entry) return NULL;
        //   return FetchPreparedStatementResultDesc(entry);
        Node::ExecuteStmt(_) => rt::execute_stmt_result_desc::call(mcx, parsetree),
        // case T_ExplainStmt: return ExplainResultDesc((ExplainStmt *) parsetree);
        Node::ExplainStmt(_) => rt::explain_result_desc::call(mcx, parsetree),
        // case T_VariableShowStmt: return GetPGVariableResultDesc(n->name);
        Node::VariableShowStmt(n) => {
            rt::get_pg_variable_result_desc::call(mcx, n.name.as_deref())
        }
        // default: return NULL;
        _ => None,
    };
    Ok(desc)
}

/// `QueryReturnsTuples` (utility.c:2136-2160, `#ifdef NOT_USED`) — true if this
/// `Query` will send output to the destination. Ported for completeness; the C
/// definition is compiled out behind `NOT_USED`.
pub fn QueryReturnsTuples<'mcx>(
    parsetree: &types_nodes::copy_query::Query<'mcx>,
) -> PgResult<bool> {
    let result = match parsetree.commandType {
        // case CMD_SELECT: return true;  /* returns tuples */
        CMD_SELECT => true,
        // case CMD_INSERT/UPDATE/DELETE/MERGE: the forms with RETURNING return
        // tuples; otherwise fall through to the default `false`.
        CMD_INSERT | types_nodes::nodes::CMD_UPDATE | CMD_DELETE | CMD_MERGE => {
            !parsetree.returningList.is_empty()
        }
        // case CMD_UTILITY: return UtilityReturnsTuples(parsetree->utilityStmt);
        CMD_UTILITY => match &parsetree.utilityStmt {
            Some(inner) => return UtilityReturnsTuples(inner),
            None => false,
        },
        // case CMD_UNKNOWN / CMD_NOTHING: probably shouldn't get here.
        _ => false, // default
    };
    Ok(result)
}

/// `UtilityContainsQuery` (utility.c:2178-2206) — return the contained `Query`
/// of an EXPLAIN / CREATE-TABLE-AS / DECLARE-CURSOR utility statement, or `None`.
///
/// We assume it is invoked only on already-parse-analyzed statements (so the
/// contained `query` is a [`Node::Query`]). Drills down through nested
/// utility-`Query` wrappers to a non-utility `Query`, matching C's
/// `castNode(Query, …)` recursion.
pub fn UtilityContainsQuery<'a, 'mcx>(parsetree: &'a Node<'mcx>) -> Option<&'a Node<'mcx>> {
    // switch (nodeTag(parsetree)): each of the three arms pulls out `->query`.
    let qry = match parsetree {
        Node::DeclareCursorStmt(stmt) => stmt.query.as_deref(),
        Node::ExplainStmt(stmt) => stmt.query.as_deref(),
        Node::CreateTableAsStmt(stmt) => stmt.query.as_deref(),
        // default: return NULL;
        _ => return None,
    };

    // `castNode(Query, …)`: the analyzed contained statement is a Query.
    match qry {
        Some(node) => match node.as_query() {
            Some(q) => {
                if q.commandType == CMD_UTILITY {
                    // return UtilityContainsQuery(qry->utilityStmt);
                    match q.utilityStmt.as_deref() {
                        Some(inner) => UtilityContainsQuery(inner),
                        None => None,
                    }
                } else {
                    // return qry;
                    Some(node)
                }
            }
            // Pre-analysis (or an unexpected shape): no contained Query yet.
            None => None,
        },
        None => None,
    }
}
