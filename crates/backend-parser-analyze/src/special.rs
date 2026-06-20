//! The DECLARE CURSOR and CALL path of `parser/analyze.c`:
//! `transformDeclareCursorStmt` (analyze.c:3017) and `transformCallStmt`
//! (analyze.c:3237).
//!
//! Both are represented as CMD_UTILITY `Query` nodes, but the contained query
//! / function-call must be transformed during parse analysis so parser-hook
//! side effects happen at the expected time.

use alloc::vec::Vec;

use mcx::Mcx;
use types_error::{PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_CURSOR_DEFINITION, ERROR};
use types_nodes::copy_query::Query;
use types_nodes::nodes::{CmdType, Node};
use types_nodes::parsestmt::{ParseExprKind, ParseState};
use types_nodes::portalcmds::{
    CURSOR_OPT_ASENSITIVE, CURSOR_OPT_HOLD, CURSOR_OPT_INSENSITIVE, CURSOR_OPT_NO_SCROLL,
    CURSOR_OPT_SCROLL,
};

use backend_utils_error::ereport;

use crate::elog_error;
use crate::transformStmt;

/// `transformDeclareCursorStmt(pstate, stmt)` (analyze.c:3017) — transform a
/// DECLARE CURSOR statement.
///
/// DECLARE CURSOR is like other utility statements in that we emit it as a
/// CMD_UTILITY `Query` node; however, we must first transform the contained
/// query (a SELECT), so that side effects of parser hooks happen at the
/// expected time.
pub fn transformDeclareCursorStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    stmt: &types_nodes::ddlnodes::DeclareCursorStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    if (stmt.options & CURSOR_OPT_SCROLL) != 0 && (stmt.options & CURSOR_OPT_NO_SCROLL) != 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_CURSOR_DEFINITION)
            /* translator: %s is a SQL keyword */
            .errmsg("cannot specify both SCROLL and NO SCROLL")
            .into_error());
    }

    if (stmt.options & CURSOR_OPT_ASENSITIVE) != 0 && (stmt.options & CURSOR_OPT_INSENSITIVE) != 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_CURSOR_DEFINITION)
            /* translator: %s is a SQL keyword */
            .errmsg("cannot specify both ASENSITIVE and INSENSITIVE")
            .into_error());
    }

    /* Transform contained query, not allowing SELECT INTO */
    let inner = stmt
        .query
        .as_deref()
        .ok_or_else(|| elog_error("DECLARE CURSOR: stmt->query is NULL"))?;
    let query = transformStmt(mcx, pstate, inner)?;

    /* Grammar should not have allowed anything but SELECT */
    if query.commandType != CmdType::CMD_SELECT {
        return Err(elog_error("unexpected non-SELECT command in DECLARE CURSOR"));
    }

    /*
     * We also disallow data-modifying WITH in a cursor.  (This could be
     * allowed, but the semantics of when the updates occur might be
     * surprising.)
     */
    if query.hasModifyingCTE {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("DECLARE CURSOR must not contain data-modifying statements in WITH")
            .into_error());
    }

    /* FOR UPDATE and WITH HOLD are not compatible */
    if !query.rowMarks.is_empty() && (stmt.options & CURSOR_OPT_HOLD) != 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(alloc::format!(
                "DECLARE CURSOR WITH HOLD ... {} is not supported",
                first_rowmark_strength(&query)?
            ))
            .errdetail("Holdable cursors must be READ ONLY.")
            .into_error());
    }

    /* FOR UPDATE and SCROLL are not compatible */
    if !query.rowMarks.is_empty() && (stmt.options & CURSOR_OPT_SCROLL) != 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(alloc::format!(
                "DECLARE SCROLL CURSOR ... {} is not supported",
                first_rowmark_strength(&query)?
            ))
            .errdetail("Scrollable cursors must be READ ONLY.")
            .into_error());
    }

    /* FOR UPDATE and INSENSITIVE are not compatible */
    if !query.rowMarks.is_empty() && (stmt.options & CURSOR_OPT_INSENSITIVE) != 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_CURSOR_DEFINITION)
            .errmsg(alloc::format!(
                "DECLARE INSENSITIVE CURSOR ... {} is not valid",
                first_rowmark_strength(&query)?
            ))
            .errdetail("Insensitive cursors must be READ ONLY.")
            .into_error());
    }

    /*
     * represent the command as a utility Query. The C edits stmt->query in
     * place; we rebuild the DeclareCursorStmt carrying the transformed inner
     * Query so the executor reads the analyzed query.
     */
    let mut new_stmt = stmt.clone_in(mcx)?;
    new_stmt.query = Some(mcx::alloc_in(mcx, Node::mk_query(mcx, query)?)?);

    let mut result = Query::new(mcx);
    result.commandType = CmdType::CMD_UTILITY;
    result.utilityStmt = Some(mcx::alloc_in(mcx, Node::mk_declare_cursor_stmt(mcx, new_stmt)?)?);
    Ok(result)
}

/// Return `LCS_asString(((RowMarkClause *) linitial(query->rowMarks))->strength)`
/// for the cursor-compatibility error messages.
fn first_rowmark_strength<'mcx>(query: &Query<'mcx>) -> PgResult<&'static str> {
    let rm = query
        .rowMarks
        .first()
        .and_then(|n| n.as_ref().as_rowmarkclause())
        .ok_or_else(|| elog_error("DECLARE CURSOR: rowMarks head is not a RowMarkClause"))?;
    Ok(crate::locking::LCS_asString(rm.strength))
}

/// `transformCallStmt(pstate, stmt)` (analyze.c:3237) — transform a CALL.
///
/// Does standard parse analysis on the procedure call and its arguments to
/// identify the procedure, expands named/default arguments, then splits the
/// argument list into input args (`fexpr->args`) and output args
/// (`stmt->outargs`) per `proargmodes`. Represented as a CMD_UTILITY `Query`.
pub fn transformCallStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    stmt: &types_nodes::ddlnodes::CallStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    use types_nodes::primnodes::Expr;

    let funccall_node = stmt
        .funccall
        .as_deref()
        .ok_or_else(|| elog_error("CALL: stmt->funccall is NULL"))?;
    let funccall = funccall_node
        .as_funccall()
        .ok_or_else(|| elog_error("CALL: stmt->funccall is not a FuncCall"))?
        .clone_in(mcx)?;

    /*
     * First, do standard parse analysis on the procedure call and its
     * arguments, allowing us to identify the called procedure.
     */
    let mut targs: Vec<Expr> = Vec::new();
    targs
        .try_reserve(funccall.args.len())
        .map_err(|_| mcx.oom(funccall.args.len()))?;
    for lc in funccall.args.iter() {
        let arg = backend_parser_parse_expr::transformExpr(
            pstate,
            Some(lc.as_ref().clone_in(mcx)?),
            ParseExprKind::EXPR_KIND_CALL_ARGUMENT,
        )?;
        if let Some(a) = arg {
            targs.push(a);
        }
    }

    let last_srf = backend_parser_parse_expr::last_srf_expr(pstate);
    let funcname = backend_parser_parse_expr::clone_namelist_pgstrings(&funccall.funcname, mcx)?;
    let node = backend_parser_func::ParseFuncOrColumn(
        pstate,
        &funcname,
        targs,
        last_srf.as_ref(),
        Some(&funccall),
        true, /* proc_call */
        funccall.location,
    )?;
    let mut node = node.ok_or_else(|| elog_error("CALL: ParseFuncOrColumn returned NULL"))?;

    backend_parser_parse_collate::assign_expr_collations(Some(pstate), &mut node)?;

    /* castNode(FuncExpr, node) */
    let mut fexpr = match node {
        Expr::FuncExpr(f) => f,
        _ => return Err(elog_error("CALL: ParseFuncOrColumn did not return a FuncExpr")),
    };

    /*
     * Expand the argument list to deal with named-argument notation and
     * default arguments. For ordinary FuncExprs this'd be done during
     * planning, but a CallStmt doesn't go through planning.
     *
     * SearchSysCache1(PROCOID, fexpr->funcid) — the get_func_form seam reads the
     * pg_proc row by-value (and Errs with the C "cache lookup failed" message).
     */
    let proc = backend_optimizer_util_clauses_seams::get_func_form::call(fexpr.funcid)?;

    fexpr.args = backend_optimizer_util_clauses::expand_function_arguments(
        core::mem::take(&mut fexpr.args),
        true, /* include_out_arguments */
        fexpr.funcresulttype,
        fexpr.funcid,
        &proc,
    )?;

    /*
     * Fetch proargmodes; if it's null, there are no output args. Otherwise
     * split the list into input arguments in fexpr->args and output arguments
     * in stmt->outargs. INOUT arguments appear in both lists.
     */
    let mut outargs: Vec<Expr> = Vec::new();
    if let Some(argmodes) = proc.proargmodes.as_ref() {
        use types_catalog::pg_proc::{
            PROARGMODE_IN, PROARGMODE_INOUT, PROARGMODE_OUT, PROARGMODE_VARIADIC,
        };
        let numargs = fexpr.args.len();
        if argmodes.len() != numargs {
            return Err(elog_error(alloc::format!(
                "proargmodes is not a 1-D char array of length {} or it contains nulls",
                numargs
            )));
        }
        let mut inargs: Vec<Expr> = Vec::new();
        let args = core::mem::take(&mut fexpr.args);
        for (i, n) in args.into_iter().enumerate() {
            let mode = argmodes[i];
            if mode == PROARGMODE_IN || mode == PROARGMODE_VARIADIC {
                inargs.push(n);
            } else if mode == PROARGMODE_OUT {
                outargs.push(n);
            } else if mode == PROARGMODE_INOUT {
                let copy = n.clone_in(mcx)?;
                inargs.push(n);
                outargs.push(copy);
            } else {
                /* note we don't support PROARGMODE_TABLE */
                return Err(elog_error(alloc::format!(
                    "invalid argmode {} for procedure",
                    mode as u8 as char
                )));
            }
        }
        fexpr.args = inargs;
    }

    /*
     * stmt->funcexpr = fexpr; stmt->outargs = outargs; rebuild the CallStmt
     * carrying the analyzed function expr + output args (the C edits the node
     * in place; the owned model rebuilds it).
     */
    let mut new_stmt = stmt.clone_in(mcx)?;
    new_stmt.funcexpr = Some(mcx::alloc_in(mcx, Node::mk_expr(mcx, Expr::FuncExpr(fexpr))?)?);
    new_stmt.outargs = {
        let mut v = mcx::vec_with_capacity_in(mcx, outargs.len())?;
        for o in outargs {
            v.push(mcx::alloc_in(mcx, Node::mk_expr(mcx, o)?)?);
        }
        v
    };

    /* represent the command as a utility Query */
    let mut result = Query::new(mcx);
    result.commandType = CmdType::CMD_UTILITY;
    result.utilityStmt = Some(mcx::alloc_in(mcx, Node::mk_call_stmt(mcx, new_stmt)?)?);
    Ok(result)
}
