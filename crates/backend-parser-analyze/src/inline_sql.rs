//! `inline_function`'s SQL-language body (clauses.c:4553 onward) — installed
//! into the `inline_sql_function` seam from this crate.
//!
//! `clauses.c`'s `inline_function` runs the cheap pg_proc catalog gates, the
//! `active_fns` recursion guard, and the final recursive re-simplification
//! in-crate (it owns the fold `EceContext`); the parser-dependent middle — parse
//! the function body (`prosqlbody` if present, else `pg_parse_query(prosrc)` +
//! `parse_analyze_fixedparams`), the ~20-condition "simple SELECT expression"
//! gate, `check_sql_fn_retval`'s scalar coercion check, and
//! `substitute_actual_parameters` with its per-parameter usecount machinery —
//! cannot live in the fold crate (which must not depend on the parser). It rides
//! the `inline_sql_function` seam, installed here: `backend-parser-analyze` is
//! the lowest crate that owns both the parser (`raw_parser` /
//! `parse_analyze_fixedparams`) and may depend on the fold crate's `contain_*`
//! walkers without a dependency cycle.
//!
//! Returns the SUBSTITUTED expression (`Ok(Some)`), not yet re-simplified; the
//! `inline_function` caller re-runs the fold mutator under its `active_fns`
//! guard. `Ok(None)` is every C `goto fail` decline.
//!
//! Named deferrals (loud or conservative-decline — never silent wrong answers):
//!   * `object_aclcheck(ACL_EXECUTE)` / `FmgrHookIsNeeded`: handled in-crate by
//!     the caller (single-user boot-superuser model, no fmgr hooks).
//!   * The multi-use-parameter "expensive" cost gate (clauses.c:4830,
//!     `cost_qual_eval > 10 * cpu_operator_cost`): the tree's `cost_qual_eval`
//!     is arena-keyed (`PlannerInfo` + `NodeId`) and has no root-free owned-`Expr`
//!     form, so when a parameter is used more than once we apply the
//!     subplan/volatility checks we can express faithfully and otherwise
//!     CONSERVATIVELY DECLINE (`Ok(None)` — C's `goto fail`). This never inlines
//!     incorrectly; it only declines to inline a rare multi-use-expensive-arg
//!     call, which then runs un-inlined (correct, just less aggressive).

use mcx::Mcx;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult};
use types_nodes::nodes::CmdType;
use types_nodes::primnodes::{CoercionForm, CollateExpr, Const, Expr, ParamKind};
use types_parsenodes::{CoercionContext, RawParseMode};

use backend_nodes_core::nodefuncs::{expr_collation, expr_type, expression_tree_mutator};
use backend_optimizer_util_clauses_seams::PgProcSimple;

/// `RECORDOID` (pg_type.dat).
const RECORDOID: Oid = 2249;
/// `PROVOLATILE_IMMUTABLE` / `PROVOLATILE_STABLE` (pg_proc.h).
const PROVOLATILE_IMMUTABLE: u8 = b'i';
const PROVOLATILE_STABLE: u8 = b's';

/// The `inline_sql_function` seam body. See module docs.
#[allow(clippy::too_many_arguments)]
pub fn inline_sql_function<'mcx>(
    mcx: Mcx<'mcx>,
    form: &PgProcSimple,
    prosrc: &str,
    prosqlbody: Option<&str>,
    _funcid: Oid,
    result_type: Oid,
    result_collid: Oid,
    input_collid: Oid,
    args: &[Expr],
    _funcvariadic: bool,
    _estimate: bool,
) -> PgResult<Option<Expr>> {
    // ---- parse the body (clauses.c:4646/4666) -----------------------------
    // raw_parser borrows the source for 'mcx, so re-home prosrc into the mcx
    // arena once (lives for the whole fn so the borrow checker is satisfied).
    // Re-home prosrc into the mcx arena and leak the binding to 'mcx so
    // raw_parser (which borrows its source for 'mcx) can hold it.
    let prosrc_mcx: &'mcx str = {
        let boxed = mcx::alloc_in(mcx, mcx::PgString::from_str_in(prosrc, mcx)?)?;
        mcx::leak_in(boxed).as_str()
    };

    // If we have prosqlbody, pay attention to that not prosrc.
    let querytree: types_nodes::copy_query::Query<'mcx> = if let Some(body) = prosqlbody {
        // n = stringToNode(prosqlbody); if (IsA(n, List)) query_list =
        // linitial_node(List, n); else query_list = list_make1(n);
        let n = backend_nodes_core::read::string_to_node(mcx, body)?;
        let query = extract_single_body_query(mcx, &n)?;
        match query {
            Some(q) => q,
            None => return Ok(None),
        }
    } else {
        // pg_parse_query(prosrc); punt on more than one command.
        let raw_list = backend_parser_driver::raw_parser(
            mcx,
            prosrc_mcx,
            RawParseMode::RAW_PARSE_DEFAULT,
        )?;
        if raw_list.len() != 1 {
            return Ok(None);
        }
        // prepare_sql_fn_parse_info + sql_fn_parser_setup (clauses.c:4674/4690):
        // a `$n` resolves against proargtypes and a body bareword that names an
        // argument resolves to its Param, under the call's input collation.
        let nargs = form.proargtypes.len();
        let argnames = match &form.proargnames {
            Some(names) if names.len() >= nargs && nargs > 0 => Some(names.clone()),
            _ => None,
        };
        let pinfo = types_nodes::parsestmt::SqlFnParseInfo::new(
            form.proname.clone(),
            input_collid,
            form.proargtypes.clone(),
            argnames,
        );
        crate::parse_analyze_sql_function(mcx, &raw_list[0], prosrc_mcx, pinfo)?
    };

    // ---- the "simple SELECT expression" gate (clauses.c:4700) -------------
    let jointree_empty = match querytree.jointree.as_deref() {
        None => true,
        Some(jt) => jt.fromlist.is_empty() && jt.quals.is_none(),
    };
    if querytree.commandType != CmdType::CMD_SELECT
        || querytree.hasAggs
        || querytree.hasWindowFuncs
        || querytree.hasTargetSRFs
        || querytree.hasSubLinks
        || !querytree.cteList.is_empty()
        || !querytree.rtable.is_empty()
        || !jointree_empty
        || !querytree.groupClause.is_empty()
        || !querytree.groupingSets.is_empty()
        || querytree.havingQual.is_some()
        || !querytree.windowClause.is_empty()
        || !querytree.distinctClause.is_empty()
        || !querytree.sortClause.is_empty()
        || querytree.limitOffset.is_some()
        || querytree.limitCount.is_some()
        || querytree.setOperations.is_some()
        || querytree.targetList.len() != 1
    {
        return Ok(None);
    }

    // ---- check_sql_fn_retval (clauses.c:4743) -----------------------------
    // The inline gate above already rejects every shape except a single
    // plain-SELECT tlist entry, so only the scalar leg of check_sql_fn_retval is
    // reachable: the lone tlist expression must be coercible to rettype, and a
    // coercion is inserted in place when needed.
    //
    // Grab the tlist expression (clauses.c:4757).
    let mut newexpr = match querytree.targetList[0].expr.as_deref() {
        Some(e) => e.clone(),
        None => return Ok(None),
    };

    // check_sql_fn_retval scalar leg: coerce the tlist expr to rettype
    // (COERCION_ASSIGNMENT / COERCE_IMPLICIT_CAST). On failure C raises a
    // "return type mismatch" error; that error would equally fire at runtime,
    // so we surface it rather than silently declining.
    let src_type = expr_type(Some(&newexpr))?;
    if src_type != result_type && result_type != RECORDOID {
        let coerced = backend_parser_coerce::coerce_to_target_type(
            mcx,
            None,
            newexpr.clone(),
            src_type,
            result_type,
            -1,
            CoercionContext::COERCION_ASSIGNMENT,
            CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        )?;
        match coerced {
            Some(mut c) => {
                backend_parser_parse_collate::assign_expr_collations(None, &mut c)?;
                newexpr = c;
            }
            None => {
                return Err(PgError::error(format!(
                    "return type mismatch in function declared to return type {}: actual return type is {}",
                    result_type, src_type
                )));
            }
        }
    }

    // If the SQL function returns VOID / the coercion still doesn't match the
    // declared result type, we can't inline (clauses.c:4768).
    if expr_type(Some(&newexpr))? != result_type {
        return Ok(None);
    }

    // ---- volatility / strictness checks (clauses.c:4778) ------------------
    if form.provolatile == PROVOLATILE_IMMUTABLE
        && backend_optimizer_util_clauses::contain_mutable_functions(Some(&newexpr))?
    {
        return Ok(None);
    } else if form.provolatile == PROVOLATILE_STABLE
        && backend_optimizer_util_clauses::contain_volatile_functions(Some(&newexpr))?
    {
        return Ok(None);
    }
    if form.proisstrict
        && backend_optimizer_util_clauses::contain_nonstrict_functions(Some(&newexpr))?
    {
        return Ok(None);
    }

    // Context-dependent nodes in the ARGUMENT list (clauses.c:4793).
    for a in args {
        if backend_optimizer_util_clauses::contain_context_dependent_node(Some(a))? {
            return Ok(None);
        }
    }

    // ---- substitute parameters + usecount checks (clauses.c:4802) ---------
    let mut usecounts = vec![0i32; form.pronargs as usize];
    let newexpr = substitute_actual_parameters(mcx, newexpr, args, &mut usecounts)?;

    for (i, param) in args.iter().enumerate() {
        let count = usecounts.get(i).copied().unwrap_or(0);
        if count == 0 {
            // Param not used at all: uncool if func is strict.
            if form.proisstrict {
                return Ok(None);
            }
        } else if count != 1 {
            // Param used multiple times: uncool if expensive or volatile.
            if backend_optimizer_util_clauses::contain_subplans(Some(param))? {
                return Ok(None);
            }
            // The `cost_qual_eval > 10 * cpu_operator_cost` expensiveness gate
            // has no root-free owned-Expr form in the tree (see module docs):
            // conservatively decline unless the multiply-used argument is a
            // trivial leaf (Var/Const/Param), which is unconditionally cheap.
            if !is_trivially_cheap(param) {
                return Ok(None);
            }
            if backend_optimizer_util_clauses::contain_volatile_functions(Some(param))? {
                return Ok(None);
            }
        }
    }

    // ---- result-collation wrap (clauses.c:4862) ---------------------------
    let mut newexpr = newexpr;
    if result_collid != InvalidOid {
        let exprcoll = expr_collation(Some(&newexpr))?;
        if exprcoll != InvalidOid && exprcoll != result_collid {
            newexpr = Expr::CollateExpr(CollateExpr {
                arg: Some(Box::new(newexpr)),
                collOid: result_collid,
                location: -1,
            });
        }
    }

    // The recursive re-simplification (clauses.c:4890) and the active_fns guard
    // run in the in-crate caller. Return the substituted expression.
    Ok(Some(newexpr))
}

/// `stringToNode(prosqlbody)` → the single body `Query` (clauses.c:4651). `n`
/// is either a `List` (whose first element is the query list) or a bare Query.
/// `Ok(None)` for `list_length(query_list) != 1`.
fn extract_single_body_query<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    n: &'a types_nodes::nodes::Node<'mcx>,
) -> PgResult<Option<types_nodes::copy_query::Query<'mcx>>> {
    use types_nodes::nodes::ntag;
    let query_list: &[types_nodes::nodes::NodePtr<'mcx>] = match n.node_tag() {
        ntag::T_List => {
            let outer = n.expect_list();
            // query_list = linitial_node(List, n): the first element is itself a
            // List of Query nodes.
            let first = match outer.first() {
                Some(p) => p,
                None => return Ok(None),
            };
            match (**first).node_tag() {
                ntag::T_List => &(**first).expect_list()[..],
                // A bare Query as the sole element (defensive).
                ntag::T_Query => core::slice::from_ref(first),
                _ => return Ok(None),
            }
        }
        ntag::T_Query => {
            // else query_list = list_make1(n).
            return Ok(Some(query_clone_from_node(mcx, n)?));
        }
        _ => return Ok(None),
    };
    if query_list.len() != 1 {
        return Ok(None);
    }
    query_clone_from_node(mcx, &query_list[0]).map(Some)
}

/// Clone a `Query` out of a `Node::Query` node ptr (the body parse yields owned
/// nodes; we read its `Query`).
fn query_clone_from_node<'mcx>(
    mcx: Mcx<'mcx>,
    n: &types_nodes::nodes::Node<'mcx>,
) -> PgResult<types_nodes::copy_query::Query<'mcx>> {
    match n.as_query() {
        Some(q) => q.clone_in(mcx),
        None => Err(PgError::error(
            "inline_sql_function: prosqlbody element is not a Query",
        )),
    }
}

/// `substitute_actual_parameters` (clauses.c:4909): replace each `PARAM_EXTERN`
/// `Param` by the actual argument expression, counting uses.
fn substitute_actual_parameters<'mcx>(
    mcx: Mcx<'mcx>,
    node: Expr,
    args: &[Expr],
    usecounts: &mut [i32],
) -> PgResult<Expr> {
    if let Some(p) = node.as_param() {
        if p.paramkind != ParamKind::PARAM_EXTERN {
            return Err(PgError::error(format!(
                "inline_sql_function: unexpected paramkind {:?} in SQL function body",
                p.paramkind
            )));
        }
        let id = p.paramid;
        if id <= 0 || id as usize > args.len() {
            return Err(PgError::error(format!(
                "inline_sql_function: invalid paramid {id} in SQL function body"
            )));
        }
        usecounts[(id - 1) as usize] += 1;
        // C: copyObject(param) — the actual argument expression may embed a
        // `SubLink` (e.g. a sub-SELECT argument) whose `subselect` Query carries
        // context-allocated children, so a plain derived `.clone()` panics. The
        // sanctioned deep-copy into the planner arena is `Expr::clone_in`.
        return args[(id - 1) as usize].clone_in(mcx);
    }
    // expression_tree_mutator over children; thread the (rare) error out.
    let mut err: Option<PgError> = None;
    let out = expression_tree_mutator(node, &mut |child| {
        if err.is_some() {
            return Expr::Const(Const::default());
        }
        match substitute_actual_parameters(mcx, child, args, usecounts) {
            Ok(n) => n,
            Err(e) => {
                err = Some(e);
                Expr::Const(Const::default())
            }
        }
    });
    match err {
        Some(e) => Err(e),
        None => Ok(out),
    }
}

/// A trivially-cheap leaf for the multi-use expensiveness gate: `Var` / `Const`
/// / `Param` carry no per-tuple cost, so C's `cost_qual_eval` is unconditionally
/// under the `10 * cpu_operator_cost` threshold for them.
fn is_trivially_cheap(node: &Expr) -> bool {
    node.as_var().is_some() || node.is_const() || node.as_param().is_some()
}

