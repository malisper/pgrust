//! The faithfully-ported (grounded) predicate / estimator / in-place-rewrite
//! surface of `clauses.c`, over the lifetime-free `Expr` tree.
//!
//! Each function is a 1:1 transcription of the same-named `clauses.c` routine.
//! Catalog reads funnel through `backend-utils-cache-lsyscache-seams` (and the
//! outward clauses/var seams); the generic recursion engine comes from
//! `::nodes_core::nodefuncs`.

use alloc::vec::Vec;

use ::mcx::{Mcx, PgBox};
use ::types_core::{InvalidOid, Oid};
use ::types_error::{PgError, PgResult};
use ::nodes::bitmapset::Bitmapset;
use ::nodes::primnodes::{
    etag, BoolExprType, BoolTestType, Expr, NullTestType, OpExpr, ParamKind, ScalarArrayOpExpr,
    SubLinkType,
};

use ::nodes_core::bitmapset as bms;
use ::nodes_core::bitmapset::BMS_Membership;
use ::nodes_core::multibitmapset as mbms;
use ::nodes_core::multibitmapset::MultiBitmapset;
use ::nodes_core::nodefuncs::{
    check_functions_in_node_ref, expression_tree_walker, expression_tree_walker as etw,
    set_opfuncid,
};

use clauses_seams as clauses_seam;
use var_seams as var_seam;
use lsyscache_seams as lsyscache;

use crate::leaf::clamp_row_est;

// ---------------------------------------------------------------------------
// Constants (pg_proc.h / fmgroids.h / primnodes.h).
// ---------------------------------------------------------------------------

/// `PROVOLATILE_IMMUTABLE` (`'i'`).
const PROVOLATILE_IMMUTABLE: u8 = b'i';
/// `PROVOLATILE_VOLATILE` (`'v'`).
const PROVOLATILE_VOLATILE: u8 = b'v';

/// `PROPARALLEL_SAFE` (`'s'`).
const PROPARALLEL_SAFE: u8 = b's';
/// `PROPARALLEL_RESTRICTED` (`'r'`).
const PROPARALLEL_RESTRICTED: u8 = b'r';
/// `PROPARALLEL_UNSAFE` (`'u'`).
const PROPARALLEL_UNSAFE: u8 = b'u';

/// `F_NEXTVAL` — the `nextval(regclass)` builtin function OID (fmgroids.h, PG18).
const F_NEXTVAL: Oid = 1574;

/// `FirstLowInvalidHeapAttributeNumber` (access/sysattr.h), as `i32`.
const FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER: i32 =
    types_tuple::heaptuple::FirstLowInvalidHeapAttributeNumber as i32;

/// `MIN_ARRAY_SIZE_FOR_HASHED_SAOP` (clauses.c).
const MIN_ARRAY_SIZE_FOR_HASHED_SAOP: i32 = 9;

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

fn elog_error(msg: impl Into<alloc::string::String>) -> PgError {
    PgError::error(msg.into())
}

/// `expression_tree_walker(node, walker, NULL)` default recursion returning a
/// `PgResult<bool>` (errors captured out of the closure). C aborts the walk on
/// the first `true`; so do we.
fn walk_default(
    node: &Expr,
    f: &mut dyn FnMut(Option<&Expr>) -> PgResult<bool>,
) -> PgResult<bool> {
    let mut err: Option<PgError> = None;
    let aborted = expression_tree_walker(Some(node), &mut |n| match f(Some(n)) {
        Ok(b) => b,
        Err(e) => {
            err = Some(e);
            true
        }
    });
    match err {
        Some(e) => Err(e),
        None => Ok(aborted),
    }
}

/// `check_functions_in_node(node, checker, ctx)` over a `&Expr`.
///
/// The engine's `check_functions_in_node` takes `&mut Expr` only to lazily fill
/// `opfuncid` from `opno` on `OpExpr`/`ScalarArrayOpExpr` in place. Read-only
/// callers (the C invokes `contain_volatile_functions` etc. on the live tree)
/// use the non-mutating [`check_functions_in_node_ref`], which resolves the
/// same `opfuncid` by lookup without cloning the node — a shallow `Expr::clone`
/// panics on an `Aggref` (its args are a context-allocated `TargetEntry` list).
fn check_functions_in_node_const<F: FnMut(Oid) -> bool>(
    node: &Expr,
    checker: &mut F,
) -> PgResult<bool> {
    check_functions_in_node_ref(node, checker)
}

// ===========================================================================
// Aggregate-function clause manipulation (clauses.c:172)
// ===========================================================================

/// `contain_agg_clause(clause)` (clauses.c:178).
pub fn contain_agg_clause(clause: Option<&Expr>) -> PgResult<bool> {
    contain_agg_clause_walker(clause)
}

fn contain_agg_clause_walker(node: Option<&Expr>) -> PgResult<bool> {
    let Some(node) = node else { return Ok(false) };
    if node.is_aggref() {
        return Ok(true); // Assert(agglevelsup == 0)
    }
    if node.is_groupingfunc() {
        return Ok(true); // Assert(agglevelsup == 0)
    }
    // Assert(!IsA(node, SubLink));
    walk_default(node, &mut contain_agg_clause_walker)
}

// ===========================================================================
// Window-function clause manipulation (clauses.c:216)
// ===========================================================================

/// `contain_window_function(clause)` (clauses.c:216).
pub fn contain_window_function(clause: Option<&Expr>) -> PgResult<bool> {
    contain_windowfuncs(clause)
}

fn contain_windowfuncs(node: Option<&Expr>) -> PgResult<bool> {
    let Some(node) = node else { return Ok(false) };
    if node.is_windowfunc() {
        return Ok(true);
    }
    walk_default(node, &mut contain_windowfuncs)
}

/// `WindowFuncLists` (clauses.h) — `WindowFunc` nodes organized by winref.
pub struct WindowFuncLists<'mcx> {
    pub num_window_funcs: i32,
    pub max_win_ref: u32,
    /// `windowFuncs[winref]` — the `WindowFunc` expressions found, by winref.
    pub window_funcs: Vec<Vec<Expr<'mcx>>>,
}

/// `find_window_functions(clause, maxWinRef)` (clauses.c:228).
pub fn find_window_functions<'b>(
    mcx: Mcx<'b>,
    clause: Option<&Expr<'_>>,
    max_win_ref: u32,
) -> PgResult<WindowFuncLists<'b>> {
    let mut window_funcs: Vec<Vec<Expr<'b>>> = Vec::with_capacity((max_win_ref as usize) + 1);
    for _ in 0..=(max_win_ref as usize) {
        window_funcs.push(Vec::new());
    }
    let mut lists = WindowFuncLists {
        num_window_funcs: 0,
        max_win_ref,
        window_funcs,
    };
    let mut err: Option<PgError> = None;
    find_window_functions_walker(mcx, clause, &mut lists, &mut err);
    if let Some(e) = err {
        return Err(e);
    }
    Ok(lists)
}

/// `find_window_functions((Node *) tlist, maxWinRef)` for a targetlist — runs
/// the window-func walker over each targetlist expression, accumulating into a
/// single [`WindowFuncLists`]. The C passes the whole `List *` to the walker
/// (which descends List nodes); here the planner's `processed_tlist` is a list
/// of `TargetEntry` handles, so the caller resolves each entry's expression and
/// passes the slice.
pub fn find_window_functions_in_exprs<'b>(
    mcx: Mcx<'b>,
    exprs: &[&Expr<'_>],
    max_win_ref: u32,
) -> PgResult<WindowFuncLists<'b>> {
    let mut window_funcs: Vec<Vec<Expr<'b>>> = Vec::with_capacity((max_win_ref as usize) + 1);
    for _ in 0..=(max_win_ref as usize) {
        window_funcs.push(Vec::new());
    }
    let mut lists = WindowFuncLists {
        num_window_funcs: 0,
        max_win_ref,
        window_funcs,
    };
    let mut err: Option<PgError> = None;
    for expr in exprs {
        find_window_functions_walker(mcx, Some(expr), &mut lists, &mut err);
        if err.is_some() {
            break;
        }
    }
    if let Some(e) = err {
        return Err(e);
    }
    Ok(lists)
}

fn find_window_functions_walker<'b>(
    mcx: Mcx<'b>,
    node: Option<&Expr<'_>>,
    lists: &mut WindowFuncLists<'b>,
    err: &mut Option<PgError>,
) -> bool {
    let Some(node) = node else { return false };
    if let Some(wfunc) = node.as_windowfunc() {
        // winref is unsigned, so one-sided test is OK
        if wfunc.winref > lists.max_win_ref {
            *err = Some(elog_error(alloc::format!(
                "WindowFunc contains out-of-range winref {}",
                wfunc.winref
            )));
            return true;
        }
        // C appends the WindowFunc pointer (`lappend`); the owned model needs a
        // deep copy. A plain `.clone()` would panic on a context-allocated child
        // (the WindowFunc's args may carry an Aggref, as in `SUM(SUM(x)) OVER`),
        // so deep-copy via `Expr::clone_in`.
        match node.clone_in(mcx) {
            Ok(copy) => lists.window_funcs[wfunc.winref as usize].push(copy),
            Err(e) => {
                *err = Some(e);
                return true;
            }
        }
        lists.num_window_funcs += 1;
        // No need to recurse into the arguments/filter.
        return false;
    }
    // Assert(!IsA(node, SubLink));
    expression_tree_walker(Some(node), &mut |n| {
        find_window_functions_walker(mcx, Some(n), lists, err)
    })
}

// ===========================================================================
// Support for expressions returning sets (clauses.c:287)
// ===========================================================================

/// `expression_returns_set_rows(root, clause)` (clauses.c:287).
pub fn expression_returns_set_rows(clause: Option<&Expr>) -> PgResult<f64> {
    let Some(clause) = clause else { return Ok(1.0) };
    if let Some(expr) = clause.as_funcexpr() {
        if expr.funcretset {
            let rows = clauses_seam::get_function_rows::call(expr.funcid, clause)?;
            return Ok(clamp_row_est(rows));
        }
    }
    if let Some(expr) = clause.as_opexpr() {
        if expr.opretset {
            // C: set_opfuncid((OpExpr *) clause) then reads clause->opfuncid.
            // A shallow `clause.clone()` to get an owned `&mut` would deep-copy the
            // OpExpr's `args`, which may carry an `Aggref`/`SubPlan`/`SubLink`
            // child whose derived `Expr::clone` panics (e.g.
            // `rank() over (order by sum(a)+sum(b))`). Resolve the funcid by
            // catalog lookup without mutating/cloning the node — the same
            // read-only precedent as `find_nonnullable_rels` below.
            let funcid = ::nodes_core::nodefuncs::resolved_opfuncid(expr.opno, expr.opfuncid)?;
            let rows = clauses_seam::get_function_rows::call(funcid, clause)?;
            return Ok(clamp_row_est(rows));
        }
    }
    Ok(1.0)
}

// ===========================================================================
// Subplan clause manipulation (clauses.c:328)
// ===========================================================================

/// `contain_subplans(clause)` (clauses.c:328) — the `Option<&Expr>` entry.
pub fn contain_subplans(clause: Option<&Expr>) -> PgResult<bool> {
    contain_subplans_walker(clause)
}

/// The installed seam body: `contain_subplans((Node *) exprs)` over an
/// implicitly-AND'd `List *` (one VALUES row's expression list, from
/// `nodeValuesscan`). A pure structural predicate; the C walker descends the
/// list like any other node, so we just walk each element. Infallible (the
/// structural test cannot ereport).
pub fn contain_subplans_slice(clause: &[Expr]) -> bool {
    for e in clause {
        match contain_subplans_walker(Some(e)) {
            Ok(true) => return true,
            Ok(false) => {}
            // The structural predicate never ereports; defensively treat an
            // error as "found" would be wrong, so propagate as false-on-Ok and
            // panic on the impossible Err to surface a model bug rather than
            // silently mis-answer.
            Err(e) => panic!("contain_subplans: unexpected error: {}", e.message()),
        }
    }
    false
}

fn contain_subplans_walker(node: Option<&Expr>) -> PgResult<bool> {
    let Some(node) = node else { return Ok(false) };
    if node.is_subplan() || node.is_alternativesubplan() || node.is_sublink() {
        return Ok(true);
    }
    walk_default(node, &mut contain_subplans_walker)
}

// ===========================================================================
// Check clauses for mutable functions (clauses.c:368)
// ===========================================================================

/// `contain_mutable_functions(clause)` (clauses.c:368).
pub fn contain_mutable_functions(clause: Option<&Expr>) -> PgResult<bool> {
    contain_mutable_functions_walker(clause)
}

fn contain_mutable_functions_checker(func_id: Oid) -> PgResult<bool> {
    Ok(lsyscache::func_volatile::call(func_id)? != PROVOLATILE_IMMUTABLE)
}

fn contain_mutable_functions_walker(node: Option<&Expr>) -> PgResult<bool> {
    let Some(node) = node else { return Ok(false) };

    // Check for mutable functions in node itself. (check_functions_in_node's
    // checker is infallible in C; ours reaches a fallible seam, so capture the
    // error out of the closure.)
    let mut checker_err: Option<PgError> = None;
    let found = check_functions_in_node_const(node, &mut |id| {
        match contain_mutable_functions_checker(id) {
            Ok(b) => b,
            Err(e) => {
                checker_err = Some(e);
                true
            }
        }
    })?;
    if let Some(e) = checker_err {
        return Err(e);
    }
    if found {
        return Ok(true);
    }

    if let Some(ctor) = node.as_jsonconstructorexpr() {
        // is_jsonb = ctor->returning->format->format_type == JS_FORMAT_JSONB.
        let is_jsonb = match &ctor.returning {
            Some(r) => match &r.format {
                Some(f) => {
                    f.format_type == ::nodes::primnodes::JsonFormatType::JS_FORMAT_JSONB
                }
                None => false,
            },
            None => false,
        };
        for arg in &ctor.args {
            let typid = ::nodes_core::nodefuncs::expr_type(Some(arg))?;
            let immutable = if is_jsonb {
                clauses_seam::to_jsonb_is_immutable::call(typid)?
            } else {
                clauses_seam::to_json_is_immutable::call(typid)?
            };
            if !immutable {
                return Ok(true);
            }
        }
        // Check all subnodes (fall through).
    }

    if let Some(jexpr) = node.as_jsonexpr() {
        // if (!IsA(jexpr->path_spec, Const)) return true;
        match jexpr.path_spec.as_deref() {
            Some(ps) if ps.is_const() => {
                let cnst = ps.expect_const();
                // Assert(consttype == JSONPATHOID)
                if cnst.constisnull {
                    return Ok(false);
                }
                if clauses_seam::jsp_is_mutable::call(node)? {
                    return Ok(true);
                }
            }
            _ => return Ok(true),
        }
    }

    if node.is_sqlvaluefunction() {
        // all variants of SQLValueFunction are stable
        return Ok(true);
    }
    if node.is_nextvalueexpr() {
        // NextValueExpr is volatile
        return Ok(true);
    }

    // Recurse to check arguments. (The C also has a Query arm via
    // query_tree_walker; the Expr model never produces a Query child, so that
    // arm is unreachable here.)
    walk_default(node, &mut contain_mutable_functions_walker)
}

// ===========================================================================
// Check clauses for volatile functions (clauses.c:536)
// ===========================================================================

/// `contain_volatile_functions(clause)` (clauses.c:536).
pub fn contain_volatile_functions(clause: Option<&Expr>) -> PgResult<bool> {
    contain_volatile_functions_walker(clause)
}

fn contain_volatile_functions_checker(func_id: Oid) -> PgResult<bool> {
    Ok(lsyscache::func_volatile::call(func_id)? == PROVOLATILE_VOLATILE)
}

fn contain_volatile_functions_walker(node: Option<&Expr>) -> PgResult<bool> {
    let Some(node) = node else { return Ok(false) };

    let mut checker_err: Option<PgError> = None;
    let found = check_functions_in_node_const(node, &mut |id| {
        match contain_volatile_functions_checker(id) {
            Ok(b) => b,
            Err(e) => {
                checker_err = Some(e);
                true
            }
        }
    })?;
    if let Some(e) = checker_err {
        return Err(e);
    }
    if found {
        return Ok(true);
    }

    if node.is_nextvalueexpr() {
        // NextValueExpr is volatile
        return Ok(true);
    }

    // C `expression_tree_walker` walks a SubLink's `subselect` (a Query) and the
    // `contain_volatile_functions_walker` `IsA(node, Query)` arm recurses into it
    // via `query_tree_walker`. The Expr-typed `expression_tree_walker` here drops
    // the sub-Query, so do that recursion explicitly: without it a scalar
    // sub-SELECT containing a volatile (e.g. `(select random())` in a subquery
    // targetlist) is not seen, and `is_simple_subquery` wrongly pulls the
    // subquery up — duplicating the volatile across each upper reference.
    if let Expr::SubLink(sublink) = node {
        if let Some(subselect) = sublink.subselect.as_deref() {
            if contain_volatile_functions_in_query(subselect)? {
                return Ok(true);
            }
        }
    }

    // The C also looks through RestrictInfo / PathTarget (caching their
    // hasvolatile flag); the Expr model carries neither, so those arms are
    // unreachable here.
    walk_default(node, &mut contain_volatile_functions_walker)
}

/// The `IsA(node, Query)` arm of `contain_volatile_functions_walker`: recurse
/// into a (sub)Query's expression children via `query_tree_walker`, dispatching
/// each reached `Node` back to the Expr-level walker (and re-recursing on any
/// nested sub-Query).
fn contain_volatile_functions_in_query(
    query: &::nodes::copy_query::Query,
) -> PgResult<bool> {
    let mut err: Option<PgError> = None;
    let aborted = ::nodes_core::node_walker::query_tree_walker(
        query,
        &mut |n| match contain_volatile_functions_node(n) {
            Ok(b) => b,
            Err(e) => {
                err = Some(e);
                true
            }
        },
        0,
    );
    if let Some(e) = err {
        return Err(e);
    }
    Ok(aborted)
}

/// Dispatch a `Node` reached during the volatile scan: a `Query` recurses via
/// [`contain_volatile_functions_in_query`]; every other Node is an `Expr` and
/// runs the per-node Expr walker.
fn contain_volatile_functions_node(
    node: &::nodes::nodes::Node,
) -> PgResult<bool> {
    if node.is_query() {
        return contain_volatile_functions_in_query(node.expect_query());
    }
    match node.as_expr() {
        Some(expr) => contain_volatile_functions_walker(Some(expr)),
        None => Ok(false),
    }
}

/// `contain_volatile_functions_not_nextval(clause)` (clauses.c:671).
pub fn contain_volatile_functions_not_nextval(clause: Option<&Expr>) -> PgResult<bool> {
    contain_volatile_functions_not_nextval_walker(clause)
}

fn contain_volatile_functions_not_nextval_checker(func_id: Oid) -> PgResult<bool> {
    Ok(func_id != F_NEXTVAL && lsyscache::func_volatile::call(func_id)? == PROVOLATILE_VOLATILE)
}

fn contain_volatile_functions_not_nextval_walker(node: Option<&Expr>) -> PgResult<bool> {
    let Some(node) = node else { return Ok(false) };
    let mut checker_err: Option<PgError> = None;
    let found = check_functions_in_node_const(node, &mut |id| {
        match contain_volatile_functions_not_nextval_checker(id) {
            Ok(b) => b,
            Err(e) => {
                checker_err = Some(e);
                true
            }
        }
    })?;
    if let Some(e) = checker_err {
        return Err(e);
    }
    if found {
        return Ok(true);
    }
    walk_default(node, &mut contain_volatile_functions_not_nextval_walker)
}

// ===========================================================================
// Parallel-hazard checks (clauses.c:731)
// ===========================================================================

/// `max_parallel_hazard_context` (clauses.c).
struct MaxParallelHazardContext {
    max_hazard: u8,
    max_interesting: u8,
    safe_param_ids: Vec<i32>,
}

/// `is_parallel_safe(root, node)` (clauses.c:751) — detect whether the given
/// expr contains only parallel-safe functions.
///
/// `root->glob->maxParallelHazard` and the init-plan `setParam` IDs come from
/// the planner wiring layer, which the `Expr` model does not thread; the caller
/// supplies the precomputed `max_parallel_hazard_glob` /
/// `param_exec_types_is_empty` / `safe_param_ids` (matching the C inputs).
pub fn is_parallel_safe(
    max_parallel_hazard_glob: u8,
    param_exec_types_is_empty: bool,
    safe_param_ids: Vec<i32>,
    node: Option<&Expr>,
) -> PgResult<bool> {
    // If max_parallel_hazard found nothing unsafe AND there are no initplan
    // setParams, the whole query is parallel-safe — short-circuit.
    if max_parallel_hazard_glob == PROPARALLEL_SAFE && param_exec_types_is_empty {
        return Ok(true);
    }
    let mut context = MaxParallelHazardContext {
        max_hazard: PROPARALLEL_SAFE,
        max_interesting: PROPARALLEL_RESTRICTED,
        safe_param_ids,
    };
    let mut err: Option<PgError> = None;
    let hazardous = max_parallel_hazard_walker(node, &mut context, &mut err);
    if let Some(e) = err {
        return Err(e);
    }
    Ok(!hazardous)
}

/// `max_parallel_hazard_test(proparallel, context)` (clauses.c:792).
fn max_parallel_hazard_test(
    proparallel: u8,
    context: &mut MaxParallelHazardContext,
    err: &mut Option<PgError>,
) -> bool {
    if proparallel == PROPARALLEL_SAFE {
        // nothing to see here, move along
    } else if proparallel == PROPARALLEL_RESTRICTED {
        // increase max_hazard to RESTRICTED
        context.max_hazard = proparallel;
        // done if we are not expecting any unsafe functions
        if context.max_interesting == proparallel {
            return true;
        }
    } else if proparallel == PROPARALLEL_UNSAFE {
        context.max_hazard = proparallel;
        return true;
    } else {
        *err = Some(elog_error(alloc::format!(
            "unrecognized proparallel value \"{}\"",
            proparallel as char
        )));
        return true;
    }
    false
}

fn max_parallel_hazard_checker(
    func_id: Oid,
    context: &mut MaxParallelHazardContext,
    err: &mut Option<PgError>,
) -> bool {
    match lsyscache::func_parallel::call(func_id) {
        Ok(p) => max_parallel_hazard_test(p, context, err),
        Err(e) => {
            *err = Some(e);
            true
        }
    }
}

fn max_parallel_hazard_walker(
    node: Option<&Expr>,
    context: &mut MaxParallelHazardContext,
    err: &mut Option<PgError>,
) -> bool {
    let Some(node) = node else { return false };

    // Check for hazardous functions in node itself. Read-only: the by-ref
    // checker resolves opfuncids without mutating (a shallow node clone panics
    // on an Aggref's context-allocated TargetEntry args).
    {
        let aborted = match check_functions_in_node_ref(node, &mut |id| {
            max_parallel_hazard_checker(id, context, err)
        }) {
            Ok(b) => b,
            Err(e) => {
                *err = Some(e);
                true
            }
        };
        if aborted {
            return true;
        }
    }

    // CoerceToDomain / NextValueExpr / WindowFunc / SubLink markers, then the
    // SubPlan/Param special handling. (RestrictInfo look-through and the Query
    // rowMarks/recursion arms are absent: the Expr model carries no
    // RestrictInfo or Query node.)
    if node.is_coercetodomain() {
        if max_parallel_hazard_test(PROPARALLEL_RESTRICTED, context, err) {
            return true;
        }
    } else if node.is_nextvalueexpr() {
        if max_parallel_hazard_test(PROPARALLEL_UNSAFE, context, err) {
            return true;
        }
    } else if node.is_windowfunc() {
        if max_parallel_hazard_test(PROPARALLEL_RESTRICTED, context, err) {
            return true;
        }
    } else if node.is_sublink() {
        if max_parallel_hazard_test(PROPARALLEL_RESTRICTED, context, err) {
            return true;
        }
    } else if let Some(sp) = node.as_subplan() {
        let subplan = &sp.0;
        if !subplan.parallel_safe
            && max_parallel_hazard_test(PROPARALLEL_RESTRICTED, context, err)
        {
            return true;
        }
        // Temporarily add the subplan's paramIds to safe_param_ids while
        // examining the testexpr.
        let save_len = context.safe_param_ids.len();
        for &pid in subplan.paramIds.iter() {
            context.safe_param_ids.push(pid);
        }
        if max_parallel_hazard_walker(subplan.testexpr.as_deref(), context, err) {
            return true; // no need to restore safe_param_ids
        }
        context.safe_param_ids.truncate(save_len);
        // we must also check args, but no special Param treatment there
        for a in subplan.args.iter() {
            if max_parallel_hazard_walker(Some(&**a), context, err) {
                return true;
            }
        }
        return false;
    } else if let Some(param) = node.as_param() {
        if param.paramkind == ParamKind::PARAM_EXTERN {
            return false;
        }
        if param.paramkind != ParamKind::PARAM_EXEC
            || !context.safe_param_ids.contains(&param.paramid)
        {
            if max_parallel_hazard_test(PROPARALLEL_RESTRICTED, context, err) {
                return true;
            }
        }
        return false;
    }

    // Recurse to check arguments.
    expression_tree_walker(Some(node), &mut |n| {
        max_parallel_hazard_walker(Some(n), context, err)
    })
}

/// `max_parallel_hazard(parse)` (clauses.c:731).
///
/// Top-level whole-query parallel-hazard scan: walk the Query tree (including
/// subselects via `query_tree_walker`) and return the worst `proparallel`
/// hazard found (`PROPARALLEL_SAFE` / `_RESTRICTED` / `_UNSAFE`). The planner
/// uses this to set `glob->parallelModeOK`.
///
/// C runs `max_parallel_hazard_walker((Node *) parse, &context)` with
/// `max_interesting = PROPARALLEL_UNSAFE`. The walker is a `Node` walker; over
/// the owned model the per-node logic for `Expr`-shaped nodes is the existing
/// [`max_parallel_hazard_walker`] (function check + the CoerceToDomain /
/// NextValueExpr / WindowFunc / SubLink / SubPlan / Param arms + `Expr`
/// recursion), and the `IsA(node, Query)` arm (rowMarks → unsafe, else recurse
/// via `query_tree_walker`) is added here.
pub fn max_parallel_hazard(parse: &::nodes::copy_query::Query) -> PgResult<u8> {
    let mut context = MaxParallelHazardContext {
        max_hazard: PROPARALLEL_SAFE,
        max_interesting: PROPARALLEL_UNSAFE,
        safe_param_ids: Vec::new(),
    };
    let mut err: Option<PgError> = None;
    max_parallel_hazard_walker_query(parse, &mut context, &mut err);
    if let Some(e) = err {
        return Err(e);
    }
    Ok(context.max_hazard)
}

/// The `Node`-level dispatch of `max_parallel_hazard_walker` (clauses.c:826) for
/// the whole-query scan. The C walker is invoked on a `Node`; in this model a
/// scanned `Node` is either a `Query` (handled here) or an `Expr` (delegated to
/// the `Expr`-level [`max_parallel_hazard_walker`]). Returns `true` to abort.
fn max_parallel_hazard_walker_node(
    node: &::nodes::nodes::Node,
    context: &mut MaxParallelHazardContext,
    err: &mut Option<PgError>,
) -> bool {
    // else if (IsA(node, Query)) { ... } (clauses.c:945)
    if node.is_query() {
        return max_parallel_hazard_walker_query(node.expect_query(), context, err);
    }
    // Every other Node reached during the scan is an Expr; run the Expr-level
    // per-node logic (function check + IsA arms + Expr recursion). A Node that
    // carries no Expr (none occur on this scan path) is a no-op, matching the C
    // walker falling through to expression_tree_walker on an unhandled node.
    match node.as_expr() {
        Some(expr) => max_parallel_hazard_walker(Some(expr), context, err),
        None => false,
    }
}

/// The `IsA(node, Query)` arm of `max_parallel_hazard_walker` (clauses.c:945).
fn max_parallel_hazard_walker_query(
    query: &::nodes::copy_query::Query,
    context: &mut MaxParallelHazardContext,
    err: &mut Option<PgError>,
) -> bool {
    // SELECT FOR UPDATE/SHARE must be treated as unsafe.
    if !query.rowMarks.is_empty() {
        context.max_hazard = PROPARALLEL_UNSAFE;
        return true;
    }

    // Recurse into subselects: query_tree_walker(query, walker, context, 0).
    // The callback is the Node walker, which re-dispatches Query vs Expr.
    ::nodes_core::node_walker::query_tree_walker(
        query,
        &mut |n| max_parallel_hazard_walker_node(n, context, err),
        0,
    )
}

// ===========================================================================
// Check clauses for nonstrict functions (clauses.c:991)
// ===========================================================================

/// `contain_nonstrict_functions(clause)` (clauses.c:991).
pub fn contain_nonstrict_functions(clause: Option<&Expr>) -> PgResult<bool> {
    contain_nonstrict_functions_walker(clause)
}

fn contain_nonstrict_functions_checker(func_id: Oid) -> PgResult<bool> {
    Ok(!lsyscache::func_strict::call(func_id)?)
}

fn contain_nonstrict_functions_walker(node: Option<&Expr>) -> PgResult<bool> {
    let Some(node) = node else { return Ok(false) };

    match node.expr_tag() {
        etag::T_Aggref => return Ok(true),
        etag::T_GroupingFunc => return Ok(true),
        etag::T_WindowFunc => return Ok(true),
        etag::T_SubscriptingRef => {
            let sbsref = node.as_subscriptingref().unwrap();
            // Subscripting assignment is always presumed nonstrict.
            if sbsref.refassgnexpr.is_some() {
                return Ok(true);
            }
            match clauses_seam::subscripting_fetch_strict::call(sbsref.refcontainertype)? {
                Some(fetch_strict) if fetch_strict => { /* fall through */ }
                _ => return Ok(true),
            }
        }
        etag::T_DistinctExpr => return Ok(true),
        etag::T_NullIfExpr => return Ok(true),
        etag::T_BoolExpr => {
            let expr = node.as_boolexpr().unwrap();
            if expr.boolop == BoolExprType::AND_EXPR || expr.boolop == BoolExprType::OR_EXPR {
                return Ok(true);
            }
        }
        etag::T_SubLink => return Ok(true),
        etag::T_SubPlan => return Ok(true),
        etag::T_AlternativeSubPlan => return Ok(true),
        etag::T_FieldStore => return Ok(true),
        etag::T_CoerceViaIO => {
            let c = node.as_coerceviaio().unwrap();
            return contain_nonstrict_functions_walker(c.arg.as_deref());
        }
        etag::T_ArrayCoerceExpr => {
            let c = node.as_arraycoerceexpr().unwrap();
            return contain_nonstrict_functions_walker(c.arg.as_deref());
        }
        etag::T_CaseExpr => return Ok(true),
        etag::T_ArrayExpr => return Ok(true),
        etag::T_RowExpr => return Ok(true),
        etag::T_RowCompareExpr => return Ok(true),
        etag::T_CoalesceExpr => return Ok(true),
        etag::T_MinMaxExpr => return Ok(true),
        etag::T_XmlExpr => return Ok(true),
        etag::T_NullTest => return Ok(true),
        etag::T_BooleanTest => return Ok(true),
        etag::T_JsonConstructorExpr => return Ok(true),
        _ => {}
    }

    let mut checker_err: Option<PgError> = None;
    let found = check_functions_in_node_const(node, &mut |id| {
        match contain_nonstrict_functions_checker(id) {
            Ok(b) => b,
            Err(e) => {
                checker_err = Some(e);
                true
            }
        }
    })?;
    if let Some(e) = checker_err {
        return Err(e);
    }
    if found {
        return Ok(true);
    }
    walk_default(node, &mut contain_nonstrict_functions_walker)
}

// ===========================================================================
// Check clauses for Params (clauses.c:1137)
// ===========================================================================

/// `contain_exec_param(clause, param_ids)` (clauses.c:1137).
pub fn contain_exec_param(clause: Option<&Expr>, param_ids: &[i32]) -> PgResult<bool> {
    contain_exec_param_walker(clause, param_ids)
}

fn contain_exec_param_walker(node: Option<&Expr>, param_ids: &[i32]) -> PgResult<bool> {
    let Some(node) = node else { return Ok(false) };
    if let Some(p) = node.as_param() {
        if p.paramkind == ParamKind::PARAM_EXEC && param_ids.contains(&p.paramid) {
            return Ok(true);
        }
    }
    walk_default(node, &mut |n| contain_exec_param_walker(n, param_ids))
}

// ===========================================================================
// Check clauses for context-dependent nodes (clauses.c:1179)
// ===========================================================================

const CCDN_CASETESTEXPR_OK: i32 = 0x0001;

/// `contain_context_dependent_node(clause)` (clauses.c:1179).
pub fn contain_context_dependent_node(clause: Option<&Expr>) -> PgResult<bool> {
    let mut flags = 0;
    contain_context_dependent_node_walker(clause, &mut flags)
}

fn contain_context_dependent_node_walker(node: Option<&Expr>, flags: &mut i32) -> PgResult<bool> {
    let Some(node) = node else { return Ok(false) };
    if node.is_casetestexpr() {
        return Ok((*flags & CCDN_CASETESTEXPR_OK) == 0);
    } else if let Some(caseexpr) = node.as_caseexpr() {
        if caseexpr.arg.is_some() {
            let save_flags = *flags;
            *flags |= CCDN_CASETESTEXPR_OK;
            let res = walk_default(node, &mut |n| {
                contain_context_dependent_node_walker(n, flags)
            });
            *flags = save_flags;
            return res;
        }
    } else if let Some(ac) = node.as_arraycoerceexpr() {
        // Check the array expression.
        if contain_context_dependent_node_walker(ac.arg.as_deref(), flags)? {
            return Ok(true);
        }
        // Check the elemexpr, which is allowed to contain CaseTestExpr.
        let save_flags = *flags;
        *flags |= CCDN_CASETESTEXPR_OK;
        let res = contain_context_dependent_node_walker(ac.elemexpr.as_deref(), flags);
        *flags = save_flags;
        return res;
    }
    walk_default(node, &mut |n| {
        contain_context_dependent_node_walker(n, flags)
    })
}

// ===========================================================================
// Check clauses for Vars passed to non-leakproof functions (clauses.c:1263)
// ===========================================================================

/// `contain_leaked_vars(clause)` (clauses.c:1263).
pub fn contain_leaked_vars(clause: Option<&Expr>) -> PgResult<bool> {
    contain_leaked_vars_walker(clause)
}

fn contain_leaked_vars_checker(func_id: Oid) -> PgResult<bool> {
    Ok(!lsyscache::get_func_leakproof::call(func_id)?)
}

fn contain_leaked_vars_walker(node: Option<&Expr>) -> PgResult<bool> {
    let Some(node) = node else { return Ok(false) };

    match node.expr_tag() {
        // These node types don't contain function calls directly; but something
        // further down might (fall through to recurse).
        etag::T_Var
        | etag::T_Const
        | etag::T_Param
        | etag::T_ArrayExpr
        | etag::T_FieldSelect
        | etag::T_FieldStore
        | etag::T_NamedArgExpr
        | etag::T_BoolExpr
        | etag::T_RelabelType
        | etag::T_CollateExpr
        | etag::T_CaseExpr
        | etag::T_CaseTestExpr
        | etag::T_RowExpr
        | etag::T_SQLValueFunction
        | etag::T_NullTest
        | etag::T_BooleanTest
        | etag::T_NextValueExpr
        | etag::T_ReturningExpr => {}

        etag::T_FuncExpr
        | etag::T_OpExpr
        | etag::T_DistinctExpr
        | etag::T_NullIfExpr
        | etag::T_ScalarArrayOpExpr
        | etag::T_CoerceViaIO
        | etag::T_ArrayCoerceExpr => {
            let mut checker_err: Option<PgError> = None;
            let found = check_functions_in_node_const(node, &mut |id| {
                match contain_leaked_vars_checker(id) {
                    Ok(b) => b,
                    Err(e) => {
                        checker_err = Some(e);
                        true
                    }
                }
            })?;
            if let Some(e) = checker_err {
                return Err(e);
            }
            if found && var_seam::contain_var_clause::call(node) {
                return Ok(true);
            }
        }

        etag::T_SubscriptingRef => {
            let sbsref = node.as_subscriptingref().unwrap();
            let leakproof =
                match clauses_seam::subscripting_leakproof::call(sbsref.refcontainertype)? {
                    Some((fetch_leakproof, store_leakproof)) => {
                        if sbsref.refassgnexpr.is_some() {
                            store_leakproof
                        } else {
                            fetch_leakproof
                        }
                    }
                    None => false,
                };
            if !leakproof && var_seam::contain_var_clause::call(node) {
                return Ok(true);
            }
        }

        etag::T_RowCompareExpr => {
            let rcexpr = node.as_rowcompareexpr().unwrap();
            // forthree(opid in opnos, larg in largs, rarg in rargs)
            let n = rcexpr
                .opnos
                .len()
                .min(rcexpr.largs.len())
                .min(rcexpr.rargs.len());
            for i in 0..n {
                let funcid = lsyscache::get_opcode::call(rcexpr.opnos[i])?;
                if !lsyscache::get_func_leakproof::call(funcid)?
                    && (var_seam::contain_var_clause::call(&rcexpr.largs[i])
                        || var_seam::contain_var_clause::call(&rcexpr.rargs[i]))
                {
                    return Ok(true);
                }
            }
        }

        etag::T_MinMaxExpr => {
            let minmaxexpr = node.as_minmaxexpr().unwrap();
            // MinMaxExpr is leakproof if the comparison function it calls is.
            let cmp_proc = clauses_seam::type_cmp_proc::call(minmaxexpr.minmaxtype)?;
            let leakproof = if cmp_proc != InvalidOid {
                lsyscache::get_func_leakproof::call(cmp_proc)?
            } else {
                false
            };
            if !leakproof {
                for arg in &minmaxexpr.args {
                    if var_seam::contain_var_clause::call(arg) {
                        return Ok(true);
                    }
                }
            }
        }

        etag::T_CurrentOfExpr => {
            // WHERE CURRENT OF doesn't contain leaky function calls.
            return Ok(false);
        }

        _ => {
            // Unrecognized node tag: assume it might be leaky.
            return Ok(true);
        }
    }
    walk_default(node, &mut contain_leaked_vars_walker)
}

// ===========================================================================
// find_nonnullable_rels (clauses.c:1457)
// ===========================================================================

/// `find_nonnullable_rels(clause)` (clauses.c:1457) — which base rels are
/// forced nonnullable by the given clause. The relids set is allocated in `mcx`.
pub fn find_nonnullable_rels<'mcx>(
    mcx: Mcx<'mcx>,
    clause: Option<&Expr>,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    find_nonnullable_rels_walker(mcx, clause, true)
}

fn find_nonnullable_rels_walker<'mcx>(
    mcx: Mcx<'mcx>,
    node: Option<&Expr>,
    top_level: bool,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    let mut result: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None;
    let Some(node) = node else { return Ok(None) };

    if let Some(var) = node.as_var() {
        if var.varlevelsup == 0 {
            result = Some(bms::bms_make_singleton(mcx, var.varno)?);
        }
    } else if let Some(expr) = node.as_boolexpr() {
        match expr.boolop {
            BoolExprType::AND_EXPR => {
                if top_level {
                    result = find_nonnullable_rels_list(mcx, &expr.args, top_level)?;
                } else {
                    result = nonnullable_rels_intersect(mcx, &expr.args, top_level)?;
                }
            }
            BoolExprType::OR_EXPR => {
                result = nonnullable_rels_intersect(mcx, &expr.args, top_level)?;
            }
            BoolExprType::NOT_EXPR => {
                result = find_nonnullable_rels_list(mcx, &expr.args, false)?;
            }
        }
    } else if let Some(expr) = node.as_funcexpr() {
        if lsyscache::func_strict::call(expr.funcid)? {
            result = find_nonnullable_rels_list(mcx, &expr.args, false)?;
        }
    } else if let Some(op) = node.as_opexpr() {
        // Resolve opfuncid without cloning the node — its args may carry an
        // owned-subtree child (SubPlan/SubLink) whose derived `Expr::clone`
        // panics. (C's set_opfuncid scribbles the field in place; we read-only.)
        let opfuncid = ::nodes_core::nodefuncs::resolved_opfuncid(op.opno, op.opfuncid)?;
        if lsyscache::func_strict::call(opfuncid)? {
            result = find_nonnullable_rels_list(mcx, &op.args, false)?;
        }
    } else if let Some(expr) = node.as_scalararrayopexpr() {
        if is_strict_saop(mcx, expr, true)? {
            result = find_nonnullable_rels_list(mcx, &expr.args, false)?;
        }
    } else if let Some(expr) = node.as_relabeltype() {
        result = find_nonnullable_rels_walker(mcx, expr.arg.as_deref(), top_level)?;
    } else if let Some(expr) = node.as_coerceviaio() {
        result = find_nonnullable_rels_walker(mcx, expr.arg.as_deref(), top_level)?;
    } else if let Some(expr) = node.as_arraycoerceexpr() {
        result = find_nonnullable_rels_walker(mcx, expr.arg.as_deref(), top_level)?;
    } else if let Some(expr) = node.as_convertrowtypeexpr() {
        result = find_nonnullable_rels_walker(mcx, expr.arg.as_deref(), top_level)?;
    } else if let Some(expr) = node.as_collateexpr() {
        result = find_nonnullable_rels_walker(mcx, expr.arg.as_deref(), top_level)?;
    } else if let Some(expr) = node.as_nulltest() {
        if top_level && expr.nulltesttype == NullTestType::IS_NOT_NULL && !expr.argisrow {
            result = find_nonnullable_rels_walker(mcx, expr.arg.as_deref(), false)?;
        }
    } else if let Some(expr) = node.as_booleantest() {
        let btt = expr.booltesttype;
        if top_level
            && (btt == BoolTestType::IS_TRUE
                || btt == BoolTestType::IS_FALSE
                || btt == BoolTestType::IS_NOT_UNKNOWN)
        {
            result = find_nonnullable_rels_walker(mcx, expr.arg.as_deref(), false)?;
        }
    } else if let Some(sp) = node.as_subplan() {
        let splan = &sp.0;
        let slt = splan.subLinkType;
        if (top_level && slt == SubLinkType::Any) || slt == SubLinkType::RowCompare {
            result = find_nonnullable_rels_walker(
                mcx,
                splan.testexpr.as_deref(),
                top_level,
            )?;
        }
    } else if let Some(phv) = node.as_placeholdervar() {
        // If the contained expression forces any rels non-nullable, so does the
        // PHV. The lifetime-free PHV carries phexpr as an owned Box<Expr>.
        result = find_nonnullable_rels_walker(mcx, phv.phexpr.as_deref(), top_level)?;
        // C: if (phlevelsup == 0 && bms_membership(phrels) == SINGLETON)
        //        result = bms_add_members(result, phrels);
        if phv.phlevelsup == 0 {
            let phrels = exprrelids_to_bms(mcx, &phv.phrels)?;
            if bms::bms_membership(phrels.as_deref()) == BMS_Membership::BMS_SINGLETON {
                result = bms::bms_add_members(mcx, result, phrels.as_deref())?;
            }
        }
    }
    Ok(result)
}

/// The `IsA(node, List)` arm of `find_nonnullable_rels_walker`: union over the
/// arms, passing `top_level` unmodified.
fn find_nonnullable_rels_list<'mcx>(
    mcx: Mcx<'mcx>,
    args: &[Expr],
    top_level: bool,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    let mut result: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None;
    for a in args {
        let sub = find_nonnullable_rels_walker(mcx, Some(a), top_level)?;
        result = bms::bms_join(result, sub);
    }
    Ok(result)
}

/// The shared OR / below-top-level-AND intersection over `expr->args`.
fn nonnullable_rels_intersect<'mcx>(
    mcx: Mcx<'mcx>,
    args: &[Expr],
    top_level: bool,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    let mut result: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None;
    let mut first = true;
    for a in args {
        let subresult = find_nonnullable_rels_walker(mcx, Some(a), top_level)?;
        if first {
            result = subresult;
            first = false;
        } else {
            result = bms::bms_int_members(result, subresult.as_deref());
        }
        if bms::bms_is_empty(result.as_deref()) {
            break;
        }
    }
    Ok(result)
}

/// Convert the lifetime-free `ExprRelids` (a `PlaceHolderVar`/`Var`'s word
/// storage) into an `mcx`-allocated `Bitmapset` for the `bms_*` algebra. The
/// empty word vector is the empty set (`None`).
fn exprrelids_to_bms<'mcx>(
    mcx: Mcx<'mcx>,
    relids: &::nodes::primnodes::ExprRelids,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    let mut result: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None;
    // Words are 64-bit; bit i of word w is member (w*64 + i).
    for (w, &word) in relids.words.iter().enumerate() {
        let mut bits = word;
        while bits != 0 {
            let b = bits.trailing_zeros() as usize;
            let member = (w * 64 + b) as i32;
            result = Some(bms::bms_add_member(mcx, result, member)?);
            bits &= bits - 1;
        }
    }
    Ok(result)
}

// ===========================================================================
// find_nonnullable_vars (clauses.c:1708)
// ===========================================================================

/// `find_nonnullable_vars(clause)` (clauses.c:1708) — which Vars are forced
/// nonnullable by the given clause. Result is a multibitmapset (in `mcx`).
pub fn find_nonnullable_vars<'mcx>(
    mcx: Mcx<'mcx>,
    clause: Option<&Expr>,
) -> PgResult<MultiBitmapset<'mcx>> {
    find_nonnullable_vars_walker(mcx, clause, true)
}

fn find_nonnullable_vars_walker<'mcx>(
    mcx: Mcx<'mcx>,
    node: Option<&Expr>,
    top_level: bool,
) -> PgResult<MultiBitmapset<'mcx>> {
    let mut result: MultiBitmapset<'mcx> = ::mcx::vec_with_capacity_in(mcx, 0)?;
    let Some(node) = node else { return Ok(result) };

    if let Some(var) = node.as_var() {
        if var.varlevelsup == 0 {
            result = mbms::mbms_add_member(
                mcx,
                result,
                var.varno,
                var.varattno as i32 - FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER,
            )?;
        }
    } else if let Some(expr) = node.as_funcexpr() {
        if lsyscache::func_strict::call(expr.funcid)? {
            result = find_nonnullable_vars_list(mcx, &expr.args, false)?;
        }
    } else if node.is_opexpr() {
        let mut tmp = node.clone();
        let opfuncid = {
            let op = tmp.as_opexpr_mut().expect("is_opexpr");
            set_opfuncid(op)?;
            op.opfuncid
        };
        if lsyscache::func_strict::call(opfuncid)? {
            let args = tmp.as_opexpr().expect("is_opexpr").args.clone();
            result = find_nonnullable_vars_list(mcx, &args, false)?;
        }
    } else if let Some(expr) = node.as_scalararrayopexpr() {
        if is_strict_saop(mcx, expr, true)? {
            result = find_nonnullable_vars_list(mcx, &expr.args, false)?;
        }
    } else if let Some(expr) = node.as_boolexpr() {
        match expr.boolop {
            BoolExprType::AND_EXPR => {
                if top_level {
                    result = find_nonnullable_vars_list(mcx, &expr.args, top_level)?;
                } else {
                    result = nonnullable_vars_intersect(mcx, &expr.args, top_level)?;
                }
            }
            BoolExprType::OR_EXPR => {
                result = nonnullable_vars_intersect(mcx, &expr.args, top_level)?;
            }
            BoolExprType::NOT_EXPR => {
                result = find_nonnullable_vars_list(mcx, &expr.args, false)?;
            }
        }
    } else if let Some(expr) = node.as_relabeltype() {
        result = find_nonnullable_vars_walker(mcx, expr.arg.as_deref(), top_level)?;
    } else if let Some(expr) = node.as_coerceviaio() {
        result = find_nonnullable_vars_walker(mcx, expr.arg.as_deref(), false)?;
    } else if let Some(expr) = node.as_arraycoerceexpr() {
        result = find_nonnullable_vars_walker(mcx, expr.arg.as_deref(), top_level)?;
    } else if let Some(expr) = node.as_convertrowtypeexpr() {
        result = find_nonnullable_vars_walker(mcx, expr.arg.as_deref(), top_level)?;
    } else if let Some(expr) = node.as_collateexpr() {
        result = find_nonnullable_vars_walker(mcx, expr.arg.as_deref(), top_level)?;
    } else if let Some(expr) = node.as_nulltest() {
        if top_level && expr.nulltesttype == NullTestType::IS_NOT_NULL && !expr.argisrow {
            result = find_nonnullable_vars_walker(mcx, expr.arg.as_deref(), false)?;
        }
    } else if let Some(expr) = node.as_booleantest() {
        let btt = expr.booltesttype;
        if top_level
            && (btt == BoolTestType::IS_TRUE
                || btt == BoolTestType::IS_FALSE
                || btt == BoolTestType::IS_NOT_UNKNOWN)
        {
            result = find_nonnullable_vars_walker(mcx, expr.arg.as_deref(), false)?;
        }
    } else if let Some(sp) = node.as_subplan() {
        let splan = &sp.0;
        let slt = splan.subLinkType;
        if (top_level && slt == SubLinkType::Any) || slt == SubLinkType::RowCompare {
            result = find_nonnullable_vars_walker(
                mcx,
                splan.testexpr.as_deref(),
                top_level,
            )?;
        }
    } else if let Some(phv) = node.as_placeholdervar() {
        result = find_nonnullable_vars_walker(mcx, phv.phexpr.as_deref(), top_level)?;
    }
    Ok(result)
}

/// The `IsA(node, List)` arm of `find_nonnullable_vars_walker`: union.
fn find_nonnullable_vars_list<'mcx>(
    mcx: Mcx<'mcx>,
    args: &[Expr],
    top_level: bool,
) -> PgResult<MultiBitmapset<'mcx>> {
    let mut result: MultiBitmapset<'mcx> = ::mcx::vec_with_capacity_in(mcx, 0)?;
    for a in args {
        let sub = find_nonnullable_vars_walker(mcx, Some(a), top_level)?;
        result = mbms::mbms_add_members(mcx, result, &sub)?;
    }
    Ok(result)
}

/// The shared OR / below-top-level-AND intersection over `expr->args`.
fn nonnullable_vars_intersect<'mcx>(
    mcx: Mcx<'mcx>,
    args: &[Expr],
    top_level: bool,
) -> PgResult<MultiBitmapset<'mcx>> {
    let mut result: MultiBitmapset<'mcx> = ::mcx::vec_with_capacity_in(mcx, 0)?;
    let mut first = true;
    for a in args {
        let subresult = find_nonnullable_vars_walker(mcx, Some(a), top_level)?;
        if first {
            result = subresult;
            first = false;
        } else {
            result = mbms::mbms_int_members(result, &subresult);
        }
        // if (result == NIL) break;
        if result.is_empty() {
            break;
        }
    }
    Ok(result)
}

// ===========================================================================
// find_forced_null_vars / find_forced_null_var (clauses.c:1917)
// ===========================================================================

/// `find_forced_null_vars(node)` (clauses.c:1917) — which Vars must be NULL for
/// the given clause to return TRUE. Result is a multibitmapset (in `mcx`).
pub fn find_forced_null_vars<'mcx>(
    mcx: Mcx<'mcx>,
    node: Option<&Expr>,
) -> PgResult<MultiBitmapset<'mcx>> {
    let mut result: MultiBitmapset<'mcx> = ::mcx::vec_with_capacity_in(mcx, 0)?;
    let Some(node) = node else { return Ok(result) };
    // Check single-clause cases using subroutine.
    if let Some(var) = find_forced_null_var(Some(node)).and_then(|n| n.as_var()) {
        result = mbms::mbms_add_member(
            mcx,
            result,
            var.varno,
            var.varattno as i32 - FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER,
        )?;
    } else if let Some(expr) = node.as_boolexpr() {
        if expr.boolop == BoolExprType::AND_EXPR {
            // At top level we can just recurse to the List case.
            result = find_forced_null_vars_list(mcx, &expr.args)?;
        }
    }
    Ok(result)
}

/// The `IsA(node, List)` arm of `find_forced_null_vars`: union.
fn find_forced_null_vars_list<'mcx>(
    mcx: Mcx<'mcx>,
    args: &[Expr],
) -> PgResult<MultiBitmapset<'mcx>> {
    let mut result: MultiBitmapset<'mcx> = ::mcx::vec_with_capacity_in(mcx, 0)?;
    for a in args {
        let sub = find_forced_null_vars(mcx, Some(a))?;
        result = mbms::mbms_add_members(mcx, result, &sub)?;
    }
    Ok(result)
}

/// `find_forced_null_var(node)` (clauses.c:1978) — the Var forced null by the
/// given clause, or `None` if not an `IS NULL`-type clause.
pub fn find_forced_null_var<'a, 'mcx>(node: Option<&'a Expr<'mcx>>) -> Option<&'a Expr<'mcx>> {
    let node = node?;
    if let Some(expr) = node.as_nulltest() {
        // check for var IS NULL
        if expr.nulltesttype == NullTestType::IS_NULL && !expr.argisrow {
            if let Some(arg) = expr.arg.as_deref() {
                if let Some(var) = arg.as_var() {
                    if var.varlevelsup == 0 {
                        return Some(arg);
                    }
                }
            }
        }
    } else if let Some(expr) = node.as_booleantest() {
        // var IS UNKNOWN is equivalent to var IS NULL
        if expr.booltesttype == BoolTestType::IS_UNKNOWN {
            if let Some(arg) = expr.arg.as_deref() {
                if let Some(var) = arg.as_var() {
                    if var.varlevelsup == 0 {
                        return Some(arg);
                    }
                }
            }
        }
    }
    None
}

// ===========================================================================
// is_strict_saop (clauses.c:2026)
// ===========================================================================

/// `is_strict_saop(expr, falseOK)` (clauses.c:2026).
pub(crate) fn is_strict_saop<'mcx>(
    mcx: Mcx<'mcx>,
    expr: &ScalarArrayOpExpr,
    false_ok: bool,
) -> PgResult<bool> {
    // The contained operator must be strict. C's set_sa_opfuncid scribbles
    // opfuncid in place; cloning the whole node to get an owned `&mut` would
    // deep-copy `args`, panicking if a child is an `Aggref`/`SubPlan`/`SubLink`
    // (derived `Expr::clone` traps). Resolve it read-only via catalog lookup,
    // the same precedent as `expression_returns_set_rows` / `find_nonnullable_rels`.
    let opfuncid = ::nodes_core::nodefuncs::resolved_opfuncid(expr.opno, expr.opfuncid)?;
    if !lsyscache::func_strict::call(opfuncid)? {
        return Ok(false);
    }
    // If ANY and falseOK, that's all we need to check.
    if expr.useOr && false_ok {
        return Ok(true);
    }
    // Else, we have to see if the array is provably non-empty.
    // Assert(list_length(expr->args) == 2);
    let rightop = expr.args.get(1);
    if let Some(c) = rightop.and_then(|n| n.as_const()) {
        if c.constisnull {
            return Ok(false);
        }
        let nitems = arrayfuncs_seams::array_const_nitems::call(
            mcx,
            c.constvalue.as_ref_bytes(),
        )?;
        if nitems > 0 {
            return Ok(true);
        }
    } else if let Some(arrayexpr) = rightop.and_then(|n| n.as_arrayexpr()) {
        if !arrayexpr.elements.is_empty() && !arrayexpr.multidims {
            return Ok(true);
        }
    }
    Ok(false)
}

// ===========================================================================
// Pseudo-constant clause checks (clauses.c:2089)
// ===========================================================================

/// `is_pseudo_constant_clause(clause)` (clauses.c:2089).
pub fn is_pseudo_constant_clause(clause: Option<&Expr>) -> PgResult<bool> {
    // Look for Vars first; only check for volatile functions if no Vars.
    let has_var = match clause {
        Some(c) => var_seam::contain_var_clause::call(c),
        None => false,
    };
    if !has_var && !contain_volatile_functions(clause)? {
        return Ok(true);
    }
    Ok(false)
}

/// `is_pseudo_constant_clause_relids(clause, relids)` (clauses.c:2108).
pub fn is_pseudo_constant_clause_relids(
    clause: Option<&Expr>,
    relids: Option<&Bitmapset>,
) -> PgResult<bool> {
    if bms::bms_is_empty(relids) && !contain_volatile_functions(clause)? {
        return Ok(true);
    }
    Ok(false)
}

// ===========================================================================
// General clause-manipulating routines (clauses.c:2131)
// ===========================================================================

/// `NumRelids(root, clause)` (clauses.c:2131) — the number of distinct base
/// relations referenced in `clause`. Subtracting `root->outer_join_rels`
/// requires a live `PlannerInfo`, which the `Expr` model does not thread, so the
/// whole routine rides the var/planner-owned `num_relids` seam.
pub fn num_relids(clause: Option<&Expr>) -> PgResult<i32> {
    match clause {
        Some(c) => var_seam::num_relids::call(c),
        None => Ok(0),
    }
}

/// `CommuteOpExpr(clause)` (clauses.c:2148) — commute a binary operator clause.
/// XXX the clause is destructively modified!
pub fn CommuteOpExpr(clause: &mut OpExpr) -> PgResult<()> {
    // Sanity checks: caller is at fault if these fail. (is_opclause is always
    // true for an OpExpr; check binary arity.)
    if clause.args.len() != 2 {
        return Err(elog_error("cannot commute non-binary-operator clause"));
    }
    let opoid = lsyscache::get_commutator::call(clause.opno)?;
    if opoid == InvalidOid {
        return Err(elog_error(alloc::format!(
            "could not find commutator for operator {}",
            clause.opno
        )));
    }
    // modify the clause in-place!
    clause.opno = opoid;
    clause.opfuncid = InvalidOid;
    // opresulttype, opretset, opcollid, inputcollid need not change.
    clause.args.swap(0, 1);
    Ok(())
}

// ===========================================================================
// convert_saop_to_hashed_saop (clauses.c:2287)
// ===========================================================================

/// `convert_saop_to_hashed_saop(node)` (clauses.c:2287) — recursively search
/// `node` for `ScalarArrayOpExpr`s and fill in the hash function for any that
/// would be useful to evaluate using a hash table. Destructively modifies
/// `node` in place.
pub fn convert_saop_to_hashed_saop<'mcx>(mcx: Mcx<'mcx>, node: &mut Expr<'mcx>) -> PgResult<()> {
    // The Expr model exposes only the consume/rebuild `expression_tree_mutator`
    // for in-place rewrites; take the node out, transform it, and put it back.
    let taken = core::mem::replace(node, Expr::Const(::nodes::primnodes::Const::default()));
    let mut err: Option<PgError> = None;
    *node = convert_saop_to_hashed_saop_walker(mcx, taken, &mut err);
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Visit `node`: if it is a `ScalarArrayOpExpr`, fill in the hash function as
/// per clauses.c, then recurse into its children (via the mutator). Returns the
/// (possibly modified) node.
fn convert_saop_to_hashed_saop_walker<'mcx>(
    mcx: Mcx<'mcx>,
    mut node: Expr<'mcx>,
    err: &mut Option<PgError>,
) -> Expr<'mcx> {
    if err.is_some() {
        return node;
    }
    // If this node is a SAOP, fill in the hash function (no recursion into a
    // matched SAOP, mirroring the C `return false` after handling it).
    if let Some(saop) = node.as_scalararrayopexpr_mut() {
        match try_hash_saop(mcx, saop) {
            Ok(true) => return node, // handled; don't recurse into the SAOP
            Ok(false) => {}          // not handled; fall through to recurse
            Err(e) => {
                *err = Some(e);
                return node;
            }
        }
    }

    // Recurse into children (the mutator rebuilds the node with each child
    // transformed — equivalent to C's expression_tree_walker recursion).
    ::nodes_core::nodefuncs::expression_tree_mutator(node, &mut |child| {
        convert_saop_to_hashed_saop_walker(mcx, child, err)
    })
}

/// Try to fill in the hash function of a single `ScalarArrayOpExpr`. Returns
/// `Ok(true)` if this was a hashable SAOP whose handling is complete (the C
/// `return false` — don't recurse), `Ok(false)` if not a hashable SAOP (fall
/// through to recurse), or `Err` on a catalog lookup failure.
fn try_hash_saop<'mcx>(mcx: Mcx<'mcx>, saop: &mut ScalarArrayOpExpr) -> PgResult<bool> {
    // arrayarg = (Expr *) lsecond(saop->args); must be a non-null array Const.
    let arrconst = match saop.args.get(1).and_then(|n| n.as_const()) {
        Some(c) if !c.constisnull => c.clone(),
        _ => return Ok(false),
    };
    if saop.useOr {
        if let Some((lefthashfunc, righthashfunc)) =
            lsyscache::get_op_hash_functions::call(saop.opno)?
        {
            if lefthashfunc == righthashfunc {
                let nitems = arrayfuncs_seams::array_const_nitems::call(
                    mcx,
                    arrconst.constvalue.as_ref_bytes(),
                )?;
                if nitems >= MIN_ARRAY_SIZE_FOR_HASHED_SAOP {
                    saop.hashfuncid = lefthashfunc;
                }
                return Ok(true);
            }
        }
    } else {
        // !saop->useOr
        let negator = lsyscache::get_negator::call(saop.opno)?;
        if negator != InvalidOid {
            if let Some((lefthashfunc, righthashfunc)) =
                lsyscache::get_op_hash_functions::call(negator)?
            {
                if lefthashfunc == righthashfunc {
                    let nitems = arrayfuncs_seams::array_const_nitems::call(
                        mcx,
                        arrconst.constvalue.as_ref_bytes(),
                    )?;
                    if nitems >= MIN_ARRAY_SIZE_FOR_HASHED_SAOP {
                        saop.hashfuncid = lefthashfunc;
                        saop.negfuncid = lsyscache::get_opcode::call(negator)?;
                    }
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

// ===========================================================================
// pull_paramids (clauses.c:5419)
// ===========================================================================

/// `pull_paramids(expr)` (clauses.c:5419) — the set of `paramid`s of all `Param`
/// nodes in `expr` (a relids set, allocated in `mcx`).
pub fn pull_paramids<'mcx>(
    mcx: Mcx<'mcx>,
    expr: Option<&Expr>,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    let mut result: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None;
    let mut err: Option<PgError> = None;
    pull_paramids_walker(mcx, expr, &mut result, &mut err);
    if let Some(e) = err {
        return Err(e);
    }
    Ok(result)
}

fn pull_paramids_walker<'mcx>(
    mcx: Mcx<'mcx>,
    node: Option<&Expr>,
    result: &mut Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    err: &mut Option<PgError>,
) -> bool {
    let Some(node) = node else { return false };
    if let Some(param) = node.as_param() {
        // *context = bms_add_member(*context, param->paramid);
        match bms::bms_add_member(mcx, result.take(), param.paramid) {
            Ok(set) => *result = Some(set),
            Err(e) => {
                *err = Some(e);
                return true;
            }
        }
        return false;
    }
    etw(Some(node), &mut |n| pull_paramids_walker(mcx, Some(n), result, err))
}
