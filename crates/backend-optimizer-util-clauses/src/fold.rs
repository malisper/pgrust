//! The constant-folding engine: `eval_const_expressions` /
//! `estimate_expression_value` (+ the `eval_const_expressions_mutator` arm
//! cluster) and `make_SAOP_expr`, ported 1:1 from `clauses.c` (PostgreSQL 18.3)
//! over the lifetime-free owned [`Expr`] tree.
//!
//! # Model
//!
//! * The recursion engine is
//!   [`backend_nodes_core::nodefuncs::expression_tree_mutator`]
//!   (`ece_generic_processing`); errors thread through the non-`Result` mutator
//!   callback via a captured error slot.
//! * `eval_const_expressions_context`: this model has no `PlannerInfo` and no
//!   bound `ParamListInfo`, so the `T_Param` arm never substitutes a bound
//!   parameter (identical to C with `boundParams == NULL`) and
//!   `record_plan_*_dependency` is skipped (identical to C with `root == NULL`).
//! * Function EXECUTION rides the `fmgr_call` seam (the executor's
//!   strictness-honoring `FunctionCallInvoke`); the `pg_proc` form read rides
//!   `get_func_form`; the in-crate evaluator handles the `Const` / all-`Const`
//!   `FuncExpr` / `OpExpr` / `NullIfExpr` shapes, with the rest of
//!   `ece_evaluate_expr` riding `evaluate_expr_fallback` (C folds these through
//!   the executor, so a silent skip is not allowed — the seam panics until its
//!   owner lands).
//! * SQL-function inlining: the cheap catalog gates are in-crate; the
//!   parse/analyze/rewrite body rides `inline_sql_function`.
//! * C's `check_stack_depth()` is an explicit recursion-depth guard.

use alloc::boxed::Box;
use alloc::collections::VecDeque;
use alloc::format;
use alloc::vec::Vec;

use mcx::Mcx;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult};
use types_nodes::primnodes::{
    etag, ArrayExpr, BoolExprType, CaseWhen, CoercionForm, Const, Expr, NullTest, NullTestType,
    ScalarArrayOpExpr,
};

use backend_nodes_core::makefuncs::{make_andclause, make_bool_const, make_const, make_null_const};
use backend_nodes_core::nodefuncs::{
    apply_relabel_type, expr_collation, expr_type, expr_typmod, expression_tree_mutator,
    expression_tree_walker, fix_opfuncids, set_opfuncid, set_sa_opfuncid,
};

use backend_optimizer_prep_prepqual_seams::negate_clause as negate_clause_seam;
use backend_optimizer_util_clauses_seams as clauses_seam;
use backend_optimizer_util_clauses_seams::PgProcSimple;
use backend_utils_cache_lsyscache_seams as lsyscache;

use crate::grounded::contain_mutable_functions;

// ---------------------------------------------------------------------------
// Constants (pg_type.dat / pg_operator.dat / pg_proc.h / pg_config_manual.h).
// ---------------------------------------------------------------------------

/// `INT4OID` (pg_type.dat).
const INT4OID: Oid = 23;
/// `OIDOID` (pg_type.dat).
const OIDOID: Oid = 26;
/// `RECORDOID` (pg_type.dat).
const RECORDOID: Oid = 2249;
/// `CSTRINGOID` (pg_type.dat).
const CSTRINGOID: Oid = 2275;
/// `BOOLOID` (pg_type.dat).
const BOOLOID: Oid = 16;

/// `BooleanEqualOperator` (pg_operator.dat).
const BOOLEAN_EQUAL_OPERATOR: Oid = 91;
/// `BooleanNotEqualOperator` (pg_operator.dat).
const BOOLEAN_NOT_EQUAL_OPERATOR: Oid = 85;

const PROVOLATILE_IMMUTABLE: u8 = b'i';
const PROVOLATILE_STABLE: u8 = b's';

/// `FUNC_MAX_ARGS` (pg_config_manual.h).
const FUNC_MAX_ARGS: i32 = 100;

/// Recursion guard standing in for C's `check_stack_depth()`.
const MAX_FOLD_DEPTH: u32 = 4096;

/// The canonical `Datum<'static>` carried by a `Const`.
type CDatum = types_tuple::backend_access_common_heaptuple::Datum<'static>;

/// `DatumGetBool(d)` — `((bool) ((d) & 1))`.
#[inline]
fn datum_get_bool(d: &types_tuple::backend_access_common_heaptuple::Datum<'_>) -> bool {
    (d.as_usize() & 1) != 0
}

// ---------------------------------------------------------------------------
// Context + public entry points
// ---------------------------------------------------------------------------

/// `eval_const_expressions_context` (clauses.c:61). `boundParams` / `root` are
/// absent in this model (see module docs); `active_fns` is present, threaded
/// through the SQL-function inliner's recursion guard.
struct EceContext<'mcx> {
    mcx: Mcx<'mcx>,
    /// Constant test value for the CASE construct currently being examined.
    case_val: Option<Expr>,
    /// Unsafe (estimation-time-only) transformations OK?
    estimate: bool,
    /// Recursion depth (C: `check_stack_depth()`).
    depth: u32,
    /// `active_fns` (clauses.c:64) — funcids currently being inlined, the
    /// recursion guard for directly/indirectly recursive SQL functions.
    active_fns: Vec<Oid>,
}

/// C: `Node *eval_const_expressions(PlannerInfo *root, Node *node)`
/// (clauses.c:2254). Safe transformations only.
pub fn eval_const_expressions(mcx: Mcx<'_>, node: Expr) -> PgResult<Expr> {
    let mut ctx = EceContext {
        mcx,
        case_val: None,
        estimate: false,
        depth: 0,
        active_fns: Vec::new(),
    };
    mutate(node, &mut ctx)
}

/// C: `Node *estimate_expression_value(PlannerInfo *root, Node *node)`
/// (clauses.c:2395). Estimation mode: unsafe transformations OK (stable
/// functions fold; PlaceHolderVars strip).
pub fn estimate_expression_value(mcx: Mcx<'_>, node: Expr) -> PgResult<Expr> {
    let mut ctx = EceContext {
        mcx,
        case_val: None,
        estimate: true,
        depth: 0,
        active_fns: Vec::new(),
    };
    mutate(node, &mut ctx)
}

// ---------------------------------------------------------------------------
// ece_* helpers (clauses.c:2409-2435)
// ---------------------------------------------------------------------------

fn placeholder_node() -> Expr {
    Expr::Const(Const::default())
}

fn bool_const(value: bool, isnull: bool) -> Expr {
    Expr::Const(make_bool_const(value, isnull))
}

/// `ece_generic_processing(node)` — copy the node, const-simplifying its
/// arguments via `expression_tree_mutator`.
fn ece_generic_processing(node: Expr, ctx: &mut EceContext) -> PgResult<Expr> {
    let mut err: Option<PgError> = None;
    let out = expression_tree_mutator(node, &mut |child| {
        if err.is_some() {
            return placeholder_node();
        }
        match mutate(child, ctx) {
            Ok(n) => n,
            Err(e) => {
                err = Some(e);
                placeholder_node()
            }
        }
    });
    match err {
        Some(e) => Err(e),
        None => Ok(out),
    }
}

/// `MUTATE` over a `Vec<Expr>` child list.
fn mutate_list(list: Vec<Expr>, ctx: &mut EceContext) -> PgResult<Vec<Expr>> {
    let mut out: Vec<Expr> = Vec::with_capacity(list.len());
    for n in list {
        out.push(mutate(n, ctx)?);
    }
    Ok(out)
}

/// `MUTATE` over an `Option<Box<Expr>>` child.
fn mutate_opt(opt: Option<Box<Expr>>, ctx: &mut EceContext) -> PgResult<Option<Box<Expr>>> {
    match opt {
        Some(b) => Ok(Some(Box::new(mutate(*b, ctx)?))),
        None => Ok(None),
    }
}

/// `ece_all_arguments_const(node)` — true if all of `node`'s direct expression
/// children are `Const`.
fn ece_all_arguments_const(node: &Expr) -> bool {
    // C's contain_non_const_walker: Const -> keep going; anything else -> abort.
    !expression_tree_walker(Some(node), &mut |child| !child.is_const())
}

/// `ece_function_is_safe(funcid, context)` (clauses.c:3744).
fn ece_function_is_safe(funcid: Oid, ctx: &EceContext) -> PgResult<bool> {
    let provolatile = lsyscache::func_volatile::call(funcid)?;
    Ok(provolatile == PROVOLATILE_IMMUTABLE || (ctx.estimate && provolatile == PROVOLATILE_STABLE))
}

/// `ece_evaluate_expr(node)` — `evaluate_expr` with the node's own
/// type/typmod/collation.
fn ece_evaluate_expr(node: Expr, ctx: &EceContext) -> PgResult<Expr> {
    let t = expr_type(Some(&node))?;
    let tm = expr_typmod(Some(&node))?;
    let c = expr_collation(Some(&node))?;
    evaluate_expr(ctx.mcx, node, t, tm, c)
}

/// Unwrap a child the C tree dereferences unconditionally.
fn req(opt: Option<Box<Expr>>, what: &str) -> PgResult<Expr> {
    opt.map(|b| *b)
        .ok_or_else(|| PgError::error(format!("eval_const_expressions: unexpected NULL {what}")))
}

// ---------------------------------------------------------------------------
// The mutator (clauses.c:2440)
// ---------------------------------------------------------------------------

fn mutate(node: Expr, ctx: &mut EceContext) -> PgResult<Expr> {
    // since this function recurses, it could be driven to stack overflow
    ctx.depth += 1;
    if ctx.depth > MAX_FOLD_DEPTH {
        ctx.depth -= 1;
        return Err(PgError::error(
            "stack depth limit exceeded (eval_const_expressions recursion guard)",
        ));
    }
    let r = mutate_inner(node, ctx);
    ctx.depth -= 1;
    r
}

fn mutate_inner(node: Expr, ctx: &mut EceContext) -> PgResult<Expr> {
    match node.expr_tag() {
        // T_Param (clauses.c:2447): with no bound ParamListInfo there is never
        // a value to substitute; C copies the Param. We own the node, return it.
        etag::T_Param => Ok(node),

        etag::T_WindowFunc => arm_windowfunc(node, ctx),
        etag::T_FuncExpr => arm_funcexpr(node, ctx),
        etag::T_OpExpr => arm_opexpr(node, ctx),
        etag::T_DistinctExpr => arm_distinctexpr(node, ctx),
        etag::T_NullIfExpr => arm_nullifexpr(node, ctx),
        etag::T_ScalarArrayOpExpr => arm_saop(node, ctx),
        etag::T_BoolExpr => arm_boolexpr(node, ctx),
        etag::T_JsonValueExpr => arm_jsonvalueexpr(node, ctx),

        // Return a SubPlan unchanged --- too late to do anything with it.
        etag::T_SubPlan | etag::T_AlternativeSubPlan => Ok(node),

        etag::T_RelabelType => {
            let r = node.expect_into_relabeltype();
            let arg = mutate(req(r.arg, "RelabelType.arg")?, ctx)?;
            apply_relabel_type(
                arg,
                r.resulttype,
                r.resulttypmod,
                r.resultcollid,
                r.relabelformat,
                r.location,
                true,
            )
        }
        etag::T_CoerceViaIO => arm_coerceviaio(node, ctx),
        etag::T_ArrayCoerceExpr => arm_arraycoerce(node, ctx),
        etag::T_CollateExpr => {
            // We replace CollateExpr with RelabelType.
            let c = node.expect_into_collateexpr();
            let arg = mutate(req(c.arg, "CollateExpr.arg")?, ctx)?;
            let t = expr_type(Some(&arg))?;
            let tm = expr_typmod(Some(&arg))?;
            apply_relabel_type(
                arg,
                t,
                tm,
                c.collOid,
                CoercionForm::COERCE_IMPLICIT_CAST,
                c.location,
                true,
            )
        }
        etag::T_CaseExpr => arm_case(node, ctx),
        etag::T_CaseTestExpr => {
            // If we know a constant test value for the current CASE construct,
            // substitute it for the placeholder.
            match &ctx.case_val {
                Some(v) => Ok(v.clone()),
                None => Ok(node),
            }
        }
        // Generic handling for node types whose own processing is immutable and
        // which need only "simplify if all inputs are constants".
        etag::T_SubscriptingRef | etag::T_ArrayExpr | etag::T_RowExpr | etag::T_MinMaxExpr => {
            let node = ece_generic_processing(node, ctx)?;
            if ece_all_arguments_const(&node) {
                return ece_evaluate_expr(node, ctx);
            }
            Ok(node)
        }
        etag::T_CoalesceExpr => arm_coalesce(node, ctx),
        etag::T_SQLValueFunction => {
            // All variants of SQLValueFunction are stable: fold in estimate mode.
            if ctx.estimate {
                let (t, tm) = {
                    let svf = node.as_sqlvaluefunction().expect("matched");
                    (svf.r#type, svf.typmod)
                };
                evaluate_expr(ctx.mcx, node, t, tm, InvalidOid)
            } else {
                Ok(node)
            }
        }
        etag::T_FieldSelect => arm_fieldselect(node, ctx),
        etag::T_NullTest => arm_nulltest(node, ctx),
        etag::T_BooleanTest => arm_booleantest(node, ctx),
        etag::T_CoerceToDomain => arm_coercetodomain(node, ctx),
        etag::T_PlaceHolderVar if ctx.estimate => {
            // In estimation mode, strip the PlaceHolderVar node altogether.
            let phv = node.expect_into_placeholdervar();
            let phexpr = phv.phexpr.map(|b| *b).ok_or_else(|| {
                PgError::error("eval_const_expressions: PlaceHolderVar without phexpr")
            })?;
            mutate(phexpr, ctx)
        }
        etag::T_ConvertRowtypeExpr => arm_convertrowtype(node, ctx),
        // For any node type not handled above, copy the node unchanged but
        // const-simplify its subexpressions.
        _ => ece_generic_processing(node, ctx),
    }
}

// ---------------------------------------------------------------------------
// Function / operator arms
// ---------------------------------------------------------------------------

/// T_WindowFunc arm (clauses.c:2497).
fn arm_windowfunc(node: Expr, ctx: &mut EceContext) -> PgResult<Expr> {
    let mut wf = node.expect_into_windowfunc();
    let form = clauses_seam::get_func_form::call(wf.winfnoid)?;
    let args = expand_function_arguments(
        core::mem::take(&mut wf.args),
        wf.wintype,
        wf.winfnoid,
        &form,
    )?;
    // Now, recursively simplify the args ...
    wf.args = mutate_list(args, ctx)?;
    // ... and the filter expression, which isn't.
    wf.aggfilter = mutate_opt(wf.aggfilter, ctx)?;
    Ok(Expr::WindowFunc(wf))
}

/// T_FuncExpr arm (clauses.c:2552).
fn arm_funcexpr(node: Expr, ctx: &mut EceContext) -> PgResult<Expr> {
    let result_typmod = expr_typmod(Some(&node))?;
    let mut f = node.expect_into_funcexpr();
    let args = core::mem::take(&mut f.args);
    let (simple, args) = simplify_function(
        f.funcid,
        f.funcresulttype,
        result_typmod,
        f.funccollid,
        f.inputcollid,
        args,
        f.funcvariadic,
        true,
        true,
        ctx,
    )?;
    if let Some(s) = simple {
        return Ok(s);
    }
    f.args = args;
    Ok(Expr::FuncExpr(f))
}

/// T_OpExpr arm (clauses.c:2589).
fn arm_opexpr(mut node: Expr, ctx: &mut EceContext) -> PgResult<Expr> {
    // Need to get OID of underlying function.
    set_opfuncid(node.as_opexpr_mut().expect("OpExpr"))?;
    let mut op = node.expect_into_opexpr();
    let args = core::mem::take(&mut op.args);
    let (simple, mut args) = simplify_function(
        op.opfuncid,
        op.opresulttype,
        -1,
        op.opcollid,
        op.inputcollid,
        args,
        false,
        true,
        true,
        ctx,
    )?;
    if let Some(s) = simple {
        return Ok(s);
    }
    // boolean equality / inequality with one constant argument
    if op.opno == BOOLEAN_EQUAL_OPERATOR || op.opno == BOOLEAN_NOT_EQUAL_OPERATOR {
        match simplify_boolean_equality(op.opno, args)? {
            Ok(simple) => return Ok(simple),
            Err(back) => args = back,
        }
    }
    op.args = args;
    Ok(Expr::OpExpr(op))
}

/// T_DistinctExpr arm (clauses.c:2643).
fn arm_distinctexpr(node: Expr, ctx: &mut EceContext) -> PgResult<Expr> {
    let mut d = node.expect_into_distinctexpr();
    d.args = mutate_list(core::mem::take(&mut d.args), ctx)?;

    let mut has_null_input = false;
    let mut all_null_input = true;
    let mut has_nonconst_input = false;
    for a in &d.args {
        match a.as_const() {
            Some(c) => {
                has_null_input |= c.constisnull;
                all_null_input &= c.constisnull;
            }
            None => has_nonconst_input = true,
        }
    }

    if !has_nonconst_input {
        // all nulls? then not distinct
        if all_null_input {
            return Ok(bool_const(false, false));
        }
        // one null? then distinct
        if has_null_input {
            return Ok(bool_const(true, false));
        }
        // otherwise try to evaluate the '=' operator
        if d.opfuncid == InvalidOid {
            d.opfuncid = lsyscache::get_opcode::call(d.opno)?;
        }
        let args = core::mem::take(&mut d.args);
        let (simple, args) = simplify_function(
            d.opfuncid,
            d.opresulttype,
            -1,
            d.opcollid,
            d.inputcollid,
            args,
            false,
            false,
            false,
            ctx,
        )?;
        if let Some(s) = simple {
            // Since the underlying operator is "=", must negate its result.
            let mut c = s.expect_into_const();
            c.constvalue = CDatum::from_bool(!datum_get_bool(&c.constvalue));
            return Ok(Expr::Const(c));
        }
        d.args = args;
    }
    Ok(Expr::DistinctExpr(d))
}

/// T_NullIfExpr arm (clauses.c:2749).
fn arm_nullifexpr(node: Expr, ctx: &mut EceContext) -> PgResult<Expr> {
    let node = ece_generic_processing(node, ctx)?;
    let mut ne = node.expect_into_nullifexpr();

    let mut has_nonconst_input = false;
    for a in &ne.args {
        match a.as_const() {
            Some(c) => {
                if c.constisnull {
                    return ne.args.into_iter().next().ok_or_else(|| {
                        PgError::error("arm_nullifexpr: NullIfExpr without arguments")
                    });
                }
            }
            None => has_nonconst_input = true,
        }
    }
    if ne.opfuncid == InvalidOid {
        ne.opfuncid = lsyscache::get_opcode::call(ne.opno)?;
    }
    if !has_nonconst_input && ece_function_is_safe(ne.opfuncid, ctx)? {
        return ece_evaluate_expr(Expr::NullIfExpr(ne), ctx);
    }
    Ok(Expr::NullIfExpr(ne))
}

/// T_ScalarArrayOpExpr arm (clauses.c:2780).
fn arm_saop(node: Expr, ctx: &mut EceContext) -> PgResult<Expr> {
    let mut node = ece_generic_processing(node, ctx)?;
    set_sa_opfuncid(node.as_scalararrayopexpr_mut().expect("SAOP"))?;
    let opfuncid = node.as_scalararrayopexpr().expect("SAOP").opfuncid;
    if ece_all_arguments_const(&node) && ece_function_is_safe(opfuncid, ctx)? {
        return ece_evaluate_expr(node, ctx);
    }
    Ok(node)
}

// ---------------------------------------------------------------------------
// BoolExpr arm + simplify_or/and_arguments + simplify_boolean_equality
// ---------------------------------------------------------------------------

/// T_BoolExpr arm (clauses.c:2802).
fn arm_boolexpr(node: Expr, ctx: &mut EceContext) -> PgResult<Expr> {
    let be = node.expect_into_boolexpr();
    match be.boolop {
        BoolExprType::OR_EXPR => {
            let mut have_null = false;
            let mut force_true = false;
            let mut newargs =
                simplify_or_arguments(be.args, ctx, &mut have_null, &mut force_true)?;
            if force_true {
                return Ok(bool_const(true, false));
            }
            if have_null {
                newargs.push(bool_const(false, true));
            }
            if newargs.is_empty() {
                return Ok(bool_const(false, false));
            }
            if newargs.len() == 1 {
                return Ok(newargs.pop().expect("len checked"));
            }
            Ok(make_orclause_(newargs))
        }
        BoolExprType::AND_EXPR => {
            let mut have_null = false;
            let mut force_false = false;
            let mut newargs =
                simplify_and_arguments(be.args, ctx, &mut have_null, &mut force_false)?;
            if force_false {
                return Ok(bool_const(false, false));
            }
            if have_null {
                newargs.push(bool_const(false, true));
            }
            if newargs.is_empty() {
                return Ok(bool_const(true, false));
            }
            if newargs.len() == 1 {
                return Ok(newargs.pop().expect("len checked"));
            }
            Ok(make_andclause(newargs))
        }
        BoolExprType::NOT_EXPR => {
            if be.args.len() != 1 {
                return Err(PgError::error(format!(
                    "NOT clause has {} arguments",
                    be.args.len()
                )));
            }
            let arg = mutate(be.args.into_iter().next().expect("len checked"), ctx)?;
            // Use negate_clause() to see if we can simplify away the NOT.
            negate_clause_seam::call(arg)
        }
    }
}

/// `make_orclause` returns `Expr` already in this repo.
#[inline]
fn make_orclause_(args: Vec<Expr>) -> Expr {
    backend_nodes_core::makefuncs::make_orclause(args)
}

#[inline]
fn is_orclause(n: &Expr) -> bool {
    n.as_boolexpr()
        .map(|b| b.boolop == BoolExprType::OR_EXPR)
        .unwrap_or(false)
}

#[inline]
fn is_andclause(n: &Expr) -> bool {
    n.as_boolexpr()
        .map(|b| b.boolop == BoolExprType::AND_EXPR)
        .unwrap_or(false)
}

/// `simplify_or_arguments` (clauses.c:3792).
fn simplify_or_arguments(
    args: Vec<Expr>,
    ctx: &mut EceContext,
    have_null: &mut bool,
    force_true: &mut bool,
) -> PgResult<Vec<Expr>> {
    let mut newargs: Vec<Expr> = Vec::new();
    let mut unprocessed: VecDeque<Expr> = args.into();
    while let Some(arg) = unprocessed.pop_front() {
        if is_orclause(&arg) {
            let subargs = arg.expect_into_boolexpr().args;
            for sub in subargs.into_iter().rev() {
                unprocessed.push_front(sub);
            }
            continue;
        }
        let arg = mutate(arg, ctx)?;
        if is_orclause(&arg) {
            let subargs = arg.expect_into_boolexpr().args;
            for sub in subargs.into_iter().rev() {
                unprocessed.push_front(sub);
            }
            continue;
        }
        if let Some(c) = arg.as_const() {
            if c.constisnull {
                *have_null = true;
            } else if datum_get_bool(&c.constvalue) {
                *force_true = true;
                return Ok(Vec::new());
            }
            continue; // drop constant-false input
        }
        newargs.push(arg);
    }
    Ok(newargs)
}

/// `simplify_and_arguments` (clauses.c:3898).
fn simplify_and_arguments(
    args: Vec<Expr>,
    ctx: &mut EceContext,
    have_null: &mut bool,
    force_false: &mut bool,
) -> PgResult<Vec<Expr>> {
    let mut newargs: Vec<Expr> = Vec::new();
    let mut unprocessed: VecDeque<Expr> = args.into();
    while let Some(arg) = unprocessed.pop_front() {
        if is_andclause(&arg) {
            let subargs = arg.expect_into_boolexpr().args;
            for sub in subargs.into_iter().rev() {
                unprocessed.push_front(sub);
            }
            continue;
        }
        let arg = mutate(arg, ctx)?;
        if is_andclause(&arg) {
            let subargs = arg.expect_into_boolexpr().args;
            for sub in subargs.into_iter().rev() {
                unprocessed.push_front(sub);
            }
            continue;
        }
        if let Some(c) = arg.as_const() {
            if c.constisnull {
                *have_null = true;
            } else if !datum_get_bool(&c.constvalue) {
                *force_false = true;
                return Ok(Vec::new());
            }
            continue; // drop constant-true input
        }
        newargs.push(arg);
    }
    Ok(newargs)
}

/// `simplify_boolean_equality(opno, args)` (clauses.c:3992). Returns
/// `Ok(Ok(simplified))` or `Ok(Err(args))` (the unchanged list).
fn simplify_boolean_equality(
    opno: Oid,
    mut args: Vec<Expr>,
) -> PgResult<Result<Expr, Vec<Expr>>> {
    if args.len() != 2 {
        return Err(PgError::error(format!(
            "boolean-equality operator with {} arguments",
            args.len()
        )));
    }
    let left_is_nonnull_const = args[0]
        .as_const()
        .map(|c| {
            debug_assert!(!c.constisnull);
            true
        })
        .unwrap_or(false);
    if left_is_nonnull_const {
        let rightop = args.pop().expect("len checked");
        let leftop = args.pop().expect("len checked");
        let lval = datum_get_bool(&leftop.expect_const().constvalue);
        return Ok(Ok(if opno == BOOLEAN_EQUAL_OPERATOR {
            if lval {
                rightop // true = foo
            } else {
                negate_clause_seam::call(rightop)? // false = foo
            }
        } else if lval {
            negate_clause_seam::call(rightop)? // true <> foo
        } else {
            rightop // false <> foo
        }));
    }
    let right_is_const = args[1].as_const().is_some();
    if right_is_const {
        let rightop = args.pop().expect("len checked");
        let leftop = args.pop().expect("len checked");
        let rc = rightop.expect_into_const();
        debug_assert!(!rc.constisnull);
        let rval = datum_get_bool(&rc.constvalue);
        return Ok(Ok(if opno == BOOLEAN_EQUAL_OPERATOR {
            if rval {
                leftop // foo = true
            } else {
                negate_clause_seam::call(leftop)? // foo = false
            }
        } else if rval {
            negate_clause_seam::call(leftop)? // foo <> true
        } else {
            leftop // foo <> false
        }));
    }
    Ok(Err(args))
}

// ---------------------------------------------------------------------------
// Coercion / CASE / COALESCE / test arms
// ---------------------------------------------------------------------------

/// T_CoerceViaIO arm (clauses.c:2967).
fn arm_coerceviaio(node: Expr, ctx: &mut EceContext) -> PgResult<Expr> {
    let mut cv = node.expect_into_coerceviaio();
    let arg = req(cv.arg.take(), "CoerceViaIO.arg")?;

    // Coercion functions don't care about input collation: pass InvalidOid.
    let (outfunc, _outtypisvarlena) =
        lsyscache::get_type_output_info::call(expr_type(Some(&arg))?)?;
    let (infunc, intypioparam) = lsyscache::get_type_input_info::call(cv.resulttype)?;

    // Make a List so we can use simplify_function.
    let args = alloc::vec![arg];
    let (simple, mut args) = simplify_function(
        outfunc, CSTRINGOID, -1, InvalidOid, InvalidOid, args, false, true, true, ctx,
    )?;
    if let Some(simple) = simple {
        // successfully simplified output fn; supply all 3 input-fn args.
        let a1 = Expr::Const(make_const(
            ctx.mcx,
            OIDOID,
            -1,
            InvalidOid,
            core::mem::size_of::<Oid>() as i32,
            CDatum::from_oid(intypioparam),
            false,
            true,
        )?);
        let a2 = Expr::Const(make_const(
            ctx.mcx,
            INT4OID,
            -1,
            InvalidOid,
            core::mem::size_of::<i32>() as i32,
            CDatum::from_i32(-1),
            false,
            true,
        )?);
        args = alloc::vec![simple, a1, a2];
        let (simple2, back) = simplify_function(
            infunc,
            cv.resulttype,
            -1,
            cv.resultcollid,
            InvalidOid,
            args,
            false,
            false,
            true,
            ctx,
        )?;
        if let Some(s) = simple2 {
            return Ok(s);
        }
        args = back;
    }
    // Build a replacement CoerceViaIO using the possibly-simplified argument.
    cv.arg = Some(Box::new(args.into_iter().next().ok_or_else(|| {
        PgError::error("CoerceViaIO simplification lost its argument")
    })?));
    Ok(Expr::CoerceViaIO(cv))
}

/// T_ArrayCoerceExpr arm (clauses.c:3053).
fn arm_arraycoerce(node: Expr, ctx: &mut EceContext) -> PgResult<Expr> {
    let mut ac = node.expect_into_arraycoerceexpr();
    ac.arg = mutate_opt(ac.arg, ctx)?;

    // Prevent the contained CaseTestExpr from absorbing an outer CASE value.
    let save_case_val = ctx.case_val.take();
    ac.elemexpr = mutate_opt(ac.elemexpr, ctx)?;
    ctx.case_val = save_case_val;

    // Fold if constant argument and immutable per-element expr. Exception:
    // don't treat CoerceToDomain as immutable.
    let foldable = match (&ac.arg, &ac.elemexpr) {
        (Some(arg), Some(elem)) => {
            arg.is_const()
                && !elem.is_coercetodomain()
                && !contain_mutable_functions(Some(elem))?
        }
        _ => false,
    };
    if foldable {
        return ece_evaluate_expr(Expr::ArrayCoerceExpr(ac), ctx);
    }
    Ok(Expr::ArrayCoerceExpr(ac))
}

/// T_CaseExpr arm (clauses.c:3120).
fn arm_case(node: Expr, ctx: &mut EceContext) -> PgResult<Expr> {
    let mut caseexpr = node.expect_into_caseexpr();

    // Simplify the test expression, if any.
    let mut newarg = match caseexpr.arg.take() {
        Some(a) => Some(mutate(*a, ctx)?),
        None => None,
    };

    // Set up for contained CaseTestExpr nodes.
    let save_case_val = ctx.case_val.take();
    if newarg.as_ref().map(|n| n.is_const()).unwrap_or(false) {
        ctx.case_val = newarg.take();
    } else {
        ctx.case_val = None;
    }

    let mut newargs: Vec<CaseWhen> = Vec::new();
    let mut const_true_cond = false;
    let mut defresult: Option<Expr> = None;
    let when_outcome = (|| -> PgResult<()> {
        for w in core::mem::take(&mut caseexpr.args) {
            let mut oldcasewhen: CaseWhen = w;
            // Simplify this alternative's test condition.
            let casecond = mutate(req(oldcasewhen.expr.take(), "CaseWhen.expr")?, ctx)?;

            // Constant FALSE/NULL test -> drop this WHEN clause.
            if let Some(c) = casecond.as_const() {
                if c.constisnull || !datum_get_bool(&c.constvalue) {
                    continue;
                }
                const_true_cond = true;
            }

            let caseresult = mutate(req(oldcasewhen.result.take(), "CaseWhen.result")?, ctx)?;

            if !const_true_cond {
                oldcasewhen.expr = Some(Box::new(casecond));
                oldcasewhen.result = Some(Box::new(caseresult));
                newargs.push(oldcasewhen);
                continue;
            }
            // Found TRUE condition: remaining alternatives unreachable.
            defresult = Some(caseresult);
            break;
        }
        Ok(())
    })();

    let case_val_restore = save_case_val;

    if when_outcome.is_ok() && !const_true_cond {
        match caseexpr.defresult.take() {
            Some(d) => match mutate(*d, ctx) {
                Ok(n) => defresult = Some(n),
                Err(e) => {
                    ctx.case_val = case_val_restore;
                    return Err(e);
                }
            },
            None => defresult = None,
        }
    }
    ctx.case_val = case_val_restore;
    when_outcome?;

    if newargs.is_empty() {
        return match defresult {
            Some(d) => Ok(d),
            None => Err(PgError::error("CASE without default result")),
        };
    }
    caseexpr.arg = newarg.map(Box::new);
    caseexpr.args = newargs;
    caseexpr.defresult = defresult.map(Box::new);
    Ok(Expr::CaseExpr(caseexpr))
}

/// T_CoalesceExpr arm (clauses.c:3291).
fn arm_coalesce(node: Expr, ctx: &mut EceContext) -> PgResult<Expr> {
    let mut coalesce = node.expect_into_coalesceexpr();
    let mut newargs: Vec<Expr> = Vec::new();
    for arg in core::mem::take(&mut coalesce.args) {
        let e = mutate(arg, ctx)?;
        if let Some(c) = e.as_const() {
            if c.constisnull {
                continue; // drop null constant
            }
            if newargs.is_empty() {
                return Ok(e); // first non-null constant is the result
            }
            newargs.push(e);
            break; // following args unreachable
        }
        newargs.push(e);
    }
    if newargs.is_empty() {
        return Ok(Expr::Const(make_null_const(
            ctx.mcx,
            coalesce.coalescetype,
            -1,
            coalesce.coalescecollid,
        )?));
    }
    coalesce.args = newargs;
    Ok(Expr::CoalesceExpr(coalesce))
}

/// T_FieldSelect arm (clauses.c:3359).
fn arm_fieldselect(node: Expr, ctx: &mut EceContext) -> PgResult<Expr> {
    let mut fselect = node.expect_into_fieldselect();
    let arg = mutate(req(fselect.arg.take(), "FieldSelect.arg")?, ctx)?;

    // Whole-row Var -> plain column Var if the rowtype field still matches.
    if let Some(v) = arg.as_var() {
        if v.varattno == 0 /* InvalidAttrNumber */ && v.varlevelsup == 0 {
            if rowtype_field_matches(
                v.vartype,
                fselect.fieldnum as i32,
                fselect.resulttype,
                fselect.resulttypmod,
                fselect.resultcollid,
            )? {
                let mut newvar = backend_nodes_core::makefuncs::make_var(
                    v.varno,
                    fselect.fieldnum,
                    fselect.resulttype,
                    fselect.resulttypmod,
                    fselect.resultcollid,
                    v.varlevelsup,
                );
                // Nullable by the same rels as the original.
                newvar.varnullingrels = v.varnullingrels.clone();
                return Ok(Expr::Var(newvar));
            }
        }
    }
    // RowExpr -> substitute the selected field directly.
    if let Some(rowexpr) = arg.as_rowexpr() {
        let fieldnum = fselect.fieldnum as i32;
        if fieldnum > 0 && fieldnum as usize <= rowexpr.args.len() {
            let fld = &rowexpr.args[fieldnum as usize - 1];
            if rowtype_field_matches(
                rowexpr.row_typeid,
                fieldnum,
                fselect.resulttype,
                fselect.resulttypmod,
                fselect.resultcollid,
            )? && fselect.resulttype == expr_type(Some(fld))?
                && fselect.resulttypmod == expr_typmod(Some(fld))?
                && fselect.resultcollid == expr_collation(Some(fld))?
            {
                let mut rowexpr = arg.expect_into_rowexpr();
                return Ok(rowexpr.args.swap_remove(fieldnum as usize - 1));
            }
        }
    }
    // Const composite -> fold the field extraction (via the executor fallback).
    let arg_is_matching_const = match arg.as_const() {
        Some(c) => rowtype_field_matches(
            c.consttype,
            fselect.fieldnum as i32,
            fselect.resulttype,
            fselect.resulttypmod,
            fselect.resultcollid,
        )?,
        None => false,
    };
    fselect.arg = Some(Box::new(arg));
    if arg_is_matching_const {
        return ece_evaluate_expr(Expr::FieldSelect(fselect), ctx);
    }
    Ok(Expr::FieldSelect(fselect))
}

/// `rowtype_field_matches` (clauses.c:2176). The RECORD short-circuit is
/// in-crate; the catalog read is the seam.
fn rowtype_field_matches(
    rowtypeid: Oid,
    fieldnum: i32,
    expectedtype: Oid,
    expectedtypmod: i32,
    expectedcollation: Oid,
) -> PgResult<bool> {
    // No issue for RECORD, since there is no way to ALTER such a type.
    if rowtypeid == RECORDOID {
        return Ok(true);
    }
    clauses_seam::rowtype_field_matches_lookup::call(
        rowtypeid,
        fieldnum,
        expectedtype,
        expectedtypmod,
        expectedcollation,
    )
}

/// T_NullTest arm (clauses.c:3456).
fn arm_nulltest(node: Expr, ctx: &mut EceContext) -> PgResult<Expr> {
    let mut ntest = node.expect_into_nulltest();
    let arg = mutate(req(ntest.arg.take(), "NullTest.arg")?, ctx)?;

    if ntest.argisrow && arg.is_rowexpr() {
        // Break ROW(...) IS [NOT] NULL into per-field tests.
        let rarg = arg.expect_into_rowexpr();
        let mut newargs: Vec<Expr> = Vec::new();
        for relem in rarg.args {
            if let Some(carg) = relem.as_const() {
                let refuted = if carg.constisnull {
                    ntest.nulltesttype == NullTestType::IS_NOT_NULL
                } else {
                    ntest.nulltesttype == NullTestType::IS_NULL
                };
                if refuted {
                    return Ok(bool_const(false, false));
                }
                continue;
            }
            newargs.push(Expr::NullTest(NullTest {
                arg: Some(Box::new(relem)),
                nulltesttype: ntest.nulltesttype,
                argisrow: false,
                // newntest->location = ntest->location;
                location: ntest.location,
            }));
        }
        if newargs.is_empty() {
            return Ok(bool_const(true, false));
        }
        if newargs.len() == 1 {
            return Ok(newargs.pop().expect("len checked"));
        }
        return Ok(make_andclause(newargs));
    }
    if !ntest.argisrow {
        if let Some(carg) = arg.as_const() {
            let result = match ntest.nulltesttype {
                NullTestType::IS_NULL => carg.constisnull,
                NullTestType::IS_NOT_NULL => !carg.constisnull,
            };
            return Ok(bool_const(result, false));
        }
    }
    ntest.arg = Some(Box::new(arg));
    Ok(Expr::NullTest(ntest))
}

/// T_BooleanTest arm (clauses.c:3547).
fn arm_booleantest(node: Expr, ctx: &mut EceContext) -> PgResult<Expr> {
    use types_nodes::primnodes::BoolTestType::*;
    let mut btest = node.expect_into_booleantest();
    let arg = mutate(req(btest.arg.take(), "BooleanTest.arg")?, ctx)?;
    if let Some(carg) = arg.as_const() {
        let isnull = carg.constisnull;
        let val = !isnull && datum_get_bool(&carg.constvalue);
        let result = match btest.booltesttype {
            IS_TRUE => !isnull && val,
            IS_NOT_TRUE => isnull || !val,
            IS_FALSE => !isnull && !val,
            IS_NOT_FALSE => isnull || val,
            IS_UNKNOWN => isnull,
            IS_NOT_UNKNOWN => !isnull,
        };
        return Ok(bool_const(result, false));
    }
    btest.arg = Some(Box::new(arg));
    Ok(Expr::BooleanTest(btest))
}

/// T_CoerceToDomain arm (clauses.c:3607).
fn arm_coercetodomain(node: Expr, ctx: &mut EceContext) -> PgResult<Expr> {
    let mut cdomain = node.expect_into_coercetodomain();
    let arg = mutate(req(cdomain.arg.take(), "CoerceToDomain.arg")?, ctx)?;
    if ctx.estimate || !clauses_seam::domain_has_constraints::call(cdomain.resulttype)? {
        // C records a plan-invalidation dependency here when root != NULL &&
        // !estimate; there is no PlannerInfo (matches C's root == NULL).
        return apply_relabel_type(
            arg,
            cdomain.resulttype,
            cdomain.resulttypmod,
            cdomain.resultcollid,
            cdomain.coercionformat,
            cdomain.location,
            true,
        );
    }
    cdomain.arg = Some(Box::new(arg));
    Ok(Expr::CoerceToDomain(cdomain))
}

/// T_ConvertRowtypeExpr arm (clauses.c:3669).
fn arm_convertrowtype(node: Expr, ctx: &mut EceContext) -> PgResult<Expr> {
    let mut cre = node.expect_into_convertrowtypeexpr();
    let mut arg = mutate(req(cre.arg.take(), "ConvertRowtypeExpr.arg")?, ctx)?;

    // Collapse nested ConvertRowtypeExpr.
    if arg.is_convertrowtypeexpr() {
        let argcre = arg.expect_into_convertrowtypeexpr();
        if cre.convertformat == CoercionForm::COERCE_IMPLICIT_CAST {
            cre.convertformat = argcre.convertformat;
        }
        arg = req(argcre.arg, "ConvertRowtypeExpr.arg (nested)")?;
    }
    let is_const = arg.is_const();
    cre.arg = Some(Box::new(arg));
    if is_const {
        return ece_evaluate_expr(Expr::ConvertRowtypeExpr(cre), ctx);
    }
    Ok(Expr::ConvertRowtypeExpr(cre))
}

/// T_JsonValueExpr arm (clauses.c:2876).
fn arm_jsonvalueexpr(node: Expr, ctx: &mut EceContext) -> PgResult<Expr> {
    let mut jve = node.expect_into_jsonvalueexpr();
    let formatted = match jve.formatted_expr.take() {
        Some(f) => Some(mutate(*f, ctx)?),
        None => None,
    };
    if let Some(f) = &formatted {
        if f.is_const() {
            return Ok(formatted.expect("checked is_const"));
        }
    }
    jve.raw_expr = mutate_opt(jve.raw_expr, ctx)?;
    jve.formatted_expr = formatted.map(Box::new);
    Ok(Expr::JsonValueExpr(jve))
}

// ---------------------------------------------------------------------------
// simplify_function + argument expansion (clauses.c:4061-4425)
// ---------------------------------------------------------------------------

/// `simplify_function` (clauses.c:4061). Three strategies: execute
/// (`evaluate_function`), consult the planner support function, or inline.
fn simplify_function(
    funcid: Oid,
    result_type: Oid,
    result_typmod: i32,
    result_collid: Oid,
    input_collid: Oid,
    mut args: Vec<Expr>,
    funcvariadic: bool,
    process_args: bool,
    allow_non_const: bool,
    ctx: &mut EceContext,
) -> PgResult<(Option<Expr>, Vec<Expr>)> {
    let form = clauses_seam::get_func_form::call(funcid)?;

    if process_args {
        args = expand_function_arguments(args, result_type, funcid, &form)?;
        args = mutate_list(args, ctx)?;
    }

    let mut newexpr = evaluate_function(
        funcid,
        result_type,
        result_typmod,
        result_collid,
        input_collid,
        &args,
        &form,
        ctx,
    )?;

    if newexpr.is_none() && allow_non_const && form.prosupport != InvalidOid {
        newexpr = clauses_seam::call_support_simplify::call(
            form.prosupport,
            funcid,
            result_type,
            result_collid,
            input_collid,
            &args,
            funcvariadic,
            ctx.estimate,
        )?;
    }

    if newexpr.is_none() && allow_non_const {
        newexpr = inline_function(
            funcid,
            result_type,
            result_collid,
            input_collid,
            &args,
            funcvariadic,
            &form,
            ctx,
        )?;
    }

    Ok((newexpr, args))
}

/// `expand_function_arguments` (clauses.c:4177) with
/// `include_out_arguments = false`.
fn expand_function_arguments(
    mut args: Vec<Expr>,
    result_type: Oid,
    funcid: Oid,
    form: &PgProcSimple,
) -> PgResult<Vec<Expr>> {
    let pronargs = form.pronargs as i32;
    let has_named_args = args.iter().any(|a| a.is_namedargexpr());

    if has_named_args {
        args = reorder_function_arguments(args, pronargs, funcid, form)?;
        args = recheck_cast_function_args(args, result_type, form)?;
    } else if (args.len() as i32) < pronargs {
        args = add_function_defaults(args, pronargs, funcid, form)?;
        args = recheck_cast_function_args(args, result_type, form)?;
    }
    Ok(args)
}

/// `reorder_function_arguments` (clauses.c:4258).
fn reorder_function_arguments(
    args: Vec<Expr>,
    pronargs: i32,
    funcid: Oid,
    form: &PgProcSimple,
) -> PgResult<Vec<Expr>> {
    let nargsprovided = args.len() as i32;
    debug_assert!(nargsprovided <= pronargs);
    if pronargs < 0 || pronargs > FUNC_MAX_ARGS {
        return Err(PgError::error("too many function arguments"));
    }

    let mut argarray: Vec<Option<Expr>> = (0..pronargs).map(|_| None).collect();
    let mut i = 0usize;
    for arg in args {
        if !arg.is_namedargexpr() {
            debug_assert!(argarray[i].is_none());
            argarray[i] = Some(arg);
            i += 1;
        } else {
            let na = arg.expect_into_namedargexpr();
            let argnumber = na.argnumber;
            if argnumber < 0 || argnumber >= pronargs {
                return Err(PgError::error(format!(
                    "invalid named-argument position {argnumber}"
                )));
            }
            debug_assert!(argarray[argnumber as usize].is_none());
            argarray[argnumber as usize] = Some(req(na.arg, "NamedArgExpr.arg")?);
        }
    }

    if nargsprovided < pronargs {
        let defaults = clauses_seam::fetch_function_defaults::call(funcid)?;
        let mut i = pronargs - form.pronargdefaults as i32;
        for d in defaults {
            if i >= 0 && (i as usize) < argarray.len() && argarray[i as usize].is_none() {
                argarray[i as usize] = Some(d);
            }
            i += 1;
        }
    }

    let mut out: Vec<Expr> = Vec::with_capacity(argarray.len());
    for slot in argarray {
        let n = slot.ok_or_else(|| {
            PgError::error("function call is missing an argument with no default")
        })?;
        out.push(n);
    }
    Ok(out)
}

/// `add_function_defaults` (clauses.c:4328).
fn add_function_defaults(
    mut args: Vec<Expr>,
    pronargs: i32,
    funcid: Oid,
    _form: &PgProcSimple,
) -> PgResult<Vec<Expr>> {
    let nargsprovided = args.len() as i32;
    let mut defaults = clauses_seam::fetch_function_defaults::call(funcid)?;
    let ndelete = nargsprovided + defaults.len() as i32 - pronargs;
    if ndelete < 0 {
        return Err(PgError::error("not enough default arguments"));
    }
    if ndelete > 0 {
        defaults.drain(0..ndelete as usize);
    }
    for d in defaults {
        args.push(d);
    }
    Ok(args)
}

/// `recheck_cast_function_args` (clauses.c:4382) — the
/// `enforce_generic_type_consistency` + `make_fn_arguments` legs ride the seam;
/// the FUNC_MAX_ARGS / pronargs sanity checks stay in-crate.
fn recheck_cast_function_args(
    args: Vec<Expr>,
    result_type: Oid,
    form: &PgProcSimple,
) -> PgResult<Vec<Expr>> {
    if args.len() as i32 > FUNC_MAX_ARGS {
        return Err(PgError::error("too many function arguments"));
    }
    debug_assert_eq!(args.len(), form.proargtypes.len());
    clauses_seam::recheck_cast_function_args::call(
        args,
        result_type,
        form.proargtypes.clone(),
        form.prorettype,
    )
}

/// `evaluate_function` (clauses.c:4427).
fn evaluate_function(
    funcid: Oid,
    result_type: Oid,
    result_typmod: i32,
    result_collid: Oid,
    input_collid: Oid,
    args: &[Expr],
    form: &PgProcSimple,
    ctx: &EceContext,
) -> PgResult<Option<Expr>> {
    // Can't simplify if it returns a set or RECORD.
    if form.proretset || form.prorettype == RECORDOID {
        return Ok(None);
    }

    let mut has_nonconst_input = false;
    let mut has_null_input = false;
    for a in args {
        match a.as_const() {
            Some(c) => has_null_input |= c.constisnull,
            None => has_nonconst_input = true,
        }
    }

    // Strict + constant-NULL input -> NULL constant.
    if form.proisstrict && has_null_input {
        return Ok(Some(Expr::Const(make_null_const(
            ctx.mcx,
            result_type,
            result_typmod,
            result_collid,
        )?)));
    }
    // Otherwise can simplify only if all inputs are constants.
    if has_nonconst_input {
        return Ok(None);
    }
    // Only immutable functions (stable too, for estimation).
    if form.provolatile == PROVOLATILE_IMMUTABLE
        || (ctx.estimate && form.provolatile == PROVOLATILE_STABLE)
    {
        // okay
    } else {
        return Ok(None);
    }

    // All-Const arguments: exactly one fmgr invocation.
    let mut pairs: Vec<(CDatum, bool, Oid)> = Vec::with_capacity(args.len());
    for a in args.iter() {
        let c = a.expect_const();
        pairs.push((c.constvalue.clone(), c.constisnull, c.consttype));
    }
    // C: newexpr = makeNode(FuncExpr); ... fmgr_info_set_expr((Node*)newexpr,
    // &finfo). Synthesize the call node so a polymorphic function (e.g.
    // `int4range`'s `range_constructor2`) can read `funcresulttype` /
    // declared arg types out of it (`get_fn_expr_rettype/argtype`).
    let newexpr = backend_nodes_core::makefuncs::make_func_expr(
        funcid,
        result_type,
        args.to_vec(),
        result_collid,
        input_collid,
        CoercionForm::COERCE_EXPLICIT_CALL,
    );
    Ok(Some(fmgr_fold(
        ctx.mcx,
        funcid,
        input_collid,
        pairs,
        result_type,
        result_typmod,
        result_collid,
        Some(&newexpr),
    )?))
}

/// `inline_function` (clauses.c:4553). The cheap catalog gates + the
/// `active_fns` recursion guard + the recursive re-simplification
/// (clauses.c:4890) run in-crate; the prosqlbody/prosrc parse + "simple SELECT
/// expression" gate + `check_sql_fn_retval` + `substitute_actual_parameters` +
/// usecount machinery rides the `inline_sql_function` seam (it cannot live in
/// this crate, which must not depend on the parser).
fn inline_function(
    funcid: Oid,
    result_type: Oid,
    result_collid: Oid,
    input_collid: Oid,
    args: &[Expr],
    funcvariadic: bool,
    form: &PgProcSimple,
    ctx: &mut EceContext,
) -> PgResult<Option<Expr>> {
    // Forget it if the function is not SQL-language or has other showstopper
    // properties. (prokind / proretset / pronargs are paranoia, as in C.)
    if !form.prolang_is_sql
        || form.prokind != PROKIND_FUNCTION
        || form.prosecdef
        || form.proretset
        || form.prorettype == RECORDOID
        || !form.proconfig_isnull
        || args.len() != form.pronargs as usize
    {
        return Ok(None);
    }

    // Check for recursive function, and give up trying to expand if so
    // (clauses.c:4594, `list_member_oid(context->active_fns, funcid)`).
    if ctx.active_fns.contains(&funcid) {
        return Ok(None);
    }

    // object_aclcheck(ProcedureRelationId, funcid, GetUserId(), ACL_EXECUTE)
    // and FmgrHookIsNeeded(funcid) (clauses.c:4598/4602): the single-user
    // `postgres` boot-superuser model passes ACL unconditionally and installs
    // no fmgr hooks, matching every other consumer's external treatment.

    // Fetch the function body (clauses.c:4628/4646): prosrc + the cooked
    // prosqlbody (NULL for the classic `AS 'SELECT ...'` form).
    let (prosrc, prosqlbody) = clauses_seam::get_func_sql_body::call(ctx.mcx, funcid)?;
    let prosqlbody_ref = prosqlbody.as_ref().map(|s| s.as_str());

    // Parse/analyze + gate + substitute (the parser-dependent body) via the
    // seam. Returns the SUBSTITUTED expression, NOT yet re-simplified.
    let substituted = clauses_seam::inline_sql_function::call(
        ctx.mcx,
        form,
        prosrc.as_str(),
        prosqlbody_ref,
        funcid,
        result_type,
        result_collid,
        input_collid,
        args,
        funcvariadic,
        ctx.estimate,
    )?;
    let newexpr = match substituted {
        Some(e) => e,
        None => return Ok(None),
    };

    // Recursively try to simplify the modified expression (clauses.c:4890).
    // Here we must add the current function to the context list of active
    // functions, and remove it on the way out (the recursion guard above).
    ctx.active_fns.push(funcid);
    let result = mutate(newexpr, ctx);
    ctx.active_fns.pop();
    result.map(Some)
}

/// `PROKIND_FUNCTION` (`pg_proc.h`).
const PROKIND_FUNCTION: u8 = b'f';

// ---------------------------------------------------------------------------
// evaluate_expr (clauses.c:4798) — the in-crate evaluator
// ---------------------------------------------------------------------------

/// `evaluate_expr(expr, result_type, result_typmod, result_collation)`.
///
/// Evaluates the shapes the mutator actually produces here — `Const`, all-`Const`
/// `FuncExpr`/`OpExpr`, `NullIfExpr` — through the `fmgr_call` seam; every other
/// shape rides the executor-backed `evaluate_expr_fallback` seam (NEVER silently
/// returned unsimplified, because C does fold them).
pub fn evaluate_expr<'mcx>(
    mcx: Mcx<'mcx>,
    mut expr: Expr,
    result_type: Oid,
    result_typmod: i32,
    result_collation: Oid,
) -> PgResult<Expr> {
    // Make sure any opfuncids are filled in.
    fix_opfuncids(&mut expr)?;

    match expr.expr_tag() {
        etag::T_Const => Ok(expr),
        etag::T_FuncExpr => {
            let call = {
                let f = expr.as_funcexpr().expect("FuncExpr");
                const_datum_args(&f.args).map(|pairs| (f.funcid, f.inputcollid, pairs))
            };
            match call {
                Some((funcid, inputcollid, pairs)) => fmgr_fold(
                    mcx,
                    funcid,
                    inputcollid,
                    pairs,
                    result_type,
                    result_typmod,
                    result_collation,
                    Some(&expr),
                ),
                None => clauses_seam::evaluate_expr_fallback::call(
                    expr,
                    result_type,
                    result_typmod,
                    result_collation,
                ),
            }
        }
        etag::T_OpExpr => {
            let call = {
                let o = expr.as_opexpr().expect("OpExpr");
                const_datum_args(&o.args).map(|pairs| (o.opfuncid, o.inputcollid, pairs))
            };
            match call {
                Some((opfuncid, inputcollid, pairs)) => fmgr_fold(
                    mcx,
                    opfuncid,
                    inputcollid,
                    pairs,
                    result_type,
                    result_typmod,
                    result_collation,
                    Some(&expr),
                ),
                None => clauses_seam::evaluate_expr_fallback::call(
                    expr,
                    result_type,
                    result_typmod,
                    result_collation,
                ),
            }
        }
        etag::T_NullIfExpr => {
            // NULLIF(a, b): evaluate `a = b`; if true the result is NULL, else
            // a. (The mutator only sends two non-NULL Const args here.)
            let call = {
                let n = expr.as_nullifexpr().expect("NullIfExpr");
                const_datum_args(&n.args).map(|pairs| (n.opfuncid, n.inputcollid, pairs))
            };
            match call {
                Some((opfuncid, inputcollid, pairs)) => {
                    let first = pairs
                        .first()
                        .cloned()
                        .ok_or_else(|| PgError::error("NULLIF without arguments"))?;
                    let pairs: Vec<(
                        types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
                        bool,
                        Oid,
                    )> = pairs;
                    let (eq, eq_isnull) = clauses_seam::fmgr_call::call(
                        mcx,
                        opfuncid,
                        inputcollid,
                        pairs,
                        BOOLOID,
                        Some(&expr),
                    )?;
                    if !eq_isnull && datum_get_bool(&eq) {
                        Ok(Expr::Const(make_null_const(
                            mcx,
                            result_type,
                            result_typmod,
                            result_collation,
                        )?))
                    } else {
                        let (typlen, typbyval) = lsyscache::get_typlenbyval::call(result_type)?;
                        Ok(Expr::Const(make_const(
                            mcx,
                            result_type,
                            result_typmod,
                            result_collation,
                            typlen as i32,
                            first.0,
                            first.1,
                            typbyval,
                        )?))
                    }
                }
                None => clauses_seam::evaluate_expr_fallback::call(
                    expr,
                    result_type,
                    result_typmod,
                    result_collation,
                ),
            }
        }
        // SAOP / MinMax / Row / SubscriptingRef / FieldSelect-on-Const /
        // ConvertRowtype / ArrayCoerce / SQLValueFunction / multidim ArrayExpr /
        // anything else: loud-defer to the executor-backed seam.
        _ => clauses_seam::evaluate_expr_fallback::call(
            expr,
            result_type,
            result_typmod,
            result_collation,
        ),
    }
}

/// All-`Const` argument extraction: `Some(triples)` iff every argument is Const.
/// Each triple is `(value, isnull, consttype)` — the Const's CONCRETE type (what
/// `get_fn_expr_argtype` would resolve for the call).
fn const_datum_args(args: &[Expr]) -> Option<Vec<(CDatum, bool, Oid)>> {
    let mut pairs: Vec<(CDatum, bool, Oid)> = Vec::with_capacity(args.len());
    for a in args {
        let c = a.as_const()?;
        pairs.push((c.constvalue.clone(), c.constisnull, c.consttype));
    }
    Some(pairs)
}

/// Execute `funcid` over the evaluated arguments and wrap the result in a
/// `Const` (the tail of C's `evaluate_expr`: `get_typlenbyval` + `makeConst`).
fn fmgr_fold<'mcx>(
    mcx: Mcx<'mcx>,
    funcid: Oid,
    inputcollid: Oid,
    args: Vec<(types_tuple::backend_access_common_heaptuple::Datum<'mcx>, bool, Oid)>,
    result_type: Oid,
    result_typmod: i32,
    result_collation: Oid,
    // C `evaluate_function` runs `fmgr_info_set_expr((Node *) newexpr, &finfo)`
    // so a const-folded polymorphic function reads its declared types; thread the
    // synthesized call node (`None` for the cast/coerce-fold sites that have no
    // FuncExpr/OpExpr, matching C's NULL fn_expr there).
    fn_expr: Option<&Expr>,
) -> PgResult<Expr> {
    let (value, isnull) =
        clauses_seam::fmgr_call::call(mcx, funcid, inputcollid, args, result_type, fn_expr)?;
    let (typlen, typbyval) = lsyscache::get_typlenbyval::call(result_type)?;
    Ok(Expr::Const(make_const(
        mcx,
        result_type,
        result_typmod,
        result_collation,
        typlen as i32,
        value,
        isnull,
        typbyval,
    )?))
}

// ---------------------------------------------------------------------------
// make_SAOP_expr (clauses.c:5451)
// ---------------------------------------------------------------------------

/// C: `make_SAOP_expr(...)` — build `leftexpr op ANY(ARRAY[exprs])`; `None` if
/// `coltype` has no array type.
///
/// The const-array fast path (all-Const `exprs` -> a folded array `Const`) rides
/// the executor-backed `evaluate_expr_fallback` over a single-dimension
/// `ArrayExpr`; the array constructor itself is not duplicated here. The
/// non-const path builds an `ArrayExpr`.
pub fn make_SAOP_expr(
    mcx: Mcx<'_>,
    oper: Oid,
    leftexpr: Expr,
    coltype: Oid,
    arraycollid: Oid,
    inputcollid: Oid,
    exprs: Vec<Expr>,
    have_non_const: bool,
) -> PgResult<Option<Expr>> {
    let arraytype = match lsyscache::get_array_type::call(coltype)? {
        Some(t) => t,
        None => return Ok(None),
    };

    // Build the array operand. When all elements are Const we still fold it to
    // an array Const, but via the executor evaluator over a single-dim
    // ArrayExpr (the same path eval_const_expressions' T_ArrayExpr arm uses).
    let array_node: Expr = {
        let ae = Expr::ArrayExpr(ArrayExpr {
            array_typeid: arraytype,
            // array_collid is set by parse_collate.c (C leaves it 0).
            array_collid: InvalidOid,
            element_typeid: coltype,
            elements: exprs,
            multidims: false,
            // arrayExpr->location = -1;
            location: -1,
        });
        if have_non_const {
            ae
        } else {
            // Fold the all-Const ArrayExpr to an array Const.
            evaluate_expr(mcx, ae, arraytype, -1, arraycollid)?
        }
    };

    let opfuncid = lsyscache::get_opcode::call(oper)?;
    Ok(Some(Expr::ScalarArrayOpExpr(ScalarArrayOpExpr {
        opno: oper,
        opfuncid,
        hashfuncid: InvalidOid,
        negfuncid: InvalidOid,
        useOr: true,
        inputcollid,
        args: alloc::vec![leftexpr, array_node],
        location: -1,
    })))
}
