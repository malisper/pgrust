//! eval_const_expressions / estimate_expression_value (clauses.c).
//!
//! C divergences: the mutator is identity-preserving (walker.rs module doc);
//! `root` is unthreaded — C reads it only for boundParams substitution and
//! invalItems recording, neither modeled yet (PlannerGlobal carries no
//! boundParams). The evaluate_expr seam installer must record invalItems.

use lsyscache::get_typlenbyval;
use mcx::Mcx;
use syscache_seams::PgProcShape;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult};
use types_nodes::primnodes::{CoercionForm, Const, FuncExpr, OpExpr};
use types_nodes::{Node, NodeList, NodeTag};

use crate::walker::{deferred, expression_tree_mutator, mutate_list};

const RECORDOID: Oid = 2249;
const INT4OID: Oid = 23;
const BOOLEAN_EQUAL_OPERATOR: Oid = 91;
const BOOLEAN_NOT_EQUAL_OPERATOR: Oid = 85;

use crate::classify::{PROVOLATILE_IMMUTABLE, PROVOLATILE_STABLE};

struct EceContext<'mcx> {
    mcx: Mcx<'mcx>,
    estimate: bool,
}

pub fn eval_const_expressions<'mcx>(mcx: Mcx<'mcx>, node: Node<'mcx>) -> PgResult<Node<'mcx>> {
    let cx = EceContext { mcx, estimate: false };
    Ok(ece_mutator(node, &cx)?.unwrap_or(node))
}

pub fn estimate_expression_value<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let cx = EceContext { mcx, estimate: true };
    Ok(ece_mutator(node, &cx)?.unwrap_or(node))
}

fn ece_mutator<'mcx>(node: Node<'mcx>, cx: &EceContext<'mcx>) -> PgResult<Option<Node<'mcx>>> {
    match node.node_tag() {
        // PARAM_EXTERN substitution needs boundParams (unthreaded, see top).
        NodeTag::T_Param => Ok(None),
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            let (simple, new_args) = simplify_function(
                cx,
                f.funcid,
                f.funcresulttype,
                func_expr_typmod(f),
                f.funccollid,
                f.inputcollid,
                &f.args,
                f.funcvariadic,
                true,
                true,
            )?;
            if simple.is_some() {
                return Ok(simple);
            }
            match new_args {
                None => Ok(None),
                Some(args) => Ok(Some(Node::mk(
                    cx.mcx,
                    FuncExpr {
                        funcid: f.funcid,
                        funcresulttype: f.funcresulttype,
                        funcretset: f.funcretset,
                        funcvariadic: f.funcvariadic,
                        funcformat: f.funcformat,
                        funccollid: f.funccollid,
                        inputcollid: f.inputcollid,
                        args,
                        location: f.location,
                    },
                )?)),
            }
        }
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().unwrap();
            // set_opfuncid, without C's memo write-back (walker.rs).
            let opfuncid = if o.opfuncid == 0 {
                lsyscache::get_opcode(o.opno)?
            } else {
                o.opfuncid
            };
            let (simple, new_args) = simplify_function(
                cx,
                opfuncid,
                o.opresulttype,
                -1,
                o.opcollid,
                o.inputcollid,
                &o.args,
                false,
                true,
                true,
            )?;
            if simple.is_some() {
                return Ok(simple);
            }
            if o.opno == BOOLEAN_EQUAL_OPERATOR || o.opno == BOOLEAN_NOT_EQUAL_OPERATOR {
                let args = new_args.as_ref().unwrap_or(&o.args);
                if let Some(simple) = simplify_boolean_equality(o.opno, args) {
                    return Ok(Some(simple));
                }
            }
            match new_args {
                None if opfuncid == o.opfuncid => Ok(None),
                new_args => {
                    let args = match new_args {
                        Some(a) => a,
                        None => o.args.clone_in(cx.mcx)?,
                    };
                    Ok(Some(Node::mk(
                        cx.mcx,
                        OpExpr {
                            opno: o.opno,
                            opfuncid,
                            opresulttype: o.opresulttype,
                            opretset: o.opretset,
                            opcollid: o.opcollid,
                            inputcollid: o.inputcollid,
                            args,
                            location: o.location,
                        },
                    )?))
                }
            }
        }
        NodeTag::T_Var
        | NodeTag::T_Const
        | NodeTag::T_RangeTblRef
        | NodeTag::T_CaseTestExpr
        | NodeTag::T_CurrentOfExpr
        | NodeTag::T_SortGroupClause => Ok(None),
        NodeTag::T_TargetEntry | NodeTag::T_FromExpr | NodeTag::T_List => {
            expression_tree_mutator(cx.mcx, node, &mut |n| ece_mutator(n, cx))
        }
        other => deferred("eval_const_expressions_mutator", other),
    }
}

/// exprIsLengthCoercion shape: a two-arg cast whose second arg is a
/// non-null int4 Const carries that typmod.
fn func_expr_typmod(f: &FuncExpr<'_>) -> i32 {
    if !matches!(
        f.funcformat,
        CoercionForm::COERCE_EXPLICIT_CAST | CoercionForm::COERCE_IMPLICIT_CAST
    ) || f.args.len() != 2
    {
        return -1;
    }
    match f.args.nth(1).as_const() {
        Some(c) if c.consttype == INT4OID && !c.constisnull => c.constvalue.as_i32(),
        _ => -1,
    }
}

#[cold]
fn func_lookup_failed(funcid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("cache lookup failed for function {funcid}")))
}

/// Returns (simplified-expression,
/// possibly-rewritten args); `None` args = unchanged. The executor-evaluation
/// leg rides the clauses_seams::evaluate_expr seam; SupportRequestSimplify
/// dispatch and SQL-function inlining defer loud.
#[allow(clippy::too_many_arguments)]
fn simplify_function<'mcx>(
    cx: &EceContext<'mcx>,
    funcid: Oid,
    result_type: Oid,
    result_typmod: i32,
    result_collid: Oid,
    input_collid: Oid,
    args: &NodeList<'mcx>,
    funcvariadic: bool,
    process_args: bool,
    allow_non_const: bool,
) -> PgResult<(Option<Node<'mcx>>, Option<NodeList<'mcx>>)> {
    let shape = syscache_seams::lookup_pg_proc_shape::call(funcid)?
        .ok_or_else(|| func_lookup_failed(funcid))?;

    let mut new_args: Option<NodeList<'mcx>> = None;
    if process_args {
        expand_function_arguments_gate(args, &shape);
        new_args = mutate_list(cx.mcx, args, &mut |n| ece_mutator(n, cx))?;
    }
    let eff_args = new_args.as_ref().unwrap_or(args);

    let newexpr = evaluate_function(
        cx,
        funcid,
        result_type,
        result_typmod,
        result_collid,
        input_collid,
        eff_args,
        funcvariadic,
        &shape,
    )?;

    if newexpr.is_none() && allow_non_const && shape.prosupport != InvalidOid {
        panic!(
            "simplify_function deferred: SupportRequestSimplify dispatch for prosupport {} (funcid {funcid})",
            shape.prosupport
        );
    }
    if newexpr.is_none() && allow_non_const && fmgr_core::fmgr_isbuiltin(funcid).is_none() {
        // A builtin is internal-language, which C's inline_function rejects
        // up front; anything else needs the prolang gate + SQL inliner.
        panic!("simplify_function deferred: inline_function for non-builtin funcid {funcid}");
    }
    Ok((newexpr, new_args))
}

/// Pass-through case (positional, no defaults) returns args unchanged;
/// named-arg reordering and default insertion need proargdefaults decode.
fn expand_function_arguments_gate(args: &NodeList<'_>, shape: &PgProcShape) {
    for a in args {
        if a.node_tag() == NodeTag::T_NamedArgExpr {
            panic!("expand_function_arguments deferred: named-argument notation");
        }
    }
    if args.len() < shape.pronargs as usize {
        panic!("expand_function_arguments deferred: default-argument insertion");
    }
}

#[allow(clippy::too_many_arguments)]
fn evaluate_function<'mcx>(
    cx: &EceContext<'mcx>,
    funcid: Oid,
    result_type: Oid,
    result_typmod: i32,
    result_collid: Oid,
    input_collid: Oid,
    args: &NodeList<'mcx>,
    funcvariadic: bool,
    shape: &PgProcShape,
) -> PgResult<Option<Node<'mcx>>> {
    if shape.proretset || shape.prorettype == RECORDOID {
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
    if shape.proisstrict && has_null_input {
        return Ok(Some(make_null_const(cx.mcx, result_type, result_typmod, result_collid)?));
    }
    if has_nonconst_input {
        return Ok(None);
    }
    let volatility_ok = shape.provolatile == PROVOLATILE_IMMUTABLE
        || (cx.estimate && shape.provolatile == PROVOLATILE_STABLE);
    if !volatility_ok {
        return Ok(None);
    }
    // C hands evaluate_expr a fresh FuncExpr sharing the args List pointer;
    // list headers are by-value here, so the cells are copied (small, cold).
    let call = Node::mk(
        cx.mcx,
        FuncExpr {
            funcid,
            funcresulttype: result_type,
            funcretset: false,
            funcvariadic,
            funcformat: CoercionForm::COERCE_EXPLICIT_CALL,
            funccollid: result_collid,
            inputcollid: input_collid,
            args: args.clone_in(cx.mcx)?,
            location: -1,
        },
    )?;
    clauses_seams::evaluate_expr::call(cx.mcx, call, result_type, result_typmod, result_collid)
        .map(Some)
}

fn make_null_const<'mcx>(
    mcx: Mcx<'mcx>,
    typ: Oid,
    typmod: i32,
    collid: Oid,
) -> PgResult<Node<'mcx>> {
    let (typlen, typbyval) = get_typlenbyval(typ)?;
    Node::mk(
        mcx,
        Const {
            consttype: typ,
            consttypmod: typmod,
            constcollid: collid,
            constlen: typlen as i32,
            constvalue: datum::Datum::null(),
            constisnull: true,
            constbyval: typbyval,
            location: -1,
        },
    )
}

/// Reduce "x = true" to "x", "x <> false" to "x"; the NOT-wrapping legs
/// need negate_clause + BoolExpr vocabulary — deferred loud.
fn simplify_boolean_equality<'mcx>(opno: Oid, args: &NodeList<'mcx>) -> Option<Node<'mcx>> {
    debug_assert_eq!(args.len(), 2);
    let (leftop, rightop) = (args.nth(0), args.nth(1));
    let eq = opno == BOOLEAN_EQUAL_OPERATOR;
    if let Some(c) = leftop.as_const() {
        debug_assert!(!c.constisnull);
        return if c.constvalue.as_bool() == eq {
            Some(rightop)
        } else {
            panic!("simplify_boolean_equality deferred: negate_clause (prepqual) unported");
        };
    }
    if let Some(c) = rightop.as_const() {
        debug_assert!(!c.constisnull);
        return if c.constvalue.as_bool() == eq {
            Some(leftop)
        } else {
            panic!("simplify_boolean_equality deferred: negate_clause (prepqual) unported");
        };
    }
    None
}

/// ece_all_arguments_const: no non-Const among the node's children.
pub fn all_arguments_const(node: Node<'_>) -> PgResult<bool> {
    struct NonConst;
    impl<'mcx> crate::walker::NodeWalker<'mcx> for NonConst {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            match node.node_tag() {
                NodeTag::T_Const => Ok(false),
                NodeTag::T_List => crate::walker::walk_list(node.as_list().unwrap(), self),
                _ => Ok(true),
            }
        }
    }
    Ok(!crate::walker::expression_tree_walker(node, &mut NonConst)?)
}
