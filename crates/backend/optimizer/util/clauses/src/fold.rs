//! eval_const_expressions / estimate_expression_value (clauses.c).
//!
//! C divergences: the mutator is identity-preserving (walker.rs module doc);
//! `root` is unthreaded — its boundParams read is an explicit ParamListHandle
//! argument here; invalItems recording is not modeled (the evaluate_expr seam
//! installer must record invalItems).

use datum::Datum;
use lsyscache::get_typlenbyval;
use mcx::Mcx;
use syscache_seams::PgProcShape;
use types_core::{InvalidOid, Oid, OidIsValid};
use types_error::{PgError, PgResult};
use types_nodes::primnodes::{
    BoolExpr, BoolExprType, CaseExpr, CaseWhen, CoalesceExpr, CoerceViaIO, CoercionForm, Const,
    FuncExpr, OpExpr, ParamKind,
};
use types_nodes::{Node, NodeList, NodeTag};
use types_portal::params::{ParamExternData, PARAM_FLAG_CONST};
use types_portal::{params, ParamListHandle};

use crate::walker::{deferred, expression_tree_mutator, mutate_list};

const RECORDOID: Oid = 2249;
const INT4OID: Oid = 23;
const BOOLOID: Oid = 16;
const OIDOID: Oid = 26;
const CSTRINGOID: Oid = 2275;
const BOOLEAN_EQUAL_OPERATOR: Oid = 91;
const BOOLEAN_NOT_EQUAL_OPERATOR: Oid = 85;

use crate::classify::{PROVOLATILE_IMMUTABLE, PROVOLATILE_STABLE};

struct EceContext<'mcx> {
    mcx: Mcx<'mcx>,
    estimate: bool,
    bound_params: ParamListHandle,
    // C context->case_val: the constant test value of the innermost
    // simple-form CASE being simplified (save/restore in the CASE arm).
    case_val: core::cell::Cell<Option<Node<'mcx>>>,
}

pub fn eval_const_expressions<'mcx>(mcx: Mcx<'mcx>, node: Node<'mcx>) -> PgResult<Node<'mcx>> {
    eval_const_expressions_with_params(mcx, node, ParamListHandle::NULL)
}

pub fn eval_const_expressions_with_params<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    bound_params: ParamListHandle,
) -> PgResult<Node<'mcx>> {
    let cx =
        EceContext { mcx, estimate: false, bound_params, case_val: core::cell::Cell::new(None) };
    Ok(ece_mutator(node, &cx)?.unwrap_or(node))
}

pub fn estimate_expression_value<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let cx = EceContext {
        mcx,
        estimate: true,
        bound_params: ParamListHandle::NULL,
        case_val: core::cell::Cell::new(None),
    };
    Ok(ece_mutator(node, &cx)?.unwrap_or(node))
}

// The T_Param arm's substitution leg: a bound PARAM_FLAG_CONST extern param
// becomes a Const (custom plans see the value; estimate mode substitutes any
// bound value, exactly C).
fn substitute_bound_param<'mcx>(
    node: Node<'mcx>,
    cx: &EceContext<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    let param = node.as_param().unwrap();
    if param.paramkind != ParamKind::PARAM_EXTERN
        || cx.bound_params.is_null()
        || param.paramid <= 0
        || param.paramid as usize > params::num_params(cx.bound_params)
    {
        return Ok(None);
    }
    let prm: ParamExternData =
        params::with(cx.bound_params, |p| p[(param.paramid - 1) as usize]);
    if !OidIsValid(prm.ptype) {
        return Ok(None);
    }
    if !(cx.estimate || (prm.pflags & PARAM_FLAG_CONST) != 0) {
        return Ok(None);
    }
    debug_assert_eq!(prm.ptype, param.paramtype);
    let (typlen, typbyval) = get_typlenbyval(param.paramtype)?;
    let pval = if prm.isnull || typbyval {
        prm.value
    } else {
        datum_copy_in(cx.mcx, prm.value, typlen)?
    };
    Ok(Some(Node::mk(
        cx.mcx,
        Const {
            consttype: param.paramtype,
            consttypmod: param.paramtypmod,
            constcollid: param.paramcollid,
            constlen: typlen as i32,
            constvalue: pval,
            constisnull: prm.isnull,
            constbyval: typbyval,
            location: param.location,
        },
    )?))
}

// datumCopy (datum.c) scoped to bound-parameter substitution; by-ref sources
// here are input-function results (4B-header varlenas, never toast pointers).
fn datum_copy_in<'mcx>(mcx: Mcx<'mcx>, value: Datum, typlen: i16) -> PgResult<Datum> {
    let p = value.as_usize() as *const u8;
    if p.is_null() {
        return Ok(Datum::null());
    }
    let size = match typlen {
        -1 => {
            // SAFETY: non-null by-ref varlena datum (see above).
            unsafe { datum::VarlenaRef::from_ptr(p).varsize() }
        }
        -2 => {
            let mut n = 0usize;
            // SAFETY: non-null NUL-terminated cstring datum.
            while unsafe { *p.add(n) } != 0 {
                n += 1;
            }
            n + 1
        }
        l => {
            debug_assert!(l > 0);
            l as usize
        }
    };
    // SAFETY: `size` bytes readable per the arms above.
    let src = unsafe { core::slice::from_raw_parts(p, size) };
    let out = mcx::slice_in(mcx, src)?;
    Ok(Datum::from_usize(out.leak().as_ptr() as usize))
}

fn ece_mutator<'mcx>(node: Node<'mcx>, cx: &EceContext<'mcx>) -> PgResult<Option<Node<'mcx>>> {
    match node.node_tag() {
        NodeTag::T_Param => substitute_bound_param(node, cx),
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
                if let Some(simple) = simplify_boolean_equality(cx.mcx, o.opno, args)? {
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
        NodeTag::T_BoolExpr => {
            let b = node.as_bool_expr().unwrap();
            match b.boolop {
                BoolExprType::OR_EXPR | BoolExprType::AND_EXPR => {
                    let is_or = b.boolop == BoolExprType::OR_EXPR;
                    let mut newargs = NodeList::nil();
                    let mut have_null = false;
                    if simplify_bool_arguments(cx, &b.args, is_or, &mut newargs, &mut have_null)?
                    {
                        return Ok(Some(make_bool_const(cx.mcx, is_or, false)?));
                    }
                    if have_null {
                        newargs.lappend(cx.mcx, make_bool_const(cx.mcx, false, true)?)?;
                    }
                    if newargs.is_nil() {
                        return Ok(Some(make_bool_const(cx.mcx, !is_or, false)?));
                    }
                    if newargs.len() == 1 {
                        return Ok(Some(newargs.nth(0)));
                    }
                    Ok(Some(Node::mk(
                        cx.mcx,
                        BoolExpr { boolop: b.boolop, args: newargs, location: -1 },
                    )?))
                }
                BoolExprType::NOT_EXPR => {
                    debug_assert_eq!(b.args.len(), 1);
                    let arg = b.args.nth(0);
                    let arg = ece_mutator(arg, cx)?.unwrap_or(arg);
                    Ok(Some(negate_clause(cx.mcx, arg)?))
                }
            }
        }
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().unwrap();
            let arg = ece_mutator(r.arg, cx)?.unwrap_or(r.arg);
            apply_relabel_type(
                cx.mcx,
                arg,
                r.resulttype,
                r.resulttypmod,
                r.resultcollid,
                r.relabelformat,
                r.location,
            )
            .map(Some)
        }
        NodeTag::T_CoerceViaIO => {
            let e = node.as_coerce_via_io().unwrap();
            let mut args = NodeList::make1(cx.mcx, e.arg)?;
            let (outfunc, _) = lsyscache::getTypeOutputInfo(coerce_arg_type(e.arg))?;
            let (infunc, intypioparam) = lsyscache::getTypeInputInfo(e.resulttype)?;

            let (simple, new_args) = simplify_function(
                cx,
                outfunc,
                CSTRINGOID,
                -1,
                InvalidOid,
                InvalidOid,
                &args,
                false,
                true,
                true,
            )?;
            if let Some(a) = new_args {
                args = a;
            }
            if let Some(simple) = simple {
                let mut inargs = NodeList::make1(cx.mcx, simple)?;
                inargs.lappend(
                    cx.mcx,
                    Node::mk(
                        cx.mcx,
                        Const {
                            consttype: OIDOID,
                            consttypmod: -1,
                            constcollid: InvalidOid,
                            constlen: 4,
                            constvalue: Datum::from_oid(intypioparam),
                            constisnull: false,
                            constbyval: true,
                            location: -1,
                        },
                    )?,
                )?;
                inargs.lappend(
                    cx.mcx,
                    Node::mk(
                        cx.mcx,
                        Const {
                            consttype: INT4OID,
                            consttypmod: -1,
                            constcollid: InvalidOid,
                            constlen: 4,
                            constvalue: Datum::from_i32(-1),
                            constisnull: false,
                            constbyval: true,
                            location: -1,
                        },
                    )?,
                )?;
                let (simple, _) = simplify_function(
                    cx,
                    infunc,
                    e.resulttype,
                    -1,
                    e.resultcollid,
                    InvalidOid,
                    &inargs,
                    false,
                    false,
                    true,
                )?;
                if simple.is_some() {
                    return Ok(simple);
                }
            }
            Ok(Some(Node::mk(
                cx.mcx,
                CoerceViaIO {
                    arg: args.nth(0),
                    resulttype: e.resulttype,
                    resultcollid: e.resultcollid,
                    coerceformat: e.coerceformat,
                    location: e.location,
                },
            )?))
        }
        NodeTag::T_CaseExpr => {
            let ce = node.as_case_expr().unwrap();
            let mut newarg = match ce.arg {
                Some(a) => Some(ece_mutator(a, cx)?.unwrap_or(a)),
                None => None,
            };
            let save_case_val = cx.case_val.replace(match newarg {
                Some(n) if n.node_tag() == NodeTag::T_Const => newarg.take(),
                _ => None,
            });
            let restore = |r: PgResult<Option<Node<'mcx>>>| {
                cx.case_val.set(save_case_val);
                r
            };
            let mut newargs = NodeList::nil();
            let mut const_true_cond = false;
            let mut defresult: Option<Node<'mcx>> = None;
            for w in &ce.args {
                let cw = w.as_case_when().expect("CASE args are CaseWhen");
                let expr = cw.expr.expect("CaseWhen.expr is never NULL");
                let casecond = match ece_mutator(expr, cx) {
                    Ok(c) => c.unwrap_or(expr),
                    Err(e) => return restore(Err(e)),
                };
                if let Some(c) = casecond.as_const() {
                    if c.constisnull || !c.constvalue.as_bool() {
                        continue;
                    }
                    const_true_cond = true;
                }
                let result = cw.result.expect("CaseWhen.result is never NULL");
                let caseresult = match ece_mutator(result, cx) {
                    Ok(c) => c.unwrap_or(result),
                    Err(e) => return restore(Err(e)),
                };
                if !const_true_cond {
                    let ncw = match Node::mk(
                        cx.mcx,
                        CaseWhen {
                            expr: Some(casecond),
                            result: Some(caseresult),
                            location: cw.location,
                        },
                    ) {
                        Ok(n) => n,
                        Err(e) => return restore(Err(e)),
                    };
                    if let Err(e) = newargs.lappend(cx.mcx, ncw) {
                        return restore(Err(e));
                    }
                    continue;
                }
                defresult = Some(caseresult);
                break;
            }
            if !const_true_cond {
                // transformCaseExpr always supplies an ELSE (implicit NULL).
                let dr = ce.defresult.expect("CaseExpr.defresult is never NULL");
                defresult = Some(match ece_mutator(dr, cx) {
                    Ok(d) => d.unwrap_or(dr),
                    Err(e) => return restore(Err(e)),
                });
            }
            cx.case_val.set(save_case_val);
            if newargs.is_nil() {
                return Ok(defresult);
            }
            Ok(Some(Node::mk(
                cx.mcx,
                CaseExpr {
                    casetype: ce.casetype,
                    casecollid: ce.casecollid,
                    arg: newarg,
                    args: newargs,
                    defresult,
                    location: ce.location,
                },
            )?))
        }
        NodeTag::T_CaseTestExpr => match cx.case_val.get() {
            // C copyObject(case_val); the Const is rebuilt (never shared).
            Some(v) => Ok(Some(Node::mk(cx.mcx, *v.as_const().unwrap())?)),
            None => Ok(None),
        },
        NodeTag::T_CoalesceExpr => {
            let co = node.as_coalesce_expr().unwrap();
            let mut newargs = NodeList::nil();
            for a in &co.args {
                let e = ece_mutator(a, cx)?.unwrap_or(a);
                if let Some(c) = e.as_const() {
                    if c.constisnull {
                        continue;
                    }
                    if newargs.is_nil() {
                        return Ok(Some(e));
                    }
                    newargs.lappend(cx.mcx, e)?;
                    break;
                }
                newargs.lappend(cx.mcx, e)?;
            }
            if newargs.is_nil() {
                return Ok(Some(make_null_const(
                    cx.mcx,
                    co.coalescetype,
                    -1,
                    co.coalescecollid,
                )?));
            }
            Ok(Some(Node::mk(
                cx.mcx,
                CoalesceExpr {
                    coalescetype: co.coalescetype,
                    coalescecollid: co.coalescecollid,
                    args: newargs,
                    location: co.location,
                },
            )?))
        }
        // C's immutable-inputs generic arm: simplify args, fold whole node
        // when every input is Const (SubscriptingRef/ArrayExpr/RowExpr wait
        // for their vocabularies).
        NodeTag::T_MinMaxExpr => {
            let new = expression_tree_mutator(cx.mcx, node, &mut |n| ece_mutator(n, cx))?;
            let eff = new.unwrap_or(node);
            if all_arguments_const(eff)? {
                let mm = eff.as_min_max_expr().unwrap();
                return clauses_seams::evaluate_expr::call(
                    cx.mcx,
                    eff,
                    mm.minmaxtype,
                    -1,
                    mm.minmaxcollid,
                )
                .map(Some);
            }
            Ok(new)
        }
        NodeTag::T_NullTest => {
            use types_nodes::primnodes::{NullTest, NullTestType};
            let nt = node.as_null_test().unwrap();
            if nt.argisrow {
                deferred("eval_const_expressions_mutator: row-type NullTest", node.node_tag());
            }
            let old_arg = nt.arg.expect("NullTest.arg");
            let arg = ece_mutator(old_arg, cx)?;
            let eff = arg.unwrap_or(old_arg);
            if let Some(carg) = eff.as_const() {
                let result = match nt.nulltesttype {
                    NullTestType::IS_NULL => carg.constisnull,
                    NullTestType::IS_NOT_NULL => !carg.constisnull,
                };
                return Ok(Some(make_bool_const(cx.mcx, result, false)?));
            }
            match arg {
                None => Ok(None),
                Some(arg) => Ok(Some(Node::mk(
                    cx.mcx,
                    NullTest {
                        arg: Some(arg),
                        nulltesttype: nt.nulltesttype,
                        argisrow: nt.argisrow,
                        location: nt.location,
                    },
                )?)),
            }
        }
        NodeTag::T_Var
        | NodeTag::T_Const
        | NodeTag::T_RangeTblRef
        | NodeTag::T_CurrentOfExpr
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_SortGroupClause => Ok(None),
        // Aggref takes C's default ece_generic_processing arm: fold inside
        // the aggregate's arguments, never the Aggref itself. SubLink likewise
        // (C folds testexpr only; the sub-Query waits for SS_process_sublinks).
        NodeTag::T_Aggref
        | NodeTag::T_WindowFunc
        | NodeTag::T_TargetEntry
        | NodeTag::T_FromExpr
        | NodeTag::T_SubLink
        | NodeTag::T_List => {
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

// C simplify_or_arguments/simplify_and_arguments: flatten nested same-op
// BoolExprs (pre- and post-simplification), fold Const inputs. Returns true
// on C's forceTrue/forceFalse.
fn simplify_bool_arguments<'mcx>(
    cx: &EceContext<'mcx>,
    args: &NodeList<'mcx>,
    is_or: bool,
    newargs: &mut NodeList<'mcx>,
    have_null: &mut bool,
) -> PgResult<bool> {
    let same_op = |n: Node<'mcx>| {
        n.as_bool_expr().filter(|b| {
            b.boolop
                == if is_or { BoolExprType::OR_EXPR } else { BoolExprType::AND_EXPR }
        })
    };
    for arg in args {
        if let Some(sub) = same_op(arg) {
            if simplify_bool_arguments(cx, &sub.args, is_or, newargs, have_null)? {
                return Ok(true);
            }
            continue;
        }
        let arg = ece_mutator(arg, cx)?.unwrap_or(arg);
        if let Some(sub) = same_op(arg) {
            if simplify_bool_arguments(cx, &sub.args, is_or, newargs, have_null)? {
                return Ok(true);
            }
            continue;
        }
        if let Some(c) = arg.as_const() {
            if c.constisnull {
                *have_null = true;
            } else if c.constvalue.as_bool() == is_or {
                return Ok(true);
            }
            continue;
        }
        newargs.lappend(cx.mcx, arg)?;
    }
    Ok(false)
}

// negate_clause (prepqual.c): C's unlisted tags fall through to an explicit
// NOT; tags C simplifies but this vocabulary lacks stay loud above.
fn negate_clause<'mcx>(mcx: Mcx<'mcx>, node: Node<'mcx>) -> PgResult<Node<'mcx>> {
    match node.node_tag() {
        NodeTag::T_Const => {
            let c = node.as_const().unwrap();
            debug_assert_eq!(c.consttype, BOOLOID);
            if c.constisnull {
                return make_bool_const(mcx, false, true);
            }
            make_bool_const(mcx, !c.constvalue.as_bool(), false)
        }
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().unwrap();
            let negator = lsyscache::get_negator(o.opno)?;
            if negator != InvalidOid {
                // C leaves opfuncid InvalidOid for set_opfuncid's lazy memo
                // write-back; sealed shared nodes can't take the memo, so the
                // same get_opcode probe runs here instead.
                return Node::mk(
                    mcx,
                    OpExpr {
                        opno: negator,
                        opfuncid: lsyscache::get_opcode(negator)?,
                        opresulttype: o.opresulttype,
                        opretset: o.opretset,
                        opcollid: o.opcollid,
                        inputcollid: o.inputcollid,
                        args: o.args.clone_in(mcx)?,
                        location: o.location,
                    },
                );
            }
            crate::classify::make_notclause(mcx, node)
        }
        NodeTag::T_BoolExpr => {
            let b = node.as_bool_expr().unwrap();
            match b.boolop {
                // NOT over AND/OR: the negated args can't yield same-op
                // BoolExprs (recursion already simplified), so flatness holds
                // without pull_ands/pull_ors (C's argument verbatim).
                BoolExprType::AND_EXPR | BoolExprType::OR_EXPR => {
                    let mut nargs = NodeList::nil();
                    for arg in &b.args {
                        nargs.lappend(mcx, negate_clause(mcx, arg)?)?;
                    }
                    if b.boolop == BoolExprType::AND_EXPR {
                        crate::classify::make_orclause(mcx, nargs)
                    } else {
                        crate::classify::make_andclause(mcx, nargs)
                    }
                }
                BoolExprType::NOT_EXPR => Ok(b.args.nth(0)),
            }
        }
        NodeTag::T_NullTest => {
            let nt = node.as_null_test().unwrap();
            if !nt.argisrow {
                use types_nodes::primnodes::{NullTest, NullTestType};
                return Node::mk(
                    mcx,
                    NullTest {
                        arg: nt.arg,
                        nulltesttype: if nt.nulltesttype == NullTestType::IS_NULL {
                            NullTestType::IS_NOT_NULL
                        } else {
                            NullTestType::IS_NULL
                        },
                        argisrow: false,
                        location: nt.location,
                    },
                );
            }
            crate::classify::make_notclause(mcx, node)
        }
        other @ (NodeTag::T_ScalarArrayOpExpr | NodeTag::T_BooleanTest) => panic!(
            "negate_clause (prepqual.c): {other:?} simplification unported — \
             unit backend-optimizer-prep-prepqual"
        ),
        _ => crate::classify::make_notclause(mcx, node),
    }
}

pub fn make_bool_const<'mcx>(mcx: Mcx<'mcx>, value: bool, isnull: bool) -> PgResult<Node<'mcx>> {
    Node::mk(
        mcx,
        Const {
            consttype: BOOLOID,
            consttypmod: -1,
            constcollid: InvalidOid,
            constlen: 1,
            constvalue: Datum::from_bool(value),
            constisnull: isnull,
            constbyval: true,
            location: -1,
        },
    )
}

// Closed-set exprType over CoerceViaIO's possible transformed args.
fn coerce_arg_type(node: Node<'_>) -> Oid {
    match node.node_tag() {
        NodeTag::T_Const => node.as_const().unwrap().consttype,
        NodeTag::T_Var => node.as_var().unwrap().vartype,
        NodeTag::T_Param => node.as_param().unwrap().paramtype,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().funcresulttype,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().opresulttype,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resulttype,
        NodeTag::T_CoerceViaIO => node.as_coerce_via_io().unwrap().resulttype,
        other => deferred("coerce_arg_type (exprType)", other),
    }
}

fn coerce_arg_typmod(node: Node<'_>) -> i32 {
    match node.node_tag() {
        NodeTag::T_Const => node.as_const().unwrap().consttypmod,
        NodeTag::T_Var => node.as_var().unwrap().vartypmod,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resulttypmod,
        _ => -1,
    }
}

fn coerce_arg_collation(node: Node<'_>) -> Oid {
    match node.node_tag() {
        NodeTag::T_Const => node.as_const().unwrap().constcollid,
        NodeTag::T_Var => node.as_var().unwrap().varcollid,
        NodeTag::T_Param => node.as_param().unwrap().paramcollid,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().funccollid,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().opcollid,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resultcollid,
        NodeTag::T_CoerceViaIO => node.as_coerce_via_io().unwrap().resultcollid,
        other => deferred("coerce_arg_collation (exprCollation)", other),
    }
}

pub fn is_polymorphic_type(t: Oid) -> bool {
    use types_core::catalog::{
        ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID,
        ANYCOMPATIBLENONARRAYOID, ANYCOMPATIBLEOID, ANYCOMPATIBLERANGEOID, ANYELEMENTOID,
        ANYENUMOID, ANYMULTIRANGEOID, ANYNONARRAYOID, ANYRANGEOID,
    };
    matches!(
        t,
        ANYELEMENTOID
            | ANYARRAYOID
            | ANYNONARRAYOID
            | ANYENUMOID
            | ANYRANGEOID
            | ANYMULTIRANGEOID
            | ANYCOMPATIBLEOID
            | ANYCOMPATIBLEARRAYOID
            | ANYCOMPATIBLENONARRAYOID
            | ANYCOMPATIBLERANGEOID
            | ANYCOMPATIBLEMULTIRANGEOID
    )
}

/// C applyRelabelType (nodeFuncs.c), overwrite_ok=false (Consts rebuilt).
pub fn apply_relabel_type<'mcx>(
    mcx: Mcx<'mcx>,
    arg: Node<'mcx>,
    rtype: Oid,
    rtypmod: i32,
    rcollid: Oid,
    rformat: CoercionForm,
    rlocation: i32,
) -> PgResult<Node<'mcx>> {
    use types_nodes::primnodes::RelabelType;
    let mut arg = arg;
    while let Some(r) = arg.as_relabel_type() {
        arg = r.arg;
    }
    if let Some(c) = arg.as_const() {
        return Node::mk(
            mcx,
            Const { consttype: rtype, consttypmod: rtypmod, constcollid: rcollid, ..*c },
        );
    }
    if coerce_arg_type(arg) == rtype
        && coerce_arg_typmod(arg) == rtypmod
        && coerce_arg_collation(arg) == rcollid
    {
        return Ok(arg);
    }
    Node::mk(
        mcx,
        RelabelType {
            arg,
            resulttype: rtype,
            resulttypmod: rtypmod,
            resultcollid: rcollid,
            relabelformat: rformat,
            location: rlocation,
        },
    )
}

/// Reduce "x = true" to "x", "x = false" to NOT x (ditto <>, inverted).
fn simplify_boolean_equality<'mcx>(
    mcx: Mcx<'mcx>,
    opno: Oid,
    args: &NodeList<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    debug_assert_eq!(args.len(), 2);
    let (leftop, rightop) = (args.nth(0), args.nth(1));
    let eq = opno == BOOLEAN_EQUAL_OPERATOR;
    if let Some(c) = leftop.as_const() {
        debug_assert!(!c.constisnull);
        return Ok(Some(if c.constvalue.as_bool() == eq {
            rightop
        } else {
            negate_clause(mcx, rightop)?
        }));
    }
    if let Some(c) = rightop.as_const() {
        debug_assert!(!c.constisnull);
        return Ok(Some(if c.constvalue.as_bool() == eq {
            leftop
        } else {
            negate_clause(mcx, leftop)?
        }));
    }
    Ok(None)
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
