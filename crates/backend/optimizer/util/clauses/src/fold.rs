//! eval_const_expressions / estimate_expression_value (clauses.c).
//!
//! C divergences: the mutator is identity-preserving (walker.rs module doc);
//! `root` is unthreaded — its boundParams read is an explicit ParamListHandle
//! argument here; invalItems recording is not modeled (the evaluate_expr seam
//! installer must record invalItems); inline_function likewise skips
//! record_plan_function_dependency (same root-unthreaded gap).

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
    // C context->active_fns: SQL functions currently being inlined
    // (inline_function's recursion guard).
    active_fns: core::cell::RefCell<mcx::PgVec<'mcx, Oid>>,
    // Domains whose constraint-less CoerceToDomain was folded away; C
    // record_plan_type_dependency writes them to root->glob->invalItems when
    // context->root is set (clauses.c:3630). Planner callers harvest these.
    type_deps: core::cell::RefCell<Vec<Oid>>,
}

fn ece_context<'mcx>(
    mcx: Mcx<'mcx>,
    estimate: bool,
    bound_params: ParamListHandle,
) -> EceContext<'mcx> {
    EceContext {
        mcx,
        estimate,
        bound_params,
        case_val: core::cell::Cell::new(None),
        active_fns: core::cell::RefCell::new(mcx::PgVec::new_in(mcx)),
        type_deps: core::cell::RefCell::new(Vec::new()),
    }
}

pub fn eval_const_expressions<'mcx>(mcx: Mcx<'mcx>, node: Node<'mcx>) -> PgResult<Node<'mcx>> {
    eval_const_expressions_with_params(mcx, node, ParamListHandle::NULL)
}

pub fn eval_const_expressions_with_params<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    bound_params: ParamListHandle,
) -> PgResult<Node<'mcx>> {
    let cx = ece_context(mcx, false, bound_params);
    Ok(ece_mutator(node, &cx)?.unwrap_or(node))
}

/// The context->root != NULL lane of C eval_const_expressions: folded
/// constraint-less domains are appended to `type_deps` for the caller's
/// record_plan_type_dependency (setrefs.c:3594).
pub fn eval_const_expressions_planner<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    bound_params: ParamListHandle,
    type_deps: &mut Vec<Oid>,
) -> PgResult<Node<'mcx>> {
    let cx = ece_context(mcx, false, bound_params);
    let r = ece_mutator(node, &cx)?.unwrap_or(node);
    type_deps.append(&mut cx.type_deps.borrow_mut());
    Ok(r)
}

pub fn estimate_expression_value<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let cx = ece_context(mcx, true, ParamListHandle::NULL);
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

// datumCopy (datum.c) scoped to bound-parameter substitution; by-ref varlena
// sources carry any header form (fmgr_sql binds raw tuple datums: short 1B
// headers and toast pointers included), so the -1 arm is C's VARSIZE_ANY.
fn datum_copy_in<'mcx>(mcx: Mcx<'mcx>, value: Datum, typlen: i16) -> PgResult<Datum> {
    let p = value.as_usize() as *const u8;
    if p.is_null() {
        return Ok(Datum::null());
    }
    let size = match typlen {
        -1 => {
            // SAFETY: non-null by-ref varlena datum, readable for its
            // header-declared (VARSIZE_ANY) size.
            unsafe {
                let b0 = *p;
                if b0 == 0x01 {
                    // VARHDRSZ_EXTERNAL + VARTAG_SIZE (postgres.h); the toast
                    // pointer itself is copied, exactly datumCopy.
                    2 + match *p.add(1) {
                        18 => 16,
                        1 => 8,
                        2 | 3 => panic!(
                            "datum_copy_in: expanded-object flatten (EOH_flatten_into) unported"
                        ),
                        tag => panic!("datum_copy_in: unknown vartag {tag}"),
                    }
                } else if b0 & 0x01 != 0 {
                    (b0 as usize >> 1) & 0x7F
                } else {
                    datum::VarlenaRef::from_ptr(p).varsize()
                }
            }
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
    stack_depth::check_stack_depth()?;
    match node.node_tag() {
        NodeTag::T_Param => substitute_bound_param(node, cx),
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().unwrap();
            let arg = ece_mutator(r.arg, cx)?.unwrap_or(r.arg);
            Ok(Some(nodes_core::node_funcs::apply_relabel_type(
                cx.mcx,
                arg,
                r.resulttype,
                r.resulttypmod,
                r.resultcollid,
                r.relabelformat,
                r.location,
            )?))
        }
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
        NodeTag::T_ScalarArrayOpExpr => {
            use types_nodes::primnodes::ScalarArrayOpExpr;
            let sa = node.as_scalar_array_op_expr().unwrap();
            let new_args = mutate_list(cx.mcx, &sa.args, &mut |n| ece_mutator(n, cx))?;
            // set_sa_opfuncid (nodeFuncs.c).
            let opfuncid =
                if sa.opfuncid == 0 { lsyscache::get_opcode(sa.opno)? } else { sa.opfuncid };
            let all_const =
                new_args.as_ref().unwrap_or(&sa.args).iter().all(|a| a.as_const().is_some());
            let changed = new_args.is_some() || opfuncid != sa.opfuncid;
            if !all_const || !ece_function_is_safe(cx, opfuncid)? {
                if !changed {
                    return Ok(None);
                }
            }
            let args = match new_args {
                Some(a) => a,
                None => sa.args.clone_in(cx.mcx)?,
            };
            let new_node = Node::mk(
                cx.mcx,
                ScalarArrayOpExpr {
                    opno: sa.opno,
                    opfuncid,
                    hashfuncid: sa.hashfuncid,
                    negfuncid: sa.negfuncid,
                    useOr: sa.useOr,
                    inputcollid: sa.inputcollid,
                    args,
                    location: sa.location,
                },
            )?;
            if all_const && ece_function_is_safe(cx, opfuncid)? {
                return clauses_seams::evaluate_expr::call(cx.mcx, new_node, BOOLOID, -1, 0)
                    .map(Some);
            }
            Ok(Some(new_node))
        }
        NodeTag::T_RowExpr => {
            let r = node.as_row_expr().unwrap();
            let new_args = mutate_list(cx.mcx, &r.args, &mut |n| ece_mutator(n, cx))?;
            let all_const =
                new_args.as_ref().unwrap_or(&r.args).iter().all(|e| e.as_const().is_some());
            if !all_const && new_args.is_none() {
                return Ok(None);
            }
            let args = match new_args {
                Some(a) => a,
                None => r.args.clone_in(cx.mcx)?,
            };
            let new_node = Node::mk(
                cx.mcx,
                types_nodes::primnodes::RowExpr {
                    args,
                    row_typeid: r.row_typeid,
                    row_format: r.row_format,
                    colnames: r.colnames.clone_in(cx.mcx)?,
                    location: r.location,
                },
            )?;
            if all_const {
                return clauses_seams::evaluate_expr::call(cx.mcx, new_node, r.row_typeid, -1, 0)
                    .map(Some);
            }
            Ok(Some(new_node))
        }
        NodeTag::T_ArrayExpr => {
            use types_nodes::primnodes::ArrayExpr;
            let a = node.as_array_expr().unwrap();
            let new_elements =
                mutate_list(cx.mcx, &a.elements, &mut |n| ece_mutator(n, cx))?;
            let all_const =
                new_elements.as_ref().unwrap_or(&a.elements).iter().all(|e| e.as_const().is_some());
            if !all_const && new_elements.is_none() {
                return Ok(None);
            }
            let elements = match new_elements {
                Some(e) => e,
                None => a.elements.clone_in(cx.mcx)?,
            };
            let new_node = Node::mk(
                cx.mcx,
                ArrayExpr {
                    array_typeid: a.array_typeid,
                    array_collid: a.array_collid,
                    element_typeid: a.element_typeid,
                    elements,
                    multidims: a.multidims,
                    list_start: a.list_start,
                    list_end: a.list_end,
                    location: a.location,
                },
            )?;
            if all_const {
                return clauses_seams::evaluate_expr::call(
                    cx.mcx,
                    new_node,
                    a.array_typeid,
                    -1,
                    a.array_collid,
                )
                .map(Some);
            }
            Ok(Some(new_node))
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
        // C: CollateExpr is replaced with an equivalent RelabelType.
        NodeTag::T_CollateExpr => {
            let c = node.as_collate_expr().unwrap();
            let arg = ece_mutator(c.arg, cx)?.unwrap_or(c.arg);
            nodes_core::node_funcs::apply_relabel_type(
                cx.mcx,
                arg,
                nodes_core::node_funcs::expr_type(arg),
                nodes_core::node_funcs::expr_typmod(arg),
                c.collOid,
                CoercionForm::COERCE_IMPLICIT_CAST,
                c.location,
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
        NodeTag::T_ArrayCoerceExpr => {
            let ac = node.as_array_coerce_expr().unwrap();
            let arg = ece_mutator(ac.arg, cx)?.unwrap_or(ac.arg);
            // The elemexpr's CaseTestExpr must not absorb an outer CASE value.
            let save_case_val = cx.case_val.replace(None);
            let elemexpr = match ac.elemexpr {
                Some(e) => {
                    let r = ece_mutator(e, cx);
                    cx.case_val.set(save_case_val);
                    Some(r?.unwrap_or(e))
                }
                None => {
                    cx.case_val.set(save_case_val);
                    None
                }
            };
            let new = Node::mk(
                cx.mcx,
                types_nodes::ArrayCoerceExpr {
                    arg,
                    elemexpr,
                    resulttype: ac.resulttype,
                    resulttypmod: ac.resulttypmod,
                    resultcollid: ac.resultcollid,
                    coerceformat: ac.coerceformat,
                    location: ac.location,
                },
            )?;
            // A CoerceToDomain elemexpr keeps the domain's runtime checks.
            if arg.node_tag() == NodeTag::T_Const
                && elemexpr
                    .is_some_and(|e| e.node_tag() != NodeTag::T_CoerceToDomain)
                && !crate::classify::contain_mutable_functions(elemexpr.unwrap())?
            {
                return clauses_seams::evaluate_expr::call(
                    cx.mcx,
                    new,
                    ac.resulttype,
                    ac.resulttypmod,
                    ac.resultcollid,
                )
                .map(Some);
            }
            Ok(Some(new))
        }
        NodeTag::T_ConvertRowtypeExpr => {
            let cr = node.as_convert_rowtype_expr().unwrap();
            let mut arg = ece_mutator(cr.arg, cx)?.unwrap_or(cr.arg);
            let mut convertformat = cr.convertformat;
            // C: a nested ConvertRowtypeExpr is redundant (by-name mapping
            // composes); keep the inner format under an implicit outer cast.
            if let Some(inner) = arg.as_convert_rowtype_expr() {
                arg = inner.arg;
                if convertformat == CoercionForm::COERCE_IMPLICIT_CAST {
                    convertformat = inner.convertformat;
                }
            }
            let new = Node::mk(
                cx.mcx,
                types_nodes::ConvertRowtypeExpr {
                    arg,
                    resulttype: cr.resulttype,
                    convertformat,
                    location: cr.location,
                },
            )?;
            if arg.node_tag() == NodeTag::T_Const {
                return clauses_seams::evaluate_expr::call(
                    cx.mcx,
                    new,
                    cr.resulttype,
                    -1,
                    InvalidOid,
                )
                .map(Some);
            }
            Ok(Some(new))
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
        NodeTag::T_ArrayExpr => {
            let new = expression_tree_mutator(cx.mcx, node, &mut |n| ece_mutator(n, cx))?;
            let eff = new.unwrap_or(node);
            if all_arguments_const(eff)? {
                let a = eff.as_array_expr().unwrap();
                return clauses_seams::evaluate_expr::call(
                    cx.mcx,
                    eff,
                    a.array_typeid,
                    -1,
                    a.array_collid,
                )
                .map(Some);
            }
            Ok(new)
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let new = expression_tree_mutator(cx.mcx, node, &mut |n| ece_mutator(n, cx))?;
            let eff = new.unwrap_or(node);
            let sa = eff.as_scalar_array_op_expr().unwrap();
            // set_sa_opfuncid, without C's memo write-back (walker.rs).
            let opfuncid =
                if sa.opfuncid == 0 { lsyscache::get_opcode(sa.opno)? } else { sa.opfuncid };
            // ece_function_is_safe: non-volatile folds (estimation lane off).
            const PROVOLATILE_VOLATILE: i8 = b'v' as i8;
            if lsyscache::func_volatile(opfuncid)? != PROVOLATILE_VOLATILE
                && all_arguments_const(eff)?
            {
                let refolded = if opfuncid != sa.opfuncid || new.is_none() {
                    Node::mk(
                        cx.mcx,
                        types_nodes::ScalarArrayOpExpr {
                            opno: sa.opno,
                            opfuncid,
                            hashfuncid: sa.hashfuncid,
                            negfuncid: sa.negfuncid,
                            useOr: sa.useOr,
                            inputcollid: sa.inputcollid,
                            args: sa.args.clone_in(cx.mcx)?,
                            location: sa.location,
                        },
                    )?
                } else {
                    eff
                };
                return clauses_seams::evaluate_expr::call(
                    cx.mcx,
                    refolded,
                    types_core::catalog::BOOLOID,
                    -1,
                    InvalidOid,
                )
                .map(Some);
            }
            if opfuncid != sa.opfuncid {
                return Ok(Some(Node::mk(
                    cx.mcx,
                    types_nodes::ScalarArrayOpExpr {
                        opno: sa.opno,
                        opfuncid,
                        hashfuncid: sa.hashfuncid,
                        negfuncid: sa.negfuncid,
                        useOr: sa.useOr,
                        inputcollid: sa.inputcollid,
                        args: sa.args.clone_in(cx.mcx)?,
                        location: sa.location,
                    },
                )?));
            }
            Ok(new)
        }
        NodeTag::T_SubscriptingRef => {
            let new = expression_tree_mutator(cx.mcx, node, &mut |n| ece_mutator(n, cx))?;
            let eff = new.unwrap_or(node);
            let sr = eff.as_subscripting_ref().unwrap();
            if sr.refassgnexpr.is_none() && all_arguments_const(eff)? {
                return clauses_seams::evaluate_expr::call(
                    cx.mcx,
                    eff,
                    sr.refrestype,
                    sr.reftypmod,
                    sr.refcollid,
                )
                .map(Some);
            }
            Ok(new)
        }
        // C has no dedicated FieldStore arm: it falls to the default
        // ece_generic_processing (recurse into arg/newvals, never fold).
        NodeTag::T_FieldStore => {
            expression_tree_mutator(cx.mcx, node, &mut |n| ece_mutator(n, cx))
        }
        NodeTag::T_NullTest => {
            use types_nodes::primnodes::{NullTest, NullTestType};
            let nt = node.as_null_test().unwrap();
            let old_arg = nt.arg.expect("NullTest.arg");
            let arg = ece_mutator(old_arg, cx)?;
            let eff = arg.unwrap_or(old_arg);
            if nt.argisrow && eff.node_tag() == NodeTag::T_RowExpr {
                // C breaks ROW(...) IS [NOT] NULL into scalar per-field tests
                // (non-recursive semantics; see ExecEvalRowNullInt).
                let rarg = eff.as_row_expr().unwrap();
                let mut newargs = NodeList::nil();
                for relem in &rarg.args {
                    if let Some(carg) = relem.as_const() {
                        let refutes = if carg.constisnull {
                            nt.nulltesttype == NullTestType::IS_NOT_NULL
                        } else {
                            nt.nulltesttype == NullTestType::IS_NULL
                        };
                        if refutes {
                            return Ok(Some(make_bool_const(cx.mcx, false, false)?));
                        }
                        continue;
                    }
                    let newntest = Node::mk(
                        cx.mcx,
                        NullTest {
                            arg: Some(relem),
                            nulltesttype: nt.nulltesttype,
                            argisrow: false,
                            location: nt.location,
                        },
                    )?;
                    newargs.lappend(cx.mcx, newntest)?;
                }
                if newargs.is_nil() {
                    return Ok(Some(make_bool_const(cx.mcx, true, false)?));
                }
                if newargs.len() == 1 {
                    return Ok(Some(newargs.first().expect("one arg")));
                }
                return Ok(Some(Node::mk(
                    cx.mcx,
                    BoolExpr { boolop: BoolExprType::AND_EXPR, args: newargs, location: -1 },
                )?));
            }
            if !nt.argisrow {
                if let Some(carg) = eff.as_const() {
                    let result = match nt.nulltesttype {
                        NullTestType::IS_NULL => carg.constisnull,
                        NullTestType::IS_NOT_NULL => !carg.constisnull,
                    };
                    return Ok(Some(make_bool_const(cx.mcx, result, false)?));
                }
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
        NodeTag::T_BooleanTest => {
            use types_nodes::{BoolTestType, BooleanTest};
            let bt = node.as_boolean_test().unwrap();
            let old_arg = bt.arg.expect("BooleanTest.arg");
            let arg = ece_mutator(old_arg, cx)?;
            let eff = arg.unwrap_or(old_arg);
            if let Some(carg) = eff.as_const() {
                let v = carg.constvalue.as_bool();
                let result = match bt.booltesttype {
                    BoolTestType::IS_TRUE => !carg.constisnull && v,
                    BoolTestType::IS_NOT_TRUE => carg.constisnull || !v,
                    BoolTestType::IS_FALSE => !carg.constisnull && !v,
                    BoolTestType::IS_NOT_FALSE => carg.constisnull || v,
                    BoolTestType::IS_UNKNOWN => carg.constisnull,
                    BoolTestType::IS_NOT_UNKNOWN => !carg.constisnull,
                };
                return Ok(Some(make_bool_const(cx.mcx, result, false)?));
            }
            match arg {
                None => Ok(None),
                Some(arg) => Ok(Some(Node::mk(
                    cx.mcx,
                    BooleanTest {
                        arg: Some(arg),
                        booltesttype: bt.booltesttype,
                        location: bt.location,
                    },
                )?)),
            }
        }
        NodeTag::T_CoerceToDomain => {
            let cd = node.as_coerce_to_domain().unwrap();
            let arg = ece_mutator(cd.arg, cx)?;
            // C also substitutes when the domain has no constraints, after
            // record_plan_type_dependency (clauses.c:3626-3631): the fold is
            // only plan-safe if ALTER DOMAIN invalidates the cached plan.
            if cx.estimate || !typcache_seams::domain_has_constraints::call(cd.resulttype)? {
                if !cx.estimate {
                    cx.type_deps.borrow_mut().push(cd.resulttype);
                }
                let eff = arg.unwrap_or(cd.arg);
                return Ok(Some(apply_relabel_type(
                    cx.mcx,
                    eff,
                    cd.resulttype,
                    cd.resulttypmod,
                    cd.resultcollid,
                    cd.coercionformat,
                    cd.location,
                )?));
            }
            match arg {
                None => Ok(None),
                Some(arg) => Ok(Some(Node::mk(
                    cx.mcx,
                    types_nodes::CoerceToDomain {
                        arg,
                        resulttype: cd.resulttype,
                        resulttypmod: cd.resulttypmod,
                        resultcollid: cd.resultcollid,
                        coercionformat: cd.coercionformat,
                        location: cd.location,
                    },
                )?)),
            }
        }
        NodeTag::T_DistinctExpr => {
            use types_nodes::DistinctExpr;
            let d = node.as_distinct_expr().unwrap();
            let new_args = mutate_list(cx.mcx, &d.args, &mut |n| ece_mutator(n, cx))?;
            let eff_args = new_args.as_ref().unwrap_or(&d.args);

            let mut has_null_input = false;
            let mut all_null_input = true;
            let mut has_nonconst_input = false;
            for arg in eff_args.iter() {
                match arg.as_const() {
                    Some(c) => {
                        has_null_input |= c.constisnull;
                        all_null_input &= c.constisnull;
                    }
                    None => has_nonconst_input = true,
                }
            }
            let opfuncid = if d.opfuncid == 0 {
                lsyscache::get_opcode(d.opno)?
            } else {
                d.opfuncid
            };
            if !has_nonconst_input {
                if all_null_input {
                    return Ok(Some(make_bool_const(cx.mcx, false, false)?));
                }
                if has_null_input {
                    return Ok(Some(make_bool_const(cx.mcx, true, false)?));
                }
                let (simple, _) = simplify_function(
                    cx,
                    opfuncid,
                    d.opresulttype,
                    -1,
                    d.opcollid,
                    d.inputcollid,
                    eff_args,
                    false,
                    false,
                    false,
                )?;
                if let Some(simple) = simple {
                    let c = simple.as_const().expect("simplify_function returns a Const");
                    // Underlying operator is "="; negate its result.
                    return Ok(Some(make_bool_const(
                        cx.mcx,
                        !c.constvalue.as_bool(),
                        c.constisnull,
                    )?));
                }
            }
            match new_args {
                None if opfuncid == d.opfuncid => Ok(None),
                new_args => {
                    let args = match new_args {
                        Some(a) => a,
                        None => d.args.clone_in(cx.mcx)?,
                    };
                    Ok(Some(Node::mk(
                        cx.mcx,
                        DistinctExpr {
                            opno: d.opno,
                            opfuncid,
                            opresulttype: d.opresulttype,
                            opretset: d.opretset,
                            opcollid: d.opcollid,
                            inputcollid: d.inputcollid,
                            args,
                            location: d.location,
                        },
                    )?))
                }
            }
        }
        NodeTag::T_NullIfExpr => {
            use types_nodes::NullIfExpr;
            let e = node.as_null_if_expr().unwrap();
            let new_args = mutate_list(cx.mcx, &e.args, &mut |n| ece_mutator(n, cx))?;
            let eff_args = new_args.as_ref().unwrap_or(&e.args);

            // A NULL input can't compare equal: NULLIF yields the first arg.
            let mut has_nonconst_input = false;
            for arg in eff_args.iter() {
                match arg.as_const() {
                    Some(c) if c.constisnull => return Ok(Some(eff_args.nth(0))),
                    Some(_) => {}
                    None => has_nonconst_input = true,
                }
            }
            // set_opfuncid (nodeFuncs.c).
            let opfuncid =
                if e.opfuncid == 0 { lsyscache::get_opcode(e.opno)? } else { e.opfuncid };
            // ece_evaluate_expr: exprTypmod(NullIfExpr) is the first arg's.
            let first_typmod = nodes_core::node_funcs::expr_typmod(eff_args.nth(0));
            if !has_nonconst_input && ece_function_is_safe(cx, opfuncid)? {
                let args = match new_args {
                    Some(a) => a,
                    None => e.args.clone_in(cx.mcx)?,
                };
                let new_node = Node::mk(
                    cx.mcx,
                    NullIfExpr {
                        opno: e.opno,
                        opfuncid,
                        opresulttype: e.opresulttype,
                        opretset: e.opretset,
                        opcollid: e.opcollid,
                        inputcollid: e.inputcollid,
                        args,
                        location: e.location,
                    },
                )?;
                return clauses_seams::evaluate_expr::call(
                    cx.mcx,
                    new_node,
                    e.opresulttype,
                    first_typmod,
                    e.opcollid,
                )
                .map(Some);
            }
            match new_args {
                None if opfuncid == e.opfuncid => Ok(None),
                new_args => {
                    let args = match new_args {
                        Some(a) => a,
                        None => e.args.clone_in(cx.mcx)?,
                    };
                    Ok(Some(Node::mk(
                        cx.mcx,
                        NullIfExpr {
                            opno: e.opno,
                            opfuncid,
                            opresulttype: e.opresulttype,
                            opretset: e.opretset,
                            opcollid: e.opcollid,
                            inputcollid: e.inputcollid,
                            args,
                            location: e.location,
                        },
                    )?))
                }
            }
        }
        NodeTag::T_CoerceToDomainValue => Ok(None),
        NodeTag::T_Var
        | NodeTag::T_Const
        | NodeTag::T_RangeTblRef
        | NodeTag::T_CurrentOfExpr
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_NextValueExpr
        | NodeTag::T_MergeSupportFunc
        | NodeTag::T_SortGroupClause => Ok(None),
        // C's T_WindowFunc arm can't simplify the node but still expands
        // named args/defaults in its argument list before recursing.
        NodeTag::T_WindowFunc => {
            let wf = node.as_window_func().unwrap();
            match expand_function_arguments_opt(cx.mcx, &wf.args, false, wf.wintype, wf.winfnoid)? {
                None => expression_tree_mutator(cx.mcx, node, &mut |n| ece_mutator(n, cx)),
                Some(args) => {
                    let expanded = Node::mk(
                        cx.mcx,
                        types_nodes::primnodes::WindowFunc {
                            winfnoid: wf.winfnoid,
                            wintype: wf.wintype,
                            wincollid: wf.wincollid,
                            inputcollid: wf.inputcollid,
                            args,
                            aggfilter: wf.aggfilter,
                            runCondition: wf.runCondition.clone_in(cx.mcx)?,
                            winref: wf.winref,
                            winstar: wf.winstar,
                            winagg: wf.winagg,
                            location: wf.location,
                        },
                    )?;
                    Ok(Some(
                        expression_tree_mutator(cx.mcx, expanded, &mut |n| ece_mutator(n, cx))?
                            .unwrap_or(expanded),
                    ))
                }
            }
        }
        // Aggref takes C's default ece_generic_processing arm: fold inside
        // the aggregate's arguments, never the Aggref itself. SubLink likewise
        // (C folds testexpr only; the sub-Query waits for SS_process_sublinks).
        NodeTag::T_Aggref
        | NodeTag::T_TargetEntry
        | NodeTag::T_FromExpr
        | NodeTag::T_SubLink
        | NodeTag::T_XmlExpr
        | NodeTag::T_TableFunc
        | NodeTag::T_WindowFuncRunCondition
        | NodeTag::T_TableSampleClause
        | NodeTag::T_ReturningExpr
        | NodeTag::T_List => {
            expression_tree_mutator(cx.mcx, node, &mut |n| ece_mutator(n, cx))
        }
        // C's generic arm again (GroupingFunc lives outside the nodes_core
        // mutator vocabulary): fold inside args; refs/cols never change.
        NodeTag::T_GroupingFunc => {
            let g = node.as_grouping_func().unwrap();
            let mut changed = false;
            let mut args = NodeList::nil();
            for a in &g.args {
                let e = ece_mutator(a, cx)?;
                changed |= e.is_some();
                args.lappend(cx.mcx, e.unwrap_or(a))?;
            }
            if !changed {
                return Ok(None);
            }
            Ok(Some(Node::mk(
                cx.mcx,
                types_nodes::primnodes::GroupingFunc {
                    args,
                    refs: g.refs.clone_in(cx.mcx)?,
                    cols: g.cols.clone_in(cx.mcx)?,
                    agglevelsup: g.agglevelsup,
                    location: g.location,
                },
            )?))
        }
        // C T_JsonValueExpr arm (clauses.c:2916): a Const formatted_expr
        // elides the JsonValueExpr; else fold both legs.
        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().unwrap();
            let formatted = j.formatted_expr.expect("formatted_expr");
            let new_formatted = ece_mutator(formatted, cx)?.unwrap_or(formatted);
            if new_formatted.as_const().is_some() {
                return Ok(Some(new_formatted));
            }
            let raw = j.raw_expr.expect("raw_expr");
            let new_raw = ece_mutator(raw, cx)?.unwrap_or(raw);
            // C's mutator copies unconditionally, so raw_expr never aliases
            // nodes inside formatted_expr; preprocess_aggrefs relies on that
            // (a shared volatile Aggref would be renumbered twice).
            let new_raw = copyfuncs::copy_object(cx.mcx, new_raw)?;
            Ok(Some(Node::mk(
                cx.mcx,
                types_nodes::JsonValueExpr {
                    raw_expr: Some(new_raw),
                    formatted_expr: Some(new_formatted),
                    format: j.format,
                },
            )?))
        }
        // C default ece_generic_processing over the remaining SQL/JSON nodes.
        NodeTag::T_JsonConstructorExpr
        | NodeTag::T_JsonIsPredicate
        | NodeTag::T_JsonExpr
        | NodeTag::T_JsonBehavior => {
            expression_tree_mutator(cx.mcx, node, &mut |n| ece_mutator(n, cx))
        }
        // C's generic arm: mutate both sides; the op/family lists never change.
        NodeTag::T_RowCompareExpr => {
            let rc = node.as_row_compare_expr().unwrap();
            let largs = mutate_list(cx.mcx, &rc.largs, &mut |n| ece_mutator(n, cx))?;
            let rargs = mutate_list(cx.mcx, &rc.rargs, &mut |n| ece_mutator(n, cx))?;
            if largs.is_none() && rargs.is_none() {
                return Ok(None);
            }
            Ok(Some(Node::mk(
                cx.mcx,
                types_nodes::RowCompareExpr {
                    cmptype: rc.cmptype,
                    opnos: rc.opnos.clone_in(cx.mcx)?,
                    opfamilies: rc.opfamilies.clone_in(cx.mcx)?,
                    inputcollids: rc.inputcollids.clone_in(cx.mcx)?,
                    largs: match largs {
                        Some(l) => l,
                        None => rc.largs.clone_in(cx.mcx)?,
                    },
                    rargs: match rargs {
                        Some(l) => l,
                        None => rc.rargs.clone_in(cx.mcx)?,
                    },
                },
            )?))
        }
        NodeTag::T_FieldSelect => {
            let fs = node.as_field_select().unwrap();
            let arg = ece_mutator(fs.arg, cx)?.unwrap_or(fs.arg);
            if let Some(v) = arg.as_var() {
                if v.varattno == types_core::InvalidAttrNumber
                    && v.varlevelsup == 0
                    && rowtype_field_matches(
                        cx.mcx,
                        v.vartype,
                        fs.fieldnum as i32,
                        fs.resulttype,
                        fs.resulttypmod,
                        fs.resultcollid,
                    )?
                {
                    let newvar = Node::mk_var(
                        cx.mcx,
                        v.varno,
                        fs.fieldnum,
                        fs.resulttype,
                        fs.resulttypmod,
                        fs.resultcollid,
                        v.varlevelsup,
                    )?;
                    // C copies varreturningtype/varnullingrels from the old Var.
                    // SAFETY: freshly built node, no other reference.
                    unsafe {
                        newvar
                            .with_mut::<types_nodes::Var, _>(|nv| {
                                nv.varreturningtype = v.varreturningtype;
                                nv.varnullingrels = v.varnullingrels.clone_in(cx.mcx).expect("bms clone");
                            })
                            .unwrap();
                    }
                    return Ok(Some(newvar));
                }
            }
            if let Some(r) = arg.as_row_expr() {
                let f = fs.fieldnum as usize;
                if fs.fieldnum > 0 && f <= r.args.len() {
                    let fld = r.args.nth(f - 1);
                    if rowtype_field_matches(
                        cx.mcx,
                        r.row_typeid,
                        fs.fieldnum as i32,
                        fs.resulttype,
                        fs.resulttypmod,
                        fs.resultcollid,
                    )? && fs.resulttype == nodes_core::node_funcs::expr_type(fld)
                        && fs.resulttypmod == nodes_core::node_funcs::expr_typmod(fld)
                        && fs.resultcollid == nodes_core::node_funcs::expr_collation(fld)
                    {
                        return Ok(Some(fld));
                    }
                }
            }
            // C also const-folds a Const arg via ece_evaluate_expr — unfolded
            // here (runtime FieldSelect evaluates it identically).
            Ok(Some(Node::mk(
                cx.mcx,
                types_nodes::FieldSelect {
                    arg,
                    fieldnum: fs.fieldnum,
                    resulttype: fs.resulttype,
                    resulttypmod: fs.resulttypmod,
                    resultcollid: fs.resultcollid,
                },
            )?))
        }
        NodeTag::T_PlaceHolderVar => {
            let phv = node.as_place_holder_var().unwrap();
            if cx.estimate {
                // Estimation mode strips the PHV (assume not nulled).
                return Ok(Some(ece_mutator(phv.phexpr, cx)?.unwrap_or(phv.phexpr)));
            }
            match ece_mutator(phv.phexpr, cx)? {
                None => Ok(None),
                Some(e) => Ok(Some(Node::mk(
                    cx.mcx,
                    types_nodes::primnodes::PlaceHolderVar {
                        phexpr: e,
                        phrels: phv.phrels.clone_in(cx.mcx)?,
                        phnullingrels: phv.phnullingrels.clone_in(cx.mcx)?,
                        phid: phv.phid,
                        phlevelsup: phv.phlevelsup,
                    },
                )?)),
            }
        }
        // C: "Return a SubPlan unchanged --- too late to do anything with it"
        // (reached when folding runs over an expression that already went
        // through SS_process_sublinks).
        NodeTag::T_SubPlan | NodeTag::T_AlternativeSubPlan => Ok(None),
        other => deferred("eval_const_expressions_mutator", other),
    }
}

/// exprIsLengthCoercion shape: a 2- or 3-arg cast whose second arg is a
/// non-null int4 Const carries that typmod.
fn func_expr_typmod(f: &FuncExpr<'_>) -> i32 {
    if !matches!(
        f.funcformat,
        CoercionForm::COERCE_EXPLICIT_CAST | CoercionForm::COERCE_IMPLICIT_CAST
    ) || !(2..=3).contains(&f.args.len())
    {
        return -1;
    }
    match f.args.nth(1).as_const() {
        Some(c) if c.consttype == INT4OID && !c.constisnull => c.constvalue.as_i32(),
        _ => -1,
    }
}

#[cold]
pub(crate) fn func_lookup_failed(funcid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("cache lookup failed for function {funcid}")))
}

/// Returns (simplified-expression,
/// possibly-rewritten args); `None` args = unchanged. The executor-evaluation
/// leg rides the clauses_seams::evaluate_expr seam; a prosupport
/// SupportRequestSimplify rewrite defers loud.
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
        let expanded = expand_function_arguments_opt(cx.mcx, args, false, result_type, funcid)?;
        let base = expanded.as_ref().unwrap_or(args);
        new_args = match mutate_list(cx.mcx, base, &mut |n| ece_mutator(n, cx))? {
            Some(l) => Some(l),
            None => expanded,
        };
    }
    let eff_args = new_args.as_ref().unwrap_or(args);

    let mut newexpr = evaluate_function(
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
        let fcall = Node::mk(
            cx.mcx,
            FuncExpr {
                funcid,
                funcresulttype: result_type,
                funcretset: shape.proretset,
                funcvariadic,
                funcformat: CoercionForm::COERCE_EXPLICIT_CALL,
                funccollid: result_collid,
                inputcollid: input_collid,
                args: eff_args.clone_in(cx.mcx)?,
                location: -1,
            },
        )?;
        let mut req =
            types_nodes::supportnodes::SupportRequestSimplify::new(Some(fcall), Some(cx.mcx));
        let addr = core::ptr::from_mut(&mut req) as usize;
        let result =
            fmgr_core::oid_function_call1_coll(shape.prosupport, 0, Datum::from_usize(addr))?;
        if result.as_usize() != 0 {
            // SAFETY: prosupport simplify contract — the rewrite is a sealed
            // node the callee allocated in the request's mcx.
            let node = unsafe {
                Node::from_raw(core::ptr::NonNull::new_unchecked(result.as_usize() as *mut ()))
            };
            newexpr = Some(ece_mutator(node, cx)?.unwrap_or(node));
        }
    }
    let newexpr = match newexpr {
        None if allow_non_const => inline_function(
            cx,
            funcid,
            result_type,
            result_collid,
            input_collid,
            eff_args,
            &shape,
        )?,
        e => e,
    };
    Ok((newexpr, new_args))
}

const PROKIND_FUNCTION: i8 = b'f' as i8;
const ACL_EXECUTE: u64 = 1 << 7;
const ACLCHECK_OK: i32 = 0;
const FUNC_MAX_ARGS: usize = 100;

// inline_function (clauses.c): expand a simple SQL-language function call
// in place. The parser-dependent middle (body parse/analyze, simple-SELECT
// gate, check_sql_fn_retval, parameter substitution) rides the
// inline_sql_function seam; record_plan_function_dependency is not modeled
// (module doc, same gap as invalItems).
fn inline_function<'mcx>(
    cx: &EceContext<'mcx>,
    funcid: Oid,
    result_type: Oid,
    result_collid: Oid,
    input_collid: Oid,
    args: &NodeList<'mcx>,
    shape: &PgProcShape,
) -> PgResult<Option<Node<'mcx>>> {
    if shape.prolang != fmgr_core::SQL_LANGUAGE_ID
        || shape.prokind != PROKIND_FUNCTION
        || shape.prosecdef
        || shape.proretset
        || shape.prorettype == RECORDOID
        || !shape.proconfig_isnull
        || shape.pronargs as usize != args.len()
    {
        return Ok(None);
    }
    if cx.active_fns.borrow().contains(&funcid) {
        return Ok(None);
    }
    let userid = miscinit_seams::get_user_id::call();
    let aclresult = aclchk_seams::object_aclcheck::call(
        types_core::catalog::PROCEDURE_RELATION_ID,
        funcid,
        userid,
        ACL_EXECUTE,
    )?;
    if aclresult != ACLCHECK_OK {
        return Ok(None);
    }
    let Some(newexpr) = clauses_seams::inline_sql_function::call(
        cx.mcx,
        funcid,
        result_type,
        result_collid,
        input_collid,
        args,
    )?
    else {
        return Ok(None);
    };
    {
        let mut af = cx.active_fns.borrow_mut();
        af.try_reserve(1).map_err(|_| cx.mcx.oom(1))?;
        af.push(funcid);
    }
    let result = ece_mutator(newexpr, cx);
    cx.active_fns.borrow_mut().pop();
    // C's sql_inline_error_callback is still installed across the recursive
    // re-simplification; the parse-region legs run inside the seam body.
    let result = result.map_err(|e| sql_inline_recursion_error(cx.mcx, funcid, e))?;
    Ok(Some(result.unwrap_or(newexpr)))
}

#[track_caller]
#[cold]
fn sql_inline_recursion_error<'mcx>(
    mcx: Mcx<'mcx>,
    funcid: Oid,
    e: Box<PgError>,
) -> Box<PgError> {
    let mut err = *e;
    let name = match lsyscache::function::get_func_name(mcx, funcid) {
        Ok(Some(n)) => n.as_str().to_string(),
        _ => funcid.to_string(),
    };
    err.add_context_line(format!("SQL function \"{name}\" during inlining"));
    Box::new(err)
}

pub fn expand_function_arguments<'mcx>(
    mcx: Mcx<'mcx>,
    args: &NodeList<'mcx>,
    include_out_arguments: bool,
    result_type: Oid,
    funcid: Oid,
) -> PgResult<NodeList<'mcx>> {
    match expand_function_arguments_opt(mcx, args, include_out_arguments, result_type, funcid)? {
        Some(l) => Ok(l),
        None => args.clone_in(mcx),
    }
}

// None = unchanged (C returns the input list untouched in that case).
fn expand_function_arguments_opt<'mcx>(
    mcx: Mcx<'mcx>,
    args: &NodeList<'mcx>,
    include_out_arguments: bool,
    result_type: Oid,
    funcid: Oid,
) -> PgResult<Option<NodeList<'mcx>>> {
    let shape = syscache_seams::lookup_pg_proc_shape::call(funcid)?
        .ok_or_else(|| func_lookup_failed(funcid))?;
    let has_named_args = args.iter().any(|a| a.node_tag() == NodeTag::T_NamedArgExpr);
    // Fast path mirrors C's fall-through: no catalog array reads when there
    // is nothing to expand.
    if !include_out_arguments && !has_named_args && args.len() >= shape.pronargs as usize {
        return Ok(None);
    }

    let (_, proargtypes) = syscache_seams::lookup_pg_proc_signature::call(mcx, funcid)?
        .ok_or_else(|| func_lookup_failed(funcid))?;
    let mut pronargs = shape.pronargs as usize;
    let mut proargtypes = proargtypes;
    if include_out_arguments {
        let arrays = syscache_seams::pg_proc_result_arrays::call(mcx, funcid)?
            .ok_or_else(|| func_lookup_failed(funcid))?;
        if let Some(all) = arrays.proallargtypes {
            debug_assert!(all.len() >= pronargs);
            pronargs = all.len();
            proargtypes = all;
        }
    }

    if has_named_args {
        let args = reorder_function_arguments(mcx, args, pronargs, funcid)?;
        Ok(Some(recheck_cast_function_args(mcx, args, result_type, proargtypes.as_slice())?))
    } else if args.len() < pronargs {
        let args = add_function_defaults(mcx, args, pronargs, funcid)?;
        Ok(Some(recheck_cast_function_args(mcx, args, result_type, proargtypes.as_slice())?))
    } else {
        Ok(None)
    }
}

fn reorder_function_arguments<'mcx>(
    mcx: Mcx<'mcx>,
    args: &NodeList<'mcx>,
    pronargs: usize,
    funcid: Oid,
) -> PgResult<NodeList<'mcx>> {
    let nargsprovided = args.len();
    debug_assert!(nargsprovided <= pronargs);
    if pronargs > FUNC_MAX_ARGS {
        return Err(Box::new(PgError::error("too many function arguments".to_string())));
    }
    let mut argarray: mcx::PgVec<'mcx, Option<Node<'mcx>>> =
        mcx::vec_with_capacity_in(mcx, pronargs)?;
    for _ in 0..pronargs {
        argarray.push(None);
    }
    let mut i = 0;
    for arg in args {
        match arg.as_variant::<types_nodes::primnodes::NamedArgExpr>() {
            None => {
                debug_assert!(argarray[i].is_none());
                argarray[i] = Some(arg);
                i += 1;
            }
            Some(na) => {
                let n = na.argnumber as usize;
                debug_assert!(na.argnumber >= 0 && n < pronargs);
                debug_assert!(argarray[n].is_none());
                argarray[n] = na.arg;
            }
        }
    }
    if nargsprovided < pronargs {
        let defaults = fetch_function_defaults(mcx, funcid)?;
        let mut i = pronargs - defaults.len();
        for d in defaults.iter() {
            if argarray[i].is_none() {
                argarray[i] = Some(d);
            }
            i += 1;
        }
    }
    let mut out = NodeList::nil();
    for slot in argarray.iter() {
        out.lappend(mcx, slot.expect("reorder_function_arguments: unfilled argument slot"))?;
    }
    Ok(out)
}

fn add_function_defaults<'mcx>(
    mcx: Mcx<'mcx>,
    args: &NodeList<'mcx>,
    pronargs: usize,
    funcid: Oid,
) -> PgResult<NodeList<'mcx>> {
    let defaults = fetch_function_defaults(mcx, funcid)?;
    let ndelete = (args.len() + defaults.len()).checked_sub(pronargs).ok_or_else(|| {
        Box::new(PgError::error("not enough default arguments".to_string()))
    })?;
    let mut out = NodeList::nil();
    for a in args {
        out.lappend(mcx, a)?;
    }
    for (i, d) in defaults.iter().enumerate() {
        if i >= ndelete {
            out.lappend(mcx, d)?;
        }
    }
    Ok(out)
}

fn fetch_function_defaults<'mcx>(mcx: Mcx<'mcx>, funcid: Oid) -> PgResult<NodeList<'mcx>> {
    let src = syscache_seams::pg_proc_proargdefaults::call(mcx, funcid)?
        .ok_or_else(|| func_lookup_failed(funcid))?
        .unwrap_or_else(|| panic!("proargdefaults is null for function {funcid}"));
    let node = readfuncs::stringToNode(mcx, src.as_str())?;
    let Some(list) = node.as_list() else {
        panic!("proargdefaults of {funcid} is not a List");
    };
    list.clone_in(mcx)
}

fn recheck_cast_function_args<'mcx>(
    mcx: Mcx<'mcx>,
    args: NodeList<'mcx>,
    result_type: Oid,
    proargtypes: &[Oid],
) -> PgResult<NodeList<'mcx>> {
    if args.len() > FUNC_MAX_ARGS {
        return Err(Box::new(PgError::error("too many function arguments".to_string())));
    }
    let mut actual_arg_types: mcx::PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, args.len())?;
    for a in &args {
        actual_arg_types.push(nodes_core::node_funcs::expr_type(a));
    }
    debug_assert_eq!(proargtypes.len(), args.len());
    let mut declared_arg_types: mcx::PgVec<'mcx, Oid> =
        mcx::vec_with_capacity_in(mcx, args.len())?;
    for &t in proargtypes {
        declared_arg_types.push(t);
    }
    let rettype = coerce::enforce_generic_type_consistency(
        actual_arg_types.as_slice(),
        declared_arg_types.as_mut_slice(),
        result_type,
        false,
    )?;
    if result_type != rettype {
        return Err(Box::new(PgError::error(
            "function's resolved result type changed during planning".to_string(),
        )));
    }
    clauses_seams::make_fn_arguments_nullstate::call(
        mcx,
        &args,
        actual_arg_types.as_slice(),
        declared_arg_types.as_slice(),
    )
}

// ece_function_is_safe (clauses.c).
fn ece_function_is_safe(cx: &EceContext<'_>, funcid: Oid) -> PgResult<bool> {
    let shape = syscache_seams::lookup_pg_proc_shape::call(funcid)?
        .ok_or_else(|| func_lookup_failed(funcid))?;
    Ok(shape.provolatile == PROVOLATILE_IMMUTABLE
        || (cx.estimate && shape.provolatile == PROVOLATILE_STABLE))
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
pub fn negate_clause<'mcx>(mcx: Mcx<'mcx>, node: Node<'mcx>) -> PgResult<Node<'mcx>> {
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
        NodeTag::T_ScalarArrayOpExpr => {
            use types_nodes::primnodes::ScalarArrayOpExpr;
            let sa = node.as_scalar_array_op_expr().unwrap();
            let negator = lsyscache::get_negator(sa.opno)?;
            if negator != 0 {
                return Node::mk(
                    mcx,
                    ScalarArrayOpExpr {
                        opno: negator,
                        // Same eager get_opcode as the OpExpr arm above (no
                        // lazy set_sa_opfuncid memo on sealed shared nodes).
                        opfuncid: lsyscache::get_opcode(negator)?,
                        hashfuncid: 0,
                        negfuncid: 0,
                        useOr: !sa.useOr,
                        inputcollid: sa.inputcollid,
                        args: sa.args.clone_in(mcx)?,
                        location: sa.location,
                    },
                );
            }
            crate::classify::make_notclause(mcx, node)
        }
        NodeTag::T_BooleanTest => {
            use types_nodes::{BoolTestType, BooleanTest};
            let bt = node.as_boolean_test().unwrap();
            Node::mk(
                mcx,
                BooleanTest {
                    arg: bt.arg,
                    booltesttype: match bt.booltesttype {
                        BoolTestType::IS_TRUE => BoolTestType::IS_NOT_TRUE,
                        BoolTestType::IS_NOT_TRUE => BoolTestType::IS_TRUE,
                        BoolTestType::IS_FALSE => BoolTestType::IS_NOT_FALSE,
                        BoolTestType::IS_NOT_FALSE => BoolTestType::IS_FALSE,
                        BoolTestType::IS_UNKNOWN => BoolTestType::IS_NOT_UNKNOWN,
                        BoolTestType::IS_NOT_UNKNOWN => BoolTestType::IS_UNKNOWN,
                    },
                    location: bt.location,
                },
            )
        }
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
        NodeTag::T_CoerceToDomain => node.as_coerce_to_domain().unwrap().resulttype,
        NodeTag::T_CoerceToDomainValue => node.as_coerce_to_domain_value().unwrap().typeId,
        _ => nodes_core::expr_type(node),
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

pub use nodes_core::node_funcs::apply_relabel_type;

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

// rowtype_field_matches (clauses.c): whole-row rowtypes only, never RECORD.
fn rowtype_field_matches(
    mcx: Mcx<'_>,
    rowtypeid: Oid,
    fieldnum: i32,
    expectedtype: Oid,
    expectedtypmod: i32,
    expectedcollation: Oid,
) -> PgResult<bool> {
    if rowtypeid == types_core::catalog::RECORDOID {
        return Ok(true);
    }
    // C uses lookup_rowtype_tupdesc_domain: a whole-row Var can be a domain
    // over composite.
    let tupdesc = typcache_seams::lookup_rowtype_tupdesc_copy::call(
        mcx,
        lsyscache::getBaseType(rowtypeid)?,
        -1,
    )?;
    if fieldnum <= 0 || fieldnum > tupdesc.natts {
        return Ok(false);
    }
    let attr = tupdesc.attr(fieldnum as usize - 1);
    Ok(!attr.attisdropped
        && attr.atttypid == expectedtype
        && attr.atttypmod == expectedtypmod
        && attr.attcollation == expectedcollation)
}
