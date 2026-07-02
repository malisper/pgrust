extern crate std;

use std::sync::Once;

use datum::Datum;
use mcx::{Mcx, MemoryContext};
use syscache_seams::PgProcShape;
use types_error::PgResult;
use types_nodes::parsenodes::Query;
use types_nodes::primnodes::{FromExpr, FuncExpr, OpExpr, ParamKind};
use types_nodes::{Node, NodeList, NodeTag};
use types_tuple::PgTypeShape;

use crate::classify::*;
use crate::fold::{all_arguments_const, eval_const_expressions};
use crate::walker::{expression_tree_mutator, expression_tree_walker, NodeWalker};

const F_INT4PL: u32 = 177;
const F_BOOLEQ: u32 = 60;
const F_FAKE_VOLATILE: u32 = 9990;
const F_FAKE_RESTRICTED: u32 = 9991;

fn shape(provolatile: u8, proparallel: u8, strict: bool, rettype: u32) -> PgProcShape {
    PgProcShape {
        pronamespace: 11,
        prorettype: rettype,
        provariadic: 0,
        prosupport: 0,
        pronargs: 2,
        prokind: b'f' as i8,
        provolatile: provolatile as i8,
        proparallel: proparallel as i8,
        proretset: false,
        proisstrict: strict,
        proleakproof: false,
    }
}

fn install_fixtures() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        syscache_seams::lookup_pg_proc_shape::set(|funcid| {
            Ok(match funcid {
                F_INT4PL => Some(shape(b'i', b's', true, 23)),
                F_BOOLEQ => Some(shape(b'i', b's', true, 16)),
                F_NEXTVAL => Some(shape(b'v', b'u', true, 20)),
                F_FAKE_VOLATILE => Some(shape(b'v', b's', true, 23)),
                F_FAKE_RESTRICTED => Some(shape(b'i', b'r', true, 23)),
                _ => None,
            })
        });
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok(match typid {
                23 => Some(PgTypeShape {
                    typlen: 4,
                    typbyval: true,
                    typalign: b'i' as i8,
                    typstorage: b'p' as i8,
                    typcollation: 0,
                }),
                _ => None,
            })
        });
        var_seams::contain_var_clause::set(fixture_contain_var_clause);
    });
}

fn fixture_contain_var_clause(node: Node<'_>) -> bool {
    struct V;
    impl<'mcx> NodeWalker<'mcx> for V {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if node.node_tag() == NodeTag::T_Var {
                return Ok(true);
            }
            expression_tree_walker(node, self)
        }
    }
    V.visit(node).unwrap()
}

fn cx() -> MemoryContext {
    install_fixtures();
    MemoryContext::new_bump("clauses-test")
}

fn int4_const(mcx: Mcx<'_>, v: Option<i32>) -> Node<'_> {
    let (val, isnull) = match v {
        Some(v) => (Datum::from_i32(v), false),
        None => (Datum::null(), true),
    };
    Node::mk_const(mcx, 23, -1, 0, 4, val, isnull, true).unwrap()
}

fn op_expr<'mcx>(
    mcx: Mcx<'mcx>,
    opno: u32,
    opfuncid: u32,
    rettype: u32,
    args: &[Node<'mcx>],
) -> Node<'mcx> {
    Node::mk(
        mcx,
        OpExpr {
            opno,
            opfuncid,
            opresulttype: rettype,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args: NodeList::from_slice(mcx, args).unwrap(),
            location: -1,
        },
    )
    .unwrap()
}

fn func_expr<'mcx>(mcx: Mcx<'mcx>, funcid: u32, args: &[Node<'mcx>]) -> Node<'mcx> {
    Node::mk(
        mcx,
        FuncExpr {
            funcid,
            funcresulttype: 23,
            args: NodeList::from_slice(mcx, args).unwrap(),
            ..Default::default()
        },
    )
    .unwrap()
}

#[test]
fn mutable_and_volatile_classification() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let var = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
    let plus = op_expr(mcx, 551, F_INT4PL, 23, &[var, int4_const(mcx, Some(1))]);
    let te = Node::mk_target_entry(mcx, plus, 1, None, false).unwrap();
    assert!(!contain_mutable_functions(te).unwrap());
    assert!(!contain_volatile_functions(te).unwrap());

    let vol = func_expr(mcx, F_FAKE_VOLATILE, &[var]);
    assert!(contain_mutable_functions(vol).unwrap());
    assert!(contain_volatile_functions(vol).unwrap());

    let nextval = func_expr(mcx, F_NEXTVAL, &[int4_const(mcx, Some(1))]);
    assert!(contain_volatile_functions(nextval).unwrap());
    assert!(!contain_volatile_functions_not_nextval(nextval).unwrap());
}

#[test]
fn exec_params_and_paramids() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let p7 = Node::mk(
        mcx,
        types_nodes::primnodes::Param {
            paramkind: ParamKind::PARAM_EXEC,
            paramid: 7,
            paramtype: 23,
            ..Default::default()
        },
    )
    .unwrap();
    let p3 = Node::mk(
        mcx,
        types_nodes::primnodes::Param {
            paramkind: ParamKind::PARAM_EXTERN,
            paramid: 3,
            paramtype: 23,
            ..Default::default()
        },
    )
    .unwrap();
    let expr = op_expr(mcx, 551, F_INT4PL, 23, &[p7, p3]);
    assert!(contain_exec_param(expr, &[7]).unwrap());
    assert!(!contain_exec_param(expr, &[8]).unwrap());
    let ids = pull_paramids(mcx, expr).unwrap();
    assert!(ids.is_member(7) && ids.is_member(3));
    assert_eq!(ids.num_members(), 2);
}

#[test]
fn eval_const_folds_strict_null() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let var = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
    let plus = op_expr(mcx, 551, F_INT4PL, 23, &[int4_const(mcx, None), var]);
    let folded = eval_const_expressions(mcx, plus).unwrap();
    let c = folded.as_const().expect("folded to Const");
    assert!(c.constisnull);
    assert_eq!(c.consttype, 23);
    assert_eq!(c.constlen, 4);
    assert!(c.constbyval);
}

#[test]
#[should_panic(expected = "seam not installed")]
fn eval_const_all_const_defers_to_evaluate_expr_seam() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let plus = op_expr(
        mcx,
        551,
        F_INT4PL,
        23,
        &[int4_const(mcx, Some(1)), int4_const(mcx, Some(2))],
    );
    let _ = eval_const_expressions(mcx, plus);
}

#[test]
fn eval_const_boolean_equality() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let var = Node::mk_var(mcx, 1, 1, 16, -1, 0, 0).unwrap();
    let true_const = Node::mk_const(mcx, 16, -1, 0, 1, Datum::from_bool(true), false, true).unwrap();
    let eq = op_expr(mcx, 91, F_BOOLEQ, 16, &[var, true_const]);
    let folded = eval_const_expressions(mcx, eq).unwrap();
    let v = folded.as_var().expect("x = true reduces to x");
    assert_eq!(v.varno, 1);
}

#[test]
fn eval_const_identity_when_unchanged() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let var = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
    let plus = op_expr(mcx, 551, F_INT4PL, 23, &[var, int4_const(mcx, Some(1))]);
    let out = eval_const_expressions(mcx, plus).unwrap();
    let o = out.as_op_expr().unwrap();
    assert_eq!(o.opno, 551);
    assert!(o.args.nth(0).as_var().is_some());
}

#[test]
fn mutator_identity_shares_input() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let var = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
    let te = Node::mk_target_entry(mcx, var, 1, None, false).unwrap();
    let out = expression_tree_mutator(mcx, te, &mut |_| Ok(None)).unwrap();
    assert!(out.is_none());
}

#[test]
fn all_arguments_const_checks_children_only() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let consts = op_expr(
        mcx,
        551,
        F_INT4PL,
        23,
        &[int4_const(mcx, Some(1)), int4_const(mcx, Some(2))],
    );
    assert!(all_arguments_const(consts).unwrap());
    let var = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
    let mixed = op_expr(mcx, 551, F_INT4PL, 23, &[var, int4_const(mcx, Some(2))]);
    assert!(!all_arguments_const(mixed).unwrap());
}

#[test]
#[should_panic(expected = "deferred")]
fn unported_vocab_walks_loud() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let il = Node::mk_int_list(mcx, types_nodes::IntList::make1(mcx, 1).unwrap()).unwrap();
    struct Nop;
    impl<'mcx> NodeWalker<'mcx> for Nop {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            expression_tree_walker(node, self)
        }
    }
    let _ = expression_tree_walker(il, &mut Nop);
}

#[test]
fn parallel_hazard_over_query() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let var = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
    let safe_fn = func_expr(mcx, F_INT4PL, &[var, var]);
    let te = Node::mk_target_entry(mcx, safe_fn, 1, None, false).unwrap();
    let jointree = Node::mk_mut(mcx, FromExpr::default()).unwrap().seal_ref();
    let mut q = Query::default();
    q.targetList = NodeList::make1(mcx, te).unwrap();
    q.jointree = Some(jointree);
    assert_eq!(max_parallel_hazard(&q).unwrap(), PROPARALLEL_SAFE);

    let restricted = func_expr(mcx, F_FAKE_RESTRICTED, &[var, var]);
    let te2 = Node::mk_target_entry(mcx, restricted, 1, None, false).unwrap();
    let mut q2 = Query::default();
    q2.targetList = NodeList::make1(mcx, te2).unwrap();
    q2.jointree = Some(jointree);
    assert_eq!(max_parallel_hazard(&q2).unwrap(), PROPARALLEL_RESTRICTED);
    assert!(is_parallel_safe(PROPARALLEL_SAFE, true, &[], restricted).unwrap());
    assert!(!is_parallel_safe(PROPARALLEL_RESTRICTED, false, &[], restricted).unwrap());
}

#[test]
fn pseudo_constant_and_leaked_vars() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let var = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
    let c = int4_const(mcx, Some(5));
    assert!(is_pseudo_constant_clause(c).unwrap());
    assert!(!is_pseudo_constant_clause(var).unwrap());
    assert!(is_pseudo_constant_clause_relids(c, None).unwrap());

    // int4pl is not leakproof in the fixture; a Var arg makes it leaky.
    let leaky = op_expr(mcx, 551, F_INT4PL, 23, &[var, c]);
    assert!(contain_leaked_vars(leaky).unwrap());
    let no_vars = op_expr(mcx, 551, F_INT4PL, 23, &[c, c]);
    assert!(!contain_leaked_vars(no_vars).unwrap());
}

#[test]
fn nonstrict_and_srf_rows() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let var = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
    let strict = op_expr(mcx, 551, F_INT4PL, 23, &[var, var]);
    assert!(!contain_nonstrict_functions(strict).unwrap());
    assert_eq!(expression_returns_set_rows(Some(strict)).unwrap(), 1.0);
    assert_eq!(expression_returns_set_rows(None).unwrap(), 1.0);
    assert!(!contain_agg_clause(strict).unwrap());
    assert!(!contain_window_function(strict).unwrap());
    assert!(!contain_subplans(strict).unwrap());
    assert!(!contain_context_dependent_node(strict).unwrap());
}
