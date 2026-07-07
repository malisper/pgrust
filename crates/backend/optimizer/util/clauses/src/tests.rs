extern crate std;

use std::sync::Once;

use datum::Datum;
use mcx::{Mcx, MemoryContext};
use syscache_seams::PgProcShape;
use types_error::PgResult;
use types_nodes::parsenodes::Query;
use types_nodes::primnodes::{FromExpr, FuncExpr, NullIfExpr, OpExpr, ParamKind};
use types_nodes::{Node, NodeList, NodeTag};
use types_tuple::PgTypeShape;

use crate::classify::*;
use crate::fold::{all_arguments_const, eval_const_expressions};
use crate::walker::{expression_tree_mutator, expression_tree_walker, NodeWalker};

const F_INT4PL: u32 = 177;
const F_BOOLEQ: u32 = 60;
const F_INT4EQ: u32 = 65;
const F_FAKE_VOLATILE: u32 = 9990;
const F_FAKE_RESTRICTED: u32 = 9991;
const F_TEXTEQ_GATED: u32 = 67;
const F_FAKE_NONSTRICT: u32 = 9994;
const OP_FAKE_EQ: u32 = 9901;
const OP_FAKE_NE: u32 = 9902;
const F_TEXTREGEXEQ_SUPPORT: u32 = 1364;
const F_FAKE_SQL_INLINE: u32 = 9995;
const F_FAKE_SQL_REC: u32 = 9996;

fn shape(provolatile: u8, proparallel: u8, strict: bool, rettype: u32) -> PgProcShape {
    PgProcShape {
        prolang: 12,
        prosecdef: false,
        proconfig_isnull: true,
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
                F_INT4EQ => Some(shape(b'i', b's', true, 16)),
                F_NEXTVAL => Some(shape(b'v', b'u', true, 20)),
                F_FAKE_VOLATILE => Some(shape(b'v', b's', true, 23)),
                F_FAKE_RESTRICTED => Some(shape(b'i', b'r', true, 23)),
                F_TEXTEQ_GATED => {
                    let mut sh = shape(b'i', b's', true, 16);
                    sh.prosupport = F_TEXTREGEXEQ_SUPPORT;
                    Some(sh)
                }
                F_FAKE_NONSTRICT => Some(shape(b'i', b's', false, 16)),
                F_FAKE_SQL_INLINE | F_FAKE_SQL_REC => {
                    let mut sh = shape(b'v', b'u', false, 23);
                    sh.prolang = 14;
                    sh.pronargs = 1;
                    Some(sh)
                }
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
        syscache_seams::lookup_pg_operator_shape::set(|opno| {
            let op = |negate: u32| syscache_seams::PgOperatorShape { oprnamespace: 11,
                oprleft: 25,
                oprright: 25,
                oprresult: 16,
                oprcom: opno,
                oprnegate: negate,
                oprcode: F_BOOLEQ,
                oprrest: 0,
                oprjoin: 0,
                oprcanmerge: false,
                oprcanhash: false,
            };
            Ok(match opno {
                OP_FAKE_EQ => Some(op(OP_FAKE_NE)),
                OP_FAKE_NE => Some(op(OP_FAKE_EQ)),
                _ => None,
            })
        });
        var_seams::contain_var_clause::set(fixture_contain_var_clause);
        miscinit_seams::get_user_id::set(|| 10);
        aclchk_seams::object_aclcheck::set(|_, _, _, _| Ok(0));
        clauses_seams::inline_sql_function::set(|mcx, funcid, _, _, _, args| {
            Ok(match funcid {
                F_FAKE_SQL_INLINE => {
                    Some(op_expr(mcx, 551, F_INT4PL, 23, &[args.nth(0), int4_const(mcx, Some(1))]))
                }
                F_FAKE_SQL_REC => Some(func_expr(mcx, F_FAKE_SQL_REC, &[args.nth(0)])),
                _ => None,
            })
        });
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
    assert!(is_parallel_safe(PROPARALLEL_SAFE, true, Vec::new(), restricted).unwrap());
    assert!(!is_parallel_safe(PROPARALLEL_RESTRICTED, false, Vec::new(), restricted).unwrap());
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

fn bool_c(mcx: Mcx<'_>, v: Option<bool>) -> Node<'_> {
    let (val, isnull) = match v {
        Some(v) => (Datum::from_bool(v), false),
        None => (Datum::null(), true),
    };
    Node::mk_const(mcx, 16, -1, 0, 1, val, isnull, true).unwrap()
}

fn bool_expr<'mcx>(
    mcx: Mcx<'mcx>,
    op: types_nodes::BoolExprType,
    args: &[Node<'mcx>],
) -> Node<'mcx> {
    Node::mk(
        mcx,
        types_nodes::BoolExpr {
            boolop: op,
            args: NodeList::from_slice(mcx, args).unwrap(),
            location: -1,
        },
    )
    .unwrap()
}

#[test]
fn eval_const_bool_and_or_not() {
    use types_nodes::BoolExprType::*;
    let ctx = cx();
    let mcx = ctx.mcx();

    // true AND false -> false (forceFalse).
    let e = bool_expr(mcx, AND_EXPR, &[bool_c(mcx, Some(true)), bool_c(mcx, Some(false))]);
    let c = eval_const_expressions(mcx, e).unwrap().as_const().unwrap().clone();
    assert!(!c.constvalue.as_bool() && !c.constisnull && c.consttype == 16);

    // true OR false -> true (forceTrue).
    let e = bool_expr(mcx, OR_EXPR, &[bool_c(mcx, Some(true)), bool_c(mcx, Some(false))]);
    let c = eval_const_expressions(mcx, e).unwrap().as_const().unwrap().clone();
    assert!(c.constvalue.as_bool() && !c.constisnull);

    // NOT true -> false via negate_clause.
    let e = bool_expr(mcx, NOT_EXPR, &[bool_c(mcx, Some(true))]);
    let c = eval_const_expressions(mcx, e).unwrap().as_const().unwrap().clone();
    assert!(!c.constvalue.as_bool() && !c.constisnull);

    // NULL AND true -> NULL bool (haveNull, all non-forcing consts dropped).
    let e = bool_expr(mcx, AND_EXPR, &[bool_c(mcx, None), bool_c(mcx, Some(true))]);
    let c = eval_const_expressions(mcx, e).unwrap().as_const().unwrap().clone();
    assert!(c.constisnull && c.consttype == 16);

    // Nested OR flattens: (x OR false) OR false -> x.
    let var = Node::mk_var(mcx, 1, 1, 16, -1, 0, 0).unwrap();
    let inner = bool_expr(mcx, OR_EXPR, &[var, bool_c(mcx, Some(false))]);
    let e = bool_expr(mcx, OR_EXPR, &[inner, bool_c(mcx, Some(false))]);
    let out = eval_const_expressions(mcx, e).unwrap();
    assert!(out.as_var().is_some());

    // x AND false -> false even with a non-const input.
    let var = Node::mk_var(mcx, 1, 1, 16, -1, 0, 0).unwrap();
    let e = bool_expr(mcx, AND_EXPR, &[var, bool_c(mcx, Some(false))]);
    let c = eval_const_expressions(mcx, e).unwrap().as_const().unwrap().clone();
    assert!(!c.constvalue.as_bool() && !c.constisnull);
}

fn case_when<'mcx>(mcx: Mcx<'mcx>, cond: Node<'mcx>, result: Node<'mcx>) -> Node<'mcx> {
    Node::mk(
        mcx,
        types_nodes::primnodes::CaseWhen { expr: Some(cond), result: Some(result), location: -1 },
    )
    .unwrap()
}

fn case_expr<'mcx>(
    mcx: Mcx<'mcx>,
    arg: Option<Node<'mcx>>,
    whens: &[Node<'mcx>],
    defresult: Node<'mcx>,
) -> Node<'mcx> {
    Node::mk(
        mcx,
        types_nodes::primnodes::CaseExpr {
            casetype: 23,
            casecollid: 0,
            arg,
            args: NodeList::from_slice(mcx, whens).unwrap(),
            defresult: Some(defresult),
            location: -1,
        },
    )
    .unwrap()
}

#[test]
fn eval_const_case_expr() {
    let ctx = cx();
    let mcx = ctx.mcx();

    // CASE WHEN true THEN 1 ELSE 2 END -> 1 (const TRUE prunes to result).
    let e = case_expr(
        mcx,
        None,
        &[case_when(mcx, bool_c(mcx, Some(true)), int4_const(mcx, Some(1)))],
        int4_const(mcx, Some(2)),
    );
    let c = eval_const_expressions(mcx, e).unwrap().as_const().unwrap().clone();
    assert_eq!(c.constvalue.as_i32(), 1);

    // CASE WHEN false THEN 1 ELSE 2 END -> 2 (all-FALSE reduces to ELSE).
    let e = case_expr(
        mcx,
        None,
        &[case_when(mcx, bool_c(mcx, Some(false)), int4_const(mcx, Some(1)))],
        int4_const(mcx, Some(2)),
    );
    let c = eval_const_expressions(mcx, e).unwrap().as_const().unwrap().clone();
    assert_eq!(c.constvalue.as_i32(), 2);

    // NULL condition drops the alternative, like FALSE.
    let e = case_expr(
        mcx,
        None,
        &[case_when(mcx, bool_c(mcx, None), int4_const(mcx, Some(1)))],
        int4_const(mcx, Some(2)),
    );
    let c = eval_const_expressions(mcx, e).unwrap().as_const().unwrap().clone();
    assert_eq!(c.constvalue.as_i32(), 2);

    // Non-const condition: WHEN kept, FALSE sibling dropped, TRUE tail folds
    // into the new default.
    let var = Node::mk_var(mcx, 1, 1, 16, -1, 0, 0).unwrap();
    let e = case_expr(
        mcx,
        None,
        &[
            case_when(mcx, bool_c(mcx, Some(false)), int4_const(mcx, Some(1))),
            case_when(mcx, var, int4_const(mcx, Some(2))),
            case_when(mcx, bool_c(mcx, Some(true)), int4_const(mcx, Some(3))),
        ],
        int4_const(mcx, Some(4)),
    );
    let out = eval_const_expressions(mcx, e).unwrap();
    let ce = out.as_case_expr().expect("still a CaseExpr");
    assert_eq!(ce.args.len(), 1);
    let w = ce.args.nth(0).as_case_when().unwrap();
    assert!(w.expr.unwrap().as_var().is_some());
    assert_eq!(ce.defresult.unwrap().as_const().unwrap().constvalue.as_i32(), 3);
}

#[test]
fn eval_const_case_arg_form() {
    let ctx = cx();
    let mcx = ctx.mcx();
    // bool-typed placeholder used directly as the WHEN condition: proves the
    // case_val substitution without needing the evaluate_expr seam (the
    // parser's int4eq shape folds e2e — case-arg-e2e.sh EXPLAIN checks).
    let case_test = || {
        Node::mk(
            mcx,
            types_nodes::primnodes::CaseTestExpr { typeId: 16, typeMod: -1, collation: 0 },
        )
        .unwrap()
    };

    // CASE true WHEN <testval> THEN 1 ELSE 2 END -> 1.
    let e = case_expr(
        mcx,
        Some(bool_c(mcx, Some(true))),
        &[case_when(mcx, case_test(), int4_const(mcx, Some(1)))],
        int4_const(mcx, Some(2)),
    );
    let c = eval_const_expressions(mcx, e).unwrap().as_const().unwrap().clone();
    assert_eq!(c.constvalue.as_i32(), 1);

    // NULL const arg substitutes NULL: condition drops, ELSE remains.
    let e = case_expr(
        mcx,
        Some(bool_c(mcx, None)),
        &[case_when(mcx, case_test(), int4_const(mcx, Some(1)))],
        int4_const(mcx, Some(2)),
    );
    let c = eval_const_expressions(mcx, e).unwrap().as_const().unwrap().clone();
    assert_eq!(c.constvalue.as_i32(), 2);

    // Non-const arg: no substitution — arg kept, placeholder untouched.
    let var = Node::mk_var(mcx, 1, 1, 16, -1, 0, 0).unwrap();
    let e = case_expr(
        mcx,
        Some(var),
        &[case_when(mcx, case_test(), int4_const(mcx, Some(1)))],
        int4_const(mcx, Some(2)),
    );
    let out = eval_const_expressions(mcx, e).unwrap();
    let ce = out.as_case_expr().expect("still a CaseExpr");
    assert!(ce.arg.unwrap().as_var().is_some());
    assert_eq!(
        ce.args.nth(0).as_case_when().unwrap().expr.unwrap().node_tag(),
        NodeTag::T_CaseTestExpr
    );
}

#[test]
fn eval_const_coalesce() {
    let ctx = cx();
    let mcx = ctx.mcx();

    fn coalesce<'mcx>(mcx: Mcx<'mcx>, args: &[Node<'mcx>]) -> Node<'mcx> {
        Node::mk(
            mcx,
            types_nodes::primnodes::CoalesceExpr {
                coalescetype: 23,
                coalescecollid: 0,
                args: NodeList::from_slice(mcx, args).unwrap(),
                location: -1,
            },
        )
        .unwrap()
    }

    // COALESCE(NULL, 42) -> 42.
    let e = coalesce(mcx, &[int4_const(mcx, None), int4_const(mcx, Some(42))]);
    let c = eval_const_expressions(mcx, e).unwrap().as_const().unwrap().clone();
    assert_eq!(c.constvalue.as_i32(), 42);
    assert!(!c.constisnull);

    // All-null -> typed NULL Const.
    let e = coalesce(mcx, &[int4_const(mcx, None), int4_const(mcx, None)]);
    let c = eval_const_expressions(mcx, e).unwrap().as_const().unwrap().clone();
    assert!(c.constisnull && c.consttype == 23);

    // Var head: null dropped, first following non-null const kept, tail cut.
    let var = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
    let e = coalesce(
        mcx,
        &[var, int4_const(mcx, None), int4_const(mcx, Some(7)), int4_const(mcx, Some(8))],
    );
    let out = eval_const_expressions(mcx, e).unwrap();
    let co = out.as_coalesce_expr().expect("still a CoalesceExpr");
    assert_eq!(co.args.len(), 2);
    assert!(co.args.nth(0).as_var().is_some());
    assert_eq!(co.args.nth(1).as_const().unwrap().constvalue.as_i32(), 7);
}

fn minmax<'mcx>(mcx: Mcx<'mcx>, least: bool, args: &[Node<'mcx>]) -> Node<'mcx> {
    use types_nodes::primnodes::{MinMaxExpr, MinMaxOp};
    Node::mk(
        mcx,
        MinMaxExpr {
            minmaxtype: 23,
            minmaxcollid: 0,
            inputcollid: 0,
            op: if least { MinMaxOp::IS_LEAST } else { MinMaxOp::IS_GREATEST },
            args: NodeList::from_slice(mcx, args).unwrap(),
            location: -1,
        },
    )
    .unwrap()
}

#[test]
fn eval_const_minmax_nonconst_keeps_node() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let var = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
    let e = minmax(mcx, false, &[var, int4_const(mcx, Some(2))]);
    let out = eval_const_expressions(mcx, e).unwrap();
    assert!(out.as_min_max_expr().is_some());
}

#[test]
#[should_panic(expected = "seam not installed")]
fn eval_const_minmax_all_const_defers_to_evaluate_expr_seam() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let e = minmax(mcx, true, &[int4_const(mcx, Some(1)), int4_const(mcx, Some(2))]);
    let _ = eval_const_expressions(mcx, e);
}

fn saop<'mcx>(
    mcx: Mcx<'mcx>,
    opno: u32,
    opfuncid: u32,
    use_or: bool,
    args: &[Node<'mcx>],
) -> Node<'mcx> {
    Node::mk(
        mcx,
        types_nodes::ScalarArrayOpExpr {
            opno,
            opfuncid,
            useOr: use_or,
            args: NodeList::from_slice(mcx, args).unwrap(),
            location: -1,
            ..Default::default()
        },
    )
    .unwrap()
}

#[test]
fn relabel_over_const_folds_to_retyped_const() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let con = int4_const(mcx, Some(5));
    let relabel = Node::mk_relabel_type(
        mcx,
        con,
        26,
        -1,
        0,
        types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
    )
    .unwrap();
    let out = eval_const_expressions(mcx, relabel).unwrap();
    let c = out.as_const().unwrap();
    assert_eq!((c.consttype, c.constvalue.as_i32(), c.constisnull), (26, 5, false));
}

#[test]
fn negate_clause_flips_saop_through_negator() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let var = Node::mk_var(mcx, 1, 1, 25, -1, 100, 0).unwrap();
    let param = Node::mk(
        mcx,
        types_nodes::Param {
            paramkind: ParamKind::PARAM_EXTERN,
            paramid: 1,
            paramtype: 1009,
            paramtypmod: -1,
            paramcollid: 0,
            location: -1,
        },
    )
    .unwrap();
    let inner = saop(mcx, OP_FAKE_EQ, F_BOOLEQ, true, &[var, param]);
    let not = Node::mk(
        mcx,
        types_nodes::BoolExpr {
            boolop: types_nodes::BoolExprType::NOT_EXPR,
            args: NodeList::from_slice(mcx, &[inner]).unwrap(),
            location: -1,
        },
    )
    .unwrap();
    let out = eval_const_expressions(mcx, not).unwrap();
    let s = out.as_scalar_array_op_expr().unwrap();
    // Unlike C, negate_clause resolves opfuncid eagerly (no lazy memo).
    assert_eq!((s.opno, s.opfuncid, s.useOr), (OP_FAKE_NE, F_BOOLEQ, false));
}

#[test]
fn prosupport_null_simplify_allowlist_stays_quiet() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let var = Node::mk_var(mcx, 1, 1, 25, -1, 100, 0).unwrap();
    let var2 = Node::mk_var(mcx, 1, 2, 25, -1, 100, 0).unwrap();
    let call = Node::mk(
        mcx,
        FuncExpr {
            funcid: F_TEXTEQ_GATED,
            funcresulttype: 16,
            args: NodeList::from_slice(mcx, &[var, var2]).unwrap(),
            ..Default::default()
        },
    )
    .unwrap();
    let out = eval_const_expressions(mcx, call).unwrap();
    assert_eq!(out.as_func_expr().unwrap().funcid, F_TEXTEQ_GATED);
}

#[test]
fn find_nonnullable_rels_uses_strict_saop() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let var = Node::mk_var(mcx, 3, 1, 25, -1, 100, 0).unwrap();
    let param = Node::mk(
        mcx,
        types_nodes::Param {
            paramkind: ParamKind::PARAM_EXTERN,
            paramid: 1,
            paramtype: 1009,
            paramtypmod: -1,
            paramcollid: 0,
            location: -1,
        },
    )
    .unwrap();
    let strict = saop(mcx, OP_FAKE_EQ, F_BOOLEQ, true, &[var, param]);
    let rels = find_nonnullable_rels(mcx, Some(strict)).unwrap();
    assert!(rels.is_member(3));

    let lax = saop(mcx, OP_FAKE_EQ, F_FAKE_NONSTRICT, true, &[var, param]);
    let rels = find_nonnullable_rels(mcx, Some(lax)).unwrap();
    assert!(rels.is_empty());
}

#[test]
fn inline_replaces_sql_function_call() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let var = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
    let call = func_expr(mcx, F_FAKE_SQL_INLINE, &[var]);
    let out = eval_const_expressions(mcx, call).unwrap();
    let o = out.as_op_expr().expect("inlined to the seam-provided OpExpr");
    assert_eq!(o.opfuncid, F_INT4PL);
    assert!(o.args.nth(0).as_var().is_some());
    assert_eq!(o.args.nth(1).as_const().unwrap().constvalue.as_i32(), 1);
}

#[test]
fn inline_recursion_guard_stops_self_expansion() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let var = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
    let call = func_expr(mcx, F_FAKE_SQL_REC, &[var]);
    let out = eval_const_expressions(mcx, call).unwrap();
    let f = out.as_func_expr().expect("one expansion, inner call kept");
    assert_eq!(f.funcid, F_FAKE_SQL_REC);
}

#[test]
fn mbms_int_members_and_is_member() {
    let ctx = MemoryContext::new_bump("clauses-test");
    let mcx = ctx.mcx();

    let mut a: MultiBitmapset<'_> = mcx::PgVec::new_in(mcx);
    mbms_add_member(mcx, &mut a, 0, 3).unwrap();
    mbms_add_member(mcx, &mut a, 1, 7).unwrap();
    mbms_add_member(mcx, &mut a, 2, 70).unwrap();

    assert!(mbms_is_member(0, 3, &a));
    assert!(mbms_is_member(2, 70, &a));
    assert!(!mbms_is_member(2, 71, &a));
    assert!(!mbms_is_member(3, 0, &a));

    let mut b: MultiBitmapset<'_> = mcx::PgVec::new_in(mcx);
    mbms_add_member(mcx, &mut b, 0, 3).unwrap();
    mbms_add_member(mcx, &mut b, 1, 8).unwrap();

    mbms_int_members(&mut a, &b);
    assert_eq!(a.len(), 2);
    assert!(mbms_is_member(0, 3, &a));
    assert!(!mbms_is_member(1, 7, &a));
    assert!(!mbms_is_member(2, 70, &a));

    let empty: MultiBitmapset<'_> = mcx::PgVec::new_in(mcx);
    mbms_int_members(&mut a, &empty);
    assert!(a.is_empty());
}

#[test]
#[should_panic(expected = "negative multibitmapset member index")]
fn mbms_is_member_negative_is_loud() {
    let ctx = MemoryContext::new_bump("clauses-test");
    let mcx = ctx.mcx();
    let mut a: MultiBitmapset<'_> = mcx::PgVec::new_in(mcx);
    mbms_add_member(mcx, &mut a, 0, 1).unwrap();
    mbms_is_member(0, -1, &a);
}

#[test]
#[should_panic(expected = "negative multibitmapset member index")]
fn mbms_add_member_negative_is_loud() {
    let ctx = MemoryContext::new_bump("clauses-test");
    let mcx = ctx.mcx();
    let mut a: MultiBitmapset<'_> = mcx::PgVec::new_in(mcx);
    let _ = mbms_add_member(mcx, &mut a, -1, 1);
}

fn nullif<'mcx>(mcx: Mcx<'mcx>, args: &[Node<'mcx>]) -> Node<'mcx> {
    Node::mk(
        mcx,
        NullIfExpr {
            opno: 96,
            opfuncid: F_INT4EQ,
            opresulttype: 23,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args: NodeList::from_slice(mcx, args).unwrap(),
            location: -1,
        },
    )
    .unwrap()
}

#[test]
fn eval_const_nullif_null_arg_yields_first_arg() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let var = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
    // NULLIF(x, NULL): NULL compares equal to nothing; reduces to x.
    let e = nullif(mcx, &[var, int4_const(mcx, None)]);
    let folded = eval_const_expressions(mcx, e).unwrap();
    let v = folded.as_var().expect("NULLIF(x, NULL) reduces to x");
    assert_eq!(v.varno, 1);
    // NULLIF(NULL, x): reduces to the first (null) argument.
    let e = nullif(mcx, &[int4_const(mcx, None), var]);
    let folded = eval_const_expressions(mcx, e).unwrap();
    let c = folded.as_const().expect("NULLIF(NULL, x) reduces to the null const");
    assert!(c.constisnull);
    assert_eq!(c.consttype, 23);
}

#[test]
#[should_panic(expected = "seam not installed")]
fn eval_const_nullif_all_const_defers_to_evaluate_expr_seam() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let e = nullif(mcx, &[int4_const(mcx, Some(1)), int4_const(mcx, Some(2))]);
    let _ = eval_const_expressions(mcx, e);
}

#[test]
fn eval_const_nullif_nonconst_keeps_node() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let var = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
    let e = nullif(mcx, &[var, int4_const(mcx, Some(1))]);
    let out = eval_const_expressions(mcx, e).unwrap();
    let n = out.as_null_if_expr().expect("stays a NullIfExpr");
    assert_eq!(n.opno, 96);
    assert!(n.args.nth(0).as_var().is_some());
}
