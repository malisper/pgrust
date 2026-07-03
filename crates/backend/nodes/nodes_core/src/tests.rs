use mcx::{Mcx, MemoryContext};
use types_nodes::parsenodes::Query;
use types_nodes::primnodes::{FromExpr, OpExpr, Param, ParamKind, RangeVar};
use types_nodes::rawnodes::{A_Expr_Kind, ResTarget, SelectStmt};
use types_nodes::{Node, NodeList, NodeTag};

use super::*;

fn cx() -> MemoryContext {
    MemoryContext::new_bump("nodes_core-test")
}

struct CountParams {
    analyzed: usize,
    raw: usize,
}

impl<'mcx> NodeWalker<'mcx> for CountParams {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_Param => {
                self.analyzed += 1;
                Ok(false)
            }
            NodeTag::T_ParamRef => {
                self.raw += 1;
                Ok(false)
            }
            _ => raw_expression_tree_walker(node, self),
        }
    }
}

fn extern_param(mcx: Mcx<'_>, id: i32) -> Node<'_> {
    Node::mk(
        mcx,
        Param {
            paramkind: ParamKind::PARAM_EXTERN,
            paramid: id,
            paramtype: 23,
            paramtypmod: -1,
            paramcollid: 0,
            location: -1,
        },
    )
    .unwrap()
}

#[test]
fn expression_walker_reaches_nested_args() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let p = extern_param(mcx, 1);
    let op = Node::mk(
        mcx,
        OpExpr {
            opno: 96,
            opfuncid: 65,
            opresulttype: 16,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args: NodeList::from_slice(mcx, &[p, p]).unwrap(),
            location: -1,
        },
    )
    .unwrap();
    let te = Node::mk_target_entry(mcx, op, 1, None, false).unwrap();

    struct W(usize);
    impl<'mcx> NodeWalker<'mcx> for W {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if node.node_tag() == NodeTag::T_Param {
                self.0 += 1;
                return Ok(false);
            }
            expression_tree_walker(node, self)
        }
    }
    let mut w = W(0);
    assert!(!expression_tree_walker(te, &mut w).unwrap());
    assert_eq!(w.0, 2);
}

#[test]
fn query_walker_covers_targetlist_and_jointree_quals() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let p1 = extern_param(mcx, 1);
    let te = Node::mk_target_entry(mcx, p1, 1, None, false).unwrap();
    let p2 = extern_param(mcx, 2);
    let jointree = Node::mk_mut(
        mcx,
        FromExpr { fromlist: NodeList::nil(), quals: Some(p2) },
    )
    .unwrap()
    .seal_ref();
    let query = Query {
        targetList: NodeList::from_slice(mcx, &[te]).unwrap(),
        jointree: Some(jointree),
        ..Query::default()
    };

    struct W(usize);
    impl<'mcx> NodeWalker<'mcx> for W {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if node.node_tag() == NodeTag::T_Param {
                self.0 += 1;
                return Ok(false);
            }
            expression_tree_walker(node, self)
        }
    }
    let mut w = W(0);
    assert!(!query_tree_walker(&query, &mut w, 0).unwrap());
    assert_eq!(w.0, 2);
}

#[test]
fn raw_walker_descends_select_stmt_and_set_op_args() {
    let ctx = cx();
    let mcx = ctx.mcx();

    fn leaf_select<'mcx>(mcx: Mcx<'mcx>, paramno: i32) -> SelectStmt<'mcx> {
        let col = Node::mk_column_ref(mcx, NodeList::nil(), -1).unwrap();
        let pref = Node::mk_param_ref(mcx, paramno, -1).unwrap();
        let aexpr =
            Node::mk_a_expr(mcx, A_Expr_Kind::AEXPR_OP, NodeList::nil(), Some(col), Some(pref), -1)
                .unwrap();
        let rt = Node::mk_res_target(mcx, None, NodeList::nil(), Some(aexpr), -1).unwrap();
        SelectStmt {
            targetList: NodeList::from_slice(mcx, &[rt]).unwrap(),
            whereClause: Some(Node::mk_param_ref(mcx, paramno + 100, -1).unwrap()),
            ..SelectStmt::default()
        }
    }

    let larg = Node::mk_mut(mcx, leaf_select(mcx, 1)).unwrap().seal_ref();
    let rarg = Node::mk_mut(mcx, leaf_select(mcx, 2)).unwrap().seal_ref();
    let union =
        Node::mk(mcx, SelectStmt { larg: Some(larg), rarg: Some(rarg), ..SelectStmt::default() })
            .unwrap();

    let mut w = CountParams { analyzed: 0, raw: 0 };
    assert!(!raw_expression_tree_walker(union, &mut w).unwrap());
    assert_eq!(w.raw, 4);
    assert_eq!(w.analyzed, 0);
}

#[test]
fn raw_walker_alias_ref_hook_defaults_to_noop() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let alias = Node::mk_mut(
        mcx,
        types_nodes::primnodes::Alias { aliasname: Some("t"), colnames: NodeList::nil() },
    )
    .unwrap()
    .seal_ref();
    let rv = Node::mk(mcx, RangeVar { alias: Some(alias), ..RangeVar::default() }).unwrap();

    let mut w = CountParams { analyzed: 0, raw: 0 };
    assert!(!raw_expression_tree_walker(rv, &mut w).unwrap());

    struct SeesAlias(bool);
    impl<'mcx> NodeWalker<'mcx> for SeesAlias {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            raw_expression_tree_walker(node, self)
        }
        fn visit_alias_ref(&mut self, _a: &'mcx Alias<'mcx>) -> PgResult<bool> {
            self.0 = true;
            Ok(true)
        }
    }
    let mut s = SeesAlias(false);
    assert!(raw_expression_tree_walker(rv, &mut s).unwrap());
    assert!(s.0);
}

#[test]
fn mutator_preserves_identity_when_unchanged() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let p = extern_param(mcx, 1);
    let te = Node::mk_target_entry(mcx, p, 1, None, false).unwrap();
    let out = expression_tree_mutator(mcx, te, &mut |_| Ok(None)).unwrap();
    assert!(out.is_none());
}

#[test]
fn mutator_rebuilds_on_change() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let p = extern_param(mcx, 1);
    let te = Node::mk_target_entry(mcx, p, 1, None, false).unwrap();
    let replacement = extern_param(mcx, 2);
    let out = expression_tree_mutator(mcx, te, &mut |n| {
        Ok((n.node_tag() == NodeTag::T_Param).then_some(replacement))
    })
    .unwrap()
    .expect("changed child rebuilds the TargetEntry");
    let new_te = out.as_target_entry().unwrap();
    assert_eq!(new_te.expr.as_param().unwrap().paramid, 2);
}

#[test]
#[should_panic(expected = "raw_expression_tree_walker")]
fn raw_walker_unported_vocab_is_loud() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let p = extern_param(mcx, 1);
    let te = Node::mk_target_entry(mcx, p, 1, None, false).unwrap();
    let rt = Node::mk(
        mcx,
        ResTarget { name: None, indirection: NodeList::nil(), val: Some(te), location: -1 },
    )
    .unwrap();
    let mut w = CountParams { analyzed: 0, raw: 0 };
    let _ = raw_expression_tree_walker(rt, &mut w);
}

fn text_const(mcx: Mcx<'_>) -> Node<'_> {
    Node::mk_const(mcx, 25, -1, 100, -1, datum::Datum::null(), true, false).unwrap()
}

#[test]
fn apply_relabel_type_retypes_const_in_place() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let con = text_const(mcx);
    let out = node_funcs::apply_relabel_type(
        mcx,
        con,
        19,
        -1,
        950,
        types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
        7,
    )
    .unwrap();
    let out = out.as_const().unwrap();
    assert_eq!((out.consttype, out.consttypmod, out.constcollid), (19, -1, 950));
    assert_eq!(out.location, -1);
}

#[test]
fn apply_relabel_type_strips_nested_relabels_and_nets_out() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let var = Node::mk_var(mcx, 1, 1, 25, -1, 100, 0).unwrap();
    let inner = Node::mk_relabel_type(
        mcx,
        var,
        19,
        -1,
        950,
        types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
    )
    .unwrap();
    let out = node_funcs::apply_relabel_type(
        mcx,
        inner,
        25,
        -1,
        100,
        types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
        -1,
    )
    .unwrap();
    assert!(out.ptr_eq(var));
}

#[test]
fn apply_relabel_type_wraps_when_types_differ() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let var = Node::mk_var(mcx, 1, 1, 25, -1, 100, 0).unwrap();
    let out = node_funcs::apply_relabel_type(
        mcx,
        var,
        19,
        -1,
        950,
        types_nodes::CoercionForm::COERCE_EXPLICIT_CAST,
        3,
    )
    .unwrap();
    let r = out.as_relabel_type().unwrap();
    assert!(r.arg.ptr_eq(var));
    assert_eq!((r.resulttype, r.resultcollid, r.location), (19, 950, 3));
}

#[test]
fn walker_and_mutator_cover_saop_array_relabel_case() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let p1 = extern_param(mcx, 1);
    let p2 = extern_param(mcx, 2);
    let arr = Node::mk(
        mcx,
        types_nodes::ArrayExpr {
            array_typeid: 1009,
            element_typeid: 25,
            elements: NodeList::from_slice(mcx, &[p2]).unwrap(),
            list_start: -1,
            list_end: -1,
            location: -1,
            ..Default::default()
        },
    )
    .unwrap();
    let saop = Node::mk(
        mcx,
        types_nodes::ScalarArrayOpExpr {
            opno: 98,
            opfuncid: 67,
            useOr: true,
            args: NodeList::from_slice(mcx, &[p1, arr]).unwrap(),
            location: -1,
            ..Default::default()
        },
    )
    .unwrap();
    let relabel = Node::mk_relabel_type(
        mcx,
        saop,
        16,
        -1,
        0,
        types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
    )
    .unwrap();

    struct Count(usize);
    impl<'mcx> NodeWalker<'mcx> for Count {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if node.node_tag() == NodeTag::T_Param {
                self.0 += 1;
                return Ok(false);
            }
            expression_tree_walker(node, self)
        }
    }
    let mut w = Count(0);
    assert!(!expression_tree_walker(relabel, &mut w).unwrap());
    assert_eq!(w.0, 2);

    assert!(expression_tree_mutator(mcx, relabel, &mut |_| Ok(None)).unwrap().is_none());
    let replacement = extern_param(mcx, 9);
    let out = expression_tree_mutator(mcx, relabel, &mut |n| {
        if n.node_tag() == NodeTag::T_Param && n.as_param().unwrap().paramid == 1 {
            Ok(Some(replacement))
        } else {
            expression_tree_mutator(mcx, n, &mut |n2| {
                Ok((n2.node_tag() == NodeTag::T_Param
                    && n2.as_param().unwrap().paramid == 1)
                    .then_some(replacement))
            })
        }
    })
    .unwrap()
    .expect("substituted param rebuilds the tree");
    let new_saop = out.as_relabel_type().unwrap().arg.as_scalar_array_op_expr().unwrap();
    assert_eq!(new_saop.args.nth(0).as_param().unwrap().paramid, 9);
    assert!(new_saop.args.nth(1).ptr_eq(arr));
}
