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

// Michael's wasm-REPL bug (a): generate_series in FROM of a recursive CTE hit
// the deferred arm. C raw_expression_tree_walker T_RangeFunction walks
// functions, alias, coldeflist.
#[test]
fn raw_walker_range_function_walks_functions_and_coldeflist() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let fc = Node::mk_param_ref(mcx, 1, -1).unwrap();
    let cd = Node::mk_param_ref(mcx, 2, -1).unwrap();
    let alias = Node::mk_mut(
        mcx,
        types_nodes::primnodes::Alias { aliasname: Some("x"), colnames: NodeList::nil() },
    )
    .unwrap()
    .seal_ref();
    let rf = Node::mk(
        mcx,
        types_nodes::rawnodes::RangeFunction {
            functions: NodeList::from_slice(mcx, &[fc]).unwrap(),
            alias: Some(alias),
            coldeflist: NodeList::from_slice(mcx, &[cd]).unwrap(),
            ..types_nodes::rawnodes::RangeFunction::default()
        },
    )
    .unwrap();
    let mut w = CountParams { analyzed: 0, raw: 0 };
    assert!(!raw_expression_tree_walker(rf, &mut w).unwrap());
    assert_eq!(w.raw, 2);
}

// Michael's wasm-REPL bug (b): CASE in a recursive term hit the deferred arm.
// C raw_expression_tree_walker T_CaseExpr walks arg, each CaseWhen's
// expr/result (no callback on the CaseWhen itself), then defresult.
#[test]
fn raw_walker_case_expr_walks_arg_whens_defresult() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let when = Node::mk(
        mcx,
        types_nodes::primnodes::CaseWhen {
            expr: Some(Node::mk_param_ref(mcx, 2, -1).unwrap()),
            result: Some(Node::mk_param_ref(mcx, 3, -1).unwrap()),
            location: -1,
        },
    )
    .unwrap();
    let case = Node::mk(
        mcx,
        types_nodes::primnodes::CaseExpr {
            arg: Some(Node::mk_param_ref(mcx, 1, -1).unwrap()),
            args: NodeList::from_slice(mcx, &[when]).unwrap(),
            defresult: Some(Node::mk_param_ref(mcx, 4, -1).unwrap()),
            ..types_nodes::primnodes::CaseExpr::default()
        },
    )
    .unwrap();
    let mut w = CountParams { analyzed: 0, raw: 0 };
    assert!(!raw_expression_tree_walker(case, &mut w).unwrap());
    assert_eq!(w.raw, 4);
}

// C raw_expression_tree_walker's default arm is elog(ERROR, "unrecognized
// node type: %d") (nodeFuncs.c:4640-4642): an XX000 error, not a panic.
#[test]
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
    let err = raw_expression_tree_walker(rt, &mut w).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
    assert_eq!(
        err.message(),
        format!("unrecognized node type: {}", NodeTag::T_TargetEntry as u16)
    );
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

const CPRINT_CORPUS: &str = include_str!("../cprint_corpus.txt");
const CPRINT_EXPECTED: &str = include_str!("../cprint_expected.txt");

fn corpus_entries() -> Vec<&'static str> {
    let mut v: Vec<&str> = CPRINT_CORPUS.split('\n').collect();
    if v.last() == Some(&"") {
        v.pop();
    }
    v
}

// (entry index, "FORMAT"|"PRETTY") -> expected bytes between the markers.
fn expected_sections() -> Vec<(usize, &'static str, &'static str)> {
    let mut out = Vec::new();
    let mut rest = CPRINT_EXPECTED;
    loop {
        let Some(start) = rest.find("#ENTRY ") else { break };
        let hdr_end = rest[start..].find('\n').map(|p| start + p + 1).unwrap();
        let hdr = &rest[start..hdr_end - 1];
        let mut it = hdr.split(' ');
        it.next();
        let idx: usize = it.next().unwrap().parse().unwrap();
        let kind = it.next().unwrap();
        let body_end =
            rest[hdr_end..].find("#ENTRY ").map(|p| hdr_end + p).unwrap_or(rest.len());
        out.push((idx, kind, &rest[hdr_end..body_end]));
        rest = &rest[body_end..];
        if rest.is_empty() {
            break;
        }
    }
    out
}

#[test]
fn print_formatters_match_c_oracle() {
    if CPRINT_EXPECTED.is_empty() {
        // Bootstrap window only: scripts/print-oracle-e2e.sh FAILs the CI cluster
        // job until the fixture is vendored, so this skip cannot go stale.
        eprintln!("cprint_expected.txt empty — oracle comparison skipped");
        return;
    }
    let ctx = cx();
    let mcx = ctx.mcx();
    let entries = corpus_entries();
    let sections = expected_sections();
    assert_eq!(sections.len(), entries.len() * 2, "oracle section count");
    for (idx, kind, want) in sections {
        let dump = entries[idx];
        let got = match kind {
            "FORMAT" => print::format_node_dump(mcx, dump).unwrap(),
            "PRETTY" => print::pretty_format_node_dump(mcx, dump).unwrap(),
            other => panic!("bad oracle section kind {other}"),
        };
        assert_eq!(got.as_str(), want, "entry {idx} {kind}: dump={dump:?}");
    }
}

// Corpus lines 0-2 are pinned nodeToString outputs; if outfuncs drifts, the
// oracle corpus (and C fixtures) must be regenerated together.
#[test]
fn print_corpus_head_matches_node_to_string() {
    use datum::Datum;
    use types_nodes::primnodes::{Const, Var, VarReturningType};
    let ctx = cx();
    let mcx = ctx.mcx();
    let entries = corpus_entries();
    let int4_const = |v: i32| Const {
        consttype: 23,
        consttypmod: -1,
        constcollid: 0,
        constlen: 4,
        constvalue: Datum::from_i32(v),
        constisnull: false,
        constbyval: true,
        location: 7,
    };
    let c42 = Node::mk(mcx, int4_const(42)).unwrap();
    assert_eq!(outfuncs::nodeToString(mcx, c42).unwrap().as_str(), entries[0]);
    let var = Node::mk(
        mcx,
        Var {
            varno: 1,
            varattno: 2,
            vartype: 23,
            vartypmod: -1,
            varcollid: 0,
            varnullingrels: types_nodes::Bitmapset::empty(),
            varlevelsup: 0,
            varreturningtype: VarReturningType::VAR_RETURNING_DEFAULT,
            varnosyn: 1,
            varattnosyn: 2,
            location: 33,
        },
    )
    .unwrap();
    let zero = Node::mk(mcx, int4_const(0)).unwrap();
    let mut args = NodeList::nil();
    args.lappend(mcx, var).unwrap();
    args.lappend(mcx, zero).unwrap();
    let op = Node::mk(
        mcx,
        OpExpr {
            opno: 521,
            opfuncid: 147,
            opresulttype: 16,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args,
            location: 35,
        },
    )
    .unwrap();
    assert_eq!(outfuncs::nodeToString(mcx, op).unwrap().as_str(), entries[1]);
}

#[test]
fn expr_input_collation_arms() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let mut args = NodeList::nil();
    args.lappend(mcx, extern_param(mcx, 1)).unwrap();
    let op = Node::mk(
        mcx,
        OpExpr {
            opno: 98,
            opfuncid: 67,
            opresulttype: 16,
            opretset: false,
            opcollid: 0,
            inputcollid: 100,
            args,
            location: -1,
        },
    )
    .unwrap();
    assert_eq!(expr_input_collation(op), 100);
    assert_eq!(expr_input_collation(extern_param(mcx, 1)), types_core::InvalidOid);
}

#[test]
fn qoe_mutator_non_query_applies_directly() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let p = extern_param(mcx, 1);
    let mut m = |n: Node<'_>| {
        Ok(if n.node_tag() == NodeTag::T_Param { Some(extern_param(mcx, 2)) } else { None })
    };
    let out = query_or_expression_tree_mutator(mcx, p, &mut m, 0).unwrap().unwrap();
    assert_eq!(out.as_param().unwrap().paramid, 2);
}

#[test]
fn make_whole_row_var_default_arms() {
    use types_nodes::parsenodes::{RTEKind, RangeTblEntry};
    let ctx = cx();
    let mcx = ctx.mcx();
    let mut rte = RangeTblEntry::default();
    rte.rtekind = RTEKind::RTE_VALUES;
    let v = makefuncs::make_whole_row_var(mcx, &rte, 3, 1, false).unwrap();
    assert_eq!(v.vartype, types_core::catalog::RECORDOID);
    assert_eq!(v.varno, 3);
    assert_eq!(v.varattno, 0);
    assert_eq!(v.varlevelsup, 1);
    let mut rte = RangeTblEntry::default();
    rte.rtekind = RTEKind::RTE_SUBQUERY;
    let v = makefuncs::make_whole_row_var(mcx, &rte, 1, 0, true).unwrap();
    assert_eq!(v.vartype, types_core::catalog::RECORDOID);
}

#[test]
fn on_conflict_expr_walker_reaches_all_five_fields() {
    use types_nodes::primnodes::{InferenceElem, OnConflictAction, OnConflictExpr};
    let ctx = cx();
    let mcx = ctx.mcx();
    let mk_te = |id| {
        Node::mk_target_entry(mcx, extern_param(mcx, id), 1, None, false).unwrap()
    };
    let ie = Node::mk(
        mcx,
        InferenceElem { expr: Some(extern_param(mcx, 1)), infercollid: 0, inferopclass: 0 },
    )
    .unwrap();
    let oc = Node::mk(
        mcx,
        OnConflictExpr {
            action: OnConflictAction::ONCONFLICT_UPDATE,
            arbiterElems: NodeList::from_slice(mcx, &[ie]).unwrap(),
            arbiterWhere: Some(extern_param(mcx, 2)),
            constraint: 0,
            onConflictSet: NodeList::from_slice(mcx, &[mk_te(3)]).unwrap(),
            onConflictWhere: Some(extern_param(mcx, 4)),
            exclRelIndex: 2,
            exclRelTlist: NodeList::from_slice(mcx, &[mk_te(5)]).unwrap(),
        },
    )
    .unwrap();

    struct W(Vec<i32>);
    impl<'mcx> NodeWalker<'mcx> for W {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if let Some(p) = node.as_param() {
                self.0.push(p.paramid);
                return Ok(false);
            }
            expression_tree_walker(node, self)
        }
    }
    let mut w = W(Vec::new());
    assert!(!expression_tree_walker(oc, &mut w).unwrap());
    assert_eq!(w.0, vec![1, 2, 3, 4, 5]);
}

#[test]
fn on_conflict_expr_mutator_identity_and_rebuild() {
    use types_nodes::primnodes::{OnConflictAction, OnConflictExpr};
    let ctx = cx();
    let mcx = ctx.mcx();
    let set_te =
        Node::mk_target_entry(mcx, extern_param(mcx, 1), 1, None, false).unwrap();
    let oc = Node::mk(
        mcx,
        OnConflictExpr {
            action: OnConflictAction::ONCONFLICT_UPDATE,
            arbiterElems: NodeList::nil(),
            arbiterWhere: None,
            constraint: 0,
            onConflictSet: NodeList::from_slice(mcx, &[set_te]).unwrap(),
            onConflictWhere: Some(extern_param(mcx, 7)),
            exclRelIndex: 2,
            exclRelTlist: NodeList::nil(),
        },
    )
    .unwrap();

    assert!(expression_tree_mutator(mcx, oc, &mut |_| Ok(None)).unwrap().is_none());

    let replacement = extern_param(mcx, 9);
    let out = expression_tree_mutator(mcx, oc, &mut |n| {
        Ok((n.as_param().is_some_and(|p| p.paramid == 7)).then_some(replacement))
    })
    .unwrap()
    .expect("changed onConflictWhere rebuilds the node");
    let new_oc = out.as_on_conflict_expr().unwrap();
    assert_eq!(new_oc.onConflictWhere.unwrap().as_param().unwrap().paramid, 9);
    assert_eq!(new_oc.exclRelIndex, 2);
    assert_eq!(new_oc.onConflictSet.len(), 1);
}

#[test]
fn expr_location_covers_c_arms_and_defaults_to_minus_one() {
    use types_nodes::parsenodes::DefElem;
    use types_nodes::primnodes::{Const, TargetEntry};
    use types_nodes::rawnodes::{SortBy, WindowDef};

    let ctx = cx();
    let mcx = ctx.mcx();
    let con = Node::mk(
        mcx,
        Const {
            consttype: 23,
            consttypmod: -1,
            constcollid: 0,
            constlen: 4,
            constvalue: datum::Datum::from_i32(1),
            constisnull: false,
            constbyval: true,
            location: 7,
        },
    )
    .unwrap();

    // C exprLocation: SortBy reports its argument's location (the operator,
    // if any, is ignored).
    let sb = Node::mk(mcx, SortBy { node: Some(con), ..SortBy::default() }).unwrap();
    assert_eq!(node_funcs::expr_location(sb), 7);

    // WindowDef carries its own location.
    let wd = Node::mk(mcx, WindowDef { location: 11, ..WindowDef::default() }).unwrap();
    assert_eq!(node_funcs::expr_location(wd), 11);

    // TargetEntry reports its expression's location.
    let te = Node::mk(
        mcx,
        TargetEntry {
            expr: con,
            resno: 1,
            resname: None,
            ressortgroupref: 0,
            resorigtbl: 0,
            resorigcol: 0,
            resjunk: false,
        },
    )
    .unwrap();
    assert_eq!(node_funcs::expr_location(te), 7);

    // C's default arm: a tag exprLocation has no case for is "just unknown"
    // (-1), even when the node carries a location field.
    let de = Node::mk(
        mcx,
        DefElem {
            defnamespace: None,
            defname: Some("x"),
            arg: None,
            defaction: types_nodes::parsenodes::DefElemAction::DEFELEM_UNSPEC,
            location: 33,
        },
    )
    .unwrap();
    assert_eq!(node_funcs::expr_location(de), -1);

    // A_Indirection is also absent from C's 18.3 switch: -1, NOT the arg's
    // location.
    let ai = Node::mk(
        mcx,
        types_nodes::rawnodes::A_Indirection {
            arg: Some(con),
            indirection: types_nodes::NodeList::nil(),
        },
    )
    .unwrap();
    assert_eq!(node_funcs::expr_location(ai), -1);
}

// InferenceElem reads through its expr (C exprType/exprCollation arms);
// exprTypmod's default for tags without an arm is C's -1, not an error.
#[test]
fn expr_accessors_inference_elem_and_typmod_default() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let var = Node::mk(
        mcx,
        types_nodes::primnodes::Var {
            varno: 1,
            varattno: 1,
            vartype: 25,
            vartypmod: 7,
            varcollid: 100,
            ..Default::default()
        },
    )
    .unwrap();
    let ie = Node::mk(
        mcx,
        types_nodes::primnodes::InferenceElem {
            expr: Some(var),
            infercollid: 0,
            inferopclass: 0,
        },
    )
    .unwrap();
    assert_eq!(node_funcs::expr_type(ie), 25);
    assert_eq!(node_funcs::expr_collation(ie), 100);
    // InferenceElem has no exprTypmod arm in C: default -1.
    assert_eq!(node_funcs::expr_typmod(ie), -1);
}

#[test]
fn array_expr_typmod_agrees_or_minus_one() {
    // C nodeFuncs.c exprTypmod T_ArrayExpr: all elements agreeing on
    // type+typmod yield that typmod (timetz(0)[] keeps 0), else -1.
    let ctx = cx();
    let mcx = ctx.mcx();
    const TIMETZOID: types_core::Oid = 1266;
    fn timetz(mcx: Mcx<'_>, typmod: i32) -> Node<'_> {
        Node::mk(
            mcx,
            types_nodes::primnodes::Const {
                consttype: TIMETZOID,
                consttypmod: typmod,
                constcollid: 0,
                constlen: 12,
                constvalue: datum::Datum::from_i32(0),
                constisnull: true,
                constbyval: false,
                location: -1,
            },
        )
        .unwrap()
    }
    fn arr<'m>(mcx: Mcx<'m>, elems: &[Node<'m>]) -> Node<'m> {
        Node::mk(
            mcx,
            types_nodes::ArrayExpr {
                array_typeid: 1270, // _timetz
                element_typeid: TIMETZOID,
                elements: NodeList::from_slice(mcx, elems).unwrap(),
                list_start: -1,
                list_end: -1,
                location: -1,
                ..Default::default()
            },
        )
        .unwrap()
    }
    let e0a = timetz(mcx, 0);
    let e0b = timetz(mcx, 0);
    let e3 = timetz(mcx, 3);
    assert_eq!(node_funcs::expr_typmod(arr(mcx, &[e0a, e0b])), 0);
    assert_eq!(node_funcs::expr_typmod(arr(mcx, &[e0a, e3])), -1);
    assert_eq!(node_funcs::expr_typmod(arr(mcx, &[])), -1);
    // element type disagreeing with the common type is -1
    let p = extern_param(mcx, 1);
    assert_eq!(node_funcs::expr_typmod(arr(mcx, &[p])), -1);
}

// RECURSION GUARD (C parity: nodeFuncs.c:2111/2966/4010 — every walker
// invocation calls check_stack_depth). Deep BoolExpr nesting must surface
// as a structured 54001 under a lowered budget, and walk clean under a
// generous one (control pins "guard fired", not "tree malformed").
#[test]
fn walker_deep_nesting_raises_54001_with_the_guard_armed() {
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            stack_depth_core::set_stack_base();
            let ctx = cx();
            let mcx = ctx.mcx();
            fn deep<'mcx>(mcx: Mcx<'mcx>) -> Node<'mcx> {
                let mut node = extern_param(mcx, 1);
                for _ in 0..4000 {
                    node = Node::mk(
                        mcx,
                        types_nodes::primnodes::BoolExpr {
                            boolop: types_nodes::primnodes::BoolExprType::NOT_EXPR,
                            args: NodeList::from_slice(mcx, &[node]).unwrap(),
                            location: -1,
                        },
                    )
                    .unwrap();
                }
                node
            }
            let node = deep(mcx);
            let node_b = deep(mcx);

            struct W(usize);
            impl<'mcx> NodeWalker<'mcx> for W {
                fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
                    self.0 += 1;
                    expression_tree_walker(node, self)
                }
            }

            // control: clean at a generous limit
            stack_depth_core::assign_max_stack_depth(16 * 1024);
            let mut w = W(0);
            assert!(expression_tree_walker(node, &mut w).is_ok());
            assert_eq!(w.0, 4000);

            // guard: 54001 at a tight budget
            stack_depth_core::set_enforced_stack_budget_for_tests(200 * 1024);
            let mut w2 = W(0);
            let err = expression_tree_walker(node, &mut w2)
                .expect_err("walker guard must fire at a 200kB budget");
            assert_eq!(err.sqlstate(), types_error::ERRCODE_STATEMENT_TOO_COMPLEX);

            // equal(): same witness through the panic-payload guard (distinct
            // trees — ptr_eq short-circuits identical handles before the guard)
            let err = std::panic::catch_unwind(|| types_nodes::equal(node, node_b))
                .expect_err("equal() must trip the guard at a 200kB budget");
            let err = types_error::pg_error_from_panic(err)
                .expect("payload restores to the 54001 PgError");
            assert_eq!(err.sqlstate(), types_error::ERRCODE_STATEMENT_TOO_COMPLEX);

            stack_depth_core::assign_max_stack_depth(100);
        })
        .unwrap()
        .join()
        .unwrap();
}

// ---------------------------------------------------------------------------
// audit-18.6 remediation b024 (fp-nodes-nodeFuncs-p1/p2, fp-nodes-print):
// every witness below asserts the C 18.6 nodeFuncs.c / print.c contract.
// ---------------------------------------------------------------------------

fn int4_const_at(mcx: Mcx<'_>, v: i32, location: i32) -> Node<'_> {
    Node::mk(
        mcx,
        types_nodes::primnodes::Const {
            consttype: 23,
            consttypmod: -1,
            constcollid: 0,
            constlen: 4,
            constvalue: datum::Datum::from_i32(v),
            constisnull: false,
            constbyval: true,
            location,
        },
    )
    .unwrap()
}

fn sort_group_clause(mcx: Mcx<'_>, sortgroupref: u32) -> Node<'_> {
    Node::mk(
        mcx,
        types_nodes::parsenodes::SortGroupClause {
            tleSortGroupRef: sortgroupref,
            eqop: 96,
            sortop: 97,
            reverse_sort: false,
            nulls_first: false,
            hashable: true,
        },
    )
    .unwrap()
}

/// Records the tag of every node the engine hands back to the walker.
struct TagTrail(Vec<NodeTag>);
impl<'mcx> NodeWalker<'mcx> for TagTrail {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        self.0.push(node.node_tag());
        expression_tree_walker(node, self)
    }
}

fn count_tag(trail: &[NodeTag], tag: NodeTag) -> usize {
    trail.iter().filter(|t| **t == tag).count()
}

// C strip_implicit_coercions (nodeFuncs.c:744-750): an implicit
// CoerceToDomain is stripped like the other five coercion node types.
#[test]
fn strip_implicit_coercions_strips_implicit_coerce_to_domain() {
    use types_nodes::primnodes::CoercionForm;
    let ctx = cx();
    let mcx = ctx.mcx();
    let c = int4_const_at(mcx, 5, -1);
    let domain = |arg, form| {
        Node::mk(
            mcx,
            types_nodes::CoerceToDomain {
                arg,
                resulttype: 16462,
                resulttypmod: -1,
                resultcollid: 0,
                coercionformat: form,
                location: -1,
            },
        )
        .unwrap()
    };
    let implicit = domain(c, CoercionForm::COERCE_IMPLICIT_CAST);
    assert_eq!(strip_implicit_coercions(implicit).node_tag(), NodeTag::T_Const);
    // Stacked implicit coercions strip all the way down (C recurses).
    let relabel = Node::mk(
        mcx,
        types_nodes::RelabelType {
            arg: implicit,
            resulttype: 26,
            resulttypmod: -1,
            resultcollid: 0,
            relabelformat: CoercionForm::COERCE_IMPLICIT_CAST,
            location: -1,
        },
    )
    .unwrap();
    assert_eq!(strip_implicit_coercions(relabel).node_tag(), NodeTag::T_Const);
    // Explicit casts and SQL-syntax coercions are kept.
    let explicit = domain(c, CoercionForm::COERCE_EXPLICIT_CAST);
    assert_eq!(strip_implicit_coercions(explicit).node_tag(), NodeTag::T_CoerceToDomain);
    let sql_syntax = domain(c, CoercionForm::COERCE_SQL_SYNTAX);
    assert_eq!(strip_implicit_coercions(sql_syntax).node_tag(), NodeTag::T_CoerceToDomain);
}

fn install_int4_eq_operator_seam() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        syscache_seams::lookup_pg_operator_shape::set(|opno| {
            Ok(match opno {
                96 => Some(syscache_seams::PgOperatorShape {
                    oprnamespace: 11,
                    oprleft: 23,
                    oprright: 23,
                    oprresult: 16,
                    oprcom: 96,
                    oprnegate: 518,
                    oprcode: 65,
                    oprrest: 101,
                    oprjoin: 105,
                    oprcanmerge: true,
                    oprcanhash: true,
                }),
                _ => None,
            })
        });
    });
}

// C fix_opfuncids_walker (nodeFuncs.c:1844-1855): set_opfuncid on OpExpr,
// DistinctExpr and NullIfExpr, set_sa_opfuncid on ScalarArrayOpExpr.
#[test]
fn fix_opfuncids_fills_distinct_nullif_and_scalar_array_ops() {
    install_int4_eq_operator_seam();
    let ctx = cx();
    let mcx = ctx.mcx();
    let p = extern_param(mcx, 1);
    let pair = || NodeList::from_slice(mcx, &[p, p]).unwrap();
    let op = Node::mk(
        mcx,
        OpExpr {
            opno: 96,
            opfuncid: 0,
            opresulttype: 16,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args: pair(),
            location: -1,
        },
    )
    .unwrap();
    let distinct = Node::mk(
        mcx,
        types_nodes::DistinctExpr {
            opno: 96,
            opfuncid: 0,
            opresulttype: 16,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args: pair(),
            location: -1,
        },
    )
    .unwrap();
    let nullif = Node::mk(
        mcx,
        types_nodes::NullIfExpr {
            opno: 96,
            opfuncid: 0,
            opresulttype: 23,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args: pair(),
            location: -1,
        },
    )
    .unwrap();
    let saop = Node::mk(
        mcx,
        types_nodes::primnodes::ScalarArrayOpExpr {
            opno: 96,
            opfuncid: 0,
            useOr: true,
            args: pair(),
            location: -1,
            ..types_nodes::primnodes::ScalarArrayOpExpr::default()
        },
    )
    .unwrap();
    // Nested under a BoolExpr so the walk (not just the root) is exercised.
    let and = Node::mk(
        mcx,
        types_nodes::primnodes::BoolExpr {
            boolop: types_nodes::primnodes::BoolExprType::AND_EXPR,
            args: NodeList::from_slice(mcx, &[op, distinct, nullif, saop]).unwrap(),
            location: -1,
        },
    )
    .unwrap();
    fix_opfuncids(and).unwrap();
    assert_eq!(op.as_op_expr().unwrap().opfuncid, 65, "OpExpr");
    assert_eq!(distinct.as_distinct_expr().unwrap().opfuncid, 65, "DistinctExpr");
    assert_eq!(nullif.as_null_if_expr().unwrap().opfuncid, 65, "NullIfExpr");
    assert_eq!(saop.as_scalar_array_op_expr().unwrap().opfuncid, 65, "ScalarArrayOpExpr");
}

// C exprType (nodeFuncs.c:96-130): MULTIEXPR sublinks are RECORD; only
// EXPR/ARRAY sublinks look inside the subselect, so any other sublink over an
// untransformed subselect still answers (boolean / -1 / InvalidOid).
#[test]
fn expr_accessors_multiexpr_sublink_is_record_and_skip_the_subselect_otherwise() {
    use types_nodes::primnodes::SubLinkType;
    let ctx = cx();
    let mcx = ctx.mcx();
    let te = Node::mk_target_entry(mcx, int4_const_at(mcx, 1, -1), 1, None, false).unwrap();
    let subselect = Node::mk(
        mcx,
        Query { targetList: NodeList::from_slice(mcx, &[te]).unwrap(), ..Query::default() },
    )
    .unwrap();
    let sublink = |kind, subselect| {
        Node::mk(
            mcx,
            types_nodes::SubLink {
                subLinkType: kind,
                subLinkId: 1,
                testexpr: None,
                operName: NodeList::nil(),
                subselect,
                location: -1,
            },
        )
        .unwrap()
    };
    let multi = sublink(SubLinkType::MULTIEXPR_SUBLINK, subselect);
    assert_eq!(expr_type(multi), 2249, "C: MULTIEXPR is always considered to return RECORD");
    assert_eq!(expr_typmod(multi), -1);
    assert_eq!(expr_collation(multi), 0);

    let raw = Node::mk_string(mcx, "untransformed").unwrap();
    let exists = sublink(SubLinkType::EXISTS_SUBLINK, raw);
    assert_eq!(expr_type(exists), 16);
    assert_eq!(expr_typmod(exists), -1);
    assert_eq!(expr_collation(exists), 0);
    let any = sublink(SubLinkType::ANY_SUBLINK, raw);
    assert_eq!(expr_type(any), 16);
    // EXPR sublinks do consult the subselect's first target column.
    let expr = sublink(SubLinkType::EXPR_SUBLINK, subselect);
    assert_eq!(expr_type(expr), 23);
}

// C expression_tree_mutator T_SetOperationStmt (nodeFuncs.c:3640-3649): larg
// and rarg are mutated; "We do not mutate groupClauses by default".
#[test]
fn mutator_set_operation_stmt_skips_group_clauses() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let rtr = |i| Node::mk(mcx, types_nodes::primnodes::RangeTblRef { rtindex: i }).unwrap();
    let so = Node::mk(
        mcx,
        types_nodes::parsenodes::SetOperationStmt {
            op: types_nodes::parsenodes::SetOperation::SETOP_UNION,
            larg: Some(rtr(1)),
            rarg: Some(rtr(2)),
            groupClauses: NodeList::from_slice(mcx, &[sort_group_clause(mcx, 1)]).unwrap(),
            ..types_nodes::parsenodes::SetOperationStmt::default()
        },
    )
    .unwrap();
    let mut seen = Vec::new();
    let out = expression_tree_mutator(mcx, so, &mut |n| {
        seen.push(n.node_tag());
        Ok(None)
    })
    .unwrap();
    assert!(out.is_none());
    assert_eq!(seen, [NodeTag::T_RangeTblRef, NodeTag::T_RangeTblRef]);
}

// C expression_tree_walker T_CommonTableExpr (nodeFuncs.c:2423-2439) walks
// ctequery, search_clause and cycle_clause; T_CTECycleClause (:2408-2417)
// walks cycle_mark_value and cycle_mark_default; T_CTESearchClause is a leaf.
#[test]
fn walker_reaches_cte_search_and_cycle_clauses() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let cycle = Node::mk(
        mcx,
        types_nodes::parsenodes::CTECycleClause {
            cycle_mark_column: Some("c"),
            cycle_mark_value: Some(int4_const_at(mcx, 1, -1)),
            cycle_mark_default: Some(int4_const_at(mcx, 0, -1)),
            cycle_path_column: Some("p"),
            cycle_mark_type: 23,
            cycle_mark_typmod: -1,
            location: -1,
            ..types_nodes::parsenodes::CTECycleClause::default()
        },
    )
    .unwrap();
    let search = Node::mk(
        mcx,
        types_nodes::parsenodes::CTESearchClause {
            search_breadth_first: true,
            search_seq_column: Some("s"),
            location: -1,
            ..types_nodes::parsenodes::CTESearchClause::default()
        },
    )
    .unwrap();
    let cte = Node::mk(
        mcx,
        types_nodes::parsenodes::CommonTableExpr {
            ctename: Some("g"),
            ctequery: Some(Node::mk(mcx, Query::default()).unwrap()),
            search_clause: Some(search),
            cycle_clause: Some(cycle),
            ..types_nodes::parsenodes::CommonTableExpr::default()
        },
    )
    .unwrap();
    let mut w = TagTrail(Vec::new());
    assert!(!expression_tree_walker(cte, &mut w).unwrap());
    assert_eq!(count_tag(&w.0, NodeTag::T_Query), 1);
    assert_eq!(count_tag(&w.0, NodeTag::T_CTESearchClause), 1);
    assert_eq!(count_tag(&w.0, NodeTag::T_CTECycleClause), 1);
    assert_eq!(count_tag(&w.0, NodeTag::T_Const), 2, "cycle mark value + default");
}

// C expression_tree_walker arms (nodeFuncs.c:2399-2412 WindowClause,
// :2440-2500 SQL/JSON constructors, :2570-2585 partition prune steps,
// :2625-2632 AppendRelInfo) that the port lacked.
#[test]
fn walker_covers_window_clause_json_constructor_and_planner_arms() {
    use types_nodes::rawnodes::{
        JsonAggConstructor, JsonArrayAgg, JsonArrayConstructor, JsonArrayQueryConstructor,
        JsonKeyValue, JsonObjectAgg, JsonObjectConstructor,
    };
    let ctx = cx();
    let mcx = ctx.mcx();
    let c = |v| int4_const_at(mcx, v, -1);
    let trail = |node| {
        let mut w = TagTrail(Vec::new());
        assert!(!expression_tree_walker(node, &mut w).unwrap());
        w.0
    };

    let wc = Node::mk(
        mcx,
        types_nodes::parsenodes::WindowClause {
            partitionClause: NodeList::from_slice(mcx, &[sort_group_clause(mcx, 1)]).unwrap(),
            orderClause: NodeList::from_slice(mcx, &[sort_group_clause(mcx, 2)]).unwrap(),
            startOffset: Some(c(1)),
            endOffset: Some(c(2)),
            ..types_nodes::parsenodes::WindowClause::default()
        },
    )
    .unwrap();
    let t = trail(wc);
    assert_eq!(count_tag(&t, NodeTag::T_SortGroupClause), 2);
    assert_eq!(count_tag(&t, NodeTag::T_Const), 2);

    let kv = Node::mk(mcx, JsonKeyValue { key: Some(c(1)), value: Some(c(2)) }).unwrap();
    assert_eq!(count_tag(&trail(kv), NodeTag::T_Const), 2);
    let obj = Node::mk(
        mcx,
        JsonObjectConstructor {
            exprs: NodeList::from_slice(mcx, &[kv]).unwrap(),
            ..JsonObjectConstructor::default()
        },
    )
    .unwrap();
    let t = trail(obj);
    assert_eq!(count_tag(&t, NodeTag::T_JsonKeyValue), 1);
    assert_eq!(count_tag(&t, NodeTag::T_Const), 2);
    let arr = Node::mk(
        mcx,
        JsonArrayConstructor {
            exprs: NodeList::from_slice(mcx, &[c(1), c(2), c(3)]).unwrap(),
            ..JsonArrayConstructor::default()
        },
    )
    .unwrap();
    assert_eq!(count_tag(&trail(arr), NodeTag::T_Const), 3);
    let arrq = Node::mk(
        mcx,
        JsonArrayQueryConstructor { query: Some(c(1)), ..JsonArrayQueryConstructor::default() },
    )
    .unwrap();
    assert_eq!(count_tag(&trail(arrq), NodeTag::T_Const), 1);
    let agg = Node::mk(
        mcx,
        JsonAggConstructor {
            agg_filter: Some(c(1)),
            agg_order: NodeList::from_slice(mcx, &[c(2)]).unwrap(),
            over: Some(c(3)),
            ..JsonAggConstructor::default()
        },
    )
    .unwrap();
    assert_eq!(count_tag(&trail(agg), NodeTag::T_Const), 3);
    let objagg = Node::mk(
        mcx,
        JsonObjectAgg {
            constructor: Some(agg),
            arg: Some(kv),
            absent_on_null: false,
            unique: false,
        },
    )
    .unwrap();
    let t = trail(objagg);
    assert_eq!(count_tag(&t, NodeTag::T_JsonAggConstructor), 1);
    assert_eq!(count_tag(&t, NodeTag::T_JsonKeyValue), 1);
    assert_eq!(count_tag(&t, NodeTag::T_Const), 5);
    let arragg = Node::mk(
        mcx,
        JsonArrayAgg { constructor: Some(agg), arg: Some(c(9)), ..JsonArrayAgg::default() },
    )
    .unwrap();
    assert_eq!(count_tag(&trail(arragg), NodeTag::T_Const), 4);

    let step = Node::mk(
        mcx,
        types_nodes::plannodes::PartitionPruneStepOp {
            exprs: NodeList::from_slice(mcx, &[c(1), c(2)]).unwrap(),
            ..types_nodes::plannodes::PartitionPruneStepOp::default()
        },
    )
    .unwrap();
    assert_eq!(count_tag(&trail(step), NodeTag::T_Const), 2);
    let combine =
        Node::mk(mcx, types_nodes::plannodes::PartitionPruneStepCombine::default()).unwrap();
    assert!(trail(combine).is_empty());
    let ari = Node::mk(
        mcx,
        types_nodes::plannodes::AppendRelInfo {
            parent_relid: 1,
            child_relid: 2,
            parent_reltype: 0,
            child_reltype: 0,
            num_child_cols: 0,
            parent_colnos: &[],
            parent_reloid: 0,
        },
    )
    .unwrap();
    assert!(trail(ari).is_empty());
}

// C expression_tree_mutator arms the port lacked: T_Query returns the node
// (nodeFuncs.c:3486-3488), T_WindowClause (:3489-3501), T_CTECycleClause
// (:3502-3512), T_CommonTableExpr (:3513-3532), T_JsonFormat (:2998) and
// T_JsonReturning (:3365) leaves, T_PartitionPruneStepOp (:3612-3622),
// T_PartitionPruneStepCombine (:3623-3625), T_AppendRelInfo (:3683-3693).
#[test]
fn mutator_covers_query_window_cte_json_and_planner_arms() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let c = |v| int4_const_at(mcx, v, -1);
    let replacement = c(99);
    let is99 = |n: Node<'_>| n.as_const().is_some_and(|k| k.constvalue.as_i32() == 99);
    // A C-shaped callback: replaces every Const, recurses through the engine
    // for everything else; counts how often it runs.
    fn replace_consts<'mcx>(
        mcx: Mcx<'mcx>,
        n: Node<'mcx>,
        replacement: Node<'mcx>,
        calls: &std::cell::Cell<usize>,
    ) -> PgResult<Option<Node<'mcx>>> {
        calls.set(calls.get() + 1);
        if n.node_tag() == NodeTag::T_Const {
            return Ok(Some(replacement));
        }
        expression_tree_mutator(mcx, n, &mut |c| replace_consts(mcx, c, replacement, calls))
    }
    let calls = std::cell::Cell::new(0usize);
    let mut m = |n| replace_consts(mcx, n, replacement, &calls);

    let q = Node::mk(
        mcx,
        Query { targetList: NodeList::from_slice(mcx, &[c(1)]).unwrap(), ..Query::default() },
    )
    .unwrap();
    assert!(expression_tree_mutator(mcx, q, &mut m).unwrap().is_none());
    assert_eq!(calls.get(), 0, "C: do nothing with a sub-Query");

    let wc = Node::mk(
        mcx,
        types_nodes::parsenodes::WindowClause {
            partitionClause: NodeList::from_slice(mcx, &[sort_group_clause(mcx, 1)]).unwrap(),
            startOffset: Some(c(1)),
            endOffset: None,
            ..types_nodes::parsenodes::WindowClause::default()
        },
    )
    .unwrap();
    let out = expression_tree_mutator(mcx, wc, &mut m).unwrap().expect("startOffset changed");
    let nwc = out.as_window_clause().unwrap();
    assert!(is99(nwc.startOffset.unwrap()));
    assert!(nwc.endOffset.is_none());
    assert_eq!(nwc.partitionClause.len(), 1);

    let cycle = Node::mk(
        mcx,
        types_nodes::parsenodes::CTECycleClause {
            cycle_mark_column: Some("c"),
            cycle_mark_value: Some(c(1)),
            cycle_mark_default: Some(c(0)),
            cycle_mark_type: 23,
            ..types_nodes::parsenodes::CTECycleClause::default()
        },
    )
    .unwrap();
    let out = expression_tree_mutator(mcx, cycle, &mut m).unwrap().expect("marks changed");
    let ncc = out.as_cte_cycle_clause().unwrap();
    assert!(is99(ncc.cycle_mark_value.unwrap()) && is99(ncc.cycle_mark_default.unwrap()));
    assert_eq!(ncc.cycle_mark_column, Some("c"));
    assert_eq!(ncc.cycle_mark_type, 23);

    let cte = Node::mk(
        mcx,
        types_nodes::parsenodes::CommonTableExpr {
            ctename: Some("g"),
            ctequery: Some(q),
            search_clause: Some(
                Node::mk(mcx, types_nodes::parsenodes::CTESearchClause::default()).unwrap(),
            ),
            cycle_clause: Some(cycle),
            ..types_nodes::parsenodes::CommonTableExpr::default()
        },
    )
    .unwrap();
    let out = expression_tree_mutator(mcx, cte, &mut m).unwrap().expect("cycle clause changed");
    let ncte = out.as_common_table_expr().unwrap();
    assert_eq!(ncte.ctename, Some("g"));
    let ncycle = ncte.cycle_clause.unwrap().as_cte_cycle_clause().unwrap();
    assert!(is99(ncycle.cycle_mark_value.unwrap()));
    assert!(ncte.search_clause.is_some());
    assert_eq!(ncte.ctequery.unwrap().node_tag(), NodeTag::T_Query);
    // Unchanged CTE shares the input.
    let cte2 = Node::mk(
        mcx,
        types_nodes::parsenodes::CommonTableExpr {
            ctequery: Some(q),
            ..types_nodes::parsenodes::CommonTableExpr::default()
        },
    )
    .unwrap();
    assert!(expression_tree_mutator(mcx, cte2, &mut m).unwrap().is_none());

    let fmt = Node::mk(mcx, types_nodes::primnodes::JsonFormat::default()).unwrap();
    assert!(expression_tree_mutator(mcx, fmt, &mut m).unwrap().is_none());
    let ret = Node::mk(
        mcx,
        types_nodes::primnodes::JsonReturning { format: None, typid: 114, typmod: -1 },
    )
    .unwrap();
    assert!(expression_tree_mutator(mcx, ret, &mut m).unwrap().is_none());

    let step = Node::mk(
        mcx,
        types_nodes::plannodes::PartitionPruneStepOp {
            step_id: 3,
            exprs: NodeList::from_slice(mcx, &[c(1)]).unwrap(),
            ..types_nodes::plannodes::PartitionPruneStepOp::default()
        },
    )
    .unwrap();
    let out = expression_tree_mutator(mcx, step, &mut m).unwrap().expect("exprs changed");
    let nstep = out.as_partition_prune_step_op().unwrap();
    assert_eq!(nstep.step_id, 3);
    assert!(is99(nstep.exprs.nth(0)));
    let combine =
        Node::mk(mcx, types_nodes::plannodes::PartitionPruneStepCombine::default()).unwrap();
    assert!(expression_tree_mutator(mcx, combine, &mut m).unwrap().is_none());
    let ari = Node::mk(
        mcx,
        types_nodes::plannodes::AppendRelInfo {
            parent_relid: 1,
            child_relid: 2,
            parent_reltype: 0,
            child_reltype: 0,
            num_child_cols: 0,
            parent_colnos: &[],
            parent_reloid: 0,
        },
    )
    .unwrap();
    assert!(expression_tree_mutator(mcx, ari, &mut m).unwrap().is_none());
}

// C exprLocation T_CollateClause (nodeFuncs.c:1709-1712): "just use
// argument's location" — the COLLATE keyword's own position is not consulted.
#[test]
fn expr_location_collate_clause_uses_the_argument_only() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let collate = |arg_loc, loc| {
        let arg = Node::mk(
            mcx,
            types_nodes::rawnodes::ColumnRef { fields: NodeList::nil(), location: arg_loc },
        )
        .unwrap();
        Node::mk(
            mcx,
            types_nodes::rawnodes::CollateClause {
                arg: Some(arg),
                collname: NodeList::nil(),
                location: loc,
            },
        )
        .unwrap()
    };
    assert_eq!(expr_location(collate(-1, 17)), -1);
    assert_eq!(expr_location(collate(5, 17)), 5);
    assert_eq!(expr_location(collate(30, 17)), 30);
}

// C's default arms are elog(ERROR, "unrecognized node type: %d") — XX000,
// catchable — in expression_tree_walker (nodeFuncs.c:2665-2667) and
// expression_tree_mutator (:3744-3746).
#[test]
fn walker_and_mutator_report_unrecognized_node_type_as_internal_error() {
    let ctx = cx();
    let mcx = ctx.mcx();
    // A raw-grammar node the analyzed-tree walker/mutator do not know.
    let rv = Node::mk(mcx, RangeVar::default()).unwrap();
    let expected = format!("unrecognized node type: {}", NodeTag::T_RangeVar as u16);

    let mut w = CountParams { analyzed: 0, raw: 0 };
    let err = expression_tree_walker(rv, &mut w).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
    assert_eq!(err.message(), expected);

    let err = expression_tree_mutator(mcx, rv, &mut |_| Ok(None)).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
    assert_eq!(err.message(), expected);
}

// C print_expr (print.c:361) reaches get_rte_attribute_name
// (parse_relation.c), whose out-of-range attnum is elog(ERROR, "invalid
// attnum %d for rangetable entry %s") — an error, not a panic.
#[test]
fn print_expr_invalid_attnum_is_an_internal_error() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let eref = Node::mk_mut(
        mcx,
        Alias {
            aliasname: Some("v"),
            colnames: NodeList::from_slice(mcx, &[Node::mk_string(mcx, "a").unwrap()]).unwrap(),
        },
    )
    .unwrap()
    .seal_ref();
    let rte = Node::mk(
        mcx,
        types_nodes::parsenodes::RangeTblEntry {
            rtekind: types_nodes::parsenodes::RTEKind::RTE_VALUES,
            eref: Some(eref),
            ..types_nodes::parsenodes::RangeTblEntry::default()
        },
    )
    .unwrap();
    let rtable = NodeList::from_slice(mcx, &[rte]).unwrap();
    let var = Node::mk(
        mcx,
        types_nodes::primnodes::Var { varno: 1, varattno: 7, vartype: 23, ..Default::default() },
    )
    .unwrap();
    let err = print::print_expr(mcx, Some(var), &rtable).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
    assert_eq!(err.message(), "invalid attnum 7 for rangetable entry v");
}

// C query_or_expression_tree_mutator (nodeFuncs.c:3965-3975) hands a Query
// to query_tree_mutator (nodeFuncs.c:3773-3856): the result is a fresh flat
// copy whose targetList and jointree quals carry the callback's replacements
// (MUTATE(query->targetList), MUTATE(query->jointree)); the input Query is
// left untouched. A C-shaped callback: replaces every Param, recurses
// through the engine for everything else.
#[test]
fn qoe_mutator_query_arm_runs_query_tree_mutator() {
    let ctx = cx();
    let mcx = ctx.mcx();
    fn bump_params<'mcx>(mcx: Mcx<'mcx>, n: Node<'mcx>) -> PgResult<Option<Node<'mcx>>> {
        if let Some(p) = n.as_param() {
            return Ok(Some(extern_param(mcx, p.paramid + 10)));
        }
        expression_tree_mutator(mcx, n, &mut |c| bump_params(mcx, c))
    }
    let te = Node::mk_target_entry(mcx, extern_param(mcx, 1), 1, None, false).unwrap();
    let jointree = Node::mk_mut(
        mcx,
        FromExpr { fromlist: NodeList::nil(), quals: Some(extern_param(mcx, 2)) },
    )
    .unwrap()
    .seal_ref();
    let q = Node::mk(
        mcx,
        Query {
            targetList: NodeList::from_slice(mcx, &[te]).unwrap(),
            jointree: Some(jointree),
            ..Query::default()
        },
    )
    .unwrap();
    let out = query_or_expression_tree_mutator(mcx, q, &mut |n| bump_params(mcx, n), 0)
        .unwrap_or_else(|e| panic!("Query arm must run query_tree_mutator: {}", e.message()))
        .expect("query_tree_mutator returns the (flat-copied) Query");
    let nq = out.as_query().expect("a Query comes back");
    let param_of = |te: Node<'_>| te.as_target_entry().unwrap().expr.as_param().unwrap().paramid;
    assert_eq!(param_of(nq.targetList.nth(0)), 11);
    assert_eq!(nq.jointree.unwrap().quals.unwrap().as_param().unwrap().paramid, 12);
    // FLATCOPY(newquery, query, Query): the caller's Query is untouched.
    let oq = q.as_query().unwrap();
    assert!(!core::ptr::eq(oq, nq));
    assert_eq!(param_of(oq.targetList.nth(0)), 1);
    assert_eq!(oq.jointree.unwrap().quals.unwrap().as_param().unwrap().paramid, 2);
}

// ---- audit-18.6 wave 2 w2-018-nodes-1 (nodeFuncs.c query engines) ----

// C query_tree_walker runs the callback on the root jointree FromExpr itself
// (nodeFuncs.c:2720 `WALK(query->jointree)`). It is a typed `&FromExpr`
// here, offered through `visit_from_expr_ref`; before the hook the engine
// walked its fields directly and no walker could observe the root.
#[test]
fn query_tree_walker_offers_the_root_jointree_through_the_hook() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let te = Node::mk_target_entry(mcx, extern_param(mcx, 1), 1, None, false).unwrap();
    let jointree = Node::mk_mut(
        mcx,
        FromExpr { fromlist: NodeList::nil(), quals: Some(extern_param(mcx, 2)) },
    )
    .unwrap()
    .seal_ref();
    let query = Query {
        targetList: NodeList::from_slice(mcx, &[te]).unwrap(),
        jointree: Some(jointree),
        ..Query::default()
    };

    struct Roots {
        roots: usize,
        params: usize,
    }
    impl<'mcx> NodeWalker<'mcx> for Roots {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if node.node_tag() == NodeTag::T_Param {
                self.params += 1;
                return Ok(false);
            }
            expression_tree_walker(node, self)
        }
        fn visit_from_expr_ref(&mut self, f: &'mcx FromExpr<'mcx>) -> PgResult<bool> {
            self.roots += 1;
            // A C callback that intercepts T_FromExpr recurses itself.
            Ok(walk_list(&f.fromlist, self)? || walk_opt(f.quals, self)?)
        }
    }
    let mut w = Roots { roots: 0, params: 0 };
    assert!(!query_tree_walker(&query, &mut w, 0).unwrap());
    assert_eq!((w.roots, w.params), (1, 2));

    // The default hook keeps every existing walker's view: fields only.
    struct Plain(usize);
    impl<'mcx> NodeWalker<'mcx> for Plain {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if node.node_tag() == NodeTag::T_Param {
                self.0 += 1;
                return Ok(false);
            }
            expression_tree_walker(node, self)
        }
    }
    let mut plain = Plain(0);
    assert!(!query_tree_walker(&query, &mut plain, 0).unwrap());
    assert_eq!(plain.0, 2);

    // An intercepting walker may stop at the root (C: the callback returns
    // true from WALK(query->jointree)).
    struct StopAtRoot;
    impl<'mcx> NodeWalker<'mcx> for StopAtRoot {
        fn visit(&mut self, _node: Node<'mcx>) -> PgResult<bool> {
            Ok(false)
        }
        fn visit_from_expr_ref(&mut self, _f: &'mcx FromExpr<'mcx>) -> PgResult<bool> {
            Ok(true)
        }
    }
    assert!(query_tree_walker(&query, &mut StopAtRoot, 0).unwrap());
}

// C query_tree_mutator / range_table_mutator (nodeFuncs.c:3773-3927): RTE
// subqueries reach the callback as Query nodes (a C-shaped callback recurses
// with query_tree_mutator), VALUES lists and WindowClause frame offsets are
// mutated (the latter outside QTW_EXAMINE_SORTGROUP), QTW_IGNORE_RT_SUBQUERIES
// copies the subquery as-is without consulting the callback, and
// QTW_DONT_COPY_QUERY scribbles the caller's Query in place.
#[test]
fn query_tree_mutator_covers_rtable_window_offsets_and_flags() {
    use types_nodes::parsenodes::{RTEKind, RangeTblEntry, WindowClause};
    let ctx = cx();
    let mcx = ctx.mcx();
    fn bump<'mcx>(mcx: Mcx<'mcx>, n: Node<'mcx>) -> PgResult<Option<Node<'mcx>>> {
        if let Some(p) = n.as_param() {
            return Ok(Some(extern_param(mcx, p.paramid + 10)));
        }
        if n.as_query().is_some() {
            return Ok(Some(query_tree_mutator(mcx, n, &mut |c| bump(mcx, c), 0)?));
        }
        expression_tree_mutator(mcx, n, &mut |c| bump(mcx, c))
    }
    let param_of = |te: Node<'_>| te.as_target_entry().unwrap().expr.as_param().unwrap().paramid;
    let te = Node::mk_target_entry(mcx, extern_param(mcx, 1), 1, None, false).unwrap();
    let sub = mcx::alloc_leak_in(
        mcx,
        Query { targetList: NodeList::from_slice(mcx, &[te]).unwrap(), ..Query::default() },
    )
    .unwrap();
    let rte_sub = Node::mk(
        mcx,
        RangeTblEntry {
            rtekind: RTEKind::RTE_SUBQUERY,
            subquery: Some(sub),
            ..RangeTblEntry::default()
        },
    )
    .unwrap();
    let values_row =
        Node::mk_list(mcx, NodeList::from_slice(mcx, &[extern_param(mcx, 2)]).unwrap()).unwrap();
    let rte_values = Node::mk(
        mcx,
        RangeTblEntry {
            rtekind: RTEKind::RTE_VALUES,
            values_lists: NodeList::from_slice(mcx, &[values_row]).unwrap(),
            ..RangeTblEntry::default()
        },
    )
    .unwrap();
    let wc = Node::mk(
        mcx,
        WindowClause { startOffset: Some(extern_param(mcx, 3)), ..WindowClause::default() },
    )
    .unwrap();
    let q = Node::mk(
        mcx,
        Query {
            rtable: NodeList::from_slice(mcx, &[rte_sub, rte_values]).unwrap(),
            windowClause: NodeList::from_slice(mcx, &[wc]).unwrap(),
            ..Query::default()
        },
    )
    .unwrap();

    let out = query_tree_mutator(mcx, q, &mut |n| bump(mcx, n), 0).unwrap();
    let nq = out.as_query().unwrap();
    assert!(!core::ptr::eq(nq, q.as_query().unwrap()), "FLATCOPY: a fresh Query");
    let rte = |i: usize| nq.rtable.nth(i).as_range_tbl_entry().unwrap();
    assert_eq!(param_of(rte(0).subquery.unwrap().targetList.nth(0)), 11);
    assert_eq!(
        rte(1).values_lists.nth(0).as_list().unwrap().nth(0).as_param().unwrap().paramid,
        12
    );
    let start = nq.windowClause.nth(0).as_window_clause().unwrap().startOffset.unwrap();
    assert_eq!(start.as_param().unwrap().paramid, 13);
    assert_eq!(param_of(sub.targetList.nth(0)), 1, "the caller's subquery is untouched");

    let out = query_tree_mutator(mcx, q, &mut |n| bump(mcx, n), QTW_IGNORE_RT_SUBQUERIES).unwrap();
    let copied = out.as_query().unwrap().rtable.nth(0).as_range_tbl_entry().unwrap().subquery.unwrap();
    assert!(!core::ptr::eq(copied, sub), "copyObject(rte->subquery)");
    assert_eq!(param_of(copied.targetList.nth(0)), 1);

    let out = query_tree_mutator(mcx, q, &mut |n| bump(mcx, n), QTW_DONT_COPY_QUERY).unwrap();
    let oq = q.as_query().unwrap();
    assert!(core::ptr::eq(out.as_query().unwrap(), oq), "in place: the same Query");
    let sub_now = oq.rtable.nth(0).as_range_tbl_entry().unwrap().subquery.unwrap();
    assert_eq!(param_of(sub_now.targetList.nth(0)), 11);
}

fn values_rte<'mcx>(mcx: Mcx<'mcx>, alias: &'mcx str, cols: &[&'mcx str]) -> Node<'mcx> {
    use types_nodes::parsenodes::{RTEKind, RangeTblEntry};
    use types_nodes::primnodes::Alias;
    let mut colnames = Vec::new();
    for c in cols {
        colnames.push(Node::mk_string(mcx, c).unwrap());
    }
    let eref = Node::mk_mut(
        mcx,
        Alias { aliasname: Some(alias), colnames: NodeList::from_slice(mcx, &colnames).unwrap() },
    )
    .unwrap()
    .seal_ref();
    Node::mk(
        mcx,
        RangeTblEntry { rtekind: RTEKind::RTE_VALUES, eref: Some(eref), ..RangeTblEntry::default() },
    )
    .unwrap()
}

// C print_pathkeys (print.c:430-465): "(" per pathkey "(" the members of the
// CANONICAL EquivalenceClass (ec_merged chased) through print_expr, ", "
// separated ")" ... ")\n"; a NULL member expression prints "<>".
#[test]
fn print_pathkeys_walks_canonical_eclass_members_in_c_format() {
    use types_nodes::primnodes::Var;
    use types_pathnodes::relids::relids_singleton;
    use types_pathnodes::{
        EquivalenceClass, EquivalenceMember, PathKey, PlannerInfo, COMPARE_LT, NodeId,
    };
    let ctx = cx();
    let mcx = ctx.mcx();
    let rtable = NodeList::from_slice(
        mcx,
        &[values_rte(mcx, "t", &["a", "b"]), values_rte(mcx, "u", &["c"])],
    )
    .unwrap();
    let mut root = PlannerInfo::new(mcx);
    // varno 0 = a member whose em_expr is the NULL handle
    fn member<'mcx>(
        mcx: Mcx<'mcx>,
        root: &mut PlannerInfo<'mcx>,
        varno: i32,
        varattno: i16,
    ) -> types_pathnodes::EmId {
        let em_expr = if varno == 0 {
            NodeId::default()
        } else {
            let var = Node::mk(mcx, Var { varno, varattno, vartype: 23, ..Default::default() })
                .unwrap();
            root.alloc_expr_node(var)
        };
        root.alloc_em(EquivalenceMember {
            em_expr,
            em_relids: relids_singleton(mcx, varno.max(1) as u32),
            em_datatype: 23,
            ..Default::default()
        })
    }
    let m_ta = member(mcx, &mut root, 1, 1);
    let m_uc = member(mcx, &mut root, 2, 1);
    let m_tb = member(mcx, &mut root, 1, 2);
    let m_null = member(mcx, &mut root, 0, 0);
    let mut canonical = EquivalenceClass::new(mcx);
    canonical.ec_members.push(m_ta);
    canonical.ec_members.push(m_uc);
    let canonical = root.alloc_ec(canonical);
    // a stale (merged-away) EC still reachable from a non-canonical PathKey
    let mut merged = EquivalenceClass::new(mcx);
    merged.ec_members.push(m_tb);
    merged.ec_merged = Some(canonical);
    let merged = root.alloc_ec(merged);
    let mut second = EquivalenceClass::new(mcx);
    second.ec_members.push(m_tb);
    second.ec_members.push(m_null);
    let second = root.alloc_ec(second);
    let pathkey = |ec| PathKey {
        pk_eclass: Some(ec),
        pk_opfamily: 1976,
        pk_cmptype: COMPARE_LT,
        pk_nulls_first: false,
    };

    let text = print::format_pathkeys(mcx, &root, &[pathkey(merged), pathkey(second)], &rtable)
        .unwrap();
    assert_eq!(core::str::from_utf8(&text).unwrap(), "((t.a, u.c), (t.b, <>))\n");
    let text = print::format_pathkeys(mcx, &root, &[], &rtable).unwrap();
    assert_eq!(core::str::from_utf8(&text).unwrap(), "()\n");
}

// C print_slot (print.c:496-511): TupIsNull (NULL or empty slot) prints
// "tuple is null.", a slot without a descriptor "no tuple descriptor.";
// only a filled slot reaches debugtup.
#[test]
fn print_slot_preamble_matches_c_early_returns() {
    use std::rc::Rc;
    use types_slot::{SlotData, TupleSlotKind, TupleTableSlot, VirtualTupleTableSlot};
    use types_tuple::TupleDescData;
    let ctx = cx();
    let mcx = ctx.mcx();
    let mut slot = SlotData::Virtual(VirtualTupleTableSlot {
        base: TupleTableSlot::new_in(mcx, TupleSlotKind::Virtual),
        data: mcx::PgVec::new_in(mcx),
    });
    assert!(slot.base().is_empty());
    assert_eq!(print::slot_preamble(&slot), Some("tuple is null.\n"));
    slot.base_mut().mark_not_empty();
    assert_eq!(print::slot_preamble(&slot), Some("no tuple descriptor.\n"));
    // set_descriptor requires an empty slot (ExecSetSlotDescriptor clears it)
    slot.base_mut().mark_empty();
    let desc = Rc::new(TupleDescData {
        natts: 0,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: mcx::PgVec::new_in(mcx),
        attrs: mcx::PgVec::new_in(mcx),
    });
    slot.base_mut().set_descriptor(mcx, desc);
    slot.base_mut().mark_not_empty();
    assert_eq!(print::slot_preamble(&slot), None);
    assert!(print::print_slot(None).is_ok());
}

#[test]
fn expr_collation_next_value_expr_is_invalid_oid() {
    let ctx = cx();
    let nve = Node::mk(
        ctx.mcx(),
        types_nodes::primnodes::NextValueExpr { seqid: 16385, typeId: 23 },
    )
    .unwrap();
    assert_eq!(node_funcs::expr_type(nve), 23);
    assert_eq!(node_funcs::expr_collation(nve), types_core::InvalidOid);
}

// print.c wraps at byte 78 regardless of character boundaries; a multibyte
// identifier straddling the wrap must not panic the formatter.
#[test]
fn node_dump_wrap_inside_a_multibyte_char_does_not_panic() {
    let ctx = cx();
    let mcx = ctx.mcx();
    let dump = format!("{}é{}", "x".repeat(77), "y".repeat(10));
    let flat = print::format_node_dump(mcx, &dump).unwrap();
    let lines: Vec<&str> = flat.as_str().lines().collect();
    assert_eq!(lines.len(), 2, "{flat}");
    assert_eq!(lines[0], format!("{}\u{FFFD}", "x".repeat(77)));
    assert_eq!(lines[1], format!("\u{FFFD}{}", "y".repeat(10)));
    let pretty = print::pretty_format_node_dump(mcx, &format!("{{A :b {dump}}}")).unwrap();
    assert!(pretty.as_str().lines().all(|l| l.len() <= 78 + 3), "{pretty}");
}
