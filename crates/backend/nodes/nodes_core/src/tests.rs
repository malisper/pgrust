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
