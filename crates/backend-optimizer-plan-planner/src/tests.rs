//! Unit tests for the #159 Node↔Expr keystone wiring in `subquery_planner`'s
//! expression-preprocessing block. These prove that `preprocess_expression`
//! (eval_const_expressions / canonicalize_qual) and `preprocess_qual_conditions`
//! run end-to-end over the now concretely-typed `Query` expression fields — the
//! step that previously panicked unconditionally for every query.

extern crate alloc;

use alloc::vec;

use mcx::{MemoryContext, PgBox, PgVec};
use types_nodes::nodes::Node;
use types_nodes::primnodes::{Expr, Var};
use types_nodes::rawnodes::{FromExpr, RangeTblRef};
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{
    NodeTag, Path, PathNode, PathTarget, PlannerInfo, RelId, RelOptInfo, VOLATILITY_UNKNOWN,
};

use crate::{
    apply_scanjoin_target_to_paths, preprocess_expression, preprocess_qual_conditions,
    EXPRKIND_QUAL, EXPRKIND_TARGET,
};

/// A non-null boolean `Const` (the canonical leaf the limit/qual paths see after
/// const-folding).
fn bool_const(value: bool) -> Expr {
    Expr::Const(backend_nodes_core::makefuncs::make_bool_const(value, false))
}

/// `preprocess_expression` over a bare `Const` returns the (folded) `Const`
/// rather than panicking — the EXPRKIND_TARGET path runs `eval_const_expressions`.
#[test]
fn preprocess_expression_target_const_roundtrips() {
    let cx = MemoryContext::new("planner-test");
    let mcx = cx.mcx();
    let root = PlannerInfo::default();
    let out = preprocess_expression(mcx, &root, Some(bool_const(true)), EXPRKIND_TARGET)
        .expect("preprocess_expression must not error");
    match out {
        Some(Expr::Const(_)) => {}
        other => panic!("expected folded Const, got {other:?}"),
    }
}

/// `preprocess_expression(None, _)` short-circuits to `None`.
#[test]
fn preprocess_expression_none_is_none() {
    let cx = MemoryContext::new("planner-test2");
    let mcx = cx.mcx();
    let root = PlannerInfo::default();
    let out = preprocess_expression(mcx, &root, None, EXPRKIND_QUAL)
        .expect("preprocess_expression must not error");
    assert!(out.is_none());
}

/// `preprocess_qual_conditions` over a `FromExpr` whose `quals` is a
/// `Node::Expr(Const)` (the analyzed-jointree shape for `WHERE <bool const>`)
/// preprocesses the qual in place and keeps it wrapped as `Node::Expr` — the
/// jointree-quals bridge the keystone unblocks.
#[test]
fn preprocess_qual_conditions_fromexpr_qual() {
    let cx = MemoryContext::new("planner-test3");
    let mcx = cx.mcx();
    let root = PlannerInfo::default();

    // FromExpr { fromlist: [], quals: Some(Node::Expr(Const true)) }.
    let qual = PgBox::new_in(Node::Expr(bool_const(true)), mcx);
    let from = FromExpr {
        fromlist: PgVec::new_in(mcx),
        quals: Some(qual),
    };
    let mut node = Node::mk_from_expr(mcx, from);

    preprocess_qual_conditions(mcx, &root, &mut node)
        .expect("preprocess_qual_conditions must not panic for a Const qual");

    match node.into_fromexpr() {
        Some(f) => match f.quals.as_deref() {
            Some(n) if n.is_const() => {}
            other => panic!("expected re-wrapped Node::Expr(Const) qual, got {other:?}"),
        },
        None => panic!("jointree top must stay a FromExpr"),
    }
}

/// A `RangeTblRef` leaf in the jointree (the single-table FROM shape) has no
/// quals to process; `preprocess_qual_conditions` is a no-op.
#[test]
fn preprocess_qual_conditions_rangetblref_noop() {
    let cx = MemoryContext::new("planner-test4");
    let mcx = cx.mcx();
    let root = PlannerInfo::default();
    let mut node = Node::mk_range_tbl_ref(mcx, RangeTblRef { rtindex: 1 });
    preprocess_qual_conditions(mcx, &root, &mut node).expect("RangeTblRef leaf must be a no-op");
}

// ---------------------------------------------------------------------------
// grouping_planner spine helpers: apply_scanjoin_target_to_paths (planner.c)
// and SS_charge_for_initplans (subselect.c, re-exported by init-subselect).
// ---------------------------------------------------------------------------

/// Build a fresh `PathTarget` carrying the given expr handles (no sortgrouprefs).
fn target_with_exprs(exprs: alloc::vec::Vec<types_pathnodes::NodeId>) -> PathTarget {
    PathTarget {
        exprs,
        sortgrouprefs: alloc::vec::Vec::new(),
        cost: Default::default(),
        width: 0,
        has_volatile_expr: VOLATILITY_UNKNOWN,
    }
}

/// Build a plain base `Path` for `rel` with the given pathtarget.
fn base_path(rel: RelId, target: PathTarget) -> PathNode {
    PathNode::Path(Path {
        type_: NodeTag(0),
        pathtype: NodeTag(0),
        parent: rel,
        pathtarget: Some(alloc::boxed::Box::new(target)),
        param_info: None,
        parallel_aware: false,
        parallel_safe: false,
        parallel_workers: 0,
        rows: 0.0,
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: 0.0,
        pathkeys: alloc::vec::Vec::new(),
    })
}

/// `apply_scanjoin_target_to_paths` with `tlist_same_exprs = true` injects the
/// scan/join target's sortgrouprefs into the existing path's pathtarget and sets
/// the rel's reltarget to the scan/join target — without creating a projection
/// path.
#[test]
fn apply_scanjoin_target_same_exprs_injects_sortgrouprefs() {
    let cx = MemoryContext::new("apply-sjt-same");
    let mcx = cx.mcx();
    let mut root = PlannerInfo::default();
    let run = PlannerRun::new(mcx);

    // One expr handle, one rel, one path emitting a target over that expr.
    let e0 = root.alloc_node(Expr::Var(Var { varno: 1, ..Var::default() }));
    let rel = root.alloc_rel(RelOptInfo {
        reltarget: Some(alloc::boxed::Box::new(target_with_exprs(vec![e0]))),
        ..RelOptInfo::default()
    });
    let path = root.alloc_path(base_path(rel, target_with_exprs(vec![e0])));
    root.rel_mut(rel).pathlist.push(path);

    // Scan/join target: same exprs, but a nonzero sortgroupref to inject.
    let mut sjt = target_with_exprs(vec![e0]);
    sjt.sortgrouprefs = vec![5u32];

    apply_scanjoin_target_to_paths(&run, &mut root, rel, &sjt, /*parallel_safe=*/ true, /*same=*/ true)
        .expect("apply_scanjoin_target_to_paths must not error");

    // The existing path was kept (no projection path created) and its target's
    // sortgrouprefs were injected.
    assert_eq!(root.rel(rel).pathlist, vec![path]);
    let pt = root.path(path).base().pathtarget.as_ref().expect("pathtarget");
    assert_eq!(pt.sortgrouprefs, vec![5u32]);

    // The rel's reltarget is now the scan/join target.
    let rt = root.rel(rel).reltarget.as_ref().expect("reltarget");
    assert_eq!(rt.exprs, vec![e0]);
    assert_eq!(rt.sortgrouprefs, vec![5u32]);
}

/// `SS_charge_for_initplans` is a no-op (returns immediately, mutating nothing)
/// when the query has no initPlans — the simple-SELECT case.
#[test]
fn ss_charge_for_initplans_noop_without_initplans() {
    let mut root = PlannerInfo::default();
    let rel = root.alloc_rel(RelOptInfo::default());
    let path = root.alloc_path(base_path(rel, target_with_exprs(alloc::vec::Vec::new())));
    root.rel_mut(rel).pathlist.push(path);

    assert!(root.init_plans.is_empty());
    let before = root.path(path).base().total_cost;
    backend_optimizer_plan_init_subselect::finalize::SS_charge_for_initplans(&mut root, rel);
    // Costs unchanged.
    assert_eq!(root.path(path).base().total_cost, before);
    // Path list intact.
    assert_eq!(root.rel(rel).pathlist, vec![path]);
}
