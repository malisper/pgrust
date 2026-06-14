//! Tests for the structural EXPLAIN slice.

use mcx::{alloc_in, MemoryContext, PgBox, PgVec};
use types_explain::{ExplainFormat, ExplainState};
use types_nodes::execnodes::PlanStateData;
use types_nodes::nodeindexscan::Plan;
use types_nodes::noderesult::{Result as ResultPlan, ResultState};
use types_nodes::nodes::Node;
use types_nodes::planstate::PlanStateNode;

fn empty_plan<'mcx>() -> Plan<'mcx> {
    Plan {
        startup_cost: 0.00,
        total_cost: 0.01,
        targetlist: None,
        qual: None,
        plan_rows: 1.0,
        parallel_aware: false,
        async_capable: false,
        plan_node_id: 0,
        plan_width: 4,
        lefttree: None,
        righttree: None,
        extParam: None,
        allParam: None,
    }
}

/// `EXPLAIN (COSTS ON)` of a bare `Result` plan node renders the node name and
/// the cost/rows/width line in TEXT format — the structural slice's core.
#[test]
fn result_node_text_with_costs() {
    let ctx = MemoryContext::new("explain-test");
    let mcx = ctx.mcx();

    // Build a Result plan node and its plan-state.
    let plan_node: PgBox<'_, Node<'_>> = alloc_in(
        mcx,
        Node::Result(ResultPlan {
            plan: empty_plan(),
            resconstantqual: None,
        }),
    )
    .unwrap();

    let mut ps = PlanStateData::default();
    // planstate->plan = (Plan *) node;
    ps.plan = Some(mcx::leak_in(plan_node));
    let result_state = ResultState {
        ps,
        resconstantqual: None,
        rs_done: false,
        rs_checkqual: false,
    };
    let planstate = PlanStateNode::Result(alloc_in(mcx, result_state).unwrap());

    let mut es = ExplainState::new_in(mcx);
    es.costs = true;
    es.format = ExplainFormat::EXPLAIN_FORMAT_TEXT;

    let ancestors: PgVec<'_, PgBox<'_, Node<'_>>> = PgVec::new_in(mcx);
    crate::walk::ExplainNode(&mut es, mcx, &planstate, &ancestors, None, None).unwrap();

    let out = es.str.as_str();
    assert!(out.contains("Result"), "output should name the node: {out:?}");
    assert!(
        out.contains("(cost=0.00..0.01 rows=1 width=4)"),
        "output should carry the cost line: {out:?}"
    );
}

/// A child-bearing plan recurses: a `Result` whose `lefttree` is another
/// `Result` renders both node lines with the indented `->` arrow on the child.
#[test]
fn nested_result_recursion_text() {
    let ctx = MemoryContext::new("explain-test");
    let mcx = ctx.mcx();

    // Outer Result plan node.
    let outer_plan: PgBox<'_, Node<'_>> = alloc_in(
        mcx,
        Node::Result(ResultPlan {
            plan: empty_plan(),
            resconstantqual: None,
        }),
    )
    .unwrap();
    let inner_plan: PgBox<'_, Node<'_>> = alloc_in(
        mcx,
        Node::Result(ResultPlan {
            plan: empty_plan(),
            resconstantqual: None,
        }),
    )
    .unwrap();

    // Inner plan-state.
    let mut inner_ps = PlanStateData::default();
    inner_ps.plan = Some(mcx::leak_in(inner_plan));
    let inner_state = PlanStateNode::Result(
        alloc_in(
            mcx,
            ResultState {
                ps: inner_ps,
                resconstantqual: None,
                rs_done: false,
                rs_checkqual: false,
            },
        )
        .unwrap(),
    );

    // Outer plan-state with lefttree = inner.
    let mut outer_ps = PlanStateData::default();
    outer_ps.plan = Some(mcx::leak_in(outer_plan));
    outer_ps.lefttree = Some(alloc_in(mcx, inner_state).unwrap());
    let planstate = PlanStateNode::Result(
        alloc_in(
            mcx,
            ResultState {
                ps: outer_ps,
                resconstantqual: None,
                rs_done: false,
                rs_checkqual: false,
            },
        )
        .unwrap(),
    );

    let mut es = ExplainState::new_in(mcx);
    es.costs = false;
    es.format = ExplainFormat::EXPLAIN_FORMAT_TEXT;

    let ancestors: PgVec<'_, PgBox<'_, Node<'_>>> = PgVec::new_in(mcx);
    crate::walk::ExplainNode(&mut es, mcx, &planstate, &ancestors, None, None).unwrap();

    let out = es.str.as_str();
    // Two "Result" lines, the child carrying the "->  " arrow.
    assert_eq!(out.matches("Result").count(), 2, "two node lines: {out:?}");
    assert!(out.contains("->  Result"), "child arrow: {out:?}");
}
