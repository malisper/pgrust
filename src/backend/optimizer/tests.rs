use super::bestpath::{self, CostSelector};
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::nodes::datum::Value;
use crate::include::nodes::pathnodes::{Path, PathKey, PathTarget, RelOptInfo, RelOptKind};
use crate::include::nodes::plannodes::PlanEstimate;
use crate::include::nodes::primnodes::{Expr, OpExpr, OpExprKind, OrderByEntry, QueryColumn, TargetEntry, Var};

fn int4() -> SqlType {
    SqlType::new(SqlTypeKind::Int4)
}

fn var(varno: usize, attno: usize) -> crate::include::nodes::primnodes::Expr {
    crate::include::nodes::primnodes::Expr::Var(Var {
        varno,
        varattno: attno,
        varlevelsup: 0,
        vartype: int4(),
    })
}

fn pathkey(expr: crate::include::nodes::primnodes::Expr) -> PathKey {
    PathKey {
        expr,
        descending: false,
        nulls_first: None,
    }
}

fn values_path(slot_id: usize, startup_cost: f64, total_cost: f64) -> Path {
    Path::Values {
        plan_info: PlanEstimate::new(startup_cost, total_cost, 10.0, 2),
        slot_id,
        rows: vec![vec![
            crate::include::nodes::primnodes::Expr::Const(Value::Int32(1)),
            crate::include::nodes::primnodes::Expr::Const(Value::Int32(2)),
        ]],
        output_columns: vec![
            QueryColumn {
                name: "a".into(),
                sql_type: int4(),
            },
            QueryColumn {
                name: "b".into(),
                sql_type: int4(),
            },
        ],
    }
}

fn ordered_path(slot_id: usize, startup_cost: f64, total_cost: f64, key_attno: usize) -> Path {
    Path::OrderBy {
        plan_info: PlanEstimate::new(startup_cost, total_cost, 10.0, 2),
        input: Box::new(values_path(slot_id, startup_cost, total_cost)),
        items: vec![OrderByEntry {
            expr: var(slot_id, key_attno),
            descending: false,
            nulls_first: None,
        }],
    }
}

#[test]
fn set_cheapest_tracks_startup_and_total_paths() {
    let mut rel = RelOptInfo::new(
        vec![1],
        RelOptKind::BaseRel,
        PathTarget::new(vec![var(1, 1), var(1, 2)]),
    );
    rel.add_path(values_path(1, 1.0, 10.0));
    rel.add_path(values_path(2, 5.0, 6.0));

    bestpath::set_cheapest(&mut rel);

    assert_eq!(
        rel.cheapest_startup_path()
            .expect("startup path")
            .plan_info()
            .startup_cost
            .as_f64(),
        1.0
    );
    assert_eq!(
        rel.cheapest_total_path()
            .expect("total path")
            .plan_info()
            .total_cost
            .as_f64(),
        6.0
    );
    assert_eq!(rel.rows, 10.0);
}

#[test]
fn cheapest_path_for_pathkeys_prefers_cheapest_matching_path() {
    let mut rel = RelOptInfo::new(
        vec![1],
        RelOptKind::BaseRel,
        PathTarget::new(vec![var(10, 1), var(10, 2)]),
    );
    let required = vec![pathkey(var(10, 1))];
    rel.add_path(values_path(10, 1.0, 1.0));
    rel.add_path(ordered_path(10, 4.0, 9.0, 1));
    rel.add_path(ordered_path(10, 3.0, 7.0, 1));

    let chosen = bestpath::get_cheapest_path_for_pathkeys(&rel, &required, CostSelector::Total)
        .expect("matching ordered path");

    assert_eq!(chosen.plan_info().total_cost.as_f64(), 7.0);
}

#[test]
fn choose_final_path_falls_back_to_cheapest_total_without_match() {
    let mut rel = RelOptInfo::new(
        vec![1],
        RelOptKind::BaseRel,
        PathTarget::new(vec![var(1, 1)]),
    );
    rel.add_path(values_path(1, 2.0, 2.0));
    rel.add_path(values_path(2, 1.0, 5.0));
    bestpath::set_cheapest(&mut rel);

    let chosen =
        bestpath::choose_final_path(&rel, &[pathkey(var(1_000, 1))]).expect("final path");

    assert_eq!(chosen.plan_info().total_cost.as_f64(), 2.0);
}

#[test]
fn projection_keeps_hidden_order_pathkeys() {
    let order_expr = var(10, 1);
    let ordered = ordered_path(10, 1.0, 1.0, 1);
    let projection = Path::Projection {
        plan_info: PlanEstimate::new(1.0, 1.5, 10.0, 1),
        slot_id: 20,
        input: Box::new(ordered),
        targets: vec![TargetEntry::new(
            "expr",
            crate::include::nodes::primnodes::Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::Add,
                opresulttype: int4(),
                args: vec![
                    order_expr.clone(),
                    crate::include::nodes::primnodes::Expr::Const(Value::Int32(1)),
                ],
            })),
            int4(),
            1,
        )],
    };

    assert_eq!(projection.pathkeys(), vec![pathkey(order_expr)]);
}

#[test]
fn join_input_rewrite_maps_whole_composite_expr_to_join_alias_slot() {
    let merged = Expr::Coalesce(Box::new(var(1, 1)), Box::new(var(1, 2)));
    let right = Path::Projection {
        plan_info: PlanEstimate::new(1.0, 1.5, 10.0, 1),
        slot_id: 30,
        input: Box::new(values_path(1, 1.0, 1.0)),
        targets: vec![TargetEntry::new("merged", merged.clone(), int4(), 1)],
    };
    let left = values_path(2, 1.0, 1.0);
    let mut join_layout = left.output_vars();
    join_layout.extend(right.output_vars());

    let rewritten =
        super::rewrite_semantic_expr_for_join_inputs(merged, &left, &right, &join_layout);

    assert_eq!(rewritten, var(30, 1));
}

#[test]
fn projection_rewrite_does_not_chase_plain_var_through_subquery_boundary() {
    let inner = Path::Projection {
        plan_info: PlanEstimate::new(1.0, 1.5, 10.0, 1),
        slot_id: 1_000_100,
        input: Box::new(values_path(1, 1.0, 1.0)),
        targets: vec![TargetEntry::new("name", var(1, 1), int4(), 1)],
    };
    let outer = Path::Projection {
        plan_info: PlanEstimate::new(1.5, 2.0, 10.0, 1),
        slot_id: 4,
        input: Box::new(inner),
        targets: vec![TargetEntry::new("name", var(1_000_100, 1), int4(), 1)],
    };

    let rewritten =
        super::rewrite_semantic_expr_for_path(var(1, 1), &outer, &outer.output_vars());

    assert_eq!(rewritten, var(1, 1));
}
