use super::bestpath::{self, CostSelector};
use crate::backend::catalog::catalog::column_desc;
use crate::backend::optimizer::util;
use crate::backend::parser::{analyze_select_query_with_outer, parse_select};
use crate::backend::parser::analyze::LiteralDefaultCatalog;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::nodes::datum::Value;
use crate::include::nodes::pathnodes::{Path, PathKey, PathTarget, PlannerInfo, RelOptInfo, RelOptKind};
use crate::include::nodes::plannodes::{Plan, PlanEstimate};
use crate::include::nodes::primnodes::{
    AttrNumber, Expr, JoinType, OpExpr, OpExprKind, OrderByEntry, QueryColumn, RelationDesc,
    TargetEntry, Var,
    INNER_VAR, OUTER_VAR,
};

fn int4() -> SqlType {
    SqlType::new(SqlTypeKind::Int4)
}

fn bool_ty() -> SqlType {
    SqlType::new(SqlTypeKind::Bool)
}

fn var(varno: usize, attno: usize) -> crate::include::nodes::primnodes::Expr {
    crate::include::nodes::primnodes::Expr::Var(Var {
        varno,
        varattno: attno as AttrNumber,
        varlevelsup: 0,
        vartype: int4(),
    })
}

fn pathkey(expr: crate::include::nodes::primnodes::Expr) -> PathKey {
    PathKey {
        expr,
        ressortgroupref: 0,
        descending: false,
        nulls_first: None,
    }
}

fn pathkey_with_ref(expr: crate::include::nodes::primnodes::Expr, ressortgroupref: usize) -> PathKey {
    PathKey {
        expr,
        ressortgroupref,
        descending: false,
        nulls_first: None,
    }
}

fn eq(left: Expr, right: Expr) -> Expr {
    Expr::op_auto(OpExprKind::Eq, vec![left, right])
}

fn gt(left: Expr, right: Expr) -> Expr {
    Expr::op_auto(OpExprKind::Gt, vec![left, right])
}

fn is_special_user_var(expr: &Expr, varno: usize, index: usize) -> bool {
    matches!(
        expr,
        Expr::Var(Var {
            varno: actual_varno,
            varattno,
            varlevelsup: 0,
            ..
        }) if *actual_varno == varno && *varattno == (index + 1) as AttrNumber
    )
}

fn restrict(clause: Expr) -> crate::include::nodes::pathnodes::RestrictInfo {
    crate::include::nodes::pathnodes::RestrictInfo::new(clause.clone(), super::expr_relids(&clause))
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
            ressortgroupref: 0,
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

    let chosen = bestpath::choose_final_path(&rel, &[pathkey(var(1_000, 1))]).expect("final path");

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
fn projection_output_target_keeps_sortgrouprefs() {
    let projection = Path::Projection {
        plan_info: PlanEstimate::new(1.0, 1.5, 10.0, 1),
        slot_id: 20,
        input: Box::new(values_path(10, 1.0, 1.0)),
        targets: vec![
            TargetEntry::new("a", var(10, 1), int4(), 1).with_sort_group_ref(11),
            TargetEntry::new("b", var(10, 2), int4(), 2),
        ],
    };

    assert_eq!(projection.output_target().sortgrouprefs, vec![11, 0]);
}

#[test]
fn normalize_rte_path_preserves_projection_sortgrouprefs() {
    let catalog = LiteralDefaultCatalog;
    let ordered_projection = Path::OrderBy {
        plan_info: PlanEstimate::new(1.0, 1.5, 10.0, 1),
        input: Box::new(Path::Projection {
            plan_info: PlanEstimate::new(1.0, 1.2, 10.0, 1),
            slot_id: 20,
            input: Box::new(values_path(10, 1.0, 1.0)),
            targets: vec![
                TargetEntry::new("a", var(10, 1), int4(), 1).with_sort_group_ref(17),
                TargetEntry::new("b", var(10, 2), int4(), 2),
            ],
        }),
        items: vec![OrderByEntry {
            expr: var(20, 1),
            ressortgroupref: 17,
            descending: false,
            nulls_first: None,
        }],
    };
    let desc = RelationDesc {
        columns: vec![
            column_desc("a", int4(), true),
            column_desc("b", int4(), true),
        ],
    };

    let normalized = util::normalize_rte_path(1, &desc, ordered_projection, &catalog);

    assert_eq!(normalized.output_target().sortgrouprefs, vec![17, 0]);
    assert_eq!(normalized.pathkeys(), vec![pathkey_with_ref(var(1, 1), 17)]);
}

fn planner_info_for_sql(sql: &str) -> PlannerInfo {
    let catalog = LiteralDefaultCatalog;
    let stmt = parse_select(sql).expect("parse");
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog, &[], None, &[], &[]).expect("analyze");
    PlannerInfo::new(query)
}

#[test]
fn required_query_pathkeys_for_path_keeps_sortgroup_identified_keys() {
    let root = planner_info_for_sql("select column1 as a from (values (1)) v order by a");
    let sortgroupref = root.query_pathkeys[0].ressortgroupref;
    let path = Path::Projection {
        plan_info: PlanEstimate::new(1.0, 1.5, 10.0, 1),
        slot_id: 20,
        input: Box::new(values_path(10, 1.0, 1.0)),
        targets: vec![TargetEntry::new("a", var(10, 1), int4(), 1).with_sort_group_ref(sortgroupref)],
    };

    let required = util::required_query_pathkeys_for_path(&root, &path);

    assert_eq!(required, root.query_pathkeys);
    assert!(required.iter().all(|key| key.ressortgroupref != 0));
}

#[test]
fn projection_pathkeys_prefer_sortgroupref_identity() {
    let ordered = Path::OrderBy {
        plan_info: PlanEstimate::new(1.0, 1.5, 10.0, 1),
        input: Box::new(values_path(10, 1.0, 1.0)),
        items: vec![OrderByEntry {
            expr: var(10, 2),
            ressortgroupref: 17,
            descending: false,
            nulls_first: None,
        }],
    };
    let projection = Path::Projection {
        plan_info: PlanEstimate::new(1.0, 1.6, 10.0, 1),
        slot_id: 20,
        input: Box::new(ordered),
        targets: vec![
            TargetEntry::new("a", var(10, 1), int4(), 1),
            TargetEntry::new("b", var(10, 2), int4(), 2).with_sort_group_ref(17),
        ],
    };

    assert_eq!(projection.pathkeys(), vec![pathkey_with_ref(var(20, 2), 17)]);
}

#[test]
fn projection_pathkeys_follow_passthrough_position() {
    let projection = Path::Projection {
        plan_info: PlanEstimate::new(1.0, 1.6, 10.0, 1),
        slot_id: 20,
        input: Box::new(ordered_path(10, 1.0, 1.0, 2)),
        targets: vec![
            TargetEntry::new("a", var(10, 1), int4(), 1),
            TargetEntry::new("b", var(10, 2), int4(), 2),
        ],
    };

    assert_eq!(projection.pathkeys(), vec![pathkey(var(20, 2))]);
}

#[test]
fn projection_pathkeys_fall_back_to_expr_match_for_non_identity_projection() {
    let projection = Path::Projection {
        plan_info: PlanEstimate::new(1.0, 1.6, 10.0, 1),
        slot_id: 20,
        input: Box::new(ordered_path(10, 1.0, 1.0, 1)),
        targets: vec![
            TargetEntry::new("b", var(10, 2), int4(), 1),
            TargetEntry::new("a", var(10, 1), int4(), 2),
        ],
    };

    assert_eq!(projection.pathkeys(), vec![pathkey(var(20, 2))]);
}

#[test]
fn into_plan_projection_lowers_via_child_tlist_identity() {
    let input = Path::Projection {
        plan_info: PlanEstimate::new(1.0, 1.5, 10.0, 1),
        slot_id: 20,
        input: Box::new(values_path(10, 1.0, 1.0)),
        targets: vec![TargetEntry::new("a", var(10, 1), int4(), 1)],
    };
    let plan = Path::Projection {
        plan_info: PlanEstimate::new(1.0, 1.6, 10.0, 1),
        slot_id: 21,
        input: Box::new(input),
        targets: vec![TargetEntry::new("a", var(10, 1), int4(), 1)],
    }
    .into_plan();

    match plan {
        Plan::Projection { targets, .. } => {
            assert_eq!(targets.len(), 1);
            assert!(is_special_user_var(&targets[0].expr, OUTER_VAR, 0));
        }
        other => panic!("expected projection plan, got {other:?}"),
    }
}

#[test]
fn into_plan_filter_lowers_via_child_tlist_identity() {
    let input = Path::Projection {
        plan_info: PlanEstimate::new(1.0, 1.5, 10.0, 1),
        slot_id: 20,
        input: Box::new(values_path(10, 1.0, 1.0)),
        targets: vec![TargetEntry::new("a", var(10, 1), int4(), 1)],
    };
    let plan = Path::Filter {
        plan_info: PlanEstimate::new(1.0, 1.7, 10.0, 1),
        input: Box::new(input),
        predicate: gt(var(10, 1), Expr::Const(Value::Int32(0))),
    }
    .into_plan();

    match plan {
        Plan::Filter { predicate, .. } => match predicate {
            Expr::Op(op) => {
                assert!(is_special_user_var(&op.args[0], OUTER_VAR, 0));
                assert_eq!(op.args[1], Expr::Const(Value::Int32(0)));
            }
            other => panic!("expected filter op, got {other:?}"),
        },
        other => panic!("expected filter plan, got {other:?}"),
    }
}

#[test]
fn into_plan_order_by_lowers_via_child_sortgroupref() {
    let input = Path::Projection {
        plan_info: PlanEstimate::new(1.0, 1.5, 10.0, 1),
        slot_id: 20,
        input: Box::new(values_path(10, 1.0, 1.0)),
        targets: vec![TargetEntry::new("a", var(10, 1), int4(), 1).with_sort_group_ref(17)],
    };
    let plan = Path::OrderBy {
        plan_info: PlanEstimate::new(1.0, 1.7, 10.0, 1),
        input: Box::new(input),
        items: vec![OrderByEntry {
            expr: var(10, 1),
            ressortgroupref: 17,
            descending: false,
            nulls_first: None,
        }],
    }
    .into_plan();

    match plan {
        Plan::OrderBy { items, .. } => {
            assert_eq!(items.len(), 1);
            assert!(is_special_user_var(&items[0].expr, OUTER_VAR, 0));
        }
        other => panic!("expected order by plan, got {other:?}"),
    }
}

#[test]
fn required_query_pathkeys_for_path_falls_back_when_input_lacks_sortgroup_identity() {
    let root = planner_info_for_sql("select column1 as a from (values (1)) v order by a");
    let path = Path::Projection {
        plan_info: PlanEstimate::new(1.0, 1.5, 10.0, 1),
        slot_id: 20,
        input: Box::new(values_path(10, 1.0, 1.0)),
        targets: vec![TargetEntry::new("a", var(10, 1), int4(), 1)],
    };

    let required = util::required_query_pathkeys_for_path(&root, &path);
    let lowered = util::lower_pathkeys_for_path(&root, &path, &root.query_pathkeys);

    assert_eq!(required, lowered);
}

#[test]
fn required_query_pathkeys_for_path_falls_back_for_zero_ref_keys() {
    let mut root = planner_info_for_sql("select 1");
    root.query_pathkeys = vec![pathkey(var(10, 1))];
    let path = Path::Projection {
        plan_info: PlanEstimate::new(1.0, 1.5, 10.0, 1),
        slot_id: 20,
        input: Box::new(values_path(10, 1.0, 1.0)),
        targets: vec![TargetEntry::new("a", var(10, 1), int4(), 1)],
    };

    let required = util::required_query_pathkeys_for_path(&root, &path);

    assert_eq!(required, vec![pathkey(var(20, 1))]);
}

#[test]
fn rel_exposes_required_pathkey_identity_only_when_a_path_matches() {
    let root = planner_info_for_sql("select column1 as a from (values (1)) v order by a");
    let sortgroupref = root.query_pathkeys[0].ressortgroupref;
    let matching_path = Path::Projection {
        plan_info: PlanEstimate::new(1.0, 1.5, 10.0, 1),
        slot_id: 20,
        input: Box::new(values_path(10, 1.0, 1.0)),
        targets: vec![TargetEntry::new("a", var(10, 1), int4(), 1).with_sort_group_ref(sortgroupref)],
    };
    let non_matching_path = Path::Projection {
        plan_info: PlanEstimate::new(2.0, 2.5, 10.0, 1),
        slot_id: 21,
        input: Box::new(values_path(11, 2.0, 2.0)),
        targets: vec![TargetEntry::new("a", var(11, 1), int4(), 1)],
    };
    let mut rel = RelOptInfo::new(vec![1], RelOptKind::UpperRel, PathTarget::from_target_list(&[]));
    rel.add_path(non_matching_path.clone());
    assert!(!util::rel_exposes_required_pathkey_identity(&rel, &root.query_pathkeys));
    rel.add_path(matching_path.clone());
    assert!(util::path_exposes_required_pathkey_identity(&matching_path, &root.query_pathkeys));
    assert!(util::rel_exposes_required_pathkey_identity(&rel, &root.query_pathkeys));
}

#[test]
fn required_query_pathkeys_for_rel_falls_back_when_rel_lacks_identity() {
    let root = planner_info_for_sql("select column1 as a from (values (1)) v order by a");
    let path = Path::Projection {
        plan_info: PlanEstimate::new(1.0, 1.5, 10.0, 1),
        slot_id: 20,
        input: Box::new(values_path(10, 1.0, 1.0)),
        targets: vec![TargetEntry::new("a", var(10, 1), int4(), 1)],
    };
    let mut rel = RelOptInfo::new(vec![1], RelOptKind::UpperRel, PathTarget::from_target_list(&[]));
    rel.add_path(path);

    let required = util::required_query_pathkeys_for_rel(&root, &rel);
    let lowered = util::lower_pathkeys_for_rel(&root, &rel, &root.query_pathkeys);

    assert_eq!(required, lowered);
}

#[test]
fn required_query_pathkeys_for_rel_keeps_sortgroup_identified_keys_when_rel_has_matching_path() {
    let root = planner_info_for_sql("select column1 as a from (values (1)) v order by a");
    let sortgroupref = root.query_pathkeys[0].ressortgroupref;
    let path = Path::Projection {
        plan_info: PlanEstimate::new(1.0, 1.5, 10.0, 1),
        slot_id: 20,
        input: Box::new(values_path(10, 1.0, 1.0)),
        targets: vec![TargetEntry::new("a", var(10, 1), int4(), 1).with_sort_group_ref(sortgroupref)],
    };
    let mut rel = RelOptInfo::new(vec![1], RelOptKind::UpperRel, PathTarget::from_target_list(&[]));
    rel.add_path(path);

    let required = util::required_query_pathkeys_for_rel(&root, &rel);

    assert_eq!(required, root.query_pathkeys);
}

#[test]
fn join_input_rewrite_keeps_composite_expr_semantic_until_late_rewrite() {
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
        super::rewrite_semantic_expr_for_join_inputs(None, merged, &left, &right, &join_layout);

    assert_eq!(
        rewritten,
        Expr::Coalesce(Box::new(var(1, 1)), Box::new(var(1, 2)))
    );
}

#[test]
fn projection_rewrite_maps_semantic_var_to_current_projection_slot() {
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

    let rewritten = super::rewrite_semantic_expr_for_path(var(1, 1), &outer, &outer.output_vars());

    assert_eq!(rewritten, var(4, 1));
}

#[test]
fn join_input_rewrite_maps_var_through_projected_join_output_slot() {
    let right = Path::Projection {
        plan_info: PlanEstimate::new(1.0, 1.5, 10.0, 5),
        slot_id: 3,
        input: Box::new(values_path(4, 1.0, 1.0)),
        targets: vec![
            TargetEntry::new("a1", var(1, 1), int4(), 1),
            TargetEntry::new("a2", var(1, 2), int4(), 2),
            TargetEntry::new("b1", var(2, 1), int4(), 3),
            TargetEntry::new("b2", var(2, 2), int4(), 4),
            TargetEntry::new("c1", var(4, 1), int4(), 5),
        ],
    };
    let left = Path::Projection {
        plan_info: PlanEstimate::new(1.0, 1.5, 10.0, 1),
        slot_id: 6,
        input: Box::new(values_path(6, 1.0, 1.0)),
        targets: vec![TargetEntry::new("left", var(6, 1), int4(), 1)],
    };
    let expr = Expr::Op(Box::new(OpExpr {
        opno: 0,
        opfuncid: 0,
        op: OpExprKind::Eq,
        opresulttype: bool_ty(),
        args: vec![
            Expr::Coalesce(
                Box::new(var(4, 1)),
                Box::new(crate::include::nodes::primnodes::Expr::Const(Value::Int32(
                    1,
                ))),
            ),
            var(6, 1),
        ],
    }));
    let mut join_layout = left.output_vars();
    join_layout.extend(right.output_vars());

    let rewritten =
        super::rewrite_semantic_expr_for_join_inputs(None, expr, &left, &right, &join_layout);

    assert_eq!(
        rewritten,
        Expr::Op(Box::new(OpExpr {
            opno: 0,
            opfuncid: 0,
            op: OpExprKind::Eq,
            opresulttype: bool_ty(),
            args: vec![
                Expr::Coalesce(
                    Box::new(var(3, 5)),
                    Box::new(crate::include::nodes::primnodes::Expr::Const(Value::Int32(
                        1
                    ))),
                ),
                var(6, 1),
            ],
        }))
    );
}

#[test]
fn build_join_paths_emits_nested_loop_and_hash_join_for_equijoin() {
    let paths = super::build_join_paths(
        values_path(1, 1.0, 10.0),
        values_path(2, 2.0, 20.0),
        &[1],
        &[2],
        JoinType::Inner,
        vec![restrict(eq(var(1, 1), var(2, 1)))],
    );

    assert!(
        paths
            .iter()
            .any(|path| matches!(path, Path::NestedLoopJoin { .. }))
    );
    assert!(
        paths
            .iter()
            .any(|path| matches!(path, Path::HashJoin { .. }))
    );
}

#[test]
fn extract_hash_join_clauses_splits_residual_predicates() {
    let clauses = super::extract_hash_join_clauses(
        &[
            restrict(eq(var(1, 1), var(2, 1))),
            restrict(gt(var(1, 2), var(2, 2))),
        ],
        &[1],
        &[2],
    )
    .expect("hash join clauses");

    assert_eq!(
        clauses.hash_clauses,
        vec![restrict(eq(var(1, 1), var(2, 1)))]
    );
    assert_eq!(clauses.outer_hash_keys, vec![var(1, 1)]);
    assert_eq!(clauses.inner_hash_keys, vec![var(2, 1)]);
    assert_eq!(
        clauses.join_clauses,
        vec![restrict(gt(var(1, 2), var(2, 2)))]
    );
}

#[test]
fn build_join_paths_skips_hash_join_for_cross_and_non_equi_joins() {
    let cross_paths = super::build_join_paths(
        values_path(1, 1.0, 10.0),
        values_path(2, 2.0, 20.0),
        &[1],
        &[2],
        JoinType::Cross,
        vec![restrict(eq(var(1, 1), var(2, 1)))],
    );
    assert!(
        !cross_paths
            .iter()
            .any(|path| matches!(path, Path::HashJoin { .. }))
    );

    let non_equi_paths = super::build_join_paths(
        values_path(1, 1.0, 10.0),
        values_path(2, 2.0, 20.0),
        &[1],
        &[2],
        JoinType::Inner,
        vec![restrict(gt(var(1, 1), var(2, 1)))],
    );
    assert!(
        !non_equi_paths
            .iter()
            .any(|path| matches!(path, Path::HashJoin { .. }))
    );
}

#[test]
fn hash_join_path_lowers_to_hash_join_plan_with_hash_inner() {
    let plan = Path::HashJoin {
        plan_info: PlanEstimate::new(5.0, 15.0, 10.0, 4),
        left: Box::new(values_path(1, 1.0, 10.0)),
        right: Box::new(values_path(2, 2.0, 20.0)),
        kind: JoinType::Inner,
        hash_clauses: vec![restrict(eq(var(1, 1), var(2, 1)))],
        outer_hash_keys: vec![var(1, 1)],
        inner_hash_keys: vec![var(2, 1)],
        restrict_clauses: vec![
            restrict(eq(var(1, 1), var(2, 1))),
            restrict(gt(var(1, 2), var(2, 2))),
        ],
    }
    .into_plan();

    match plan {
        Plan::HashJoin {
            kind,
            right,
            hash_clauses,
            hash_keys,
            join_qual,
            qual,
            ..
        } => {
            assert_eq!(kind, JoinType::Inner);
            assert_eq!(hash_keys.len(), 1);
            assert!(is_special_user_var(&hash_keys[0], OUTER_VAR, 0));
            assert_eq!(hash_clauses.len(), 1);
            match &hash_clauses[0] {
                Expr::Op(op) => {
                    assert_eq!(op.op, OpExprKind::Eq);
                    assert_eq!(op.args.len(), 2);
                    assert!(is_special_user_var(&op.args[0], OUTER_VAR, 0));
                    assert!(is_special_user_var(&op.args[1], INNER_VAR, 0));
                }
                other => panic!("expected hash clause op, got {other:?}"),
            }
            assert_eq!(join_qual.len(), 1);
            match &join_qual[0] {
                Expr::Op(op) => {
                    assert_eq!(op.op, OpExprKind::Gt);
                    assert_eq!(op.args.len(), 2);
                    assert!(is_special_user_var(&op.args[0], OUTER_VAR, 1));
                    assert!(is_special_user_var(&op.args[1], INNER_VAR, 1));
                }
                other => panic!("expected join qual op, got {other:?}"),
            }
            assert!(qual.is_empty());
            match *right {
                Plan::Hash {
                    hash_keys,
                    input,
                    plan_info,
                } => {
                    assert_eq!(hash_keys.len(), 1);
                    assert!(is_special_user_var(&hash_keys[0], OUTER_VAR, 0));
                    assert_eq!(
                        plan_info.startup_cost.as_f64(),
                        input.plan_info().total_cost.as_f64()
                    );
                    assert_eq!(
                        plan_info.total_cost.as_f64(),
                        input.plan_info().total_cost.as_f64()
                    );
                }
                other => panic!("expected hash inner, got {:?}", other),
            }
        }
        other => panic!("expected hash join, got {:?}", other),
    }
}
