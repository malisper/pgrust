use super::bestpath::{self, CostSelector};
use crate::backend::catalog::Catalog;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::optimizer::pathnodes::rte_slot_id;
use crate::backend::optimizer::util;
use crate::backend::parser::analyze::LiteralDefaultCatalog;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::backend::parser::{analyze_select_query_with_outer, parse_select};
use crate::include::nodes::datum::Value;
use crate::include::nodes::pathnodes::{
    Path, PathKey, PathTarget, PlannerInfo, RelOptInfo, RelOptKind,
};
use crate::include::nodes::plannodes::{Plan, PlanEstimate};
use crate::include::nodes::primnodes::{
    Aggref, AttrNumber, Expr, INNER_VAR, JoinType, OUTER_VAR, OpExpr, OpExprKind, OrderByEntry,
    Param, ParamKind, QueryColumn, RelationDesc, TargetEntry, Var, WindowFrameBound, user_attrno,
};

fn int4() -> SqlType {
    SqlType::new(SqlTypeKind::Int4)
}

fn oid() -> SqlType {
    SqlType::new(SqlTypeKind::Oid)
}

fn regprocedure() -> SqlType {
    SqlType::new(SqlTypeKind::RegProcedure)
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

fn typed_var(
    varno: usize,
    attno: usize,
    vartype: SqlType,
) -> crate::include::nodes::primnodes::Expr {
    crate::include::nodes::primnodes::Expr::Var(Var {
        varno,
        varattno: attno as AttrNumber,
        varlevelsup: 0,
        vartype,
    })
}

fn pathkey(expr: crate::include::nodes::primnodes::Expr) -> PathKey {
    PathKey {
        expr,
        ressortgroupref: 0,
        descending: false,
        nulls_first: None,
        collation_oid: None,
    }
}

fn pathkey_with_ref(
    expr: crate::include::nodes::primnodes::Expr,
    ressortgroupref: usize,
) -> PathKey {
    PathKey {
        expr,
        ressortgroupref,
        descending: false,
        nulls_first: None,
        collation_oid: None,
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
    super::make_restrict_info(clause)
}

fn values_output_columns() -> Vec<QueryColumn> {
    vec![
        QueryColumn {
            name: "a".into(),
            sql_type: int4(),
            wire_type_oid: None,
        },
        QueryColumn {
            name: "b".into(),
            sql_type: int4(),
            wire_type_oid: None,
        },
    ]
}

fn values_path(slot_id: usize, startup_cost: f64, total_cost: f64) -> Path {
    let output_columns = values_output_columns();
    Path::Values {
        plan_info: PlanEstimate::new(startup_cost, total_cost, 10.0, 2),
        pathtarget: PathTarget::new(vec![var(slot_id, 1), var(slot_id, 2)]),
        slot_id,
        rows: vec![vec![
            crate::include::nodes::primnodes::Expr::Const(Value::Int32(1)),
            crate::include::nodes::primnodes::Expr::Const(Value::Int32(2)),
        ]],
        output_columns,
    }
}

fn projection_path(
    slot_id: usize,
    startup_cost: f64,
    total_cost: f64,
    input: Path,
    targets: Vec<TargetEntry>,
) -> Path {
    let pathtarget = PathTarget::from_target_list(&targets);
    Path::Projection {
        plan_info: PlanEstimate::new(startup_cost, total_cost, 10.0, targets.len()),
        pathtarget,
        slot_id,
        input: Box::new(input),
        targets,
    }
}

fn filter_path(startup_cost: f64, total_cost: f64, input: Path, predicate: Expr) -> Path {
    let pathtarget = input.semantic_output_target();
    Path::Filter {
        plan_info: PlanEstimate::new(startup_cost, total_cost, 10.0, input.columns().len()),
        pathtarget,
        input: Box::new(input),
        predicate,
    }
}

fn order_by_path(
    startup_cost: f64,
    total_cost: f64,
    input: Path,
    items: Vec<OrderByEntry>,
) -> Path {
    let pathtarget = input.semantic_output_target();
    Path::OrderBy {
        plan_info: PlanEstimate::new(startup_cost, total_cost, 10.0, input.columns().len()),
        pathtarget,
        input: Box::new(input),
        items,
    }
}

fn project_set_pathtarget(
    slot_id: usize,
    targets: &[crate::include::nodes::primnodes::ProjectSetTarget],
) -> PathTarget {
    PathTarget::new(
        targets
            .iter()
            .enumerate()
            .map(|(index, target)| match target {
                crate::include::nodes::primnodes::ProjectSetTarget::Scalar(entry) => {
                    entry.expr.clone()
                }
                crate::include::nodes::primnodes::ProjectSetTarget::Set { sql_type, .. } => {
                    Expr::Var(Var {
                        varno: slot_id,
                        varattno: user_attrno(index),
                        varlevelsup: 0,
                        vartype: *sql_type,
                    })
                }
            })
            .collect(),
    )
}

fn project_set_path(
    slot_id: usize,
    startup_cost: f64,
    total_cost: f64,
    input: Path,
    targets: Vec<crate::include::nodes::primnodes::ProjectSetTarget>,
) -> Path {
    let pathtarget = project_set_pathtarget(slot_id, &targets);
    Path::ProjectSet {
        plan_info: PlanEstimate::new(startup_cost, total_cost, 10.0, targets.len()),
        pathtarget,
        slot_id,
        input: Box::new(input),
        targets,
    }
}

fn join_output_columns(left: &Path, right: &Path) -> Vec<QueryColumn> {
    let mut output_columns = left.columns();
    output_columns.extend(right.columns());
    output_columns
}

fn join_pathtarget(left: &Path, right: &Path) -> PathTarget {
    let left_target = left.semantic_output_target();
    let right_target = right.semantic_output_target();
    let mut exprs = left_target.exprs;
    exprs.extend(right_target.exprs);
    let mut sortgrouprefs = left_target.sortgrouprefs;
    sortgrouprefs.extend(right_target.sortgrouprefs);
    PathTarget::with_sortgrouprefs(exprs, sortgrouprefs)
}

fn ordered_path(slot_id: usize, startup_cost: f64, total_cost: f64, key_attno: usize) -> Path {
    order_by_path(
        startup_cost,
        total_cost,
        values_path(slot_id, startup_cost, total_cost),
        vec![OrderByEntry {
            expr: var(slot_id, key_attno),
            ressortgroupref: 0,
            descending: false,
            nulls_first: None,
            collation_oid: None,
        }],
    )
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
    let projection = projection_path(
        20,
        1.0,
        1.5,
        ordered,
        vec![TargetEntry::new(
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
                collation_oid: None,
            })),
            int4(),
            1,
        )],
    );

    assert_eq!(projection.pathkeys(), vec![pathkey(order_expr)]);
}

#[test]
fn projection_output_target_keeps_sortgrouprefs() {
    let projection = projection_path(
        20,
        1.0,
        1.5,
        values_path(10, 1.0, 1.0),
        vec![
            TargetEntry::new("a", var(10, 1), int4(), 1).with_sort_group_ref(11),
            TargetEntry::new("b", var(10, 2), int4(), 2),
        ],
    );

    assert_eq!(projection.output_target().sortgrouprefs, vec![11, 0]);
}

#[test]
fn normalize_rte_path_preserves_projection_sortgrouprefs() {
    let catalog = LiteralDefaultCatalog;
    let ordered_projection = order_by_path(
        1.0,
        1.5,
        projection_path(
            20,
            1.0,
            1.2,
            values_path(10, 1.0, 1.0),
            vec![
                TargetEntry::new("a", var(10, 1), int4(), 1).with_sort_group_ref(17),
                TargetEntry::new("b", var(10, 2), int4(), 2),
            ],
        ),
        vec![OrderByEntry {
            expr: var(20, 1),
            ressortgroupref: 17,
            descending: false,
            nulls_first: None,
            collation_oid: None,
        }],
    );
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

#[test]
fn normalize_rte_path_records_passthrough_input_positions() {
    let catalog = LiteralDefaultCatalog;
    let desc = RelationDesc {
        columns: vec![
            column_desc("b", int4(), true),
            column_desc("a", int4(), true),
        ],
    };
    let input = projection_path(
        20,
        1.0,
        1.2,
        values_path(10, 1.0, 1.0),
        vec![
            TargetEntry::new("a", var(10, 1), int4(), 1),
            TargetEntry::new("b", var(10, 2), int4(), 2),
        ],
    );

    let normalized = super::util::project_to_slot_layout_internal(
        None,
        30,
        &desc,
        input,
        PathTarget::new(vec![var(10, 2), var(10, 1)]),
        &catalog,
    );

    match normalized {
        Path::Projection { targets, .. } => {
            assert_eq!(targets[0].input_resno, Some(2));
            assert_eq!(targets[1].input_resno, Some(1));
        }
        other => panic!("expected projection path, got {other:?}"),
    }
}

#[test]
fn normalize_rte_path_projects_internal_values_slots_to_rte_vars() {
    let catalog = LiteralDefaultCatalog;
    let desc = RelationDesc {
        columns: vec![
            column_desc("a", int4(), true),
            column_desc("b", int4(), true),
        ],
    };
    let normalized = util::normalize_rte_path(
        1,
        &desc,
        Path::Values {
            plan_info: PlanEstimate::new(1.0, 1.0, 1.0, 2),
            pathtarget: PathTarget::with_sortgrouprefs(vec![var(1, 1), var(1, 2)], vec![17, 0]),
            slot_id: rte_slot_id(1),
            rows: vec![vec![
                crate::include::nodes::primnodes::Expr::Const(Value::Int32(1)),
                crate::include::nodes::primnodes::Expr::Const(Value::Int32(2)),
            ]],
            output_columns: values_output_columns(),
        },
        &catalog,
    );

    assert_eq!(
        normalized.semantic_output_vars(),
        vec![var(1, 1), var(1, 2)]
    );
    assert_eq!(normalized.output_target().sortgrouprefs, vec![17, 0]);

    match normalized {
        Path::Projection {
            slot_id,
            targets,
            input,
            ..
        } => {
            assert_eq!(slot_id, 1);
            assert_eq!(targets[0].expr, var(1, 1));
            assert_eq!(targets[1].expr, var(1, 2));
            assert_eq!(targets[0].input_resno, Some(1));
            assert_eq!(targets[1].input_resno, Some(2));
            assert_eq!(targets[0].ressortgroupref, 17);
            assert!(
                matches!(*input, Path::Values { slot_id, .. } if slot_id == rte_slot_id(1)),
                "expected boundary projection to wrap the internal values slot"
            );
        }
        other => panic!("expected projection, got {other:?}"),
    }
}

fn planner_info_for_sql(sql: &str) -> PlannerInfo {
    let catalog = LiteralDefaultCatalog;
    let stmt = parse_select(sql).expect("parse");
    let (query, _) = analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[])
        .expect("analyze");
    PlannerInfo::new(query)
}

fn planned_stmt_for_sql(sql: &str) -> crate::include::nodes::plannodes::PlannedStmt {
    let catalog = LiteralDefaultCatalog;
    planned_stmt_for_sql_with_catalog(sql, &catalog)
}

fn planned_stmt_for_sql_with_catalog(
    sql: &str,
    catalog: &dyn crate::backend::parser::CatalogLookup,
) -> crate::include::nodes::plannodes::PlannedStmt {
    let stmt = parse_select(sql).expect("parse");
    let (query, _) = analyze_select_query_with_outer(&stmt, catalog, &[], None, None, &[], &[])
        .expect("analyze");
    super::planner(query, catalog).expect("plan")
}

fn planned_stmt_for_values_sql(sql: &str) -> crate::include::nodes::plannodes::PlannedStmt {
    let catalog = LiteralDefaultCatalog;
    let stmt = parse_select(sql).expect("parse");
    let (query, _) = analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[])
        .expect("analyze");
    super::planner(query, &catalog).expect("plan")
}

fn catalog_with_indexed_items() -> Catalog {
    let mut catalog = Catalog::default();
    let table = catalog
        .create_table(
            "items",
            RelationDesc {
                columns: vec![column_desc("id", int4(), false)],
            },
        )
        .expect("create test catalog relation");
    let index = catalog
        .create_index("items_id_idx", "items", false, &["id".into()])
        .expect("create test catalog index");
    catalog
        .set_index_ready_valid(index.relation_oid, true, true)
        .expect("mark test catalog index usable");
    catalog
        .set_relation_stats(table.relation_oid, 128, 10_000.0)
        .expect("seed test catalog table stats");
    catalog
        .set_relation_stats(index.relation_oid, 32, 10_000.0)
        .expect("seed test catalog index stats");
    catalog
}

fn catalog_with_people_and_pets() -> Catalog {
    let mut catalog = Catalog::default();
    catalog
        .create_table(
            "people",
            RelationDesc {
                columns: vec![column_desc("id", int4(), false)],
            },
        )
        .expect("create people table");
    catalog
        .create_table(
            "pets",
            RelationDesc {
                columns: vec![column_desc("owner_id", int4(), true)],
            },
        )
        .expect("create pets table");
    catalog
}

fn plan_contains(plan: &Plan, predicate: impl Copy + Fn(&Plan) -> bool) -> bool {
    if predicate(plan) {
        return true;
    }
    match plan {
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::Values { .. }
        | Plan::FunctionScan { .. }
        | Plan::WorkTableScan { .. } => false,
        Plan::Append { children, .. } | Plan::SetOp { children, .. } => {
            children.iter().any(|child| plan_contains(child, predicate))
        }
        Plan::Hash { input, .. }
        | Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::ProjectSet { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::BitmapHeapScan {
            bitmapqual: input, ..
        }
        | Plan::CteScan {
            cte_plan: input, ..
        } => plan_contains(input, predicate),
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::RecursiveUnion {
            anchor: left,
            recursive: right,
            ..
        } => plan_contains(left, predicate) || plan_contains(right, predicate),
    }
}

#[test]
fn outer_join_preserved_side_where_qual_pushes_to_base_scan() {
    let catalog = catalog_with_people_and_pets();
    let planned = planned_stmt_for_sql_with_catalog(
        "select p.id
         from people p left join pets q on q.owner_id = p.id
         where p.id = 1",
        &catalog,
    );

    assert!(
        plan_contains(&planned.plan_tree, |plan| {
            matches!(
                plan,
                Plan::Filter { input, .. }
                    if matches!(
                        input.as_ref(),
                        Plan::SeqScan { relation_name, .. } if relation_name == "people"
                    )
            )
        }),
        "expected preserved-side WHERE qual to be pushed to the people base scan, got {:?}",
        planned.plan_tree
    );
}

#[test]
fn outer_join_paths_keep_logical_left_orientation() {
    let catalog = catalog_with_people_and_pets();
    let planned = planned_stmt_for_sql_with_catalog(
        "select p.id
         from people p join people p2 on p.id = p2.id
           left join pets q on q.owner_id = p.id",
        &catalog,
    );

    assert!(
        !plan_contains(&planned.plan_tree, |plan| {
            matches!(
                plan,
                Plan::NestedLoopJoin {
                    kind: JoinType::Right,
                    ..
                } | Plan::HashJoin {
                    kind: JoinType::Right,
                    ..
                }
            )
        }),
        "expected special join path construction to keep logical left-join orientation, got {:?}",
        planned.plan_tree
    );
}

fn find_seq_scan(plan: &Plan) -> Option<&Plan> {
    match plan {
        Plan::SeqScan { .. } => Some(plan),
        Plan::Hash { input, .. }
        | Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::ProjectSet { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::BitmapHeapScan {
            bitmapqual: input, ..
        } => find_seq_scan(input),
        Plan::Append { children, .. } | Plan::SetOp { children, .. } => {
            children.iter().find_map(find_seq_scan)
        }
        Plan::NestedLoopJoin { left, right, .. } | Plan::HashJoin { left, right, .. } => {
            find_seq_scan(left).or_else(|| find_seq_scan(right))
        }
        Plan::Result { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::Values { .. }
        | Plan::FunctionScan { .. }
        | Plan::WorkTableScan { .. }
        | Plan::RecursiveUnion { .. }
        | Plan::CteScan { .. } => None,
    }
}

fn count_plan_nodes(plan: &Plan, predicate: impl Copy + Fn(&Plan) -> bool) -> usize {
    let here = usize::from(predicate(plan));
    here + match plan {
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::Values { .. }
        | Plan::FunctionScan { .. }
        | Plan::WorkTableScan { .. } => 0,
        Plan::Append { children, .. } | Plan::SetOp { children, .. } => children
            .iter()
            .map(|child| count_plan_nodes(child, predicate))
            .sum(),
        Plan::Hash { input, .. }
        | Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::ProjectSet { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::BitmapHeapScan {
            bitmapqual: input, ..
        }
        | Plan::CteScan {
            cte_plan: input, ..
        } => count_plan_nodes(input, predicate),
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::RecursiveUnion {
            anchor: left,
            recursive: right,
            ..
        } => count_plan_nodes(left, predicate) + count_plan_nodes(right, predicate),
    }
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        return (*message).to_string();
    }
    format!("{payload:?}")
}

#[test]
fn planned_rangefuncs_lateral_full_join_has_no_root_ext_params() {
    let sql = r#"
select *
from (values (1),(2)) v1(r1)
    left join lateral (
        select *
        from generate_series(1, v1.r1) as gs1
        left join lateral (
            select *
            from generate_series(1, gs1) as gs2
            left join generate_series(1, gs2) as gs3 on true
        ) as ss1 on true
        full join generate_series(1, v1.r1) as gs4 on false
    ) as ss0 on true
"#;
    let planned = planned_stmt_for_sql(sql);
    assert!(
        planned.ext_params.is_empty(),
        "unexpected root ext params: {:?}",
        planned.ext_params
    );
    assert!(plan_contains(&planned.plan_tree, |plan| {
        matches!(
            plan,
            Plan::NestedLoopJoin {
                kind: crate::include::nodes::primnodes::JoinType::Left,
                ..
            }
        )
    }));
    assert!(!plan_contains(&planned.plan_tree, |plan| {
        matches!(
            plan,
            Plan::NestedLoopJoin {
                kind: crate::include::nodes::primnodes::JoinType::Full,
                nest_params,
                ..
            } if !nest_params.is_empty()
        )
    }));
}

#[test]
fn planned_correlated_cte_subquery_rebases_hidden_cte_boundary_params() {
    let planned = planned_stmt_for_values_sql(
        "select (
            with cte(foo) as (values (x))
            select (select foo from cte)
         )
         from (values (0), (1)) as t(x)",
    );
    assert!(
        planned.ext_params.is_empty(),
        "unexpected root ext params: {:?}",
        planned.ext_params
    );
}

#[test]
fn planned_window_query_uses_projection_windowagg_orderby() {
    let planned = planned_stmt_for_values_sql(
        "select row_number() over (order by x) from (values (1), (2)) as t(x)",
    );
    match planned.plan_tree {
        Plan::Projection { input, .. } => match *input {
            Plan::WindowAgg { input, .. } => assert!(matches!(*input, Plan::OrderBy { .. })),
            other => panic!("expected WindowAgg below projection, got {other:?}"),
        },
        other => panic!("expected final projection, got {other:?}"),
    }
}

#[test]
fn planned_grouped_window_query_keeps_aggregate_below_windowagg() {
    let planned = planned_stmt_for_values_sql(
        "select x, sum(count(*)) over () from (values (1), (2)) as t(x) group by x order by x",
    );
    assert!(plan_contains(&planned.plan_tree, |plan| matches!(
        plan,
        Plan::Aggregate { .. }
    )));
    assert!(plan_contains(&planned.plan_tree, |plan| matches!(
        plan,
        Plan::WindowAgg { .. }
    )));
    match planned.plan_tree {
        Plan::Projection { input, .. } => match *input {
            Plan::OrderBy { input, .. } => match *input {
                Plan::WindowAgg { input, .. } => {
                    assert!(plan_contains(&input, |plan| matches!(
                        plan,
                        Plan::Aggregate { .. }
                    )));
                }
                other => panic!("expected WindowAgg under final order by, got {other:?}"),
            },
            other => panic!("expected final order by, got {other:?}"),
        },
        other => panic!("expected final projection, got {other:?}"),
    }
}

#[test]
fn planned_grouped_window_aggregate_uses_aggregate_output_slot() {
    let sql = "select x, y, sum(z) as gsum, sum(sum(z)) over (partition by y order by x) as wsum \
        from (values (1, 1, 10), (2, 1, 20), (1, 2, 7)) as t(x, y, z) group by x, y";
    let catalog = LiteralDefaultCatalog;
    let stmt = parse_select(sql).expect("parse");
    let (query, _) = analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[])
        .expect("analyze");
    let func = &query.window_clauses[0].functions[0];
    assert!(matches!(
        &func.kind,
        crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref)
            if matches!(aggref.args.as_slice(), [Expr::Aggref(_)])
    ));
    assert!(matches!(func.args.as_slice(), [Expr::Aggref(_)]));

    let planned = super::planner(query, &catalog).expect("plan");

    assert!(plan_contains(&planned.plan_tree, |plan| match plan {
        Plan::WindowAgg { clause, .. } => {
            clause.spec.partition_by.len() == 1
                && is_special_user_var(&clause.spec.partition_by[0], OUTER_VAR, 1)
                && clause.spec.order_by.len() == 1
                && is_special_user_var(&clause.spec.order_by[0].expr, OUTER_VAR, 0)
                && clause.functions.len() == 1
                && matches!(
                    &clause.functions[0].kind,
                    crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref)
                        if aggref.args.len() == 1
                            && is_special_user_var(&aggref.args[0], OUTER_VAR, 2)
                )
        }
        _ => false,
    }));
}

#[test]
fn planned_grouped_named_window_uses_named_spec() {
    let sql = "select x, y, sum(z) as gsum, sum(sum(z)) over win as wsum \
        from (values (1, 1, 10), (2, 1, 20), (1, 2, 7)) as t(x, y, z) \
        group by x, y window win as (partition by y order by x)";
    let catalog = LiteralDefaultCatalog;
    let stmt = parse_select(sql).expect("parse");
    let (query, _) = analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[])
        .expect("analyze");
    let planned = super::planner(query, &catalog).expect("plan");

    assert!(plan_contains(&planned.plan_tree, |plan| match plan {
        Plan::WindowAgg { clause, .. } => {
            clause.spec.partition_by.len() == 1
                && is_special_user_var(&clause.spec.partition_by[0], OUTER_VAR, 1)
                && clause.spec.order_by.len() == 1
                && is_special_user_var(&clause.spec.order_by[0].expr, OUTER_VAR, 0)
        }
        _ => false,
    }));
}

#[test]
fn planned_window_frame_offsets_from_join_input_are_lowered() {
    let planned = planned_stmt_for_sql(
        "select sum(t.x) over (order by t.x rows between u.y preceding and u.y following), t.x \
         from (values (1), (2)) as t(x), (values (1)) as u(y)",
    );

    assert!(plan_contains(&planned.plan_tree, |plan| match plan {
        Plan::WindowAgg { clause, .. } => {
            clause.spec.order_by.len() == 1
                && is_special_user_var(&clause.spec.order_by[0].expr, OUTER_VAR, 0)
                && matches!(
                    &clause.spec.frame.start_bound,
                    WindowFrameBound::OffsetPreceding(expr) if is_special_user_var(expr, OUTER_VAR, 1)
                )
                && matches!(
                    &clause.spec.frame.end_bound,
                    WindowFrameBound::OffsetFollowing(expr) if is_special_user_var(expr, OUTER_VAR, 1)
                )
        }
        _ => false,
    }));
}

#[test]
fn planned_distinct_window_specs_stack_windowagg_nodes() {
    let planned = planned_stmt_for_values_sql(
        "select row_number() over (order by x), rank() over (partition by x order by x) from (values (1), (2)) as t(x)",
    );
    assert_eq!(
        count_plan_nodes(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::WindowAgg { .. }
        )),
        2
    );
}

#[test]
fn executable_plan_validator_reports_node_and_field() {
    let plan = Plan::Projection {
        plan_info: PlanEstimate::new(1.0, 1.0, 1.0, 1),
        input: Box::new(values_path(10, 1.0, 1.0).into_plan()),
        targets: vec![TargetEntry::new(
            "bad",
            Expr::Aggref(Box::new(Aggref {
                aggfnoid: 0,
                aggtype: int4(),
                aggvariadic: false,
                aggdistinct: false,
                args: vec![],
                aggorder: vec![],
                aggfilter: None,
                agglevelsup: 0,
                aggno: 0,
            })),
            int4(),
            1,
        )],
    };

    let panic = std::panic::catch_unwind(|| {
        super::setrefs::validate_executable_plan_for_tests(&plan);
    })
    .expect_err("validator should reject planner-only expressions");

    let message = panic_message(panic);
    assert!(message.contains("Projection.targets"));
    assert!(message.contains("Aggref"));
}

#[test]
fn planner_path_validator_rejects_executor_only_refs() {
    let path = filter_path(
        1.0,
        1.0,
        values_path(10, 1.0, 1.0),
        Expr::Param(Param {
            paramkind: ParamKind::Exec,
            paramid: 1,
            paramtype: bool_ty(),
        }),
    );

    let panic = std::panic::catch_unwind(|| {
        super::setrefs::validate_planner_path_for_tests(&path);
    })
    .expect_err("validator should reject executor-only planner refs");

    let message = panic_message(panic);
    assert!(message.contains("Filter.predicate"));
    assert!(message.contains("PARAM_EXEC"));
}

#[test]
fn required_query_pathkeys_for_path_keeps_sortgroup_identified_keys() {
    let root = planner_info_for_sql("select column1 as a from (values (1)) v order by a");
    let sortgroupref = root.query_pathkeys[0].ressortgroupref;
    let path = projection_path(
        20,
        1.0,
        1.5,
        values_path(10, 1.0, 1.0),
        vec![TargetEntry::new("a", var(10, 1), int4(), 1).with_sort_group_ref(sortgroupref)],
    );

    let required = util::required_query_pathkeys_for_path(&root, &path);

    assert_eq!(required, root.query_pathkeys);
    assert!(required.iter().all(|key| key.ressortgroupref != 0));
}

#[test]
fn projection_pathkeys_prefer_sortgroupref_identity() {
    let ordered = order_by_path(
        1.0,
        1.5,
        values_path(10, 1.0, 1.0),
        vec![OrderByEntry {
            expr: var(10, 2),
            ressortgroupref: 17,
            descending: false,
            nulls_first: None,
            collation_oid: None,
        }],
    );
    let projection = projection_path(
        20,
        1.0,
        1.6,
        ordered,
        vec![
            TargetEntry::new("a", var(10, 1), int4(), 1),
            TargetEntry::new("b", var(10, 2), int4(), 2).with_sort_group_ref(17),
        ],
    );

    assert_eq!(
        projection.pathkeys(),
        vec![pathkey_with_ref(var(20, 2), 17)]
    );
}

#[test]
fn projection_pathkeys_follow_passthrough_position() {
    let projection = projection_path(
        20,
        1.0,
        1.6,
        ordered_path(10, 1.0, 1.0, 2),
        vec![
            TargetEntry::new("a", var(10, 1), int4(), 1),
            TargetEntry::new("b", var(10, 2), int4(), 2),
        ],
    );

    assert_eq!(projection.pathkeys(), vec![pathkey(var(10, 2))]);
}

#[test]
fn projection_pathkeys_fall_back_to_expr_match_for_non_identity_projection() {
    let projection = projection_path(
        20,
        1.0,
        1.6,
        ordered_path(10, 1.0, 1.0, 1),
        vec![
            TargetEntry::new("b", var(10, 2), int4(), 1),
            TargetEntry::new("a", var(10, 1), int4(), 2),
        ],
    );

    assert_eq!(projection.pathkeys(), vec![pathkey(var(10, 1))]);
}

#[test]
fn into_plan_projection_lowers_via_child_tlist_identity() {
    let input = projection_path(
        20,
        1.0,
        1.5,
        values_path(10, 1.0, 1.0),
        vec![TargetEntry::new("a", var(10, 1), int4(), 1)],
    );
    let plan = projection_path(
        21,
        1.0,
        1.6,
        input,
        vec![TargetEntry::new("a", var(10, 1), int4(), 1)],
    )
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
    let input = projection_path(
        20,
        1.0,
        1.5,
        values_path(10, 1.0, 1.0),
        vec![TargetEntry::new("a", var(10, 1), int4(), 1)],
    );
    let plan = filter_path(
        1.0,
        1.7,
        input,
        gt(var(10, 1), Expr::Const(Value::Int32(0))),
    )
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
    let input = projection_path(
        20,
        1.0,
        1.5,
        values_path(10, 1.0, 1.0),
        vec![TargetEntry::new("a", var(10, 1), int4(), 1).with_sort_group_ref(17)],
    );
    let plan = order_by_path(
        1.0,
        1.7,
        input,
        vec![OrderByEntry {
            expr: var(10, 1),
            ressortgroupref: 17,
            descending: false,
            nulls_first: None,
            collation_oid: None,
        }],
    )
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
fn into_plan_project_set_set_arg_lowers_via_child_tlist_identity() {
    let input = projection_path(
        20,
        1.0,
        1.5,
        values_path(10, 1.0, 1.0),
        vec![TargetEntry::new("a", var(10, 1), int4(), 1)],
    );
    let plan = project_set_path(
        21,
        1.0,
        1.7,
        input,
        vec![crate::include::nodes::primnodes::ProjectSetTarget::Set {
            name: "g".into(),
            source_expr: Expr::Const(Value::Null),
            call: crate::include::nodes::primnodes::SetReturningCall::GenerateSeries {
                func_oid: 1,
                func_variadic: false,
                start: var(10, 1),
                stop: Expr::Const(Value::Int32(3)),
                step: Expr::Const(Value::Int32(1)),
                output_columns: vec![QueryColumn {
                    name: "g".into(),
                    sql_type: int4(),
                    wire_type_oid: None,
                }],
                with_ordinality: false,
            },
            sql_type: int4(),
            column_index: 1,
        }],
    )
    .into_plan();

    match plan {
        Plan::ProjectSet { targets, .. } => match &targets[0] {
            crate::include::nodes::primnodes::ProjectSetTarget::Set { call, .. } => match call {
                crate::include::nodes::primnodes::SetReturningCall::GenerateSeries {
                    start,
                    stop,
                    step,
                    ..
                } => {
                    assert!(is_special_user_var(start, OUTER_VAR, 0));
                    assert_eq!(*stop, Expr::Const(Value::Int32(3)));
                    assert_eq!(*step, Expr::Const(Value::Int32(1)));
                }
                other => panic!("expected generate_series call, got {other:?}"),
            },
            other => panic!("expected set target, got {other:?}"),
        },
        other => panic!("expected project set plan, got {other:?}"),
    }
}

#[test]
fn planner_keeps_function_scan_filter_semantic_until_setrefs() {
    let planned = planned_stmt_for_sql("select * from generate_series(1, 3) as g(x) where x > 1");

    match planned.plan_tree {
        Plan::Filter {
            predicate, input, ..
        } => {
            match predicate {
                Expr::Op(op) => {
                    assert!(is_special_user_var(&op.args[0], OUTER_VAR, 0));
                    assert_eq!(op.args[1], Expr::Const(Value::Int32(1)));
                }
                other => panic!("expected filter op, got {other:?}"),
            }
            assert!(matches!(
                *input,
                Plan::FunctionScan { .. } | Plan::Projection { .. }
            ));
        }
        other => panic!("expected top-level filter plan, got {other:?}"),
    }
}

#[test]
fn planner_places_lock_rows_between_order_by_and_limit() {
    let mut catalog = Catalog::default();
    catalog
        .create_table(
            "items",
            RelationDesc {
                columns: vec![column_desc("id", int4(), false)],
            },
        )
        .expect("create table");
    let stmt = parse_select("select id from items order by id limit 1 for update").expect("parse");
    let (query, _) = analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[])
        .expect("analyze");
    let planned = super::planner(query, &catalog).expect("plan");

    let Plan::Limit { input, .. } = &planned.plan_tree else {
        panic!("expected limit at top, got {:?}", planned.plan_tree);
    };
    let Plan::LockRows {
        input, row_marks, ..
    } = input.as_ref()
    else {
        panic!("expected lock rows below limit, got {:?}", input);
    };
    assert!(matches!(input.as_ref(), Plan::OrderBy { .. }));
    assert_eq!(row_marks.len(), 1);
    assert_eq!(row_marks[0].rtindex, 1);
    assert_eq!(
        row_marks[0].strength,
        crate::include::nodes::parsenodes::SelectLockingClause::ForUpdate
    );
}

#[test]
fn planner_keeps_recursive_cte_filter_semantic_until_setrefs() {
    let planned = planned_stmt_for_sql(
        "with recursive t(n) as (values (1) union all select n + 1 from t where n < 3) \
         select * from t where n > 1",
    );

    assert!(plan_contains(&planned.plan_tree, |plan| matches!(
        plan,
        Plan::CteScan { .. }
    )));
    assert!(plan_contains(&planned.plan_tree, |plan| match plan {
        Plan::Filter { predicate, .. } => match predicate {
            Expr::Op(op) => {
                is_special_user_var(&op.args[0], OUTER_VAR, 0)
                    && op.args[1] == Expr::Const(Value::Int32(1))
            }
            _ => false,
        },
        _ => false,
    }));
}

#[test]
fn planner_keeps_recursive_project_set_scalar_semantic_until_setrefs() {
    let planned = planned_stmt_for_sql(
        "with recursive t(n) as (values (1) union all select n + 1 from t where n < 2) \
         select n + 1, generate_series(1, n) from t",
    );

    assert!(plan_contains(&planned.plan_tree, |plan| matches!(
        plan,
        Plan::ProjectSet { .. }
    )));
    assert!(plan_contains(&planned.plan_tree, |plan| match plan {
        Plan::ProjectSet { targets, .. } => targets.iter().any(|target| match target {
            crate::include::nodes::primnodes::ProjectSetTarget::Scalar(entry) =>
                match &entry.expr {
                    Expr::Op(op) => {
                        is_special_user_var(&op.args[0], OUTER_VAR, 0)
                            && op.args[1] == Expr::Const(Value::Int32(1))
                    }
                    _ => false,
                },
            crate::include::nodes::primnodes::ProjectSetTarget::Set { .. } => false,
        }),
        _ => false,
    }));
}

#[test]
fn planner_lowers_setop_children_with_their_own_roots() {
    let planned = planned_stmt_for_sql(
        "select x
         from (values (1)) base(x)
         union all
         select l.x + r.y
         from (values (1)) l(x)
         join (values (2)) r(y) on true",
    );

    assert!(matches!(planned.plan_tree, Plan::SetOp { .. }));
}

#[test]
fn planner_uses_metadata_fallback_when_live_pages_are_unavailable() {
    let mut catalog = Catalog::default();
    catalog
        .create_table(
            "items",
            RelationDesc {
                columns: vec![column_desc("id", int4(), false)],
            },
        )
        .expect("create test catalog relation");

    let stmt = parse_select("select * from items").expect("parse");
    let (query, _) = analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[])
        .expect("analyze");
    let planned = super::planner(query, &catalog).expect("plan");

    match find_seq_scan(&planned.plan_tree).expect("seq scan plan") {
        Plan::SeqScan { plan_info, .. } => assert_eq!(plan_info.plan_rows.as_f64(), 1000.0),
        other => panic!("expected seq scan plan, got {other:?}"),
    }
}

#[test]
fn planner_rewrites_simple_max_aggregate_into_limit_index_subplan() {
    let catalog = catalog_with_indexed_items();
    let stmt = parse_select("select max(id) from items where id < 42").expect("parse");
    let (query, _) = analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[])
        .expect("analyze");
    let planned = super::planner(query, &catalog).expect("plan");

    assert_eq!(planned.subplans.len(), 1);
    assert!(!plan_contains(&planned.plan_tree, |plan| matches!(
        plan,
        Plan::Aggregate { .. }
    )));

    let subplan = &planned.subplans[0];
    assert!(plan_contains(subplan, |plan| matches!(
        plan,
        Plan::Limit { .. }
    )));
    assert!(plan_contains(subplan, |plan| matches!(
        plan,
        Plan::IndexScan { .. }
    )));
    assert!(!plan_contains(subplan, |plan| matches!(
        plan,
        Plan::Aggregate { .. }
    )));

    assert!(plan_contains(subplan, |plan| matches!(
        plan,
        Plan::IndexScan { direction, .. }
            if *direction == crate::include::access::relscan::ScanDirection::Backward
    )));
}

#[test]
fn planner_rewrites_multiple_minmax_aggregates_into_multiple_subplans() {
    let catalog = catalog_with_indexed_items();
    let stmt = parse_select("select min(id), max(id) from items").expect("parse");
    let (query, _) = analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[])
        .expect("analyze");
    let planned = super::planner(query, &catalog).expect("plan");

    assert_eq!(planned.subplans.len(), 2);
    assert!(planned.subplans.iter().all(|subplan| {
        plan_contains(subplan, |plan| matches!(plan, Plan::Limit { .. }))
            && plan_contains(subplan, |plan| matches!(plan, Plan::IndexScan { .. }))
            && !plan_contains(subplan, |plan| matches!(plan, Plan::Aggregate { .. }))
    }));
    assert!(planned.subplans.iter().any(|subplan| {
        plan_contains(subplan, |plan| {
            matches!(
                plan,
                Plan::IndexScan { direction, .. }
                    if *direction == crate::include::access::relscan::ScanDirection::Forward
            )
        })
    }));
    assert!(planned.subplans.iter().any(|subplan| {
        plan_contains(subplan, |plan| {
            matches!(
                plan,
                Plan::IndexScan { direction, .. }
                    if *direction == crate::include::access::relscan::ScanDirection::Backward
            )
        })
    }));
}

#[test]
fn explain_shows_initplan_for_rewritten_minmax_aggregate() {
    let catalog = catalog_with_indexed_items();
    let stmt = parse_select("select max(id) from items where id < 42").expect("parse");
    let (query, _) = analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[])
        .expect("analyze");
    let planned = super::planner(query, &catalog).expect("plan");

    let mut lines = Vec::new();
    crate::backend::commands::explain::format_explain_plan_with_subplans(
        &planned.plan_tree,
        &planned.subplans,
        0,
        false,
        &mut lines,
    );

    assert!(lines.iter().any(|line| line == "  InitPlan 1"));
    assert!(lines.iter().any(|line| line.trim() == "Limit"));
    assert!(lines.iter().any(|line| line.contains("Index Scan")));
    assert!(!lines.iter().any(|line| line.contains("Aggregate")));
}

#[test]
fn planner_keeps_nested_sublink_max_as_aggregate() {
    let catalog = catalog_with_indexed_items();
    let stmt = parse_select(
        "select (select max((select i.id from items i where i.id = o.id))) from items o",
    )
    .expect("parse");
    let (query, _) = analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[])
        .expect("analyze");
    let planned = super::planner(query, &catalog).expect("plan");

    assert!(
        planned
            .subplans
            .iter()
            .any(|subplan| plan_contains(subplan, |plan| matches!(plan, Plan::Aggregate { .. })))
    );
}

#[test]
fn planner_rewrites_correlated_min_with_index_subplan() {
    let catalog = catalog_with_indexed_items();
    let stmt =
        parse_select("select o.id, (select min(id) from items where id > o.id) from items o")
            .expect("parse");
    let (query, _) = analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[])
        .expect("analyze");
    let planned = super::planner(query, &catalog).expect("plan");

    assert!(planned.subplans.iter().any(|subplan| {
        plan_contains(subplan, |plan| matches!(plan, Plan::Limit { .. }))
            && plan_contains(subplan, |plan| matches!(plan, Plan::IndexScan { .. }))
    }));
}

#[test]
fn planner_lowers_outer_aggregate_refs_in_correlated_subqueries() {
    let catalog = catalog_with_people_and_pets();
    let stmt = parse_select(
        "select p.id from people p group by p.id having exists (select 1 from pets q where sum(p.id) = q.owner_id)",
    )
    .expect("parse");
    let (query, _) = analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[])
        .expect("analyze");
    let planned = super::planner(query, &catalog).expect("plan");

    super::setrefs::validate_executable_plan_for_tests(&planned.plan_tree);
    for subplan in &planned.subplans {
        super::setrefs::validate_executable_plan_for_tests(subplan);
    }

    let debug = format!("{planned:#?}");
    assert!(debug.contains("paramkind: Exec"), "{debug}");
    assert!(!debug.contains("Aggref"), "{debug}");
}

#[test]
fn planned_lockstep_project_set_keeps_both_visible_targets_as_sets() {
    let catalog = LiteralDefaultCatalog;
    let stmt = parse_select(
        "select generate_series(1, 2), unnest(ARRAY['a', 'b', 'c']::varchar[]) order by 1, 2",
    )
    .expect("parse");
    let (query, _) = analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[])
        .expect("analyze");
    let planned = super::planner(query, &catalog).expect("plan");
    assert!(matches!(planned.plan_tree, Plan::OrderBy { .. }));

    fn find_project_set(plan: &Plan) -> Option<&Plan> {
        match plan {
            Plan::ProjectSet { .. } => Some(plan),
            Plan::Hash { input, .. }
            | Plan::Filter { input, .. }
            | Plan::Projection { input, .. }
            | Plan::OrderBy { input, .. }
            | Plan::Limit { input, .. }
            | Plan::LockRows { input, .. }
            | Plan::Aggregate { input, .. }
            | Plan::WindowAgg { input, .. }
            | Plan::BitmapHeapScan {
                bitmapqual: input, ..
            } => find_project_set(input),
            Plan::Append { children, .. } | Plan::SetOp { children, .. } => {
                children.iter().find_map(find_project_set)
            }
            Plan::NestedLoopJoin { left, right, .. } | Plan::HashJoin { left, right, .. } => {
                find_project_set(left).or_else(|| find_project_set(right))
            }
            Plan::Result { .. }
            | Plan::SeqScan { .. }
            | Plan::IndexScan { .. }
            | Plan::BitmapIndexScan { .. }
            | Plan::Values { .. }
            | Plan::FunctionScan { .. }
            | Plan::WorkTableScan { .. }
            | Plan::RecursiveUnion { .. }
            | Plan::SubqueryScan { .. }
            | Plan::CteScan { .. } => None,
        }
    }

    match find_project_set(&planned.plan_tree).expect("project set plan") {
        Plan::ProjectSet { targets, .. } => {
            assert!(matches!(
                &targets[0],
                crate::include::nodes::primnodes::ProjectSetTarget::Set { .. }
            ));
            assert!(matches!(
                &targets[1],
                crate::include::nodes::primnodes::ProjectSetTarget::Set { .. }
            ));
        }
        _ => unreachable!(),
    }
}

#[test]
fn into_plan_nested_loop_join_lowers_join_qual_via_child_tlist_identity() {
    let left = projection_path(
        20,
        1.0,
        1.5,
        values_path(10, 1.0, 1.0),
        vec![TargetEntry::new("a", var(10, 1), int4(), 1)],
    );
    let right = projection_path(
        21,
        2.0,
        2.5,
        values_path(11, 2.0, 2.0),
        vec![TargetEntry::new("b", var(11, 1), int4(), 1)],
    );
    let pathtarget = join_pathtarget(&left, &right);
    let output_columns = join_output_columns(&left, &right);
    let plan = Path::NestedLoopJoin {
        plan_info: PlanEstimate::new(5.0, 6.0, 10.0, 2),
        pathtarget,
        output_columns,
        left: Box::new(left),
        right: Box::new(right),
        kind: JoinType::Inner,
        restrict_clauses: vec![restrict(eq(var(10, 1), var(11, 1)))],
    }
    .into_plan();

    match plan {
        Plan::NestedLoopJoin { join_qual, .. } => {
            assert_eq!(join_qual.len(), 1);
            match &join_qual[0] {
                Expr::Op(op) => {
                    assert!(is_special_user_var(&op.args[0], OUTER_VAR, 0));
                    assert!(is_special_user_var(&op.args[1], INNER_VAR, 0));
                }
                other => panic!("expected join qual op, got {other:?}"),
            }
        }
        other => panic!("expected nested loop join plan, got {other:?}"),
    }
}

#[test]
fn into_plan_hash_join_lowers_hash_clause_via_child_tlist_identity() {
    let left = projection_path(
        20,
        1.0,
        1.5,
        values_path(10, 1.0, 1.0),
        vec![TargetEntry::new("a", var(10, 1), int4(), 1)],
    );
    let right = projection_path(
        21,
        2.0,
        2.5,
        values_path(11, 2.0, 2.0),
        vec![TargetEntry::new("b", var(11, 1), int4(), 1)],
    );
    let pathtarget = join_pathtarget(&left, &right);
    let output_columns = join_output_columns(&left, &right);
    let plan = Path::HashJoin {
        plan_info: PlanEstimate::new(5.0, 6.0, 10.0, 2),
        pathtarget,
        output_columns,
        left: Box::new(left),
        right: Box::new(right),
        kind: JoinType::Inner,
        hash_clauses: vec![restrict(eq(var(10, 1), var(11, 1)))],
        outer_hash_keys: vec![var(10, 1)],
        inner_hash_keys: vec![var(11, 1)],
        restrict_clauses: vec![restrict(eq(var(10, 1), var(11, 1)))],
    }
    .into_plan();

    match plan {
        Plan::HashJoin {
            hash_clauses,
            hash_keys,
            right,
            ..
        } => {
            assert_eq!(hash_keys.len(), 1);
            assert!(is_special_user_var(&hash_keys[0], OUTER_VAR, 0));
            assert_eq!(hash_clauses.len(), 1);
            match &hash_clauses[0] {
                Expr::Op(op) => {
                    assert!(is_special_user_var(&op.args[0], OUTER_VAR, 0));
                    assert!(is_special_user_var(&op.args[1], INNER_VAR, 0));
                }
                other => panic!("expected hash clause op, got {other:?}"),
            }
            match *right {
                Plan::Hash { hash_keys, .. } => {
                    assert_eq!(hash_keys.len(), 1);
                    assert!(is_special_user_var(&hash_keys[0], OUTER_VAR, 0));
                }
                other => panic!("expected hash plan, got {other:?}"),
            }
        }
        other => panic!("expected hash join plan, got {other:?}"),
    }
}

#[test]
fn required_query_pathkeys_for_path_falls_back_when_input_lacks_sortgroup_identity() {
    let root = planner_info_for_sql("select column1 as a from (values (1)) v order by a");
    let path = projection_path(
        20,
        1.0,
        1.5,
        values_path(10, 1.0, 1.0),
        vec![TargetEntry::new("a", var(10, 1), int4(), 1)],
    );

    let required = util::required_query_pathkeys_for_path(&root, &path);
    let lowered = util::lower_pathkeys_for_path(&root, &path, &root.query_pathkeys);

    assert_eq!(required, lowered);
}

#[test]
fn required_query_pathkeys_for_path_falls_back_for_zero_ref_keys() {
    let mut root = planner_info_for_sql("select 1");
    root.query_pathkeys = vec![pathkey(var(10, 1))];
    let path = projection_path(
        20,
        1.0,
        1.5,
        values_path(10, 1.0, 1.0),
        vec![TargetEntry::new("a", var(10, 1), int4(), 1)],
    );

    let required = util::required_query_pathkeys_for_path(&root, &path);

    assert_eq!(required, vec![pathkey(var(10, 1))]);
}

#[test]
fn lower_pathkeys_for_path_strips_binary_coercible_casts() {
    let root = planner_info_for_sql("select 1");
    let input = Path::Values {
        plan_info: PlanEstimate::new(1.0, 1.0, 1.0, 1),
        pathtarget: PathTarget::new(vec![typed_var(10, 1, oid())]),
        slot_id: 10,
        rows: vec![vec![Expr::Const(Value::Int64(1))]],
        output_columns: vec![QueryColumn {
            name: "oidcol".into(),
            sql_type: oid(),
            wire_type_oid: None,
        }],
    };
    let path = projection_path(
        20,
        1.0,
        1.5,
        input,
        vec![TargetEntry::new(
            "oidcol",
            typed_var(10, 1, oid()),
            oid(),
            1,
        )],
    );
    let cast_key = PathKey {
        expr: Expr::Cast(Box::new(typed_var(10, 1, oid())), regprocedure()),
        ressortgroupref: 0,
        descending: false,
        nulls_first: None,
        collation_oid: None,
    };

    let lowered = util::lower_pathkeys_for_path(&root, &path, &[cast_key]);

    assert_eq!(lowered, vec![pathkey(typed_var(10, 1, oid()))]);
}

#[test]
fn rel_exposes_required_pathkey_identity_only_when_a_path_matches() {
    let root = planner_info_for_sql("select column1 as a from (values (1)) v order by a");
    let sortgroupref = root.query_pathkeys[0].ressortgroupref;
    let matching_path = projection_path(
        20,
        1.0,
        1.5,
        values_path(10, 1.0, 1.0),
        vec![TargetEntry::new("a", var(10, 1), int4(), 1).with_sort_group_ref(sortgroupref)],
    );
    let non_matching_path = projection_path(
        21,
        2.0,
        2.5,
        values_path(11, 2.0, 2.0),
        vec![TargetEntry::new("a", var(11, 1), int4(), 1)],
    );
    let mut rel = RelOptInfo::new(
        vec![1],
        RelOptKind::UpperRel,
        PathTarget::from_target_list(&[]),
    );
    rel.add_path(non_matching_path.clone());
    assert!(!util::rel_exposes_required_pathkey_identity(
        &rel,
        &root.query_pathkeys
    ));
    rel.add_path(matching_path.clone());
    assert!(util::path_exposes_required_pathkey_identity(
        &matching_path,
        &root.query_pathkeys
    ));
    assert!(util::rel_exposes_required_pathkey_identity(
        &rel,
        &root.query_pathkeys
    ));
}

#[test]
fn required_query_pathkeys_for_rel_falls_back_when_rel_lacks_identity() {
    let root = planner_info_for_sql("select column1 as a from (values (1)) v order by a");
    let path = projection_path(
        20,
        1.0,
        1.5,
        values_path(10, 1.0, 1.0),
        vec![TargetEntry::new("a", var(10, 1), int4(), 1)],
    );
    let mut rel = RelOptInfo::new(
        vec![1],
        RelOptKind::UpperRel,
        PathTarget::from_target_list(&[]),
    );
    rel.add_path(path);

    let required = util::required_query_pathkeys_for_rel(&root, &rel);
    let lowered = util::lower_pathkeys_for_rel(&root, &rel, &root.query_pathkeys);

    assert_eq!(required, lowered);
}

#[test]
fn required_query_pathkeys_for_rel_keeps_sortgroup_identified_keys_when_rel_has_matching_path() {
    let root = planner_info_for_sql("select column1 as a from (values (1)) v order by a");
    let sortgroupref = root.query_pathkeys[0].ressortgroupref;
    let path = projection_path(
        20,
        1.0,
        1.5,
        values_path(10, 1.0, 1.0),
        vec![TargetEntry::new("a", var(10, 1), int4(), 1).with_sort_group_ref(sortgroupref)],
    );
    let mut rel = RelOptInfo::new(
        vec![1],
        RelOptKind::UpperRel,
        PathTarget::from_target_list(&[]),
    );
    rel.add_path(path);

    let required = util::required_query_pathkeys_for_rel(&root, &rel);

    assert_eq!(required, root.query_pathkeys);
}

#[test]
fn projection_rewrite_maps_semantic_var_to_current_projection_slot() {
    let inner = projection_path(
        1_000_100,
        1.0,
        1.5,
        values_path(1, 1.0, 1.0),
        vec![TargetEntry::new("name", var(1, 1), int4(), 1)],
    );
    let outer = projection_path(
        4,
        1.5,
        2.0,
        inner,
        vec![TargetEntry::new("name", var(1_000_100, 1), int4(), 1)],
    );

    let rewritten =
        super::rewrite::rewrite_semantic_expr_for_path(var(1, 1), &outer, &outer.output_vars());

    assert_eq!(rewritten, var(1_000_100, 1));
}

#[test]
fn swapped_join_candidate_keeps_logical_pathtarget_order() {
    let paths = super::build_join_paths(
        values_path(1, 1.0, 10.0),
        values_path(2, 2.0, 20.0),
        &[1],
        &[2],
        JoinType::Inner,
        vec![restrict(eq(var(1, 1), var(2, 1)))],
    );

    let swapped = paths
        .into_iter()
        .find(|path| match path {
            Path::NestedLoopJoin { left, .. } => left.output_vars().first() == Some(&var(2, 1)),
            _ => false,
        })
        .expect("swapped nested loop join");

    assert_eq!(
        swapped.semantic_output_vars(),
        vec![var(1, 1), var(1, 2), var(2, 1), var(2, 2)]
    );
    assert_eq!(
        swapped.output_vars(),
        vec![var(2, 1), var(2, 2), var(1, 1), var(1, 2)]
    );
}

#[test]
fn projection_above_swapped_join_uses_physical_join_slot_indexes() {
    let logical_left = projection_path(
        20,
        1.0,
        1.5,
        values_path(10, 1.0, 1.0),
        vec![TargetEntry::new("left_col", var(10, 1), int4(), 1)],
    );
    let logical_right = projection_path(
        21,
        2.0,
        2.5,
        values_path(11, 2.0, 2.0),
        vec![TargetEntry::new("right_col", var(11, 1), int4(), 1)],
    );
    let swapped_join = Path::NestedLoopJoin {
        plan_info: PlanEstimate::new(5.0, 6.0, 10.0, 2),
        pathtarget: join_pathtarget(&logical_left, &logical_right),
        output_columns: join_output_columns(&logical_left, &logical_right),
        left: Box::new(logical_right.clone()),
        right: Box::new(logical_left.clone()),
        kind: JoinType::Inner,
        restrict_clauses: vec![restrict(eq(var(10, 1), var(11, 1)))],
    };
    let plan = projection_path(
        30,
        6.5,
        7.0,
        swapped_join,
        vec![
            TargetEntry::new("left_col", var(10, 1), int4(), 1),
            TargetEntry::new("right_col", var(11, 1), int4(), 2),
        ],
    )
    .into_plan();

    match plan {
        Plan::Projection { targets, .. } => {
            assert!(is_special_user_var(&targets[0].expr, OUTER_VAR, 1));
            assert!(is_special_user_var(&targets[1].expr, OUTER_VAR, 0));
        }
        other => panic!("expected projection plan, got {other:?}"),
    }
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
fn extract_hash_join_clauses_commutes_join_sides_from_restrictinfo_metadata() {
    let clauses =
        super::extract_hash_join_clauses(&[restrict(eq(var(2, 1), var(1, 1)))], &[1], &[2])
            .expect("hash join clauses");

    assert_eq!(
        clauses.hash_clauses,
        vec![restrict(eq(var(2, 1), var(1, 1)))]
    );
    assert_eq!(clauses.outer_hash_keys, vec![var(1, 1)]);
    assert_eq!(clauses.inner_hash_keys, vec![var(2, 1)]);
    assert!(clauses.join_clauses.is_empty());
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
    let left = values_path(1, 1.0, 10.0);
    let right = values_path(2, 2.0, 20.0);
    let plan = Path::HashJoin {
        plan_info: PlanEstimate::new(5.0, 15.0, 10.0, 4),
        pathtarget: join_pathtarget(&left, &right),
        output_columns: join_output_columns(&left, &right),
        left: Box::new(left),
        right: Box::new(right),
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
