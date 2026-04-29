use super::bestpath::{self, CostSelector};
use crate::backend::catalog::catalog::column_desc;
use crate::backend::catalog::{Catalog, CatalogIndexBuildOptions};
use crate::backend::optimizer::pathnodes::rte_slot_id;
use crate::backend::optimizer::util;
use crate::backend::parser::CatalogLookup;
use crate::backend::parser::analyze::LiteralDefaultCatalog;
use crate::backend::parser::{
    IndexColumnDef, LoweredPartitionSpec, ParseOptions, PartitionBoundSpec,
    PartitionRangeDatumValue, PartitionStrategy, SerializedPartitionValue, SqlType, SqlTypeKind,
    Statement, parse_statement_with_options, pg_partitioned_table_row, serialize_partition_bound,
};
use crate::backend::parser::{analyze_select_query_with_outer, parse_select};
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, CURRENT_DATABASE_OID, PUBLIC_NAMESPACE_OID, PgInheritsRow,
};
use crate::include::nodes::datum::Value;
use crate::include::nodes::pathnodes::{
    Path, PathKey, PathTarget, PlannerConfig, PlannerInfo, RelOptInfo, RelOptKind,
};
use crate::include::nodes::plannodes::{
    AggregateStrategy, IndexScanKeyArgument, Plan, PlanEstimate, PlannedStmt,
};
use crate::include::nodes::primnodes::{
    Aggref, AttrNumber, Expr, INNER_VAR, JoinType, OUTER_VAR, OpExpr, OpExprKind, OrderByEntry,
    Param, ParamKind, QueryColumn, RelationDesc, TargetEntry, Var, WindowFrameBound, user_attrno,
};

fn int4() -> SqlType {
    SqlType::new(SqlTypeKind::Int4)
}

fn box_ty() -> SqlType {
    SqlType::new(SqlTypeKind::Box)
}

fn polygon_ty() -> SqlType {
    SqlType::new(SqlTypeKind::Polygon)
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
    values_path_with_rows(slot_id, startup_cost, total_cost, 10.0)
}

fn values_path_with_rows(slot_id: usize, startup_cost: f64, total_cost: f64, rows: f64) -> Path {
    let output_columns = values_output_columns();
    Path::Values {
        plan_info: PlanEstimate::new(startup_cost, total_cost, rows, 2),
        pathtarget: PathTarget::new(vec![var(slot_id, 1), var(slot_id, 2)]),
        slot_id,
        rows: vec![vec![
            crate::include::nodes::primnodes::Expr::Const(Value::Int32(1)),
            crate::include::nodes::primnodes::Expr::Const(Value::Int32(2)),
        ]],
        output_columns,
    }
}

fn filtered_values_path_with_rows(
    slot_id: usize,
    startup_cost: f64,
    total_cost: f64,
    rows: f64,
) -> Path {
    let input = values_path_with_rows(slot_id, 0.0, 0.03, 3.0);
    let pathtarget = input.semantic_output_target();
    Path::Filter {
        plan_info: PlanEstimate::new(startup_cost, total_cost, rows, 2),
        pathtarget,
        predicate: gt(var(slot_id, 1), Expr::Const(Value::Int32(0))),
        input: Box::new(input),
    }
}

fn seqscan_path_with_rows(slot_id: usize, startup_cost: f64, total_cost: f64, rows: f64) -> Path {
    let output_columns = values_output_columns();
    let rel_number = slot_id as u32;
    Path::SeqScan {
        plan_info: PlanEstimate::new(startup_cost, total_cost, rows, 2),
        pathtarget: PathTarget::new(vec![var(slot_id, 1), var(slot_id, 2)]),
        source_id: slot_id,
        rel: crate::RelFileLocator {
            spc_oid: 0,
            db_oid: 0,
            rel_number,
        },
        relation_name: format!("t{slot_id}"),
        relation_oid: rel_number,
        relkind: 'r',
        relispopulated: true,
        disabled: false,
        toast: None,
        desc: RelationDesc {
            columns: output_columns
                .into_iter()
                .map(|column| column_desc(column.name, column.sql_type, true))
                .collect(),
        },
    }
}

fn ordered_path_with_rows(
    slot_id: usize,
    startup_cost: f64,
    total_cost: f64,
    rows: f64,
    key_attno: usize,
) -> Path {
    order_by_path(
        startup_cost,
        total_cost,
        values_path_with_rows(slot_id, startup_cost, total_cost, rows),
        vec![OrderByEntry {
            expr: var(slot_id, key_attno),
            ressortgroupref: 0,
            descending: false,
            nulls_first: None,
            collation_oid: None,
        }],
    )
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
    let rows = input.plan_info().plan_rows.as_f64();
    Path::OrderBy {
        plan_info: PlanEstimate::new(startup_cost, total_cost, rows, input.columns().len()),
        pathtarget,
        input: Box::new(input),
        items,
        display_items: Vec::new(),
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
    ordered_path_with_rows(slot_id, startup_cost, total_cost, 10.0, key_attno)
}

fn assert_float_near(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 1e-9,
        "expected {expected}, got {actual}"
    );
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
fn pathkeys_satisfy_equivalent_default_null_ordering() {
    let actual = vec![pathkey(var(10, 1))];
    let mut required = pathkey(var(10, 1));
    required.nulls_first = Some(false);

    assert!(bestpath::pathkeys_satisfy(&actual, &[required]));

    let actual_desc = vec![PathKey {
        descending: true,
        ..pathkey(var(10, 1))
    }];
    let required_desc = PathKey {
        descending: true,
        nulls_first: Some(true),
        ..pathkey(var(10, 1))
    };

    assert!(bestpath::pathkeys_satisfy(&actual_desc, &[required_desc]));
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
    let query = super::root::prepare_query_for_planning(query, &catalog);
    let query = super::pull_up_sublinks(query);
    let aggregate_layout = super::groupby_rewrite::build_aggregate_layout(&query, &catalog);
    PlannerInfo::new(query, aggregate_layout)
}

fn planned_stmt_for_sql(sql: &str) -> crate::include::nodes::plannodes::PlannedStmt {
    let catalog = LiteralDefaultCatalog;
    planned_stmt_for_sql_with_catalog(sql, &catalog)
}

fn planned_stmt_for_sql_with_catalog(
    sql: &str,
    catalog: &dyn crate::backend::parser::CatalogLookup,
) -> crate::include::nodes::plannodes::PlannedStmt {
    planned_stmt_for_sql_with_catalog_and_config(sql, catalog, PlannerConfig::default())
}

fn planned_stmt_for_sql_with_catalog_and_config(
    sql: &str,
    catalog: &dyn crate::backend::parser::CatalogLookup,
    config: PlannerConfig,
) -> crate::include::nodes::plannodes::PlannedStmt {
    let stmt = parse_select_for_optimizer_test(sql).expect("parse");
    let (query, _) = analyze_select_query_with_outer(&stmt, catalog, &[], None, None, &[], &[])
        .expect("analyze");
    super::planner_with_config(query, catalog, config).expect("plan")
}

fn parse_select_for_optimizer_test(
    sql: &str,
) -> Result<crate::backend::parser::SelectStatement, crate::backend::parser::ParseError> {
    stacker::grow(32 * 1024 * 1024, || {
        match parse_statement_with_options(
            sql,
            ParseOptions {
                max_stack_depth_kb: 32768,
                ..ParseOptions::default()
            },
        )? {
            Statement::Select(stmt) => Ok(stmt),
            other => Err(crate::backend::parser::ParseError::UnexpectedToken {
                expected: "SELECT",
                actual: format!("{other:?}"),
            }),
        }
    })
}

fn planned_stmt_for_sql_with_catalog_and_larger_parse_stack(
    sql: &str,
    catalog: &dyn crate::backend::parser::CatalogLookup,
) -> crate::include::nodes::plannodes::PlannedStmt {
    let stmt = parse_select_for_optimizer_test(sql).expect("parse");
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

fn aggregate_layout_for_sql_with_catalog(
    sql: &str,
    catalog: &dyn crate::backend::parser::CatalogLookup,
) -> crate::include::nodes::pathnodes::AggregateLayout {
    let stmt = parse_select(sql).expect("parse");
    let (query, _) = analyze_select_query_with_outer(&stmt, catalog, &[], None, None, &[], &[])
        .expect("analyze");
    let query = super::root::prepare_query_for_planning(query, catalog);
    let query = super::pull_up_sublinks(query);
    super::groupby_rewrite::build_aggregate_layout(&query, catalog)
}

fn explain_lines_for_planned_stmt(planned: &PlannedStmt) -> Vec<String> {
    let mut lines = Vec::new();
    crate::backend::commands::explain::format_explain_plan_with_subplans(
        &planned.plan_tree,
        &planned.subplans,
        0,
        false,
        &mut lines,
    );
    lines
}

fn int4_btree_options(num_keys: usize, indnullsnotdistinct: bool) -> CatalogIndexBuildOptions {
    CatalogIndexBuildOptions {
        am_oid: crate::include::catalog::BTREE_AM_OID,
        indclass: vec![crate::include::catalog::INT4_BTREE_OPCLASS_OID; num_keys],
        indclass_options: vec![Vec::new(); num_keys],
        indcollation: vec![0; num_keys],
        indoption: vec![0; num_keys],
        reloptions: None,
        indnullsnotdistinct,
        indisexclusion: false,
        indimmediate: true,
        btree_options: None,
        brin_options: None,
        gist_options: None,
        gin_options: None,
        hash_options: None,
    }
}

fn box_spgist_options(num_keys: usize) -> CatalogIndexBuildOptions {
    CatalogIndexBuildOptions {
        am_oid: crate::include::catalog::SPGIST_AM_OID,
        indclass: vec![crate::include::catalog::BOX_SPGIST_OPCLASS_OID; num_keys],
        indclass_options: vec![Vec::new(); num_keys],
        indcollation: vec![0; num_keys],
        indoption: vec![0; num_keys],
        reloptions: None,
        indnullsnotdistinct: false,
        indisexclusion: false,
        indimmediate: true,
        btree_options: None,
        brin_options: None,
        gist_options: None,
        gin_options: None,
        hash_options: None,
    }
}

fn polygon_spgist_options(num_keys: usize) -> CatalogIndexBuildOptions {
    CatalogIndexBuildOptions {
        am_oid: crate::include::catalog::SPGIST_AM_OID,
        indclass: vec![crate::include::catalog::POLY_SPGIST_OPCLASS_OID; num_keys],
        indclass_options: vec![Vec::new(); num_keys],
        indcollation: vec![0; num_keys],
        indoption: vec![0; num_keys],
        reloptions: None,
        indnullsnotdistinct: false,
        indisexclusion: false,
        indimmediate: true,
        btree_options: None,
        brin_options: None,
        gist_options: None,
        gin_options: None,
        hash_options: None,
    }
}

fn add_ready_index(
    catalog: &mut Catalog,
    table_name: &str,
    index_name: &str,
    unique: bool,
    primary: bool,
    columns: &[IndexColumnDef],
    options: Option<CatalogIndexBuildOptions>,
    predicate_sql: Option<&str>,
) {
    let relation_oid = catalog
        .lookup_any_relation(table_name)
        .expect("table should exist")
        .relation_oid;
    let entry = match options.as_ref() {
        Some(options) => catalog
            .create_index_for_relation_with_options_and_flags(
                index_name,
                relation_oid,
                unique,
                primary,
                columns,
                options,
                predicate_sql,
            )
            .expect("create index"),
        None => catalog
            .create_index_for_relation_with_flags(
                index_name,
                relation_oid,
                unique,
                primary,
                columns,
            )
            .expect("create index"),
    };
    catalog
        .set_index_ready_valid(entry.relation_oid, true, true)
        .expect("mark index ready");
}

fn var_keys(exprs: &[Expr]) -> Vec<(usize, AttrNumber)> {
    exprs
        .iter()
        .map(|expr| match expr {
            Expr::Var(Var {
                varno,
                varattno,
                varlevelsup: 0,
                ..
            }) => (*varno, *varattno),
            other => panic!("expected simple Var, got {other:?}"),
        })
        .collect()
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

fn catalog_with_indexed_later_column() -> Catalog {
    let mut catalog = Catalog::default();
    let table = catalog
        .create_table(
            "items",
            RelationDesc {
                columns: vec![
                    column_desc("id", int4(), false),
                    column_desc("hundred", int4(), false),
                ],
            },
        )
        .expect("create test catalog relation");
    let index = catalog
        .create_index("items_hundred_idx", "items", false, &["hundred".into()])
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

fn catalog_with_distinct_on_tbl() -> Catalog {
    let mut catalog = Catalog::default();
    let table = catalog
        .create_table(
            "distinct_on_tbl",
            RelationDesc {
                columns: vec![
                    column_desc("x", int4(), false),
                    column_desc("y", int4(), false),
                    column_desc("z", int4(), false),
                ],
            },
        )
        .expect("create distinct_on_tbl");
    let index = catalog
        .create_index(
            "distinct_on_tbl_x_y_idx",
            "distinct_on_tbl",
            false,
            &["x".into(), "y".into()],
        )
        .expect("create distinct_on_tbl index");
    catalog
        .set_index_ready_valid(index.relation_oid, true, true)
        .expect("mark distinct_on_tbl index usable");
    catalog
        .set_relation_stats(table.relation_oid, 128, 10_000.0)
        .expect("seed distinct_on_tbl stats");
    catalog
        .set_relation_stats(index.relation_oid, 32, 10_000.0)
        .expect("seed distinct_on_tbl index stats");
    catalog
}

fn catalog_with_distinct_on_limit_tbl() -> Catalog {
    let mut catalog = Catalog::default();
    let table = catalog
        .create_table(
            "limit_tbl",
            RelationDesc {
                columns: vec![
                    column_desc("four", int4(), false),
                    column_desc("two", int4(), false),
                    column_desc("hundred", int4(), false),
                ],
            },
        )
        .expect("create limit_tbl");
    let index = catalog
        .create_index("limit_tbl_hundred", "limit_tbl", false, &["hundred".into()])
        .expect("create limit_tbl index");
    catalog
        .set_index_ready_valid(index.relation_oid, true, true)
        .expect("mark limit_tbl index usable");
    catalog
        .set_relation_stats(table.relation_oid, 128, 10_000.0)
        .expect("seed limit_tbl stats");
    catalog
        .set_relation_stats(index.relation_oid, 32, 10_000.0)
        .expect("seed limit_tbl index stats");
    catalog
}

fn catalog_with_noncovering_indexed_items() -> Catalog {
    let mut catalog = Catalog::default();
    let table = catalog
        .create_table(
            "items",
            RelationDesc {
                columns: vec![
                    column_desc("id", int4(), false),
                    column_desc("payload", int4(), true),
                ],
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

fn catalog_with_spgist_box_temp() -> Catalog {
    let mut catalog = Catalog::default();
    let table = catalog
        .create_table(
            "box_temp",
            RelationDesc {
                columns: vec![column_desc("f1", box_ty(), true)],
            },
        )
        .expect("create test catalog relation");
    let index = catalog
        .create_index_for_relation_with_options_and_flags(
            "box_spgist",
            table.relation_oid,
            false,
            false,
            &[IndexColumnDef::from("f1")],
            &box_spgist_options(1),
            None,
        )
        .expect("create test catalog index");
    catalog
        .set_index_ready_valid(index.relation_oid, true, true)
        .expect("mark test catalog index usable");
    catalog
        .set_relation_stats(table.relation_oid, 512, 10_000.0)
        .expect("seed test catalog table stats");
    catalog
        .set_relation_stats(index.relation_oid, 32, 10_000.0)
        .expect("seed test catalog index stats");
    catalog
}

fn catalog_with_unique_indexed_items() -> Catalog {
    let mut catalog = Catalog::default();
    let table = catalog
        .create_table(
            "items",
            RelationDesc {
                columns: vec![
                    column_desc("id", int4(), false),
                    column_desc("payload", int4(), true),
                ],
            },
        )
        .expect("create test catalog relation");
    let index = catalog
        .create_index("items_id_idx", "items", true, &["id".into()])
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

fn catalog_with_inherited_indexed_items()
-> crate::backend::utils::cache::visible_catalog::VisibleCatalog {
    fn btree_options(
        catalog: &Catalog,
        relation_oid: u32,
        columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
    ) -> crate::backend::catalog::CatalogIndexBuildOptions {
        let table = catalog
            .get_by_oid(relation_oid)
            .expect("relation for inherited test index");
        let mut indclass = Vec::with_capacity(columns.len());
        let mut indcollation = Vec::with_capacity(columns.len());
        let mut indoption = Vec::with_capacity(columns.len());
        for column in columns {
            let table_column = table
                .desc
                .columns
                .iter()
                .find(|candidate| candidate.name.eq_ignore_ascii_case(&column.name))
                .expect("indexed column present");
            indclass.push(
                crate::include::catalog::default_btree_opclass_oid(
                    crate::backend::utils::cache::catcache::sql_type_oid(table_column.sql_type),
                )
                .expect("default btree opclass"),
            );
            indcollation.push(0);
            let mut option = 0i16;
            if column.descending {
                option |= 0x0001;
            }
            if column.nulls_first.unwrap_or(column.descending) {
                option |= 0x0002;
            }
            indoption.push(option);
        }
        crate::backend::catalog::CatalogIndexBuildOptions {
            am_oid: crate::include::catalog::BTREE_AM_OID,
            indclass,
            indclass_options: vec![Vec::new(); indcollation.len()],
            indcollation,
            indoption,
            reloptions: None,
            indnullsnotdistinct: false,
            indisexclusion: false,
            indimmediate: true,
            btree_options: None,
            brin_options: None,
            gist_options: None,
            gin_options: None,
            hash_options: None,
        }
    }

    let mut catalog = Catalog::default();

    let parent = catalog
        .create_table(
            "items",
            RelationDesc {
                columns: vec![column_desc("id", int4(), false)],
            },
        )
        .expect("create parent table");
    let parent_index = catalog
        .create_index("items_id_idx", "items", false, &["id".into()])
        .expect("create parent index");

    let child1 = catalog
        .create_table(
            "items1",
            RelationDesc {
                columns: vec![column_desc("id", int4(), false)],
            },
        )
        .expect("create first child table");
    let child1_index = catalog
        .create_index("items1_id_idx", "items1", false, &["id".into()])
        .expect("create first child index");

    let child2 = catalog
        .create_table(
            "items2",
            RelationDesc {
                columns: vec![column_desc("id", int4(), false)],
            },
        )
        .expect("create second child table");
    let child3 = catalog
        .create_table(
            "items3",
            RelationDesc {
                columns: vec![column_desc("id", int4(), false)],
            },
        )
        .expect("create third child table");
    let child2_index = catalog
        .create_index_for_relation_with_options_and_flags(
            "items2_id_idx",
            child2.relation_oid,
            false,
            false,
            &[crate::include::nodes::parsenodes::IndexColumnDef {
                name: "id".into(),
                expr_sql: None,
                expr_type: None,
                collation: None,
                opclass: None,
                opclass_options: Vec::new(),
                descending: true,
                nulls_first: None,
            }],
            &btree_options(
                &catalog,
                child2.relation_oid,
                &[crate::include::nodes::parsenodes::IndexColumnDef {
                    name: "id".into(),
                    expr_sql: None,
                    expr_type: None,
                    collation: None,
                    opclass: None,
                    opclass_options: Vec::new(),
                    descending: true,
                    nulls_first: None,
                }],
            ),
            None,
        )
        .expect("create second child index");
    let child3_index = catalog
        .create_index_for_relation_with_options_and_flags(
            "items3_id_idx",
            child3.relation_oid,
            false,
            false,
            &[crate::include::nodes::parsenodes::IndexColumnDef::from(
                "id",
            )],
            &btree_options(
                &catalog,
                child3.relation_oid,
                &[crate::include::nodes::parsenodes::IndexColumnDef::from(
                    "id",
                )],
            ),
            Some("id is not null"),
        )
        .expect("create third child index");

    catalog
        .attach_inheritance(child1.relation_oid, &[parent.relation_oid])
        .expect("attach first child inheritance");
    catalog
        .attach_inheritance(child2.relation_oid, &[parent.relation_oid])
        .expect("attach second child inheritance");
    catalog
        .attach_inheritance(child3.relation_oid, &[parent.relation_oid])
        .expect("attach third child inheritance");

    for index_oid in [
        parent_index.relation_oid,
        child1_index.relation_oid,
        child2_index.relation_oid,
        child3_index.relation_oid,
    ] {
        catalog
            .set_index_ready_valid(index_oid, true, true)
            .expect("mark inherited index usable");
    }

    for relation_oid in [
        parent.relation_oid,
        child1.relation_oid,
        child2.relation_oid,
        child3.relation_oid,
    ] {
        catalog
            .set_relation_stats(relation_oid, 128, 10_000.0)
            .expect("seed inherited table stats");
    }
    for relation_oid in [
        parent_index.relation_oid,
        child1_index.relation_oid,
        child2_index.relation_oid,
        child3_index.relation_oid,
    ] {
        catalog
            .set_relation_stats(relation_oid, 32, 10_000.0)
            .expect("seed inherited index stats");
    }

    crate::backend::parser::CatalogLookup::materialize_visible_catalog(&catalog)
        .expect("materialize inherited optimizer test catalog")
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

fn catalog_with_matching_range_partitions() -> Catalog {
    let mut catalog = Catalog::default();
    create_partitioned_table_pair(&mut catalog, "lp", "rp");
    catalog
}

fn create_partitioned_table_pair(catalog: &mut Catalog, left: &str, right: &str) {
    let left_oid = create_partitioned_table(catalog, left);
    let right_oid = create_partitioned_table(catalog, right);

    create_range_partition(catalog, left_oid, &format!("{left}_p1"), 0, 10, 1);
    create_range_partition(catalog, left_oid, &format!("{left}_p3"), 20, 30, 2);
    create_range_partition(catalog, left_oid, &format!("{left}_p2"), 10, 20, 3);
    create_range_partition(catalog, right_oid, &format!("{right}_p1"), 0, 10, 1);
    create_range_partition(catalog, right_oid, &format!("{right}_p3"), 20, 30, 2);
    create_range_partition(catalog, right_oid, &format!("{right}_p2"), 10, 20, 3);
}

fn create_partitioned_table(catalog: &mut Catalog, name: &str) -> u32 {
    let desc = RelationDesc {
        columns: vec![
            column_desc("k", int4(), false),
            column_desc("v", int4(), true),
        ],
    };
    let entry = catalog
        .create_table_with_relkind(
            name,
            desc,
            PUBLIC_NAMESPACE_OID,
            CURRENT_DATABASE_OID,
            'p',
            'p',
            BOOTSTRAP_SUPERUSER_OID,
        )
        .expect("create partitioned table");
    let spec = LoweredPartitionSpec {
        strategy: PartitionStrategy::Range,
        key_columns: vec!["k".into()],
        key_exprs: vec![var(1, 1)],
        key_types: vec![int4()],
        key_sqls: vec!["k".into()],
        partattrs: vec![1],
        partclass: vec![0],
        partcollation: vec![0],
    };
    let relation_oid = entry.relation_oid;
    let table = catalog.tables.get_mut(&name.to_ascii_lowercase()).unwrap();
    table.relhassubclass = true;
    table.partitioned_table = Some(pg_partitioned_table_row(relation_oid, &spec, 0));
    relation_oid
}

fn create_range_partition(
    catalog: &mut Catalog,
    parent_oid: u32,
    name: &str,
    from: i32,
    to: i32,
    inhseqno: i32,
) -> u32 {
    let desc = RelationDesc {
        columns: vec![
            column_desc("k", int4(), false),
            column_desc("v", int4(), true),
        ],
    };
    let entry = catalog
        .create_table(name, desc)
        .expect("create partition child");
    let bound = PartitionBoundSpec::Range {
        from: vec![PartitionRangeDatumValue::Value(
            SerializedPartitionValue::Int32(from),
        )],
        to: vec![PartitionRangeDatumValue::Value(
            SerializedPartitionValue::Int32(to),
        )],
        is_default: false,
    };
    let relation_oid = entry.relation_oid;
    let table = catalog.tables.get_mut(&name.to_ascii_lowercase()).unwrap();
    table.relispartition = true;
    table.relpartbound = Some(serialize_partition_bound(&bound).expect("serialize bound"));
    catalog.inherits.push(PgInheritsRow {
        inhrelid: relation_oid,
        inhparent: parent_oid,
        inhseqno,
        inhdetachpending: false,
    });
    relation_oid
}

fn create_list_partitioned_table(catalog: &mut Catalog, name: &str) -> u32 {
    let desc = RelationDesc {
        columns: vec![
            column_desc("k", int4(), false),
            column_desc("v", int4(), true),
        ],
    };
    let entry = catalog
        .create_table_with_relkind(
            name,
            desc,
            PUBLIC_NAMESPACE_OID,
            CURRENT_DATABASE_OID,
            'p',
            'p',
            BOOTSTRAP_SUPERUSER_OID,
        )
        .expect("create list partitioned table");
    let spec = LoweredPartitionSpec {
        strategy: PartitionStrategy::List,
        key_columns: vec!["k".into()],
        key_exprs: vec![var(1, 1)],
        key_types: vec![int4()],
        key_sqls: vec!["k".into()],
        partattrs: vec![1],
        partclass: vec![0],
        partcollation: vec![0],
    };
    let relation_oid = entry.relation_oid;
    let table = catalog.tables.get_mut(&name.to_ascii_lowercase()).unwrap();
    table.relhassubclass = true;
    table.partitioned_table = Some(pg_partitioned_table_row(relation_oid, &spec, 0));
    relation_oid
}

fn create_list_partition(
    catalog: &mut Catalog,
    parent_oid: u32,
    name: &str,
    values: &[i32],
    is_default: bool,
    inhseqno: i32,
) -> u32 {
    let desc = RelationDesc {
        columns: vec![
            column_desc("k", int4(), false),
            column_desc("v", int4(), true),
        ],
    };
    let entry = catalog
        .create_table(name, desc)
        .expect("create list partition child");
    let bound = PartitionBoundSpec::List {
        values: values
            .iter()
            .copied()
            .map(SerializedPartitionValue::Int32)
            .collect(),
        is_default,
    };
    let relation_oid = entry.relation_oid;
    let table = catalog.tables.get_mut(&name.to_ascii_lowercase()).unwrap();
    table.relispartition = true;
    table.relpartbound = Some(serialize_partition_bound(&bound).expect("serialize bound"));
    catalog.inherits.push(PgInheritsRow {
        inhrelid: relation_oid,
        inhparent: parent_oid,
        inhseqno,
        inhdetachpending: false,
    });
    relation_oid
}

fn add_ready_k_index(catalog: &mut Catalog, table_name: &str) {
    let table_oid = catalog
        .lookup_any_relation(table_name)
        .expect("table should exist")
        .relation_oid;
    let index = catalog
        .create_index(
            format!("{table_name}_k_idx"),
            table_name,
            false,
            &[IndexColumnDef::from("k")],
        )
        .expect("create partition child index");
    catalog
        .set_index_ready_valid(index.relation_oid, true, true)
        .expect("mark index ready");
    catalog
        .set_relation_stats(table_oid, 128, 10_000.0)
        .expect("seed table stats");
    catalog
        .set_relation_stats(index.relation_oid, 8, 10_000.0)
        .expect("seed index stats");
}

fn create_abc_range_partitioned_table(catalog: &mut Catalog, name: &str) -> u32 {
    let desc = RelationDesc {
        columns: vec![
            column_desc("a", int4(), false),
            column_desc("b", int4(), false),
            column_desc("c", int4(), false),
        ],
    };
    let entry = catalog
        .create_table_with_relkind(
            name,
            desc,
            PUBLIC_NAMESPACE_OID,
            CURRENT_DATABASE_OID,
            'p',
            'p',
            BOOTSTRAP_SUPERUSER_OID,
        )
        .expect("create abc partitioned table");
    let spec = LoweredPartitionSpec {
        strategy: PartitionStrategy::Range,
        key_columns: vec!["a".into()],
        key_exprs: vec![var(1, 1)],
        key_types: vec![int4()],
        key_sqls: vec!["a".into()],
        partattrs: vec![1],
        partclass: vec![0],
        partcollation: vec![0],
    };
    let relation_oid = entry.relation_oid;
    let table = catalog.tables.get_mut(&name.to_ascii_lowercase()).unwrap();
    table.relhassubclass = true;
    table.partitioned_table = Some(pg_partitioned_table_row(relation_oid, &spec, 0));
    relation_oid
}

fn create_abc_range_partition(
    catalog: &mut Catalog,
    parent_oid: u32,
    name: &str,
    from: i32,
    to: i32,
    inhseqno: i32,
) -> u32 {
    let desc = RelationDesc {
        columns: vec![
            column_desc("a", int4(), false),
            column_desc("b", int4(), false),
            column_desc("c", int4(), false),
        ],
    };
    let entry = catalog
        .create_table(name, desc)
        .expect("create abc partition child");
    let bound = PartitionBoundSpec::Range {
        from: vec![PartitionRangeDatumValue::Value(
            SerializedPartitionValue::Int32(from),
        )],
        to: vec![PartitionRangeDatumValue::Value(
            SerializedPartitionValue::Int32(to),
        )],
        is_default: false,
    };
    let relation_oid = entry.relation_oid;
    let table = catalog.tables.get_mut(&name.to_ascii_lowercase()).unwrap();
    table.relispartition = true;
    table.relpartbound = Some(serialize_partition_bound(&bound).expect("serialize bound"));
    catalog.inherits.push(PgInheritsRow {
        inhrelid: relation_oid,
        inhparent: parent_oid,
        inhseqno,
        inhdetachpending: false,
    });
    relation_oid
}

fn add_ready_abc_order_index(catalog: &mut Catalog, table_name: &str) {
    let table_oid = catalog
        .lookup_any_relation(table_name)
        .expect("table should exist")
        .relation_oid;
    let index_name = format!("{table_name}_a_abs_b_c_idx");
    add_ready_index(
        catalog,
        table_name,
        &index_name,
        false,
        false,
        &[
            IndexColumnDef::from("a"),
            IndexColumnDef {
                name: "expr".into(),
                expr_sql: Some("abs(b)".into()),
                expr_type: Some(int4()),
                collation: None,
                opclass: None,
                opclass_options: Vec::new(),
                descending: false,
                nulls_first: None,
            },
            IndexColumnDef::from("c"),
        ],
        Some(int4_btree_options(3, false)),
        None,
    );
    let index_oid = catalog
        .lookup_any_relation(&index_name)
        .expect("index should exist")
        .relation_oid;
    catalog
        .set_relation_stats(table_oid, 128, 10_000.0)
        .expect("seed table stats");
    catalog
        .set_relation_stats(index_oid, 8, 10_000.0)
        .expect("seed index stats");
}

fn catalog_with_indexed_range_partitions() -> Catalog {
    let mut catalog = Catalog::default();
    let parent_oid = create_partitioned_table(&mut catalog, "rp");
    create_range_partition(&mut catalog, parent_oid, "rp_p1", 0, 10, 1);
    create_range_partition(&mut catalog, parent_oid, "rp_p3", 20, 30, 2);
    create_range_partition(&mut catalog, parent_oid, "rp_p2", 10, 20, 3);
    for child in ["rp_p1", "rp_p2", "rp_p3"] {
        add_ready_k_index(&mut catalog, child);
    }
    catalog
}

fn catalog_with_expression_indexed_range_partitions() -> Catalog {
    let mut catalog = Catalog::default();
    let parent_oid = create_abc_range_partitioned_table(&mut catalog, "erp");
    create_abc_range_partition(&mut catalog, parent_oid, "erp_p1", 0, 10, 1);
    create_abc_range_partition(&mut catalog, parent_oid, "erp_p2", 10, 20, 2);
    for child in ["erp_p1", "erp_p2"] {
        add_ready_abc_order_index(&mut catalog, child);
    }
    catalog
}

fn catalog_with_interleaved_list_partitions() -> Catalog {
    let mut catalog = Catalog::default();
    let parent_oid = create_list_partitioned_table(&mut catalog, "lp");
    create_list_partition(&mut catalog, parent_oid, "lp_p35", &[3, 5], false, 1);
    create_list_partition(&mut catalog, parent_oid, "lp_p4", &[4], false, 2);
    for child in ["lp_p35", "lp_p4"] {
        add_ready_k_index(&mut catalog, child);
    }
    catalog
}

fn append_with_join_children(plan: &Plan) -> Option<&[Plan]> {
    match plan {
        Plan::Append { children, .. }
            if children.iter().all(|child| {
                matches!(
                    child,
                    Plan::NestedLoopJoin { .. } | Plan::HashJoin { .. } | Plan::MergeJoin { .. }
                )
            }) =>
        {
            Some(children)
        }
        Plan::Append { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::SetOp { children, .. } => children.iter().find_map(append_with_join_children),
        Plan::Hash { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
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
        } => append_with_join_children(input),
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. }
        | Plan::RecursiveUnion {
            anchor: left,
            recursive: right,
            ..
        } => append_with_join_children(left).or_else(|| append_with_join_children(right)),
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::Values { .. }
        | Plan::FunctionScan { .. }
        | Plan::WorkTableScan { .. } => None,
    }
}

fn child_relation_names(plan: &Plan) -> Vec<String> {
    let mut names = Vec::new();
    collect_relation_names(plan, &mut names);
    names
}

fn collect_relation_names(plan: &Plan, names: &mut Vec<String>) {
    match plan {
        Plan::SeqScan { relation_name, .. }
        | Plan::IndexOnlyScan { relation_name, .. }
        | Plan::IndexScan { relation_name, .. } => names.push(
            relation_name
                .split_once(' ')
                .map(|(name, _)| name)
                .unwrap_or(relation_name)
                .to_string(),
        ),
        Plan::Append { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::SetOp { children, .. } => {
            for child in children {
                collect_relation_names(child, names);
            }
        }
        Plan::Hash { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
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
        } => collect_relation_names(input, names),
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. }
        | Plan::RecursiveUnion {
            anchor: left,
            recursive: right,
            ..
        } => {
            collect_relation_names(left, names);
            collect_relation_names(right, names);
        }
        Plan::Result { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::Values { .. }
        | Plan::FunctionScan { .. }
        | Plan::WorkTableScan { .. } => {}
    }
}

#[test]
fn ordered_range_partition_query_uses_append_without_sort() {
    let catalog = catalog_with_indexed_range_partitions();
    let planned = planned_stmt_for_sql_with_catalog("select k from rp order by k", &catalog);

    assert!(
        plan_contains(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::Append { .. }
        )),
        "expected ordered append path, got {:?}",
        planned.plan_tree
    );
    assert!(
        !plan_contains(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::OrderBy { .. } | Plan::MergeAppend { .. }
        )),
        "ordered range partitions should not require Sort or MergeAppend: {:?}",
        planned.plan_tree
    );
    assert_eq!(
        child_relation_names(&planned.plan_tree),
        vec!["rp_p1", "rp_p2", "rp_p3"]
    );
}

#[test]
fn descending_ordered_range_partition_query_reverses_append_children() {
    let catalog = catalog_with_indexed_range_partitions();
    let planned = planned_stmt_for_sql_with_catalog("select k from rp order by k desc", &catalog);

    assert!(
        plan_contains(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::Append { .. }
        )),
        "expected ordered append path, got {:?}",
        planned.plan_tree
    );
    assert!(
        !plan_contains(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::OrderBy { .. } | Plan::MergeAppend { .. }
        )),
        "descending ordered range partitions should not require Sort or MergeAppend: {:?}",
        planned.plan_tree
    );
    assert_eq!(
        child_relation_names(&planned.plan_tree),
        vec!["rp_p3", "rp_p2", "rp_p1"]
    );
}

#[test]
fn interleaved_list_partition_order_uses_merge_append() {
    let catalog = catalog_with_interleaved_list_partitions();
    let planned = planned_stmt_for_sql_with_catalog("select k from lp order by k", &catalog);

    assert!(
        plan_contains(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::MergeAppend { .. }
        )),
        "interleaved list bounds should use merge append, got {:?}",
        planned.plan_tree
    );
    assert!(
        !plan_contains(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::OrderBy { .. }
        )),
        "interleaved list partitions have ordered children and should not require Sort: {:?}",
        planned.plan_tree
    );
}

#[test]
fn expression_index_ordered_range_partition_query_uses_append() {
    let catalog = catalog_with_expression_indexed_range_partitions();
    let planned = planned_stmt_for_sql_with_catalog(
        "select a, b, c from erp order by a, abs(b), c",
        &catalog,
    );

    assert!(
        plan_contains(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::Append { .. }
        )),
        "expected expression-index ordered append path, got {:?}",
        planned.plan_tree
    );
    assert!(
        !plan_contains(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::OrderBy { .. } | Plan::MergeAppend { .. }
        )),
        "translated expression pathkeys should not require Sort or MergeAppend: {:?}",
        planned.plan_tree
    );
    assert!(
        plan_contains(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::IndexOnlyScan { .. } | Plan::IndexScan { .. }
        )),
        "expected child index paths, got {:?}",
        planned.plan_tree
    );
}

#[test]
fn range_partition_is_not_null_keeps_non_default_partitions() {
    let catalog = catalog_with_indexed_range_partitions();
    let planned = planned_stmt_for_sql_with_catalog(
        "select k from rp where k is not null and k < 15",
        &catalog,
    );

    assert!(
        !plan_contains(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::Result { .. }
        )),
        "range IS NOT NULL should not prune all non-default partitions: {:?}",
        planned.plan_tree
    );
    assert_eq!(
        child_relation_names(&planned.plan_tree),
        vec!["rp_p1", "rp_p2"]
    );
}

#[test]
fn partitionwise_join_guc_off_keeps_join_over_appends() {
    let catalog = catalog_with_matching_range_partitions();
    let planned = planned_stmt_for_sql_with_catalog(
        "select lp.k, rp.v from lp join rp on lp.k = rp.k",
        &catalog,
    );

    assert!(
        append_with_join_children(&planned.plan_tree).is_none(),
        "partitionwise append should not be selected while the GUC is off: {:?}",
        planned.plan_tree
    );
    assert!(
        plan_contains(&planned.plan_tree, |plan| {
            matches!(
                plan,
                Plan::NestedLoopJoin { left, right, .. }
                    if matches!(left.as_ref(), Plan::Append { .. })
                        || matches!(right.as_ref(), Plan::Append { .. })
            ) || matches!(
                plan,
                Plan::HashJoin { left, right, .. }
                    if matches!(left.as_ref(), Plan::Append { .. })
                        || matches!(right.as_ref(), Plan::Hash { input, .. } if matches!(input.as_ref(), Plan::Append { .. }))
            )
        }),
        "expected the current unified-append join shape, got {:?}",
        planned.plan_tree
    );
}

#[test]
fn partitionwise_join_guc_on_builds_append_of_child_joins_in_bound_order() {
    let catalog = catalog_with_matching_range_partitions();
    let planned = planned_stmt_for_sql_with_catalog_and_config(
        "select lp.k, rp.v from lp join rp on lp.k = rp.k",
        &catalog,
        PlannerConfig {
            enable_partitionwise_join: true,
            ..PlannerConfig::default()
        },
    );
    let children = append_with_join_children(&planned.plan_tree).unwrap_or_else(|| {
        panic!(
            "expected append of child joins, got {:?}",
            planned.plan_tree
        )
    });
    let child_names = children
        .iter()
        .map(child_relation_names)
        .collect::<Vec<_>>();

    assert_eq!(
        child_names,
        vec![
            vec!["lp_p1".to_string(), "rp_p1".to_string()],
            vec!["lp_p2".to_string(), "rp_p2".to_string()],
            vec!["lp_p3".to_string(), "rp_p3".to_string()],
        ]
    );
}

#[test]
fn partitionwise_join_requires_complete_key_equality() {
    let catalog = catalog_with_matching_range_partitions();
    let planned = planned_stmt_for_sql_with_catalog_and_config(
        "select lp.k, rp.v from lp join rp on lp.v = rp.v",
        &catalog,
        PlannerConfig {
            enable_partitionwise_join: true,
            ..PlannerConfig::default()
        },
    );

    assert!(
        append_with_join_children(&planned.plan_tree).is_none(),
        "non-key equality should not use partitionwise join: {:?}",
        planned.plan_tree
    );
}

fn catalog_with_t1_primary_key() -> Catalog {
    let mut catalog = Catalog::default();
    catalog
        .create_table(
            "t1",
            RelationDesc {
                columns: vec![
                    column_desc("a", int4(), false),
                    column_desc("b", int4(), false),
                    column_desc("c", int4(), false),
                    column_desc("d", int4(), false),
                ],
            },
        )
        .expect("create t1");
    add_ready_index(
        &mut catalog,
        "t1",
        "t1_pkey",
        true,
        true,
        &[IndexColumnDef::from("a"), IndexColumnDef::from("b")],
        None,
        None,
    );
    catalog
}

fn catalog_with_t2_unique_z(z_nullable: bool, indnullsnotdistinct: bool) -> Catalog {
    let mut catalog = Catalog::default();
    catalog
        .create_table(
            "t2",
            RelationDesc {
                columns: vec![
                    column_desc("x", int4(), false),
                    column_desc("y", int4(), false),
                    column_desc("z", int4(), z_nullable),
                ],
            },
        )
        .expect("create t2");
    add_ready_index(
        &mut catalog,
        "t2",
        "t2_z_key",
        true,
        false,
        &[IndexColumnDef::from("z")],
        Some(int4_btree_options(1, indnullsnotdistinct)),
        None,
    );
    catalog
}

fn catalog_with_t3_multiple_unique_keys() -> Catalog {
    let mut catalog = Catalog::default();
    catalog
        .create_table(
            "t3",
            RelationDesc {
                columns: vec![
                    column_desc("x", int4(), false),
                    column_desc("y", int4(), false),
                    column_desc("z", int4(), false),
                ],
            },
        )
        .expect("create t3");
    add_ready_index(
        &mut catalog,
        "t3",
        "t3_x_key",
        true,
        false,
        &[IndexColumnDef::from("x")],
        None,
        None,
    );
    add_ready_index(
        &mut catalog,
        "t3",
        "t3_xy_key",
        true,
        false,
        &[IndexColumnDef::from("x"), IndexColumnDef::from("y")],
        None,
        None,
    );
    catalog
}

fn catalog_with_t4_partial_and_expression_indexes() -> Catalog {
    let mut catalog = Catalog::default();
    catalog
        .create_table(
            "t4",
            RelationDesc {
                columns: vec![
                    column_desc("x", int4(), false),
                    column_desc("y", int4(), false),
                    column_desc("z", int4(), false),
                ],
            },
        )
        .expect("create t4");
    add_ready_index(
        &mut catalog,
        "t4",
        "t4_x_partial_key",
        true,
        false,
        &[IndexColumnDef::from("x")],
        Some(int4_btree_options(1, false)),
        Some("x > 0"),
    );
    add_ready_index(
        &mut catalog,
        "t4",
        "t4_expr_key",
        true,
        false,
        &[IndexColumnDef {
            name: "expr".into(),
            expr_sql: Some("(x + 1)".into()),
            expr_type: Some(int4()),
            collation: None,
            opclass: None,
            opclass_options: Vec::new(),
            descending: false,
            nulls_first: None,
        }],
        Some(int4_btree_options(1, false)),
        None,
    );
    catalog
}

fn plan_contains(plan: &Plan, predicate: impl Copy + Fn(&Plan) -> bool) -> bool {
    if predicate(plan) {
        return true;
    }
    match plan {
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::Values { .. }
        | Plan::FunctionScan { .. }
        | Plan::WorkTableScan { .. } => false,
        Plan::Append { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::SetOp { children, .. } => {
            children.iter().any(|child| plan_contains(child, predicate))
        }
        Plan::Hash { input, .. }
        | Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Unique { input, .. }
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
        | Plan::MergeJoin { left, right, .. }
        | Plan::RecursiveUnion {
            anchor: left,
            recursive: right,
            ..
        } => plan_contains(left, predicate) || plan_contains(right, predicate),
    }
}

fn strip_projections(plan: &Plan) -> &Plan {
    let mut current = plan;
    while let Plan::Projection { input, .. } = current {
        current = input;
    }
    current
}

fn validate_planned_stmt_for_tests(planned: &PlannedStmt) {
    super::setrefs::validate_executable_plan_for_tests_with_params(
        &planned.plan_tree,
        &planned.ext_params,
    );
}

#[test]
fn comma_join_with_equality_predicate_can_choose_hash_or_merge_join() {
    let planned = planned_stmt_for_sql(
        "select * \
         from (values (1), (2)) a(id), (values (1), (3)) b(id) \
         where a.id = b.id",
    );

    assert!(
        plan_contains(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::HashJoin { .. } | Plan::MergeJoin { .. }
        )),
        "expected comma join with equality predicate to choose hash or merge join, got {:?}",
        planned.plan_tree
    );
}

#[test]
fn cross_join_with_where_equality_predicate_can_choose_hash_or_merge_join() {
    let planned = planned_stmt_for_sql(
        "select * \
         from (values (1), (2)) a(id) cross join (values (1), (3)) b(id) \
         where a.id = b.id",
    );

    assert!(
        plan_contains(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::HashJoin { .. } | Plan::MergeJoin { .. }
        )),
        "expected cross join with equality predicate to choose hash or merge join, got {:?}",
        planned.plan_tree
    );
}

#[test]
fn pg_proc_alias_sanity_self_join_can_choose_hash_or_merge_join() {
    let catalog = Catalog::default();
    let planned = planned_stmt_for_sql_with_catalog(
        "select distinct p1.prorettype::regtype, p2.prorettype::regtype \
         from pg_proc as p1, pg_proc as p2 \
         where p1.oid != p2.oid \
           and p1.prosrc = p2.prosrc \
           and p1.prolang = 12 and p2.prolang = 12 \
           and p1.prokind != 'a' and p2.prokind != 'a' \
           and p1.prosrc not like E'range\\\\_constructor_' \
           and p2.prosrc not like E'range\\\\_constructor_' \
           and p1.prosrc not like E'multirange\\\\_constructor_' \
           and p2.prosrc not like E'multirange\\\\_constructor_' \
           and p1.prorettype < p2.prorettype \
         order by 1, 2",
        &catalog,
    );

    assert!(
        plan_contains(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::HashJoin { .. } | Plan::MergeJoin { .. }
        )),
        "expected pg_proc alias sanity self-join to choose hash or merge join, got {:?}",
        planned.plan_tree
    );
}

fn find_aggregate_plan(plan: &Plan) -> Option<&Plan> {
    match plan {
        Plan::Aggregate { .. } => Some(plan),
        Plan::Append { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::SetOp { children, .. } => children.iter().find_map(find_aggregate_plan),
        Plan::Hash { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::ProjectSet { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::BitmapHeapScan {
            bitmapqual: input, ..
        }
        | Plan::CteScan {
            cte_plan: input, ..
        } => find_aggregate_plan(input),
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. }
        | Plan::RecursiveUnion {
            anchor: left,
            recursive: right,
            ..
        } => find_aggregate_plan(left).or_else(|| find_aggregate_plan(right)),
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::Values { .. }
        | Plan::FunctionScan { .. }
        | Plan::WorkTableScan { .. } => None,
    }
}

#[test]
fn aggregate_layout_removes_non_primary_key_group_columns() {
    let catalog = catalog_with_t1_primary_key();
    let layout =
        aggregate_layout_for_sql_with_catalog("select * from t1 group by a, b, c, d", &catalog);

    assert_eq!(var_keys(&layout.group_by), vec![(1, 1), (1, 2)]);
    assert_eq!(var_keys(&layout.passthrough_exprs), vec![(1, 3), (1, 4)]);
}

#[test]
fn aggregate_layout_keeps_group_columns_when_primary_key_is_incomplete() {
    let catalog = catalog_with_t1_primary_key();
    let layout =
        aggregate_layout_for_sql_with_catalog("select a, c from t1 group by a, c, d", &catalog);

    assert_eq!(var_keys(&layout.group_by), vec![(1, 1), (1, 3), (1, 4)]);
    assert!(layout.passthrough_exprs.is_empty());
}

#[test]
fn aggregate_layout_skips_nullable_unique_key() {
    let catalog = catalog_with_t2_unique_z(true, false);
    let layout =
        aggregate_layout_for_sql_with_catalog("select y, z from t2 group by y, z", &catalog);

    assert_eq!(var_keys(&layout.group_by), vec![(1, 2), (1, 3)]);
    assert!(layout.passthrough_exprs.is_empty());
}

#[test]
fn aggregate_layout_uses_not_null_unique_key() {
    let catalog = catalog_with_t2_unique_z(false, false);
    let layout =
        aggregate_layout_for_sql_with_catalog("select y, z from t2 group by y, z", &catalog);

    assert_eq!(var_keys(&layout.group_by), vec![(1, 3)]);
    assert_eq!(var_keys(&layout.passthrough_exprs), vec![(1, 2)]);
}

#[test]
fn aggregate_layout_uses_nulls_not_distinct_unique_key() {
    let catalog = catalog_with_t2_unique_z(true, true);
    let layout =
        aggregate_layout_for_sql_with_catalog("select y, z from t2 group by y, z", &catalog);

    assert_eq!(var_keys(&layout.group_by), vec![(1, 3)]);
    assert_eq!(var_keys(&layout.passthrough_exprs), vec![(1, 2)]);
}

#[test]
fn aggregate_layout_picks_smallest_unique_key() {
    let catalog = catalog_with_t3_multiple_unique_keys();
    let layout =
        aggregate_layout_for_sql_with_catalog("select x, y, z from t3 group by x, y, z", &catalog);

    assert_eq!(var_keys(&layout.group_by), vec![(1, 1)]);
    assert_eq!(var_keys(&layout.passthrough_exprs), vec![(1, 2), (1, 3)]);
}

#[test]
fn aggregate_layout_ignores_partial_and_expression_indexes() {
    let catalog = catalog_with_t4_partial_and_expression_indexes();
    let layout =
        aggregate_layout_for_sql_with_catalog("select x, y, z from t4 group by x, y, z", &catalog);

    assert_eq!(var_keys(&layout.group_by), vec![(1, 1), (1, 2), (1, 3)]);
    assert!(layout.passthrough_exprs.is_empty());
}

#[test]
fn aggregate_layout_collapses_inner_join_duplicate_group_keys() {
    let catalog = catalog_with_people_and_pets();
    let layout = aggregate_layout_for_sql_with_catalog(
        "select p.id, q.owner_id
         from people p
         join pets q on q.owner_id = p.id
         group by p.id, q.owner_id",
        &catalog,
    );

    assert_eq!(layout.group_by.len(), 1);
    assert_eq!(layout.passthrough_exprs.len(), 1);
    assert!(matches!(layout.group_by[0], Expr::Var(_)));
    assert!(matches!(layout.passthrough_exprs[0], Expr::Var(_)));
}

#[test]
fn aggregate_layout_drops_unreferenced_join_duplicate_group_keys() {
    let catalog = catalog_with_people_and_pets();
    let sql = "select p.id
               from people p
               join pets q on q.owner_id = p.id
               group by p.id, q.owner_id";
    let layout = aggregate_layout_for_sql_with_catalog(sql, &catalog);

    assert_eq!(layout.group_by.len(), 1);
    assert!(matches!(layout.group_by[0], Expr::Var(_)));
    assert!(layout.passthrough_exprs.is_empty());

    let planned = planned_stmt_for_sql_with_catalog(sql, &catalog);
    assert!(plan_contains(&planned.plan_tree, |plan| match plan {
        Plan::Aggregate {
            group_by,
            passthrough_exprs,
            ..
        } => group_by.len() == 1 && passthrough_exprs.is_empty(),
        _ => false,
    }));
}

#[test]
fn explain_hides_projection_for_trimmed_aggregate_output_after_reduction() {
    let catalog = catalog_with_t1_primary_key();
    let planned =
        planned_stmt_for_sql_with_catalog("select a, c from t1 group by a, b, c, d", &catalog);
    let lines = explain_lines_for_planned_stmt(&planned);
    let rendered = lines.join("\n");

    assert!(
        lines.iter().any(|line| line.trim() == "HashAggregate"),
        "{rendered}"
    );
    assert!(
        lines.iter().any(|line| line.trim() == "Group Key: a, b"),
        "{rendered}"
    );
    assert!(
        !lines.iter().any(|line| line.contains("Projection")),
        "{rendered}"
    );
}

#[test]
fn explain_hides_projection_for_trimmed_aggregate_output_without_reduction() {
    let catalog = catalog_with_t1_primary_key();
    let planned =
        planned_stmt_for_sql_with_catalog("select a, c from t1 group by a, c, d", &catalog);
    let lines = explain_lines_for_planned_stmt(&planned);
    let rendered = lines.join("\n");

    assert!(
        lines.iter().any(|line| line.trim() == "HashAggregate"),
        "{rendered}"
    );
    assert!(
        lines.iter().any(|line| line.trim() == "Group Key: a, c, d"),
        "{rendered}"
    );
    assert!(
        !lines.iter().any(|line| line.contains("Projection")),
        "{rendered}"
    );
}

#[test]
fn cartesian_join_plans_as_cross_join_without_hidden_order_by() {
    let catalog = catalog_with_people_and_pets();
    let planned = planned_stmt_for_sql_with_catalog(
        "select p.id, q.owner_id from people p, pets q",
        &catalog,
    );

    assert!(
        plan_contains(&planned.plan_tree, |plan| {
            matches!(
                plan,
                Plan::NestedLoopJoin {
                    kind: JoinType::Cross,
                    ..
                }
            )
        }),
        "expected cartesian join to stay a cross/nested-loop join, got {:?}",
        planned.plan_tree
    );
    assert!(
        !plan_contains(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::OrderBy { .. }
        )),
        "unordered cartesian join must not synthesize a final sort, got {:?}",
        planned.plan_tree
    );
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
                        Plan::SeqScan { relation_name, .. } if relation_name.starts_with("people")
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
                } | Plan::MergeJoin {
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
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::ProjectSet { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::BitmapHeapScan {
            bitmapqual: input, ..
        } => find_seq_scan(input),
        Plan::Append { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::SetOp { children, .. } => children.iter().find_map(find_seq_scan),
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. } => {
            find_seq_scan(left).or_else(|| find_seq_scan(right))
        }
        Plan::Result { .. }
        | Plan::IndexOnlyScan { .. }
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
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::Values { .. }
        | Plan::FunctionScan { .. }
        | Plan::WorkTableScan { .. } => 0,
        Plan::Append { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::SetOp { children, .. } => children
            .iter()
            .map(|child| count_plan_nodes(child, predicate))
            .sum(),
        Plan::Hash { input, .. }
        | Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Unique { input, .. }
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
        | Plan::MergeJoin { left, right, .. }
        | Plan::RecursiveUnion {
            anchor: left,
            recursive: right,
            ..
        } => count_plan_nodes(left, predicate) + count_plan_nodes(right, predicate),
    }
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
fn distinct_grouped_aggregate_uses_sorted_strategy() {
    let planned = planned_stmt_for_values_sql(
        "select grp, sum(distinct val) from (values (2, 1), (1, 1), (2, 2)) as t(grp, val) group by grp",
    );
    let aggregate = find_aggregate_plan(&planned.plan_tree).expect("aggregate plan");
    match aggregate {
        Plan::Aggregate {
            strategy, input, ..
        } => {
            assert_eq!(*strategy, AggregateStrategy::Sorted);
            assert!(matches!(input.as_ref(), Plan::OrderBy { .. }));
        }
        other => panic!("expected aggregate plan, got {other:?}"),
    }
}

#[test]
fn ordinary_grouped_aggregate_uses_hashed_strategy() {
    let planned = planned_stmt_for_values_sql(
        "select grp, count(*) from (values (2), (1), (2)) as t(grp) group by grp",
    );
    let aggregate = find_aggregate_plan(&planned.plan_tree).expect("aggregate plan");
    match aggregate {
        Plan::Aggregate {
            strategy, input, ..
        } => {
            assert_eq!(*strategy, AggregateStrategy::Hashed);
            assert!(!matches!(input.as_ref(), Plan::OrderBy { .. }));
        }
        other => panic!("expected aggregate plan, got {other:?}"),
    }
}

#[test]
fn disabled_hashagg_uses_sorted_grouping_strategy() {
    let catalog = LiteralDefaultCatalog;
    let planned = planned_stmt_for_sql_with_catalog_and_config(
        "select grp, count(*) from (values (2), (1), (2)) as t(grp) group by grp",
        &catalog,
        PlannerConfig {
            enable_hashagg: false,
            ..PlannerConfig::default()
        },
    );
    let aggregate = find_aggregate_plan(&planned.plan_tree).expect("aggregate plan");
    match aggregate {
        Plan::Aggregate {
            strategy, input, ..
        } => {
            assert_eq!(*strategy, AggregateStrategy::Sorted);
            assert!(matches!(input.as_ref(), Plan::OrderBy { .. }));
        }
        other => panic!("expected aggregate plan, got {other:?}"),
    }
}

#[test]
fn aggregate_pathkeys_follow_strategy() {
    let key = pathkey(var(10, 1));
    let hashed = Path::Aggregate {
        plan_info: PlanEstimate::default(),
        pathtarget: PathTarget::new(vec![var(10, 1)]),
        slot_id: 20,
        strategy: AggregateStrategy::Hashed,
        disabled: false,
        pathkeys: vec![key.clone()],
        input: Box::new(values_path(10, 1.0, 1.0)),
        group_by: vec![var(10, 1)],
        passthrough_exprs: Vec::new(),
        accumulators: Vec::new(),
        having: None,
        output_columns: vec![QueryColumn {
            name: "grp".into(),
            sql_type: int4(),
            wire_type_oid: None,
        }],
    };
    assert!(hashed.pathkeys().is_empty());

    let sorted = Path::Aggregate {
        plan_info: PlanEstimate::default(),
        pathtarget: PathTarget::new(vec![var(10, 1)]),
        slot_id: 20,
        strategy: AggregateStrategy::Sorted,
        disabled: false,
        pathkeys: vec![key.clone()],
        input: Box::new(values_path(10, 1.0, 1.0)),
        group_by: vec![var(10, 1)],
        passthrough_exprs: Vec::new(),
        accumulators: Vec::new(),
        having: None,
        output_columns: vec![QueryColumn {
            name: "grp".into(),
            sql_type: int4(),
            wire_type_oid: None,
        }],
    };
    assert_eq!(sorted.pathkeys(), vec![key]);
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
fn planned_window_frame_constant_offsets_are_preserved() {
    let planned = planned_stmt_for_sql(
        "select sum(t.x) over (order by t.x rows between 1 preceding and 2 following), t.x \
         from (values (1), (2)) as t(x)",
    );

    assert!(plan_contains(&planned.plan_tree, |plan| match plan {
        Plan::WindowAgg { clause, .. } => {
            clause.spec.order_by.len() == 1
                && is_special_user_var(&clause.spec.order_by[0].expr, OUTER_VAR, 0)
                && matches!(
                    &clause.spec.frame.start_bound,
                    WindowFrameBound::OffsetPreceding(offset)
                        if matches!(offset.expr, Expr::Const(Value::Int64(1)))
                )
                && matches!(
                    &clause.spec.frame.end_bound,
                    WindowFrameBound::OffsetFollowing(offset)
                        if matches!(offset.expr, Expr::Const(Value::Int64(2)))
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

fn invalid_executable_projection_plan() -> Plan {
    Plan::Projection {
        plan_info: PlanEstimate::new(1.0, 1.0, 1.0, 1),
        input: Box::new(values_path(10, 1.0, 1.0).into_plan()),
        targets: vec![TargetEntry::new(
            "bad",
            Expr::Aggref(Box::new(Aggref {
                aggfnoid: 0,
                aggtype: int4(),
                aggvariadic: false,
                aggdistinct: false,
                direct_args: vec![],
                args: vec![],
                aggorder: vec![],
                aggfilter: None,
                agglevelsup: 0,
                aggno: 0,
            })),
            int4(),
            1,
        )],
    }
}

fn invalid_planner_filter_path() -> Path {
    filter_path(
        1.0,
        1.0,
        values_path(10, 1.0, 1.0),
        Expr::Param(Param {
            paramkind: ParamKind::Exec,
            paramid: 1,
            paramtype: bool_ty(),
        }),
    )
}

// Some optimized nightly test builds abort while unwinding caught panics, so
// keep these intentional validator panics isolated from the parent test process.
fn run_optimizer_validator_panic_child(case: &str) -> String {
    let output = std::process::Command::new(std::env::current_exe().expect("current test binary"))
        .env("PGRUST_OPTIMIZER_VALIDATOR_PANIC_CASE", case)
        .arg("backend::optimizer::tests::optimizer_validator_panic_child")
        .arg("--exact")
        .arg("--ignored")
        .arg("--nocapture")
        .output()
        .expect("run validator panic child");
    assert!(
        !output.status.success(),
        "validator child should reject invalid {case}"
    );
    let mut message = String::from_utf8_lossy(&output.stderr).into_owned();
    message.push_str(&String::from_utf8_lossy(&output.stdout));
    message
}

#[test]
#[ignore]
fn optimizer_validator_panic_child() {
    match std::env::var("PGRUST_OPTIMIZER_VALIDATOR_PANIC_CASE")
        .expect("validator panic case")
        .as_str()
    {
        "executable_projection" => {
            super::setrefs::validate_executable_plan_for_tests(
                &invalid_executable_projection_plan(),
            );
        }
        "planner_filter" => {
            super::setrefs::validate_planner_path_for_tests(&invalid_planner_filter_path());
        }
        case => panic!("unknown validator panic case: {case}"),
    }
}

#[test]
fn executable_plan_validator_reports_node_and_field() {
    let message = run_optimizer_validator_panic_child("executable_projection");
    assert!(message.contains("Projection.targets"));
    assert!(message.contains("Aggref"));
}

#[test]
fn planner_path_validator_rejects_executor_only_refs() {
    let message = run_optimizer_validator_panic_child("planner_filter");
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
                timezone: None,
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
            match *input {
                Plan::FunctionScan {
                    call, table_alias, ..
                } => {
                    assert_eq!(table_alias.as_deref(), Some("g"));
                    assert_eq!(call.output_columns()[0].name, "x");
                }
                other => panic!("expected function scan input, got {other:?}"),
            }
        }
        other => panic!("expected top-level filter plan, got {other:?}"),
    }
}

#[test]
fn planner_estimates_constant_generate_series_rows() {
    let planned = planned_stmt_for_sql(
        "select * from generate_series(timestamp '2024-02-01', timestamp '2024-03-01', interval '7 day') g(s)",
    );
    match planned.plan_tree {
        Plan::FunctionScan { plan_info, .. } => {
            assert_eq!(plan_info.plan_rows.as_f64().round() as u64, 5);
        }
        other => panic!("expected function scan, got {other:?}"),
    }

    let planned = planned_stmt_for_sql("select * from generate_series(1.0, 25.0, 2.0) g(s)");
    match planned.plan_tree {
        Plan::FunctionScan { plan_info, .. } => {
            assert_eq!(plan_info.plan_rows.as_f64().round() as u64, 13);
        }
        other => panic!("expected function scan, got {other:?}"),
    }
}

#[test]
fn planner_estimates_left_join_to_grouped_subquery_as_outer_rows() {
    let mut catalog = Catalog::default();
    let table = catalog
        .create_table(
            "grouping_unique",
            RelationDesc {
                columns: vec![column_desc("x", int4(), false)],
            },
        )
        .expect("create grouping_unique");
    catalog
        .set_relation_stats(table.relation_oid, 10, 1000.0)
        .expect("seed grouping_unique stats");

    let planned = planned_stmt_for_sql_with_catalog(
        "select * from generate_series(1, 1) t1 left join \
         (select x from grouping_unique t2 group by x) as q1 on t1.t1 = q1.x",
        &catalog,
    );
    assert_eq!(
        planned.plan_tree.plan_info().plan_rows.as_f64().round() as u64,
        1
    );
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

    let Plan::Limit { input, .. } = strip_projections(&planned.plan_tree) else {
        panic!("expected limit at top, got {:?}", planned.plan_tree);
    };
    let Plan::LockRows {
        input, row_marks, ..
    } = input.as_ref()
    else {
        panic!("expected lock rows below limit, got {:?}", input);
    };
    assert!(matches!(
        strip_projections(input.as_ref()),
        Plan::OrderBy { .. }
    ));
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
fn planner_lowers_union_all_append_children_with_their_own_roots() {
    let planned = planned_stmt_for_sql(
        "select x
         from (values (1)) base(x)
         union all
         select l.x + r.y
         from (values (1)) l(x)
         join (values (2)) r(y) on true",
    );

    assert!(matches!(planned.plan_tree, Plan::Append { .. }));
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
fn planner_rewrites_simple_min_aggregate_into_forward_index_only_subplan() {
    let catalog = catalog_with_indexed_items();
    let stmt = parse_select("select min(id) from items").expect("parse");
    let (query, _) = analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[])
        .expect("analyze");
    let planned = super::planner(query, &catalog).expect("plan");

    assert_eq!(planned.subplans.len(), 1);
    let subplan = &planned.subplans[0];
    assert!(plan_contains(subplan, |plan| matches!(
        plan,
        Plan::Limit { .. }
    )));
    assert!(plan_contains(subplan, |plan| matches!(
        plan,
        Plan::IndexOnlyScan { direction, .. }
            if *direction == crate::include::access::relscan::ScanDirection::Forward
    )));
    assert!(plan_contains(subplan, |plan| match plan {
        Plan::IndexOnlyScan { keys, .. } | Plan::IndexScan { keys, .. } => {
            keys.len() == 1
                && keys.iter().any(|key| {
                    key.strategy == 1
                        && matches!(&key.argument, IndexScanKeyArgument::Const(Value::Null))
                })
        }
        _ => false,
    }));
    assert!(!plan_contains(subplan, |plan| matches!(
        plan,
        Plan::Aggregate { .. } | Plan::Filter { .. } | Plan::OrderBy { .. }
    )));
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
        Plan::IndexOnlyScan { .. }
    )));
    assert!(!plan_contains(subplan, |plan| matches!(
        plan,
        Plan::Aggregate { .. }
    )));

    assert!(plan_contains(subplan, |plan| match plan {
        Plan::IndexOnlyScan { direction, .. } => {
            *direction == crate::include::access::relscan::ScanDirection::Backward
        }
        Plan::IndexScan {
            direction,
            index_only,
            ..
        } => {
            *direction == crate::include::access::relscan::ScanDirection::Backward && *index_only
        }
        _ => false,
    }));
    assert!(plan_contains(subplan, |plan| match plan {
        Plan::IndexOnlyScan { keys, .. } | Plan::IndexScan { keys, .. } => {
            keys.iter().any(|key| {
                key.strategy == 1
                    && matches!(&key.argument, IndexScanKeyArgument::Const(Value::Null))
            }) && keys.iter().any(|key| {
                key.strategy == 1
                    && matches!(&key.argument, IndexScanKeyArgument::Const(Value::Int32(42)))
            })
        }
        _ => false,
    }));
    assert!(!plan_contains(subplan, |plan| matches!(
        plan,
        Plan::Filter { .. } | Plan::OrderBy { .. }
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
            && plan_contains(subplan, |plan| matches!(plan, Plan::IndexOnlyScan { .. }))
            && !plan_contains(subplan, |plan| matches!(plan, Plan::Aggregate { .. }))
    }));
    assert!(planned.subplans.iter().any(|subplan| {
        plan_contains(subplan, |plan| {
            matches!(
                plan,
                Plan::IndexOnlyScan { direction, .. }
                    if *direction == crate::include::access::relscan::ScanDirection::Forward
            )
        })
    }));
    assert!(planned.subplans.iter().any(|subplan| {
        plan_contains(subplan, |plan| {
            matches!(
                plan,
                Plan::IndexOnlyScan { direction, .. }
                    if *direction == crate::include::access::relscan::ScanDirection::Backward
            )
        })
    }));
}

#[test]
fn planner_uses_unique_for_simple_select_distinct() {
    let catalog = catalog_with_indexed_items();
    let planned = planned_stmt_for_sql_with_catalog("select distinct id from items", &catalog);

    assert!(matches!(planned.plan_tree, Plan::Unique { .. }));
    assert_eq!(
        count_plan_nodes(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::Unique { .. }
        )),
        1
    );
    assert!(!plan_contains(&planned.plan_tree, |plan| matches!(
        plan,
        Plan::SetOp { .. }
    )));
}

#[test]
fn planner_preserves_distinct_before_final_order_projection() {
    let catalog = catalog_with_distinct_on_tbl();
    let planned = planned_stmt_for_sql_with_catalog(
        "select distinct y, x from distinct_on_tbl order by y",
        &catalog,
    );

    assert_eq!(
        count_plan_nodes(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::Unique { .. }
                | Plan::Aggregate {
                    strategy: AggregateStrategy::Hashed,
                    ..
                }
        )),
        1,
        "{:#?}",
        planned.plan_tree
    );
    assert!(plan_contains(&planned.plan_tree, |plan| matches!(
        plan,
        Plan::OrderBy { .. }
    )));
}

#[test]
fn explain_hash_distinct_group_key_uses_distinct_expr() {
    let catalog = LiteralDefaultCatalog;
    let planned = planned_stmt_for_sql_with_catalog_and_config(
        "select distinct g%1000 from generate_series(0,9999) g",
        &catalog,
        PlannerConfig {
            enable_sort: false,
            ..PlannerConfig::default()
        },
    );
    let lines = explain_lines_for_planned_stmt(&planned);

    assert!(
        lines.iter().any(|line| line == "  Group Key: (g % 1000)"),
        "{lines:#?}"
    );
}

#[test]
fn explain_window_over_grouped_subquery_hides_group_projection() {
    let catalog = catalog_with_distinct_on_limit_tbl();
    let planned = planned_stmt_for_sql_with_catalog_and_config(
        "select first_value(max(x)) over (), y
         from (select four as x, two + hundred as y from limit_tbl) ss
         group by y",
        &catalog,
        PlannerConfig {
            enable_sort: false,
            ..PlannerConfig::default()
        },
    );
    let lines = explain_lines_for_planned_stmt(&planned);

    assert!(
        lines.iter().any(|line| line.trim() == "WindowAgg"),
        "{lines:#?}"
    );
    assert!(
        lines.iter().any(|line| line.trim() == "->  HashAggregate"),
        "{lines:#?}"
    );
    assert!(
        !lines.iter().any(|line| line.trim() == "->  Projection"),
        "{lines:#?}"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.trim() == "Group Key: (limit_tbl.two + limit_tbl.hundred)"),
        "{lines:#?}"
    );
}

#[test]
fn planner_keeps_unique_for_ordered_select_distinct() {
    let catalog = catalog_with_indexed_items();
    let planned = planned_stmt_for_sql_with_catalog(
        "select distinct id from items order by id desc",
        &catalog,
    );

    assert_eq!(
        count_plan_nodes(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::Unique { .. }
        )),
        1
    );
}

#[test]
fn planner_keeps_unique_for_ordered_select_distinct_saop_index_path() {
    let catalog = catalog_with_indexed_later_column();
    let planned = planned_stmt_for_sql_with_catalog_and_config(
        "select distinct hundred from items where hundred in (47, 48, 72, 82) order by hundred desc",
        &catalog,
        PlannerConfig {
            enable_seqscan: false,
            enable_bitmapscan: false,
            ..PlannerConfig::default()
        },
    );

    assert_eq!(
        count_plan_nodes(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::Unique { .. }
        )),
        1
    );
}

#[test]
fn planner_uses_index_order_for_distinct_on_reordered_keys() {
    let catalog = catalog_with_distinct_on_tbl();
    let planned = planned_stmt_for_sql_with_catalog(
        "select distinct on (y, x) x, y from distinct_on_tbl",
        &catalog,
    );

    assert_eq!(
        count_plan_nodes(&planned.plan_tree, |plan| {
            matches!(plan, Plan::Unique { key_indices, .. } if key_indices.len() == 2)
        }),
        1
    );
    fn skip_projection(plan: &Plan) -> &Plan {
        match plan {
            Plan::Projection { input, .. } => input.as_ref(),
            other => other,
        }
    }
    let unique_input = match skip_projection(&planned.plan_tree) {
        Plan::Unique { input, .. } => Some(skip_projection(input.as_ref())),
        _ => None,
    };
    assert!(matches!(
        unique_input,
        Some(Plan::IndexOnlyScan { index_name, .. } | Plan::IndexScan { index_name, .. })
            if index_name == "distinct_on_tbl_x_y_idx"
    ));
}

#[test]
fn planner_lowers_constant_distinct_on_key_to_limit() {
    let catalog = catalog_with_distinct_on_limit_tbl();
    let planned = planned_stmt_for_sql_with_catalog(
        "select distinct on (four) four, hundred from limit_tbl where four = 0 order by 1, 2",
        &catalog,
    );

    assert_eq!(
        count_plan_nodes(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::Limit { .. }
        )),
        1
    );
    assert_eq!(
        count_plan_nodes(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::Unique { .. }
        )),
        0
    );
    assert!(plan_contains(&planned.plan_tree, |plan| matches!(
        plan,
        Plan::IndexScan { index_name, .. } if index_name == "limit_tbl_hundred"
    ) || matches!(
        plan,
        Plan::IndexOnlyScan { index_name, .. } if index_name == "limit_tbl_hundred"
    )));
}

#[test]
fn planner_rewrites_distinct_minmax_with_unique_index_only_subplans() {
    let catalog = catalog_with_indexed_items();
    let planned =
        planned_stmt_for_sql_with_catalog("select distinct min(id), max(id) from items", &catalog);

    assert_eq!(planned.subplans.len(), 2);
    assert!(plan_contains(&planned.plan_tree, |plan| matches!(
        plan,
        Plan::Unique { .. }
    )));
    assert!(!plan_contains(&planned.plan_tree, |plan| matches!(
        plan,
        Plan::Aggregate { .. } | Plan::SetOp { .. }
    )));
    assert!(planned.subplans.iter().all(|subplan| {
        plan_contains(subplan, |plan| matches!(plan, Plan::Limit { .. }))
            && plan_contains(subplan, |plan| matches!(plan, Plan::IndexOnlyScan { .. }))
            && !plan_contains(subplan, |plan| matches!(plan, Plan::Aggregate { .. }))
    }));
}

#[test]
fn planner_rewrites_inherited_minmax_with_directional_index_only_subplans() {
    let catalog = catalog_with_inherited_indexed_items();
    let planned = planned_stmt_for_sql_with_catalog("select min(id), max(id) from items", &catalog);

    assert_eq!(planned.subplans.len(), 2);
    assert!(planned.subplans.iter().all(|subplan| {
        plan_contains(subplan, |plan| matches!(plan, Plan::Limit { .. }))
            && plan_contains(subplan, |plan| matches!(plan, Plan::IndexOnlyScan { .. }))
    }));
    assert!(planned.subplans.iter().any(|subplan| {
        plan_contains(subplan, |plan| {
            matches!(
                plan,
                Plan::IndexOnlyScan { direction, .. }
                    if *direction == crate::include::access::relscan::ScanDirection::Forward
            )
        })
    }));
    assert!(planned.subplans.iter().any(|subplan| {
        plan_contains(subplan, |plan| {
            matches!(
                plan,
                Plan::IndexOnlyScan { direction, .. }
                    if *direction == crate::include::access::relscan::ScanDirection::Backward
            )
        })
    }));
}

#[test]
fn planner_keeps_index_scan_when_index_is_not_covering() {
    let catalog = catalog_with_noncovering_indexed_items();
    let planned = planned_stmt_for_sql_with_catalog(
        "select id, payload from items where id < 42 order by id limit 1",
        &catalog,
    );

    assert!(plan_contains(&planned.plan_tree, |plan| matches!(
        plan,
        Plan::IndexScan { .. }
    )));
    assert!(!plan_contains(&planned.plan_tree, |plan| matches!(
        plan,
        Plan::IndexOnlyScan { .. }
    )));
}

#[test]
fn planner_uses_spgist_box_index_only_when_opclass_can_return_data() {
    let catalog = catalog_with_spgist_box_temp();
    let planned = planned_stmt_for_sql_with_catalog(
        "select * from box_temp where f1 << '(10,20),(30,40)'::box",
        &catalog,
    );

    assert!(plan_contains(&planned.plan_tree, |plan| matches!(
        plan,
        Plan::IndexOnlyScan { index_name, .. } if index_name == "box_spgist"
    )));
    let lines = explain_lines_for_planned_stmt(&planned);
    assert!(
        lines
            .iter()
            .any(|line| line.contains("Index Only Scan using box_spgist on box_temp")),
        "expected SP-GiST box covering query to use index-only scan, got {lines:?}"
    );
}

#[test]
fn planner_uses_spgist_polygon_distance_ordering_for_window_input() {
    let mut catalog = Catalog::default();
    let table = catalog
        .create_table(
            "quad_poly_tbl",
            RelationDesc {
                columns: vec![
                    column_desc("id", int4(), false),
                    column_desc("p", polygon_ty(), true),
                ],
            },
        )
        .expect("create test catalog relation");
    let index = catalog
        .create_index_for_relation_with_options_and_flags(
            "quad_poly_tbl_idx",
            table.relation_oid,
            false,
            false,
            &[IndexColumnDef::from("p")],
            &polygon_spgist_options(1),
            None,
        )
        .expect("create test catalog index");
    catalog
        .set_index_ready_valid(index.relation_oid, true, true)
        .expect("mark test catalog index usable");
    catalog
        .set_relation_stats(table.relation_oid, 512, 11_003.0)
        .expect("seed test catalog table stats");
    catalog
        .set_relation_stats(index.relation_oid, 64, 11_003.0)
        .expect("seed test catalog index stats");

    let planned = planned_stmt_for_sql_with_catalog_and_config(
        "select rank() over (order by p <-> point '123,456') n, \
                p <-> point '123,456' dist, id \
         from quad_poly_tbl \
         where p <@ polygon '((300,300),(400,600),(600,500),(700,200))'",
        &catalog,
        PlannerConfig {
            enable_seqscan: false,
            enable_bitmapscan: false,
            ..PlannerConfig::default()
        },
    );
    let lines = explain_lines_for_planned_stmt(&planned);

    assert!(
        plan_contains(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::IndexScan {
                index_name,
                order_by_keys,
                ..
            } if index_name == "quad_poly_tbl_idx" && !order_by_keys.is_empty()
        )),
        "expected ordered polygon SP-GiST index scan, got {lines:?}"
    );
    assert!(
        !plan_contains(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::OrderBy { .. }
        )),
        "expected no explicit sort, got {lines:?}"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.contains("Order By: (p <-> '(123,456)'::point)")),
        "expected distance operator in ordered index explain, got {lines:?}"
    );
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
    assert!(lines.iter().any(|line| line.trim() == "->  Limit"));
    assert!(
        lines
            .iter()
            .any(|line| matches!(line.trim(), "Result" | "->  Result"))
    );
    assert!(
        lines
            .iter()
            .any(|line| line.contains("Index Scan") || line.contains("Index Only Scan"))
    );
    assert!(!lines.iter().any(|line| line.contains("Aggregate")));
}

#[test]
fn explain_formats_distinct_minmax_with_unique_and_index_only_scan() {
    let catalog = catalog_with_inherited_indexed_items();
    let planned =
        planned_stmt_for_sql_with_catalog("select distinct min(id), max(id) from items", &catalog);

    let mut lines = Vec::new();
    crate::backend::commands::explain::format_explain_plan_with_subplans(
        &planned.plan_tree,
        &planned.subplans,
        0,
        false,
        &mut lines,
    );

    assert!(lines.iter().any(|line| line.trim() == "Unique"));
    assert!(lines.iter().any(|line| line.trim() == "->  Result"));
    assert!(lines.iter().any(|line| line.contains("Index Only Scan")));
}

#[test]
fn planner_preserves_ordered_index_path_under_limit() {
    let catalog = catalog_with_indexed_items();
    let planned =
        planned_stmt_for_sql_with_catalog("select id from items order by id limit 1", &catalog);

    assert!(matches!(planned.plan_tree, Plan::Limit { .. }));
    assert!(plan_contains(&planned.plan_tree, |plan| matches!(
        plan,
        Plan::IndexOnlyScan { direction, .. }
            if *direction == crate::include::access::relscan::ScanDirection::Forward
    )));
    assert!(!plan_contains(&planned.plan_tree, |plan| matches!(
        plan,
        Plan::OrderBy { .. }
    )));
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
            && plan_contains(subplan, |plan| matches!(plan, Plan::IndexOnlyScan { .. }))
    }));
}

#[test]
fn planner_uses_runtime_index_key_for_correlated_limit_subplan() {
    fn contains_exec_param(expr: &Expr) -> bool {
        match expr {
            Expr::Param(Param {
                paramkind: ParamKind::Exec,
                ..
            }) => true,
            Expr::Op(op) => op.args.iter().any(contains_exec_param),
            Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => contains_exec_param(inner),
            _ => false,
        }
    }

    let catalog = catalog_with_indexed_items();
    let planned = planned_stmt_for_sql_with_catalog(
        "select o.id, (select i.id from items i where i.id = o.id + 1 limit 1) from items o",
        &catalog,
    );

    assert!(planned.subplans.iter().any(|subplan| {
        plan_contains(subplan, |plan| matches!(plan, Plan::Limit { .. }))
            && plan_contains(subplan, |plan| match plan {
                Plan::IndexOnlyScan { keys, .. } => keys.iter().any(|key| {
                    matches!(
                        &key.argument,
                        IndexScanKeyArgument::Runtime(expr) if contains_exec_param(expr)
                    )
                }),
                _ => false,
            })
    }));
    validate_planned_stmt_for_tests(&planned);
}

#[test]
fn planner_simplifies_outer_max_of_unique_scalar_sublink() {
    let catalog = catalog_with_unique_indexed_items();
    let planned = planned_stmt_for_sql_with_catalog_and_larger_parse_stack(
        "select (select max((select i.payload from items i where i.id = o.id))) from items o",
        &catalog,
    );

    assert!(
        plan_contains(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::SeqScan { .. }
        )),
        "expected the outer query to keep scanning one output row per outer tuple: {planned:#?}"
    );
    assert!(
        planned
            .subplans
            .iter()
            .all(|subplan| !plan_contains(subplan, |plan| matches!(plan, Plan::Aggregate { .. }))),
        "outer max should not remain as a per-row aggregate subplan: {planned:#?}"
    );
    assert!(
        planned
            .subplans
            .iter()
            .any(|subplan| plan_contains(subplan, |plan| matches!(plan, Plan::IndexScan { .. }))),
        "expected the remaining scalar lookup subplan to use the unique index: {planned:#?}"
    );

    validate_planned_stmt_for_tests(&planned);
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

    validate_planned_stmt_for_tests(&planned);

    let debug = format!("{planned:#?}");
    assert!(debug.contains("paramkind: Exec"), "{debug}");
    assert!(!debug.contains("Aggref"), "{debug}");
}

#[test]
fn planner_lowers_outer_aggregate_filter_refs_in_scalar_subqueries() {
    let planned = planned_stmt_for_sql(
        "select (select count(*) filter (where outer_c <> 0) \
         from (values (1)) t0(inner_c)) \
         from (values (2),(3)) t1(outer_c)",
    );

    validate_planned_stmt_for_tests(&planned);

    assert!(
        plan_contains(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::Aggregate { .. }
        )),
        "outer FILTER reference should make the parent query aggregate: {planned:#?}"
    );
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
            | Plan::IncrementalSort { input, .. }
            | Plan::Limit { input, .. }
            | Plan::LockRows { input, .. }
            | Plan::Unique { input, .. }
            | Plan::Aggregate { input, .. }
            | Plan::WindowAgg { input, .. }
            | Plan::BitmapHeapScan {
                bitmapqual: input, ..
            } => find_project_set(input),
            Plan::Append { children, .. }
            | Plan::BitmapOr { children, .. }
            | Plan::MergeAppend { children, .. }
            | Plan::SetOp { children, .. } => children.iter().find_map(find_project_set),
            Plan::NestedLoopJoin { left, right, .. }
            | Plan::HashJoin { left, right, .. }
            | Plan::MergeJoin { left, right, .. } => {
                find_project_set(left).or_else(|| find_project_set(right))
            }
            Plan::Result { .. }
            | Plan::SeqScan { .. }
            | Plan::IndexOnlyScan { .. }
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
fn grouped_target_srf_uses_project_set_before_aggregate() {
    let planned = planned_stmt_for_sql(
        "select * from \
         (select generate_series(1, a) as g, count(*) from (values (1), (2)) v(a) group by 1) ss \
         where ss.g = 1",
    );

    validate_planned_stmt_for_tests(&planned);

    assert!(
        plan_contains(&planned.plan_tree, |plan| matches!(
            plan,
            Plan::SubqueryScan { .. }
        )),
        "expected grouped SRF subquery boundary to stay visible: {planned:#?}"
    );

    fn aggregate_reads_project_set(plan: &Plan) -> bool {
        match plan {
            Plan::Aggregate { input, .. } => {
                plan_contains(input, |child| matches!(child, Plan::ProjectSet { .. }))
            }
            Plan::Hash { input, .. }
            | Plan::Filter { input, .. }
            | Plan::Projection { input, .. }
            | Plan::OrderBy { input, .. }
            | Plan::IncrementalSort { input, .. }
            | Plan::Limit { input, .. }
            | Plan::LockRows { input, .. }
            | Plan::Unique { input, .. }
            | Plan::WindowAgg { input, .. }
            | Plan::ProjectSet { input, .. }
            | Plan::BitmapHeapScan {
                bitmapqual: input, ..
            }
            | Plan::SubqueryScan { input, .. }
            | Plan::CteScan {
                cte_plan: input, ..
            } => aggregate_reads_project_set(input),
            Plan::Append { children, .. }
            | Plan::BitmapOr { children, .. }
            | Plan::MergeAppend { children, .. }
            | Plan::SetOp { children, .. } => children.iter().any(aggregate_reads_project_set),
            Plan::NestedLoopJoin { left, right, .. }
            | Plan::HashJoin { left, right, .. }
            | Plan::MergeJoin { left, right, .. }
            | Plan::RecursiveUnion {
                anchor: left,
                recursive: right,
                ..
            } => aggregate_reads_project_set(left) || aggregate_reads_project_set(right),
            Plan::Result { .. }
            | Plan::SeqScan { .. }
            | Plan::IndexOnlyScan { .. }
            | Plan::IndexScan { .. }
            | Plan::BitmapIndexScan { .. }
            | Plan::Values { .. }
            | Plan::FunctionScan { .. }
            | Plan::WorkTableScan { .. } => false,
        }
    }

    assert!(
        aggregate_reads_project_set(&planned.plan_tree),
        "expected grouped SRF to be projected before aggregation: {planned:#?}"
    );
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
fn cross_values_join_emits_swapped_candidate_with_logical_pathtarget_order() {
    let paths = super::build_join_paths(
        values_path_with_rows(1, 0.0, 0.03, 3.0),
        values_path_with_rows(2, 0.0, 0.03, 3.0),
        &[1],
        &[2],
        JoinType::Cross,
        vec![],
    );

    let swapped = paths
        .into_iter()
        .find(|path| match path {
            Path::NestedLoopJoin {
                left,
                kind: JoinType::Cross,
                ..
            } => left.output_vars().first() == Some(&var(2, 1)),
            _ => false,
        })
        .expect("swapped values cross join");

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
fn cross_values_join_prefers_filtered_values_side_as_outer() {
    let paths = super::build_join_paths(
        values_path_with_rows(1, 0.0, 0.03, 3.0),
        filtered_values_path_with_rows(2, 0.0, 0.0475, 2.985),
        &[1],
        &[2],
        JoinType::Cross,
        vec![],
    );

    let default = paths
        .iter()
        .find(|path| match path {
            Path::NestedLoopJoin { left, .. } => left.output_vars().first() == Some(&var(1, 1)),
            _ => false,
        })
        .expect("default values cross join");
    let swapped = paths
        .iter()
        .find(|path| match path {
            Path::NestedLoopJoin {
                left,
                kind: JoinType::Cross,
                ..
            } => left.output_vars().first() == Some(&var(2, 1)),
            _ => false,
        })
        .expect("swapped values cross join");

    assert!(
        swapped.plan_info().total_cost.as_f64() < default.plan_info().total_cost.as_f64(),
        "expected filtered values side as cheaper outer: default={:?}, swapped={:?}",
        default.plan_info(),
        swapped.plan_info()
    );

    let best = paths
        .iter()
        .min_by(|left, right| {
            left.plan_info()
                .total_cost
                .as_f64()
                .partial_cmp(&right.plan_info().total_cost.as_f64())
                .unwrap()
        })
        .expect("best values cross join path");
    match best {
        Path::NestedLoopJoin {
            left,
            kind: JoinType::Cross,
            ..
        } => {
            assert_eq!(left.output_vars().first(), Some(&var(2, 1)));
        }
        other => panic!("expected swapped values cross join best path, got {other:?}"),
    }
}

#[test]
fn nested_loop_cost_prefers_materializing_smaller_inner_side() {
    let paths = super::build_join_paths(
        seqscan_path_with_rows(1, 0.0, 1.04, 2.0),
        seqscan_path_with_rows(2, 0.0, 1.03, 3.0),
        &[1],
        &[2],
        JoinType::Cross,
        vec![],
    );

    let default = paths
        .iter()
        .find(|path| match path {
            Path::NestedLoopJoin { left, .. } => left.output_vars().first() == Some(&var(1, 1)),
            _ => false,
        })
        .expect("default nested loop");
    let swapped = paths
        .iter()
        .find(|path| match path {
            Path::NestedLoopJoin { left, .. } => left.output_vars().first() == Some(&var(2, 1)),
            _ => false,
        })
        .expect("swapped nested loop");

    assert!(
        swapped.plan_info().total_cost.as_f64() < default.plan_info().total_cost.as_f64(),
        "expected materialized smaller inner to be cheaper: default={:?}, swapped={:?}",
        default.plan_info(),
        swapped.plan_info()
    );

    let best = paths
        .iter()
        .min_by(|left, right| {
            left.plan_info()
                .total_cost
                .as_f64()
                .partial_cmp(&right.plan_info().total_cost.as_f64())
                .unwrap()
        })
        .expect("best path");
    match best {
        Path::NestedLoopJoin { left, .. } => {
            assert_eq!(left.output_vars().first(), Some(&var(2, 1)));
        }
        other => panic!("expected nested loop best path, got {other:?}"),
    }
}

#[test]
fn hash_join_cost_accounts_for_build_probe_qual_and_output_cpu() {
    let eq_clause = restrict(eq(var(1, 1), var(2, 1)));
    let residual_clause = restrict(gt(var(1, 2), var(2, 2)));
    let paths = super::build_join_paths(
        values_path_with_rows(1, 2.0, 12.0, 1_000.0),
        values_path_with_rows(2, 3.0, 23.0, 2_000.0),
        &[1],
        &[2],
        JoinType::Inner,
        vec![eq_clause.clone(), residual_clause.clone()],
    );
    let hash = paths
        .iter()
        .find(|path| {
            matches!(
                path,
                Path::HashJoin { left, .. } if left.output_vars().first() == Some(&var(1, 1))
            )
        })
        .expect("hash join path");

    let left_rows = 1_000.0;
    let right_rows = 2_000.0;
    let hash_candidate_rows = left_rows * right_rows * super::DEFAULT_EQ_SEL;
    let rows = left_rows * right_rows * super::DEFAULT_EQ_SEL * super::DEFAULT_INEQ_SEL;
    let build_cpu = right_rows * (super::CPU_OPERATOR_COST + super::CPU_TUPLE_COST);
    let probe_cpu = left_rows * super::CPU_OPERATOR_COST;
    let hash_qual_cpu =
        hash_candidate_rows * super::predicate_cost(&eq_clause.clause) * super::CPU_OPERATOR_COST;
    let residual_cpu = rows
        * (super::CPU_TUPLE_COST
            + super::predicate_cost(&residual_clause.clause) * super::CPU_OPERATOR_COST);
    let output_cpu = rows * super::CPU_TUPLE_COST;
    let startup = 2.0 + 23.0 + build_cpu;
    let total = startup + (12.0 - 2.0) + probe_cpu + hash_qual_cpu + residual_cpu + output_cpu;

    assert_float_near(hash.plan_info().startup_cost.as_f64(), startup);
    assert_float_near(hash.plan_info().total_cost.as_f64(), total);
}

#[test]
fn merge_join_cost_accounts_for_key_qual_and_output_cpu() {
    let eq_clause = restrict(eq(var(1, 1), var(2, 1)));
    let residual_clause = restrict(gt(var(1, 2), var(2, 2)));
    let paths = super::build_join_paths(
        ordered_path_with_rows(1, 2.0, 12.0, 1_000.0, 1),
        ordered_path_with_rows(2, 3.0, 23.0, 2_000.0, 1),
        &[1],
        &[2],
        JoinType::Inner,
        vec![eq_clause.clone(), residual_clause.clone()],
    );
    let merge = paths
        .iter()
        .find(|path| {
            matches!(
                path,
                Path::MergeJoin { left, .. } if left.output_vars().first() == Some(&var(1, 1))
            )
        })
        .expect("merge join path");

    let left_rows = 1_000.0;
    let right_rows = 2_000.0;
    let merge_candidate_rows = left_rows * right_rows * super::DEFAULT_EQ_SEL;
    let rows = left_rows * right_rows * super::DEFAULT_EQ_SEL * super::DEFAULT_INEQ_SEL;
    let key_compare_cpu = (left_rows + right_rows) * super::CPU_OPERATOR_COST;
    let merge_qual_cpu =
        merge_candidate_rows * super::predicate_cost(&eq_clause.clause) * super::CPU_OPERATOR_COST;
    let residual_cpu = rows
        * (super::CPU_TUPLE_COST
            + super::predicate_cost(&residual_clause.clause) * super::CPU_OPERATOR_COST);
    let output_cpu = rows * super::CPU_TUPLE_COST;
    let total = 12.0 + 23.0 + key_compare_cpu + merge_qual_cpu + residual_cpu + output_cpu;

    assert_float_near(merge.plan_info().startup_cost.as_f64(), 2.0 + 3.0);
    assert_float_near(merge.plan_info().total_cost.as_f64(), total);
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
    assert!(
        paths
            .iter()
            .any(|path| matches!(path, Path::MergeJoin { .. }))
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
fn extract_merge_join_clauses_splits_residual_predicates() {
    let clauses = super::extract_merge_join_clauses(
        &[
            restrict(eq(var(1, 1), var(2, 1))),
            restrict(gt(var(1, 2), var(2, 2))),
        ],
        &[1],
        &[2],
    )
    .expect("merge join clauses");

    assert_eq!(
        clauses.merge_clauses,
        vec![restrict(eq(var(1, 1), var(2, 1)))]
    );
    assert_eq!(clauses.outer_merge_keys, vec![var(1, 1)]);
    assert_eq!(clauses.inner_merge_keys, vec![var(2, 1)]);
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
    assert!(
        !cross_paths
            .iter()
            .any(|path| matches!(path, Path::MergeJoin { .. }))
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
    assert!(
        !non_equi_paths
            .iter()
            .any(|path| matches!(path, Path::MergeJoin { .. }))
    );
}

#[test]
fn merge_join_path_sorts_unordered_inputs() {
    let paths = super::build_join_paths(
        values_path(1, 1.0, 10.0),
        values_path(2, 2.0, 20.0),
        &[1],
        &[2],
        JoinType::Inner,
        vec![restrict(eq(var(1, 1), var(2, 1)))],
    );

    let merge = paths
        .iter()
        .find_map(|path| match path {
            Path::MergeJoin { left, right, .. }
                if matches!(left.as_ref(), Path::OrderBy { .. })
                    && matches!(right.as_ref(), Path::OrderBy { .. }) =>
            {
                Some(path)
            }
            _ => None,
        })
        .expect("merge join with sorted inputs");

    match merge {
        Path::MergeJoin {
            left,
            right,
            outer_merge_keys,
            inner_merge_keys,
            ..
        } => {
            assert_eq!(outer_merge_keys, &vec![var(1, 1)]);
            assert_eq!(inner_merge_keys, &vec![var(2, 1)]);
            assert!(matches!(left.as_ref(), Path::OrderBy { .. }));
            assert!(matches!(right.as_ref(), Path::OrderBy { .. }));
        }
        other => panic!("expected merge join, got {other:?}"),
    }
}

#[test]
fn swapped_merge_join_candidate_keeps_logical_pathtarget_order() {
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
            Path::MergeJoin { left, .. } => left.output_vars().first() == Some(&var(2, 1)),
            _ => false,
        })
        .expect("swapped merge join");

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

#[test]
fn merge_join_path_lowers_to_merge_join_plan_with_executable_keys() {
    let left = values_path(1, 1.0, 10.0);
    let right = values_path(2, 2.0, 20.0);
    let plan = Path::MergeJoin {
        plan_info: PlanEstimate::new(5.0, 15.0, 10.0, 4),
        pathtarget: join_pathtarget(&left, &right),
        output_columns: join_output_columns(&left, &right),
        left: Box::new(left),
        right: Box::new(right),
        kind: JoinType::Inner,
        merge_clauses: vec![restrict(eq(var(1, 1), var(2, 1)))],
        outer_merge_keys: vec![var(1, 1)],
        inner_merge_keys: vec![var(2, 1)],
        restrict_clauses: vec![
            restrict(eq(var(1, 1), var(2, 1))),
            restrict(gt(var(1, 2), var(2, 2))),
        ],
    }
    .into_plan();

    match plan {
        Plan::MergeJoin {
            kind,
            merge_clauses,
            outer_merge_keys,
            inner_merge_keys,
            join_qual,
            qual,
            ..
        } => {
            assert_eq!(kind, JoinType::Inner);
            assert_eq!(outer_merge_keys.len(), 1);
            assert!(is_special_user_var(&outer_merge_keys[0], OUTER_VAR, 0));
            assert_eq!(inner_merge_keys.len(), 1);
            assert!(is_special_user_var(&inner_merge_keys[0], OUTER_VAR, 0));
            assert_eq!(merge_clauses.len(), 1);
            match &merge_clauses[0] {
                Expr::Op(op) => {
                    assert_eq!(op.op, OpExprKind::Eq);
                    assert_eq!(op.args.len(), 2);
                    assert!(is_special_user_var(&op.args[0], OUTER_VAR, 0));
                    assert!(is_special_user_var(&op.args[1], INNER_VAR, 0));
                }
                other => panic!("expected merge clause op, got {other:?}"),
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
        }
        other => panic!("expected merge join plan, got {other:?}"),
    }
}
