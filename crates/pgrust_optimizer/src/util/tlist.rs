use pgrust_analyze::CatalogLookup;
use pgrust_nodes::pathnodes::{Path, PathKey, PathTarget, PlannerInfo, RelOptInfo};
use pgrust_nodes::plannodes::PlanEstimate;
use pgrust_nodes::primnodes::{
    AggAccum, Expr, OrderByEntry, QueryColumn, RelationDesc, TargetEntry, Var, user_attrno,
};

use super::super::expand_join_rte_vars;
use super::super::optimize_path_with_config;
use super::super::pathnodes::{
    PathMethods, expr_sql_type, layout_candidate_for_expr, lower_agg_output_expr,
    lower_expr_to_path_output,
};
use super::indexed_pathtarget::IndexedPathTarget;

pub(super) fn pathkeys_to_order_items(pathkeys: &[PathKey]) -> Vec<OrderByEntry> {
    pathkeys
        .iter()
        .map(|key| OrderByEntry {
            expr: key.expr.clone(),
            ressortgroupref: key.ressortgroupref,
            descending: key.descending,
            nulls_first: key.nulls_first,
            collation_oid: key.collation_oid,
        })
        .collect()
}

pub(super) fn build_aggregate_output_columns(
    group_by: &[Expr],
    passthrough_exprs: &[Expr],
    accumulators: &[AggAccum],
) -> Vec<QueryColumn> {
    let mut output_columns =
        Vec::with_capacity(group_by.len() + passthrough_exprs.len() + accumulators.len());
    for (index, expr) in group_by.iter().enumerate() {
        output_columns.push(QueryColumn {
            name: format!("group{}", index + 1),
            sql_type: expr_sql_type(expr),
            wire_type_oid: None,
        });
    }
    for (index, expr) in passthrough_exprs.iter().enumerate() {
        output_columns.push(QueryColumn {
            name: format!("pass{}", index + 1),
            sql_type: expr_sql_type(expr),
            wire_type_oid: None,
        });
    }
    for (index, accum) in accumulators.iter().enumerate() {
        output_columns.push(QueryColumn {
            name: format!("agg{}", index + 1),
            sql_type: accum.sql_type,
            wire_type_oid: None,
        });
    }
    output_columns
}

pub fn project_to_slot_layout_internal(
    root: Option<&PlannerInfo>,
    slot_id: usize,
    desc: &RelationDesc,
    input: Path,
    target: PathTarget,
    catalog: &dyn CatalogLookup,
) -> Path {
    let input_target = input.semantic_output_target();
    let layout = input_target.exprs.clone();
    let passthrough_input_resno = |expr: &Expr| {
        input_target
            .exprs
            .iter()
            .position(|candidate| candidate == expr)
            .map(|index| index + 1)
    };
    let rewritten_targets = desc
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let expr = target
                .exprs
                .get(index)
                .cloned()
                .or_else(|| layout.get(index).cloned())
                .unwrap_or_else(|| {
                    panic!(
                        "projection target layout is shorter than descriptor width while projecting slot {slot_id}"
                    )
                });
            let ressortgroupref = target
                .sortgrouprefs
                .get(index)
                .copied()
                .or_else(|| input_target.sortgrouprefs.get(index).copied())
                .unwrap_or(0);
            let input_resno = passthrough_input_resno(&expr);
            TargetEntry::new(column.name.clone(), expr, column.sql_type, index + 1)
                .with_sort_group_ref(ressortgroupref)
                .with_input_resno_opt(input_resno)
        })
        .collect::<Vec<_>>();
    let rewritten_targets = annotate_targets_for_input(root, &input, &rewritten_targets);

    optimize_path_with_config(
        Path::Projection {
            plan_info: PlanEstimate::default(),
            pathtarget: PathTarget::from_target_list(&rewritten_targets),
            slot_id,
            input: Box::new(input),
            targets: rewritten_targets,
        },
        catalog,
        root.map(|root| root.config).unwrap_or_default(),
    )
}

pub(super) fn project_to_slot_layout(
    slot_id: usize,
    desc: &RelationDesc,
    input: Path,
    target: PathTarget,
    catalog: &dyn CatalogLookup,
) -> Path {
    project_to_slot_layout_internal(None, slot_id, desc, input, target, catalog)
}

pub fn normalize_rte_path(
    rtindex: usize,
    desc: &RelationDesc,
    input: Path,
    catalog: &dyn CatalogLookup,
) -> Path {
    let input_target = input.semantic_output_target();
    let names_match = input
        .columns()
        .iter()
        .map(|column| (&column.name, column.sql_type))
        .eq(desc
            .columns
            .iter()
            .map(|column| (&column.name, column.sql_type)));
    let desired_layout = PathTarget::new(
        desc.columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                Expr::Var(Var {
                    varno: rtindex,
                    varattno: user_attrno(index),
                    varlevelsup: 0,
                    vartype: column.sql_type,
                    collation_oid: None,
                })
            })
            .collect(),
    );
    let target = PathTarget::with_sortgrouprefs(
        desired_layout.exprs.clone(),
        input_target.sortgrouprefs.clone(),
    );
    if names_match
        && input.semantic_output_vars() == desired_layout.exprs
        && input.output_vars() == desired_layout.exprs
    {
        input
    } else {
        project_to_slot_layout(rtindex, desc, input, target, catalog)
    }
}

pub(super) fn annotate_targets_for_input(
    _root: Option<&PlannerInfo>,
    path: &Path,
    targets: &[TargetEntry],
) -> Vec<TargetEntry> {
    let input_target = path.semantic_output_target();
    let indexed_input = IndexedPathTarget::new(&input_target);
    let projects_project_set_output = matches!(path, Path::ProjectSet { .. })
        || matches!(path, Path::OrderBy { input, .. } if matches!(input.as_ref(), Path::ProjectSet { .. }))
        || matches!(path, Path::IncrementalSort { input, .. } if matches!(input.as_ref(), Path::ProjectSet { .. }));
    targets
        .iter()
        .cloned()
        .map(|target| {
            let input_resno = if projects_project_set_output
                && matches!(target.expr, Expr::Const(pgrust_nodes::datum::Value::Null))
                && target.resno >= 1
                && target.resno <= input_target.exprs.len()
            {
                Some(target.resno)
            } else {
                indexed_input
                    .match_index(&target.expr, target.ressortgroupref)
                    .map(|index| index + 1)
            };
            TargetEntry {
                input_resno,
                ..target
            }
        })
        .collect()
}

pub fn lower_pathkeys_for_path(
    root: &PlannerInfo,
    path: &Path,
    pathkeys: &[PathKey],
) -> Vec<PathKey> {
    match aggregate_layout(path) {
        Some((group_by, passthrough_exprs)) => pathkeys
            .iter()
            .cloned()
            .map(|key| PathKey {
                expr: lower_agg_output_expr(
                    expand_join_rte_vars(root, key.expr),
                    group_by,
                    passthrough_exprs,
                    &path.output_vars(),
                ),
                ressortgroupref: key.ressortgroupref,
                descending: key.descending,
                nulls_first: key.nulls_first,
                collation_oid: key.collation_oid,
            })
            .collect(),
        None => pathkeys
            .iter()
            .cloned()
            .map(|key| {
                let layout = path.semantic_output_vars();
                let output_target = path.output_target();
                let indexed_output = IndexedPathTarget::new(&output_target);
                let expr = lower_expr_to_path_output(
                    Some(root),
                    path,
                    key.expr.clone(),
                    key.ressortgroupref,
                )
                .or_else(|| indexed_output.matched_expr(&key.expr, key.ressortgroupref))
                .or_else(|| layout_candidate_for_expr(Some(root), &key.expr, &layout))
                .unwrap_or_else(|| key.expr.clone());
                PathKey {
                    expr,
                    ressortgroupref: key.ressortgroupref,
                    descending: key.descending,
                    nulls_first: key.nulls_first,
                    collation_oid: key.collation_oid,
                }
            })
            .collect(),
    }
}

pub fn lower_pathkeys_for_rel(
    root: &PlannerInfo,
    rel: &RelOptInfo,
    pathkeys: &[PathKey],
) -> Vec<PathKey> {
    rel.pathlist
        .first()
        .map(|path| lower_pathkeys_for_path(root, path, pathkeys))
        .unwrap_or_else(|| pathkeys.to_vec())
}

pub(super) fn pathkeys_are_fully_identified(pathkeys: &[PathKey]) -> bool {
    pathkeys.iter().all(|key| key.ressortgroupref != 0)
}

pub fn path_exposes_required_pathkey_identity(path: &Path, pathkeys: &[PathKey]) -> bool {
    let output_target = path.output_target();
    pathkeys.iter().all(|key| {
        key.ressortgroupref != 0 && output_target.sortgrouprefs.contains(&key.ressortgroupref)
    })
}

pub fn rel_exposes_required_pathkey_identity(rel: &RelOptInfo, pathkeys: &[PathKey]) -> bool {
    rel.pathlist
        .iter()
        .any(|path| path_exposes_required_pathkey_identity(path, pathkeys))
}

pub fn required_query_pathkeys_for_path(root: &PlannerInfo, path: &Path) -> Vec<PathKey> {
    if pathkeys_are_fully_identified(&root.query_pathkeys)
        && path_exposes_required_pathkey_identity(path, &root.query_pathkeys)
    {
        root.query_pathkeys.clone()
    } else {
        lower_pathkeys_for_path(root, path, &root.query_pathkeys)
    }
}

pub fn required_query_pathkeys_for_rel(root: &PlannerInfo, rel: &RelOptInfo) -> Vec<PathKey> {
    if pathkeys_are_fully_identified(&root.query_pathkeys)
        && rel_exposes_required_pathkey_identity(rel, &root.query_pathkeys)
    {
        root.query_pathkeys.clone()
    } else {
        lower_pathkeys_for_rel(root, rel, &root.query_pathkeys)
    }
}

pub(super) fn projection_is_identity(path: &Path, targets: &[TargetEntry]) -> bool {
    let input_columns = path.columns();
    let layout = path.semantic_output_vars();
    targets.len() == input_columns.len()
        && targets.iter().enumerate().all(|(index, target)| {
            (target.expr == layout[index] || target.input_resno == Some(index + 1))
                && target.name == input_columns[index].name
        })
}

fn aggregate_layout(path: &Path) -> Option<(&[Expr], &[Expr])> {
    match path {
        Path::Aggregate {
            group_by,
            passthrough_exprs,
            ..
        } => Some((group_by, passthrough_exprs)),
        Path::Filter { input, .. }
        | Path::OrderBy { input, .. }
        | Path::IncrementalSort { input, .. }
        | Path::Limit { input, .. } => aggregate_layout(input),
        _ => None,
    }
}

pub(super) fn aggregate_group_by(path: &Path) -> Option<&[Expr]> {
    aggregate_layout(path).map(|(group_by, _)| group_by)
}
