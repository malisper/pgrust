use crate::backend::catalog::catalog::column_desc;
use crate::backend::parser::CatalogLookup;
use crate::include::nodes::parsenodes::RangeTblEntryKind;
use crate::include::nodes::pathnodes::{
    AppendRelInfo, PartitionInfo, PartitionMember, Path, PathTarget, PlannerInfo, RelOptInfo,
    RelOptKind, RestrictInfo,
};
use crate::include::nodes::plannodes::PlanEstimate;
use crate::include::nodes::primnodes::{
    Expr, JoinType, OpExprKind, QueryColumn, RelationDesc, user_attrno,
};

use super::bestpath;
use super::inherit::{append_translation, translate_append_rel_expr};
use super::joininfo;
use super::path::build_join_paths_with_root;
use super::pathnodes::next_synthetic_slot_id;
use super::util::{project_to_slot_layout, strip_binary_coercible_casts};
use super::{flatten_join_alias_vars, optimize_path_with_config, relids_union};

pub(super) fn generate_partitionwise_join_path(
    root: &mut PlannerInfo,
    left_rel: &RelOptInfo,
    right_rel: &RelOptInfo,
    kind: JoinType,
    join_restrict_clauses: &[RestrictInfo],
    reltarget: &PathTarget,
    output_columns: &[QueryColumn],
    catalog: &dyn CatalogLookup,
) -> Option<(Path, Option<PartitionInfo>)> {
    let left_info = left_rel.partition_info.as_ref()?;
    let right_info = right_rel.partition_info.as_ref()?;
    if query_targets_whole_row_rel(root, &left_rel.relids)
        || query_targets_whole_row_rel(root, &right_rel.relids)
        || query_accumulators_whole_row_rel(root, &left_rel.relids)
        || query_accumulators_whole_row_rel(root, &right_rel.relids)
    {
        return None;
    }
    if !partitionwise_join_is_legal(root, left_rel, right_rel, kind, join_restrict_clauses) {
        return None;
    }

    let mut children = Vec::new();
    let mut members = Vec::new();
    for (left_member, right_member) in left_info.members.iter().zip(right_info.members.iter()) {
        if left_member.bound != right_member.bound {
            return None;
        }
        let child_join = ensure_child_join_rel(
            root,
            left_member,
            right_member,
            kind,
            join_restrict_clauses,
            catalog,
        )?;
        let child_path = child_join.cheapest_total_path()?.clone();
        let child_output_columns = child_path.columns();
        children.push(project_to_slot_layout(
            next_synthetic_slot_id(),
            &relation_desc_for_output_columns(&child_output_columns),
            child_path,
            child_join.reltarget.clone(),
            catalog,
        ));
        members.push(PartitionMember {
            relids: child_join.relids.clone(),
            bound: left_member.bound.clone(),
        });
    }

    if children.is_empty() {
        return None;
    }

    let desc = relation_desc_for_output_columns(output_columns);
    let append = optimize_path_with_config(
        Path::Append {
            plan_info: PlanEstimate::default(),
            pathtarget: reltarget.clone(),
            pathkeys: Vec::new(),
            relids: relids_union(&left_rel.relids, &right_rel.relids),
            source_id: next_synthetic_slot_id(),
            desc,
            child_roots: Vec::new(),
            partition_prune: None,
            children,
        },
        catalog,
        root.config,
    );
    let partition_info = join_partition_info(left_info, right_info, kind, members);
    Some((append, partition_info))
}

fn partitionwise_join_is_legal(
    root: &PlannerInfo,
    left_rel: &RelOptInfo,
    right_rel: &RelOptInfo,
    kind: JoinType,
    join_restrict_clauses: &[RestrictInfo],
) -> bool {
    if !root.config.enable_partitionwise_join {
        return false;
    }
    if !left_rel.consider_partitionwise_join || !right_rel.consider_partitionwise_join {
        return false;
    }
    let (Some(left), Some(right)) = (&left_rel.partition_info, &right_rel.partition_info) else {
        return false;
    };
    if left.strategy != right.strategy
        || left.partattrs.len() != right.partattrs.len()
        || left.partclass != right.partclass
        || left.partcollation != right.partcollation
        || left.members.len() != right.members.len()
    {
        return false;
    }
    left.key_exprs
        .iter()
        .zip(right.key_exprs.iter())
        .all(|(left_key, right_key)| {
            have_partition_key_equality(root, kind, join_restrict_clauses, left_key, right_key)
        })
}

fn have_partition_key_equality(
    root: &PlannerInfo,
    kind: JoinType,
    clauses: &[RestrictInfo],
    left_key: &Expr,
    right_key: &Expr,
) -> bool {
    let left_key = normalized_expr(root, left_key.clone());
    let right_key = normalized_expr(root, right_key.clone());
    clauses.iter().any(|restrict| {
        if !matches!(kind, JoinType::Inner | JoinType::Cross) && restrict.is_pushed_down {
            return false;
        }
        let Expr::Op(op) = &restrict.clause else {
            return false;
        };
        if op.op != OpExprKind::Eq || op.args.len() != 2 {
            return false;
        }
        let left_arg = normalized_expr(root, op.args[0].clone());
        let right_arg = normalized_expr(root, op.args[1].clone());
        (left_arg == left_key && right_arg == right_key)
            || (left_arg == right_key && right_arg == left_key)
    })
}

fn normalized_expr(root: &PlannerInfo, expr: Expr) -> Expr {
    strip_binary_coercible_casts(&flatten_join_alias_vars(root, expr))
}

fn query_targets_whole_row_rel(root: &PlannerInfo, relids: &[usize]) -> bool {
    root.parse.target_list.iter().any(|target| {
        let expr = super::joininfo::flatten_join_alias_vars_query(&root.parse, target.expr.clone());
        expr_is_whole_row_rel(root, &expr, relids)
    })
}

fn query_accumulators_whole_row_rel(root: &PlannerInfo, relids: &[usize]) -> bool {
    root.parse.accumulators.iter().any(|accum| {
        accum
            .args
            .iter()
            .chain(accum.direct_args.iter())
            .any(|expr| {
                let expr =
                    super::joininfo::flatten_join_alias_vars_query(&root.parse, expr.clone());
                expr_is_whole_row_rel(root, &expr, relids)
            })
    })
}

fn expr_is_whole_row_rel(root: &PlannerInfo, expr: &Expr, relids: &[usize]) -> bool {
    match expr {
        Expr::Row {
            descriptor, fields, ..
        } => {
            row_type_targets_rel(root, descriptor.typrelid, relids)
                || relids.iter().any(|relid| {
                    let Some(rte) = root.parse.rtable.get(relid.saturating_sub(1)) else {
                        return false;
                    };
                    fields.len() == rte.desc.columns.len()
                        && fields.iter().enumerate().all(|(index, (_, expr))| {
                            matches!(
                                expr,
                                Expr::Var(var)
                                    if var.varno == *relid
                                        && var.varlevelsup == 0
                                        && var.varattno == user_attrno(index)
                            )
                        })
                })
        }
        Expr::Case(case_expr) => {
            row_type_targets_rel(root, case_expr.casetype.typrelid, relids)
                || case_expr
                    .arg
                    .as_deref()
                    .is_some_and(|arg| expr_is_whole_row_rel(root, arg, relids))
                || case_expr.args.iter().any(|arm| {
                    expr_is_whole_row_rel(root, &arm.expr, relids)
                        || expr_is_whole_row_rel(root, &arm.result, relids)
                })
                || expr_is_whole_row_rel(root, &case_expr.defresult, relids)
        }
        Expr::Op(op) => op
            .args
            .iter()
            .any(|arg| expr_is_whole_row_rel(root, arg, relids)),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .any(|arg| expr_is_whole_row_rel(root, arg, relids)),
        Expr::Func(func) => func
            .args
            .iter()
            .any(|arg| expr_is_whole_row_rel(root, arg, relids)),
        Expr::Cast(inner, _)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::Collate { expr: inner, .. } => expr_is_whole_row_rel(root, inner, relids),
        Expr::Coalesce(left, right)
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right) => {
            expr_is_whole_row_rel(root, left, relids) || expr_is_whole_row_rel(root, right, relids)
        }
        _ => false,
    }
}

fn row_type_targets_rel(root: &PlannerInfo, typrelid: u32, relids: &[usize]) -> bool {
    typrelid != 0
        && relids.iter().any(|relid| {
            root.parse
                .rtable
                .get(relid.saturating_sub(1))
                .is_some_and(|rte| match &rte.kind {
                    RangeTblEntryKind::Relation { relation_oid, .. } => *relation_oid == typrelid,
                    _ => false,
                })
        })
}

fn ensure_child_join_rel(
    root: &mut PlannerInfo,
    left_member: &PartitionMember,
    right_member: &PartitionMember,
    kind: JoinType,
    parent_clauses: &[RestrictInfo],
    catalog: &dyn CatalogLookup,
) -> Option<RelOptInfo> {
    let relids = relids_union(&left_member.relids, &right_member.relids);
    if let Some(existing) = find_join_rel(root, &relids)
        && !existing.pathlist.is_empty()
    {
        return Some(existing);
    }

    let left_rel = rel_for_member(root, &left_member.relids)?;
    let right_rel = rel_for_member(root, &right_member.relids)?;
    let reltarget = child_join_reltarget(&left_rel, &right_rel, kind);
    let output_columns = child_join_output_columns(&left_rel, &right_rel, kind);
    let child_clauses = translate_restrict_clauses(root, parent_clauses, left_member, right_member);
    let mut candidate_paths = collect_child_join_paths(
        root,
        catalog,
        &left_rel,
        &right_rel,
        kind,
        &child_clauses,
        &reltarget,
        &output_columns,
    );
    let mut partition_info = None;
    if let Some((append, info)) = generate_partitionwise_join_path(
        root,
        &left_rel,
        &right_rel,
        kind,
        &child_clauses,
        &reltarget,
        &output_columns,
        catalog,
    ) {
        candidate_paths.push(append);
        partition_info = info;
    }
    if candidate_paths.is_empty() {
        return None;
    }

    let join_index = match root
        .join_rel_list
        .iter()
        .position(|join_rel| join_rel.relids == relids)
    {
        Some(index) => index,
        None => {
            root.join_rel_list.push(RelOptInfo::new(
                relids.clone(),
                RelOptKind::JoinRel,
                reltarget,
            ));
            root.join_rel_list.len() - 1
        }
    };
    let join_rel = root.join_rel_list.get_mut(join_index)?;
    join_rel.joininfo.extend(child_clauses);
    join_rel.pathlist.extend(candidate_paths);
    if let Some(partition_info) = partition_info {
        join_rel.consider_partitionwise_join = true;
        join_rel.partition_info = Some(partition_info);
    }
    bestpath::set_cheapest(join_rel);
    Some(join_rel.clone())
}

fn find_join_rel(root: &PlannerInfo, relids: &[usize]) -> Option<RelOptInfo> {
    root.join_rel_list
        .iter()
        .find(|join_rel| join_rel.relids == relids)
        .cloned()
}

fn rel_for_member(root: &PlannerInfo, relids: &[usize]) -> Option<RelOptInfo> {
    if relids.len() == 1 {
        return root
            .simple_rel_array
            .get(relids[0])
            .and_then(Option::as_ref)
            .cloned();
    }
    find_join_rel(root, relids)
}

fn child_join_reltarget(
    left_rel: &RelOptInfo,
    right_rel: &RelOptInfo,
    kind: JoinType,
) -> PathTarget {
    let mut exprs = left_rel.reltarget.exprs.clone();
    let mut sortgrouprefs = left_rel.reltarget.sortgrouprefs.clone();
    if !matches!(kind, JoinType::Semi | JoinType::Anti) {
        exprs.extend(right_rel.reltarget.exprs.clone());
        sortgrouprefs.extend(right_rel.reltarget.sortgrouprefs.clone());
    }
    PathTarget::with_sortgrouprefs(exprs, sortgrouprefs)
}

fn child_join_output_columns(
    left_rel: &RelOptInfo,
    right_rel: &RelOptInfo,
    kind: JoinType,
) -> Vec<QueryColumn> {
    let mut output_columns = left_rel
        .cheapest_total_path()
        .map(Path::columns)
        .unwrap_or_default();
    if !matches!(kind, JoinType::Semi | JoinType::Anti) {
        output_columns.extend(
            right_rel
                .cheapest_total_path()
                .map(Path::columns)
                .unwrap_or_default(),
        );
    }
    output_columns
}

fn collect_child_join_paths(
    root: &PlannerInfo,
    catalog: &dyn CatalogLookup,
    left_rel: &RelOptInfo,
    right_rel: &RelOptInfo,
    kind: JoinType,
    join_restrict_clauses: &[RestrictInfo],
    reltarget: &PathTarget,
    output_columns: &[QueryColumn],
) -> Vec<Path> {
    let mut paths = Vec::new();
    for left_path in &left_rel.pathlist {
        for right_path in &right_rel.pathlist {
            paths.extend(
                build_join_paths_with_root(
                    root,
                    catalog,
                    left_path.clone(),
                    right_path.clone(),
                    &left_rel.relids,
                    &right_rel.relids,
                    kind,
                    join_restrict_clauses.to_vec(),
                    reltarget.clone(),
                    output_columns.to_vec(),
                )
                .into_iter()
                .map(prefer_hash_join_with_smaller_inner),
            );
        }
    }
    paths
}

fn prefer_hash_join_with_smaller_inner(path: Path) -> Path {
    let Path::HashJoin {
        plan_info,
        pathtarget,
        left,
        right,
        kind,
        hash_clauses,
        outer_hash_keys,
        inner_hash_keys,
        restrict_clauses,
        output_columns,
    } = path
    else {
        return path;
    };
    let left_rows = left.plan_info().plan_rows.as_f64();
    let right_rows = right.plan_info().plan_rows.as_f64();
    let total_cost = if right_rows <= left_rows {
        plan_info.total_cost.as_f64() * 0.5
    } else {
        plan_info.total_cost.as_f64()
    };
    Path::HashJoin {
        plan_info: PlanEstimate::new(
            plan_info.startup_cost.as_f64(),
            total_cost,
            plan_info.plan_rows.as_f64(),
            plan_info.plan_width,
        ),
        pathtarget,
        left,
        right,
        kind,
        hash_clauses,
        outer_hash_keys,
        inner_hash_keys,
        restrict_clauses,
        output_columns,
    }
}

fn translate_restrict_clauses(
    root: &PlannerInfo,
    clauses: &[RestrictInfo],
    left_member: &PartitionMember,
    right_member: &PartitionMember,
) -> Vec<RestrictInfo> {
    let translations = member_translations(root, left_member)
        .into_iter()
        .chain(member_translations(root, right_member))
        .collect::<Vec<_>>();
    clauses
        .iter()
        .map(|restrict| {
            let clause = translations
                .iter()
                .fold(restrict.clause.clone(), |expr, info| {
                    translate_append_rel_expr(expr, info)
                });
            joininfo::translated_restrict_info(clause, restrict)
        })
        .collect()
}

fn member_translations(root: &PlannerInfo, member: &PartitionMember) -> Vec<AppendRelInfo> {
    member
        .relids
        .iter()
        .filter_map(|relid| append_translation(root, *relid).cloned())
        .collect()
}

fn join_partition_info(
    left: &PartitionInfo,
    right: &PartitionInfo,
    kind: JoinType,
    members: Vec<PartitionMember>,
) -> Option<PartitionInfo> {
    if matches!(kind, JoinType::Full) {
        return None;
    }
    let key_exprs = if matches!(kind, JoinType::Right) {
        right.key_exprs.clone()
    } else {
        left.key_exprs.clone()
    };
    Some(PartitionInfo {
        strategy: left.strategy,
        partattrs: left.partattrs.clone(),
        partclass: left.partclass.clone(),
        partcollation: left.partcollation.clone(),
        key_exprs,
        members,
    })
}

fn relation_desc_for_output_columns(output_columns: &[QueryColumn]) -> RelationDesc {
    RelationDesc {
        columns: output_columns
            .iter()
            .map(|column| column_desc(column.name.clone(), column.sql_type, true))
            .collect(),
    }
}
