use std::cmp::Ordering;

use crate::RelFileLocator;
use crate::backend::executor::Value;
use crate::backend::parser::CatalogLookup;
use crate::include::catalog::BTREE_AM_OID;
use crate::include::nodes::parsenodes::{JoinTreeNode, RangeTblEntryKind};
use crate::include::nodes::pathnodes::{
    Path, PathKey, PathTarget, PlannerInfo, RelOptInfo, RelOptKind, RestrictInfo, SpecialJoinInfo,
};
use crate::include::nodes::plannodes::PlanEstimate;
use crate::include::nodes::primnodes::{
    Expr, JoinType, OrderByEntry, QueryColumn, RelationDesc, TargetEntry, ToastRelationRef,
};

use super::super::bestpath;
use super::super::has_grouping;
use super::super::inherit::{
    append_child_rtindexes, append_translation, expand_inherited_rtentries,
};
use super::super::joininfo;
use super::super::optimize_path;
use super::super::pathnodes::{expr_sql_type, next_synthetic_slot_id, rewrite_expr_against_layout};
use super::super::plan::{grouping_planner, make_pathtarget_projection_rel};
use super::super::util::{
    build_aggregate_output_columns, layout_candidate_for_expr, lower_pathkeys_for_rel,
    normalize_rte_path, pathkeys_to_order_items, project_to_slot_layout,
    project_to_slot_layout_internal, rewrite_semantic_expr_for_path,
    rewrite_semantic_expr_for_path_or_expand_join_vars,
};
use super::super::{
    JoinBuildSpec, and_exprs, exact_join_rtindex, expand_join_rte_vars, expr_relids,
    flatten_and_conjuncts, has_outer_joins, is_pushable_base_clause, relids_disjoint,
    relids_overlap, relids_subset, relids_union, reverse_join_type,
};
use super::{
    build_index_path_spec, build_join_paths_with_root, estimate_index_candidate,
    estimate_seqscan_candidate, relation_stats, restore_join_output_order,
};

fn collect_inner_join_clauses(root: &PlannerInfo) -> Vec<RestrictInfo> {
    fn walk(root: &PlannerInfo, node: &JoinTreeNode, clauses: &mut Vec<RestrictInfo>) {
        if let JoinTreeNode::JoinExpr {
            left,
            right,
            kind,
            quals,
            ..
        } = node
        {
            walk(root, left, clauses);
            walk(root, right, clauses);
            if matches!(kind, JoinType::Inner | JoinType::Cross) {
                clauses.push(joininfo::make_restrict_info(expand_join_rte_vars(
                    root,
                    quals.clone(),
                )));
            }
        }
    }

    let mut clauses = Vec::new();
    if let Some(jointree) = root.parse.jointree.as_ref() {
        walk(root, jointree, &mut clauses);
    }
    if !has_outer_joins(root) {
        if let Some(where_qual) = root.parse.where_qual.as_ref() {
            clauses.extend(
                flatten_and_conjuncts(where_qual)
                    .into_iter()
                    .map(|clause| expand_join_rte_vars(root, clause))
                    .filter(|clause| expr_relids(clause).len() > 1)
                    .map(joininfo::make_restrict_info),
            );
        }
    }
    clauses
}

pub(super) fn residual_where_qual(root: &PlannerInfo) -> Option<Expr> {
    let Some(where_qual) = root.parse.where_qual.as_ref() else {
        return None;
    };
    let clauses = flatten_and_conjuncts(where_qual)
        .into_iter()
        .map(|clause| expand_join_rte_vars(root, clause))
        .filter(|clause| {
            let relids = expr_relids(clause);
            has_outer_joins(root) || !is_pushable_base_clause(root, &relids)
        })
        .collect();
    and_exprs(clauses)
}

fn assign_base_restrictinfo(root: &mut PlannerInfo) {
    for rel in root.simple_rel_array.iter_mut().flatten() {
        rel.baserestrictinfo.clear();
        rel.joininfo.clear();
    }
    if has_outer_joins(root) {
        return;
    }
    let Some(where_qual) = root.parse.where_qual.as_ref() else {
        return;
    };
    for clause in flatten_and_conjuncts(where_qual) {
        let restrict = joininfo::make_restrict_info(expand_join_rte_vars(root, clause));
        if !is_pushable_base_clause(root, &restrict.required_relids) {
            continue;
        }
        let relid = restrict.required_relids[0];
        if let Some(rel) = root
            .simple_rel_array
            .get_mut(relid)
            .and_then(Option::as_mut)
        {
            rel.baserestrictinfo.push(restrict);
        }
    }
}

fn base_filter_expr(rel: &RelOptInfo) -> Option<Expr> {
    super::super::and_exprs(
        rel.baserestrictinfo
            .iter()
            .map(|restrict| restrict.clause.clone())
            .collect(),
    )
}

fn query_order_items_for_base_rel(root: &PlannerInfo, rtindex: usize) -> Option<Vec<OrderByEntry>> {
    if root.query_pathkeys.is_empty() {
        return None;
    }
    let expanded_pathkeys = root
        .query_pathkeys
        .iter()
        .cloned()
        .map(|key| PathKey {
            expr: expand_join_rte_vars(root, key.expr),
            descending: key.descending,
            nulls_first: key.nulls_first,
        })
        .collect::<Vec<_>>();
    if expanded_pathkeys
        .iter()
        .all(|key| expr_relids(&key.expr).iter().all(|relid| *relid == rtindex))
    {
        Some(pathkeys_to_order_items(&expanded_pathkeys))
    } else {
        None
    }
}

fn collect_relation_access_paths(
    rtindex: usize,
    heap_rel: RelFileLocator,
    relation_oid: u32,
    toast: Option<ToastRelationRef>,
    desc: RelationDesc,
    filter: Option<Expr>,
    query_order_items: Option<Vec<OrderByEntry>>,
    catalog: &dyn CatalogLookup,
) -> Vec<Path> {
    let stats = relation_stats(catalog, relation_oid, &desc);
    let mut paths = vec![
        estimate_seqscan_candidate(
            rtindex,
            heap_rel,
            relation_oid,
            toast,
            desc.clone(),
            &stats,
            filter.clone(),
            None,
        )
        .plan,
    ];
    if let Some(order_items) = query_order_items.clone() {
        paths.push(
            estimate_seqscan_candidate(
                rtindex,
                heap_rel,
                relation_oid,
                toast,
                desc.clone(),
                &stats,
                filter.clone(),
                Some(order_items),
            )
            .plan,
        );
    }
    for index in catalog
        .index_relations_for_heap(relation_oid)
        .iter()
        .filter(|index| {
            index.index_meta.indisvalid
                && index.index_meta.indisready
                && !index.index_meta.indkey.is_empty()
                && index.index_meta.am_oid == BTREE_AM_OID
        })
    {
        let Some(spec) = build_index_path_spec(filter.as_ref(), None, index) else {
            continue;
        };
        paths.push(
            estimate_index_candidate(
                rtindex,
                heap_rel,
                toast,
                desc.clone(),
                &stats,
                spec,
                None,
                catalog,
            )
            .plan,
        );
        if let Some(order_items) = query_order_items.as_ref()
            && let Some(spec) = build_index_path_spec(filter.as_ref(), Some(order_items), index)
        {
            paths.push(
                estimate_index_candidate(
                    rtindex,
                    heap_rel,
                    toast,
                    desc.clone(),
                    &stats,
                    spec,
                    Some(order_items.clone()),
                    catalog,
                )
                .plan,
            );
        }
    }
    paths
}

fn cheapest_relation_access_path(
    rtindex: usize,
    heap_rel: RelFileLocator,
    relation_oid: u32,
    toast: Option<ToastRelationRef>,
    desc: RelationDesc,
    filter: Option<Expr>,
    catalog: &dyn CatalogLookup,
) -> Path {
    collect_relation_access_paths(
        rtindex,
        heap_rel,
        relation_oid,
        toast,
        desc,
        filter,
        None,
        catalog,
    )
    .into_iter()
    .min_by(|left, right| {
        left.plan_info()
            .total_cost
            .as_f64()
            .partial_cmp(&right.plan_info().total_cost.as_f64())
            .unwrap_or(Ordering::Equal)
    })
    .unwrap_or(Path::Result {
        plan_info: PlanEstimate::default(),
    })
}

fn set_base_rel_pathlist(root: &mut PlannerInfo, rtindex: usize, catalog: &dyn CatalogLookup) {
    let Some(rte) = root.parse.rtable.get(rtindex.saturating_sub(1)).cloned() else {
        return;
    };
    if root
        .simple_rel_array
        .get(rtindex)
        .and_then(Option::as_ref)
        .is_some_and(|rel| !rel.pathlist.is_empty())
    {
        return;
    }
    let child_rtindexes = append_child_rtindexes(root, rtindex);
    if !child_rtindexes.is_empty()
        && let RangeTblEntryKind::Relation {
            rel: heap_rel,
            relation_oid,
            relkind: _,
            toast,
        } = rte.kind.clone()
    {
        let filter = root
            .simple_rel_array
            .get(rtindex)
            .and_then(Option::as_ref)
            .and_then(base_filter_expr);
        let mut children = vec![normalize_rte_path(
            rtindex,
            &rte.desc,
            cheapest_relation_access_path(
                rtindex,
                heap_rel,
                relation_oid,
                toast,
                rte.desc.clone(),
                filter,
                catalog,
            ),
            catalog,
        )];
        for child_rtindex in child_rtindexes {
            set_base_rel_pathlist(root, child_rtindex, catalog);
            let Some(child_path) = root
                .simple_rel_array
                .get(child_rtindex)
                .and_then(Option::as_ref)
                .and_then(|rel| rel.cheapest_total_path())
                .cloned()
            else {
                continue;
            };
            let translated_vars = append_translation(root, child_rtindex)
                .map(|info| info.translated_vars.clone())
                .unwrap_or_default();
            children.push(project_to_slot_layout(
                rtindex,
                &rte.desc,
                child_path,
                translated_vars,
                catalog,
            ));
        }
        let append = optimize_path(
            Path::Append {
                plan_info: PlanEstimate::default(),
                source_id: rtindex,
                desc: rte.desc.clone(),
                children,
            },
            catalog,
        );
        let Some(rel) = root
            .simple_rel_array
            .get_mut(rtindex)
            .and_then(Option::as_mut)
        else {
            return;
        };
        rel.add_path(append);
        bestpath::set_cheapest(rel);
        return;
    }
    let query_order_items = query_order_items_for_base_rel(root, rtindex);
    let Some(rel) = root
        .simple_rel_array
        .get_mut(rtindex)
        .and_then(Option::as_mut)
    else {
        return;
    };

    match rte.kind {
        RangeTblEntryKind::Result => rel.add_path(optimize_path(
            Path::Result {
                plan_info: PlanEstimate::default(),
            },
            catalog,
        )),
        RangeTblEntryKind::Relation {
            rel: heap_rel,
            relation_oid,
            relkind: _,
            toast,
        } => rel.pathlist.extend(collect_relation_access_paths(
            rtindex,
            heap_rel,
            relation_oid,
            toast,
            rte.desc.clone(),
            base_filter_expr(rel),
            query_order_items,
            catalog,
        )),
        RangeTblEntryKind::Values {
            rows,
            output_columns,
        } => {
            let mut path = optimize_path(
                Path::Values {
                    plan_info: PlanEstimate::default(),
                    slot_id: rtindex,
                    rows,
                    output_columns,
                },
                catalog,
            );
            if let Some(filter) = base_filter_expr(rel) {
                path = optimize_path(
                    Path::Filter {
                        plan_info: PlanEstimate::default(),
                        predicate: rewrite_expr_against_layout(filter, &path.output_vars()),
                        input: Box::new(path),
                    },
                    catalog,
                );
            }
            rel.add_path(path);
        }
        RangeTblEntryKind::Function { call } => {
            let mut path = optimize_path(
                Path::FunctionScan {
                    plan_info: PlanEstimate::default(),
                    slot_id: rtindex,
                    call,
                },
                catalog,
            );
            if let Some(filter) = base_filter_expr(rel) {
                path = optimize_path(
                    Path::Filter {
                        plan_info: PlanEstimate::default(),
                        predicate: rewrite_expr_against_layout(filter, &path.output_vars()),
                        input: Box::new(path),
                    },
                    catalog,
                );
            }
            rel.add_path(path);
        }
        RangeTblEntryKind::Subquery { query } => {
            let mut subroot = PlannerInfo::new(*query);
            let scanjoin_rel = query_planner(&mut subroot, catalog);
            let final_rel = grouping_planner(&mut subroot, scanjoin_rel, catalog);
            let required_pathkeys =
                lower_pathkeys_for_rel(&subroot, &final_rel, &subroot.query_pathkeys);
            let mut path = bestpath::choose_final_path(&final_rel, &required_pathkeys)
                .cloned()
                .unwrap_or(Path::Result {
                    plan_info: PlanEstimate::default(),
                });
            path = normalize_rte_path(rtindex, &rte.desc, path, catalog);
            if let Some(filter) = base_filter_expr(rel) {
                path = optimize_path(
                    Path::Filter {
                        plan_info: PlanEstimate::default(),
                        predicate: rewrite_expr_against_layout(filter, &path.output_vars()),
                        input: Box::new(path),
                    },
                    catalog,
                );
            }
            rel.add_path(path);
        }
        RangeTblEntryKind::Join { .. } => unreachable!("join RTEs are not base relations"),
    }
    bestpath::set_cheapest(rel);
}

fn set_base_rel_pathlists(root: &mut PlannerInfo, catalog: &dyn CatalogLookup) {
    let max_rtindex = root.simple_rel_array.len().saturating_sub(1);
    for rtindex in 1..=max_rtindex {
        if root
            .simple_rel_array
            .get(rtindex)
            .and_then(Option::as_ref)
            .is_some()
        {
            set_base_rel_pathlist(root, rtindex, catalog);
        }
    }
}

fn build_join_qual(
    kind: JoinType,
    explicit_qual: Option<Expr>,
    left_relids: &[usize],
    right_relids: &[usize],
    inner_join_clauses: &[RestrictInfo],
) -> Expr {
    let join_relids = relids_union(left_relids, right_relids);
    let mut clauses = Vec::new();
    if let Some(explicit_qual) = explicit_qual {
        clauses.push(explicit_qual);
    }
    if matches!(kind, JoinType::Inner | JoinType::Cross) {
        for restrict in inner_join_clauses {
            let clause = &restrict.clause;
            let clause_relids = &restrict.required_relids;
            if clause_relids.len() <= 1 {
                continue;
            }
            if relids_subset(&clause_relids, &join_relids)
                && !relids_subset(&clause_relids, left_relids)
                && !relids_subset(&clause_relids, right_relids)
                && !clauses.contains(clause)
            {
                clauses.push(clause.clone());
            }
        }
    }
    and_exprs(clauses).unwrap_or(Expr::Const(Value::Bool(true)))
}

fn maybe_project_join_alias(
    rtindex: usize,
    input: Path,
    root: &PlannerInfo,
    reltarget: &PathTarget,
    catalog: &dyn CatalogLookup,
) -> Path {
    let Some(rte) = root.parse.rtable.get(rtindex.saturating_sub(1)) else {
        return input;
    };
    let RangeTblEntryKind::Join { joinaliasvars, .. } = &rte.kind else {
        return input;
    };
    let layout = input.output_vars();
    let desired_layout = PathTarget::from_rte(rtindex, rte).exprs;
    let alias_target_exprs = joinaliasvars
        .iter()
        .cloned()
        .map(|expr| {
            let original_expr = expr.clone();
            let rewritten = rewrite_semantic_expr_for_path(original_expr.clone(), &input, &layout);
            if rewritten == original_expr && !layout.contains(&rewritten) {
                let expanded = expand_join_rte_vars(root, original_expr.clone());
                if expanded != original_expr {
                    rewrite_semantic_expr_for_path(expanded, &input, &layout)
                } else {
                    rewritten
                }
            } else {
                rewritten
            }
        })
        .collect::<Vec<_>>();
    let extra_exprs = reltarget
        .exprs
        .iter()
        .filter(|expr| layout_candidate_for_expr(root, expr, &desired_layout).is_none())
        .cloned()
        .map(|expr| rewrite_semantic_expr_for_path_or_expand_join_vars(root, expr, &input, &layout))
        .collect::<Vec<_>>();
    if extra_exprs.is_empty() && (layout == desired_layout || alias_target_exprs == layout) {
        return input;
    }
    if extra_exprs.is_empty() {
        return project_to_slot_layout_internal(
            Some(root),
            rtindex,
            &rte.desc,
            input,
            alias_target_exprs,
            catalog,
        );
    }

    let mut targets = rte
        .desc
        .columns
        .iter()
        .zip(alias_target_exprs)
        .enumerate()
        .map(|(index, (column, expr))| {
            TargetEntry::new(column.name.clone(), expr, column.sql_type, index + 1)
        })
        .collect::<Vec<_>>();
    let base_resno = targets.len();
    targets.extend(
        extra_exprs
            .into_iter()
            .enumerate()
            .map(|(index, expr): (usize, Expr)| {
                let resno = base_resno + index + 1;
                TargetEntry::new(
                    format!("support{resno}"),
                    expr.clone(),
                    expr_sql_type(&expr),
                    resno,
                )
            }),
    );

    optimize_path(
        Path::Projection {
            plan_info: PlanEstimate::default(),
            slot_id: next_synthetic_slot_id(),
            input: Box::new(input),
            targets,
        },
        catalog,
    )
}

fn top_join_rtindex(root: &PlannerInfo) -> Option<usize> {
    match root.parse.jointree.as_ref() {
        Some(JoinTreeNode::JoinExpr { rtindex, .. }) => root
            .parse
            .rtable
            .get(rtindex.saturating_sub(1))
            .filter(|rte| matches!(rte.kind, RangeTblEntryKind::Join { .. }))
            .map(|_| *rtindex),
        _ => None,
    }
}

fn normalize_join_output_rel(
    root: &PlannerInfo,
    input_rel: RelOptInfo,
    rtindex: usize,
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    let Some(rte) = root.parse.rtable.get(rtindex.saturating_sub(1)) else {
        return input_rel;
    };
    let mut rel = RelOptInfo::new(
        input_rel.relids.clone(),
        input_rel.reloptkind,
        PathTarget::from_rte(rtindex, rte),
    );
    for path in input_rel.pathlist {
        rel.add_path(maybe_project_join_alias(
            rtindex,
            path,
            root,
            &rel.reltarget,
            catalog,
        ));
    }
    bestpath::set_cheapest(&mut rel);
    rel
}

fn base_rels_at_level(root: &PlannerInfo, level: usize) -> Vec<RelOptInfo> {
    if level == 1 {
        root.simple_rel_array
            .iter()
            .skip(1)
            .filter_map(|rel| {
                rel.clone()
                    .filter(|rel| rel.reloptkind != RelOptKind::OtherMemberRel)
            })
            .collect()
    } else {
        root.join_rel_list
            .iter()
            .filter(|rel| rel.relids.len() == level)
            .cloned()
            .collect()
    }
}

fn find_join_rel_index(root: &PlannerInfo, relids: &[usize]) -> Option<usize> {
    root.join_rel_list
        .iter()
        .position(|rel| rel.relids == relids)
}

fn join_reltarget(
    _root: &PlannerInfo,
    _relids: &[usize],
    left_rel: &RelOptInfo,
    right_rel: &RelOptInfo,
) -> PathTarget {
    let mut exprs = left_rel.reltarget.exprs.clone();
    exprs.extend(right_rel.reltarget.exprs.clone());
    PathTarget::new(exprs)
}

fn join_spec_for_special_join(sjinfo: &SpecialJoinInfo, reversed: bool) -> JoinBuildSpec {
    JoinBuildSpec {
        kind: if reversed {
            reverse_join_type(sjinfo.jointype)
        } else {
            sjinfo.jointype
        },
        reversed,
        rtindex: Some(sjinfo.rtindex),
        explicit_qual: Some(sjinfo.join_quals.clone()),
    }
}

fn special_join_relids(sjinfo: &SpecialJoinInfo) -> Vec<usize> {
    relids_union(&sjinfo.syn_lefthand, &sjinfo.syn_righthand)
}

fn rel_contains_special_join(relids: &[usize], sjinfo: &SpecialJoinInfo) -> bool {
    relids_subset(&sjinfo.min_lefthand, relids) && relids_subset(&sjinfo.min_righthand, relids)
}

fn rel_matches_special_join(
    sjinfo: &SpecialJoinInfo,
    left_relids: &[usize],
    right_relids: &[usize],
) -> Option<bool> {
    if relids_subset(&sjinfo.min_lefthand, left_relids)
        && relids_subset(&sjinfo.min_righthand, right_relids)
    {
        Some(false)
    } else if relids_subset(&sjinfo.min_lefthand, right_relids)
        && relids_subset(&sjinfo.min_righthand, left_relids)
    {
        Some(true)
    } else {
        None
    }
}

fn relids_match_ojrelid(root: &PlannerInfo, relids: &[usize], ojrelid: usize) -> bool {
    root.join_info_list
        .iter()
        .any(|sjinfo| sjinfo.ojrelid == Some(ojrelid) && special_join_relids(sjinfo) == relids)
}

fn input_crosses_rhs_boundary(input_relids: &[usize], sjinfo: &SpecialJoinInfo) -> bool {
    relids_overlap(input_relids, &sjinfo.min_righthand)
        && !relids_subset(input_relids, &sjinfo.min_righthand)
}

fn input_can_commute_past_special_join(
    root: &PlannerInfo,
    input_relids: &[usize],
    sjinfo: &SpecialJoinInfo,
) -> bool {
    if !input_crosses_rhs_boundary(input_relids, sjinfo) {
        return true;
    }
    sjinfo
        .commute_below_l
        .iter()
        .chain(sjinfo.commute_below_r.iter())
        .any(|ojrelid| relids_match_ojrelid(root, input_relids, *ojrelid))
}

fn violates_full_join_barrier(
    root: &PlannerInfo,
    sjinfo: &SpecialJoinInfo,
    left_relids: &[usize],
    right_relids: &[usize],
    joinrelids: &[usize],
) -> bool {
    if sjinfo.jointype != JoinType::Full {
        return false;
    }
    let full_relids = special_join_relids(sjinfo);
    relids_overlap(&full_relids, joinrelids)
        && full_relids != joinrelids
        && full_relids != left_relids
        && full_relids != right_relids
        && (relids_overlap(&full_relids, left_relids) || relids_overlap(&full_relids, right_relids))
        && !rel_contains_special_join(left_relids, sjinfo)
        && !rel_contains_special_join(right_relids, sjinfo)
        && !input_can_commute_past_special_join(root, left_relids, sjinfo)
        && !input_can_commute_past_special_join(root, right_relids, sjinfo)
}

fn join_is_legal(
    root: &PlannerInfo,
    left_rel: &RelOptInfo,
    right_rel: &RelOptInfo,
) -> Option<JoinBuildSpec> {
    let joinrelids = relids_union(&left_rel.relids, &right_rel.relids);
    let mut matched_sj: Option<(&SpecialJoinInfo, bool)> = None;
    let mut must_be_leftjoin = false;

    for sjinfo in &root.join_info_list {
        if violates_full_join_barrier(
            root,
            sjinfo,
            &left_rel.relids,
            &right_rel.relids,
            &joinrelids,
        ) {
            return None;
        }
        if !relids_overlap(&sjinfo.min_righthand, &joinrelids) {
            continue;
        }
        if relids_subset(&joinrelids, &sjinfo.min_righthand) {
            continue;
        }
        if rel_contains_special_join(&left_rel.relids, sjinfo) {
            continue;
        }
        if rel_contains_special_join(&right_rel.relids, sjinfo) {
            continue;
        }

        if let Some(reversed) =
            rel_matches_special_join(sjinfo, &left_rel.relids, &right_rel.relids)
        {
            if matched_sj.is_some() {
                return None;
            }
            matched_sj = Some((sjinfo, reversed));
            continue;
        }

        if relids_overlap(&left_rel.relids, &sjinfo.min_righthand)
            && relids_overlap(&right_rel.relids, &sjinfo.min_righthand)
        {
            if input_can_commute_past_special_join(root, &left_rel.relids, sjinfo)
                && input_can_commute_past_special_join(root, &right_rel.relids, sjinfo)
            {
                continue;
            }
            return None;
        }
        if sjinfo.jointype != JoinType::Left || relids_overlap(&joinrelids, &sjinfo.min_lefthand) {
            return None;
        }
        must_be_leftjoin = true;
    }

    if must_be_leftjoin
        && !matched_sj
            .is_some_and(|(sjinfo, _)| sjinfo.jointype == JoinType::Left && sjinfo.lhs_strict)
    {
        return None;
    }

    if let Some((sjinfo, reversed)) = matched_sj {
        return Some(join_spec_for_special_join(sjinfo, reversed));
    }

    Some(JoinBuildSpec {
        kind: JoinType::Inner,
        reversed: false,
        rtindex: None,
        explicit_qual: None,
    })
}

fn make_join_rel(
    root: &mut PlannerInfo,
    left_rel: &RelOptInfo,
    right_rel: &RelOptInfo,
    catalog: &dyn CatalogLookup,
) -> Option<RelOptInfo> {
    if !relids_disjoint(&left_rel.relids, &right_rel.relids) {
        return None;
    }
    let relids = relids_union(&left_rel.relids, &right_rel.relids);
    let spec = join_is_legal(root, left_rel, right_rel)?;
    let reltarget = join_reltarget(root, &relids, left_rel, right_rel);
    let join_qual = build_join_qual(
        spec.kind,
        spec.explicit_qual.clone(),
        &left_rel.relids,
        &right_rel.relids,
        &root.inner_join_clauses,
    );
    let join_rel_index = match find_join_rel_index(root, &relids) {
        Some(index) => index,
        None => {
            root.join_rel_list.push(RelOptInfo::new(
                relids.clone(),
                RelOptKind::JoinRel,
                reltarget.clone(),
            ));
            root.join_rel_list.len() - 1
        }
    };
    let mut candidate_paths = Vec::new();
    let output_rtindex = spec
        .rtindex
        .filter(|rtindex| exact_join_rtindex(root, &relids) == Some(*rtindex))
        .or_else(|| exact_join_rtindex(root, &relids));
    for left_path in &left_rel.pathlist {
        for right_path in &right_rel.pathlist {
            let paths = build_join_paths_with_root(
                root,
                left_path.clone(),
                right_path.clone(),
                &left_rel.relids,
                &right_rel.relids,
                spec.kind,
                join_qual.clone(),
            );
            for path in paths {
                let path = if spec.reversed {
                    restore_join_output_order(
                        path,
                        &right_path.columns(),
                        &left_path.columns(),
                        &right_path.output_vars(),
                        &left_path.output_vars(),
                    )
                } else {
                    path
                };
                let path = match output_rtindex {
                    Some(rtindex) => {
                        maybe_project_join_alias(rtindex, path, root, &reltarget, catalog)
                    }
                    None => path,
                };
                candidate_paths.push(path);
            }
        }
    }
    let join_rel = root
        .join_rel_list
        .get_mut(join_rel_index)
        .expect("join rel just inserted or found");
    if !join_rel
        .joininfo
        .iter()
        .any(|info| info.clause == join_qual)
    {
        join_rel
            .joininfo
            .push(joininfo::make_restrict_info(join_qual.clone()));
    }
    for path in candidate_paths {
        join_rel.add_path(path);
    }
    bestpath::set_cheapest(join_rel);
    let rel = join_rel.clone();
    let rel = if let Some(rtindex) = spec
        .rtindex
        .filter(|rtindex| exact_join_rtindex(root, &relids) == Some(*rtindex))
    {
        normalize_join_output_rel(root, rel, rtindex, catalog)
    } else {
        rel
    };
    root.join_rel_list[join_rel_index] = rel.clone();
    Some(rel)
}

fn join_search_one_level(root: &mut PlannerInfo, level: usize, catalog: &dyn CatalogLookup) {
    for left_level in 1..level {
        let right_level = level - left_level;
        if left_level > right_level {
            continue;
        }
        let left_rels = base_rels_at_level(root, left_level);
        let right_rels = base_rels_at_level(root, right_level);
        for left_rel in &left_rels {
            for right_rel in &right_rels {
                if left_level == right_level && left_rel.relids >= right_rel.relids {
                    continue;
                }
                let _ = make_join_rel(root, left_rel, right_rel, catalog);
            }
        }
    }
}

pub(super) fn make_one_rel(root: &mut PlannerInfo, catalog: &dyn CatalogLookup) -> RelOptInfo {
    assign_base_restrictinfo(root);
    expand_inherited_rtentries(root, catalog);
    root.inner_join_clauses = collect_inner_join_clauses(root);
    set_base_rel_pathlists(root, catalog);
    let query_relids = root.all_query_relids();
    if query_relids.is_empty() {
        let mut rel = RelOptInfo::new(
            Vec::new(),
            RelOptKind::UpperRel,
            PathTarget::new(Vec::new()),
        );
        rel.add_path(optimize_path(
            Path::Result {
                plan_info: PlanEstimate::default(),
            },
            catalog,
        ));
        bestpath::set_cheapest(&mut rel);
        return rel;
    }
    if query_relids.len() == 1 {
        return root.simple_rel_array[query_relids[0]]
            .clone()
            .expect("single base relation reloptinfo");
    }
    for level in 2..=query_relids.len() {
        join_search_one_level(root, level, catalog);
    }
    let rel = root
        .join_rel_list
        .iter()
        .find(|rel| rel.relids == query_relids)
        .cloned()
        .unwrap_or_else(|| panic!("failed to build join rel for relids {:?}", query_relids));
    if let Some(rtindex) = top_join_rtindex(root) {
        normalize_join_output_rel(root, rel, rtindex, catalog)
    } else {
        rel
    }
}

pub(super) fn query_planner(root: &mut PlannerInfo, catalog: &dyn CatalogLookup) -> RelOptInfo {
    let mut rel = make_one_rel(root, catalog);
    if has_grouping(root) && rel.relids.len() > 1 && rel.reltarget != root.scanjoin_target {
        rel = make_pathtarget_projection_rel(root, rel, &root.scanjoin_target, catalog, false);
    }
    rel
}
