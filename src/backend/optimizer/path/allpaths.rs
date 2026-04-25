use std::cmp::Ordering;

use crate::RelFileLocator;
use crate::backend::parser::CatalogLookup;
use crate::include::nodes::parsenodes::{JoinTreeNode, RangeTblEntryKind};
use crate::include::nodes::pathnodes::{
    Path, PathKey, PathTarget, PlannerConfig, PlannerInfo, PlannerSubroot, RelOptInfo, RelOptKind,
    RestrictInfo, SpecialJoinInfo,
};
use crate::include::nodes::plannodes::PlanEstimate;
use crate::include::nodes::primnodes::{
    Expr, JoinType, OrderByEntry, QueryColumn, RelationDesc, ToastRelationRef, Var, user_attrno,
};

use super::super::bestpath;
use super::super::inherit::{
    append_child_rtindexes, append_translation, expand_inherited_rtentries,
};
use super::super::joininfo;
use super::super::optimize_path;
use super::super::partition_prune::partition_may_satisfy_filter;
use super::super::partitionwise;
use super::super::pathnodes::{next_synthetic_slot_id, rte_slot_id, slot_output_target};
use super::super::plan::grouping_planner;
use super::super::util::{
    normalize_rte_path, pathkeys_to_order_items, project_to_slot_layout,
    required_query_pathkeys_for_rel,
};
use super::super::{
    JoinBuildSpec, and_exprs, expand_join_rte_vars, expr_relids, flatten_and_conjuncts,
    has_outer_joins, is_pushable_base_clause, relids_disjoint, relids_overlap, relids_subset,
    relids_union, reverse_join_type,
};
use super::{
    build_index_path_spec, build_join_paths_with_root, estimate_index_candidate,
    estimate_seqscan_candidate, relation_stats,
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
        .filter(|clause| !is_pushable_base_clause(root, &expr_relids(clause)))
        .collect();
    and_exprs(clauses)
}

fn assign_base_restrictinfo(root: &mut PlannerInfo) {
    for rel in root.simple_rel_array.iter_mut().flatten() {
        rel.baserestrictinfo.clear();
        rel.joininfo.clear();
    }
    for (rtindex, rte) in root.parse.rtable.iter().enumerate() {
        if !matches!(rte.kind, RangeTblEntryKind::Relation { .. }) {
            continue;
        }
        let relid = rtindex + 1;
        let Some(rel) = root
            .simple_rel_array
            .get_mut(relid)
            .and_then(Option::as_mut)
        else {
            continue;
        };
        rel.baserestrictinfo.extend(
            rte.security_quals
                .iter()
                .cloned()
                .map(joininfo::make_restrict_info),
        );
    }
    if let Some(where_qual) = root.parse.where_qual.as_ref() {
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
}

fn base_filter_expr(rel: &RelOptInfo) -> Option<Expr> {
    super::super::and_exprs(
        rel.baserestrictinfo
            .iter()
            .map(|restrict| restrict.clause.clone())
            .collect(),
    )
}

fn order_items_for_base_rel_pathkeys(
    root: &PlannerInfo,
    rtindex: usize,
    pathkeys: &[PathKey],
) -> Option<Vec<OrderByEntry>> {
    if pathkeys.is_empty() {
        return None;
    }
    let expanded_pathkeys = pathkeys
        .iter()
        .cloned()
        .map(|key| PathKey {
            expr: expand_join_rte_vars(root, key.expr),
            ressortgroupref: key.ressortgroupref,
            descending: key.descending,
            nulls_first: key.nulls_first,
            collation_oid: key.collation_oid,
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

fn query_order_items_for_base_rel(root: &PlannerInfo, rtindex: usize) -> Option<Vec<OrderByEntry>> {
    order_items_for_base_rel_pathkeys(root, rtindex, &root.query_pathkeys)
}

fn collect_relation_access_paths(
    rtindex: usize,
    heap_rel: RelFileLocator,
    relation_name: String,
    relation_oid: u32,
    relkind: char,
    relispopulated: bool,
    toast: Option<ToastRelationRef>,
    desc: RelationDesc,
    filter: Option<Expr>,
    query_order_items: Option<Vec<OrderByEntry>>,
    catalog: &dyn CatalogLookup,
) -> Vec<Path> {
    if relkind == 'p' {
        return Vec::new();
    }
    let stats = relation_stats(catalog, relation_oid, &desc);
    let mut paths = vec![
        estimate_seqscan_candidate(
            rtindex,
            heap_rel,
            relation_name.clone(),
            relation_oid,
            relkind,
            relispopulated,
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
                relation_name.clone(),
                relation_oid,
                relkind,
                relispopulated,
                toast,
                desc.clone(),
                &stats,
                filter.clone(),
                Some(order_items),
            )
            .plan,
        );
    }
    if relkind != 'r' {
        return paths;
    }
    for index in catalog
        .index_relations_for_heap(relation_oid)
        .iter()
        .filter(|index| {
            index.index_meta.indisvalid
                && index.index_meta.indisready
                && !index.index_meta.indisexclusion
                && !index.index_meta.indkey.is_empty()
        })
    {
        if let Some(spec) = build_index_path_spec(filter.as_ref(), None, index) {
            paths.push(
                estimate_index_candidate(
                    rtindex,
                    heap_rel,
                    relation_name.clone(),
                    relation_oid,
                    toast,
                    desc.clone(),
                    &stats,
                    spec,
                    None,
                    catalog,
                )
                .plan,
            );
        }
        if let Some(order_items) = query_order_items.as_ref()
            && let Some(spec) = build_index_path_spec(filter.as_ref(), Some(order_items), index)
        {
            paths.push(
                estimate_index_candidate(
                    rtindex,
                    heap_rel,
                    relation_name.clone(),
                    relation_oid,
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

fn collect_relation_ordered_index_paths(
    rtindex: usize,
    heap_rel: RelFileLocator,
    relation_name: String,
    relation_oid: u32,
    toast: Option<ToastRelationRef>,
    desc: RelationDesc,
    filter: Option<Expr>,
    order_items: &[OrderByEntry],
    catalog: &dyn CatalogLookup,
) -> Vec<Path> {
    let stats = relation_stats(catalog, relation_oid, &desc);
    let mut paths = Vec::new();
    for index in catalog
        .index_relations_for_heap(relation_oid)
        .iter()
        .filter(|index| {
            index.index_meta.indisvalid
                && index.index_meta.indisready
                && !index.index_meta.indisexclusion
                && !index.index_meta.indkey.is_empty()
        })
    {
        if let Some(spec) = build_index_path_spec(filter.as_ref(), Some(order_items), index) {
            if !spec.removes_order {
                continue;
            }
            paths.push(
                estimate_index_candidate(
                    rtindex,
                    heap_rel,
                    relation_name.clone(),
                    relation_oid,
                    toast,
                    desc.clone(),
                    &stats,
                    spec,
                    Some(order_items.to_vec()),
                    catalog,
                )
                .plan,
            );
        }
    }
    paths
}

pub(super) fn relation_ordered_index_paths(
    root: &PlannerInfo,
    rtindex: usize,
    pathkeys: &[PathKey],
    catalog: &dyn CatalogLookup,
) -> Vec<Path> {
    let Some(order_items) = order_items_for_base_rel_pathkeys(root, rtindex, pathkeys) else {
        return Vec::new();
    };
    let Some(rel) = root.simple_rel_array.get(rtindex).and_then(Option::as_ref) else {
        return Vec::new();
    };
    let Some(rte) = root.parse.rtable.get(rtindex - 1) else {
        return Vec::new();
    };
    match &rte.kind {
        RangeTblEntryKind::Relation {
            rel: heap_rel,
            relation_oid,
            relkind,
            relispopulated: _,
            toast,
        } if *relkind == 'r' => collect_relation_ordered_index_paths(
            rtindex,
            *heap_rel,
            rte.alias
                .clone()
                .unwrap_or_else(|| format!("rel {}", heap_rel.rel_number)),
            *relation_oid,
            *toast,
            rte.desc.clone(),
            base_filter_expr(rel),
            &order_items,
            catalog,
        ),
        _ => Vec::new(),
    }
}

fn cheapest_relation_access_path(
    rtindex: usize,
    heap_rel: RelFileLocator,
    relation_name: String,
    relation_oid: u32,
    relkind: char,
    relispopulated: bool,
    toast: Option<ToastRelationRef>,
    desc: RelationDesc,
    filter: Option<Expr>,
    catalog: &dyn CatalogLookup,
) -> Path {
    collect_relation_access_paths(
        rtindex,
        heap_rel,
        relation_name,
        relation_oid,
        relkind,
        relispopulated,
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
        pathtarget: PathTarget::new(Vec::new()),
    })
}

fn plan_query_path(
    query: crate::include::nodes::parsenodes::Query,
    catalog: &dyn CatalogLookup,
    config: PlannerConfig,
) -> (PlannerInfo, Path) {
    let query = super::super::root::prepare_query_for_planning(query);
    let mut root = PlannerInfo::new_with_config(query, config);
    let scanjoin_rel = query_planner(&mut root, catalog);
    let final_rel = grouping_planner(&mut root, scanjoin_rel, catalog);
    let required_pathkeys = required_query_pathkeys_for_rel(&root, &final_rel);
    let path = bestpath::choose_final_path(&final_rel, &required_pathkeys)
        .cloned()
        .unwrap_or(Path::Result {
            plan_info: PlanEstimate::default(),
            pathtarget: PathTarget::new(Vec::new()),
        });
    (root, path)
}

fn build_recursive_union_path(
    recursive_union: crate::include::nodes::parsenodes::RecursiveUnionQuery,
    catalog: &dyn CatalogLookup,
    config: PlannerConfig,
) -> Path {
    let anchor_query = recursive_union.anchor.clone();
    let recursive_query = recursive_union.recursive.clone();
    let (anchor_root, anchor_path) = plan_query_path(recursive_union.anchor, catalog, config);
    let (recursive_root, recursive_path) =
        plan_query_path(recursive_union.recursive, catalog, config);
    let slot_id = next_synthetic_slot_id();
    let output_columns = recursive_union
        .output_desc
        .columns
        .iter()
        .map(|column| QueryColumn {
            name: column.name.clone(),
            sql_type: column.sql_type,
            wire_type_oid: None,
        })
        .collect::<Vec<_>>();
    optimize_path(
        Path::RecursiveUnion {
            plan_info: PlanEstimate::default(),
            pathtarget: slot_output_target(slot_id, &output_columns, |column| column.sql_type),
            slot_id,
            worktable_id: recursive_union.worktable_id,
            distinct: recursive_union.distinct,
            anchor_root: PlannerSubroot::new(anchor_root),
            recursive_root: PlannerSubroot::new(recursive_root),
            anchor_query: Box::new(anchor_query),
            recursive_query: Box::new(recursive_query),
            recursive_references_worktable: recursive_union.recursive_references_worktable,
            output_columns,
            anchor: Box::new(anchor_path),
            recursive: Box::new(recursive_path),
        },
        catalog,
    )
}

fn build_set_operation_rel(root: &mut PlannerInfo, catalog: &dyn CatalogLookup) -> RelOptInfo {
    let set_operation = root
        .parse
        .set_operation
        .clone()
        .expect("set-operation rel requested without set_operation query");
    let source_id = 1usize;
    let desc = set_operation.output_desc;
    let (child_roots, children) = set_operation
        .inputs
        .into_iter()
        .map(|query| {
            let (child_root, path) = plan_query_path(query, catalog, root.config);
            (
                Some(PlannerSubroot::new(child_root)),
                project_to_slot_layout(
                    source_id,
                    &desc,
                    path.clone(),
                    path.output_target(),
                    catalog,
                ),
            )
        })
        .unzip::<_, _, Vec<_>, Vec<_>>();
    let output_columns = desc
        .columns
        .iter()
        .map(|column| QueryColumn {
            name: column.name.clone(),
            sql_type: column.sql_type,
            wire_type_oid: None,
        })
        .collect::<Vec<_>>();
    let set_op = optimize_path(
        Path::SetOp {
            plan_info: PlanEstimate::default(),
            pathtarget: slot_output_target(source_id, &output_columns, |column| column.sql_type),
            slot_id: source_id,
            op: set_operation.op,
            output_columns,
            child_roots,
            children,
        },
        catalog,
    );
    let mut rel = RelOptInfo::new(
        Vec::new(),
        RelOptKind::UpperRel,
        set_op.semantic_output_target(),
    );
    rel.add_path(set_op);
    bestpath::set_cheapest(&mut rel);
    rel
}

fn build_cte_scan_path(
    rtindex: usize,
    cte_id: usize,
    query: crate::include::nodes::parsenodes::Query,
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
    config: PlannerConfig,
) -> Path {
    let query = super::super::root::prepare_query_for_planning(query);
    let (subroot, cte_path) = if let Some(recursive_union) = query.recursive_union.clone() {
        (
            PlannerInfo::new_with_config(
                super::super::root::prepare_query_for_planning(query.clone()),
                config,
            ),
            build_recursive_union_path(*recursive_union, catalog, config),
        )
    } else {
        plan_query_path(query.clone(), catalog, config)
    };
    let output_columns = desc
        .columns
        .iter()
        .map(|column| QueryColumn {
            name: column.name.clone(),
            sql_type: column.sql_type,
            wire_type_oid: None,
        })
        .collect::<Vec<_>>();
    Path::CteScan {
        plan_info: cte_path.plan_info(),
        pathtarget: slot_output_target(rtindex, &output_columns, |column| column.sql_type),
        slot_id: rte_slot_id(rtindex),
        cte_id,
        subroot: PlannerSubroot::new(subroot),
        query: Box::new(query),
        cte_plan: Box::new(cte_path),
        output_columns,
    }
}

fn build_subquery_scan_path(
    rtindex: usize,
    query: crate::include::nodes::parsenodes::Query,
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
    config: PlannerConfig,
) -> Path {
    let query = super::super::root::prepare_query_for_planning(query);
    let (subroot, input) = if let Some(recursive_union) = query.recursive_union.clone() {
        (
            PlannerInfo::new_with_config(
                super::super::root::prepare_query_for_planning(query.clone()),
                config,
            ),
            build_recursive_union_path(*recursive_union, catalog, config),
        )
    } else {
        plan_query_path(query.clone(), catalog, config)
    };
    let input_vars = input.semantic_output_vars();
    let pathkeys = input
        .pathkeys()
        .into_iter()
        .filter_map(|key| {
            input_vars
                .iter()
                .position(|expr| *expr == key.expr)
                .and_then(|index| {
                    desc.columns
                        .get(index)
                        .map(|column| (index, column.sql_type))
                })
                .map(|(index, sql_type)| PathKey {
                    expr: Expr::Var(Var {
                        varno: rtindex,
                        varattno: user_attrno(index),
                        varlevelsup: 0,
                        vartype: sql_type,
                    }),
                    ressortgroupref: key.ressortgroupref,
                    descending: key.descending,
                    nulls_first: key.nulls_first,
                    collation_oid: key.collation_oid,
                })
        })
        .collect();
    let output_columns = desc
        .columns
        .iter()
        .map(|column| QueryColumn {
            name: column.name.clone(),
            sql_type: column.sql_type,
            wire_type_oid: None,
        })
        .collect::<Vec<_>>();
    Path::SubqueryScan {
        plan_info: input.plan_info(),
        pathtarget: slot_output_target(rtindex, &output_columns, |column| column.sql_type),
        rtindex,
        subroot: PlannerSubroot::new(subroot),
        query: Box::new(query),
        input: Box::new(input),
        output_columns,
        pathkeys,
    }
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
    if let RangeTblEntryKind::Relation {
        rel: heap_rel,
        relation_oid,
        relkind,
        relispopulated,
        toast,
    } = rte.kind.clone()
        && (relkind == 'p' || !child_rtindexes.is_empty())
    {
        let filter = root
            .simple_rel_array
            .get(rtindex)
            .and_then(Option::as_ref)
            .and_then(base_filter_expr);
        let mut children = Vec::new();
        if relkind != 'p' {
            children.push(normalize_rte_path(
                rtindex,
                &rte.desc,
                cheapest_relation_access_path(
                    rtindex,
                    heap_rel,
                    rte.alias
                        .clone()
                        .unwrap_or_else(|| format!("rel {}", heap_rel.rel_number)),
                    relation_oid,
                    relkind,
                    relispopulated,
                    toast,
                    rte.desc.clone(),
                    filter.clone(),
                    catalog,
                ),
                catalog,
            ));
        }
        for child_rtindex in child_rtindexes {
            let child_relation_oid = root
                .parse
                .rtable
                .get(child_rtindex.saturating_sub(1))
                .and_then(|child_rte| {
                    if let RangeTblEntryKind::Relation { relation_oid, .. } = child_rte.kind {
                        Some(relation_oid)
                    } else {
                        None
                    }
                });
            if let Some(child_oid) = child_relation_oid
                && !partition_may_satisfy_filter(catalog, relation_oid, child_oid, filter.as_ref())
            {
                continue;
            }
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
                PathTarget::new(translated_vars),
                catalog,
            ));
        }
        let append = optimize_path(
            Path::Append {
                plan_info: PlanEstimate::default(),
                pathtarget: slot_output_target(rtindex, &rte.desc.columns, |column| {
                    column.sql_type
                }),
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
                pathtarget: PathTarget::new(Vec::new()),
            },
            catalog,
        )),
        RangeTblEntryKind::Relation {
            rel: heap_rel,
            relation_oid,
            relkind,
            relispopulated,
            toast,
        } => rel.pathlist.extend(collect_relation_access_paths(
            rtindex,
            heap_rel,
            rte.alias
                .clone()
                .unwrap_or_else(|| format!("rel {}", heap_rel.rel_number)),
            relation_oid,
            relkind,
            relispopulated,
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
                    pathtarget: slot_output_target(rtindex, &output_columns, |column| {
                        column.sql_type
                    }),
                    slot_id: rte_slot_id(rtindex),
                    rows,
                    output_columns,
                },
                catalog,
            );
            path = normalize_rte_path(rtindex, &rte.desc, path, catalog);
            if let Some(filter) = base_filter_expr(rel) {
                path = optimize_path(
                    Path::Filter {
                        plan_info: PlanEstimate::default(),
                        pathtarget: path.semantic_output_target(),
                        predicate: filter,
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
                    pathtarget: slot_output_target(rtindex, call.output_columns(), |column| {
                        column.sql_type
                    }),
                    slot_id: rte_slot_id(rtindex),
                    call,
                },
                catalog,
            );
            path = normalize_rte_path(rtindex, &rte.desc, path, catalog);
            if let Some(filter) = base_filter_expr(rel) {
                path = optimize_path(
                    Path::Filter {
                        plan_info: PlanEstimate::default(),
                        pathtarget: path.semantic_output_target(),
                        predicate: filter,
                        input: Box::new(path),
                    },
                    catalog,
                );
            }
            rel.add_path(path);
        }
        RangeTblEntryKind::WorkTable { worktable_id } => {
            let mut path = optimize_path(
                Path::WorkTableScan {
                    plan_info: PlanEstimate::default(),
                    pathtarget: slot_output_target(
                        rtindex,
                        &rte.desc
                            .columns
                            .iter()
                            .map(|column| QueryColumn {
                                name: column.name.clone(),
                                sql_type: column.sql_type,
                                wire_type_oid: None,
                            })
                            .collect::<Vec<_>>(),
                        |column| column.sql_type,
                    ),
                    slot_id: rte_slot_id(rtindex),
                    worktable_id,
                    output_columns: rte
                        .desc
                        .columns
                        .iter()
                        .map(|column| QueryColumn {
                            name: column.name.clone(),
                            sql_type: column.sql_type,
                            wire_type_oid: None,
                        })
                        .collect(),
                },
                catalog,
            );
            path = normalize_rte_path(rtindex, &rte.desc, path, catalog);
            if let Some(filter) = base_filter_expr(rel) {
                path = optimize_path(
                    Path::Filter {
                        plan_info: PlanEstimate::default(),
                        pathtarget: path.semantic_output_target(),
                        predicate: filter,
                        input: Box::new(path),
                    },
                    catalog,
                );
            }
            rel.add_path(path);
        }
        RangeTblEntryKind::Cte { cte_id, query } => {
            let mut path =
                build_cte_scan_path(rtindex, cte_id, *query, &rte.desc, catalog, root.config);
            path = normalize_rte_path(rtindex, &rte.desc, path, catalog);
            if let Some(filter) = base_filter_expr(rel) {
                path = optimize_path(
                    Path::Filter {
                        plan_info: PlanEstimate::default(),
                        pathtarget: path.semantic_output_target(),
                        predicate: filter,
                        input: Box::new(path),
                    },
                    catalog,
                );
            }
            rel.add_path(path);
        }
        RangeTblEntryKind::Subquery { query } => {
            let mut path =
                build_subquery_scan_path(rtindex, *query, &rte.desc, catalog, root.config);
            if let Some(filter) = base_filter_expr(rel) {
                path = optimize_path(
                    Path::Filter {
                        plan_info: PlanEstimate::default(),
                        pathtarget: path.semantic_output_target(),
                        predicate: filter,
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

fn build_join_restrict_clauses(
    root: &PlannerInfo,
    kind: JoinType,
    explicit_qual: Option<Expr>,
    left_relids: &[usize],
    right_relids: &[usize],
    inner_join_clauses: &[RestrictInfo],
) -> Vec<RestrictInfo> {
    let join_relids = relids_union(left_relids, right_relids);
    let mut clauses = Vec::new();
    if let Some(explicit_qual) = explicit_qual {
        let explicit_qual = expand_join_rte_vars(root, explicit_qual);
        clauses.extend(
            flatten_and_conjuncts(&explicit_qual)
                .into_iter()
                .map(|clause| joininfo::make_restrict_info_with_pushdown(clause, false)),
        );
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
                && !clauses
                    .iter()
                    .any(|existing: &RestrictInfo| existing.clause == *clause)
            {
                clauses.push(restrict.clone());
            }
        }
    }
    clauses
}

#[derive(Clone, Copy)]
enum LevelRelRef {
    Base(usize),
    Join(usize),
}

fn rel_at_level<'a>(root: &'a PlannerInfo, rel_ref: LevelRelRef) -> &'a RelOptInfo {
    match rel_ref {
        LevelRelRef::Base(rtindex) => root.simple_rel_array[rtindex]
            .as_ref()
            .expect("base rel ref should point at an existing rel"),
        LevelRelRef::Join(index) => root
            .join_rel_list
            .get(index)
            .expect("join rel ref should point at an existing rel"),
    }
}

fn rel_refs_at_level(root: &PlannerInfo, level: usize) -> Vec<LevelRelRef> {
    if level == 1 {
        root.simple_rel_array
            .iter()
            .enumerate()
            .skip(1)
            .filter_map(|(rtindex, rel)| {
                rel.as_ref()
                    .filter(|rel| rel.reloptkind != RelOptKind::OtherMemberRel)
                    .map(|_| LevelRelRef::Base(rtindex))
            })
            .collect()
    } else {
        root.join_rel_list
            .iter()
            .enumerate()
            .filter(|(_, rel)| rel.relids.len() == level)
            .map(|(index, _)| LevelRelRef::Join(index))
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
    left_ref: LevelRelRef,
    right_ref: LevelRelRef,
    catalog: &dyn CatalogLookup,
) -> Option<()> {
    let (
        relids,
        reltarget,
        output_columns,
        join_restrict_clauses,
        mut candidate_paths,
        partitionwise_left_rel,
        partitionwise_right_rel,
        partitionwise_kind,
    ) = {
        let left_rel = rel_at_level(root, left_ref);
        let right_rel = rel_at_level(root, right_ref);
        if !relids_disjoint(&left_rel.relids, &right_rel.relids) {
            return None;
        }
        let relids = relids_union(&left_rel.relids, &right_rel.relids);
        let spec = join_is_legal(root, left_rel, right_rel)?;
        let (logical_left_rel, logical_right_rel) = if spec.reversed {
            (right_rel, left_rel)
        } else {
            (left_rel, right_rel)
        };
        let logical_kind = if spec.reversed {
            reverse_join_type(spec.kind)
        } else {
            spec.kind
        };
        let reltarget = join_reltarget(
            root,
            &relids,
            logical_left_rel,
            logical_right_rel,
            logical_kind,
        );
        let mut output_columns = logical_left_rel
            .cheapest_total_path()
            .map(Path::columns)
            .unwrap_or_default();
        if !matches!(logical_kind, JoinType::Semi | JoinType::Anti) {
            output_columns.extend(
                logical_right_rel
                    .cheapest_total_path()
                    .map(Path::columns)
                    .unwrap_or_default(),
            );
        }
        let join_restrict_clauses = build_join_restrict_clauses(
            root,
            logical_kind,
            spec.explicit_qual.clone(),
            &logical_left_rel.relids,
            &logical_right_rel.relids,
            &root.inner_join_clauses,
        );
        let mut join_restrict_clauses_for_rel = join_restrict_clauses.clone();
        let mut candidate_paths = collect_join_candidate_paths(
            root,
            logical_left_rel,
            logical_right_rel,
            logical_kind,
            &join_restrict_clauses,
            &reltarget,
            &output_columns,
        );
        let mut partitionwise_left_rel = logical_left_rel.clone();
        let mut partitionwise_right_rel = logical_right_rel.clone();
        let mut partitionwise_kind = logical_kind;
        if candidate_paths.is_empty() && spec.reversed {
            let fallback_join_restrict_clauses = build_join_restrict_clauses(
                root,
                spec.kind,
                spec.explicit_qual.clone(),
                &left_rel.relids,
                &right_rel.relids,
                &root.inner_join_clauses,
            );
            candidate_paths = collect_join_candidate_paths(
                root,
                left_rel,
                right_rel,
                spec.kind,
                &fallback_join_restrict_clauses,
                &reltarget,
                &output_columns,
            );
            if !candidate_paths.is_empty() {
                join_restrict_clauses_for_rel = fallback_join_restrict_clauses;
                partitionwise_left_rel = left_rel.clone();
                partitionwise_right_rel = right_rel.clone();
                partitionwise_kind = spec.kind;
            }
        }
        (
            relids,
            reltarget,
            output_columns,
            join_restrict_clauses_for_rel,
            candidate_paths,
            partitionwise_left_rel,
            partitionwise_right_rel,
            partitionwise_kind,
        )
    };
    let partition_info_for_rel = partitionwise::generate_partitionwise_join_path(
        root,
        &partitionwise_left_rel,
        &partitionwise_right_rel,
        partitionwise_kind,
        &join_restrict_clauses,
        &reltarget,
        &output_columns,
        catalog,
    )
    .and_then(|(path, partition_info)| {
        candidate_paths.push(prefer_partitionwise_path_cost(path, &candidate_paths));
        partition_info
    });
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
    let join_rel = root
        .join_rel_list
        .get_mut(join_rel_index)
        .expect("join rel just inserted or found");
    if !join_rel.joininfo.iter().any(|info| {
        join_restrict_clauses
            .iter()
            .any(|clause| clause.clause == info.clause)
    }) {
        join_rel.joininfo.extend(join_restrict_clauses.clone());
    }
    for path in candidate_paths {
        join_rel.add_path(path);
    }
    if let Some(partition_info) = partition_info_for_rel {
        join_rel.consider_partitionwise_join = true;
        join_rel.partition_info = Some(partition_info);
    }
    bestpath::set_cheapest(join_rel);
    Some(())
}

fn prefer_partitionwise_path_cost(path: Path, existing_paths: &[Path]) -> Path {
    let Some(best_existing_total) = existing_paths
        .iter()
        .map(|path| path.plan_info().total_cost.as_f64())
        .min_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal))
    else {
        return path;
    };
    if path.plan_info().total_cost.as_f64() < best_existing_total {
        return path;
    }
    match path {
        Path::Append {
            plan_info,
            pathtarget,
            source_id,
            desc,
            children,
        } => Path::Append {
            plan_info: PlanEstimate::new(
                plan_info.startup_cost.as_f64(),
                best_existing_total * 0.99,
                plan_info.plan_rows.as_f64(),
                plan_info.plan_width,
            ),
            pathtarget,
            source_id,
            desc,
            children,
        },
        other => other,
    }
}

fn collect_join_candidate_paths(
    root: &PlannerInfo,
    left_rel: &RelOptInfo,
    right_rel: &RelOptInfo,
    kind: JoinType,
    join_restrict_clauses: &[RestrictInfo],
    reltarget: &PathTarget,
    output_columns: &[QueryColumn],
) -> Vec<Path> {
    let mut candidate_paths = Vec::new();
    for left_path in &left_rel.pathlist {
        for right_path in &right_rel.pathlist {
            let paths = build_join_paths_with_root(
                root,
                left_path.clone(),
                right_path.clone(),
                &left_rel.relids,
                &right_rel.relids,
                kind,
                join_restrict_clauses.to_vec(),
                reltarget.clone(),
                output_columns.to_vec(),
            );
            for path in paths {
                candidate_paths.push(path);
            }
        }
    }
    candidate_paths
}

fn join_search_one_level(root: &mut PlannerInfo, level: usize, catalog: &dyn CatalogLookup) {
    for left_level in 1..level {
        let right_level = level - left_level;
        if left_level > right_level {
            continue;
        }
        let left_refs = rel_refs_at_level(root, left_level);
        let right_refs = rel_refs_at_level(root, right_level);
        for left_ref in left_refs {
            for right_ref in &right_refs {
                if left_level == right_level
                    && rel_at_level(root, left_ref).relids >= rel_at_level(root, *right_ref).relids
                {
                    continue;
                }
                let _ = make_join_rel(root, left_ref, *right_ref, catalog);
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
                pathtarget: PathTarget::new(Vec::new()),
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
    rel
}

pub(super) fn query_planner(root: &mut PlannerInfo, catalog: &dyn CatalogLookup) -> RelOptInfo {
    if root.parse.set_operation.is_some() {
        return build_set_operation_rel(root, catalog);
    }
    make_one_rel(root, catalog)
}
