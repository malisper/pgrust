#![allow(dead_code)]

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::nodes::datum::Value;
use crate::include::nodes::pathnodes::{Path, PathKey, PathTarget, PlannerInfo};
use crate::include::nodes::plannodes::{AggregateStrategy, Plan, PlanEstimate};
use crate::include::nodes::primnodes::{
    AggAccum, Aggref, BoolExpr, Expr, ExprArraySubscript, FuncExpr, JoinType, OpExpr,
    ProjectSetTarget, QueryColumn, ScalarArrayOpExpr, SubLinkType, TargetEntry, Var, WindowClause,
    user_attrno,
};

use super::inherit::{append_translation, translate_append_rel_expr};
use super::util::{IndexedPathTarget, simple_var_key, strip_binary_coercible_casts};

// Keep planner-generated slots in disjoint high ranges so executor/planner identities never
// collide with parse-time rtindex Vars. RTE-backed scan slots use a stable derived range while
// synthesized slots use an allocator-backed range.
const SYNTHETIC_SLOT_ID_BASE: usize = 1_000_000;
const RTE_SLOT_ID_BASE: usize = 2_000_000;

static NEXT_SYNTHETIC_SLOT_ID: AtomicUsize = AtomicUsize::new(SYNTHETIC_SLOT_ID_BASE);

pub(crate) fn next_synthetic_slot_id() -> usize {
    NEXT_SYNTHETIC_SLOT_ID.fetch_add(1, Ordering::Relaxed)
}

pub(crate) fn is_synthetic_slot_id(slot_id: usize) -> bool {
    slot_id >= SYNTHETIC_SLOT_ID_BASE
}

pub(crate) fn rte_slot_id(rtindex: usize) -> usize {
    RTE_SLOT_ID_BASE + rtindex
}

pub(crate) fn rte_slot_varno(slot_id: usize) -> Option<usize> {
    if slot_id >= RTE_SLOT_ID_BASE {
        Some(slot_id - RTE_SLOT_ID_BASE)
    } else {
        None
    }
}

impl Path {
    pub fn into_plan(self) -> Plan {
        super::setrefs::create_plan_without_root(self)
    }

    pub fn plan_info(&self) -> PlanEstimate {
        match self {
            Self::Result { plan_info, .. }
            | Self::Append { plan_info, .. }
            | Self::SeqScan { plan_info, .. }
            | Self::IndexScan { plan_info, .. }
            | Self::BitmapIndexScan { plan_info, .. }
            | Self::BitmapHeapScan { plan_info, .. }
            | Self::Filter { plan_info, .. }
            | Self::NestedLoopJoin { plan_info, .. }
            | Self::HashJoin { plan_info, .. }
            | Self::MergeJoin { plan_info, .. }
            | Self::Projection { plan_info, .. }
            | Self::OrderBy { plan_info, .. }
            | Self::Limit { plan_info, .. }
            | Self::LockRows { plan_info, .. }
            | Self::Aggregate { plan_info, .. }
            | Self::WindowAgg { plan_info, .. }
            | Self::SubqueryScan { plan_info, .. }
            | Self::CteScan { plan_info, .. }
            | Self::WorkTableScan { plan_info, .. }
            | Self::RecursiveUnion { plan_info, .. }
            | Self::SetOp { plan_info, .. }
            | Self::Values { plan_info, .. }
            | Self::FunctionScan { plan_info, .. }
            | Self::ProjectSet { plan_info, .. } => *plan_info,
        }
    }

    pub fn columns(&self) -> Vec<QueryColumn> {
        match self {
            Self::Result { .. } => Vec::new(),
            Self::Append { desc, .. } => desc
                .columns
                .iter()
                .map(|c| QueryColumn {
                    name: c.name.clone(),
                    sql_type: c.sql_type,
                    wire_type_oid: None,
                })
                .collect(),
            Self::SeqScan { desc, .. } | Self::IndexScan { desc, .. } => desc
                .columns
                .iter()
                .map(|c| QueryColumn {
                    name: c.name.clone(),
                    sql_type: c.sql_type,
                    wire_type_oid: None,
                })
                .collect(),
            Self::BitmapIndexScan { .. } => Vec::new(),
            Self::BitmapHeapScan { desc, .. } => desc
                .columns
                .iter()
                .map(|c| QueryColumn {
                    name: c.name.clone(),
                    sql_type: c.sql_type,
                    wire_type_oid: None,
                })
                .collect(),
            Self::Filter { input, .. }
            | Self::OrderBy { input, .. }
            | Self::Limit { input, .. }
            | Self::LockRows { input, .. } => input.columns(),
            Self::Projection { targets, .. } => targets
                .iter()
                .map(|t| QueryColumn {
                    name: t.name.clone(),
                    sql_type: t.sql_type,
                    wire_type_oid: None,
                })
                .collect(),
            Self::Aggregate { output_columns, .. } => output_columns.clone(),
            Self::WindowAgg { output_columns, .. } => output_columns.clone(),
            Self::SubqueryScan { output_columns, .. } => output_columns.clone(),
            Self::CteScan { output_columns, .. } => output_columns.clone(),
            Self::WorkTableScan { output_columns, .. }
            | Self::RecursiveUnion { output_columns, .. }
            | Self::SetOp { output_columns, .. } => output_columns.clone(),
            Self::NestedLoopJoin { output_columns, .. }
            | Self::HashJoin { output_columns, .. }
            | Self::MergeJoin { output_columns, .. } => output_columns.clone(),
            Self::FunctionScan { call, .. } => call.output_columns().to_vec(),
            Self::Values { output_columns, .. } => output_columns.clone(),
            Self::ProjectSet { targets, .. } => targets
                .iter()
                .map(|target| match target {
                    ProjectSetTarget::Scalar(entry) => QueryColumn {
                        name: entry.name.clone(),
                        sql_type: entry.sql_type,
                        wire_type_oid: None,
                    },
                    ProjectSetTarget::Set { name, sql_type, .. } => QueryColumn {
                        name: name.clone(),
                        sql_type: *sql_type,
                        wire_type_oid: None,
                    },
                })
                .collect(),
        }
    }

    pub fn output_vars(&self) -> Vec<Expr> {
        match self {
            Self::Result { .. } => Vec::new(),
            Self::Append {
                source_id, desc, ..
            } => slot_output_vars(*source_id, &desc.columns, |column| column.sql_type),
            Self::SeqScan {
                source_id, desc, ..
            }
            | Self::IndexScan {
                source_id, desc, ..
            }
            | Self::BitmapHeapScan {
                source_id, desc, ..
            } => slot_output_vars(*source_id, &desc.columns, |column| column.sql_type),
            Self::BitmapIndexScan { .. } => Vec::new(),
            Self::Filter { input, .. }
            | Self::OrderBy { input, .. }
            | Self::Limit { input, .. }
            | Self::LockRows { input, .. } => input.output_vars(),
            Self::Projection {
                slot_id, targets, ..
            } => targets
                .iter()
                .enumerate()
                .map(|(index, target)| slot_var(*slot_id, user_attrno(index), target.sql_type))
                .collect(),
            Self::Aggregate {
                slot_id,
                group_by,
                accumulators,
                ..
            } => aggregate_output_vars(*slot_id, group_by, accumulators),
            Self::WindowAgg {
                slot_id,
                output_columns,
                ..
            } => slot_output_vars(*slot_id, output_columns, |column| column.sql_type),
            Self::Values {
                slot_id,
                output_columns,
                ..
            } => slot_output_vars(*slot_id, output_columns, |column| column.sql_type),
            Self::CteScan {
                slot_id,
                output_columns,
                ..
            } => slot_output_vars(*slot_id, output_columns, |column| column.sql_type),
            Self::WorkTableScan {
                slot_id,
                output_columns,
                ..
            }
            | Self::RecursiveUnion {
                slot_id,
                output_columns,
                ..
            }
            | Self::SetOp {
                slot_id,
                output_columns,
                ..
            } => slot_output_vars(*slot_id, output_columns, |column| column.sql_type),
            Self::FunctionScan { slot_id, call, .. } => {
                slot_output_vars(*slot_id, call.output_columns(), |column| column.sql_type)
            }
            Self::SubqueryScan {
                rtindex,
                output_columns,
                ..
            } => slot_output_vars(rte_slot_id(*rtindex), output_columns, |column| {
                column.sql_type
            }),
            Self::ProjectSet {
                slot_id, targets, ..
            } => targets
                .iter()
                .enumerate()
                .map(|(index, target)| match target {
                    ProjectSetTarget::Scalar(entry) => {
                        slot_var(*slot_id, user_attrno(index), entry.sql_type)
                    }
                    ProjectSetTarget::Set { sql_type, .. } => {
                        slot_var(*slot_id, user_attrno(index), *sql_type)
                    }
                })
                .collect(),
            Self::NestedLoopJoin {
                left, right, kind, ..
            }
            | Self::HashJoin {
                left, right, kind, ..
            }
            | Self::MergeJoin {
                left, right, kind, ..
            } => {
                let mut vars = left.output_vars();
                if !matches!(kind, JoinType::Semi | JoinType::Anti) {
                    vars.extend(right.output_vars());
                }
                vars
            }
        }
    }

    pub fn semantic_output_vars(&self) -> Vec<Expr> {
        self.semantic_output_target().exprs
    }

    pub fn output_target(&self) -> PathTarget {
        match self {
            Self::Filter { input, .. }
            | Self::OrderBy { input, .. }
            | Self::Limit { input, .. }
            | Self::LockRows { input, .. } => input.output_target(),
            Self::Projection {
                slot_id, targets, ..
            } => PathTarget::with_sortgrouprefs(
                targets
                    .iter()
                    .enumerate()
                    .map(|(index, target)| slot_var(*slot_id, user_attrno(index), target.sql_type))
                    .collect(),
                targets
                    .iter()
                    .map(|target| target.ressortgroupref)
                    .collect(),
            ),
            Self::WindowAgg {
                slot_id,
                output_columns,
                input,
                ..
            } => {
                let mut sortgrouprefs = input.output_target().sortgrouprefs;
                sortgrouprefs.resize(output_columns.len(), 0);
                PathTarget::with_sortgrouprefs(
                    slot_output_vars(*slot_id, output_columns, |column| column.sql_type),
                    sortgrouprefs,
                )
            }
            _ => PathTarget::new(self.output_vars()),
        }
    }

    pub fn semantic_output_target(&self) -> PathTarget {
        match self {
            Self::Result { pathtarget, .. }
            | Self::Append { pathtarget, .. }
            | Self::SeqScan { pathtarget, .. }
            | Self::IndexScan { pathtarget, .. }
            | Self::BitmapIndexScan { pathtarget, .. }
            | Self::BitmapHeapScan { pathtarget, .. }
            | Self::Filter { pathtarget, .. }
            | Self::NestedLoopJoin { pathtarget, .. }
            | Self::HashJoin { pathtarget, .. }
            | Self::MergeJoin { pathtarget, .. }
            | Self::Projection { pathtarget, .. }
            | Self::OrderBy { pathtarget, .. }
            | Self::Limit { pathtarget, .. }
            | Self::LockRows { pathtarget, .. }
            | Self::Aggregate { pathtarget, .. }
            | Self::WindowAgg { pathtarget, .. }
            | Self::Values { pathtarget, .. }
            | Self::FunctionScan { pathtarget, .. }
            | Self::SubqueryScan { pathtarget, .. }
            | Self::CteScan { pathtarget, .. }
            | Self::WorkTableScan { pathtarget, .. }
            | Self::RecursiveUnion { pathtarget, .. }
            | Self::SetOp { pathtarget, .. }
            | Self::ProjectSet { pathtarget, .. } => pathtarget.clone(),
        }
    }

    pub fn pathkeys(&self) -> Vec<PathKey> {
        match self {
            Self::Result { .. }
            | Self::Append { .. }
            | Self::SeqScan { .. }
            | Self::BitmapIndexScan { .. }
            | Self::BitmapHeapScan { .. }
            | Self::CteScan { .. }
            | Self::WorkTableScan { .. }
            | Self::RecursiveUnion { .. }
            | Self::SetOp { .. }
            | Self::Values { .. }
            | Self::FunctionScan { .. }
            | Self::ProjectSet { .. } => Vec::new(),
            Self::Aggregate {
                strategy, pathkeys, ..
            } if *strategy == AggregateStrategy::Sorted => pathkeys.clone(),
            Self::Aggregate { .. } => Vec::new(),
            Self::IndexScan { pathkeys, .. } => pathkeys.clone(),
            Self::SubqueryScan { pathkeys, .. } => pathkeys.clone(),
            Self::Filter { input, .. }
            | Self::Limit { input, .. }
            | Self::LockRows { input, .. } => input.pathkeys(),
            Self::Projection {
                slot_id,
                targets,
                input,
                ..
            } => project_pathkeys(*slot_id, input, targets, &input.pathkeys()),
            Self::WindowAgg { input, .. } => input.pathkeys(),
            Self::OrderBy { items, .. } => items
                .iter()
                .map(|item| PathKey {
                    expr: item.expr.clone(),
                    ressortgroupref: item.ressortgroupref,
                    descending: item.descending,
                    nulls_first: item.nulls_first,
                    collation_oid: item.collation_oid,
                })
                .collect(),
            Self::NestedLoopJoin { left, kind, .. }
                if matches!(
                    kind,
                    JoinType::Inner
                        | JoinType::Cross
                        | JoinType::Left
                        | JoinType::Semi
                        | JoinType::Anti
                ) =>
            {
                left.pathkeys()
            }
            Self::MergeJoin { left, kind, .. }
                if matches!(
                    kind,
                    JoinType::Inner | JoinType::Left | JoinType::Semi | JoinType::Anti
                ) =>
            {
                left.pathkeys()
            }
            Self::HashJoin { .. } => Vec::new(),
            Self::NestedLoopJoin { .. } | Self::MergeJoin { .. } => Vec::new(),
        }
    }
}

pub(super) fn layout_candidate_for_expr(
    _root: Option<&PlannerInfo>,
    expr: &Expr,
    layout: &[Expr],
) -> Option<Expr> {
    let expr_var = simple_var_key(expr);
    let stripped_expr = strip_binary_coercible_casts(expr);
    layout
        .iter()
        .find(|candidate| {
            expr_var.is_some_and(|key| simple_var_key(candidate) == Some(key))
                || strip_binary_coercible_casts(candidate) == stripped_expr
        })
        .cloned()
}

pub(super) fn lower_expr_to_path_output(
    root: Option<&PlannerInfo>,
    path: &Path,
    expr: Expr,
    ressortgroupref: usize,
) -> Option<Expr> {
    lower_expr_to_path_output_internal(root, path, &expr, ressortgroupref).or_else(|| {
        root.and_then(|root| {
            let translated = appendrel_expr_for_path(root, path, expr.clone());
            (translated != expr)
                .then_some(translated)
                .and_then(|translated| {
                    lower_expr_to_path_output_internal(
                        Some(root),
                        path,
                        &translated,
                        ressortgroupref,
                    )
                })
        })
    })
}

fn lower_expr_to_path_output_internal(
    root: Option<&PlannerInfo>,
    path: &Path,
    expr: &Expr,
    ressortgroupref: usize,
) -> Option<Expr> {
    match path {
        Path::Projection { input, targets, .. } => {
            if let Some(candidate) = projection_output_match(
                root,
                targets,
                &input.semantic_output_target(),
                expr,
                ressortgroupref,
            ) {
                return Some(candidate);
            }
        }
        Path::ProjectSet { input, targets, .. } => {
            if let Some(candidate) = project_set_output_match(
                root,
                targets,
                &input.semantic_output_target(),
                expr,
                ressortgroupref,
            ) {
                return Some(candidate);
            }
        }
        _ => {}
    }
    let output_target = path.output_target();
    IndexedPathTarget::new(&output_target)
        .matched_expr(expr, ressortgroupref)
        .or_else(|| layout_candidate_for_expr(root, expr, &output_target.exprs))
}

fn projection_output_match(
    _root: Option<&PlannerInfo>,
    targets: &[TargetEntry],
    input_target: &PathTarget,
    expr: &Expr,
    ressortgroupref: usize,
) -> Option<Expr> {
    let target_pathtarget = PathTarget::from_target_list(targets);
    let indexed_targets = IndexedPathTarget::new(&target_pathtarget);
    let indexed_input = IndexedPathTarget::new(input_target);
    (ressortgroupref != 0)
        .then(|| {
            indexed_targets
                .index_for_sortgroupref(ressortgroupref)
                .and_then(|index| targets.get(index))
                .map(|target| target.expr.clone())
        })
        .flatten()
        .or_else(|| {
            indexed_input
                .match_index(expr, 0)
                .and_then(|index| {
                    targets
                        .iter()
                        .find(|target| target.input_resno == Some(index + 1))
                })
                .map(|target| target.expr.clone())
        })
        .or_else(|| {
            indexed_targets
                .match_index(expr, ressortgroupref)
                .and_then(|index| targets.get(index))
                .map(|target| target.expr.clone())
        })
}

fn project_set_output_match(
    root: Option<&PlannerInfo>,
    targets: &[ProjectSetTarget],
    input_target: &PathTarget,
    expr: &Expr,
    ressortgroupref: usize,
) -> Option<Expr> {
    targets.iter().find_map(|target| match target {
        ProjectSetTarget::Scalar(entry) => projection_output_match(
            root,
            std::slice::from_ref(entry),
            input_target,
            expr,
            ressortgroupref,
        ),
        ProjectSetTarget::Set { source_expr, .. } if source_expr == expr => {
            Some(source_expr.clone())
        }
        ProjectSetTarget::Set { .. } => None,
    })
}

fn appendrel_expr_for_path(root: &PlannerInfo, path: &Path, expr: Expr) -> Expr {
    let relids = super::path_relids(path);
    if relids.len() != 1 {
        return expr;
    }
    append_translation(root, relids[0])
        .map(|info| translate_append_rel_expr(expr.clone(), info))
        .unwrap_or(expr)
}

pub(super) fn exprs_match_for_path_layout(
    _root: Option<&PlannerInfo>,
    left: &Expr,
    right: &Expr,
) -> bool {
    simple_var_key(left)
        .zip(simple_var_key(right))
        .is_some_and(|(left, right)| left == right)
        || normalize_expr_for_path_layout(None, left) == normalize_expr_for_path_layout(None, right)
}

pub(super) fn normalize_expr_for_path_layout(_root: Option<&PlannerInfo>, expr: &Expr) -> Expr {
    strip_binary_coercible_casts(expr)
}

pub(crate) fn slot_output_target<T>(
    varno: usize,
    columns: &[T],
    sql_type: impl Fn(&T) -> SqlType,
) -> PathTarget {
    PathTarget::new(slot_output_vars(varno, columns, sql_type))
}

fn slot_output_vars<T>(
    slot_id: usize,
    columns: &[T],
    sql_type: impl Fn(&T) -> SqlType,
) -> Vec<Expr> {
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| slot_var(slot_id, user_attrno(index), sql_type(column)))
        .collect()
}

fn slot_var(
    slot_id: usize,
    attno: crate::include::nodes::primnodes::AttrNumber,
    vartype: SqlType,
) -> Expr {
    Expr::Var(Var {
        varno: slot_id,
        varattno: attno,
        varlevelsup: 0,
        vartype,
    })
}

fn project_pathkeys(
    slot_id: usize,
    input: &Path,
    targets: &[TargetEntry],
    input_pathkeys: &[PathKey],
) -> Vec<PathKey> {
    let input_target = input.semantic_output_target();
    input_pathkeys
        .iter()
        .map(|key| {
            let expr = targets
                .iter()
                .enumerate()
                .find(|(_, target)| {
                    key.ressortgroupref != 0 && target.ressortgroupref == key.ressortgroupref
                })
                .map(|(index, target)| {
                    if target.input_resno.is_some() {
                        target.expr.clone()
                    } else {
                        slot_var(slot_id, user_attrno(index), target.sql_type)
                    }
                })
                .or_else(|| {
                    input_target
                        .exprs
                        .iter()
                        .position(|expr| *expr == key.expr)
                        .and_then(|input_index| {
                            targets
                                .iter()
                                .find(|target| target.input_resno == Some(input_index + 1))
                                .map(|target| target.expr.clone())
                        })
                })
                .or_else(|| {
                    targets
                        .iter()
                        .find(|target| target.expr == key.expr)
                        .map(|target| target.expr.clone())
                })
                .unwrap_or_else(|| key.expr.clone());
            PathKey {
                expr,
                ressortgroupref: key.ressortgroupref,
                descending: key.descending,
                nulls_first: key.nulls_first,
                collation_oid: key.collation_oid,
            }
        })
        .collect()
}

fn aggregate_output_expr(accum: &AggAccum, aggno: usize) -> Expr {
    Expr::Aggref(Box::new(Aggref {
        aggfnoid: accum.aggfnoid,
        aggtype: accum.sql_type,
        aggvariadic: accum.agg_variadic,
        aggdistinct: accum.distinct,
        direct_args: accum.direct_args.clone(),
        args: accum.args.clone(),
        aggorder: accum.order_by.clone(),
        aggfilter: accum.filter.clone(),
        agglevelsup: 0,
        aggno,
    }))
}

fn aggregate_output_target(group_by: &[Expr], accumulators: &[AggAccum]) -> PathTarget {
    let mut exprs = Vec::with_capacity(group_by.len() + accumulators.len());
    exprs.extend(group_by.iter().cloned());
    exprs.extend(
        accumulators
            .iter()
            .enumerate()
            .map(|(aggno, accum)| aggregate_output_expr(accum, aggno)),
    );
    PathTarget::new(exprs)
}

fn project_set_output_target(targets: &[ProjectSetTarget]) -> PathTarget {
    PathTarget::with_sortgrouprefs(
        targets
            .iter()
            .map(|target| match target {
                ProjectSetTarget::Scalar(entry) => entry.expr.clone(),
                ProjectSetTarget::Set { source_expr, .. } => source_expr.clone(),
            })
            .collect(),
        targets
            .iter()
            .map(|target| match target {
                ProjectSetTarget::Scalar(entry) => entry.ressortgroupref,
                ProjectSetTarget::Set { .. } => 0,
            })
            .collect(),
    )
}

pub(super) fn window_output_columns(input: &Path, clause: &WindowClause) -> Vec<QueryColumn> {
    let mut output_columns = input.columns();
    output_columns.extend(clause.functions.iter().map(|func| QueryColumn {
        name: format!("win{}", func.winno + 1),
        sql_type: func.result_type,
        wire_type_oid: None,
    }));
    output_columns
}

fn window_semantic_output_target(input: &Path, clause: &WindowClause) -> PathTarget {
    let mut exprs = input.semantic_output_vars();
    let mut sortgrouprefs = input.semantic_output_target().sortgrouprefs;
    for func in &clause.functions {
        exprs.push(Expr::WindowFunc(Box::new(func.clone())));
        sortgrouprefs.push(0);
    }
    PathTarget::with_sortgrouprefs(exprs, sortgrouprefs)
}

pub(super) fn aggregate_output_vars(
    slot_id: usize,
    group_by: &[Expr],
    accumulators: &[AggAccum],
) -> Vec<Expr> {
    let mut vars = Vec::with_capacity(group_by.len() + accumulators.len());
    for (index, expr) in group_by.iter().enumerate() {
        vars.push(slot_var(slot_id, user_attrno(index), expr_sql_type(expr)));
    }
    for (index, accum) in accumulators.iter().enumerate() {
        vars.push(slot_var(
            slot_id,
            user_attrno(group_by.len() + index),
            accum.sql_type,
        ));
    }
    vars
}

pub(super) fn lower_agg_output_expr(
    expr: Expr,
    group_by: &[Expr],
    agg_output_layout: &[Expr],
) -> Expr {
    if let Some(index) = group_by.iter().position(|group_expr| *group_expr == expr) {
        return agg_output_layout[index].clone();
    }
    match expr {
        Expr::Aggref(aggref) => agg_output_layout
            .get(group_by.len() + aggref.aggno)
            .cloned()
            .unwrap_or_else(|| panic!("aggregate output slot {} missing", aggref.aggno)),
        Expr::Op(op) => Expr::Op(Box::new(OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| lower_agg_output_expr(arg, group_by, agg_output_layout))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| lower_agg_output_expr(arg, group_by, agg_output_layout))
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| lower_agg_output_expr(arg, group_by, agg_output_layout))
                .collect(),
            ..*func
        })),
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            left: Box::new(lower_agg_output_expr(
                *saop.left,
                group_by,
                agg_output_layout,
            )),
            right: Box::new(lower_agg_output_expr(
                *saop.right,
                group_by,
                agg_output_layout,
            )),
            ..*saop
        })),
        Expr::Cast(inner, ty) => Expr::Cast(
            Box::new(lower_agg_output_expr(*inner, group_by, agg_output_layout)),
            ty,
        ),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
            collation_oid,
        } => Expr::Like {
            expr: Box::new(lower_agg_output_expr(*expr, group_by, agg_output_layout)),
            pattern: Box::new(lower_agg_output_expr(*pattern, group_by, agg_output_layout)),
            escape: escape
                .map(|expr| Box::new(lower_agg_output_expr(*expr, group_by, agg_output_layout))),
            case_insensitive,
            negated,
            collation_oid,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
            collation_oid,
        } => Expr::Similar {
            expr: Box::new(lower_agg_output_expr(*expr, group_by, agg_output_layout)),
            pattern: Box::new(lower_agg_output_expr(*pattern, group_by, agg_output_layout)),
            escape: escape
                .map(|expr| Box::new(lower_agg_output_expr(*expr, group_by, agg_output_layout))),
            negated,
            collation_oid,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(lower_agg_output_expr(
            *inner,
            group_by,
            agg_output_layout,
        ))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(lower_agg_output_expr(
            *inner,
            group_by,
            agg_output_layout,
        ))),
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(lower_agg_output_expr(*left, group_by, agg_output_layout)),
            Box::new(lower_agg_output_expr(*right, group_by, agg_output_layout)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(lower_agg_output_expr(*left, group_by, agg_output_layout)),
            Box::new(lower_agg_output_expr(*right, group_by, agg_output_layout)),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| lower_agg_output_expr(element, group_by, agg_output_layout))
                .collect(),
            array_type,
        },
        Expr::SubLink(sublink) => {
            Expr::SubLink(Box::new(crate::include::nodes::primnodes::SubLink {
                testexpr: sublink.testexpr.map(|expr| {
                    Box::new(lower_agg_output_expr(*expr, group_by, agg_output_layout))
                }),
                ..*sublink
            }))
        }
        Expr::SubPlan(subplan) => {
            Expr::SubPlan(Box::new(crate::include::nodes::primnodes::SubPlan {
                testexpr: subplan.testexpr.map(|expr| {
                    Box::new(lower_agg_output_expr(*expr, group_by, agg_output_layout))
                }),
                ..*subplan
            }))
        }
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(lower_agg_output_expr(*left, group_by, agg_output_layout)),
            Box::new(lower_agg_output_expr(*right, group_by, agg_output_layout)),
        ),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(lower_agg_output_expr(*array, group_by, agg_output_layout)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript
                        .lower
                        .map(|expr| lower_agg_output_expr(expr, group_by, agg_output_layout)),
                    upper: subscript
                        .upper
                        .map(|expr| lower_agg_output_expr(expr, group_by, agg_output_layout)),
                })
                .collect(),
        },
        other => other,
    }
}

pub(super) fn expr_sql_type(expr: &Expr) -> SqlType {
    match expr {
        Expr::Var(var) => var.vartype,
        Expr::Param(param) => param.paramtype,
        Expr::Aggref(aggref) => aggref.aggtype,
        Expr::WindowFunc(window_func) => window_func.result_type,
        Expr::Op(op) => op.opresulttype,
        Expr::Func(func) => func
            .funcresulttype
            .unwrap_or(SqlType::new(SqlTypeKind::Text)),
        Expr::SetReturning(srf) => srf.sql_type,
        Expr::Bool(_)
        | Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::IsDistinctFrom(_, _)
        | Expr::IsNotDistinctFrom(_, _)
        | Expr::Like { .. }
        | Expr::Similar { .. }
        | Expr::ScalarArrayOp(_) => SqlType::new(SqlTypeKind::Bool),
        Expr::Cast(_, ty) => *ty,
        Expr::Collate { expr, .. } => expr_sql_type(expr),
        Expr::ArrayLiteral { array_type, .. } => *array_type,
        Expr::Row { descriptor, .. } => descriptor.sql_type(),
        Expr::FieldSelect { field_type, .. } => *field_type,
        Expr::Coalesce(left, right) => expr_sql_type_maybe(left)
            .or_else(|| expr_sql_type_maybe(right))
            .unwrap_or(SqlType::new(SqlTypeKind::Text)),
        Expr::Case(case_expr) => case_expr.casetype,
        Expr::CaseTest(case_test) => case_test.type_id,
        Expr::SubLink(sublink) => match sublink.sublink_type {
            SubLinkType::ExistsSubLink
            | SubLinkType::AnySubLink(_)
            | SubLinkType::AllSubLink(_) => SqlType::new(SqlTypeKind::Bool),
            SubLinkType::ArraySubLink => SqlType::array_of(
                sublink
                    .subselect
                    .target_list
                    .first()
                    .map(|target| target.sql_type)
                    .unwrap_or(SqlType::new(SqlTypeKind::Text)),
            ),
            SubLinkType::ExprSubLink => sublink
                .subselect
                .target_list
                .first()
                .map(|target| target.sql_type)
                .unwrap_or(SqlType::new(SqlTypeKind::Text)),
        },
        Expr::SubPlan(subplan) => match subplan.sublink_type {
            SubLinkType::ExistsSubLink
            | SubLinkType::AnySubLink(_)
            | SubLinkType::AllSubLink(_) => SqlType::new(SqlTypeKind::Bool),
            SubLinkType::ArraySubLink => SqlType::array_of(
                subplan
                    .first_col_type
                    .unwrap_or(SqlType::new(SqlTypeKind::Text)),
            ),
            SubLinkType::ExprSubLink => subplan
                .first_col_type
                .unwrap_or(SqlType::new(SqlTypeKind::Text)),
        },
        Expr::Const(value) => value_sql_type_hint(value),
        Expr::Random => SqlType::new(SqlTypeKind::Float8),
        Expr::CurrentDate => SqlType::new(SqlTypeKind::Date),
        Expr::CurrentUser | Expr::SessionUser | Expr::CurrentRole => {
            SqlType::new(SqlTypeKind::Name)
        }
        Expr::CurrentTime { .. } => SqlType::new(SqlTypeKind::TimeTz),
        Expr::CurrentTimestamp { .. } => SqlType::new(SqlTypeKind::TimestampTz),
        Expr::LocalTime { .. } => SqlType::new(SqlTypeKind::Time),
        Expr::LocalTimestamp { .. } => SqlType::new(SqlTypeKind::Timestamp),
        Expr::Xml(_) | Expr::ArraySubscript { .. } => {
            crate::include::nodes::primnodes::expr_sql_type_hint(expr)
                .unwrap_or(SqlType::new(SqlTypeKind::Text))
        }
    }
}

fn expr_sql_type_maybe(expr: &Expr) -> Option<SqlType> {
    match expr {
        Expr::Param(param) => Some(param.paramtype),
        other => crate::include::nodes::primnodes::expr_sql_type_hint(other)
            .or_else(|| Some(expr_sql_type(other))),
    }
}

fn value_sql_type_hint(value: &Value) -> SqlType {
    value
        .sql_type_hint()
        .unwrap_or(SqlType::new(SqlTypeKind::Text))
}
