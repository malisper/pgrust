use std::sync::atomic::{AtomicUsize, Ordering};

use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::{JoinTreeNode, Query, RangeTblEntry, RangeTblEntryKind};
use crate::include::nodes::pathnodes::PlannerPath;
use crate::include::nodes::plannodes::{Plan, PlanEstimate};
use crate::include::nodes::primnodes::{
    AggAccum, Aggref, BoolExpr, Expr, ExprArraySubscript, FuncExpr, OpExpr, OrderByEntry,
    ProjectSetTarget, QueryColumn, ScalarArrayOpExpr, SetReturningCall, SubLinkType, TargetEntry,
    Var,
};

struct PlannerPathBuilder {
    next_slot_id: usize,
}

static NEXT_SYNTHETIC_SLOT_ID: AtomicUsize = AtomicUsize::new(1);

pub(crate) fn next_synthetic_slot_id() -> usize {
    NEXT_SYNTHETIC_SLOT_ID.fetch_add(1, Ordering::Relaxed)
}

impl PlannerPath {
    pub fn from_query(query: Query) -> Self {
        let next_slot_id = query.rtable.len() + 1;
        PlannerPathBuilder { next_slot_id }.from_query(query)
    }

    pub fn into_plan(self) -> Plan {
        match self {
            Self::Result { plan_info } => Plan::Result { plan_info },
            Self::SeqScan {
                plan_info,
                source_id: _,
                rel,
                relation_oid,
                toast,
                desc,
            } => Plan::SeqScan {
                plan_info,
                rel,
                relation_oid,
                toast,
                desc,
            },
            Self::IndexScan {
                plan_info,
                source_id: _,
                rel,
                index_rel,
                am_oid,
                toast,
                desc,
                index_meta,
                keys,
                direction,
            } => Plan::IndexScan {
                plan_info,
                rel,
                index_rel,
                am_oid,
                toast,
                desc,
                index_meta,
                keys,
                direction,
            },
            Self::Filter {
                plan_info,
                input,
                predicate,
            } => {
                let layout = input.output_vars();
                Plan::Filter {
                    plan_info,
                    input: Box::new(input.into_plan()),
                    predicate: lower_expr_to_plan_layout(predicate, &layout),
                }
            }
            Self::NestedLoopJoin {
                plan_info,
                left,
                right,
                kind,
                on,
            } => {
                let mut layout = left.output_vars();
                layout.extend(right.output_vars());
                Plan::NestedLoopJoin {
                    plan_info,
                    left: Box::new(left.into_plan()),
                    right: Box::new(right.into_plan()),
                    kind,
                    on: lower_expr_to_plan_layout(on, &layout),
                }
            }
            Self::Projection {
                plan_info,
                input,
                targets,
                ..
            } => {
                let layout = input.output_vars();
                Plan::Projection {
                    plan_info,
                    input: Box::new(input.into_plan()),
                    targets: targets
                        .into_iter()
                        .map(|target| lower_target_entry_to_plan_layout(target, &layout))
                        .collect(),
                }
            }
            Self::OrderBy {
                plan_info,
                input,
                items,
            } => {
                let layout = input.output_vars();
                Plan::OrderBy {
                    plan_info,
                    input: Box::new(input.into_plan()),
                    items: items
                        .into_iter()
                        .map(|item| lower_order_by_entry_to_plan_layout(item, &layout))
                        .collect(),
                }
            }
            Self::Limit {
                plan_info,
                input,
                limit,
                offset,
            } => Plan::Limit {
                plan_info,
                input: Box::new(input.into_plan()),
                limit,
                offset,
            },
            Self::Aggregate {
                plan_info,
                slot_id,
                input,
                group_by,
                accumulators,
                having,
                output_columns,
                ..
            } => {
                let layout = input.output_vars();
                let aggregate_layout = aggregate_output_vars(slot_id, &group_by, &accumulators);
                Plan::Aggregate {
                    plan_info,
                    input: Box::new(input.into_plan()),
                    group_by: group_by
                        .into_iter()
                        .map(|expr| lower_expr_to_plan_layout(expr, &layout))
                        .collect(),
                    accumulators: accumulators
                        .into_iter()
                        .map(|accum| lower_agg_accum_to_plan_layout(accum, &layout))
                        .collect(),
                    having: having.map(|expr| lower_expr_to_plan_layout(expr, &aggregate_layout)),
                    output_columns,
                }
            }
            Self::Values {
                plan_info,
                rows,
                output_columns,
                ..
            } => Plan::Values {
                plan_info,
                rows: rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|expr| lower_expr_to_plan_layout(expr, &[]))
                            .collect()
                    })
                    .collect(),
                output_columns,
            },
            Self::FunctionScan {
                plan_info, call, ..
            } => Plan::FunctionScan {
                plan_info,
                call: lower_set_returning_call_to_plan_layout(call, &[]),
            },
            Self::ProjectSet {
                plan_info,
                input,
                targets,
                ..
            } => {
                let layout = input.output_vars();
                Plan::ProjectSet {
                    plan_info,
                    input: Box::new(input.into_plan()),
                    targets: targets
                        .into_iter()
                        .map(|target| lower_project_set_target_to_plan_layout(target, &layout))
                        .collect(),
                }
            }
        }
    }

    pub fn plan_info(&self) -> PlanEstimate {
        match self {
            Self::Result { plan_info }
            | Self::SeqScan { plan_info, .. }
            | Self::IndexScan { plan_info, .. }
            | Self::Filter { plan_info, .. }
            | Self::NestedLoopJoin { plan_info, .. }
            | Self::Projection { plan_info, .. }
            | Self::OrderBy { plan_info, .. }
            | Self::Limit { plan_info, .. }
            | Self::Aggregate { plan_info, .. }
            | Self::Values { plan_info, .. }
            | Self::FunctionScan { plan_info, .. }
            | Self::ProjectSet { plan_info, .. } => *plan_info,
        }
    }

    pub fn columns(&self) -> Vec<QueryColumn> {
        match self {
            Self::Result { .. } => Vec::new(),
            Self::SeqScan { desc, .. } | Self::IndexScan { desc, .. } => desc
                .columns
                .iter()
                .map(|c| QueryColumn {
                    name: c.name.clone(),
                    sql_type: c.sql_type,
                })
                .collect(),
            Self::Filter { input, .. }
            | Self::OrderBy { input, .. }
            | Self::Limit { input, .. } => input.columns(),
            Self::Projection { targets, .. } => targets
                .iter()
                .map(|t| QueryColumn {
                    name: t.name.clone(),
                    sql_type: t.sql_type,
                })
                .collect(),
            Self::Aggregate { output_columns, .. } => output_columns.clone(),
            Self::NestedLoopJoin { left, right, .. } => {
                let mut cols = left.columns();
                cols.extend(right.columns());
                cols
            }
            Self::FunctionScan { call, .. } => call.output_columns().to_vec(),
            Self::Values { output_columns, .. } => output_columns.clone(),
            Self::ProjectSet { targets, .. } => targets
                .iter()
                .map(|target| match target {
                    ProjectSetTarget::Scalar(entry) => QueryColumn {
                        name: entry.name.clone(),
                        sql_type: entry.sql_type,
                    },
                    ProjectSetTarget::Set { name, sql_type, .. } => QueryColumn {
                        name: name.clone(),
                        sql_type: *sql_type,
                    },
                })
                .collect(),
        }
    }

    pub fn output_vars(&self) -> Vec<Expr> {
        match self {
            Self::Result { .. } => Vec::new(),
            Self::SeqScan {
                source_id, desc, ..
            }
            | Self::IndexScan {
                source_id, desc, ..
            } => slot_output_vars(*source_id, &desc.columns, |column| column.sql_type),
            Self::Filter { input, .. }
            | Self::OrderBy { input, .. }
            | Self::Limit { input, .. } => input.output_vars(),
            Self::Projection {
                slot_id, targets, ..
            } => targets
                .iter()
                .enumerate()
                .map(|(index, target)| slot_var(*slot_id, index + 1, target.sql_type))
                .collect(),
            Self::Aggregate {
                slot_id,
                group_by,
                accumulators,
                ..
            } => aggregate_output_vars(*slot_id, group_by, accumulators),
            Self::Values {
                slot_id,
                output_columns,
                ..
            } => slot_output_vars(*slot_id, output_columns, |column| column.sql_type),
            Self::FunctionScan { slot_id, call, .. } => {
                slot_output_vars(*slot_id, call.output_columns(), |column| column.sql_type)
            }
            Self::ProjectSet {
                slot_id, targets, ..
            } => targets
                .iter()
                .enumerate()
                .map(|(index, target)| match target {
                    ProjectSetTarget::Scalar(entry) => {
                        slot_var(*slot_id, index + 1, entry.sql_type)
                    }
                    ProjectSetTarget::Set { sql_type, .. } => {
                        slot_var(*slot_id, index + 1, *sql_type)
                    }
                })
                .collect(),
            Self::NestedLoopJoin { left, right, .. } => {
                let mut vars = left.output_vars();
                vars.extend(right.output_vars());
                vars
            }
        }
    }
}

impl PlannerPathBuilder {
    fn alloc_slot_id(&mut self) -> usize {
        let id = self.next_slot_id;
        self.next_slot_id += 1;
        id
    }

    fn from_query(&mut self, query: Query) -> PlannerPath {
        let mut plan = match query.jointree {
            Some(jointree) => self.from_jointree(jointree, query.rtable),
            None => PlannerPath::Result {
                plan_info: PlanEstimate::default(),
            },
        };

        if let Some(predicate) = query.where_qual {
            let layout = plan.output_vars();
            plan = PlannerPath::Filter {
                plan_info: PlanEstimate::default(),
                input: Box::new(plan),
                predicate: rewrite_expr_against_layout(predicate, &layout),
            };
        }

        let has_agg = !query.group_by.is_empty()
            || !query.accumulators.is_empty()
            || query.having_qual.is_some();
        if has_agg {
            let input_layout = plan.output_vars();
            let group_by = query
                .group_by
                .iter()
                .cloned()
                .map(|expr| rewrite_expr_against_layout(expr, &input_layout))
                .collect::<Vec<_>>();
            let slot_id = self.alloc_slot_id();
            let agg_output_layout = aggregate_output_vars(slot_id, &group_by, &query.accumulators);
            let lowered_targets = query
                .target_list
                .into_iter()
                .map(|target| TargetEntry {
                    expr: lower_agg_output_expr(
                        rewrite_expr_against_layout(target.expr, &input_layout),
                        &group_by,
                        &agg_output_layout,
                    ),
                    ..target
                })
                .collect::<Vec<_>>();
            let lowered_sort_clause = query
                .sort_clause
                .into_iter()
                .map(|item| OrderByEntry {
                    expr: lower_agg_output_expr(
                        rewrite_expr_against_layout(item.expr, &input_layout),
                        &group_by,
                        &agg_output_layout,
                    ),
                    descending: item.descending,
                    nulls_first: item.nulls_first,
                })
                .collect::<Vec<_>>();
            let lowered_having = query.having_qual.map(|expr| {
                lower_agg_output_expr(
                    rewrite_expr_against_layout(expr, &input_layout),
                    &group_by,
                    &agg_output_layout,
                )
            });
            let output_columns = lowered_targets
                .iter()
                .map(|target| QueryColumn {
                    name: target.name.clone(),
                    sql_type: target.sql_type,
                })
                .collect();
            plan = PlannerPath::Aggregate {
                plan_info: PlanEstimate::default(),
                slot_id,
                input: Box::new(plan),
                group_by,
                accumulators: query.accumulators,
                having: lowered_having,
                output_columns,
            };
            if !lowered_sort_clause.is_empty() {
                plan = PlannerPath::OrderBy {
                    plan_info: PlanEstimate::default(),
                    input: Box::new(plan),
                    items: lowered_sort_clause,
                };
            }
            if query.limit_count.is_some() || query.limit_offset != 0 {
                plan = PlannerPath::Limit {
                    plan_info: PlanEstimate::default(),
                    input: Box::new(plan),
                    limit: query.limit_count,
                    offset: query.limit_offset,
                };
            }
            return self.add_projection(plan, lowered_targets);
        }

        let project_set_targets = query.project_set;
        let projection_targets = query.target_list;
        let projection_before_sort = project_set_targets.is_some();

        if let Some(targets) = project_set_targets {
            let layout = plan.output_vars();
            plan = PlannerPath::ProjectSet {
                plan_info: PlanEstimate::default(),
                slot_id: self.alloc_slot_id(),
                input: Box::new(plan),
                targets: targets
                    .into_iter()
                    .map(|target| rewrite_project_set_target_against_layout(target, &layout))
                    .collect(),
            };
            plan = self.add_projection(plan, projection_targets.clone());
        }

        if !query.sort_clause.is_empty() {
            let layout = plan.output_vars();
            plan = PlannerPath::OrderBy {
                plan_info: PlanEstimate::default(),
                input: Box::new(plan),
                items: query
                    .sort_clause
                    .into_iter()
                    .map(|item| OrderByEntry {
                        expr: rewrite_expr_against_layout(item.expr, &layout),
                        descending: item.descending,
                        nulls_first: item.nulls_first,
                    })
                    .collect(),
            };
        }

        if query.limit_count.is_some() || query.limit_offset != 0 {
            plan = PlannerPath::Limit {
                plan_info: PlanEstimate::default(),
                input: Box::new(plan),
                limit: query.limit_count,
                offset: query.limit_offset,
            };
        }

        if !projection_before_sort {
            plan = self.maybe_add_projection(plan, projection_targets);
        }

        plan
    }

    fn add_projection(&mut self, input: PlannerPath, targets: Vec<TargetEntry>) -> PlannerPath {
        let layout = input.output_vars();
        PlannerPath::Projection {
            plan_info: PlanEstimate::default(),
            slot_id: self.alloc_slot_id(),
            input: Box::new(input),
            targets: targets
                .into_iter()
                .map(|target| rewrite_target_entry_against_layout(target, &layout))
                .collect(),
        }
    }

    fn maybe_add_projection(
        &mut self,
        input: PlannerPath,
        targets: Vec<TargetEntry>,
    ) -> PlannerPath {
        let input_columns = input.columns();
        let layout = input.output_vars();
        let is_identity = targets.len() == input_columns.len()
            && targets.iter().enumerate().all(|(index, target)| {
                rewrite_expr_against_layout(target.expr.clone(), &layout) == layout[index]
                    && target.name == input_columns[index].name
            });
        if is_identity {
            input
        } else {
            self.add_projection(input, targets)
        }
    }

    fn from_jointree(&mut self, jointree: JoinTreeNode, rtable: Vec<RangeTblEntry>) -> PlannerPath {
        match jointree {
            JoinTreeNode::RangeTblRef(rtindex) => {
                let rte = rtable
                    .get(rtindex.saturating_sub(1))
                    .cloned()
                    .expect("range table entry for rtindex");
                match rte.kind {
                    RangeTblEntryKind::Result => PlannerPath::Result {
                        plan_info: PlanEstimate::default(),
                    },
                    RangeTblEntryKind::Relation {
                        rel,
                        relation_oid,
                        relkind: _,
                        toast,
                    } => PlannerPath::SeqScan {
                        plan_info: PlanEstimate::default(),
                        source_id: rtindex,
                        rel,
                        relation_oid,
                        toast,
                        desc: rte.desc,
                    },
                    RangeTblEntryKind::Values {
                        rows,
                        output_columns,
                    } => PlannerPath::Values {
                        plan_info: PlanEstimate::default(),
                        slot_id: rtindex,
                        rows: rows
                            .into_iter()
                            .map(|row| {
                                row.into_iter()
                                    .map(|expr| rewrite_expr_against_layout(expr, &[]))
                                    .collect()
                            })
                            .collect(),
                        output_columns,
                    },
                    RangeTblEntryKind::Function { call } => PlannerPath::FunctionScan {
                        plan_info: PlanEstimate::default(),
                        slot_id: rtindex,
                        call: rewrite_set_returning_call_against_layout(call, &[]),
                    },
                    RangeTblEntryKind::Subquery { query } => {
                        let output_columns = query.columns();
                        let input = self.from_query(*query);
                        let layout = input.output_vars();
                        PlannerPath::Projection {
                            plan_info: PlanEstimate::default(),
                            slot_id: rtindex,
                            input: Box::new(input),
                            targets: output_columns
                                .into_iter()
                                .enumerate()
                                .map(|(index, column)| {
                                    TargetEntry::new(
                                        column.name,
                                        layout.get(index).cloned().unwrap_or_else(|| {
                                            slot_var(rtindex, index + 1, column.sql_type)
                                        }),
                                        column.sql_type,
                                        index + 1,
                                    )
                                })
                                .collect(),
                        }
                    }
                    RangeTblEntryKind::Join { .. } => unreachable!(
                        "join RTEs are referenced through JoinExpr nodes, not bare RangeTblRef"
                    ),
                }
            }
            JoinTreeNode::JoinExpr {
                left,
                right,
                kind,
                quals,
                rtindex,
            } => {
                let join_rte = rtable.get(rtindex.saturating_sub(1)).cloned();
                let left = self.from_jointree(*left, rtable.clone());
                let right = self.from_jointree(*right, rtable);
                let mut layout = left.output_vars();
                layout.extend(right.output_vars());
                let join = PlannerPath::NestedLoopJoin {
                    plan_info: PlanEstimate::default(),
                    left: Box::new(left),
                    right: Box::new(right),
                    kind,
                    on: rewrite_expr_against_layout(quals, &layout),
                };
                let Some(rte) = join_rte else {
                    return join;
                };
                let RangeTblEntryKind::Join { joinaliasvars, .. } = &rte.kind else {
                    return join;
                };
                if join_alias_vars_match_layout(joinaliasvars, &layout) {
                    return join;
                }
                PlannerPath::Projection {
                    plan_info: PlanEstimate::default(),
                    slot_id: rtindex,
                    input: Box::new(join),
                    targets: joinaliasvars
                        .iter()
                        .enumerate()
                        .map(|(index, expr)| {
                            TargetEntry::new(
                                rte.desc.columns[index].name.clone(),
                                rewrite_expr_against_layout(expr.clone(), &layout),
                                rte.desc.columns[index].sql_type,
                                index + 1,
                            )
                        })
                        .collect(),
                }
            }
        }
    }
}

fn slot_output_vars<T>(
    slot_id: usize,
    columns: &[T],
    sql_type: impl Fn(&T) -> SqlType,
) -> Vec<Expr> {
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| slot_var(slot_id, index + 1, sql_type(column)))
        .collect()
}

fn slot_var(slot_id: usize, attno: usize, vartype: SqlType) -> Expr {
    Expr::Var(Var {
        varno: slot_id,
        varattno: attno,
        varlevelsup: 0,
        vartype,
    })
}

fn aggregate_output_vars(
    slot_id: usize,
    group_by: &[Expr],
    accumulators: &[AggAccum],
) -> Vec<Expr> {
    let mut vars = Vec::with_capacity(group_by.len() + accumulators.len());
    for (index, expr) in group_by.iter().enumerate() {
        vars.push(slot_var(slot_id, index + 1, expr_sql_type(expr)));
    }
    for (index, accum) in accumulators.iter().enumerate() {
        vars.push(slot_var(
            slot_id,
            group_by.len() + index + 1,
            accum.sql_type,
        ));
    }
    vars
}

fn join_alias_vars_match_layout(joinaliasvars: &[Expr], layout: &[Expr]) -> bool {
    joinaliasvars.len() == layout.len()
        && joinaliasvars
            .iter()
            .cloned()
            .map(|expr| rewrite_expr_against_layout(expr, layout))
            .zip(layout.iter())
            .all(|(expr, expected)| expr == *expected)
}

fn rewrite_target_entry_against_layout(target: TargetEntry, layout: &[Expr]) -> TargetEntry {
    TargetEntry {
        expr: rewrite_expr_against_layout(target.expr, layout),
        ..target
    }
}

fn lower_target_entry_to_plan_layout(target: TargetEntry, layout: &[Expr]) -> TargetEntry {
    TargetEntry {
        expr: lower_expr_to_plan_layout(target.expr, layout),
        ..target
    }
}

fn lower_order_by_entry_to_plan_layout(item: OrderByEntry, layout: &[Expr]) -> OrderByEntry {
    OrderByEntry {
        expr: lower_expr_to_plan_layout(item.expr, layout),
        ..item
    }
}

fn rewrite_project_set_target_against_layout(
    target: ProjectSetTarget,
    layout: &[Expr],
) -> ProjectSetTarget {
    match target {
        ProjectSetTarget::Scalar(entry) => {
            ProjectSetTarget::Scalar(rewrite_target_entry_against_layout(entry, layout))
        }
        ProjectSetTarget::Set {
            name,
            call,
            sql_type,
            column_index,
        } => ProjectSetTarget::Set {
            name,
            call: rewrite_set_returning_call_against_layout(call, layout),
            sql_type,
            column_index,
        },
    }
}

fn lower_project_set_target_to_plan_layout(
    target: ProjectSetTarget,
    layout: &[Expr],
) -> ProjectSetTarget {
    match target {
        ProjectSetTarget::Scalar(entry) => {
            ProjectSetTarget::Scalar(lower_target_entry_to_plan_layout(entry, layout))
        }
        ProjectSetTarget::Set {
            name,
            call,
            sql_type,
            column_index,
        } => ProjectSetTarget::Set {
            name,
            call: lower_set_returning_call_to_plan_layout(call, layout),
            sql_type,
            column_index,
        },
    }
}

fn rewrite_set_returning_call_against_layout(
    call: SetReturningCall,
    layout: &[Expr],
) -> SetReturningCall {
    match call {
        SetReturningCall::GenerateSeries {
            func_oid,
            func_variadic,
            start,
            stop,
            step,
            output,
        } => SetReturningCall::GenerateSeries {
            func_oid,
            func_variadic,
            start: rewrite_expr_against_layout(start, layout),
            stop: rewrite_expr_against_layout(stop, layout),
            step: rewrite_expr_against_layout(step, layout),
            output,
        },
        SetReturningCall::Unnest {
            func_oid,
            func_variadic,
            args,
            output_columns,
        } => SetReturningCall::Unnest {
            func_oid,
            func_variadic,
            args: args
                .into_iter()
                .map(|arg| rewrite_expr_against_layout(arg, layout))
                .collect(),
            output_columns,
        },
        SetReturningCall::JsonTableFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
        } => SetReturningCall::JsonTableFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(|arg| rewrite_expr_against_layout(arg, layout))
                .collect(),
            output_columns,
        },
        SetReturningCall::RegexTableFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
        } => SetReturningCall::RegexTableFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(|arg| rewrite_expr_against_layout(arg, layout))
                .collect(),
            output_columns,
        },
        SetReturningCall::TextSearchTableFunction {
            kind,
            args,
            output_columns,
        } => SetReturningCall::TextSearchTableFunction {
            kind,
            args: args
                .into_iter()
                .map(|arg| rewrite_expr_against_layout(arg, layout))
                .collect(),
            output_columns,
        },
    }
}

fn lower_set_returning_call_to_plan_layout(
    call: SetReturningCall,
    layout: &[Expr],
) -> SetReturningCall {
    match call {
        SetReturningCall::GenerateSeries {
            func_oid,
            func_variadic,
            start,
            stop,
            step,
            output,
        } => SetReturningCall::GenerateSeries {
            func_oid,
            func_variadic,
            start: lower_expr_to_plan_layout(start, layout),
            stop: lower_expr_to_plan_layout(stop, layout),
            step: lower_expr_to_plan_layout(step, layout),
            output,
        },
        SetReturningCall::Unnest {
            func_oid,
            func_variadic,
            args,
            output_columns,
        } => SetReturningCall::Unnest {
            func_oid,
            func_variadic,
            args: args
                .into_iter()
                .map(|arg| lower_expr_to_plan_layout(arg, layout))
                .collect(),
            output_columns,
        },
        SetReturningCall::JsonTableFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
        } => SetReturningCall::JsonTableFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(|arg| lower_expr_to_plan_layout(arg, layout))
                .collect(),
            output_columns,
        },
        SetReturningCall::RegexTableFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
        } => SetReturningCall::RegexTableFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(|arg| lower_expr_to_plan_layout(arg, layout))
                .collect(),
            output_columns,
        },
        SetReturningCall::TextSearchTableFunction {
            kind,
            args,
            output_columns,
        } => SetReturningCall::TextSearchTableFunction {
            kind,
            args: args
                .into_iter()
                .map(|arg| lower_expr_to_plan_layout(arg, layout))
                .collect(),
            output_columns,
        },
    }
}

fn lower_agg_accum_to_plan_layout(accum: AggAccum, layout: &[Expr]) -> AggAccum {
    AggAccum {
        args: accum
            .args
            .into_iter()
            .map(|arg| lower_expr_to_plan_layout(arg, layout))
            .collect(),
        ..accum
    }
}

fn lower_agg_output_expr(expr: Expr, group_by: &[Expr], agg_output_layout: &[Expr]) -> Expr {
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
        } => Expr::Like {
            expr: Box::new(lower_agg_output_expr(*expr, group_by, agg_output_layout)),
            pattern: Box::new(lower_agg_output_expr(*pattern, group_by, agg_output_layout)),
            escape: escape
                .map(|expr| Box::new(lower_agg_output_expr(*expr, group_by, agg_output_layout))),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(lower_agg_output_expr(*expr, group_by, agg_output_layout)),
            pattern: Box::new(lower_agg_output_expr(*pattern, group_by, agg_output_layout)),
            escape: escape
                .map(|expr| Box::new(lower_agg_output_expr(*expr, group_by, agg_output_layout))),
            negated,
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

fn rewrite_expr_against_layout(expr: Expr, layout: &[Expr]) -> Expr {
    match expr {
        Expr::Column(index) => layout.get(index).cloned().unwrap_or(Expr::Column(index)),
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(Aggref {
            args: aggref
                .args
                .into_iter()
                .map(|arg| rewrite_expr_against_layout(arg, layout))
                .collect(),
            ..*aggref
        })),
        Expr::Op(op) => Expr::Op(Box::new(OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| rewrite_expr_against_layout(arg, layout))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| rewrite_expr_against_layout(arg, layout))
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| rewrite_expr_against_layout(arg, layout))
                .collect(),
            ..*func
        })),
        Expr::SubLink(sublink) => {
            Expr::SubLink(Box::new(crate::include::nodes::primnodes::SubLink {
                testexpr: sublink
                    .testexpr
                    .map(|expr| Box::new(rewrite_expr_against_layout(*expr, layout))),
                ..*sublink
            }))
        }
        Expr::SubPlan(subplan) => {
            Expr::SubPlan(Box::new(crate::include::nodes::primnodes::SubPlan {
                testexpr: subplan
                    .testexpr
                    .map(|expr| Box::new(rewrite_expr_against_layout(*expr, layout))),
                ..*subplan
            }))
        }
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            left: Box::new(rewrite_expr_against_layout(*saop.left, layout)),
            right: Box::new(rewrite_expr_against_layout(*saop.right, layout)),
            ..*saop
        })),
        Expr::Cast(inner, ty) => {
            Expr::Cast(Box::new(rewrite_expr_against_layout(*inner, layout)), ty)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(rewrite_expr_against_layout(*expr, layout)),
            pattern: Box::new(rewrite_expr_against_layout(*pattern, layout)),
            escape: escape.map(|expr| Box::new(rewrite_expr_against_layout(*expr, layout))),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(rewrite_expr_against_layout(*expr, layout)),
            pattern: Box::new(rewrite_expr_against_layout(*pattern, layout)),
            escape: escape.map(|expr| Box::new(rewrite_expr_against_layout(*expr, layout))),
            negated,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(rewrite_expr_against_layout(*inner, layout))),
        Expr::IsNotNull(inner) => {
            Expr::IsNotNull(Box::new(rewrite_expr_against_layout(*inner, layout)))
        }
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(rewrite_expr_against_layout(*left, layout)),
            Box::new(rewrite_expr_against_layout(*right, layout)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(rewrite_expr_against_layout(*left, layout)),
            Box::new(rewrite_expr_against_layout(*right, layout)),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| rewrite_expr_against_layout(element, layout))
                .collect(),
            array_type,
        },
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(rewrite_expr_against_layout(*left, layout)),
            Box::new(rewrite_expr_against_layout(*right, layout)),
        ),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(rewrite_expr_against_layout(*array, layout)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript
                        .lower
                        .map(|expr| rewrite_expr_against_layout(expr, layout)),
                    upper: subscript
                        .upper
                        .map(|expr| rewrite_expr_against_layout(expr, layout)),
                })
                .collect(),
        },
        other => other,
    }
}

fn lower_expr_to_plan_layout(expr: Expr, layout: &[Expr]) -> Expr {
    if let Some(index) = layout.iter().position(|candidate| *candidate == expr) {
        return Expr::Column(index);
    }
    match expr {
        Expr::Var(var) if var.varlevelsup > 0 => Expr::OuterColumn {
            depth: var.varlevelsup - 1,
            index: var.varattno.saturating_sub(1),
        },
        Expr::Var(var) => Expr::Column(var.varattno.saturating_sub(1)),
        Expr::Aggref(_) => {
            panic!("Aggref should be lowered to aggregate output vars before create_plan")
        }
        Expr::Op(op) => Expr::Op(Box::new(OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| lower_expr_to_plan_layout(arg, layout))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| lower_expr_to_plan_layout(arg, layout))
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| lower_expr_to_plan_layout(arg, layout))
                .collect(),
            ..*func
        })),
        Expr::SubLink(sublink) => {
            Expr::SubLink(Box::new(crate::include::nodes::primnodes::SubLink {
                testexpr: sublink
                    .testexpr
                    .map(|expr| Box::new(lower_expr_to_plan_layout(*expr, layout))),
                ..*sublink
            }))
        }
        Expr::SubPlan(subplan) => {
            Expr::SubPlan(Box::new(crate::include::nodes::primnodes::SubPlan {
                testexpr: subplan
                    .testexpr
                    .map(|expr| Box::new(lower_expr_to_plan_layout(*expr, layout))),
                ..*subplan
            }))
        }
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            left: Box::new(lower_expr_to_plan_layout(*saop.left, layout)),
            right: Box::new(lower_expr_to_plan_layout(*saop.right, layout)),
            ..*saop
        })),
        Expr::Cast(inner, ty) => {
            Expr::Cast(Box::new(lower_expr_to_plan_layout(*inner, layout)), ty)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(lower_expr_to_plan_layout(*expr, layout)),
            pattern: Box::new(lower_expr_to_plan_layout(*pattern, layout)),
            escape: escape.map(|expr| Box::new(lower_expr_to_plan_layout(*expr, layout))),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(lower_expr_to_plan_layout(*expr, layout)),
            pattern: Box::new(lower_expr_to_plan_layout(*pattern, layout)),
            escape: escape.map(|expr| Box::new(lower_expr_to_plan_layout(*expr, layout))),
            negated,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(lower_expr_to_plan_layout(*inner, layout))),
        Expr::IsNotNull(inner) => {
            Expr::IsNotNull(Box::new(lower_expr_to_plan_layout(*inner, layout)))
        }
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(lower_expr_to_plan_layout(*left, layout)),
            Box::new(lower_expr_to_plan_layout(*right, layout)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(lower_expr_to_plan_layout(*left, layout)),
            Box::new(lower_expr_to_plan_layout(*right, layout)),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| lower_expr_to_plan_layout(element, layout))
                .collect(),
            array_type,
        },
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(lower_expr_to_plan_layout(*left, layout)),
            Box::new(lower_expr_to_plan_layout(*right, layout)),
        ),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(lower_expr_to_plan_layout(*array, layout)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript
                        .lower
                        .map(|expr| lower_expr_to_plan_layout(expr, layout)),
                    upper: subscript
                        .upper
                        .map(|expr| lower_expr_to_plan_layout(expr, layout)),
                })
                .collect(),
        },
        other => other,
    }
}

fn expr_sql_type(expr: &Expr) -> SqlType {
    match expr {
        Expr::Var(var) => var.vartype,
        Expr::Aggref(aggref) => aggref.aggtype,
        Expr::Op(op) => op.opresulttype,
        Expr::Func(func) => func
            .funcresulttype
            .unwrap_or(SqlType::new(SqlTypeKind::Text)),
        Expr::Bool(_)
        | Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::IsDistinctFrom(_, _)
        | Expr::IsNotDistinctFrom(_, _)
        | Expr::Like { .. }
        | Expr::Similar { .. }
        | Expr::ScalarArrayOp(_) => SqlType::new(SqlTypeKind::Bool),
        Expr::Cast(_, ty) => *ty,
        Expr::ArrayLiteral { array_type, .. } => *array_type,
        Expr::Coalesce(left, right) => expr_sql_type_maybe(left)
            .or_else(|| expr_sql_type_maybe(right))
            .unwrap_or(SqlType::new(SqlTypeKind::Text)),
        Expr::SubLink(sublink) => match sublink.sublink_type {
            SubLinkType::ExistsSubLink
            | SubLinkType::AnySubLink(_)
            | SubLinkType::AllSubLink(_) => SqlType::new(SqlTypeKind::Bool),
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
            SubLinkType::ExprSubLink => subplan
                .first_col_type
                .unwrap_or(SqlType::new(SqlTypeKind::Text)),
        },
        Expr::Const(value) => value_sql_type_hint(value),
        Expr::Random => SqlType::new(SqlTypeKind::Float8),
        Expr::CurrentDate => SqlType::new(SqlTypeKind::Date),
        Expr::CurrentTime { .. } => SqlType::new(SqlTypeKind::TimeTz),
        Expr::CurrentTimestamp { .. } => SqlType::new(SqlTypeKind::TimestampTz),
        Expr::LocalTime { .. } => SqlType::new(SqlTypeKind::Time),
        Expr::LocalTimestamp { .. } => SqlType::new(SqlTypeKind::Timestamp),
        Expr::Column(_) | Expr::OuterColumn { .. } | Expr::ArraySubscript { .. } => {
            SqlType::new(SqlTypeKind::Text)
        }
    }
}

fn expr_sql_type_maybe(expr: &Expr) -> Option<SqlType> {
    match expr {
        Expr::Column(_) | Expr::OuterColumn { .. } | Expr::ArraySubscript { .. } => None,
        other => Some(expr_sql_type(other)),
    }
}

fn value_sql_type_hint(value: &Value) -> SqlType {
    match value {
        Value::Int16(_) => SqlType::new(SqlTypeKind::Int2),
        Value::Int32(_) => SqlType::new(SqlTypeKind::Int4),
        Value::Int64(_) => SqlType::new(SqlTypeKind::Int8),
        Value::Date(_) => SqlType::new(SqlTypeKind::Date),
        Value::Time(_) => SqlType::new(SqlTypeKind::Time),
        Value::TimeTz(_) => SqlType::new(SqlTypeKind::TimeTz),
        Value::Timestamp(_) => SqlType::new(SqlTypeKind::Timestamp),
        Value::TimestampTz(_) => SqlType::new(SqlTypeKind::TimestampTz),
        Value::Bit(_) => SqlType::new(SqlTypeKind::Bit),
        Value::Bytea(_) => SqlType::new(SqlTypeKind::Bytea),
        Value::Point(_) => SqlType::new(SqlTypeKind::Point),
        Value::Lseg(_) => SqlType::new(SqlTypeKind::Lseg),
        Value::Path(_) => SqlType::new(SqlTypeKind::Path),
        Value::Line(_) => SqlType::new(SqlTypeKind::Line),
        Value::Box(_) => SqlType::new(SqlTypeKind::Box),
        Value::Polygon(_) => SqlType::new(SqlTypeKind::Polygon),
        Value::Circle(_) => SqlType::new(SqlTypeKind::Circle),
        Value::Float64(_) => SqlType::new(SqlTypeKind::Float8),
        Value::Numeric(_) => SqlType::new(SqlTypeKind::Numeric),
        Value::Json(_) => SqlType::new(SqlTypeKind::Json),
        Value::Jsonb(_) => SqlType::new(SqlTypeKind::Jsonb),
        Value::JsonPath(_) => SqlType::new(SqlTypeKind::JsonPath),
        Value::TsVector(_) => SqlType::new(SqlTypeKind::TsVector),
        Value::TsQuery(_) => SqlType::new(SqlTypeKind::TsQuery),
        Value::Text(_) | Value::TextRef(_, _) => SqlType::new(SqlTypeKind::Text),
        Value::InternalChar(_) => SqlType::new(SqlTypeKind::InternalChar),
        Value::Bool(_) => SqlType::new(SqlTypeKind::Bool),
        Value::Array(_) | Value::PgArray(_) | Value::Null => SqlType::new(SqlTypeKind::Text),
    }
}
