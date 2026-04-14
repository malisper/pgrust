use std::sync::atomic::{AtomicUsize, Ordering};

use crate::include::nodes::parsenodes::{JoinTreeNode, Query, RangeTblEntryKind};
use crate::include::nodes::pathnodes::{
    PlannerJoinArraySubscript, PlannerJoinExpr, PlannerOrderByEntry, PlannerPath,
    PlannerProjectSetTarget, PlannerTargetEntry,
};
use crate::include::nodes::plannodes::{Plan, PlanEstimate};
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::{
    BoolExpr, BoolExprType, Expr, ExprArraySubscript, FuncExpr, OpExpr, OpExprKind,
    QueryColumn, ScalarArrayOpExpr,
};

struct PlannerPathBuilder {
    next_slot_id: usize,
}

static NEXT_SYNTHETIC_SLOT_ID: AtomicUsize = AtomicUsize::new(1);

pub(crate) fn next_synthetic_slot_id() -> usize {
    NEXT_SYNTHETIC_SLOT_ID.fetch_add(1, Ordering::Relaxed)
}

fn planner_join_from_op<F>(op: &OpExpr, recurse: F) -> PlannerJoinExpr
where
    F: Fn(&Expr) -> PlannerJoinExpr + Copy,
{
    let unary = |arg: &Expr, ctor: fn(Box<PlannerJoinExpr>) -> PlannerJoinExpr| {
        ctor(Box::new(recurse(arg)))
    };
    let binary =
        |args: &[Expr], ctor: fn(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>) -> PlannerJoinExpr| {
            let [left, right] = args else {
                panic!("malformed OpExpr {:?}: expected binary args", op.op);
            };
            ctor(Box::new(recurse(left)), Box::new(recurse(right)))
        };
    match op.op {
        OpExprKind::UnaryPlus => {
            let [arg] = op.args.as_slice() else {
                panic!("malformed unary plus OpExpr");
            };
            unary(arg, PlannerJoinExpr::UnaryPlus)
        }
        OpExprKind::Negate => {
            let [arg] = op.args.as_slice() else {
                panic!("malformed negate OpExpr");
            };
            unary(arg, PlannerJoinExpr::Negate)
        }
        OpExprKind::BitNot => {
            let [arg] = op.args.as_slice() else {
                panic!("malformed bit-not OpExpr");
            };
            unary(arg, PlannerJoinExpr::BitNot)
        }
        OpExprKind::Add => binary(&op.args, PlannerJoinExpr::Add),
        OpExprKind::Sub => binary(&op.args, PlannerJoinExpr::Sub),
        OpExprKind::BitAnd => binary(&op.args, PlannerJoinExpr::BitAnd),
        OpExprKind::BitOr => binary(&op.args, PlannerJoinExpr::BitOr),
        OpExprKind::BitXor => binary(&op.args, PlannerJoinExpr::BitXor),
        OpExprKind::Shl => binary(&op.args, PlannerJoinExpr::Shl),
        OpExprKind::Shr => binary(&op.args, PlannerJoinExpr::Shr),
        OpExprKind::Mul => binary(&op.args, PlannerJoinExpr::Mul),
        OpExprKind::Div => binary(&op.args, PlannerJoinExpr::Div),
        OpExprKind::Mod => binary(&op.args, PlannerJoinExpr::Mod),
        OpExprKind::Concat => binary(&op.args, PlannerJoinExpr::Concat),
        OpExprKind::Eq => binary(&op.args, PlannerJoinExpr::Eq),
        OpExprKind::NotEq => binary(&op.args, PlannerJoinExpr::NotEq),
        OpExprKind::Lt => binary(&op.args, PlannerJoinExpr::Lt),
        OpExprKind::LtEq => binary(&op.args, PlannerJoinExpr::LtEq),
        OpExprKind::Gt => binary(&op.args, PlannerJoinExpr::Gt),
        OpExprKind::GtEq => binary(&op.args, PlannerJoinExpr::GtEq),
        OpExprKind::RegexMatch => binary(&op.args, PlannerJoinExpr::RegexMatch),
        OpExprKind::ArrayOverlap => binary(&op.args, PlannerJoinExpr::ArrayOverlap),
        OpExprKind::JsonbContains => binary(&op.args, PlannerJoinExpr::JsonbContains),
        OpExprKind::JsonbContained => binary(&op.args, PlannerJoinExpr::JsonbContained),
        OpExprKind::JsonbExists => binary(&op.args, PlannerJoinExpr::JsonbExists),
        OpExprKind::JsonbExistsAny => binary(&op.args, PlannerJoinExpr::JsonbExistsAny),
        OpExprKind::JsonbExistsAll => binary(&op.args, PlannerJoinExpr::JsonbExistsAll),
        OpExprKind::JsonbPathExists => binary(&op.args, PlannerJoinExpr::JsonbPathExists),
        OpExprKind::JsonbPathMatch => binary(&op.args, PlannerJoinExpr::JsonbPathMatch),
        OpExprKind::JsonGet => binary(&op.args, PlannerJoinExpr::JsonGet),
        OpExprKind::JsonGetText => binary(&op.args, PlannerJoinExpr::JsonGetText),
        OpExprKind::JsonPath => binary(&op.args, PlannerJoinExpr::JsonPath),
        OpExprKind::JsonPathText => binary(&op.args, PlannerJoinExpr::JsonPathText),
    }
}

fn planner_join_from_bool<F>(bool_expr: &BoolExpr, recurse: F) -> PlannerJoinExpr
where
    F: Fn(&Expr) -> PlannerJoinExpr + Copy,
{
    match bool_expr.boolop {
        BoolExprType::And => {
            let mut args = bool_expr.args.iter();
            let first = args.next().map(recurse).unwrap_or(PlannerJoinExpr::Const(Value::Bool(true)));
            args.fold(first, |left, right| {
                PlannerJoinExpr::And(Box::new(left), Box::new(recurse(right)))
            })
        }
        BoolExprType::Or => {
            let mut args = bool_expr.args.iter();
            let first =
                args.next().map(recurse).unwrap_or(PlannerJoinExpr::Const(Value::Bool(false)));
            args.fold(first, |left, right| {
                PlannerJoinExpr::Or(Box::new(left), Box::new(recurse(right)))
            })
        }
        BoolExprType::Not => {
            let inner = bool_expr
                .args
                .first()
                .map(recurse)
                .unwrap_or(PlannerJoinExpr::Const(Value::Bool(false)));
            PlannerJoinExpr::Not(Box::new(inner))
        }
    }
}

fn planner_join_from_func<F>(func: &FuncExpr, recurse: F) -> PlannerJoinExpr
where
    F: Fn(&Expr) -> PlannerJoinExpr + Copy,
{
    PlannerJoinExpr::FuncCall {
        func_oid: func.funcid,
        func: crate::include::catalog::builtin_scalar_function_for_proc_oid(func.funcid)
            .unwrap_or_else(|| panic!("planner function {:?} lacks builtin mapping", func.funcid)),
        args: func.args.iter().map(recurse).collect(),
        func_variadic: func.funcvariadic,
    }
}

fn planner_join_from_scalar_array<F>(saop: &ScalarArrayOpExpr, recurse: F) -> PlannerJoinExpr
where
    F: Fn(&Expr) -> PlannerJoinExpr + Copy,
{
    if saop.use_or {
        PlannerJoinExpr::AnyArray {
            left: Box::new(recurse(&saop.left)),
            op: saop.op,
            right: Box::new(recurse(&saop.right)),
        }
    } else {
        PlannerJoinExpr::AllArray {
            left: Box::new(recurse(&saop.left)),
            op: saop.op,
            right: Box::new(recurse(&saop.right)),
        }
    }
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
                    predicate: predicate.into_input_expr_with_layout(&layout),
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
                    on: on.into_input_expr_with_layout(&layout),
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
                        .map(|target| target.into_target_entry_with_layout(&layout))
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
                        .map(|item| item.into_order_by_entry_with_layout(&layout))
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
                input,
                group_by,
                accumulators,
                having,
                output_columns,
                ..
            } => {
                let layout = input.output_vars();
                Plan::Aggregate {
                    plan_info,
                    input: Box::new(input.into_plan()),
                    group_by: group_by
                        .into_iter()
                        .map(|expr| expr.into_input_expr_with_layout(&layout))
                        .collect(),
                    accumulators,
                    having: having.map(|expr| expr.into_input_expr_with_layout(&layout)),
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
                            .map(|expr| expr.into_input_expr_with_layout(&[]))
                            .collect()
                    })
                    .collect(),
                output_columns,
            },
            Self::FunctionScan {
                plan_info, call, ..
            } => Plan::FunctionScan { plan_info, call },
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
                        .map(|target| target.into_project_set_target_with_layout(&layout))
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
                    PlannerProjectSetTarget::Scalar(entry) => QueryColumn {
                        name: entry.name.clone(),
                        sql_type: entry.sql_type,
                    },
                    PlannerProjectSetTarget::Set { name, sql_type, .. } => QueryColumn {
                        name: name.clone(),
                        sql_type: *sql_type,
                    },
                })
                .collect(),
        }
    }

    pub fn output_vars(&self) -> Vec<PlannerJoinExpr> {
        match self {
            Self::Result { .. } => Vec::new(),
            Self::SeqScan {
                source_id,
                relation_oid,
                desc,
                ..
            } => desc
                .columns
                .iter()
                .enumerate()
                .map(|(index, _)| PlannerJoinExpr::BaseColumn {
                    source_id: *source_id,
                    relation_oid: *relation_oid,
                    index,
                })
                .collect(),
            Self::IndexScan {
                source_id,
                desc,
                index_meta,
                ..
            } => desc
                .columns
                .iter()
                .enumerate()
                .map(|(index, _)| PlannerJoinExpr::BaseColumn {
                    source_id: *source_id,
                    relation_oid: index_meta.indrelid,
                    index,
                })
                .collect(),
            Self::Filter { input, .. }
            | Self::OrderBy { input, .. }
            | Self::Limit { input, .. } => input.output_vars(),
            Self::Projection {
                slot_id, targets, ..
            } => targets
                .iter()
                .enumerate()
                .map(|(index, _)| PlannerJoinExpr::SyntheticColumn {
                    slot_id: *slot_id,
                    index,
                })
                .collect(),
            Self::Aggregate {
                slot_id,
                output_columns,
                ..
            } => output_columns
                .iter()
                .enumerate()
                .map(|(index, _)| PlannerJoinExpr::SyntheticColumn {
                    slot_id: *slot_id,
                    index,
                })
                .collect(),
            Self::Values {
                slot_id,
                output_columns,
                ..
            } => output_columns
                .iter()
                .enumerate()
                .map(|(index, _)| PlannerJoinExpr::SyntheticColumn {
                    slot_id: *slot_id,
                    index,
                })
                .collect(),
            Self::FunctionScan { slot_id, call, .. } => call
                .output_columns()
                .iter()
                .enumerate()
                .map(|(index, _)| PlannerJoinExpr::SyntheticColumn {
                    slot_id: *slot_id,
                    index,
                })
                .collect(),
            Self::ProjectSet {
                slot_id, targets, ..
            } => targets
                .iter()
                .enumerate()
                .map(|(index, _)| PlannerJoinExpr::SyntheticColumn {
                    slot_id: *slot_id,
                    index,
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
                predicate: PlannerJoinExpr::from_input_expr_with_layout(&predicate, &layout),
            };
        }

        let has_agg = !query.group_by.is_empty()
            || !query.accumulators.is_empty()
            || query.having_qual.is_some();
        if has_agg {
            let layout = plan.output_vars();
            plan = PlannerPath::Aggregate {
                plan_info: PlanEstimate::default(),
                slot_id: self.alloc_slot_id(),
                input: Box::new(plan),
                group_by: query
                    .group_by
                    .iter()
                    .map(|expr| PlannerJoinExpr::from_input_expr_with_layout(expr, &layout))
                    .collect(),
                accumulators: query.accumulators,
                having: query
                    .having_qual
                    .as_ref()
                    .map(|expr| PlannerJoinExpr::from_input_expr_with_layout(expr, &layout)),
                output_columns: query
                    .target_list
                    .iter()
                    .map(|target| QueryColumn {
                        name: target.name.clone(),
                        sql_type: target.sql_type,
                    })
                    .collect(),
            };
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
                    .map(|target| {
                        PlannerProjectSetTarget::from_project_set_target_with_layout(
                            target, &layout,
                        )
                    })
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
                    .map(|item| PlannerOrderByEntry {
                        expr: PlannerJoinExpr::from_input_expr_with_layout(&item.expr, &layout),
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
            plan = if has_agg {
                self.add_projection(plan, projection_targets)
            } else {
                self.maybe_add_projection(plan, projection_targets)
            };
        }

        plan
    }

    fn add_projection(
        &mut self,
        input: PlannerPath,
        targets: Vec<crate::backend::executor::TargetEntry>,
    ) -> PlannerPath {
        let layout = input.output_vars();
        PlannerPath::Projection {
            plan_info: PlanEstimate::default(),
            slot_id: self.alloc_slot_id(),
            input: Box::new(input),
            targets: targets
                .into_iter()
                .map(|target| PlannerTargetEntry::from_target_entry_with_layout(target, &layout))
                .collect(),
        }
    }

    fn maybe_add_projection(
        &mut self,
        input: PlannerPath,
        targets: Vec<crate::backend::executor::TargetEntry>,
    ) -> PlannerPath {
        let input_columns = input.columns();
        let layout = input.output_vars();
        let is_identity = targets.len() == input_columns.len()
            && targets.iter().enumerate().all(|(index, target)| {
                PlannerJoinExpr::from_input_expr_with_layout(&target.expr, &layout) == layout[index]
                    && target.name == input_columns[index].name
            });
        if is_identity {
            input
        } else {
            self.add_projection(input, targets)
        }
    }

    fn from_jointree(
        &mut self,
        jointree: JoinTreeNode,
        rtable: Vec<crate::include::nodes::parsenodes::RangeTblEntry>,
    ) -> PlannerPath {
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
                            .iter()
                            .map(|row| {
                                row.iter()
                                    .map(|expr| {
                                        PlannerJoinExpr::from_input_expr_with_layout(expr, &[])
                                    })
                                    .collect()
                            })
                            .collect(),
                        output_columns,
                    },
                    RangeTblEntryKind::Function { call } => PlannerPath::FunctionScan {
                        plan_info: PlanEstimate::default(),
                        slot_id: rtindex,
                        call,
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
                                    PlannerTargetEntry::from_target_entry_with_layout(
                                        crate::backend::executor::TargetEntry::new(
                                            column.name,
                                            Expr::Column(index),
                                            column.sql_type,
                                            index + 1,
                                        ),
                                        &layout,
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
                    on: PlannerJoinExpr::from_input_expr_with_layout(&quals, &layout),
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
                            PlannerTargetEntry::from_target_entry_with_layout(
                                crate::backend::executor::TargetEntry::new(
                                    rte.desc.columns[index].name.clone(),
                                    expr.clone(),
                                    rte.desc.columns[index].sql_type,
                                    index + 1,
                                ),
                                &layout,
                            )
                        })
                        .collect(),
                }
            }
        }
    }

}

fn join_alias_vars_match_layout(joinaliasvars: &[Expr], layout: &[PlannerJoinExpr]) -> bool {
    joinaliasvars.len() == layout.len()
        && joinaliasvars.iter().zip(layout.iter()).all(|(expr, expected)| {
            PlannerJoinExpr::from_input_expr_with_layout(expr, layout) == *expected
        })
}

impl PlannerTargetEntry {
    pub(crate) fn from_target_entry(target: crate::backend::executor::TargetEntry) -> Self {
        Self {
            name: target.name,
            expr: PlannerJoinExpr::from_input_expr(&target.expr),
            sql_type: target.sql_type,
            resno: target.resno,
            ressortgroupref: target.ressortgroupref,
            resjunk: target.resjunk,
        }
    }

    pub(crate) fn from_target_entry_with_layout(
        target: crate::backend::executor::TargetEntry,
        layout: &[PlannerJoinExpr],
    ) -> Self {
        Self {
            name: target.name,
            expr: PlannerJoinExpr::from_input_expr_with_layout(&target.expr, layout),
            sql_type: target.sql_type,
            resno: target.resno,
            ressortgroupref: target.ressortgroupref,
            resjunk: target.resjunk,
        }
    }

    pub(crate) fn into_target_entry(self) -> crate::backend::executor::TargetEntry {
        crate::backend::executor::TargetEntry {
            name: self.name,
            expr: self.expr.into_input_expr(),
            sql_type: self.sql_type,
            resno: self.resno,
            ressortgroupref: self.ressortgroupref,
            resjunk: self.resjunk,
        }
    }

    pub(crate) fn into_target_entry_with_layout(
        self,
        layout: &[PlannerJoinExpr],
    ) -> crate::backend::executor::TargetEntry {
        crate::backend::executor::TargetEntry {
            name: self.name,
            expr: self.expr.into_input_expr_with_layout(layout),
            sql_type: self.sql_type,
            resno: self.resno,
            ressortgroupref: self.ressortgroupref,
            resjunk: self.resjunk,
        }
    }
}

impl PlannerOrderByEntry {
    pub(crate) fn from_order_by_entry(item: crate::backend::executor::OrderByEntry) -> Self {
        Self {
            expr: PlannerJoinExpr::from_input_expr(&item.expr),
            descending: item.descending,
            nulls_first: item.nulls_first,
        }
    }

    pub(crate) fn from_order_by_entry_with_layout(
        item: crate::backend::executor::OrderByEntry,
        layout: &[PlannerJoinExpr],
    ) -> Self {
        Self {
            expr: PlannerJoinExpr::from_input_expr_with_layout(&item.expr, layout),
            descending: item.descending,
            nulls_first: item.nulls_first,
        }
    }

    pub(crate) fn into_order_by_entry(self) -> crate::backend::executor::OrderByEntry {
        crate::backend::executor::OrderByEntry {
            expr: self.expr.into_input_expr(),
            descending: self.descending,
            nulls_first: self.nulls_first,
        }
    }

    pub(crate) fn into_order_by_entry_with_layout(
        self,
        layout: &[PlannerJoinExpr],
    ) -> crate::backend::executor::OrderByEntry {
        crate::backend::executor::OrderByEntry {
            expr: self.expr.into_input_expr_with_layout(layout),
            descending: self.descending,
            nulls_first: self.nulls_first,
        }
    }
}

impl PlannerProjectSetTarget {
    fn from_project_set_target(target: crate::include::nodes::plannodes::ProjectSetTarget) -> Self {
        match target {
            crate::include::nodes::plannodes::ProjectSetTarget::Scalar(entry) => {
                Self::Scalar(PlannerTargetEntry::from_target_entry(entry))
            }
            crate::include::nodes::plannodes::ProjectSetTarget::Set {
                name,
                call,
                sql_type,
                column_index,
            } => Self::Set {
                name,
                call,
                sql_type,
                column_index,
            },
        }
    }

    fn from_project_set_target_with_layout(
        target: crate::include::nodes::plannodes::ProjectSetTarget,
        layout: &[PlannerJoinExpr],
    ) -> Self {
        match target {
            crate::include::nodes::plannodes::ProjectSetTarget::Scalar(entry) => Self::Scalar(
                PlannerTargetEntry::from_target_entry_with_layout(entry, layout),
            ),
            crate::include::nodes::plannodes::ProjectSetTarget::Set {
                name,
                call,
                sql_type,
                column_index,
            } => Self::Set {
                name,
                call,
                sql_type,
                column_index,
            },
        }
    }

    fn into_project_set_target(self) -> crate::include::nodes::plannodes::ProjectSetTarget {
        match self {
            Self::Scalar(entry) => crate::include::nodes::plannodes::ProjectSetTarget::Scalar(
                entry.into_target_entry(),
            ),
            Self::Set {
                name,
                call,
                sql_type,
                column_index,
            } => crate::include::nodes::plannodes::ProjectSetTarget::Set {
                name,
                call,
                sql_type,
                column_index,
            },
        }
    }

    fn into_project_set_target_with_layout(
        self,
        layout: &[PlannerJoinExpr],
    ) -> crate::include::nodes::plannodes::ProjectSetTarget {
        match self {
            Self::Scalar(entry) => crate::include::nodes::plannodes::ProjectSetTarget::Scalar(
                entry.into_target_entry_with_layout(layout),
            ),
            Self::Set {
                name,
                call,
                sql_type,
                column_index,
            } => crate::include::nodes::plannodes::ProjectSetTarget::Set {
                name,
                call,
                sql_type,
                column_index,
            },
        }
    }
}

impl PlannerJoinExpr {
    fn layout_position(layout: &[PlannerJoinExpr], needle: &PlannerJoinExpr) -> Option<usize> {
        layout.iter().position(|candidate| candidate == needle)
    }

    fn from_var_with_layout(
        var: &crate::include::nodes::primnodes::Var,
        layout: &[PlannerJoinExpr],
    ) -> Self {
        if var.varlevelsup > 0 {
            return Self::OuterColumn {
                depth: var.varlevelsup - 1,
                index: var.varattno.saturating_sub(1),
            };
        }
        let attno = var.varattno.saturating_sub(1);
        layout
            .iter()
            .find(|expr| match expr {
                PlannerJoinExpr::BaseColumn {
                    source_id, index, ..
                } => *source_id == var.varno && *index == attno,
                PlannerJoinExpr::SyntheticColumn { slot_id, index } => {
                    *slot_id == var.varno && *index == attno
                }
                _ => false,
            })
            .cloned()
            .or_else(|| layout.get(attno).cloned())
            .unwrap_or(Self::InputColumn(attno))
    }

    fn from_var(var: &crate::include::nodes::primnodes::Var) -> Self {
        if var.varlevelsup > 0 {
            Self::OuterColumn {
                depth: var.varlevelsup - 1,
                index: var.varattno.saturating_sub(1),
            }
        } else {
            Self::InputColumn(var.varattno.saturating_sub(1))
        }
    }

    pub fn from_input_expr_with_layout(expr: &Expr, layout: &[PlannerJoinExpr]) -> Self {
        match expr {
            Expr::Op(op) => planner_join_from_op(op, |expr| {
                Self::from_input_expr_with_layout(expr, layout)
            }),
            Expr::Bool(bool_expr) => planner_join_from_bool(bool_expr, |expr| {
                Self::from_input_expr_with_layout(expr, layout)
            }),
            Expr::Func(func) => planner_join_from_func(func, |expr| {
                Self::from_input_expr_with_layout(expr, layout)
            }),
            Expr::ScalarArrayOp(saop) => planner_join_from_scalar_array(saop, |expr| {
                Self::from_input_expr_with_layout(expr, layout)
            }),
            Expr::Var(var) => Self::from_var_with_layout(var, layout),
            Expr::Column(index) => layout
                .get(*index)
                .cloned()
                .unwrap_or(Self::InputColumn(*index)),
            Expr::OuterColumn { depth, index } => Self::OuterColumn {
                depth: *depth,
                index: *index,
            },
            Expr::Const(value) => Self::Const(value.clone()),
            Expr::Add(left, right) => Self::Add(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::Sub(left, right) => Self::Sub(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::BitAnd(left, right) => Self::BitAnd(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::BitOr(left, right) => Self::BitOr(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::BitXor(left, right) => Self::BitXor(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::Shl(left, right) => Self::Shl(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::Shr(left, right) => Self::Shr(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::Mul(left, right) => Self::Mul(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::Div(left, right) => Self::Div(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::Mod(left, right) => Self::Mod(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::Concat(left, right) => Self::Concat(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::UnaryPlus(inner) => {
                Self::UnaryPlus(Box::new(Self::from_input_expr_with_layout(inner, layout)))
            }
            Expr::Negate(inner) => {
                Self::Negate(Box::new(Self::from_input_expr_with_layout(inner, layout)))
            }
            Expr::BitNot(inner) => {
                Self::BitNot(Box::new(Self::from_input_expr_with_layout(inner, layout)))
            }
            Expr::Cast(inner, sql_type) => Self::Cast(
                Box::new(Self::from_input_expr_with_layout(inner, layout)),
                *sql_type,
            ),
            Expr::Eq(left, right) => Self::Eq(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::NotEq(left, right) => Self::NotEq(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::Lt(left, right) => Self::Lt(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::LtEq(left, right) => Self::LtEq(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::Gt(left, right) => Self::Gt(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::GtEq(left, right) => Self::GtEq(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::RegexMatch(left, right) => Self::RegexMatch(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::Like {
                expr,
                pattern,
                escape,
                case_insensitive,
                negated,
            } => Self::Like {
                expr: Box::new(Self::from_input_expr_with_layout(expr, layout)),
                pattern: Box::new(Self::from_input_expr_with_layout(pattern, layout)),
                escape: escape
                    .as_ref()
                    .map(|inner| Box::new(Self::from_input_expr_with_layout(inner, layout))),
                case_insensitive: *case_insensitive,
                negated: *negated,
            },
            Expr::Similar {
                expr,
                pattern,
                escape,
                negated,
            } => Self::Similar {
                expr: Box::new(Self::from_input_expr_with_layout(expr, layout)),
                pattern: Box::new(Self::from_input_expr_with_layout(pattern, layout)),
                escape: escape
                    .as_ref()
                    .map(|inner| Box::new(Self::from_input_expr_with_layout(inner, layout))),
                negated: *negated,
            },
            Expr::And(left, right) => Self::And(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::Or(left, right) => Self::Or(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::Not(inner) => {
                Self::Not(Box::new(Self::from_input_expr_with_layout(inner, layout)))
            }
            Expr::IsNull(inner) => {
                Self::IsNull(Box::new(Self::from_input_expr_with_layout(inner, layout)))
            }
            Expr::IsNotNull(inner) => {
                Self::IsNotNull(Box::new(Self::from_input_expr_with_layout(inner, layout)))
            }
            Expr::IsDistinctFrom(left, right) => Self::IsDistinctFrom(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::IsNotDistinctFrom(left, right) => Self::IsNotDistinctFrom(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::ArrayLiteral {
                elements,
                array_type,
            } => Self::ArrayLiteral {
                elements: elements
                    .iter()
                    .map(|element| Self::from_input_expr_with_layout(element, layout))
                    .collect(),
                array_type: *array_type,
            },
            Expr::ArrayOverlap(left, right) => Self::ArrayOverlap(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::JsonbContains(left, right) => Self::JsonbContains(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::JsonbContained(left, right) => Self::JsonbContained(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::JsonbExists(left, right) => Self::JsonbExists(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::JsonbExistsAny(left, right) => Self::JsonbExistsAny(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::JsonbExistsAll(left, right) => Self::JsonbExistsAll(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::JsonbPathExists(left, right) => Self::JsonbPathExists(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::JsonbPathMatch(left, right) => Self::JsonbPathMatch(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::SubLink(sublink) => Self::SubLink(sublink.clone()),
            Expr::SubPlan(subplan) => Self::SubPlan(subplan.clone()),
            Expr::Coalesce(left, right) => Self::Coalesce(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::AnyArray { left, op, right } => Self::AnyArray {
                left: Box::new(Self::from_input_expr_with_layout(left, layout)),
                op: *op,
                right: Box::new(Self::from_input_expr_with_layout(right, layout)),
            },
            Expr::AllArray { left, op, right } => Self::AllArray {
                left: Box::new(Self::from_input_expr_with_layout(left, layout)),
                op: *op,
                right: Box::new(Self::from_input_expr_with_layout(right, layout)),
            },
            Expr::ArraySubscript { array, subscripts } => Self::ArraySubscript {
                array: Box::new(Self::from_input_expr_with_layout(array, layout)),
                subscripts: subscripts
                    .iter()
                    .map(|subscript| PlannerJoinArraySubscript {
                        is_slice: subscript.is_slice,
                        lower: subscript
                            .lower
                            .as_ref()
                            .map(|expr| Self::from_input_expr_with_layout(expr, layout)),
                        upper: subscript
                            .upper
                            .as_ref()
                            .map(|expr| Self::from_input_expr_with_layout(expr, layout)),
                    })
                    .collect(),
            },
            Expr::Random => Self::Random,
            Expr::JsonGet(left, right) => Self::JsonGet(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::JsonGetText(left, right) => Self::JsonGetText(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::JsonPath(left, right) => Self::JsonPath(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::JsonPathText(left, right) => Self::JsonPathText(
                Box::new(Self::from_input_expr_with_layout(left, layout)),
                Box::new(Self::from_input_expr_with_layout(right, layout)),
            ),
            Expr::FuncCall {
                func_oid,
                func,
                args,
                func_variadic,
            } => Self::FuncCall {
                func_oid: *func_oid,
                func: *func,
                args: args
                    .iter()
                    .map(|arg| Self::from_input_expr_with_layout(arg, layout))
                    .collect(),
                func_variadic: *func_variadic,
            },
            Expr::CurrentDate => Self::CurrentDate,
            Expr::CurrentTime { precision } => Self::CurrentTime {
                precision: *precision,
            },
            Expr::CurrentTimestamp { precision } => Self::CurrentTimestamp {
                precision: *precision,
            },
            Expr::LocalTime { precision } => Self::LocalTime {
                precision: *precision,
            },
            Expr::LocalTimestamp { precision } => Self::LocalTimestamp {
                precision: *precision,
            },
        }
    }

    pub fn from_input_expr(expr: &Expr) -> Self {
        match expr {
            Expr::Op(op) => planner_join_from_op(op, Self::from_input_expr),
            Expr::Bool(bool_expr) => planner_join_from_bool(bool_expr, Self::from_input_expr),
            Expr::Func(func) => planner_join_from_func(func, Self::from_input_expr),
            Expr::ScalarArrayOp(saop) => {
                planner_join_from_scalar_array(saop, Self::from_input_expr)
            }
            Expr::Var(var) => Self::from_var(var),
            Expr::Column(index) => Self::InputColumn(*index),
            Expr::OuterColumn { depth, index } => Self::OuterColumn {
                depth: *depth,
                index: *index,
            },
            Expr::Const(value) => Self::Const(value.clone()),
            Expr::Add(left, right) => Self::Add(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::Sub(left, right) => Self::Sub(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::BitAnd(left, right) => Self::BitAnd(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::BitOr(left, right) => Self::BitOr(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::BitXor(left, right) => Self::BitXor(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::Shl(left, right) => Self::Shl(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::Shr(left, right) => Self::Shr(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::Mul(left, right) => Self::Mul(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::Div(left, right) => Self::Div(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::Mod(left, right) => Self::Mod(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::Concat(left, right) => Self::Concat(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::UnaryPlus(inner) => Self::UnaryPlus(Box::new(Self::from_input_expr(inner))),
            Expr::Negate(inner) => Self::Negate(Box::new(Self::from_input_expr(inner))),
            Expr::BitNot(inner) => Self::BitNot(Box::new(Self::from_input_expr(inner))),
            Expr::Cast(inner, sql_type) => {
                Self::Cast(Box::new(Self::from_input_expr(inner)), *sql_type)
            }
            Expr::Eq(left, right) => Self::Eq(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::NotEq(left, right) => Self::NotEq(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::Lt(left, right) => Self::Lt(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::LtEq(left, right) => Self::LtEq(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::Gt(left, right) => Self::Gt(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::GtEq(left, right) => Self::GtEq(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::RegexMatch(left, right) => Self::RegexMatch(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::Like {
                expr,
                pattern,
                escape,
                case_insensitive,
                negated,
            } => Self::Like {
                expr: Box::new(Self::from_input_expr(expr)),
                pattern: Box::new(Self::from_input_expr(pattern)),
                escape: escape
                    .as_ref()
                    .map(|inner| Box::new(Self::from_input_expr(inner))),
                case_insensitive: *case_insensitive,
                negated: *negated,
            },
            Expr::Similar {
                expr,
                pattern,
                escape,
                negated,
            } => Self::Similar {
                expr: Box::new(Self::from_input_expr(expr)),
                pattern: Box::new(Self::from_input_expr(pattern)),
                escape: escape
                    .as_ref()
                    .map(|inner| Box::new(Self::from_input_expr(inner))),
                negated: *negated,
            },
            Expr::And(left, right) => Self::And(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::Or(left, right) => Self::Or(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::Not(inner) => Self::Not(Box::new(Self::from_input_expr(inner))),
            Expr::IsNull(inner) => Self::IsNull(Box::new(Self::from_input_expr(inner))),
            Expr::IsNotNull(inner) => Self::IsNotNull(Box::new(Self::from_input_expr(inner))),
            Expr::IsDistinctFrom(left, right) => Self::IsDistinctFrom(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::IsNotDistinctFrom(left, right) => Self::IsNotDistinctFrom(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::ArrayLiteral {
                elements,
                array_type,
            } => Self::ArrayLiteral {
                elements: elements.iter().map(Self::from_input_expr).collect(),
                array_type: *array_type,
            },
            Expr::ArrayOverlap(left, right) => Self::ArrayOverlap(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::JsonbContains(left, right) => Self::JsonbContains(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::JsonbContained(left, right) => Self::JsonbContained(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::JsonbExists(left, right) => Self::JsonbExists(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::JsonbExistsAny(left, right) => Self::JsonbExistsAny(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::JsonbExistsAll(left, right) => Self::JsonbExistsAll(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::JsonbPathExists(left, right) => Self::JsonbPathExists(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::JsonbPathMatch(left, right) => Self::JsonbPathMatch(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::SubLink(sublink) => Self::SubLink(sublink.clone()),
            Expr::SubPlan(subplan) => Self::SubPlan(subplan.clone()),
            Expr::Coalesce(left, right) => Self::Coalesce(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::AnyArray { left, op, right } => Self::AnyArray {
                left: Box::new(Self::from_input_expr(left)),
                op: *op,
                right: Box::new(Self::from_input_expr(right)),
            },
            Expr::AllArray { left, op, right } => Self::AllArray {
                left: Box::new(Self::from_input_expr(left)),
                op: *op,
                right: Box::new(Self::from_input_expr(right)),
            },
            Expr::ArraySubscript { array, subscripts } => Self::ArraySubscript {
                array: Box::new(Self::from_input_expr(array)),
                subscripts: subscripts
                    .iter()
                    .map(|subscript| PlannerJoinArraySubscript {
                        is_slice: subscript.is_slice,
                        lower: subscript.lower.as_ref().map(Self::from_input_expr),
                        upper: subscript.upper.as_ref().map(Self::from_input_expr),
                    })
                    .collect(),
            },
            Expr::Random => Self::Random,
            Expr::JsonGet(left, right) => Self::JsonGet(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::JsonGetText(left, right) => Self::JsonGetText(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::JsonPath(left, right) => Self::JsonPath(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::JsonPathText(left, right) => Self::JsonPathText(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::FuncCall {
                func_oid,
                func,
                args,
                func_variadic,
            } => Self::FuncCall {
                func_oid: *func_oid,
                func: *func,
                args: args.iter().map(Self::from_input_expr).collect(),
                func_variadic: *func_variadic,
            },
            Expr::CurrentDate => Self::CurrentDate,
            Expr::CurrentTime { precision } => Self::CurrentTime {
                precision: *precision,
            },
            Expr::CurrentTimestamp { precision } => Self::CurrentTimestamp {
                precision: *precision,
            },
            Expr::LocalTime { precision } => Self::LocalTime {
                precision: *precision,
            },
            Expr::LocalTimestamp { precision } => Self::LocalTimestamp {
                precision: *precision,
            },
        }
    }

    pub fn into_input_expr_with_layout(self, layout: &[PlannerJoinExpr]) -> Expr {
        match self {
            Self::InputColumn(index) => Expr::Column(index),
            expr @ Self::SyntheticColumn { index, .. } | expr @ Self::BaseColumn { index, .. } => {
                Expr::Column(Self::layout_position(layout, &expr).unwrap_or(index))
            }
            Self::LeftColumn(index) => Expr::Column(index),
            Self::RightColumn(index) => Expr::Column(index),
            Self::OuterColumn { depth, index } => Expr::OuterColumn { depth, index },
            Self::Const(value) => Expr::Const(value),
            Self::Add(left, right) => Expr::op_auto(
                OpExprKind::Add,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::Sub(left, right) => Expr::op_auto(
                OpExprKind::Sub,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::BitAnd(left, right) => Expr::op_auto(
                OpExprKind::BitAnd,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::BitOr(left, right) => Expr::op_auto(
                OpExprKind::BitOr,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::BitXor(left, right) => Expr::op_auto(
                OpExprKind::BitXor,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::Shl(left, right) => Expr::op_auto(
                OpExprKind::Shl,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::Shr(left, right) => Expr::op_auto(
                OpExprKind::Shr,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::Mul(left, right) => Expr::op_auto(
                OpExprKind::Mul,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::Div(left, right) => Expr::op_auto(
                OpExprKind::Div,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::Mod(left, right) => Expr::op_auto(
                OpExprKind::Mod,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::Concat(left, right) => Expr::op_auto(
                OpExprKind::Concat,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::UnaryPlus(inner) => Expr::op_auto(
                OpExprKind::UnaryPlus,
                vec![inner.into_input_expr_with_layout(layout)],
            ),
            Self::Negate(inner) => Expr::op_auto(
                OpExprKind::Negate,
                vec![inner.into_input_expr_with_layout(layout)],
            ),
            Self::BitNot(inner) => Expr::op_auto(
                OpExprKind::BitNot,
                vec![inner.into_input_expr_with_layout(layout)],
            ),
            Self::Cast(inner, sql_type) => Expr::Cast(
                Box::new(inner.into_input_expr_with_layout(layout)),
                sql_type,
            ),
            Self::Eq(left, right) => Expr::op_auto(
                OpExprKind::Eq,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::NotEq(left, right) => Expr::op_auto(
                OpExprKind::NotEq,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::Lt(left, right) => Expr::op_auto(
                OpExprKind::Lt,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::LtEq(left, right) => Expr::op_auto(
                OpExprKind::LtEq,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::Gt(left, right) => Expr::op_auto(
                OpExprKind::Gt,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::GtEq(left, right) => Expr::op_auto(
                OpExprKind::GtEq,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::RegexMatch(left, right) => Expr::op_auto(
                OpExprKind::RegexMatch,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::Like {
                expr,
                pattern,
                escape,
                case_insensitive,
                negated,
            } => Expr::Like {
                expr: Box::new(expr.into_input_expr_with_layout(layout)),
                pattern: Box::new(pattern.into_input_expr_with_layout(layout)),
                escape: escape.map(|inner| Box::new(inner.into_input_expr_with_layout(layout))),
                case_insensitive,
                negated,
            },
            Self::Similar {
                expr,
                pattern,
                escape,
                negated,
            } => Expr::Similar {
                expr: Box::new(expr.into_input_expr_with_layout(layout)),
                pattern: Box::new(pattern.into_input_expr_with_layout(layout)),
                escape: escape.map(|inner| Box::new(inner.into_input_expr_with_layout(layout))),
                negated,
            },
            Self::And(left, right) => Expr::bool_expr(
                BoolExprType::And,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::Or(left, right) => Expr::bool_expr(
                BoolExprType::Or,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::Not(inner) => Expr::bool_expr(
                BoolExprType::Not,
                vec![inner.into_input_expr_with_layout(layout)],
            ),
            Self::IsNull(inner) => {
                Expr::IsNull(Box::new(inner.into_input_expr_with_layout(layout)))
            }
            Self::IsNotNull(inner) => {
                Expr::IsNotNull(Box::new(inner.into_input_expr_with_layout(layout)))
            }
            Self::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
                Box::new(left.into_input_expr_with_layout(layout)),
                Box::new(right.into_input_expr_with_layout(layout)),
            ),
            Self::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
                Box::new(left.into_input_expr_with_layout(layout)),
                Box::new(right.into_input_expr_with_layout(layout)),
            ),
            Self::ArrayLiteral {
                elements,
                array_type,
            } => Expr::ArrayLiteral {
                elements: elements
                    .into_iter()
                    .map(|element| element.into_input_expr_with_layout(layout))
                    .collect(),
                array_type,
            },
            Self::ArrayOverlap(left, right) => Expr::op_auto(
                OpExprKind::ArrayOverlap,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::JsonbContains(left, right) => Expr::op_auto(
                OpExprKind::JsonbContains,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::JsonbContained(left, right) => Expr::op_auto(
                OpExprKind::JsonbContained,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::JsonbExists(left, right) => Expr::op_auto(
                OpExprKind::JsonbExists,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::JsonbExistsAny(left, right) => Expr::op_auto(
                OpExprKind::JsonbExistsAny,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::JsonbExistsAll(left, right) => Expr::op_auto(
                OpExprKind::JsonbExistsAll,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::JsonbPathExists(left, right) => Expr::op_auto(
                OpExprKind::JsonbPathExists,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::JsonbPathMatch(left, right) => Expr::op_auto(
                OpExprKind::JsonbPathMatch,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::SubLink(sublink) => Expr::SubLink(sublink),
            Self::SubPlan(subplan) => Expr::SubPlan(subplan),
            Self::Coalesce(left, right) => Expr::Coalesce(
                Box::new(left.into_input_expr_with_layout(layout)),
                Box::new(right.into_input_expr_with_layout(layout)),
            ),
            Self::AnyArray { left, op, right } => Expr::scalar_array_op(
                op,
                true,
                left.into_input_expr_with_layout(layout),
                right.into_input_expr_with_layout(layout),
            ),
            Self::AllArray { left, op, right } => Expr::scalar_array_op(
                op,
                false,
                left.into_input_expr_with_layout(layout),
                right.into_input_expr_with_layout(layout),
            ),
            Self::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
                array: Box::new(array.into_input_expr_with_layout(layout)),
                subscripts: subscripts
                    .into_iter()
                    .map(|subscript| ExprArraySubscript {
                        is_slice: subscript.is_slice,
                        lower: subscript
                            .lower
                            .map(|expr| expr.into_input_expr_with_layout(layout)),
                        upper: subscript
                            .upper
                            .map(|expr| expr.into_input_expr_with_layout(layout)),
                    })
                    .collect(),
            },
            Self::Random => Expr::Random,
            Self::JsonGet(left, right) => Expr::op_auto(
                OpExprKind::JsonGet,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::JsonGetText(left, right) => Expr::op_auto(
                OpExprKind::JsonGetText,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::JsonPath(left, right) => Expr::op_auto(
                OpExprKind::JsonPath,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::JsonPathText(left, right) => Expr::op_auto(
                OpExprKind::JsonPathText,
                vec![
                    left.into_input_expr_with_layout(layout),
                    right.into_input_expr_with_layout(layout),
                ],
            ),
            Self::FuncCall {
                func_oid,
                args,
                func_variadic,
                ..
            } => Expr::func(
                func_oid,
                None,
                func_variadic,
                args.into_iter()
                    .map(|arg| arg.into_input_expr_with_layout(layout))
                    .collect(),
            ),
            Self::CurrentDate => Expr::CurrentDate,
            Self::CurrentTime { precision } => Expr::CurrentTime { precision },
            Self::CurrentTimestamp { precision } => Expr::CurrentTimestamp { precision },
            Self::LocalTime { precision } => Expr::LocalTime { precision },
            Self::LocalTimestamp { precision } => Expr::LocalTimestamp { precision },
        }
    }

    pub fn from_base_input_expr(expr: &Expr, relation_oid: u32) -> Self {
        match expr {
            Expr::Op(op) => planner_join_from_op(op, |expr| {
                Self::from_base_input_expr(expr, relation_oid)
            }),
            Expr::Bool(bool_expr) => planner_join_from_bool(bool_expr, |expr| {
                Self::from_base_input_expr(expr, relation_oid)
            }),
            Expr::Func(func) => planner_join_from_func(func, |expr| {
                Self::from_base_input_expr(expr, relation_oid)
            }),
            Expr::ScalarArrayOp(saop) => planner_join_from_scalar_array(saop, |expr| {
                Self::from_base_input_expr(expr, relation_oid)
            }),
            Expr::Var(var) if var.varlevelsup > 0 => Self::OuterColumn {
                depth: var.varlevelsup - 1,
                index: var.varattno.saturating_sub(1),
            },
            Expr::Var(var) => Self::BaseColumn {
                source_id: var.varno,
                relation_oid,
                index: var.varattno.saturating_sub(1),
            },
            Expr::Column(index) => Self::BaseColumn {
                source_id: 0,
                relation_oid,
                index: *index,
            },
            Expr::OuterColumn { depth, index } => Self::OuterColumn {
                depth: *depth,
                index: *index,
            },
            Expr::Const(value) => Self::Const(value.clone()),
            Expr::Add(left, right) => Self::Add(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::Sub(left, right) => Self::Sub(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::BitAnd(left, right) => Self::BitAnd(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::BitOr(left, right) => Self::BitOr(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::BitXor(left, right) => Self::BitXor(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::Shl(left, right) => Self::Shl(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::Shr(left, right) => Self::Shr(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::Mul(left, right) => Self::Mul(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::Div(left, right) => Self::Div(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::Mod(left, right) => Self::Mod(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::Concat(left, right) => Self::Concat(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::UnaryPlus(inner) => {
                Self::UnaryPlus(Box::new(Self::from_base_input_expr(inner, relation_oid)))
            }
            Expr::Negate(inner) => {
                Self::Negate(Box::new(Self::from_base_input_expr(inner, relation_oid)))
            }
            Expr::BitNot(inner) => {
                Self::BitNot(Box::new(Self::from_base_input_expr(inner, relation_oid)))
            }
            Expr::Cast(inner, sql_type) => Self::Cast(
                Box::new(Self::from_base_input_expr(inner, relation_oid)),
                *sql_type,
            ),
            Expr::Eq(left, right) => Self::Eq(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::NotEq(left, right) => Self::NotEq(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::Lt(left, right) => Self::Lt(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::LtEq(left, right) => Self::LtEq(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::Gt(left, right) => Self::Gt(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::GtEq(left, right) => Self::GtEq(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::RegexMatch(left, right) => Self::RegexMatch(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::Like {
                expr,
                pattern,
                escape,
                case_insensitive,
                negated,
            } => Self::Like {
                expr: Box::new(Self::from_base_input_expr(expr, relation_oid)),
                pattern: Box::new(Self::from_base_input_expr(pattern, relation_oid)),
                escape: escape
                    .as_ref()
                    .map(|inner| Box::new(Self::from_base_input_expr(inner, relation_oid))),
                case_insensitive: *case_insensitive,
                negated: *negated,
            },
            Expr::Similar {
                expr,
                pattern,
                escape,
                negated,
            } => Self::Similar {
                expr: Box::new(Self::from_base_input_expr(expr, relation_oid)),
                pattern: Box::new(Self::from_base_input_expr(pattern, relation_oid)),
                escape: escape
                    .as_ref()
                    .map(|inner| Box::new(Self::from_base_input_expr(inner, relation_oid))),
                negated: *negated,
            },
            Expr::And(left, right) => Self::And(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::Or(left, right) => Self::Or(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::Not(inner) => {
                Self::Not(Box::new(Self::from_base_input_expr(inner, relation_oid)))
            }
            Expr::IsNull(inner) => {
                Self::IsNull(Box::new(Self::from_base_input_expr(inner, relation_oid)))
            }
            Expr::IsNotNull(inner) => {
                Self::IsNotNull(Box::new(Self::from_base_input_expr(inner, relation_oid)))
            }
            Expr::IsDistinctFrom(left, right) => Self::IsDistinctFrom(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::IsNotDistinctFrom(left, right) => Self::IsNotDistinctFrom(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::ArrayLiteral {
                elements,
                array_type,
            } => Self::ArrayLiteral {
                elements: elements
                    .iter()
                    .map(|element| Self::from_base_input_expr(element, relation_oid))
                    .collect(),
                array_type: *array_type,
            },
            Expr::ArrayOverlap(left, right) => Self::ArrayOverlap(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::JsonbContains(left, right) => Self::JsonbContains(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::JsonbContained(left, right) => Self::JsonbContained(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::JsonbExists(left, right) => Self::JsonbExists(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::JsonbExistsAny(left, right) => Self::JsonbExistsAny(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::JsonbExistsAll(left, right) => Self::JsonbExistsAll(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::JsonbPathExists(left, right) => Self::JsonbPathExists(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::JsonbPathMatch(left, right) => Self::JsonbPathMatch(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::SubLink(sublink) => Self::SubLink(sublink.clone()),
            Expr::SubPlan(subplan) => Self::SubPlan(subplan.clone()),
            Expr::Coalesce(left, right) => Self::Coalesce(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::AnyArray { left, op, right } => Self::AnyArray {
                left: Box::new(Self::from_base_input_expr(left, relation_oid)),
                op: *op,
                right: Box::new(Self::from_base_input_expr(right, relation_oid)),
            },
            Expr::AllArray { left, op, right } => Self::AllArray {
                left: Box::new(Self::from_base_input_expr(left, relation_oid)),
                op: *op,
                right: Box::new(Self::from_base_input_expr(right, relation_oid)),
            },
            Expr::ArraySubscript { array, subscripts } => Self::ArraySubscript {
                array: Box::new(Self::from_base_input_expr(array, relation_oid)),
                subscripts: subscripts
                    .iter()
                    .map(|subscript| PlannerJoinArraySubscript {
                        is_slice: subscript.is_slice,
                        lower: subscript
                            .lower
                            .as_ref()
                            .map(|expr| Self::from_base_input_expr(expr, relation_oid)),
                        upper: subscript
                            .upper
                            .as_ref()
                            .map(|expr| Self::from_base_input_expr(expr, relation_oid)),
                    })
                    .collect(),
            },
            Expr::Random => Self::Random,
            Expr::JsonGet(left, right) => Self::JsonGet(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::JsonGetText(left, right) => Self::JsonGetText(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::JsonPath(left, right) => Self::JsonPath(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::JsonPathText(left, right) => Self::JsonPathText(
                Box::new(Self::from_base_input_expr(left, relation_oid)),
                Box::new(Self::from_base_input_expr(right, relation_oid)),
            ),
            Expr::FuncCall {
                func_oid,
                func,
                args,
                func_variadic,
            } => Self::FuncCall {
                func_oid: *func_oid,
                func: *func,
                args: args
                    .iter()
                    .map(|arg| Self::from_base_input_expr(arg, relation_oid))
                    .collect(),
                func_variadic: *func_variadic,
            },
            Expr::CurrentDate => Self::CurrentDate,
            Expr::CurrentTime { precision } => Self::CurrentTime {
                precision: *precision,
            },
            Expr::CurrentTimestamp { precision } => Self::CurrentTimestamp {
                precision: *precision,
            },
            Expr::LocalTime { precision } => Self::LocalTime {
                precision: *precision,
            },
            Expr::LocalTimestamp { precision } => Self::LocalTimestamp {
                precision: *precision,
            },
        }
    }

    pub fn into_input_expr(self) -> Expr {
        match self {
            Self::InputColumn(index) => Expr::Column(index),
            Self::SyntheticColumn { index, .. } => Expr::Column(index),
            Self::BaseColumn { index, .. } => Expr::Column(index),
            Self::LeftColumn(index) => Expr::Column(index),
            Self::RightColumn(index) => Expr::Column(index),
            Self::OuterColumn { depth, index } => Expr::OuterColumn { depth, index },
            Self::Const(value) => Expr::Const(value),
            Self::Add(left, right) => Expr::op_auto(
                OpExprKind::Add,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::Sub(left, right) => Expr::op_auto(
                OpExprKind::Sub,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::BitAnd(left, right) => Expr::op_auto(
                OpExprKind::BitAnd,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::BitOr(left, right) => Expr::op_auto(
                OpExprKind::BitOr,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::BitXor(left, right) => Expr::op_auto(
                OpExprKind::BitXor,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::Shl(left, right) => Expr::op_auto(
                OpExprKind::Shl,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::Shr(left, right) => Expr::op_auto(
                OpExprKind::Shr,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::Mul(left, right) => Expr::op_auto(
                OpExprKind::Mul,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::Div(left, right) => Expr::op_auto(
                OpExprKind::Div,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::Mod(left, right) => Expr::op_auto(
                OpExprKind::Mod,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::Concat(left, right) => Expr::op_auto(
                OpExprKind::Concat,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::UnaryPlus(inner) => Expr::op_auto(OpExprKind::UnaryPlus, vec![inner.into_input_expr()]),
            Self::Negate(inner) => Expr::op_auto(OpExprKind::Negate, vec![inner.into_input_expr()]),
            Self::BitNot(inner) => Expr::op_auto(OpExprKind::BitNot, vec![inner.into_input_expr()]),
            Self::Cast(inner, sql_type) => Expr::Cast(Box::new(inner.into_input_expr()), sql_type),
            Self::Eq(left, right) => Expr::op_auto(
                OpExprKind::Eq,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::NotEq(left, right) => Expr::op_auto(
                OpExprKind::NotEq,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::Lt(left, right) => Expr::op_auto(
                OpExprKind::Lt,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::LtEq(left, right) => Expr::op_auto(
                OpExprKind::LtEq,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::Gt(left, right) => Expr::op_auto(
                OpExprKind::Gt,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::GtEq(left, right) => Expr::op_auto(
                OpExprKind::GtEq,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::RegexMatch(left, right) => Expr::op_auto(
                OpExprKind::RegexMatch,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::Like {
                expr,
                pattern,
                escape,
                case_insensitive,
                negated,
            } => Expr::Like {
                expr: Box::new(expr.into_input_expr()),
                pattern: Box::new(pattern.into_input_expr()),
                escape: escape.map(|inner| Box::new(inner.into_input_expr())),
                case_insensitive,
                negated,
            },
            Self::Similar {
                expr,
                pattern,
                escape,
                negated,
            } => Expr::Similar {
                expr: Box::new(expr.into_input_expr()),
                pattern: Box::new(pattern.into_input_expr()),
                escape: escape.map(|inner| Box::new(inner.into_input_expr())),
                negated,
            },
            Self::And(left, right) => Expr::bool_expr(
                BoolExprType::And,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::Or(left, right) => Expr::bool_expr(
                BoolExprType::Or,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::Not(inner) => Expr::bool_expr(BoolExprType::Not, vec![inner.into_input_expr()]),
            Self::IsNull(inner) => Expr::IsNull(Box::new(inner.into_input_expr())),
            Self::IsNotNull(inner) => Expr::IsNotNull(Box::new(inner.into_input_expr())),
            Self::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::ArrayLiteral {
                elements,
                array_type,
            } => Expr::ArrayLiteral {
                elements: elements
                    .into_iter()
                    .map(|element| element.into_input_expr())
                    .collect(),
                array_type,
            },
            Self::ArrayOverlap(left, right) => Expr::op_auto(
                OpExprKind::ArrayOverlap,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::JsonbContains(left, right) => Expr::op_auto(
                OpExprKind::JsonbContains,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::JsonbContained(left, right) => Expr::op_auto(
                OpExprKind::JsonbContained,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::JsonbExists(left, right) => Expr::op_auto(
                OpExprKind::JsonbExists,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::JsonbExistsAny(left, right) => Expr::op_auto(
                OpExprKind::JsonbExistsAny,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::JsonbExistsAll(left, right) => Expr::op_auto(
                OpExprKind::JsonbExistsAll,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::JsonbPathExists(left, right) => Expr::op_auto(
                OpExprKind::JsonbPathExists,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::JsonbPathMatch(left, right) => Expr::op_auto(
                OpExprKind::JsonbPathMatch,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::SubLink(sublink) => Expr::SubLink(sublink),
            Self::SubPlan(subplan) => Expr::SubPlan(subplan),
            Self::Coalesce(left, right) => Expr::Coalesce(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::AnyArray { left, op, right } => {
                Expr::scalar_array_op(op, true, left.into_input_expr(), right.into_input_expr())
            }
            Self::AllArray { left, op, right } => {
                Expr::scalar_array_op(op, false, left.into_input_expr(), right.into_input_expr())
            }
            Self::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
                array: Box::new(array.into_input_expr()),
                subscripts: subscripts
                    .into_iter()
                    .map(|subscript| ExprArraySubscript {
                        is_slice: subscript.is_slice,
                        lower: subscript.lower.map(|expr| expr.into_input_expr()),
                        upper: subscript.upper.map(|expr| expr.into_input_expr()),
                    })
                    .collect(),
            },
            Self::Random => Expr::Random,
            Self::JsonGet(left, right) => Expr::op_auto(
                OpExprKind::JsonGet,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::JsonGetText(left, right) => Expr::op_auto(
                OpExprKind::JsonGetText,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::JsonPath(left, right) => Expr::op_auto(
                OpExprKind::JsonPath,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::JsonPathText(left, right) => Expr::op_auto(
                OpExprKind::JsonPathText,
                vec![left.into_input_expr(), right.into_input_expr()],
            ),
            Self::FuncCall {
                func_oid,
                args,
                func_variadic,
                ..
            } => Expr::func(
                func_oid,
                None,
                func_variadic,
                args.into_iter().map(|arg| arg.into_input_expr()).collect(),
            ),
            Self::CurrentDate => Expr::CurrentDate,
            Self::CurrentTime { precision } => Expr::CurrentTime { precision },
            Self::CurrentTimestamp { precision } => Expr::CurrentTimestamp { precision },
            Self::LocalTime { precision } => Expr::LocalTime { precision },
            Self::LocalTimestamp { precision } => Expr::LocalTimestamp { precision },
        }
    }

    pub fn from_expr(expr: &Expr, left_width: usize) -> Self {
        match expr {
            Expr::Op(op) => planner_join_from_op(op, |expr| Self::from_expr(expr, left_width)),
            Expr::Bool(bool_expr) => {
                planner_join_from_bool(bool_expr, |expr| Self::from_expr(expr, left_width))
            }
            Expr::Func(func) => planner_join_from_func(func, |expr| Self::from_expr(expr, left_width)),
            Expr::ScalarArrayOp(saop) => {
                planner_join_from_scalar_array(saop, |expr| Self::from_expr(expr, left_width))
            }
            Expr::Var(var) if var.varlevelsup > 0 => Self::OuterColumn {
                depth: var.varlevelsup - 1,
                index: var.varattno.saturating_sub(1),
            },
            Expr::Var(var) => {
                let index = var.varattno.saturating_sub(1);
                if index < left_width {
                    Self::LeftColumn(index)
                } else {
                    Self::RightColumn(index - left_width)
                }
            }
            Expr::Column(index) if *index < left_width => Self::LeftColumn(*index),
            Expr::Column(index) => Self::RightColumn(index - left_width),
            Expr::OuterColumn { depth, index } => Self::OuterColumn {
                depth: *depth,
                index: *index,
            },
            Expr::Const(value) => Self::Const(value.clone()),
            Expr::Add(left, right) => Self::Add(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::Sub(left, right) => Self::Sub(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::BitAnd(left, right) => Self::BitAnd(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::BitOr(left, right) => Self::BitOr(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::BitXor(left, right) => Self::BitXor(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::Shl(left, right) => Self::Shl(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::Shr(left, right) => Self::Shr(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::Mul(left, right) => Self::Mul(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::Div(left, right) => Self::Div(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::Mod(left, right) => Self::Mod(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::Concat(left, right) => Self::Concat(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::UnaryPlus(inner) => Self::UnaryPlus(Box::new(Self::from_expr(inner, left_width))),
            Expr::Negate(inner) => Self::Negate(Box::new(Self::from_expr(inner, left_width))),
            Expr::BitNot(inner) => Self::BitNot(Box::new(Self::from_expr(inner, left_width))),
            Expr::Cast(inner, sql_type) => {
                Self::Cast(Box::new(Self::from_expr(inner, left_width)), *sql_type)
            }
            Expr::Eq(left, right) => Self::Eq(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::NotEq(left, right) => Self::NotEq(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::Lt(left, right) => Self::Lt(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::LtEq(left, right) => Self::LtEq(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::Gt(left, right) => Self::Gt(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::GtEq(left, right) => Self::GtEq(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::RegexMatch(left, right) => Self::RegexMatch(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::Like {
                expr,
                pattern,
                escape,
                case_insensitive,
                negated,
            } => Self::Like {
                expr: Box::new(Self::from_expr(expr, left_width)),
                pattern: Box::new(Self::from_expr(pattern, left_width)),
                escape: escape
                    .as_ref()
                    .map(|inner| Box::new(Self::from_expr(inner, left_width))),
                case_insensitive: *case_insensitive,
                negated: *negated,
            },
            Expr::Similar {
                expr,
                pattern,
                escape,
                negated,
            } => Self::Similar {
                expr: Box::new(Self::from_expr(expr, left_width)),
                pattern: Box::new(Self::from_expr(pattern, left_width)),
                escape: escape
                    .as_ref()
                    .map(|inner| Box::new(Self::from_expr(inner, left_width))),
                negated: *negated,
            },
            Expr::And(left, right) => Self::And(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::Or(left, right) => Self::Or(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::Not(inner) => Self::Not(Box::new(Self::from_expr(inner, left_width))),
            Expr::IsNull(inner) => Self::IsNull(Box::new(Self::from_expr(inner, left_width))),
            Expr::IsNotNull(inner) => Self::IsNotNull(Box::new(Self::from_expr(inner, left_width))),
            Expr::IsDistinctFrom(left, right) => Self::IsDistinctFrom(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::IsNotDistinctFrom(left, right) => Self::IsNotDistinctFrom(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::ArrayLiteral {
                elements,
                array_type,
            } => Self::ArrayLiteral {
                elements: elements
                    .iter()
                    .map(|element| Self::from_expr(element, left_width))
                    .collect(),
                array_type: *array_type,
            },
            Expr::ArrayOverlap(left, right) => Self::ArrayOverlap(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::JsonbContains(left, right) => Self::JsonbContains(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::JsonbContained(left, right) => Self::JsonbContained(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::JsonbExists(left, right) => Self::JsonbExists(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::JsonbExistsAny(left, right) => Self::JsonbExistsAny(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::JsonbExistsAll(left, right) => Self::JsonbExistsAll(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::JsonbPathExists(left, right) => Self::JsonbPathExists(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::JsonbPathMatch(left, right) => Self::JsonbPathMatch(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::SubLink(sublink) => Self::SubLink(sublink.clone()),
            Expr::SubPlan(subplan) => Self::SubPlan(subplan.clone()),
            Expr::Coalesce(left, right) => Self::Coalesce(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::AnyArray { left, op, right } => Self::AnyArray {
                left: Box::new(Self::from_expr(left, left_width)),
                op: *op,
                right: Box::new(Self::from_expr(right, left_width)),
            },
            Expr::AllArray { left, op, right } => Self::AllArray {
                left: Box::new(Self::from_expr(left, left_width)),
                op: *op,
                right: Box::new(Self::from_expr(right, left_width)),
            },
            Expr::ArraySubscript { array, subscripts } => Self::ArraySubscript {
                array: Box::new(Self::from_expr(array, left_width)),
                subscripts: subscripts
                    .iter()
                    .map(|subscript| PlannerJoinArraySubscript {
                        is_slice: subscript.is_slice,
                        lower: subscript
                            .lower
                            .as_ref()
                            .map(|expr| Self::from_expr(expr, left_width)),
                        upper: subscript
                            .upper
                            .as_ref()
                            .map(|expr| Self::from_expr(expr, left_width)),
                    })
                    .collect(),
            },
            Expr::Random => Self::Random,
            Expr::JsonGet(left, right) => Self::JsonGet(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::JsonGetText(left, right) => Self::JsonGetText(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::JsonPath(left, right) => Self::JsonPath(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::JsonPathText(left, right) => Self::JsonPathText(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::FuncCall {
                func_oid,
                func,
                args,
                func_variadic,
            } => Self::FuncCall {
                func_oid: *func_oid,
                func: *func,
                args: args
                    .iter()
                    .map(|arg| Self::from_expr(arg, left_width))
                    .collect(),
                func_variadic: *func_variadic,
            },
            Expr::CurrentDate => Self::CurrentDate,
            Expr::CurrentTime { precision } => Self::CurrentTime {
                precision: *precision,
            },
            Expr::CurrentTimestamp { precision } => Self::CurrentTimestamp {
                precision: *precision,
            },
            Expr::LocalTime { precision } => Self::LocalTime {
                precision: *precision,
            },
            Expr::LocalTimestamp { precision } => Self::LocalTimestamp {
                precision: *precision,
            },
        }
    }

    pub fn into_expr(self, left_width: usize) -> Expr {
        match self {
            Self::InputColumn(index) => Expr::Column(index),
            Self::SyntheticColumn { index, .. } => Expr::Column(index),
            Self::BaseColumn { index, .. } => Expr::Column(index),
            Self::LeftColumn(index) => Expr::Column(index),
            Self::RightColumn(index) => Expr::Column(left_width + index),
            Self::OuterColumn { depth, index } => Expr::OuterColumn { depth, index },
            Self::Const(value) => Expr::Const(value),
            Self::Add(left, right) => Expr::Add(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::Sub(left, right) => Expr::Sub(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::BitAnd(left, right) => Expr::BitAnd(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::BitOr(left, right) => Expr::BitOr(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::BitXor(left, right) => Expr::BitXor(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::Shl(left, right) => Expr::Shl(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::Shr(left, right) => Expr::Shr(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::Mul(left, right) => Expr::Mul(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::Div(left, right) => Expr::Div(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::Mod(left, right) => Expr::Mod(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::Concat(left, right) => Expr::Concat(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::UnaryPlus(inner) => Expr::UnaryPlus(Box::new(inner.into_expr(left_width))),
            Self::Negate(inner) => Expr::Negate(Box::new(inner.into_expr(left_width))),
            Self::BitNot(inner) => Expr::BitNot(Box::new(inner.into_expr(left_width))),
            Self::Cast(inner, sql_type) => {
                Expr::Cast(Box::new(inner.into_expr(left_width)), sql_type)
            }
            Self::Eq(left, right) => Expr::Eq(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::NotEq(left, right) => Expr::NotEq(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::Lt(left, right) => Expr::Lt(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::LtEq(left, right) => Expr::LtEq(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::Gt(left, right) => Expr::Gt(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::GtEq(left, right) => Expr::GtEq(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::RegexMatch(left, right) => Expr::RegexMatch(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::Like {
                expr,
                pattern,
                escape,
                case_insensitive,
                negated,
            } => Expr::Like {
                expr: Box::new(expr.into_expr(left_width)),
                pattern: Box::new(pattern.into_expr(left_width)),
                escape: escape.map(|inner| Box::new(inner.into_expr(left_width))),
                case_insensitive,
                negated,
            },
            Self::Similar {
                expr,
                pattern,
                escape,
                negated,
            } => Expr::Similar {
                expr: Box::new(expr.into_expr(left_width)),
                pattern: Box::new(pattern.into_expr(left_width)),
                escape: escape.map(|inner| Box::new(inner.into_expr(left_width))),
                negated,
            },
            Self::And(left, right) => Expr::And(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::Or(left, right) => Expr::Or(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::Not(inner) => Expr::Not(Box::new(inner.into_expr(left_width))),
            Self::IsNull(inner) => Expr::IsNull(Box::new(inner.into_expr(left_width))),
            Self::IsNotNull(inner) => Expr::IsNotNull(Box::new(inner.into_expr(left_width))),
            Self::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::ArrayLiteral {
                elements,
                array_type,
            } => Expr::ArrayLiteral {
                elements: elements
                    .into_iter()
                    .map(|element| element.into_expr(left_width))
                    .collect(),
                array_type,
            },
            Self::ArrayOverlap(left, right) => Expr::ArrayOverlap(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::JsonbContains(left, right) => Expr::JsonbContains(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::JsonbContained(left, right) => Expr::JsonbContained(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::JsonbExists(left, right) => Expr::JsonbExists(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::JsonbExistsAny(left, right) => Expr::JsonbExistsAny(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::JsonbExistsAll(left, right) => Expr::JsonbExistsAll(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::JsonbPathExists(left, right) => Expr::JsonbPathExists(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::JsonbPathMatch(left, right) => Expr::JsonbPathMatch(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::SubLink(sublink) => Expr::SubLink(sublink),
            Self::SubPlan(subplan) => Expr::SubPlan(subplan),
            Self::Coalesce(left, right) => Expr::Coalesce(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::AnyArray { left, op, right } => Expr::AnyArray {
                left: Box::new(left.into_expr(left_width)),
                op,
                right: Box::new(right.into_expr(left_width)),
            },
            Self::AllArray { left, op, right } => Expr::AllArray {
                left: Box::new(left.into_expr(left_width)),
                op,
                right: Box::new(right.into_expr(left_width)),
            },
            Self::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
                array: Box::new(array.into_expr(left_width)),
                subscripts: subscripts
                    .into_iter()
                    .map(|subscript| ExprArraySubscript {
                        is_slice: subscript.is_slice,
                        lower: subscript.lower.map(|expr| expr.into_expr(left_width)),
                        upper: subscript.upper.map(|expr| expr.into_expr(left_width)),
                    })
                    .collect(),
            },
            Self::Random => Expr::Random,
            Self::JsonGet(left, right) => Expr::JsonGet(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::JsonGetText(left, right) => Expr::JsonGetText(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::JsonPath(left, right) => Expr::JsonPath(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::JsonPathText(left, right) => Expr::JsonPathText(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::FuncCall {
                func_oid,
                func,
                args,
                func_variadic,
            } => Expr::FuncCall {
                func_oid,
                func,
                args: args
                    .into_iter()
                    .map(|arg| arg.into_expr(left_width))
                    .collect(),
                func_variadic,
            },
            Self::CurrentDate => Expr::CurrentDate,
            Self::CurrentTime { precision } => Expr::CurrentTime { precision },
            Self::CurrentTimestamp { precision } => Expr::CurrentTimestamp { precision },
            Self::LocalTime { precision } => Expr::LocalTime { precision },
            Self::LocalTimestamp { precision } => Expr::LocalTimestamp { precision },
        }
    }

    pub fn swap_inputs(&self) -> Self {
        match self {
            Self::InputColumn(index) => Self::InputColumn(*index),
            Self::SyntheticColumn { slot_id, index } => Self::SyntheticColumn {
                slot_id: *slot_id,
                index: *index,
            },
            Self::BaseColumn {
                source_id,
                relation_oid,
                index,
            } => Self::BaseColumn {
                source_id: *source_id,
                relation_oid: *relation_oid,
                index: *index,
            },
            Self::LeftColumn(index) => Self::RightColumn(*index),
            Self::RightColumn(index) => Self::LeftColumn(*index),
            Self::OuterColumn { depth, index } => Self::OuterColumn {
                depth: *depth,
                index: *index,
            },
            Self::Const(value) => Self::Const(value.clone()),
            Self::Add(left, right) => {
                Self::Add(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::Sub(left, right) => {
                Self::Sub(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::BitAnd(left, right) => {
                Self::BitAnd(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::BitOr(left, right) => {
                Self::BitOr(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::BitXor(left, right) => {
                Self::BitXor(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::Shl(left, right) => {
                Self::Shl(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::Shr(left, right) => {
                Self::Shr(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::Mul(left, right) => {
                Self::Mul(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::Div(left, right) => {
                Self::Div(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::Mod(left, right) => {
                Self::Mod(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::Concat(left, right) => {
                Self::Concat(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::UnaryPlus(inner) => Self::UnaryPlus(Box::new(inner.swap_inputs())),
            Self::Negate(inner) => Self::Negate(Box::new(inner.swap_inputs())),
            Self::BitNot(inner) => Self::BitNot(Box::new(inner.swap_inputs())),
            Self::Cast(inner, sql_type) => Self::Cast(Box::new(inner.swap_inputs()), *sql_type),
            Self::Eq(left, right) => {
                Self::Eq(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::NotEq(left, right) => {
                Self::NotEq(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::Lt(left, right) => {
                Self::Lt(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::LtEq(left, right) => {
                Self::LtEq(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::Gt(left, right) => {
                Self::Gt(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::GtEq(left, right) => {
                Self::GtEq(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::RegexMatch(left, right) => {
                Self::RegexMatch(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::Like {
                expr,
                pattern,
                escape,
                case_insensitive,
                negated,
            } => Self::Like {
                expr: Box::new(expr.swap_inputs()),
                pattern: Box::new(pattern.swap_inputs()),
                escape: escape.as_ref().map(|inner| Box::new(inner.swap_inputs())),
                case_insensitive: *case_insensitive,
                negated: *negated,
            },
            Self::Similar {
                expr,
                pattern,
                escape,
                negated,
            } => Self::Similar {
                expr: Box::new(expr.swap_inputs()),
                pattern: Box::new(pattern.swap_inputs()),
                escape: escape.as_ref().map(|inner| Box::new(inner.swap_inputs())),
                negated: *negated,
            },
            Self::And(left, right) => {
                Self::And(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::Or(left, right) => {
                Self::Or(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::Not(inner) => Self::Not(Box::new(inner.swap_inputs())),
            Self::IsNull(inner) => Self::IsNull(Box::new(inner.swap_inputs())),
            Self::IsNotNull(inner) => Self::IsNotNull(Box::new(inner.swap_inputs())),
            Self::IsDistinctFrom(left, right) => {
                Self::IsDistinctFrom(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::IsNotDistinctFrom(left, right) => {
                Self::IsNotDistinctFrom(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::ArrayLiteral {
                elements,
                array_type,
            } => Self::ArrayLiteral {
                elements: elements.iter().map(Self::swap_inputs).collect(),
                array_type: *array_type,
            },
            Self::ArrayOverlap(left, right) => {
                Self::ArrayOverlap(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::JsonbContains(left, right) => {
                Self::JsonbContains(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::JsonbContained(left, right) => {
                Self::JsonbContained(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::JsonbExists(left, right) => {
                Self::JsonbExists(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::JsonbExistsAny(left, right) => {
                Self::JsonbExistsAny(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::JsonbExistsAll(left, right) => {
                Self::JsonbExistsAll(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::JsonbPathExists(left, right) => {
                Self::JsonbPathExists(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::JsonbPathMatch(left, right) => {
                Self::JsonbPathMatch(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::SubLink(sublink) => Self::SubLink(sublink.clone()),
            Self::SubPlan(subplan) => Self::SubPlan(subplan.clone()),
            Self::Coalesce(left, right) => {
                Self::Coalesce(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::AnyArray { left, op, right } => Self::AnyArray {
                left: Box::new(left.swap_inputs()),
                op: *op,
                right: Box::new(right.swap_inputs()),
            },
            Self::AllArray { left, op, right } => Self::AllArray {
                left: Box::new(left.swap_inputs()),
                op: *op,
                right: Box::new(right.swap_inputs()),
            },
            Self::ArraySubscript { array, subscripts } => Self::ArraySubscript {
                array: Box::new(array.swap_inputs()),
                subscripts: subscripts
                    .iter()
                    .map(|subscript| PlannerJoinArraySubscript {
                        is_slice: subscript.is_slice,
                        lower: subscript.lower.as_ref().map(Self::swap_inputs),
                        upper: subscript.upper.as_ref().map(Self::swap_inputs),
                    })
                    .collect(),
            },
            Self::Random => Self::Random,
            Self::JsonGet(left, right) => {
                Self::JsonGet(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::JsonGetText(left, right) => {
                Self::JsonGetText(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::JsonPath(left, right) => {
                Self::JsonPath(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::JsonPathText(left, right) => {
                Self::JsonPathText(Box::new(left.swap_inputs()), Box::new(right.swap_inputs()))
            }
            Self::FuncCall {
                func_oid,
                func,
                args,
                func_variadic,
            } => Self::FuncCall {
                func_oid: *func_oid,
                func: *func,
                args: args.iter().map(Self::swap_inputs).collect(),
                func_variadic: *func_variadic,
            },
            Self::CurrentDate => Self::CurrentDate,
            Self::CurrentTime { precision } => Self::CurrentTime {
                precision: *precision,
            },
            Self::CurrentTimestamp { precision } => Self::CurrentTimestamp {
                precision: *precision,
            },
            Self::LocalTime { precision } => Self::LocalTime {
                precision: *precision,
            },
            Self::LocalTimestamp { precision } => Self::LocalTimestamp {
                precision: *precision,
            },
        }
    }
}
