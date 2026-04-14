use super::*;
use crate::include::nodes::primnodes::{SubLink, SubPlan};
use crate::include::executor::execdesc::CommandType;
use crate::include::nodes::parsenodes::{JoinTreeNode, Query, RangeTblEntry, RangeTblEntryKind};
use crate::include::nodes::primnodes::{ExprArraySubscript, SortGroupClause, Var};

#[derive(Debug)]
struct DecomposedQuery {
    from: BoundFromPlan,
    where_qual: Option<Expr>,
    group_by: Vec<Expr>,
    accumulators: Vec<AggAccum>,
    having_qual: Option<Expr>,
    target_list: Option<Vec<TargetEntry>>,
    project_set: Option<Vec<ProjectSetTarget>>,
    sort_clause: Vec<OrderByEntry>,
    limit_count: Option<usize>,
    limit_offset: usize,
}

#[derive(Default)]
struct QueryRteBuilder {
    rtable: Vec<RangeTblEntry>,
}

pub(super) fn analyze_select_query_with_outer(
    stmt: &SelectStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<GroupedOuterScope>,
    outer_ctes: &[BoundCte],
    expanded_views: &[u32],
) -> Result<(Query, BoundScope), ParseError> {
    let (plan, scope) = super::bind_select_query_with_outer(
        stmt,
        catalog,
        outer_scopes,
        grouped_outer,
        outer_ctes,
        expanded_views,
    )?;
    Ok((query_from_bound_select_plan(plan), scope))
}

pub(super) fn analyze_values_query_with_outer(
    stmt: &ValuesStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<GroupedOuterScope>,
    outer_ctes: &[BoundCte],
    expanded_views: &[u32],
) -> Result<(Query, BoundScope), ParseError> {
    let (plan, scope) = super::bind_values_query_with_outer(
        stmt,
        catalog,
        outer_scopes,
        grouped_outer,
        outer_ctes,
        expanded_views,
    )?;
    Ok((query_from_bound_select_plan(plan), scope))
}

pub(super) fn query_from_bound_select_plan(plan: BoundSelectPlan) -> Query {
    let decomposed = decompose_bound_select_plan(plan);
    let mut builder = QueryRteBuilder::default();
    let (jointree, output_columns, output_exprs) = builder.from_bound_from_plan(decomposed.from);
    let has_agg = !decomposed.group_by.is_empty()
        || !decomposed.accumulators.is_empty()
        || decomposed.having_qual.is_some();
    let has_project_set = decomposed.project_set.is_some();
    let target_list = normalize_target_list(decomposed.target_list.map_or_else(
        || identity_target_list(&output_columns, &output_exprs),
        |targets| {
            if has_agg || has_project_set {
                targets
            } else {
                rewrite_target_entries(targets, &output_exprs)
            }
        },
    ));
    let sort_inputs = if has_agg || has_project_set {
        decomposed.sort_clause
    } else {
        rewrite_order_by_entries(decomposed.sort_clause, &output_exprs)
    };
    let sort_clause = sort_inputs
        .into_iter()
        .enumerate()
        .map(|(index, item)| SortGroupClause {
            expr: item.expr,
            tle_sort_group_ref: index + 1,
            descending: item.descending,
            nulls_first: item.nulls_first,
        })
        .collect();
    Query {
        command_type: CommandType::Select,
        rtable: builder.rtable,
        jointree,
        target_list,
        where_qual: decomposed
            .where_qual
            .map(|expr| rewrite_expr_columns(expr, &output_exprs)),
        group_by: decomposed
            .group_by
            .into_iter()
            .map(|expr| rewrite_expr_columns(expr, &output_exprs))
            .collect(),
        accumulators: rewrite_agg_accums(decomposed.accumulators, &output_exprs),
        having_qual: decomposed.having_qual,
        sort_clause,
        limit_count: decomposed.limit_count,
        limit_offset: decomposed.limit_offset,
        project_set: decomposed
            .project_set
            .map(|targets| rewrite_project_set_targets(targets, &output_exprs)),
    }
}

fn decompose_bound_select_plan(plan: BoundSelectPlan) -> DecomposedQuery {
    match plan {
        BoundSelectPlan::From(from) => DecomposedQuery {
            from,
            where_qual: None,
            group_by: Vec::new(),
            accumulators: Vec::new(),
            having_qual: None,
            target_list: None,
            project_set: None,
            sort_clause: Vec::new(),
            limit_count: None,
            limit_offset: 0,
        },
        BoundSelectPlan::Filter { input, predicate } => {
            let mut query = decompose_bound_select_plan(*input);
            query.where_qual = Some(predicate);
            query
        }
        BoundSelectPlan::OrderBy { input, items } => {
            let mut query = decompose_bound_select_plan(*input);
            query.sort_clause = items;
            query
        }
        BoundSelectPlan::Limit {
            input,
            limit,
            offset,
        } => {
            let mut query = decompose_bound_select_plan(*input);
            query.limit_count = limit;
            query.limit_offset = offset;
            query
        }
        BoundSelectPlan::Aggregate {
            input,
            group_by,
            accumulators,
            having,
            ..
        } => {
            let mut query = decompose_bound_select_plan(*input);
            query.group_by = group_by;
            query.accumulators = accumulators;
            query.having_qual = having;
            query
        }
        BoundSelectPlan::Projection { input, targets } => {
            let mut query = decompose_bound_select_plan(*input);
            query.target_list = Some(targets);
            query
        }
        BoundSelectPlan::ProjectSet { input, targets } => {
            let mut query = decompose_bound_select_plan(*input);
            query.project_set = Some(targets);
            query
        }
    }
}

impl QueryRteBuilder {
    fn push_rte(&mut self, rte: RangeTblEntry) -> usize {
        self.rtable.push(rte);
        self.rtable.len()
    }

    fn from_bound_from_plan(
        &mut self,
        plan: BoundFromPlan,
    ) -> (Option<JoinTreeNode>, Vec<QueryColumn>, Vec<Expr>) {
        match plan {
            BoundFromPlan::Result => (None, Vec::new(), Vec::new()),
            BoundFromPlan::SeqScan {
                rel,
                relation_oid,
                toast,
                desc,
            } => {
                let columns = desc
                    .columns
                    .iter()
                    .map(|column| QueryColumn {
                        name: column.name.clone(),
                        sql_type: column.sql_type,
                    })
                    .collect::<Vec<_>>();
                let rtindex = self.push_rte(RangeTblEntry {
                    alias: None,
                    desc,
                    kind: RangeTblEntryKind::Relation {
                        rel,
                        relation_oid,
                        toast,
                    },
                });
                let output_exprs = rte_output_exprs(rtindex, &columns);
                (
                    Some(JoinTreeNode::RangeTblRef(rtindex)),
                    columns,
                    output_exprs,
                )
            }
            BoundFromPlan::Values {
                rows,
                output_columns,
            } => {
                let desc = RelationDesc {
                    columns: output_columns
                        .iter()
                        .map(|column| column_desc(column.name.clone(), column.sql_type, true))
                        .collect(),
                };
                let rtindex = self.push_rte(RangeTblEntry {
                    alias: None,
                    desc,
                    kind: RangeTblEntryKind::Values {
                        rows,
                        output_columns: output_columns.clone(),
                    },
                });
                let output_exprs = rte_output_exprs(rtindex, &output_columns);
                (
                    Some(JoinTreeNode::RangeTblRef(rtindex)),
                    output_columns,
                    output_exprs,
                )
            }
            BoundFromPlan::FunctionScan { call } => {
                let output_columns = call.output_columns().to_vec();
                let desc = RelationDesc {
                    columns: output_columns
                        .iter()
                        .map(|column| column_desc(column.name.clone(), column.sql_type, true))
                        .collect(),
                };
                let rtindex = self.push_rte(RangeTblEntry {
                    alias: None,
                    desc,
                    kind: RangeTblEntryKind::Function { call },
                });
                let output_exprs = rte_output_exprs(rtindex, &output_columns);
                (
                    Some(JoinTreeNode::RangeTblRef(rtindex)),
                    output_columns,
                    output_exprs,
                )
            }
            BoundFromPlan::NestedLoopJoin {
                left,
                right,
                kind,
                on,
            } => {
                let (left_tree, mut left_columns, mut left_exprs) =
                    self.from_bound_from_plan(*left);
                let (right_tree, right_columns, right_exprs) = self.from_bound_from_plan(*right);
                left_columns.extend(right_columns);
                left_exprs.extend(right_exprs);
                let jointree = match (left_tree, right_tree) {
                    (Some(left), Some(right)) => Some(JoinTreeNode::JoinExpr {
                        left: Box::new(left),
                        right: Box::new(right),
                        kind,
                        quals: rewrite_expr_columns(on, &left_exprs),
                    }),
                    (Some(tree), None) | (None, Some(tree)) => Some(tree),
                    (None, None) => None,
                };
                (jointree, left_columns, left_exprs)
            }
            BoundFromPlan::Projection { input, targets } => {
                let query = query_from_bound_from_projection(*input, targets);
                let desc = RelationDesc {
                    columns: query
                        .columns()
                        .into_iter()
                        .map(|column| column_desc(column.name, column.sql_type, true))
                        .collect(),
                };
                let output_columns = query.columns();
                let rtindex = self.push_rte(RangeTblEntry {
                    alias: None,
                    desc,
                    kind: RangeTblEntryKind::Subquery {
                        query: Box::new(query),
                    },
                });
                let output_exprs = rte_output_exprs(rtindex, &output_columns);
                (
                    Some(JoinTreeNode::RangeTblRef(rtindex)),
                    output_columns,
                    output_exprs,
                )
            }
            BoundFromPlan::Subquery(query) => {
                let desc = RelationDesc {
                    columns: query
                        .columns()
                        .into_iter()
                        .map(|column| column_desc(column.name, column.sql_type, true))
                        .collect(),
                };
                let output_columns = query.columns();
                let rtindex = self.push_rte(RangeTblEntry {
                    alias: None,
                    desc,
                    kind: RangeTblEntryKind::Subquery { query },
                });
                let output_exprs = rte_output_exprs(rtindex, &output_columns);
                (
                    Some(JoinTreeNode::RangeTblRef(rtindex)),
                    output_columns,
                    output_exprs,
                )
            }
        }
    }
}

fn query_from_bound_from_projection(input: BoundFromPlan, targets: Vec<TargetEntry>) -> Query {
    let mut builder = QueryRteBuilder::default();
    let (jointree, output_columns, output_exprs) = builder.from_bound_from_plan(input);
    let target_list = normalize_target_list(if targets.is_empty() {
        identity_target_list(&output_columns, &output_exprs)
    } else {
        rewrite_target_entries(targets, &output_exprs)
    });
    Query {
        command_type: CommandType::Select,
        rtable: builder.rtable,
        jointree,
        target_list,
        where_qual: None,
        group_by: Vec::new(),
        accumulators: Vec::new(),
        having_qual: None,
        sort_clause: Vec::new(),
        limit_count: None,
        limit_offset: 0,
        project_set: None,
    }
}

fn identity_target_list(columns: &[QueryColumn], output_exprs: &[Expr]) -> Vec<TargetEntry> {
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            TargetEntry::new(
                column.name.clone(),
                output_exprs
                    .get(index)
                    .cloned()
                    .unwrap_or(Expr::Column(index)),
                column.sql_type,
                index + 1,
            )
        })
        .collect()
}

fn normalize_target_list(mut targets: Vec<TargetEntry>) -> Vec<TargetEntry> {
    for (index, target) in targets.iter_mut().enumerate() {
        target.resno = index + 1;
    }
    targets
}

fn rte_output_exprs(rtindex: usize, columns: &[QueryColumn]) -> Vec<Expr> {
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            Expr::Var(Var {
                varno: rtindex,
                varattno: index + 1,
                varlevelsup: 0,
                vartype: column.sql_type,
            })
        })
        .collect()
}

fn rewrite_target_entries(targets: Vec<TargetEntry>, output_exprs: &[Expr]) -> Vec<TargetEntry> {
    targets
        .into_iter()
        .map(|target| TargetEntry {
            expr: rewrite_expr_columns(target.expr, output_exprs),
            ..target
        })
        .collect()
}

fn rewrite_order_by_entries(items: Vec<OrderByEntry>, output_exprs: &[Expr]) -> Vec<OrderByEntry> {
    items
        .into_iter()
        .map(|item| OrderByEntry {
            expr: rewrite_expr_columns(item.expr, output_exprs),
            descending: item.descending,
            nulls_first: item.nulls_first,
        })
        .collect()
}

fn rewrite_project_set_targets(
    targets: Vec<ProjectSetTarget>,
    output_exprs: &[Expr],
) -> Vec<ProjectSetTarget> {
    targets
        .into_iter()
        .map(|target| match target {
            ProjectSetTarget::Scalar(entry) => ProjectSetTarget::Scalar(TargetEntry {
                expr: rewrite_expr_columns(entry.expr, output_exprs),
                ..entry
            }),
            ProjectSetTarget::Set {
                name,
                call,
                sql_type,
                column_index,
            } => ProjectSetTarget::Set {
                name,
                call: rewrite_set_returning_call(call, output_exprs),
                sql_type,
                column_index,
            },
        })
        .collect()
}

fn rewrite_set_returning_call(call: SetReturningCall, output_exprs: &[Expr]) -> SetReturningCall {
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
            start: rewrite_expr_columns(start, output_exprs),
            stop: rewrite_expr_columns(stop, output_exprs),
            step: rewrite_expr_columns(step, output_exprs),
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
                .map(|expr| rewrite_expr_columns(expr, output_exprs))
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
                .map(|expr| rewrite_expr_columns(expr, output_exprs))
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
                .map(|expr| rewrite_expr_columns(expr, output_exprs))
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
                .map(|expr| rewrite_expr_columns(expr, output_exprs))
                .collect(),
            output_columns,
        },
    }
}

fn rewrite_agg_accums(accumulators: Vec<AggAccum>, output_exprs: &[Expr]) -> Vec<AggAccum> {
    accumulators
        .into_iter()
        .map(|accum| AggAccum {
            args: accum
                .args
                .into_iter()
                .map(|expr| rewrite_expr_columns(expr, output_exprs))
                .collect(),
            ..accum
        })
        .collect()
}

fn rewrite_expr_columns(expr: Expr, output_exprs: &[Expr]) -> Expr {
    let expr = expr.into_legacy_shape();
    let rewritten = match expr {
        Expr::Column(index) => output_exprs
            .get(index)
            .cloned()
            .unwrap_or(Expr::Column(index)),
        Expr::OuterColumn { .. } | Expr::Var(_) | Expr::Const(_) | Expr::Random => expr,
        Expr::Add(left, right) => Expr::Add(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::Sub(left, right) => Expr::Sub(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::BitAnd(left, right) => Expr::BitAnd(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::BitOr(left, right) => Expr::BitOr(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::BitXor(left, right) => Expr::BitXor(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::Shl(left, right) => Expr::Shl(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::Shr(left, right) => Expr::Shr(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::Mul(left, right) => Expr::Mul(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::Div(left, right) => Expr::Div(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::Mod(left, right) => Expr::Mod(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::Concat(left, right) => Expr::Concat(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::UnaryPlus(inner) => {
            Expr::UnaryPlus(Box::new(rewrite_expr_columns(*inner, output_exprs)))
        }
        Expr::Negate(inner) => Expr::Negate(Box::new(rewrite_expr_columns(*inner, output_exprs))),
        Expr::BitNot(inner) => Expr::BitNot(Box::new(rewrite_expr_columns(*inner, output_exprs))),
        Expr::Cast(inner, ty) => {
            Expr::Cast(Box::new(rewrite_expr_columns(*inner, output_exprs)), ty)
        }
        Expr::Eq(left, right) => Expr::Eq(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::NotEq(left, right) => Expr::NotEq(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::Lt(left, right) => Expr::Lt(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::LtEq(left, right) => Expr::LtEq(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::Gt(left, right) => Expr::Gt(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::GtEq(left, right) => Expr::GtEq(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::RegexMatch(left, right) => Expr::RegexMatch(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(rewrite_expr_columns(*expr, output_exprs)),
            pattern: Box::new(rewrite_expr_columns(*pattern, output_exprs)),
            escape: escape.map(|expr| Box::new(rewrite_expr_columns(*expr, output_exprs))),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(rewrite_expr_columns(*expr, output_exprs)),
            pattern: Box::new(rewrite_expr_columns(*pattern, output_exprs)),
            escape: escape.map(|expr| Box::new(rewrite_expr_columns(*expr, output_exprs))),
            negated,
        },
        Expr::And(left, right) => Expr::And(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::Or(left, right) => Expr::Or(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::Not(inner) => Expr::Not(Box::new(rewrite_expr_columns(*inner, output_exprs))),
        Expr::IsNull(inner) => Expr::IsNull(Box::new(rewrite_expr_columns(*inner, output_exprs))),
        Expr::IsNotNull(inner) => {
            Expr::IsNotNull(Box::new(rewrite_expr_columns(*inner, output_exprs)))
        }
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|expr| rewrite_expr_columns(expr, output_exprs))
                .collect(),
            array_type,
        },
        Expr::ArrayOverlap(left, right) => Expr::ArrayOverlap(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::JsonbContains(left, right) => Expr::JsonbContains(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::JsonbContained(left, right) => Expr::JsonbContained(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::JsonbExists(left, right) => Expr::JsonbExists(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::JsonbExistsAny(left, right) => Expr::JsonbExistsAny(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::JsonbExistsAll(left, right) => Expr::JsonbExistsAll(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::JsonbPathExists(left, right) => Expr::JsonbPathExists(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::JsonbPathMatch(left, right) => Expr::JsonbPathMatch(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::SubLink(sublink) => Expr::SubLink(Box::new(SubLink {
            testexpr: sublink
                .testexpr
                .map(|expr| Box::new(rewrite_expr_columns(*expr, output_exprs))),
            ..*sublink
        })),
        Expr::SubPlan(subplan) => Expr::SubPlan(Box::new(SubPlan {
            testexpr: subplan
                .testexpr
                .map(|expr| Box::new(rewrite_expr_columns(*expr, output_exprs))),
            ..*subplan
        })),
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::AnyArray { left, op, right } => Expr::AnyArray {
            left: Box::new(rewrite_expr_columns(*left, output_exprs)),
            op,
            right: Box::new(rewrite_expr_columns(*right, output_exprs)),
        },
        Expr::AllArray { left, op, right } => Expr::AllArray {
            left: Box::new(rewrite_expr_columns(*left, output_exprs)),
            op,
            right: Box::new(rewrite_expr_columns(*right, output_exprs)),
        },
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(rewrite_expr_columns(*array, output_exprs)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript
                        .lower
                        .map(|expr| rewrite_expr_columns(expr, output_exprs)),
                    upper: subscript
                        .upper
                        .map(|expr| rewrite_expr_columns(expr, output_exprs)),
                })
                .collect(),
        },
        Expr::JsonGet(left, right) => Expr::JsonGet(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::JsonGetText(left, right) => Expr::JsonGetText(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::JsonPath(left, right) => Expr::JsonPath(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::JsonPathText(left, right) => Expr::JsonPathText(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::FuncCall {
            func_oid,
            func,
            args,
            func_variadic,
        } => Expr::FuncCall {
            func_oid,
            func,
            args: args
                .into_iter()
                .map(|expr| rewrite_expr_columns(expr, output_exprs))
                .collect(),
            func_variadic,
        },
        Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => expr,
        Expr::Op(_) | Expr::Bool(_) | Expr::Func(_) | Expr::ScalarArrayOp(_) => {
            unreachable!("legacy rewrite should not see PG-shaped Expr")
        }
    };
    rewritten.into_pg_semantic_shape()
}
