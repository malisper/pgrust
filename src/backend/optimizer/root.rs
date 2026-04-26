use crate::backend::parser::{CatalogLookup, ParseError};
use crate::include::catalog::builtin_aggregate_function_for_proc_oid;
use crate::include::executor::execdesc::CommandType;
use crate::include::nodes::parsenodes::{
    JoinTreeNode, Query, QueryRowMark, RangeTblEntry, RangeTblEntryKind, SelectLockingClause,
};
use crate::include::nodes::pathnodes::{
    AggregateLayout, PathTarget, PlannerConfig, PlannerInfo, RelOptInfo,
};
use crate::include::nodes::primnodes::{
    AggAccum, AggFunc, Aggref, Expr, SetReturningCall, SortGroupClause, SubLink, SubLinkType,
    TargetEntry, Var, expr_contains_set_returning, is_system_attr, set_returning_call_exprs,
};

use super::path::flatten_and_conjuncts;

use super::joininfo::build_special_join_info;
use super::pathnodes::expr_sql_type;

pub(super) fn prepare_query_for_planning(query: Query, catalog: &dyn CatalogLookup) -> Query {
    prepare_query_tree_for_planning(query, catalog)
}

fn prepare_query_tree_for_planning(query: Query, catalog: &dyn CatalogLookup) -> Query {
    let query = rewrite_target_outer_aggregate_sublinks(query, catalog);
    super::constfold::fold_query_constants_best_effort(rewrite_minmax_aggregate_query(query))
}

pub(super) fn prepare_query_for_locking(query: Query) -> Result<Query, ParseError> {
    prepare_query_for_locking_with_inherited(query, None)
}

fn prepare_query_for_locking_with_inherited(
    mut query: Query,
    inherited_lock: Option<SelectLockingClause>,
) -> Result<Query, ParseError> {
    let effective_lock = match (query.locking_clause, inherited_lock) {
        (Some(local), Some(inherited)) => Some(local.strongest(inherited)),
        (Some(local), None) => Some(local),
        (None, Some(inherited)) => Some(inherited),
        (None, None) => None,
    };

    if let Some(strength) = effective_lock {
        validate_select_locking(&query, strength)?;
    }

    query.rtable = query
        .rtable
        .into_iter()
        .map(|rte| prepare_rte_for_locking(rte, effective_lock))
        .collect::<Result<Vec<_>, _>>()?;
    query.target_list = query
        .target_list
        .into_iter()
        .map(prepare_target_entry_for_locking)
        .collect::<Result<Vec<_>, _>>()?;
    query.where_qual = query.where_qual.map(prepare_expr_for_locking).transpose()?;
    query.group_by = query
        .group_by
        .into_iter()
        .map(prepare_expr_for_locking)
        .collect::<Result<Vec<_>, _>>()?;
    query.accumulators = query
        .accumulators
        .into_iter()
        .map(prepare_agg_accum_for_locking)
        .collect::<Result<Vec<_>, _>>()?;
    query.window_clauses = query
        .window_clauses
        .into_iter()
        .map(prepare_window_clause_for_locking)
        .collect::<Result<Vec<_>, _>>()?;
    query.having_qual = query
        .having_qual
        .map(prepare_expr_for_locking)
        .transpose()?;
    query.sort_clause = query
        .sort_clause
        .into_iter()
        .map(prepare_sort_clause_for_locking)
        .collect::<Result<Vec<_>, _>>()?;
    query.recursive_union = query
        .recursive_union
        .map(|recursive_union| prepare_recursive_union_for_locking(*recursive_union).map(Box::new))
        .transpose()?;
    query.set_operation = query
        .set_operation
        .map(|set_operation| prepare_set_operation_for_locking(*set_operation).map(Box::new))
        .transpose()?;

    query.row_marks.clear();
    if let Some(strength) = effective_lock {
        let mut row_marks = Vec::new();
        if let Some(jointree) = &query.jointree {
            collect_query_row_marks(&query.rtable, jointree, strength, &mut row_marks);
        }
        row_marks.sort_by_key(|mark| mark.rtindex);
        row_marks.dedup_by(|left, right| {
            if left.rtindex == right.rtindex {
                left.strength = left.strength.strongest(right.strength);
                true
            } else {
                false
            }
        });
        query.row_marks = row_marks;
    }
    query.locking_clause = None;
    Ok(query)
}

fn validate_select_locking(query: &Query, strength: SelectLockingClause) -> Result<(), ParseError> {
    if query.set_operation.is_some() {
        return Err(ParseError::FeatureNotSupportedMessage(format!(
            "{} is not allowed with UNION/INTERSECT/EXCEPT",
            strength.sql()
        )));
    }
    if query.distinct {
        return Err(ParseError::FeatureNotSupportedMessage(format!(
            "{} is not allowed with DISTINCT clause",
            strength.sql()
        )));
    }
    if !query.group_by.is_empty() {
        return Err(ParseError::FeatureNotSupportedMessage(format!(
            "{} is not allowed with GROUP BY clause",
            strength.sql()
        )));
    }
    if query.having_qual.is_some() {
        return Err(ParseError::FeatureNotSupportedMessage(format!(
            "{} is not allowed with HAVING clause",
            strength.sql()
        )));
    }
    if !query.accumulators.is_empty() {
        return Err(ParseError::FeatureNotSupportedMessage(format!(
            "{} is not allowed with aggregate functions",
            strength.sql()
        )));
    }
    if !query.window_clauses.is_empty() {
        return Err(ParseError::FeatureNotSupportedMessage(format!(
            "{} is not allowed with window functions",
            strength.sql()
        )));
    }
    if query.has_target_srfs {
        return Err(ParseError::FeatureNotSupportedMessage(format!(
            "{} is not allowed with set-returning functions in the target list",
            strength.sql()
        )));
    }
    if query.recursive_union.is_some() {
        return Err(ParseError::FeatureNotSupported(
            "FOR UPDATE/SHARE in a recursive query is not implemented".into(),
        ));
    }
    if jointree_contains_values(query.jointree.as_ref(), &query.rtable) {
        return Err(ParseError::FeatureNotSupportedMessage(format!(
            "{} cannot be applied to VALUES",
            strength.sql()
        )));
    }
    Ok(())
}

fn jointree_contains_values(jointree: Option<&JoinTreeNode>, rtable: &[RangeTblEntry]) -> bool {
    let Some(jointree) = jointree else {
        return false;
    };
    match jointree {
        JoinTreeNode::RangeTblRef(rtindex) => rtable
            .get(rtindex.saturating_sub(1))
            .is_some_and(|rte| matches!(rte.kind, RangeTblEntryKind::Values { .. })),
        JoinTreeNode::JoinExpr { left, right, .. } => {
            jointree_contains_values(Some(left), rtable)
                || jointree_contains_values(Some(right), rtable)
        }
    }
}

fn collect_query_row_marks(
    rtable: &[RangeTblEntry],
    jointree: &JoinTreeNode,
    strength: SelectLockingClause,
    row_marks: &mut Vec<QueryRowMark>,
) {
    match jointree {
        JoinTreeNode::RangeTblRef(rtindex) => {
            let Some(rte) = rtable.get(rtindex.saturating_sub(1)) else {
                return;
            };
            match rte.kind {
                RangeTblEntryKind::Relation { .. } => {
                    if let Some(existing) = row_marks
                        .iter_mut()
                        .find(|row_mark| row_mark.rtindex == *rtindex)
                    {
                        existing.strength = existing.strength.strongest(strength);
                    } else {
                        row_marks.push(QueryRowMark {
                            rtindex: *rtindex,
                            strength,
                        });
                    }
                }
                RangeTblEntryKind::Join { .. }
                | RangeTblEntryKind::Values { .. }
                | RangeTblEntryKind::Function { .. }
                | RangeTblEntryKind::WorkTable { .. }
                | RangeTblEntryKind::Cte { .. }
                | RangeTblEntryKind::Subquery { .. }
                | RangeTblEntryKind::Result => {}
            }
        }
        JoinTreeNode::JoinExpr { left, right, .. } => {
            collect_query_row_marks(rtable, left, strength, row_marks);
            collect_query_row_marks(rtable, right, strength, row_marks);
        }
    }
}

fn prepare_rte_for_locking(
    rte: RangeTblEntry,
    inherited_lock: Option<SelectLockingClause>,
) -> Result<RangeTblEntry, ParseError> {
    Ok(RangeTblEntry {
        security_quals: rte
            .security_quals
            .into_iter()
            .map(prepare_expr_for_locking)
            .collect::<Result<Vec<_>, _>>()?,
        kind: match rte.kind {
            RangeTblEntryKind::Subquery { query } => RangeTblEntryKind::Subquery {
                query: Box::new(prepare_query_for_locking_with_inherited(
                    *query,
                    inherited_lock,
                )?),
            },
            RangeTblEntryKind::Cte { cte_id, query } => RangeTblEntryKind::Cte {
                cte_id,
                query: Box::new(prepare_query_for_locking_with_inherited(*query, None)?),
            },
            RangeTblEntryKind::Values {
                rows,
                output_columns,
            } => RangeTblEntryKind::Values {
                rows: rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(prepare_expr_for_locking)
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                output_columns,
            },
            RangeTblEntryKind::Function { call } => RangeTblEntryKind::Function {
                call: prepare_set_returning_call_for_locking(call)?,
            },
            other => other,
        },
        ..rte
    })
}

fn prepare_target_entry_for_locking(target: TargetEntry) -> Result<TargetEntry, ParseError> {
    Ok(TargetEntry {
        expr: prepare_expr_for_locking(target.expr)?,
        ..target
    })
}

fn prepare_sort_clause_for_locking(item: SortGroupClause) -> Result<SortGroupClause, ParseError> {
    Ok(SortGroupClause {
        expr: prepare_expr_for_locking(item.expr)?,
        ..item
    })
}

fn prepare_agg_accum_for_locking(accum: AggAccum) -> Result<AggAccum, ParseError> {
    Ok(AggAccum {
        args: accum
            .args
            .into_iter()
            .map(prepare_expr_for_locking)
            .collect::<Result<Vec<_>, _>>()?,
        order_by: accum
            .order_by
            .into_iter()
            .map(|item| {
                Ok(crate::include::nodes::primnodes::OrderByEntry {
                    expr: prepare_expr_for_locking(item.expr)?,
                    ..item
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
        filter: accum.filter.map(prepare_expr_for_locking).transpose()?,
        ..accum
    })
}

fn prepare_window_clause_for_locking(
    clause: crate::include::nodes::primnodes::WindowClause,
) -> Result<crate::include::nodes::primnodes::WindowClause, ParseError> {
    Ok(crate::include::nodes::primnodes::WindowClause {
        spec: crate::include::nodes::primnodes::WindowSpec {
            partition_by: clause
                .spec
                .partition_by
                .into_iter()
                .map(prepare_expr_for_locking)
                .collect::<Result<Vec<_>, _>>()?,
            order_by: clause
                .spec
                .order_by
                .into_iter()
                .map(|item| {
                    Ok(crate::include::nodes::primnodes::OrderByEntry {
                        expr: prepare_expr_for_locking(item.expr)?,
                        ..item
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
            frame: prepare_window_frame_for_locking(clause.spec.frame)?,
        },
        functions: clause
            .functions
            .into_iter()
            .map(prepare_window_func_for_locking)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn prepare_window_frame_for_locking(
    frame: crate::include::nodes::primnodes::WindowFrame,
) -> Result<crate::include::nodes::primnodes::WindowFrame, ParseError> {
    use crate::include::nodes::primnodes::WindowFrameBound;

    let prepare_bound = |bound| match bound {
        WindowFrameBound::OffsetPreceding(offset) => {
            let expr = prepare_expr_for_locking(offset.expr.clone())?;
            Ok(WindowFrameBound::OffsetPreceding(offset.with_expr(expr)))
        }
        WindowFrameBound::OffsetFollowing(offset) => {
            let expr = prepare_expr_for_locking(offset.expr.clone())?;
            Ok(WindowFrameBound::OffsetFollowing(offset.with_expr(expr)))
        }
        other => Ok(other),
    };
    Ok(crate::include::nodes::primnodes::WindowFrame {
        mode: frame.mode,
        start_bound: prepare_bound(frame.start_bound)?,
        end_bound: prepare_bound(frame.end_bound)?,
    })
}

fn prepare_window_func_for_locking(
    func: crate::include::nodes::primnodes::WindowFuncExpr,
) -> Result<crate::include::nodes::primnodes::WindowFuncExpr, ParseError> {
    use crate::include::nodes::primnodes::WindowFuncKind;

    Ok(crate::include::nodes::primnodes::WindowFuncExpr {
        kind: match func.kind {
            WindowFuncKind::Aggregate(aggref) => WindowFuncKind::Aggregate(Aggref {
                args: aggref
                    .args
                    .into_iter()
                    .map(prepare_expr_for_locking)
                    .collect::<Result<Vec<_>, _>>()?,
                aggorder: aggref
                    .aggorder
                    .into_iter()
                    .map(|item| {
                        Ok(crate::include::nodes::primnodes::OrderByEntry {
                            expr: prepare_expr_for_locking(item.expr)?,
                            ..item
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                aggfilter: aggref.aggfilter.map(prepare_expr_for_locking).transpose()?,
                ..aggref
            }),
            WindowFuncKind::Builtin(kind) => WindowFuncKind::Builtin(kind),
        },
        args: func
            .args
            .into_iter()
            .map(prepare_expr_for_locking)
            .collect::<Result<Vec<_>, _>>()?,
        ..func
    })
}

fn prepare_set_returning_call_for_locking(
    call: SetReturningCall,
) -> Result<SetReturningCall, ParseError> {
    Ok(match call {
        SetReturningCall::GenerateSeries {
            func_oid,
            func_variadic,
            start,
            stop,
            step,
            timezone,
            output_columns,
            with_ordinality,
        } => SetReturningCall::GenerateSeries {
            func_oid,
            func_variadic,
            start: prepare_expr_for_locking(start)?,
            stop: prepare_expr_for_locking(stop)?,
            step: prepare_expr_for_locking(step)?,
            timezone: timezone.map(prepare_expr_for_locking).transpose()?,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::PartitionTree {
            func_oid,
            func_variadic,
            relid,
            output_columns,
        } => SetReturningCall::PartitionTree {
            func_oid,
            func_variadic,
            relid: prepare_expr_for_locking(relid)?,
            output_columns,
        },
        SetReturningCall::PartitionAncestors {
            func_oid,
            func_variadic,
            relid,
            output_columns,
        } => SetReturningCall::PartitionAncestors {
            func_oid,
            func_variadic,
            relid: prepare_expr_for_locking(relid)?,
            output_columns,
        },
        SetReturningCall::PgLockStatus {
            func_oid,
            func_variadic,
            output_columns,
            with_ordinality,
        } => SetReturningCall::PgLockStatus {
            func_oid,
            func_variadic,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::TxidSnapshotXip {
            func_oid,
            func_variadic,
            arg,
            output_columns,
            with_ordinality,
        } => SetReturningCall::TxidSnapshotXip {
            func_oid,
            func_variadic,
            arg: prepare_expr_for_locking(arg)?,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::Unnest {
            func_oid,
            func_variadic,
            args,
            output_columns,
            with_ordinality,
        } => SetReturningCall::Unnest {
            func_oid,
            func_variadic,
            args: args
                .into_iter()
                .map(prepare_expr_for_locking)
                .collect::<Result<Vec<_>, _>>()?,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::JsonTableFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
            with_ordinality,
        } => SetReturningCall::JsonTableFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(prepare_expr_for_locking)
                .collect::<Result<Vec<_>, _>>()?,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::JsonRecordFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
            record_type,
            with_ordinality,
        } => SetReturningCall::JsonRecordFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(prepare_expr_for_locking)
                .collect::<Result<Vec<_>, _>>()?,
            output_columns,
            record_type,
            with_ordinality,
        },
        SetReturningCall::RegexTableFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
            with_ordinality,
        } => SetReturningCall::RegexTableFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(prepare_expr_for_locking)
                .collect::<Result<Vec<_>, _>>()?,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::StringTableFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
            with_ordinality,
        } => SetReturningCall::StringTableFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(prepare_expr_for_locking)
                .collect::<Result<Vec<_>, _>>()?,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::TextSearchTableFunction {
            kind,
            args,
            output_columns,
            with_ordinality,
        } => SetReturningCall::TextSearchTableFunction {
            kind,
            args: args
                .into_iter()
                .map(prepare_expr_for_locking)
                .collect::<Result<Vec<_>, _>>()?,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::UserDefined {
            proc_oid,
            func_variadic,
            args,
            output_columns,
            with_ordinality,
        } => SetReturningCall::UserDefined {
            proc_oid,
            func_variadic,
            args: args
                .into_iter()
                .map(prepare_expr_for_locking)
                .collect::<Result<Vec<_>, _>>()?,
            output_columns,
            with_ordinality,
        },
    })
}

fn prepare_recursive_union_for_locking(
    recursive_union: crate::include::nodes::parsenodes::RecursiveUnionQuery,
) -> Result<crate::include::nodes::parsenodes::RecursiveUnionQuery, ParseError> {
    Ok(crate::include::nodes::parsenodes::RecursiveUnionQuery {
        output_desc: recursive_union.output_desc,
        anchor: prepare_query_for_locking_with_inherited(recursive_union.anchor, None)?,
        recursive: prepare_query_for_locking_with_inherited(recursive_union.recursive, None)?,
        distinct: recursive_union.distinct,
        recursive_references_worktable: recursive_union.recursive_references_worktable,
        worktable_id: recursive_union.worktable_id,
    })
}

fn prepare_set_operation_for_locking(
    set_operation: crate::include::nodes::parsenodes::SetOperationQuery,
) -> Result<crate::include::nodes::parsenodes::SetOperationQuery, ParseError> {
    Ok(crate::include::nodes::parsenodes::SetOperationQuery {
        output_desc: set_operation.output_desc,
        op: set_operation.op,
        inputs: set_operation
            .inputs
            .into_iter()
            .map(|query| prepare_query_for_locking_with_inherited(query, None))
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn prepare_expr_for_locking(expr: Expr) -> Result<Expr, ParseError> {
    Ok(match expr {
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(Aggref {
            args: aggref
                .args
                .into_iter()
                .map(prepare_expr_for_locking)
                .collect::<Result<Vec<_>, _>>()?,
            aggorder: aggref
                .aggorder
                .into_iter()
                .map(|item| {
                    Ok(crate::include::nodes::primnodes::OrderByEntry {
                        expr: prepare_expr_for_locking(item.expr)?,
                        ..item
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
            aggfilter: aggref.aggfilter.map(prepare_expr_for_locking).transpose()?,
            ..*aggref
        })),
        Expr::WindowFunc(window_func) => {
            Expr::WindowFunc(Box::new(prepare_window_func_for_locking(*window_func)?))
        }
        Expr::Op(op) => Expr::Op(Box::new(crate::include::nodes::primnodes::OpExpr {
            args: op
                .args
                .into_iter()
                .map(prepare_expr_for_locking)
                .collect::<Result<Vec<_>, _>>()?,
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(crate::include::nodes::primnodes::BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(prepare_expr_for_locking)
                .collect::<Result<Vec<_>, _>>()?,
            ..*bool_expr
        })),
        Expr::Case(case_expr) => Expr::Case(Box::new(crate::include::nodes::primnodes::CaseExpr {
            arg: case_expr
                .arg
                .map(|arg| prepare_expr_for_locking(*arg).map(Box::new))
                .transpose()?,
            args: case_expr
                .args
                .into_iter()
                .map(|arm| {
                    Ok(crate::include::nodes::primnodes::CaseWhen {
                        expr: prepare_expr_for_locking(arm.expr)?,
                        result: prepare_expr_for_locking(arm.result)?,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
            defresult: Box::new(prepare_expr_for_locking(*case_expr.defresult)?),
            ..*case_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(crate::include::nodes::primnodes::FuncExpr {
            args: func
                .args
                .into_iter()
                .map(prepare_expr_for_locking)
                .collect::<Result<Vec<_>, _>>()?,
            ..*func
        })),
        Expr::SubLink(sublink) => Expr::SubLink(Box::new(SubLink {
            testexpr: sublink
                .testexpr
                .map(|expr| prepare_expr_for_locking(*expr).map(Box::new))
                .transpose()?,
            subselect: Box::new(prepare_query_for_locking_with_inherited(
                *sublink.subselect,
                None,
            )?),
            ..*sublink
        })),
        Expr::SubPlan(subplan) => {
            Expr::SubPlan(Box::new(crate::include::nodes::primnodes::SubPlan {
                testexpr: subplan
                    .testexpr
                    .map(|expr| prepare_expr_for_locking(*expr).map(Box::new))
                    .transpose()?,
                args: subplan
                    .args
                    .into_iter()
                    .map(prepare_expr_for_locking)
                    .collect::<Result<Vec<_>, _>>()?,
                ..*subplan
            }))
        }
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(
            crate::include::nodes::primnodes::ScalarArrayOpExpr {
                left: Box::new(prepare_expr_for_locking(*saop.left)?),
                right: Box::new(prepare_expr_for_locking(*saop.right)?),
                ..*saop
            },
        )),
        Expr::Xml(xml) => Expr::Xml(Box::new(crate::include::nodes::primnodes::XmlExpr {
            named_args: xml
                .named_args
                .into_iter()
                .map(prepare_expr_for_locking)
                .collect::<Result<Vec<_>, _>>()?,
            args: xml
                .args
                .into_iter()
                .map(prepare_expr_for_locking)
                .collect::<Result<Vec<_>, _>>()?,
            ..*xml
        })),
        Expr::Cast(inner, ty) => Expr::Cast(Box::new(prepare_expr_for_locking(*inner)?), ty),
        Expr::Collate {
            expr,
            collation_oid,
        } => Expr::Collate {
            expr: Box::new(prepare_expr_for_locking(*expr)?),
            collation_oid,
        },
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
            collation_oid,
        } => Expr::Like {
            expr: Box::new(prepare_expr_for_locking(*expr)?),
            pattern: Box::new(prepare_expr_for_locking(*pattern)?),
            escape: escape
                .map(|expr| prepare_expr_for_locking(*expr).map(Box::new))
                .transpose()?,
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
            expr: Box::new(prepare_expr_for_locking(*expr)?),
            pattern: Box::new(prepare_expr_for_locking(*pattern)?),
            escape: escape
                .map(|expr| prepare_expr_for_locking(*expr).map(Box::new))
                .transpose()?,
            negated,
            collation_oid,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(prepare_expr_for_locking(*inner)?)),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(prepare_expr_for_locking(*inner)?)),
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(prepare_expr_for_locking(*left)?),
            Box::new(prepare_expr_for_locking(*right)?),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(prepare_expr_for_locking(*left)?),
            Box::new(prepare_expr_for_locking(*right)?),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(prepare_expr_for_locking)
                .collect::<Result<Vec<_>, _>>()?,
            array_type,
        },
        Expr::Row { descriptor, fields } => Expr::Row {
            descriptor,
            fields: fields
                .into_iter()
                .map(|(name, expr)| Ok((name, prepare_expr_for_locking(expr)?)))
                .collect::<Result<Vec<_>, _>>()?,
        },
        Expr::FieldSelect {
            expr,
            field,
            field_type,
        } => Expr::FieldSelect {
            expr: Box::new(prepare_expr_for_locking(*expr)?),
            field,
            field_type,
        },
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(prepare_expr_for_locking(*left)?),
            Box::new(prepare_expr_for_locking(*right)?),
        ),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(prepare_expr_for_locking(*array)?),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| {
                    Ok(crate::include::nodes::primnodes::ExprArraySubscript {
                        is_slice: subscript.is_slice,
                        lower: subscript.lower.map(prepare_expr_for_locking).transpose()?,
                        upper: subscript.upper.map(prepare_expr_for_locking).transpose()?,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        },
        other => other,
    })
}

impl PlannerInfo {
    pub fn new(parse: Query, aggregate_layout: AggregateLayout) -> Self {
        Self::new_with_config(parse, aggregate_layout, PlannerConfig::default())
    }

    pub fn new_with_config(
        parse: Query,
        aggregate_layout: AggregateLayout,
        mut config: PlannerConfig,
    ) -> Self {
        if !parse.row_marks.is_empty() {
            config.retain_partial_index_filters = true;
        }
        let processed_tlist = make_processed_tlist(&parse);
        let final_target = PathTarget::from_target_list(&parse.target_list);
        let query_pathkeys = PathTarget::from_sort_clause(&parse.sort_clause, &processed_tlist);
        let sort_input_target = make_sort_input_target(&parse, &processed_tlist, &final_target);
        let group_input_target = if has_grouping(&parse) {
            make_group_input_target(&parse, &aggregate_layout.group_by)
        } else {
            sort_input_target.clone()
        };
        let grouped_target = if has_grouping(&parse) {
            build_grouped_target(&aggregate_layout, &parse.accumulators)
        } else {
            final_target.clone()
        };
        let window_input_target = if has_windowing(&parse) {
            make_window_input_target(&parse, &processed_tlist, &grouped_target)
        } else {
            sort_input_target.clone()
        };
        let scanjoin_target = build_scanjoin_target(
            &parse,
            &group_input_target,
            &window_input_target,
            &sort_input_target,
            &final_target,
        );
        let simple_rel_array = build_simple_rel_array(&parse.rtable);
        let join_info_list = build_special_join_info(&parse);
        Self {
            config,
            processed_tlist,
            scanjoin_target,
            group_input_target,
            grouped_target,
            window_input_target,
            sort_input_target,
            final_target,
            query_pathkeys,
            simple_rel_array,
            append_rel_infos: vec![None; parse.rtable.len() + 1],
            join_rel_list: Vec::new(),
            upper_rels: Vec::new(),
            join_info_list,
            inner_join_clauses: Vec::new(),
            aggregate_layout,
            final_rel: None,
            parse,
        }
    }

    pub fn aggregate_group_by(&self) -> &[Expr] {
        &self.aggregate_layout.group_by
    }

    pub fn aggregate_passthrough_exprs(&self) -> &[Expr] {
        &self.aggregate_layout.passthrough_exprs
    }
}

fn rewrite_minmax_aggregate_query(query: Query) -> Query {
    if !query.group_by.is_empty()
        || query.having_qual.is_some()
        || !query.window_clauses.is_empty()
        || query.has_target_srfs
        || query.recursive_union.is_some()
        || query.set_operation.is_some()
        || query.accumulators.is_empty()
    {
        return query;
    }

    let Some(rewritten_aggs) = query
        .accumulators
        .iter()
        .map(|accum| build_minmax_sublink(&query, accum))
        .collect::<Option<Vec<_>>>()
    else {
        return query;
    };

    let target_list = query
        .target_list
        .iter()
        .cloned()
        .map(|mut target| {
            target.expr = rewrite_minmax_aggrefs(target.expr, &rewritten_aggs);
            target
        })
        .collect::<Vec<_>>();

    if target_list
        .iter()
        .any(|target| expr_contains_local_var_outside_subquery(&target.expr))
    {
        return query;
    }

    Query {
        command_type: query.command_type,
        depends_on_row_security: query.depends_on_row_security,
        rtable: Vec::new(),
        jointree: None,
        target_list,
        distinct: query.distinct,
        distinct_on: query.distinct_on,
        where_qual: None,
        group_by: Vec::new(),
        accumulators: Vec::new(),
        window_clauses: Vec::new(),
        having_qual: None,
        // Ungrouped aggregate queries always produce at most one row, so any
        // outer ORDER BY is semantically redundant after the rewrite.
        sort_clause: Vec::new(),
        limit_count: query.limit_count,
        limit_offset: query.limit_offset,
        locking_clause: query.locking_clause,
        row_marks: query.row_marks,
        has_target_srfs: false,
        recursive_union: None,
        set_operation: None,
    }
}

fn rewrite_target_outer_aggregate_sublinks(mut query: Query, catalog: &dyn CatalogLookup) -> Query {
    for target_idx in 0..query.target_list.len() {
        let expr = query.target_list[target_idx].expr.clone();
        if let Some(rewritten) = rewrite_target_outer_aggregate_sublink_expr(expr, catalog) {
            query.target_list[target_idx].expr = rewritten;
        }
    }
    query
}

fn rewrite_target_outer_aggregate_sublink_expr(
    expr: Expr,
    catalog: &dyn CatalogLookup,
) -> Option<Expr> {
    let Expr::SubLink(outer_sublink) = expr else {
        return None;
    };
    if !matches!(outer_sublink.sublink_type, SubLinkType::ExprSubLink)
        || outer_sublink.testexpr.is_some()
    {
        return None;
    }
    let aggregate_query = &outer_sublink.subselect;
    if !aggregate_query.rtable.is_empty()
        || aggregate_query.jointree.is_some()
        || aggregate_query.target_list.len() != 1
        || aggregate_query.where_qual.is_some()
        || !aggregate_query.group_by.is_empty()
        || !aggregate_query.window_clauses.is_empty()
        || aggregate_query.having_qual.is_some()
        || !aggregate_query.sort_clause.is_empty()
        || aggregate_query.limit_count.is_some()
        || aggregate_query.limit_offset != 0
        || aggregate_query.locking_clause.is_some()
        || !aggregate_query.row_marks.is_empty()
        || aggregate_query.has_target_srfs
        || aggregate_query.recursive_union.is_some()
        || aggregate_query.set_operation.is_some()
    {
        return None;
    }
    let Expr::Aggref(aggref) = aggregate_query.target_list[0].expr.clone() else {
        return None;
    };
    if builtin_aggregate_function_for_proc_oid(aggref.aggfnoid)
        .is_none_or(|func| !matches!(func, AggFunc::Min | AggFunc::Max))
        || aggref.aggvariadic
        || aggref.aggdistinct
        || !aggref.aggorder.is_empty()
        || aggref.aggfilter.is_some()
    {
        return None;
    }
    if aggref.args.len() != 1 {
        return None;
    }
    let Expr::SubLink(inner_sublink) = aggref.args[0].clone() else {
        return None;
    };
    if !scalar_sublink_has_unique_lookup(&inner_sublink, catalog) {
        return None;
    }
    Some(Expr::SubLink(inner_sublink))
}

fn scalar_sublink_has_unique_lookup(
    sublink: &crate::include::nodes::primnodes::SubLink,
    catalog: &dyn CatalogLookup,
) -> bool {
    if !matches!(sublink.sublink_type, SubLinkType::ExprSubLink) || sublink.testexpr.is_some() {
        return false;
    }
    let subquery = &sublink.subselect;
    if !matches!(subquery.command_type, CommandType::Select)
        || !subquery.group_by.is_empty()
        || !subquery.accumulators.is_empty()
        || !subquery.window_clauses.is_empty()
        || subquery.having_qual.is_some()
        || !subquery.sort_clause.is_empty()
        || subquery.limit_count.is_some()
        || subquery.limit_offset != 0
        || subquery.locking_clause.is_some()
        || !subquery.row_marks.is_empty()
        || subquery.has_target_srfs
        || subquery.recursive_union.is_some()
        || subquery.set_operation.is_some()
        || subquery.target_list.len() != 1
        || subquery.target_list[0].resjunk
    {
        return false;
    }

    let Some(JoinTreeNode::RangeTblRef(1)) = &subquery.jointree else {
        return false;
    };
    if subquery.rtable.len() != 1 {
        return false;
    }
    let Some(where_qual) = &subquery.where_qual else {
        return false;
    };
    if expr_contains_sublink_for_minmax_rewrite(where_qual) {
        return false;
    }
    if !matches!(
        subquery.target_list[0].expr,
        Expr::Var(Var {
            varno: 1,
            varlevelsup: 0,
            ..
        })
    ) {
        return false;
    }

    let RangeTblEntryKind::Relation { relation_oid, .. } = subquery.rtable[0].kind else {
        return false;
    };
    let equality_columns = flatten_and_conjuncts(where_qual)
        .into_iter()
        .filter_map(unique_lookup_equality_column)
        .collect::<Vec<_>>();
    if equality_columns.is_empty() {
        return false;
    }

    catalog
        .index_relations_for_heap(relation_oid)
        .into_iter()
        .any(|index| {
            index.index_meta.indisunique
                && index.index_meta.indisvalid
                && index.index_meta.indisready
                && index.index_meta.indpred.is_none()
                && index.index_exprs.is_empty()
                && index
                    .index_meta
                    .indkey
                    .iter()
                    .take(index.index_meta.indnkeyatts.max(0) as usize)
                    .all(|attno| {
                        *attno > 0 && equality_columns.iter().any(|column| column == attno)
                    })
        })
}

fn unique_lookup_equality_column(expr: Expr) -> Option<i16> {
    let Expr::Op(op) = strip_unique_lookup_casts(expr) else {
        return None;
    };
    if op.op != crate::include::nodes::primnodes::OpExprKind::Eq || op.args.len() != 2 {
        return None;
    }
    let left = strip_unique_lookup_casts(op.args[0].clone());
    let right = strip_unique_lookup_casts(op.args[1].clone());
    unique_lookup_column_match(&left, &right).or_else(|| unique_lookup_column_match(&right, &left))
}

fn unique_lookup_column_match(local: &Expr, other: &Expr) -> Option<i16> {
    let Expr::Var(var) = local else {
        return None;
    };
    if var.varno != 1 || var.varlevelsup != 0 || is_system_attr(var.varattno) {
        return None;
    }
    let Ok(attno) = i16::try_from(var.varattno) else {
        return None;
    };
    (!expr_contains_local_var_for_unique_lookup(other)).then_some(attno)
}

fn expr_contains_local_var_for_unique_lookup(expr: &Expr) -> bool {
    match expr {
        Expr::Var(var) => var.varlevelsup == 0,
        Expr::Aggref(aggref) => {
            aggref
                .args
                .iter()
                .any(expr_contains_local_var_for_unique_lookup)
                || aggref
                    .aggfilter
                    .as_ref()
                    .is_some_and(expr_contains_local_var_for_unique_lookup)
        }
        Expr::WindowFunc(window_func) => window_func
            .args
            .iter()
            .any(expr_contains_local_var_for_unique_lookup),
        Expr::Op(op) => op
            .args
            .iter()
            .any(expr_contains_local_var_for_unique_lookup),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .any(expr_contains_local_var_for_unique_lookup),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_deref()
                .is_some_and(expr_contains_local_var_for_unique_lookup)
                || case_expr.args.iter().any(|arm| {
                    expr_contains_local_var_for_unique_lookup(&arm.expr)
                        || expr_contains_local_var_for_unique_lookup(&arm.result)
                })
                || expr_contains_local_var_for_unique_lookup(&case_expr.defresult)
        }
        Expr::Func(func) => func
            .args
            .iter()
            .any(expr_contains_local_var_for_unique_lookup),
        Expr::SetReturning(srf) => set_returning_call_exprs(&srf.call)
            .into_iter()
            .any(expr_contains_local_var_for_unique_lookup),
        Expr::ScalarArrayOp(saop) => {
            expr_contains_local_var_for_unique_lookup(&saop.left)
                || expr_contains_local_var_for_unique_lookup(&saop.right)
        }
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => expr_contains_local_var_for_unique_lookup(inner),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Expr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_local_var_for_unique_lookup(expr)
                || expr_contains_local_var_for_unique_lookup(pattern)
                || escape
                    .as_deref()
                    .is_some_and(expr_contains_local_var_for_unique_lookup)
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_contains_local_var_for_unique_lookup(left)
                || expr_contains_local_var_for_unique_lookup(right)
        }
        Expr::ArrayLiteral { elements, .. } => elements
            .iter()
            .any(expr_contains_local_var_for_unique_lookup),
        Expr::Row { fields, .. } => fields
            .iter()
            .any(|(_, expr)| expr_contains_local_var_for_unique_lookup(expr)),
        Expr::FieldSelect { expr, .. } => expr_contains_local_var_for_unique_lookup(expr),
        Expr::ArraySubscript { array, subscripts } => {
            expr_contains_local_var_for_unique_lookup(array)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(expr_contains_local_var_for_unique_lookup)
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(expr_contains_local_var_for_unique_lookup)
                })
        }
        Expr::Xml(xml) => xml
            .child_exprs()
            .any(expr_contains_local_var_for_unique_lookup),
        Expr::SubLink(_) | Expr::SubPlan(_) => true,
        Expr::Param(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::Random
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
    }
}

fn strip_unique_lookup_casts(expr: Expr) -> Expr {
    match expr {
        Expr::Cast(inner, _) => strip_unique_lookup_casts(*inner),
        Expr::Collate { expr, .. } => strip_unique_lookup_casts(*expr),
        other => other,
    }
}

fn build_minmax_sublink(query: &Query, accum: &AggAccum) -> Option<Expr> {
    let func = builtin_aggregate_function_for_proc_oid(accum.aggfnoid)?;
    let descending = match func {
        AggFunc::Min => false,
        AggFunc::Max => true,
        _ => return None,
    };
    if accum.agg_variadic || accum.distinct || !accum.order_by.is_empty() || accum.filter.is_some()
    {
        return None;
    }
    let [arg] = accum.args.as_slice() else {
        return None;
    };
    if !is_minmax_indexable_arg(arg) {
        return None;
    }
    if query
        .where_qual
        .as_ref()
        .is_some_and(expr_contains_sublink_for_minmax_rewrite)
    {
        return None;
    }

    let target =
        TargetEntry::new("?column?", arg.clone(), accum.sql_type, 1).with_sort_group_ref(1);
    let sort_clause = vec![SortGroupClause {
        expr: arg.clone(),
        tle_sort_group_ref: 1,
        descending,
        nulls_first: None,
        collation_oid: None,
    }];
    let where_qual = combine_quals([
        query
            .where_qual
            .clone()
            .map(raise_outer_varlevels_for_minmax_rewrite),
        accum.filter.clone(),
        Some(Expr::IsNotNull(Box::new(arg.clone()))),
    ]);
    let subselect = Query {
        command_type: CommandType::Select,
        depends_on_row_security: query.depends_on_row_security,
        rtable: query.rtable.clone(),
        jointree: query.jointree.clone(),
        target_list: vec![target],
        distinct: false,
        distinct_on: Vec::new(),
        where_qual,
        group_by: Vec::new(),
        accumulators: Vec::new(),
        window_clauses: Vec::new(),
        having_qual: None,
        sort_clause,
        limit_count: Some(1),
        limit_offset: 0,
        locking_clause: None,
        row_marks: Vec::new(),
        has_target_srfs: false,
        recursive_union: None,
        set_operation: None,
    };
    Some(Expr::SubLink(Box::new(SubLink {
        sublink_type: SubLinkType::ExprSubLink,
        testexpr: None,
        subselect: Box::new(subselect),
    })))
}

fn is_minmax_indexable_arg(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Var(var) if var.varlevelsup == 0 && !is_system_attr(var.varattno)
    )
}

fn raise_outer_varlevels_for_minmax_rewrite(expr: Expr) -> Expr {
    match expr {
        Expr::Var(mut var) if var.varlevelsup > 0 => {
            var.varlevelsup += 1;
            Expr::Var(var)
        }
        Expr::Op(op) => Expr::Op(Box::new(crate::include::nodes::primnodes::OpExpr {
            args: op
                .args
                .into_iter()
                .map(raise_outer_varlevels_for_minmax_rewrite)
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(crate::include::nodes::primnodes::BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(raise_outer_varlevels_for_minmax_rewrite)
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(crate::include::nodes::primnodes::FuncExpr {
            args: func
                .args
                .into_iter()
                .map(raise_outer_varlevels_for_minmax_rewrite)
                .collect(),
            ..*func
        })),
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(
            crate::include::nodes::primnodes::ScalarArrayOpExpr {
                left: Box::new(raise_outer_varlevels_for_minmax_rewrite(*saop.left)),
                right: Box::new(raise_outer_varlevels_for_minmax_rewrite(*saop.right)),
                ..*saop
            },
        )),
        Expr::Cast(inner, ty) => Expr::Cast(
            Box::new(raise_outer_varlevels_for_minmax_rewrite(*inner)),
            ty,
        ),
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(raise_outer_varlevels_for_minmax_rewrite(*left)),
            Box::new(raise_outer_varlevels_for_minmax_rewrite(*right)),
        ),
        Expr::IsNull(inner) => {
            Expr::IsNull(Box::new(raise_outer_varlevels_for_minmax_rewrite(*inner)))
        }
        Expr::IsNotNull(inner) => {
            Expr::IsNotNull(Box::new(raise_outer_varlevels_for_minmax_rewrite(*inner)))
        }
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(raise_outer_varlevels_for_minmax_rewrite(*left)),
            Box::new(raise_outer_varlevels_for_minmax_rewrite(*right)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(raise_outer_varlevels_for_minmax_rewrite(*left)),
            Box::new(raise_outer_varlevels_for_minmax_rewrite(*right)),
        ),
        other => other,
    }
}

fn expr_contains_sublink_for_minmax_rewrite(expr: &Expr) -> bool {
    match expr {
        Expr::SubLink(_) | Expr::SubPlan(_) => true,
        Expr::Aggref(aggref) => {
            aggref
                .args
                .iter()
                .any(expr_contains_sublink_for_minmax_rewrite)
                || aggref
                    .aggfilter
                    .as_ref()
                    .is_some_and(expr_contains_sublink_for_minmax_rewrite)
        }
        Expr::WindowFunc(window_func) => window_func
            .args
            .iter()
            .any(expr_contains_sublink_for_minmax_rewrite),
        Expr::Op(op) => op.args.iter().any(expr_contains_sublink_for_minmax_rewrite),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .any(expr_contains_sublink_for_minmax_rewrite),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_deref()
                .is_some_and(expr_contains_sublink_for_minmax_rewrite)
                || case_expr.args.iter().any(|arm| {
                    expr_contains_sublink_for_minmax_rewrite(&arm.expr)
                        || expr_contains_sublink_for_minmax_rewrite(&arm.result)
                })
                || expr_contains_sublink_for_minmax_rewrite(&case_expr.defresult)
        }
        Expr::Func(func) => func
            .args
            .iter()
            .any(expr_contains_sublink_for_minmax_rewrite),
        Expr::SetReturning(srf) => set_returning_call_exprs(&srf.call)
            .into_iter()
            .any(expr_contains_sublink_for_minmax_rewrite),
        Expr::ScalarArrayOp(saop) => {
            expr_contains_sublink_for_minmax_rewrite(&saop.left)
                || expr_contains_sublink_for_minmax_rewrite(&saop.right)
        }
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => expr_contains_sublink_for_minmax_rewrite(inner),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Expr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_sublink_for_minmax_rewrite(expr)
                || expr_contains_sublink_for_minmax_rewrite(pattern)
                || escape
                    .as_deref()
                    .is_some_and(expr_contains_sublink_for_minmax_rewrite)
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_contains_sublink_for_minmax_rewrite(left)
                || expr_contains_sublink_for_minmax_rewrite(right)
        }
        Expr::ArrayLiteral { elements, .. } => elements
            .iter()
            .any(expr_contains_sublink_for_minmax_rewrite),
        Expr::Row { fields, .. } => fields
            .iter()
            .any(|(_, expr)| expr_contains_sublink_for_minmax_rewrite(expr)),
        Expr::FieldSelect { expr, .. } => expr_contains_sublink_for_minmax_rewrite(expr),
        Expr::ArraySubscript { array, subscripts } => {
            expr_contains_sublink_for_minmax_rewrite(array)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(expr_contains_sublink_for_minmax_rewrite)
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(expr_contains_sublink_for_minmax_rewrite)
                })
        }
        Expr::Xml(xml) => xml
            .child_exprs()
            .any(expr_contains_sublink_for_minmax_rewrite),
        Expr::Param(_)
        | Expr::Var(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::Random
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
    }
}

fn combine_quals<const N: usize>(quals: [Option<Expr>; N]) -> Option<Expr> {
    super::path::and_exprs(quals.into_iter().flatten().collect())
}

fn rewrite_minmax_aggrefs(expr: Expr, rewritten_aggs: &[Expr]) -> Expr {
    match expr {
        Expr::Aggref(aggref) => rewritten_aggs
            .get(aggref.aggno)
            .cloned()
            .unwrap_or(Expr::Aggref(aggref)),
        Expr::WindowFunc(window_func) => {
            Expr::WindowFunc(Box::new(crate::include::nodes::primnodes::WindowFuncExpr {
                kind: match window_func.kind {
                    crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) => {
                        crate::include::nodes::primnodes::WindowFuncKind::Aggregate(Aggref {
                            args: aggref
                                .args
                                .into_iter()
                                .map(|arg| rewrite_minmax_aggrefs(arg, rewritten_aggs))
                                .collect(),
                            aggorder: aggref
                                .aggorder
                                .into_iter()
                                .map(|item| crate::include::nodes::primnodes::OrderByEntry {
                                    expr: rewrite_minmax_aggrefs(item.expr, rewritten_aggs),
                                    ..item
                                })
                                .collect(),
                            aggfilter: aggref
                                .aggfilter
                                .map(|expr| rewrite_minmax_aggrefs(expr, rewritten_aggs)),
                            ..aggref
                        })
                    }
                    crate::include::nodes::primnodes::WindowFuncKind::Builtin(kind) => {
                        crate::include::nodes::primnodes::WindowFuncKind::Builtin(kind)
                    }
                },
                args: window_func
                    .args
                    .into_iter()
                    .map(|arg| rewrite_minmax_aggrefs(arg, rewritten_aggs))
                    .collect(),
                ..*window_func
            }))
        }
        Expr::Op(op) => Expr::Op(Box::new(crate::include::nodes::primnodes::OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| rewrite_minmax_aggrefs(arg, rewritten_aggs))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(crate::include::nodes::primnodes::BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| rewrite_minmax_aggrefs(arg, rewritten_aggs))
                .collect(),
            ..*bool_expr
        })),
        Expr::Case(case_expr) => Expr::Case(Box::new(crate::include::nodes::primnodes::CaseExpr {
            arg: case_expr
                .arg
                .map(|expr| Box::new(rewrite_minmax_aggrefs(*expr, rewritten_aggs))),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| crate::include::nodes::primnodes::CaseWhen {
                    expr: rewrite_minmax_aggrefs(arm.expr, rewritten_aggs),
                    result: rewrite_minmax_aggrefs(arm.result, rewritten_aggs),
                })
                .collect(),
            defresult: Box::new(rewrite_minmax_aggrefs(*case_expr.defresult, rewritten_aggs)),
            ..*case_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(crate::include::nodes::primnodes::FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| rewrite_minmax_aggrefs(arg, rewritten_aggs))
                .collect(),
            ..*func
        })),
        Expr::SubLink(sublink) => Expr::SubLink(Box::new(SubLink {
            testexpr: sublink
                .testexpr
                .map(|expr| Box::new(rewrite_minmax_aggrefs(*expr, rewritten_aggs))),
            ..*sublink
        })),
        Expr::SubPlan(subplan) => {
            Expr::SubPlan(Box::new(crate::include::nodes::primnodes::SubPlan {
                testexpr: subplan
                    .testexpr
                    .map(|expr| Box::new(rewrite_minmax_aggrefs(*expr, rewritten_aggs))),
                args: subplan
                    .args
                    .into_iter()
                    .map(|arg| rewrite_minmax_aggrefs(arg, rewritten_aggs))
                    .collect(),
                ..*subplan
            }))
        }
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(
            crate::include::nodes::primnodes::ScalarArrayOpExpr {
                left: Box::new(rewrite_minmax_aggrefs(*saop.left, rewritten_aggs)),
                right: Box::new(rewrite_minmax_aggrefs(*saop.right, rewritten_aggs)),
                ..*saop
            },
        )),
        Expr::Xml(xml) => Expr::Xml(Box::new(crate::include::nodes::primnodes::XmlExpr {
            named_args: xml
                .named_args
                .into_iter()
                .map(|arg| rewrite_minmax_aggrefs(arg, rewritten_aggs))
                .collect(),
            args: xml
                .args
                .into_iter()
                .map(|arg| rewrite_minmax_aggrefs(arg, rewritten_aggs))
                .collect(),
            ..*xml
        })),
        Expr::Cast(inner, ty) => {
            Expr::Cast(Box::new(rewrite_minmax_aggrefs(*inner, rewritten_aggs)), ty)
        }
        Expr::Collate {
            expr,
            collation_oid,
        } => Expr::Collate {
            expr: Box::new(rewrite_minmax_aggrefs(*expr, rewritten_aggs)),
            collation_oid,
        },
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
            collation_oid,
        } => Expr::Like {
            expr: Box::new(rewrite_minmax_aggrefs(*expr, rewritten_aggs)),
            pattern: Box::new(rewrite_minmax_aggrefs(*pattern, rewritten_aggs)),
            escape: escape.map(|expr| Box::new(rewrite_minmax_aggrefs(*expr, rewritten_aggs))),
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
            expr: Box::new(rewrite_minmax_aggrefs(*expr, rewritten_aggs)),
            pattern: Box::new(rewrite_minmax_aggrefs(*pattern, rewritten_aggs)),
            escape: escape.map(|expr| Box::new(rewrite_minmax_aggrefs(*expr, rewritten_aggs))),
            negated,
            collation_oid,
        },
        Expr::IsNull(inner) => {
            Expr::IsNull(Box::new(rewrite_minmax_aggrefs(*inner, rewritten_aggs)))
        }
        Expr::IsNotNull(inner) => {
            Expr::IsNotNull(Box::new(rewrite_minmax_aggrefs(*inner, rewritten_aggs)))
        }
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(rewrite_minmax_aggrefs(*left, rewritten_aggs)),
            Box::new(rewrite_minmax_aggrefs(*right, rewritten_aggs)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(rewrite_minmax_aggrefs(*left, rewritten_aggs)),
            Box::new(rewrite_minmax_aggrefs(*right, rewritten_aggs)),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| rewrite_minmax_aggrefs(element, rewritten_aggs))
                .collect(),
            array_type,
        },
        Expr::Row { descriptor, fields } => Expr::Row {
            descriptor,
            fields: fields
                .into_iter()
                .map(|(name, expr)| (name, rewrite_minmax_aggrefs(expr, rewritten_aggs)))
                .collect(),
        },
        Expr::FieldSelect {
            expr,
            field,
            field_type,
        } => Expr::FieldSelect {
            expr: Box::new(rewrite_minmax_aggrefs(*expr, rewritten_aggs)),
            field,
            field_type,
        },
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(rewrite_minmax_aggrefs(*left, rewritten_aggs)),
            Box::new(rewrite_minmax_aggrefs(*right, rewritten_aggs)),
        ),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(rewrite_minmax_aggrefs(*array, rewritten_aggs)),
            subscripts: subscripts
                .into_iter()
                .map(
                    |subscript| crate::include::nodes::primnodes::ExprArraySubscript {
                        is_slice: subscript.is_slice,
                        lower: subscript
                            .lower
                            .map(|expr| rewrite_minmax_aggrefs(expr, rewritten_aggs)),
                        upper: subscript
                            .upper
                            .map(|expr| rewrite_minmax_aggrefs(expr, rewritten_aggs)),
                    },
                )
                .collect(),
        },
        other => other,
    }
}

fn expr_contains_local_var_outside_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::Var(var) => var.varlevelsup == 0,
        Expr::Aggref(aggref) => {
            aggref
                .args
                .iter()
                .any(expr_contains_local_var_outside_subquery)
                || aggref
                    .aggfilter
                    .as_ref()
                    .is_some_and(expr_contains_local_var_outside_subquery)
        }
        Expr::WindowFunc(window_func) => {
            window_func
                .args
                .iter()
                .any(expr_contains_local_var_outside_subquery)
                || match &window_func.kind {
                    crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) => aggref
                        .aggfilter
                        .as_ref()
                        .is_some_and(expr_contains_local_var_outside_subquery),
                    crate::include::nodes::primnodes::WindowFuncKind::Builtin(_) => false,
                }
        }
        Expr::Op(op) => op.args.iter().any(expr_contains_local_var_outside_subquery),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .any(expr_contains_local_var_outside_subquery),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_deref()
                .is_some_and(expr_contains_local_var_outside_subquery)
                || case_expr.args.iter().any(|arm| {
                    expr_contains_local_var_outside_subquery(&arm.expr)
                        || expr_contains_local_var_outside_subquery(&arm.result)
                })
                || expr_contains_local_var_outside_subquery(&case_expr.defresult)
        }
        Expr::Func(func) => func
            .args
            .iter()
            .any(expr_contains_local_var_outside_subquery),
        Expr::SetReturning(srf) => set_returning_call_exprs(&srf.call)
            .into_iter()
            .any(expr_contains_local_var_outside_subquery),
        Expr::SubLink(sublink) => sublink
            .testexpr
            .as_deref()
            .is_some_and(expr_contains_local_var_outside_subquery),
        Expr::SubPlan(subplan) => {
            subplan
                .testexpr
                .as_deref()
                .is_some_and(expr_contains_local_var_outside_subquery)
                || subplan
                    .args
                    .iter()
                    .any(expr_contains_local_var_outside_subquery)
        }
        Expr::ScalarArrayOp(saop) => {
            expr_contains_local_var_outside_subquery(&saop.left)
                || expr_contains_local_var_outside_subquery(&saop.right)
        }
        Expr::Xml(xml) => xml
            .child_exprs()
            .any(expr_contains_local_var_outside_subquery),
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => expr_contains_local_var_outside_subquery(inner),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Expr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_local_var_outside_subquery(expr)
                || expr_contains_local_var_outside_subquery(pattern)
                || escape
                    .as_deref()
                    .is_some_and(expr_contains_local_var_outside_subquery)
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_contains_local_var_outside_subquery(left)
                || expr_contains_local_var_outside_subquery(right)
        }
        Expr::ArrayLiteral { elements, .. } => elements
            .iter()
            .any(expr_contains_local_var_outside_subquery),
        Expr::Row { fields, .. } => fields
            .iter()
            .any(|(_, expr)| expr_contains_local_var_outside_subquery(expr)),
        Expr::FieldSelect { expr, .. } => expr_contains_local_var_outside_subquery(expr),
        Expr::ArraySubscript { array, subscripts } => {
            expr_contains_local_var_outside_subquery(array)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(expr_contains_local_var_outside_subquery)
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(expr_contains_local_var_outside_subquery)
                })
        }
        Expr::Param(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::Random
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
    }
}

pub(super) fn build_projection_targets_for_pathtarget(target: &PathTarget) -> Vec<TargetEntry> {
    target
        .exprs
        .iter()
        .enumerate()
        .map(|(index, expr)| {
            TargetEntry::new(
                format!("col{}", index + 1),
                expr.clone(),
                expr_sql_type(expr),
                index + 1,
            )
            .with_sort_group_ref(target.get_pathtarget_sortgroupref(index))
        })
        .collect()
}

pub(super) fn build_simple_rel_array(rtable: &[RangeTblEntry]) -> Vec<Option<RelOptInfo>> {
    let mut simple_rel_array = vec![None];
    simple_rel_array.extend(
        rtable
            .iter()
            .enumerate()
            .map(|(index, rte)| match &rte.kind {
                RangeTblEntryKind::Join { .. } => None,
                _ => Some(RelOptInfo::from_rte(index + 1, rte)),
            }),
    );
    simple_rel_array
}

fn has_grouping(query: &Query) -> bool {
    !query.group_by.is_empty() || !query.accumulators.is_empty() || query.having_qual.is_some()
}

fn has_windowing(query: &Query) -> bool {
    !query.window_clauses.is_empty()
}

fn make_processed_tlist(parse: &Query) -> Vec<TargetEntry> {
    let mut processed_tlist = parse.target_list.clone();
    let mut next_sort_group_ref = processed_tlist
        .iter()
        .map(|target| target.ressortgroupref.max(target.resno))
        .max()
        .unwrap_or(0)
        + 1;
    let mut next_resno = processed_tlist.len() + 1;

    for clause in parse.sort_clause.iter().chain(parse.distinct_on.iter()) {
        let matching_index = processed_tlist
            .iter()
            .position(|target| {
                clause.tle_sort_group_ref != 0 && target.resno == clause.tle_sort_group_ref
            })
            .or_else(|| {
                processed_tlist.iter().position(|target| {
                    clause.tle_sort_group_ref != 0
                        && target.ressortgroupref == clause.tle_sort_group_ref
                })
            })
            .or_else(|| {
                processed_tlist
                    .iter()
                    .position(|target| target.expr == clause.expr)
            });
        if let Some(target) = matching_index.and_then(|index| processed_tlist.get_mut(index)) {
            if target.ressortgroupref == 0 {
                target.ressortgroupref = if clause.tle_sort_group_ref != 0 {
                    clause.tle_sort_group_ref
                } else {
                    let next = next_sort_group_ref;
                    next_sort_group_ref += 1;
                    next
                };
            }
            continue;
        }

        processed_tlist.push(
            TargetEntry::new(
                "?column?",
                clause.expr.clone(),
                expr_sql_type(&clause.expr),
                next_resno,
            )
            .with_sort_group_ref(next_sort_group_ref)
            .as_resjunk(),
        );
        next_sort_group_ref += 1;
        next_resno += 1;
    }

    processed_tlist
}

fn make_sort_input_target(
    parse: &Query,
    processed_tlist: &[TargetEntry],
    final_target: &PathTarget,
) -> PathTarget {
    if parse.sort_clause.is_empty() && parse.distinct_on.is_empty() {
        return final_target.clone();
    }

    if parse.has_target_srfs {
        let have_srf_sortcols = processed_tlist
            .iter()
            .any(|target| target.ressortgroupref != 0 && expr_contains_set_returning(&target.expr));
        if have_srf_sortcols {
            return PathTarget::from_target_list(processed_tlist);
        }
        let mut input_target = PathTarget::new(Vec::new());
        for target in processed_tlist {
            if expr_contains_set_returning(&target.expr) {
                let mut supporting_inputs = Vec::new();
                collect_supporting_inputs(&target.expr, &mut supporting_inputs);
                input_target.add_new_columns_to_pathtarget(supporting_inputs);
                continue;
            }
            input_target.add_column_to_pathtarget(target.expr.clone(), target.ressortgroupref);
        }
        return input_target;
    }

    PathTarget::from_target_list(processed_tlist)
}

fn make_group_input_target(parse: &Query, group_by: &[Expr]) -> PathTarget {
    let mut exprs = Vec::new();
    for group_expr in group_by {
        push_expr(&mut exprs, group_expr.clone());
    }
    for target in &parse.target_list {
        collect_group_input_exprs(&target.expr, group_by, &mut exprs);
    }
    for accum in &parse.accumulators {
        for arg in &accum.args {
            collect_group_input_exprs(arg, group_by, &mut exprs);
        }
        if let Some(filter) = accum.filter.as_ref() {
            collect_group_input_exprs(filter, group_by, &mut exprs);
        }
    }
    if let Some(having) = parse.having_qual.as_ref() {
        collect_group_input_exprs(having, group_by, &mut exprs);
    }
    if let Some(where_qual) = parse.where_qual.as_ref() {
        collect_group_input_exprs(where_qual, group_by, &mut exprs);
    }
    PathTarget::new(exprs)
}

fn build_grouped_target(layout: &AggregateLayout, accumulators: &[AggAccum]) -> PathTarget {
    let mut exprs = Vec::with_capacity(
        layout.group_by.len() + layout.passthrough_exprs.len() + accumulators.len(),
    );
    exprs.extend(layout.group_by.iter().cloned());
    exprs.extend(layout.passthrough_exprs.iter().cloned());
    exprs.extend(accumulators.iter().enumerate().map(|(aggno, accum)| {
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
    }));
    PathTarget::new(exprs)
}

fn build_scanjoin_target(
    parse: &Query,
    group_input_target: &PathTarget,
    window_input_target: &PathTarget,
    sort_input_target: &PathTarget,
    final_target: &PathTarget,
) -> PathTarget {
    let exprs = if has_grouping(parse) {
        group_input_target.exprs.clone()
    } else if has_windowing(parse) {
        window_input_target.exprs.clone()
    } else if !parse.sort_clause.is_empty() || !parse.distinct_on.is_empty() {
        sort_input_target.exprs.clone()
    } else {
        final_target.exprs.clone()
    };
    PathTarget::new(exprs)
}

fn make_window_input_target(
    parse: &Query,
    processed_tlist: &[TargetEntry],
    grouped_target: &PathTarget,
) -> PathTarget {
    let mut input_target = if has_grouping(parse) {
        grouped_target.clone()
    } else {
        PathTarget::new(Vec::new())
    };
    for target in processed_tlist {
        collect_window_input_exprs(&target.expr, has_grouping(parse), &mut input_target);
    }
    for clause in &parse.sort_clause {
        collect_window_input_exprs(&clause.expr, has_grouping(parse), &mut input_target);
    }
    for clause in &parse.window_clauses {
        for expr in &clause.spec.partition_by {
            collect_window_input_exprs(expr, has_grouping(parse), &mut input_target);
        }
        for item in &clause.spec.order_by {
            collect_window_input_exprs(&item.expr, has_grouping(parse), &mut input_target);
        }
        match &clause.spec.frame.start_bound {
            crate::include::nodes::primnodes::WindowFrameBound::OffsetPreceding(offset)
            | crate::include::nodes::primnodes::WindowFrameBound::OffsetFollowing(offset) => {
                collect_window_input_exprs(&offset.expr, has_grouping(parse), &mut input_target);
            }
            crate::include::nodes::primnodes::WindowFrameBound::UnboundedPreceding
            | crate::include::nodes::primnodes::WindowFrameBound::CurrentRow
            | crate::include::nodes::primnodes::WindowFrameBound::UnboundedFollowing => {}
        }
        match &clause.spec.frame.end_bound {
            crate::include::nodes::primnodes::WindowFrameBound::OffsetPreceding(offset)
            | crate::include::nodes::primnodes::WindowFrameBound::OffsetFollowing(offset) => {
                collect_window_input_exprs(&offset.expr, has_grouping(parse), &mut input_target);
            }
            crate::include::nodes::primnodes::WindowFrameBound::UnboundedPreceding
            | crate::include::nodes::primnodes::WindowFrameBound::CurrentRow
            | crate::include::nodes::primnodes::WindowFrameBound::UnboundedFollowing => {}
        }
    }
    input_target
}

fn collect_window_input_exprs(expr: &Expr, preserve_expr: bool, target: &mut PathTarget) {
    if preserve_expr && !expr_contains_window_func(expr) {
        target.add_column_to_pathtarget(expr.clone(), 0);
        return;
    }
    if let Expr::WindowFunc(window_func) = expr {
        for arg in &window_func.args {
            collect_window_input_exprs(arg, preserve_expr, target);
        }
        if let crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) =
            &window_func.kind
        {
            if let Some(filter) = aggref.aggfilter.as_ref() {
                collect_window_input_exprs(filter, preserve_expr, target);
            }
        }
        return;
    }
    let mut supporting = Vec::new();
    collect_supporting_inputs(expr, &mut supporting);
    target.add_new_columns_to_pathtarget(supporting);
}

fn expr_contains_window_func(expr: &Expr) -> bool {
    match expr {
        Expr::WindowFunc(_) => true,
        Expr::Aggref(aggref) => {
            aggref.args.iter().any(expr_contains_window_func)
                || aggref
                    .aggfilter
                    .as_ref()
                    .is_some_and(expr_contains_window_func)
        }
        Expr::Op(op) => op.args.iter().any(expr_contains_window_func),
        Expr::Bool(bool_expr) => bool_expr.args.iter().any(expr_contains_window_func),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_deref()
                .is_some_and(expr_contains_window_func)
                || case_expr.args.iter().any(|arm| {
                    expr_contains_window_func(&arm.expr) || expr_contains_window_func(&arm.result)
                })
                || expr_contains_window_func(&case_expr.defresult)
        }
        Expr::Func(func) => func.args.iter().any(expr_contains_window_func),
        Expr::SetReturning(srf) => set_returning_call_exprs(&srf.call)
            .into_iter()
            .any(expr_contains_window_func),
        Expr::SubLink(sublink) => sublink
            .testexpr
            .as_deref()
            .is_some_and(expr_contains_window_func),
        Expr::SubPlan(subplan) => subplan
            .testexpr
            .as_deref()
            .is_some_and(expr_contains_window_func),
        Expr::ScalarArrayOp(saop) => {
            expr_contains_window_func(&saop.left) || expr_contains_window_func(&saop.right)
        }
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => expr_contains_window_func(inner),
        Expr::Param(_)
        | Expr::Var(_)
        | Expr::CaseTest(_)
        | Expr::Const(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Expr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_window_func(expr)
                || expr_contains_window_func(pattern)
                || escape.as_deref().is_some_and(expr_contains_window_func)
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_contains_window_func(left) || expr_contains_window_func(right)
        }
        Expr::ArrayLiteral { elements, .. } => elements.iter().any(expr_contains_window_func),
        Expr::Row { fields, .. } => fields
            .iter()
            .any(|(_, expr)| expr_contains_window_func(expr)),
        Expr::FieldSelect { expr, .. } => expr_contains_window_func(expr),
        Expr::ArraySubscript { array, subscripts } => {
            expr_contains_window_func(array)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(expr_contains_window_func)
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(expr_contains_window_func)
                })
        }
        Expr::Xml(xml) => xml.child_exprs().any(expr_contains_window_func),
    }
}

fn push_expr(exprs: &mut Vec<Expr>, expr: Expr) {
    if !exprs.contains(&expr) {
        exprs.push(expr);
    }
}

fn collect_group_input_exprs(expr: &Expr, group_by: &[Expr], exprs: &mut Vec<Expr>) {
    if group_by.contains(expr) {
        push_expr(exprs, expr.clone());
        return;
    }
    match expr {
        Expr::Var(_) => push_expr(exprs, expr.clone()),
        Expr::Param(_) => {}
        Expr::Aggref(aggref) => {
            for arg in &aggref.args {
                collect_group_input_exprs(arg, group_by, exprs);
            }
            if let Some(filter) = aggref.aggfilter.as_ref() {
                collect_group_input_exprs(filter, group_by, exprs);
            }
        }
        Expr::WindowFunc(window_func) => {
            for arg in &window_func.args {
                collect_group_input_exprs(arg, group_by, exprs);
            }
            if let crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) =
                &window_func.kind
            {
                if let Some(filter) = aggref.aggfilter.as_ref() {
                    collect_group_input_exprs(filter, group_by, exprs);
                }
            }
        }
        Expr::Op(op) => collect_expr_vec(&op.args, group_by, exprs),
        Expr::Bool(bool_expr) => collect_expr_vec(&bool_expr.args, group_by, exprs),
        Expr::Case(case_expr) => {
            if let Some(arg) = &case_expr.arg {
                collect_group_input_exprs(arg, group_by, exprs);
            }
            for arm in &case_expr.args {
                collect_group_input_exprs(&arm.expr, group_by, exprs);
                collect_group_input_exprs(&arm.result, group_by, exprs);
            }
            collect_group_input_exprs(&case_expr.defresult, group_by, exprs);
        }
        Expr::CaseTest(_) => {}
        Expr::Func(func) => collect_expr_vec(&func.args, group_by, exprs),
        Expr::SetReturning(srf) => {
            for arg in set_returning_call_exprs(&srf.call) {
                collect_group_input_exprs(arg, group_by, exprs);
            }
        }
        Expr::SubLink(sublink) => {
            if let Some(testexpr) = &sublink.testexpr {
                collect_group_input_exprs(testexpr, group_by, exprs);
            }
            collect_query_outer_refs(&sublink.subselect, 1, exprs);
        }
        Expr::SubPlan(subplan) => {
            if let Some(testexpr) = &subplan.testexpr {
                collect_group_input_exprs(testexpr, group_by, exprs);
            }
        }
        Expr::ScalarArrayOp(saop) => {
            collect_group_input_exprs(&saop.left, group_by, exprs);
            collect_group_input_exprs(&saop.right, group_by, exprs);
        }
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => {
            collect_group_input_exprs(inner, group_by, exprs);
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Expr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            collect_group_input_exprs(expr, group_by, exprs);
            collect_group_input_exprs(pattern, group_by, exprs);
            if let Some(escape) = escape.as_deref() {
                collect_group_input_exprs(escape, group_by, exprs);
            }
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            collect_group_input_exprs(left, group_by, exprs);
            collect_group_input_exprs(right, group_by, exprs);
        }
        Expr::ArrayLiteral { elements, .. } => {
            for element in elements {
                collect_group_input_exprs(element, group_by, exprs);
            }
        }
        Expr::Row { fields, .. } => {
            for (_, expr) in fields {
                collect_group_input_exprs(expr, group_by, exprs);
            }
        }
        Expr::FieldSelect { expr, .. } => collect_group_input_exprs(expr, group_by, exprs),
        Expr::ArraySubscript { array, subscripts } => {
            collect_group_input_exprs(array, group_by, exprs);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_group_input_exprs(lower, group_by, exprs);
                }
                if let Some(upper) = &subscript.upper {
                    collect_group_input_exprs(upper, group_by, exprs);
                }
            }
        }
        Expr::Xml(xml) => {
            for child in xml.child_exprs() {
                collect_group_input_exprs(child, group_by, exprs);
            }
        }
        Expr::Const(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => {}
    }
}

fn collect_expr_vec(args: &[Expr], group_by: &[Expr], exprs: &mut Vec<Expr>) {
    for arg in args {
        collect_group_input_exprs(arg, group_by, exprs);
    }
}

fn collect_supporting_inputs(expr: &Expr, exprs: &mut Vec<Expr>) {
    match expr {
        Expr::Var(_) => push_expr(exprs, expr.clone()),
        Expr::Param(_) => {}
        Expr::Aggref(aggref) => {
            for arg in &aggref.args {
                collect_supporting_inputs(arg, exprs);
            }
            if let Some(filter) = aggref.aggfilter.as_ref() {
                collect_supporting_inputs(filter, exprs);
            }
        }
        Expr::WindowFunc(window_func) => {
            for arg in &window_func.args {
                collect_supporting_inputs(arg, exprs);
            }
            if let crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) =
                &window_func.kind
            {
                if let Some(filter) = aggref.aggfilter.as_ref() {
                    collect_supporting_inputs(filter, exprs);
                }
            }
        }
        Expr::Op(op) => {
            for arg in &op.args {
                collect_supporting_inputs(arg, exprs);
            }
        }
        Expr::Bool(bool_expr) => {
            for arg in &bool_expr.args {
                collect_supporting_inputs(arg, exprs);
            }
        }
        Expr::Case(case_expr) => {
            if let Some(arg) = &case_expr.arg {
                collect_supporting_inputs(arg, exprs);
            }
            for arm in &case_expr.args {
                collect_supporting_inputs(&arm.expr, exprs);
                collect_supporting_inputs(&arm.result, exprs);
            }
            collect_supporting_inputs(&case_expr.defresult, exprs);
        }
        Expr::CaseTest(_) => {}
        Expr::Func(func) => {
            for arg in &func.args {
                collect_supporting_inputs(arg, exprs);
            }
        }
        Expr::SetReturning(srf) => {
            for arg in set_returning_call_exprs(&srf.call) {
                collect_supporting_inputs(arg, exprs);
            }
        }
        Expr::SubLink(sublink) => {
            if let Some(testexpr) = &sublink.testexpr {
                collect_supporting_inputs(testexpr, exprs);
            }
            collect_query_outer_refs(&sublink.subselect, 1, exprs);
        }
        Expr::SubPlan(subplan) => {
            if let Some(testexpr) = &subplan.testexpr {
                collect_supporting_inputs(testexpr, exprs);
            }
        }
        Expr::ScalarArrayOp(saop) => {
            collect_supporting_inputs(&saop.left, exprs);
            collect_supporting_inputs(&saop.right, exprs);
        }
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => {
            collect_supporting_inputs(inner, exprs);
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Expr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            collect_supporting_inputs(expr, exprs);
            collect_supporting_inputs(pattern, exprs);
            if let Some(escape) = escape.as_deref() {
                collect_supporting_inputs(escape, exprs);
            }
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            collect_supporting_inputs(left, exprs);
            collect_supporting_inputs(right, exprs);
        }
        Expr::ArrayLiteral { elements, .. } => {
            for element in elements {
                collect_supporting_inputs(element, exprs);
            }
        }
        Expr::Row { fields, .. } => {
            for (_, expr) in fields {
                collect_supporting_inputs(expr, exprs);
            }
        }
        Expr::FieldSelect { expr, .. } => collect_supporting_inputs(expr, exprs),
        Expr::ArraySubscript { array, subscripts } => {
            collect_supporting_inputs(array, exprs);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_supporting_inputs(lower, exprs);
                }
                if let Some(upper) = &subscript.upper {
                    collect_supporting_inputs(upper, exprs);
                }
            }
        }
        Expr::Xml(xml) => {
            for child in xml.child_exprs() {
                collect_supporting_inputs(child, exprs);
            }
        }
        Expr::Const(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => {}
    }
}

fn collect_query_outer_refs(query: &Query, levelsup: usize, exprs: &mut Vec<Expr>) {
    for target in &query.target_list {
        collect_query_outer_refs_expr(&target.expr, levelsup, exprs);
    }
    if let Some(where_qual) = query.where_qual.as_ref() {
        collect_query_outer_refs_expr(where_qual, levelsup, exprs);
    }
    for expr in &query.group_by {
        collect_query_outer_refs_expr(expr, levelsup, exprs);
    }
    for accum in &query.accumulators {
        for arg in &accum.args {
            collect_query_outer_refs_expr(arg, levelsup, exprs);
        }
        if let Some(filter) = accum.filter.as_ref() {
            collect_query_outer_refs_expr(filter, levelsup, exprs);
        }
    }
    if let Some(having) = query.having_qual.as_ref() {
        collect_query_outer_refs_expr(having, levelsup, exprs);
    }
    for clause in &query.sort_clause {
        collect_query_outer_refs_expr(&clause.expr, levelsup, exprs);
    }
    if let Some(jointree) = query.jointree.as_ref() {
        collect_jointree_outer_refs(jointree, levelsup, exprs);
    }
    for rte in &query.rtable {
        match &rte.kind {
            RangeTblEntryKind::Values { rows, .. } => {
                for row in rows {
                    for expr in row {
                        collect_query_outer_refs_expr(expr, levelsup, exprs);
                    }
                }
            }
            RangeTblEntryKind::Function { call } => {
                collect_set_returning_call_outer_refs(call, levelsup, exprs)
            }
            RangeTblEntryKind::Cte { query, .. } => {
                collect_query_outer_refs(query, levelsup + 1, exprs)
            }
            RangeTblEntryKind::Subquery { query } => {
                collect_query_outer_refs(query, levelsup + 1, exprs)
            }
            RangeTblEntryKind::WorkTable { .. } => {}
            RangeTblEntryKind::Result
            | RangeTblEntryKind::Relation { .. }
            | RangeTblEntryKind::Join { .. } => {}
        }
    }
}

fn collect_jointree_supporting_inputs(node: &JoinTreeNode, exprs: &mut Vec<Expr>) {
    if let JoinTreeNode::JoinExpr {
        left, right, quals, ..
    } = node
    {
        collect_jointree_supporting_inputs(left, exprs);
        collect_jointree_supporting_inputs(right, exprs);
        collect_supporting_inputs(quals, exprs);
    }
}

fn collect_jointree_outer_refs(node: &JoinTreeNode, levelsup: usize, exprs: &mut Vec<Expr>) {
    match node {
        JoinTreeNode::RangeTblRef(_) => {}
        JoinTreeNode::JoinExpr {
            left, right, quals, ..
        } => {
            collect_jointree_outer_refs(left, levelsup, exprs);
            collect_jointree_outer_refs(right, levelsup, exprs);
            collect_query_outer_refs_expr(quals, levelsup, exprs);
        }
    }
}

fn collect_set_returning_call_outer_refs(
    call: &SetReturningCall,
    levelsup: usize,
    exprs: &mut Vec<Expr>,
) {
    match call {
        SetReturningCall::GenerateSeries {
            start,
            stop,
            step,
            timezone,
            ..
        } => {
            collect_query_outer_refs_expr(start, levelsup, exprs);
            collect_query_outer_refs_expr(stop, levelsup, exprs);
            collect_query_outer_refs_expr(step, levelsup, exprs);
            if let Some(timezone) = timezone {
                collect_query_outer_refs_expr(timezone, levelsup, exprs);
            }
        }
        SetReturningCall::PartitionTree { relid, .. }
        | SetReturningCall::PartitionAncestors { relid, .. } => {
            collect_query_outer_refs_expr(relid, levelsup, exprs);
        }
        SetReturningCall::PgLockStatus { .. } => {}
        SetReturningCall::TxidSnapshotXip { arg, .. } => {
            collect_query_outer_refs_expr(arg, levelsup, exprs);
        }
        SetReturningCall::Unnest { args, .. }
        | SetReturningCall::JsonTableFunction { args, .. }
        | SetReturningCall::JsonRecordFunction { args, .. }
        | SetReturningCall::RegexTableFunction { args, .. }
        | SetReturningCall::StringTableFunction { args, .. }
        | SetReturningCall::TextSearchTableFunction { args, .. }
        | SetReturningCall::UserDefined { args, .. } => {
            for arg in args {
                collect_query_outer_refs_expr(arg, levelsup, exprs);
            }
        }
    }
}

fn collect_query_outer_refs_expr(expr: &Expr, levelsup: usize, exprs: &mut Vec<Expr>) {
    match expr {
        Expr::Var(var) if var.varlevelsup == levelsup => push_expr(
            exprs,
            Expr::Var(Var {
                varlevelsup: 0,
                ..*var
            }),
        ),
        Expr::Var(_) | Expr::Param(_) | Expr::Const(_) | Expr::Random => {}
        Expr::CurrentDate
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => {}
        Expr::FieldSelect { expr, .. } | Expr::Collate { expr, .. } => {
            collect_query_outer_refs_expr(expr, levelsup, exprs)
        }
        Expr::Aggref(aggref) => {
            for arg in &aggref.args {
                collect_query_outer_refs_expr(arg, levelsup, exprs);
            }
            if let Some(filter) = aggref.aggfilter.as_ref() {
                collect_query_outer_refs_expr(filter, levelsup, exprs);
            }
        }
        Expr::WindowFunc(window_func) => {
            for arg in &window_func.args {
                collect_query_outer_refs_expr(arg, levelsup, exprs);
            }
            if let crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) =
                &window_func.kind
            {
                if let Some(filter) = aggref.aggfilter.as_ref() {
                    collect_query_outer_refs_expr(filter, levelsup, exprs);
                }
            }
        }
        Expr::Op(op) => {
            for arg in &op.args {
                collect_query_outer_refs_expr(arg, levelsup, exprs);
            }
        }
        Expr::Bool(bool_expr) => {
            for arg in &bool_expr.args {
                collect_query_outer_refs_expr(arg, levelsup, exprs);
            }
        }
        Expr::Case(case_expr) => {
            if let Some(arg) = &case_expr.arg {
                collect_query_outer_refs_expr(arg, levelsup, exprs);
            }
            for arm in &case_expr.args {
                collect_query_outer_refs_expr(&arm.expr, levelsup, exprs);
                collect_query_outer_refs_expr(&arm.result, levelsup, exprs);
            }
            collect_query_outer_refs_expr(&case_expr.defresult, levelsup, exprs);
        }
        Expr::CaseTest(_) => {}
        Expr::Func(func) => {
            for arg in &func.args {
                collect_query_outer_refs_expr(arg, levelsup, exprs);
            }
        }
        Expr::SetReturning(srf) => {
            for arg in set_returning_call_exprs(&srf.call) {
                collect_query_outer_refs_expr(arg, levelsup, exprs);
            }
        }
        Expr::SubLink(sublink) => {
            if let Some(testexpr) = &sublink.testexpr {
                collect_query_outer_refs_expr(testexpr, levelsup, exprs);
            }
            collect_query_outer_refs(&sublink.subselect, levelsup + 1, exprs);
        }
        Expr::SubPlan(subplan) => {
            if let Some(testexpr) = &subplan.testexpr {
                collect_query_outer_refs_expr(testexpr, levelsup, exprs);
            }
        }
        Expr::ScalarArrayOp(saop) => {
            collect_query_outer_refs_expr(&saop.left, levelsup, exprs);
            collect_query_outer_refs_expr(&saop.right, levelsup, exprs);
        }
        Expr::Cast(inner, _) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            collect_query_outer_refs_expr(inner, levelsup, exprs);
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Expr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            collect_query_outer_refs_expr(expr, levelsup, exprs);
            collect_query_outer_refs_expr(pattern, levelsup, exprs);
            if let Some(escape) = escape.as_deref() {
                collect_query_outer_refs_expr(escape, levelsup, exprs);
            }
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            collect_query_outer_refs_expr(left, levelsup, exprs);
            collect_query_outer_refs_expr(right, levelsup, exprs);
        }
        Expr::ArrayLiteral { elements, .. } => {
            for element in elements {
                collect_query_outer_refs_expr(element, levelsup, exprs);
            }
        }
        Expr::Row { fields, .. } => {
            for (_, expr) in fields {
                collect_query_outer_refs_expr(expr, levelsup, exprs);
            }
        }
        Expr::ArraySubscript { array, subscripts } => {
            collect_query_outer_refs_expr(array, levelsup, exprs);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_query_outer_refs_expr(lower, levelsup, exprs);
                }
                if let Some(upper) = &subscript.upper {
                    collect_query_outer_refs_expr(upper, levelsup, exprs);
                }
            }
        }
        Expr::Xml(xml) => {
            for child in xml.child_exprs() {
                collect_query_outer_refs_expr(child, levelsup, exprs);
            }
        }
    }
}
