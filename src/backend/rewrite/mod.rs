mod row_security;
mod rules;
mod view_dml;
mod views;

use row_security::apply_query_row_security_with_active_relations;
pub(crate) use row_security::{
    RlsWriteCheck, RlsWriteCheckSource, build_target_relation_row_security,
    relation_has_row_security,
};
pub(crate) use rules::{format_stored_rule_definition, split_stored_rule_action_sql};
pub(crate) use view_dml::{
    NonUpdatableViewColumnReason, ResolvedAutoViewTarget, ViewDmlEvent, ViewDmlRewriteError,
    resolve_auto_updatable_view_target,
};
pub(crate) use views::{
    format_view_definition, load_view_return_query, load_view_return_select,
    refresh_query_relation_descriptors, render_view_query_sql, split_stored_view_definition_sql,
};

use crate::backend::parser::{CatalogLookup, ParseError};
use crate::include::nodes::parsenodes::{Query, RangeTblEntry, RangeTblEntryKind};
use crate::include::nodes::primnodes::{
    AggAccum, Expr, ExprArraySubscript, SetReturningCall, SetReturningExpr, SortGroupClause,
    SubLink, TargetEntry, WindowClause, WindowFrame, WindowFrameBound, WindowFuncExpr,
    WindowFuncKind, WindowSpec,
};
use views::rewrite_view_relation_query;

pub(crate) fn pg_rewrite_query(
    query: Query,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<Query>, ParseError> {
    let mut active_policy_relations = Vec::new();
    Ok(vec![rewrite_query(
        query,
        catalog,
        &[],
        &mut active_policy_relations,
    )?])
}

fn rewrite_query(
    query: Query,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
    active_policy_relations: &mut Vec<u32>,
) -> Result<Query, ParseError> {
    let mut rewritten = Query {
        rtable: query
            .rtable
            .into_iter()
            .map(|rte| rewrite_rte(rte, catalog, expanded_views, active_policy_relations))
            .collect::<Result<Vec<_>, _>>()?,
        where_qual: query
            .where_qual
            .map(|expr| {
                rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
            })
            .transpose()?,
        group_by: query
            .group_by
            .into_iter()
            .map(|expr| {
                rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
            })
            .collect::<Result<Vec<_>, _>>()?,
        accumulators: query
            .accumulators
            .into_iter()
            .map(|accum| rewrite_agg_accum(accum, catalog, expanded_views, active_policy_relations))
            .collect::<Result<Vec<_>, _>>()?,
        window_clauses: query
            .window_clauses
            .into_iter()
            .map(|clause| {
                rewrite_window_clause(clause, catalog, expanded_views, active_policy_relations)
            })
            .collect::<Result<Vec<_>, _>>()?,
        having_qual: query
            .having_qual
            .map(|expr| {
                rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
            })
            .transpose()?,
        target_list: query
            .target_list
            .into_iter()
            .map(|target| {
                rewrite_target_entry(target, catalog, expanded_views, active_policy_relations)
            })
            .collect::<Result<Vec<_>, _>>()?,
        sort_clause: query
            .sort_clause
            .into_iter()
            .map(|clause| {
                rewrite_sort_group_clause(clause, catalog, expanded_views, active_policy_relations)
            })
            .collect::<Result<Vec<_>, _>>()?,
        distinct_on: query
            .distinct_on
            .into_iter()
            .map(|clause| {
                rewrite_sort_group_clause(clause, catalog, expanded_views, active_policy_relations)
            })
            .collect::<Result<Vec<_>, _>>()?,
        has_target_srfs: query.has_target_srfs,
        recursive_union: query
            .recursive_union
            .map(|recursive_union| {
                Ok(Box::new(
                    crate::include::nodes::parsenodes::RecursiveUnionQuery {
                        output_desc: recursive_union.output_desc,
                        anchor: rewrite_query(
                            recursive_union.anchor,
                            catalog,
                            expanded_views,
                            active_policy_relations,
                        )?,
                        recursive: rewrite_query(
                            recursive_union.recursive,
                            catalog,
                            expanded_views,
                            active_policy_relations,
                        )?,
                        distinct: recursive_union.distinct,
                        recursive_references_worktable: recursive_union
                            .recursive_references_worktable,
                        worktable_id: recursive_union.worktable_id,
                    },
                ))
            })
            .transpose()?,
        set_operation: query
            .set_operation
            .map(|set_operation| {
                Ok(Box::new(
                    crate::include::nodes::parsenodes::SetOperationQuery {
                        output_desc: set_operation.output_desc,
                        op: set_operation.op,
                        inputs: set_operation
                            .inputs
                            .into_iter()
                            .map(|input| {
                                rewrite_query(
                                    input,
                                    catalog,
                                    expanded_views,
                                    active_policy_relations,
                                )
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    },
                ))
            })
            .transpose()?,
        ..query
    };
    apply_query_row_security_with_active_relations(
        &mut rewritten,
        catalog,
        active_policy_relations,
    )?;
    Ok(rewritten)
}

pub(super) fn rewrite_policy_expr(
    expr: Expr,
    catalog: &dyn CatalogLookup,
    active_policy_relations: &mut Vec<u32>,
) -> Result<Expr, ParseError> {
    rewrite_semantic_expr(expr, catalog, &[], active_policy_relations)
}

fn rewrite_rte(
    rte: RangeTblEntry,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
    active_policy_relations: &mut Vec<u32>,
) -> Result<RangeTblEntry, ParseError> {
    let kind = match rte.kind {
        RangeTblEntryKind::Relation {
            rel: _,
            relation_oid,
            relkind,
            relispopulated: _,
            toast: _,
        } if relkind == 'v' => {
            let analyzed = rewrite_view_relation_query(
                relation_oid,
                &rte.desc,
                rte.alias.as_deref(),
                catalog,
                expanded_views,
            )?;
            let mut next_views = expanded_views.to_vec();
            next_views.push(relation_oid);
            RangeTblEntryKind::Subquery {
                query: Box::new(rewrite_query(
                    analyzed,
                    catalog,
                    &next_views,
                    active_policy_relations,
                )?),
            }
        }
        RangeTblEntryKind::Relation {
            rel,
            relation_oid,
            relkind,
            relispopulated,
            toast,
        } => RangeTblEntryKind::Relation {
            rel,
            relation_oid,
            relkind,
            relispopulated,
            toast,
        },
        RangeTblEntryKind::Join {
            jointype,
            joinmergedcols,
            joinaliasvars,
            joinleftcols,
            joinrightcols,
        } => RangeTblEntryKind::Join {
            jointype,
            joinmergedcols,
            joinaliasvars: joinaliasvars
                .into_iter()
                .map(|expr| {
                    rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
                })
                .collect::<Result<Vec<_>, _>>()?,
            joinleftcols,
            joinrightcols,
        },
        RangeTblEntryKind::Values {
            rows,
            output_columns,
        } => RangeTblEntryKind::Values {
            rows: rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|expr| {
                            rewrite_semantic_expr(
                                expr,
                                catalog,
                                expanded_views,
                                active_policy_relations,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<Vec<_>, _>>()?,
            output_columns,
        },
        RangeTblEntryKind::Function { call } => RangeTblEntryKind::Function {
            call: rewrite_set_returning_call(
                call,
                catalog,
                expanded_views,
                active_policy_relations,
            )?,
        },
        RangeTblEntryKind::Cte { cte_id, query } => RangeTblEntryKind::Cte {
            cte_id,
            query: Box::new(rewrite_query(
                *query,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
        },
        RangeTblEntryKind::Subquery { query } => RangeTblEntryKind::Subquery {
            query: Box::new(rewrite_query(
                *query,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
        },
        RangeTblEntryKind::WorkTable { worktable_id } => {
            RangeTblEntryKind::WorkTable { worktable_id }
        }
        RangeTblEntryKind::Result => RangeTblEntryKind::Result,
    };
    Ok(RangeTblEntry { kind, ..rte })
}

fn rewrite_target_entry(
    target: TargetEntry,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
    active_policy_relations: &mut Vec<u32>,
) -> Result<TargetEntry, ParseError> {
    Ok(TargetEntry {
        expr: rewrite_semantic_expr(
            target.expr,
            catalog,
            expanded_views,
            active_policy_relations,
        )?,
        ..target
    })
}

fn rewrite_sort_group_clause(
    clause: SortGroupClause,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
    active_policy_relations: &mut Vec<u32>,
) -> Result<SortGroupClause, ParseError> {
    Ok(SortGroupClause {
        expr: rewrite_semantic_expr(
            clause.expr,
            catalog,
            expanded_views,
            active_policy_relations,
        )?,
        ..clause
    })
}

fn rewrite_window_clause(
    clause: WindowClause,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
    active_policy_relations: &mut Vec<u32>,
) -> Result<WindowClause, ParseError> {
    Ok(WindowClause {
        spec: WindowSpec {
            partition_by: clause
                .spec
                .partition_by
                .into_iter()
                .map(|expr| {
                    rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
                })
                .collect::<Result<Vec<_>, _>>()?,
            order_by: clause
                .spec
                .order_by
                .into_iter()
                .map(|item| {
                    Ok(crate::include::nodes::primnodes::OrderByEntry {
                        expr: rewrite_semantic_expr(
                            item.expr,
                            catalog,
                            expanded_views,
                            active_policy_relations,
                        )?,
                        ..item
                    })
                })
                .collect::<Result<Vec<_>, ParseError>>()?,
            frame: WindowFrame {
                mode: clause.spec.frame.mode,
                start_bound: rewrite_window_frame_bound(
                    clause.spec.frame.start_bound,
                    catalog,
                    expanded_views,
                    active_policy_relations,
                )?,
                end_bound: rewrite_window_frame_bound(
                    clause.spec.frame.end_bound,
                    catalog,
                    expanded_views,
                    active_policy_relations,
                )?,
            },
        },
        functions: clause
            .functions
            .into_iter()
            .map(|func| {
                rewrite_window_func_expr(func, catalog, expanded_views, active_policy_relations)
            })
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn rewrite_window_frame_bound(
    bound: WindowFrameBound,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
    active_policy_relations: &mut Vec<u32>,
) -> Result<WindowFrameBound, ParseError> {
    Ok(match bound {
        WindowFrameBound::OffsetPreceding(offset) => {
            let expr = rewrite_semantic_expr(
                offset.expr.clone(),
                catalog,
                expanded_views,
                active_policy_relations,
            )?;
            WindowFrameBound::OffsetPreceding(offset.with_expr(expr))
        }
        WindowFrameBound::OffsetFollowing(offset) => {
            let expr = rewrite_semantic_expr(
                offset.expr.clone(),
                catalog,
                expanded_views,
                active_policy_relations,
            )?;
            WindowFrameBound::OffsetFollowing(offset.with_expr(expr))
        }
        other => other,
    })
}

fn rewrite_window_func_expr(
    func: WindowFuncExpr,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
    active_policy_relations: &mut Vec<u32>,
) -> Result<WindowFuncExpr, ParseError> {
    Ok(WindowFuncExpr {
        kind: match func.kind {
            WindowFuncKind::Aggregate(aggref) => WindowFuncKind::Aggregate(
                match rewrite_semantic_expr(
                    Expr::Aggref(Box::new(aggref)),
                    catalog,
                    expanded_views,
                    active_policy_relations,
                )? {
                    Expr::Aggref(aggref) => *aggref,
                    other => unreachable!("aggregate rewrite returned non-Aggref: {other:?}"),
                },
            ),
            WindowFuncKind::Builtin(kind) => WindowFuncKind::Builtin(kind),
        },
        args: func
            .args
            .into_iter()
            .map(|arg| rewrite_semantic_expr(arg, catalog, expanded_views, active_policy_relations))
            .collect::<Result<Vec<_>, _>>()?,
        ..func
    })
}

fn rewrite_agg_accum(
    accum: AggAccum,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
    active_policy_relations: &mut Vec<u32>,
) -> Result<AggAccum, ParseError> {
    Ok(AggAccum {
        args: accum
            .args
            .into_iter()
            .map(|expr| {
                rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
            })
            .collect::<Result<Vec<_>, _>>()?,
        order_by: accum
            .order_by
            .into_iter()
            .map(|item| {
                Ok(crate::include::nodes::primnodes::OrderByEntry {
                    expr: rewrite_semantic_expr(
                        item.expr,
                        catalog,
                        expanded_views,
                        active_policy_relations,
                    )?,
                    ..item
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
        filter: accum
            .filter
            .map(|expr| {
                rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
            })
            .transpose()?,
        ..accum
    })
}

fn rewrite_set_returning_call(
    call: SetReturningCall,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
    active_policy_relations: &mut Vec<u32>,
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
            start: rewrite_semantic_expr(start, catalog, expanded_views, active_policy_relations)?,
            stop: rewrite_semantic_expr(stop, catalog, expanded_views, active_policy_relations)?,
            step: rewrite_semantic_expr(step, catalog, expanded_views, active_policy_relations)?,
            timezone: timezone
                .map(|timezone| {
                    rewrite_semantic_expr(
                        timezone,
                        catalog,
                        expanded_views,
                        active_policy_relations,
                    )
                })
                .transpose()?,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::GenerateSubscripts {
            func_oid,
            func_variadic,
            array,
            dimension,
            reverse,
            output_columns,
            with_ordinality,
        } => SetReturningCall::GenerateSubscripts {
            func_oid,
            func_variadic,
            array: rewrite_semantic_expr(array, catalog, expanded_views, active_policy_relations)?,
            dimension: rewrite_semantic_expr(
                dimension,
                catalog,
                expanded_views,
                active_policy_relations,
            )?,
            reverse: reverse
                .map(|reverse| {
                    rewrite_semantic_expr(reverse, catalog, expanded_views, active_policy_relations)
                })
                .transpose()?,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::PartitionTree {
            func_oid,
            func_variadic,
            relid,
            output_columns,
            with_ordinality,
        } => SetReturningCall::PartitionTree {
            func_oid,
            func_variadic,
            relid: rewrite_semantic_expr(relid, catalog, expanded_views, active_policy_relations)?,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::PartitionAncestors {
            func_oid,
            func_variadic,
            relid,
            output_columns,
            with_ordinality,
        } => SetReturningCall::PartitionAncestors {
            func_oid,
            func_variadic,
            relid: rewrite_semantic_expr(relid, catalog, expanded_views, active_policy_relations)?,
            output_columns,
            with_ordinality,
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
            arg: rewrite_semantic_expr(arg, catalog, expanded_views, active_policy_relations)?,
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
                .map(|expr| {
                    rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
                })
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
                .map(|expr| {
                    rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
                })
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
                .map(|expr| {
                    rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
                })
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
                .map(|expr| {
                    rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
                })
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
                .map(|expr| {
                    rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
                })
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
                .map(|expr| {
                    rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
                })
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
                .map(|expr| {
                    rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
                })
                .collect::<Result<Vec<_>, _>>()?,
            output_columns,
            with_ordinality,
        },
    })
}

fn rewrite_semantic_expr(
    expr: Expr,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
    active_policy_relations: &mut Vec<u32>,
) -> Result<Expr, ParseError> {
    Ok(match expr {
        other @ (Expr::Var(_)
        | Expr::Param(_)
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
        | Expr::LocalTimestamp { .. }) => other,
        Expr::Op(op) => Expr::Op(Box::new(crate::include::nodes::primnodes::OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| {
                    rewrite_semantic_expr(arg, catalog, expanded_views, active_policy_relations)
                })
                .collect::<Result<Vec<_>, _>>()?,
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(crate::include::nodes::primnodes::BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| {
                    rewrite_semantic_expr(arg, catalog, expanded_views, active_policy_relations)
                })
                .collect::<Result<Vec<_>, _>>()?,
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(crate::include::nodes::primnodes::FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| {
                    rewrite_semantic_expr(arg, catalog, expanded_views, active_policy_relations)
                })
                .collect::<Result<Vec<_>, _>>()?,
            ..*func
        })),
        Expr::SetReturning(srf) => Expr::SetReturning(Box::new(SetReturningExpr {
            call: rewrite_set_returning_call(
                srf.call,
                catalog,
                expanded_views,
                active_policy_relations,
            )?,
            ..*srf
        })),
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(crate::include::nodes::primnodes::Aggref {
            args: aggref
                .args
                .into_iter()
                .map(|arg| {
                    rewrite_semantic_expr(arg, catalog, expanded_views, active_policy_relations)
                })
                .collect::<Result<Vec<_>, _>>()?,
            aggorder: aggref
                .aggorder
                .into_iter()
                .map(|item| {
                    Ok(crate::include::nodes::primnodes::OrderByEntry {
                        expr: rewrite_semantic_expr(
                            item.expr,
                            catalog,
                            expanded_views,
                            active_policy_relations,
                        )?,
                        ..item
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
            aggfilter: aggref
                .aggfilter
                .map(|expr| {
                    rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
                })
                .transpose()?,
            ..*aggref
        })),
        Expr::WindowFunc(window_func) => Expr::WindowFunc(Box::new(rewrite_window_func_expr(
            *window_func,
            catalog,
            expanded_views,
            active_policy_relations,
        )?)),
        Expr::SubLink(sublink) => Expr::SubLink(Box::new(SubLink {
            testexpr: sublink
                .testexpr
                .map(|expr| {
                    rewrite_semantic_expr(*expr, catalog, expanded_views, active_policy_relations)
                })
                .transpose()?
                .map(Box::new),
            subselect: Box::new(rewrite_query(
                *sublink.subselect,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            ..*sublink
        })),
        Expr::SubPlan(_) => {
            return Err(ParseError::UnexpectedToken {
                expected: "semantic query expression before planning",
                actual: "SubPlan".into(),
            });
        }
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(
            crate::include::nodes::primnodes::ScalarArrayOpExpr {
                left: Box::new(rewrite_semantic_expr(
                    *saop.left,
                    catalog,
                    expanded_views,
                    active_policy_relations,
                )?),
                right: Box::new(rewrite_semantic_expr(
                    *saop.right,
                    catalog,
                    expanded_views,
                    active_policy_relations,
                )?),
                ..*saop
            },
        )),
        Expr::Xml(xml_expr) => {
            let crate::include::nodes::primnodes::XmlExpr {
                op,
                name,
                named_args,
                arg_names,
                args,
                xml_option,
                indent,
                target_type,
                standalone,
            } = *xml_expr;
            Expr::Xml(Box::new(crate::include::nodes::primnodes::XmlExpr {
                op,
                name,
                named_args: named_args
                    .into_iter()
                    .map(|arg| {
                        rewrite_semantic_expr(arg, catalog, expanded_views, active_policy_relations)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                arg_names,
                args: args
                    .into_iter()
                    .map(|arg| {
                        rewrite_semantic_expr(arg, catalog, expanded_views, active_policy_relations)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                xml_option,
                indent,
                target_type,
                standalone,
            }))
        }
        Expr::Cast(inner, ty) => Expr::Cast(
            Box::new(rewrite_semantic_expr(
                *inner,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            ty,
        ),
        Expr::Collate {
            expr,
            collation_oid,
        } => Expr::Collate {
            expr: Box::new(rewrite_semantic_expr(
                *expr,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            collation_oid,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(rewrite_semantic_expr(
            *inner,
            catalog,
            expanded_views,
            active_policy_relations,
        )?)),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(rewrite_semantic_expr(
            *inner,
            catalog,
            expanded_views,
            active_policy_relations,
        )?)),
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(rewrite_semantic_expr(
                *left,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            Box::new(rewrite_semantic_expr(
                *right,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(rewrite_semantic_expr(
                *left,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            Box::new(rewrite_semantic_expr(
                *right,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
        ),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
            collation_oid,
        } => Expr::Like {
            expr: Box::new(rewrite_semantic_expr(
                *expr,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            pattern: Box::new(rewrite_semantic_expr(
                *pattern,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            escape: escape
                .map(|expr| {
                    rewrite_semantic_expr(*expr, catalog, expanded_views, active_policy_relations)
                })
                .transpose()?
                .map(Box::new),
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
            expr: Box::new(rewrite_semantic_expr(
                *expr,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            pattern: Box::new(rewrite_semantic_expr(
                *pattern,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            escape: escape
                .map(|expr| {
                    rewrite_semantic_expr(*expr, catalog, expanded_views, active_policy_relations)
                })
                .transpose()?
                .map(Box::new),
            negated,
            collation_oid,
        },
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| {
                    rewrite_semantic_expr(element, catalog, expanded_views, active_policy_relations)
                })
                .collect::<Result<Vec<_>, _>>()?,
            array_type,
        },
        Expr::Row { descriptor, fields } => Expr::Row {
            descriptor,
            fields: fields
                .into_iter()
                .map(|(name, expr)| {
                    Ok((
                        name,
                        rewrite_semantic_expr(
                            expr,
                            catalog,
                            expanded_views,
                            active_policy_relations,
                        )?,
                    ))
                })
                .collect::<Result<Vec<_>, ParseError>>()?,
        },
        Expr::FieldSelect {
            expr,
            field,
            field_type,
        } => Expr::FieldSelect {
            expr: Box::new(rewrite_semantic_expr(
                *expr,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            field,
            field_type,
        },
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(rewrite_semantic_expr(
                *left,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            Box::new(rewrite_semantic_expr(
                *right,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
        ),
        Expr::Case(case_expr) => Expr::Case(Box::new(crate::include::nodes::primnodes::CaseExpr {
            arg: case_expr
                .arg
                .map(|arg| {
                    rewrite_semantic_expr(*arg, catalog, expanded_views, active_policy_relations)
                        .map(Box::new)
                })
                .transpose()?,
            args: case_expr
                .args
                .into_iter()
                .map(|arm| {
                    Ok(crate::include::nodes::primnodes::CaseWhen {
                        expr: rewrite_semantic_expr(
                            arm.expr,
                            catalog,
                            expanded_views,
                            active_policy_relations,
                        )?,
                        result: rewrite_semantic_expr(
                            arm.result,
                            catalog,
                            expanded_views,
                            active_policy_relations,
                        )?,
                    })
                })
                .collect::<Result<Vec<_>, ParseError>>()?,
            defresult: Box::new(rewrite_semantic_expr(
                *case_expr.defresult,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            ..*case_expr
        })),
        Expr::CaseTest(case_test) => Expr::CaseTest(case_test),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(rewrite_semantic_expr(
                *array,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| {
                    Ok(ExprArraySubscript {
                        is_slice: subscript.is_slice,
                        lower: subscript
                            .lower
                            .map(|expr| {
                                rewrite_semantic_expr(
                                    expr,
                                    catalog,
                                    expanded_views,
                                    active_policy_relations,
                                )
                            })
                            .transpose()?,
                        upper: subscript
                            .upper
                            .map(|expr| {
                                rewrite_semantic_expr(
                                    expr,
                                    catalog,
                                    expanded_views,
                                    active_policy_relations,
                                )
                            })
                            .transpose()?,
                    })
                })
                .collect::<Result<Vec<_>, ParseError>>()?,
        },
    })
}
