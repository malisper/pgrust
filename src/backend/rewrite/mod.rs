use crate::backend::parser::analyze::analyze_view_rule_sql;
use crate::backend::parser::{CatalogLookup, ParseError};
use crate::include::nodes::parsenodes::{Query, RangeTblEntry, RangeTblEntryKind};
use crate::include::nodes::primnodes::{
    AggAccum, Expr, ExprArraySubscript, ProjectSetTarget, SetReturningCall, SortGroupClause,
    SubLink, TargetEntry,
};

pub(crate) fn pg_rewrite_query(
    query: Query,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<Query>, ParseError> {
    Ok(vec![rewrite_query(query, catalog, &[])?])
}

fn rewrite_query(
    query: Query,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
) -> Result<Query, ParseError> {
    Ok(Query {
        rtable: query
            .rtable
            .into_iter()
            .map(|rte| rewrite_rte(rte, catalog, expanded_views))
            .collect::<Result<Vec<_>, _>>()?,
        where_qual: query
            .where_qual
            .map(|expr| rewrite_semantic_expr(expr, catalog, expanded_views))
            .transpose()?,
        group_by: query
            .group_by
            .into_iter()
            .map(|expr| rewrite_semantic_expr(expr, catalog, expanded_views))
            .collect::<Result<Vec<_>, _>>()?,
        accumulators: query
            .accumulators
            .into_iter()
            .map(|accum| rewrite_agg_accum(accum, catalog, expanded_views))
            .collect::<Result<Vec<_>, _>>()?,
        having_qual: query
            .having_qual
            .map(|expr| rewrite_semantic_expr(expr, catalog, expanded_views))
            .transpose()?,
        target_list: query
            .target_list
            .into_iter()
            .map(|target| rewrite_target_entry(target, catalog, expanded_views))
            .collect::<Result<Vec<_>, _>>()?,
        sort_clause: query
            .sort_clause
            .into_iter()
            .map(|clause| rewrite_sort_group_clause(clause, catalog, expanded_views))
            .collect::<Result<Vec<_>, _>>()?,
        project_set: query
            .project_set
            .map(|targets| {
                targets
                    .into_iter()
                    .map(|target| rewrite_project_set_target(target, catalog, expanded_views))
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?,
        recursive_union: query
            .recursive_union
            .map(|recursive_union| {
                Ok(Box::new(
                    crate::include::nodes::parsenodes::RecursiveUnionQuery {
                        output_desc: recursive_union.output_desc,
                        anchor: rewrite_query(recursive_union.anchor, catalog, expanded_views)?,
                        recursive: rewrite_query(
                            recursive_union.recursive,
                            catalog,
                            expanded_views,
                        )?,
                        distinct: recursive_union.distinct,
                        worktable_id: recursive_union.worktable_id,
                    },
                ))
            })
            .transpose()?,
        ..query
    })
}

fn rewrite_rte(
    rte: RangeTblEntry,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
) -> Result<RangeTblEntry, ParseError> {
    let kind = match rte.kind {
        RangeTblEntryKind::Relation {
            rel,
            relation_oid,
            relkind,
            toast,
        } if relkind == 'v' => {
            let analyzed = analyze_view_rule_sql(
                relation_oid,
                &rte.desc,
                rte.alias.as_deref(),
                catalog,
                expanded_views,
            )?;
            let mut next_views = expanded_views.to_vec();
            next_views.push(relation_oid);
            RangeTblEntryKind::Subquery {
                query: Box::new(rewrite_query(analyzed, catalog, &next_views)?),
            }
        }
        RangeTblEntryKind::Relation {
            rel,
            relation_oid,
            relkind,
            toast,
        } => RangeTblEntryKind::Relation {
            rel,
            relation_oid,
            relkind,
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
                .map(|expr| rewrite_semantic_expr(expr, catalog, expanded_views))
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
                        .map(|expr| rewrite_semantic_expr(expr, catalog, expanded_views))
                        .collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<Vec<_>, _>>()?,
            output_columns,
        },
        RangeTblEntryKind::Function { call } => RangeTblEntryKind::Function {
            call: rewrite_set_returning_call(call, catalog, expanded_views)?,
        },
        RangeTblEntryKind::Cte { cte_id, query } => RangeTblEntryKind::Cte {
            cte_id,
            query: Box::new(rewrite_query(*query, catalog, expanded_views)?),
        },
        RangeTblEntryKind::Subquery { query } => RangeTblEntryKind::Subquery {
            query: Box::new(rewrite_query(*query, catalog, expanded_views)?),
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
) -> Result<TargetEntry, ParseError> {
    Ok(TargetEntry {
        expr: rewrite_semantic_expr(target.expr, catalog, expanded_views)?,
        ..target
    })
}

fn rewrite_sort_group_clause(
    clause: SortGroupClause,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
) -> Result<SortGroupClause, ParseError> {
    Ok(SortGroupClause {
        expr: rewrite_semantic_expr(clause.expr, catalog, expanded_views)?,
        ..clause
    })
}

fn rewrite_project_set_target(
    target: ProjectSetTarget,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
) -> Result<ProjectSetTarget, ParseError> {
    match target {
        ProjectSetTarget::Scalar(entry) => Ok(ProjectSetTarget::Scalar(rewrite_target_entry(
            entry,
            catalog,
            expanded_views,
        )?)),
        ProjectSetTarget::Set {
            name,
            call,
            sql_type,
            column_index,
        } => Ok(ProjectSetTarget::Set {
            name,
            call: rewrite_set_returning_call(call, catalog, expanded_views)?,
            sql_type,
            column_index,
        }),
    }
}

fn rewrite_agg_accum(
    accum: AggAccum,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
) -> Result<AggAccum, ParseError> {
    Ok(AggAccum {
        args: accum
            .args
            .into_iter()
            .map(|expr| rewrite_semantic_expr(expr, catalog, expanded_views))
            .collect::<Result<Vec<_>, _>>()?,
        filter: accum
            .filter
            .map(|expr| rewrite_semantic_expr(expr, catalog, expanded_views))
            .transpose()?,
        ..accum
    })
}

fn rewrite_set_returning_call(
    call: SetReturningCall,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
) -> Result<SetReturningCall, ParseError> {
    Ok(match call {
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
            start: rewrite_semantic_expr(start, catalog, expanded_views)?,
            stop: rewrite_semantic_expr(stop, catalog, expanded_views)?,
            step: rewrite_semantic_expr(step, catalog, expanded_views)?,
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
                .map(|expr| rewrite_semantic_expr(expr, catalog, expanded_views))
                .collect::<Result<Vec<_>, _>>()?,
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
                .map(|expr| rewrite_semantic_expr(expr, catalog, expanded_views))
                .collect::<Result<Vec<_>, _>>()?,
            output_columns,
        },
        SetReturningCall::JsonPopulateRecordSet {
            func_oid,
            func_variadic,
            args,
            row_columns,
            output_columns,
            recordset,
            return_record_value,
        } => SetReturningCall::JsonPopulateRecordSet {
            func_oid,
            func_variadic,
            args: args
                .into_iter()
                .map(|expr| rewrite_semantic_expr(expr, catalog, expanded_views))
                .collect::<Result<Vec<_>, _>>()?,
            row_columns,
            output_columns,
            recordset,
            return_record_value,
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
                .map(|expr| rewrite_semantic_expr(expr, catalog, expanded_views))
                .collect::<Result<Vec<_>, _>>()?,
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
                .map(|expr| rewrite_semantic_expr(expr, catalog, expanded_views))
                .collect::<Result<Vec<_>, _>>()?,
            output_columns,
        },
        SetReturningCall::UserDefined {
            proc_oid,
            func_variadic,
            args,
            output_columns,
        } => SetReturningCall::UserDefined {
            proc_oid,
            func_variadic,
            args: args
                .into_iter()
                .map(|expr| rewrite_semantic_expr(expr, catalog, expanded_views))
                .collect::<Result<Vec<_>, _>>()?,
            output_columns,
        },
    })
}

fn rewrite_semantic_expr(
    expr: Expr,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
) -> Result<Expr, ParseError> {
    Ok(match expr {
        other @ (Expr::Var(_)
        | Expr::Param(_)
        | Expr::Const(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. }) => other,
        Expr::Op(op) => Expr::Op(Box::new(crate::include::nodes::primnodes::OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| rewrite_semantic_expr(arg, catalog, expanded_views))
                .collect::<Result<Vec<_>, _>>()?,
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(crate::include::nodes::primnodes::BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| rewrite_semantic_expr(arg, catalog, expanded_views))
                .collect::<Result<Vec<_>, _>>()?,
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(crate::include::nodes::primnodes::FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| rewrite_semantic_expr(arg, catalog, expanded_views))
                .collect::<Result<Vec<_>, _>>()?,
            ..*func
        })),
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(crate::include::nodes::primnodes::Aggref {
            args: aggref
                .args
                .into_iter()
                .map(|arg| rewrite_semantic_expr(arg, catalog, expanded_views))
                .collect::<Result<Vec<_>, _>>()?,
            aggfilter: aggref
                .aggfilter
                .map(|expr| rewrite_semantic_expr(expr, catalog, expanded_views))
                .transpose()?,
            ..*aggref
        })),
        Expr::SubLink(sublink) => Expr::SubLink(Box::new(SubLink {
            testexpr: sublink
                .testexpr
                .map(|expr| rewrite_semantic_expr(*expr, catalog, expanded_views))
                .transpose()?
                .map(Box::new),
            subselect: Box::new(rewrite_query(*sublink.subselect, catalog, expanded_views)?),
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
                left: Box::new(rewrite_semantic_expr(*saop.left, catalog, expanded_views)?),
                right: Box::new(rewrite_semantic_expr(*saop.right, catalog, expanded_views)?),
                ..*saop
            },
        )),
        Expr::Cast(inner, ty) => Expr::Cast(
            Box::new(rewrite_semantic_expr(*inner, catalog, expanded_views)?),
            ty,
        ),
        Expr::IsNull(inner) => Expr::IsNull(Box::new(rewrite_semantic_expr(
            *inner,
            catalog,
            expanded_views,
        )?)),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(rewrite_semantic_expr(
            *inner,
            catalog,
            expanded_views,
        )?)),
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(rewrite_semantic_expr(*left, catalog, expanded_views)?),
            Box::new(rewrite_semantic_expr(*right, catalog, expanded_views)?),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(rewrite_semantic_expr(*left, catalog, expanded_views)?),
            Box::new(rewrite_semantic_expr(*right, catalog, expanded_views)?),
        ),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(rewrite_semantic_expr(*expr, catalog, expanded_views)?),
            pattern: Box::new(rewrite_semantic_expr(*pattern, catalog, expanded_views)?),
            escape: escape
                .map(|expr| rewrite_semantic_expr(*expr, catalog, expanded_views))
                .transpose()?
                .map(Box::new),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(rewrite_semantic_expr(*expr, catalog, expanded_views)?),
            pattern: Box::new(rewrite_semantic_expr(*pattern, catalog, expanded_views)?),
            escape: escape
                .map(|expr| rewrite_semantic_expr(*expr, catalog, expanded_views))
                .transpose()?
                .map(Box::new),
            negated,
        },
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| rewrite_semantic_expr(element, catalog, expanded_views))
                .collect::<Result<Vec<_>, _>>()?,
            array_type,
        },
        Expr::Row { fields } => Expr::Row {
            fields: fields
                .into_iter()
                .map(|(name, expr)| {
                    Ok((name, rewrite_semantic_expr(expr, catalog, expanded_views)?))
                })
                .collect::<Result<Vec<_>, ParseError>>()?,
        },
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(rewrite_semantic_expr(*left, catalog, expanded_views)?),
            Box::new(rewrite_semantic_expr(*right, catalog, expanded_views)?),
        ),
        Expr::Case(case_expr) => Expr::Case(Box::new(crate::include::nodes::primnodes::CaseExpr {
            arg: case_expr
                .arg
                .map(|arg| rewrite_semantic_expr(*arg, catalog, expanded_views).map(Box::new))
                .transpose()?,
            args: case_expr
                .args
                .into_iter()
                .map(|arm| {
                    Ok(crate::include::nodes::primnodes::CaseWhen {
                        expr: rewrite_semantic_expr(arm.expr, catalog, expanded_views)?,
                        result: rewrite_semantic_expr(arm.result, catalog, expanded_views)?,
                    })
                })
                .collect::<Result<Vec<_>, ParseError>>()?,
            defresult: Box::new(rewrite_semantic_expr(
                *case_expr.defresult,
                catalog,
                expanded_views,
            )?),
            ..*case_expr
        })),
        Expr::CaseTest(case_test) => Expr::CaseTest(case_test),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(rewrite_semantic_expr(*array, catalog, expanded_views)?),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| {
                    Ok(ExprArraySubscript {
                        is_slice: subscript.is_slice,
                        lower: subscript
                            .lower
                            .map(|expr| rewrite_semantic_expr(expr, catalog, expanded_views))
                            .transpose()?,
                        upper: subscript
                            .upper
                            .map(|expr| rewrite_semantic_expr(expr, catalog, expanded_views))
                            .transpose()?,
                    })
                })
                .collect::<Result<Vec<_>, ParseError>>()?,
        },
    })
}
