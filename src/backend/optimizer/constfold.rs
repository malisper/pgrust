use crate::backend::executor::expr_geometry::eval_geometry_function;
use crate::backend::executor::expr_numeric::eval_power_function;
use crate::backend::executor::expr_ops::{
    add_values, bitwise_and_values, bitwise_not_value, bitwise_or_values, bitwise_xor_values,
    concat_values, div_values, mod_values, mul_values, negate_value, not_equal_values,
    order_values, shift_left_values, shift_right_values, sub_values, values_are_distinct,
};
use crate::backend::executor::{ExecError, Value, cast_value};
use crate::backend::parser::ParseError;
use crate::include::catalog::builtin_type_row_by_oid;
use crate::include::catalog::pg_proc::builtin_aggregate_function_for_proc_oid;
use crate::include::nodes::parsenodes::{
    JoinTreeNode, Query, RangeTblEntry, RangeTblEntryKind, RecursiveUnionQuery, SetOperationQuery,
    SqlType, SqlTypeKind,
};
use crate::include::nodes::primnodes::{
    AggAccum, AggFunc, Aggref, BoolExpr, BoolExprType, BuiltinScalarFunction, CaseExpr, CaseWhen,
    Expr, ExprArraySubscript, FuncExpr, OpExpr, OpExprKind, OrderByEntry, RowsFromSource,
    ScalarFunctionImpl, SetReturningCall, SortGroupClause, SqlJsonQueryFunction,
    SqlJsonTableBehavior, SqlJsonTablePassingArg, SubLinkType, TargetEntry, WindowClause,
    WindowFrame, WindowFrameBound, WindowFuncExpr, WindowFuncKind, XmlExpr,
};

pub(crate) fn fold_query_constants(query: Query) -> Result<Query, ParseError> {
    simplify_query(query)
}

pub(crate) fn fold_query_constants_best_effort(query: Query) -> Query {
    let original = query.clone();
    fold_query_constants(query).unwrap_or(original)
}

pub(crate) fn fold_expr_constants(expr: Expr) -> Result<Expr, ParseError> {
    simplify_expr(expr, None)
}

fn const_value_is_sql_null(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Record(record) => record
            .fields
            .iter()
            .all(|field| matches!(field, Value::Null)),
        _ => false,
    }
}

fn const_value_is_sql_not_null(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Record(record) => {
            !record.fields.is_empty()
                && record
                    .fields
                    .iter()
                    .all(|field| !matches!(field, Value::Null))
        }
        _ => true,
    }
}

fn simplify_query(query: Query) -> Result<Query, ParseError> {
    Ok(Query {
        command_type: query.command_type,
        depends_on_row_security: query.depends_on_row_security,
        rtable: query
            .rtable
            .into_iter()
            .map(simplify_rte)
            .collect::<Result<Vec<_>, _>>()?,
        jointree: query.jointree.map(simplify_jointree).transpose()?,
        target_list: query
            .target_list
            .into_iter()
            .map(simplify_target_entry)
            .collect::<Result<Vec<_>, _>>()?,
        distinct: query.distinct,
        distinct_on: query
            .distinct_on
            .into_iter()
            .map(simplify_sort_group_clause)
            .collect::<Result<Vec<_>, _>>()?,
        where_qual: query.where_qual.map(simplify_where_qual).transpose()?,
        group_by: query
            .group_by
            .into_iter()
            .map(|expr| simplify_expr(expr, None))
            .collect::<Result<Vec<_>, _>>()?,
        group_by_refs: query.group_by_refs,
        grouping_sets: query.grouping_sets,
        accumulators: query
            .accumulators
            .into_iter()
            .map(simplify_agg_accum)
            .collect::<Result<Vec<_>, _>>()?,
        window_clauses: query
            .window_clauses
            .into_iter()
            .map(simplify_window_clause)
            .collect::<Result<Vec<_>, _>>()?,
        having_qual: query
            .having_qual
            .map(|expr| simplify_expr(expr, None))
            .transpose()?,
        sort_clause: query
            .sort_clause
            .into_iter()
            .map(simplify_sort_group_clause)
            .collect::<Result<Vec<_>, _>>()?,
        constraint_deps: query.constraint_deps,
        limit_count: query.limit_count,
        limit_offset: query.limit_offset,
        locking_clause: query.locking_clause,
        locking_targets: query.locking_targets,
        row_marks: query.row_marks,
        has_target_srfs: query.has_target_srfs,
        recursive_union: query
            .recursive_union
            .map(|query| simplify_recursive_union(*query).map(Box::new))
            .transpose()?,
        set_operation: query
            .set_operation
            .map(|query| simplify_set_operation(*query).map(Box::new))
            .transpose()?,
    })
}

fn simple_query_where_qual_is_empty(query: &Query) -> bool {
    matches!(
        query.where_qual.as_ref(),
        Some(Expr::Const(Value::Bool(false)) | Expr::Const(Value::Null))
    ) && query.accumulators.is_empty()
        && query.group_by.is_empty()
        && query.having_qual.is_none()
        && query.set_operation.is_none()
        && query.recursive_union.is_none()
}

fn simplify_rte(rte: RangeTblEntry) -> Result<RangeTblEntry, ParseError> {
    Ok(RangeTblEntry {
        security_quals: rte
            .security_quals
            .into_iter()
            .map(|expr| simplify_expr(expr, None))
            .collect::<Result<Vec<_>, _>>()?,
        kind: match rte.kind {
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
                    .map(|expr| simplify_expr(expr, None))
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
                            .map(|expr| simplify_expr(expr, None))
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                output_columns,
            },
            RangeTblEntryKind::Function { call } => RangeTblEntryKind::Function {
                call: simplify_set_returning_call(call)?,
            },
            RangeTblEntryKind::Cte { cte_id, query } => RangeTblEntryKind::Cte {
                cte_id,
                query: Box::new(simplify_query(*query)?),
            },
            RangeTblEntryKind::Subquery { query } => RangeTblEntryKind::Subquery {
                query: Box::new(simplify_query(*query)?),
            },
            other => other,
        },
        ..rte
    })
}

fn simplify_jointree(node: JoinTreeNode) -> Result<JoinTreeNode, ParseError> {
    match node {
        JoinTreeNode::RangeTblRef(_) => Ok(node),
        JoinTreeNode::JoinExpr {
            left,
            right,
            kind,
            quals,
            rtindex,
        } => Ok(JoinTreeNode::JoinExpr {
            left: Box::new(simplify_jointree(*left)?),
            right: Box::new(simplify_jointree(*right)?),
            kind,
            quals: simplify_expr(quals, None)?,
            rtindex,
        }),
    }
}

fn simplify_recursive_union(query: RecursiveUnionQuery) -> Result<RecursiveUnionQuery, ParseError> {
    Ok(RecursiveUnionQuery {
        output_desc: query.output_desc,
        anchor: simplify_query(query.anchor)?,
        recursive: simplify_query(query.recursive)?,
        distinct: query.distinct,
        recursive_references_worktable: query.recursive_references_worktable,
        worktable_id: query.worktable_id,
    })
}

fn simplify_set_operation(query: SetOperationQuery) -> Result<SetOperationQuery, ParseError> {
    Ok(SetOperationQuery {
        output_desc: query.output_desc,
        op: query.op,
        inputs: query
            .inputs
            .into_iter()
            .map(simplify_query)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn simplify_target_entry(target: TargetEntry) -> Result<TargetEntry, ParseError> {
    Ok(TargetEntry {
        expr: simplify_expr(target.expr, None)?,
        ..target
    })
}

fn simplify_sort_group_clause(item: SortGroupClause) -> Result<SortGroupClause, ParseError> {
    Ok(SortGroupClause {
        expr: simplify_expr(item.expr, None)?,
        ..item
    })
}

fn simplify_order_by_entry(item: OrderByEntry) -> Result<OrderByEntry, ParseError> {
    Ok(OrderByEntry {
        expr: simplify_expr(item.expr, None)?,
        ..item
    })
}

fn simplify_set_returning_call(call: SetReturningCall) -> Result<SetReturningCall, ParseError> {
    Ok(match call {
        SetReturningCall::RowsFrom {
            items,
            output_columns,
            with_ordinality,
        } => SetReturningCall::RowsFrom {
            items: items
                .into_iter()
                .map(|item| {
                    Ok(crate::include::nodes::primnodes::RowsFromItem {
                        source: match item.source {
                            RowsFromSource::Function(call) => {
                                RowsFromSource::Function(simplify_set_returning_call(call)?)
                            }
                            RowsFromSource::Project {
                                output_exprs,
                                output_columns,
                            } => RowsFromSource::Project {
                                output_exprs: output_exprs
                                    .into_iter()
                                    .map(|expr| simplify_expr(expr, None))
                                    .collect::<Result<Vec<_>, ParseError>>()?,
                                output_columns,
                            },
                        },
                        column_definitions: item.column_definitions,
                    })
                })
                .collect::<Result<Vec<_>, ParseError>>()?,
            output_columns,
            with_ordinality,
        },
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
            start: simplify_expr(start, None)?,
            stop: simplify_expr(stop, None)?,
            step: simplify_expr(step, None)?,
            timezone: timezone
                .map(|timezone| simplify_expr(timezone, None))
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
            array: simplify_expr(array, None)?,
            dimension: simplify_expr(dimension, None)?,
            reverse: reverse
                .map(|reverse| simplify_expr(reverse, None))
                .transpose()?,
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
            args: simplify_exprs(args)?,
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
            args: simplify_exprs(args)?,
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
            args: simplify_exprs(args)?,
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
            args: simplify_exprs(args)?,
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
            args: simplify_exprs(args)?,
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
            relid: simplify_expr(relid, None)?,
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
            relid: simplify_expr(relid, None)?,
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
        SetReturningCall::PgSequences {
            output_columns,
            with_ordinality,
        } => SetReturningCall::PgSequences {
            output_columns,
            with_ordinality,
        },
        SetReturningCall::InformationSchemaSequences {
            output_columns,
            with_ordinality,
        } => SetReturningCall::InformationSchemaSequences {
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
            arg: simplify_expr(arg, None)?,
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
            args: simplify_exprs(args)?,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::UserDefined {
            proc_oid,
            function_name,
            func_variadic,
            args,
            inlined_expr,
            output_columns,
            with_ordinality,
        } => SetReturningCall::UserDefined {
            proc_oid,
            function_name,
            func_variadic,
            args: simplify_exprs(args)?,
            inlined_expr: inlined_expr
                .map(|expr| simplify_expr(*expr, None).map(Box::new))
                .transpose()?,
            output_columns,
            with_ordinality,
        },
        sql @ (SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_)) => {
            sql.try_map_exprs(|expr| simplify_expr(expr, None))?
        }
    })
}

fn simplify_agg_accum(accum: AggAccum) -> Result<AggAccum, ParseError> {
    let args = simplify_exprs(accum.args)?;
    Ok(AggAccum {
        args: canonicalize_aggregate_args(accum.aggfnoid, accum.distinct, args),
        order_by: accum
            .order_by
            .into_iter()
            .map(simplify_order_by_entry)
            .collect::<Result<Vec<_>, _>>()?,
        filter: accum
            .filter
            .map(|expr| simplify_expr(expr, None))
            .transpose()?,
        ..accum
    })
}

fn simplify_aggref(aggref: Aggref) -> Result<Aggref, ParseError> {
    let args = simplify_exprs(aggref.args)?;
    Ok(Aggref {
        args: canonicalize_aggregate_args(aggref.aggfnoid, aggref.aggdistinct, args),
        aggorder: aggref
            .aggorder
            .into_iter()
            .map(simplify_order_by_entry)
            .collect::<Result<Vec<_>, _>>()?,
        aggfilter: aggref
            .aggfilter
            .map(|expr| simplify_expr(expr, None))
            .transpose()?,
        ..aggref
    })
}

fn canonicalize_aggregate_args(aggfnoid: u32, distinct: bool, args: Vec<Expr>) -> Vec<Expr> {
    if distinct
        || builtin_aggregate_function_for_proc_oid(aggfnoid) != Some(AggFunc::Count)
        || args.len() != 1
        || !expr_is_known_nonnull(&args[0])
    {
        return args;
    }
    Vec::new()
}

fn expr_is_known_nonnull(expr: &Expr) -> bool {
    match expr {
        Expr::Const(Value::Null) => false,
        Expr::Const(_) => true,
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => expr_is_known_nonnull(inner),
        _ => false,
    }
}

fn simplify_window_clause(clause: WindowClause) -> Result<WindowClause, ParseError> {
    Ok(WindowClause {
        spec: simplify_window_spec(clause.spec)?,
        functions: clause
            .functions
            .into_iter()
            .map(simplify_window_func_expr)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn simplify_window_spec(
    spec: crate::include::nodes::primnodes::WindowSpec,
) -> Result<crate::include::nodes::primnodes::WindowSpec, ParseError> {
    Ok(crate::include::nodes::primnodes::WindowSpec {
        partition_by: simplify_exprs(spec.partition_by)?,
        order_by: spec
            .order_by
            .into_iter()
            .map(simplify_order_by_entry)
            .collect::<Result<Vec<_>, _>>()?,
        frame: simplify_window_frame(spec.frame)?,
    })
}

fn simplify_window_frame(frame: WindowFrame) -> Result<WindowFrame, ParseError> {
    Ok(WindowFrame {
        mode: frame.mode,
        start_bound: simplify_window_frame_bound(frame.start_bound)?,
        end_bound: simplify_window_frame_bound(frame.end_bound)?,
        exclusion: frame.exclusion,
    })
}

fn simplify_window_frame_bound(bound: WindowFrameBound) -> Result<WindowFrameBound, ParseError> {
    Ok(match bound {
        WindowFrameBound::OffsetPreceding(offset) => {
            let expr = simplify_expr(offset.expr.clone(), None)?;
            WindowFrameBound::OffsetPreceding(offset.with_expr(expr))
        }
        WindowFrameBound::OffsetFollowing(offset) => {
            let expr = simplify_expr(offset.expr.clone(), None)?;
            WindowFrameBound::OffsetFollowing(offset.with_expr(expr))
        }
        other => other,
    })
}

fn simplify_window_func_expr(window_func: WindowFuncExpr) -> Result<WindowFuncExpr, ParseError> {
    Ok(WindowFuncExpr {
        kind: match window_func.kind {
            WindowFuncKind::Aggregate(aggref) => {
                WindowFuncKind::Aggregate(simplify_aggref(aggref)?)
            }
            other => other,
        },
        args: simplify_exprs(window_func.args)?,
        ..window_func
    })
}

fn simplify_xml_expr(xml: XmlExpr, case_test_value: Option<&Value>) -> Result<XmlExpr, ParseError> {
    Ok(XmlExpr {
        named_args: xml
            .named_args
            .into_iter()
            .map(|expr| simplify_expr(expr, case_test_value))
            .collect::<Result<Vec<_>, _>>()?,
        args: xml
            .args
            .into_iter()
            .map(|expr| simplify_expr(expr, case_test_value))
            .collect::<Result<Vec<_>, _>>()?,
        ..xml
    })
}

fn simplify_exprs(exprs: Vec<Expr>) -> Result<Vec<Expr>, ParseError> {
    exprs
        .into_iter()
        .map(|expr| simplify_expr(expr, None))
        .collect::<Result<Vec<_>, _>>()
}

fn simplify_func_expr(func: FuncExpr, case_test_value: Option<&Value>) -> Result<Expr, ParseError> {
    if builtin_preserves_arg_types_during_constfold(func.implementation) {
        return Ok(Expr::Func(Box::new(func)));
    }
    let args = func
        .args
        .into_iter()
        .map(|expr| simplify_expr(expr, case_test_value))
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(values) = const_expr_values(&args)
        && let Some(expr) =
            try_fold_optional_eval(evaluate_const_func(func.implementation, &values))?
    {
        return Ok(expr);
    }
    Ok(Expr::Func(Box::new(FuncExpr { args, ..func })))
}

fn builtin_preserves_arg_types_during_constfold(implementation: ScalarFunctionImpl) -> bool {
    matches!(
        implementation,
        ScalarFunctionImpl::Builtin(
            BuiltinScalarFunction::PgRestoreRelationStats
                | BuiltinScalarFunction::PgClearRelationStats
                | BuiltinScalarFunction::PgRestoreAttributeStats
                | BuiltinScalarFunction::PgClearAttributeStats
                | BuiltinScalarFunction::SatisfiesHashPartition
        )
    )
}

fn simplify_expr(expr: Expr, case_test_value: Option<&Value>) -> Result<Expr, ParseError> {
    match expr {
        other @ (Expr::Var(_)
        | Expr::Param(_)
        | Expr::Const(_)
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
        | Expr::LocalTimestamp { .. }) => Ok(other),
        Expr::CaseTest(case_test) => Ok(case_test_value
            .cloned()
            .map(Expr::Const)
            .unwrap_or(Expr::CaseTest(case_test))),
        Expr::Aggref(aggref) => Ok(Expr::Aggref(Box::new(simplify_aggref(*aggref)?))),
        Expr::GroupingKey(grouping_key) => Ok(Expr::GroupingKey(Box::new(
            crate::include::nodes::primnodes::GroupingKeyExpr {
                expr: Box::new(simplify_expr(*grouping_key.expr, case_test_value)?),
                ref_id: grouping_key.ref_id,
            },
        ))),
        Expr::GroupingFunc(grouping_func) => Ok(Expr::GroupingFunc(Box::new(
            crate::include::nodes::primnodes::GroupingFuncExpr {
                args: grouping_func
                    .args
                    .into_iter()
                    .map(|arg| simplify_expr(arg, case_test_value))
                    .collect::<Result<Vec<_>, _>>()?,
                ..*grouping_func
            },
        ))),
        Expr::WindowFunc(window_func) => Ok(Expr::WindowFunc(Box::new(simplify_window_func_expr(
            *window_func,
        )?))),
        Expr::Op(op) => simplify_op_expr(*op, case_test_value),
        Expr::Bool(bool_expr) => simplify_bool_expr(*bool_expr, case_test_value),
        Expr::Case(case_expr) => simplify_case_expr(*case_expr, case_test_value),
        Expr::Func(func) => simplify_func_expr(*func, case_test_value),
        Expr::SqlJsonQueryFunction(func) => {
            Ok(Expr::SqlJsonQueryFunction(Box::new(SqlJsonQueryFunction {
                context: simplify_expr(func.context, case_test_value)?,
                path: simplify_expr(func.path, case_test_value)?,
                passing: func
                    .passing
                    .into_iter()
                    .map(|arg| {
                        Ok(SqlJsonTablePassingArg {
                            name: arg.name,
                            expr: simplify_expr(arg.expr, case_test_value)?,
                        })
                    })
                    .collect::<Result<Vec<_>, ParseError>>()?,
                on_empty: simplify_sql_json_behavior(func.on_empty, case_test_value)?,
                on_error: simplify_sql_json_behavior(func.on_error, case_test_value)?,
                ..*func
            })))
        }
        Expr::SetReturning(srf) => Ok(Expr::SetReturning(Box::new(
            crate::include::nodes::primnodes::SetReturningExpr {
                call: simplify_set_returning_call(srf.call)?,
                ..*srf
            },
        ))),
        Expr::SubLink(sublink) => {
            let testexpr = sublink
                .testexpr
                .map(|expr| simplify_expr(*expr, case_test_value).map(Box::new))
                .transpose()?;
            let subselect = simplify_query(*sublink.subselect)?;
            if matches!(sublink.sublink_type, SubLinkType::ExistsSubLink)
                && simple_query_where_qual_is_empty(&subselect)
            {
                return Ok(Expr::Const(Value::Bool(false)));
            }
            Ok(Expr::SubLink(Box::new(
                crate::include::nodes::primnodes::SubLink {
                    testexpr,
                    subselect: Box::new(subselect),
                    ..*sublink
                },
            )))
        }
        Expr::SubPlan(subplan) => Ok(Expr::SubPlan(Box::new(
            crate::include::nodes::primnodes::SubPlan {
                testexpr: subplan
                    .testexpr
                    .map(|expr| simplify_expr(*expr, case_test_value).map(Box::new))
                    .transpose()?,
                args: subplan
                    .args
                    .into_iter()
                    .map(|expr| simplify_expr(expr, case_test_value))
                    .collect::<Result<Vec<_>, _>>()?,
                ..*subplan
            },
        ))),
        Expr::ScalarArrayOp(saop) => Ok(Expr::ScalarArrayOp(Box::new(
            crate::include::nodes::primnodes::ScalarArrayOpExpr {
                left: Box::new(simplify_expr(*saop.left, case_test_value)?),
                right: Box::new(simplify_scalar_array_rhs_expr(
                    *saop.right,
                    case_test_value,
                )?),
                ..*saop
            },
        ))),
        Expr::Xml(xml) => Ok(Expr::Xml(Box::new(simplify_xml_expr(
            *xml,
            case_test_value,
        )?))),
        Expr::Cast(inner, ty) => {
            let inner = simplify_expr(*inner, case_test_value)?;
            if let Expr::Const(value) = &inner {
                if cast_is_const_fold_safe(value, ty)
                    && let Some(expr) = try_fold_eval(cast_value(value.clone(), ty))?
                {
                    return Ok(expr);
                }
            }
            Ok(Expr::Cast(Box::new(inner), ty))
        }
        Expr::Collate {
            expr,
            collation_oid,
        } => {
            let expr = simplify_expr(*expr, case_test_value)?;
            Ok(Expr::Collate {
                expr: Box::new(expr),
                collation_oid,
            })
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
            collation_oid,
        } => Ok(Expr::Like {
            expr: Box::new(simplify_expr(*expr, case_test_value)?),
            pattern: Box::new(simplify_expr(*pattern, case_test_value)?),
            escape: escape
                .map(|expr| simplify_expr(*expr, case_test_value).map(Box::new))
                .transpose()?,
            case_insensitive,
            negated,
            collation_oid,
        }),
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
            collation_oid,
        } => Ok(Expr::Similar {
            expr: Box::new(simplify_expr(*expr, case_test_value)?),
            pattern: Box::new(simplify_expr(*pattern, case_test_value)?),
            escape: escape
                .map(|expr| simplify_expr(*expr, case_test_value).map(Box::new))
                .transpose()?,
            negated,
            collation_oid,
        }),
        Expr::IsNull(inner) => {
            let inner = simplify_expr(*inner, case_test_value)?;
            Ok(match inner {
                Expr::Const(value) => Expr::Const(Value::Bool(const_value_is_sql_null(&value))),
                other => Expr::IsNull(Box::new(other)),
            })
        }
        Expr::IsNotNull(inner) => {
            let inner = simplify_expr(*inner, case_test_value)?;
            Ok(match inner {
                Expr::Const(value) => Expr::Const(Value::Bool(const_value_is_sql_not_null(&value))),
                other => Expr::IsNotNull(Box::new(other)),
            })
        }
        Expr::IsDistinctFrom(left, right) => {
            let left = simplify_expr(*left, case_test_value)?;
            let right = simplify_expr(*right, case_test_value)?;
            match (&left, &right) {
                (Expr::Const(left), Expr::Const(right)) => {
                    Ok(Expr::Const(Value::Bool(values_are_distinct(left, right))))
                }
                _ => Ok(Expr::IsDistinctFrom(Box::new(left), Box::new(right))),
            }
        }
        Expr::IsNotDistinctFrom(left, right) => {
            let left = simplify_expr(*left, case_test_value)?;
            let right = simplify_expr(*right, case_test_value)?;
            match (&left, &right) {
                (Expr::Const(left), Expr::Const(right)) => {
                    Ok(Expr::Const(Value::Bool(!values_are_distinct(left, right))))
                }
                _ => Ok(Expr::IsNotDistinctFrom(Box::new(left), Box::new(right))),
            }
        }
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Ok(Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|expr| simplify_expr(expr, case_test_value))
                .collect::<Result<Vec<_>, _>>()?,
            array_type,
        }),
        Expr::Row { descriptor, fields } => Ok(Expr::Row {
            descriptor: descriptor.clone(),
            fields: fields
                .into_iter()
                .map(|(name, expr)| Ok((name, simplify_expr(expr, case_test_value)?)))
                .collect::<Result<Vec<_>, ParseError>>()?,
        })
        .and_then(|expr| {
            if let Expr::Row { descriptor, fields } = expr {
                if let Some(values) = fields
                    .iter()
                    .map(|(_, expr)| const_expr_value(expr).cloned())
                    .collect::<Option<Vec<_>>>()
                {
                    return Ok(Expr::Const(Value::Record(
                        crate::include::nodes::datum::RecordValue::from_descriptor(
                            descriptor, values,
                        ),
                    )));
                }
                Ok(Expr::Row { descriptor, fields })
            } else {
                Ok(expr)
            }
        }),
        Expr::FieldSelect {
            expr,
            field,
            field_type,
        } => {
            let expr = simplify_expr(*expr, case_test_value)?;
            if let Expr::Row { fields, .. } = &expr
                && let Some((_, selected)) = fields
                    .iter()
                    .find(|(candidate, _)| candidate.eq_ignore_ascii_case(&field))
            {
                return Ok(selected.clone());
            }
            if let Expr::Cast(inner, _) = &expr
                && let Expr::Row { fields, .. } = inner.as_ref()
                && let Some((_, selected)) = fields
                    .iter()
                    .find(|(candidate, _)| candidate.eq_ignore_ascii_case(&field))
            {
                return Ok(selected.clone());
            }
            Ok(Expr::FieldSelect {
                expr: Box::new(expr),
                field,
                field_type,
            })
        }
        Expr::Coalesce(left, right) => simplify_coalesce_expr(*left, *right, case_test_value),
        Expr::ArraySubscript { array, subscripts } => Ok(Expr::ArraySubscript {
            array: Box::new(simplify_expr(*array, case_test_value)?),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| simplify_array_subscript(subscript, case_test_value))
                .collect::<Result<Vec<_>, _>>()?,
        }),
    }
}

fn simplify_sql_json_behavior(
    behavior: SqlJsonTableBehavior,
    case_test_value: Option<&Value>,
) -> Result<SqlJsonTableBehavior, ParseError> {
    match behavior {
        SqlJsonTableBehavior::Default(expr) => Ok(SqlJsonTableBehavior::Default(simplify_expr(
            expr,
            case_test_value,
        )?)),
        other => Ok(other),
    }
}

fn simplify_scalar_array_rhs_expr(
    expr: Expr,
    case_test_value: Option<&Value>,
) -> Result<Expr, ParseError> {
    match expr {
        Expr::Cast(inner, ty) => {
            let inner = simplify_expr(*inner, case_test_value)?;
            if ty.is_array && matches!(inner, Expr::Const(Value::Null)) {
                Ok(Expr::Cast(Box::new(inner), ty))
            } else {
                simplify_expr(Expr::Cast(Box::new(inner), ty), case_test_value)
            }
        }
        Expr::Collate {
            expr,
            collation_oid,
        } => Ok(Expr::Collate {
            expr: Box::new(simplify_scalar_array_rhs_expr(*expr, case_test_value)?),
            collation_oid,
        }),
        other => simplify_expr(other, case_test_value),
    }
}

fn evaluate_const_func(
    implementation: ScalarFunctionImpl,
    args: &[Value],
) -> Result<Option<Value>, ExecError> {
    if let ScalarFunctionImpl::Builtin(builtin) = implementation
        && let Some(result) = eval_geometry_function(builtin, args)
    {
        return result.map(Some);
    }
    match implementation {
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::Power) => {
            eval_power_function(args).map(Some)
        }
        _ => Ok(None),
    }
}

fn simplify_where_qual(expr: Expr) -> Result<Expr, ParseError> {
    simplify_expr(expr, None).map(simplify_where_null_scalar_array_ops)
}

fn simplify_where_null_scalar_array_ops(expr: Expr) -> Expr {
    match expr {
        Expr::Const(Value::Null) => Expr::Const(Value::Bool(false)),
        Expr::Bool(bool_expr) => {
            let args = bool_expr
                .args
                .into_iter()
                .map(simplify_where_null_scalar_array_ops)
                .collect::<Vec<_>>();
            match bool_expr.boolop {
                BoolExprType::And => simplify_where_and_args(args),
                BoolExprType::Or => simplify_where_or_args(args),
                BoolExprType::Not => Expr::Bool(Box::new(BoolExpr {
                    boolop: BoolExprType::Not,
                    args,
                })),
            }
        }
        other => other,
    }
}

fn simplify_where_and_args(args: Vec<Expr>) -> Expr {
    let mut kept = Vec::new();
    for arg in args {
        match arg {
            Expr::Const(Value::Bool(false)) | Expr::Const(Value::Null) => {
                return Expr::Const(Value::Bool(false));
            }
            Expr::Const(Value::Bool(true)) => {}
            other => kept.push(other),
        }
    }
    match kept.len() {
        0 => Expr::Const(Value::Bool(true)),
        1 => kept.pop().expect("one arg"),
        _ => Expr::Bool(Box::new(BoolExpr {
            boolop: BoolExprType::And,
            args: kept,
        })),
    }
}

fn simplify_where_or_args(args: Vec<Expr>) -> Expr {
    let mut kept = Vec::new();
    for arg in args {
        match arg {
            Expr::Const(Value::Bool(true)) => return Expr::Const(Value::Bool(true)),
            Expr::Const(Value::Bool(false)) | Expr::Const(Value::Null) => {}
            other => kept.push(other),
        }
    }
    match kept.len() {
        0 => Expr::Const(Value::Bool(false)),
        1 => kept.pop().expect("one arg"),
        _ => Expr::Bool(Box::new(BoolExpr {
            boolop: BoolExprType::Or,
            args: kept,
        })),
    }
}

fn cast_is_const_fold_safe(value: &Value, target: SqlType) -> bool {
    if matches!(
        target.kind,
        SqlTypeKind::Name
            | SqlTypeKind::Oid
            | SqlTypeKind::RegProc
            | SqlTypeKind::RegClass
            | SqlTypeKind::RegType
            | SqlTypeKind::RegRole
            | SqlTypeKind::RegNamespace
            | SqlTypeKind::RegOper
            | SqlTypeKind::RegOperator
            | SqlTypeKind::RegProcedure
            | SqlTypeKind::RegCollation
            | SqlTypeKind::RegConfig
            | SqlTypeKind::RegDictionary
            | SqlTypeKind::Int2Vector
            | SqlTypeKind::OidVector
    ) {
        return false;
    }
    if matches!(value, Value::Null)
        && matches!(target.kind, SqlTypeKind::Record | SqlTypeKind::Composite)
    {
        return false;
    }
    // :HACK: Dynamic type OIDs can be domains whose nullability and CHECK
    // constraints require the executor's catalog-aware cast path.
    if target.type_oid != 0 && builtin_type_row_by_oid(target.type_oid).is_none() {
        return false;
    }
    let Some(source) = value.sql_type_hint() else {
        return true;
    };
    // Datetime text input can depend on DateStyle, TimeZone, IntervalStyle, or
    // the transaction timestamp for special values like now/today.
    if matches!(
        target.element_type().kind,
        SqlTypeKind::Date
            | SqlTypeKind::Time
            | SqlTypeKind::TimeTz
            | SqlTypeKind::Timestamp
            | SqlTypeKind::TimestampTz
            | SqlTypeKind::Interval
    ) && matches!(
        source.element_type().kind,
        SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar
    ) {
        return false;
    }
    if !target.is_array
        && matches!(
            target.kind,
            SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar
        )
        && matches!(
            source.element_type().kind,
            SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar
        )
    {
        return false;
    }
    !matches!(
        (source.kind, target.kind),
        (SqlTypeKind::TimestampTz | SqlTypeKind::TimeTz, _)
            | (_, SqlTypeKind::TimestampTz | SqlTypeKind::TimeTz)
    )
}

fn simplify_array_subscript(
    subscript: ExprArraySubscript,
    case_test_value: Option<&Value>,
) -> Result<ExprArraySubscript, ParseError> {
    Ok(ExprArraySubscript {
        lower: subscript
            .lower
            .map(|expr| simplify_expr(expr, case_test_value))
            .transpose()?,
        upper: subscript
            .upper
            .map(|expr| simplify_expr(expr, case_test_value))
            .transpose()?,
        ..subscript
    })
}

fn simplify_coalesce_expr(
    left: Expr,
    right: Expr,
    case_test_value: Option<&Value>,
) -> Result<Expr, ParseError> {
    let left = simplify_expr(left, case_test_value)?;
    match left {
        Expr::Const(Value::Null) => simplify_expr(right, case_test_value),
        Expr::Const(_) => Ok(left),
        left => {
            let right = simplify_expr(right, case_test_value)?;
            if matches!(right, Expr::Const(Value::Null)) {
                Ok(left)
            } else {
                Ok(Expr::Coalesce(Box::new(left), Box::new(right)))
            }
        }
    }
}

fn simplify_case_expr(
    case_expr: CaseExpr,
    _outer_case_test_value: Option<&Value>,
) -> Result<Expr, ParseError> {
    let arg = case_expr
        .arg
        .map(|expr| simplify_expr(*expr, None).map(Box::new))
        .transpose()?;
    let case_test_value = arg.as_deref().and_then(const_expr_value).cloned();
    let mut new_args = Vec::new();
    let mut defresult = None;

    for arm in case_expr.args {
        let cond = simplify_expr(arm.expr, case_test_value.as_ref())?;
        if matches!(
            cond,
            Expr::Const(Value::Null) | Expr::Const(Value::Bool(false))
        ) {
            continue;
        }
        if matches!(cond, Expr::Const(Value::Bool(true))) {
            defresult = Some(simplify_expr(arm.result, case_test_value.as_ref())?);
            break;
        }
        new_args.push(CaseWhen {
            expr: cond,
            result: simplify_expr(arm.result, case_test_value.as_ref())?,
        });
    }

    let defresult = match defresult {
        Some(result) => result,
        None => simplify_expr(*case_expr.defresult, case_test_value.as_ref())?,
    };
    if new_args.is_empty() {
        return Ok(defresult);
    }
    Ok(Expr::Case(Box::new(CaseExpr {
        casetype: case_expr.casetype,
        arg: arg.filter(|expr| !matches!(expr.as_ref(), Expr::Const(_))),
        args: new_args,
        defresult: Box::new(defresult),
    })))
}

fn simplify_bool_expr(
    bool_expr: BoolExpr,
    case_test_value: Option<&Value>,
) -> Result<Expr, ParseError> {
    match bool_expr.boolop {
        BoolExprType::Not => {
            let [inner] = bool_expr.args.try_into().map_err(|_| {
                ParseError::FeatureNotSupportedMessage("malformed NOT expression".into())
            })?;
            let inner = simplify_expr(inner, case_test_value)?;
            Ok(match inner {
                Expr::Const(Value::Bool(value)) => Expr::Const(Value::Bool(!value)),
                Expr::Const(Value::Null) => Expr::Const(Value::Null),
                other => Expr::Bool(Box::new(BoolExpr {
                    boolop: BoolExprType::Not,
                    args: vec![other],
                })),
            })
        }
        BoolExprType::And => {
            let mut args = Vec::new();
            let mut saw_null = false;
            for arg in bool_expr.args {
                let arg = simplify_expr(arg, case_test_value)?;
                match arg {
                    Expr::Const(Value::Bool(false)) => return Ok(Expr::Const(Value::Bool(false))),
                    Expr::Const(Value::Bool(true)) => {}
                    Expr::Const(Value::Null) => saw_null = true,
                    other => args.push(other),
                }
            }
            if args.is_empty() {
                return Ok(if saw_null {
                    Expr::Const(Value::Null)
                } else {
                    Expr::Const(Value::Bool(true))
                });
            }
            if args.len() == 1 && !saw_null {
                return Ok(args.pop().expect("one bool arg"));
            }
            if and_args_have_contradictory_equalities(&args) {
                return Ok(Expr::Const(Value::Bool(false)));
            }
            if saw_null {
                args.push(Expr::Const(Value::Null));
            }
            Ok(Expr::Bool(Box::new(BoolExpr {
                boolop: BoolExprType::And,
                args,
            })))
        }
        BoolExprType::Or => {
            let mut args = Vec::new();
            let mut saw_null = false;
            for arg in bool_expr.args {
                let arg = simplify_expr(arg, case_test_value)?;
                match arg {
                    Expr::Const(Value::Bool(true)) => return Ok(Expr::Const(Value::Bool(true))),
                    Expr::Const(Value::Bool(false)) => {}
                    Expr::Const(Value::Null) => saw_null = true,
                    other => args.push(other),
                }
            }
            if args.is_empty() {
                return Ok(if saw_null {
                    Expr::Const(Value::Null)
                } else {
                    Expr::Const(Value::Bool(false))
                });
            }
            if args.len() == 1 && !saw_null {
                return Ok(args.pop().expect("one bool arg"));
            }
            if saw_null {
                args.push(Expr::Const(Value::Null));
            }
            Ok(Expr::Bool(Box::new(BoolExpr {
                boolop: BoolExprType::Or,
                args,
            })))
        }
    }
}

fn and_args_have_contradictory_equalities(args: &[Expr]) -> bool {
    let mut equalities: Vec<(&Expr, &Value, Option<u32>)> = Vec::new();
    for arg in args {
        let Some((expr, value, collation_oid)) = equality_to_const(arg) else {
            continue;
        };
        if matches!(value, Value::Null) {
            continue;
        }
        if equalities
            .iter()
            .any(|(existing_expr, existing_value, existing_collation_oid)| {
                *existing_expr == expr
                    && *existing_collation_oid == collation_oid
                    && *existing_value != value
            })
        {
            return true;
        }
        equalities.push((expr, value, collation_oid));
    }
    false
}

fn equality_to_const(expr: &Expr) -> Option<(&Expr, &Value, Option<u32>)> {
    let Expr::Op(op) = expr else {
        return None;
    };
    if op.op != OpExprKind::Eq || op.args.len() != 2 {
        return None;
    }
    let collation_oid = op_equality_collation(op);
    match (&op.args[0], &op.args[1]) {
        (left, Expr::Const(value)) => Some((left, value, collation_oid)),
        (Expr::Const(value), right) => Some((right, value, collation_oid)),
        _ => None,
    }
}

fn op_equality_collation(op: &OpExpr) -> Option<u32> {
    op.collation_oid
        .or_else(|| op.args.iter().find_map(top_level_explicit_collation))
}

fn top_level_explicit_collation(expr: &Expr) -> Option<u32> {
    match expr {
        Expr::Collate { collation_oid, .. } => Some(*collation_oid),
        Expr::Cast(inner, _) => top_level_explicit_collation(inner),
        _ => None,
    }
}

fn simplify_op_expr(op: OpExpr, case_test_value: Option<&Value>) -> Result<Expr, ParseError> {
    let args = op
        .args
        .into_iter()
        .map(|expr| simplify_expr(expr, case_test_value))
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(values) = const_expr_values(&args) {
        if let Some(expr) =
            try_fold_optional_eval(evaluate_const_op(op.op, &values, op.collation_oid))?
        {
            return Ok(expr);
        }
    }
    Ok(Expr::Op(Box::new(OpExpr { args, ..op })))
}

fn evaluate_const_op(
    op: OpExprKind,
    args: &[Value],
    collation_oid: Option<u32>,
) -> Result<Option<Value>, ExecError> {
    let value = match (op, args) {
        (OpExprKind::UnaryPlus, [value]) => value.clone(),
        (OpExprKind::Negate, [value]) => negate_value(value.clone())?,
        (OpExprKind::BitNot, [value]) => bitwise_not_value(value.clone())?,
        (OpExprKind::Add, [left, right]) => add_values(left.clone(), right.clone())?,
        (OpExprKind::Sub, [left, right]) => sub_values(left.clone(), right.clone())?,
        (OpExprKind::BitAnd, [left, right]) => bitwise_and_values(left.clone(), right.clone())?,
        (OpExprKind::BitOr, [left, right]) => bitwise_or_values(left.clone(), right.clone())?,
        (OpExprKind::BitXor, [left, right]) => bitwise_xor_values(left.clone(), right.clone())?,
        (OpExprKind::Shl, [left, right]) => shift_left_values(left.clone(), right.clone())?,
        (OpExprKind::Shr, [left, right]) => shift_right_values(left.clone(), right.clone())?,
        (OpExprKind::Mul, [left, right]) => mul_values(left.clone(), right.clone())?,
        (OpExprKind::Div, [left, right]) => div_values(left.clone(), right.clone())?,
        (OpExprKind::Mod, [left, right]) => mod_values(left.clone(), right.clone())?,
        (OpExprKind::Concat, [left, right]) => concat_values(left.clone(), right.clone())?,
        (OpExprKind::Eq, [left, right]) => crate::backend::executor::expr_ops::compare_values(
            "=",
            left.clone(),
            right.clone(),
            collation_oid,
        )?,
        (OpExprKind::NotEq, [left, right]) => {
            not_equal_values(left.clone(), right.clone(), collation_oid)?
        }
        (OpExprKind::Lt, [left, right]) => {
            order_values("<", left.clone(), right.clone(), collation_oid)?
        }
        (OpExprKind::LtEq, [left, right]) => {
            order_values("<=", left.clone(), right.clone(), collation_oid)?
        }
        (OpExprKind::Gt, [left, right]) => {
            order_values(">", left.clone(), right.clone(), collation_oid)?
        }
        (OpExprKind::GtEq, [left, right]) => {
            order_values(">=", left.clone(), right.clone(), collation_oid)?
        }
        _ => return Ok(None),
    };
    Ok(Some(value))
}

fn try_fold_eval(result: Result<Value, ExecError>) -> Result<Option<Expr>, ParseError> {
    match result {
        Ok(value) => Ok(Some(Expr::Const(value))),
        Err(err) if should_raise_const_eval_error(&err) => Err(parse_error_from_exec(err)),
        Err(_) => Ok(None),
    }
}

fn try_fold_optional_eval(
    result: Result<Option<Value>, ExecError>,
) -> Result<Option<Expr>, ParseError> {
    match result {
        Ok(Some(value)) => Ok(Some(Expr::Const(value))),
        Ok(None) => Ok(None),
        Err(err) if should_raise_const_eval_error(&err) => Err(parse_error_from_exec(err)),
        Err(_) => Ok(None),
    }
}

fn should_raise_const_eval_error(err: &ExecError) -> bool {
    matches!(err, ExecError::DivisionByZero(_))
}

fn parse_error_from_exec(err: ExecError) -> ParseError {
    match err {
        ExecError::Parse(err) => err,
        ExecError::DetailedError {
            message,
            detail,
            hint,
            sqlstate,
        } => ParseError::DetailedError {
            message,
            detail,
            hint,
            sqlstate,
        },
        ExecError::DivisionByZero(_) => ParseError::DetailedError {
            message: "division by zero".into(),
            detail: None,
            hint: None,
            sqlstate: "22012",
        },
        other => {
            ParseError::FeatureNotSupportedMessage(format!("constant folding failed: {other:?}"))
        }
    }
}

fn const_expr_value(expr: &Expr) -> Option<&Value> {
    match expr {
        Expr::Const(value) => Some(value),
        _ => None,
    }
}

fn const_expr_values(exprs: &[Expr]) -> Option<Vec<Value>> {
    exprs
        .iter()
        .map(|expr| const_expr_value(expr).cloned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::include::nodes::primnodes::{ScalarArrayOpExpr, Var};

    fn int_var() -> Expr {
        Expr::Var(Var {
            varno: 1,
            varattno: 1,
            varlevelsup: 0,
            vartype: SqlType::new(SqlTypeKind::Int4),
        })
    }

    #[test]
    fn where_scalar_array_null_preserves_plan_qual() {
        let expr = Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            op: crate::backend::parser::SubqueryComparisonOp::Eq,
            use_or: true,
            left: Box::new(int_var()),
            right: Box::new(Expr::Cast(
                Box::new(Expr::Const(Value::Null)),
                SqlType::array_of(SqlType::new(SqlTypeKind::Int4)),
            )),
            collation_oid: None,
        }));

        assert!(matches!(
            simplify_where_qual(expr).unwrap(),
            Expr::ScalarArrayOp(_)
        ));
    }

    #[test]
    fn where_scalar_array_null_keeps_non_binary_left_cast() {
        let expr = Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            op: crate::backend::parser::SubqueryComparisonOp::Eq,
            use_or: true,
            left: Box::new(Expr::Cast(
                Box::new(Expr::Var(Var {
                    varno: 1,
                    varattno: 1,
                    varlevelsup: 0,
                    vartype: SqlType::new(SqlTypeKind::Timestamp),
                })),
                SqlType::new(SqlTypeKind::TimestampTz),
            )),
            right: Box::new(Expr::Cast(
                Box::new(Expr::Const(Value::Null)),
                SqlType::array_of(SqlType::new(SqlTypeKind::TimestampTz)),
            )),
            collation_oid: None,
        }));

        assert!(matches!(
            simplify_where_qual(expr).unwrap(),
            Expr::ScalarArrayOp(_)
        ));
    }

    #[test]
    fn stable_to_char_is_not_constant_folded() {
        let expr = Expr::builtin_func(
            BuiltinScalarFunction::ToChar,
            Some(SqlType::new(SqlTypeKind::Text)),
            false,
            vec![
                Expr::Const(Value::Int32(125)),
                Expr::Const(Value::Text("999".into())),
            ],
        );

        assert!(matches!(
            simplify_expr(expr, None).unwrap(),
            Expr::Func(func)
                if matches!(
                    func.implementation,
                    ScalarFunctionImpl::Builtin(BuiltinScalarFunction::ToChar)
                )
        ));
    }
}
