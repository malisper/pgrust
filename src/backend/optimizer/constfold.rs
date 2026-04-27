use crate::backend::executor::expr_geometry::eval_geometry_function;
use crate::backend::executor::expr_numeric::eval_power_function;
use crate::backend::executor::expr_ops::{
    add_values, bitwise_and_values, bitwise_not_value, bitwise_or_values, bitwise_xor_values,
    concat_values, div_values, mod_values, mul_values, negate_value, not_equal_values,
    order_values, shift_left_values, shift_right_values, sub_values, values_are_distinct,
};
use crate::backend::executor::{ExecError, Value, cast_value, eval_to_char_function};
use crate::backend::parser::ParseError;
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::include::catalog::builtin_range_spec_by_oid;
use crate::include::catalog::pg_proc::builtin_aggregate_function_for_proc_oid;
use crate::include::nodes::parsenodes::{
    JoinTreeNode, Query, RangeTblEntry, RangeTblEntryKind, RecursiveUnionQuery, SetOperationQuery,
    SqlType, SqlTypeKind,
};
use crate::include::nodes::primnodes::{
    AggAccum, AggFunc, Aggref, BoolExpr, BoolExprType, BuiltinScalarFunction, CaseExpr, CaseWhen,
    Expr, ExprArraySubscript, FuncExpr, OpExpr, OpExprKind, OrderByEntry, ScalarFunctionImpl,
    SetReturningCall, SortGroupClause, TargetEntry, WindowClause, WindowFrame, WindowFrameBound,
    WindowFuncExpr, WindowFuncKind, XmlExpr,
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
        where_qual: query
            .where_qual
            .map(|expr| simplify_expr(expr, None))
            .transpose()?,
        group_by: query
            .group_by
            .into_iter()
            .map(|expr| simplify_expr(expr, None))
            .collect::<Result<Vec<_>, _>>()?,
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
        limit_count: query.limit_count,
        limit_offset: query.limit_offset,
        locking_clause: query.locking_clause,
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
            output_columns,
            with_ordinality,
        } => SetReturningCall::UserDefined {
            proc_oid,
            function_name,
            func_variadic,
            args: simplify_exprs(args)?,
            output_columns,
            with_ordinality,
        },
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
    if stats_import_builtin_preserves_arg_types(func.implementation) {
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

fn stats_import_builtin_preserves_arg_types(implementation: ScalarFunctionImpl) -> bool {
    matches!(
        implementation,
        ScalarFunctionImpl::Builtin(
            BuiltinScalarFunction::PgRestoreRelationStats
                | BuiltinScalarFunction::PgClearRelationStats
                | BuiltinScalarFunction::PgRestoreAttributeStats
                | BuiltinScalarFunction::PgClearAttributeStats
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
        Expr::WindowFunc(window_func) => Ok(Expr::WindowFunc(Box::new(simplify_window_func_expr(
            *window_func,
        )?))),
        Expr::Op(op) => simplify_op_expr(*op, case_test_value),
        Expr::Bool(bool_expr) => simplify_bool_expr(*bool_expr, case_test_value),
        Expr::Case(case_expr) => simplify_case_expr(*case_expr, case_test_value),
        Expr::Func(func) => simplify_func_expr(*func, case_test_value),
        Expr::SetReturning(srf) => Ok(Expr::SetReturning(Box::new(
            crate::include::nodes::primnodes::SetReturningExpr {
                call: simplify_set_returning_call(srf.call)?,
                ..*srf
            },
        ))),
        Expr::SubLink(sublink) => Ok(Expr::SubLink(Box::new(
            crate::include::nodes::primnodes::SubLink {
                testexpr: sublink
                    .testexpr
                    .map(|expr| simplify_expr(*expr, case_test_value).map(Box::new))
                    .transpose()?,
                subselect: Box::new(simplify_query(*sublink.subselect)?),
                ..*sublink
            },
        ))),
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
                right: Box::new(simplify_expr(*saop.right, case_test_value)?),
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
            if matches!(expr, Expr::Const(_)) {
                Ok(expr)
            } else {
                Ok(Expr::Collate {
                    expr: Box::new(expr),
                    collation_oid,
                })
            }
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
                Expr::Const(Value::Null) => Expr::Const(Value::Bool(true)),
                Expr::Const(_) => Expr::Const(Value::Bool(false)),
                other => Expr::IsNull(Box::new(other)),
            })
        }
        Expr::IsNotNull(inner) => {
            let inner = simplify_expr(*inner, case_test_value)?;
            Ok(match inner {
                Expr::Const(Value::Null) => Expr::Const(Value::Bool(false)),
                Expr::Const(_) => Expr::Const(Value::Bool(true)),
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
            descriptor,
            fields: fields
                .into_iter()
                .map(|(name, expr)| Ok((name, simplify_expr(expr, case_test_value)?)))
                .collect::<Result<Vec<_>, ParseError>>()?,
        }),
        Expr::FieldSelect {
            expr,
            field,
            field_type,
        } => Ok(Expr::FieldSelect {
            expr: Box::new(simplify_expr(*expr, case_test_value)?),
            field,
            field_type,
        }),
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
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::ToChar) => {
            eval_to_char_function(args, &DateTimeConfig::default()).map(Some)
        }
        _ => Ok(None),
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
    // :HACK: Dynamic range OIDs include domains-over-range. Domain CHECK
    // enforcement needs the executor's catalog-aware cast path, so don't fold
    // those casts with this catalog-free helper.
    if !target.is_array
        && matches!(target.kind, SqlTypeKind::Range)
        && target.type_oid != 0
        && builtin_range_spec_by_oid(target.type_oid).is_none()
    {
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
