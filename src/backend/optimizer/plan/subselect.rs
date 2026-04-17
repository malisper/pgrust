use crate::backend::parser::CatalogLookup;
use crate::include::nodes::plannodes::{Plan, PlannedStmt};
use crate::include::nodes::primnodes::{
    AggAccum, Expr, ExprArraySubscript, ProjectSetTarget, SetReturningCall, SubLink, SubPlan,
};

use super::planner::planner;

pub(super) fn append_planned_subquery(
    planned_stmt: PlannedStmt,
    subplans: &mut Vec<Plan>,
) -> usize {
    let base = subplans.len();
    subplans.extend(
        planned_stmt
            .subplans
            .into_iter()
            .map(|plan| rebase_plan_subplan_ids(plan, base)),
    );
    let plan_id = subplans.len();
    subplans.push(rebase_plan_subplan_ids(planned_stmt.plan_tree, base));
    plan_id
}

fn lower_sublink_to_subplan(
    sublink: SubLink,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
) -> Expr {
    let testexpr = sublink
        .testexpr
        .map(|expr| Box::new(finalize_expr_subqueries(*expr, catalog, subplans)));
    let first_col_type = sublink
        .subselect
        .target_list
        .first()
        .map(|target| target.sql_type);
    let plan_id = append_planned_subquery(planner(*sublink.subselect, catalog), subplans);
    Expr::SubPlan(Box::new(SubPlan {
        sublink_type: sublink.sublink_type,
        testexpr,
        first_col_type,
        plan_id,
        par_param: Vec::new(),
        args: Vec::new(),
    }))
}

pub(super) fn finalize_expr_subqueries(
    expr: Expr,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
) -> Expr {
    match expr {
        other @ (Expr::Var(_)
        | Expr::Param(_)
        | Expr::Const(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. }) => other,
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(crate::include::nodes::primnodes::Aggref {
            args: aggref
                .args
                .into_iter()
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                .collect(),
            aggfilter: aggref
                .aggfilter
                .map(|expr| finalize_expr_subqueries(expr, catalog, subplans)),
            ..*aggref
        })),
        Expr::WindowFunc(window_func) => Expr::WindowFunc(Box::new(
            crate::include::nodes::primnodes::WindowFuncExpr {
                kind: match window_func.kind {
                    crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) => {
                        crate::include::nodes::primnodes::WindowFuncKind::Aggregate(
                            match finalize_expr_subqueries(
                                Expr::Aggref(Box::new(aggref)),
                                catalog,
                                subplans,
                            ) {
                                Expr::Aggref(aggref) => *aggref,
                                other => unreachable!(
                                    "window aggregate rewrite returned non-Aggref: {other:?}"
                                ),
                            },
                        )
                    }
                    crate::include::nodes::primnodes::WindowFuncKind::Builtin(kind) => {
                        crate::include::nodes::primnodes::WindowFuncKind::Builtin(kind)
                    }
                },
                args: window_func
                    .args
                    .into_iter()
                    .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                    .collect(),
                ..*window_func
            },
        )),
        Expr::Op(op) => Expr::Op(Box::new(crate::include::nodes::primnodes::OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(crate::include::nodes::primnodes::BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                .collect(),
            ..*bool_expr
        })),
        Expr::Case(case_expr) => Expr::Case(Box::new(crate::include::nodes::primnodes::CaseExpr {
            arg: case_expr
                .arg
                .map(|expr| Box::new(finalize_expr_subqueries(*expr, catalog, subplans))),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| crate::include::nodes::primnodes::CaseWhen {
                    expr: finalize_expr_subqueries(arm.expr, catalog, subplans),
                    result: finalize_expr_subqueries(arm.result, catalog, subplans),
                })
                .collect(),
            defresult: Box::new(finalize_expr_subqueries(
                *case_expr.defresult,
                catalog,
                subplans,
            )),
            ..*case_expr
        })),
        Expr::CaseTest(case_test) => Expr::CaseTest(case_test),
        Expr::Func(func) => Expr::Func(Box::new(crate::include::nodes::primnodes::FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                .collect(),
            ..*func
        })),
        Expr::SubLink(sublink) => lower_sublink_to_subplan(*sublink, catalog, subplans),
        Expr::SubPlan(subplan) => Expr::SubPlan(Box::new(SubPlan {
            testexpr: subplan
                .testexpr
                .map(|expr| Box::new(finalize_expr_subqueries(*expr, catalog, subplans))),
            args: subplan
                .args
                .into_iter()
                .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                .collect(),
            ..*subplan
        })),
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(
            crate::include::nodes::primnodes::ScalarArrayOpExpr {
                left: Box::new(finalize_expr_subqueries(*saop.left, catalog, subplans)),
                right: Box::new(finalize_expr_subqueries(*saop.right, catalog, subplans)),
                ..*saop
            },
        )),
        Expr::Cast(inner, ty) => Expr::Cast(
            Box::new(finalize_expr_subqueries(*inner, catalog, subplans)),
            ty,
        ),
        Expr::IsNull(inner) => Expr::IsNull(Box::new(finalize_expr_subqueries(
            *inner, catalog, subplans,
        ))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(finalize_expr_subqueries(
            *inner, catalog, subplans,
        ))),
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(finalize_expr_subqueries(*left, catalog, subplans)),
            Box::new(finalize_expr_subqueries(*right, catalog, subplans)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(finalize_expr_subqueries(*left, catalog, subplans)),
            Box::new(finalize_expr_subqueries(*right, catalog, subplans)),
        ),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(finalize_expr_subqueries(*expr, catalog, subplans)),
            pattern: Box::new(finalize_expr_subqueries(*pattern, catalog, subplans)),
            escape: escape.map(|expr| Box::new(finalize_expr_subqueries(*expr, catalog, subplans))),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(finalize_expr_subqueries(*expr, catalog, subplans)),
            pattern: Box::new(finalize_expr_subqueries(*pattern, catalog, subplans)),
            escape: escape.map(|expr| Box::new(finalize_expr_subqueries(*expr, catalog, subplans))),
            negated,
        },
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| finalize_expr_subqueries(element, catalog, subplans))
                .collect(),
            array_type,
        },
        Expr::Row { fields } => Expr::Row {
            fields: fields
                .into_iter()
                .map(|(name, expr)| (name, finalize_expr_subqueries(expr, catalog, subplans)))
                .collect(),
        },
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(finalize_expr_subqueries(*left, catalog, subplans)),
            Box::new(finalize_expr_subqueries(*right, catalog, subplans)),
        ),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(finalize_expr_subqueries(*array, catalog, subplans)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript
                        .lower
                        .map(|expr| finalize_expr_subqueries(expr, catalog, subplans)),
                    upper: subscript
                        .upper
                        .map(|expr| finalize_expr_subqueries(expr, catalog, subplans)),
                })
                .collect(),
        },
    }
}

fn finalize_set_returning_call(
    call: SetReturningCall,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
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
            start: finalize_expr_subqueries(start, catalog, subplans),
            stop: finalize_expr_subqueries(stop, catalog, subplans),
            step: finalize_expr_subqueries(step, catalog, subplans),
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
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
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
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
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
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
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
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                .collect(),
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
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                .collect(),
            output_columns,
        },
    }
}

fn finalize_agg_accum(
    accum: AggAccum,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
) -> AggAccum {
    let AggAccum {
        aggfnoid,
        agg_variadic,
        args,
        filter,
        distinct,
        sql_type,
    } = accum;
    AggAccum {
        aggfnoid,
        agg_variadic,
        args: args
            .into_iter()
            .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
            .collect(),
        filter: filter.map(|expr| finalize_expr_subqueries(expr, catalog, subplans)),
        distinct,
        sql_type,
    }
}

fn rebase_expr_subplan_ids(expr: Expr, base: usize) -> Expr {
    match expr {
        other @ (Expr::Var(_)
        | Expr::Param(_)
        | Expr::Const(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. }) => other,
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(crate::include::nodes::primnodes::Aggref {
            args: aggref
                .args
                .into_iter()
                .map(|arg| rebase_expr_subplan_ids(arg, base))
                .collect(),
            aggfilter: aggref
                .aggfilter
                .map(|expr| rebase_expr_subplan_ids(expr, base)),
            ..*aggref
        })),
        Expr::WindowFunc(window_func) => Expr::WindowFunc(Box::new(
            crate::include::nodes::primnodes::WindowFuncExpr {
                kind: match window_func.kind {
                    crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) => {
                        crate::include::nodes::primnodes::WindowFuncKind::Aggregate(
                            match rebase_expr_subplan_ids(Expr::Aggref(Box::new(aggref)), base) {
                                Expr::Aggref(aggref) => *aggref,
                                other => unreachable!(
                                    "window aggregate rebase returned non-Aggref: {other:?}"
                                ),
                            },
                        )
                    }
                    crate::include::nodes::primnodes::WindowFuncKind::Builtin(kind) => {
                        crate::include::nodes::primnodes::WindowFuncKind::Builtin(kind)
                    }
                },
                args: window_func
                    .args
                    .into_iter()
                    .map(|arg| rebase_expr_subplan_ids(arg, base))
                    .collect(),
                ..*window_func
            },
        )),
        Expr::Op(op) => Expr::Op(Box::new(crate::include::nodes::primnodes::OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| rebase_expr_subplan_ids(arg, base))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(crate::include::nodes::primnodes::BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| rebase_expr_subplan_ids(arg, base))
                .collect(),
            ..*bool_expr
        })),
        Expr::Case(case_expr) => Expr::Case(Box::new(crate::include::nodes::primnodes::CaseExpr {
            arg: case_expr
                .arg
                .map(|expr| Box::new(rebase_expr_subplan_ids(*expr, base))),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| crate::include::nodes::primnodes::CaseWhen {
                    expr: rebase_expr_subplan_ids(arm.expr, base),
                    result: rebase_expr_subplan_ids(arm.result, base),
                })
                .collect(),
            defresult: Box::new(rebase_expr_subplan_ids(*case_expr.defresult, base)),
            ..*case_expr
        })),
        Expr::CaseTest(case_test) => Expr::CaseTest(case_test),
        Expr::Func(func) => Expr::Func(Box::new(crate::include::nodes::primnodes::FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| rebase_expr_subplan_ids(arg, base))
                .collect(),
            ..*func
        })),
        Expr::SubLink(sublink) => Expr::SubLink(Box::new(SubLink {
            testexpr: sublink
                .testexpr
                .map(|expr| Box::new(rebase_expr_subplan_ids(*expr, base))),
            ..*sublink
        })),
        Expr::SubPlan(subplan) => Expr::SubPlan(Box::new(SubPlan {
            testexpr: subplan
                .testexpr
                .map(|expr| Box::new(rebase_expr_subplan_ids(*expr, base))),
            args: subplan
                .args
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
            first_col_type: subplan.first_col_type,
            plan_id: subplan.plan_id + base,
            sublink_type: subplan.sublink_type,
            par_param: subplan.par_param,
        })),
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(
            crate::include::nodes::primnodes::ScalarArrayOpExpr {
                left: Box::new(rebase_expr_subplan_ids(*saop.left, base)),
                right: Box::new(rebase_expr_subplan_ids(*saop.right, base)),
                ..*saop
            },
        )),
        Expr::Cast(inner, ty) => Expr::Cast(Box::new(rebase_expr_subplan_ids(*inner, base)), ty),
        Expr::IsNull(inner) => Expr::IsNull(Box::new(rebase_expr_subplan_ids(*inner, base))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(rebase_expr_subplan_ids(*inner, base))),
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(rebase_expr_subplan_ids(*left, base)),
            Box::new(rebase_expr_subplan_ids(*right, base)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(rebase_expr_subplan_ids(*left, base)),
            Box::new(rebase_expr_subplan_ids(*right, base)),
        ),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(rebase_expr_subplan_ids(*expr, base)),
            pattern: Box::new(rebase_expr_subplan_ids(*pattern, base)),
            escape: escape.map(|expr| Box::new(rebase_expr_subplan_ids(*expr, base))),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(rebase_expr_subplan_ids(*expr, base)),
            pattern: Box::new(rebase_expr_subplan_ids(*pattern, base)),
            escape: escape.map(|expr| Box::new(rebase_expr_subplan_ids(*expr, base))),
            negated,
        },
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
            array_type,
        },
        Expr::Row { fields } => Expr::Row {
            fields: fields
                .into_iter()
                .map(|(name, expr)| (name, rebase_expr_subplan_ids(expr, base)))
                .collect(),
        },
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(rebase_expr_subplan_ids(*left, base)),
            Box::new(rebase_expr_subplan_ids(*right, base)),
        ),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(rebase_expr_subplan_ids(*array, base)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript
                        .lower
                        .map(|expr| rebase_expr_subplan_ids(expr, base)),
                    upper: subscript
                        .upper
                        .map(|expr| rebase_expr_subplan_ids(expr, base)),
                })
                .collect(),
        },
    }
}

fn rebase_set_returning_call_subplan_ids(call: SetReturningCall, base: usize) -> SetReturningCall {
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
            start: rebase_expr_subplan_ids(start, base),
            stop: rebase_expr_subplan_ids(stop, base),
            step: rebase_expr_subplan_ids(step, base),
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
                .map(|arg| rebase_expr_subplan_ids(arg, base))
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
                .map(|arg| rebase_expr_subplan_ids(arg, base))
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
                .map(|arg| rebase_expr_subplan_ids(arg, base))
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
                .map(|arg| rebase_expr_subplan_ids(arg, base))
                .collect(),
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
                .map(|arg| rebase_expr_subplan_ids(arg, base))
                .collect(),
            output_columns,
        },
    }
}

fn rebase_agg_accum_subplan_ids(accum: AggAccum, base: usize) -> AggAccum {
    AggAccum {
        args: accum
            .args
            .into_iter()
            .map(|arg| rebase_expr_subplan_ids(arg, base))
            .collect(),
        filter: accum.filter.map(|expr| rebase_expr_subplan_ids(expr, base)),
        ..accum
    }
}

fn rebase_window_clause_subplan_ids(
    clause: crate::include::nodes::primnodes::WindowClause,
    base: usize,
) -> crate::include::nodes::primnodes::WindowClause {
    crate::include::nodes::primnodes::WindowClause {
        spec: crate::include::nodes::primnodes::WindowSpec {
            partition_by: clause
                .spec
                .partition_by
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
            order_by: clause
                .spec
                .order_by
                .into_iter()
                .map(|item| crate::include::nodes::primnodes::OrderByEntry {
                    expr: rebase_expr_subplan_ids(item.expr, base),
                    ..item
                })
                .collect(),
        },
        functions: clause
            .functions
            .into_iter()
            .map(|func| crate::include::nodes::primnodes::WindowFuncExpr {
                kind: match func.kind {
                    crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) => {
                        crate::include::nodes::primnodes::WindowFuncKind::Aggregate(
                            match rebase_expr_subplan_ids(Expr::Aggref(Box::new(aggref)), base) {
                                Expr::Aggref(aggref) => *aggref,
                                other => unreachable!(
                                    "window aggregate rebase returned non-Aggref: {other:?}"
                                ),
                            },
                        )
                    }
                    crate::include::nodes::primnodes::WindowFuncKind::Builtin(kind) => {
                        crate::include::nodes::primnodes::WindowFuncKind::Builtin(kind)
                    }
                },
                args: func
                    .args
                    .into_iter()
                    .map(|arg| rebase_expr_subplan_ids(arg, base))
                    .collect(),
                ..func
            })
            .collect(),
    }
}

fn rebase_plan_subplan_ids(plan: Plan, base: usize) -> Plan {
    match plan {
        Plan::Result { .. } | Plan::SeqScan { .. } | Plan::IndexScan { .. } => plan,
        Plan::Append {
            plan_info,
            source_id,
            desc,
            children,
        } => Plan::Append {
            plan_info,
            source_id,
            desc,
            children: children
                .into_iter()
                .map(|child| rebase_plan_subplan_ids(child, base))
                .collect(),
        },
        Plan::SetOp {
            plan_info,
            op,
            output_columns,
            children,
        } => Plan::SetOp {
            plan_info,
            op,
            output_columns,
            children: children
                .into_iter()
                .map(|child| rebase_plan_subplan_ids(child, base))
                .collect(),
        },
        Plan::Hash {
            plan_info,
            input,
            hash_keys,
        } => Plan::Hash {
            plan_info,
            input: Box::new(rebase_plan_subplan_ids(*input, base)),
            hash_keys: hash_keys
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
        },
        Plan::NestedLoopJoin {
            plan_info,
            left,
            right,
            kind,
            nest_params,
            join_qual,
            qual,
        } => Plan::NestedLoopJoin {
            plan_info,
            left: Box::new(rebase_plan_subplan_ids(*left, base)),
            right: Box::new(rebase_plan_subplan_ids(*right, base)),
            kind,
            nest_params: nest_params
                .into_iter()
                .map(|param| crate::include::nodes::plannodes::ExecParamSource {
                    paramid: param.paramid,
                    expr: rebase_expr_subplan_ids(param.expr, base),
                })
                .collect(),
            join_qual: join_qual
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
            qual: qual
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
        },
        Plan::HashJoin {
            plan_info,
            left,
            right,
            kind,
            hash_clauses,
            hash_keys,
            join_qual,
            qual,
        } => Plan::HashJoin {
            plan_info,
            left: Box::new(rebase_plan_subplan_ids(*left, base)),
            right: Box::new(rebase_plan_subplan_ids(*right, base)),
            kind,
            hash_clauses: hash_clauses
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
            hash_keys: hash_keys
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
            join_qual: join_qual
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
            qual: qual
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
        },
        Plan::Filter {
            plan_info,
            input,
            predicate,
        } => Plan::Filter {
            plan_info,
            input: Box::new(rebase_plan_subplan_ids(*input, base)),
            predicate: rebase_expr_subplan_ids(predicate, base),
        },
        Plan::OrderBy {
            plan_info,
            input,
            items,
        } => Plan::OrderBy {
            plan_info,
            input: Box::new(rebase_plan_subplan_ids(*input, base)),
            items: items
                .into_iter()
                .map(|item| crate::include::nodes::primnodes::OrderByEntry {
                    expr: rebase_expr_subplan_ids(item.expr, base),
                    ressortgroupref: item.ressortgroupref,
                    descending: item.descending,
                    nulls_first: item.nulls_first,
                })
                .collect(),
        },
        Plan::Limit {
            plan_info,
            input,
            limit,
            offset,
        } => Plan::Limit {
            plan_info,
            input: Box::new(rebase_plan_subplan_ids(*input, base)),
            limit,
            offset,
        },
        Plan::Projection {
            plan_info,
            input,
            targets,
        } => Plan::Projection {
            plan_info,
            input: Box::new(rebase_plan_subplan_ids(*input, base)),
            targets: targets
                .into_iter()
                .map(|target| crate::include::nodes::primnodes::TargetEntry {
                    expr: rebase_expr_subplan_ids(target.expr, base),
                    ..target
                })
                .collect(),
        },
        Plan::Aggregate {
            plan_info,
            input,
            group_by,
            accumulators,
            having,
            output_columns,
        } => Plan::Aggregate {
            plan_info,
            input: Box::new(rebase_plan_subplan_ids(*input, base)),
            group_by: group_by
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
            accumulators: accumulators
                .into_iter()
                .map(|accum| rebase_agg_accum_subplan_ids(accum, base))
                .collect(),
            having: having.map(|expr| rebase_expr_subplan_ids(expr, base)),
            output_columns,
        },
        Plan::WindowAgg {
            plan_info,
            input,
            clause,
            output_columns,
        } => Plan::WindowAgg {
            plan_info,
            input: Box::new(rebase_plan_subplan_ids(*input, base)),
            clause: rebase_window_clause_subplan_ids(clause, base),
            output_columns,
        },
        Plan::FunctionScan { plan_info, call } => Plan::FunctionScan {
            plan_info,
            call: rebase_set_returning_call_subplan_ids(call, base),
        },
        Plan::SubqueryScan {
            plan_info,
            input,
            output_columns,
        } => Plan::SubqueryScan {
            plan_info,
            input: Box::new(rebase_plan_subplan_ids(*input, base)),
            output_columns,
        },
        Plan::CteScan {
            plan_info,
            cte_id,
            cte_plan,
            output_columns,
        } => Plan::CteScan {
            plan_info,
            cte_id,
            cte_plan: Box::new(rebase_plan_subplan_ids(*cte_plan, base)),
            output_columns,
        },
        Plan::WorkTableScan {
            plan_info,
            worktable_id,
            output_columns,
        } => Plan::WorkTableScan {
            plan_info,
            worktable_id,
            output_columns,
        },
        Plan::RecursiveUnion {
            plan_info,
            worktable_id,
            distinct,
            output_columns,
            anchor,
            recursive,
        } => Plan::RecursiveUnion {
            plan_info,
            worktable_id,
            distinct,
            output_columns,
            anchor: Box::new(rebase_plan_subplan_ids(*anchor, base)),
            recursive: Box::new(rebase_plan_subplan_ids(*recursive, base)),
        },
        Plan::Values {
            plan_info,
            rows,
            output_columns,
        } => Plan::Values {
            plan_info,
            rows: rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|expr| rebase_expr_subplan_ids(expr, base))
                        .collect()
                })
                .collect(),
            output_columns,
        },
        Plan::ProjectSet {
            plan_info,
            input,
            targets,
        } => Plan::ProjectSet {
            plan_info,
            input: Box::new(rebase_plan_subplan_ids(*input, base)),
            targets: targets
                .into_iter()
                .map(|target| match target {
                    ProjectSetTarget::Scalar(entry) => {
                        ProjectSetTarget::Scalar(crate::include::nodes::primnodes::TargetEntry {
                            expr: rebase_expr_subplan_ids(entry.expr, base),
                            ..entry
                        })
                    }
                    ProjectSetTarget::Set {
                        name,
                        call,
                        sql_type,
                        column_index,
                    } => ProjectSetTarget::Set {
                        name,
                        call: rebase_set_returning_call_subplan_ids(call, base),
                        sql_type,
                        column_index,
                    },
                })
                .collect(),
        },
    }
}

pub(super) fn finalize_plan_subqueries(
    plan: Plan,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
) -> Plan {
    match plan {
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::IndexScan { .. }
        | Plan::WorkTableScan { .. } => plan,
        Plan::Append {
            plan_info,
            source_id,
            desc,
            children,
        } => Plan::Append {
            plan_info,
            source_id,
            desc,
            children: children
                .into_iter()
                .map(|child| finalize_plan_subqueries(child, catalog, subplans))
                .collect(),
        },
        Plan::SetOp {
            plan_info,
            op,
            output_columns,
            children,
        } => Plan::SetOp {
            plan_info,
            op,
            output_columns,
            children: children
                .into_iter()
                .map(|child| finalize_plan_subqueries(child, catalog, subplans))
                .collect(),
        },
        Plan::Hash {
            plan_info,
            input,
            hash_keys,
        } => Plan::Hash {
            plan_info,
            input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
            hash_keys: hash_keys
                .into_iter()
                .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                .collect(),
        },
        Plan::NestedLoopJoin {
            plan_info,
            left,
            right,
            kind,
            nest_params,
            join_qual,
            qual,
        } => Plan::NestedLoopJoin {
            plan_info,
            left: Box::new(finalize_plan_subqueries(*left, catalog, subplans)),
            right: Box::new(finalize_plan_subqueries(*right, catalog, subplans)),
            kind,
            nest_params: nest_params
                .into_iter()
                .map(|param| crate::include::nodes::plannodes::ExecParamSource {
                    paramid: param.paramid,
                    expr: finalize_expr_subqueries(param.expr, catalog, subplans),
                })
                .collect(),
            join_qual: join_qual
                .into_iter()
                .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                .collect(),
            qual: qual
                .into_iter()
                .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                .collect(),
        },
        Plan::HashJoin {
            plan_info,
            left,
            right,
            kind,
            hash_clauses,
            hash_keys,
            join_qual,
            qual,
        } => Plan::HashJoin {
            plan_info,
            left: Box::new(finalize_plan_subqueries(*left, catalog, subplans)),
            right: Box::new(finalize_plan_subqueries(*right, catalog, subplans)),
            kind,
            hash_clauses: hash_clauses
                .into_iter()
                .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                .collect(),
            hash_keys: hash_keys
                .into_iter()
                .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                .collect(),
            join_qual: join_qual
                .into_iter()
                .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                .collect(),
            qual: qual
                .into_iter()
                .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                .collect(),
        },
        Plan::Filter {
            plan_info,
            input,
            predicate,
        } => Plan::Filter {
            plan_info,
            input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
            predicate: finalize_expr_subqueries(predicate, catalog, subplans),
        },
        Plan::OrderBy {
            plan_info,
            input,
            items,
        } => Plan::OrderBy {
            plan_info,
            input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
            items: items
                .into_iter()
                .map(|item| crate::include::nodes::primnodes::OrderByEntry {
                    expr: finalize_expr_subqueries(item.expr, catalog, subplans),
                    ressortgroupref: item.ressortgroupref,
                    descending: item.descending,
                    nulls_first: item.nulls_first,
                })
                .collect(),
        },
        Plan::Limit {
            plan_info,
            input,
            limit,
            offset,
        } => Plan::Limit {
            plan_info,
            input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
            limit,
            offset,
        },
        Plan::Projection {
            plan_info,
            input,
            targets,
        } => Plan::Projection {
            plan_info,
            input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
            targets: targets
                .into_iter()
                .map(|target| crate::include::nodes::primnodes::TargetEntry {
                    name: target.name,
                    expr: finalize_expr_subqueries(target.expr, catalog, subplans),
                    sql_type: target.sql_type,
                    resno: target.resno,
                    ressortgroupref: target.ressortgroupref,
                    input_resno: target.input_resno,
                    resjunk: target.resjunk,
                })
                .collect(),
        },
        Plan::Aggregate {
            plan_info,
            input,
            group_by,
            accumulators,
            having,
            output_columns,
        } => Plan::Aggregate {
            plan_info,
            input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
            group_by: group_by
                .into_iter()
                .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                .collect(),
            accumulators: accumulators
                .into_iter()
                .map(|accum| finalize_agg_accum(accum, catalog, subplans))
                .collect(),
            having: having.map(|expr| finalize_expr_subqueries(expr, catalog, subplans)),
            output_columns,
        },
        Plan::WindowAgg {
            plan_info,
            input,
            clause,
            output_columns,
        } => Plan::WindowAgg {
            plan_info,
            input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
            clause: crate::include::nodes::primnodes::WindowClause {
                spec: crate::include::nodes::primnodes::WindowSpec {
                    partition_by: clause
                        .spec
                        .partition_by
                        .into_iter()
                        .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                        .collect(),
                    order_by: clause
                        .spec
                        .order_by
                        .into_iter()
                        .map(|item| crate::include::nodes::primnodes::OrderByEntry {
                            expr: finalize_expr_subqueries(item.expr, catalog, subplans),
                            ..item
                        })
                        .collect(),
                },
                functions: clause
                    .functions
                    .into_iter()
                    .map(|func| crate::include::nodes::primnodes::WindowFuncExpr {
                        kind: match func.kind {
                            crate::include::nodes::primnodes::WindowFuncKind::Aggregate(
                                aggref,
                            ) => crate::include::nodes::primnodes::WindowFuncKind::Aggregate(
                                match finalize_expr_subqueries(
                                    Expr::Aggref(Box::new(aggref)),
                                    catalog,
                                    subplans,
                                ) {
                                    Expr::Aggref(aggref) => *aggref,
                                    other => unreachable!(
                                        "window aggregate rewrite returned non-Aggref: {other:?}"
                                    ),
                                },
                            ),
                            crate::include::nodes::primnodes::WindowFuncKind::Builtin(kind) => {
                                crate::include::nodes::primnodes::WindowFuncKind::Builtin(kind)
                            }
                        },
                        args: func
                            .args
                            .into_iter()
                            .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                            .collect(),
                        ..func
                    })
                    .collect(),
            },
            output_columns,
        },
        Plan::FunctionScan { plan_info, call } => Plan::FunctionScan {
            plan_info,
            call: finalize_set_returning_call(call, catalog, subplans),
        },
        Plan::SubqueryScan {
            plan_info,
            input,
            output_columns,
        } => Plan::SubqueryScan {
            plan_info,
            input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
            output_columns,
        },
        Plan::CteScan {
            plan_info,
            cte_id,
            cte_plan,
            output_columns,
        } => Plan::CteScan {
            plan_info,
            cte_id,
            cte_plan: Box::new(finalize_plan_subqueries(*cte_plan, catalog, subplans)),
            output_columns,
        },
        Plan::RecursiveUnion {
            plan_info,
            worktable_id,
            distinct,
            output_columns,
            anchor,
            recursive,
        } => Plan::RecursiveUnion {
            plan_info,
            worktable_id,
            distinct,
            output_columns,
            anchor: Box::new(finalize_plan_subqueries(*anchor, catalog, subplans)),
            recursive: Box::new(finalize_plan_subqueries(*recursive, catalog, subplans)),
        },
        Plan::Values {
            plan_info,
            rows,
            output_columns,
        } => Plan::Values {
            plan_info,
            rows: rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                        .collect()
                })
                .collect(),
            output_columns,
        },
        Plan::ProjectSet {
            plan_info,
            input,
            targets,
        } => Plan::ProjectSet {
            plan_info,
            input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
            targets: targets
                .into_iter()
                .map(|target| match target {
                    ProjectSetTarget::Scalar(entry) => {
                        ProjectSetTarget::Scalar(crate::include::nodes::primnodes::TargetEntry {
                            name: entry.name,
                            expr: finalize_expr_subqueries(entry.expr, catalog, subplans),
                            sql_type: entry.sql_type,
                            resno: entry.resno,
                            ressortgroupref: entry.ressortgroupref,
                            input_resno: entry.input_resno,
                            resjunk: entry.resjunk,
                        })
                    }
                    ProjectSetTarget::Set {
                        name,
                        call,
                        sql_type,
                        column_index,
                    } => ProjectSetTarget::Set {
                        name,
                        call: finalize_set_returning_call(call, catalog, subplans),
                        sql_type,
                        column_index,
                    },
                })
                .collect(),
        },
    }
}
