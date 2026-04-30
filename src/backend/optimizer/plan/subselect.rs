#![allow(dead_code)]

use crate::backend::parser::CatalogLookup;
use crate::include::nodes::plannodes::{Plan, PlannedStmt};
use crate::include::nodes::primnodes::{
    AggAccum, Expr, ExprArraySubscript, ProjectSetTarget, RowsFromItem, RowsFromSource,
    SetReturningCall, SqlJsonQueryFunction, SqlJsonTableBehavior, SqlJsonTablePassingArg, SubLink,
    SubPlan,
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

pub(super) fn append_uncorrelated_planned_subquery(
    planned_stmt: PlannedStmt,
    subplans: &mut Vec<Plan>,
) -> usize {
    if planned_stmt.ext_params.is_empty()
        && !planned_stmt
            .subplans
            .iter()
            .chain(std::iter::once(&planned_stmt.plan_tree))
            .any(plan_contains_volatile_expr)
        && let Some(plan_id) = find_existing_uncorrelated_subquery_bundle(&planned_stmt, subplans)
    {
        return plan_id;
    }
    append_planned_subquery(planned_stmt, subplans)
}

fn find_existing_uncorrelated_subquery_bundle(
    planned_stmt: &PlannedStmt,
    subplans: &[Plan],
) -> Option<usize> {
    let bundle_len = planned_stmt.subplans.len() + 1;
    if bundle_len > subplans.len() {
        return None;
    }
    for base in 0..=subplans.len().saturating_sub(bundle_len) {
        let subplan_match = planned_stmt
            .subplans
            .iter()
            .enumerate()
            .all(|(offset, plan)| {
                subplans.get(base + offset) == Some(&rebase_plan_subplan_ids(plan.clone(), base))
            });
        if !subplan_match {
            continue;
        }
        let plan_id = base + planned_stmt.subplans.len();
        if subplans.get(plan_id)
            == Some(&rebase_plan_subplan_ids(
                planned_stmt.plan_tree.clone(),
                base,
            ))
        {
            return Some(plan_id);
        }
    }
    None
}

fn plan_contains_volatile_expr(plan: &Plan) -> bool {
    match plan {
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::WorkTableScan { .. } => false,
        Plan::Append { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::SetOp { children, .. } => children.iter().any(plan_contains_volatile_expr),
        Plan::Hash {
            input, hash_keys, ..
        } => plan_contains_volatile_expr(input) || hash_keys.iter().any(expr_is_volatile_for_dedup),
        Plan::Filter {
            input, predicate, ..
        } => plan_contains_volatile_expr(input) || expr_is_volatile_for_dedup(predicate),
        Plan::Projection { input, targets, .. } => {
            plan_contains_volatile_expr(input)
                || targets
                    .iter()
                    .any(|target| expr_is_volatile_for_dedup(&target.expr))
        }
        Plan::Aggregate {
            input,
            group_by,
            passthrough_exprs,
            accumulators,
            having,
            ..
        } => {
            plan_contains_volatile_expr(input)
                || group_by.iter().any(expr_is_volatile_for_dedup)
                || passthrough_exprs.iter().any(expr_is_volatile_for_dedup)
                || accumulators
                    .iter()
                    .any(agg_accum_contains_volatile_for_dedup)
                || having.as_ref().is_some_and(expr_is_volatile_for_dedup)
        }
        Plan::OrderBy { input, items, .. } | Plan::IncrementalSort { input, items, .. } => {
            plan_contains_volatile_expr(input)
                || items
                    .iter()
                    .any(|item| expr_is_volatile_for_dedup(&item.expr))
        }
        Plan::SubqueryScan { input, filter, .. } => {
            plan_contains_volatile_expr(input)
                || filter.as_ref().is_some_and(expr_is_volatile_for_dedup)
        }
        Plan::Materialize { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Gather { input, .. }
        | Plan::CteScan {
            cte_plan: input, ..
        } => plan_contains_volatile_expr(input),
        _ => true,
    }
}

fn agg_accum_contains_volatile_for_dedup(accum: &AggAccum) -> bool {
    accum.direct_args.iter().any(expr_is_volatile_for_dedup)
        || accum.args.iter().any(expr_is_volatile_for_dedup)
        || accum
            .order_by
            .iter()
            .any(|item| expr_is_volatile_for_dedup(&item.expr))
        || accum
            .filter
            .as_ref()
            .is_some_and(expr_is_volatile_for_dedup)
}

fn expr_is_volatile_for_dedup(expr: &Expr) -> bool {
    match expr {
        Expr::Const(_)
        | Expr::Var(_)
        | Expr::Param(_)
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole => false,
        Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. }
        | Expr::Func(_) => true,
        Expr::Op(op) => op.args.iter().any(expr_is_volatile_for_dedup),
        Expr::Bool(bool_expr) => bool_expr.args.iter().any(expr_is_volatile_for_dedup),
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::FieldSelect { expr: inner, .. } => expr_is_volatile_for_dedup(inner),
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_is_volatile_for_dedup(left) || expr_is_volatile_for_dedup(right)
        }
        Expr::ScalarArrayOp(saop) => {
            expr_is_volatile_for_dedup(&saop.left) || expr_is_volatile_for_dedup(&saop.right)
        }
        Expr::ArrayLiteral { elements, .. } => elements.iter().any(expr_is_volatile_for_dedup),
        Expr::Row { fields, .. } => fields
            .iter()
            .any(|(_, expr)| expr_is_volatile_for_dedup(expr)),
        _ => true,
    }
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
    let target_width = sublink.subselect.target_list.len();
    let planned_stmt = planner(*sublink.subselect, catalog)
        .expect("locking validation should complete before subplan lowering");
    let par_param = planned_stmt
        .ext_params
        .iter()
        .map(|param| param.paramid)
        .collect::<Vec<_>>();
    let args = planned_stmt
        .ext_params
        .iter()
        .map(|param| finalize_expr_subqueries(param.expr.clone(), catalog, subplans))
        .collect::<Vec<_>>();
    let plan_id = append_uncorrelated_planned_subquery(planned_stmt, subplans);
    Expr::SubPlan(Box::new(SubPlan {
        sublink_type: sublink.sublink_type,
        testexpr,
        first_col_type,
        target_width,
        plan_id,
        par_param,
        args,
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
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
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
            aggorder: aggref
                .aggorder
                .into_iter()
                .map(|item| crate::include::nodes::primnodes::OrderByEntry {
                    expr: finalize_expr_subqueries(item.expr, catalog, subplans),
                    ..item
                })
                .collect(),
            aggfilter: aggref
                .aggfilter
                .map(|expr| finalize_expr_subqueries(expr, catalog, subplans)),
            ..*aggref
        })),
        Expr::WindowFunc(window_func) => {
            Expr::WindowFunc(Box::new(crate::include::nodes::primnodes::WindowFuncExpr {
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
            }))
        }
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
        Expr::SqlJsonQueryFunction(func) => {
            Expr::SqlJsonQueryFunction(Box::new(SqlJsonQueryFunction {
                context: finalize_expr_subqueries(func.context, catalog, subplans),
                path: finalize_expr_subqueries(func.path, catalog, subplans),
                passing: func
                    .passing
                    .into_iter()
                    .map(|arg| SqlJsonTablePassingArg {
                        name: arg.name,
                        expr: finalize_expr_subqueries(arg.expr, catalog, subplans),
                    })
                    .collect(),
                on_empty: finalize_sql_json_behavior(func.on_empty, catalog, subplans),
                on_error: finalize_sql_json_behavior(func.on_error, catalog, subplans),
                ..*func
            }))
        }
        Expr::SetReturning(srf) => Expr::SetReturning(Box::new(
            crate::include::nodes::primnodes::SetReturningExpr {
                call: finalize_set_returning_call(srf.call, catalog, subplans),
                ..*srf
            },
        )),
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
        Expr::Collate {
            expr,
            collation_oid,
        } => Expr::Collate {
            expr: Box::new(finalize_expr_subqueries(*expr, catalog, subplans)),
            collation_oid,
        },
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
            collation_oid,
        } => Expr::Like {
            expr: Box::new(finalize_expr_subqueries(*expr, catalog, subplans)),
            pattern: Box::new(finalize_expr_subqueries(*pattern, catalog, subplans)),
            escape: escape.map(|expr| Box::new(finalize_expr_subqueries(*expr, catalog, subplans))),
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
            expr: Box::new(finalize_expr_subqueries(*expr, catalog, subplans)),
            pattern: Box::new(finalize_expr_subqueries(*pattern, catalog, subplans)),
            escape: escape.map(|expr| Box::new(finalize_expr_subqueries(*expr, catalog, subplans))),
            negated,
            collation_oid,
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
        Expr::Row { descriptor, fields } => Expr::Row {
            descriptor,
            fields: fields
                .into_iter()
                .map(|(name, expr)| (name, finalize_expr_subqueries(expr, catalog, subplans)))
                .collect(),
        },
        Expr::FieldSelect {
            expr,
            field,
            field_type,
        } => Expr::FieldSelect {
            expr: Box::new(finalize_expr_subqueries(*expr, catalog, subplans)),
            field,
            field_type,
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
        Expr::Xml(xml) => Expr::Xml(Box::new(crate::include::nodes::primnodes::XmlExpr {
            named_args: xml
                .named_args
                .into_iter()
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                .collect(),
            args: xml
                .args
                .into_iter()
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                .collect(),
            ..*xml
        })),
    }
}

fn finalize_sql_json_behavior(
    behavior: SqlJsonTableBehavior,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
) -> SqlJsonTableBehavior {
    match behavior {
        SqlJsonTableBehavior::Default(expr) => {
            SqlJsonTableBehavior::Default(finalize_expr_subqueries(expr, catalog, subplans))
        }
        other => other,
    }
}

fn finalize_set_returning_call(
    call: SetReturningCall,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
) -> SetReturningCall {
    match call {
        SetReturningCall::RowsFrom {
            items,
            output_columns,
            with_ordinality,
        } => SetReturningCall::RowsFrom {
            items: items
                .into_iter()
                .map(|item| RowsFromItem {
                    source: match item.source {
                        RowsFromSource::Function(call) => RowsFromSource::Function(
                            finalize_set_returning_call(call, catalog, subplans),
                        ),
                        RowsFromSource::Project {
                            output_exprs,
                            output_columns,
                        } => RowsFromSource::Project {
                            output_exprs: output_exprs
                                .into_iter()
                                .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                                .collect(),
                            output_columns,
                        },
                    },
                    column_definitions: item.column_definitions,
                })
                .collect(),
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
            start: finalize_expr_subqueries(start, catalog, subplans),
            stop: finalize_expr_subqueries(stop, catalog, subplans),
            step: finalize_expr_subqueries(step, catalog, subplans),
            timezone: timezone
                .map(|timezone| finalize_expr_subqueries(timezone, catalog, subplans)),
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
            array: finalize_expr_subqueries(array, catalog, subplans),
            dimension: finalize_expr_subqueries(dimension, catalog, subplans),
            reverse: reverse.map(|reverse| finalize_expr_subqueries(reverse, catalog, subplans)),
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
            relid: finalize_expr_subqueries(relid, catalog, subplans),
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
            relid: finalize_expr_subqueries(relid, catalog, subplans),
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
            arg: finalize_expr_subqueries(arg, catalog, subplans),
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
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                .collect(),
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
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                .collect(),
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
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                .collect(),
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
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                .collect(),
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
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                .collect(),
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
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                .collect(),
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
            args: args
                .into_iter()
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                .collect(),
            inlined_expr: inlined_expr
                .map(|expr| Box::new(finalize_expr_subqueries(*expr, catalog, subplans))),
            output_columns,
            with_ordinality,
        },
        sql @ (SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_)) => {
            sql.map_exprs(|arg| finalize_expr_subqueries(arg, catalog, subplans))
        }
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
        direct_args,
        args,
        order_by,
        filter,
        distinct,
        sql_type,
    } = accum;
    AggAccum {
        aggfnoid,
        agg_variadic,
        direct_args: direct_args
            .into_iter()
            .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
            .collect(),
        args: args
            .into_iter()
            .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
            .collect(),
        order_by: order_by
            .into_iter()
            .map(|item| crate::include::nodes::primnodes::OrderByEntry {
                expr: finalize_expr_subqueries(item.expr, catalog, subplans),
                ..item
            })
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
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
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
            aggorder: aggref
                .aggorder
                .into_iter()
                .map(|item| crate::include::nodes::primnodes::OrderByEntry {
                    expr: rebase_expr_subplan_ids(item.expr, base),
                    ..item
                })
                .collect(),
            aggfilter: aggref
                .aggfilter
                .map(|expr| rebase_expr_subplan_ids(expr, base)),
            ..*aggref
        })),
        Expr::WindowFunc(window_func) => {
            Expr::WindowFunc(Box::new(crate::include::nodes::primnodes::WindowFuncExpr {
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
            }))
        }
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
        Expr::SqlJsonQueryFunction(func) => {
            Expr::SqlJsonQueryFunction(Box::new(SqlJsonQueryFunction {
                context: rebase_expr_subplan_ids(func.context, base),
                path: rebase_expr_subplan_ids(func.path, base),
                passing: func
                    .passing
                    .into_iter()
                    .map(|arg| SqlJsonTablePassingArg {
                        name: arg.name,
                        expr: rebase_expr_subplan_ids(arg.expr, base),
                    })
                    .collect(),
                on_empty: rebase_sql_json_behavior(func.on_empty, base),
                on_error: rebase_sql_json_behavior(func.on_error, base),
                ..*func
            }))
        }
        Expr::SetReturning(srf) => Expr::SetReturning(Box::new(
            crate::include::nodes::primnodes::SetReturningExpr {
                call: rebase_set_returning_call_subplan_ids(srf.call, base),
                ..*srf
            },
        )),
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
            target_width: subplan.target_width,
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
        Expr::Collate {
            expr,
            collation_oid,
        } => Expr::Collate {
            expr: Box::new(rebase_expr_subplan_ids(*expr, base)),
            collation_oid,
        },
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
            collation_oid,
        } => Expr::Like {
            expr: Box::new(rebase_expr_subplan_ids(*expr, base)),
            pattern: Box::new(rebase_expr_subplan_ids(*pattern, base)),
            escape: escape.map(|expr| Box::new(rebase_expr_subplan_ids(*expr, base))),
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
            expr: Box::new(rebase_expr_subplan_ids(*expr, base)),
            pattern: Box::new(rebase_expr_subplan_ids(*pattern, base)),
            escape: escape.map(|expr| Box::new(rebase_expr_subplan_ids(*expr, base))),
            negated,
            collation_oid,
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
        Expr::Row { descriptor, fields } => Expr::Row {
            descriptor,
            fields: fields
                .into_iter()
                .map(|(name, expr)| (name, rebase_expr_subplan_ids(expr, base)))
                .collect(),
        },
        Expr::FieldSelect {
            expr,
            field,
            field_type,
        } => Expr::FieldSelect {
            expr: Box::new(rebase_expr_subplan_ids(*expr, base)),
            field,
            field_type,
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
        Expr::Xml(xml) => Expr::Xml(Box::new(crate::include::nodes::primnodes::XmlExpr {
            named_args: xml
                .named_args
                .into_iter()
                .map(|arg| rebase_expr_subplan_ids(arg, base))
                .collect(),
            args: xml
                .args
                .into_iter()
                .map(|arg| rebase_expr_subplan_ids(arg, base))
                .collect(),
            ..*xml
        })),
    }
}

fn rebase_sql_json_behavior(behavior: SqlJsonTableBehavior, base: usize) -> SqlJsonTableBehavior {
    match behavior {
        SqlJsonTableBehavior::Default(expr) => {
            SqlJsonTableBehavior::Default(rebase_expr_subplan_ids(expr, base))
        }
        other => other,
    }
}

fn rebase_set_returning_call_subplan_ids(call: SetReturningCall, base: usize) -> SetReturningCall {
    match call {
        SetReturningCall::RowsFrom {
            items,
            output_columns,
            with_ordinality,
        } => SetReturningCall::RowsFrom {
            items: items
                .into_iter()
                .map(|item| RowsFromItem {
                    source: match item.source {
                        RowsFromSource::Function(call) => RowsFromSource::Function(
                            rebase_set_returning_call_subplan_ids(call, base),
                        ),
                        RowsFromSource::Project {
                            output_exprs,
                            output_columns,
                        } => RowsFromSource::Project {
                            output_exprs: output_exprs
                                .into_iter()
                                .map(|expr| rebase_expr_subplan_ids(expr, base))
                                .collect(),
                            output_columns,
                        },
                    },
                    column_definitions: item.column_definitions,
                })
                .collect(),
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
            start: rebase_expr_subplan_ids(start, base),
            stop: rebase_expr_subplan_ids(stop, base),
            step: rebase_expr_subplan_ids(step, base),
            timezone: timezone.map(|timezone| rebase_expr_subplan_ids(timezone, base)),
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
            array: rebase_expr_subplan_ids(array, base),
            dimension: rebase_expr_subplan_ids(dimension, base),
            reverse: reverse.map(|reverse| rebase_expr_subplan_ids(reverse, base)),
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
            relid: rebase_expr_subplan_ids(relid, base),
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
            relid: rebase_expr_subplan_ids(relid, base),
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
            arg: rebase_expr_subplan_ids(arg, base),
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
                .map(|arg| rebase_expr_subplan_ids(arg, base))
                .collect(),
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
                .map(|arg| rebase_expr_subplan_ids(arg, base))
                .collect(),
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
                .map(|arg| rebase_expr_subplan_ids(arg, base))
                .collect(),
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
                .map(|arg| rebase_expr_subplan_ids(arg, base))
                .collect(),
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
                .map(|arg| rebase_expr_subplan_ids(arg, base))
                .collect(),
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
                .map(|arg| rebase_expr_subplan_ids(arg, base))
                .collect(),
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
            args: args
                .into_iter()
                .map(|arg| rebase_expr_subplan_ids(arg, base))
                .collect(),
            inlined_expr: inlined_expr.map(|expr| Box::new(rebase_expr_subplan_ids(*expr, base))),
            output_columns,
            with_ordinality,
        },
        sql @ (SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_)) => {
            sql.map_exprs(|arg| rebase_expr_subplan_ids(arg, base))
        }
    }
}

fn rebase_agg_accum_subplan_ids(accum: AggAccum, base: usize) -> AggAccum {
    AggAccum {
        args: accum
            .args
            .into_iter()
            .map(|arg| rebase_expr_subplan_ids(arg, base))
            .collect(),
        order_by: accum
            .order_by
            .into_iter()
            .map(|item| crate::include::nodes::primnodes::OrderByEntry {
                expr: rebase_expr_subplan_ids(item.expr, base),
                ..item
            })
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
            frame: crate::include::nodes::primnodes::WindowFrame {
                mode: clause.spec.frame.mode,
                start_bound: rebase_window_frame_bound_subplan_ids(
                    clause.spec.frame.start_bound,
                    base,
                ),
                end_bound: rebase_window_frame_bound_subplan_ids(clause.spec.frame.end_bound, base),
                exclusion: clause.spec.frame.exclusion,
            },
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

fn rebase_window_frame_bound_subplan_ids(
    bound: crate::include::nodes::primnodes::WindowFrameBound,
    base: usize,
) -> crate::include::nodes::primnodes::WindowFrameBound {
    match bound {
        crate::include::nodes::primnodes::WindowFrameBound::OffsetPreceding(offset) => {
            let expr = rebase_expr_subplan_ids(offset.expr.clone(), base);
            crate::include::nodes::primnodes::WindowFrameBound::OffsetPreceding(
                offset.with_expr(expr),
            )
        }
        crate::include::nodes::primnodes::WindowFrameBound::OffsetFollowing(offset) => {
            let expr = rebase_expr_subplan_ids(offset.expr.clone(), base);
            crate::include::nodes::primnodes::WindowFrameBound::OffsetFollowing(
                offset.with_expr(expr),
            )
        }
        other => other,
    }
}

fn rebase_partition_prune_info(
    mut info: crate::include::nodes::plannodes::PartitionPrunePlan,
    base: usize,
) -> crate::include::nodes::plannodes::PartitionPrunePlan {
    info.filter = rebase_expr_subplan_ids(info.filter, base);
    info
}

fn rebase_plan_subplan_ids(plan: Plan, base: usize) -> Plan {
    match plan {
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. } => plan,
        Plan::MergeAppend {
            plan_info,
            source_id,
            desc,
            items,
            partition_prune,
            children,
        } => Plan::MergeAppend {
            plan_info,
            source_id,
            desc,
            items: items
                .into_iter()
                .map(|item| crate::include::nodes::primnodes::OrderByEntry {
                    expr: rebase_expr_subplan_ids(item.expr, base),
                    ..item
                })
                .collect(),
            partition_prune: partition_prune.map(|info| rebase_partition_prune_info(info, base)),
            children: children
                .into_iter()
                .map(|child| rebase_plan_subplan_ids(child, base))
                .collect(),
        },
        Plan::Unique {
            plan_info,
            key_indices,
            input,
        } => Plan::Unique {
            plan_info,
            key_indices,
            input: Box::new(rebase_plan_subplan_ids(*input, base)),
        },
        Plan::BitmapIndexScan {
            plan_info,
            source_id,
            rel,
            relation_oid,
            index_rel,
            index_name,
            am_oid,
            desc,
            index_desc,
            index_meta,
            keys,
            index_quals,
        } => Plan::BitmapIndexScan {
            plan_info,
            source_id,
            rel,
            relation_oid,
            index_rel,
            index_name,
            am_oid,
            desc,
            index_desc,
            index_meta,
            keys,
            index_quals: index_quals
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
        },
        Plan::BitmapOr {
            plan_info,
            children,
        } => Plan::BitmapOr {
            plan_info,
            children: children
                .into_iter()
                .map(|child| rebase_plan_subplan_ids(child, base))
                .collect(),
        },
        Plan::BitmapAnd {
            plan_info,
            children,
        } => Plan::BitmapAnd {
            plan_info,
            children: children
                .into_iter()
                .map(|child| rebase_plan_subplan_ids(child, base))
                .collect(),
        },
        Plan::BitmapHeapScan {
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            toast,
            desc,
            bitmapqual,
            recheck_qual,
            filter_qual,
        } => Plan::BitmapHeapScan {
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            toast,
            desc,
            bitmapqual: Box::new(rebase_plan_subplan_ids(*bitmapqual, base)),
            recheck_qual: recheck_qual
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
            filter_qual: filter_qual
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
        },
        Plan::Append {
            plan_info,
            source_id,
            desc,
            partition_prune,
            children,
        } => Plan::Append {
            plan_info,
            source_id,
            desc,
            partition_prune: partition_prune.map(|info| rebase_partition_prune_info(info, base)),
            children: children
                .into_iter()
                .map(|child| rebase_plan_subplan_ids(child, base))
                .collect(),
        },
        Plan::SetOp {
            plan_info,
            op,
            strategy,
            output_columns,
            children,
        } => Plan::SetOp {
            plan_info,
            op,
            strategy,
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
        Plan::Materialize { plan_info, input } => Plan::Materialize {
            plan_info,
            input: Box::new(rebase_plan_subplan_ids(*input, base)),
        },
        Plan::Memoize {
            plan_info,
            input,
            cache_keys,
            cache_key_labels,
            key_paramids,
            dependent_paramids,
            binary_mode,
            single_row,
            est_entries,
        } => Plan::Memoize {
            plan_info,
            input: Box::new(rebase_plan_subplan_ids(*input, base)),
            cache_keys: cache_keys
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
            cache_key_labels,
            key_paramids,
            dependent_paramids,
            binary_mode,
            single_row,
            est_entries,
        },
        Plan::Gather {
            plan_info,
            input,
            workers_planned,
            single_copy,
        } => Plan::Gather {
            plan_info,
            input: Box::new(rebase_plan_subplan_ids(*input, base)),
            workers_planned,
            single_copy,
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
                    label: param.label,
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
        Plan::MergeJoin {
            plan_info,
            left,
            right,
            kind,
            merge_clauses,
            outer_merge_keys,
            inner_merge_keys,
            merge_key_descending,
            join_qual,
            qual,
        } => Plan::MergeJoin {
            plan_info,
            left: Box::new(rebase_plan_subplan_ids(*left, base)),
            right: Box::new(rebase_plan_subplan_ids(*right, base)),
            kind,
            merge_clauses: merge_clauses
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
            outer_merge_keys: outer_merge_keys
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
            inner_merge_keys: inner_merge_keys
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
            merge_key_descending,
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
            display_items,
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
                    collation_oid: item.collation_oid,
                })
                .collect(),
            display_items,
        },
        Plan::IncrementalSort {
            plan_info,
            input,
            items,
            presorted_count,
            display_items,
            presorted_display_items,
        } => Plan::IncrementalSort {
            plan_info,
            input: Box::new(rebase_plan_subplan_ids(*input, base)),
            items: items
                .into_iter()
                .map(|item| crate::include::nodes::primnodes::OrderByEntry {
                    expr: rebase_expr_subplan_ids(item.expr, base),
                    ressortgroupref: item.ressortgroupref,
                    descending: item.descending,
                    nulls_first: item.nulls_first,
                    collation_oid: item.collation_oid,
                })
                .collect(),
            presorted_count,
            display_items,
            presorted_display_items,
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
        Plan::LockRows {
            plan_info,
            input,
            row_marks,
        } => Plan::LockRows {
            plan_info,
            input: Box::new(rebase_plan_subplan_ids(*input, base)),
            row_marks,
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
            strategy,
            phase,
            disabled,
            input,
            group_by,
            passthrough_exprs,
            accumulators,
            semantic_accumulators,
            semantic_output_names,
            having,
            output_columns,
        } => Plan::Aggregate {
            plan_info,
            strategy,
            phase,
            disabled,
            input: Box::new(rebase_plan_subplan_ids(*input, base)),
            group_by: group_by
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
            passthrough_exprs: passthrough_exprs
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
            accumulators: accumulators
                .into_iter()
                .map(|accum| rebase_agg_accum_subplan_ids(accum, base))
                .collect(),
            semantic_accumulators: semantic_accumulators.map(|accumulators| {
                accumulators
                    .into_iter()
                    .map(|accum| rebase_agg_accum_subplan_ids(accum, base))
                    .collect()
            }),
            semantic_output_names,
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
        Plan::FunctionScan {
            plan_info,
            call,
            table_alias,
        } => Plan::FunctionScan {
            plan_info,
            call: rebase_set_returning_call_subplan_ids(call, base),
            table_alias,
        },
        Plan::SubqueryScan {
            plan_info,
            input,
            scan_name,
            filter,
            output_columns,
        } => Plan::SubqueryScan {
            plan_info,
            input: Box::new(rebase_plan_subplan_ids(*input, base)),
            scan_name,
            filter: filter.map(|expr| rebase_expr_subplan_ids(expr, base)),
            output_columns,
        },
        Plan::CteScan {
            plan_info,
            cte_id,
            cte_name,
            cte_plan,
            output_columns,
        } => Plan::CteScan {
            plan_info,
            cte_id,
            cte_name,
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
            recursive_references_worktable,
            output_columns,
            anchor,
            recursive,
        } => Plan::RecursiveUnion {
            plan_info,
            worktable_id,
            distinct,
            recursive_references_worktable,
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
                        source_expr,
                        call,
                        sql_type,
                        column_index,
                    } => ProjectSetTarget::Set {
                        name,
                        source_expr: rebase_expr_subplan_ids(source_expr, base),
                        call: rebase_set_returning_call_subplan_ids(call, base),
                        sql_type,
                        column_index,
                    },
                })
                .collect(),
        },
    }
}

fn finalize_partition_prune_info(
    mut info: crate::include::nodes::plannodes::PartitionPrunePlan,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
) -> crate::include::nodes::plannodes::PartitionPrunePlan {
    info.filter = finalize_expr_subqueries(info.filter, catalog, subplans);
    info
}

pub(super) fn finalize_plan_subqueries(
    plan: Plan,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
) -> Plan {
    match plan {
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::WorkTableScan { .. } => plan,
        Plan::BitmapOr {
            plan_info,
            children,
        } => Plan::BitmapOr {
            plan_info,
            children: children
                .into_iter()
                .map(|child| finalize_plan_subqueries(child, catalog, subplans))
                .collect(),
        },
        Plan::BitmapAnd {
            plan_info,
            children,
        } => Plan::BitmapAnd {
            plan_info,
            children: children
                .into_iter()
                .map(|child| finalize_plan_subqueries(child, catalog, subplans))
                .collect(),
        },
        Plan::MergeAppend {
            plan_info,
            source_id,
            desc,
            items,
            partition_prune,
            children,
        } => Plan::MergeAppend {
            plan_info,
            source_id,
            desc,
            items: items
                .into_iter()
                .map(|item| crate::include::nodes::primnodes::OrderByEntry {
                    expr: finalize_expr_subqueries(item.expr, catalog, subplans),
                    ..item
                })
                .collect(),
            partition_prune: partition_prune
                .map(|info| finalize_partition_prune_info(info, catalog, subplans)),
            children: children
                .into_iter()
                .map(|child| finalize_plan_subqueries(child, catalog, subplans))
                .collect(),
        },
        Plan::Unique {
            plan_info,
            key_indices,
            input,
        } => Plan::Unique {
            plan_info,
            key_indices,
            input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
        },
        Plan::BitmapHeapScan {
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            toast,
            desc,
            bitmapqual,
            recheck_qual,
            filter_qual,
        } => Plan::BitmapHeapScan {
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            toast,
            desc,
            bitmapqual: Box::new(finalize_plan_subqueries(*bitmapqual, catalog, subplans)),
            recheck_qual: recheck_qual
                .into_iter()
                .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                .collect(),
            filter_qual: filter_qual
                .into_iter()
                .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                .collect(),
        },
        Plan::Append {
            plan_info,
            source_id,
            desc,
            partition_prune,
            children,
        } => Plan::Append {
            plan_info,
            source_id,
            desc,
            partition_prune: partition_prune
                .map(|info| finalize_partition_prune_info(info, catalog, subplans)),
            children: children
                .into_iter()
                .map(|child| finalize_plan_subqueries(child, catalog, subplans))
                .collect(),
        },
        Plan::SetOp {
            plan_info,
            op,
            strategy,
            output_columns,
            children,
        } => Plan::SetOp {
            plan_info,
            op,
            strategy,
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
        Plan::Materialize { plan_info, input } => Plan::Materialize {
            plan_info,
            input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
        },
        Plan::Memoize {
            plan_info,
            input,
            cache_keys,
            cache_key_labels,
            key_paramids,
            dependent_paramids,
            binary_mode,
            single_row,
            est_entries,
        } => Plan::Memoize {
            plan_info,
            input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
            cache_keys: cache_keys
                .into_iter()
                .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                .collect(),
            cache_key_labels,
            key_paramids,
            dependent_paramids,
            binary_mode,
            single_row,
            est_entries,
        },
        Plan::Gather {
            plan_info,
            input,
            workers_planned,
            single_copy,
        } => Plan::Gather {
            plan_info,
            input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
            workers_planned,
            single_copy,
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
                    label: param.label,
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
        Plan::MergeJoin {
            plan_info,
            left,
            right,
            kind,
            merge_clauses,
            outer_merge_keys,
            inner_merge_keys,
            merge_key_descending,
            join_qual,
            qual,
        } => Plan::MergeJoin {
            plan_info,
            left: Box::new(finalize_plan_subqueries(*left, catalog, subplans)),
            right: Box::new(finalize_plan_subqueries(*right, catalog, subplans)),
            kind,
            merge_clauses: merge_clauses
                .into_iter()
                .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                .collect(),
            outer_merge_keys: outer_merge_keys
                .into_iter()
                .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                .collect(),
            inner_merge_keys: inner_merge_keys
                .into_iter()
                .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                .collect(),
            merge_key_descending,
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
            display_items,
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
                    collation_oid: item.collation_oid,
                })
                .collect(),
            display_items,
        },
        Plan::IncrementalSort {
            plan_info,
            input,
            items,
            presorted_count,
            display_items,
            presorted_display_items,
        } => Plan::IncrementalSort {
            plan_info,
            input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
            items: items
                .into_iter()
                .map(|item| crate::include::nodes::primnodes::OrderByEntry {
                    expr: finalize_expr_subqueries(item.expr, catalog, subplans),
                    ressortgroupref: item.ressortgroupref,
                    descending: item.descending,
                    nulls_first: item.nulls_first,
                    collation_oid: item.collation_oid,
                })
                .collect(),
            presorted_count,
            display_items,
            presorted_display_items,
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
        Plan::LockRows {
            plan_info,
            input,
            row_marks,
        } => Plan::LockRows {
            plan_info,
            input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
            row_marks,
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
            strategy,
            phase,
            disabled,
            input,
            group_by,
            passthrough_exprs,
            accumulators,
            semantic_accumulators,
            semantic_output_names,
            having,
            output_columns,
        } => Plan::Aggregate {
            plan_info,
            strategy,
            phase,
            disabled,
            input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
            group_by: group_by
                .into_iter()
                .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                .collect(),
            passthrough_exprs: passthrough_exprs
                .into_iter()
                .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                .collect(),
            accumulators: accumulators
                .into_iter()
                .map(|accum| finalize_agg_accum(accum, catalog, subplans))
                .collect(),
            semantic_accumulators: semantic_accumulators.map(|accumulators| {
                accumulators
                    .into_iter()
                    .map(|accum| finalize_agg_accum(accum, catalog, subplans))
                    .collect()
            }),
            semantic_output_names,
            having: having.map(|expr| finalize_expr_subqueries(expr, catalog, subplans)),
            output_columns,
        },
        Plan::WindowAgg {
            plan_info,
            input,
            clause,
            output_columns,
        } => {
            Plan::WindowAgg {
                plan_info,
                input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
                clause:
                    crate::include::nodes::primnodes::WindowClause {
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
                            frame: crate::include::nodes::primnodes::WindowFrame {
                                mode: clause.spec.frame.mode,
                                start_bound: finalize_window_frame_bound_subqueries(
                                    clause.spec.frame.start_bound,
                                    catalog,
                                    subplans,
                                ),
                                end_bound: finalize_window_frame_bound_subqueries(
                                    clause.spec.frame.end_bound,
                                    catalog,
                                    subplans,
                                ),
                                exclusion: clause.spec.frame.exclusion,
                            },
                        },
                        functions:
                            clause
                                .functions
                                .into_iter()
                                .map(|func| {
                                    crate::include::nodes::primnodes::WindowFuncExpr {
                        kind: match func.kind {
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
                        args: func
                            .args
                            .into_iter()
                            .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                            .collect(),
                        ..func
                    }
                                })
                                .collect(),
                    },
                output_columns,
            }
        }
        Plan::FunctionScan {
            plan_info,
            call,
            table_alias,
        } => Plan::FunctionScan {
            plan_info,
            call: finalize_set_returning_call(call, catalog, subplans),
            table_alias,
        },
        Plan::SubqueryScan {
            plan_info,
            input,
            scan_name,
            filter,
            output_columns,
        } => Plan::SubqueryScan {
            plan_info,
            input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
            scan_name,
            filter: filter.map(|expr| finalize_expr_subqueries(expr, catalog, subplans)),
            output_columns,
        },
        Plan::CteScan {
            plan_info,
            cte_id,
            cte_name,
            cte_plan,
            output_columns,
        } => Plan::CteScan {
            plan_info,
            cte_id,
            cte_name,
            cte_plan: Box::new(finalize_plan_subqueries(*cte_plan, catalog, subplans)),
            output_columns,
        },
        Plan::RecursiveUnion {
            plan_info,
            worktable_id,
            distinct,
            recursive_references_worktable,
            output_columns,
            anchor,
            recursive,
        } => Plan::RecursiveUnion {
            plan_info,
            worktable_id,
            distinct,
            recursive_references_worktable,
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
                        source_expr,
                        call,
                        sql_type,
                        column_index,
                    } => ProjectSetTarget::Set {
                        name,
                        source_expr: finalize_expr_subqueries(source_expr, catalog, subplans),
                        call: finalize_set_returning_call(call, catalog, subplans),
                        sql_type,
                        column_index,
                    },
                })
                .collect(),
        },
    }
}

fn finalize_window_frame_bound_subqueries(
    bound: crate::include::nodes::primnodes::WindowFrameBound,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
) -> crate::include::nodes::primnodes::WindowFrameBound {
    match bound {
        crate::include::nodes::primnodes::WindowFrameBound::OffsetPreceding(offset) => {
            let expr = finalize_expr_subqueries(offset.expr.clone(), catalog, subplans);
            crate::include::nodes::primnodes::WindowFrameBound::OffsetPreceding(
                offset.with_expr(expr),
            )
        }
        crate::include::nodes::primnodes::WindowFrameBound::OffsetFollowing(offset) => {
            let expr = finalize_expr_subqueries(offset.expr.clone(), catalog, subplans);
            crate::include::nodes::primnodes::WindowFrameBound::OffsetFollowing(
                offset.with_expr(expr),
            )
        }
        other => other,
    }
}
