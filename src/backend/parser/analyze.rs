use crate::RelFileLocator;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::{
    AggAccum, AggFunc, BuiltinScalarFunction, Expr, JsonTableFunction, Plan, QueryColumn,
    RelationDesc, TargetEntry, Value,
};

use super::parsenodes::*;
pub use crate::backend::catalog::catalog::{Catalog, CatalogEntry};

#[derive(Debug, Clone)]
pub(crate) struct BoundScope {
    pub(crate) desc: RelationDesc,
    pub(crate) columns: Vec<ScopeColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScopeColumn {
    pub(crate) output_name: String,
    pub(crate) relation_name: Option<String>,
}

fn empty_scope() -> BoundScope {
    BoundScope {
        desc: RelationDesc {
            columns: Vec::new(),
        },
        columns: Vec::new(),
    }
}

fn resolve_scalar_function(name: &str) -> Option<BuiltinScalarFunction> {
    match name.to_ascii_lowercase().as_str() {
        "random" => Some(BuiltinScalarFunction::Random),
        "to_json" => Some(BuiltinScalarFunction::ToJson),
        "to_jsonb" => Some(BuiltinScalarFunction::ToJsonb),
        "array_to_json" => Some(BuiltinScalarFunction::ArrayToJson),
        "json_build_array" => Some(BuiltinScalarFunction::JsonBuildArray),
        "json_build_object" => Some(BuiltinScalarFunction::JsonBuildObject),
        "json_object" => Some(BuiltinScalarFunction::JsonObject),
        "json_typeof" => Some(BuiltinScalarFunction::JsonTypeof),
        "json_array_length" => Some(BuiltinScalarFunction::JsonArrayLength),
        "json_extract_path" => Some(BuiltinScalarFunction::JsonExtractPath),
        "json_extract_path_text" => Some(BuiltinScalarFunction::JsonExtractPathText),
        "jsonb_typeof" => Some(BuiltinScalarFunction::JsonbTypeof),
        "jsonb_array_length" => Some(BuiltinScalarFunction::JsonbArrayLength),
        "jsonb_extract_path" => Some(BuiltinScalarFunction::JsonbExtractPath),
        "jsonb_extract_path_text" => Some(BuiltinScalarFunction::JsonbExtractPathText),
        "jsonb_build_array" => Some(BuiltinScalarFunction::JsonbBuildArray),
        "jsonb_build_object" => Some(BuiltinScalarFunction::JsonbBuildObject),
        "left" => Some(BuiltinScalarFunction::Left),
        "repeat" => Some(BuiltinScalarFunction::Repeat),
        _ => None,
    }
}

fn resolve_json_table_function(name: &str) -> Option<JsonTableFunction> {
    match name.to_ascii_lowercase().as_str() {
        "json_object_keys" => Some(JsonTableFunction::ObjectKeys),
        "json_each" => Some(JsonTableFunction::Each),
        "json_each_text" => Some(JsonTableFunction::EachText),
        "json_array_elements" => Some(JsonTableFunction::ArrayElements),
        "json_array_elements_text" => Some(JsonTableFunction::ArrayElementsText),
        "jsonb_object_keys" => Some(JsonTableFunction::JsonbObjectKeys),
        "jsonb_each" => Some(JsonTableFunction::JsonbEach),
        "jsonb_each_text" => Some(JsonTableFunction::JsonbEachText),
        "jsonb_array_elements" => Some(JsonTableFunction::JsonbArrayElements),
        "jsonb_array_elements_text" => Some(JsonTableFunction::JsonbArrayElementsText),
        _ => None,
    }
}

fn validate_scalar_function_arity(
    func: BuiltinScalarFunction,
    args: &[SqlExpr],
) -> Result<(), ParseError> {
    let valid = match func {
        BuiltinScalarFunction::Random => args.is_empty(),
        BuiltinScalarFunction::ToJson | BuiltinScalarFunction::ToJsonb => args.len() == 1,
        BuiltinScalarFunction::ArrayToJson => matches!(args.len(), 1 | 2),
        BuiltinScalarFunction::JsonBuildArray | BuiltinScalarFunction::JsonBuildObject => true,
        BuiltinScalarFunction::JsonObject => matches!(args.len(), 1 | 2),
        BuiltinScalarFunction::JsonTypeof
        | BuiltinScalarFunction::JsonArrayLength
        | BuiltinScalarFunction::JsonbTypeof
        | BuiltinScalarFunction::JsonbArrayLength => args.len() == 1,
        BuiltinScalarFunction::JsonExtractPath
        | BuiltinScalarFunction::JsonExtractPathText
        | BuiltinScalarFunction::JsonbExtractPath
        | BuiltinScalarFunction::JsonbExtractPathText => !args.is_empty(),
        BuiltinScalarFunction::JsonbBuildArray | BuiltinScalarFunction::JsonbBuildObject => true,
        BuiltinScalarFunction::Left | BuiltinScalarFunction::Repeat => args.len() == 2,
    };

    if valid {
        Ok(())
    } else {
        Err(ParseError::UnexpectedToken {
            expected: "valid builtin function arity",
            actual: format!("{func:?}({} args)", args.len()),
        })
    }
}

fn validate_aggregate_arity(func: AggFunc, args: &[SqlExpr]) -> Result<(), ParseError> {
    let valid = match func {
        AggFunc::Count => args.len() <= 1,
        AggFunc::Sum
        | AggFunc::Avg
        | AggFunc::Min
        | AggFunc::Max
        | AggFunc::JsonAgg
        | AggFunc::JsonbAgg => args.len() == 1,
        AggFunc::JsonObjectAgg | AggFunc::JsonbObjectAgg => args.len() == 2,
    };
    if valid {
        Ok(())
    } else {
        Err(ParseError::UnexpectedToken {
            expected: "valid aggregate arity",
            actual: format!("{}({} args)", func.name(), args.len()),
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct GroupedOuterScope {
    scope: BoundScope,
    group_by_exprs: Vec<SqlExpr>,
}

#[derive(Debug, Clone, Copy)]
enum ResolvedColumn {
    Local(usize),
    Outer { depth: usize, index: usize },
}

pub fn create_relation_desc(stmt: &CreateTableStatement) -> RelationDesc {
    RelationDesc {
        columns: stmt
            .columns
            .iter()
            .map(|column| column_desc(column.name.clone(), column.ty, column.nullable))
            .collect(),
    }
}

fn normalize_create_table_name_parts(
    schema_name: Option<&str>,
    table_name: &str,
    persistence: TablePersistence,
    on_commit: OnCommitAction,
) -> Result<(String, TablePersistence), ParseError> {
    let effective_persistence = match schema_name.map(|s| s.to_ascii_lowercase()) {
        Some(schema) if schema == "pg_temp" => TablePersistence::Temporary,
        Some(schema) => {
            if persistence == TablePersistence::Temporary {
                return Err(ParseError::TempTableInNonTempSchema(schema));
            }
            return Err(ParseError::UnsupportedQualifiedName(format!(
                "{schema}.{table_name}"
            )));
        }
        None => persistence,
    };

    if on_commit != OnCommitAction::PreserveRows
        && effective_persistence != TablePersistence::Temporary
    {
        return Err(ParseError::OnCommitOnlyForTempTables);
    }

    Ok((table_name.to_ascii_lowercase(), effective_persistence))
}

pub fn normalize_create_table_name(
    stmt: &CreateTableStatement,
) -> Result<(String, TablePersistence), ParseError> {
    normalize_create_table_name_parts(
        stmt.schema_name.as_deref(),
        &stmt.table_name,
        stmt.persistence,
        stmt.on_commit,
    )
}

pub fn normalize_create_table_as_name(
    stmt: &CreateTableAsStatement,
) -> Result<(String, TablePersistence), ParseError> {
    normalize_create_table_name_parts(
        stmt.schema_name.as_deref(),
        &stmt.table_name,
        stmt.persistence,
        stmt.on_commit,
    )
}

pub fn bind_create_table(
    stmt: &CreateTableStatement,
    catalog: &mut Catalog,
) -> Result<CatalogEntry, ParseError> {
    let (table_name, _) = normalize_create_table_name(stmt)?;
    catalog
        .create_table(table_name, create_relation_desc(stmt))
        .map_err(|err| match err {
            crate::backend::catalog::catalog::CatalogError::TableAlreadyExists(name) => {
                ParseError::TableAlreadyExists(name)
            }
            crate::backend::catalog::catalog::CatalogError::UnknownTable(name) => {
                ParseError::TableDoesNotExist(name)
            }
            crate::backend::catalog::catalog::CatalogError::UnknownType(name) => {
                ParseError::UnsupportedType(name)
            }
            crate::backend::catalog::catalog::CatalogError::Io(_)
            | crate::backend::catalog::catalog::CatalogError::Corrupt(_) => {
                ParseError::UnexpectedToken {
                    expected: "valid catalog state",
                    actual: "catalog error".into(),
                }
            }
        })
}

pub fn build_plan(stmt: &SelectStatement, catalog: &Catalog) -> Result<Plan, ParseError> {
    build_plan_with_outer(stmt, catalog, &[], None)
}

fn build_plan_with_outer(
    stmt: &SelectStatement,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<GroupedOuterScope>,
) -> Result<Plan, ParseError> {
    if stmt.targets.is_empty() && stmt.from.is_none() {
        return Err(ParseError::EmptySelectList);
    }

    let (base, scope) = if let Some(from) = &stmt.from {
        bind_from_item(from, catalog, outer_scopes, grouped_outer.as_ref())?
    } else {
        (Plan::Result, empty_scope())
    };

    if let Some(predicate) = &stmt.where_clause {
        if expr_contains_agg(predicate) {
            return Err(ParseError::AggInWhere);
        }
    }

    let mut plan = if let Some(predicate) = &stmt.where_clause {
        Plan::Filter {
            input: Box::new(base),
            predicate: bind_expr_with_outer(
                predicate,
                &scope,
                catalog,
                outer_scopes,
                grouped_outer.as_ref(),
            )?,
        }
    } else {
        base
    };

    let needs_agg =
        !stmt.group_by.is_empty() || targets_contain_agg(&stmt.targets) || stmt.having.is_some();

    if needs_agg {
        let mut aggs: Vec<(AggFunc, Vec<SqlExpr>, bool)> = Vec::new();
        for target in &stmt.targets {
            collect_aggs(&target.expr, &mut aggs);
        }
        if let Some(having) = &stmt.having {
            collect_aggs(having, &mut aggs);
        }

        let group_keys: Vec<Expr> = stmt
            .group_by
            .iter()
            .map(|e| bind_expr_with_outer(e, &scope, catalog, outer_scopes, grouped_outer.as_ref()))
            .collect::<Result<_, _>>()?;

        let accumulators: Vec<AggAccum> = aggs
            .iter()
            .map(|(func, args, distinct)| {
                validate_aggregate_arity(*func, args)?;
                let arg_type = args.first().map(|e| {
                    infer_sql_expr_type(e, &scope, catalog, outer_scopes, grouped_outer.as_ref())
                });
                Ok(AggAccum {
                    func: *func,
                    args: args
                        .iter()
                        .map(|e| {
                            bind_expr_with_outer(
                                e,
                                &scope,
                                catalog,
                                outer_scopes,
                                grouped_outer.as_ref(),
                            )
                        })
                        .collect::<Result<_, _>>()?,
                    distinct: *distinct,
                    sql_type: aggregate_sql_type(*func, arg_type),
                })
            })
            .collect::<Result<_, _>>()?;

        let n_keys = group_keys.len();
        let mut output_columns: Vec<QueryColumn> = Vec::new();
        for gk in &stmt.group_by {
            output_columns.push(QueryColumn {
                name: sql_expr_name(gk),
                sql_type: infer_sql_expr_type(
                    gk,
                    &scope,
                    catalog,
                    outer_scopes,
                    grouped_outer.as_ref(),
                ),
            });
        }
        for (func, args, _) in &aggs {
            output_columns.push(QueryColumn {
                name: func.name().to_string(),
                sql_type: aggregate_sql_type(
                    *func,
                    args.first().map(|e| {
                        infer_sql_expr_type(
                            e,
                            &scope,
                            catalog,
                            outer_scopes,
                            grouped_outer.as_ref(),
                        )
                    }),
                ),
            });
        }

        let having = stmt
            .having
            .as_ref()
            .map(|e| {
                bind_agg_output_expr(
                    e,
                    &stmt.group_by,
                    &scope,
                    catalog,
                    outer_scopes,
                    grouped_outer.as_ref(),
                    &aggs,
                    n_keys,
                )
            })
            .transpose()?;

        plan = Plan::Aggregate {
            input: Box::new(plan),
            group_by: group_keys,
            accumulators,
            having,
            output_columns: output_columns.clone(),
        };

        if !stmt.order_by.is_empty() {
            plan = Plan::OrderBy {
                input: Box::new(plan),
                items: stmt
                    .order_by
                    .iter()
                    .map(|item| {
                        Ok(crate::backend::executor::OrderByEntry {
                            expr: bind_agg_output_expr(
                                &item.expr,
                                &stmt.group_by,
                                &scope,
                                catalog,
                                outer_scopes,
                                grouped_outer.as_ref(),
                                &aggs,
                                n_keys,
                            )?,
                            descending: item.descending,
                            nulls_first: item.nulls_first,
                        })
                    })
                    .collect::<Result<Vec<_>, ParseError>>()?,
            };
        }

        if stmt.limit.is_some() || stmt.offset.is_some() {
            plan = Plan::Limit {
                input: Box::new(plan),
                limit: stmt.limit,
                offset: stmt.offset.unwrap_or(0),
            };
        }

        let targets: Vec<TargetEntry> = if stmt.targets.len() == 1
            && matches!(stmt.targets[0].expr, SqlExpr::Column(ref name) if name == "*")
        {
            output_columns
                .iter()
                .enumerate()
                .map(|(i, name)| TargetEntry {
                    name: name.name.clone(),
                    expr: Expr::Column(i),
                    sql_type: name.sql_type,
                })
                .collect()
        } else {
            stmt.targets
                .iter()
                .map(|item| {
                    Ok(TargetEntry {
                        name: item.output_name.clone(),
                        expr: bind_agg_output_expr(
                            &item.expr,
                            &stmt.group_by,
                            &scope,
                            catalog,
                            outer_scopes,
                            grouped_outer.as_ref(),
                            &aggs,
                            n_keys,
                        )?,
                        sql_type: infer_sql_expr_type(
                            &item.expr,
                            &scope,
                            catalog,
                            outer_scopes,
                            grouped_outer.as_ref(),
                        ),
                    })
                })
                .collect::<Result<_, _>>()?
        };

        Ok(Plan::Projection {
            input: Box::new(plan),
            targets,
        })
    } else {
        if !stmt.order_by.is_empty() {
            plan = Plan::OrderBy {
                input: Box::new(plan),
                items: stmt
                    .order_by
                    .iter()
                    .map(|item| {
                        Ok(crate::backend::executor::OrderByEntry {
                            expr: bind_expr_with_outer(
                                &item.expr,
                                &scope,
                                catalog,
                                outer_scopes,
                                grouped_outer.as_ref(),
                            )?,
                            descending: item.descending,
                            nulls_first: item.nulls_first,
                        })
                    })
                    .collect::<Result<Vec<_>, ParseError>>()?,
            };
        }

        if stmt.limit.is_some() || stmt.offset.is_some() {
            plan = Plan::Limit {
                input: Box::new(plan),
                limit: stmt.limit,
                offset: stmt.offset.unwrap_or(0),
            };
        }

        let targets = bind_select_targets(
            &stmt.targets,
            &scope,
            catalog,
            outer_scopes,
            grouped_outer.as_ref(),
        )?;

        // Optimization: skip Projection if it's an identity mapping (select *)
        let is_identity = targets.len() == scope.columns.len()
            && targets.iter().enumerate().all(|(i, t)| {
                matches!(&t.expr, Expr::Column(c) if *c == i)
                    && t.name == scope.columns[i].output_name
            });

        if is_identity {
            Ok(plan)
        } else {
            Ok(Plan::Projection {
                input: Box::new(plan),
                targets,
            })
        }
    }
}

fn bind_select_targets(
    targets: &[SelectItem],
    scope: &BoundScope,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<Vec<TargetEntry>, ParseError> {
    if targets.len() == 1 && matches!(targets[0].expr, SqlExpr::Column(ref name) if name == "*") {
        return Ok(scope
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| TargetEntry {
                name: column.output_name.clone(),
                expr: Expr::Column(index),
                sql_type: scope.desc.columns[index].sql_type,
            })
            .collect());
    }

    targets
        .iter()
        .map(|item| {
            Ok(TargetEntry {
                name: item.output_name.clone(),
                expr: bind_expr_with_outer(
                    &item.expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                )?,
                sql_type: infer_sql_expr_type(
                    &item.expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                ),
            })
        })
        .collect()
}

#[allow(dead_code)]
pub(crate) fn bind_expr(expr: &SqlExpr, scope: &BoundScope) -> Result<Expr, ParseError> {
    bind_expr_with_outer(expr, scope, &Catalog::default(), &[], None)
}

pub(crate) fn bind_expr_with_outer(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<Expr, ParseError> {
    Ok(match expr {
        SqlExpr::Column(name) => {
            match resolve_column_with_outer(scope, outer_scopes, name, grouped_outer)? {
                ResolvedColumn::Local(index) => Expr::Column(index),
                ResolvedColumn::Outer { depth, index } => Expr::OuterColumn { depth, index },
            }
        }
        SqlExpr::Const(value) => Expr::Const(value.clone()),
        SqlExpr::IntegerLiteral(value) => Expr::Const(bind_integer_literal(value)?),
        SqlExpr::NumericLiteral(value) => Expr::Const(bind_numeric_literal(value)?),
        SqlExpr::Add(left, right) => bind_arithmetic_expr(
            "+",
            Expr::Add,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
        SqlExpr::Sub(left, right) => bind_arithmetic_expr(
            "-",
            Expr::Sub,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
        SqlExpr::Mul(left, right) => bind_arithmetic_expr(
            "*",
            Expr::Mul,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
        SqlExpr::Div(left, right) => bind_arithmetic_expr(
            "/",
            Expr::Div,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
        SqlExpr::Mod(left, right) => bind_arithmetic_expr(
            "%",
            Expr::Mod,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
        SqlExpr::Concat(left, right) => bind_concat_expr(
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
        SqlExpr::UnaryPlus(inner) => Expr::UnaryPlus(Box::new(bind_expr_with_outer(
            inner,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?)),
        SqlExpr::Negate(inner) => Expr::Negate(Box::new(bind_expr_with_outer(
            inner,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?)),
        SqlExpr::Cast(inner, ty) => {
            let bound_inner = if let SqlExpr::ArrayLiteral(elements) = inner.as_ref() {
                Expr::ArrayLiteral {
                    elements: elements
                        .iter()
                        .map(|element| {
                            bind_expr_with_outer(
                                element,
                                scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                            )
                        })
                        .collect::<Result<_, _>>()?,
                    array_type: *ty,
                }
            } else {
                bind_expr_with_outer(inner, scope, catalog, outer_scopes, grouped_outer)?
            };
            Expr::Cast(Box::new(bound_inner), *ty)
        }
        SqlExpr::Eq(left, right) => bind_comparison_expr(
            Expr::Eq,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
        SqlExpr::NotEq(left, right) => bind_comparison_expr(
            Expr::NotEq,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
        SqlExpr::Lt(left, right) => bind_comparison_expr(
            Expr::Lt,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
        SqlExpr::LtEq(left, right) => bind_comparison_expr(
            Expr::LtEq,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
        SqlExpr::Gt(left, right) => bind_comparison_expr(
            Expr::Gt,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
        SqlExpr::GtEq(left, right) => bind_comparison_expr(
            Expr::GtEq,
            left,
            right,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?,
        SqlExpr::RegexMatch(left, right) => Expr::RegexMatch(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::And(left, right) => Expr::And(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::Or(left, right) => Expr::Or(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::Not(inner) => Expr::Not(Box::new(bind_expr_with_outer(
            inner,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?)),
        SqlExpr::IsNull(inner) => Expr::IsNull(Box::new(bind_expr_with_outer(
            inner,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?)),
        SqlExpr::IsNotNull(inner) => Expr::IsNotNull(Box::new(bind_expr_with_outer(
            inner,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
        )?)),
        SqlExpr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::ArrayLiteral(elements) => Expr::ArrayLiteral {
            elements: elements
                .iter()
                .map(|element| {
                    bind_expr_with_outer(element, scope, catalog, outer_scopes, grouped_outer)
                })
                .collect::<Result<_, _>>()?,
            array_type: infer_array_literal_type(
                elements,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?,
        },
        SqlExpr::ArrayOverlap(left, right) => Expr::ArrayOverlap(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::AggCall { .. } => {
            return Err(ParseError::UnexpectedToken {
                expected: "non-aggregate expression",
                actual: "aggregate function".into(),
            });
        }
        SqlExpr::ScalarSubquery(select) => {
            let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
            child_outer.push(scope.clone());
            child_outer.extend_from_slice(outer_scopes);
            let plan = build_plan_with_outer(select, catalog, &child_outer, None)?;
            ensure_single_column_subquery(&plan)?;
            Expr::ScalarSubquery(Box::new(plan))
        }
        SqlExpr::Exists(select) => {
            let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
            child_outer.push(scope.clone());
            child_outer.extend_from_slice(outer_scopes);
            Expr::ExistsSubquery(Box::new(build_plan_with_outer(
                select,
                catalog,
                &child_outer,
                None,
            )?))
        }
        SqlExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
            child_outer.push(scope.clone());
            child_outer.extend_from_slice(outer_scopes);
            let subquery_plan = build_plan_with_outer(subquery, catalog, &child_outer, None)?;
            ensure_single_column_subquery(&subquery_plan)?;
            let any_expr = Expr::AnySubquery {
                left: Box::new(bind_expr_with_outer(
                    expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                )?),
                op: SubqueryComparisonOp::Eq,
                subquery: Box::new(subquery_plan),
            };
            if *negated {
                Expr::Not(Box::new(any_expr))
            } else {
                any_expr
            }
        }
        SqlExpr::QuantifiedSubquery {
            left,
            op,
            is_all,
            subquery,
        } => {
            let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
            child_outer.push(scope.clone());
            child_outer.extend_from_slice(outer_scopes);
            let subquery_plan = build_plan_with_outer(subquery, catalog, &child_outer, None)?;
            ensure_single_column_subquery(&subquery_plan)?;
            if *is_all {
                Expr::AllSubquery {
                    left: Box::new(bind_expr_with_outer(
                        left,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                    )?),
                    op: *op,
                    subquery: Box::new(subquery_plan),
                }
            } else {
                Expr::AnySubquery {
                    left: Box::new(bind_expr_with_outer(
                        left,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                    )?),
                    op: *op,
                    subquery: Box::new(subquery_plan),
                }
            }
        }
        SqlExpr::QuantifiedArray {
            left,
            op,
            is_all,
            array,
        } => {
            if *is_all {
                Expr::AllArray {
                    left: Box::new(bind_expr_with_outer(
                        left,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                    )?),
                    op: *op,
                    right: Box::new(bind_expr_with_outer(
                        array,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                    )?),
                }
            } else {
                Expr::AnyArray {
                    left: Box::new(bind_expr_with_outer(
                        left,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                    )?),
                    op: *op,
                    right: Box::new(bind_expr_with_outer(
                        array,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                    )?),
                }
            }
        }
        SqlExpr::Random => Expr::Random,
        SqlExpr::JsonGet(left, right) => Expr::JsonGet(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::JsonGetText(left, right) => Expr::JsonGetText(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::JsonPath(left, right) => Expr::JsonPath(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::JsonPathText(left, right) => Expr::JsonPathText(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::JsonbContains(left, right) => Expr::JsonbContains(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::JsonbContained(left, right) => Expr::JsonbContained(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::JsonbExists(left, right) => Expr::JsonbExists(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::JsonbExistsAny(left, right) => Expr::JsonbExistsAny(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::JsonbExistsAll(left, right) => Expr::JsonbExistsAll(
            Box::new(bind_expr_with_outer(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
            Box::new(bind_expr_with_outer(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?),
        ),
        SqlExpr::FuncCall { name, args } => {
            let func =
                resolve_scalar_function(name).ok_or_else(|| ParseError::UnexpectedToken {
                    expected: "supported builtin function",
                    actual: name.clone(),
                })?;
            validate_scalar_function_arity(func, args)?;
            bind_scalar_function_call(func, args, scope, catalog, outer_scopes, grouped_outer)?
        }
        SqlExpr::CurrentTimestamp => Expr::CurrentTimestamp,
    })
}

fn bind_scalar_function_call(
    func: BuiltinScalarFunction,
    args: &[SqlExpr],
    scope: &BoundScope,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<Expr, ParseError> {
    let bound_args = args
        .iter()
        .map(|arg| bind_expr_with_outer(arg, scope, catalog, outer_scopes, grouped_outer))
        .collect::<Result<Vec<_>, _>>()?;
    match func {
        BuiltinScalarFunction::Left | BuiltinScalarFunction::Repeat => {
            let left_type = infer_sql_expr_type(&args[0], scope, catalog, outer_scopes, grouped_outer);
            let right_type = infer_sql_expr_type(&args[1], scope, catalog, outer_scopes, grouped_outer);
            if !should_use_text_concat(&args[0], left_type, &args[0], left_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "text argument",
                    actual: format!("{func:?}({})", sql_type_name(left_type)),
                });
            }
            if !is_numeric_family(right_type) {
                return Err(ParseError::UnexpectedToken {
                    expected: "integer argument",
                    actual: format!("{func:?}({})", sql_type_name(right_type)),
                });
            }
            Ok(Expr::FuncCall {
                func,
                args: vec![
                    coerce_bound_expr(bound_args[0].clone(), left_type, SqlType::new(SqlTypeKind::Text)),
                    coerce_bound_expr(bound_args[1].clone(), right_type, SqlType::new(SqlTypeKind::Int4)),
                ],
            })
        }
        _ => Ok(Expr::FuncCall {
            func,
            args: bound_args,
        }),
    }
}

fn bind_arithmetic_expr(
    op: &'static str,
    make: fn(Box<Expr>, Box<Expr>) -> Expr,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<Expr, ParseError> {
    let left_type = infer_sql_expr_type(left, scope, catalog, outer_scopes, grouped_outer);
    let right_type = infer_sql_expr_type(right, scope, catalog, outer_scopes, grouped_outer);
    let common = resolve_numeric_binary_type(op, left_type, right_type)?;
    let left = coerce_bound_expr(
        bind_expr_with_outer(left, scope, catalog, outer_scopes, grouped_outer)?,
        left_type,
        common,
    );
    let right = coerce_bound_expr(
        bind_expr_with_outer(right, scope, catalog, outer_scopes, grouped_outer)?,
        right_type,
        common,
    );
    Ok(make(Box::new(left), Box::new(right)))
}

fn bind_comparison_expr(
    make: fn(Box<Expr>, Box<Expr>) -> Expr,
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<Expr, ParseError> {
    let left_type = infer_sql_expr_type(left, scope, catalog, outer_scopes, grouped_outer);
    let right_type = infer_sql_expr_type(right, scope, catalog, outer_scopes, grouped_outer);
    let left_bound = bind_expr_with_outer(left, scope, catalog, outer_scopes, grouped_outer)?;
    let right_bound = bind_expr_with_outer(right, scope, catalog, outer_scopes, grouped_outer)?;
    let (left, right) = if is_numeric_family(left_type) && is_numeric_family(right_type) {
        let common = resolve_numeric_binary_type("=", left_type, right_type)?;
        (
            coerce_bound_expr(left_bound, left_type, common),
            coerce_bound_expr(right_bound, right_type, common),
        )
    } else {
        (left_bound, right_bound)
    };
    Ok(make(Box::new(left), Box::new(right)))
}

fn bind_concat_expr(
    left: &SqlExpr,
    right: &SqlExpr,
    scope: &BoundScope,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<Expr, ParseError> {
    let left_type = infer_sql_expr_type(left, scope, catalog, outer_scopes, grouped_outer);
    let right_type = infer_sql_expr_type(right, scope, catalog, outer_scopes, grouped_outer);
    let left_bound = bind_expr_with_outer(left, scope, catalog, outer_scopes, grouped_outer)?;
    let right_bound = bind_expr_with_outer(right, scope, catalog, outer_scopes, grouped_outer)?;
    bind_concat_operands(left, left_type, left_bound, right, right_type, right_bound)
}

fn bind_concat_operands(
    left_sql: &SqlExpr,
    left_type: SqlType,
    left_bound: Expr,
    right_sql: &SqlExpr,
    right_type: SqlType,
    right_bound: Expr,
) -> Result<Expr, ParseError> {
    if left_type.kind == SqlTypeKind::Jsonb
        && !left_type.is_array
        && right_type.kind == SqlTypeKind::Jsonb
        && !right_type.is_array
    {
        return Ok(Expr::Concat(Box::new(left_bound), Box::new(right_bound)));
    }

    if left_type.is_array || right_type.is_array {
        let element_type = resolve_array_concat_element_type(left_type, right_type)?;
        let left_expr = if left_type.is_array {
            coerce_bound_expr(left_bound, left_type, SqlType::array_of(element_type))
        } else {
            coerce_bound_expr(left_bound, left_type, element_type)
        };
        let right_expr = if right_type.is_array {
            coerce_bound_expr(right_bound, right_type, SqlType::array_of(element_type))
        } else {
            coerce_bound_expr(right_bound, right_type, element_type)
        };
        return Ok(Expr::Concat(Box::new(left_expr), Box::new(right_expr)));
    }

    if should_use_text_concat(left_sql, left_type, right_sql, right_type) {
        let text_type = SqlType::new(SqlTypeKind::Text);
        let left_expr = coerce_bound_expr(left_bound, left_type, text_type);
        let right_expr = coerce_bound_expr(right_bound, right_type, text_type);
        return Ok(Expr::Concat(Box::new(left_expr), Box::new(right_expr)));
    }

    Err(ParseError::UndefinedOperator {
        op: "||",
        left_type: sql_type_name(left_type),
        right_type: sql_type_name(right_type),
    })
}

fn coerce_bound_expr(expr: Expr, from: SqlType, to: SqlType) -> Expr {
    if from.element_type() == to.element_type() {
        expr
    } else {
        Expr::Cast(Box::new(expr), to)
    }
}

fn resolve_numeric_binary_type(
    op: &'static str,
    left: SqlType,
    right: SqlType,
) -> Result<SqlType, ParseError> {
    use SqlTypeKind::*;
    let left = left.element_type();
    let right = right.element_type();
    if op == "%" && (matches!(left.kind, Float4 | Float8) || matches!(right.kind, Float4 | Float8))
    {
        return Err(ParseError::UndefinedOperator {
            op,
            left_type: sql_type_name(left),
            right_type: sql_type_name(right),
        });
    }
    if matches!(left.kind, Float8) || matches!(right.kind, Float8) {
        return Ok(SqlType::new(Float8));
    }
    if matches!(left.kind, Float4) || matches!(right.kind, Float4) {
        return Ok(SqlType::new(Float4));
    }
    if matches!(left.kind, Numeric) || matches!(right.kind, Numeric) {
        return Ok(SqlType::new(Numeric));
    }
    if matches!(left.kind, Int8) || matches!(right.kind, Int8) {
        return Ok(SqlType::new(Int8));
    }
    if matches!(left.kind, Int4) || matches!(right.kind, Int4) {
        return Ok(SqlType::new(Int4));
    }
    Ok(SqlType::new(Int2))
}

fn sql_type_name(ty: SqlType) -> String {
    match ty.kind {
        SqlTypeKind::Int2 => "smallint",
        SqlTypeKind::Int4 => "integer",
        SqlTypeKind::Int8 => "bigint",
        SqlTypeKind::Float4 => "real",
        SqlTypeKind::Float8 => "double precision",
        SqlTypeKind::Numeric => "numeric",
        SqlTypeKind::Json => "json",
        SqlTypeKind::Jsonb => "jsonb",
        SqlTypeKind::Text => "text",
        SqlTypeKind::Bool => "boolean",
        SqlTypeKind::Timestamp => "timestamp",
        SqlTypeKind::Char => "character",
        SqlTypeKind::Varchar => "character varying",
    }
    .to_string()
}

fn is_numeric_family(ty: SqlType) -> bool {
    matches!(
        ty.element_type().kind,
        SqlTypeKind::Int2
            | SqlTypeKind::Int4
            | SqlTypeKind::Int8
            | SqlTypeKind::Float4
            | SqlTypeKind::Float8
            | SqlTypeKind::Numeric
    )
}

fn is_text_like_type(ty: SqlType) -> bool {
    matches!(
        ty.element_type().kind,
        SqlTypeKind::Text | SqlTypeKind::Char | SqlTypeKind::Varchar
    )
}

fn is_string_literal_expr(expr: &SqlExpr) -> bool {
    matches!(
        expr,
        SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
    )
}

fn should_use_text_concat(
    left_expr: &SqlExpr,
    left_type: SqlType,
    right_expr: &SqlExpr,
    right_type: SqlType,
) -> bool {
    if left_type.is_array || right_type.is_array {
        return false;
    }
    is_text_like_type(left_type)
        || is_text_like_type(right_type)
        || is_string_literal_expr(left_expr)
        || is_string_literal_expr(right_expr)
}

fn resolve_common_scalar_type(left: SqlType, right: SqlType) -> Option<SqlType> {
    let left = left.element_type();
    let right = right.element_type();
    if left == right {
        return Some(left);
    }
    if is_text_like_type(left) && is_text_like_type(right) {
        return Some(SqlType::new(SqlTypeKind::Text));
    }
    if is_numeric_family(left) && is_numeric_family(right) {
        return resolve_numeric_binary_type("+", left, right).ok();
    }
    None
}

fn resolve_array_concat_element_type(left: SqlType, right: SqlType) -> Result<SqlType, ParseError> {
    let left_elem = left.element_type();
    let right_elem = right.element_type();
    if left.is_array && right.is_array {
        return resolve_common_scalar_type(left_elem, right_elem).ok_or(ParseError::UndefinedOperator {
            op: "||",
            left_type: sql_type_name(left),
            right_type: sql_type_name(right),
        });
    }
    if left.is_array {
        return resolve_common_scalar_type(left_elem, right_elem).ok_or(ParseError::UndefinedOperator {
            op: "||",
            left_type: sql_type_name(left),
            right_type: sql_type_name(right),
        });
    }
    if right.is_array {
        return resolve_common_scalar_type(left_elem, right_elem).ok_or(ParseError::UndefinedOperator {
            op: "||",
            left_type: sql_type_name(left),
            right_type: sql_type_name(right),
        });
    }
    Err(ParseError::UndefinedOperator {
        op: "||",
        left_type: sql_type_name(left),
        right_type: sql_type_name(right),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundInsertStatement {
    pub rel: RelFileLocator,
    pub desc: RelationDesc,
    pub target_columns: Vec<usize>,
    pub values: Vec<Vec<Expr>>,
}

/// A pre-bound insert plan that can be executed repeatedly with different
/// parameter values, avoiding re-parsing and re-binding on each call.
#[derive(Debug, Clone)]
pub struct PreparedInsert {
    pub rel: RelFileLocator,
    pub desc: RelationDesc,
    pub target_columns: Vec<usize>,
    pub num_params: usize,
}

pub fn bind_insert_prepared(
    table_name: &str,
    columns: Option<&[String]>,
    num_params: usize,
    catalog: &Catalog,
) -> Result<PreparedInsert, ParseError> {
    let entry = catalog
        .get(table_name)
        .ok_or_else(|| ParseError::UnknownTable(table_name.to_string()))?;

    let target_columns = if let Some(columns) = columns {
        let scope = scope_for_relation(Some(table_name), &entry.desc);
        columns
            .iter()
            .map(|column| resolve_column(&scope, column))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        (0..entry.desc.columns.len()).collect()
    };

    if target_columns.len() != num_params {
        return Err(ParseError::InvalidInsertTargetCount {
            expected: target_columns.len(),
            actual: num_params,
        });
    }

    Ok(PreparedInsert {
        rel: entry.rel,
        desc: entry.desc.clone(),
        target_columns,
        num_params,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundUpdateStatement {
    pub rel: RelFileLocator,
    pub desc: RelationDesc,
    pub assignments: Vec<BoundAssignment>,
    pub predicate: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundDeleteStatement {
    pub rel: RelFileLocator,
    pub desc: RelationDesc,
    pub predicate: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundAssignment {
    pub column_index: usize,
    pub expr: Expr,
}

pub fn bind_insert(
    stmt: &InsertStatement,
    catalog: &Catalog,
) -> Result<BoundInsertStatement, ParseError> {
    let entry = catalog
        .get(&stmt.table_name)
        .ok_or_else(|| ParseError::UnknownTable(stmt.table_name.clone()))?;
    let scope = scope_for_relation(Some(&stmt.table_name), &entry.desc);

    let target_columns = if let Some(columns) = &stmt.columns {
        columns
            .iter()
            .map(|column| resolve_column(&scope, column))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        (0..entry.desc.columns.len()).collect()
    };

    for row in &stmt.values {
        if target_columns.len() != row.len() {
            return Err(ParseError::InvalidInsertTargetCount {
                expected: target_columns.len(),
                actual: row.len(),
            });
        }
    }

    Ok(BoundInsertStatement {
        rel: entry.rel,
        desc: entry.desc.clone(),
        target_columns,
        values: stmt
            .values
            .iter()
            .map(|row| {
                row.iter()
                    .map(|expr| bind_expr_with_outer(expr, &scope, catalog, &[], None))
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?,
    })
}

pub fn bind_update(
    stmt: &UpdateStatement,
    catalog: &Catalog,
) -> Result<BoundUpdateStatement, ParseError> {
    let entry = catalog
        .get(&stmt.table_name)
        .ok_or_else(|| ParseError::UnknownTable(stmt.table_name.clone()))?;
    let scope = scope_for_relation(Some(&stmt.table_name), &entry.desc);

    Ok(BoundUpdateStatement {
        rel: entry.rel,
        desc: entry.desc.clone(),
        assignments: stmt
            .assignments
            .iter()
            .map(|assignment| {
                Ok(BoundAssignment {
                    column_index: resolve_column(&scope, &assignment.column)?,
                    expr: bind_expr_with_outer(&assignment.expr, &scope, catalog, &[], None)?,
                })
            })
            .collect::<Result<Vec<_>, ParseError>>()?,
        predicate: stmt
            .where_clause
            .as_ref()
            .map(|expr| bind_expr_with_outer(expr, &scope, catalog, &[], None))
            .transpose()?,
    })
}

pub fn bind_delete(
    stmt: &DeleteStatement,
    catalog: &Catalog,
) -> Result<BoundDeleteStatement, ParseError> {
    let entry = catalog
        .get(&stmt.table_name)
        .ok_or_else(|| ParseError::UnknownTable(stmt.table_name.clone()))?;
    let scope = scope_for_relation(Some(&stmt.table_name), &entry.desc);

    Ok(BoundDeleteStatement {
        rel: entry.rel,
        desc: entry.desc.clone(),
        predicate: stmt
            .where_clause
            .as_ref()
            .map(|expr| bind_expr_with_outer(expr, &scope, catalog, &[], None))
            .transpose()?,
    })
}

fn resolve_column(scope: &BoundScope, name: &str) -> Result<usize, ParseError> {
    if name == "*" {
        return Err(ParseError::UnexpectedToken {
            expected: "named column",
            actual: "*".into(),
        });
    }
    if let Some((relation, column_name)) = name.rsplit_once('.') {
        let mut matches = scope.columns.iter().enumerate().filter(|(_, column)| {
            column
                .relation_name
                .as_deref()
                .is_some_and(|visible_relation| visible_relation.eq_ignore_ascii_case(relation))
                && column.output_name.eq_ignore_ascii_case(column_name)
        });
        let first = matches
            .next()
            .ok_or_else(|| ParseError::UnknownColumn(name.to_string()))?;
        if matches.next().is_some() {
            return Err(ParseError::UnexpectedToken {
                expected: "unambiguous column reference",
                actual: name.to_string(),
            });
        }
        return Ok(first.0);
    }

    let mut matches = scope
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.output_name.eq_ignore_ascii_case(name));
    let first = matches
        .next()
        .ok_or_else(|| ParseError::UnknownColumn(name.to_string()))?;
    if matches.next().is_some() {
        return Err(ParseError::UnexpectedToken {
            expected: "unambiguous column reference",
            actual: name.to_string(),
        });
    }
    Ok(first.0)
}

fn resolve_column_with_outer(
    scope: &BoundScope,
    outer_scopes: &[BoundScope],
    name: &str,
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<ResolvedColumn, ParseError> {
    if let Ok(index) = resolve_column(scope, name) {
        return Ok(ResolvedColumn::Local(index));
    }

    for (depth, outer_scope) in outer_scopes.iter().enumerate() {
        if let Ok(index) = resolve_column(outer_scope, name) {
            if depth == 0 {
                if let Some(grouped) = grouped_outer {
                    if scopes_match(&grouped.scope, outer_scope)
                        && !outer_column_is_grouped(index, &grouped.scope, &grouped.group_by_exprs)
                    {
                        return Err(ParseError::UngroupedColumn(name.to_string()));
                    }
                }
            }
            return Ok(ResolvedColumn::Outer { depth, index });
        }
    }

    Err(ParseError::UnknownColumn(name.to_string()))
}

fn scopes_match(left: &BoundScope, right: &BoundScope) -> bool {
    left.columns == right.columns && left.desc == right.desc
}

fn outer_column_is_grouped(index: usize, scope: &BoundScope, group_by_exprs: &[SqlExpr]) -> bool {
    group_by_exprs.iter().any(|expr| match expr {
        SqlExpr::Column(name) => resolve_column(scope, name)
            .ok()
            .is_some_and(|group_idx| group_idx == index),
        _ => false,
    })
}

fn bind_from_item(
    stmt: &FromItem,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<(Plan, BoundScope), ParseError> {
    match stmt {
        FromItem::Table { name } => {
            let entry = catalog
                .get(name)
                .ok_or_else(|| ParseError::UnknownTable(name.clone()))?;
            let desc = entry.desc.clone();
            Ok((
                Plan::SeqScan {
                    rel: entry.rel,
                    desc: desc.clone(),
                },
                scope_for_relation(Some(name), &desc),
            ))
        }
        FromItem::FunctionCall { name, args } => match name.as_str() {
            "generate_series" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(ParseError::UnexpectedToken {
                        expected: "generate_series(start, stop[, step])",
                        actual: format!("generate_series with {} arguments", args.len()),
                    });
                }
                let empty_scope = empty_scope();
                let start = bind_expr_with_outer(
                    &args[0],
                    &empty_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                )?;
                let stop = bind_expr_with_outer(
                    &args[1],
                    &empty_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                )?;
                let step = if args.len() == 3 {
                    bind_expr_with_outer(
                        &args[2],
                        &empty_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                    )?
                } else {
                    Expr::Const(Value::Int32(1))
                };
                let desc = RelationDesc {
                    columns: vec![column_desc(
                        "generate_series",
                        SqlType::new(SqlTypeKind::Int4),
                        false,
                    )],
                };
                let scope = scope_for_relation(Some(name), &desc);
                Ok((
                    Plan::GenerateSeries {
                        start,
                        stop,
                        step,
                        output: QueryColumn {
                            name: "generate_series".to_string(),
                            sql_type: SqlType::new(SqlTypeKind::Int4),
                        },
                    },
                    scope,
                ))
            }
            "unnest" => {
                if args.is_empty() {
                    return Err(ParseError::UnexpectedToken {
                        expected: "unnest(array_expr [, array_expr ...])",
                        actual: "unnest()".into(),
                    });
                }
                let empty_scope = empty_scope();
                let mut bound_args = Vec::with_capacity(args.len());
                let mut output_columns = Vec::with_capacity(args.len());
                let mut desc_columns = Vec::with_capacity(args.len());
                for (idx, arg) in args.iter().enumerate() {
                    let arg_type = infer_sql_expr_type(
                        arg,
                        &empty_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                    );
                    if !arg_type.is_array {
                        return Err(ParseError::UnexpectedToken {
                            expected: "array argument to unnest",
                            actual: format!("{arg:?}"),
                        });
                    }
                    let element_type = arg_type.element_type();
                    let column_name = if idx == 0 {
                        "unnest".to_string()
                    } else {
                        format!("unnest_{}", idx + 1)
                    };
                    bound_args.push(bind_expr_with_outer(
                        arg,
                        &empty_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                    )?);
                    output_columns.push(QueryColumn {
                        name: column_name.clone(),
                        sql_type: element_type,
                    });
                    desc_columns.push(column_desc(column_name, element_type, true));
                }
                let desc = RelationDesc {
                    columns: desc_columns,
                };
                let scope = scope_for_relation(Some(name), &desc);
                Ok((
                    Plan::Unnest {
                        args: bound_args,
                        output_columns,
                    },
                    scope,
                ))
            }
            other => {
                if let Some(kind) = resolve_json_table_function(other) {
                    if args.len() != 1 {
                        return Err(ParseError::UnexpectedToken {
                            expected: "single json argument",
                            actual: format!("{other} with {} arguments", args.len()),
                        });
                    }
                    let empty_scope = empty_scope();
                    let arg = bind_expr_with_outer(
                        &args[0],
                        &empty_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                    )?;
                    let output_columns = match kind {
                        JsonTableFunction::ObjectKeys => {
                            vec![QueryColumn::text("json_object_keys")]
                        }
                        JsonTableFunction::Each => vec![
                            QueryColumn::text("key"),
                            QueryColumn {
                                name: "value".into(),
                                sql_type: SqlType::new(SqlTypeKind::Json),
                            },
                        ],
                        JsonTableFunction::EachText => {
                            vec![QueryColumn::text("key"), QueryColumn::text("value")]
                        }
                        JsonTableFunction::ArrayElements => vec![QueryColumn {
                            name: "json_array_elements".into(),
                            sql_type: SqlType::new(SqlTypeKind::Json),
                        }],
                        JsonTableFunction::ArrayElementsText => {
                            vec![QueryColumn::text("json_array_elements_text")]
                        }
                        JsonTableFunction::JsonbObjectKeys => {
                            vec![QueryColumn::text("jsonb_object_keys")]
                        }
                        JsonTableFunction::JsonbEach => vec![
                            QueryColumn::text("key"),
                            QueryColumn {
                                name: "value".into(),
                                sql_type: SqlType::new(SqlTypeKind::Jsonb),
                            },
                        ],
                        JsonTableFunction::JsonbEachText => {
                            vec![QueryColumn::text("key"), QueryColumn::text("value")]
                        }
                        JsonTableFunction::JsonbArrayElements => vec![QueryColumn {
                            name: "jsonb_array_elements".into(),
                            sql_type: SqlType::new(SqlTypeKind::Jsonb),
                        }],
                        JsonTableFunction::JsonbArrayElementsText => {
                            vec![QueryColumn::text("jsonb_array_elements_text")]
                        }
                    };
                    let desc = RelationDesc {
                        columns: output_columns
                            .iter()
                            .map(|col| column_desc(col.name.clone(), col.sql_type, true))
                            .collect(),
                    };
                    let scope = scope_for_relation(Some(name), &desc);
                    Ok((
                        Plan::JsonTableFunction {
                            kind,
                            arg,
                            output_columns,
                        },
                        scope,
                    ))
                } else {
                    Err(ParseError::UnknownTable(other.to_string()))
                }
            }
        },
        FromItem::DerivedTable(select) => {
            let plan = build_plan_with_outer(select, catalog, &[], None)?;
            let desc = synthetic_desc_from_plan(&plan);
            Ok((plan, scope_for_relation(None, &desc)))
        }
        FromItem::Join {
            left,
            right,
            kind,
            on,
        } => {
            let (left_plan, left_scope) =
                bind_from_item(left, catalog, outer_scopes, grouped_outer)?;
            let (right_plan, right_scope) =
                bind_from_item(right, catalog, outer_scopes, grouped_outer)?;
            let scope = combine_scopes(&left_scope, &right_scope);
            let on = match (kind, on) {
                (JoinKind::Inner, Some(on)) => {
                    bind_expr_with_outer(on, &scope, catalog, outer_scopes, grouped_outer)?
                }
                (JoinKind::Cross, None) => Expr::Const(Value::Bool(true)),
                _ => {
                    return Err(ParseError::UnexpectedToken {
                        expected: "valid join clause",
                        actual: format!("{stmt:?}"),
                    });
                }
            };
            Ok((
                Plan::NestedLoopJoin {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    on,
                },
                scope,
            ))
        }
        FromItem::Alias {
            source,
            alias,
            column_aliases,
        } => {
            let (plan, scope) = bind_from_item(source, catalog, outer_scopes, grouped_outer)?;
            apply_relation_alias(plan, scope, alias, column_aliases)
        }
    }
}

fn scope_for_relation(relation_name: Option<&str>, desc: &RelationDesc) -> BoundScope {
    BoundScope {
        desc: desc.clone(),
        columns: desc
            .columns
            .iter()
            .map(|column| ScopeColumn {
                output_name: column.name.clone(),
                relation_name: relation_name.map(str::to_string),
            })
            .collect(),
    }
}

fn combine_scopes(left: &BoundScope, right: &BoundScope) -> BoundScope {
    let mut desc = left.desc.clone();
    desc.columns.extend(right.desc.columns.clone());
    let mut columns = left.columns.clone();
    columns.extend(right.columns.clone());
    BoundScope { desc, columns }
}

fn synthetic_desc_from_plan(plan: &Plan) -> RelationDesc {
    RelationDesc {
        columns: plan
            .column_names()
            .into_iter()
            .zip(plan.columns().into_iter())
            .map(|(name, col)| column_desc(name, col.sql_type, true))
            .collect(),
    }
}

fn apply_relation_alias(
    mut plan: Plan,
    scope: BoundScope,
    alias: &str,
    column_aliases: &[String],
) -> Result<(Plan, BoundScope), ParseError> {
    if column_aliases.len() > scope.columns.len() {
        return Err(ParseError::UnexpectedToken {
            expected: "column alias count to be less than or equal to source column count",
            actual: format!(
                "{} aliases for {} columns",
                column_aliases.len(),
                scope.columns.len()
            ),
        });
    }

    let mut desc = scope.desc.clone();
    let mut columns = scope.columns.clone();
    let mut renamed = false;

    for (index, column) in columns.iter_mut().enumerate() {
        if let Some(new_name) = column_aliases.get(index) {
            renamed |= column.output_name != *new_name;
            column.output_name = new_name.clone();
            desc.columns[index].name = new_name.clone();
            desc.columns[index].storage.name = new_name.clone();
        }
        column.relation_name = Some(alias.to_string());
    }

    if renamed {
        plan = Plan::Projection {
            input: Box::new(plan),
            targets: columns
                .iter()
                .enumerate()
                .map(|(index, column)| TargetEntry {
                    name: column.output_name.clone(),
                    expr: Expr::Column(index),
                    sql_type: desc.columns[index].sql_type,
                })
                .collect(),
        };
    }

    Ok((plan, BoundScope { desc, columns }))
}

fn expr_contains_agg(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::AggCall { .. } => true,
        SqlExpr::Column(_)
        | SqlExpr::Const(_)
        | SqlExpr::IntegerLiteral(_)
        | SqlExpr::NumericLiteral(_)
        | SqlExpr::ScalarSubquery(_)
        | SqlExpr::Exists(_)
        | SqlExpr::InSubquery { .. }
        | SqlExpr::QuantifiedSubquery { .. }
        | SqlExpr::Random
        | SqlExpr::FuncCall { .. }
        | SqlExpr::CurrentTimestamp => false,
        SqlExpr::ArrayLiteral(elements) => elements.iter().any(expr_contains_agg),
        SqlExpr::ArrayOverlap(l, r)
        | SqlExpr::QuantifiedArray {
            left: l, array: r, ..
        }
        | SqlExpr::JsonGet(l, r)
        | SqlExpr::JsonGetText(l, r)
        | SqlExpr::JsonPath(l, r)
        | SqlExpr::JsonPathText(l, r)
        | SqlExpr::JsonbContains(l, r)
        | SqlExpr::JsonbContained(l, r)
        | SqlExpr::JsonbExists(l, r)
        | SqlExpr::JsonbExistsAny(l, r)
        | SqlExpr::JsonbExistsAll(l, r) => expr_contains_agg(l) || expr_contains_agg(r),
        SqlExpr::Cast(inner, _) => expr_contains_agg(inner),
        SqlExpr::Add(l, r)
        | SqlExpr::Sub(l, r)
        | SqlExpr::Mul(l, r)
        | SqlExpr::Div(l, r)
        | SqlExpr::Mod(l, r)
        | SqlExpr::Concat(l, r)
        | SqlExpr::Eq(l, r)
        | SqlExpr::NotEq(l, r)
        | SqlExpr::Lt(l, r)
        | SqlExpr::LtEq(l, r)
        | SqlExpr::Gt(l, r)
        | SqlExpr::GtEq(l, r)
        | SqlExpr::RegexMatch(l, r)
        | SqlExpr::And(l, r)
        | SqlExpr::Or(l, r)
        | SqlExpr::IsDistinctFrom(l, r)
        | SqlExpr::IsNotDistinctFrom(l, r) => expr_contains_agg(l) || expr_contains_agg(r),
        SqlExpr::UnaryPlus(inner)
        | SqlExpr::Negate(inner)
        | SqlExpr::Not(inner)
        | SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner) => expr_contains_agg(inner),
    }
}

fn targets_contain_agg(targets: &[SelectItem]) -> bool {
    targets.iter().any(|t| expr_contains_agg(&t.expr))
}

fn collect_aggs(expr: &SqlExpr, aggs: &mut Vec<(AggFunc, Vec<SqlExpr>, bool)>) {
    match expr {
        SqlExpr::AggCall {
            func,
            args,
            distinct,
        } => {
            let entry = (*func, args.clone(), *distinct);
            if !aggs.contains(&entry) {
                aggs.push(entry);
            }
        }
        SqlExpr::Column(_)
        | SqlExpr::Const(_)
        | SqlExpr::IntegerLiteral(_)
        | SqlExpr::NumericLiteral(_)
        | SqlExpr::ScalarSubquery(_)
        | SqlExpr::Exists(_)
        | SqlExpr::InSubquery { .. }
        | SqlExpr::QuantifiedSubquery { .. }
        | SqlExpr::Random
        | SqlExpr::CurrentTimestamp => {}
        SqlExpr::FuncCall { args, .. } => {
            for arg in args {
                collect_aggs(arg, aggs);
            }
        }
        SqlExpr::ArrayLiteral(elements) => {
            for element in elements {
                collect_aggs(element, aggs);
            }
        }
        SqlExpr::ArrayOverlap(l, r)
        | SqlExpr::QuantifiedArray {
            left: l, array: r, ..
        }
        | SqlExpr::JsonGet(l, r)
        | SqlExpr::JsonGetText(l, r)
        | SqlExpr::JsonPath(l, r)
        | SqlExpr::JsonPathText(l, r)
        | SqlExpr::JsonbContains(l, r)
        | SqlExpr::JsonbContained(l, r)
        | SqlExpr::JsonbExists(l, r)
        | SqlExpr::JsonbExistsAny(l, r)
        | SqlExpr::JsonbExistsAll(l, r) => {
            collect_aggs(l, aggs);
            collect_aggs(r, aggs);
        }
        SqlExpr::Cast(inner, _) => collect_aggs(inner, aggs),
        SqlExpr::Add(l, r)
        | SqlExpr::Sub(l, r)
        | SqlExpr::Mul(l, r)
        | SqlExpr::Div(l, r)
        | SqlExpr::Mod(l, r)
        | SqlExpr::Concat(l, r)
        | SqlExpr::Eq(l, r)
        | SqlExpr::NotEq(l, r)
        | SqlExpr::Lt(l, r)
        | SqlExpr::LtEq(l, r)
        | SqlExpr::Gt(l, r)
        | SqlExpr::GtEq(l, r)
        | SqlExpr::RegexMatch(l, r)
        | SqlExpr::And(l, r)
        | SqlExpr::Or(l, r)
        | SqlExpr::IsDistinctFrom(l, r)
        | SqlExpr::IsNotDistinctFrom(l, r) => {
            collect_aggs(l, aggs);
            collect_aggs(r, aggs);
        }
        SqlExpr::UnaryPlus(inner)
        | SqlExpr::Negate(inner)
        | SqlExpr::Not(inner)
        | SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner) => collect_aggs(inner, aggs),
    }
}

fn sql_expr_name(expr: &SqlExpr) -> String {
    match expr {
        SqlExpr::Column(name) => name.clone(),
        SqlExpr::AggCall { func, .. } => func.name().to_string(),
        SqlExpr::ScalarSubquery(_)
        | SqlExpr::Exists(_)
        | SqlExpr::InSubquery { .. }
        | SqlExpr::QuantifiedSubquery { .. }
        | SqlExpr::ArrayLiteral(_)
        | SqlExpr::ArrayOverlap(_, _)
        | SqlExpr::QuantifiedArray { .. } => "?column?".to_string(),
        _ => "?column?".to_string(),
    }
}

fn infer_sql_expr_type(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> SqlType {
    match expr {
        SqlExpr::Column(name) => {
            match resolve_column_with_outer(scope, outer_scopes, name, grouped_outer) {
                Ok(ResolvedColumn::Local(idx)) => scope.desc.columns.get(idx).map(|c| c.sql_type),
                Ok(ResolvedColumn::Outer { depth, index }) => outer_scopes
                    .get(depth)
                    .and_then(|s| s.desc.columns.get(index).map(|c| c.sql_type)),
                Err(_) => None,
            }
            .unwrap_or(SqlType::new(SqlTypeKind::Text))
        }
        SqlExpr::Const(Value::Int16(_)) => SqlType::new(SqlTypeKind::Int2),
        SqlExpr::Const(Value::Int32(_)) => SqlType::new(SqlTypeKind::Int4),
        SqlExpr::Const(Value::Int64(_)) => SqlType::new(SqlTypeKind::Int8),
        SqlExpr::Const(Value::Bool(_)) => SqlType::new(SqlTypeKind::Bool),
        SqlExpr::Const(Value::Numeric(_)) => SqlType::new(SqlTypeKind::Numeric),
        SqlExpr::Const(Value::Json(_)) => SqlType::new(SqlTypeKind::Json),
        SqlExpr::Const(Value::Jsonb(_)) => SqlType::new(SqlTypeKind::Jsonb),
        SqlExpr::Const(Value::Text(_))
        | SqlExpr::Const(Value::TextRef(_, _))
        | SqlExpr::Const(Value::Null) => SqlType::new(SqlTypeKind::Text),
        SqlExpr::Const(Value::Array(_)) => SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
        SqlExpr::Const(Value::Float64(_)) => SqlType::new(SqlTypeKind::Float8),
        SqlExpr::IntegerLiteral(value) => infer_integer_literal_type(value),
        SqlExpr::NumericLiteral(_) => SqlType::new(SqlTypeKind::Numeric),
        SqlExpr::Add(left, right)
        | SqlExpr::Sub(left, right)
        | SqlExpr::Mul(left, right)
        | SqlExpr::Div(left, right)
        | SqlExpr::Mod(left, right) => infer_arithmetic_sql_type(
            expr,
            infer_sql_expr_type(left, scope, catalog, outer_scopes, grouped_outer),
            infer_sql_expr_type(right, scope, catalog, outer_scopes, grouped_outer),
        ),
        SqlExpr::Concat(left, right) => infer_concat_sql_type(
            expr,
            infer_sql_expr_type(left, scope, catalog, outer_scopes, grouped_outer),
            infer_sql_expr_type(right, scope, catalog, outer_scopes, grouped_outer),
        ),
        SqlExpr::UnaryPlus(inner) => {
            infer_sql_expr_type(inner, scope, catalog, outer_scopes, grouped_outer)
        }
        SqlExpr::Negate(inner) => {
            infer_sql_expr_type(inner, scope, catalog, outer_scopes, grouped_outer)
        }
        SqlExpr::Cast(_, ty) => *ty,
        SqlExpr::Eq(_, _)
        | SqlExpr::NotEq(_, _)
        | SqlExpr::Lt(_, _)
        | SqlExpr::LtEq(_, _)
        | SqlExpr::Gt(_, _)
        | SqlExpr::GtEq(_, _)
        | SqlExpr::RegexMatch(_, _)
        | SqlExpr::And(_, _)
        | SqlExpr::Or(_, _)
        | SqlExpr::Not(_)
        | SqlExpr::IsNull(_)
        | SqlExpr::IsNotNull(_)
        | SqlExpr::IsDistinctFrom(_, _)
        | SqlExpr::IsNotDistinctFrom(_, _)
        | SqlExpr::ArrayOverlap(_, _)
        | SqlExpr::JsonbContains(_, _)
        | SqlExpr::JsonbContained(_, _)
        | SqlExpr::JsonbExists(_, _)
        | SqlExpr::JsonbExistsAny(_, _)
        | SqlExpr::JsonbExistsAll(_, _)
        | SqlExpr::QuantifiedArray { .. } => SqlType::new(SqlTypeKind::Bool),
        SqlExpr::JsonGet(left, _) | SqlExpr::JsonPath(left, _) => {
            let left_type = infer_sql_expr_type(left, scope, catalog, outer_scopes, grouped_outer);
            if matches!(left_type.element_type().kind, SqlTypeKind::Jsonb) {
                SqlType::new(SqlTypeKind::Jsonb)
            } else {
                SqlType::new(SqlTypeKind::Json)
            }
        }
        SqlExpr::JsonGetText(_, _) | SqlExpr::JsonPathText(_, _) => SqlType::new(SqlTypeKind::Text),
        SqlExpr::AggCall { func, args, .. } => aggregate_sql_type(
            *func,
            args.first()
                .map(|expr| infer_sql_expr_type(expr, scope, catalog, outer_scopes, grouped_outer)),
        ),
        SqlExpr::ArrayLiteral(elements) => {
            infer_array_literal_type(elements, scope, catalog, outer_scopes, grouped_outer)
                .unwrap_or(SqlType::array_of(SqlType::new(SqlTypeKind::Text)))
        }
        SqlExpr::ScalarSubquery(select) => {
            build_plan_with_outer(select, catalog, outer_scopes, grouped_outer.cloned())
                .ok()
                .and_then(|plan| {
                    let cols = plan.columns();
                    if cols.len() == 1 {
                        Some(cols[0].sql_type)
                    } else {
                        None
                    }
                })
                .unwrap_or(SqlType::new(SqlTypeKind::Text))
        }
        SqlExpr::Exists(_) | SqlExpr::InSubquery { .. } | SqlExpr::QuantifiedSubquery { .. } => {
            SqlType::new(SqlTypeKind::Bool)
        }
        SqlExpr::Random => SqlType::new(SqlTypeKind::Float8),
        SqlExpr::FuncCall { name, .. } => match resolve_scalar_function(name) {
            Some(BuiltinScalarFunction::Random) => SqlType::new(SqlTypeKind::Float8),
            Some(BuiltinScalarFunction::ToJson)
            | Some(BuiltinScalarFunction::ArrayToJson)
            | Some(BuiltinScalarFunction::JsonBuildArray)
            | Some(BuiltinScalarFunction::JsonBuildObject)
            | Some(BuiltinScalarFunction::JsonObject) => SqlType::new(SqlTypeKind::Json),
            Some(BuiltinScalarFunction::ToJsonb)
            | Some(BuiltinScalarFunction::JsonbExtractPath)
            | Some(BuiltinScalarFunction::JsonbBuildArray)
            | Some(BuiltinScalarFunction::JsonbBuildObject) => SqlType::new(SqlTypeKind::Jsonb),
            Some(BuiltinScalarFunction::JsonTypeof)
            | Some(BuiltinScalarFunction::JsonExtractPathText)
            | Some(BuiltinScalarFunction::JsonbTypeof)
            | Some(BuiltinScalarFunction::JsonbExtractPathText)
            | Some(BuiltinScalarFunction::Left)
            | Some(BuiltinScalarFunction::Repeat) => SqlType::new(SqlTypeKind::Text),
            Some(BuiltinScalarFunction::JsonArrayLength)
            | Some(BuiltinScalarFunction::JsonbArrayLength) => SqlType::new(SqlTypeKind::Int4),
            Some(BuiltinScalarFunction::JsonExtractPath) => SqlType::new(SqlTypeKind::Json),
            None => SqlType::new(SqlTypeKind::Text),
        },
        SqlExpr::CurrentTimestamp => SqlType::new(SqlTypeKind::Timestamp),
    }
}

fn infer_integer_literal_type(value: &str) -> SqlType {
    if value.parse::<i32>().is_ok() {
        SqlType::new(SqlTypeKind::Int4)
    } else if value.parse::<i64>().is_ok() {
        SqlType::new(SqlTypeKind::Int8)
    } else {
        SqlType::new(SqlTypeKind::Numeric)
    }
}

fn infer_arithmetic_sql_type(expr: &SqlExpr, left: SqlType, right: SqlType) -> SqlType {
    use SqlTypeKind::*;

    let left = left.element_type();
    let right = right.element_type();

    let has_float8 = matches!(left.kind, Float8) || matches!(right.kind, Float8);
    let has_float4 = matches!(left.kind, Float4) || matches!(right.kind, Float4);
    if has_float8 {
        return SqlType::new(Float8);
    }
    if has_float4 {
        return SqlType::new(Float4);
    }
    if matches!(left.kind, Numeric) || matches!(right.kind, Numeric) {
        return SqlType::new(Numeric);
    }

    let widest_int = if matches!(left.kind, Int8) || matches!(right.kind, Int8) {
        Int8
    } else if matches!(left.kind, Int4) || matches!(right.kind, Int4) {
        Int4
    } else {
        Int2
    };

    match expr {
        SqlExpr::Div(_, _) | SqlExpr::Mod(_, _) => SqlType::new(widest_int),
        SqlExpr::Add(_, _) | SqlExpr::Sub(_, _) | SqlExpr::Mul(_, _) => SqlType::new(widest_int),
        _ => SqlType::new(Int4),
    }
}

fn infer_concat_sql_type(expr: &SqlExpr, left: SqlType, right: SqlType) -> SqlType {
    let _ = expr;
    if left.kind == SqlTypeKind::Jsonb && !left.is_array && right.kind == SqlTypeKind::Jsonb && !right.is_array {
        return SqlType::new(SqlTypeKind::Jsonb);
    }
    if left.is_array || right.is_array {
        if let Ok(element_type) = resolve_array_concat_element_type(left, right) {
            return SqlType::array_of(element_type);
        }
    }
    SqlType::new(SqlTypeKind::Text)
}

fn bind_integer_literal(value: &str) -> Result<Value, ParseError> {
    if let Ok(parsed) = value.parse::<i32>() {
        Ok(Value::Int32(parsed))
    } else if let Ok(parsed) = value.parse::<i64>() {
        Ok(Value::Int64(parsed))
    } else if value.chars().all(|ch| ch.is_ascii_digit()) {
        Ok(Value::Numeric(value.into()))
    } else {
        Err(ParseError::InvalidInteger(value.to_string()))
    }
}

fn bind_numeric_literal(value: &str) -> Result<Value, ParseError> {
    value
        .parse::<f64>()
        .map(|_| Value::Numeric(value.into()))
        .map_err(|_| ParseError::InvalidNumeric(value.to_string()))
}

fn ensure_single_column_subquery(plan: &Plan) -> Result<(), ParseError> {
    if plan.columns().len() == 1 {
        Ok(())
    } else {
        Err(ParseError::SubqueryMustReturnOneColumn)
    }
}

fn aggregate_sql_type(func: AggFunc, arg_type: Option<SqlType>) -> SqlType {
    use SqlTypeKind::*;

    match func {
        AggFunc::Count => SqlType::new(Int8),
        AggFunc::Sum => match arg_type.map(|t| t.element_type().kind) {
            Some(Int2 | Int4) => SqlType::new(Int8),
            Some(Int8 | Numeric) => SqlType::new(Numeric),
            Some(Float4) => SqlType::new(Float4),
            Some(Float8) => SqlType::new(Float8),
            Some(kind) => SqlType::new(kind),
            None => SqlType::new(Int8),
        },
        AggFunc::Avg => match arg_type.map(|t| t.element_type().kind) {
            Some(Int2 | Int4 | Int8 | Numeric) => SqlType::new(Numeric),
            Some(Float4 | Float8) => SqlType::new(Float8),
            Some(kind) => SqlType::new(kind),
            None => SqlType::new(Numeric),
        },
        AggFunc::Min | AggFunc::Max => arg_type.unwrap_or(SqlType::new(Text)),
        AggFunc::JsonAgg => SqlType::new(Json),
        AggFunc::JsonbAgg => SqlType::new(Jsonb),
        AggFunc::JsonObjectAgg => SqlType::new(Json),
        AggFunc::JsonbObjectAgg => SqlType::new(Jsonb),
    }
}

fn bind_agg_output_expr(
    expr: &SqlExpr,
    group_by_exprs: &[SqlExpr],
    input_scope: &BoundScope,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    agg_list: &[(AggFunc, Vec<SqlExpr>, bool)],
    n_keys: usize,
) -> Result<Expr, ParseError> {
    for (i, gk) in group_by_exprs.iter().enumerate() {
        if gk == expr {
            return Ok(Expr::Column(i));
        }
    }

    match expr {
        SqlExpr::AggCall {
            func,
            args,
            distinct,
        } => {
            let entry = (*func, args.clone(), *distinct);
            for (i, agg) in agg_list.iter().enumerate() {
                if *agg == entry {
                    return Ok(Expr::Column(n_keys + i));
                }
            }
            Err(ParseError::UnexpectedToken {
                expected: "known aggregate",
                actual: format!("{}(...)", func.name()),
            })
        }
        SqlExpr::Column(name) => {
            let col_index =
                match resolve_column_with_outer(input_scope, outer_scopes, name, grouped_outer)? {
                    ResolvedColumn::Local(index) => index,
                    ResolvedColumn::Outer { depth, index } => {
                        return Ok(Expr::OuterColumn { depth, index });
                    }
                };
            for (i, gk) in group_by_exprs.iter().enumerate() {
                if let SqlExpr::Column(gk_name) = gk {
                    if let Ok(gk_index) = resolve_column(input_scope, gk_name) {
                        if gk_index == col_index {
                            return Ok(Expr::Column(i));
                        }
                    }
                }
            }
            Err(ParseError::UngroupedColumn(name.clone()))
        }
        SqlExpr::Const(v) => Ok(Expr::Const(v.clone())),
        SqlExpr::IntegerLiteral(value) => Ok(Expr::Const(bind_integer_literal(value)?)),
        SqlExpr::NumericLiteral(value) => Ok(Expr::Const(bind_numeric_literal(value)?)),
        SqlExpr::Add(l, r) => Ok(Expr::Add(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Sub(l, r) => Ok(Expr::Sub(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Mul(l, r) => Ok(Expr::Mul(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Div(l, r) => Ok(Expr::Div(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Mod(l, r) => Ok(Expr::Mod(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Concat(l, r) => {
            let left_expr = bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?;
            let right_expr = bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?;
            let left_type = infer_sql_expr_type(l, input_scope, catalog, outer_scopes, grouped_outer);
            let right_type =
                infer_sql_expr_type(r, input_scope, catalog, outer_scopes, grouped_outer);
            bind_concat_operands(l, left_type, left_expr, r, right_type, right_expr)
        }
        SqlExpr::UnaryPlus(inner) => Ok(Expr::UnaryPlus(Box::new(bind_agg_output_expr(
            inner,
            group_by_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        )?))),
        SqlExpr::Negate(inner) => Ok(Expr::Negate(Box::new(bind_agg_output_expr(
            inner,
            group_by_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        )?))),
        SqlExpr::Cast(inner, ty) => {
            let bound_inner = if let SqlExpr::ArrayLiteral(elements) = inner.as_ref() {
                Expr::ArrayLiteral {
                    elements: elements
                        .iter()
                        .map(|element| {
                            bind_agg_output_expr(
                                element,
                                group_by_exprs,
                                input_scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                                agg_list,
                                n_keys,
                            )
                        })
                        .collect::<Result<_, _>>()?,
                    array_type: *ty,
                }
            } else {
                bind_agg_output_expr(
                    inner,
                    group_by_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?
            };
            Ok(Expr::Cast(Box::new(bound_inner), *ty))
        }
        SqlExpr::Eq(l, r) => Ok(Expr::Eq(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::NotEq(l, r) => Ok(Expr::NotEq(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Lt(l, r) => Ok(Expr::Lt(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::LtEq(l, r) => Ok(Expr::LtEq(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Gt(l, r) => Ok(Expr::Gt(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::GtEq(l, r) => Ok(Expr::GtEq(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::RegexMatch(l, r) => Ok(Expr::RegexMatch(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::And(l, r) => Ok(Expr::And(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Or(l, r) => Ok(Expr::Or(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::Not(inner) => Ok(Expr::Not(Box::new(bind_agg_output_expr(
            inner,
            group_by_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        )?))),
        SqlExpr::IsNull(inner) => Ok(Expr::IsNull(Box::new(bind_agg_output_expr(
            inner,
            group_by_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        )?))),
        SqlExpr::IsNotNull(inner) => Ok(Expr::IsNotNull(Box::new(bind_agg_output_expr(
            inner,
            group_by_exprs,
            input_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            agg_list,
            n_keys,
        )?))),
        SqlExpr::IsDistinctFrom(l, r) => Ok(Expr::IsDistinctFrom(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::IsNotDistinctFrom(l, r) => Ok(Expr::IsNotDistinctFrom(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::ArrayLiteral(elements) => Ok(Expr::ArrayLiteral {
            elements: elements
                .iter()
                .map(|element| {
                    bind_agg_output_expr(
                        element,
                        group_by_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )
                })
                .collect::<Result<_, _>>()?,
            array_type: infer_array_literal_type(
                elements,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
            )?,
        }),
        SqlExpr::ArrayOverlap(l, r) => Ok(Expr::ArrayOverlap(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonGet(l, r) => Ok(Expr::JsonGet(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonGetText(l, r) => Ok(Expr::JsonGetText(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonPath(l, r) => Ok(Expr::JsonPath(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonPathText(l, r) => Ok(Expr::JsonPathText(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonbContains(l, r) => Ok(Expr::JsonbContains(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonbContained(l, r) => Ok(Expr::JsonbContained(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonbExists(l, r) => Ok(Expr::JsonbExists(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonbExistsAny(l, r) => Ok(Expr::JsonbExistsAny(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::JsonbExistsAll(l, r) => Ok(Expr::JsonbExistsAll(
            Box::new(bind_agg_output_expr(
                l,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
            Box::new(bind_agg_output_expr(
                r,
                group_by_exprs,
                input_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                agg_list,
                n_keys,
            )?),
        )),
        SqlExpr::ScalarSubquery(select) => {
            let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
            child_outer.push(input_scope.clone());
            child_outer.extend_from_slice(outer_scopes);
            let plan = build_plan_with_outer(
                select,
                catalog,
                &child_outer,
                Some(GroupedOuterScope {
                    scope: input_scope.clone(),
                    group_by_exprs: group_by_exprs.to_vec(),
                }),
            )?;
            ensure_single_column_subquery(&plan)?;
            Ok(Expr::ScalarSubquery(Box::new(plan)))
        }
        SqlExpr::Exists(select) => {
            let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
            child_outer.push(input_scope.clone());
            child_outer.extend_from_slice(outer_scopes);
            Ok(Expr::ExistsSubquery(Box::new(build_plan_with_outer(
                select,
                catalog,
                &child_outer,
                Some(GroupedOuterScope {
                    scope: input_scope.clone(),
                    group_by_exprs: group_by_exprs.to_vec(),
                }),
            )?)))
        }
        SqlExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
            child_outer.push(input_scope.clone());
            child_outer.extend_from_slice(outer_scopes);
            let subquery_plan = build_plan_with_outer(
                subquery,
                catalog,
                &child_outer,
                Some(GroupedOuterScope {
                    scope: input_scope.clone(),
                    group_by_exprs: group_by_exprs.to_vec(),
                }),
            )?;
            ensure_single_column_subquery(&subquery_plan)?;
            let any = Expr::AnySubquery {
                left: Box::new(bind_agg_output_expr(
                    expr,
                    group_by_exprs,
                    input_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    agg_list,
                    n_keys,
                )?),
                op: SubqueryComparisonOp::Eq,
                subquery: Box::new(subquery_plan),
            };
            if *negated {
                Ok(Expr::Not(Box::new(any)))
            } else {
                Ok(any)
            }
        }
        SqlExpr::QuantifiedSubquery {
            left,
            op,
            is_all,
            subquery,
        } => {
            let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
            child_outer.push(input_scope.clone());
            child_outer.extend_from_slice(outer_scopes);
            let subquery_plan = build_plan_with_outer(
                subquery,
                catalog,
                &child_outer,
                Some(GroupedOuterScope {
                    scope: input_scope.clone(),
                    group_by_exprs: group_by_exprs.to_vec(),
                }),
            )?;
            ensure_single_column_subquery(&subquery_plan)?;
            if *is_all {
                Ok(Expr::AllSubquery {
                    left: Box::new(bind_agg_output_expr(
                        left,
                        group_by_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )?),
                    op: *op,
                    subquery: Box::new(subquery_plan),
                })
            } else {
                Ok(Expr::AnySubquery {
                    left: Box::new(bind_agg_output_expr(
                        left,
                        group_by_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )?),
                    op: *op,
                    subquery: Box::new(subquery_plan),
                })
            }
        }
        SqlExpr::QuantifiedArray {
            left,
            op,
            is_all,
            array,
        } => {
            if *is_all {
                Ok(Expr::AllArray {
                    left: Box::new(bind_agg_output_expr(
                        left,
                        group_by_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )?),
                    op: *op,
                    right: Box::new(bind_agg_output_expr(
                        array,
                        group_by_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )?),
                })
            } else {
                Ok(Expr::AnyArray {
                    left: Box::new(bind_agg_output_expr(
                        left,
                        group_by_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )?),
                    op: *op,
                    right: Box::new(bind_agg_output_expr(
                        array,
                        group_by_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )?),
                })
            }
        }
        SqlExpr::Random => Ok(Expr::Random),
        SqlExpr::FuncCall { name, args } => {
            let func =
                resolve_scalar_function(name).ok_or_else(|| ParseError::UnexpectedToken {
                    expected: "supported builtin function",
                    actual: name.clone(),
                })?;
            validate_scalar_function_arity(func, args)?;
            let bound_args = args
                .iter()
                .map(|arg| {
                    bind_agg_output_expr(
                        arg,
                        group_by_exprs,
                        input_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        agg_list,
                        n_keys,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            match func {
                BuiltinScalarFunction::Left | BuiltinScalarFunction::Repeat => {
                    let left_type =
                        infer_sql_expr_type(&args[0], input_scope, catalog, outer_scopes, grouped_outer);
                    let right_type =
                        infer_sql_expr_type(&args[1], input_scope, catalog, outer_scopes, grouped_outer);
                    if !should_use_text_concat(&args[0], left_type, &args[0], left_type) {
                        return Err(ParseError::UnexpectedToken {
                            expected: "text argument",
                            actual: format!("{func:?}({})", sql_type_name(left_type)),
                        });
                    }
                    if !is_numeric_family(right_type) {
                        return Err(ParseError::UnexpectedToken {
                            expected: "integer argument",
                            actual: format!("{func:?}({})", sql_type_name(right_type)),
                        });
                    }
                    Ok(Expr::FuncCall {
                        func,
                        args: vec![
                            coerce_bound_expr(
                                bound_args[0].clone(),
                                left_type,
                                SqlType::new(SqlTypeKind::Text),
                            ),
                            coerce_bound_expr(
                                bound_args[1].clone(),
                                right_type,
                                SqlType::new(SqlTypeKind::Int4),
                            ),
                        ],
                    })
                }
                _ => Ok(Expr::FuncCall {
                    func,
                    args: bound_args,
                }),
            }
        }
        SqlExpr::CurrentTimestamp => Ok(Expr::CurrentTimestamp),
    }
}

fn infer_array_literal_type(
    elements: &[SqlExpr],
    scope: &BoundScope,
    catalog: &Catalog,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<SqlType, ParseError> {
    for element in elements {
        if matches!(element, SqlExpr::Const(Value::Null)) {
            continue;
        }
        return Ok(SqlType::array_of(
            infer_sql_expr_type(element, scope, catalog, outer_scopes, grouped_outer)
                .element_type(),
        ));
    }
    Err(ParseError::UnexpectedToken {
        expected: "ARRAY[...] with a typed element or explicit cast",
        actual: "ARRAY[]".into(),
    })
}
