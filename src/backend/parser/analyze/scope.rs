use super::*;
use crate::backend::storage::smgr::RelFileLocator;
use crate::include::nodes::plannodes::JoinType;

#[derive(Debug, Clone)]
pub(crate) struct BoundScope {
    pub(crate) desc: RelationDesc,
    pub(crate) columns: Vec<ScopeColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScopeColumn {
    pub(crate) output_name: String,
    pub(crate) relation_names: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct GroupedOuterScope {
    pub(crate) scope: BoundScope,
    pub(crate) group_by_exprs: Vec<SqlExpr>,
}

#[derive(Debug, Clone)]
pub(crate) struct BoundCte {
    pub(crate) name: String,
    pub(crate) plan: Plan,
    pub(crate) desc: RelationDesc,
}

#[derive(Debug, Clone)]
pub struct BoundRelation {
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub namespace_oid: u32,
    pub relpersistence: char,
    pub relkind: char,
    pub desc: RelationDesc,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum ResolvedColumn {
    Local(usize),
    Outer { depth: usize, index: usize },
}

pub(super) fn empty_scope() -> BoundScope {
    BoundScope {
        desc: RelationDesc {
            columns: Vec::new(),
        },
        columns: Vec::new(),
    }
}

pub(super) fn bind_values_rows(
    rows: &[Vec<SqlExpr>],
    column_names: Option<&[String]>,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<(Plan, BoundScope), ParseError> {
    let width = rows.first().map(Vec::len).unwrap_or(0);
    for row in rows {
        if row.len() != width {
            return Err(ParseError::UnexpectedToken {
                expected: "VALUES rows with consistent column counts",
                actual: format!("VALUES row has {} columns, expected {width}", row.len()),
            });
        }
    }

    if let Some(column_names) = column_names
        && column_names.len() != width
    {
        return Err(ParseError::UnexpectedToken {
            expected: "VALUES column alias count matching VALUES width",
            actual: format!(
                "VALUES has {width} columns but {} column aliases were specified",
                column_names.len()
            ),
        });
    }

    let empty = empty_scope();
    let mut column_types = Vec::with_capacity(width);
    for col_idx in 0..width {
        let mut common = None;
        let mut common_expr: Option<&SqlExpr> = None;
        for row in rows {
            let inferred = infer_sql_expr_type_with_ctes(
                &row[col_idx],
                &empty,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            common = Some(match common {
                None => {
                    common_expr = Some(&row[col_idx]);
                    inferred.element_type()
                }
                Some(existing) => {
                    let existing = coerce_unknown_string_literal_type(
                        common_expr.expect("common expr"),
                        existing,
                        inferred,
                    );
                    let adjusted =
                        coerce_unknown_string_literal_type(&row[col_idx], inferred, existing);
                    let resolved =
                        resolve_common_scalar_type(existing, adjusted).ok_or_else(|| {
                            ParseError::UnexpectedToken {
                                expected: "VALUES columns with a common type",
                                actual: format!(
                                    "VALUES column {} cannot reconcile {} and {}",
                                    col_idx + 1,
                                    sql_type_name(existing),
                                    sql_type_name(adjusted)
                                ),
                            }
                        })?;
                    common_expr = Some(&row[col_idx]);
                    resolved
                }
            });
        }
        column_types.push(common.unwrap_or(SqlType::new(SqlTypeKind::Text)));
    }

    let bound_rows = rows
        .iter()
        .map(|row| {
            row.iter()
                .zip(column_types.iter())
                .map(|(expr, ty)| {
                    let from = infer_sql_expr_type_with_ctes(
                        expr,
                        &empty,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                    Ok(coerce_bound_expr(
                        bind_expr_with_outer_and_ctes(
                            expr,
                            &empty,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        )?,
                        from,
                        *ty,
                    ))
                })
                .collect::<Result<Vec<_>, ParseError>>()
        })
        .collect::<Result<Vec<_>, _>>()?;

    let output_columns = column_types
        .iter()
        .enumerate()
        .map(|(idx, ty)| QueryColumn {
            name: column_names
                .and_then(|names| names.get(idx))
                .cloned()
                .unwrap_or_else(|| format!("column{}", idx + 1)),
            sql_type: *ty,
        })
        .collect::<Vec<_>>();
    let desc = RelationDesc {
        columns: output_columns
            .iter()
            .map(|col| column_desc(col.name.clone(), col.sql_type, true))
            .collect(),
    };
    Ok((
        Plan::Values {
            rows: bound_rows,
            output_columns,
        },
        scope_for_relation(None, &desc),
    ))
}

pub(super) fn resolve_column(scope: &BoundScope, name: &str) -> Result<usize, ParseError> {
    if name == "*" {
        return Err(ParseError::UnexpectedToken {
            expected: "named column",
            actual: "*".into(),
        });
    }
    if let Some((relation, column_name)) = name.rsplit_once('.') {
        let mut matches = scope.columns.iter().enumerate().filter(|(_, column)| {
            column
                .relation_names
                .iter()
                .any(|visible_relation| visible_relation.eq_ignore_ascii_case(relation))
                && column.output_name.eq_ignore_ascii_case(column_name)
        });
        let first = matches
            .next()
            .ok_or_else(|| ParseError::UnknownColumn(name.to_string()))?;
        if matches.next().is_some() {
            return Err(ParseError::AmbiguousColumn(name.to_string()));
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
        return Err(ParseError::AmbiguousColumn(name.to_string()));
    }
    Ok(first.0)
}

pub(super) fn resolve_column_with_outer(
    scope: &BoundScope,
    outer_scopes: &[BoundScope],
    name: &str,
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<ResolvedColumn, ParseError> {
    match resolve_column(scope, name) {
        Ok(index) => return Ok(ResolvedColumn::Local(index)),
        Err(ParseError::AmbiguousColumn(name)) => return Err(ParseError::AmbiguousColumn(name)),
        Err(ParseError::UnknownColumn(_)) => {}
        Err(other) => return Err(other),
    }

    for (depth, outer_scope) in outer_scopes.iter().enumerate() {
        match resolve_column(outer_scope, name) {
            Ok(index) => {
                if depth == 0
                    && let Some(grouped) = grouped_outer
                    && scopes_match(&grouped.scope, outer_scope)
                    && !outer_column_is_grouped(index, &grouped.scope, &grouped.group_by_exprs)
                {
                    let column = &outer_scope.columns[index];
                    let display_name = column
                        .relation_names
                        .first()
                        .map(|relation_name| format!("{relation_name}.{}", column.output_name))
                        .unwrap_or_else(|| column.output_name.clone());
                    return Err(ParseError::UngroupedColumn {
                        display_name,
                        token: name.to_string(),
                        clause: UngroupedColumnClause::Other,
                    });
                }
                return Ok(ResolvedColumn::Outer { depth, index });
            }
            Err(ParseError::AmbiguousColumn(name)) => {
                return Err(ParseError::AmbiguousColumn(name));
            }
            Err(ParseError::UnknownColumn(_)) => {}
            Err(other) => return Err(other),
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

pub(super) fn bind_from_item_with_ctes(
    stmt: &FromItem,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<(Plan, BoundScope), ParseError> {
    match stmt {
        FromItem::Table { name } => {
            if let Some(cte) = ctes.iter().find(|cte| cte.name.eq_ignore_ascii_case(name)) {
                return Ok((cte.plan.clone(), scope_for_relation(Some(name), &cte.desc)));
            }
            let entry = lookup_relation(catalog, name)?;
            let desc = entry.desc.clone();
            Ok((
                Plan::SeqScan {
                    rel: entry.rel,
                    relation_oid: entry.relation_oid,
                    desc: desc.clone(),
                },
                scope_for_relation(Some(name), &desc),
            ))
        }
        FromItem::Values { rows } => {
            bind_values_rows(rows, None, catalog, outer_scopes, grouped_outer, ctes)
        }
        FromItem::FunctionCall { name, args } => {
            let args = lower_named_table_function_args(name, args)?;
            match name.as_str() {
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
                let start_type = infer_sql_expr_type(
                    &args[0],
                    &empty_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                );
                let stop_type = infer_sql_expr_type(
                    &args[1],
                    &empty_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                );
                let common = resolve_numeric_binary_type("+", start_type, stop_type)?;
                if !matches!(
                    common.kind,
                    SqlTypeKind::Int4 | SqlTypeKind::Int8 | SqlTypeKind::Numeric
                ) {
                    return Err(ParseError::UnexpectedToken {
                        expected: "generate_series integer or numeric arguments",
                        actual: sql_type_name(common),
                    });
                }
                let step = if args.len() == 3 {
                    let step_expr = bind_expr_with_outer(
                        &args[2],
                        &empty_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                    )?;
                    let step_type = infer_sql_expr_type(
                        &args[2],
                        &empty_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                    );
                    coerce_bound_expr(step_expr, step_type, common)
                } else {
                    match common.kind {
                        SqlTypeKind::Int8 => Expr::Const(Value::Int64(1)),
                        SqlTypeKind::Numeric => Expr::Const(Value::Numeric(
                            crate::include::nodes::datum::NumericValue::from_i64(1),
                        )),
                        _ => Expr::Const(Value::Int32(1)),
                    }
                };
                let desc = RelationDesc {
                    columns: vec![column_desc("generate_series", common, false)],
                };
                let scope = scope_for_relation(Some(name), &desc);
                Ok((
                    Plan::FunctionScan {
                        call: SetReturningCall::GenerateSeries {
                            start: coerce_bound_expr(start, start_type, common),
                            stop: coerce_bound_expr(stop, stop_type, common),
                            step,
                            output: QueryColumn {
                                name: "generate_series".to_string(),
                                sql_type: common,
                            },
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
                    Plan::FunctionScan {
                        call: SetReturningCall::Unnest {
                            args: bound_args,
                            output_columns,
                        },
                    },
                    scope,
                ))
            }
            "pg_input_error_info" => {
                if args.len() != 2 {
                    return Err(ParseError::UnexpectedToken {
                        expected: "pg_input_error_info(text, text)",
                        actual: format!("pg_input_error_info with {} arguments", args.len()),
                    });
                }
                let empty_scope = empty_scope();
                let text_type = SqlType::new(SqlTypeKind::Text);
                let left_type = infer_sql_expr_type(
                    &args[0],
                    &empty_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                );
                let right_type = infer_sql_expr_type(
                    &args[1],
                    &empty_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                );
                let left = coerce_bound_expr(
                    bind_expr_with_outer(
                        &args[0],
                        &empty_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                    )?,
                    left_type,
                    text_type,
                );
                let right = coerce_bound_expr(
                    bind_expr_with_outer(
                        &args[1],
                        &empty_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                    )?,
                    right_type,
                    text_type,
                );
                let output_columns = vec![
                    QueryColumn::text("message"),
                    QueryColumn::text("detail"),
                    QueryColumn::text("hint"),
                    QueryColumn::text("sql_error_code"),
                ];
                let desc = RelationDesc {
                    columns: output_columns
                        .iter()
                        .map(|col| column_desc(col.name.clone(), col.sql_type, true))
                        .collect(),
                };
                let scope = scope_for_relation(Some(name), &desc);
                Ok((
                    Plan::Projection {
                        input: Box::new(Plan::Result),
                        targets: vec![
                            TargetEntry {
                                name: "message".into(),
                                expr: Expr::FuncCall {
                                    func: BuiltinScalarFunction::PgInputErrorMessage,
                                    args: vec![left.clone(), right.clone()],
                                },
                                sql_type: text_type,
                            },
                            TargetEntry {
                                name: "detail".into(),
                                expr: Expr::FuncCall {
                                    func: BuiltinScalarFunction::PgInputErrorDetail,
                                    args: vec![left.clone(), right.clone()],
                                },
                                sql_type: text_type,
                            },
                            TargetEntry {
                                name: "hint".into(),
                                expr: Expr::FuncCall {
                                    func: BuiltinScalarFunction::PgInputErrorHint,
                                    args: vec![left.clone(), right.clone()],
                                },
                                sql_type: text_type,
                            },
                            TargetEntry {
                                name: "sql_error_code".into(),
                                expr: Expr::FuncCall {
                                    func: BuiltinScalarFunction::PgInputErrorSqlState,
                                    args: vec![left, right],
                                },
                                sql_type: text_type,
                            },
                        ],
                    },
                    scope,
                ))
            }
            other => {
                if let Some(kind) = resolve_json_table_function(other) {
                    let empty_scope = empty_scope();
                    let bound_args = args
                        .iter()
                        .map(|arg| {
                            bind_expr_with_outer(
                                arg,
                                &empty_scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()?;
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
                        JsonTableFunction::JsonbPathQuery => vec![QueryColumn {
                            name: "jsonb_path_query".into(),
                            sql_type: SqlType::new(SqlTypeKind::Jsonb),
                        }],
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
                        Plan::FunctionScan {
                            call: SetReturningCall::JsonTableFunction {
                                kind,
                                args: bound_args,
                                output_columns,
                            },
                        },
                        scope,
                    ))
                } else if let Some(kind) = resolve_regex_table_function(other) {
                    let empty_scope = empty_scope();
                    let bound_args = args
                        .iter()
                        .map(|arg| {
                            bind_expr_with_outer(
                                arg,
                                &empty_scope,
                                catalog,
                                outer_scopes,
                                grouped_outer,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let output_columns = match kind {
                        crate::include::nodes::plannodes::RegexTableFunction::Matches => vec![QueryColumn {
                            name: "regexp_matches".into(),
                            sql_type: SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                        }],
                        crate::include::nodes::plannodes::RegexTableFunction::SplitToTable => {
                            vec![QueryColumn::text("regexp_split_to_table")]
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
                        Plan::FunctionScan {
                            call: SetReturningCall::RegexTableFunction {
                                kind,
                                args: bound_args,
                                output_columns,
                            },
                        },
                        scope,
                    ))
                } else {
                    Err(ParseError::UnknownTable(other.to_string()))
                }
            }
        }
        },
        FromItem::DerivedTable(select) => {
            let plan = build_plan_with_outer(select, catalog, &[], None, ctes)?;
            let desc = synthetic_desc_from_plan(&plan);
            Ok((plan, scope_for_relation(None, &desc)))
        }
        FromItem::Join {
            left,
            right,
            kind,
            constraint,
        } => {
            let (left_plan, left_scope) =
                bind_from_item_with_ctes(left, catalog, outer_scopes, grouped_outer, ctes)?;
            let (right_plan, right_scope) =
                bind_from_item_with_ctes(right, catalog, outer_scopes, grouped_outer, ctes)?;
            let raw_scope = combine_scopes(&left_scope, &right_scope);
            let (on, projection) = bind_join_constraint_with_ctes(
                kind,
                constraint,
                &left_scope,
                &right_scope,
                &raw_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            let plan = Plan::NestedLoopJoin {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                kind: plan_join_type(*kind),
                on,
            };
            if let Some((targets, scope)) = projection {
                Ok((
                    Plan::Projection {
                        input: Box::new(plan),
                        targets,
                    },
                    scope,
                ))
            } else {
                Ok((plan, raw_scope))
            }
        }
        FromItem::Alias {
            source,
            alias,
            column_aliases,
            preserve_source_names,
        } => {
            let (plan, scope) =
                bind_from_item_with_ctes(source, catalog, outer_scopes, grouped_outer, ctes)?;
            apply_relation_alias(plan, scope, alias, column_aliases, *preserve_source_names)
        }
    }
}

pub(super) fn lookup_relation(
    catalog: &dyn CatalogLookup,
    name: &str,
) -> Result<BoundRelation, ParseError> {
    catalog
        .lookup_relation(name)
        .ok_or_else(|| ParseError::UnknownTable(name.to_string()))
}

pub(super) fn scope_for_relation(relation_name: Option<&str>, desc: &RelationDesc) -> BoundScope {
    BoundScope {
        desc: desc.clone(),
        columns: desc
            .columns
            .iter()
            .map(|column| ScopeColumn {
                output_name: column.name.clone(),
                relation_names: relation_name.into_iter().map(str::to_string).collect(),
            })
            .collect(),
    }
}

pub(super) fn combine_scopes(left: &BoundScope, right: &BoundScope) -> BoundScope {
    let mut desc = left.desc.clone();
    desc.columns.extend(right.desc.columns.clone());
    let mut columns = left.columns.clone();
    columns.extend(right.columns.clone());
    BoundScope { desc, columns }
}

fn plan_join_type(kind: JoinKind) -> JoinType {
    match kind {
        JoinKind::Inner => JoinType::Inner,
        JoinKind::Cross => JoinType::Cross,
        JoinKind::Left => JoinType::Left,
        JoinKind::Right => JoinType::Right,
        JoinKind::Full => JoinType::Full,
    }
}

type JoinProjection = Option<(Vec<TargetEntry>, BoundScope)>;

#[allow(clippy::too_many_arguments)]
fn bind_join_constraint_with_ctes(
    kind: &JoinKind,
    constraint: &JoinConstraint,
    left_scope: &BoundScope,
    right_scope: &BoundScope,
    raw_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<(Expr, JoinProjection), ParseError> {
    match constraint {
        JoinConstraint::None => {
            if !matches!(kind, JoinKind::Cross) {
                return Err(ParseError::UnexpectedToken {
                    expected: "valid join clause",
                    actual: format!("{kind:?}"),
                });
            }
            Ok((Expr::Const(Value::Bool(true)), None))
        }
        JoinConstraint::On(on) => Ok((
            bind_expr_with_outer_and_ctes(
                on,
                raw_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            None,
        )),
        JoinConstraint::Using(columns) => bind_join_using_projection(columns, left_scope, right_scope),
        JoinConstraint::Natural => {
            let columns = natural_join_columns(left_scope, right_scope);
            bind_join_using_projection(&columns, left_scope, right_scope)
        }
    }
}

fn natural_join_columns(left_scope: &BoundScope, right_scope: &BoundScope) -> Vec<String> {
    let mut out = Vec::new();
    for left in &left_scope.columns {
        if right_scope
            .columns
            .iter()
            .any(|right| right.output_name.eq_ignore_ascii_case(&left.output_name))
            && !out
                .iter()
                .any(|name: &String| name.eq_ignore_ascii_case(&left.output_name))
        {
            out.push(left.output_name.clone());
        }
    }
    out
}

fn bind_join_using_projection(
    columns: &[String],
    left_scope: &BoundScope,
    right_scope: &BoundScope,
) -> Result<(Expr, JoinProjection), ParseError> {
    let mut using_pairs = Vec::with_capacity(columns.len());
    for name in columns {
        let left_index = resolve_column(left_scope, name)?;
        let right_index = resolve_column(right_scope, name)?;
        using_pairs.push((name.clone(), left_index, right_index));
    }

    let on = using_pairs.iter().fold(Expr::Const(Value::Bool(true)), |expr, (_, left, right)| {
        let right_index = left_scope.columns.len() + *right;
        let predicate = Expr::Eq(Box::new(Expr::Column(*left)), Box::new(Expr::Column(right_index)));
        match expr {
            Expr::Const(Value::Bool(true)) => predicate,
            other => Expr::And(Box::new(other), Box::new(predicate)),
        }
    });

    let mut targets = Vec::new();
    let mut desc_columns = Vec::new();
    let mut scope_columns = Vec::new();
    let mut used_left = vec![false; left_scope.columns.len()];
    let mut used_right = vec![false; right_scope.columns.len()];

    for (name, left_index, right_index) in &using_pairs {
        used_left[*left_index] = true;
        used_right[*right_index] = true;
        let left_ty = left_scope.desc.columns[*left_index].sql_type;
        let left_expr = Expr::Column(*left_index);
        let right_expr = Expr::Column(left_scope.columns.len() + *right_index);
        targets.push(TargetEntry {
            name: name.clone(),
            expr: Expr::Coalesce(Box::new(left_expr), Box::new(right_expr)),
            sql_type: left_ty,
        });
        desc_columns.push(column_desc(name.clone(), left_ty, true));
        scope_columns.push(ScopeColumn {
            output_name: name.clone(),
            relation_names: vec![],
        });
    }

    for (index, column) in left_scope.columns.iter().enumerate() {
        if used_left[index] {
            continue;
        }
        targets.push(TargetEntry {
            name: column.output_name.clone(),
            expr: Expr::Column(index),
            sql_type: left_scope.desc.columns[index].sql_type,
        });
        desc_columns.push(left_scope.desc.columns[index].clone());
        scope_columns.push(column.clone());
    }

    for (index, column) in right_scope.columns.iter().enumerate() {
        if used_right[index] {
            continue;
        }
        let raw_index = left_scope.columns.len() + index;
        targets.push(TargetEntry {
            name: column.output_name.clone(),
            expr: Expr::Column(raw_index),
            sql_type: right_scope.desc.columns[index].sql_type,
        });
        desc_columns.push(right_scope.desc.columns[index].clone());
        scope_columns.push(column.clone());
    }

    let scope = BoundScope {
        desc: RelationDesc {
            columns: desc_columns,
        },
        columns: scope_columns,
    };
    Ok((on, Some((targets, scope))))
}

fn synthetic_desc_from_plan(plan: &Plan) -> RelationDesc {
    RelationDesc {
        columns: plan
            .column_names()
            .into_iter()
            .zip(plan.columns())
            .map(|(name, col)| column_desc(name, col.sql_type, true))
            .collect(),
    }
}

fn apply_relation_alias(
    mut plan: Plan,
    scope: BoundScope,
    alias: &str,
    column_aliases: &[String],
    preserve_source_names: bool,
) -> Result<(Plan, BoundScope), ParseError> {
    if column_aliases.len() > scope.columns.len() {
        return Err(ParseError::UnexpectedToken {
            expected: "table alias column count to match source columns",
            actual: format!(
                "table \"{alias}\" has {} columns available but {} columns specified",
                scope.columns.len(),
                column_aliases.len(),
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
    }

    if preserve_source_names {
        let alias_only_anonymous = columns.iter().any(|column| column.relation_names.is_empty());
        for column in &mut columns {
            if alias_only_anonymous && !column.relation_names.is_empty() {
                continue;
            }
            if !column
                .relation_names
                .iter()
                .any(|name| name.eq_ignore_ascii_case(alias))
            {
                column.relation_names.push(alias.to_string());
            }
        }
    } else {
        for column in &mut columns {
            column.relation_names = vec![alias.to_string()];
        }
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
