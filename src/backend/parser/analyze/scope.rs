use super::*;

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

#[derive(Debug, Clone)]
pub(crate) struct GroupedOuterScope {
    pub(crate) scope: BoundScope,
    pub(crate) group_by_exprs: Vec<SqlExpr>,
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

pub(super) fn resolve_column_with_outer(
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
            if depth == 0
                && let Some(grouped) = grouped_outer
                && scopes_match(&grouped.scope, outer_scope)
                && !outer_column_is_grouped(index, &grouped.scope, &grouped.group_by_exprs)
            {
                return Err(ParseError::UngroupedColumn(name.to_string()));
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

pub(super) fn bind_from_item(
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
                        JsonTableFunction::ObjectKeys => vec![QueryColumn::text("json_object_keys")],
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

pub(super) fn scope_for_relation(relation_name: Option<&str>, desc: &RelationDesc) -> BoundScope {
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

pub(super) fn combine_scopes(left: &BoundScope, right: &BoundScope) -> BoundScope {
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
