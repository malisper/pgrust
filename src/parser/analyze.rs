use crate::RelFileLocator;
use crate::catalog::column_desc;
use crate::executor::{AggAccum, AggFunc, Expr, Plan, QueryColumn, RelationDesc, TargetEntry, Value};

pub use crate::catalog::{Catalog, CatalogEntry};
use super::parsenodes::*;

#[derive(Debug, Clone)]
pub(crate) struct BoundScope {
    pub(crate) desc: RelationDesc,
    pub(crate) columns: Vec<ScopeColumn>,
}

#[derive(Debug, Clone)]
pub(crate) struct ScopeColumn {
    pub(crate) output_name: String,
    pub(crate) relation_name: Option<String>,
}

fn empty_scope() -> BoundScope {
    BoundScope {
        desc: RelationDesc { columns: Vec::new() },
        columns: Vec::new(),
    }
}

pub fn create_relation_desc(stmt: &CreateTableStatement) -> RelationDesc {
    RelationDesc {
        columns: stmt
            .columns
            .iter()
            .map(|column| {
                column_desc(
                    column.name.clone(),
                    column.ty,
                    column.nullable,
                )
            })
            .collect(),
    }
}

pub fn bind_create_table(
    stmt: &CreateTableStatement,
    catalog: &mut Catalog,
) -> Result<CatalogEntry, ParseError> {
    catalog
        .create_table(stmt.table_name.clone(), create_relation_desc(stmt))
        .map_err(|err| match err {
            crate::catalog::CatalogError::TableAlreadyExists(name) => {
                ParseError::TableAlreadyExists(name)
            }
            crate::catalog::CatalogError::UnknownTable(name) => ParseError::TableDoesNotExist(name),
            crate::catalog::CatalogError::UnknownType(name) => ParseError::UnsupportedType(name),
            crate::catalog::CatalogError::Io(_)
            | crate::catalog::CatalogError::Corrupt(_) => ParseError::UnexpectedToken {
                expected: "valid catalog state",
                actual: "catalog error".into(),
            },
        })
}

pub fn build_plan(stmt: &SelectStatement, catalog: &Catalog) -> Result<Plan, ParseError> {
    if stmt.targets.is_empty() && stmt.from.is_none() {
        return Err(ParseError::EmptySelectList);
    }

    let (base, scope) = if let Some(from) = &stmt.from {
        bind_from_item(from, catalog)?
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
            predicate: bind_expr(predicate, &scope)?,
        }
    } else {
        base
    };

    let needs_agg = !stmt.group_by.is_empty()
        || targets_contain_agg(&stmt.targets)
        || stmt.having.is_some();

    if needs_agg {
        let mut aggs: Vec<(AggFunc, Option<SqlExpr>, bool)> = Vec::new();
        for target in &stmt.targets {
            collect_aggs(&target.expr, &mut aggs);
        }
        if let Some(having) = &stmt.having {
            collect_aggs(having, &mut aggs);
        }

        let group_keys: Vec<Expr> = stmt
            .group_by
            .iter()
            .map(|e| bind_expr(e, &scope))
            .collect::<Result<_, _>>()?;

        let accumulators: Vec<AggAccum> = aggs
            .iter()
            .map(|(func, arg, distinct)| {
                Ok(AggAccum {
                    func: *func,
                    arg: arg.as_ref().map(|e| bind_expr(e, &scope)).transpose()?,
                    distinct: *distinct,
                })
            })
            .collect::<Result<_, _>>()?;

        let n_keys = group_keys.len();
        let mut output_columns: Vec<QueryColumn> = Vec::new();
        for gk in &stmt.group_by {
            output_columns.push(QueryColumn {
                name: sql_expr_name(gk),
                sql_type: infer_sql_expr_type(gk, &scope),
            });
        }
        for (func, _, _) in &aggs {
            output_columns.push(QueryColumn {
                name: func.name().to_string(),
                sql_type: aggregate_sql_type(*func),
            });
        }

        let having = stmt
            .having
            .as_ref()
            .map(|e| bind_agg_output_expr(e, &stmt.group_by, &scope, &aggs, n_keys))
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
                        Ok(crate::executor::OrderByEntry {
                            expr: bind_agg_output_expr(
                                &item.expr,
                                &stmt.group_by,
                                &scope,
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
                            &aggs,
                            n_keys,
                        )?,
                        sql_type: infer_sql_expr_type(&item.expr, &scope),
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
                        Ok(crate::executor::OrderByEntry {
                            expr: bind_expr(&item.expr, &scope)?,
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

        let targets = bind_select_targets(&stmt.targets, &scope)?;

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
                expr: bind_expr(&item.expr, scope)?,
                sql_type: infer_sql_expr_type(&item.expr, scope),
            })
        })
        .collect()
}

pub(crate) fn bind_expr(expr: &SqlExpr, scope: &BoundScope) -> Result<Expr, ParseError> {
    Ok(match expr {
        SqlExpr::Column(name) => Expr::Column(resolve_column(scope, name)?),
        SqlExpr::Const(value) => Expr::Const(value.clone()),
        SqlExpr::Add(left, right) => Expr::Add(
            Box::new(bind_expr(left, scope)?),
            Box::new(bind_expr(right, scope)?),
        ),
        SqlExpr::Negate(inner) => Expr::Negate(Box::new(bind_expr(inner, scope)?)),
        SqlExpr::Cast(inner, ty) => Expr::Cast(
            Box::new(bind_expr(inner, scope)?),
            *ty,
        ),
        SqlExpr::Eq(left, right) => Expr::Eq(
            Box::new(bind_expr(left, scope)?),
            Box::new(bind_expr(right, scope)?),
        ),
        SqlExpr::Lt(left, right) => Expr::Lt(
            Box::new(bind_expr(left, scope)?),
            Box::new(bind_expr(right, scope)?),
        ),
        SqlExpr::Gt(left, right) => Expr::Gt(
            Box::new(bind_expr(left, scope)?),
            Box::new(bind_expr(right, scope)?),
        ),
        SqlExpr::RegexMatch(left, right) => Expr::RegexMatch(
            Box::new(bind_expr(left, scope)?),
            Box::new(bind_expr(right, scope)?),
        ),
        SqlExpr::And(left, right) => Expr::And(
            Box::new(bind_expr(left, scope)?),
            Box::new(bind_expr(right, scope)?),
        ),
        SqlExpr::Or(left, right) => Expr::Or(
            Box::new(bind_expr(left, scope)?),
            Box::new(bind_expr(right, scope)?),
        ),
        SqlExpr::Not(inner) => Expr::Not(Box::new(bind_expr(inner, scope)?)),
        SqlExpr::IsNull(inner) => Expr::IsNull(Box::new(bind_expr(inner, scope)?)),
        SqlExpr::IsNotNull(inner) => Expr::IsNotNull(Box::new(bind_expr(inner, scope)?)),
        SqlExpr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(bind_expr(left, scope)?),
            Box::new(bind_expr(right, scope)?),
        ),
        SqlExpr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(bind_expr(left, scope)?),
            Box::new(bind_expr(right, scope)?),
        ),
        SqlExpr::AggCall { .. } => {
            return Err(ParseError::UnexpectedToken {
                expected: "non-aggregate expression",
                actual: "aggregate function".into(),
            })
        }
        SqlExpr::Random => Expr::Random,
        SqlExpr::CurrentTimestamp => Expr::CurrentTimestamp,
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
                    .map(|expr| bind_expr(expr, &scope))
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
                    expr: bind_expr(&assignment.expr, &scope)?,
                })
            })
            .collect::<Result<Vec<_>, ParseError>>()?,
        predicate: stmt
            .where_clause
            .as_ref()
            .map(|expr| bind_expr(expr, &scope))
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
            .map(|expr| bind_expr(expr, &scope))
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
        let mut matches = scope
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| {
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

fn bind_from_item(stmt: &FromItem, catalog: &Catalog) -> Result<(Plan, BoundScope), ParseError> {
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
                let start = bind_expr(&args[0], &empty_scope)?;
                let stop = bind_expr(&args[1], &empty_scope)?;
                let step = if args.len() == 3 {
                    bind_expr(&args[2], &empty_scope)?
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
            other => Err(ParseError::UnknownTable(other.to_string())),
        },
        FromItem::DerivedTable(select) => {
            let plan = build_plan(select, catalog)?;
            let desc = synthetic_desc_from_plan(&plan);
            Ok((plan, scope_for_relation(None, &desc)))
        }
        FromItem::Join {
            left,
            right,
            kind,
            on,
        } => {
            let (left_plan, left_scope) = bind_from_item(left, catalog)?;
            let (right_plan, right_scope) = bind_from_item(right, catalog)?;
            let scope = combine_scopes(&left_scope, &right_scope);
            let on = match (kind, on) {
                (JoinKind::Inner, Some(on)) => bind_expr(on, &scope)?,
                (JoinKind::Cross, None) => Expr::Const(Value::Bool(true)),
                _ => {
                    return Err(ParseError::UnexpectedToken {
                        expected: "valid join clause",
                        actual: format!("{stmt:?}"),
                    })
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
            let (plan, scope) = bind_from_item(source, catalog)?;
            apply_relation_alias(plan, scope, alias, column_aliases)
        }
    }
}

fn scope_for_relation(relation_name: Option<&str>, desc: &RelationDesc) -> BoundScope {
    BoundScope {
        desc: desc.clone(),
        columns: desc.columns.iter().map(|column| ScopeColumn {
            output_name: column.name.clone(),
            relation_name: relation_name.map(str::to_string),
        }).collect(),
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
            actual: format!("{} aliases for {} columns", column_aliases.len(), scope.columns.len()),
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
        SqlExpr::Column(_) | SqlExpr::Const(_) | SqlExpr::Random | SqlExpr::CurrentTimestamp => false,
        SqlExpr::Cast(inner, _) => expr_contains_agg(inner),
        SqlExpr::Add(l, r) | SqlExpr::Eq(l, r) | SqlExpr::Lt(l, r) | SqlExpr::Gt(l, r)
        | SqlExpr::RegexMatch(l, r)
        | SqlExpr::And(l, r) | SqlExpr::Or(l, r) | SqlExpr::IsDistinctFrom(l, r)
        | SqlExpr::IsNotDistinctFrom(l, r) => expr_contains_agg(l) || expr_contains_agg(r),
        SqlExpr::Negate(inner) | SqlExpr::Not(inner) | SqlExpr::IsNull(inner) | SqlExpr::IsNotNull(inner) => expr_contains_agg(inner),
    }
}

fn targets_contain_agg(targets: &[SelectItem]) -> bool {
    targets.iter().any(|t| expr_contains_agg(&t.expr))
}

fn collect_aggs(expr: &SqlExpr, aggs: &mut Vec<(AggFunc, Option<SqlExpr>, bool)>) {
    match expr {
        SqlExpr::AggCall { func, arg, distinct } => {
            let entry = (*func, arg.as_deref().cloned(), *distinct);
            if !aggs.contains(&entry) { aggs.push(entry); }
        }
        SqlExpr::Column(_) | SqlExpr::Const(_) | SqlExpr::Random | SqlExpr::CurrentTimestamp => {}
        SqlExpr::Cast(inner, _) => collect_aggs(inner, aggs),
        SqlExpr::Add(l, r) | SqlExpr::Eq(l, r) | SqlExpr::Lt(l, r) | SqlExpr::Gt(l, r)
        | SqlExpr::RegexMatch(l, r)
        | SqlExpr::And(l, r) | SqlExpr::Or(l, r) | SqlExpr::IsDistinctFrom(l, r)
        | SqlExpr::IsNotDistinctFrom(l, r) => { collect_aggs(l, aggs); collect_aggs(r, aggs); }
        SqlExpr::Negate(inner) | SqlExpr::Not(inner) | SqlExpr::IsNull(inner) | SqlExpr::IsNotNull(inner) => collect_aggs(inner, aggs),
    }
}

fn sql_expr_name(expr: &SqlExpr) -> String {
    match expr {
        SqlExpr::Column(name) => name.clone(),
        SqlExpr::AggCall { func, .. } => func.name().to_string(),
        _ => "?column?".to_string(),
    }
}

fn infer_sql_expr_type(expr: &SqlExpr, scope: &BoundScope) -> SqlType {
    match expr {
        SqlExpr::Column(name) => resolve_column(scope, name)
            .ok()
            .and_then(|idx| scope.desc.columns.get(idx).map(|c| c.sql_type))
            .unwrap_or(SqlType::new(SqlTypeKind::Text)),
        SqlExpr::Const(Value::Int32(_)) => SqlType::new(SqlTypeKind::Int4),
        SqlExpr::Const(Value::Bool(_)) => SqlType::new(SqlTypeKind::Bool),
        SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _)) | SqlExpr::Const(Value::Null) => {
            SqlType::new(SqlTypeKind::Text)
        }
        SqlExpr::Const(Value::Float64(_)) => SqlType::new(SqlTypeKind::Text),
        SqlExpr::Add(_, _) => SqlType::new(SqlTypeKind::Int4),
        SqlExpr::Negate(inner) => infer_sql_expr_type(inner, scope),
        SqlExpr::Cast(_, ty) => *ty,
        SqlExpr::Eq(_, _)
        | SqlExpr::Lt(_, _)
        | SqlExpr::Gt(_, _)
        | SqlExpr::RegexMatch(_, _)
        | SqlExpr::And(_, _)
        | SqlExpr::Or(_, _)
        | SqlExpr::Not(_)
        | SqlExpr::IsNull(_)
        | SqlExpr::IsNotNull(_)
        | SqlExpr::IsDistinctFrom(_, _)
        | SqlExpr::IsNotDistinctFrom(_, _) => SqlType::new(SqlTypeKind::Bool),
        SqlExpr::AggCall { func, .. } => aggregate_sql_type(*func),
        SqlExpr::Random => SqlType::new(SqlTypeKind::Text),
        SqlExpr::CurrentTimestamp => SqlType::new(SqlTypeKind::Timestamp),
    }
}

fn aggregate_sql_type(func: AggFunc) -> SqlType {
    match func {
        AggFunc::Count | AggFunc::Sum | AggFunc::Avg => SqlType::new(SqlTypeKind::Int4),
        AggFunc::Min | AggFunc::Max => SqlType::new(SqlTypeKind::Text),
    }
}

fn bind_agg_output_expr(
    expr: &SqlExpr,
    group_by_exprs: &[SqlExpr],
    input_scope: &BoundScope,
    agg_list: &[(AggFunc, Option<SqlExpr>, bool)],
    n_keys: usize,
) -> Result<Expr, ParseError> {
    for (i, gk) in group_by_exprs.iter().enumerate() {
        if gk == expr { return Ok(Expr::Column(i)); }
    }

    match expr {
        SqlExpr::AggCall { func, arg, distinct } => {
            let entry = (*func, arg.as_deref().cloned(), *distinct);
            for (i, agg) in agg_list.iter().enumerate() {
                if *agg == entry { return Ok(Expr::Column(n_keys + i)); }
            }
            Err(ParseError::UnexpectedToken { expected: "known aggregate", actual: format!("{}(...)", func.name()) })
        }
        SqlExpr::Column(name) => {
            let col_index = resolve_column(input_scope, name)?;
            for (i, gk) in group_by_exprs.iter().enumerate() {
                if let SqlExpr::Column(gk_name) = gk {
                    if let Ok(gk_index) = resolve_column(input_scope, gk_name) {
                        if gk_index == col_index { return Ok(Expr::Column(i)); }
                    }
                }
            }
            Err(ParseError::UngroupedColumn(name.clone()))
        }
        SqlExpr::Const(v) => Ok(Expr::Const(v.clone())),
        SqlExpr::Add(l, r) => Ok(Expr::Add(Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?), Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::Negate(inner) => Ok(Expr::Negate(Box::new(bind_agg_output_expr(inner, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::Cast(inner, ty) => Ok(Expr::Cast(
            Box::new(bind_agg_output_expr(inner, group_by_exprs, input_scope, agg_list, n_keys)?),
            *ty,
        )),
        SqlExpr::Eq(l, r) => Ok(Expr::Eq(Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?), Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::Lt(l, r) => Ok(Expr::Lt(Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?), Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::Gt(l, r) => Ok(Expr::Gt(Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?), Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::RegexMatch(l, r) => Ok(Expr::RegexMatch(Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?), Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::And(l, r) => Ok(Expr::And(Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?), Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::Or(l, r) => Ok(Expr::Or(Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?), Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::Not(inner) => Ok(Expr::Not(Box::new(bind_agg_output_expr(inner, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::IsNull(inner) => Ok(Expr::IsNull(Box::new(bind_agg_output_expr(inner, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::IsNotNull(inner) => Ok(Expr::IsNotNull(Box::new(bind_agg_output_expr(inner, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::IsDistinctFrom(l, r) => Ok(Expr::IsDistinctFrom(Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?), Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::IsNotDistinctFrom(l, r) => Ok(Expr::IsNotDistinctFrom(Box::new(bind_agg_output_expr(l, group_by_exprs, input_scope, agg_list, n_keys)?), Box::new(bind_agg_output_expr(r, group_by_exprs, input_scope, agg_list, n_keys)?))),
        SqlExpr::Random => Ok(Expr::Random),
        SqlExpr::CurrentTimestamp => Ok(Expr::CurrentTimestamp),
    }
}
