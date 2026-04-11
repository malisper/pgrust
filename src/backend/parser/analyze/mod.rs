mod agg;
mod agg_output;
mod agg_output_special;
mod coerce;
mod expr;
mod functions;
mod infer;
mod scope;

use crate::RelFileLocator;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::{
    AggAccum, AggFunc, BuiltinScalarFunction, Expr, JsonTableFunction, Plan, QueryColumn,
    RelationDesc, TargetEntry, Value,
};

use super::parsenodes::*;
pub use crate::backend::catalog::catalog::{Catalog, CatalogEntry};
use agg::*;
use agg_output::*;
use coerce::*;
use expr::*;
use functions::*;
use infer::*;
use scope::*;

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
