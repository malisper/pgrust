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
use crate::backend::utils::cache::relcache::RelCache;
use agg::*;
use agg_output::*;
use coerce::*;
use expr::*;
use functions::*;
use infer::*;
use scope::*;

pub trait CatalogLookup {
    fn lookup_relation(&self, name: &str) -> Option<BoundRelation>;
    fn visible_table_names(&self) -> Vec<String>;
}

impl CatalogLookup for Catalog {
    fn lookup_relation(&self, name: &str) -> Option<BoundRelation> {
        let relcache = RelCache::from_catalog(self);
        relcache.get_by_name(name).map(|entry| BoundRelation {
            rel: entry.rel,
            relation_oid: entry.relation_oid,
            desc: entry.desc.clone(),
        })
    }

    fn visible_table_names(&self) -> Vec<String> {
        let mut names = self
            .table_names()
            .filter(|name| !name.contains('.'))
            .filter(|name| !name.starts_with("pg_temp."))
            .filter(|name| !name.starts_with("pg_"))
            .map(str::to_string)
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        names
    }
}

impl CatalogLookup for RelCache {
    fn lookup_relation(&self, name: &str) -> Option<BoundRelation> {
        self.get_by_name(name).map(|entry| BoundRelation {
            rel: entry.rel,
            relation_oid: entry.relation_oid,
            desc: entry.desc.clone(),
        })
    }

    fn visible_table_names(&self) -> Vec<String> {
        let mut names = self
            .entries()
            .map(|(name, _)| name)
            .filter(|name| !name.contains('.'))
            .filter(|name| !name.starts_with("pg_temp."))
            .filter(|name| !name.starts_with("pg_"))
            .map(str::to_string)
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        names
    }
}

pub fn create_relation_desc(stmt: &CreateTableStatement) -> RelationDesc {
    RelationDesc {
        columns: stmt
            .columns
            .iter()
            .map(|column| {
                let mut desc = column_desc(column.name.clone(), column.ty, column.nullable);
                desc.default_expr = column.default_expr.clone();
                desc
            })
            .collect(),
    }
}

pub(crate) fn bind_scalar_expr_in_scope(
    expr: &SqlExpr,
    columns: &[(String, SqlType)],
    catalog: &dyn CatalogLookup,
) -> Result<(Expr, SqlType), ParseError> {
    let scope = BoundScope {
        desc: RelationDesc {
            columns: columns
                .iter()
                .map(|(name, sql_type)| column_desc(name.clone(), *sql_type, true))
                .collect(),
        },
        columns: columns
            .iter()
            .map(|(name, _)| ScopeColumn {
                output_name: name.clone(),
                relation_name: None,
            })
            .collect(),
    };
    let empty_outer = Vec::new();
    let bound = bind_expr_with_outer(expr, &scope, catalog, &empty_outer, None)?;
    let sql_type = infer_sql_expr_type(expr, &scope, catalog, &empty_outer, None);
    Ok((bound, sql_type))
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

fn relation_desc_from_plan(plan: &Plan) -> RelationDesc {
    RelationDesc {
        columns: plan
            .column_names()
            .into_iter()
            .zip(plan.columns())
            .map(|(name, col)| column_desc(name, col.sql_type, true))
            .collect(),
    }
}

fn apply_cte_column_names(
    plan: Plan,
    desc: RelationDesc,
    column_names: &[String],
) -> Result<(Plan, RelationDesc), ParseError> {
    if column_names.is_empty() {
        return Ok((plan, desc));
    }
    if column_names.len() != desc.columns.len() {
        return Err(ParseError::UnexpectedToken {
            expected: "CTE column alias count matching query width",
            actual: format!(
                "CTE query has {} columns but {} column aliases were specified",
                desc.columns.len(),
                column_names.len()
            ),
        });
    }
    let renamed_desc = RelationDesc {
        columns: desc
            .columns
            .iter()
            .zip(column_names.iter())
            .map(|(column, name)| {
                let mut column = column.clone();
                column.name = name.clone();
                column.storage.name = name.clone();
                column
            })
            .collect(),
    };
    let projection = Plan::Projection {
        input: Box::new(plan),
        targets: renamed_desc
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| TargetEntry {
                name: column.name.clone(),
                expr: Expr::Column(index),
                sql_type: column.sql_type,
            })
            .collect(),
    };
    Ok((projection, renamed_desc))
}

fn bind_ctes(
    ctes: &[CommonTableExpr],
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<GroupedOuterScope>,
    outer_ctes: &[BoundCte],
) -> Result<Vec<BoundCte>, ParseError> {
    let mut bound = Vec::with_capacity(ctes.len());
    for cte in ctes {
        let mut visible = bound.clone();
        visible.extend_from_slice(outer_ctes);
        let (plan, desc) = match &cte.body {
            CteBody::Select(select) => {
                let plan =
                    build_plan_with_outer(select, catalog, outer_scopes, grouped_outer.clone(), &visible)?;
                let desc = relation_desc_from_plan(&plan);
                apply_cte_column_names(plan, desc, &cte.column_names)?
            }
            CteBody::Values(values) => {
                let plan = build_values_plan_with_outer(
                    values,
                    catalog,
                    outer_scopes,
                    grouped_outer.clone(),
                    &visible,
                )?;
                let desc = relation_desc_from_plan(&plan);
                apply_cte_column_names(plan, desc, &cte.column_names)?
            }
        };
        bound.push(BoundCte {
            name: cte.name.clone(),
            plan,
            desc,
        });
    }
    Ok(bound)
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

pub fn build_plan(stmt: &SelectStatement, catalog: &dyn CatalogLookup) -> Result<Plan, ParseError> {
    build_plan_with_outer(stmt, catalog, &[], None, &[])
}

pub fn build_values_plan(stmt: &ValuesStatement, catalog: &dyn CatalogLookup) -> Result<Plan, ParseError> {
    build_values_plan_with_outer(stmt, catalog, &[], None, &[])
}

fn build_values_plan_with_outer(
    stmt: &ValuesStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<GroupedOuterScope>,
    outer_ctes: &[BoundCte],
) -> Result<Plan, ParseError> {
    let local_ctes =
        bind_ctes(&stmt.with, catalog, outer_scopes, grouped_outer.clone(), outer_ctes)?;
    let mut visible_ctes = local_ctes;
    visible_ctes.extend_from_slice(outer_ctes);
    let (mut plan, scope) =
        bind_values_rows(&stmt.rows, None, catalog, outer_scopes, grouped_outer.as_ref(), &visible_ctes)?;
    let output_columns = match &plan {
        Plan::Values { output_columns, .. } => output_columns.clone(),
        other => {
            return Err(ParseError::UnexpectedToken {
                expected: "VALUES plan",
                actual: format!("{other:?}"),
            });
        }
    };
    let targets = output_columns
        .iter()
        .enumerate()
        .map(|(index, column)| TargetEntry {
            name: column.name.clone(),
            expr: Expr::Column(index),
            sql_type: column.sql_type,
        })
        .collect::<Vec<_>>();

    if !stmt.order_by.is_empty() {
        plan = Plan::OrderBy {
            input: Box::new(plan),
            items: bind_order_by_items(&stmt.order_by, &targets, |expr| {
                bind_expr_with_outer_and_ctes(
                    expr,
                    &scope,
                    catalog,
                    outer_scopes,
                    grouped_outer.as_ref(),
                    &visible_ctes,
                )
            })?,
        };
    }

    if stmt.limit.is_some() || stmt.offset.is_some() {
        plan = Plan::Limit {
            input: Box::new(plan),
            limit: stmt.limit,
            offset: stmt.offset.unwrap_or(0),
        };
    }

    Ok(plan)
}

fn build_plan_with_outer(
    stmt: &SelectStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<GroupedOuterScope>,
    outer_ctes: &[BoundCte],
) -> Result<Plan, ParseError> {
    let local_ctes = bind_ctes(&stmt.with, catalog, outer_scopes, grouped_outer.clone(), outer_ctes)?;
    let mut visible_ctes = local_ctes;
    visible_ctes.extend_from_slice(outer_ctes);

    if stmt.targets.is_empty() && stmt.from.is_none() {
        return Err(ParseError::EmptySelectList);
    }

    let (base, scope) = if let Some(from) = &stmt.from {
        bind_from_item_with_ctes(from, catalog, outer_scopes, grouped_outer.as_ref(), &visible_ctes)?
    } else {
        (Plan::Result, empty_scope())
    };

    if let Some(predicate) = &stmt.where_clause {
        if expr_contains_agg(predicate) {
            return Err(ParseError::AggInWhere);
        }
    }

    let filtered_plan = if let Some(predicate) = &stmt.where_clause {
        Plan::Filter {
            input: Box::new(base),
            predicate: bind_expr_with_outer_and_ctes(
                predicate,
                &scope,
                catalog,
                outer_scopes,
                grouped_outer.as_ref(),
                &visible_ctes,
            )?,
        }
    } else {
        base
    };

    let needs_agg =
        !stmt.group_by.is_empty() || targets_contain_agg(&stmt.targets) || stmt.having.is_some();

    let can_skip_scan_for_degenerate_having = needs_agg
        && stmt.group_by.is_empty()
        && !targets_contain_agg(&stmt.targets)
        && stmt
            .having
            .as_ref()
            .is_some_and(|having| !expr_contains_agg(having) && !expr_references_input_scope(having))
        && stmt
            .targets
            .iter()
            .all(|target| !expr_references_input_scope(&target.expr));

    let mut plan = if can_skip_scan_for_degenerate_having {
        Plan::Result
    } else {
        filtered_plan
    };

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
            .map(|e| {
                bind_expr_with_outer_and_ctes(
                    e,
                    &scope,
                    catalog,
                    outer_scopes,
                    grouped_outer.as_ref(),
                    &visible_ctes,
                )
            })
            .collect::<Result<_, _>>()?;

        let accumulators: Vec<AggAccum> = aggs
            .iter()
            .map(|(func, args, distinct)| {
                validate_aggregate_arity(*func, args)?;
                let arg_type = args.first().map(|e| {
                    infer_sql_expr_type_with_ctes(
                        e,
                        &scope,
                        catalog,
                        outer_scopes,
                        grouped_outer.as_ref(),
                        &visible_ctes,
                    )
                });
                Ok(AggAccum {
                    func: *func,
                    args: args
                        .iter()
                        .map(|e| {
                            bind_expr_with_outer_and_ctes(
                                e,
                                &scope,
                                catalog,
                                outer_scopes,
                                grouped_outer.as_ref(),
                                &visible_ctes,
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
                sql_type: infer_sql_expr_type_with_ctes(
                    gk,
                    &scope,
                    catalog,
                    outer_scopes,
                    grouped_outer.as_ref(),
                    &visible_ctes,
                ),
            });
        }
        for (func, args, _) in &aggs {
            output_columns.push(QueryColumn {
                name: func.name().to_string(),
                sql_type: aggregate_sql_type(
                    *func,
                    args.first().map(|e| {
                        infer_sql_expr_type_with_ctes(
                            e,
                            &scope,
                            catalog,
                            outer_scopes,
                            grouped_outer.as_ref(),
                            &visible_ctes,
                        )
                    }),
                ),
            });
        }

        let having = stmt
            .having
            .as_ref()
            .map(|e| {
                bind_agg_output_expr_in_clause(
                    e,
                    UngroupedColumnClause::Having,
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
                        expr: bind_agg_output_expr_in_clause(
                            &item.expr,
                            UngroupedColumnClause::SelectTarget,
                            &stmt.group_by,
                            &scope,
                            catalog,
                            outer_scopes,
                            grouped_outer.as_ref(),
                            &aggs,
                            n_keys,
                        )?,
                        sql_type: infer_sql_expr_type_with_ctes(
                            &item.expr,
                            &scope,
                            catalog,
                            outer_scopes,
                            grouped_outer.as_ref(),
                            &visible_ctes,
                        ),
                    })
                })
                .collect::<Result<_, _>>()?
        };

        if !stmt.order_by.is_empty() {
            plan = Plan::OrderBy {
                input: Box::new(plan),
                items: bind_order_by_items(
                    &stmt.order_by,
                    &targets,
                    |expr| {
                        bind_agg_output_expr_in_clause(
                            expr,
                            UngroupedColumnClause::SelectTarget,
                            &stmt.group_by,
                            &scope,
                            catalog,
                            outer_scopes,
                            grouped_outer.as_ref(),
                            &aggs,
                            n_keys,
                        )
                    },
                )?,
            };
        }

        if stmt.limit.is_some() || stmt.offset.is_some() {
            plan = Plan::Limit {
                input: Box::new(plan),
                limit: stmt.limit,
                offset: stmt.offset.unwrap_or(0),
            };
        }

        Ok(Plan::Projection {
            input: Box::new(plan),
            targets,
        })
    } else {
        let targets = bind_select_targets(
            &stmt.targets,
            &scope,
            catalog,
            outer_scopes,
            grouped_outer.as_ref(),
            &visible_ctes,
        )?;

        if !stmt.order_by.is_empty() {
            plan = Plan::OrderBy {
                input: Box::new(plan),
                items: bind_order_by_items(&stmt.order_by, &targets, |expr| {
                    bind_expr_with_outer_and_ctes(
                        expr,
                        &scope,
                        catalog,
                        outer_scopes,
                        grouped_outer.as_ref(),
                        &visible_ctes,
                    )
                })?,
            };
        }

        if stmt.limit.is_some() || stmt.offset.is_some() {
            plan = Plan::Limit {
                input: Box::new(plan),
                limit: stmt.limit,
                offset: stmt.offset.unwrap_or(0),
            };
        }

        // Optimization: skip Projection if it's an identity mapping (select *)
        let is_identity = targets.len() == scope.columns.len()
            && targets.iter().enumerate().all(|(i, t)| {
                matches!(t.expr, Expr::Column(c) if c == i)
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

fn bind_order_by_items(
    items: &[OrderByItem],
    targets: &[TargetEntry],
    bind_expr: impl Fn(&SqlExpr) -> Result<Expr, ParseError>,
) -> Result<Vec<crate::backend::executor::OrderByEntry>, ParseError> {
    items.iter()
        .map(|item| {
            let expr = match &item.expr {
                SqlExpr::IntegerLiteral(value) => {
                    if let Ok(ordinal) = value.parse::<usize>() {
                        if ordinal > 0 && ordinal <= targets.len() {
                            targets[ordinal - 1].expr.clone()
                        } else {
                            return Err(ParseError::UnexpectedToken {
                                expected: "ORDER BY position in select list",
                                actual: value.clone(),
                            });
                        }
                    } else {
                        bind_expr(&item.expr)?
                    }
                }
                _ => bind_expr(&item.expr)?,
            };
            Ok(crate::backend::executor::OrderByEntry {
                expr,
                descending: item.descending,
                nulls_first: item.nulls_first,
            })
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundInsertStatement {
    pub rel: RelFileLocator,
    pub desc: RelationDesc,
    pub column_defaults: Vec<Expr>,
    pub target_columns: Vec<usize>,
    pub source: BoundInsertSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundInsertSource {
    Values(Vec<Vec<Expr>>),
    DefaultValues(Vec<Expr>),
    Select(Box<Plan>),
}

/// A pre-bound insert plan that can be executed repeatedly with different
/// parameter values, avoiding re-parsing and re-binding on each call.
#[derive(Debug, Clone)]
pub struct PreparedInsert {
    pub rel: RelFileLocator,
    pub desc: RelationDesc,
    pub column_defaults: Vec<Expr>,
    pub target_columns: Vec<usize>,
    pub num_params: usize,
}

fn bind_insert_column_defaults(
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
    local_ctes: &[BoundCte],
) -> Result<Vec<Expr>, ParseError> {
    desc.columns
        .iter()
        .map(|column| {
            column
                .default_expr
                .as_ref()
                .map(|sql| {
                    let expr = crate::backend::parser::parse_expr(sql)?;
                    bind_expr_with_outer_and_ctes(
                        &expr,
                        &empty_scope(),
                        catalog,
                        &[],
                        None,
                        local_ctes,
                    )
                })
                .transpose()
                .map(|expr| expr.unwrap_or(Expr::Const(Value::Null)))
        })
        .collect()
}

pub fn bind_insert_prepared(
    table_name: &str,
    columns: Option<&[String]>,
    num_params: usize,
    catalog: &dyn CatalogLookup,
) -> Result<PreparedInsert, ParseError> {
    let entry = lookup_relation(catalog, table_name)?;
    let column_defaults = bind_insert_column_defaults(&entry.desc, catalog, &[])?;

    let target_columns = if let Some(columns) = columns {
        let scope = scope_for_relation(Some(table_name), &entry.desc);
        columns
            .iter()
            .map(|column| resolve_column(&scope, column))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        if num_params > entry.desc.columns.len() {
            return Err(ParseError::InvalidInsertTargetCount {
                expected: entry.desc.columns.len(),
                actual: num_params,
            });
        }
        (0..num_params).collect()
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
        column_defaults,
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
    catalog: &dyn CatalogLookup,
) -> Result<BoundInsertStatement, ParseError> {
    let local_ctes = bind_ctes(&stmt.with, catalog, &[], None, &[])?;
    let entry = lookup_relation(catalog, &stmt.table_name)?;
    let column_defaults = bind_insert_column_defaults(&entry.desc, catalog, &local_ctes)?;
    let scope = scope_for_relation(Some(&stmt.table_name), &entry.desc);

    let source = match &stmt.source {
        InsertSource::Values(rows) => {
            let target_columns = if let Some(columns) = &stmt.columns {
                columns
                    .iter()
                    .map(|column| resolve_column(&scope, column))
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                let width = rows.first().map(Vec::len).unwrap_or(0);
                if width > entry.desc.columns.len() {
                    return Err(ParseError::InvalidInsertTargetCount {
                        expected: entry.desc.columns.len(),
                        actual: width,
                    });
                }
                (0..width).collect()
            };
            for row in rows {
                if target_columns.len() != row.len() {
                    return Err(ParseError::InvalidInsertTargetCount {
                        expected: target_columns.len(),
                        actual: row.len(),
                    });
                }
            }
            let bound_rows = rows
                .iter()
                .map(|row| {
                    row.iter()
                        .zip(target_columns.iter())
                        .map(|(expr, column_index)| match expr {
                            SqlExpr::Default => Ok(column_defaults[*column_index].clone()),
                            _ => bind_expr_with_outer_and_ctes(
                                expr,
                                &scope,
                                catalog,
                                &[],
                                None,
                                &local_ctes,
                            ),
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<Vec<_>, _>>()?;
            (target_columns, BoundInsertSource::Values(bound_rows))
        }
        InsertSource::DefaultValues => (
            (0..entry.desc.columns.len()).collect(),
            BoundInsertSource::DefaultValues(column_defaults.clone()),
        ),
        InsertSource::Select(select) => {
            let plan = build_plan_with_outer(select, catalog, &[], None, &local_ctes)?;
            let actual = plan.columns().len();
            let target_columns = if let Some(columns) = &stmt.columns {
                columns
                    .iter()
                    .map(|column| resolve_column(&scope, column))
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                if actual > entry.desc.columns.len() {
                    return Err(ParseError::InvalidInsertTargetCount {
                        expected: entry.desc.columns.len(),
                        actual,
                    });
                }
                (0..actual).collect()
            };
            if target_columns.len() != actual {
                return Err(ParseError::InvalidInsertTargetCount {
                    expected: target_columns.len(),
                    actual,
                });
            }
            (target_columns, BoundInsertSource::Select(Box::new(plan)))
        }
    };
    let (target_columns, source) = source;

    Ok(BoundInsertStatement {
        rel: entry.rel,
        desc: entry.desc.clone(),
        column_defaults,
        target_columns,
        source,
    })
}

pub fn bind_update(
    stmt: &UpdateStatement,
    catalog: &dyn CatalogLookup,
) -> Result<BoundUpdateStatement, ParseError> {
    let local_ctes = bind_ctes(&stmt.with, catalog, &[], None, &[])?;
    let entry = lookup_relation(catalog, &stmt.table_name)?;
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
                    expr: bind_expr_with_outer_and_ctes(
                        &assignment.expr,
                        &scope,
                        catalog,
                        &[],
                        None,
                        &local_ctes,
                    )?,
                })
            })
            .collect::<Result<Vec<_>, ParseError>>()?,
        predicate: stmt
            .where_clause
            .as_ref()
            .map(|expr| bind_expr_with_outer_and_ctes(expr, &scope, catalog, &[], None, &local_ctes))
            .transpose()?,
    })
}

pub fn bind_delete(
    stmt: &DeleteStatement,
    catalog: &dyn CatalogLookup,
) -> Result<BoundDeleteStatement, ParseError> {
    let local_ctes = bind_ctes(&stmt.with, catalog, &[], None, &[])?;
    let entry = lookup_relation(catalog, &stmt.table_name)?;
    let scope = scope_for_relation(Some(&stmt.table_name), &entry.desc);

    Ok(BoundDeleteStatement {
        rel: entry.rel,
        desc: entry.desc.clone(),
        predicate: stmt
            .where_clause
            .as_ref()
            .map(|expr| bind_expr_with_outer_and_ctes(expr, &scope, catalog, &[], None, &local_ctes))
            .transpose()?,
    })
}
