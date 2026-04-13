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
    AggAccum, AggFunc, BuiltinScalarFunction, Expr, JsonTableFunction, OrderByEntry, Plan,
    QueryColumn, RelationDesc, TargetEntry, Value,
};
use crate::include::catalog::{
    PgCastRow, PgOperatorRow, PgProcRow, PgTypeRow, bootstrap_pg_cast_rows,
    bootstrap_pg_operator_rows, bootstrap_pg_proc_rows, builtin_type_rows,
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
pub use scope::BoundRelation;
use scope::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundIndexRelation {
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub desc: RelationDesc,
    pub index_meta: crate::backend::utils::cache::relcache::IndexRelCacheEntry,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundModifyRowSource {
    Heap,
    Index {
        index: BoundIndexRelation,
        keys: Vec<crate::include::access::scankey::ScanKeyData>,
    },
}

pub trait CatalogLookup {
    fn lookup_any_relation(&self, name: &str) -> Option<BoundRelation>;

    fn index_relations_for_heap(&self, _relation_oid: u32) -> Vec<BoundIndexRelation> {
        Vec::new()
    }

    fn lookup_relation(&self, name: &str) -> Option<BoundRelation> {
        self.lookup_any_relation(name)
            .filter(|entry| entry.relkind == 'r')
    }

    fn proc_rows_by_name(&self, name: &str) -> Vec<PgProcRow> {
        let normalized = normalize_catalog_lookup_name(name);
        bootstrap_pg_proc_rows()
            .into_iter()
            .filter(|row| row.proname.eq_ignore_ascii_case(normalized))
            .collect()
    }

    fn operator_by_name_left_right(
        &self,
        name: &str,
        left_type_oid: u32,
        right_type_oid: u32,
    ) -> Option<PgOperatorRow> {
        let normalized = normalize_catalog_lookup_name(name);
        bootstrap_pg_operator_rows().into_iter().find(|row| {
            row.oprname.eq_ignore_ascii_case(normalized)
                && row.oprleft == left_type_oid
                && row.oprright == right_type_oid
        })
    }

    fn cast_by_source_target(
        &self,
        source_type_oid: u32,
        target_type_oid: u32,
    ) -> Option<PgCastRow> {
        bootstrap_pg_cast_rows()
            .into_iter()
            .find(|row| row.castsource == source_type_oid && row.casttarget == target_type_oid)
    }

    fn type_rows(&self) -> Vec<PgTypeRow> {
        builtin_type_rows()
    }

    fn type_by_oid(&self, oid: u32) -> Option<PgTypeRow> {
        self.type_rows().into_iter().find(|row| row.oid == oid)
    }

    fn type_oid_for_sql_type(&self, sql_type: SqlType) -> Option<u32> {
        let mut fallback = None;
        for row in self.type_rows() {
            if row.sql_type.kind != sql_type.kind || row.sql_type.is_array != sql_type.is_array {
                continue;
            }
            if row.typrelid == 0 {
                return Some(row.oid);
            }
            fallback.get_or_insert(row.oid);
        }
        fallback
    }
}

impl CatalogLookup for Catalog {
    fn lookup_any_relation(&self, name: &str) -> Option<BoundRelation> {
        let relcache = RelCache::from_catalog(self);
        relcache.get_by_name(name).map(|entry| BoundRelation {
            rel: entry.rel,
            relation_oid: entry.relation_oid,
            namespace_oid: entry.namespace_oid,
            relpersistence: entry.relpersistence,
            relkind: entry.relkind,
            desc: entry.desc.clone(),
        })
    }

    fn index_relations_for_heap(&self, relation_oid: u32) -> Vec<BoundIndexRelation> {
        let relcache = RelCache::from_catalog(self);
        relcache
            .entries()
            .filter_map(|(_, entry)| {
                let index_meta = entry.index.as_ref()?;
                (index_meta.indrelid == relation_oid).then(|| BoundIndexRelation {
                    rel: entry.rel,
                    relation_oid: entry.relation_oid,
                    desc: entry.desc.clone(),
                    index_meta: index_meta.clone(),
                })
            })
            .collect()
    }
}

impl CatalogLookup for RelCache {
    fn lookup_any_relation(&self, name: &str) -> Option<BoundRelation> {
        self.get_by_name(name).map(|entry| BoundRelation {
            rel: entry.rel,
            relation_oid: entry.relation_oid,
            namespace_oid: entry.namespace_oid,
            relpersistence: entry.relpersistence,
            relkind: entry.relkind,
            desc: entry.desc.clone(),
        })
    }

    fn index_relations_for_heap(&self, relation_oid: u32) -> Vec<BoundIndexRelation> {
        self.entries()
            .filter_map(|(_, entry)| {
                let index_meta = entry.index.as_ref()?;
                (index_meta.indrelid == relation_oid).then(|| BoundIndexRelation {
                    rel: entry.rel,
                    relation_oid: entry.relation_oid,
                    desc: entry.desc.clone(),
                    index_meta: index_meta.clone(),
                })
            })
            .collect()
    }
}

fn normalize_catalog_lookup_name(name: &str) -> &str {
    name.strip_prefix("pg_catalog.").unwrap_or(name)
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
        Some(schema) if schema == "public" => {
            if persistence == TablePersistence::Temporary {
                return Err(ParseError::TempTableInNonTempSchema(schema));
            }
            persistence
        }
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
                let plan = build_plan_with_outer(
                    select,
                    catalog,
                    outer_scopes,
                    grouped_outer.clone(),
                    &visible,
                )?;
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
            crate::backend::catalog::catalog::CatalogError::UnknownColumn(name) => {
                ParseError::UnknownColumn(name)
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

pub fn build_values_plan(
    stmt: &ValuesStatement,
    catalog: &dyn CatalogLookup,
) -> Result<Plan, ParseError> {
    build_values_plan_with_outer(stmt, catalog, &[], None, &[])
}

fn build_values_plan_with_outer(
    stmt: &ValuesStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<GroupedOuterScope>,
    outer_ctes: &[BoundCte],
) -> Result<Plan, ParseError> {
    let local_ctes = bind_ctes(
        &stmt.with,
        catalog,
        outer_scopes,
        grouped_outer.clone(),
        outer_ctes,
    )?;
    let mut visible_ctes = local_ctes;
    visible_ctes.extend_from_slice(outer_ctes);
    let (mut plan, scope) = bind_values_rows(
        &stmt.rows,
        None,
        catalog,
        outer_scopes,
        grouped_outer.as_ref(),
        &visible_ctes,
    )?;
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
    let local_ctes = bind_ctes(
        &stmt.with,
        catalog,
        outer_scopes,
        grouped_outer.clone(),
        outer_ctes,
    )?;
    let mut visible_ctes = local_ctes;
    visible_ctes.extend_from_slice(outer_ctes);

    if stmt.targets.is_empty() && stmt.from.is_none() {
        return Err(ParseError::EmptySelectList);
    }

    let (base, scope) = if let Some(from) = &stmt.from {
        bind_from_item_with_ctes(
            from,
            catalog,
            outer_scopes,
            grouped_outer.as_ref(),
            &visible_ctes,
        )?
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
        && stmt.having.as_ref().is_some_and(|having| {
            !expr_contains_agg(having) && !expr_references_input_scope(having)
        })
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
                items: bind_order_by_items(&stmt.order_by, &targets, |expr| {
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

        plan = maybe_rewrite_index_scan(plan, catalog);

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

#[derive(Debug, Clone)]
struct IndexableQual {
    column: usize,
    strategy: u16,
    argument: Value,
    expr: Expr,
}

#[derive(Debug, Clone)]
struct ChosenIndexPath {
    index: BoundIndexRelation,
    keys: Vec<crate::include::access::scankey::ScanKeyData>,
    residual: Option<Expr>,
    direction: crate::include::access::relscan::ScanDirection,
    has_qual: bool,
    usable_prefix: usize,
    removes_order: bool,
}

fn maybe_rewrite_index_scan(plan: Plan, catalog: &dyn CatalogLookup) -> Plan {
    let (rel, relation_oid, desc, filter, order_items) = match plan {
        Plan::SeqScan {
            rel,
            relation_oid,
            desc,
        } => (rel, relation_oid, desc, None, None),
        Plan::Filter { input, predicate } => match *input {
            Plan::SeqScan {
                rel,
                relation_oid,
                desc,
            } => (rel, relation_oid, desc, Some(predicate), None),
            other => return Plan::Filter {
                input: Box::new(other),
                predicate,
            },
        },
        Plan::OrderBy { input, items } => match *input {
            Plan::SeqScan {
                rel,
                relation_oid,
                desc,
            } => (rel, relation_oid, desc, None, Some(items)),
            Plan::Filter { input, predicate } => match *input {
                Plan::SeqScan {
                    rel,
                    relation_oid,
                    desc,
                } => (rel, relation_oid, desc, Some(predicate), Some(items)),
                other => {
                    return Plan::OrderBy {
                        input: Box::new(Plan::Filter {
                            input: Box::new(other),
                            predicate,
                        }),
                        items,
                    };
                }
            },
            other => {
                return Plan::OrderBy {
                    input: Box::new(other),
                    items,
                };
            }
        },
        other => return other,
    };

    let indexes = catalog.index_relations_for_heap(relation_oid);
    choose_index_scan(rel, relation_oid, desc, filter, order_items, indexes)
}

fn rebuild_scan_plan(
    rel: RelFileLocator,
    relation_oid: u32,
    desc: RelationDesc,
    filter: Option<Expr>,
    order_items: Option<Vec<OrderByEntry>>,
) -> Plan {
    let mut plan = Plan::SeqScan {
        rel,
        relation_oid,
        desc,
    };
    if let Some(predicate) = filter {
        plan = Plan::Filter {
            input: Box::new(plan),
            predicate,
        };
    }
    if let Some(items) = order_items {
        plan = Plan::OrderBy {
            input: Box::new(plan),
            items,
        };
    }
    plan
}

fn choose_index_scan(
    rel: RelFileLocator,
    relation_oid: u32,
    desc: RelationDesc,
    filter: Option<Expr>,
    order_items: Option<Vec<OrderByEntry>>,
    indexes: Vec<BoundIndexRelation>,
) -> Plan {
    let Some(chosen) = choose_index_path(filter.as_ref(), order_items.as_deref(), &indexes) else {
        return rebuild_scan_plan(rel, relation_oid, desc, filter, order_items);
    };

    let mut plan = Plan::IndexScan {
        rel,
        index_rel: chosen.index.rel,
        am_oid: chosen.index.index_meta.am_oid,
        desc: desc.clone(),
        index_meta: chosen.index.index_meta.clone(),
        keys: chosen.keys,
        direction: chosen.direction,
    };
    if let Some(predicate) = chosen.residual {
        plan = Plan::Filter {
            input: Box::new(plan),
            predicate,
        };
    }
    if !chosen.removes_order && let Some(items) = order_items {
        plan = Plan::OrderBy {
            input: Box::new(plan),
            items,
        };
    }

    plan
}

fn choose_index_path(
    filter: Option<&Expr>,
    order_items: Option<&[OrderByEntry]>,
    indexes: &[BoundIndexRelation],
) -> Option<ChosenIndexPath> {
    let conjuncts = filter.map(flatten_and_conjuncts).unwrap_or_default();
    let parsed_quals = conjuncts
        .iter()
        .filter_map(indexable_qual)
        .collect::<Vec<_>>();

    let mut best: Option<ChosenIndexPath> = None;
    for index in indexes.iter().filter(|index| {
        index.index_meta.indisvalid
            && index.index_meta.indisready
            && !index.index_meta.indkey.is_empty()
            && index.index_meta.am_oid == crate::include::catalog::BTREE_AM_OID
    }) {
        let mut used = vec![false; parsed_quals.len()];
        let mut keys = Vec::new();
        let mut equality_prefix = 0usize;

        for attnum in &index.index_meta.indkey {
            let column = attnum.saturating_sub(1) as usize;
            if let Some((qual_idx, qual)) = parsed_quals
                .iter()
                .enumerate()
                .find(|(idx, qual)| !used[*idx] && qual.column == column && qual.strategy == 3)
            {
                used[qual_idx] = true;
                equality_prefix += 1;
                keys.push(crate::include::access::scankey::ScanKeyData {
                    attribute_number: equality_prefix as i16,
                    strategy: qual.strategy,
                    argument: qual.argument.clone(),
                });
                continue;
            }
            if let Some((qual_idx, qual)) = parsed_quals
                .iter()
                .enumerate()
                .find(|(idx, qual)| !used[*idx] && qual.column == column)
            {
                used[qual_idx] = true;
                keys.push(crate::include::access::scankey::ScanKeyData {
                    attribute_number: (equality_prefix + 1) as i16,
                    strategy: qual.strategy,
                    argument: qual.argument.clone(),
                });
            }
            break;
        }

        let usable_prefix = keys.len();
        let order_match = order_items.and_then(|items| index_order_match(items, index, equality_prefix));
        let has_qual = usable_prefix > 0;
        if !has_qual && order_match.is_none() {
            continue;
        }
        let residual = {
            let used_exprs = parsed_quals
                .iter()
                .enumerate()
                .filter_map(|(idx, qual)| used.get(idx).copied().unwrap_or(false).then_some(&qual.expr))
                .collect::<Vec<_>>();
            let residual = conjuncts
                .iter()
                .filter(|expr| !used_exprs.iter().any(|used_expr| *used_expr == *expr))
                .cloned()
                .collect::<Vec<_>>();
            and_exprs(residual)
        };

        let chosen = ChosenIndexPath {
            index: index.clone(),
            keys,
            residual,
            direction: order_match
                .as_ref()
                .map(|(_, direction)| *direction)
                .unwrap_or(crate::include::access::relscan::ScanDirection::Forward),
            has_qual,
            usable_prefix,
            removes_order: order_match.is_some(),
        };

        match &best {
            None => best = Some(chosen),
            Some(existing) => {
                if (chosen.has_qual as u8, chosen.usable_prefix, chosen.removes_order as u8)
                    > (
                        existing.has_qual as u8,
                        existing.usable_prefix,
                        existing.removes_order as u8,
                    )
                {
                    best = Some(chosen);
                }
            }
        }
    }

    best
}

fn choose_modify_row_source(
    predicate: Option<&Expr>,
    indexes: &[BoundIndexRelation],
) -> BoundModifyRowSource {
    if let Some(chosen) = choose_index_path(predicate, None, indexes).filter(|chosen| chosen.has_qual)
    {
        BoundModifyRowSource::Index {
            index: chosen.index,
            keys: chosen.keys,
        }
    } else {
        BoundModifyRowSource::Heap
    }
}

fn index_order_match(
    items: &[OrderByEntry],
    index: &BoundIndexRelation,
    equality_prefix: usize,
) -> Option<(usize, crate::include::access::relscan::ScanDirection)> {
    if items.is_empty() {
        return None;
    }
    let mut direction = None;
    let mut matched = 0usize;
    for (idx, item) in items.iter().enumerate() {
        let Expr::Column(column) = item.expr else {
            break;
        };
        let Some(attnum) = index.index_meta.indkey.get(equality_prefix + idx) else {
            break;
        };
        if *attnum as usize != column + 1 {
            break;
        }
        let item_direction = if item.descending {
            crate::include::access::relscan::ScanDirection::Backward
        } else {
            crate::include::access::relscan::ScanDirection::Forward
        };
        if let Some(existing) = direction {
            if existing != item_direction {
                return None;
            }
        } else {
            direction = Some(item_direction);
        }
        matched += 1;
    }
    (matched == items.len()).then_some((matched, direction.unwrap_or(
        crate::include::access::relscan::ScanDirection::Forward,
    )))
}

fn flatten_and_conjuncts(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::And(left, right) => {
            let mut out = flatten_and_conjuncts(left);
            out.extend(flatten_and_conjuncts(right));
            out
        }
        other => vec![other.clone()],
    }
}

fn indexable_qual(expr: &Expr) -> Option<IndexableQual> {
    fn mk(column: usize, strategy: u16, argument: &Value, expr: &Expr) -> Option<IndexableQual> {
        Some(IndexableQual {
            column,
            strategy,
            argument: argument.clone(),
            expr: expr.clone(),
        })
    }

    match expr {
        Expr::Eq(left, right) => match (&**left, &**right) {
            (Expr::Column(column), Expr::Const(value)) => mk(*column, 3, value, expr),
            (Expr::Const(value), Expr::Column(column)) => mk(*column, 3, value, expr),
            _ => None,
        },
        Expr::Lt(left, right) => match (&**left, &**right) {
            (Expr::Column(column), Expr::Const(value)) => mk(*column, 1, value, expr),
            (Expr::Const(value), Expr::Column(column)) => mk(*column, 5, value, expr),
            _ => None,
        },
        Expr::LtEq(left, right) => match (&**left, &**right) {
            (Expr::Column(column), Expr::Const(value)) => mk(*column, 2, value, expr),
            (Expr::Const(value), Expr::Column(column)) => mk(*column, 4, value, expr),
            _ => None,
        },
        Expr::Gt(left, right) => match (&**left, &**right) {
            (Expr::Column(column), Expr::Const(value)) => mk(*column, 5, value, expr),
            (Expr::Const(value), Expr::Column(column)) => mk(*column, 1, value, expr),
            _ => None,
        },
        Expr::GtEq(left, right) => match (&**left, &**right) {
            (Expr::Column(column), Expr::Const(value)) => mk(*column, 4, value, expr),
            (Expr::Const(value), Expr::Column(column)) => mk(*column, 2, value, expr),
            _ => None,
        },
        _ => None,
    }
}

fn and_exprs(mut exprs: Vec<Expr>) -> Option<Expr> {
    if exprs.is_empty() {
        return None;
    }
    let first = exprs.remove(0);
    Some(
        exprs
            .into_iter()
            .fold(first, |acc, expr| Expr::And(Box::new(acc), Box::new(expr))),
    )
}

fn bind_order_by_items(
    items: &[OrderByItem],
    targets: &[TargetEntry],
    bind_expr: impl Fn(&SqlExpr) -> Result<Expr, ParseError>,
) -> Result<Vec<crate::backend::executor::OrderByEntry>, ParseError> {
    items
        .iter()
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
    pub relation_oid: u32,
    pub desc: RelationDesc,
    pub indexes: Vec<BoundIndexRelation>,
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
    pub relation_oid: u32,
    pub desc: RelationDesc,
    pub indexes: Vec<BoundIndexRelation>,
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
        relation_oid: entry.relation_oid,
        desc: entry.desc.clone(),
        indexes: catalog.index_relations_for_heap(entry.relation_oid),
        column_defaults,
        target_columns,
        num_params,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundUpdateStatement {
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub desc: RelationDesc,
    pub row_source: BoundModifyRowSource,
    pub indexes: Vec<BoundIndexRelation>,
    pub assignments: Vec<BoundAssignment>,
    pub predicate: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundDeleteStatement {
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub desc: RelationDesc,
    pub row_source: BoundModifyRowSource,
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
        relation_oid: entry.relation_oid,
        desc: entry.desc.clone(),
        indexes: catalog.index_relations_for_heap(entry.relation_oid),
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
    let indexes = catalog.index_relations_for_heap(entry.relation_oid);
    let predicate = stmt
        .where_clause
        .as_ref()
        .map(|expr| {
            bind_expr_with_outer_and_ctes(expr, &scope, catalog, &[], None, &local_ctes)
        })
        .transpose()?;

    Ok(BoundUpdateStatement {
        rel: entry.rel,
        relation_oid: entry.relation_oid,
        desc: entry.desc.clone(),
        row_source: choose_modify_row_source(predicate.as_ref(), &indexes),
        indexes,
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
        predicate,
    })
}

pub fn bind_delete(
    stmt: &DeleteStatement,
    catalog: &dyn CatalogLookup,
) -> Result<BoundDeleteStatement, ParseError> {
    let local_ctes = bind_ctes(&stmt.with, catalog, &[], None, &[])?;
    let entry = lookup_relation(catalog, &stmt.table_name)?;
    let scope = scope_for_relation(Some(&stmt.table_name), &entry.desc);
    let predicate = stmt
        .where_clause
        .as_ref()
        .map(|expr| {
            bind_expr_with_outer_and_ctes(expr, &scope, catalog, &[], None, &local_ctes)
        })
        .transpose()?;
    let indexes = catalog.index_relations_for_heap(entry.relation_oid);

    Ok(BoundDeleteStatement {
        rel: entry.rel,
        relation_oid: entry.relation_oid,
        desc: entry.desc.clone(),
        row_source: choose_modify_row_source(predicate.as_ref(), &indexes),
        predicate,
    })
}
