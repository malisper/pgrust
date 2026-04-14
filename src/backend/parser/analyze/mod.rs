mod agg;
mod agg_output;
mod agg_output_special;
mod coerce;
mod create_table;
mod expr;
mod functions;
mod geometry;
mod infer;
mod modify;
mod paths;
mod scope;
mod system_views;
mod views;

use crate::RelFileLocator;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::{
    AggAccum, AggFunc, BuiltinScalarFunction, Expr, JsonTableFunction, OrderByEntry, Plan,
    ProjectSetTarget, QueryColumn, RelationDesc, SetReturningCall, TargetEntry, ToastRelationRef,
    Value, cast_value,
};
use crate::backend::optimizer::optimize_bound_query;
use crate::include::catalog::{
    PgCastRow, PgClassRow, PgOperatorRow, PgProcRow, PgRewriteRow, PgStatisticRow, PgTypeRow,
    bootstrap_pg_cast_rows, bootstrap_pg_operator_rows, bootstrap_pg_proc_rows, builtin_type_rows,
};

use super::parsenodes::*;
pub use crate::backend::catalog::catalog::{Catalog, CatalogEntry};
use crate::backend::utils::cache::relcache::RelCache;
use crate::backend::utils::cache::system_views::{build_pg_stats_rows, build_pg_views_rows};
pub(crate) use crate::include::nodes::plannodes::{
    BoundFromPlan, BoundSelectPlan, DeferredSelectPlan,
};
use agg::*;
use agg_output::*;
use coerce::*;
pub use create_table::*;
use expr::*;
use functions::*;
use geometry::*;
use infer::*;
pub use modify::{
    BoundArraySubscript, BoundAssignment, BoundAssignmentTarget, BoundDeleteStatement,
    BoundInsertSource, BoundInsertStatement, BoundUpdateStatement, PreparedInsert, bind_delete,
    bind_insert, bind_insert_prepared, bind_update,
};
pub use paths::BoundModifyRowSource;
use paths::bind_order_by_items;
pub use scope::BoundRelation;
use scope::*;
use system_views::*;
use views::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundIndexRelation {
    pub name: String,
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub desc: RelationDesc,
    pub index_meta: crate::backend::utils::cache::relcache::IndexRelCacheEntry,
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

    fn rewrite_rows_for_relation(&self, _relation_oid: u32) -> Vec<PgRewriteRow> {
        Vec::new()
    }

    fn class_row_by_oid(&self, _relation_oid: u32) -> Option<PgClassRow> {
        None
    }

    fn statistic_rows_for_relation(&self, _relation_oid: u32) -> Vec<PgStatisticRow> {
        Vec::new()
    }

    fn pg_views_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn pg_stats_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }
}

impl CatalogLookup for Catalog {
    fn lookup_any_relation(&self, name: &str) -> Option<BoundRelation> {
        let relcache = RelCache::from_catalog(self);
        relcache.get_by_name(name).map(|entry| BoundRelation {
            rel: entry.rel,
            relation_oid: entry.relation_oid,
            toast: toast_relation_from_cache(&relcache, entry),
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
            .filter_map(|(name, entry)| {
                let index_meta = entry.index.as_ref()?;
                (index_meta.indrelid == relation_oid).then(|| BoundIndexRelation {
                    name: name.rsplit('.').next().unwrap_or(name).to_string(),
                    rel: entry.rel,
                    relation_oid: entry.relation_oid,
                    desc: entry.desc.clone(),
                    index_meta: index_meta.clone(),
                })
            })
            .collect()
    }

    fn rewrite_rows_for_relation(&self, relation_oid: u32) -> Vec<PgRewriteRow> {
        self.rewrite_rows_for_relation(relation_oid).to_vec()
    }

    fn class_row_by_oid(&self, relation_oid: u32) -> Option<PgClassRow> {
        let catcache = crate::backend::utils::cache::catcache::CatCache::from_catalog(self);
        catcache.class_by_oid(relation_oid).cloned()
    }

    fn statistic_rows_for_relation(&self, relation_oid: u32) -> Vec<PgStatisticRow> {
        let catcache = crate::backend::utils::cache::catcache::CatCache::from_catalog(self);
        catcache
            .statistic_rows()
            .into_iter()
            .filter(|row| row.starelid == relation_oid)
            .collect()
    }

    fn pg_views_rows(&self) -> Vec<Vec<Value>> {
        let catcache = crate::backend::utils::cache::catcache::CatCache::from_catalog(self);
        build_pg_views_rows(
            catcache.namespace_rows(),
            catcache.class_rows(),
            catcache.rewrite_rows(),
        )
    }

    fn pg_stats_rows(&self) -> Vec<Vec<Value>> {
        let catcache = crate::backend::utils::cache::catcache::CatCache::from_catalog(self);
        build_pg_stats_rows(
            catcache.namespace_rows(),
            catcache.class_rows(),
            catcache.attribute_rows(),
            catcache.statistic_rows(),
        )
    }
}

impl CatalogLookup for RelCache {
    fn lookup_any_relation(&self, name: &str) -> Option<BoundRelation> {
        self.get_by_name(name).map(|entry| BoundRelation {
            rel: entry.rel,
            relation_oid: entry.relation_oid,
            toast: toast_relation_from_cache(self, entry),
            namespace_oid: entry.namespace_oid,
            relpersistence: entry.relpersistence,
            relkind: entry.relkind,
            desc: entry.desc.clone(),
        })
    }

    fn index_relations_for_heap(&self, relation_oid: u32) -> Vec<BoundIndexRelation> {
        self.entries()
            .filter_map(|(name, entry)| {
                let index_meta = entry.index.as_ref()?;
                (index_meta.indrelid == relation_oid).then(|| BoundIndexRelation {
                    name: name.rsplit('.').next().unwrap_or(name).to_string(),
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

fn toast_relation_from_cache(
    relcache: &RelCache,
    entry: &crate::backend::utils::cache::relcache::RelCacheEntry,
) -> Option<ToastRelationRef> {
    let toast_oid = entry.reltoastrelid;
    (toast_oid != 0)
        .then(|| relcache.get_by_oid(toast_oid))
        .flatten()
        .map(|toast| ToastRelationRef {
            rel: toast.rel,
            relation_oid: toast.relation_oid,
        })
}

#[derive(Default)]
struct LiteralDefaultCatalog;

impl CatalogLookup for LiteralDefaultCatalog {
    fn lookup_any_relation(&self, _name: &str) -> Option<BoundRelation> {
        None
    }
}

fn literal_sql_expr_value(expr: &SqlExpr) -> Option<Value> {
    match expr {
        SqlExpr::Const(value) => Some(value.clone()),
        SqlExpr::IntegerLiteral(value) => Some(Value::Text(value.clone().into())),
        SqlExpr::NumericLiteral(value) => Some(Value::Text(value.clone().into())),
        SqlExpr::UnaryPlus(inner) => literal_sql_expr_value(inner),
        SqlExpr::Negate(inner) => match literal_sql_expr_value(inner)? {
            Value::Text(text) => Some(Value::Text(format!("-{}", text.as_str()).into())),
            Value::TextRef(_, _) => None,
            Value::Int16(v) => Some(Value::Int16(-v)),
            Value::Int32(v) => Some(Value::Int32(-v)),
            Value::Int64(v) => Some(Value::Int64(-v)),
            Value::Float64(v) => Some(Value::Float64(-v)),
            Value::Numeric(v) => Some(Value::Numeric(v.negate())),
            _ => None,
        },
        SqlExpr::Cast(inner, ty) => {
            let inner = literal_sql_expr_value(inner)?;
            cast_value(inner, *ty).ok()
        }
        SqlExpr::ArrayLiteral(items) => {
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                values.push(literal_sql_expr_value(item)?);
            }
            Some(Value::Array(values))
        }
        _ => None,
    }
}

pub fn derive_literal_default_value(sql: &str, target: SqlType) -> Result<Value, ParseError> {
    let parsed = crate::backend::parser::parse_expr(sql)?;
    let value = if let Some(value) = literal_sql_expr_value(&parsed) {
        value
    } else {
        let catalog = LiteralDefaultCatalog;
        let (bound, from_type) = bind_scalar_expr_in_scope(&parsed, &[], &catalog)?;
        if matches!(bound, Expr::Column(_) | Expr::OuterColumn { .. }) {
            return Err(ParseError::UnexpectedToken {
                expected: "literal DEFAULT expression",
                actual: sql.to_string(),
            });
        }
        match cast_value(
            match bound {
                Expr::Const(value) => value,
                _ => {
                    return Err(ParseError::UnexpectedToken {
                        expected: "literal DEFAULT expression",
                        actual: sql.to_string(),
                    });
                }
            },
            if from_type == target { target } else { target },
        ) {
            Ok(value) => value,
            Err(_) => {
                return Err(ParseError::UnexpectedToken {
                    expected: "literal DEFAULT expression",
                    actual: sql.to_string(),
                });
            }
        }
    };
    cast_value(value, target).map_err(|_| ParseError::UnexpectedToken {
        expected: "literal DEFAULT expression",
        actual: sql.to_string(),
    })
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
                relation_names: vec![],
                hidden_invalid_relation_names: vec![],
                hidden_missing_relation_names: vec![],
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

pub fn normalize_create_view_name(stmt: &CreateViewStatement) -> Result<String, ParseError> {
    normalize_create_table_name_parts(
        stmt.schema_name.as_deref(),
        &stmt.view_name,
        TablePersistence::Permanent,
        OnCommitAction::PreserveRows,
    )
    .map(|(name, _)| name)
}

fn relation_desc_from_bound_select_plan(plan: &BoundSelectPlan) -> RelationDesc {
    RelationDesc {
        columns: plan
            .columns()
            .into_iter()
            .map(|col| column_desc(col.name, col.sql_type, true))
            .collect(),
    }
}

fn apply_cte_column_names(
    plan: BoundSelectPlan,
    desc: RelationDesc,
    column_names: &[String],
) -> Result<(BoundSelectPlan, RelationDesc), ParseError> {
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
    let projection = BoundSelectPlan::Projection {
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
    expanded_views: &[u32],
) -> Result<Vec<BoundCte>, ParseError> {
    let mut bound = Vec::with_capacity(ctes.len());
    for cte in ctes {
        let mut visible = bound.clone();
        visible.extend_from_slice(outer_ctes);
        let (plan, desc) = match &cte.body {
            CteBody::Select(select) => {
                let (plan, _) = bind_select_query_with_outer(
                    select,
                    catalog,
                    outer_scopes,
                    grouped_outer.clone(),
                    &visible,
                    expanded_views,
                )?;
                let desc = relation_desc_from_bound_select_plan(&plan);
                apply_cte_column_names(plan, desc, &cte.column_names)?
            }
            CteBody::Values(values) => {
                let (plan, _) = bind_values_query_with_outer(
                    values,
                    catalog,
                    outer_scopes,
                    grouped_outer.clone(),
                    &visible,
                    expanded_views,
                )?;
                let desc = relation_desc_from_bound_select_plan(&plan);
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
        .create_table(table_name, create_relation_desc(stmt)?)
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
            crate::backend::catalog::catalog::CatalogError::UniqueViolation(name) => {
                let _ = name;
                ParseError::UnexpectedToken {
                    expected: "valid catalog state",
                    actual: "catalog error".into(),
                }
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
    build_plan_with_outer(stmt, catalog, &[], None, &[], &[])
}

pub fn build_values_plan(
    stmt: &ValuesStatement,
    catalog: &dyn CatalogLookup,
) -> Result<Plan, ParseError> {
    build_values_plan_with_outer(stmt, catalog, &[], None, &[], &[])
}

fn bind_values_query_with_outer(
    stmt: &ValuesStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<GroupedOuterScope>,
    outer_ctes: &[BoundCte],
    expanded_views: &[u32],
) -> Result<(BoundSelectPlan, BoundScope), ParseError> {
    let local_ctes = bind_ctes(
        &stmt.with,
        catalog,
        outer_scopes,
        grouped_outer.clone(),
        outer_ctes,
        expanded_views,
    )?;
    let mut visible_ctes = local_ctes;
    visible_ctes.extend_from_slice(outer_ctes);
    let (base, scope) = bind_values_rows(
        &stmt.rows,
        None,
        catalog,
        outer_scopes,
        grouped_outer.as_ref(),
        &visible_ctes,
    )?;
    let output_columns = base.columns();
    let mut plan = BoundSelectPlan::From(base);
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
        plan = BoundSelectPlan::OrderBy {
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
        plan = BoundSelectPlan::Limit {
            input: Box::new(plan),
            limit: stmt.limit,
            offset: stmt.offset.unwrap_or(0),
        };
    }

    Ok((plan, scope))
}

fn build_values_plan_with_outer(
    stmt: &ValuesStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<GroupedOuterScope>,
    outer_ctes: &[BoundCte],
    expanded_views: &[u32],
) -> Result<Plan, ParseError> {
    let (plan, _) = bind_values_query_with_outer(
        stmt,
        catalog,
        outer_scopes,
        grouped_outer,
        outer_ctes,
        expanded_views,
    )?;
    Ok(optimize_bound_query(plan, catalog))
}

fn bind_select_query_with_outer(
    stmt: &SelectStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<GroupedOuterScope>,
    outer_ctes: &[BoundCte],
    expanded_views: &[u32],
) -> Result<(BoundSelectPlan, BoundScope), ParseError> {
    let local_ctes = bind_ctes(
        &stmt.with,
        catalog,
        outer_scopes,
        grouped_outer.clone(),
        outer_ctes,
        expanded_views,
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
            expanded_views,
        )?
    } else {
        (BoundFromPlan::Result, empty_scope())
    };
    if let Some(predicate) = &stmt.where_clause {
        if expr_contains_agg(predicate) {
            return Err(ParseError::AggInWhere);
        }
    }

    let filtered_plan = if let Some(predicate) = &stmt.where_clause {
        BoundSelectPlan::Filter {
            input: Box::new(BoundSelectPlan::From(base)),
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
        BoundSelectPlan::From(base)
    };

    let needs_agg =
        !stmt.group_by.is_empty() || targets_contain_agg(&stmt.targets) || stmt.having.is_some();

    if needs_agg && select_targets_contain_set_returning_call(&stmt.targets) {
        return Err(ParseError::UnexpectedToken {
            expected: "select-list set-returning function in a non-aggregate query",
            actual: "set-returning function in aggregate query".into(),
        });
    }

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
        BoundSelectPlan::From(BoundFromPlan::Result)
    } else {
        filtered_plan
    };

    if needs_agg {
        let mut aggs: Vec<(AggFunc, Vec<SqlFunctionArg>, bool, bool)> = Vec::new();
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
            .map(|(func, args, distinct, func_variadic)| {
                if aggregate_args_are_named(args) {
                    return Err(ParseError::UnexpectedToken {
                        expected: "aggregate arguments without names",
                        actual: func.name().into(),
                    });
                }
                let arg_values: Vec<SqlExpr> = args.iter().map(|arg| arg.value.clone()).collect();
                validate_aggregate_arity(*func, &arg_values)?;
                let arg_types = arg_values
                    .iter()
                    .map(|e| {
                        infer_sql_expr_type_with_ctes(
                            e,
                            &scope,
                            catalog,
                            outer_scopes,
                            grouped_outer.as_ref(),
                            &visible_ctes,
                        )
                    })
                    .collect::<Vec<_>>();
                let resolved =
                    resolve_function_call(catalog, func.name(), &arg_types, *func_variadic).ok();
                Ok(AggAccum {
                    aggfnoid: resolved.as_ref().map(|call| call.proc_oid).unwrap_or(0),
                    agg_variadic: resolved
                        .as_ref()
                        .map(|call| call.func_variadic)
                        .unwrap_or(*func_variadic),
                    func: *func,
                    args: arg_values
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
                    sql_type: aggregate_sql_type(*func, arg_types.first().copied()),
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
        for (func, args, _, _) in &aggs {
            output_columns.push(QueryColumn {
                name: func.name().to_string(),
                sql_type: aggregate_sql_type(
                    *func,
                    args.first().map(|e| {
                        infer_sql_expr_type_with_ctes(
                            &e.value,
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

        plan = BoundSelectPlan::Aggregate {
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
            plan = BoundSelectPlan::OrderBy {
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
            plan = BoundSelectPlan::Limit {
                input: Box::new(plan),
                limit: stmt.limit,
                offset: stmt.offset.unwrap_or(0),
            };
        }

        Ok((
            BoundSelectPlan::Projection {
                input: Box::new(plan),
                targets,
            },
            scope,
        ))
    } else {
        let bound_targets = bind_select_targets(
            &stmt.targets,
            &scope,
            catalog,
            outer_scopes,
            grouped_outer.as_ref(),
            &visible_ctes,
        )?;

        match bound_targets {
            BoundSelectTargets::Plain(targets) => {
                if !stmt.order_by.is_empty() {
                    plan = BoundSelectPlan::OrderBy {
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
                    plan = BoundSelectPlan::Limit {
                        input: Box::new(plan),
                        limit: stmt.limit,
                        offset: stmt.offset.unwrap_or(0),
                    };
                }

                let is_identity = targets.len() == scope.columns.len()
                    && targets.iter().enumerate().all(|(i, t)| {
                        matches!(t.expr, Expr::Column(c) if c == i)
                            && t.name == scope.columns[i].output_name
                    });

                if is_identity {
                    Ok((plan, scope))
                } else {
                    Ok((
                        BoundSelectPlan::Projection {
                            input: Box::new(plan),
                            targets,
                        },
                        scope,
                    ))
                }
            }
            BoundSelectTargets::WithProjectSet {
                project_targets,
                final_targets,
            } => {
                plan = BoundSelectPlan::ProjectSet {
                    input: Box::new(plan),
                    targets: project_targets,
                };
                plan = BoundSelectPlan::Projection {
                    input: Box::new(plan),
                    targets: final_targets.clone(),
                };
                if !stmt.order_by.is_empty() {
                    plan = BoundSelectPlan::OrderBy {
                        input: Box::new(plan),
                        items: bind_order_by_items(&stmt.order_by, &final_targets, |expr| {
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
                    plan = BoundSelectPlan::Limit {
                        input: Box::new(plan),
                        limit: stmt.limit,
                        offset: stmt.offset.unwrap_or(0),
                    };
                }
                Ok((plan, scope))
            }
        }
    }
}

fn build_plan_with_outer(
    stmt: &SelectStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<GroupedOuterScope>,
    outer_ctes: &[BoundCte],
    expanded_views: &[u32],
) -> Result<Plan, ParseError> {
    let (plan, _) = bind_select_query_with_outer(
        stmt,
        catalog,
        outer_scopes,
        grouped_outer,
        outer_ctes,
        expanded_views,
    )?;
    Ok(optimize_bound_query(plan, catalog))
}
